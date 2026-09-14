//! Export and import file formats, without any Redis in sight.
//!
//! A `DUMP` payload only loads into a server whose RDB version is at least the
//! one that wrote it, and nothing but Redis can read one. The formats here
//! carry the data itself instead: JSON, JSON Lines, CSV, or a list of
//! `redis-cli` commands. They can be read, diffed and edited, and they load
//! into Redis, Valkey, KeyDB or Dragonfly of any version that has the type.
//!
//! Every Redis string is bytes. Text that is valid UTF-8 is written as text;
//! anything else is written as base64 (`{"base64": "..."}` in JSON, a
//! `base64:` prefix in CSV, `\xNN` escapes in a commands file), so no format
//! loses a byte.
//!
//! Reading values from the server lives in `redis_client`. This module only
//! turns a [`Record`] into text and text back into records.

use std::collections::HashMap;
use std::io::Write;

use anyhow::{Context, Result, anyhow, bail, ensure};
use base64::Engine as _;
use serde_json::{Map, Value as Json, json};

use crate::redis_client::{ExportEntry, ImportReport, encode_key};

/// Tag and version written at the top of a JSON document export.
pub const FORMAT_TAG: &str = "rediscope-export";
pub const FORMAT_VERSION: u64 = 1;
/// Most elements one written command carries, so a big collection becomes
/// many commands of a reasonable size instead of one enormous one.
pub const CHUNK: usize = 500;
/// The header row of a CSV export.
pub const CSV_HEADER: &str = "key,type,ttl_ms,field,value";
const BASE64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    /// `DUMP` payloads, hex encoded in JSON. Every type and detail survives,
    /// but only between servers with compatible RDB versions.
    Dump,
    /// One pretty-printed JSON document.
    Json,
    /// One compact JSON object per line.
    Jsonl,
    /// One row per element.
    Csv,
    /// `redis-cli` commands, one per line.
    Commands,
}

impl Format {
    pub const ALL: [Format; 5] = [
        Format::Dump,
        Format::Json,
        Format::Jsonl,
        Format::Csv,
        Format::Commands,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Dump => "dump",
            Self::Json => "json",
            Self::Jsonl => "jsonl",
            Self::Csv => "csv",
            Self::Commands => "commands",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|f| f.name().eq_ignore_ascii_case(name.trim()))
    }

    /// What the status line calls the format.
    pub fn label(self) -> &'static str {
        match self {
            Self::Dump => "DUMP payloads",
            Self::Json => "JSON",
            Self::Jsonl => "JSON Lines",
            Self::Csv => "CSV",
            Self::Commands => "redis-cli commands",
        }
    }

    /// The file extension an export in this format gets by default.
    pub fn extension(self) -> &'static str {
        match self {
            Self::Dump | Self::Json => "json",
            Self::Jsonl => "jsonl",
            Self::Csv => "csv",
            Self::Commands => "redis",
        }
    }
}

/// One key, read out of the server: its name, remaining life and value.
#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    pub key: Vec<u8>,
    /// Milliseconds left to live, `None` when the key does not expire.
    pub ttl_ms: Option<i64>,
    pub value: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// Strings, including HyperLogLogs, which are strings to Redis.
    String(Vec<u8>),
    Hash(Vec<(Vec<u8>, Vec<u8>)>),
    List(Vec<Vec<u8>>),
    Set(Vec<Vec<u8>>),
    ZSet(Vec<(Vec<u8>, f64)>),
    Stream(Vec<StreamEntry>),
    /// A RedisJSON document.
    Json(Json),
    TimeSeries(Series),
    VectorSet(VectorSet),
}

#[derive(Clone, Debug, PartialEq)]
pub struct StreamEntry {
    pub id: String,
    pub fields: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Series {
    pub samples: Vec<(i64, f64)>,
    pub labels: Vec<(String, String)>,
    pub retention_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct VectorSet {
    /// `VINFO`'s `quant-type`: `int8` (the default), `f32` or `bin`.
    pub quant: Option<String>,
    pub elements: Vec<VectorElement>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VectorElement {
    pub element: Vec<u8>,
    pub vector: Vec<f64>,
    /// The element's JSON attributes as stored, when it has any.
    pub attributes: Option<String>,
}

impl Value {
    /// The type name every format writes, the same one the key tree shows.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::String(_) => "string",
            Self::Hash(_) => "hash",
            Self::List(_) => "list",
            Self::Set(_) => "set",
            Self::ZSet(_) => "zset",
            Self::Stream(_) => "stream",
            Self::Json(_) => "json",
            Self::TimeSeries(_) => "timeseries",
            Self::VectorSet(_) => "vectorset",
        }
    }
}

// ---- writing ---------------------------------------------------------------

/// Writes records to a file one at a time, so an export never holds more
/// than one key's value in memory. [`Writer::finish`] closes the document.
pub struct Writer<W: Write> {
    out: W,
    format: Format,
    /// Commands format only: `DEL` each key before writing it.
    replace: bool,
    written: u64,
}

impl<W: Write> Writer<W> {
    pub fn new(mut out: W, format: Format, replace: bool) -> Result<Self> {
        ensure!(
            format != Format::Dump,
            "DUMP payloads are written by the dump export, not the record writer"
        );
        match format {
            Format::Json => write!(
                out,
                "{{\n  \"format\": \"{FORMAT_TAG}\",\n  \"version\": {FORMAT_VERSION},\n  \"keys\": ["
            )?,
            Format::Csv => writeln!(out, "{CSV_HEADER}")?,
            _ => {}
        }
        Ok(Self {
            out,
            format,
            replace,
            written: 0,
        })
    }

    pub fn write(&mut self, record: &Record) -> Result<()> {
        match self.format {
            Format::Json => {
                let text = serde_json::to_string_pretty(&record_json(record))?;
                let sep = if self.written == 0 { "\n" } else { ",\n" };
                // JSON strings never hold a raw newline, so indenting every
                // line of the entry cannot change a value.
                write!(self.out, "{sep}    {}", text.replace('\n', "\n    "))?;
            }
            Format::Jsonl => {
                serde_json::to_writer(&mut self.out, &record_json(record))?;
                self.out.write_all(b"\n")?;
            }
            Format::Csv => {
                for row in csv_rows(record) {
                    let line: Vec<String> = row.iter().map(|c| csv_quote(c).into_owned()).collect();
                    writeln!(self.out, "{}", line.join(","))?;
                }
            }
            Format::Commands => {
                for args in commands(record, self.replace) {
                    writeln!(self.out, "{}", command_line(&args))?;
                }
            }
            Format::Dump => unreachable!("refused in Writer::new"),
        }
        self.written += 1;
        Ok(())
    }

    /// How many records have been written.
    pub fn written(&self) -> u64 {
        self.written
    }

    pub fn finish(mut self) -> Result<W> {
        if self.format == Format::Json {
            let close = if self.written == 0 {
                "]\n}\n"
            } else {
                "\n  ]\n}\n"
            };
            self.out.write_all(close.as_bytes())?;
        }
        self.out.flush()?;
        Ok(self.out)
    }
}

/// Write old-style `DUMP` entries as the pretty JSON array they always were.
pub fn write_dump<W: Write>(mut out: W, entries: &[ExportEntry]) -> Result<W> {
    serde_json::to_writer_pretty(&mut out, entries)?;
    out.write_all(b"\n")?;
    out.flush()?;
    Ok(out)
}

// ---- JSON --------------------------------------------------------------------

/// A Redis string as JSON: itself when it is UTF-8, `{"base64": ...}` when not.
pub fn bytes_json(bytes: &[u8]) -> Json {
    match std::str::from_utf8(bytes) {
        Ok(text) => Json::String(text.to_string()),
        Err(_) => json!({ "base64": BASE64.encode(bytes) }),
    }
}

fn json_bytes(value: &Json, what: &str) -> Result<Vec<u8>> {
    match value {
        Json::String(s) => Ok(s.clone().into_bytes()),
        Json::Object(map) if map.len() == 1 => match map.get("base64") {
            Some(Json::String(b)) => BASE64
                .decode(b)
                .map_err(|e| anyhow!("{what} has invalid base64: {e}")),
            _ => bail!("{what} must be a string or {{\"base64\": \"...\"}}"),
        },
        // A number typed by hand into a list is still clearly its text.
        Json::Number(n) => Ok(n.to_string().into_bytes()),
        _ => bail!("{what} must be a string or {{\"base64\": \"...\"}}"),
    }
}

/// A double as Redis spells it, in a form it parses back exactly.
pub fn format_f64(f: f64) -> String {
    if f.is_nan() {
        "nan".into()
    } else if f.is_infinite() {
        if f > 0.0 { "inf".into() } else { "-inf".into() }
    } else if f == 0.0 || (1e-6..1e17).contains(&f.abs()) {
        format!("{f}")
    } else {
        // Rust never uses an exponent in `{}`; 1e-300 would be 300 digits.
        format!("{f:e}")
    }
}

fn f64_json(f: f64) -> Json {
    serde_json::Number::from_f64(f).map_or_else(|| Json::String(format_f64(f)), Json::Number)
}

fn json_f64(value: &Json, what: &str) -> Result<f64> {
    match value {
        Json::Number(n) => n.as_f64().ok_or_else(|| anyhow!("{what} is not a number")),
        Json::String(s) => parse_f64(s).ok_or_else(|| anyhow!("{what} is not a number: {s}")),
        _ => bail!("{what} must be a number"),
    }
}

/// A double as text. NaN is refused: Redis stores no such score or sample.
fn parse_f64(s: &str) -> Option<f64> {
    let s = s.trim();
    match s.to_ascii_lowercase().as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => Some(f64::INFINITY),
        "-inf" | "-infinity" => Some(f64::NEG_INFINITY),
        _ => s.parse::<f64>().ok().filter(|f| !f.is_nan()),
    }
}

/// Field/value pairs as an object when every field name is text and none
/// repeats, which is the readable shape. Otherwise as `[field, value]`
/// pairs, so a binary or repeated field name is not lost.
fn pairs_json(pairs: &[(Vec<u8>, Vec<u8>)]) -> Json {
    let mut map = Map::new();
    for (field, value) in pairs {
        let Ok(name) = std::str::from_utf8(field) else {
            return pairs_array(pairs);
        };
        if map.insert(name.to_string(), bytes_json(value)).is_some() {
            return pairs_array(pairs);
        }
    }
    Json::Object(map)
}

fn pairs_array(pairs: &[(Vec<u8>, Vec<u8>)]) -> Json {
    Json::Array(
        pairs
            .iter()
            .map(|(f, v)| json!([bytes_json(f), bytes_json(v)]))
            .collect(),
    )
}

fn json_pairs(value: &Json, what: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    match value {
        Json::Object(map) => map
            .iter()
            .map(|(k, v)| Ok((k.clone().into_bytes(), json_bytes(v, what)?)))
            .collect(),
        Json::Array(items) => items
            .iter()
            .map(|pair| match pair.as_array().map(Vec::as_slice) {
                Some([f, v]) => Ok((json_bytes(f, what)?, json_bytes(v, what)?)),
                _ => bail!("{what} must be an object or an array of [field, value] pairs"),
            })
            .collect(),
        _ => bail!("{what} must be an object or an array of [field, value] pairs"),
    }
}

/// Attributes that are a JSON object or array are embedded as JSON; anything
/// else stays the exact text it was.
fn attributes_json(text: &str) -> Json {
    match serde_json::from_str::<Json>(text) {
        Ok(v @ (Json::Object(_) | Json::Array(_))) => v,
        _ => Json::String(text.to_string()),
    }
}

fn json_attributes(value: Option<&Json>) -> Option<String> {
    match value? {
        Json::Null => None,
        Json::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// One record as the JSON object both JSON formats write.
pub fn record_json(record: &Record) -> Json {
    let value = match &record.value {
        Value::String(b) => bytes_json(b),
        Value::Hash(pairs) => pairs_json(pairs),
        Value::List(items) | Value::Set(items) => {
            Json::Array(items.iter().map(|i| bytes_json(i)).collect())
        }
        Value::ZSet(items) => Json::Array(
            items
                .iter()
                .map(|(m, s)| json!([bytes_json(m), f64_json(*s)]))
                .collect(),
        ),
        Value::Stream(entries) => Json::Array(
            entries
                .iter()
                .map(|e| json!({ "id": e.id, "fields": pairs_json(&e.fields) }))
                .collect(),
        ),
        Value::Json(doc) => doc.clone(),
        Value::TimeSeries(series) => {
            let mut obj = Map::new();
            obj.insert(
                "samples".into(),
                Json::Array(
                    series
                        .samples
                        .iter()
                        .map(|(t, v)| json!([t, f64_json(*v)]))
                        .collect(),
                ),
            );
            if !series.labels.is_empty() {
                obj.insert(
                    "labels".into(),
                    Json::Object(
                        series
                            .labels
                            .iter()
                            .map(|(k, v)| (k.clone(), Json::String(v.clone())))
                            .collect(),
                    ),
                );
            }
            if let Some(r) = series.retention_ms {
                obj.insert("retention_ms".into(), json!(r));
            }
            Json::Object(obj)
        }
        Value::VectorSet(set) => Json::Array(
            set.elements
                .iter()
                .map(|e| {
                    json!({
                        "element": bytes_json(&e.element),
                        "vector": e.vector.iter().map(|v| f64_json(*v)).collect::<Vec<_>>(),
                        "attributes": e.attributes.as_deref().map_or(Json::Null, attributes_json),
                    })
                })
                .collect(),
        ),
    };
    let mut obj = Map::new();
    obj.insert("key".into(), bytes_json(&record.key));
    obj.insert("type".into(), Json::String(record.value.type_name().into()));
    obj.insert(
        "ttl_ms".into(),
        record.ttl_ms.map_or(Json::Null, |t| json!(t)),
    );
    obj.insert("value".into(), value);
    if let Value::VectorSet(VectorSet { quant: Some(q), .. }) = &record.value {
        obj.insert("quant".into(), Json::String(q.clone()));
    }
    Json::Object(obj)
}

/// Read one record back from its JSON object.
pub fn json_record(entry: &Json) -> Result<Record> {
    let obj = entry
        .as_object()
        .ok_or_else(|| anyhow!("an entry must be a JSON object"))?;
    let key = json_bytes(
        obj.get("key")
            .ok_or_else(|| anyhow!("an entry has no \"key\""))?,
        "the key name",
    )?;
    let shown = encode_key(&key);
    let kind = obj
        .get("type")
        .and_then(Json::as_str)
        .ok_or_else(|| anyhow!("'{shown}' has no \"type\""))?;
    let ttl_ms = match obj.get("ttl_ms") {
        None | Some(Json::Null) => None,
        Some(t) => {
            let t = t
                .as_i64()
                .ok_or_else(|| anyhow!("'{shown}' has a ttl_ms that is not a whole number"))?;
            checked_ttl(t).map_err(|e| anyhow!("'{shown}' has {e}"))?
        }
    };
    let value = obj
        .get("value")
        .ok_or_else(|| anyhow!("'{shown}' has no \"value\""))?;
    let what = format!("a value in '{shown}'");
    let array = |v: &Json| -> Result<Vec<Json>> {
        v.as_array()
            .cloned()
            .ok_or_else(|| anyhow!("the value of '{shown}' must be an array"))
    };
    let value = match kind {
        "string" => Value::String(json_bytes(value, &what)?),
        "hash" => Value::Hash(json_pairs(value, &what)?),
        "list" => Value::List(
            array(value)?
                .iter()
                .map(|v| json_bytes(v, &what))
                .collect::<Result<_>>()?,
        ),
        "set" => Value::Set(
            array(value)?
                .iter()
                .map(|v| json_bytes(v, &what))
                .collect::<Result<_>>()?,
        ),
        "zset" => Value::ZSet(
            array(value)?
                .iter()
                .map(|pair| match pair.as_array().map(Vec::as_slice) {
                    Some([m, s]) => Ok((json_bytes(m, &what)?, json_f64(s, &what)?)),
                    _ => bail!("'{shown}' must hold [member, score] pairs"),
                })
                .collect::<Result<_>>()?,
        ),
        "stream" => Value::Stream(
            array(value)?
                .iter()
                .map(|e| {
                    let id = e
                        .get("id")
                        .and_then(Json::as_str)
                        .ok_or_else(|| anyhow!("a stream entry in '{shown}' has no id"))?;
                    let fields = e
                        .get("fields")
                        .ok_or_else(|| anyhow!("stream entry {id} in '{shown}' has no fields"))?;
                    Ok(StreamEntry {
                        id: id.to_string(),
                        fields: json_pairs(fields, &what)?,
                    })
                })
                .collect::<Result<_>>()?,
        ),
        "json" => Value::Json(value.clone()),
        "timeseries" => {
            let samples = value
                .get("samples")
                .map(&array)
                .transpose()?
                .unwrap_or_default()
                .iter()
                .map(|pair| match pair.as_array().map(Vec::as_slice) {
                    Some([t, v]) => Ok((
                        t.as_i64()
                            .ok_or_else(|| anyhow!("a timestamp in '{shown}' is not whole"))?,
                        json_f64(v, &what)?,
                    )),
                    _ => bail!("'{shown}' must hold [timestamp, value] samples"),
                })
                .collect::<Result<_>>()?;
            let labels = match value.get("labels") {
                None | Some(Json::Null) => Vec::new(),
                Some(Json::Object(map)) => map
                    .iter()
                    .map(|(k, v)| match v {
                        Json::String(s) => Ok((k.clone(), s.clone())),
                        other => Ok((k.clone(), other.to_string())),
                    })
                    .collect::<Result<_>>()?,
                Some(_) => bail!("the labels of '{shown}' must be an object"),
            };
            let retention_ms = value.get("retention_ms").and_then(Json::as_u64);
            Value::TimeSeries(Series {
                samples,
                labels,
                retention_ms,
            })
        }
        "vectorset" => Value::VectorSet(VectorSet {
            quant: obj.get("quant").and_then(Json::as_str).map(str::to_string),
            elements: array(value)?
                .iter()
                .map(|e| {
                    let element = json_bytes(
                        e.get("element")
                            .ok_or_else(|| anyhow!("an element of '{shown}' has no name"))?,
                        &what,
                    )?;
                    let vector = e
                        .get("vector")
                        .and_then(Json::as_array)
                        .ok_or_else(|| anyhow!("an element of '{shown}' has no vector"))?
                        .iter()
                        .map(|v| json_f64(v, &what))
                        .collect::<Result<Vec<f64>>>()?;
                    ensure!(
                        !vector.is_empty(),
                        "an element of '{shown}' has an empty vector"
                    );
                    Ok(VectorElement {
                        element,
                        vector,
                        attributes: json_attributes(e.get("attributes")),
                    })
                })
                .collect::<Result<_>>()?,
        }),
        other => bail!("'{shown}' has the type '{other}', which import cannot write"),
    };
    let record = Record { key, ttl_ms, value };
    validate(&record)?;
    Ok(record)
}

/// A TTL read from a file. `-1` is what `PTTL` and a dump file's `pttl` say
/// for a key that does not expire, so it means no expiry here too. Zero or
/// any other negative number would make the key vanish the moment it is
/// written, which is never what a file means, so it is refused.
///
/// The server adds a TTL to its own clock and refuses a sum that overflows,
/// and inside a transaction that refusal comes after the value was already
/// written, leaving the key without an expiry. So a TTL that far ahead is
/// refused here, with a day to spare for clocks that differ.
fn checked_ttl(t: i64) -> Result<Option<i64>> {
    const SPARE_MS: i64 = 24 * 60 * 60 * 1000;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
    let max = i64::MAX.saturating_sub(now_ms).saturating_sub(SPARE_MS);
    match t {
        -1 => Ok(None),
        t if t > max => bail!(
            "ttl_ms {t}, which is too far ahead for the server to hold: the expiry time would overflow its clock. Use at most {max}, or -1 or nothing for no expiry"
        ),
        t if t > 0 => Ok(Some(t)),
        t => bail!(
            "ttl_ms {t}, which would delete the key as it is written; use a positive number of milliseconds, or -1 or nothing for no expiry"
        ),
    }
}

/// A stream id as its two numbers, when it is written out in full.
fn stream_id(id: &str) -> Option<(u64, u64)> {
    let (ms, seq) = id.split_once('-')?;
    Some((ms.parse().ok()?, seq.parse().ok()?))
}

/// Check everything about a record that the server would otherwise refuse
/// halfway through writing it, so a bad record fails before its first
/// command is sent and never leaves a key half written or deleted.
pub fn validate(record: &Record) -> Result<()> {
    let shown = encode_key(&record.key);
    if let Some(t) = record.ttl_ms {
        checked_ttl(t).map_err(|e| anyhow!("'{shown}' has {e}"))?;
    }
    match &record.value {
        Value::ZSet(items) => {
            if let Some((member, _)) = items.iter().find(|(_, s)| s.is_nan()) {
                bail!(
                    "'{shown}' gives member '{}' a score that is not a number",
                    encode_key(member)
                );
            }
        }
        Value::Stream(entries) => {
            let mut last = (0u64, 0u64);
            for entry in entries {
                let id = stream_id(&entry.id).ok_or_else(|| {
                    anyhow!(
                        "stream entry '{}' in '{shown}' is not an id like 1700000000000-0",
                        entry.id
                    )
                })?;
                ensure!(
                    id > last,
                    "stream entry {} in '{shown}' does not come after the entry before it (ids must grow, starting above 0-0)",
                    entry.id
                );
                last = id;
            }
        }
        Value::TimeSeries(series) => {
            let mut seen = std::collections::HashSet::with_capacity(series.samples.len());
            for (t, v) in &series.samples {
                ensure!(*t >= 0, "'{shown}' has a sample at negative timestamp {t}");
                ensure!(
                    !v.is_nan(),
                    "'{shown}' has a sample at {t} that is not a number"
                );
                ensure!(
                    seen.insert(*t),
                    "'{shown}' has two samples at timestamp {t}"
                );
            }
        }
        Value::VectorSet(set) => {
            let dims = set.elements.first().map_or(0, |e| e.vector.len());
            for e in &set.elements {
                let name = encode_key(&e.element);
                ensure!(
                    !e.vector.is_empty(),
                    "element '{name}' of '{shown}' has an empty vector"
                );
                ensure!(
                    e.vector.len() == dims,
                    "element '{name}' of '{shown}' has {} dimensions, but the first element has {dims}",
                    e.vector.len()
                );
                ensure!(
                    e.vector.iter().all(|v| !v.is_nan()),
                    "element '{name}' of '{shown}' has a vector value that is not a number"
                );
            }
        }
        Value::String(_) | Value::Hash(_) | Value::List(_) | Value::Set(_) | Value::Json(_) => {}
    }
    Ok(())
}

// ---- CSV ---------------------------------------------------------------------

/// A CSV cell for a Redis string: the text itself, or `base64:` and the
/// bytes in base64 when they are not UTF-8. Text that itself starts with
/// `base64:` is encoded too, so reading a cell back is never ambiguous.
pub fn csv_cell(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) if !text.starts_with("base64:") => text.to_string(),
        _ => format!("base64:{}", BASE64.encode(bytes)),
    }
}

pub fn csv_cell_bytes(cell: &str) -> Result<Vec<u8>> {
    match cell.strip_prefix("base64:") {
        Some(b) => BASE64
            .decode(b)
            .map_err(|e| anyhow!("a base64: cell is not valid base64: {e}")),
        None => Ok(cell.as_bytes().to_vec()),
    }
}

/// RFC 4180 quoting: a cell holding a comma, a quote or a line break is
/// wrapped in quotes, with its quotes doubled.
pub fn csv_quote(cell: &str) -> std::borrow::Cow<'_, str> {
    if cell.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", cell.replace('"', "\"\"")).into()
    } else {
        cell.into()
    }
}

/// The rows of one record: key, type, ttl_ms, field and value, unquoted.
pub fn csv_rows(record: &Record) -> Vec<[String; 5]> {
    let key = csv_cell(&record.key);
    let kind = record.value.type_name().to_string();
    let ttl = record.ttl_ms.map(|t| t.to_string()).unwrap_or_default();
    let row = |field: String, value: String| [key.clone(), kind.clone(), ttl.clone(), field, value];
    match &record.value {
        Value::String(b) => vec![row(String::new(), csv_cell(b))],
        Value::Hash(pairs) => pairs
            .iter()
            .map(|(f, v)| row(csv_cell(f), csv_cell(v)))
            .collect(),
        Value::List(items) => items
            .iter()
            .enumerate()
            .map(|(i, v)| row(i.to_string(), csv_cell(v)))
            .collect(),
        Value::Set(items) => items
            .iter()
            .map(|m| row(String::new(), csv_cell(m)))
            .collect(),
        Value::ZSet(items) => items
            .iter()
            .map(|(m, s)| row(csv_cell(m), format_f64(*s)))
            .collect(),
        Value::Stream(entries) => entries
            .iter()
            .flat_map(|e| {
                e.fields.iter().map(|(f, v)| {
                    let mut field = format!("{}:", e.id).into_bytes();
                    field.extend_from_slice(f);
                    row(csv_cell(&field), csv_cell(v))
                })
            })
            .collect(),
        Value::Json(doc) => vec![row(String::new(), doc.to_string())],
        Value::TimeSeries(series) => series
            .samples
            .iter()
            .map(|(t, v)| row(t.to_string(), format_f64(*v)))
            .collect(),
        Value::VectorSet(set) => set
            .elements
            .iter()
            .map(|e| {
                let value = json!({
                    "vector": e.vector.iter().map(|v| f64_json(*v)).collect::<Vec<_>>(),
                    "attributes": e.attributes.as_deref().map_or(Json::Null, attributes_json),
                });
                row(csv_cell(&e.element), value.to_string())
            })
            .collect(),
    }
}

/// Split CSV text into rows of cells, with the line each row starts on.
pub fn parse_csv(text: &str) -> Result<Vec<(usize, Vec<String>)>> {
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut cell = String::new();
    let mut quoted = false;
    // Whether the current row has anything in it, so a blank line is skipped.
    let mut started = false;
    let mut line = 1;
    let mut row_line = 1;
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if quoted {
            match ch {
                '"' if chars.peek() == Some(&'"') => {
                    chars.next();
                    cell.push('"');
                }
                '"' => quoted = false,
                '\n' => {
                    line += 1;
                    cell.push(ch);
                }
                _ => cell.push(ch),
            }
            continue;
        }
        match ch {
            '"' if cell.is_empty() => {
                quoted = true;
                started = true;
            }
            ',' => {
                row.push(std::mem::take(&mut cell));
                started = true;
            }
            '\r' if chars.peek() == Some(&'\n') => {}
            '\n' => {
                if started || !cell.is_empty() {
                    row.push(std::mem::take(&mut cell));
                    rows.push((row_line, std::mem::take(&mut row)));
                }
                started = false;
                line += 1;
                row_line = line;
            }
            _ => {
                cell.push(ch);
                started = true;
            }
        }
    }
    ensure!(!quoted, "line {row_line}: a quoted cell is never closed");
    if started || !cell.is_empty() {
        row.push(cell);
        rows.push((row_line, row));
    }
    Ok(rows)
}

/// Read a CSV export back into records. Every row with the same key belongs
/// to one record, wherever it is in the file: a spreadsheet sorted by value
/// splits a key's rows up, and that must not lose any of them. Records keep
/// the order their keys first appear in, and elements the order of their
/// rows, except list items, which follow their index column.
pub fn csv_records(text: &str) -> Result<Vec<Record>> {
    struct Pending {
        record: Record,
        kind: String,
        ttl: String,
        line: usize,
        /// List items' index cells, parallel to the items.
        list_index: Vec<Option<u64>>,
        /// Stream entry ids to their place, so a split entry joins up.
        stream_ids: HashMap<String, usize>,
    }
    let rows = parse_csv(text)?;
    let mut rows = rows.into_iter();
    match rows.next() {
        Some((_, header)) if header.join(",") == CSV_HEADER => {}
        _ => bail!("a CSV import must start with the header {CSV_HEADER}"),
    }
    let mut records: Vec<Pending> = Vec::new();
    let mut by_key: HashMap<Vec<u8>, usize> = HashMap::new();
    for (line, cells) in rows {
        let [key, kind, ttl, field, value]: [String; 5] =
            cells.try_into().map_err(|c: Vec<String>| {
                anyhow!("line {line}: expected 5 cells, found {}", c.len())
            })?;
        let at = |e: anyhow::Error| anyhow!("line {line}: {e}");
        let name = csv_cell_bytes(&key).map_err(at)?;
        let index = match by_key.get(&name) {
            Some(&i) => {
                let first = &records[i];
                let shown = encode_key(&name);
                ensure!(
                    first.kind == kind,
                    "line {line}: '{shown}' is a {} on line {} but a {kind} here",
                    first.kind,
                    first.line
                );
                ensure!(
                    first.ttl.trim() == ttl.trim(),
                    "line {line}: '{shown}' has ttl_ms '{}' on line {} but '{ttl}' here",
                    first.ttl,
                    first.line
                );
                i
            }
            None => {
                let ttl_ms = if ttl.trim().is_empty() {
                    None
                } else {
                    let t = ttl.trim().parse::<i64>().map_err(|_| {
                        anyhow!("line {line}: ttl_ms '{ttl}' is not a whole number")
                    })?;
                    checked_ttl(t).map_err(at)?
                };
                let value = match kind.as_str() {
                    "string" => Value::String(Vec::new()),
                    "hash" => Value::Hash(Vec::new()),
                    "list" => Value::List(Vec::new()),
                    "set" => Value::Set(Vec::new()),
                    "zset" => Value::ZSet(Vec::new()),
                    "stream" => Value::Stream(Vec::new()),
                    "json" => Value::Json(Json::Null),
                    "timeseries" => Value::TimeSeries(Series::default()),
                    "vectorset" => Value::VectorSet(VectorSet::default()),
                    other => bail!("line {line}: the type '{other}' cannot be imported"),
                };
                records.push(Pending {
                    record: Record {
                        key: name.clone(),
                        ttl_ms,
                        value,
                    },
                    kind: kind.clone(),
                    ttl: ttl.clone(),
                    line,
                    list_index: Vec::new(),
                    stream_ids: HashMap::new(),
                });
                by_key.insert(name, records.len() - 1);
                records.len() - 1
            }
        };
        let pending = &mut records[index];
        match &mut pending.record.value {
            Value::String(b) => {
                ensure!(
                    pending.line == line,
                    "line {line}: a string key has one row"
                );
                *b = csv_cell_bytes(&value).map_err(at)?;
            }
            Value::Hash(pairs) => pairs.push((
                csv_cell_bytes(&field).map_err(at)?,
                csv_cell_bytes(&value).map_err(at)?,
            )),
            Value::List(items) => {
                items.push(csv_cell_bytes(&value).map_err(at)?);
                pending.list_index.push(field.trim().parse().ok());
            }
            Value::Set(items) => items.push(csv_cell_bytes(&value).map_err(at)?),
            Value::ZSet(items) => items.push((
                csv_cell_bytes(&field).map_err(at)?,
                parse_f64(&value)
                    .ok_or_else(|| anyhow!("line {line}: score '{value}' is not a number"))?,
            )),
            Value::Stream(entries) => {
                let bytes = csv_cell_bytes(&field).map_err(at)?;
                let split = bytes
                    .iter()
                    .position(|b| *b == b':')
                    .ok_or_else(|| anyhow!("line {line}: a stream field must be id:field"))?;
                let id = String::from_utf8(bytes[..split].to_vec())
                    .map_err(|_| anyhow!("line {line}: the stream id is not text"))?;
                let pair = (
                    bytes[split + 1..].to_vec(),
                    csv_cell_bytes(&value).map_err(at)?,
                );
                match pending.stream_ids.get(&id) {
                    Some(&i) => entries[i].fields.push(pair),
                    None => {
                        pending.stream_ids.insert(id.clone(), entries.len());
                        entries.push(StreamEntry {
                            id,
                            fields: vec![pair],
                        });
                    }
                }
            }
            Value::Json(doc) => {
                ensure!(pending.line == line, "line {line}: a json key has one row");
                *doc = serde_json::from_str(&value)
                    .map_err(|e| anyhow!("line {line}: the JSON document does not parse: {e}"))?;
            }
            Value::TimeSeries(series) => series.samples.push((
                field
                    .trim()
                    .parse()
                    .map_err(|_| anyhow!("line {line}: timestamp '{field}' is not whole"))?,
                parse_f64(&value)
                    .ok_or_else(|| anyhow!("line {line}: sample '{value}' is not a number"))?,
            )),
            Value::VectorSet(set) => {
                let parsed: Json = serde_json::from_str(&value)
                    .map_err(|e| anyhow!("line {line}: the element does not parse: {e}"))?;
                let vector = parsed
                    .get("vector")
                    .and_then(Json::as_array)
                    .ok_or_else(|| anyhow!("line {line}: the element has no vector"))?
                    .iter()
                    .map(|v| json_f64(v, "a vector value"))
                    .collect::<Result<Vec<f64>>>()
                    .map_err(at)?;
                set.elements.push(VectorElement {
                    element: csv_cell_bytes(&field).map_err(at)?,
                    vector,
                    attributes: json_attributes(parsed.get("attributes")),
                });
            }
        }
    }
    records
        .into_iter()
        .map(|mut pending| {
            match &mut pending.record.value {
                // Rows follow the index column when every row has one, so a
                // reordered sheet still gives the list back in order.
                Value::List(items) if pending.list_index.iter().all(Option::is_some) => {
                    let mut indexed: Vec<(u64, Vec<u8>)> = pending
                        .list_index
                        .iter()
                        .map(|i| i.unwrap_or_default())
                        .zip(std::mem::take(items))
                        .collect();
                    indexed.sort_by_key(|(i, _)| *i);
                    *items = indexed.into_iter().map(|(_, v)| v).collect();
                }
                Value::Stream(entries) if entries.iter().all(|e| stream_id(&e.id).is_some()) => {
                    entries.sort_by_key(|e| stream_id(&e.id));
                }
                _ => {}
            }
            validate(&pending.record).map_err(|e| anyhow!("line {}: {e}", pending.line))?;
            Ok(pending.record)
        })
        .collect()
}

// ---- commands ----------------------------------------------------------------

/// The commands that write one record, each as its arguments. `replace`
/// starts with `DEL`. Collections go in [`CHUNK`]s, and a TTL comes last so
/// the key only starts to expire once it holds everything.
pub fn commands(record: &Record, replace: bool) -> Vec<Vec<Vec<u8>>> {
    let mut out: Vec<Vec<Vec<u8>>> = Vec::new();
    if replace {
        out.push(vec![b"DEL".to_vec(), record.key.clone()]);
    }
    let writes = write_commands(record, &record.key);
    if writes.is_empty() {
        return out;
    }
    out.extend(writes);
    if let Some(ttl) = record.ttl_ms {
        out.push(ttl_command(&record.key, ttl));
    }
    out
}

/// `PEXPIRE` for a TTL. Zero would delete the key on arrival; a key that
/// had moments left gets one millisecond, not none.
pub fn ttl_command(key: &[u8], ttl_ms: i64) -> Vec<Vec<u8>> {
    vec![
        b"PEXPIRE".to_vec(),
        key.to_vec(),
        ttl_ms.max(1).to_string().into_bytes(),
    ]
}

/// The commands that write a record's value into `key`, without the `DEL`
/// or the TTL. Import writes a big value into a temporary name this way and
/// renames it into place at the end.
pub fn write_commands(record: &Record, key: &[u8]) -> Vec<Vec<Vec<u8>>> {
    let key = key.to_vec();
    let mut out: Vec<Vec<Vec<u8>>> = Vec::new();
    let head = |name: &str| vec![name.as_bytes().to_vec(), key.clone()];
    match &record.value {
        Value::String(b) => {
            let mut c = head("SET");
            c.push(b.clone());
            out.push(c);
        }
        Value::Hash(pairs) => {
            for chunk in pairs.chunks(CHUNK) {
                let mut c = head("HSET");
                for (f, v) in chunk {
                    c.push(f.clone());
                    c.push(v.clone());
                }
                out.push(c);
            }
        }
        Value::List(items) | Value::Set(items) => {
            let name = if matches!(record.value, Value::List(_)) {
                "RPUSH"
            } else {
                "SADD"
            };
            for chunk in items.chunks(CHUNK) {
                let mut c = head(name);
                c.extend(chunk.iter().cloned());
                out.push(c);
            }
        }
        Value::ZSet(items) => {
            for chunk in items.chunks(CHUNK) {
                let mut c = head("ZADD");
                for (m, s) in chunk {
                    c.push(format_f64(*s).into_bytes());
                    c.push(m.clone());
                }
                out.push(c);
            }
        }
        Value::Stream(entries) => {
            for e in entries.iter().filter(|e| !e.fields.is_empty()) {
                let mut c = head("XADD");
                c.push(e.id.clone().into_bytes());
                for (f, v) in &e.fields {
                    c.push(f.clone());
                    c.push(v.clone());
                }
                out.push(c);
            }
        }
        Value::Json(doc) => {
            let mut c = head("JSON.SET");
            c.push(b"$".to_vec());
            c.push(doc.to_string().into_bytes());
            out.push(c);
        }
        Value::TimeSeries(series) => {
            let mut c = head("TS.CREATE");
            if let Some(r) = series.retention_ms {
                c.push(b"RETENTION".to_vec());
                c.push(r.to_string().into_bytes());
            }
            if !series.labels.is_empty() {
                c.push(b"LABELS".to_vec());
                for (k, v) in &series.labels {
                    c.push(k.clone().into_bytes());
                    c.push(v.clone().into_bytes());
                }
            }
            out.push(c);
            for chunk in series.samples.chunks(CHUNK) {
                let mut c = vec![b"TS.MADD".to_vec()];
                for (t, v) in chunk {
                    c.push(key.clone());
                    c.push(t.to_string().into_bytes());
                    c.push(format_f64(*v).into_bytes());
                }
                out.push(c);
            }
        }
        Value::VectorSet(set) => {
            for e in &set.elements {
                let mut c = head("VADD");
                c.push(b"VALUES".to_vec());
                c.push(e.vector.len().to_string().into_bytes());
                c.extend(e.vector.iter().map(|v| format_f64(*v).into_bytes()));
                c.push(e.element.clone());
                match set.quant.as_deref() {
                    Some("f32") => c.push(b"NOQUANT".to_vec()),
                    Some("bin") => c.push(b"BIN".to_vec()),
                    _ => {}
                }
                if let Some(attrs) = &e.attributes {
                    c.push(b"SETATTR".to_vec());
                    c.push(attrs.clone().into_bytes());
                }
                out.push(c);
            }
        }
    }
    out
}

/// Most commands one pipeline carries, and roughly the most bytes. A big
/// key is sent in several pipelines of this size instead of one that could
/// outlast the server's reply timeout.
pub const PIPELINE_COMMANDS: usize = 256;
pub const PIPELINE_BYTES: usize = 1 << 20;
/// A `VADD` builds graph links as it inserts, so it counts as this many.
const VADD_WEIGHT: usize = 4;

/// Split commands into consecutive runs that each fit one pipeline: at most
/// [`PIPELINE_COMMANDS`] (a `VADD` counting as several) and about
/// [`PIPELINE_BYTES`]. A single command bigger than that goes alone.
pub fn pipeline_chunks<C: AsRef<[Vec<u8>]>>(cmds: &[C]) -> Vec<std::ops::Range<usize>> {
    let mut out = Vec::new();
    let (mut start, mut weight, mut bytes) = (0, 0, 0);
    for (i, cmd) in cmds.iter().enumerate() {
        let cmd = cmd.as_ref();
        let w = if cmd
            .first()
            .is_some_and(|name| name.eq_ignore_ascii_case(b"VADD"))
        {
            VADD_WEIGHT
        } else {
            1
        };
        let b: usize = cmd.iter().map(|a| a.len() + 16).sum();
        if i > start && (weight + w > PIPELINE_COMMANDS || bytes + b > PIPELINE_BYTES) {
            out.push(start..i);
            (start, weight, bytes) = (i, 0, 0);
        }
        weight += w;
        bytes += b;
    }
    if start < cmds.len() {
        out.push(start..cmds.len());
    }
    out
}

/// The keys a data command writes, for counting the keys a commands file
/// touches.
pub fn command_keys(args: &[Vec<u8>]) -> Vec<&[u8]> {
    let name = args
        .first()
        .map(|n| n.to_ascii_uppercase())
        .unwrap_or_default();
    let rest = args.get(1..).unwrap_or_default();
    match name.as_slice() {
        b"MSET" => rest.iter().step_by(2).map(Vec::as_slice).collect(),
        b"TS.MADD" => rest.iter().step_by(3).map(Vec::as_slice).collect(),
        b"DEL" | b"UNLINK" => rest.iter().map(Vec::as_slice).collect(),
        _ => rest.first().map(Vec::as_slice).into_iter().collect(),
    }
}

/// One argument quoted the way `redis-cli` reads it: bare when it is plain
/// ASCII, otherwise in double quotes with `\"`, `\\`, `\n`, `\r`, `\t` and
/// `\xNN` escapes. UTF-8 text stays readable inside the quotes.
pub fn quote_arg(arg: &[u8]) -> String {
    let plain = !arg.is_empty()
        && arg
            .iter()
            .all(|b| b.is_ascii_graphic() && !matches!(b, b'"' | b'\'' | b'\\'));
    if plain {
        return String::from_utf8_lossy(arg).into_owned();
    }
    use std::fmt::Write as _;
    let mut out = String::with_capacity(arg.len() + 2);
    out.push('"');
    let mut rest = arg;
    while !rest.is_empty() {
        let (text, bad) = match std::str::from_utf8(rest) {
            Ok(text) => (text, 0),
            Err(e) => (
                // Safe: valid_up_to is a char boundary of valid UTF-8.
                std::str::from_utf8(&rest[..e.valid_up_to()]).unwrap_or_default(),
                e.error_len().unwrap_or(rest.len() - e.valid_up_to()),
            ),
        };
        for ch in text.chars() {
            match ch {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                c if c.is_control() => {
                    let mut buf = [0u8; 4];
                    for b in c.encode_utf8(&mut buf).bytes() {
                        let _ = write!(out, "\\x{b:02x}");
                    }
                }
                c => out.push(c),
            }
        }
        let start = text.len();
        for b in &rest[start..start + bad] {
            let _ = write!(out, "\\x{b:02x}");
        }
        rest = &rest[start + bad..];
    }
    out.push('"');
    out
}

/// A whole command as one `redis-cli` line.
pub fn command_line(args: &[Vec<u8>]) -> String {
    args.iter()
        .map(|a| quote_arg(a))
        .collect::<Vec<_>>()
        .join(" ")
}

fn is_hex(b: u8) -> bool {
    b.is_ascii_hexdigit()
}

fn hex_value(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        _ => b - b'A' + 10,
    }
}

/// C's `isspace`: what `sdssplitargs` skips between arguments and wants
/// after a closing quote. Unlike Rust's ASCII whitespace it has `\v`.
fn is_c_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// Split one line into arguments exactly as `redis-cli` does (its
/// `sdssplitargs`): double quotes take `\xNN`, `\n`, `\r`, `\t`, `\b`, `\a`
/// and escaped characters; single quotes take only `\'`; a closing quote must
/// be followed by a space or the end of the line. Any of C's spaces separate
/// arguments, but a bare argument only ends at a space, tab, CR or LF, so a
/// form feed or vertical tab inside one is kept, as `redis-cli` keeps it.
pub fn split_line(line: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut args = Vec::new();
    let mut i = 0;
    let n = line.len();
    loop {
        while i < n && is_c_space(line[i]) {
            i += 1;
        }
        if i >= n {
            return Ok(args);
        }
        let mut current = Vec::new();
        let mut in_double = false;
        let mut in_single = false;
        loop {
            if in_double {
                ensure!(i < n, "unbalanced double quote");
                let c = line[i];
                if c == b'\\'
                    && i + 3 < n
                    && line[i + 1] == b'x'
                    && is_hex(line[i + 2])
                    && is_hex(line[i + 3])
                {
                    current.push(hex_value(line[i + 2]) * 16 + hex_value(line[i + 3]));
                    i += 4;
                    continue;
                }
                if c == b'\\' && i + 1 < n {
                    current.push(match line[i + 1] {
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'b' => 0x08,
                        b'a' => 0x07,
                        other => other,
                    });
                    i += 2;
                    continue;
                }
                if c == b'"' {
                    ensure!(
                        i + 1 >= n || is_c_space(line[i + 1]),
                        "a closing quote must be followed by a space"
                    );
                    i += 1;
                    break;
                }
                current.push(c);
                i += 1;
            } else if in_single {
                ensure!(i < n, "unbalanced single quote");
                let c = line[i];
                if c == b'\\' && i + 1 < n && line[i + 1] == b'\'' {
                    current.push(b'\'');
                    i += 2;
                    continue;
                }
                if c == b'\'' {
                    ensure!(
                        i + 1 >= n || is_c_space(line[i + 1]),
                        "a closing quote must be followed by a space"
                    );
                    i += 1;
                    break;
                }
                current.push(c);
                i += 1;
            } else {
                if i >= n || matches!(line[i], b' ' | b'\t' | b'\n' | b'\r') {
                    break;
                }
                match line[i] {
                    b'"' => in_double = true,
                    b'\'' => in_single = true,
                    c => current.push(c),
                }
                i += 1;
            }
        }
        args.push(current);
    }
}

/// One non-empty line of a commands file.
#[derive(Clone, Debug, PartialEq)]
pub struct CommandLine {
    /// 1-based, for error messages.
    pub line: usize,
    pub args: Vec<Vec<u8>>,
}

/// The commands a commands file may hold: ones that write data into a key.
/// Anything else, `FLUSHALL` or `CONFIG SET` or a script, is refused before
/// the first command is sent.
pub const DATA_COMMANDS: &[&str] = &[
    "SET",
    "SETEX",
    "PSETEX",
    "MSET",
    "APPEND",
    "HSET",
    "HMSET",
    "RPUSH",
    "LPUSH",
    "SADD",
    "ZADD",
    "XADD",
    "PFADD",
    "GEOADD",
    "JSON.SET",
    "TS.CREATE",
    "TS.ADD",
    "TS.MADD",
    "VADD",
    "EXPIRE",
    "PEXPIRE",
    "EXPIREAT",
    "PEXPIREAT",
    "PERSIST",
    "DEL",
    "UNLINK",
];

pub fn parse_commands(bytes: &[u8]) -> Result<Vec<CommandLine>> {
    let mut out = Vec::new();
    for (i, raw) in bytes.split(|b| *b == b'\n').enumerate() {
        let line = i + 1;
        let raw = raw.strip_suffix(b"\r").unwrap_or(raw);
        let args = split_line(raw).map_err(|e| anyhow!("line {line}: {e}"))?;
        let Some(head) = args.first() else {
            continue;
        };
        let name = String::from_utf8_lossy(head).to_ascii_uppercase();
        ensure!(
            DATA_COMMANDS.contains(&name.as_str()),
            "line {line}: {name} is not a data command, so nothing was imported. A commands file may hold only {}",
            DATA_COMMANDS.join(", ")
        );
        ensure!(args.len() >= 2, "line {line}: {name} names no key");
        out.push(CommandLine { line, args });
    }
    Ok(out)
}

// ---- files ---------------------------------------------------------------------

/// A file written under a temporary name beside its final one and moved into
/// place only when it is complete. An export that fails halfway leaves the
/// file it would have replaced exactly as it was, and no partial file.
///
/// A target that is a symlink stays one: the file it points to is the one
/// replaced. On Unix the temporary file is created `0600`, since an export
/// can hold secrets, and a file being replaced passes its own permissions on,
/// so an export over a private file never makes it readable to others. A new
/// file stays `0600`.
pub struct PendingFile {
    temp: std::path::PathBuf,
    target: std::path::PathBuf,
    done: bool,
}

/// Follow `path` through symlinks to the file they finally name, which need
/// not exist yet. A loop, or a chain longer than the kernel would follow, is
/// an error, and so is a link [`may_follow`] refuses.
fn resolve_links(path: &std::path::Path) -> std::io::Result<std::path::PathBuf> {
    let mut path = path.to_path_buf();
    for _ in 0..40 {
        match std::fs::symlink_metadata(&path) {
            Ok(meta) if meta.file_type().is_symlink() => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::MetadataExt;
                    let dir = path
                        .parent()
                        .filter(|p| !p.as_os_str().is_empty())
                        .unwrap_or(std::path::Path::new("."));
                    let dir_meta = std::fs::metadata(dir)?;
                    if !may_follow(meta.uid(), dir_meta.uid(), dir_meta.mode(), euid()) {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            format!(
                                "refusing to follow the symlink {}: it belongs to another user (uid {}), so it could point the export at a file of yours it should not replace",
                                path.display(),
                                meta.uid()
                            ),
                        ));
                    }
                }
                let link = std::fs::read_link(&path)?;
                path = match path.parent() {
                    Some(parent) if link.is_relative() => parent.join(link),
                    _ => link,
                };
            }
            _ => return Ok(path),
        }
    }
    Err(std::io::Error::other(
        "the path is a chain of too many symlinks",
    ))
}

/// The effective user id of this process.
#[cfg(unix)]
fn euid() -> u32 {
    // SAFETY: geteuid takes no arguments and cannot fail.
    unsafe { libc::geteuid() }
}

/// Whether an export may write through a symlink owned by `link_uid`, in a
/// directory owned by `dir_uid` with mode `dir_mode`, for the user `euid`.
/// A link of the user's own is always followed. Anyone else's is followed
/// only when it is root's, and then in a sticky world-writable directory
/// such as `/tmp` only when the directory is root's too. That is at least
/// as strict as Linux's `fs.protected_symlinks`, which this check stands in
/// for: the export opens the resolved path, so the kernel never sees the link.
/// Otherwise another user could leave a link in `/tmp` that turns an export
/// into an overwrite of the victim's `~/.ssh/authorized_keys`.
#[cfg(unix)]
fn may_follow(link_uid: u32, dir_uid: u32, dir_mode: u32, euid: u32) -> bool {
    let sticky_world_writable = dir_mode & 0o1002 == 0o1002;
    link_uid == euid || (link_uid == 0 && (!sticky_world_writable || dir_uid == 0))
}

/// The mode a new file takes from the file it replaces: that file's
/// permission bits, never setuid, setgid or sticky, and only from a regular
/// file of the user's own. Without its group (`group_kept` false) the group
/// bits go too, since they would apply to a different group. `None` keeps
/// the new file's own `0600`.
#[cfg(unix)]
fn inherited_mode(mode: u32, uid: u32, regular: bool, euid: u32, group_kept: bool) -> Option<u32> {
    if !regular || uid != euid {
        return None;
    }
    let mode = mode & 0o777;
    Some(if group_kept { mode } else { mode & !0o070 })
}

impl PendingFile {
    /// Create the temporary file in the directory of the file the target
    /// finally names, so the final rename never crosses a file system and
    /// never replaces a symlink.
    pub fn create(target: impl AsRef<std::path::Path>) -> std::io::Result<(Self, std::fs::File)> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let target = resolve_links(target.as_ref())?;
        let name = target.file_name().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "the path has no file name",
            )
        })?;
        let dir = target
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(std::path::Path::new("."));
        loop {
            let mut temp_name = std::ffi::OsString::from(".");
            temp_name.push(name);
            temp_name.push(format!(
                ".rediscope-{}-{}.tmp",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            let temp = dir.join(temp_name);
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            match options.open(&temp) {
                Ok(file) => {
                    // The umask can only narrow the mode, but say it exactly.
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        if let Err(e) = file.set_permissions(std::fs::Permissions::from_mode(0o600))
                        {
                            let _ = std::fs::remove_file(&temp);
                            return Err(e);
                        }
                    }
                    let pending = Self {
                        temp,
                        target,
                        done: false,
                    };
                    return Ok((pending, file));
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Move the finished file into place. Close every handle to it first.
    /// A file already there gives the new one its permissions first, read at
    /// this moment, so a change made during the export is kept too.
    pub fn commit(mut self) -> std::io::Result<()> {
        #[cfg(unix)]
        if let Ok(meta) = std::fs::symlink_metadata(&self.target) {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            let me = euid();
            if meta.file_type().is_file() && meta.uid() == me {
                let group_kept = std::fs::metadata(&self.temp)?.gid() == meta.gid()
                    || std::os::unix::fs::chown(&self.temp, None, Some(meta.gid())).is_ok();
                if let Some(mode) = inherited_mode(meta.mode(), meta.uid(), true, me, group_kept) {
                    std::fs::set_permissions(&self.temp, std::fs::Permissions::from_mode(mode))?;
                }
            }
        }
        std::fs::rename(&self.temp, &self.target)?;
        self.done = true;
        Ok(())
    }
}

impl Drop for PendingFile {
    fn drop(&mut self) {
        if !self.done {
            let _ = std::fs::remove_file(&self.temp);
        }
    }
}

// ---- reading an import -----------------------------------------------------

/// A file to import, parsed.
#[derive(Debug)]
pub enum Parsed {
    /// An old-style export of `DUMP` payloads.
    Dump(Vec<ExportEntry>),
    Records(Vec<Record>, Format),
    Commands(Vec<CommandLine>),
}

impl Parsed {
    pub fn format(&self) -> Format {
        match self {
            Self::Dump(_) => Format::Dump,
            Self::Records(_, format) => *format,
            Self::Commands(_) => Format::Commands,
        }
    }
}

/// Work out what a file is from its content and parse it: a JSON array of
/// `DUMP` entries or of records, a JSON document, JSON Lines, CSV with the
/// export header, and anything else as `redis-cli` commands.
pub fn parse(bytes: &[u8]) -> Result<Parsed> {
    let bytes = bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(bytes);
    let start = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let body = &bytes[start..];
    match body.first() {
        Some(b'[') => parse_array(text(bytes)?),
        Some(b'{') => parse_object_or_lines(text(bytes)?),
        _ if body.starts_with(CSV_HEADER.as_bytes())
            && matches!(body.get(CSV_HEADER.len()), None | Some(b'\r' | b'\n')) =>
        {
            Ok(Parsed::Records(csv_records(text(bytes)?)?, Format::Csv))
        }
        _ => Ok(Parsed::Commands(parse_commands(bytes)?)),
    }
}

fn text(bytes: &[u8]) -> Result<&str> {
    std::str::from_utf8(bytes).context("the file looks like JSON or CSV but is not UTF-8 text")
}

/// What is wrong with some JSON, without the position serde_json appends.
fn json_reason(e: &serde_json::Error) -> String {
    let text = e.to_string();
    match text.rsplit_once(" at line ") {
        Some((reason, _)) => reason.to_string(),
        None => text,
    }
}

/// A JSON syntax error, saying where it is.
fn json_syntax(e: &serde_json::Error) -> anyhow::Error {
    anyhow!(
        "invalid JSON at line {} column {}: {}",
        e.line(),
        e.column(),
        json_reason(e)
    )
}

fn parse_array(text: &str) -> Result<Parsed> {
    let items: Vec<Json> = serde_json::from_str(text)
        .map_err(|e| json_syntax(&e).context("not a valid JSON export"))?;
    if items.first().is_some_and(|i| i.get("dump").is_some()) {
        let entries = items
            .into_iter()
            .map(serde_json::from_value::<ExportEntry>)
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("not a rediscope DUMP export")?;
        return Ok(Parsed::Dump(entries));
    }
    // An empty array has always been a valid, empty DUMP export.
    if items.is_empty() {
        return Ok(Parsed::Dump(Vec::new()));
    }
    let records = items
        .iter()
        .enumerate()
        .map(|(i, e)| json_record(e).map_err(|err| anyhow!("entry {}: {err}", i + 1)))
        .collect::<Result<_>>()?;
    Ok(Parsed::Records(records, Format::Json))
}

fn parse_object_or_lines(text: &str) -> Result<Parsed> {
    let whole = serde_json::from_str::<Json>(text);
    // A first line that is not JSON on its own means one document spread over
    // lines, broken somewhere: its own error says where.
    if let Err(e) = &whole
        && text
            .lines()
            .find(|l| !l.trim().is_empty())
            .is_some_and(|first| serde_json::from_str::<Json>(first).is_err())
    {
        return Err(json_syntax(e).context("not a valid JSON export"));
    }
    if let Ok(doc) = whole {
        if doc.get("key").is_some() {
            return Ok(Parsed::Records(vec![json_record(&doc)?], Format::Jsonl));
        }
        let tag = doc.get("format").and_then(Json::as_str);
        ensure!(
            tag == Some(FORMAT_TAG),
            "a JSON document to import must have \"format\": \"{FORMAT_TAG}\""
        );
        let version = doc.get("version").and_then(Json::as_u64).unwrap_or(0);
        ensure!(
            version == FORMAT_VERSION,
            "this export is version {version}; this rediscope reads version {FORMAT_VERSION}"
        );
        let keys = doc
            .get("keys")
            .and_then(Json::as_array)
            .ok_or_else(|| anyhow!("the export has no \"keys\" array"))?;
        let records = keys
            .iter()
            .enumerate()
            .map(|(i, e)| json_record(e).map_err(|err| anyhow!("entry {}: {err}", i + 1)))
            .collect::<Result<_>>()?;
        return Ok(Parsed::Records(records, Format::Json));
    }
    let mut records = Vec::new();
    for (i, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let entry: Json = serde_json::from_str(line).map_err(|e| {
            anyhow!(
                "line {}: not a JSON object: invalid JSON at column {}: {}",
                i + 1,
                e.column(),
                json_reason(&e)
            )
        })?;
        records.push(json_record(&entry).map_err(|e| anyhow!("line {}: {e}", i + 1))?);
    }
    Ok(Parsed::Records(records, Format::Jsonl))
}

/// One line saying what an import did, for the CLI and the status bar.
pub fn import_summary(format: Format, report: &ImportReport) -> String {
    let mut text = match format {
        Format::Commands => format!(
            "Imported {} command(s) for {} key(s) from {}",
            report.commands,
            report.keys,
            format.label()
        ),
        _ => format!("Imported {} key(s) from {}", report.keys, format.label()),
    };
    if report.skipped > 0 {
        text.push_str(&format!(", skipped {} empty collection(s)", report.skipped));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(value: Value) -> Record {
        Record {
            key: b"k".to_vec(),
            ttl_ms: Some(1500),
            value,
        }
    }

    fn every_type() -> Vec<Record> {
        let bin = vec![0x80, 0xfe, 0x00, b'A'];
        let mut out = vec![
            Record {
                key: b"plain".to_vec(),
                ttl_ms: None,
                value: Value::String(b"hello, \"world\"\nline two".to_vec()),
            },
            Record {
                key: bin.clone(),
                ttl_ms: Some(42),
                value: Value::String(bin.clone()),
            },
            record(Value::Hash(vec![
                (b"name".to_vec(), b"ada".to_vec()),
                (b"blob".to_vec(), bin.clone()),
            ])),
            record(Value::Hash(vec![(bin.clone(), b"binary field".to_vec())])),
            record(Value::List(vec![
                b"a".to_vec(),
                b"a".to_vec(),
                Vec::new(),
                bin.clone(),
            ])),
            record(Value::Set(vec![b"x".to_vec(), bin.clone()])),
            record(Value::ZSet(vec![
                (b"low".to_vec(), f64::NEG_INFINITY),
                (b"mid".to_vec(), 1.5),
                (b"tiny".to_vec(), 1e-300),
                (bin.clone(), f64::INFINITY),
            ])),
            record(Value::Stream(vec![
                StreamEntry {
                    id: "1-1".into(),
                    fields: vec![
                        (b"f".to_vec(), b"v".to_vec()),
                        (b"f".to_vec(), b"w".to_vec()),
                    ],
                },
                StreamEntry {
                    id: "2-0".into(),
                    fields: vec![(b"a:b".to_vec(), bin.clone())],
                },
            ])),
            record(Value::Json(json!({"b": [1, 2.5, "x"], "a": null}))),
            record(Value::TimeSeries(Series {
                samples: vec![(1000, 1.25), (2000, -3.0)],
                labels: vec![("sensor".into(), "t1".into())],
                retention_ms: Some(60_000),
            })),
            record(Value::VectorSet(VectorSet {
                quant: Some("f32".into()),
                elements: vec![
                    VectorElement {
                        element: b"e1".to_vec(),
                        vector: vec![0.5, -1.0],
                        attributes: Some(r#"{"year":1950}"#.into()),
                    },
                    VectorElement {
                        element: bin.clone(),
                        vector: vec![1.0, 2.0],
                        attributes: Some(r#""just text""#.into()),
                    },
                    VectorElement {
                        element: b"e3".to_vec(),
                        vector: vec![3.0, 4.0],
                        attributes: None,
                    },
                ],
            })),
        ];
        // Unique names, so consecutive records never merge in a CSV.
        for (i, r) in out.iter_mut().enumerate().skip(2) {
            r.key = format!("k{i}").into_bytes();
        }
        out.push(Record {
            key: b"base64:looks-encoded".to_vec(),
            ttl_ms: None,
            value: Value::String(b"base64:not really".to_vec()),
        });
        out
    }

    fn export(format: Format, records: &[Record]) -> Vec<u8> {
        let mut w = Writer::new(Vec::new(), format, false).unwrap();
        for r in records {
            w.write(r).unwrap();
        }
        w.finish().unwrap()
    }

    #[test]
    fn json_formats_round_trip_every_type_exactly() {
        let records = every_type();
        for format in [Format::Json, Format::Jsonl] {
            let bytes = export(format, &records);
            match parse(&bytes).unwrap() {
                Parsed::Records(back, f) => {
                    assert_eq!(f, format);
                    assert_eq!(back, records, "{format:?}");
                }
                other => panic!("{format:?} parsed as {other:?}"),
            }
        }
    }

    #[test]
    fn a_json_document_is_tagged_and_pretty() {
        let text = String::from_utf8(export(Format::Json, &every_type()[..1])).unwrap();
        assert!(text.starts_with("{\n  \"format\": \"rediscope-export\",\n  \"version\": 1,"));
        assert!(
            text.contains("\n    {\n      \"key\": \"plain\","),
            "{text}"
        );
        let doc: Json = serde_json::from_str(&text).unwrap();
        assert_eq!(doc["keys"][0]["ttl_ms"], Json::Null);
        // An empty export is still a valid document.
        let empty = export(Format::Json, &[]);
        let doc: Json = serde_json::from_slice(&empty).unwrap();
        assert_eq!(doc["keys"], json!([]));
        assert!(matches!(parse(&empty).unwrap(), Parsed::Records(r, _) if r.is_empty()));
    }

    #[test]
    fn values_have_the_documented_shapes() {
        let records = every_type();
        let shapes: Vec<Json> = records.iter().map(record_json).collect();
        assert_eq!(shapes[1]["key"], json!({"base64": "gP4AQQ=="}));
        assert_eq!(
            shapes[2]["value"],
            json!({"name": "ada", "blob": {"base64": "gP4AQQ=="}})
        );
        // A binary field name cannot be an object key: pairs instead.
        assert_eq!(
            shapes[3]["value"],
            json!([[{"base64": "gP4AQQ=="}, "binary field"]])
        );
        assert_eq!(shapes[6]["value"][0], json!(["low", "-inf"]));
        assert_eq!(shapes[6]["value"][1], json!(["mid", 1.5]));
        assert_eq!(
            shapes[7]["value"][0]["fields"],
            json!([["f", "v"], ["f", "w"]])
        );
        assert_eq!(shapes[7]["value"][1]["id"], "2-0");
        assert_eq!(shapes[8]["value"], json!({"b": [1, 2.5, "x"], "a": null}));
        assert_eq!(
            shapes[9]["value"]["samples"],
            json!([[1000, 1.25], [2000, -3.0]])
        );
        assert_eq!(shapes[9]["value"]["labels"], json!({"sensor": "t1"}));
        assert_eq!(shapes[10]["value"][0]["attributes"], json!({"year": 1950}));
        assert_eq!(shapes[10]["value"][1]["attributes"], json!("\"just text\""));
        assert_eq!(shapes[10]["quant"], "f32");
        assert_eq!(shapes[0]["type"], "string");
        assert_eq!(shapes[0]["ttl_ms"], Json::Null);
        assert_eq!(shapes[2]["ttl_ms"], 1500);
    }

    #[test]
    fn hand_written_json_is_accepted_loosely_but_never_guessed() {
        let text = r#"[{"key":"n","type":"list","value":[1,"two"]},
                       {"key":"z","type":"zset","ttl_ms":null,"value":[["m","2.5"]]}]"#;
        let Parsed::Records(r, Format::Json) = parse(text.as_bytes()).unwrap() else {
            panic!("not records");
        };
        assert_eq!(
            r[0].value,
            Value::List(vec![b"1".to_vec(), b"two".to_vec()])
        );
        assert_eq!(r[1].value, Value::ZSet(vec![(b"m".to_vec(), 2.5)]));
        for bad in [
            r#"[{"key":"k","type":"hash","value":"no"}]"#,
            r#"[{"key":"k","type":"string","value":{"base64":"%%"}}]"#,
            r#"[{"key":"k","type":"module-thing","value":1}]"#,
            r#"[{"type":"string","value":"v"}]"#,
            r#"{"format":"something-else","keys":[]}"#,
            r#"{"format":"rediscope-export","version":99,"keys":[]}"#,
        ] {
            assert!(parse(bad.as_bytes()).is_err(), "{bad}");
        }
    }

    #[test]
    fn old_dump_exports_are_still_recognised() {
        let old = r#"[
  {"key": "keep:1", "kind": "string", "pttl": -1, "dump": "0003"}
]"#;
        let Parsed::Dump(entries) = parse(old.as_bytes()).unwrap() else {
            panic!("not a dump");
        };
        assert_eq!(entries[0].key, "keep:1");
        assert!(matches!(parse(b"[]").unwrap(), Parsed::Dump(e) if e.is_empty()));
        let mut out = Vec::new();
        write_dump(&mut out, &entries).unwrap();
        assert!(matches!(parse(&out).unwrap(), Parsed::Dump(e) if e.len() == 1));
    }

    #[test]
    fn csv_quotes_cells_and_encodes_binary() {
        assert_eq!(csv_quote("plain"), "plain");
        assert_eq!(csv_quote("a,b"), "\"a,b\"");
        assert_eq!(csv_quote("say \"hi\""), "\"say \"\"hi\"\"\"");
        assert_eq!(csv_quote("two\nlines"), "\"two\nlines\"");
        assert_eq!(csv_cell(&[0xff, 0x00]), "base64:/wA=");
        assert_eq!(csv_cell(b"base64:abc"), "base64:YmFzZTY0OmFiYw==");
        assert_eq!(
            csv_cell_bytes("base64:YmFzZTY0OmFiYw==").unwrap(),
            b"base64:abc"
        );
        let rows = parse_csv("a,\"b,c\",\"d\"\"e\"\r\n\n\"multi\nline\",,\n").unwrap();
        assert_eq!(
            rows,
            vec![
                (1, vec!["a".into(), "b,c".into(), "d\"e".into()]),
                (3, vec!["multi\nline".into(), String::new(), String::new()]),
            ]
        );
        assert!(parse_csv("\"open").is_err());
    }

    #[test]
    fn csv_rows_follow_the_documented_layout() {
        let records = every_type();
        let text = String::from_utf8(export(Format::Csv, &records)).unwrap();
        let mut lines = text.lines();
        assert_eq!(lines.next(), Some(CSV_HEADER));
        assert_eq!(
            csv_rows(&records[2])[0],
            ["k2", "hash", "1500", "name", "ada"].map(String::from)
        );
        assert_eq!(csv_rows(&records[4])[1][3], "1");
        assert_eq!(
            csv_rows(&records[6])[1],
            ["k6", "zset", "1500", "mid", "1.5"].map(String::from)
        );
        assert_eq!(csv_rows(&records[7])[2][3], "2-0:a:b");
        assert_eq!(csv_rows(&records[0])[0][2], "");
        assert_eq!(csv_rows(&records[0])[0][3], "");
    }

    #[test]
    fn csv_round_trips_everything_but_series_labels_and_vector_quantization() {
        let mut records = every_type();
        for r in &mut records {
            match &mut r.value {
                Value::TimeSeries(s) => {
                    s.labels.clear();
                    s.retention_ms = None;
                }
                Value::VectorSet(v) => v.quant = None,
                _ => {}
            }
        }
        let bytes = export(Format::Csv, &records);
        let Parsed::Records(back, Format::Csv) = parse(&bytes).unwrap() else {
            panic!("not csv");
        };
        assert_eq!(back, records);
        assert!(parse(b"key,type,ttl_ms,field,value\nk,string,,\n").is_err());
    }

    #[test]
    fn redis_cli_quoting_round_trips_every_byte() {
        let samples: Vec<Vec<u8>> = vec![
            b"plain".to_vec(),
            Vec::new(),
            b"with space".to_vec(),
            b"quote\" and 'single' and \\".to_vec(),
            b"\n\r\t\x07\x08\x00\x7f".to_vec(),
            "caf\u{e9} \u{1F600}".as_bytes().to_vec(),
            (0..=255u8).collect(),
            b"\\x41 is not an escape here".to_vec(),
        ];
        for s in &samples {
            let quoted = quote_arg(s);
            assert!(!quoted.contains('\n') && !quoted.contains('\r'), "{quoted}");
            assert_eq!(
                split_line(quoted.as_bytes()).unwrap(),
                vec![s.clone()],
                "{quoted}"
            );
        }
        assert_eq!(quote_arg(b"user:1"), "user:1");
        assert_eq!(quote_arg(b""), "\"\"");
        assert_eq!(quote_arg(&[0x80, b'a']), "\"\\x80a\"");
        assert_eq!(quote_arg("é".as_bytes()), "\"é\"");
        let line = command_line(&samples);
        assert_eq!(split_line(line.as_bytes()).unwrap(), samples);
    }

    #[test]
    fn split_line_matches_redis_cli() {
        let split = |s: &str| split_line(s.as_bytes()).unwrap();
        assert_eq!(
            split("  SET  a   b "),
            vec![b"SET".to_vec(), b"a".to_vec(), b"b".to_vec()]
        );
        assert_eq!(
            split(r#"SET "a\x41\n" 'it\'s'"#)[1..],
            [b"aA\n".to_vec(), b"it's".to_vec()]
        );
        assert_eq!(split(r#"SET 'no \n escape'"#)[1], b"no \\n escape");
        assert_eq!(split(r#"SET ab"cd""#)[1], b"abcd");
        assert!(split_line(br#"SET "open"#).is_err());
        assert!(split_line(br#"SET "a"b"#).is_err());
        assert!(split("").is_empty());
    }

    #[test]
    fn commands_chunk_collections_and_set_the_ttl_last() {
        let big = Record {
            key: b"big".to_vec(),
            ttl_ms: Some(0),
            value: Value::List((0..1201).map(|i| i.to_string().into_bytes()).collect()),
        };
        let cmds = commands(&big, true);
        assert_eq!(cmds.len(), 1 + 3 + 1);
        assert_eq!(cmds[0], vec![b"DEL".to_vec(), b"big".to_vec()]);
        assert_eq!(cmds[1].len(), 2 + CHUNK);
        assert_eq!(cmds[3].len(), 2 + 201);
        assert_eq!(
            cmds[4],
            vec![b"PEXPIRE".to_vec(), b"big".to_vec(), b"1".to_vec()]
        );
        assert_eq!(commands(&big, false).len(), 4);

        let lines: Vec<String> = every_type()
            .iter()
            .flat_map(|r| commands(r, false))
            .map(|c| command_line(&c))
            .collect();
        assert!(
            lines.contains(
                &"ZADD k6 -inf low 1.5 mid 1e-300 tiny inf \"\\x80\\xfe\\x00A\"".to_string()
            ),
            "{lines:#?}"
        );
        assert!(lines.contains(&"XADD k7 1-1 f v f w".to_string()));
        assert!(lines.contains(&"TS.CREATE k9 RETENTION 60000 LABELS sensor t1".to_string()));
        assert!(lines.contains(&"TS.MADD k9 1000 1.25 k9 2000 -3".to_string()));
        assert!(
            lines.contains(
                &"VADD k10 VALUES 2 0.5 -1 e1 NOQUANT SETATTR \"{\\\"year\\\":1950}\"".to_string()
            ),
            "{lines:#?}"
        );
        assert!(lines.contains(
            &"JSON.SET k8 $ \"{\\\"b\\\":[1,2.5,\\\"x\\\"],\\\"a\\\":null}\"".to_string()
        ));

        // Parsing the written file gives back the same arguments.
        let mut w = Writer::new(Vec::new(), Format::Commands, true).unwrap();
        for r in every_type() {
            w.write(&r).unwrap();
        }
        let bytes = w.finish().unwrap();
        let Parsed::Commands(parsed) = parse(&bytes).unwrap() else {
            panic!("not commands");
        };
        let expected: Vec<Vec<Vec<u8>>> = every_type()
            .iter()
            .flat_map(|r| commands(r, true))
            .collect();
        assert_eq!(
            parsed.iter().map(|c| c.args.clone()).collect::<Vec<_>>(),
            expected
        );
        assert_eq!(parsed[1].line, 2);
    }

    #[test]
    fn a_commands_file_refuses_anything_but_data_commands() {
        let err = parse(b"SET a 1\n\nFLUSHALL\n").unwrap_err().to_string();
        assert!(err.starts_with("line 3: FLUSHALL"), "{err}");
        assert!(parse(b"SET a 1\nEVAL \"return 1\" 0\n").is_err());
        assert!(parse(b"SET\n").is_err());
        let Parsed::Commands(ok) = parse(b"set a 1\r\n\r\nrpush l x y\r\n").unwrap() else {
            panic!("not commands");
        };
        assert_eq!(ok.iter().map(|c| c.line).collect::<Vec<_>>(), [1, 3]);
    }

    #[test]
    fn formats_have_names_and_extensions() {
        for f in Format::ALL {
            assert_eq!(Format::parse(f.name()), Some(f));
        }
        assert_eq!(Format::parse("JSONL"), Some(Format::Jsonl));
        assert_eq!(Format::parse("xml"), None);
        assert_eq!(Format::Commands.extension(), "redis");
    }

    #[test]
    fn a_ttl_of_minus_one_means_no_expiry_and_other_non_positive_ones_are_refused() {
        let json = |t: &str| format!(r#"[{{"key":"k","type":"string","ttl_ms":{t},"value":"v"}}]"#);
        let csv = |t: &str| format!("{CSV_HEADER}\nk,string,{t},,v\n");
        for text in [json("-1"), csv("-1")] {
            let Parsed::Records(r, _) = parse(text.as_bytes()).unwrap() else {
                panic!("not records");
            };
            assert_eq!(r[0].ttl_ms, None, "{text}");
        }
        for t in ["0", "-2", "-5000"] {
            let err = format!("{:#}", parse(json(t).as_bytes()).unwrap_err());
            assert!(
                err.contains(&format!("'k' has ttl_ms {t}, which would delete the key")),
                "{err}"
            );
            let err = parse(csv(t).as_bytes()).unwrap_err().to_string();
            assert!(err.starts_with("line 2: ttl_ms"), "{err}");
        }
        // A key read with moments left is still written with a TTL, never zero.
        let mut r = record(Value::String(b"v".to_vec()));
        r.ttl_ms = Some(0);
        assert_eq!(commands(&r, false)[1][2], b"1");
    }

    #[test]
    fn records_the_server_would_refuse_halfway_are_refused_before_import() {
        for (text, needle) in [
            (
                r#"[{"key":"z","type":"zset","value":[["a","nan"]]}]"#,
                "is not a number: nan",
            ),
            (
                r#"[{"key":"v","type":"vectorset","value":[{"element":"a","vector":[1,2]},{"element":"b","vector":[1,2,3]}]}]"#,
                "element 'b' of 'v' has 3 dimensions, but the first element has 2",
            ),
            (
                r#"[{"key":"s","type":"stream","value":[{"id":"2-0","fields":{"a":"1"}},{"id":"1-0","fields":{"a":"1"}}]}]"#,
                "stream entry 1-0 in 's' does not come after",
            ),
            (
                r#"[{"key":"s","type":"stream","value":[{"id":"0-0","fields":{"a":"1"}}]}]"#,
                "stream entry 0-0 in 's' does not come after",
            ),
            (
                r#"[{"key":"s","type":"stream","value":[{"id":"*","fields":{"a":"1"}}]}]"#,
                "is not an id like",
            ),
            (
                r#"[{"key":"t","type":"timeseries","value":{"samples":[[5,1],[5,2]]}}]"#,
                "'t' has two samples at timestamp 5",
            ),
            (
                r#"[{"key":"t","type":"timeseries","value":{"samples":[[5,"nan"]]}}]"#,
                "is not a number",
            ),
        ] {
            let err = format!("{:#}", parse(text.as_bytes()).unwrap_err());
            assert!(err.contains(needle), "{text}\n  gave: {err}");
        }
        let err = parse(format!("{CSV_HEADER}\nz,zset,,a,1\nz,zset,,b,NaN\n").as_bytes())
            .unwrap_err()
            .to_string();
        assert_eq!(err, "line 3: score 'NaN' is not a number");
        let err = parse(
            format!("{CSV_HEADER}\nv,vectorset,,a,\"{{\"\"vector\"\":[1]}}\"\nv,vectorset,,b,\"{{\"\"vector\"\":[1,2]}}\"\n")
                .as_bytes(),
        )
        .unwrap_err()
        .to_string();
        assert!(err.starts_with("line 2: element 'b'"), "{err}");
    }

    #[test]
    fn csv_rows_of_one_key_join_up_wherever_they_are() {
        let text = format!(
            "{CSV_HEADER}\n\
             l,list,5000,1,b\n\
             s,stream,,2-0:x,1\n\
             h,hash,,f,1\n\
             l,list,5000,0,a\n\
             s,stream,,1-0:y,2\n\
             h,hash,,g,2\n\
             s,stream,,2-0:z,3\n\
             l,list,5000,2,c\n"
        );
        let records = csv_records(&text).unwrap();
        assert_eq!(
            records.iter().map(|r| r.key.clone()).collect::<Vec<_>>(),
            [b"l".to_vec(), b"s".to_vec(), b"h".to_vec()]
        );
        assert_eq!(
            records[0].value,
            Value::List(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
        );
        assert_eq!(records[0].ttl_ms, Some(5000));
        assert_eq!(
            records[1].value,
            Value::Stream(vec![
                StreamEntry {
                    id: "1-0".into(),
                    fields: vec![(b"y".to_vec(), b"2".to_vec())],
                },
                StreamEntry {
                    id: "2-0".into(),
                    fields: vec![
                        (b"x".to_vec(), b"1".to_vec()),
                        (b"z".to_vec(), b"3".to_vec())
                    ],
                },
            ])
        );
        assert_eq!(
            records[2].value,
            Value::Hash(vec![
                (b"f".to_vec(), b"1".to_vec()),
                (b"g".to_vec(), b"2".to_vec())
            ])
        );
        // The same key cannot be two things at once.
        let err = csv_records(&format!(
            "{CSV_HEADER}\nk,list,,0,a\nx,string,,,v\nk,set,,,a\n"
        ))
        .unwrap_err()
        .to_string();
        assert_eq!(err, "line 4: 'k' is a list on line 2 but a set here");
        let err = csv_records(&format!("{CSV_HEADER}\nk,list,10,0,a\nk,list,20,1,b\n"))
            .unwrap_err()
            .to_string();
        assert_eq!(err, "line 3: 'k' has ttl_ms '10' on line 2 but '20' here");
    }

    #[test]
    fn big_keys_are_sent_in_bounded_pipelines() {
        let big = |value: Value| Record {
            key: b"k".to_vec(),
            ttl_ms: None,
            value,
        };
        let items = |n: usize, size: usize| -> Vec<Vec<u8>> {
            (0..n).map(|i| format!("{i:0size$}").into_bytes()).collect()
        };
        let cases = [
            big(Value::List(items(200_000, 4))),
            big(Value::Hash(
                items(50_000, 64)
                    .into_iter()
                    .map(|i| (i.clone(), i))
                    .collect(),
            )),
            big(Value::ZSet(
                items(90_000, 8).into_iter().map(|i| (i, 1.5)).collect(),
            )),
            big(Value::String(vec![b'x'; 3 << 20])),
            big(Value::VectorSet(VectorSet {
                quant: None,
                elements: (0..80_000)
                    .map(|i| VectorElement {
                        element: i.to_string().into_bytes(),
                        vector: vec![f64::from(i); 16],
                        attributes: None,
                    })
                    .collect(),
            })),
        ];
        for record in &cases {
            let cmds = write_commands(record, &record.key);
            let chunks = pipeline_chunks(&cmds);
            // Every command is sent exactly once, in order.
            assert_eq!(chunks.first().map(|c| c.start), Some(0));
            assert_eq!(chunks.last().map(|c| c.end), Some(cmds.len()));
            assert!(chunks.windows(2).all(|w| w[0].end == w[1].start));
            for chunk in &chunks {
                let part = &cmds[chunk.clone()];
                let vadds = part.iter().filter(|c| c[0] == b"VADD").count();
                assert!(part.len() <= PIPELINE_COMMANDS);
                assert!(vadds * VADD_WEIGHT <= PIPELINE_COMMANDS);
                let bytes: usize = part.iter().flatten().map(|a| a.len() + 16).sum();
                assert!(part.len() == 1 || bytes <= PIPELINE_BYTES, "{bytes}");
            }
            if cmds.len() > 1 {
                assert!(chunks.len() > 1, "{}", record.value.type_name());
            }
        }
        assert!(pipeline_chunks::<Vec<Vec<u8>>>(&[]).is_empty());
    }

    #[test]
    fn a_commands_file_counts_every_key_a_line_writes() {
        let keys = |line: &str| {
            command_keys(&split_line(line.as_bytes()).unwrap())
                .iter()
                .map(|k| String::from_utf8_lossy(k).into_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(keys("SET a 1"), ["a"]);
        assert_eq!(keys("MSET a 1 b 2"), ["a", "b"]);
        assert_eq!(keys("del a b c"), ["a", "b", "c"]);
        assert_eq!(keys("TS.MADD t 1 1 u 2 2"), ["t", "u"]);
    }

    #[test]
    fn split_line_separates_on_the_spaces_redis_cli_does() {
        let split = |s: &[u8]| split_line(s).unwrap();
        // Between arguments any of C's spaces separate them.
        assert_eq!(
            split(b"\x0bSET\x0b\x0ca b"),
            [b"SET\x0b\x0ca".to_vec(), b"b".to_vec()]
        );
        assert_eq!(split(b"SET \x0c\x0b a"), [b"SET".to_vec(), b"a".to_vec()]);
        // A closing quote may be followed by any of them.
        assert_eq!(
            split(b"SET \"a\"\x0bb"),
            [b"SET".to_vec(), b"a".to_vec(), b"b".to_vec()]
        );
        assert_eq!(split(b"SET a\x0cb"), [b"SET".to_vec(), b"a\x0cb".to_vec()]);
    }

    #[test]
    fn json_syntax_errors_say_where() {
        let err = format!(
            "{:#}",
            parse(b"[\n  {\"key\": \"a\",\n  oops\n]").unwrap_err()
        );
        assert!(
            err.starts_with("not a valid JSON export: invalid JSON at line 3 column 3: "),
            "{err}"
        );
        let err = format!(
            "{:#}",
            parse(b"{\n  \"format\": \"rediscope-export\",\n  \"keys\": [,]\n}").unwrap_err()
        );
        assert!(err.contains("invalid JSON at line 3 column 12"), "{err}");
        let err = parse(b"{\"key\":\"a\",\"type\":\"string\",\"value\":\"1\"}\n{\"key\" 1}\n")
            .unwrap_err()
            .to_string();
        assert!(
            err.starts_with("line 2: not a JSON object: invalid JSON at column 8: "),
            "{err}"
        );
    }

    #[test]
    fn samples_and_vectors_keep_every_bit_in_json() {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut next = || loop {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let f = f64::from_bits(state);
            if f.is_finite() {
                return f;
            }
        };
        let series = Record {
            key: b"t".to_vec(),
            ttl_ms: None,
            value: Value::TimeSeries(Series {
                samples: (0..500).map(|t| (t, next())).collect(),
                ..Series::default()
            }),
        };
        let vectors = Record {
            key: b"v".to_vec(),
            ttl_ms: None,
            value: Value::VectorSet(VectorSet {
                quant: Some("f32".into()),
                elements: (0..50)
                    .map(|i| VectorElement {
                        element: format!("e{i}").into_bytes(),
                        vector: (0..8).map(|_| next()).collect(),
                        attributes: None,
                    })
                    .collect(),
            }),
        };
        let doc = Record {
            key: b"j".to_vec(),
            ttl_ms: None,
            value: Value::Json(json!({"a": 0.1 + 0.2, "b": 123_456_789.123_456_79, "c": 5e-324})),
        };
        let records = vec![series, vectors, doc];
        for format in [Format::Json, Format::Jsonl] {
            let Parsed::Records(back, _) = parse(&export(format, &records)).unwrap() else {
                panic!("not records");
            };
            assert_eq!(back, records, "{format:?}");
        }
    }

    #[test]
    fn a_pending_file_replaces_the_old_one_only_when_committed() {
        let dir = std::env::temp_dir().join(format!("rediscope-pending-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("out.json");
        std::fs::write(&path, "old").unwrap();
        let listing = || {
            let mut names: Vec<String> = std::fs::read_dir(&dir)
                .unwrap()
                .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .collect();
            names.sort();
            names
        };
        {
            let (_pending, mut file) = PendingFile::create(&path).unwrap();
            file.write_all(b"half").unwrap();
            assert_eq!(listing().len(), 2);
            // Dropped without a commit, as a failed export is.
        }
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "old");
        assert_eq!(listing(), ["out.json"]);
        let (pending, mut file) = PendingFile::create(&path).unwrap();
        file.write_all(b"new").unwrap();
        drop(file);
        pending.commit().unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "new");
        assert_eq!(listing(), ["out.json"]);
        assert!(PendingFile::create(dir.join("missing").join("out.json")).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_ttl_the_server_cannot_hold_is_refused_before_anything_is_sent() {
        for t in [i64::MAX, i64::MAX - 1_000] {
            let err = validate(&Record {
                ttl_ms: Some(t),
                ..record(Value::String(b"v".to_vec()))
            })
            .unwrap_err()
            .to_string();
            assert!(err.contains(&format!("ttl_ms {t}")), "{err}");
            assert!(err.contains("too far ahead"), "{err}");
        }
        // Far ahead, but still a time the server can hold.
        for t in [1, 3_155_760_000_000, i64::MAX / 2] {
            validate(&Record {
                ttl_ms: Some(t),
                ..record(Value::String(b"v".to_vec()))
            })
            .unwrap();
        }
        let line = format!(
            r#"{{"key":"k","type":"string","ttl_ms":{},"value":"v"}}"#,
            i64::MAX
        );
        let err = parse(line.as_bytes()).unwrap_err().to_string();
        assert!(err.contains("too far ahead"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_are_followed_only_when_their_owner_can_be_trusted() {
        let (me, other, root) = (501, 502, 0);
        let (tmp, private, shared) = (0o41777, 0o40755, 0o40777);
        // The user's own link, anywhere.
        assert!(may_follow(me, root, tmp, me));
        assert!(may_follow(me, other, tmp, me));
        assert!(may_follow(me, other, private, me));
        // Another user's link, even in that user's own directory or in /tmp.
        assert!(!may_follow(other, root, tmp, me));
        assert!(!may_follow(other, other, tmp, me));
        assert!(!may_follow(other, other, private, me));
        assert!(!may_follow(other, me, shared, me));
        // Root's link: not in a sticky world-writable directory someone else owns.
        assert!(may_follow(root, root, tmp, me));
        assert!(may_follow(root, other, private, me));
        assert!(may_follow(root, other, shared, me));
        assert!(!may_follow(root, other, tmp, me));
        // Root following root's link.
        assert!(may_follow(root, other, tmp, root));
    }

    #[cfg(unix)]
    #[test]
    fn a_link_of_the_users_own_in_a_sticky_directory_is_followed() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("rediscope-sticky-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o1777)).unwrap();
        let real = dir.join("real.json");
        std::os::unix::fs::symlink(&real, dir.join("link.json")).unwrap();
        assert_eq!(resolve_links(&dir.join("link.json")).unwrap(), real);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn an_inherited_mode_drops_special_bits_and_foreign_files() {
        let me = 501;
        assert_eq!(inherited_mode(0o100640, me, true, me, true), Some(0o640));
        assert_eq!(inherited_mode(0o106755, me, true, me, true), Some(0o755));
        assert_eq!(inherited_mode(0o101644, me, true, me, true), Some(0o644));
        // The group could not be kept: its bits would reach another group.
        assert_eq!(inherited_mode(0o100664, me, true, me, false), Some(0o604));
        // Not the user's file, or not a regular file: the new file stays 0600.
        assert_eq!(inherited_mode(0o100644, 502, true, me, true), None);
        assert_eq!(inherited_mode(0o020666, me, false, me, true), None);
    }

    /// A group the current user is in, other than `not`.
    #[cfg(unix)]
    fn other_group(not: u32) -> Option<u32> {
        let mut groups = vec![0 as libc::gid_t; 256];
        // SAFETY: the buffer holds as many entries as the length passed.
        let n = unsafe { libc::getgroups(groups.len() as libc::c_int, groups.as_mut_ptr()) };
        groups.truncate(usize::try_from(n).ok()?);
        groups.into_iter().find(|g| *g != not)
    }

    #[cfg(unix)]
    #[test]
    fn a_pending_file_keeps_permissions_and_writes_through_symlinks() {
        use std::os::unix::fs::PermissionsExt;
        let dir =
            std::env::temp_dir().join(format!("rediscope-pending-mode-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("real")).unwrap();
        let mode = |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        let write = |p: &std::path::Path, text: &str| {
            let (pending, mut file) = PendingFile::create(p).unwrap();
            // The partial file is private from the start.
            assert_eq!(file.metadata().unwrap().permissions().mode() & 0o777, 0o600);
            file.write_all(text.as_bytes()).unwrap();
            drop(file);
            pending.commit().unwrap();
        };

        // An existing file keeps its own mode, tighter or looser.
        for existing in [0o600, 0o640, 0o644] {
            let path = dir.join(format!("kept-{existing:o}.json"));
            std::fs::write(&path, "old").unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(existing)).unwrap();
            write(&path, "new");
            assert_eq!(std::fs::read_to_string(&path).unwrap(), "new");
            assert_eq!(mode(&path), existing, "{existing:o}");
        }

        // A new file may hold secrets: only its owner can read it.
        let fresh = dir.join("fresh.json");
        write(&fresh, "new");
        assert_eq!(mode(&fresh), 0o600);

        // A symlink stays a symlink, and the file it points at is replaced.
        let real = dir.join("real").join("export.json");
        std::fs::write(&real, "old").unwrap();
        std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o640)).unwrap();
        let link = dir.join("link.json");
        std::os::unix::fs::symlink("real/export.json", &link).unwrap();
        write(&link, "through the link");
        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(std::fs::read_to_string(&real).unwrap(), "through the link");
        assert_eq!(mode(&real), 0o640);

        // A link to a file that does not exist yet creates that file.
        let dangling = dir.join("dangling.json");
        std::os::unix::fs::symlink(dir.join("real").join("later.json"), &dangling).unwrap();
        write(&dangling, "created");
        assert!(
            std::fs::symlink_metadata(&dangling)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("real").join("later.json")).unwrap(),
            "created"
        );

        // Special bits are never passed on: only the permission bits are.
        let special = dir.join("setuid.json");
        std::fs::write(&special, "old").unwrap();
        std::fs::set_permissions(&special, std::fs::Permissions::from_mode(0o4750)).unwrap();
        if std::fs::metadata(&special).unwrap().permissions().mode() & 0o4000 != 0 {
            write(&special, "new");
            let full = std::fs::metadata(&special).unwrap().permissions().mode() & 0o7777;
            assert_eq!(full, 0o750, "{full:o}");
        }

        // A file in another of the user's groups keeps that group, and with it
        // the group bits.
        {
            use std::os::unix::fs::MetadataExt;
            let grouped = dir.join("grouped.json");
            std::fs::write(&grouped, "old").unwrap();
            let own = std::fs::metadata(&grouped).unwrap().gid();
            if let Some(other) = other_group(own) {
                std::os::unix::fs::chown(&grouped, None, Some(other)).unwrap();
                std::fs::set_permissions(&grouped, std::fs::Permissions::from_mode(0o640)).unwrap();
                write(&grouped, "new");
                let meta = std::fs::metadata(&grouped).unwrap();
                assert_eq!(meta.gid(), other);
                assert_eq!(meta.permissions().mode() & 0o7777, 0o640);
            }
        }

        // No temporary file is left anywhere.
        for sub in [dir.clone(), dir.join("real")] {
            for entry in std::fs::read_dir(sub).unwrap() {
                let name = entry.unwrap().file_name().to_string_lossy().into_owned();
                assert!(!name.contains(".rediscope-"), "{name}");
            }
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}
