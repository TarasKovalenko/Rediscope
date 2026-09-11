//! Value codecs: how the bytes Redis stores become text a person can read and
//! edit, and how that text becomes bytes again on save.
//!
//! Plenty of values are not text. Applications compress them (gzip, zlib,
//! zstd, lz4, brotli), pack them (MessagePack) or wrap them (base64). Without a
//! codec the value pane can only show those as a hex dump. A codec decodes
//! them for viewing and, where the round trip is safe, encodes an edit back.
//!
//! Nothing here touches the network. Decoding a large value is CPU work and a
//! custom codec runs an external program, so callers run these functions off
//! the render loop.

use std::io::{Read, Write};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};

/// Most bytes one value may decode to. A few kilobytes of gzip can expand to
/// gigabytes, so every decoder stops here rather than exhausting memory.
pub const DECODE_LIMIT: usize = 32 * 1024 * 1024;
/// Most decoded text kept for the value pane. The pane redraws the whole value
/// every frame, so more than this is cut, marked as cut, and made read-only.
pub const TEXT_LIMIT: usize = 1024 * 1024;
/// Largest value the hex view will spell out: three characters per byte, so
/// this keeps the hex text within [`TEXT_LIMIT`].
pub const HEX_LIMIT: usize = 256 * 1024;
/// Largest MessagePack value decoded. A byte of MessagePack can stand for a
/// much larger JSON value and in-memory tree, so input is capped too.
pub const MSGPACK_LIMIT: usize = 1024 * 1024;
/// How long a custom codec's program may run before it is killed.
const CUSTOM_TIMEOUT: Duration = Duration::from_secs(10);
/// The longest `timeout_secs` honoured.
const CUSTOM_TIMEOUT_MAX: u64 = 600;
/// How much of a failing program's stderr is kept for the error message.
const STDERR_LIMIT: usize = 4 * 1024;

/// A codec compiled into rediscope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Builtin {
    Gzip,
    Zlib,
    Deflate,
    Zstd,
    Lz4,
    Brotli,
    MsgPack,
    Base64,
    Hex,
}

impl Builtin {
    pub const ALL: [Builtin; 9] = [
        Builtin::Gzip,
        Builtin::Zlib,
        Builtin::Deflate,
        Builtin::Zstd,
        Builtin::Lz4,
        Builtin::Brotli,
        Builtin::MsgPack,
        Builtin::Base64,
        Builtin::Hex,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Gzip => "gzip",
            Self::Zlib => "zlib",
            Self::Deflate => "deflate",
            Self::Zstd => "zstd",
            Self::Lz4 => "lz4",
            Self::Brotli => "brotli",
            Self::MsgPack => "msgpack",
            Self::Base64 => "base64",
            Self::Hex => "hex",
        }
    }

    pub fn description(self) -> &'static str {
        match self {
            Self::Gzip => "gzip stream (RFC 1952)",
            Self::Zlib => "zlib stream (RFC 1950)",
            Self::Deflate => "raw deflate, no header (RFC 1951)",
            Self::Zstd => "Zstandard frame",
            Self::Lz4 => "LZ4 frame",
            Self::Brotli => "Brotli stream",
            Self::MsgPack => "MessagePack, shown as JSON",
            Self::Base64 => "standard base64 with padding",
            Self::Hex => "every byte as hex, editable",
        }
    }
}

/// A codec defined in the config file: an external program that reads the
/// stored bytes on stdin and prints the text to show, plus optionally one
/// that does the reverse so the value can be edited.
///
/// Commands are argument lists, never shell strings, so nothing in a value
/// can be interpreted as shell syntax — the value only ever reaches stdin.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustomCodec {
    pub name: String,
    /// Program and arguments that turn stored bytes into text.
    pub decode: Vec<String>,
    /// Program and arguments that turn edited text back into stored bytes.
    /// Without one, values shown through this codec are read-only.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub encode: Vec<String>,
    /// Seconds either program may run before it is killed. Defaults to 10.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
}

impl CustomCodec {
    /// Why this definition cannot be used, if it cannot.
    pub fn problem(&self) -> Option<&'static str> {
        let name = self.name.trim();
        if name.is_empty() {
            Some("a custom codec needs a name")
        } else if ["auto", "plain"].contains(&name) || Builtin::ALL.iter().any(|b| b.name() == name)
        {
            Some("a custom codec cannot reuse a built-in view's name")
        } else if self.decode.first().is_none_or(|p| p.trim().is_empty()) {
            Some("a custom codec needs a decode command")
        } else {
            None
        }
    }

    fn timeout(&self) -> Duration {
        self.timeout_secs
            .filter(|s| *s > 0)
            .map_or(CUSTOM_TIMEOUT, |s| {
                Duration::from_secs(s.min(CUSTOM_TIMEOUT_MAX))
            })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Codec {
    Builtin(Builtin),
    Custom(CustomCodec),
}

impl Codec {
    pub fn name(&self) -> &str {
        match self {
            Self::Builtin(b) => b.name(),
            Self::Custom(c) => &c.name,
        }
    }

    /// Whether an edit can be written back through this codec at all.
    pub fn can_encode(&self) -> bool {
        match self {
            Self::Builtin(_) => true,
            Self::Custom(c) => !c.encode.is_empty(),
        }
    }
}

/// How the value pane should look at a key's bytes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum View {
    /// Text stays text. Bytes that are not text are checked for a known
    /// compression or MessagePack signature and decoded when one matches,
    /// and shown as a hex dump otherwise.
    #[default]
    Auto,
    /// Exactly as stored: text, or a hex dump. No codec is ever applied.
    Plain,
    /// Always through this codec.
    Codec(Codec),
}

impl View {
    pub fn label(&self) -> String {
        match self {
            Self::Auto => "auto".into(),
            Self::Plain => "plain".into(),
            Self::Codec(Codec::Builtin(b)) => b.name().into(),
            Self::Codec(Codec::Custom(c)) => format!("{} (custom)", c.name),
        }
    }

    pub fn description(&self) -> String {
        match self {
            Self::Auto => "text as stored; detect compressed and MessagePack bytes".into(),
            Self::Plain => "exactly as stored: text, or a hex dump".into(),
            Self::Codec(Codec::Builtin(b)) => b.description().into(),
            Self::Codec(Codec::Custom(c)) => {
                let mut line = c.decode.join(" ");
                if c.encode.is_empty() {
                    line.push_str("  (read-only)");
                }
                line
            }
        }
    }

    /// Every view on offer: the built-in ones, then each usable custom codec.
    pub fn all(custom: &[CustomCodec]) -> Vec<View> {
        let mut views = vec![View::Auto, View::Plain];
        views.extend(Builtin::ALL.iter().map(|b| View::Codec(Codec::Builtin(*b))));
        views.extend(
            custom
                .iter()
                .filter(|c| c.problem().is_none())
                .map(|c| View::Codec(Codec::Custom(c.clone()))),
        );
        views
    }
}

/// How the text on screen was produced from the stored bytes. Kept with the
/// value so an edit can be encoded the same way and checked against exactly
/// the bytes that were read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decoding {
    pub codec: Codec,
    /// The bytes as stored.
    pub raw: Vec<u8>,
    /// Why an edit cannot be written back through the codec, when it cannot.
    pub read_only: Option<String>,
}

/// One stored value, seen through a [`View`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Shown {
    /// Shown as stored: the text itself, or a hex dump of bytes that are not.
    Plain(String),
    /// Decoded by a codec.
    Decoded { text: String, decoding: Decoding },
    /// The view's codec could not read the bytes, so they are shown as stored.
    Failed { text: String, error: String },
}

/// What decoding may spend across every value of one read: decompressed
/// bytes, and wall-clock time for custom codecs. A collection shares one, so a
/// thousand small compression bombs, or a thousand calls to a program that
/// hangs, cannot add up to more than one value is allowed.
#[derive(Debug)]
pub struct Budget {
    bytes: usize,
    deadline: Option<Instant>,
}

impl Budget {
    pub fn new(bytes: usize) -> Self {
        Self {
            bytes,
            deadline: None,
        }
    }

    fn limit(&self) -> usize {
        self.bytes.min(DECODE_LIMIT)
    }

    fn spend(&mut self, bytes: usize) {
        self.bytes = self.bytes.saturating_sub(bytes);
    }

    /// Time left for `codec`'s programs. The clock starts at the first call,
    /// and every later element of the same read shares what is left of it.
    fn time_left(&mut self, codec: &CustomCodec) -> Duration {
        let deadline = *self
            .deadline
            .get_or_insert_with(|| Instant::now() + codec.timeout());
        deadline.saturating_duration_since(Instant::now())
    }
}

/// Look at one stored value through `view`.
pub fn show(bytes: Vec<u8>, view: &View) -> Shown {
    show_with(bytes, view, &mut Budget::new(DECODE_LIMIT))
}

/// [`show`], charging what decoding costs to `budget`.
pub fn show_with(bytes: Vec<u8>, view: &View, budget: &mut Budget) -> Shown {
    match view {
        View::Plain => Shown::Plain(crate::redis_client::text_or_dump(bytes)),
        View::Auto => match String::from_utf8(bytes) {
            // Text is never second-guessed: a value that reads as text is
            // shown, and edited, exactly as it is today.
            Ok(text) => Shown::Plain(text),
            Err(e) => {
                let bytes = e.into_bytes();
                let Some(codec) = detect(&bytes).map(Codec::Builtin) else {
                    return Shown::Plain(crate::redis_client::text_or_dump(bytes));
                };
                match decode_with(&codec, &bytes, budget) {
                    Ok((text, read_only)) => Shown::Decoded {
                        text,
                        decoding: Decoding {
                            codec,
                            raw: bytes,
                            read_only,
                        },
                    },
                    // A signature match that does not decode was a
                    // coincidence, or the budget ran out: show the bytes.
                    Err(_) => Shown::Plain(crate::redis_client::text_or_dump(bytes)),
                }
            }
        },
        View::Codec(codec) => match decode_with(codec, &bytes, budget) {
            Ok((text, read_only)) => Shown::Decoded {
                text,
                decoding: Decoding {
                    codec: codec.clone(),
                    raw: bytes,
                    read_only,
                },
            },
            Err(e) => Shown::Failed {
                text: crate::redis_client::text_or_dump(bytes),
                error: format!("{e:#}"),
            },
        },
    }
}

/// The built-in codec whose signature these bytes carry. Only formats with an
/// unambiguous header are recognised; brotli, raw deflate, base64 and hex have
/// none and are only ever applied when chosen.
pub fn detect(bytes: &[u8]) -> Option<Builtin> {
    if bytes.starts_with(&[0x1f, 0x8b]) {
        return Some(Builtin::Gzip);
    }
    if bytes.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        return Some(Builtin::Zstd);
    }
    if bytes.starts_with(&[0x04, 0x22, 0x4d, 0x18]) {
        return Some(Builtin::Lz4);
    }
    if let [cmf, flg, ..] = *bytes {
        // Method 8 (deflate), a window no larger than 32K, no preset
        // dictionary, and the header check that makes the pair a multiple of 31.
        if cmf & 0x0f == 8
            && cmf >> 4 <= 7
            && flg & 0x20 == 0
            && (u16::from(cmf) << 8 | u16::from(flg)) % 31 == 0
        {
            return Some(Builtin::Zlib);
        }
    }
    if is_msgpack_container(bytes) {
        return Some(Builtin::MsgPack);
    }
    None
}

/// A MessagePack map or array that accounts for every byte. Arbitrary binary
/// seldom passes that, which is what makes it safe to try automatically.
fn is_msgpack_container(bytes: &[u8]) -> bool {
    bytes.len() <= MSGPACK_LIMIT
        && matches!(bytes.first(), Some(0x80..=0x9f | 0xdc..=0xdf))
        && matches!(
            read_msgpack(bytes),
            Ok(rmpv::Value::Map(_) | rmpv::Value::Array(_))
        )
}

/// Decode stored bytes to text, and say why the text cannot be written back
/// when it cannot.
pub fn decode(codec: &Codec, bytes: &[u8]) -> Result<(String, Option<String>)> {
    decode_with(codec, bytes, &mut Budget::new(DECODE_LIMIT))
}

fn decode_with(
    codec: &Codec,
    bytes: &[u8],
    budget: &mut Budget,
) -> Result<(String, Option<String>)> {
    let limit = budget.limit();
    if limit == 0 {
        bail!("the values before this one used up the read's decode budget");
    }
    let time = match codec {
        Codec::Custom(custom) => {
            let left = budget.time_left(custom);
            if left.is_zero() {
                bail!(
                    "the '{}' codec used up its {} s for this read",
                    custom.name,
                    custom.timeout().as_secs()
                );
            }
            left
        }
        Codec::Builtin(_) => Duration::ZERO,
    };
    // Failures are charged as well: a bomb refused at the limit still cost
    // the work of expanding it that far.
    let mut spent = 0;
    let result = decode_inner(codec, bytes, limit, time, &mut spent);
    budget.spend(spent);
    // When the read's budget, not the value, set the limit, say so: "larger
    // than 0.7 MiB" would describe the value wrongly.
    if limit < DECODE_LIMIT
        && let Err(e) = &result
        && format!("{e:#}").contains("larger than")
    {
        bail!(
            "the values before this one left only {} of the read's decode budget",
            mib(limit)
        );
    }
    result
}

fn decode_inner(
    codec: &Codec,
    bytes: &[u8],
    limit: usize,
    time: Duration,
    spent: &mut usize,
) -> Result<(String, Option<String>)> {
    let builtin = match codec {
        Codec::Builtin(b) => *b,
        Codec::Custom(custom) => {
            let out = run(&custom.decode, bytes, time, limit)
                .with_context(|| format!("{} decode", custom.name))?;
            *spent += out.len();
            let (text, mut read_only) = finish(out);
            if read_only.is_none() && custom.encode.is_empty() {
                read_only = Some(format!(
                    "the '{}' codec has no encode command configured",
                    custom.name
                ));
            }
            return Ok((text, read_only));
        }
    };
    let out = match builtin {
        Builtin::Gzip => {
            let mut d = flate2::bufread::MultiGzDecoder::new(bytes);
            let out = read_limited(&mut d, limit, spent)?;
            consumed(d.get_ref(), "gzip")?;
            out
        }
        Builtin::Zlib => {
            let mut d = flate2::bufread::ZlibDecoder::new(bytes);
            let out = read_limited(&mut d, limit, spent)?;
            consumed(d.get_ref(), "zlib")?;
            out
        }
        Builtin::Deflate => {
            let mut d = flate2::bufread::DeflateDecoder::new(bytes);
            let out = read_limited(&mut d, limit, spent)?;
            consumed(d.get_ref(), "deflate")?;
            out
        }
        Builtin::Zstd => zstd_frames(bytes, limit, spent)?,
        Builtin::Lz4 => lz4_frames(bytes, limit, spent)?,
        Builtin::Brotli => brotli_stream(bytes, limit, spent)?,
        Builtin::MsgPack => return msgpack_text(bytes, spent),
        Builtin::Base64 => {
            use base64::Engine as _;
            let engines = [
                &base64::engine::general_purpose::STANDARD,
                &base64::engine::general_purpose::STANDARD_NO_PAD,
                &base64::engine::general_purpose::URL_SAFE,
                &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            ];
            let decoded = engines
                .iter()
                .find_map(|e| e.decode(bytes).ok())
                .ok_or_else(|| anyhow!("not valid base64"))?;
            *spent += decoded.len();
            let (text, mut read_only) = finish(decoded);
            // Saving writes standard, padded base64. A value stored in another
            // flavour would come back different, so it stays read-only.
            if read_only.is_none()
                && base64::engine::general_purpose::STANDARD
                    .encode(text.as_bytes())
                    .as_bytes()
                    != bytes
            {
                read_only = Some(
                    "it is not stored as standard padded base64, so saving would change its format"
                        .into(),
                );
            }
            return Ok((text, read_only));
        }
        Builtin::Hex => {
            if bytes.len() > HEX_LIMIT {
                bail!(
                    "value is {} bytes; the hex view stops at {} KiB",
                    bytes.len(),
                    HEX_LIMIT / 1024
                );
            }
            let text = hex_text(bytes);
            *spent += text.len();
            return Ok((text, None));
        }
    };
    Ok(finish(out))
}

/// Every zstd frame in `bytes`, concatenated, as `zstd -d` would produce.
/// Skippable frames are passed over.
fn zstd_frames(bytes: &[u8], limit: usize, spent: &mut usize) -> Result<Vec<u8>> {
    use ruzstd::decoding::errors::{FrameDecoderError, ReadFrameHeaderError};
    if bytes.is_empty() {
        bail!("not a zstd frame: the value is empty");
    }
    let mut source = bytes;
    let mut out = Vec::new();
    while !source.is_empty() {
        match ruzstd::decoding::StreamingDecoder::new(&mut source) {
            Ok(mut frame) => read_into(&mut frame, &mut out, limit, spent)?,
            Err(FrameDecoderError::ReadFrameHeaderError(ReadFrameHeaderError::SkipFrame {
                length,
                ..
            })) => {
                let length = length as usize;
                if length > source.len() {
                    bail!("zstd skippable frame runs past the end of the value");
                }
                source = &source[length..];
            }
            Err(e) => bail!("not a zstd frame: {e}"),
        }
    }
    Ok(out)
}

/// Every LZ4 frame in `bytes`, concatenated, as `lz4 -d` would produce.
fn lz4_frames(bytes: &[u8], limit: usize, spent: &mut usize) -> Result<Vec<u8>> {
    if bytes.is_empty() {
        bail!("not an LZ4 frame: the value is empty");
    }
    let mut source = bytes;
    let mut out = Vec::new();
    while !source.is_empty() {
        let before = source.len();
        let mut frame = lz4_flex::frame::FrameDecoder::new(source);
        read_into(&mut frame, &mut out, limit, spent)?;
        source = frame.into_inner();
        if source.len() == before {
            bail!("{} byte(s) after the LZ4 frames are not a frame", before);
        }
    }
    Ok(out)
}

/// A brotli stream that must account for every byte of `bytes`.
fn brotli_stream(bytes: &[u8], limit: usize, spent: &mut usize) -> Result<Vec<u8>> {
    use brotli_decompressor::{BrotliDecompressStream, BrotliResult, BrotliState, StandardAlloc};
    let mut state = BrotliState::new(
        StandardAlloc::default(),
        StandardAlloc::default(),
        StandardAlloc::default(),
    );
    let mut available_in = bytes.len();
    let mut input_offset = 0;
    let mut total_out = 0;
    let mut buf = vec![0u8; 64 * 1024];
    let mut out = Vec::new();
    loop {
        let mut available_out = buf.len();
        let mut output_offset = 0;
        let result = BrotliDecompressStream(
            &mut available_in,
            &mut input_offset,
            bytes,
            &mut available_out,
            &mut output_offset,
            &mut buf,
            &mut total_out,
            &mut state,
        );
        *spent += output_offset;
        if out.len() + output_offset > limit {
            bail!("decoded value is larger than {}", mib(limit));
        }
        out.extend_from_slice(&buf[..output_offset]);
        match result {
            BrotliResult::ResultSuccess => break,
            BrotliResult::NeedsMoreOutput => continue,
            BrotliResult::NeedsMoreInput => bail!("the brotli stream is cut short"),
            BrotliResult::ResultFailure => bail!("not a brotli stream"),
        }
    }
    consumed(&bytes[input_offset..], "brotli")?;
    Ok(out)
}

/// MessagePack as JSON text. Only saved back when re-encoding the JSON
/// reproduces the stored bytes exactly.
fn msgpack_text(bytes: &[u8], spent: &mut usize) -> Result<(String, Option<String>)> {
    if bytes.len() > MSGPACK_LIMIT {
        bail!(
            "value is {} bytes; MessagePack is decoded up to {}",
            bytes.len(),
            mib(MSGPACK_LIMIT)
        );
    }
    let value = read_msgpack(bytes)?;
    let json = msgpack_to_json(&value);
    let text = serde_json::to_string(&json)?;
    *spent += text.len();
    let repacked = json_to_msgpack(&json);
    let (text, mut read_only) = shorten(text, None);
    if read_only.is_none() {
        // JSON has no binary, extension or float32 type and only string
        // keys. When the document needed one of those, saving the JSON back
        // would quietly change it.
        if repacked != value {
            read_only = Some(
                "it holds MessagePack data JSON cannot represent exactly \
                 (binary, extension, float32, NaN or non-string keys)"
                    .into(),
            );
        } else {
            let mut exact = Vec::new();
            rmpv::encode::write_value(&mut exact, &repacked)?;
            if exact != bytes {
                read_only = Some(
                    "it uses MessagePack encodings rediscope would write differently, \
                     so saving would change bytes you did not edit"
                        .into(),
                );
            }
        }
    }
    Ok((text, read_only))
}

/// Encode edited text into the bytes to store.
pub fn encode(codec: &Codec, text: &str) -> Result<Vec<u8>> {
    let data = text.as_bytes();
    Ok(match codec {
        Codec::Builtin(Builtin::Gzip) => {
            let mut w = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            w.write_all(data)?;
            w.finish()?
        }
        Codec::Builtin(Builtin::Zlib) => {
            let mut w = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
            w.write_all(data)?;
            w.finish()?
        }
        Codec::Builtin(Builtin::Deflate) => {
            let mut w =
                flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
            w.write_all(data)?;
            w.finish()?
        }
        Codec::Builtin(Builtin::Zstd) => {
            ruzstd::encoding::compress_to_vec(data, ruzstd::encoding::CompressionLevel::Fastest)
        }
        Codec::Builtin(Builtin::Lz4) => {
            let mut w = lz4_flex::frame::FrameEncoder::new(Vec::new());
            w.write_all(data)?;
            w.finish().map_err(|e| anyhow!("lz4: {e}"))?
        }
        Codec::Builtin(Builtin::Brotli) => {
            let mut out = Vec::new();
            {
                let mut w = brotli::CompressorWriter::new(&mut out, 4096, 5, 22);
                w.write_all(data)?;
                w.flush()?;
            }
            out
        }
        Codec::Builtin(Builtin::MsgPack) => {
            let json: serde_json::Value = serde_json::from_str(text)
                .map_err(|e| anyhow!("MessagePack is edited as JSON: {e}"))?;
            let mut out = Vec::new();
            rmpv::encode::write_value(&mut out, &json_to_msgpack(&json))?;
            out
        }
        Codec::Builtin(Builtin::Base64) => {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD
                .encode(data)
                .into_bytes()
        }
        Codec::Builtin(Builtin::Hex) => parse_hex(text)?,
        Codec::Custom(custom) => {
            if custom.encode.is_empty() {
                bail!(
                    "the '{}' codec has no encode command configured",
                    custom.name
                );
            }
            run(&custom.encode, data, custom.timeout(), DECODE_LIMIT)
                .with_context(|| format!("{} encode", custom.name))?
        }
    })
}

/// Read a decoder to the end, refusing to go past `limit` bytes.
fn read_limited(reader: &mut impl Read, limit: usize, spent: &mut usize) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    read_into(reader, &mut out, limit, spent)?;
    Ok(out)
}

/// Append a decoder's output to `out`, which may not grow past `limit`.
fn read_into(
    reader: &mut impl Read,
    out: &mut Vec<u8>,
    limit: usize,
    spent: &mut usize,
) -> Result<()> {
    let room = limit.saturating_sub(out.len()) as u64;
    let before = out.len();
    let result = reader.take(room + 1).read_to_end(out);
    *spent += out.len() - before;
    result.map_err(|e| anyhow!("{e}"))?;
    if out.len() > limit {
        bail!("decoded value is larger than {}", mib(limit));
    }
    Ok(())
}

/// A decoder that stopped before the end of the value left bytes it did not
/// understand. Showing, and worse saving, only the part before them would lose
/// the rest.
fn consumed(rest: &[u8], what: &str) -> Result<()> {
    if rest.is_empty() {
        Ok(())
    } else {
        bail!(
            "{} unexpected byte(s) after the end of the {what} stream",
            rest.len()
        )
    }
}

/// Decoded bytes as text. Bytes that are still not text are shown as a hex
/// dump, which is never saved back.
fn finish(bytes: Vec<u8>) -> (String, Option<String>) {
    match String::from_utf8(bytes) {
        Ok(text) => shorten(text, None),
        Err(e) => (
            crate::redis_client::text_or_dump(e.into_bytes()),
            Some("the decoded bytes are not text either and are shown as a hex dump".into()),
        ),
    }
}

/// Hold decoded text to what the value pane can draw. The pane re-renders
/// the whole value every frame, so megabytes of decoded JSON would freeze it.
/// Cut text is shown, marked as cut, and never saved back.
fn shorten(mut text: String, read_only: Option<String>) -> (String, Option<String>) {
    if text.len() <= TEXT_LIMIT {
        return (text, read_only);
    }
    let total = text.len();
    let mut cut = TEXT_LIMIT;
    while !text.is_char_boundary(cut) {
        cut -= 1;
    }
    text.truncate(cut);
    text.push_str("\n…");
    (
        text,
        Some(format!(
            "it decodes to {} and only the first {} is shown",
            mib(total),
            mib(TEXT_LIMIT)
        )),
    )
}

fn mib(bytes: usize) -> String {
    if bytes < 1024 * 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
    }
}

fn read_msgpack(bytes: &[u8]) -> Result<rmpv::Value> {
    let mut rest = bytes;
    let value = rmpv::decode::read_value(&mut rest).map_err(|e| anyhow!("not MessagePack: {e}"))?;
    if !rest.is_empty() {
        bail!(
            "not MessagePack: {} unexpected byte(s) after the value",
            rest.len()
        );
    }
    Ok(value)
}

fn msgpack_to_json(value: &rmpv::Value) -> serde_json::Value {
    use rmpv::Value as M;
    use serde_json::Value as J;
    match value {
        M::Nil => J::Null,
        M::Boolean(b) => J::Bool(*b),
        M::Integer(i) => i
            .as_u64()
            .map(J::from)
            .or_else(|| i.as_i64().map(J::from))
            .unwrap_or(J::Null),
        M::F32(f) => float_json(f64::from(*f)),
        M::F64(f) => float_json(*f),
        M::String(s) => match s.as_str() {
            Some(s) => J::String(s.to_string()),
            None => J::String(String::from_utf8_lossy(s.as_bytes()).into_owned()),
        },
        M::Binary(b) => {
            use base64::Engine as _;
            J::String(base64::engine::general_purpose::STANDARD.encode(b))
        }
        M::Array(items) => J::Array(items.iter().map(msgpack_to_json).collect()),
        M::Map(pairs) => J::Object(
            pairs
                .iter()
                .map(|(k, v)| {
                    let key = match k {
                        M::String(s) if s.as_str().is_some() => {
                            s.as_str().unwrap_or_default().into()
                        }
                        other => msgpack_to_json(other).to_string(),
                    };
                    (key, msgpack_to_json(v))
                })
                .collect(),
        ),
        M::Ext(tag, data) => {
            use base64::Engine as _;
            serde_json::json!({ "$ext": tag, "data": base64::engine::general_purpose::STANDARD.encode(data) })
        }
    }
}

fn float_json(f: f64) -> serde_json::Value {
    serde_json::Number::from_f64(f).map_or(serde_json::Value::Null, serde_json::Value::Number)
}

fn json_to_msgpack(value: &serde_json::Value) -> rmpv::Value {
    use rmpv::Value as M;
    use serde_json::Value as J;
    match value {
        J::Null => M::Nil,
        J::Bool(b) => M::Boolean(*b),
        J::Number(n) => {
            if let Some(u) = n.as_u64() {
                M::from(u)
            } else if let Some(i) = n.as_i64() {
                M::from(i)
            } else {
                M::F64(n.as_f64().unwrap_or_default())
            }
        }
        J::String(s) => M::from(s.as_str()),
        J::Array(items) => M::Array(items.iter().map(json_to_msgpack).collect()),
        J::Object(map) => M::Map(
            map.iter()
                .map(|(k, v)| (M::from(k.as_str()), json_to_msgpack(v)))
                .collect(),
        ),
    }
}

/// Bytes as lines of sixteen space-separated hex pairs.
fn hex_text(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 3);
    for (i, chunk) in bytes.chunks(16).enumerate() {
        if i > 0 {
            out.push('\n');
        }
        for (j, b) in chunk.iter().enumerate() {
            if j > 0 {
                out.push(' ');
            }
            let _ = write!(out, "{b:02x}");
        }
    }
    out
}

/// Hex text back to bytes. Whitespace is ignored, so the lines the hex view
/// shows can be edited freely.
fn parse_hex(text: &str) -> Result<Vec<u8>> {
    let digits: Vec<u8> = text.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    if !digits.len().is_multiple_of(2) {
        bail!("hex needs an even number of digits");
    }
    digits
        .chunks(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).unwrap_or("??");
            u8::from_str_radix(pair, 16).map_err(|_| anyhow!("'{pair}' is not a hex byte"))
        })
        .collect()
}

/// Run `argv` with `input` on stdin and return its stdout. The program is
/// killed when it outlives `timeout`, and its output may not exceed `limit`.
///
/// On Unix the program leads its own process group, and the whole group is
/// killed on timeout, so a shell script's children do not outlive it.
fn run(argv: &[String], input: &[u8], timeout: Duration, limit: usize) -> Result<Vec<u8>> {
    use std::process::Stdio;
    use std::sync::mpsc;

    let mut command = command(argv)?;
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("cannot start {}", command.get_program().display()))?;

    // Each pipe gets its own thread, so a program that writes before it has
    // read all of its input cannot deadlock against us.
    let mut stdin = child.stdin.take().context("no stdin")?;
    let input = input.to_vec();
    std::thread::spawn(move || {
        // A program that exits without reading everything closes the pipe.
        let _ = stdin.write_all(&input);
    });
    let (out_tx, out_rx) = mpsc::channel();
    let stdout = child.stdout.take().context("no stdout")?;
    std::thread::spawn(move || {
        let mut out = Vec::new();
        let result = stdout
            .take(limit as u64 + 1)
            .read_to_end(&mut out)
            .map(|_| out);
        let _ = out_tx.send(result);
    });
    let (err_tx, err_rx) = mpsc::channel();
    let stderr = child.stderr.take().context("no stderr")?;
    std::thread::spawn(move || {
        let mut err = Vec::new();
        let _ = stderr.take(STDERR_LIMIT as u64).read_to_end(&mut err);
        let _ = err_tx.send(err);
    });

    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            kill_group(&mut child);
            let _ = child.wait();
            bail!("timed out after {} s", timeout.as_secs().max(1));
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    // The pipes close when the program exits, unless it left a child of its
    // own holding them. Waiting is bounded either way, and whatever is still
    // holding them is killed with the group.
    let grace = deadline
        .saturating_duration_since(Instant::now())
        .max(Duration::from_millis(500));
    let out = match out_rx.recv_timeout(grace) {
        Ok(out) => out.map_err(|e| anyhow!("reading output: {e}"))?,
        Err(_) => {
            kill_group(&mut child);
            bail!("the program exited but something it started kept its output open");
        }
    };
    // Checked before the exit status: a program cut off at the limit usually
    // dies of the closed pipe, which would hide the real reason.
    if out.len() > limit {
        bail!("output is larger than {}", mib(limit));
    }
    if !status.success() {
        let err = err_rx
            .recv_timeout(Duration::from_millis(500))
            .unwrap_or_default();
        let err = String::from_utf8_lossy(&err);
        let err = err.trim();
        bail!(
            "exited with {status}{}",
            if err.is_empty() {
                String::new()
            } else {
                format!(": {err}")
            }
        );
    }
    Ok(out)
}

/// The process a custom codec runs, before its pipes are attached.
fn command(argv: &[String]) -> Result<std::process::Command> {
    let (program, args) = argv.split_first().context("no command configured")?;
    let mut command = std::process::Command::new(crate::config::expand_home(program));
    // The Redis password rediscope was started with is not the codec's
    // business.
    command.args(args).env_remove("REDISCOPE_PASSWORD");
    #[cfg(unix)]
    std::os::unix::process::CommandExt::process_group(&mut command, 0);
    Ok(command)
}

/// Kill the program and, on Unix, every process in its group.
fn kill_group(child: &mut std::process::Child) {
    #[cfg(unix)]
    if let Ok(pgid) = libc::pid_t::try_from(child.id()) {
        // SAFETY: killpg only sends a signal. The group was created for this
        // child with process_group(0), so it names nothing else while any of
        // its members is alive — which is the case on both paths that call
        // this: the child itself on timeout, or whatever still holds its
        // output after it exited. The one gap is a descendant that moved to a
        // session of its own with setsid: the group can then be empty and its
        // id reused, which needs a pid to be recycled within that window.
        unsafe {
            libc::killpg(pgid, libc::SIGKILL);
        }
    }
    let _ = child.kill();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(codec: Builtin) -> Codec {
        Codec::Builtin(codec)
    }

    const DOC: &str = r#"{"user":"ada","tags":["x","y"],"n":42}"#;

    #[test]
    fn every_builtin_round_trips_text() {
        for codec in Builtin::ALL {
            let codec = b(codec);
            if codec == b(Builtin::Hex) {
                // Hex edits hex text: the stored bytes are the document.
                let (text, read_only) = decode(&codec, DOC.as_bytes()).unwrap();
                assert_eq!(read_only, None);
                assert_eq!(encode(&codec, &text).unwrap(), DOC.as_bytes());
                continue;
            }
            let stored = encode(&codec, DOC).unwrap();
            let (text, read_only) = decode(&codec, &stored).unwrap();
            assert_eq!(read_only, None, "{}", codec.name());
            if codec == b(Builtin::MsgPack) {
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&text).unwrap(),
                    serde_json::from_str::<serde_json::Value>(DOC).unwrap()
                );
            } else {
                assert_eq!(text, DOC, "{}", codec.name());
            }
        }
    }

    #[test]
    fn compressed_values_are_recognised_by_their_header() {
        for codec in [Builtin::Gzip, Builtin::Zlib, Builtin::Zstd, Builtin::Lz4] {
            let stored = encode(&b(codec), DOC).unwrap();
            assert_eq!(detect(&stored), Some(codec), "{}", codec.name());
        }
        let packed = encode(&b(Builtin::MsgPack), DOC).unwrap();
        assert_eq!(detect(&packed), Some(Builtin::MsgPack));
    }

    #[test]
    fn formats_without_a_header_are_never_guessed() {
        for codec in [Builtin::Deflate, Builtin::Brotli] {
            let stored = encode(&b(codec), DOC).unwrap();
            assert_ne!(detect(&stored), Some(codec), "{}", codec.name());
        }
        assert_eq!(detect(b"\x80\xfe\x00\x41"), None, "random bytes");
        assert_eq!(detect(&[0x92, 0x01]), None, "array missing an element");
        assert_eq!(detect(&[0x91, 0x01, 0x02]), None, "trailing bytes");
    }

    #[test]
    fn auto_leaves_text_exactly_as_stored() {
        // Base64 and hex are text; auto must not start decoding them, or
        // every value that happens to look like base64 would change on screen.
        for text in ["hello", "aGVsbG8=", "deadbeef", DOC, ""] {
            assert_eq!(
                show(text.as_bytes().to_vec(), &View::Auto),
                Shown::Plain(text.into())
            );
        }
    }

    #[test]
    fn auto_decodes_gzipped_json_and_keeps_the_stored_bytes() {
        let stored = encode(&b(Builtin::Gzip), DOC).unwrap();
        let Shown::Decoded { text, decoding } = show(stored.clone(), &View::Auto) else {
            panic!("expected a decoded value");
        };
        assert_eq!(text, DOC);
        assert_eq!(decoding.codec, b(Builtin::Gzip));
        assert_eq!(decoding.raw, stored);
        assert_eq!(decoding.read_only, None);
    }

    #[test]
    fn auto_and_plain_show_undecodable_bytes_as_a_hex_dump() {
        let bytes = vec![0x80, 0xfe, 0x00, 0x41];
        for view in [View::Auto, View::Plain] {
            let Shown::Plain(text) = show(bytes.clone(), &view) else {
                panic!("expected plain");
            };
            assert!(crate::redis_client::is_hex_dump(&text), "{text}");
        }
        // A gzip header on garbage is a coincidence, not a gzip stream.
        let Shown::Plain(text) = show(vec![0x1f, 0x8b, 0xff, 0xff], &View::Auto) else {
            panic!("expected plain");
        };
        assert!(crate::redis_client::is_hex_dump(&text));
    }

    #[test]
    fn plain_never_decodes() {
        let stored = encode(&b(Builtin::Gzip), DOC).unwrap();
        assert!(
            matches!(show(stored, &View::Plain), Shown::Plain(t) if crate::redis_client::is_hex_dump(&t))
        );
    }

    #[test]
    fn a_chosen_codec_that_cannot_read_the_value_says_why() {
        let Shown::Failed { text, error } = show(b"hello".to_vec(), &View::Codec(b(Builtin::Gzip)))
        else {
            panic!("expected a failure");
        };
        assert_eq!(text, "hello", "the value is still shown as stored");
        assert!(!error.is_empty());
    }

    #[test]
    fn decompression_stops_at_the_limit() {
        let big = "0".repeat(4096);
        for codec in [
            Builtin::Gzip,
            Builtin::Zlib,
            Builtin::Deflate,
            Builtin::Zstd,
            Builtin::Lz4,
            Builtin::Brotli,
        ] {
            let stored = encode(&b(codec), &big).unwrap();
            let err = decode_with(&b(codec), &stored, &mut Budget::new(1024)).unwrap_err();
            assert!(
                err.to_string().contains("decode budget"),
                "{}: {err}",
                codec.name()
            );
            assert!(decode_with(&b(codec), &stored, &mut Budget::new(4096)).is_ok());
        }
    }

    #[test]
    fn a_budget_of_zero_decodes_nothing() {
        let stored = encode(&b(Builtin::Gzip), DOC).unwrap();
        assert!(matches!(
            show_with(stored.clone(), &View::Auto, &mut Budget::new(0)),
            Shown::Plain(t) if crate::redis_client::is_hex_dump(&t)
        ));
        assert!(matches!(
            show_with(stored, &View::Codec(b(Builtin::Gzip)), &mut Budget::new(0)),
            Shown::Failed { .. }
        ));
    }

    #[test]
    fn decompressed_binary_is_shown_but_read_only() {
        let mut w = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        w.write_all(&[0xff, 0x00, 0xfe]).unwrap();
        let stored = w.finish().unwrap();
        let (text, read_only) = decode(&b(Builtin::Gzip), &stored).unwrap();
        assert!(crate::redis_client::is_hex_dump(&text));
        assert!(read_only.is_some());
    }

    #[test]
    fn msgpack_that_json_cannot_hold_is_read_only() {
        let lossy = [
            rmpv::Value::Map(vec![(
                rmpv::Value::from("bin"),
                rmpv::Value::Binary(vec![1, 2]),
            )]),
            rmpv::Value::Array(vec![rmpv::Value::F32(1.5)]),
            rmpv::Value::Map(vec![(rmpv::Value::from(1), rmpv::Value::from("int key"))]),
            rmpv::Value::Array(vec![rmpv::Value::Ext(3, vec![9])]),
        ];
        for value in lossy {
            let mut stored = Vec::new();
            rmpv::encode::write_value(&mut stored, &value).unwrap();
            let (_, read_only) = decode(&b(Builtin::MsgPack), &stored).unwrap();
            assert!(read_only.is_some(), "{value:?}");
        }
        let mut stored = Vec::new();
        let exact = rmpv::Value::Map(vec![
            (rmpv::Value::from("a"), rmpv::Value::from(-5)),
            (rmpv::Value::from("b"), rmpv::Value::F64(2.5)),
            (rmpv::Value::from("c"), rmpv::Value::Nil),
            (rmpv::Value::from("d"), rmpv::Value::from(u64::MAX)),
        ]);
        rmpv::encode::write_value(&mut stored, &exact).unwrap();
        let (text, read_only) = decode(&b(Builtin::MsgPack), &stored).unwrap();
        assert_eq!(read_only, None, "{text}");
        assert_eq!(
            read_msgpack(&encode(&b(Builtin::MsgPack), &text).unwrap()).unwrap(),
            exact
        );
    }

    #[test]
    fn msgpack_edits_must_be_json() {
        assert!(encode(&b(Builtin::MsgPack), "{broken").is_err());
    }

    #[test]
    fn base64_in_another_flavour_is_read_only() {
        let (text, read_only) = decode(&b(Builtin::Base64), b"aGVsbG8=").unwrap();
        assert_eq!((text.as_str(), read_only), ("hello", None));
        let (text, read_only) = decode(&b(Builtin::Base64), b"aGVsbG8").unwrap();
        assert_eq!(text, "hello");
        assert!(read_only.is_some(), "unpadded would be saved padded");
        assert!(decode(&b(Builtin::Base64), b"not base64!").is_err());
    }

    #[test]
    fn hex_edits_ignore_whitespace_and_reject_bad_digits() {
        let (text, _) = decode(&b(Builtin::Hex), &(0u8..20).collect::<Vec<_>>()).unwrap();
        assert_eq!(text.lines().count(), 2, "sixteen bytes per line");
        assert_eq!(parse_hex(&text).unwrap(), (0u8..20).collect::<Vec<_>>());
        assert_eq!(
            parse_hex(" DE ad\n\tbe EF ").unwrap(),
            [0xde, 0xad, 0xbe, 0xef]
        );
        assert!(parse_hex("abc").is_err());
        assert!(parse_hex("zz").is_err());
        assert!(decode(&b(Builtin::Hex), &vec![0; HEX_LIMIT + 1]).is_err());
    }

    #[test]
    fn views_list_builtins_then_usable_custom_codecs() {
        let good = CustomCodec {
            name: "rot13".into(),
            decode: vec!["tr".into()],
            ..Default::default()
        };
        let broken = CustomCodec {
            name: "".into(),
            decode: vec!["tr".into()],
            ..Default::default()
        };
        let views = View::all(&[good.clone(), broken]);
        assert_eq!(views[0], View::Auto);
        assert_eq!(views[1], View::Plain);
        assert_eq!(views.len(), 2 + Builtin::ALL.len() + 1);
        assert_eq!(views.last(), Some(&View::Codec(Codec::Custom(good))));
    }

    #[test]
    fn custom_codecs_deserialize_with_defaults() {
        let c: CustomCodec = serde_json::from_str(r#"{"name":"p","decode":["cat"]}"#).unwrap();
        assert!(c.encode.is_empty());
        assert_eq!(c.timeout(), CUSTOM_TIMEOUT);
        assert!(!Codec::Custom(c.clone()).can_encode());
        let json = serde_json::to_string(&c).unwrap();
        assert!(
            !json.contains("encode") && !json.contains("timeout"),
            "{json}"
        );
    }

    #[test]
    fn concatenated_frames_decode_in_full() {
        for codec in [Builtin::Zstd, Builtin::Lz4, Builtin::Gzip] {
            let mut stored = encode(&b(codec), "first ").unwrap();
            stored.extend(encode(&b(codec), "second").unwrap());
            let (text, read_only) = decode(&b(codec), &stored).unwrap();
            assert_eq!(text, "first second", "{}", codec.name());
            assert_eq!(read_only, None);
        }
    }

    #[test]
    fn zstd_skippable_frames_are_passed_over() {
        let mut stored = vec![0x50, 0x2a, 0x4d, 0x18, 3, 0, 0, 0, 9, 9, 9];
        stored.extend(encode(&b(Builtin::Zstd), "payload").unwrap());
        assert_eq!(decode(&b(Builtin::Zstd), &stored).unwrap().0, "payload");
    }

    #[test]
    fn bytes_after_the_stream_are_never_dropped() {
        // Showing, and saving, only the part before them would lose the rest.
        for codec in [
            Builtin::Gzip,
            Builtin::Zlib,
            Builtin::Deflate,
            Builtin::Zstd,
            Builtin::Lz4,
            Builtin::Brotli,
        ] {
            let mut stored = encode(&b(codec), "first").unwrap();
            stored.extend_from_slice(b"TRAILING");
            assert!(
                decode(&b(codec), &stored).is_err(),
                "{} accepted trailing bytes",
                codec.name()
            );
            // And auto shows such a value as stored rather than half of it.
            if detect(&stored).is_some() {
                assert!(matches!(show(stored, &View::Auto), Shown::Plain(_)));
            }
        }
    }

    #[test]
    fn cut_short_streams_are_errors() {
        for codec in [Builtin::Zstd, Builtin::Lz4, Builtin::Brotli, Builtin::Gzip] {
            let stored = encode(&b(codec), &"abc".repeat(1000)).unwrap();
            let half = &stored[..stored.len() / 2];
            assert!(decode(&b(codec), half).is_err(), "{}", codec.name());
        }
        assert!(decode(&b(Builtin::Zstd), b"").is_err());
        assert!(decode(&b(Builtin::Lz4), b"").is_err());
    }

    #[test]
    fn large_decoded_text_is_cut_for_the_pane_and_read_only() {
        let big = "x".repeat(TEXT_LIMIT + 10);
        let stored = encode(&b(Builtin::Gzip), &big).unwrap();
        let (text, read_only) = decode(&b(Builtin::Gzip), &stored).unwrap();
        assert!(text.len() <= TEXT_LIMIT + 4, "{}", text.len());
        assert!(text.ends_with('…'));
        assert!(read_only.unwrap().contains("only the first"));
        // Cutting never splits a character.
        let wide = "é".repeat(TEXT_LIMIT);
        let (text, _) = shorten(wide, None);
        assert!(text.len() <= TEXT_LIMIT + 4);
    }

    #[test]
    fn oversized_msgpack_is_not_decoded() {
        let mut stored = vec![0xdd];
        stored.extend_from_slice(&((MSGPACK_LIMIT as u32) + 1).to_be_bytes());
        stored.resize(stored.len() + MSGPACK_LIMIT + 1, 0xc0);
        assert_eq!(detect(&stored), None);
        assert!(decode(&b(Builtin::MsgPack), &stored).is_err());
    }

    #[test]
    fn msgpack_that_would_be_rewritten_differently_is_read_only() {
        // 5 stored as a uint32 rather than a positive fixint.
        let stored = [0x81, 0xa1, b'n', 0xce, 0, 0, 0, 5];
        let (text, read_only) = decode(&b(Builtin::MsgPack), &stored).unwrap();
        assert_eq!(text, r#"{"n":5}"#);
        assert!(read_only.unwrap().contains("differently"));
    }

    #[test]
    fn a_shared_budget_stops_decoding_once_spent() {
        let stored = encode(&b(Builtin::Gzip), &"0".repeat(4096)).unwrap();
        let mut budget = Budget::new(6000);
        assert!(matches!(
            show_with(stored.clone(), &View::Auto, &mut budget),
            Shown::Decoded { .. }
        ));
        // 4096 spent: the next one only has 1904 left and is refused, and a
        // refusal is charged too.
        assert!(matches!(
            show_with(stored.clone(), &View::Codec(b(Builtin::Gzip)), &mut budget),
            Shown::Failed { .. }
        ));
        assert!(matches!(
            show_with(stored, &View::Auto, &mut budget),
            Shown::Plain(t) if crate::redis_client::is_hex_dump(&t)
        ));
    }

    #[test]
    fn a_spent_budget_is_named_as_the_reason() {
        let stored = encode(&b(Builtin::Gzip), &"0".repeat(4096)).unwrap();
        let err = decode_with(&b(Builtin::Gzip), &stored, &mut Budget::new(1024)).unwrap_err();
        assert!(err.to_string().contains("decode budget"), "{err}");
    }

    #[test]
    fn custom_codecs_cannot_shadow_builtin_views() {
        for name in ["gzip", "plain", "auto", " hex "] {
            let c = CustomCodec {
                name: name.into(),
                decode: vec!["cat".into()],
                ..Default::default()
            };
            assert!(c.problem().is_some(), "{name}");
        }
    }

    #[test]
    fn codec_programs_never_inherit_the_redis_password() {
        let cmd = command(&["decoder".into(), "--flag".into()]).unwrap();
        assert!(
            cmd.get_envs()
                .any(|(k, v)| k == "REDISCOPE_PASSWORD" && v.is_none()),
            "REDISCOPE_PASSWORD must be removed"
        );
        assert!(command(&[]).is_err());
    }

    #[test]
    fn a_huge_timeout_is_clamped_rather_than_overflowing() {
        let c = CustomCodec {
            timeout_secs: Some(u64::MAX),
            ..Default::default()
        };
        assert_eq!(c.timeout(), Duration::from_secs(CUSTOM_TIMEOUT_MAX));
    }

    #[cfg(unix)]
    mod custom {
        use super::*;

        fn sh(script: &str) -> Vec<String> {
            vec!["sh".into(), "-c".into(), script.into()]
        }

        fn rot13() -> Codec {
            let tr = sh("tr 'A-Za-z' 'N-ZA-Mn-za-m'");
            Codec::Custom(CustomCodec {
                name: "rot13".into(),
                decode: tr.clone(),
                encode: tr,
                timeout_secs: None,
            })
        }

        #[test]
        fn a_custom_codec_round_trips_through_its_programs() {
            let stored = encode(&rot13(), "Hello").unwrap();
            assert_eq!(stored, b"Uryyb");
            assert_eq!(decode(&rot13(), &stored).unwrap(), ("Hello".into(), None));
        }

        #[test]
        fn a_custom_codec_without_encode_is_read_only() {
            let codec = Codec::Custom(CustomCodec {
                name: "cat".into(),
                decode: vec!["cat".into()],
                ..Default::default()
            });
            let (text, read_only) = decode(&codec, b"as is").unwrap();
            assert_eq!(text, "as is");
            assert!(read_only.unwrap().contains("no encode command"));
            assert!(encode(&codec, "x").is_err());
        }

        #[test]
        fn a_failing_program_reports_its_stderr() {
            let codec = Codec::Custom(CustomCodec {
                name: "bad".into(),
                decode: sh("echo 'wrong schema' >&2; exit 3"),
                ..Default::default()
            });
            let err = format!("{:#}", decode(&codec, b"x").unwrap_err());
            assert!(err.contains("wrong schema"), "{err}");
        }

        #[test]
        fn a_hung_program_is_killed_at_the_timeout() {
            let codec = Codec::Custom(CustomCodec {
                name: "slow".into(),
                decode: sh("sleep 30"),
                timeout_secs: Some(1),
                ..Default::default()
            });
            let started = Instant::now();
            let err = format!("{:#}", decode(&codec, b"x").unwrap_err());
            assert!(err.contains("timed out"), "{err}");
            assert!(started.elapsed() < Duration::from_secs(5));
        }

        #[test]
        fn a_missing_program_is_an_error_not_a_panic() {
            let codec = Codec::Custom(CustomCodec {
                name: "gone".into(),
                decode: vec!["/nonexistent/rediscope-codec".into()],
                ..Default::default()
            });
            assert!(decode(&codec, b"x").is_err());
        }

        #[test]
        fn a_program_that_ignores_its_input_still_finishes() {
            let codec = Codec::Custom(CustomCodec {
                name: "ignore".into(),
                decode: sh("echo fixed"),
                ..Default::default()
            });
            let big = vec![b'a'; 4 * 1024 * 1024];
            assert_eq!(decode(&codec, &big).unwrap().0, "fixed\n");
        }

        #[test]
        fn elements_share_one_deadline_for_a_hung_program() {
            let view = View::Codec(Codec::Custom(CustomCodec {
                name: "slow".into(),
                decode: sh("sleep 30"),
                timeout_secs: Some(1),
                ..Default::default()
            }));
            let mut budget = Budget::new(DECODE_LIMIT);
            let started = Instant::now();
            for _ in 0..5 {
                assert!(matches!(
                    show_with(b"x".to_vec(), &view, &mut budget),
                    Shown::Failed { .. }
                ));
            }
            assert!(
                started.elapsed() < Duration::from_secs(3),
                "five elements took {:?}",
                started.elapsed()
            );
        }

        #[test]
        fn a_timeout_kills_what_the_program_started() {
            let dir = std::env::temp_dir().join(format!("rediscope-pg-{}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            let pidfile = dir.join("pid");
            let codec = Codec::Custom(CustomCodec {
                name: "forks".into(),
                decode: sh(&format!(
                    "sleep 30 & echo $! > '{}'; wait",
                    pidfile.display()
                )),
                timeout_secs: Some(1),
                ..Default::default()
            });
            assert!(decode(&codec, b"x").is_err());
            let pid: i32 = std::fs::read_to_string(&pidfile)
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            // Give the kernel a moment to deliver the signal.
            let gone = (0..100).any(|_| {
                std::thread::sleep(Duration::from_millis(20));
                // SAFETY: signal 0 only checks whether the process exists.
                unsafe { libc::kill(pid, 0) != 0 }
            });
            assert!(gone, "the background sleep outlived the timeout");
            let _ = std::fs::remove_dir_all(dir);
        }

        #[test]
        fn too_much_output_says_so() {
            let codec = Codec::Custom(CustomCodec {
                name: "loud".into(),
                decode: sh("head -c 100000 /dev/zero"),
                ..Default::default()
            });
            let err = format!(
                "{:#}",
                decode_with(&codec, b"", &mut Budget::new(1024)).unwrap_err()
            );
            // With the read's budget as the limit, the budget is named as the reason.
            assert!(err.contains("decode budget"), "{err}");
        }

        #[test]
        fn custom_output_stops_at_the_limit() {
            let codec = Codec::Custom(CustomCodec {
                name: "loud".into(),
                decode: sh("head -c 4096 /dev/zero"),
                ..Default::default()
            });
            assert!(decode_with(&codec, b"", &mut Budget::new(1024)).is_err());
        }
    }
}
