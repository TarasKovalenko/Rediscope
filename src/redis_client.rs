//! Async Redis access tailored to the TUI: SCAN-based listing, bounded value
//! reads, and typed mutators. Nothing here blocks the render loop.

use anyhow::{anyhow, Result};
use redis::aio::MultiplexedConnection;
use redis::{
    AsyncCommands, ConnectionAddr, ConnectionInfo, IntoConnectionInfo, RedisConnectionInfo,
};

use crate::config::Connection;

/// Hard ceiling on keys pulled into one tree view.
pub const KEY_LIMIT: usize = 5_000;
/// Hard ceiling on elements pulled into one value pane.
pub const VALUE_LIMIT: usize = 1_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyType {
    String,
    Hash,
    List,
    Set,
    ZSet,
    Stream,
    Other,
}

impl KeyType {
    pub fn parse(s: &str) -> Self {
        match s {
            "string" => Self::String,
            "hash" => Self::Hash,
            "list" => Self::List,
            "set" => Self::Set,
            "zset" => Self::ZSet,
            "stream" => Self::Stream,
            _ => Self::Other,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Hash => "hash",
            Self::List => "list",
            Self::Set => "set",
            Self::ZSet => "zset",
            Self::Stream => "stream",
            Self::Other => "unknown",
        }
    }

    /// Single-character badge shown in the key tree.
    pub fn badge(self) -> &'static str {
        match self {
            Self::String => "S",
            Self::Hash => "H",
            Self::List => "L",
            Self::Set => "E",
            Self::ZSet => "Z",
            Self::Stream => "X",
            Self::Other => "?",
        }
    }
}

#[derive(Clone, Debug)]
pub struct KeyInfo {
    pub name: String,
    pub kind: KeyType,
    /// -1 = no expiry, -2 = key missing.
    pub ttl: i64,
}

/// One row of a collection-typed value. `id` is whatever the mutators need to
/// address this row (hash field, list index, set/zset member, stream id).
#[derive(Clone, Debug)]
pub struct Row {
    pub id: String,
    pub cells: Vec<String>,
}

#[derive(Clone, Debug)]
pub enum KeyValue {
    Str(String),
    Rows {
        headers: Vec<&'static str>,
        rows: Vec<Row>,
        total: u64,
    },
    Unsupported(String),
}

#[derive(Clone)]
pub struct Client {
    pub conn: Connection,
    mgr: MultiplexedConnection,
}

fn connection_info(conn: &Connection) -> Result<ConnectionInfo> {
    let mut info = (conn.host.as_str(), conn.port).into_connection_info()?;
    if conn.tls {
        info = info.set_addr(ConnectionAddr::TcpTls {
            host: conn.host.clone(),
            port: conn.port,
            insecure: false,
            tls_params: None,
        });
    }
    let mut settings = RedisConnectionInfo::default().set_db(conn.db);
    if !conn.username.is_empty() {
        settings = settings.set_username(&conn.username);
    }
    let password = conn.resolved_password();
    if !password.is_empty() {
        settings = settings.set_password(&password);
    }
    Ok(info.set_redis_settings(settings))
}

impl Client {
    pub async fn connect(conn: Connection) -> Result<Self> {
        let client = redis::Client::open(connection_info(&conn)?)?;
        let mut mgr = client
            .get_multiplexed_async_connection_with_config(
                &redis::AsyncConnectionConfig::new()
                    .set_connection_timeout(Some(std::time::Duration::from_secs(5)))
                    .set_response_timeout(Some(std::time::Duration::from_secs(10))),
            )
            .await?;
        redis::cmd("PING").query_async::<()>(&mut mgr).await?;
        Ok(Self { conn, mgr })
    }

    pub async fn dbsize(&self) -> Result<u64> {
        let mut c = self.mgr.clone();
        Ok(redis::cmd("DBSIZE").query_async(&mut c).await?)
    }

    pub async fn server_line(&self) -> Result<String> {
        let mut c = self.mgr.clone();
        let info: String = redis::cmd("INFO").arg("server").query_async(&mut c).await?;
        let field = |k: &str| {
            info.lines()
                .find_map(|l| l.strip_prefix(k).map(|v| v.trim().to_string()))
                .unwrap_or_else(|| "?".into())
        };
        Ok(format!(
            "redis {} · {}",
            field("redis_version:"),
            field("redis_mode:")
        ))
    }

    /// Cursor-based keyspace listing. Never issues `KEYS *`.
    /// Returns the keys plus whether the limit truncated the result.
    pub async fn scan_keys(&self, pattern: &str, limit: usize) -> Result<(Vec<KeyInfo>, bool)> {
        let mut c = self.mgr.clone();
        let mut cursor: u64 = 0;
        let mut names: Vec<String> = Vec::new();
        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(500)
                .query_async(&mut c)
                .await?;
            names.extend(batch);
            cursor = next;
            if cursor == 0 || names.len() >= limit {
                break;
            }
        }
        let truncated = names.len() > limit;
        names.truncate(limit);
        if names.is_empty() {
            return Ok((Vec::new(), false));
        }

        let mut type_pipe = redis::pipe();
        for n in &names {
            type_pipe.cmd("TYPE").arg(n);
        }
        let types: Vec<String> = type_pipe.query_async(&mut c).await?;

        let mut ttl_pipe = redis::pipe();
        for n in &names {
            ttl_pipe.cmd("TTL").arg(n);
        }
        let ttls: Vec<i64> = ttl_pipe.query_async(&mut c).await?;

        let keys = names
            .into_iter()
            .zip(types)
            .zip(ttls)
            .map(|((name, t), ttl)| KeyInfo {
                name,
                kind: KeyType::parse(&t),
                ttl,
            })
            .collect();
        Ok((keys, truncated))
    }

    pub async fn key_info(&self, name: &str) -> Result<KeyInfo> {
        let mut c = self.mgr.clone();
        let t: String = redis::cmd("TYPE").arg(name).query_async(&mut c).await?;
        let ttl: i64 = redis::cmd("TTL").arg(name).query_async(&mut c).await?;
        Ok(KeyInfo {
            name: name.to_string(),
            kind: KeyType::parse(&t),
            ttl,
        })
    }

    /// Read a bounded window of a key's value. Collection types report their
    /// true total so the UI can say "showing 1000 of 4.2M".
    pub async fn read_value(&self, name: &str, kind: KeyType) -> Result<KeyValue> {
        let mut c = self.mgr.clone();
        let lim = VALUE_LIMIT;
        Ok(match kind {
            KeyType::String => {
                let v: Option<String> = c.get(name).await?;
                KeyValue::Str(v.unwrap_or_default())
            }
            KeyType::Hash => {
                let total: u64 = c.hlen(name).await?;
                let mut rows = Vec::new();
                let mut cursor: u64 = 0;
                loop {
                    let (next, flat): (u64, Vec<String>) = redis::cmd("HSCAN")
                        .arg(name)
                        .arg(cursor)
                        .arg("COUNT")
                        .arg(200)
                        .query_async(&mut c)
                        .await?;
                    for pair in flat.chunks(2) {
                        if let [f, v] = pair {
                            rows.push(Row {
                                id: f.clone(),
                                cells: vec![f.clone(), v.clone()],
                            });
                        }
                    }
                    cursor = next;
                    if cursor == 0 || rows.len() >= lim {
                        break;
                    }
                }
                rows.truncate(lim);
                rows.sort_by(|a, b| a.id.cmp(&b.id));
                KeyValue::Rows {
                    headers: vec!["field", "value"],
                    rows,
                    total,
                }
            }
            KeyType::List => {
                let total: u64 = c.llen(name).await?;
                let items: Vec<String> = c.lrange(name, 0, lim as isize - 1).await?;
                let rows = items
                    .into_iter()
                    .enumerate()
                    .map(|(i, v)| Row {
                        id: i.to_string(),
                        cells: vec![i.to_string(), v],
                    })
                    .collect();
                KeyValue::Rows {
                    headers: vec!["index", "value"],
                    rows,
                    total,
                }
            }
            KeyType::Set => {
                let total: u64 = c.scard(name).await?;
                let mut rows = Vec::new();
                let mut cursor: u64 = 0;
                loop {
                    let (next, batch): (u64, Vec<String>) = redis::cmd("SSCAN")
                        .arg(name)
                        .arg(cursor)
                        .arg("COUNT")
                        .arg(200)
                        .query_async(&mut c)
                        .await?;
                    for m in batch {
                        rows.push(Row {
                            id: m.clone(),
                            cells: vec![m],
                        });
                    }
                    cursor = next;
                    if cursor == 0 || rows.len() >= lim {
                        break;
                    }
                }
                rows.truncate(lim);
                rows.sort_by(|a, b| a.id.cmp(&b.id));
                KeyValue::Rows {
                    headers: vec!["member"],
                    rows,
                    total,
                }
            }
            KeyType::ZSet => {
                let total: u64 = c.zcard(name).await?;
                let items: Vec<(String, f64)> =
                    c.zrange_withscores(name, 0, lim as isize - 1).await?;
                let rows = items
                    .into_iter()
                    .map(|(m, s)| Row {
                        id: m.clone(),
                        cells: vec![m, format_score(s)],
                    })
                    .collect();
                KeyValue::Rows {
                    headers: vec!["member", "score"],
                    rows,
                    total,
                }
            }
            KeyType::Stream => {
                let total: u64 = c.xlen(name).await?;
                let raw: Vec<(String, Vec<String>)> = redis::cmd("XREVRANGE")
                    .arg(name)
                    .arg("+")
                    .arg("-")
                    .arg("COUNT")
                    .arg(lim)
                    .query_async(&mut c)
                    .await?;
                let rows = raw
                    .into_iter()
                    .map(|(id, flat)| {
                        let fields = flat
                            .chunks(2)
                            .map(|p| match p {
                                [f, v] => format!("{f}={v}"),
                                [f] => f.clone(),
                                _ => String::new(),
                            })
                            .collect::<Vec<_>>()
                            .join("  ");
                        Row {
                            id: id.clone(),
                            cells: vec![id, fields],
                        }
                    })
                    .collect();
                KeyValue::Rows {
                    headers: vec!["id", "fields"],
                    rows,
                    total,
                }
            }
            KeyType::Other => KeyValue::Unsupported(
                "This key's type has no viewer yet. Use the command console (:) to inspect it."
                    .into(),
            ),
        })
    }

    // ---- key-level mutations -------------------------------------------

    pub async fn delete_key(&self, name: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: i64 = c.del(name).await?;
        Ok(())
    }

    pub async fn rename_key(&self, old: &str, new: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        redis::cmd("RENAME")
            .arg(old)
            .arg(new)
            .query_async::<()>(&mut c)
            .await?;
        Ok(())
    }

    /// `None` removes the expiry (PERSIST).
    pub async fn set_ttl(&self, name: &str, seconds: Option<i64>) -> Result<()> {
        let mut c = self.mgr.clone();
        match seconds {
            Some(s) if s >= 0 => {
                let _: bool = c.expire(name, s).await?;
            }
            _ => {
                let _: bool = c.persist(name).await?;
            }
        }
        Ok(())
    }

    pub async fn create_key(&self, name: &str, kind: KeyType) -> Result<()> {
        let mut c = self.mgr.clone();
        match kind {
            KeyType::String => {
                let _: () = c.set(name, "").await?;
            }
            KeyType::Hash => {
                let _: i64 = c.hset(name, "field", "value").await?;
            }
            KeyType::List => {
                let _: i64 = c.rpush(name, "item").await?;
            }
            KeyType::Set => {
                let _: i64 = c.sadd(name, "member").await?;
            }
            KeyType::ZSet => {
                let _: i64 = c.zadd(name, "member", 0.0).await?;
            }
            KeyType::Stream => {
                let _: String = redis::cmd("XADD")
                    .arg(name)
                    .arg("*")
                    .arg("field")
                    .arg("value")
                    .query_async(&mut c)
                    .await?;
            }
            KeyType::Other => return Err(anyhow!("unsupported key type")),
        }
        Ok(())
    }

    pub async fn set_string(&self, name: &str, value: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: () = c.set(name, value).await?;
        Ok(())
    }

    // ---- element-level mutations ----------------------------------------

    pub async fn hash_set(&self, name: &str, field: &str, value: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: i64 = c.hset(name, field, value).await?;
        Ok(())
    }

    pub async fn hash_del(&self, name: &str, field: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: i64 = c.hdel(name, field).await?;
        Ok(())
    }

    pub async fn list_push(&self, name: &str, value: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: i64 = c.rpush(name, value).await?;
        Ok(())
    }

    pub async fn list_set(&self, name: &str, index: isize, value: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: () = c.lset(name, index, value).await?;
        Ok(())
    }

    /// Redis has no delete-by-index. Overwrite the slot with a unique sentinel,
    /// then LREM it — the standard swap-and-trim, made safe by a sentinel that
    /// cannot collide with real data.
    pub async fn list_remove_at(&self, name: &str, index: isize) -> Result<()> {
        let mut c = self.mgr.clone();
        let sentinel = sentinel();
        let _: () = c.lset(name, index, &sentinel).await?;
        let _: i64 = c.lrem(name, 1, &sentinel).await?;
        Ok(())
    }

    pub async fn set_add(&self, name: &str, member: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: i64 = c.sadd(name, member).await?;
        Ok(())
    }

    pub async fn set_remove(&self, name: &str, member: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: i64 = c.srem(name, member).await?;
        Ok(())
    }

    pub async fn zset_add(&self, name: &str, member: &str, score: f64) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: i64 = c.zadd(name, member, score).await?;
        Ok(())
    }

    pub async fn zset_remove(&self, name: &str, member: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: i64 = c.zrem(name, member).await?;
        Ok(())
    }

    pub async fn stream_add(&self, name: &str, field: &str, value: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: String = redis::cmd("XADD")
            .arg(name)
            .arg("*")
            .arg(field)
            .arg(value)
            .query_async(&mut c)
            .await?;
        Ok(())
    }

    pub async fn stream_delete(&self, name: &str, id: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: i64 = redis::cmd("XDEL")
            .arg(name)
            .arg(id)
            .query_async(&mut c)
            .await?;
        Ok(())
    }

    // ---- raw console -----------------------------------------------------

    pub async fn execute_raw(&self, line: &str) -> Result<String> {
        let parts = split_args(line)?;
        let Some((head, tail)) = parts.split_first() else {
            return Ok(String::new());
        };
        let mut c = self.mgr.clone();
        let mut cmd = redis::cmd(head);
        for a in tail {
            cmd.arg(a);
        }
        let value: redis::Value = cmd.query_async(&mut c).await?;
        Ok(format_value(&value, 0))
    }
}

fn sentinel() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("__rediscope_tombstone_{nanos:x}_{n:x}__")
}

fn format_score(s: f64) -> String {
    if s.fract() == 0.0 && s.abs() < 1e15 {
        format!("{}", s as i64)
    } else {
        format!("{s}")
    }
}

/// Render a redis reply for the console, one line per element.
pub fn format_value(v: &redis::Value, depth: usize) -> String {
    let pad = "  ".repeat(depth);
    match v {
        redis::Value::Nil => format!("{pad}(nil)"),
        redis::Value::Int(i) => format!("{pad}(integer) {i}"),
        redis::Value::Double(d) => format!("{pad}(double) {d}"),
        redis::Value::Boolean(b) => format!("{pad}({b})"),
        redis::Value::SimpleString(s) => format!("{pad}{s}"),
        redis::Value::Okay => format!("{pad}OK"),
        redis::Value::BulkString(b) => format!("{pad}{}", String::from_utf8_lossy(b)),
        redis::Value::Array(items) | redis::Value::Set(items) => {
            if items.is_empty() {
                return format!("{pad}(empty)");
            }
            items
                .iter()
                .enumerate()
                .map(|(i, item)| {
                    let rendered = format_value(item, depth + 1);
                    let trimmed = rendered.trim_start();
                    if rendered.contains('\n') {
                        format!("{pad}{}) \n{rendered}", i + 1)
                    } else {
                        format!("{pad}{}) {trimmed}", i + 1)
                    }
                })
                .collect::<Vec<_>>()
                .join("\n")
        }
        redis::Value::Map(pairs) => pairs
            .iter()
            .map(|(k, val)| {
                format!(
                    "{pad}{} = {}",
                    format_value(k, 0).trim(),
                    format_value(val, 0).trim()
                )
            })
            .collect::<Vec<_>>()
            .join("\n"),
        redis::Value::ServerError(e) => format!("{pad}(error) {e:?}"),
        other => format!("{pad}{other:?}"),
    }
}

/// Split a console line into arguments, honouring single and double quotes.
pub fn split_args(line: &str) -> Result<Vec<String>> {
    let mut args = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    let mut started = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        match quote {
            Some(q) => {
                if ch == '\\' && q == '"' {
                    if let Some(next) = chars.next() {
                        cur.push(next);
                    }
                } else if ch == q {
                    quote = None;
                } else {
                    cur.push(ch);
                }
            }
            None if ch == '"' || ch == '\'' => {
                quote = Some(ch);
                started = true;
            }
            None if ch.is_whitespace() => {
                if started {
                    args.push(std::mem::take(&mut cur));
                    started = false;
                }
            }
            None => {
                cur.push(ch);
                started = true;
            }
        }
    }
    if quote.is_some() {
        return Err(anyhow!("unbalanced quote"));
    }
    if started {
        args.push(cur);
    }
    Ok(args)
}

/// Commands that wipe data outright. The console asks before running these.
pub fn is_destructive(line: &str) -> bool {
    let head = line.split_whitespace().next().unwrap_or("");
    matches!(
        head.to_ascii_uppercase().as_str(),
        "FLUSHALL" | "FLUSHDB" | "SHUTDOWN" | "DEBUG" | "SCRIPT" | "RESET" | "SWAPDB"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_quoted_arguments() {
        let a = split_args(r#"SET  "hello world" 'it''s'  plain"#).unwrap();
        assert_eq!(a, vec!["SET", "hello world", "its", "plain"]);
    }

    #[test]
    fn preserves_empty_quoted_argument() {
        assert!(split_args(r#"SET k ""#).is_err());
        assert_eq!(split_args(r#"SET k """#).unwrap(), vec!["SET", "k", ""]);
    }

    #[test]
    fn flags_destructive_commands() {
        assert!(is_destructive(" flushall "));
        assert!(is_destructive("FLUSHDB ASYNC"));
        assert!(!is_destructive("GET foo"));
    }
}
