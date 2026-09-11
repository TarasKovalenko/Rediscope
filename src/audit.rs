//! Redacted JSONL for a local log collector. Never stores arguments, keys, values,
//! credentials, scripts, or server error text. Local files are not tamper-proof.
use crate::config::Connection;
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

static IDS: AtomicU64 = AtomicU64::new(1);
#[derive(Clone)]
pub(crate) struct Audit {
    file: Arc<Mutex<File>>,
    session: String,
    profile: String,
    db: i64,
}
#[derive(Serialize)]
struct Event<'a> {
    schema: u8,
    timestamp_ms: u128,
    session: &'a str,
    operation_id: u64,
    profile: &'a str,
    db: i64,
    action: &'a str,
    outcome: &'a str,
    /// Target count, not a claim that all targets changed. Unknown for arbitrary scripts.
    target_key_count: Option<usize>,
}
fn now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
impl Audit {
    pub fn open(p: &Connection) -> Result<Self> {
        let path = std::env::var_os("REDISCOPE_AUDIT_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(|| crate::config::config_file().with_file_name("audit.jsonl"));
        Self::at(p, path)
    }
    fn at(p: &Connection, path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Cannot create the audit directory {}", parent.display())
            })?;
        }
        if let Ok(meta) = std::fs::symlink_metadata(&path) {
            ensure!(
                meta.is_file() && !meta.file_type().is_symlink(),
                "Audit path must be a regular file"
            );
        }
        let mut opts = OpenOptions::new();
        opts.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let file = opts
            .open(&path)
            .with_context(|| format!("Cannot open the audit log at {}", path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
            session: format!(
                "{}-{}-{}",
                std::process::id(),
                now(),
                IDS.fetch_add(1, Ordering::Relaxed)
            ),
            profile: p.name.clone(),
            db: p.db,
        })
    }
    pub fn id(&self) -> u64 {
        IDS.fetch_add(1, Ordering::Relaxed)
    }
    pub fn record(&self, id: u64, action: &str, outcome: &str, count: Option<usize>) -> Result<()> {
        let event = Event {
            schema: 1,
            timestamp_ms: now(),
            session: &self.session,
            operation_id: id,
            profile: &self.profile,
            db: self.db,
            action,
            outcome,
            target_key_count: count,
        };
        let mut bytes = serde_json::to_vec(&event)?;
        bytes.push(b'\n');
        let mut file = self.file.lock().unwrap();
        file.write_all(&bytes)
            .context("Cannot append audit event")?;
        file.sync_data().context("Cannot sync audit event")
    }
}

/// Only fixed command labels are recorded; user input can never become a log field.
pub(crate) fn command(cmd: &redis::Cmd) -> (&'static str, Option<usize>) {
    let args: Vec<&[u8]> = cmd
        .args_iter()
        .filter_map(|a| {
            if let redis::Arg::Simple(v) = a {
                Some(v)
            } else {
                None
            }
        })
        .collect();
    let head = args
        .first()
        .map(|v| v.to_ascii_uppercase())
        .unwrap_or_default();
    match head.as_slice() {
        b"SET" => ("SET", Some(1)),
        b"HSET" => ("HSET", Some(1)),
        b"HDEL" => ("HDEL", Some(1)),
        b"HEXPIRE" => ("HEXPIRE", Some(1)),
        b"HPERSIST" => ("HPERSIST", Some(1)),
        b"LSET" => ("LSET", Some(1)),
        b"LREM" => ("LREM", Some(1)),
        b"RPUSH" => ("RPUSH", Some(1)),
        b"SADD" => ("SADD", Some(1)),
        b"SREM" => ("SREM", Some(1)),
        b"ZADD" => ("ZADD", Some(1)),
        b"ZREM" => ("ZREM", Some(1)),
        b"DEL" => ("DEL", Some(args.len().saturating_sub(1))),
        b"UNLINK" => ("UNLINK", Some(args.len().saturating_sub(1))),
        b"EXPIRE" => ("EXPIRE", Some(1)),
        b"PERSIST" => ("PERSIST", Some(1)),
        b"RENAME" | b"RENAMENX" => ("RENAME", Some(2)),
        b"RESTORE" => ("RESTORE", Some(1)),
        b"DUMP" => ("EXPORT_READ", Some(1)),
        b"EVAL" | b"EVALSHA" => (
            "SCRIPT",
            args.get(2)
                .and_then(|v| std::str::from_utf8(v).ok())
                .and_then(|v| v.parse().ok()),
        ),
        b"CONFIG" => ("CONFIG", Some(0)),
        b"CLIENT" => ("CLIENT", Some(0)),
        b"SLOWLOG" => ("SLOWLOG", Some(0)),
        b"FLUSHDB" => ("FLUSHDB", None),
        b"FLUSHALL" => ("FLUSHALL", None),
        b"SHUTDOWN" => ("SHUTDOWN", None),
        b"JSON.SET" => ("JSON.SET", Some(1)),
        b"TS.ADD" => ("TS.ADD", Some(1)),
        b"TS.DEL" => ("TS.DEL", Some(1)),
        b"VADD" => ("VADD", Some(1)),
        b"VREM" => ("VREM", Some(1)),
        b"VSETATTR" => ("VSETATTR", Some(1)),
        b"XADD" => ("XADD", Some(1)),
        b"XDEL" => ("XDEL", Some(1)),
        b"XGROUP" => ("XGROUP", Some(1)),
        b"XACK" => ("XACK", Some(1)),
        b"XCLAIM" => ("XCLAIM", Some(1)),
        b"PUBLISH" => ("PUBLISH", Some(0)),
        _ => ("OTHER_COMMAND", None),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn redacts_arguments_and_unknown_command_names() {
        let dir =
            std::env::temp_dir().join(format!("rediscope-audit-{}-{}", std::process::id(), now()));
        let path = dir.join("audit.jsonl");
        let a = Audit::at(&Connection::default(), path.clone()).unwrap();
        let mut cmd = redis::cmd("SET");
        cmd.arg("secret-key").arg("secret-value");
        let (name, count) = command(&cmd);
        a.record(a.id(), name, "success", count).unwrap();
        let data = std::fs::read_to_string(&path).unwrap();
        assert!(!data.contains("secret"));
        assert_eq!(command(&redis::cmd("a-secret-token")).0, "OTHER_COMMAND");
        let value: serde_json::Value = serde_json::from_str(&data).unwrap();
        assert_eq!(value["target_key_count"], 1);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}
