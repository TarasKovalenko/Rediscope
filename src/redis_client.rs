//! Async Redis access tailored to the TUI: SCAN-based listing, bounded value
//! reads, and typed mutators. Nothing here blocks the render loop.

use anyhow::{Context, Result, anyhow};
use redis::aio::MultiplexedConnection;
use redis::{
    AsyncCommands, ClientTlsConfig, ConnectionAddr, ConnectionInfo, IntoConnectionInfo,
    RedisConnectionInfo, TlsCertificates,
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
    /// RedisJSON document (`ReJSON-RL`).
    Json,
    /// RedisTimeSeries key (`TSDB-TYPE`).
    TimeSeries,
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
            "ReJSON-RL" => Self::Json,
            "TSDB-TYPE" => Self::TimeSeries,
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
            Self::Json => "json",
            Self::TimeSeries => "timeseries",
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
            Self::Json => "J",
            Self::TimeSeries => "T",
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
    /// Kept so a feature needing a connection of its own — pub/sub, which
    /// cannot share the multiplexed one — can open it.
    raw: redis::Client,
    /// The `ssh -L` process this connection rides on, dropped (and killed)
    /// with the last clone of the client.
    _tunnel: Option<std::sync::Arc<Tunnel>>,
}

/// A local port forwarded to the Redis server by an `ssh -L` child process.
/// Killed when the last client holding it goes away.
#[derive(Debug)]
pub struct Tunnel {
    child: std::process::Child,
    pub local_port: u16,
}

impl Drop for Tunnel {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Ask the OS for a free loopback port. Racy in principle, but the window
/// between the probe and ssh binding it is small and the failure is loud.
fn free_local_port() -> Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .context("cannot reserve a local port for the SSH tunnel")?;
    Ok(listener.local_addr()?.port())
}

/// Start `ssh -N -L <local>:<host>:<port> <user@jump>` and wait for the
/// forward to accept connections. Uses the system ssh, so the user's agent,
/// config and known_hosts all apply.
fn open_tunnel(conn: &Connection) -> Result<Tunnel> {
    let local_port = free_local_port()?;
    let mut cmd = std::process::Command::new("ssh");
    cmd.arg("-N")
        .arg("-T")
        // Fail loudly instead of leaving a tunnel that forwards nowhere.
        .arg("-o")
        .arg("ExitOnForwardFailure=yes")
        // Never sit waiting for a password prompt behind the alternate screen.
        .arg("-o")
        .arg("BatchMode=yes")
        .arg("-o")
        .arg("ServerAliveInterval=30")
        .arg("-p")
        .arg(conn.ssh_port.to_string())
        .arg("-L")
        .arg(format!(
            "127.0.0.1:{local_port}:{}:{}",
            conn.host, conn.port
        ));
    if !conn.ssh_key_file.trim().is_empty() {
        cmd.arg("-i")
            .arg(crate::config::expand_home(&conn.ssh_key_file));
    }
    let target = if conn.ssh_user.trim().is_empty() {
        conn.ssh_host.trim().to_string()
    } else {
        format!("{}@{}", conn.ssh_user.trim(), conn.ssh_host.trim())
    };
    cmd.arg(target);
    cmd.stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped());
    let child = cmd
        .spawn()
        .context("cannot run ssh — is the OpenSSH client installed and on PATH?")?;
    let mut tunnel = Tunnel { child, local_port };

    // Wait for ssh to bind the forward. Ten seconds covers a slow handshake
    // without hanging the UI on a jump host that will never answer.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], local_port));
    loop {
        if let Ok(Some(status)) = tunnel.child.try_wait() {
            anyhow::bail!("ssh exited before the tunnel was up ({status})");
        }
        if std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(200))
            .is_ok()
        {
            return Ok(tunnel);
        }
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for the SSH tunnel to {}", conn.ssh_host);
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

fn connection_info(conn: &Connection, password: &str, via: Option<u16>) -> Result<ConnectionInfo> {
    let (host, port) = match via {
        // Through a tunnel the socket is local, but TLS still has to be
        // validated against the real server name.
        Some(local) => ("127.0.0.1", local),
        None => (conn.host.as_str(), conn.port),
    };
    let mut info = (host, port).into_connection_info()?;
    if conn.tls {
        info = info.set_addr(ConnectionAddr::TcpTls {
            host: conn.host.clone(),
            port,
            insecure: conn.tls_insecure,
            tls_params: None,
        });
    }
    let mut settings = RedisConnectionInfo::default().set_db(conn.db);
    if !conn.username.is_empty() {
        settings = settings.set_username(&conn.username);
    }
    if !password.is_empty() {
        settings = settings.set_password(password);
    }
    Ok(info.set_redis_settings(settings))
}

impl std::fmt::Debug for Client {
    /// The live connection has no useful representation; the profile does.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("conn", &self.conn)
            .finish_non_exhaustive()
    }
}

/// Read the PEM files a profile points at. Returns `None` when the profile
/// relies on the system trust store and does not use client certificates.
fn load_tls_certificates(conn: &Connection) -> Result<Option<TlsCertificates>> {
    if !conn.tls {
        return Ok(None);
    }
    let root_cert = read_pem(&conn.tls_ca_file, "CA certificate")?;
    let cert = read_pem(&conn.tls_cert_file, "client certificate")?;
    let key = read_pem(&conn.tls_key_file, "client key")?;
    let client_tls = match (cert, key) {
        (Some(client_cert), Some(client_key)) => Some(ClientTlsConfig {
            client_cert,
            client_key,
        }),
        (None, None) => None,
        _ => anyhow::bail!("mutual TLS needs both a client certificate and a client key"),
    };
    if client_tls.is_none() && root_cert.is_none() {
        return Ok(None);
    }
    Ok(Some(TlsCertificates {
        client_tls,
        root_cert,
    }))
}

fn read_pem(path: &str, what: &str) -> Result<Option<Vec<u8>>> {
    if path.trim().is_empty() {
        return Ok(None);
    }
    let resolved = crate::config::expand_home(path);
    let bytes = std::fs::read(&resolved)
        .with_context(|| format!("cannot read the {what} at {}", resolved.display()))?;
    Ok(Some(bytes))
}

/// rustls refuses to pick a cipher-suite provider on its own when more than one
/// is compiled in, and panics at handshake time if none was installed. Do it
/// once, before the first TLS connection.
fn ensure_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // An error here means a provider is already installed, which is fine.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Resolve the password and certificates (both hit the filesystem or the OS
/// keychain) off the async runtime, then build the redis client.
async fn build_client(conn: &Connection, via: Option<u16>) -> Result<redis::Client> {
    if conn.tls {
        ensure_crypto_provider();
    }
    let probe = conn.clone();
    let (password, certs) =
        tokio::task::spawn_blocking(move || -> Result<(String, Option<TlsCertificates>)> {
            Ok((probe.resolve_password()?, load_tls_certificates(&probe)?))
        })
        .await??;
    let info = connection_info(conn, &password, via)?;
    Ok(match certs {
        Some(certs) => redis::Client::build_with_tls(info, certs)?,
        None => redis::Client::open(info)?,
    })
}

/// A parsed `INFO` reply: sections in server order, each holding its
/// `field: value` lines, plus the raw text for the "all" view.
#[derive(Clone, Debug, Default)]
pub struct ServerInfo {
    pub sections: Vec<InfoSection>,
    pub raw: String,
}

#[derive(Clone, Debug, Default)]
pub struct InfoSection {
    pub name: String,
    pub fields: Vec<(String, String)>,
}

impl ServerInfo {
    pub fn parse(raw: &str) -> Self {
        let mut sections: Vec<InfoSection> = Vec::new();
        for line in raw.lines() {
            let line = line.trim_end_matches(['\r', '\n']);
            if line.is_empty() {
                continue;
            }
            if let Some(name) = line.strip_prefix('#') {
                sections.push(InfoSection {
                    name: name.trim().to_string(),
                    fields: Vec::new(),
                });
                continue;
            }
            let Some((k, v)) = line.split_once(':') else {
                continue;
            };
            // A reply without a leading header still gets a home.
            if sections.is_empty() {
                sections.push(InfoSection {
                    name: "Server".into(),
                    fields: Vec::new(),
                });
            }
            if let Some(last) = sections.last_mut() {
                last.fields
                    .push((k.trim().to_string(), v.trim().to_string()));
            }
        }
        Self {
            sections,
            raw: raw.to_string(),
        }
    }

    /// Fields of a section, matched case-insensitively. Empty when absent.
    pub fn section(&self, name: &str) -> &[(String, String)] {
        self.sections
            .iter()
            .find(|s| s.name.eq_ignore_ascii_case(name))
            .map_or(&[][..], |s| &s.fields)
    }

    /// First value for a field name, searched across every section.
    pub fn field(&self, key: &str) -> Option<&str> {
        self.sections
            .iter()
            .flat_map(|s| &s.fields)
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// `dbN` lines from the Keyspace section, as (db, keys, expires).
    pub fn keyspace(&self) -> Vec<(String, u64, u64)> {
        self.section("Keyspace")
            .iter()
            .map(|(db, stats)| {
                let get = |name: &str| -> u64 {
                    stats
                        .split(',')
                        .find_map(|p| p.trim().strip_prefix(name)?.parse().ok())
                        .unwrap_or(0)
                };
                (db.clone(), get("keys="), get("expires="))
            })
            .collect()
    }
}

/// One exported key: enough to write it back anywhere, including its TTL.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExportEntry {
    pub key: String,
    /// The Redis type name, for a human reading the file.
    #[serde(default)]
    pub kind: String,
    /// Remaining life in milliseconds; negative means no expiry.
    #[serde(default)]
    pub pttl: i64,
    /// The `DUMP` payload, hex encoded so the file stays text.
    pub dump: String,
}

#[derive(Clone, Debug)]
pub struct StreamGroup {
    pub name: String,
    pub consumers: u64,
    pub pending: u64,
    pub last_delivered: String,
    pub lag: String,
}

#[derive(Clone, Debug)]
pub struct StreamConsumer {
    pub name: String,
    pub pending: u64,
    pub idle_ms: i64,
}

#[derive(Clone, Debug)]
pub struct PendingEntry {
    pub id: String,
    pub consumer: String,
    pub idle_ms: i64,
    pub deliveries: i64,
}

#[derive(Clone, Debug, Default)]
pub struct StreamGroupDetail {
    pub consumers: Vec<StreamConsumer>,
    pub pending: Vec<PendingEntry>,
}

/// One `SLOWLOG` entry.
#[derive(Clone, Debug)]
pub struct SlowEntry {
    pub id: i64,
    pub at: i64,
    pub micros: i64,
    pub command: String,
    pub client: String,
}

/// One row of `CLIENT LIST`, reduced to the fields worth a column.
#[derive(Clone, Debug, Default)]
pub struct ClientEntry {
    pub id: String,
    pub addr: String,
    pub name: String,
    pub age_secs: i64,
    pub idle_secs: i64,
    pub db: String,
    pub command: String,
}

/// Everything the diagnostics tabs need, read in one go so opening the pane is
/// a single round of requests rather than a request per tab.
#[derive(Clone, Debug, Default)]
pub struct Diagnostics {
    pub slowlog: Vec<SlowEntry>,
    pub clients: Vec<ClientEntry>,
    pub config: Vec<(String, String)>,
    pub latency: Vec<(String, String)>,
    pub cluster: Vec<(String, String)>,
    pub modules: Vec<String>,
}

/// The server's command list: every name, plus the ones it flags as writes.
#[derive(Clone, Debug, Default)]
pub struct CommandTable {
    pub names: Vec<String>,
    pub writes: std::collections::HashSet<String>,
}

impl CommandTable {
    /// Whether a console line would write. Unknown commands are treated as
    /// writes: on a read-only profile, refusing too much beats letting a write
    /// through because the server did not list the command.
    pub fn is_write(&self, line: &str) -> bool {
        let head = line.split_whitespace().next().unwrap_or("").to_uppercase();
        if head.is_empty() {
            return false;
        }
        if is_destructive(line) {
            return true;
        }
        if self.names.is_empty() {
            // No table (an old or restricted server): fall back to the names
            // that are unambiguously reads.
            return !READ_ONLY_COMMANDS.contains(&head.as_str());
        }
        self.writes.contains(&head) || !self.names.contains(&head)
    }
}

/// Enough of the read side to keep the console usable when the server will not
/// hand over its command table.
const READ_ONLY_COMMANDS: &[&str] = &[
    "GET",
    "MGET",
    "STRLEN",
    "GETRANGE",
    "EXISTS",
    "TYPE",
    "TTL",
    "PTTL",
    "KEYS",
    "SCAN",
    "HGET",
    "HGETALL",
    "HKEYS",
    "HVALS",
    "HLEN",
    "HMGET",
    "HSCAN",
    "HEXISTS",
    "HRANDFIELD",
    "LRANGE",
    "LLEN",
    "LINDEX",
    "LPOS",
    "SMEMBERS",
    "SCARD",
    "SISMEMBER",
    "SMISMEMBER",
    "SRANDMEMBER",
    "SSCAN",
    "SINTER",
    "SUNION",
    "SDIFF",
    "ZRANGE",
    "ZREVRANGE",
    "ZRANGEBYSCORE",
    "ZRANGEBYLEX",
    "ZSCORE",
    "ZCARD",
    "ZCOUNT",
    "ZRANK",
    "ZREVRANK",
    "ZSCAN",
    "ZRANDMEMBER",
    "XRANGE",
    "XREVRANGE",
    "XLEN",
    "XINFO",
    "XPENDING",
    "BITCOUNT",
    "BITPOS",
    "GETBIT",
    "PFCOUNT",
    "DBSIZE",
    "INFO",
    "PING",
    "ECHO",
    "TIME",
    "COMMAND",
    "CONFIG",
    "CLIENT",
    "MEMORY",
    "OBJECT",
    "LATENCY",
    "SLOWLOG",
    "ACL",
    "CLUSTER",
    "MODULE",
    "LASTSAVE",
    "RANDOMKEY",
    "DUMP",
    "SELECT",
    "HELLO",
    "AUTH",
    "JSON.GET",
    "JSON.TYPE",
    "JSON.OBJLEN",
    "TS.RANGE",
    "TS.INFO",
    "FT.SEARCH",
    "FT.INFO",
    "GEOPOS",
    "GEODIST",
    "GEOSEARCH",
    "SUBSCRIBE",
    "PSUBSCRIBE",
    "WAIT",
];

/// The result of a connection test, shown in the server list.
#[derive(Clone, Debug)]
pub struct Probe {
    pub latency_ms: f64,
    pub version: String,
    pub mode: String,
    pub dbsize: u64,
}

impl Client {
    /// Connect, time a round trip, read the server banner, then drop the
    /// connection. Used by "test connection" so it never disturbs the session.
    pub async fn probe(conn: Connection) -> Result<Probe> {
        let client = Self::connect(conn).await?;
        let mut c = client.mgr.clone();
        let start = std::time::Instant::now();
        redis::cmd("PING").query_async::<()>(&mut c).await?;
        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
        let info: String = redis::cmd("INFO").arg("server").query_async(&mut c).await?;
        Ok(Probe {
            latency_ms,
            version: info_field(&info, "redis_version:"),
            mode: info_field(&info, "redis_mode:"),
            dbsize: client.dbsize().await.unwrap_or(0),
        })
    }

    pub async fn connect(conn: Connection) -> Result<Self> {
        // Opening the tunnel shells out and blocks on a TCP probe; keep both
        // off the runtime's worker threads.
        let tunnel = if conn.uses_ssh() {
            let probe = conn.clone();
            Some(std::sync::Arc::new(
                tokio::task::spawn_blocking(move || open_tunnel(&probe)).await??,
            ))
        } else {
            None
        };
        let via = tunnel.as_ref().map(|t| t.local_port);
        let client = build_client(&conn, via).await?;
        let mut mgr = client
            .get_multiplexed_async_connection_with_config(
                &redis::AsyncConnectionConfig::new()
                    .set_connection_timeout(Some(std::time::Duration::from_secs(5)))
                    .set_response_timeout(Some(std::time::Duration::from_secs(10))),
            )
            .await?;
        redis::cmd("PING").query_async::<()>(&mut mgr).await?;
        Ok(Self {
            conn,
            mgr,
            raw: client,
            _tunnel: tunnel,
        })
    }

    /// True when the profile refuses writes. Checked before every mutation so
    /// a read-only server cannot be edited by any route, including the console.
    pub fn read_only(&self) -> bool {
        self.conn.read_only
    }

    /// A connection of its own, for pub/sub. The multiplexed connection cannot
    /// be put into subscriber mode without breaking every other caller.
    pub async fn pubsub(&self) -> Result<redis::aio::PubSub> {
        Ok(self.raw.get_async_pubsub().await?)
    }

    /// Command names for console completion, and the subset flagged `write`.
    /// `COMMAND` works on every server version, unlike `COMMAND LIST`, and one
    /// reply per connection is cheap. The write set is what a read-only
    /// profile refuses, so it comes from the server rather than a guess.
    pub async fn command_names(&self) -> Result<CommandTable> {
        let mut c = self.mgr.clone();
        let reply: redis::Value = redis::cmd("COMMAND").query_async(&mut c).await?;
        let redis::Value::Array(items) = reply else {
            return Ok(CommandTable::default());
        };
        let mut names: Vec<String> = Vec::with_capacity(items.len());
        let mut writes: std::collections::HashSet<String> = std::collections::HashSet::new();
        for item in &items {
            // Each entry is [name, arity, [flags], ...].
            let redis::Value::Array(fields) = item else {
                continue;
            };
            let Some(name) = fields.first().map(scalar) else {
                continue;
            };
            if name.is_empty() {
                continue;
            }
            let name = name.to_uppercase();
            if let Some(redis::Value::Array(flags) | redis::Value::Set(flags)) = fields.get(2)
                && flags
                    .iter()
                    .any(|f| scalar(f).eq_ignore_ascii_case("write"))
            {
                writes.insert(name.clone());
            }
            names.push(name);
        }
        names.sort_unstable();
        names.dedup();
        Ok(CommandTable { names, writes })
    }

    /// One `SCAN` batch for the namespace memory report: every key counts
    /// toward its prefix, every `stride`-th key is measured with
    /// `MEMORY USAGE`. Returns true when the keyspace has been walked.
    ///
    /// Measuring all of a large keyspace would take hours, so the stride is
    /// what keeps the report affordable; the sampled keys go out in one
    /// pipeline per batch rather than one round trip each.
    pub async fn memory_batch(
        &self,
        scan: &mut MemoryScan,
        stride: u64,
        rollup: &mut crate::memory::Rollup,
    ) -> Result<bool> {
        let mut c = self.mgr.clone();
        let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(scan.cursor)
            .arg("COUNT")
            .arg(500)
            .query_async(&mut c)
            .await?;
        scan.cursor = next;
        scan.started = true;

        let stride = stride.max(1);
        let mut sample: Vec<String> = Vec::new();
        for name in batch {
            if scan.seen.is_multiple_of(stride) {
                sample.push(name.clone());
            }
            scan.seen += 1;
            rollup.count(&name);
        }

        if !sample.is_empty() {
            let mut pipe = redis::pipe();
            for name in &sample {
                pipe.cmd("MEMORY").arg("USAGE").arg(name);
            }
            // A server with `MEMORY USAGE` disabled still gives a useful key
            // count, so a failed measurement is not a failed report.
            if let Ok(sizes) = pipe.query_async::<Vec<Option<u64>>>(&mut c).await {
                // `OBJECT FREQ` only answers under an LFU policy; asking for it
                // is best effort, and its absence just leaves the column empty.
                let mut freq_pipe = redis::pipe();
                for name in &sample {
                    freq_pipe.cmd("OBJECT").arg("FREQ").arg(name);
                }
                let freqs = freq_pipe
                    .query_async::<Vec<Option<u64>>>(&mut c)
                    .await
                    .unwrap_or_default();
                for (i, (name, size)) in sample.iter().zip(sizes).enumerate() {
                    if let Some(bytes) = size {
                        rollup.measure_with_freq(name, bytes, freqs.get(i).copied().flatten());
                    }
                }
            }
        }
        Ok(scan.cursor == 0)
    }

    pub async fn dbsize(&self) -> Result<u64> {
        let mut c = self.mgr.clone();
        Ok(redis::cmd("DBSIZE").query_async(&mut c).await?)
    }

    /// Full `INFO` for the server pane. Falls back to the default sections
    /// when a managed provider rejects `INFO all`.
    pub async fn info(&self) -> Result<ServerInfo> {
        let mut c = self.mgr.clone();
        let raw: String = match redis::cmd("INFO").arg("all").query_async(&mut c).await {
            Ok(raw) => raw,
            Err(_) => redis::cmd("INFO").query_async(&mut c).await?,
        };
        Ok(ServerInfo::parse(&raw))
    }

    pub async fn server_line(&self) -> Result<String> {
        let mut c = self.mgr.clone();
        let info: String = redis::cmd("INFO").arg("server").query_async(&mut c).await?;
        Ok(format!(
            "redis {} · {}",
            info_field(&info, "redis_version:"),
            info_field(&info, "redis_mode:")
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
            KeyType::Json => {
                // `JSON.GET key $` always answers with a one-element array;
                // the bare path gives the document as stored.
                let doc: Option<String> = redis::cmd("JSON.GET")
                    .arg(name)
                    .arg("$")
                    .query_async(&mut c)
                    .await?;
                let doc = doc.unwrap_or_else(|| "null".into());
                // Unwrap the `$` envelope so the pane shows the document.
                let text = match serde_json::from_str::<serde_json::Value>(&doc) {
                    Ok(serde_json::Value::Array(mut items)) if items.len() == 1 => {
                        serde_json::to_string(&items.remove(0)).unwrap_or(doc)
                    }
                    _ => doc,
                };
                KeyValue::Str(text)
            }
            KeyType::TimeSeries => {
                let raw: Vec<(u64, f64)> = redis::cmd("TS.RANGE")
                    .arg(name)
                    .arg("-")
                    .arg("+")
                    .arg("COUNT")
                    .arg(lim)
                    .query_async(&mut c)
                    .await?;
                let total = raw.len() as u64;
                let rows = raw
                    .into_iter()
                    .map(|(ts, v)| Row {
                        id: ts.to_string(),
                        cells: vec![ts.to_string(), format_score(v)],
                    })
                    .collect();
                KeyValue::Rows {
                    headers: vec!["timestamp", "value"],
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
            KeyType::Json => {
                redis::cmd("JSON.SET")
                    .arg(name)
                    .arg("$")
                    .arg("{}")
                    .query_async::<()>(&mut c)
                    .await?;
            }
            KeyType::TimeSeries => {
                redis::cmd("TS.CREATE")
                    .arg(name)
                    .query_async::<()>(&mut c)
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

    pub async fn json_set(&self, name: &str, path: &str, value: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        redis::cmd("JSON.SET")
            .arg(name)
            .arg(path)
            .arg(value)
            .query_async::<()>(&mut c)
            .await?;
        Ok(())
    }

    pub async fn ts_add(&self, name: &str, timestamp: &str, value: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let ts = if timestamp.trim().is_empty() {
            "*"
        } else {
            timestamp.trim()
        };
        redis::cmd("TS.ADD")
            .arg(name)
            .arg(ts)
            .arg(value.trim())
            .query_async::<i64>(&mut c)
            .await?;
        Ok(())
    }

    pub async fn ts_del(&self, name: &str, timestamp: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        // TS.DEL takes a range; one sample is the range [ts, ts].
        redis::cmd("TS.DEL")
            .arg(name)
            .arg(timestamp)
            .arg(timestamp)
            .query_async::<i64>(&mut c)
            .await?;
        Ok(())
    }

    // ---- diagnostics ------------------------------------------------------

    /// Slow log, client list, running config, latency events and cluster state.
    /// Every part is optional: a managed provider that blocks `CONFIG` or
    /// `CLIENT LIST` still gets the tabs it is allowed to see.
    pub async fn diagnostics(&self) -> Result<Diagnostics> {
        let mut c = self.mgr.clone();
        let slowlog = match redis::cmd("SLOWLOG")
            .arg("GET")
            .arg(128)
            .query_async::<redis::Value>(&mut c)
            .await
        {
            Ok(v) => parse_slowlog(&v),
            Err(_) => Vec::new(),
        };
        let clients = redis::cmd("CLIENT")
            .arg("LIST")
            .query_async::<String>(&mut c)
            .await
            .map(|raw| parse_client_list(&raw))
            .unwrap_or_default();
        let config: Vec<(String, String)> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("*")
            .query_async::<Vec<String>>(&mut c)
            .await
            .map(|flat| {
                let mut pairs: Vec<(String, String)> = flat
                    .chunks(2)
                    .filter_map(|p| match p {
                        [k, v] => Some((k.clone(), v.clone())),
                        _ => None,
                    })
                    .collect();
                pairs.sort_by(|a, b| a.0.cmp(&b.0));
                pairs
            })
            .unwrap_or_default();
        let latency = self.latency_rows(&mut c).await;
        let cluster = redis::cmd("CLUSTER")
            .arg("INFO")
            .query_async::<String>(&mut c)
            .await
            .map(|raw| {
                raw.lines()
                    .filter_map(|l| l.split_once(':'))
                    .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let modules = redis::cmd("MODULE")
            .arg("LIST")
            .query_async::<redis::Value>(&mut c)
            .await
            .map(|v| {
                as_maps(&v)
                    .into_iter()
                    .map(|m| {
                        format!(
                            "{} v{}",
                            m.get("name").cloned().unwrap_or_default(),
                            m.get("ver").cloned().unwrap_or_default()
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(Diagnostics {
            slowlog,
            clients,
            config,
            latency,
            cluster,
            modules,
        })
    }

    /// `LATENCY LATEST` plus a fresh ping sample, so the tab says something
    /// useful even on a server with latency monitoring switched off.
    async fn latency_rows(&self, c: &mut MultiplexedConnection) -> Vec<(String, String)> {
        let mut rows = Vec::new();
        let mut best = f64::MAX;
        let mut worst: f64 = 0.0;
        let mut total = 0.0;
        const SAMPLES: usize = 5;
        for _ in 0..SAMPLES {
            let start = std::time::Instant::now();
            if redis::cmd("PING").query_async::<()>(c).await.is_err() {
                break;
            }
            let ms = start.elapsed().as_secs_f64() * 1000.0;
            best = best.min(ms);
            worst = worst.max(ms);
            total += ms;
        }
        if best.is_finite() && worst > 0.0 {
            rows.push((
                "ping (5 samples)".into(),
                format!(
                    "min {best:.2} ms · avg {:.2} ms · max {worst:.2} ms",
                    total / SAMPLES as f64
                ),
            ));
        }
        if let Ok(events) = redis::cmd("LATENCY")
            .arg("LATEST")
            .query_async::<Vec<(String, i64, i64, i64)>>(c)
            .await
        {
            for (event, at, last_ms, max_ms) in events {
                rows.push((
                    event,
                    format!("last {last_ms} ms · worst {max_ms} ms · at unix {at}"),
                ));
            }
        }
        if rows.len() == 1 {
            rows.push((
                "latency events".into(),
                "none recorded — set latency-monitor-threshold to collect them".into(),
            ));
        }
        rows
    }

    /// Change one running config parameter. Not persisted to the config file;
    /// that is `CONFIG REWRITE`, which stays a console command on purpose.
    pub async fn config_set(&self, param: &str, value: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        redis::cmd("CONFIG")
            .arg("SET")
            .arg(param)
            .arg(value)
            .query_async::<()>(&mut c)
            .await?;
        Ok(())
    }

    /// Disconnect a client by id.
    pub async fn client_kill(&self, id: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        redis::cmd("CLIENT")
            .arg("KILL")
            .arg("ID")
            .arg(id)
            .query_async::<redis::Value>(&mut c)
            .await?;
        Ok(())
    }

    pub async fn slowlog_reset(&self) -> Result<()> {
        let mut c = self.mgr.clone();
        redis::cmd("SLOWLOG")
            .arg("RESET")
            .query_async::<()>(&mut c)
            .await?;
        Ok(())
    }

    // ---- bulk key operations ---------------------------------------------

    /// `UNLINK` every named key in one pipeline per chunk, so a thousand marked
    /// keys is a handful of round trips rather than a thousand.
    pub async fn delete_keys(&self, names: &[String]) -> Result<u64> {
        let mut c = self.mgr.clone();
        let mut removed = 0u64;
        for chunk in names.chunks(256) {
            let mut pipe = redis::pipe();
            for name in chunk {
                pipe.cmd("UNLINK").arg(name);
            }
            let counts: Vec<i64> = pipe.query_async(&mut c).await?;
            removed += counts.iter().map(|n| (*n).max(0) as u64).sum::<u64>();
        }
        Ok(removed)
    }

    /// Set (or with `None`, clear) the expiry on many keys at once.
    pub async fn expire_keys(&self, names: &[String], seconds: Option<i64>) -> Result<u64> {
        let mut c = self.mgr.clone();
        let mut changed = 0u64;
        for chunk in names.chunks(256) {
            let mut pipe = redis::pipe();
            for name in chunk {
                match seconds {
                    Some(s) if s >= 0 => pipe.cmd("EXPIRE").arg(name).arg(s),
                    _ => pipe.cmd("PERSIST").arg(name),
                };
            }
            let counts: Vec<i64> = pipe.query_async(&mut c).await?;
            changed += counts.iter().map(|n| (*n).max(0) as u64).sum::<u64>();
        }
        Ok(changed)
    }

    /// Copy one key's value, type and TTL somewhere else: another name, another
    /// database, or another server. `DUMP` + `RESTORE` carries every type
    /// faithfully, which a type-by-type copy would not.
    pub async fn copy_key(
        &self,
        source: &str,
        target_name: &str,
        target: &Client,
        replace: bool,
    ) -> Result<()> {
        let mut c = self.mgr.clone();
        let payload: Option<Vec<u8>> = redis::cmd("DUMP").arg(source).query_async(&mut c).await?;
        let Some(payload) = payload else {
            anyhow::bail!("'{source}' no longer exists");
        };
        // A negative TTL means "no expiry", which RESTORE spells as 0.
        let ttl: i64 = redis::cmd("PTTL").arg(source).query_async(&mut c).await?;
        let mut t = target.mgr.clone();
        let mut cmd = redis::cmd("RESTORE");
        cmd.arg(target_name).arg(ttl.max(0)).arg(payload);
        if replace {
            cmd.arg("REPLACE");
        }
        cmd.query_async::<()>(&mut t)
            .await
            .with_context(|| format!("cannot write '{target_name}' on the target server"))?;
        Ok(())
    }

    /// Keys whose *value* contains `needle`, searched case-insensitively.
    /// Walks the keyspace with `SCAN` and reads each value bounded, so it stays
    /// affordable; returns whether the limit cut the search short.
    pub async fn grep_values(
        &self,
        pattern: &str,
        needle: &str,
        limit: usize,
    ) -> Result<(Vec<KeyInfo>, bool)> {
        let (candidates, truncated) = self.scan_keys(pattern, limit).await?;
        let needle = needle.to_lowercase();
        let mut hits = Vec::new();
        for key in candidates {
            let Ok(value) = self.read_value(&key.name, key.kind).await else {
                continue;
            };
            let found = match &value {
                KeyValue::Str(s) => s.to_lowercase().contains(&needle),
                KeyValue::Rows { rows, .. } => rows
                    .iter()
                    .any(|r| r.cells.iter().any(|c| c.to_lowercase().contains(&needle))),
                KeyValue::Unsupported(_) => false,
            };
            if found {
                hits.push(key);
            }
        }
        Ok((hits, truncated))
    }

    // ---- export and import -----------------------------------------------

    /// Serialize keys with `DUMP`, so every type — and the TTL — survives the
    /// round trip through a file.
    pub async fn export_keys(&self, names: &[String]) -> Result<Vec<ExportEntry>> {
        let mut c = self.mgr.clone();
        let mut out = Vec::with_capacity(names.len());
        for chunk in names.chunks(128) {
            let mut pipe = redis::pipe();
            for name in chunk {
                pipe.cmd("DUMP").arg(name);
                pipe.cmd("PTTL").arg(name);
                pipe.cmd("TYPE").arg(name);
            }
            let replies: Vec<redis::Value> = pipe.query_async(&mut c).await?;
            for (name, triple) in chunk.iter().zip(replies.chunks(3)) {
                let [dump, pttl, kind] = triple else { continue };
                let redis::Value::BulkString(bytes) = dump else {
                    // The key expired between the scan and the dump.
                    continue;
                };
                let pttl = match pttl {
                    redis::Value::Int(i) => *i,
                    _ => -1,
                };
                out.push(ExportEntry {
                    key: name.clone(),
                    kind: format_value(kind, 0).trim().to_string(),
                    pttl,
                    dump: hex_encode(bytes),
                });
            }
        }
        Ok(out)
    }

    /// Write exported keys back. `replace` overwrites keys that already exist;
    /// without it an existing key is an error the caller reports.
    pub async fn import_entries(&self, entries: &[ExportEntry], replace: bool) -> Result<u64> {
        let mut c = self.mgr.clone();
        let mut written = 0u64;
        for entry in entries {
            let payload = hex_decode(&entry.dump)
                .with_context(|| format!("'{}' has a corrupt payload", entry.key))?;
            let mut cmd = redis::cmd("RESTORE");
            cmd.arg(&entry.key).arg(entry.pttl.max(0)).arg(payload);
            if replace {
                cmd.arg("REPLACE");
            }
            cmd.query_async::<()>(&mut c)
                .await
                .with_context(|| format!("cannot restore '{}'", entry.key))?;
            written += 1;
        }
        Ok(written)
    }

    /// Indexes this server knows about, when the search module is loaded.
    pub async fn search_indexes(&self) -> Result<Vec<String>> {
        let mut c = self.mgr.clone();
        let reply: redis::Value = redis::cmd("FT._LIST").query_async(&mut c).await?;
        Ok(match reply {
            redis::Value::Array(items) | redis::Value::Set(items) => {
                items.iter().map(scalar).collect()
            }
            _ => Vec::new(),
        })
    }

    /// Run a RediSearch query and render the reply for the results pane.
    pub async fn search(&self, index: &str, query: &str, limit: usize) -> Result<String> {
        let mut c = self.mgr.clone();
        let value: redis::Value = redis::cmd("FT.SEARCH")
            .arg(index)
            .arg(query)
            .arg("LIMIT")
            .arg(0)
            .arg(limit)
            .query_async(&mut c)
            .await?;
        Ok(format_value(&value, 0))
    }

    // ---- scripting --------------------------------------------------------

    /// Run a Lua script. `keys` become KEYS[1..], `args` become ARGV[1..].
    pub async fn eval(&self, script: &str, keys: &[String], args: &[String]) -> Result<String> {
        let mut c = self.mgr.clone();
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(script).arg(keys.len());
        for k in keys {
            cmd.arg(k);
        }
        for a in args {
            cmd.arg(a);
        }
        let value: redis::Value = cmd.query_async(&mut c).await?;
        Ok(format_value(&value, 0))
    }

    // ---- streams ----------------------------------------------------------

    /// Consumer groups on a stream, with their pending counts.
    pub async fn stream_groups(&self, key: &str) -> Result<Vec<StreamGroup>> {
        let mut c = self.mgr.clone();
        let reply: redis::Value = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(key)
            .query_async(&mut c)
            .await?;
        Ok(as_maps(&reply)
            .into_iter()
            .map(|m| StreamGroup {
                name: m.get("name").cloned().unwrap_or_default(),
                consumers: m.get("consumers").and_then(|v| v.parse().ok()).unwrap_or(0),
                pending: m.get("pending").and_then(|v| v.parse().ok()).unwrap_or(0),
                last_delivered: m.get("last-delivered-id").cloned().unwrap_or_default(),
                lag: m.get("lag").cloned().unwrap_or_else(|| "-".into()),
            })
            .collect())
    }

    /// Consumers of one group, and the entries that group has not acked.
    pub async fn stream_group_detail(&self, key: &str, group: &str) -> Result<StreamGroupDetail> {
        let mut c = self.mgr.clone();
        let consumers: redis::Value = redis::cmd("XINFO")
            .arg("CONSUMERS")
            .arg(key)
            .arg(group)
            .query_async(&mut c)
            .await?;
        let consumers = as_maps(&consumers)
            .into_iter()
            .map(|m| StreamConsumer {
                name: m.get("name").cloned().unwrap_or_default(),
                pending: m.get("pending").and_then(|v| v.parse().ok()).unwrap_or(0),
                idle_ms: m.get("idle").and_then(|v| v.parse().ok()).unwrap_or(0),
            })
            .collect();
        // `XPENDING key group - + n` lists the entries themselves, which is
        // what makes a stuck consumer visible.
        let raw: Vec<(String, String, i64, i64)> = redis::cmd("XPENDING")
            .arg(key)
            .arg(group)
            .arg("-")
            .arg("+")
            .arg(VALUE_LIMIT)
            .query_async(&mut c)
            .await
            .unwrap_or_default();
        let pending = raw
            .into_iter()
            .map(|(id, consumer, idle_ms, deliveries)| PendingEntry {
                id,
                consumer,
                idle_ms,
                deliveries,
            })
            .collect();
        Ok(StreamGroupDetail { consumers, pending })
    }

    pub async fn stream_group_create(&self, key: &str, group: &str, start: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(key)
            .arg(group)
            .arg(if start.trim().is_empty() { "$" } else { start })
            .arg("MKSTREAM")
            .query_async::<()>(&mut c)
            .await?;
        Ok(())
    }

    pub async fn stream_group_destroy(&self, key: &str, group: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        redis::cmd("XGROUP")
            .arg("DESTROY")
            .arg(key)
            .arg(group)
            .query_async::<i64>(&mut c)
            .await?;
        Ok(())
    }

    pub async fn stream_ack(&self, key: &str, group: &str, id: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        redis::cmd("XACK")
            .arg(key)
            .arg(group)
            .arg(id)
            .query_async::<i64>(&mut c)
            .await?;
        Ok(())
    }

    /// Hand a pending entry to another consumer, so work stuck behind a dead
    /// worker can be picked up again.
    pub async fn stream_claim(
        &self,
        key: &str,
        group: &str,
        consumer: &str,
        id: &str,
    ) -> Result<()> {
        let mut c = self.mgr.clone();
        redis::cmd("XCLAIM")
            .arg(key)
            .arg(group)
            .arg(consumer)
            .arg(0)
            .arg(id)
            .query_async::<redis::Value>(&mut c)
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

/// Flatten a RESP reply that is a list of maps (`XINFO`, `MODULE LIST`) into
/// string pairs. Redis answers with a map in RESP3 and a flat array in RESP2,
/// so both shapes are accepted.
fn as_maps(v: &redis::Value) -> Vec<std::collections::HashMap<String, String>> {
    let flat = |item: &redis::Value| -> std::collections::HashMap<String, String> {
        match item {
            redis::Value::Map(pairs) => pairs
                .iter()
                .map(|(k, val)| (scalar(k), scalar(val)))
                .collect(),
            redis::Value::Array(fields) => fields
                .chunks(2)
                .filter_map(|p| match p {
                    [k, val] => Some((scalar(k), scalar(val))),
                    _ => None,
                })
                .collect(),
            _ => Default::default(),
        }
    };
    match v {
        redis::Value::Array(items) | redis::Value::Set(items) => items.iter().map(flat).collect(),
        other => vec![flat(other)],
    }
}

/// One RESP value as plain text, without the console's list formatting.
fn scalar(v: &redis::Value) -> String {
    match v {
        redis::Value::BulkString(b) => String::from_utf8_lossy(b).to_string(),
        redis::Value::SimpleString(s) => s.clone(),
        redis::Value::Int(i) => i.to_string(),
        redis::Value::Double(d) => d.to_string(),
        redis::Value::Nil => String::new(),
        other => format_value(other, 0).trim().to_string(),
    }
}

/// `SLOWLOG GET` answers with `[id, unix time, microseconds, [argv], client
/// addr, client name]`.
fn parse_slowlog(v: &redis::Value) -> Vec<SlowEntry> {
    let items = match v {
        redis::Value::Array(items) | redis::Value::Set(items) => items,
        _ => return Vec::new(),
    };
    items
        .iter()
        .filter_map(|entry| {
            let redis::Value::Array(f) = entry else {
                return None;
            };
            let int = |i: usize| match f.get(i) {
                Some(redis::Value::Int(n)) => *n,
                _ => 0,
            };
            let command = match f.get(3) {
                Some(redis::Value::Array(argv)) => {
                    argv.iter().map(scalar).collect::<Vec<_>>().join(" ")
                }
                _ => String::new(),
            };
            let client = match (f.get(4), f.get(5)) {
                (Some(addr), Some(name)) => {
                    let name = scalar(name);
                    if name.is_empty() {
                        scalar(addr)
                    } else {
                        format!("{} ({name})", scalar(addr))
                    }
                }
                (Some(addr), None) => scalar(addr),
                _ => String::new(),
            };
            Some(SlowEntry {
                id: int(0),
                at: int(1),
                micros: int(2),
                command,
                client,
            })
        })
        .collect()
}

/// `CLIENT LIST` is one line per client of `field=value` pairs.
fn parse_client_list(raw: &str) -> Vec<ClientEntry> {
    raw.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|line| {
            let mut entry = ClientEntry::default();
            for (k, v) in line.split_whitespace().filter_map(|p| p.split_once('=')) {
                match k {
                    "id" => entry.id = v.to_string(),
                    "addr" => entry.addr = v.to_string(),
                    "name" => entry.name = v.to_string(),
                    "age" => entry.age_secs = v.parse().unwrap_or(0),
                    "idle" => entry.idle_secs = v.parse().unwrap_or(0),
                    "db" => entry.db = v.to_string(),
                    "cmd" => entry.command = v.to_string(),
                    _ => {}
                }
            }
            entry
        })
        .collect()
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

fn hex_decode(text: &str) -> Result<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return Err(anyhow!("odd number of hex digits"));
    }
    text.as_bytes()
        .chunks(2)
        .map(|pair| {
            let s = std::str::from_utf8(pair)?;
            Ok(u8::from_str_radix(s, 16)?)
        })
        .collect()
}

/// Pull one `key:value` line out of an `INFO` reply.
fn info_field(info: &str, key: &str) -> String {
    info.lines()
        .find_map(|l| l.strip_prefix(key).map(|v| v.trim().to_string()))
        .unwrap_or_else(|| "?".into())
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
/// Where a memory scan has got to. Held by the caller so the scan can be
/// stopped between batches without unwinding anything.
#[derive(Debug, Default, Clone)]
pub struct MemoryScan {
    cursor: u64,
    /// Keys seen so far, which is what the sampling stride counts against.
    seen: u64,
    started: bool,
}

impl MemoryScan {
    /// Fraction of the keyspace walked, judged against `dbsize`.
    pub fn progress(&self, dbsize: u64) -> f64 {
        if dbsize == 0 {
            return 1.0;
        }
        (self.seen as f64 / dbsize as f64).min(1.0)
    }
}

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

    const SAMPLE: &str = "# Server\r\nredis_version:7.2.4\r\nredis_mode:standalone\r\n\r\n# Memory\r\nused_memory_human:1.20M\r\n\r\n# Keyspace\r\ndb0:keys=12,expires=3,avg_ttl=0\r\ndb1:keys=5,expires=0,avg_ttl=0\r\n";

    #[test]
    fn parses_info_into_sections_and_fields() {
        let info = ServerInfo::parse(SAMPLE);
        assert_eq!(
            info.sections
                .iter()
                .map(|s| s.name.as_str())
                .collect::<Vec<_>>(),
            ["Server", "Memory", "Keyspace"]
        );
        assert_eq!(info.field("redis_version"), Some("7.2.4"));
        assert_eq!(info.section("memory").len(), 1);
        assert!(info.section("Replication").is_empty());
        assert_eq!(info.field("nope"), None);
    }

    #[test]
    fn reads_key_counts_out_of_the_keyspace_section() {
        let info = ServerInfo::parse(SAMPLE);
        assert_eq!(
            info.keyspace(),
            vec![("db0".to_string(), 12, 3), ("db1".to_string(), 5, 0)]
        );
    }

    #[test]
    fn parses_a_headerless_reply() {
        let info = ServerInfo::parse("redis_version:7.0.0\n");
        assert_eq!(info.field("redis_version"), Some("7.0.0"));
    }

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
