//! Async Redis access tailored to the TUI: SCAN-based listing, bounded value
//! reads, and typed mutators. Nothing here blocks the render loop.

use anyhow::{Context, Result, anyhow};
use redis::aio::MultiplexedConnection;
use redis::{
    AsyncCommands, ClientTlsConfig, ConnectionAddr, ConnectionInfo, IntoConnectionInfo,
    RedisConnectionInfo, TlsCertificates,
};

use crate::codec::{Decoding, Shown, View};
use crate::config::{Connection, Deployment};
mod edit;
mod topology;
pub use edit::{EditOutcome, EditTarget};
use topology::Transport;
pub use topology::{Node, key_slot};

#[derive(Clone, Debug, Default)]
pub struct ScanReport {
    pub keys: Vec<KeyInfo>,
    pub truncated: bool,
    pub warnings: Vec<String>,
}

/// Hard ceiling on keys pulled into one tree view.
pub const KEY_LIMIT: usize = 5_000;
/// Hard ceiling on elements pulled into one value pane.
pub const VALUE_LIMIT: usize = 1_000;
/// How far `+` can raise the key limit, one [`KEY_LIMIT`] at a time.
pub const KEY_LIMIT_MAX: usize = 50_000;
/// How far `+` can raise the element limit, one [`VALUE_LIMIT`] at a time.
pub const VALUE_LIMIT_MAX: usize = 10_000;
/// How many elements a filtered read looks at before it stops and says how
/// far it got. Scales with the element limit.
pub const FILTER_BUDGET: usize = 100_000;
/// Elements asked for per round trip by a filtered walk.
const FILTER_CHUNK: usize = 1_000;
/// Most bytes a locally matched list or stream walk pulls, per page of the
/// element limit. Server-side MATCH walks move only the matches.
pub const FILTER_BYTES: usize = 64 * 1024 * 1024;
/// About how many bytes one chunk of a locally matched walk should carry.
const CHUNK_BYTES: usize = 4 * 1024 * 1024;
/// How much of a non-text value the hex dump shows.
const HEX_DUMP_LIMIT: usize = 4_096;
/// Most decoded text one value read may produce, across all of its elements.
const DECODE_BUDGET: usize = 64 * 1024 * 1024;

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
#[derive(Clone, Debug, Default)]
pub struct Row {
    pub id: String,
    pub cells: Vec<String>,
    /// Set when the row's element value is shown decoded by a codec: how it
    /// was decoded and the bytes it came from. The id is never decoded.
    pub decoding: Option<Decoding>,
}

#[derive(Clone, Debug)]
pub enum KeyValue {
    Str(String),
    /// A string value shown through a codec: the decoded text, and how to get
    /// back to the stored bytes.
    Decoded {
        text: String,
        decoding: Decoding,
    },
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
    mgr: Transport,
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
        conn.validate_topology()?;
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
        let mgr = Transport::new(conn.clone(), client.clone()).await?;
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
        self.mgr.read_only()
    }

    /// Use the same conservative classification as the final dispatch guard.
    pub fn command_is_read_only(line: &str) -> bool {
        let Ok(args) = split_args(line) else {
            return false;
        };
        let Some(head) = args.first() else {
            return true;
        };
        let mut cmd = redis::cmd(head);
        cmd.arg(&args[1..]);
        topology::read_route(&cmd).0
    }

    pub fn production(&self) -> bool {
        self.conn.environment == crate::config::Environment::Production
    }
    pub fn write_lease_remaining(&self) -> u64 {
        self.mgr.remaining()
    }
    pub fn unlock_writes(&self, confirmation: &str) -> Result<()> {
        self.mgr.unlock(confirmation)
    }
    pub fn lock_writes(&self) -> Result<()> {
        self.mgr.lock()
    }

    /// A connection of its own, for pub/sub. The multiplexed connection cannot
    /// be put into subscriber mode without breaking every other caller.
    pub async fn pubsub(&self) -> Result<redis::aio::PubSub> {
        anyhow::ensure!(
            self.conn.deployment == Deployment::Standalone,
            "Pub/sub is not supported for discovered deployments yet"
        );
        Ok(self.raw.get_async_pubsub().await?)
    }

    /// A `MONITOR` connection of its own: every command the server runs, as
    /// it runs it. Dropping it ends the monitoring.
    pub async fn monitor(&self) -> Result<redis::aio::Monitor> {
        anyhow::ensure!(
            self.conn.deployment == Deployment::Standalone,
            "MONITOR watches one server, and is not supported for discovered deployments yet"
        );
        Ok(self.raw.get_async_monitor().await?)
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
        anyhow::ensure!(
            self.conn.deployment != Deployment::Cluster,
            "Cluster memory rollups are not available yet; use per-node diagnostics"
        );
        let mut c = self.mgr.clone();
        let (next, batch): (u64, Vec<Vec<u8>>) = redis::cmd("SCAN")
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
            let name = encode_key(&name);
            if scan.seen.is_multiple_of(stride) {
                sample.push(name.clone());
            }
            scan.seen += 1;
            rollup.count(&name);
        }

        if !sample.is_empty() {
            let mut pipe = redis::pipe();
            for name in &sample {
                pipe.cmd("MEMORY").arg("USAGE").arg(decode_key(name));
            }
            // A server with `MEMORY USAGE` disabled still gives a useful key
            // count, so a failed measurement is not a failed report.
            if let Ok(sizes) = pipe.query_async::<Vec<Option<u64>>>(&mut c).await {
                // `OBJECT FREQ` only answers under an LFU policy; asking for it
                // is best effort, and its absence just leaves the column empty.
                let mut freq_pipe = redis::pipe();
                for name in &sample {
                    freq_pipe.cmd("OBJECT").arg("FREQ").arg(decode_key(name));
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
        if self.conn.deployment == Deployment::Cluster {
            let mut total = 0;
            for ep in self.mgr.primaries().await {
                total += redis::from_redis_value::<u64>(
                    self.mgr.direct(&ep, &redis::cmd("DBSIZE")).await?,
                )?;
            }
            return Ok(total);
        }
        let mut c = self.mgr.clone();
        Ok(redis::cmd("DBSIZE").query_async(&mut c).await?)
    }

    /// Full `INFO` for the server pane. Falls back to the default sections
    /// when a managed provider rejects `INFO all`.
    pub async fn info(&self) -> Result<ServerInfo> {
        if self.conn.deployment == Deployment::Cluster {
            let mut raw = String::new();
            let mut node_sections = Vec::new();
            for ep in self.mgr.primaries().await {
                match self
                    .mgr
                    .direct(&ep, &redis::cmd("INFO"))
                    .await
                    .and_then(|v| redis::from_redis_value::<String>(v).map_err(Into::into))
                {
                    Ok(node_raw) => {
                        if raw.is_empty() {
                            raw = node_raw.clone();
                        }
                        let mut parsed = ServerInfo::parse(&node_raw);
                        for section in &mut parsed.sections {
                            section.name = format!("{}:{} {}", ep.0, ep.1, section.name);
                        }
                        node_sections.extend(parsed.sections);
                    }
                    Err(e) => node_sections.push(InfoSection {
                        name: format!("{}:{} unavailable", ep.0, ep.1),
                        fields: vec![("error".into(), e.to_string())],
                    }),
                }
            }
            let mut info = ServerInfo::parse(&raw);
            node_sections.push(InfoSection {
                name: "Topology".into(),
                fields: self.topology_rows().await,
            });
            for section in &node_sections {
                raw.push_str(&format!("\r\n# {}\r\n", section.name));
                for (k, v) in &section.fields {
                    raw.push_str(&format!("{k}:{v}\r\n"));
                }
            }
            info.sections.extend(node_sections);
            info.raw = raw;
            return Ok(info);
        }
        let mut c = self.mgr.clone();
        let mut raw: String = match redis::cmd("INFO").arg("all").query_async(&mut c).await {
            Ok(raw) => raw,
            Err(_) => redis::cmd("INFO").query_async(&mut c).await?,
        };
        if self.conn.deployment == Deployment::Sentinel {
            raw.push_str("\r\n# Topology\r\n");
            for (key, value) in self.topology_rows().await {
                raw.push_str(&format!("{key}:{value}\r\n"));
            }
        }
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
    /// Compatibility API: the boolean also signals incomplete node coverage.
    pub async fn scan_keys(&self, pattern: &str, limit: usize) -> Result<(Vec<KeyInfo>, bool)> {
        let report = self.scan_report(pattern, limit).await?;
        Ok((report.keys, report.truncated || !report.warnings.is_empty()))
    }

    pub async fn refresh_topology(&self) -> Result<Vec<Node>> {
        self.mgr.refresh().await?;
        Ok(self.mgr.nodes().await)
    }

    pub async fn scan_report(&self, pattern: &str, limit: usize) -> Result<ScanReport> {
        if self.conn.deployment != Deployment::Cluster {
            let (keys, truncated) = self.scan_standalone(pattern, limit).await?;
            return Ok(ScanReport {
                keys,
                truncated,
                warnings: vec![],
            });
        }
        let mut report = ScanReport::default();
        if let Err(e) = self.mgr.refresh().await {
            report.warnings.push(format!("Stale topology: {e}"));
        }
        let primaries = self.mgr.primaries().await;
        let nodes = self.mgr.nodes().await;
        let mut slots = vec![false; 16384];
        for n in &nodes {
            for &(a, b) in &n.slots {
                slots[a as usize..=b as usize].fill(true);
            }
        }
        let missing = slots.iter().filter(|s| !**s).count();
        if missing > 0 {
            report
                .warnings
                .push(format!("{missing} slots have no known primary"));
        }
        use futures_util::{StreamExt, stream};
        let batches = stream::iter(primaries.into_iter().map(|ep| async move {
            let mut cursor = 0u64;
            let mut names = std::collections::BTreeSet::new();
            let mut warning = None;
            let mut truncated = false;
            loop {
                let result = self
                    .mgr
                    .direct(
                        &ep,
                        redis::cmd("SCAN")
                            .arg(cursor)
                            .arg("MATCH")
                            .arg(pattern)
                            .arg("COUNT")
                            .arg(500),
                    )
                    .await
                    .and_then(|v| {
                        redis::from_redis_value::<(u64, Vec<Vec<u8>>)>(v).map_err(Into::into)
                    });
                match result {
                    Ok((next, batch)) => {
                        names.extend(batch.iter().map(|n| encode_key(n)));
                        cursor = next;
                        if names.len() >= limit || cursor == 0 {
                            truncated = cursor != 0 || names.len() > limit;
                            break;
                        }
                    }
                    Err(e) => {
                        warning = Some(format!("{}:{} SCAN unavailable: {e}", ep.0, ep.1));
                        break;
                    }
                }
            }
            (ep, names, truncated, warning)
        }))
        .buffer_unordered(8)
        .collect::<Vec<_>>()
        .await;
        let mut names = std::collections::BTreeMap::new();
        for (ep, batch, truncated, warning) in batches {
            for name in batch {
                names.entry(name).or_insert_with(|| ep.clone());
            }
            report.truncated |= truncated;
            if let Some(warning) = warning {
                report.warnings.push(warning);
            }
        }
        report.truncated |= names.len() > limit;
        let mut by_node = std::collections::BTreeMap::<_, Vec<String>>::new();
        for (name, ep) in names.into_iter().take(limit) {
            by_node.entry(ep).or_default().push(name);
        }
        for (ep, names) in by_node {
            // At most two metadata commands per retained key, in one node-local
            // pipeline. A dead node costs one failed batch, not one timeout per key.
            let metadata = self.mgr.metadata(&ep, &names).await;
            let mut unknown = 0;
            for (i, name) in names.into_iter().enumerate() {
                let pair = metadata.as_ref().ok().and_then(|values| {
                    Some((
                        values.get(i * 2)?.clone().extract_error(),
                        values.get(i * 2 + 1)?.clone().extract_error(),
                    ))
                });
                let info = match pair {
                    Some((Ok(kind), Ok(ttl))) => match (
                        redis::from_redis_value::<String>(kind),
                        redis::from_redis_value::<i64>(ttl),
                    ) {
                        (Ok(kind), Ok(ttl)) => Some(KeyInfo {
                            name: name.clone(),
                            kind: KeyType::parse(&kind),
                            ttl,
                        }),
                        _ => None,
                    },
                    Some((Err(e), _)) | Some((_, Err(e))) if e.redirect_node().is_some() => {
                        self.key_info(&name).await.ok()
                    }
                    _ => None,
                };
                match info {
                    Some(info) if info.ttl != -2 => report.keys.push(info),
                    Some(_) => {}
                    None => {
                        unknown += 1;
                        report.keys.push(KeyInfo {
                            name,
                            kind: KeyType::Other,
                            ttl: -2,
                        });
                    }
                }
            }
            if unknown > 0 {
                report.warnings.push(format!(
                    "{}:{} metadata unavailable for {unknown} keys; displayed as unknown",
                    ep.0, ep.1
                ));
            }
        }
        report.keys.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(report)
    }

    async fn scan_standalone(&self, pattern: &str, limit: usize) -> Result<(Vec<KeyInfo>, bool)> {
        let mut c = self.mgr.clone();
        let mut cursor: u64 = 0;
        let mut names: Vec<String> = Vec::new();
        loop {
            let (next, batch): (u64, Vec<Vec<u8>>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(500)
                .query_async(&mut c)
                .await?;
            names.extend(batch.iter().map(|n| encode_key(n)));
            cursor = next;
            if cursor == 0 || names.len() >= limit {
                break;
            }
        }
        let truncated = cursor != 0 || names.len() > limit;
        names.truncate(limit);
        if names.is_empty() {
            return Ok((Vec::new(), truncated));
        }

        let mut type_pipe = redis::pipe();
        for n in &names {
            type_pipe.cmd("TYPE").arg(decode_key(n));
        }
        let types: Vec<String> = type_pipe.query_async(&mut c).await?;

        let mut ttl_pipe = redis::pipe();
        for n in &names {
            ttl_pipe.cmd("TTL").arg(decode_key(n));
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
        let t: String = redis::cmd("TYPE")
            .arg(decode_key(name))
            .query_async(&mut c)
            .await?;
        let ttl: i64 = redis::cmd("TTL")
            .arg(decode_key(name))
            .query_async(&mut c)
            .await?;
        Ok(KeyInfo {
            name: name.to_string(),
            kind: KeyType::parse(&t),
            ttl,
        })
    }

    /// Read a bounded window of a key's value. Collection types report their
    /// true total so the UI can say "showing 1000 of 4.2M".
    ///
    /// Values are shown exactly as stored: text, or a hex dump of bytes that
    /// are not text. [`Client::read_value_as`] reads through a codec instead.
    pub async fn read_value(&self, name: &str, kind: KeyType) -> Result<KeyValue> {
        let (raw, _) = self.read_raw(name, kind, &Window::default()).await?;
        Ok(materialize(raw, &View::Plain).0)
    }

    /// [`Client::read_value`] seen through `view`: compressed, packed or
    /// encoded values come back decoded, carrying the bytes they came from so
    /// an edit can be encoded the same way. The second half is a notice for
    /// the status line when a chosen codec could not read some of the value.
    pub async fn read_value_as(
        &self,
        name: &str,
        kind: KeyType,
        view: &View,
    ) -> Result<(KeyValue, Option<String>)> {
        let read = self
            .read_window(name, kind, view, &Window::default())
            .await?;
        Ok((read.value, read.notice))
    }

    /// [`Client::read_value_as`] over a chosen window of a collection: more
    /// than the default number of elements, and optionally only the ones that
    /// match a pattern. Says how much of the collection the read covered.
    pub async fn read_window(
        &self,
        name: &str,
        kind: KeyType,
        view: &View,
        window: &Window,
    ) -> Result<Read> {
        let (raw, coverage) = self.read_raw(name, kind, window).await?;
        let (value, notice) = if *view == View::Plain {
            materialize(raw, view)
        } else {
            // Decompressing tens of megabytes, or waiting on a custom codec's
            // program, must not hold up the async workers.
            let view = view.clone();
            tokio::task::spawn_blocking(move || materialize(raw, &view))
                .await
                .context("decoding the value failed")?
        };
        Ok(Read {
            value,
            notice,
            coverage,
        })
    }

    async fn read_raw(
        &self,
        name: &str,
        kind: KeyType,
        window: &Window,
    ) -> Result<(RawValue, Coverage)> {
        let mut c = self.mgr.clone();
        let lim = window.limit.clamp(1, VALUE_LIMIT_MAX);
        let filter = window.filter.as_deref().filter(|f| !f.is_empty());
        // How many elements a filtered read may look at, and how many bytes
        // a locally matched walk may pull. Loading more widens both in step
        // with the limit.
        let pages = (lim / VALUE_LIMIT).max(1);
        let budget = FILTER_BUDGET as u64 * pages as u64;
        let byte_budget = FILTER_BYTES * pages;
        let whole = |total: u64| Coverage {
            filtered: false,
            complete: true,
            examined: total,
        };
        Ok(match kind {
            KeyType::String => {
                let v: Option<Vec<u8>> = c.get(decode_key(name)).await?;
                (RawValue::Str(v.unwrap_or_default()), whole(1))
            }
            KeyType::Hash => {
                let total: u64 = c.hlen(decode_key(name)).await?;
                let scan = self
                    .scan_elements("HSCAN", name, filter, lim, 2, budget)
                    .await?;
                let coverage = scan.coverage(scan.items.len() / 2, lim, total, filter.is_some());
                let mut items = scan.items.into_iter();
                let mut rows = Vec::new();
                while let (Some(f), Some(v)) = (items.next(), items.next()) {
                    let f = decode_value(f);
                    rows.push(RawRow {
                        id: f.clone(),
                        cells: vec![RawCell::Text(f), RawCell::Bytes(v)],
                    });
                }
                rows.truncate(lim);
                rows.sort_by(|a, b| a.id.cmp(&b.id));
                (
                    RawValue::Rows {
                        headers: vec!["field", "value"],
                        rows,
                        total,
                    },
                    coverage,
                )
            }
            KeyType::List => {
                let total: u64 = c.llen(decode_key(name)).await?;
                let Some(pattern) = filter else {
                    let items: Vec<Vec<u8>> =
                        c.lrange(decode_key(name), 0, lim as isize - 1).await?;
                    let rows: Vec<RawRow> = items
                        .into_iter()
                        .enumerate()
                        .map(|(i, v)| RawRow {
                            id: i.to_string(),
                            cells: vec![RawCell::Text(i.to_string()), RawCell::Bytes(v)],
                        })
                        .collect();
                    let coverage = Coverage {
                        filtered: false,
                        complete: rows.len() as u64 >= total,
                        examined: rows.len() as u64,
                    };
                    return Ok((
                        RawValue::Rows {
                            headers: vec!["index", "value"],
                            rows,
                            total,
                        },
                        coverage,
                    ));
                };
                // Lists have no MATCH: walk them in chunks and keep the
                // matches, each with its real index so edits and deletes
                // still address the right element. Every item crosses the
                // network to be matched, so the walk is held to a byte budget
                // too, and chunks shrink when items are large.
                let mut rows = Vec::new();
                let mut start = 0u64;
                let mut pace = Pace::new(byte_budget);
                while start < total && start < budget && rows.len() <= lim && pace.room() {
                    let want = (pace.chunk() as u64).min(budget - start);
                    let end = start + want - 1;
                    let items: Vec<Vec<u8>> = c
                        .lrange(decode_key(name), start as isize, end as isize)
                        .await?;
                    if items.is_empty() {
                        break;
                    }
                    pace.took(items.len(), items.iter().map(Vec::len).sum());
                    for (i, v) in items.iter().enumerate() {
                        if crate::glob::matches(pattern.as_bytes(), v) {
                            let index = (start + i as u64).to_string();
                            rows.push(RawRow {
                                id: index.clone(),
                                cells: vec![RawCell::Text(index), RawCell::Bytes(v.clone())],
                            });
                        }
                    }
                    start += items.len() as u64;
                }
                let complete = start >= total && rows.len() <= lim;
                rows.truncate(lim);
                (
                    RawValue::Rows {
                        headers: vec!["index", "value"],
                        rows,
                        total,
                    },
                    Coverage {
                        filtered: true,
                        complete,
                        examined: start.min(total),
                    },
                )
            }
            KeyType::Set => {
                let total: u64 = c.scard(decode_key(name)).await?;
                let scan = self
                    .scan_elements("SSCAN", name, filter, lim, 1, budget)
                    .await?;
                let mut rows: Vec<RawRow> = scan
                    .items
                    .iter()
                    .map(|m| RawRow {
                        id: decode_value(m.clone()),
                        cells: vec![RawCell::Bytes(m.clone())],
                    })
                    .collect();
                let coverage = scan.coverage(rows.len(), lim, total, filter.is_some());
                rows.truncate(lim);
                rows.sort_by(|a, b| a.id.cmp(&b.id));
                (
                    RawValue::Rows {
                        headers: vec!["member"],
                        rows,
                        total,
                    },
                    coverage,
                )
            }
            KeyType::ZSet => {
                let total: u64 = c.zcard(decode_key(name)).await?;
                let (items, coverage) = match filter {
                    None => {
                        let items: Vec<(Vec<u8>, f64)> = c
                            .zrange_withscores(decode_key(name), 0, lim as isize - 1)
                            .await?;
                        let coverage = Coverage {
                            filtered: false,
                            complete: items.len() as u64 >= total,
                            examined: items.len() as u64,
                        };
                        (items, coverage)
                    }
                    Some(_) => {
                        // Scan to the budget rather than stopping at the first
                        // `lim` matches, so what is shown is the lowest scores
                        // among everything examined, not an arbitrary subset.
                        let scan = self
                            .scan_elements("ZSCAN", name, filter, usize::MAX, 2, budget)
                            .await?;
                        let mut flat = scan.items.iter();
                        let mut items = Vec::new();
                        while let (Some(m), Some(s)) = (flat.next(), flat.next()) {
                            let score = std::str::from_utf8(s)
                                .ok()
                                .and_then(|s| s.parse::<f64>().ok())
                                .unwrap_or(f64::NAN);
                            items.push((m.clone(), score));
                        }
                        // ZSCAN hands members back in no order; show them in
                        // score order, the way the unfiltered view does.
                        items.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
                        let coverage = scan.coverage(items.len(), lim, total, true);
                        items.truncate(lim);
                        (items, coverage)
                    }
                };
                let rows = items
                    .into_iter()
                    .map(|(m, s)| RawRow {
                        id: decode_value(m.clone()),
                        cells: vec![RawCell::Bytes(m), RawCell::Text(format_score(s))],
                    })
                    .collect();
                (
                    RawValue::Rows {
                        headers: vec!["member", "score"],
                        rows,
                        total,
                    },
                    coverage,
                )
            }
            KeyType::Stream => {
                let total: u64 = c.xlen(decode_key(name)).await?;
                let entry = |(id, flat): (String, Vec<Vec<u8>>)| RawRow {
                    id: id.clone(),
                    cells: vec![RawCell::Text(id), RawCell::Fields(flat)],
                };
                let Some(pattern) = filter else {
                    let raw: Vec<(String, Vec<Vec<u8>>)> = redis::cmd("XREVRANGE")
                        .arg(decode_key(name))
                        .arg("+")
                        .arg("-")
                        .arg("COUNT")
                        .arg(lim)
                        .query_async(&mut c)
                        .await?;
                    let rows: Vec<RawRow> = raw.into_iter().map(entry).collect();
                    let coverage = Coverage {
                        filtered: false,
                        complete: rows.len() as u64 >= total,
                        examined: rows.len() as u64,
                    };
                    return Ok((
                        RawValue::Rows {
                            headers: vec!["id", "fields"],
                            rows,
                            total,
                        },
                        coverage,
                    ));
                };
                // Newest first, a chunk at a time, keeping entries where any
                // field name or value matches.
                let mut rows = Vec::new();
                let mut end = "+".to_string();
                let mut examined = 0u64;
                let mut reached_start = false;
                let mut pace = Pace::new(byte_budget);
                while examined < budget && rows.len() <= lim && pace.room() {
                    let asked = pace.chunk().min((budget - examined) as usize);
                    let chunk: Vec<(String, Vec<Vec<u8>>)> = redis::cmd("XREVRANGE")
                        .arg(decode_key(name))
                        .arg(&end)
                        .arg("-")
                        .arg("COUNT")
                        .arg(asked)
                        .query_async(&mut c)
                        .await?;
                    examined += chunk.len() as u64;
                    pace.took(
                        chunk.len(),
                        chunk
                            .iter()
                            .map(|(_, flat)| flat.iter().map(Vec::len).sum::<usize>())
                            .sum(),
                    );
                    let next = chunk.last().and_then(|(id, _)| previous_stream_id(id));
                    let short = chunk.len() < asked;
                    for (id, flat) in chunk {
                        if flat
                            .iter()
                            .any(|part| crate::glob::matches(pattern.as_bytes(), part))
                        {
                            rows.push(entry((id, flat)));
                        }
                    }
                    match next {
                        Some(next) if !short => end = next,
                        _ => {
                            reached_start = true;
                            break;
                        }
                    }
                }
                let complete = reached_start && rows.len() <= lim;
                rows.truncate(lim);
                (
                    RawValue::Rows {
                        headers: vec!["id", "fields"],
                        rows,
                        total,
                    },
                    Coverage {
                        filtered: true,
                        complete,
                        examined: if complete { total } else { examined.min(total) },
                    },
                )
            }
            KeyType::Json => {
                let doc: Option<Vec<u8>> = redis::cmd("JSON.GET")
                    .arg(decode_key(name))
                    .arg(".")
                    .query_async(&mut c)
                    .await?;
                (
                    RawValue::Doc(doc.map(decode_value).unwrap_or_else(|| "null".into())),
                    whole(1),
                )
            }
            KeyType::TimeSeries => {
                let raw: Vec<(u64, f64)> = redis::cmd("TS.RANGE")
                    .arg(decode_key(name))
                    .arg("-")
                    .arg("+")
                    .arg("COUNT")
                    .arg(lim)
                    .query_async(&mut c)
                    .await?;
                let total = raw.len() as u64;
                let coverage = Coverage {
                    filtered: false,
                    // TS.RANGE has no cheap total; a short page is the end.
                    complete: raw.len() < lim,
                    examined: total,
                };
                let rows = raw
                    .into_iter()
                    .map(|(ts, v)| RawRow {
                        id: ts.to_string(),
                        cells: vec![
                            RawCell::Text(ts.to_string()),
                            RawCell::Text(format_score(v)),
                        ],
                    })
                    .collect();
                (
                    RawValue::Rows {
                        headers: vec!["timestamp", "value"],
                        rows,
                        total,
                    },
                    coverage,
                )
            }
            KeyType::Other => (
                RawValue::Unsupported(
                    "This key's type has no viewer yet. Use the command console (:) to inspect it."
                        .into(),
                ),
                whole(0),
            ),
        })
    }

    /// Walk a hash, set or sorted set with its `*SCAN` command until `want`
    /// elements are in hand, the cursor comes back to 0, or a filtered walk
    /// has looked at `budget` elements. `per_item` is how many reply entries
    /// make one element (2 for field/value and member/score pairs).
    async fn scan_elements(
        &self,
        command: &str,
        name: &str,
        filter: Option<&str>,
        want: usize,
        per_item: usize,
        budget: u64,
    ) -> Result<Scanned> {
        let mut c = self.mgr.clone();
        // Unfiltered walks keep the batch size they always had. A filter
        // mostly returns nothing per call, so it asks for more work per call.
        let count = if filter.is_some() { FILTER_CHUNK } else { 200 };
        let mut items = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut cursor: u64 = 0;
        let mut examined = 0u64;
        loop {
            let mut cmd = redis::cmd(command);
            cmd.arg(decode_key(name)).arg(cursor);
            if let Some(pattern) = filter {
                cmd.arg("MATCH").arg(pattern);
            }
            cmd.arg("COUNT").arg(count);
            let (next, batch): (u64, Vec<Vec<u8>>) = cmd.query_async(&mut c).await?;
            // SCAN may return an element twice while the collection rehashes.
            let mut batch = batch.into_iter();
            while let Some(first) = batch.next() {
                let rest: Vec<Vec<u8>> = batch.by_ref().take(per_item - 1).collect();
                if seen.insert(first.clone()) {
                    items.push(first);
                    items.extend(rest);
                }
            }
            examined += count as u64;
            cursor = next;
            if cursor == 0
                || items.len() / per_item >= want
                || (filter.is_some() && examined >= budget)
            {
                break;
            }
        }
        Ok(Scanned {
            items,
            finished: cursor == 0,
            examined,
        })
    }

    // ---- key-level mutations -------------------------------------------

    pub async fn delete_key(&self, name: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: i64 = c.del(decode_key(name)).await?;
        Ok(())
    }

    pub async fn rename_key(&self, old: &str, new: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        redis::cmd("RENAME")
            .arg(decode_key(old))
            .arg(decode_key(new))
            .query_async::<()>(&mut c)
            .await?;
        Ok(())
    }

    /// `None` removes the expiry (PERSIST).
    pub async fn set_ttl(&self, name: &str, seconds: Option<i64>) -> Result<()> {
        let mut c = self.mgr.clone();
        match seconds {
            Some(s) if s >= 0 => {
                let _: bool = c.expire(decode_key(name), s).await?;
            }
            _ => {
                let _: bool = c.persist(decode_key(name)).await?;
            }
        }
        Ok(())
    }

    pub async fn create_key(&self, name: &str, kind: KeyType) -> Result<()> {
        let mut c = self.mgr.clone();
        match kind {
            KeyType::String => {
                let _: () = c.set(decode_key(name), "").await?;
            }
            KeyType::Hash => {
                let _: i64 = c.hset(decode_key(name), "field", "value").await?;
            }
            KeyType::List => {
                let _: i64 = c.rpush(decode_key(name), "item").await?;
            }
            KeyType::Set => {
                let _: i64 = c.sadd(decode_key(name), "member").await?;
            }
            KeyType::ZSet => {
                let _: i64 = c.zadd(decode_key(name), "member", 0.0).await?;
            }
            KeyType::Stream => {
                let _: String = redis::cmd("XADD")
                    .arg(decode_key(name))
                    .arg("*")
                    .arg("field")
                    .arg("value")
                    .query_async(&mut c)
                    .await?;
            }
            KeyType::Json => {
                redis::cmd("JSON.SET")
                    .arg(decode_key(name))
                    .arg("$")
                    .arg("{}")
                    .query_async::<()>(&mut c)
                    .await?;
            }
            KeyType::TimeSeries => {
                redis::cmd("TS.CREATE")
                    .arg(decode_key(name))
                    .query_async::<()>(&mut c)
                    .await?;
            }
            KeyType::Other => return Err(anyhow!("unsupported key type")),
        }
        Ok(())
    }

    pub async fn set_string(&self, name: &str, value: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        redis::cmd("SET")
            .arg(decode_key(name))
            .arg(value)
            .arg("KEEPTTL")
            .query_async::<()>(&mut c)
            .await?;
        Ok(())
    }

    // ---- element-level mutations ----------------------------------------

    pub async fn hash_set(&self, name: &str, field: &str, value: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: i64 = c.hset(decode_key(name), field, value).await?;
        Ok(())
    }

    pub async fn hash_del(&self, name: &str, field: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: i64 = c.hdel(decode_key(name), field).await?;
        Ok(())
    }

    pub async fn list_push(&self, name: &str, value: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: i64 = c.rpush(decode_key(name), value).await?;
        Ok(())
    }

    pub async fn list_set(&self, name: &str, index: isize, value: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: () = c.lset(decode_key(name), index, value).await?;
        Ok(())
    }

    /// Redis has no delete-by-index. Overwrite the slot with a unique sentinel,
    /// then LREM it — the standard swap-and-trim, made safe by a sentinel that
    /// cannot collide with real data.
    /// Remove the item at `index`, but only if it still holds `expected`.
    /// Lists shift under concurrent pushes and pops, so an index read a
    /// moment ago can already name a different item. Returns false, having
    /// changed nothing, when it does.
    pub async fn list_remove_checked(
        &self,
        name: &str,
        index: isize,
        expected: &[u8],
    ) -> Result<bool> {
        const REMOVE: &str = r#"
local current = redis.call('LINDEX', KEYS[1], ARGV[1])
if current ~= ARGV[2] then return 0 end
redis.call('LSET', KEYS[1], ARGV[1], ARGV[3])
redis.call('LREM', KEYS[1], 1, ARGV[3])
return 1
"#;
        let mut c = self.mgr.clone();
        let removed: i64 = redis::cmd("EVAL")
            .arg(REMOVE)
            .arg(1)
            .arg(decode_key(name))
            .arg(index)
            .arg(expected)
            .arg(sentinel())
            .query_async(&mut c)
            .await?;
        Ok(removed == 1)
    }

    pub async fn list_remove_at(&self, name: &str, index: isize) -> Result<()> {
        let mut c = self.mgr.clone();
        let sentinel = sentinel();
        let _: () = c.lset(decode_key(name), index, &sentinel).await?;
        let _: i64 = c.lrem(decode_key(name), 1, &sentinel).await?;
        Ok(())
    }

    pub async fn set_add(&self, name: &str, member: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: i64 = c.sadd(decode_key(name), member).await?;
        Ok(())
    }

    pub async fn set_remove(&self, name: &str, member: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: i64 = c.srem(decode_key(name), member).await?;
        Ok(())
    }

    pub async fn zset_add(&self, name: &str, member: &str, score: f64) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: i64 = c.zadd(decode_key(name), member, score).await?;
        Ok(())
    }

    pub async fn zset_remove(&self, name: &str, member: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: i64 = c.zrem(decode_key(name), member).await?;
        Ok(())
    }

    pub async fn stream_add(&self, name: &str, field: &str, value: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        let _: String = redis::cmd("XADD")
            .arg(decode_key(name))
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
            .arg(decode_key(name))
            .arg(id)
            .query_async(&mut c)
            .await?;
        Ok(())
    }

    pub async fn json_set(&self, name: &str, path: &str, value: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        redis::cmd("JSON.SET")
            .arg(decode_key(name))
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
            .arg(decode_key(name))
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
            .arg(decode_key(name))
            .arg(timestamp)
            .arg(timestamp)
            .query_async::<i64>(&mut c)
            .await?;
        Ok(())
    }

    // ---- diagnostics ------------------------------------------------------

    async fn topology_rows(&self) -> Vec<(String, String)> {
        let mut rows = vec![("deployment".into(), self.conn.deployment.name().into())];
        if let Err(e) = self.mgr.refresh().await {
            rows.push(("refresh_error".into(), e.to_string()));
        }
        if let Some(warning) = self.mgr.warning().await {
            rows.push(("coverage_warning".into(), warning));
        }
        for (i, node) in self.mgr.nodes().await.iter().enumerate() {
            let slots = node
                .slots
                .iter()
                .map(|(a, b)| format!("{a}-{b}"))
                .collect::<Vec<_>>()
                .join(",");
            let status = match self
                .mgr
                .direct(&(node.host.clone(), node.port), &redis::cmd("PING"))
                .await
            {
                Ok(_) => "reachable".to_string(),
                Err(e) => format!("unavailable: {e}"),
            };
            rows.push((
                format!("node_{i}"),
                format!(
                    "{} {}:{} {} slots=[{}] {}",
                    node.id,
                    node.host,
                    node.port,
                    if node.primary { "primary" } else { "replica" },
                    slots,
                    status
                ),
            ));
        }
        rows
    }

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
        let mut cluster: Vec<(String, String)> = redis::cmd("CLUSTER")
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
        if self.conn.deployment != Deployment::Standalone {
            cluster.extend(self.topology_rows().await);
        }
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
    async fn latency_rows(&self, c: &mut Transport) -> Vec<(String, String)> {
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
                pipe.cmd("UNLINK").arg(decode_key(name));
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
                    Some(s) if s >= 0 => pipe.cmd("EXPIRE").arg(decode_key(name)).arg(s),
                    _ => pipe.cmd("PERSIST").arg(decode_key(name)),
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
        let payload: Option<Vec<u8>> = redis::cmd("DUMP")
            .arg(decode_key(source))
            .query_async(&mut c)
            .await?;
        let Some(payload) = payload else {
            anyhow::bail!("'{source}' no longer exists");
        };
        // A negative TTL means "no expiry", which RESTORE spells as 0.
        let ttl: i64 = redis::cmd("PTTL")
            .arg(decode_key(source))
            .query_async(&mut c)
            .await?;
        let mut t = target.mgr.clone();
        let mut cmd = redis::cmd("RESTORE");
        cmd.arg(decode_key(target_name))
            .arg(ttl.max(0))
            .arg(payload);
        if replace {
            cmd.arg("REPLACE");
        }
        cmd.query_async::<()>(&mut t)
            .await
            .map_err(|e| anyhow!("cannot write '{target_name}' on the target server: {e}"))?;
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
            // Compressed and packed values are searched as their decoded text.
            let Ok((value, _)) = self.read_value_as(&key.name, key.kind, &View::Auto).await else {
                continue;
            };
            let found = match &value {
                KeyValue::Str(s) | KeyValue::Decoded { text: s, .. } => {
                    s.to_lowercase().contains(&needle)
                }
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
                pipe.cmd("DUMP").arg(decode_key(name));
                pipe.cmd("PTTL").arg(decode_key(name));
                pipe.cmd("TYPE").arg(decode_key(name));
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
            cmd.arg(decode_key(&entry.key))
                .arg(entry.pttl.max(0))
                .arg(payload);
            if replace {
                cmd.arg("REPLACE");
            }
            cmd.query_async::<()>(&mut c)
                .await
                .map_err(|e| anyhow!("cannot restore '{}': {e}", entry.key))?;
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
            cmd.arg(decode_key(k));
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
            .arg(decode_key(key))
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
            .arg(decode_key(key))
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
            .arg(decode_key(key))
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
            .arg(decode_key(key))
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
            .arg(decode_key(key))
            .arg(group)
            .query_async::<i64>(&mut c)
            .await?;
        Ok(())
    }

    pub async fn stream_ack(&self, key: &str, group: &str, id: &str) -> Result<()> {
        let mut c = self.mgr.clone();
        redis::cmd("XACK")
            .arg(decode_key(key))
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
            .arg(decode_key(key))
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
        anyhow::ensure!(
            !(matches!(
                head.to_ascii_uppercase().as_str(),
                "SELECT"
                    | "AUTH"
                    | "HELLO"
                    | "RESET"
                    | "MULTI"
                    | "EXEC"
                    | "DISCARD"
                    | "WATCH"
                    | "UNWATCH"
            ) || (head.eq_ignore_ascii_case("CLIENT")
                && tail
                    .first()
                    .is_some_and(|s| s.eq_ignore_ascii_case("REPLY")))),
            "Connection state commands are not supported in the shared console; use the profile or database selector"
        );
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

/// Redis values are byte strings, and plenty of them are not text: session
/// blobs, protobuf, MessagePack and gzip all live under ordinary looking key
/// names. Decoding those straight into a `String` fails the whole read, so a
/// value that is not UTF-8 becomes a hex dump the viewer can still show.
fn decode_value(bytes: Vec<u8>) -> String {
    match String::from_utf8(bytes) {
        Ok(text) => text,
        Err(e) => hex_dump(e.as_bytes()),
    }
}

/// Most of a `MONITOR` line's detail the feed keeps.
pub const MONITOR_DETAIL_LIMIT: usize = 2 * 1024;

/// One line of `MONITOR` output, split for the feed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MonitorLine {
    /// The command name, upper case: `SET`, `HGETALL`.
    pub command: String,
    /// Database and client, then the arguments as Redis quoted them:
    /// `db0 127.0.0.1:52100  "user:1" "ada"`.
    pub detail: String,
}

impl MonitorLine {
    /// The line as the feed keeps it. Arguments can be megabytes of escaped
    /// binary, so the detail is cut to [`MONITOR_DETAIL_LIMIT`] with a note of
    /// how much was left out. Filter before calling this, on the whole line.
    pub fn shortened(mut self) -> Self {
        if self.detail.len() > MONITOR_DETAIL_LIMIT {
            let extra = self.detail.len() - MONITOR_DETAIL_LIMIT;
            let mut cut = MONITOR_DETAIL_LIMIT;
            while !self.detail.is_char_boundary(cut) {
                cut -= 1;
            }
            self.detail.truncate(cut);
            self.detail.push_str(&format!(" … {extra} more bytes"));
        }
        self
    }
}

/// Split a `MONITOR` line such as
/// `1718000000.123456 [0 127.0.0.1:52100] "set" "user:1" "ada"`.
/// The timestamp is dropped: the feed stamps arrival itself.
pub fn parse_monitor_line(line: &str) -> Option<MonitorLine> {
    let (_, rest) = line.split_once(' ')?;
    let rest = rest.strip_prefix('[')?;
    let (source, args) = rest.split_once("] ")?;
    let (db, client) = source.split_once(' ')?;
    let args = args.trim_start();
    let quoted = args.strip_prefix('"')?;
    let end = quoted.find('"')?;
    let command = quoted[..end].to_ascii_uppercase();
    let arguments = quoted[end + 1..].trim_start();
    Some(MonitorLine {
        command,
        detail: format!("db{db} {client}  {arguments}")
            .trim_end()
            .to_string(),
    })
}

/// A stored value as text: itself when it is UTF-8, a hex dump when it is not.
pub fn text_or_dump(bytes: Vec<u8>) -> String {
    decode_value(bytes)
}

/// Which part of a collection a read covers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Window {
    /// Most elements to return.
    pub limit: usize,
    /// A Redis glob: only elements matching it are returned. Hash fields and
    /// set and sorted-set members are matched by the server; list items and
    /// stream field names and values are matched here. Always the stored bytes.
    pub filter: Option<String>,
}

impl Default for Window {
    fn default() -> Self {
        Self {
            limit: VALUE_LIMIT,
            filter: None,
        }
    }
}

/// How much of a collection a read covered, for the value pane header.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Coverage {
    /// The read was filtered.
    pub filtered: bool,
    /// Every element was looked at and every match returned: loading more
    /// would change nothing.
    pub complete: bool,
    /// About how many elements were looked at.
    pub examined: u64,
}

impl Coverage {
    /// What a read with no window information covered, judged from its rows.
    pub fn of(value: &KeyValue) -> Self {
        match value {
            KeyValue::Rows { rows, total, .. } => Self {
                filtered: false,
                complete: rows.len() as u64 >= *total,
                examined: rows.len() as u64,
            },
            _ => Self {
                filtered: false,
                complete: true,
                examined: 1,
            },
        }
    }
}

/// A value read through a view and a window.
#[derive(Clone, Debug)]
pub struct Read {
    pub value: KeyValue,
    /// Why part of the value could not be shown through the chosen view.
    pub notice: Option<String>,
    pub coverage: Coverage,
}

/// What a `*SCAN` walk collected.
struct Scanned {
    items: Vec<Vec<u8>>,
    /// The cursor came back to 0: nothing was left unvisited.
    finished: bool,
    examined: u64,
}

impl Scanned {
    fn coverage(&self, found: usize, limit: usize, total: u64, filtered: bool) -> Coverage {
        let complete = self.finished && found <= limit;
        let examined = if complete {
            total
        } else if filtered {
            self.examined.min(total)
        } else {
            found.min(limit) as u64
        };
        Coverage {
            filtered,
            complete,
            examined,
        }
    }
}

/// How big the next chunk of a locally matched walk should be. Starts small,
/// grows to [`FILTER_CHUNK`] for small elements and shrinks for large ones,
/// so one reply never carries much more than [`CHUNK_BYTES`].
struct Pace {
    chunk: usize,
    left: usize,
}

impl Pace {
    fn new(bytes: usize) -> Self {
        Self {
            chunk: 64,
            left: bytes,
        }
    }

    fn chunk(&self) -> usize {
        self.chunk
    }

    /// Whether the byte budget has any room left.
    fn room(&self) -> bool {
        self.left > 0
    }

    fn took(&mut self, elements: usize, bytes: usize) {
        self.left = self.left.saturating_sub(bytes);
        if let Some(average) = bytes.checked_div(elements) {
            // Shrink at once for large items, but grow at most twofold per
            // chunk, so a jump from tiny to huge items costs one small chunk.
            let target = (CHUNK_BYTES / average.max(1)).clamp(1, FILTER_CHUNK);
            self.chunk = target.min(self.chunk * 2);
        }
    }
}

/// The stream id just before `id`, for walking backwards with an exclusive
/// end on servers older than 6.2, which have no `(id` syntax.
fn previous_stream_id(id: &str) -> Option<String> {
    let (ms, seq) = id.split_once('-')?;
    let (ms, seq): (u64, u64) = (ms.parse().ok()?, seq.parse().ok()?);
    match (ms, seq) {
        (_, s) if s > 0 => Some(format!("{ms}-{}", s - 1)),
        (m, _) if m > 0 => Some(format!("{}-{}", m - 1, u64::MAX)),
        _ => None,
    }
}

/// A value as read from the server, before any codec has looked at it.
enum RawValue {
    Str(Vec<u8>),
    /// A RedisJSON document: always text, never passed through a codec.
    Doc(String),
    Rows {
        headers: Vec<&'static str>,
        rows: Vec<RawRow>,
        total: u64,
    },
    Unsupported(String),
}

struct RawRow {
    id: String,
    cells: Vec<RawCell>,
}

enum RawCell {
    /// Indexes, hash field names, scores, stream ids: shown as they are.
    Text(String),
    /// An element's value, which a codec may decode.
    Bytes(Vec<u8>),
    /// A stream entry's flat field/value list; the values may be decoded.
    Fields(Vec<Vec<u8>>),
}

/// Turn a raw read into what the value pane shows, decoding through `view`.
/// With [`View::Plain`] this is exactly what the pane has always shown.
fn materialize(raw: RawValue, view: &View) -> (KeyValue, Option<String>) {
    let mut failures = 0usize;
    let mut first_error = None;
    // One budget for the whole value: each element may use what the ones
    // before it left, in bytes and in time for a custom codec's program.
    let mut budget = crate::codec::Budget::new(DECODE_BUDGET);
    let mut cell = |bytes: Vec<u8>| -> (String, Option<Decoding>) {
        match crate::codec::show_with(bytes, view, &mut budget) {
            Shown::Plain(text) => (text, None),
            Shown::Decoded { text, decoding } => (text, Some(decoding)),
            Shown::Failed { text, error } => {
                failures += 1;
                first_error.get_or_insert(error);
                (text, None)
            }
        }
    };
    let value = match raw {
        RawValue::Str(bytes) => match cell(bytes) {
            (text, None) => KeyValue::Str(text),
            (text, Some(decoding)) => KeyValue::Decoded { text, decoding },
        },
        RawValue::Doc(text) => KeyValue::Str(text),
        RawValue::Unsupported(msg) => KeyValue::Unsupported(msg),
        RawValue::Rows {
            headers,
            rows,
            total,
        } => {
            let rows = rows
                .into_iter()
                .map(|row| {
                    let mut decoded = None;
                    let cells = row
                        .cells
                        .into_iter()
                        .map(|c| match c {
                            RawCell::Text(text) => text,
                            RawCell::Bytes(bytes) => {
                                let (text, decoding) = cell(bytes);
                                decoded = decoded.take().or(decoding);
                                text
                            }
                            RawCell::Fields(flat) => {
                                let mut flat = flat.into_iter();
                                let mut parts = Vec::new();
                                while let Some(f) = flat.next() {
                                    let f = decode_value(f);
                                    match flat.next() {
                                        Some(v) => {
                                            let (v, decoding) = cell(v);
                                            decoded = decoded.take().or(decoding);
                                            parts.push(format!("{f}={v}"));
                                        }
                                        None => parts.push(f),
                                    }
                                }
                                parts.join("  ")
                            }
                        })
                        .collect();
                    Row {
                        id: row.id,
                        cells,
                        decoding: decoded,
                    }
                })
                .collect();
            KeyValue::Rows {
                headers,
                rows,
                total,
            }
        }
    };
    let notice = first_error.map(|error| {
        let codec = match view {
            View::Codec(codec) => codec.name().to_string(),
            _ => "the codec".into(),
        };
        if matches!(value, KeyValue::Rows { .. }) {
            format!("{failures} element(s) could not be read as {codec} and are shown as stored: {error}")
        } else {
            format!("Could not read this value as {codec}; showing it as stored: {error}")
        }
    });
    (value, notice)
}

/// Opens every hex dump, so a dump can be recognised again and never written
/// back to the server as if it were the value it describes.
pub const BINARY_MARKER: &str = "<binary, ";

/// True when this text is a rendering of bytes rather than the bytes
/// themselves. Saving one would replace a value with its own description.
pub fn is_hex_dump(text: &str) -> bool {
    text.starts_with(BINARY_MARKER) && text.split_once(" bytes>\n").is_some()
}

/// The first `HEX_DUMP_LIMIT` bytes as offset / hex / ASCII columns.
fn hex_dump(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let shown = bytes.len().min(HEX_DUMP_LIMIT);
    let mut out = format!("{BINARY_MARKER}{} bytes>\n", bytes.len());
    for (i, chunk) in bytes[..shown].chunks(16).enumerate() {
        let hex = chunk.iter().fold(String::new(), |mut acc, b| {
            let _ = write!(acc, "{b:02x} ");
            acc
        });
        let ascii: String = chunk
            .iter()
            .map(|b| {
                if b.is_ascii_graphic() || *b == b' ' {
                    *b as char
                } else {
                    '.'
                }
            })
            .collect();
        let _ = writeln!(out, "{:08x}  {hex:<48} |{ascii}|", i * 16);
    }
    if bytes.len() > shown {
        let _ = writeln!(out, "… {} more bytes", bytes.len() - shown);
    }
    out
}

/// A key name as the rest of the program carries it: text, with every byte
/// that is not valid UTF-8 written `\xNN` and a literal backslash doubled.
/// Redis key names are arbitrary bytes, so decoding one straight into a
/// `String` fails the whole scan when a key holds a binary id.
pub fn encode_key(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len());
    let mut rest = bytes;
    loop {
        match std::str::from_utf8(rest) {
            Ok(text) => {
                push_escaped(&mut out, text);
                return out;
            }
            Err(e) => {
                let (good, bad) = rest.split_at(e.valid_up_to());
                push_escaped(&mut out, std::str::from_utf8(good).unwrap_or_default());
                // Without a length the rest of the input is one bad tail.
                let skip = e.error_len().unwrap_or(bad.len());
                for b in &bad[..skip] {
                    let _ = write!(out, "\\x{b:02x}");
                }
                rest = &bad[skip..];
            }
        }
    }
}

fn push_escaped(out: &mut String, text: &str) {
    for ch in text.chars() {
        if ch == '\\' {
            out.push_str("\\\\");
        } else {
            out.push(ch);
        }
    }
}

/// The inverse of [`encode_key`]: back to the exact bytes the server stored.
/// An escape it does not recognise stays literal, so a name a person typed by
/// hand still addresses the key they meant.
pub fn decode_key(name: &str) -> Vec<u8> {
    let b = name.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\' && i + 1 < b.len() {
            if b[i + 1] == b'\\' {
                out.push(b'\\');
                i += 2;
                continue;
            }
            if b[i + 1] == b'x'
                && i + 3 < b.len()
                && let Ok(pair) = std::str::from_utf8(&b[i + 2..i + 4])
                && let Ok(byte) = u8::from_str_radix(pair, 16)
            {
                out.push(byte);
                i += 4;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    out
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

    #[test]
    fn monitor_lines_split_into_command_and_detail() {
        let line = parse_monitor_line(
            r#"1718000000.123456 [0 127.0.0.1:52100] "set" "user:1" "ada lovelace""#,
        )
        .unwrap();
        assert_eq!(line.command, "SET");
        assert_eq!(
            line.detail,
            r#"db0 127.0.0.1:52100  "user:1" "ada lovelace""#
        );

        let lua = parse_monitor_line(r#"1718000000.1 [3 lua] "incr" "hits""#).unwrap();
        assert_eq!(
            (lua.command.as_str(), lua.detail.as_str()),
            ("INCR", r#"db3 lua  "hits""#)
        );

        let unix = parse_monitor_line(r#"1.0 [0 unix:/tmp/redis.sock] "ping""#).unwrap();
        assert_eq!(unix.command, "PING");
        assert_eq!(unix.detail, "db0 unix:/tmp/redis.sock");

        let escaped =
            parse_monitor_line(r#"1.0 [0 1.2.3.4:5] "set" "k" "say \"hi\" \x00""#).unwrap();
        assert_eq!(escaped.detail, r#"db0 1.2.3.4:5  "k" "say \"hi\" \x00""#);

        for junk in ["OK", "", "1.0 no brackets", "1.0 [0 x] no quotes"] {
            assert_eq!(parse_monitor_line(junk), None, "{junk:?}");
        }
    }

    #[test]
    fn monitor_details_are_cut_to_a_glimpse() {
        let big = "x".repeat(MONITOR_DETAIL_LIMIT * 3);
        let full = parse_monitor_line(&format!(r#"1.0 [0 1.2.3.4:5] "set" "k" "{big}""#)).unwrap();
        assert!(
            full.detail.len() > MONITOR_DETAIL_LIMIT * 3,
            "parsing keeps it whole"
        );
        let line = full.shortened();
        assert!(
            line.detail.len() < MONITOR_DETAIL_LIMIT + 40,
            "{}",
            line.detail.len()
        );
        assert!(
            line.detail.ends_with("more bytes"),
            "{}",
            &line.detail[line.detail.len() - 30..]
        );
    }

    #[test]
    fn a_walk_paces_its_chunks_by_element_size() {
        let mut pace = Pace::new(10 * CHUNK_BYTES);
        assert_eq!(pace.chunk(), 64, "starts small before sizes are known");
        pace.took(64, 64 * 100);
        assert_eq!(pace.chunk(), 128, "small elements: grows, at most twofold");
        for _ in 0..5 {
            pace.took(pace.chunk(), pace.chunk() * 100);
        }
        assert_eq!(pace.chunk(), FILTER_CHUNK, "until full chunks");
        pace.took(10, 10 * 1024 * 1024);
        assert_eq!(pace.chunk(), 4, "megabyte elements: a few at a time");
        pace.took(1, 10 * 1024 * 1024);
        assert_eq!(pace.chunk(), 1, "ten-megabyte elements: one at a time");
        assert!(pace.room());
        pace.took(1, 100 * CHUNK_BYTES);
        assert!(!pace.room(), "the byte budget is spent");
    }

    #[test]
    fn stream_ids_step_back_across_the_millisecond() {
        assert_eq!(previous_stream_id("5-3").as_deref(), Some("5-2"));
        assert_eq!(
            previous_stream_id("5-0").as_deref(),
            Some("4-18446744073709551615")
        );
        assert_eq!(previous_stream_id("0-0"), None);
        assert_eq!(previous_stream_id("garbage"), None);
    }

    #[test]
    fn coverage_of_a_plain_read_follows_its_rows() {
        let rows = |n: usize, total: u64| KeyValue::Rows {
            headers: vec!["member"],
            rows: vec![Row::default(); n],
            total,
        };
        assert!(Coverage::of(&rows(3, 3)).complete);
        assert!(!Coverage::of(&rows(1000, 5000)).complete);
        assert!(Coverage::of(&KeyValue::Str("x".into())).complete);
    }

    #[test]
    fn key_names_round_trip_through_their_escaped_form() {
        for raw in [
            b"bff:session:44ec".to_vec(),
            "ключ:1".as_bytes().to_vec(),
            vec![b'k', 0xff, 0xfe, b':', 0x00],
            b"path\\to\\key".to_vec(),
        ] {
            assert_eq!(decode_key(&encode_key(&raw)), raw, "{raw:?}");
        }
    }

    #[test]
    fn binary_key_names_escape_only_the_bad_bytes() {
        assert_eq!(encode_key(b"plain:key"), "plain:key");
        assert_eq!(encode_key(&[b'a', 0xc3, 0x28, b'b']), "a\\xc3(b");
        assert_eq!(encode_key(b"a\\b"), "a\\\\b");
    }

    #[test]
    fn an_unknown_escape_stays_literal() {
        // Someone typing a Windows path into the key box means the backslash.
        assert_eq!(decode_key("C:\\temp"), b"C:\\temp");
        assert_eq!(decode_key("tail\\x"), b"tail\\x");
    }

    #[test]
    fn text_values_decode_unchanged() {
        assert_eq!(decode_value(b"hello".to_vec()), "hello");
        assert_eq!(decode_value("héllo".as_bytes().to_vec()), "héllo");
    }

    #[test]
    fn binary_values_become_a_hex_dump() {
        // A session blob: the byte at index 1 is what the UTF-8 decoder chokes on.
        let dump = decode_value(vec![0x1f, 0x8b, 0x08, 0x00, b'i', b'd']);
        assert!(dump.starts_with("<binary, 6 bytes>"), "{dump}");
        assert!(dump.contains("1f 8b 08 00 69 64"), "{dump}");
        assert!(dump.contains("|....id|"), "{dump}");
    }

    #[test]
    fn hex_dump_stops_at_the_limit() {
        let dump = decode_value(vec![0xff; HEX_DUMP_LIMIT + 10]);
        assert!(dump.contains("… 10 more bytes"), "{dump}");
    }

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
