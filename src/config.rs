//! Saved connection profiles, persisted as JSON under the user's config dir.

use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::theme::Theme;

fn default_host() -> String {
    "127.0.0.1".to_string()
}
fn default_port() -> u16 {
    6379
}

fn default_ssh_port() -> u16 {
    22
}

/// Discovery mode. Older profiles remain standalone.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Deployment {
    #[default]
    Standalone,
    Cluster,
    Sentinel,
}

impl Deployment {
    pub fn name(self) -> &'static str {
        match self {
            Self::Standalone => "standalone",
            Self::Cluster => "cluster",
            Self::Sentinel => "sentinel",
        }
    }
}

pub(crate) fn parse_endpoint(text: &str) -> Result<(String, u16)> {
    let (host, port) = text
        .rsplit_once(':')
        .context("endpoint must be host:port (or [IPv6]:port)")?;
    let host = host.trim_matches(['[', ']']);
    anyhow::ensure!(
        !host.is_empty() && !host.contains(['/', '@', ' ']),
        "invalid endpoint host"
    );
    let port = port.parse::<u16>()?;
    anyhow::ensure!(port != 0, "endpoint port must be nonzero");
    Ok((host.to_string(), port))
}

/// Operational environment; old profiles retain development behavior.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Environment {
    #[default]
    Development,
    Staging,
    Production,
}
impl Environment {
    pub fn name(self) -> &'static str {
        match self {
            Self::Development => "development",
            Self::Staging => "staging",
            Self::Production => "production",
        }
    }
}

/// How the server list arranges saved profiles.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionView {
    /// Every profile in stored order, the way the list has always looked.
    Flat,
    /// Profiles that name a group sit under a collapsible header. With no
    /// groups at all this is the same list as [`ConnectionView::Flat`].
    /// Also what a view name from a newer version reads as, which is why it
    /// is the last variant.
    #[default]
    #[serde(other)]
    Grouped,
}

impl ConnectionView {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// Read `connection_view` without ever failing the file: a layout preference
/// that does not parse must not hide every saved profile.
fn lenient_view<'de, D: serde::Deserializer<'de>>(d: D) -> Result<ConnectionView, D::Error> {
    let value = serde_json::Value::deserialize(d)?;
    Ok(serde_json::from_value(value).unwrap_or_default())
}

/// Read `collapsed_groups` without ever failing the file. Names are trimmed
/// the way group names are, and anything that is not a name is skipped.
fn lenient_group_names<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    let value = serde_json::Value::deserialize(d)?;
    let serde_json::Value::Array(items) = value else {
        return Ok(Vec::new());
    };
    Ok(items
        .iter()
        .filter_map(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect())
}

/// A single saved server profile.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Connection {
    pub name: String,
    /// The server-list group this profile is shown under. One level only, and
    /// absent from the file for ungrouped profiles so older files round-trip.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default)]
    pub environment: Environment,
    #[serde(default)]
    pub deployment: Deployment,
    /// Additional cluster seeds or Sentinel endpoints, including ports.
    #[serde(default)]
    pub seeds: Vec<String>,
    #[serde(default)]
    pub sentinel_master: String,
    #[serde(default)]
    pub sentinel_username: String,
    /// Separate Sentinel credentials; supports environment placeholders.
    #[serde(default)]
    pub sentinel_password: String,

    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    /// A Unix domain socket to connect through instead of `host` and `port`.
    /// Empty for a TCP profile, and then left out of the file entirely.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub socket: String,
    #[serde(default)]
    pub db: i64,
    #[serde(default)]
    pub username: String,
    /// Stored as typed, but `${VAR}` / `$VAR` is expanded from the environment at
    /// connect time so a profile never has to hold a literal secret.
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub tls: bool,

    /// Keep the password in the OS keychain instead of this file.
    #[serde(default)]
    pub use_keychain: bool,

    /// PEM root certificate, when the server is not signed by a public CA.
    #[serde(default)]
    pub tls_ca_file: String,
    /// PEM client certificate and key, for mutual TLS.
    #[serde(default)]
    pub tls_cert_file: String,
    #[serde(default)]
    pub tls_key_file: String,
    /// Accept any server certificate. Useful against a self-signed dev server,
    /// dangerous anywhere else.
    #[serde(default)]
    pub tls_insecure: bool,

    /// Refuse every write from this profile: edits, deletes, and the console
    /// commands that change data. Set it on production servers.
    #[serde(default)]
    pub read_only: bool,

    /// Reach the server through `ssh -L`, e.g. a bastion in front of a managed
    /// cache. Empty means a direct connection.
    #[serde(default)]
    pub ssh_host: String,
    #[serde(default)]
    pub ssh_user: String,
    #[serde(default = "default_ssh_port")]
    pub ssh_port: u16,
    /// Private key passed to ssh as `-i`. Empty leaves the choice to ssh.
    #[serde(default)]
    pub ssh_key_file: String,
}

impl Default for Connection {
    fn default() -> Self {
        Self {
            name: String::new(),
            group: None,
            environment: Environment::default(),
            deployment: Deployment::Standalone,
            seeds: Vec::new(),
            sentinel_master: String::new(),
            sentinel_username: String::new(),
            sentinel_password: String::new(),
            host: default_host(),
            port: default_port(),
            socket: String::new(),
            db: 0,
            username: String::new(),
            password: String::new(),
            tls: false,
            use_keychain: false,
            tls_ca_file: String::new(),
            tls_cert_file: String::new(),
            tls_key_file: String::new(),
            tls_insecure: false,
            read_only: false,
            ssh_host: String::new(),
            ssh_user: String::new(),
            ssh_port: default_ssh_port(),
            ssh_key_file: String::new(),
        }
    }
}

impl Connection {
    pub fn validate_topology(&self) -> Result<()> {
        self.validate_socket()?;
        anyhow::ensure!(
            self.deployment != Deployment::Cluster || self.db == 0,
            "Cluster supports database 0 only"
        );
        anyhow::ensure!(
            self.deployment == Deployment::Standalone || !self.uses_ssh(),
            "Topology discovery requires direct access to advertised nodes; a single SSH forward is unsupported"
        );
        anyhow::ensure!(
            self.deployment != Deployment::Sentinel || !self.sentinel_master.trim().is_empty(),
            "Sentinel primary service name is required"
        );
        for seed in &self.seeds {
            parse_endpoint(seed)?;
        }
        Ok(())
    }

    /// The group this profile belongs to: trimmed, and `None` when blank.
    pub fn group_name(&self) -> Option<&str> {
        self.group
            .as_deref()
            .map(str::trim)
            .filter(|g| !g.is_empty())
    }

    /// True when this profile reaches the server through an SSH tunnel.
    pub fn uses_ssh(&self) -> bool {
        !self.ssh_host.trim().is_empty()
    }

    /// True when this profile connects through a Unix domain socket.
    pub fn uses_socket(&self) -> bool {
        !self.socket_path().is_empty()
    }

    /// The socket path as it is connected to and shown. Surrounding
    /// whitespace is dropped everywhere, so a stray space in a hand-edited
    /// file cannot make the profile look like a socket profile but dial a
    /// path that does not exist.
    pub fn socket_path(&self) -> &str {
        self.socket.trim()
    }

    /// A socket profile talks to one local server, so everything that needs a
    /// network address has nothing to work with.
    pub fn validate_socket(&self) -> Result<()> {
        if !self.uses_socket() {
            return Ok(());
        }
        anyhow::ensure!(
            cfg!(unix),
            "Unix sockets are not available on this platform; use host and port"
        );
        anyhow::ensure!(
            self.deployment == Deployment::Standalone,
            "A Unix socket reaches one server; Cluster and Sentinel need host and port"
        );
        anyhow::ensure!(
            !self.uses_ssh(),
            "A Unix socket is local; it cannot go through an SSH tunnel"
        );
        anyhow::ensure!(!self.tls, "TLS does not apply to a Unix socket");
        Ok(())
    }

    /// Where the profile points, the way the server list and title bar show
    /// it: `redis://host:port/db`, `rediss://` with TLS, or `unix://path?db=N`.
    pub fn address(&self) -> String {
        if self.uses_socket() {
            return format!("unix://{}?db={}", url_path(self.socket_path()), self.db);
        }
        let scheme = if self.tls { "rediss" } else { "redis" };
        format!("{scheme}://{}:{}/{}", self.host, self.port, self.db)
    }

    /// The server part of [`Connection::address`], for status lines.
    pub fn endpoint(&self) -> String {
        if self.uses_socket() {
            self.socket_path().to_string()
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }

    /// The password to authenticate with: the keychain entry when this profile
    /// opts in, otherwise the stored value with `${VAR}` placeholders expanded.
    ///
    /// Blocking — a keychain read talks to the OS. Call it off the render loop.
    pub fn resolve_password(&self) -> anyhow::Result<String> {
        if self.use_keychain {
            return crate::secrets::get(&self.name);
        }
        Ok(self.expanded_password())
    }

    /// Expand `${VAR}` or `$VAR` password placeholders from the environment.
    /// A password that is not a placeholder is returned unchanged.
    pub fn expanded_password(&self) -> String {
        let p = self.password.trim();
        let var = if let Some(rest) = p.strip_prefix("${") {
            rest.strip_suffix('}')
        } else {
            p.strip_prefix('$')
        };
        match var {
            Some(name)
                if !name.is_empty()
                    && name
                        .chars()
                        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_') =>
            {
                std::env::var(name).unwrap_or_default()
            }
            _ => self.password.clone(),
        }
    }

    /// Parse `redis://user:pass@host:port/db` (or `rediss://`) into a profile,
    /// or `unix:///path/to/redis.sock?db=N` (also `redis+unix://`), where the
    /// credentials go in `user` and `pass` query parameters.
    pub fn from_url(url: &str) -> Result<Self> {
        #[cfg(not(unix))]
        anyhow::ensure!(
            !["unix:", "redis+unix:", "valkey+unix:"]
                .iter()
                .any(|scheme| url.trim_start().starts_with(scheme)),
            "Unix sockets are not available on this platform: {url}"
        );
        let info: redis::ConnectionInfo = url
            .parse::<redis::ConnectionInfo>()
            .with_context(|| format!("invalid redis url: {url}"))?;
        let (host, port, tls, socket) = match info.addr() {
            redis::ConnectionAddr::Tcp(h, p) => (h.clone(), *p, false, String::new()),
            redis::ConnectionAddr::TcpTls { host, port, .. } => {
                (host.clone(), *port, true, String::new())
            }
            redis::ConnectionAddr::Unix(path) => (
                default_host(),
                default_port(),
                false,
                path.display().to_string(),
            ),
            other => anyhow::bail!("unsupported redis address: {other}"),
        };
        let settings = info.redis_settings();
        Ok(Self {
            name: if socket.is_empty() {
                host.clone()
            } else {
                socket.clone()
            },
            host,
            port,
            socket,
            db: settings.db(),
            username: settings.username().unwrap_or_default().to_string(),
            password: settings.password().unwrap_or_default().to_string(),
            tls,
            ..Default::default()
        })
    }
}

/// `path` written as the path of a URL: the characters that would end the
/// path or start an escape are percent-encoded, so an address holding a
/// space, `%`, `?` or `#` still parses back to the same socket.
fn url_path(path: &str) -> std::borrow::Cow<'_, str> {
    let escape = |c: char| matches!(c, '%' | '?' | '#' | ' ') || c.is_ascii_control();
    if !path.contains(escape) {
        return std::borrow::Cow::Borrowed(path);
    }
    let mut out = String::with_capacity(path.len() + 8);
    for c in path.chars() {
        if escape(c) {
            out.push_str(&format!("%{:02X}", c as u32));
        } else {
            out.push(c);
        }
    }
    std::borrow::Cow::Owned(out)
}

pub fn config_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("REDISCOPE_HOME") {
        return PathBuf::from(dir);
    }
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("rediscope")
}

/// Where the console keeps its command history.
pub fn history_file() -> PathBuf {
    config_dir().join("history")
}

pub fn config_file() -> PathBuf {
    config_dir().join("connections.json")
}

/// Where a profile was left last time: the database, the search pattern, the
/// folders that were open and the key that was selected. Restored on connect
/// so reopening a server lands where you were rather than at the root.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Session {
    #[serde(default)]
    pub db: i64,
    #[serde(default)]
    pub pattern: String,
    #[serde(default)]
    pub expanded: Vec<String>,
    #[serde(default)]
    pub selected_key: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Store {
    /// The UI colour theme. Missing in older files, so those keep the classic
    /// Redis look automatically.
    #[serde(default)]
    pub theme: Theme,
    #[serde(default)]
    pub connections: Vec<Connection>,
    /// Per-profile view state, keyed by profile name.
    #[serde(default)]
    pub sessions: std::collections::HashMap<String, Session>,
    /// Codecs defined by the user: external programs the value pane can view,
    /// and optionally edit, values through. Absent unless configured, so files
    /// without any are written exactly as before.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub codecs: Vec<crate::codec::CustomCodec>,
    /// Grouped or flat server list. Written only once someone picks flat.
    #[serde(
        default,
        deserialize_with = "lenient_view",
        skip_serializing_if = "ConnectionView::is_default"
    )]
    pub connection_view: ConnectionView,
    /// Groups folded shut in the server list. Every group starts expanded,
    /// so a group nobody collapsed never appears here.
    #[serde(
        default,
        deserialize_with = "lenient_group_names",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub collapsed_groups: Vec<String>,
    /// Set when the file exists but could not be read. Saving is refused while
    /// it is set, so a bad read can never overwrite good profiles with an
    /// empty list.
    #[serde(skip)]
    pub read_error: Option<String>,
}

impl Store {
    /// Load saved profiles, plus a notice to show the user when the file was
    /// not in the state we expected.
    ///
    /// Only a *missing* file starts a fresh store. A file that exists but does
    /// not parse is moved aside rather than silently replaced, and a file that
    /// cannot be read at all blocks saving, because either case used to end
    /// with the next edit overwriting every saved profile.
    pub fn load() -> (Self, Option<String>) {
        let path = config_file();
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return (
                    Self {
                        connections: vec![Connection {
                            name: "local".into(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    None,
                );
            }
            Err(e) => {
                let notice = format!("Cannot read {}: {e}", path.display());
                return (
                    Self {
                        read_error: Some(notice.clone()),
                        ..Default::default()
                    },
                    Some(format!("{notice} — saving is disabled so nothing is lost")),
                );
            }
        };
        match serde_json::from_str::<Self>(&text) {
            Ok(store) => (store, None),
            Err(e) => match quarantine(&path) {
                // The unreadable file is kept, so a bad parse costs nothing.
                Ok(kept) => (
                    Self::default(),
                    Some(format!(
                        "{} did not parse ({e}); kept a copy at {}",
                        path.display(),
                        kept.display()
                    )),
                ),
                Err(move_err) => {
                    let notice = format!("{} did not parse ({e})", path.display());
                    (
                        Self {
                            read_error: Some(notice.clone()),
                            ..Default::default()
                        },
                        Some(format!(
                            "{notice}; could not set it aside ({move_err}) — saving is disabled"
                        )),
                    )
                }
            },
        }
    }

    pub fn save(&self) -> Result<()> {
        if let Some(e) = &self.read_error {
            anyhow::bail!("refusing to overwrite the profiles that are already on disk: {e}");
        }
        let dir = config_dir();
        fs::create_dir_all(&dir).with_context(|| format!("cannot create {}", dir.display()))?;
        restrict(&dir, 0o700);
        let path = config_file();
        // A profile that opted into the keychain must not leave a copy of its
        // password behind in the file.
        let sanitized = Self {
            theme: self.theme,
            connections: self
                .connections
                .iter()
                .map(|c| {
                    if c.use_keychain {
                        Connection {
                            password: String::new(),
                            ..c.clone()
                        }
                    } else {
                        c.clone()
                    }
                })
                .collect(),
            sessions: self.sessions.clone(),
            codecs: self.codecs.clone(),
            connection_view: self.connection_view,
            // A group whose last profile was deleted or moved is gone; its
            // collapsed state must not linger for a future group of that name.
            collapsed_groups: self
                .collapsed_groups
                .iter()
                .filter(|g| {
                    self.connections
                        .iter()
                        .any(|c| c.group_name() == Some(g.as_str()))
                })
                .cloned()
                .collect(),
            read_error: None,
        };
        let text = serde_json::to_string_pretty(&sanitized)?;
        // Write beside the real file and rename over it: a crash or a full disk
        // then leaves the previous profiles intact instead of a half file.
        // A unique scratch name, so two saves in flight at once (two processes,
        // or two threads) cannot rename each other's file away.
        let tmp = path.with_extension(format!("json.tmp-{}", scratch_id()));
        // Owner-only from the moment it exists: writing first and tightening
        // afterwards would publish the credentials for the gap in between.
        write_private(&tmp, &text).with_context(|| format!("cannot write {}", tmp.display()))?;
        if path.exists() {
            let _ = fs::copy(&path, path.with_extension("json.bak"));
        }
        fs::rename(&tmp, &path).with_context(|| format!("cannot write {}", path.display()))?;
        // Profiles may hold credentials: keep them owner-only.
        restrict(&path, 0o600);
        Ok(())
    }

    /// Insert or replace by name, keeping list order stable for existing names.
    pub fn upsert(&mut self, conn: Connection, replacing: Option<&str>) {
        let target = replacing.unwrap_or(&conn.name).to_string();
        if let Some(slot) = self.connections.iter_mut().find(|c| c.name == target) {
            *slot = conn;
        } else {
            self.connections.push(conn);
        }
    }

    pub fn remove(&mut self, name: &str) {
        self.connections.retain(|c| c.name != name);
    }

    /// Copy a profile under a free name, placed directly after the original.
    /// Returns the new index, or `None` if `index` is out of range.
    pub fn duplicate(&mut self, index: usize) -> Option<usize> {
        let mut copy = self.connections.get(index)?.clone();
        copy.name = self.unique_name(&copy.name);
        // The copy has no keychain entry of its own yet.
        copy.use_keychain = false;
        self.connections.insert(index + 1, copy);
        Some(index + 1)
    }

    fn unique_name(&self, base: &str) -> String {
        // "prod", "prod copy" and "prod copy 4" all share the stem "prod", so
        // duplicating a duplicate does not stack suffixes.
        let stem = base
            .rsplit_once(" copy")
            .filter(|(_, rest)| {
                let rest = rest.trim();
                rest.is_empty() || rest.chars().all(|c| c.is_ascii_digit())
            })
            .map(|(head, _)| head)
            .unwrap_or(base);
        let mut candidate = format!("{stem} copy");
        let mut n = 2;
        while self.connections.iter().any(|c| c.name == candidate) {
            candidate = format!("{stem} copy {n}");
            n += 1;
        }
        candidate
    }

    /// Swap a profile with its neighbour. Returns the index it ended up at.
    pub fn move_by(&mut self, index: usize, delta: isize) -> usize {
        let len = self.connections.len();
        if len == 0 {
            return 0;
        }
        let target = (index as isize + delta).clamp(0, len as isize - 1) as usize;
        if target != index {
            self.connections.swap(index, target);
        }
        target
    }

    /// Swap a profile with the nearest profile in the same group, in the
    /// direction of `delta`: the grouped list shows each group's members, and
    /// the ungrouped profiles, in stored order, so that is the neighbour the
    /// user sees. Clamps at either end. Returns the index it ended up at.
    pub fn move_within_group(&mut self, index: usize, delta: isize) -> usize {
        let Some(group) = self
            .connections
            .get(index)
            .map(|c| c.group_name().map(str::to_string))
        else {
            return index;
        };
        let same = |c: &Connection| c.group_name() == group.as_deref();
        let target = if delta < 0 {
            self.connections[..index].iter().rposition(same)
        } else if delta > 0 {
            self.connections[index + 1..]
                .iter()
                .position(same)
                .map(|p| index + 1 + p)
        } else {
            None
        };
        match target {
            Some(t) => {
                self.connections.swap(index, t);
                t
            }
            None => index,
        }
    }
}

/// Expand a leading `~` so certificate paths can be written the way users type
/// them into a shell. `~\\` is accepted too, for a Windows shell.
pub fn expand_home(path: &str) -> std::path::PathBuf {
    let trimmed = path.trim();
    let rest = trimmed
        .strip_prefix("~/")
        .or_else(|| trimmed.strip_prefix("~\\"));
    if let Some(rest) = rest
        && let Some(home) = dirs::home_dir()
    {
        return home.join(rest);
    }
    PathBuf::from(trimmed)
}

#[cfg(unix)]
fn restrict(path: &std::path::Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    let _ = fs::set_permissions(path, fs::Permissions::from_mode(mode));
}

/// Create `path` unreadable to anyone but the owner and write `text` into it.
#[cfg(unix)]
pub(crate) fn write_private(path: &std::path::Path, text: &str) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(text.as_bytes())
}

/// See [`restrict`]: the containing directory carries the protection here.
#[cfg(not(unix))]
pub(crate) fn write_private(path: &std::path::Path, text: &str) -> std::io::Result<()> {
    fs::write(path, text)
}

/// Windows has no mode bits. The file lives under the per-user profile
/// directory (`%APPDATA%`), whose default ACL already keeps other standard
/// users out, so there is nothing to tighten here.
#[cfg(not(unix))]
fn restrict(_path: &std::path::Path, _mode: u32) {}

/// Unique suffix for a scratch file: pid plus a per-process counter.
fn scratch_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// Move a file we could not parse out of the way, keeping its contents under a
/// timestamped name so a hand edit is never thrown away.
fn quarantine(path: &std::path::Path) -> std::io::Result<PathBuf> {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let kept = path.with_extension(format!("json.bad-{stamp}"));
    fs::rename(path, &kept)?;
    Ok(kept)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// These tests all point REDISCOPE_HOME somewhere private; the variable is
    /// process-wide, so they must not run at the same time.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn env_guard() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Holds [`ENV_LOCK`] for the whole test and unsets everything it set, so a
    /// failing assertion cannot leak `REDISCOPE_HOME` into the next test.
    struct ScopedEnv {
        _guard: std::sync::MutexGuard<'static, ()>,
        names: Vec<&'static str>,
    }

    impl ScopedEnv {
        fn new() -> Self {
            Self {
                _guard: env_guard(),
                names: Vec::new(),
            }
        }

        fn set(&mut self, name: &'static str, value: impl AsRef<std::ffi::OsStr>) -> &mut Self {
            self.names.push(name);
            // SAFETY: ENV_LOCK serialises every environment mutation in this
            // module, and nothing else in the test binary reads the process
            // environment while the guard is held.
            unsafe { std::env::set_var(name, value) };
            self
        }
    }

    impl Drop for ScopedEnv {
        fn drop(&mut self) {
            for name in self.names.drain(..) {
                // SAFETY: as in `set` - the guard is still held here.
                unsafe { std::env::remove_var(name) };
            }
        }
    }

    #[test]
    fn url_parsing_reads_db_and_tls() {
        let c = Connection::from_url("rediss://alice:s3cret@example.com:6380/3").unwrap();
        assert_eq!(c.host, "example.com");
        assert_eq!(c.port, 6380);
        assert_eq!(c.db, 3);
        assert_eq!(c.username, "alice");
        assert_eq!(c.password, "s3cret");
        assert!(c.tls);
    }

    #[cfg(unix)]
    #[test]
    fn a_socket_address_parses_back_to_the_same_path() {
        for path in [
            "/tmp/r.sock",
            "/tmp/my dir/r.sock",
            "/tmp/100%/r.sock",
            "/tmp/a?b#c/r.sock",
            "/tmp/%20/r.sock",
            "/tmp/café/r.sock",
        ] {
            let c = Connection {
                socket: format!("  {path} "),
                db: 5,
                ..Connection::default()
            };
            assert_eq!(c.socket_path(), path);
            let back = Connection::from_url(&c.address()).unwrap();
            assert_eq!(
                (back.socket.as_str(), back.db),
                (path, 5),
                "{}",
                c.address()
            );
        }
        let plain = Connection {
            socket: "/run/redis.sock".into(),
            ..Connection::default()
        };
        assert_eq!(plain.address(), "unix:///run/redis.sock?db=0");
    }

    #[cfg(unix)]
    #[test]
    fn socket_urls_become_socket_profiles() {
        let c = Connection::from_url("unix:///run/redis/redis.sock").unwrap();
        assert_eq!(c.socket, "/run/redis/redis.sock");
        assert_eq!(c.name, "/run/redis/redis.sock");
        assert_eq!(c.db, 0);
        assert!(!c.tls);
        assert!(c.validate_topology().is_ok());

        let c = Connection::from_url("redis+unix:///tmp/r.sock?db=4&user=ada&pass=s3cret").unwrap();
        assert_eq!(c.socket, "/tmp/r.sock");
        assert_eq!(c.db, 4);
        assert_eq!(c.username, "ada");
        assert_eq!(c.password, "s3cret");
        assert_eq!(c.address(), "unix:///tmp/r.sock?db=4");
        assert_eq!(c.endpoint(), "/tmp/r.sock");
    }

    #[cfg(not(unix))]
    #[test]
    fn socket_urls_are_refused_where_there_are_no_sockets() {
        let err = Connection::from_url("unix:///run/redis.sock").unwrap_err();
        assert!(err.to_string().contains("not available"), "{err}");
        let socket = Connection {
            socket: "/run/redis.sock".into(),
            ..Default::default()
        };
        assert!(socket.validate_topology().is_err());
    }

    #[cfg(unix)]
    #[test]
    fn a_socket_profile_refuses_what_needs_a_network_address() {
        let socket = || Connection {
            socket: "/tmp/redis.sock".into(),
            ..Default::default()
        };
        assert!(socket().validate_topology().is_ok());
        let cases = [
            (
                Connection {
                    tls: true,
                    ..socket()
                },
                "TLS",
            ),
            (
                Connection {
                    ssh_host: "bastion".into(),
                    ..socket()
                },
                "SSH",
            ),
            (
                Connection {
                    deployment: Deployment::Cluster,
                    ..socket()
                },
                "Cluster",
            ),
            (
                Connection {
                    deployment: Deployment::Sentinel,
                    sentinel_master: "m".into(),
                    ..socket()
                },
                "Sentinel",
            ),
        ];
        for (conn, word) in cases {
            let err = conn.validate_topology().unwrap_err().to_string();
            assert!(err.contains(word), "{word}: {err}");
        }
        // Whitespace alone is not a socket.
        let blank = Connection {
            socket: "  ".into(),
            tls: true,
            ..Default::default()
        };
        assert!(!blank.uses_socket());
        assert!(blank.validate_topology().is_ok());
        assert_eq!(blank.address(), "rediss://127.0.0.1:6379/0");
    }

    #[test]
    fn a_socket_path_is_written_only_when_set() {
        let mut env = ScopedEnv::new();
        let dir = tempdir();
        env.set("REDISCOPE_HOME", &dir);
        let store = Store {
            connections: vec![
                Connection {
                    name: "tcp".into(),
                    ..Default::default()
                },
                Connection {
                    name: "local socket".into(),
                    socket: "/tmp/redis.sock".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        store.save().unwrap();
        let json: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(config_file()).unwrap()).unwrap();
        assert!(json["connections"][0].get("socket").is_none());
        assert_eq!(json["connections"][1]["socket"], "/tmp/redis.sock");
        let (loaded, _) = Store::load();
        assert_eq!(loaded.connections[1].socket, "/tmp/redis.sock");
        assert!(loaded.connections[0].socket.is_empty());
    }

    #[test]
    fn tilde_expands_with_either_separator() {
        let home = dirs::home_dir().expect("a home directory");
        assert_eq!(expand_home("~/certs/ca.pem"), home.join("certs/ca.pem"));
        assert_eq!(expand_home("~\\certs\\ca.pem"), home.join("certs\\ca.pem"));
        // A bare path is left exactly as typed.
        assert_eq!(expand_home("certs/ca.pem"), PathBuf::from("certs/ca.pem"));
    }

    #[test]
    fn duplicating_a_profile_picks_a_free_name() {
        let mut store = Store {
            connections: vec![
                Connection {
                    name: "prod".into(),
                    ..Default::default()
                },
                Connection {
                    name: "prod copy".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let idx = store.duplicate(0).unwrap();
        assert_eq!(idx, 1, "the copy sits right after the original");
        assert_eq!(store.connections[1].name, "prod copy 2");
        // Duplicating the copy does not stack suffixes.
        store.duplicate(1).unwrap();
        assert_eq!(store.connections[2].name, "prod copy 3");
    }

    #[test]
    fn reordering_clamps_at_the_ends() {
        let mut store = Store {
            connections: ["a", "b", "c"]
                .iter()
                .map(|n| Connection {
                    name: (*n).into(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        assert_eq!(store.move_by(0, -1), 0, "already at the top");
        assert_eq!(store.move_by(2, 1), 2, "already at the bottom");
        assert_eq!(store.move_by(1, -1), 0);
        let names: Vec<&str> = store.connections.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["b", "a", "c"]);
    }

    #[test]
    fn keychain_profiles_do_not_write_a_password_to_disk() {
        let mut env = ScopedEnv::new();
        let dir = tempdir();
        env.set("REDISCOPE_HOME", &dir);
        let store = Store {
            connections: vec![Connection {
                name: "vault".into(),
                password: "should-not-persist".into(),
                use_keychain: true,
                ..Default::default()
            }],
            ..Default::default()
        };
        store.save().unwrap();
        let text = fs::read_to_string(config_file()).unwrap();
        assert!(!text.contains("should-not-persist"));
        assert!(text.contains("\"use_keychain\": true"));
    }

    fn tempdir() -> PathBuf {
        // A timestamp alone is not unique: two tests can read the same
        // nanosecond and then share - and overwrite - one config file.
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "rediscope-test-{}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn password_placeholder_expands_from_env() {
        let mut env = ScopedEnv::new();
        env.set("REDISCOPE_TEST_PW", "hunter2");
        let c = Connection {
            password: "${REDISCOPE_TEST_PW}".into(),
            ..Default::default()
        };
        assert_eq!(c.expanded_password(), "hunter2");
        let literal = Connection {
            password: "not$aplaceholder".into(),
            ..Default::default()
        };
        assert_eq!(literal.expanded_password(), "not$aplaceholder");
    }

    /// The old behaviour of this path: a file that did not parse was replaced
    /// by an empty store, and the next edit wrote that emptiness back over
    /// every saved profile.
    #[test]
    fn an_unparseable_file_is_kept_and_never_overwritten() {
        let mut env = ScopedEnv::new();
        let dir = tempdir();
        env.set("REDISCOPE_HOME", &dir);
        fs::write(config_file(), "{ this is not json").unwrap();

        let (store, notice) = Store::load();
        assert!(store.connections.is_empty());
        let notice = notice.expect("the user is told");
        assert!(notice.contains("did not parse"), "{notice}");

        let kept: Vec<PathBuf> = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.to_string_lossy().contains(".bad-"))
            .collect();
        assert_eq!(kept.len(), 1, "the original file is still there");
        assert_eq!(fs::read_to_string(&kept[0]).unwrap(), "{ this is not json");
    }

    #[test]
    fn a_store_that_failed_to_load_refuses_to_save() {
        let store = Store {
            read_error: Some("permission denied".into()),
            ..Default::default()
        };
        let err = store.save().unwrap_err().to_string();
        assert!(err.contains("refusing to overwrite"), "{err}");
    }

    #[test]
    fn a_missing_file_still_seeds_a_local_profile() {
        let mut env = ScopedEnv::new();
        let dir = tempdir();
        env.set("REDISCOPE_HOME", &dir);
        let (store, notice) = Store::load();
        assert_eq!(store.connections.len(), 1);
        assert_eq!(store.connections[0].name, "local");
        assert!(notice.is_none(), "a first run is not an error");
    }

    #[test]
    fn an_older_file_without_a_theme_uses_the_classic_theme() {
        let store: Store = serde_json::from_str(r#"{"connections": []}"#).unwrap();
        assert_eq!(store.theme, Theme::Redis);
    }

    #[test]
    fn custom_codecs_survive_a_save_and_are_absent_when_unset() {
        let mut env = ScopedEnv::new();
        let dir = tempdir();
        env.set("REDISCOPE_HOME", &dir);
        // Files that never configured a codec are written without the key.
        Store::default().save().unwrap();
        let text = fs::read_to_string(config_file()).unwrap();
        assert!(!text.contains("codecs"), "{text}");

        let store = Store {
            codecs: vec![crate::codec::CustomCodec {
                name: "proto".into(),
                decode: vec!["protoc".into(), "--decode_raw".into()],
                ..Default::default()
            }],
            ..Default::default()
        };
        store.save().unwrap();
        let (loaded, notice) = Store::load();
        assert!(notice.is_none());
        assert_eq!(loaded.codecs, store.codecs);
    }

    #[test]
    fn the_selected_theme_survives_a_reload() {
        let mut env = ScopedEnv::new();
        let dir = tempdir();
        env.set("REDISCOPE_HOME", &dir);
        let store = Store {
            theme: Theme::Dracula,
            ..Default::default()
        };
        store.save().unwrap();

        let (loaded, notice) = Store::load();
        assert_eq!(loaded.theme, Theme::Dracula);
        assert!(notice.is_none());
    }

    #[test]
    fn saving_keeps_the_previous_file_as_a_backup() {
        let mut env = ScopedEnv::new();
        let dir = tempdir();
        env.set("REDISCOPE_HOME", &dir);
        let first = Store {
            connections: vec![Connection {
                name: "prod".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        first.save().unwrap();
        let second = Store {
            connections: vec![Connection {
                name: "staging".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        second.save().unwrap();

        let (loaded, _) = Store::load();
        assert_eq!(loaded.connections[0].name, "staging");
        let backup = fs::read_to_string(config_file().with_extension("json.bak")).unwrap();
        assert!(backup.contains("prod"), "the previous file is recoverable");
        assert!(
            !config_file().with_extension("json.tmp").exists(),
            "no temp file is left behind"
        );
    }

    /// Round trip of a v0.2.0 file: every field added since must default.
    #[test]
    fn an_older_file_still_loads_every_profile() {
        let mut env = ScopedEnv::new();
        let dir = tempdir();
        env.set("REDISCOPE_HOME", &dir);
        fs::write(
            config_file(),
            r#"{"connections":[{"name":"local","host":"127.0.0.1","port":6379},
                               {"name":"prod","host":"cache","port":6380,"tls":true}]}"#,
        )
        .unwrap();
        let (store, notice) = Store::load();
        assert!(notice.is_none());
        assert_eq!(
            store
                .connections
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            ["local", "prod"]
        );
        assert!(store.connections[1].tls);
        assert!(!store.connections[1].use_keychain);
        // Fields added after this file was written must default to the old
        // behaviour: a plain standalone server with no production locking.
        for c in &store.connections {
            assert_eq!(c.environment, Environment::Development);
            assert_eq!(c.deployment, Deployment::Standalone);
            assert!(c.seeds.is_empty());
            assert!(c.sentinel_master.is_empty());
            assert!(c.group.is_none());
            assert!(c.validate_topology().is_ok());
        }
        // And writing the file back keeps them loadable by this version.
        store.save().unwrap();
        let (reloaded, notice) = Store::load();
        assert!(notice.is_none());
        assert_eq!(reloaded.connections.len(), 2);
        assert_eq!(reloaded.connection_view, ConnectionView::Grouped);
        assert!(reloaded.collapsed_groups.is_empty());
        assert_eq!(
            reloaded.connections[1].environment,
            Environment::Development
        );
    }

    #[test]
    fn an_older_file_without_groups_writes_no_group_keys() {
        let mut env = ScopedEnv::new();
        let dir = tempdir();
        env.set("REDISCOPE_HOME", &dir);
        fs::write(
            config_file(),
            r#"{"theme":"dracula","connections":[{"name":"local","host":"127.0.0.1","port":6379}]}"#,
        )
        .unwrap();
        let (store, notice) = Store::load();
        assert!(notice.is_none());
        assert_eq!(store.connection_view, ConnectionView::Grouped);
        store.save().unwrap();
        let text = fs::read_to_string(config_file()).unwrap();
        for key in [
            "\"group\"",
            "connection_view",
            "collapsed_groups",
            "\"socket\"",
        ] {
            assert!(!text.contains(key), "{key} written: {text}");
        }
    }

    #[test]
    fn group_and_view_survive_a_reload() {
        let mut env = ScopedEnv::new();
        let dir = tempdir();
        env.set("REDISCOPE_HOME", &dir);
        let store = Store {
            connections: vec![
                Connection {
                    name: "checkout-prod".into(),
                    group: Some("checkout".into()),
                    ..Default::default()
                },
                Connection {
                    name: "local".into(),
                    ..Default::default()
                },
            ],
            connection_view: ConnectionView::Flat,
            // "gone" names no profile, so it is dropped on the way out.
            collapsed_groups: vec!["checkout".into(), "gone".into()],
            ..Default::default()
        };
        store.save().unwrap();
        let text = fs::read_to_string(config_file()).unwrap();
        assert!(text.contains("\"connection_view\": \"flat\""), "{text}");

        let (loaded, notice) = Store::load();
        assert!(notice.is_none());
        assert_eq!(loaded.connections[0].group.as_deref(), Some("checkout"));
        assert!(loaded.connections[1].group.is_none());
        assert_eq!(loaded.connection_view, ConnectionView::Flat);
        assert_eq!(loaded.collapsed_groups, ["checkout"]);
    }

    #[test]
    fn a_blank_group_is_no_group() {
        let blank = Connection {
            group: Some("  ".into()),
            ..Default::default()
        };
        assert_eq!(blank.group_name(), None);
        let padded = Connection {
            group: Some(" checkout ".into()),
            ..Default::default()
        };
        assert_eq!(padded.group_name(), Some("checkout"));
    }

    #[test]
    fn move_within_group_skips_other_groups_and_clamps() {
        let conn = |name: &str, group: Option<&str>| Connection {
            name: name.into(),
            group: group.map(str::to_string),
            ..Default::default()
        };
        let mut store = Store {
            connections: vec![
                conn("a1", Some("a")),
                conn("root1", None),
                conn("b1", Some("b")),
                conn("a2", Some("a")),
                conn("root2", None),
            ],
            ..Default::default()
        };
        let names =
            |s: &Store| -> Vec<String> { s.connections.iter().map(|c| c.name.clone()).collect() };
        assert_eq!(store.move_within_group(0, 1), 3, "jumps over other groups");
        assert_eq!(names(&store), ["a2", "root1", "b1", "a1", "root2"]);
        assert_eq!(store.move_within_group(3, 1), 3, "last of its group");
        assert_eq!(store.move_within_group(2, -1), 2, "only member of b");
        assert_eq!(store.move_within_group(4, -1), 1, "ungrouped move together");
        assert_eq!(names(&store), ["a2", "root2", "b1", "a1", "root1"]);
        assert_eq!(store.move_within_group(9, 1), 9, "out of range is a no-op");
    }

    /// AC2, strictly: once a pre-groups file has been written by this
    /// version, loading and saving it again changes not one byte, and no
    /// group-related key appears anywhere, per profile or at the top level.
    #[test]
    fn a_file_without_groups_resaves_byte_for_byte() {
        let mut env = ScopedEnv::new();
        let dir = tempdir();
        env.set("REDISCOPE_HOME", &dir);
        fs::write(
            config_file(),
            r#"{"theme":"nord","connections":[
                {"name":"local","host":"127.0.0.1","port":6379},
                {"name":"prod","host":"cache","port":6380,"environment":"production"}],
                "sessions":{"local":{"db":2}}}"#,
        )
        .unwrap();
        let (store, notice) = Store::load();
        assert!(notice.is_none(), "{notice:?}");
        store.save().unwrap();
        let first = fs::read_to_string(config_file()).unwrap();
        let (again, _) = Store::load();
        again.save().unwrap();
        let second = fs::read_to_string(config_file()).unwrap();
        assert_eq!(first, second);

        let json: serde_json::Value = serde_json::from_str(&first).unwrap();
        let top = json.as_object().unwrap();
        assert!(!top.contains_key("connection_view"), "{first}");
        assert!(!top.contains_key("collapsed_groups"), "{first}");
        for c in json["connections"].as_array().unwrap() {
            assert!(c.get("group").is_none(), "{c}");
        }
    }

    /// Hand-edited files: `"group": null` is no group, and an unused or
    /// padded collapsed entry is handled on save.
    #[test]
    fn hand_edited_group_values_load_sensibly() {
        let mut env = ScopedEnv::new();
        let dir = tempdir();
        env.set("REDISCOPE_HOME", &dir);
        fs::write(
            config_file(),
            r#"{"connections":[
                {"name":"a","group":null},
                {"name":"b","group":"  ops  "},
                {"name":"c","group":""}],
               "connection_view":"grouped",
               "collapsed_groups":["ops","nobody"]}"#,
        )
        .unwrap();
        let (store, notice) = Store::load();
        assert!(notice.is_none(), "{notice:?}");
        assert_eq!(store.connections[0].group, None);
        assert_eq!(store.connections[1].group_name(), Some("ops"));
        assert_eq!(store.connections[2].group_name(), None);
        assert_eq!(store.connection_view, ConnectionView::Grouped);

        store.save().unwrap();
        let text = fs::read_to_string(config_file()).unwrap();
        assert!(
            !text.contains("connection_view"),
            "an explicit default is not re-written: {text}"
        );
        let (reloaded, _) = Store::load();
        assert_eq!(
            reloaded.collapsed_groups,
            ["ops"],
            "the padded group still counts"
        );
    }

    #[test]
    fn move_within_group_with_no_delta_stays_put() {
        let mut store = Store {
            connections: vec![
                Connection {
                    name: "a".into(),
                    group: Some("g".into()),
                    ..Default::default()
                },
                Connection {
                    name: "b".into(),
                    group: Some(" g ".into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        assert_eq!(store.move_within_group(0, 0), 0);
        assert_eq!(store.connections[0].name, "a");
        // A padded name is the same group, so they are neighbours.
        assert_eq!(store.move_within_group(0, 5), 1);
        assert_eq!(store.connections[1].name, "a");
        assert_eq!(Store::default().move_within_group(0, -1), 0, "empty store");
    }

    /// A newer version may add a layout this one does not know, or a hand
    /// edit may mistype one. Either way the profiles must still load.
    #[test]
    fn an_unknown_or_malformed_view_falls_back_to_grouped() {
        for view in [r#""tree""#, "42", "null", r#"{"x":1}"#] {
            let text = format!(r#"{{"connections":[{{"name":"a"}}],"connection_view":{view}}}"#);
            let store: Store =
                serde_json::from_str(&text).unwrap_or_else(|e| panic!("{view}: {e}"));
            assert_eq!(store.connection_view, ConnectionView::Grouped, "{view}");
            assert_eq!(store.connections.len(), 1, "{view}");
        }
        let store: Store = serde_json::from_str(r#"{"connection_view":"flat"}"#).unwrap();
        assert_eq!(store.connection_view, ConnectionView::Flat);
    }

    #[test]
    fn collapsed_group_names_are_trimmed_and_wrong_types_ignored() {
        let store: Store = serde_json::from_str(
            r#"{"connections":[{"name":"a","group":"ops"}],
                "collapsed_groups":[" ops ", "", 7, null, "billing"]}"#,
        )
        .unwrap();
        assert_eq!(store.collapsed_groups, ["ops", "billing"]);
        assert_eq!(store.connections.len(), 1);
        for wrong in [r#""ops""#, "3", "null", r#"{"ops":true}"#] {
            let text = format!(r#"{{"connections":[{{"name":"a"}}],"collapsed_groups":{wrong}}}"#);
            let store: Store =
                serde_json::from_str(&text).unwrap_or_else(|e| panic!("{wrong}: {e}"));
            assert!(store.collapsed_groups.is_empty(), "{wrong}");
            assert_eq!(store.connections.len(), 1, "{wrong}");
        }
    }
}
