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

/// A single saved server profile.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Connection {
    pub name: String,
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
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
}

impl Default for Connection {
    fn default() -> Self {
        Self {
            name: String::new(),
            host: default_host(),
            port: default_port(),
            db: 0,
            username: String::new(),
            password: String::new(),
            tls: false,
            use_keychain: false,
            tls_ca_file: String::new(),
            tls_cert_file: String::new(),
            tls_key_file: String::new(),
            tls_insecure: false,
        }
    }
}

impl Connection {
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

    /// Parse `redis://user:pass@host:port/db` (or `rediss://`) into a profile.
    pub fn from_url(url: &str) -> Result<Self> {
        let info: redis::ConnectionInfo = url
            .parse::<redis::ConnectionInfo>()
            .with_context(|| format!("invalid redis url: {url}"))?;
        let (host, port, tls) = match info.addr() {
            redis::ConnectionAddr::Tcp(h, p) => (h.clone(), p, false),
            redis::ConnectionAddr::TcpTls { host, port, .. } => (host.clone(), port, true),
            redis::ConnectionAddr::Unix(path) => {
                anyhow::bail!("unix sockets are not supported yet: {}", path.display())
            }
            other => anyhow::bail!("unsupported redis address: {other}"),
        };
        let settings = info.redis_settings();
        Ok(Self {
            name: host.clone(),
            host,
            port: *port,
            db: settings.db(),
            username: settings.username().unwrap_or_default().to_string(),
            password: settings.password().unwrap_or_default().to_string(),
            tls,
            ..Default::default()
        })
    }
}

pub fn config_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("REDISCOPE_HOME") {
        return PathBuf::from(dir);
    }
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("rediscope")
}

pub fn config_file() -> PathBuf {
    config_dir().join("connections.json")
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Store {
    /// The UI colour theme. Missing in older files, so those keep the classic
    /// Redis look automatically.
    #[serde(default)]
    pub theme: Theme,
    #[serde(default)]
    pub connections: Vec<Connection>,
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
                        theme: Theme::default(),
                        connections: vec![Connection {
                            name: "local".into(),
                            ..Default::default()
                        }],
                        read_error: None,
                    },
                    None,
                )
            }
            Err(e) => {
                let notice = format!("Cannot read {}: {e}", path.display());
                return (
                    Self {
                        theme: Theme::default(),
                        connections: Vec::new(),
                        read_error: Some(notice.clone()),
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
                            theme: Theme::default(),
                            connections: Vec::new(),
                            read_error: Some(notice.clone()),
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
            read_error: None,
        };
        let text = serde_json::to_string_pretty(&sanitized)?;
        // Write beside the real file and rename over it: a crash or a full disk
        // then leaves the previous profiles intact instead of a half file.
        // A unique scratch name, so two saves in flight at once (two processes,
        // or two threads) cannot rename each other's file away.
        let tmp = path.with_extension(format!("json.tmp-{}", scratch_id()));
        fs::write(&tmp, text).with_context(|| format!("cannot write {}", tmp.display()))?;
        restrict(&tmp, 0o600);
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
}

/// Expand a leading `~` so certificate paths can be written the way users type
/// them into a shell. `~\\` is accepted too, for a Windows shell.
pub fn expand_home(path: &str) -> std::path::PathBuf {
    let trimmed = path.trim();
    let rest = trimmed
        .strip_prefix("~/")
        .or_else(|| trimmed.strip_prefix("~\\"));
    if let Some(rest) = rest {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    }
    PathBuf::from(trimmed)
}

#[cfg(unix)]
fn restrict(path: &std::path::Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    let _ = fs::set_permissions(path, fs::Permissions::from_mode(mode));
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
        let _guard = env_guard();
        let dir = tempdir();
        std::env::set_var("REDISCOPE_HOME", &dir);
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
        std::env::remove_var("REDISCOPE_HOME");
    }

    fn tempdir() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "rediscope-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn password_placeholder_expands_from_env() {
        std::env::set_var("REDISCOPE_TEST_PW", "hunter2");
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
        let _guard = env_guard();
        let dir = tempdir();
        std::env::set_var("REDISCOPE_HOME", &dir);
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
        std::env::remove_var("REDISCOPE_HOME");
    }

    #[test]
    fn a_store_that_failed_to_load_refuses_to_save() {
        let store = Store {
            theme: Theme::default(),
            connections: Vec::new(),
            read_error: Some("permission denied".into()),
        };
        let err = store.save().unwrap_err().to_string();
        assert!(err.contains("refusing to overwrite"), "{err}");
    }

    #[test]
    fn a_missing_file_still_seeds_a_local_profile() {
        let _guard = env_guard();
        let dir = tempdir();
        std::env::set_var("REDISCOPE_HOME", &dir);
        let (store, notice) = Store::load();
        assert_eq!(store.connections.len(), 1);
        assert_eq!(store.connections[0].name, "local");
        assert!(notice.is_none(), "a first run is not an error");
        std::env::remove_var("REDISCOPE_HOME");
    }

    #[test]
    fn an_older_file_without_a_theme_uses_the_classic_theme() {
        let store: Store = serde_json::from_str(r#"{"connections": []}"#).unwrap();
        assert_eq!(store.theme, Theme::Redis);
    }

    #[test]
    fn the_selected_theme_survives_a_reload() {
        let _guard = env_guard();
        let dir = tempdir();
        std::env::set_var("REDISCOPE_HOME", &dir);
        let store = Store {
            theme: Theme::Dracula,
            ..Default::default()
        };
        store.save().unwrap();

        let (loaded, notice) = Store::load();
        assert_eq!(loaded.theme, Theme::Dracula);
        assert!(notice.is_none());
        std::env::remove_var("REDISCOPE_HOME");
    }

    #[test]
    fn saving_keeps_the_previous_file_as_a_backup() {
        let _guard = env_guard();
        let dir = tempdir();
        std::env::set_var("REDISCOPE_HOME", &dir);
        let first = Store {
            theme: Theme::default(),
            connections: vec![Connection {
                name: "prod".into(),
                ..Default::default()
            }],
            read_error: None,
        };
        first.save().unwrap();
        let second = Store {
            theme: Theme::default(),
            connections: vec![Connection {
                name: "staging".into(),
                ..Default::default()
            }],
            read_error: None,
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
        std::env::remove_var("REDISCOPE_HOME");
    }

    /// Round trip of a v0.2.0 file: every field added since must default.
    #[test]
    fn an_older_file_still_loads_every_profile() {
        let _guard = env_guard();
        let dir = tempdir();
        std::env::set_var("REDISCOPE_HOME", &dir);
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
        std::env::remove_var("REDISCOPE_HOME");
    }
}
