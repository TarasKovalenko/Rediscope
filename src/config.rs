//! Saved connection profiles, persisted as JSON under the user's config dir.

use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

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
        }
    }
}

impl Connection {
    /// Expand `${VAR}` or `$VAR` password placeholders from the environment.
    /// A password that is not a placeholder is returned unchanged.
    pub fn resolved_password(&self) -> String {
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
    #[serde(default)]
    pub connections: Vec<Connection>,
}

impl Store {
    /// Load saved profiles. A missing file yields a single `local` profile; a
    /// corrupt file yields an empty store rather than killing the app.
    pub fn load() -> Self {
        let path = config_file();
        let Ok(text) = fs::read_to_string(&path) else {
            return Self {
                connections: vec![Connection {
                    name: "local".into(),
                    ..Default::default()
                }],
            };
        };
        serde_json::from_str(&text).unwrap_or_default()
    }

    pub fn save(&self) -> Result<()> {
        let dir = config_dir();
        fs::create_dir_all(&dir).with_context(|| format!("cannot create {}", dir.display()))?;
        restrict(&dir, 0o700);
        let path = config_file();
        let text = serde_json::to_string_pretty(self)?;
        fs::write(&path, text).with_context(|| format!("cannot write {}", path.display()))?;
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
}

#[cfg(unix)]
fn restrict(path: &std::path::Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    let _ = fs::set_permissions(path, fs::Permissions::from_mode(mode));
}

#[cfg(not(unix))]
fn restrict(_path: &std::path::Path, _mode: u32) {}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn password_placeholder_expands_from_env() {
        std::env::set_var("REDISCOPE_TEST_PW", "hunter2");
        let c = Connection {
            password: "${REDISCOPE_TEST_PW}".into(),
            ..Default::default()
        };
        assert_eq!(c.resolved_password(), "hunter2");
        let literal = Connection {
            password: "not$aplaceholder".into(),
            ..Default::default()
        };
        assert_eq!(literal.resolved_password(), "not$aplaceholder");
    }
}
