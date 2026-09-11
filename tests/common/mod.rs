//! Shared setup for the integration binaries.

// Each test binary compiles this module on its own and uses only part of it.
#![allow(dead_code)]

/// The server the suite is pointed at, from `REDISCOPE_TEST_FLAVOR`.
///
/// The compat job in CI runs the same binaries against Valkey, KeyDB and
/// Dragonfly. A test that touches something one of them does not do asks
/// here and skips just that part.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Flavor {
    Redis,
    Valkey,
    KeyDb,
    Dragonfly,
}

/// Defaults to Redis when the variable is unset. An unknown name fails loudly
/// rather than quietly running the Redis expectations.
pub fn flavor() -> Flavor {
    match std::env::var("REDISCOPE_TEST_FLAVOR")
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "" | "redis" => Flavor::Redis,
        "valkey" => Flavor::Valkey,
        "keydb" => Flavor::KeyDb,
        "dragonfly" => Flavor::Dragonfly,
        other => {
            panic!("REDISCOPE_TEST_FLAVOR={other}: expected redis, valkey, keydb or dragonfly")
        }
    }
}

/// True, after printing why, when the server under test is one of `flavors`.
pub fn skip_on(flavors: &[Flavor], reason: &str) -> bool {
    let current = flavor();
    let skip = flavors.contains(&current);
    if skip {
        eprintln!("skipped on {current:?}: {reason}");
    }
    skip
}

/// Point the config directory and the audit log at a scratch directory.
///
/// Every connection appends to `audit.jsonl` and several suites add, reorder or
/// delete profiles in `connections.json`. Without this they would write into the
/// config directory belonging to whoever runs the suite. An explicit value from
/// the environment (CI sets one) always wins.
pub fn isolate_config() {
    use std::sync::OnceLock;
    static HOME: OnceLock<std::path::PathBuf> = OnceLock::new();
    HOME.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!(
            "rediscope-tests-{}-{}",
            std::process::id(),
            std::env::args()
                .next()
                .unwrap_or_default()
                .replace(|c: char| !c.is_ascii_alphanumeric(), "-")
        ));
        std::fs::create_dir_all(&dir).unwrap();
        // SAFETY: OnceLock runs this exactly once, before any test in this
        // binary has read either variable, and nothing here spawns a thread
        // that reads the environment.
        unsafe {
            if std::env::var_os("REDISCOPE_HOME").is_none() {
                std::env::set_var("REDISCOPE_HOME", &dir);
            }
            if std::env::var_os("REDISCOPE_AUDIT_FILE").is_none() {
                std::env::set_var("REDISCOPE_AUDIT_FILE", dir.join("audit.jsonl"));
            }
        }
        dir
    });
}
