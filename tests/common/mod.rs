//! Shared setup for the integration binaries.

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
