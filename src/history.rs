//! Console command history that survives a restart.

use std::path::Path;

/// How many commands the file keeps. Old enough that `ctrl+r` still finds
/// what you ran yesterday, small enough to load without thinking about it.
const LIMIT: usize = 500;

#[derive(Debug, Default, Clone)]
pub struct History {
    entries: Vec<String>,
}

impl History {
    /// Read the history from the config directory.
    pub fn load() -> Self {
        Self::load_from(&crate::config::history_file())
    }

    /// Write it back. A history that cannot be saved is not worth interrupting
    /// anyone over, so the caller is free to ignore the error.
    pub fn save(&self) -> std::io::Result<()> {
        self.save_to(&crate::config::history_file())
    }

    /// Read the history at `path`. A missing or unreadable file is simply an
    /// empty history: losing it is a nuisance, never an error worth reporting.
    pub fn load_from(path: &Path) -> Self {
        let text = std::fs::read_to_string(path).unwrap_or_default();
        let mut entries: Vec<String> = text
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(str::to_string)
            .collect();
        let excess = entries.len().saturating_sub(LIMIT);
        entries.drain(..excess);
        Self { entries }
    }

    pub fn entries(&self) -> &[String] {
        &self.entries
    }

    /// Record `line`, unless it carries a secret or repeats the line before it.
    pub fn push(&mut self, line: &str) {
        let line = line.trim();
        if line.is_empty() || carries_a_secret(line) {
            return;
        }
        if self.entries.last().is_some_and(|prev| prev == line) {
            return;
        }
        self.entries.push(line.to_string());
        let excess = self.entries.len().saturating_sub(LIMIT);
        self.entries.drain(..excess);
    }

    pub fn save_to(&self, path: &Path) -> std::io::Result<()> {
        let mut text = self.entries.join("\n");
        text.push('\n');
        crate::config::write_private(path, &text)
    }
}

/// Commands whose arguments are credentials. These are typed into the console
/// like any other command and must not be left behind in a file.
fn carries_a_secret(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    let mut words = lower.split_whitespace();
    match words.next() {
        Some("auth") => true,
        // `HELLO 3 AUTH user pass`, and `MIGRATE ... AUTH pass`.
        Some(_) => words.any(|w| w == "auth" || w == "auth2" || w == "requirepass"),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_password_typed_into_the_console_never_reaches_the_file() {
        let mut h = History::default();
        h.push("GET user:1");
        h.push("AUTH hunter2");
        h.push("auth default hunter2");
        h.push("CONFIG SET requirepass hunter2");
        h.push("HELLO 3 AUTH default hunter2");
        assert_eq!(h.entries(), ["GET user:1"]);
    }

    #[test]
    fn repeating_a_command_does_not_repeat_it_in_the_history() {
        let mut h = History::default();
        h.push("PING");
        h.push("PING");
        h.push("DBSIZE");
        h.push("PING");
        assert_eq!(h.entries(), ["PING", "DBSIZE", "PING"]);
    }

    #[test]
    fn the_oldest_entries_fall_off_the_end() {
        let mut h = History::default();
        for i in 0..(LIMIT + 10) {
            h.push(&format!("GET key:{i}"));
        }
        assert_eq!(h.entries().len(), LIMIT);
        assert_eq!(h.entries()[0], format!("GET key:{}", 10));
    }

    #[test]
    fn a_saved_history_comes_back_in_order() {
        let dir = std::env::temp_dir().join(format!("rediscope-hist-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("history");
        let mut h = History::default();
        h.push("PING");
        h.push("GET user:1");
        h.save_to(&path).unwrap();

        let back = History::load_from(&path);
        assert_eq!(back.entries(), ["PING", "GET user:1"]);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn a_missing_file_is_an_empty_history_not_an_error() {
        let h = History::load_from(std::path::Path::new("/nowhere/rediscope/history"));
        assert!(h.entries().is_empty());
    }
}
