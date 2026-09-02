//! Optional OS keychain storage for connection passwords.
//!
//! Every call degrades gracefully: a machine with no keychain daemon (a headless
//! Linux box, a container) gets a clear error instead of a panic, and profiles
//! that do not opt in never touch this module.

use anyhow::{anyhow, Result};

const SERVICE: &str = "rediscope";

/// Whether a credential store is usable on this machine. Cheap enough to call
/// while drawing a form.
pub fn available() -> bool {
    keyring::Entry::store_status().is_ok()
}

/// Why the keychain is unusable, for display in the UI.
pub fn unavailable_reason() -> Option<String> {
    match keyring::Entry::store_status() {
        Ok(()) => None,
        Err(e) => Some(e.to_string()),
    }
}

fn entry(profile: &str) -> Result<keyring::Entry> {
    keyring::Entry::new(SERVICE, profile).map_err(|e| anyhow!("keychain unavailable: {e}"))
}

pub fn get(profile: &str) -> Result<String> {
    entry(profile)?.get_password().map_err(|e| match e {
        keyring::Error::NoEntry => anyhow!(
            "no keychain entry for '{profile}' — edit the connection and set the password again"
        ),
        other => anyhow!("could not read the keychain: {other}"),
    })
}

pub fn set(profile: &str, password: &str) -> Result<()> {
    entry(profile)?
        .set_password(password)
        .map_err(|e| anyhow!("could not write to the keychain: {e}"))
}

/// Removing a credential that was never stored is not an error.
pub fn delete(profile: &str) -> Result<()> {
    match entry(profile)?.delete_credential() {
        Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
        Err(e) => Err(anyhow!("could not remove the keychain entry: {e}")),
    }
}
