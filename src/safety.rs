//! Session-only production write leases. Redis ACLs remain the authorization boundary.
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::config::{Connection, Deployment, Environment};
use anyhow::{Result, ensure};

pub const WRITE_LEASE: Duration = Duration::from_secs(300);

#[derive(Clone, Default)]
pub(crate) struct Safety {
    until: Arc<Mutex<Option<Instant>>>,
}
impl Safety {
    pub fn remaining(&self) -> u64 {
        self.until
            .lock()
            .unwrap()
            .and_then(|t| t.checked_duration_since(Instant::now()))
            .map(|d| d.as_secs() + u64::from(d.subsec_nanos() != 0))
            .unwrap_or(0)
    }
    pub fn read_only(&self, p: &Connection) -> bool {
        p.read_only
            || p.deployment != Deployment::Standalone
            || (p.environment == Environment::Production && self.remaining() == 0)
    }
    pub fn unlock(&self, p: &Connection, confirmation: &str) -> Result<()> {
        ensure!(
            p.environment == Environment::Production,
            "Only production profiles need an unlock"
        );
        ensure!(
            !p.read_only && p.deployment == Deployment::Standalone,
            "Explicit read-only and discovered deployments cannot be unlocked"
        );
        ensure!(
            confirmation == p.name && !confirmation.is_empty(),
            "Type the exact profile name to unlock"
        );
        *self.until.lock().unwrap() = Some(Instant::now() + WRITE_LEASE);
        Ok(())
    }
    pub fn lock(&self) {
        *self.until.lock().unwrap() = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lease_is_shared_expires_and_never_overrides_hard_restrictions() {
        let s = Safety::default();
        let mut p = Connection {
            name: "prod".into(),
            environment: Environment::Production,
            ..Default::default()
        };
        assert!(s.read_only(&p));
        assert!(s.unlock(&p, "wrong").is_err());
        s.unlock(&p, "prod").unwrap();
        assert!(!s.clone().read_only(&p));
        p.read_only = true;
        assert!(s.read_only(&p));
        assert!(s.unlock(&p, "prod").is_err());
        p.read_only = false;
        p.deployment = Deployment::Cluster;
        assert!(s.read_only(&p));
        p.deployment = Deployment::Standalone;
        *s.until.lock().unwrap() = Some(Instant::now() - Duration::from_secs(1));
        assert!(s.read_only(&p));
        s.unlock(&p, "prod").unwrap();
        s.clone().lock();
        assert!(s.read_only(&p));
    }
}
