//! TLS behaviour against real redis-server instances.
//!
//! Skipped unless the ports and certificate directory are set:
//!   REDISCOPE_TLS_PORT=7800 REDISCOPE_MTLS_PORT=7801 \
//!   REDISCOPE_CERTS=/path/to/certs cargo test --test tls

use rediscope::config::Connection;
use rediscope::redis_client::Client;

fn certs() -> Option<String> {
    std::env::var("REDISCOPE_CERTS").ok()
}

fn port(var: &str) -> Option<u16> {
    std::env::var(var).ok()?.parse().ok()
}

fn base(port: u16, db: i64) -> Connection {
    Connection {
        name: "tls-test".into(),
        host: "127.0.0.1".into(),
        port,
        db,
        tls: true,
        ..Default::default()
    }
}

#[tokio::test]
async fn rejects_a_self_signed_server_without_the_ca() {
    let (Some(port), Some(_)) = (port("REDISCOPE_TLS_PORT"), certs()) else {
        return;
    };
    // No CA and no skip-verify: the handshake must fail rather than silently
    // trusting whatever the server presents.
    let err = Client::connect(base(port, 0))
        .await
        .expect_err("an unknown CA must not be trusted");
    let text = err.to_string().to_lowercase();
    assert!(
        text.contains("certificate") || text.contains("tls") || text.contains("unknown"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn connects_when_the_ca_file_is_supplied() {
    let (Some(port), Some(dir)) = (port("REDISCOPE_TLS_PORT"), certs()) else {
        return;
    };
    let conn = Connection {
        tls_ca_file: format!("{dir}/ca.crt"),
        ..base(port, 1)
    };
    let client = Client::connect(conn).await.expect("CA-verified connect");
    client.set_string("tls:hello", "world").await.unwrap();
    assert_eq!(client.execute_raw("GET tls:hello").await.unwrap(), "world");
    client.delete_key("tls:hello").await.unwrap();
}

#[tokio::test]
async fn skip_verify_connects_without_a_ca() {
    let (Some(port), Some(_)) = (port("REDISCOPE_TLS_PORT"), certs()) else {
        return;
    };
    let conn = Connection {
        tls_insecure: true,
        ..base(port, 2)
    };
    let probe = Client::probe(conn).await.expect("insecure connect");
    assert!(!probe.version.is_empty());
    assert!(probe.latency_ms >= 0.0);
}

#[tokio::test]
async fn mutual_tls_needs_the_client_certificate() {
    let (Some(port), Some(dir)) = (port("REDISCOPE_MTLS_PORT"), certs()) else {
        return;
    };
    // The server demands a client certificate, so CA-only must be refused.
    let ca_only = Connection {
        tls_ca_file: format!("{dir}/ca.crt"),
        ..base(port, 3)
    };
    assert!(
        Client::connect(ca_only).await.is_err(),
        "server requires a client certificate"
    );

    let full = Connection {
        tls_ca_file: format!("{dir}/ca.crt"),
        tls_cert_file: format!("{dir}/client.crt"),
        tls_key_file: format!("{dir}/client.key"),
        ..base(port, 3)
    };
    let client = Client::connect(full).await.expect("mTLS connect");
    assert_eq!(client.execute_raw("PING").await.unwrap(), "PONG");
}

#[tokio::test]
async fn a_missing_certificate_file_reports_its_path() {
    let Some(port) = port("REDISCOPE_TLS_PORT") else {
        return;
    };
    let conn = Connection {
        tls_ca_file: "/nope/absent-ca.crt".into(),
        ..base(port, 0)
    };
    let err = Client::connect(conn).await.unwrap_err();
    let text = err.to_string();
    assert!(text.contains("CA certificate"), "unexpected error: {text}");
    assert!(
        text.contains("/nope/absent-ca.crt"),
        "unexpected error: {text}"
    );
}
