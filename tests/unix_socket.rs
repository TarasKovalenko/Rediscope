//! Connecting through a Unix domain socket, against a real redis-server.
//!
//! Skipped unless there is a server to talk to. Either point it at a socket
//! that already exists:
//!   REDISCOPE_TEST_SOCKET=/tmp/redis.sock cargo test --test unix_socket
//! or, with the live suites switched on, it starts a disposable redis-server
//! of its own when one is on the PATH:
//!   REDISCOPE_TEST_PORT=7799 cargo test --test unix_socket
#![cfg(unix)]

mod common;

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use crossterm::event::{KeyCode, KeyEvent};
use rediscope::app::{App, Msg, Screen};
use rediscope::config::{Connection, Store};
use rediscope::redis_client::{Client, KeyType};

/// A redis-server listening only on a socket in a scratch directory. Killed,
/// and its directory removed, when dropped.
struct Server {
    child: Option<Child>,
    dir: Option<PathBuf>,
    socket: String,
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(dir) = &self.dir {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

/// The socket to test against, or `None` (after saying why) to skip.
async fn server() -> Option<Server> {
    common::isolate_config();
    if let Ok(socket) = std::env::var("REDISCOPE_TEST_SOCKET") {
        return Some(Server {
            child: None,
            dir: None,
            socket,
        });
    }
    std::env::var("REDISCOPE_TEST_PORT").ok()?;
    // Socket paths are limited to about 100 bytes, so keep this one short.
    // One server per test, so they never share a path.
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let dir = std::env::temp_dir().join(format!(
        "rediscope-sock-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let socket = dir.join("redis.sock").display().to_string();
    let child = match Command::new("redis-server")
        .args([
            "--port",
            "0",
            "--unixsocket",
            &socket,
            "--unixsocketperm",
            "700",
        ])
        .args(["--save", "", "--appendonly", "no", "--dir"])
        .arg(&dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => {
            eprintln!("skipped: cannot start redis-server ({e})");
            let _ = std::fs::remove_dir_all(&dir);
            return None;
        }
    };
    let server = Server {
        child: Some(child),
        dir: Some(dir),
        socket,
    };
    for _ in 0..200 {
        if let Ok(client) = redis::Client::open(format!("unix://{}", server.socket))
            && let Ok(mut c) = client.get_multiplexed_async_connection().await
            && redis::cmd("PING")
                .query_async::<String>(&mut c)
                .await
                .is_ok()
        {
            return Some(server);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("redis-server did not open {}", server.socket)
}

fn profile(server: &Server, db: i64) -> Connection {
    Connection {
        name: "socket".into(),
        socket: server.socket.clone(),
        db,
        ..Default::default()
    }
}

/// A socket given in REDISCOPE_TEST_SOCKET is shared, and the tests flush
/// their databases on it.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn reads_and_writes_through_the_socket() {
    let Some(server) = server().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let client = Client::connect(profile(&server, 11))
        .await
        .expect("connect");
    client.execute_raw("FLUSHDB").await.unwrap();
    client
        .create_key("sock:hello", KeyType::String)
        .await
        .unwrap();
    client.set_string("sock:hello", "world").await.unwrap();
    assert_eq!(client.execute_raw("GET sock:hello").await.unwrap(), "world");
    let (keys, truncated) = client.scan_keys("sock:*", 100).await.unwrap();
    assert!(!truncated);
    assert_eq!(keys.len(), 1);
    assert_eq!(client.dbsize().await.unwrap(), 1);

    // The database index is honoured: another index sees nothing.
    let other = Client::connect(profile(&server, 12))
        .await
        .expect("connect");
    other.execute_raw("FLUSHDB").await.unwrap();
    assert_eq!(other.dbsize().await.unwrap(), 0);
    client.execute_raw("FLUSHDB").await.unwrap();
}

#[tokio::test]
async fn a_socket_url_connects_to_its_database() {
    let Some(server) = server().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let setup = Client::connect(profile(&server, 13))
        .await
        .expect("connect");
    setup.execute_raw("FLUSHDB").await.unwrap();
    setup.set_string("sock:url", "db13").await.unwrap();

    for scheme in ["unix", "redis+unix"] {
        let conn = Connection::from_url(&format!("{scheme}://{}?db=13", server.socket)).unwrap();
        assert_eq!(conn.socket, server.socket);
        let client = Client::connect(conn).await.expect("connect by url");
        assert_eq!(client.execute_raw("GET sock:url").await.unwrap(), "db13");
    }
    setup.execute_raw("FLUSHDB").await.unwrap();
}

#[tokio::test]
async fn the_browser_opens_a_socket_profile_and_names_the_path() {
    let Some(server) = server().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let setup = Client::connect(profile(&server, 14))
        .await
        .expect("connect");
    setup.execute_raw("FLUSHDB").await.unwrap();
    setup.set_string("app:sock:1", "x").await.unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(Store::default(), tx);
    app.connect(profile(&server, 14));
    assert!(app.status.contains(&server.socket), "{}", app.status);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while app.rows.is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no keys: {}",
            app.status
        );
        if let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            app.on_msg(msg);
        }
    }
    assert!(app.screen == Screen::Browser);
    assert_eq!(app.key_count, 1);

    let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(140, 20)).unwrap();
    terminal.draw(|f| rediscope::ui::draw(f, &mut app)).unwrap();
    let title: String = (0..140)
        .map(|x| terminal.backend().buffer()[(x, 0)].symbol().to_string())
        .collect();
    assert!(
        title.contains(&format!("unix://{}?db=14", server.socket)),
        "{title}"
    );
    app.on_key(KeyEvent::from(KeyCode::Char('q')));
    setup.execute_raw("FLUSHDB").await.unwrap();
}

#[tokio::test]
async fn a_missing_socket_names_the_path_it_tried() {
    common::isolate_config();
    let conn = Connection {
        socket: "/nonexistent/rediscope-test.sock".into(),
        ..Default::default()
    };
    let err = Client::connect(conn).await.unwrap_err().to_string();
    assert!(err.contains("/nonexistent/rediscope-test.sock"), "{err}");
}
