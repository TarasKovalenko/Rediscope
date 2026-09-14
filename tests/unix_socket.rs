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
    let child = match launch(&dir, &socket) {
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
    wait_ready(&server.socket).await;
    Some(server)
}

/// A redis-server on `socket` only, keeping its files in `dir`.
fn launch(dir: &std::path::Path, socket: &str) -> std::io::Result<Child> {
    Command::new("redis-server")
        .args([
            "--port",
            "0",
            "--unixsocket",
            socket,
            "--unixsocketperm",
            "700",
        ])
        .args(["--save", "", "--appendonly", "no", "--dir"])
        .arg(dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
}

async fn wait_ready(socket: &str) {
    for _ in 0..200 {
        if let Ok(client) = redis::Client::open(format!("unix://{socket}"))
            && let Ok(mut c) = client.get_multiplexed_async_connection().await
            && redis::cmd("PING")
                .query_async::<String>(&mut c)
                .await
                .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("redis-server did not open {socket}")
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

// ---- more of the browser, the monitor and the command line over a socket ----

use crossterm::event::KeyModifiers;
use rediscope::app::{Modal, PubSubState};
use rediscope::tree::SortMode;

type Rx = tokio::sync::mpsc::UnboundedReceiver<Msg>;

fn browser_app(store: Store) -> (App, Rx) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    (App::new(store, tx), rx)
}

/// Feed messages to the app until `done` holds. The deadline only stops a
/// hung test; nothing asserts how long it took.
async fn pump(app: &mut App, rx: &mut Rx, what: &str, done: impl Fn(&App) -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while !done(app) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "waiting for {what}: {}",
            app.status
        );
        if let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            app.on_msg(msg);
        }
    }
}

async fn open_browser(app: &mut App, rx: &mut Rx, conn: Connection) {
    app.connect(conn);
    pump(app, rx, "the browser", |a| {
        !a.connecting && a.screen == Screen::Browser && !a.loading && a.client.is_some()
    })
    .await;
}

fn key_names(app: &App) -> Vec<String> {
    app.keys.iter().map(|k| k.name.clone()).collect()
}

fn title(app: &mut App) -> String {
    let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(160, 20)).unwrap();
    terminal.draw(|f| rediscope::ui::draw(f, app)).unwrap();
    (0..160)
        .map(|x| terminal.backend().buffer()[(x, 0)].symbol().to_string())
        .collect()
}

fn press(app: &mut App, code: KeyCode) {
    app.on_key(KeyEvent::new(code, KeyModifiers::NONE));
}

fn ctrl(app: &mut App, c: char) {
    app.on_key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL));
}

/// Ctrl+D, then the database index typed into the form.
fn switch_db(app: &mut App, db: i64) {
    ctrl(app, 'd');
    let Some(Modal::Form { fields, .. }) = &mut app.modal else {
        panic!("no database form")
    };
    fields[0].input.set(&db.to_string());
    press(app, KeyCode::Enter);
}

#[tokio::test]
async fn the_client_reconnects_after_the_server_restarts() {
    let Some(mut server) = server().await else {
        return;
    };
    let Some(dir) = server.dir.clone() else {
        eprintln!("skipped: cannot restart a server this suite did not start");
        return;
    };
    let _serial = SERIAL.lock().await;
    let client = Client::connect(profile(&server, 3)).await.expect("connect");
    client.set_string("restart:before", "x").await.unwrap();

    let child = server.child.as_mut().unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    // Whatever the client says while the server is gone, it must not hang
    // for good or panic.
    let _ = tokio::time::timeout(Duration::from_secs(5), client.execute_raw("PING")).await;

    server.child = Some(launch(&dir, &server.socket).expect("restart redis-server"));
    wait_ready(&server.socket).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        match client.execute_raw("PING").await {
            Ok(reply) => {
                assert_eq!(reply, "PONG");
                break;
            }
            Err(e) => assert!(
                tokio::time::Instant::now() < deadline,
                "never reconnected: {e}"
            ),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // A new server without persistence: the key is gone, and the database the
    // profile names is selected again on the new connection.
    client.set_string("restart:after", "y").await.unwrap();
    assert_eq!(client.dbsize().await.unwrap(), 1);
    let db3 = Client::connect(profile(&server, 3)).await.unwrap();
    assert_eq!(db3.execute_raw("GET restart:after").await.unwrap(), "y");
}

#[tokio::test]
async fn ctrl_d_switches_the_database_of_a_socket_profile() {
    let Some(server) = server().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let setup = Client::connect(profile(&server, 1)).await.unwrap();
    setup.execute_raw("FLUSHALL").await.unwrap();
    setup.set_string("in:one", "1").await.unwrap();
    let two = Client::connect(profile(&server, 2)).await.unwrap();
    two.set_string("in:two", "2").await.unwrap();

    let (mut app, mut rx) = browser_app(Store::default());
    open_browser(&mut app, &mut rx, profile(&server, 1)).await;
    assert_eq!(key_names(&app), ["in:one"]);

    switch_db(&mut app, 2);
    pump(&mut app, &mut rx, "db2", |a| {
        !a.connecting && a.client.as_ref().is_some_and(|c| c.conn.db == 2) && !a.loading
    })
    .await;
    assert_eq!(key_names(&app), ["in:two"]);
    let conn = &app.client.as_ref().unwrap().conn;
    assert_eq!(conn.socket, server.socket, "still the socket");
    let title = title(&mut app);
    assert!(
        title.contains(&format!("unix://{}?db=2", server.socket)),
        "{title}"
    );

    // Writes go to the database switched to.
    app.client
        .as_ref()
        .unwrap()
        .set_string("in:two:more", "3")
        .await
        .unwrap();
    assert_eq!(two.dbsize().await.unwrap(), 2);
    assert_eq!(setup.dbsize().await.unwrap(), 1);
    setup.execute_raw("FLUSHALL").await.unwrap();
}

/// `o` is a view preference for the profile; switching the database on the
/// same profile should not quietly put the tree back in name order.
#[tokio::test]
async fn the_sort_order_survives_a_database_switch() {
    let Some(server) = server().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let setup = Client::connect(profile(&server, 4)).await.unwrap();
    setup.execute_raw("FLUSHALL").await.unwrap();
    setup.set_string("k", "v").await.unwrap();

    let (mut app, mut rx) = browser_app(Store::default());
    open_browser(&mut app, &mut rx, profile(&server, 4)).await;
    press(&mut app, KeyCode::Char('o'));
    assert_eq!(app.sort, SortMode::Ttl);

    switch_db(&mut app, 5);
    pump(&mut app, &mut rx, "db5", |a| {
        !a.connecting && a.client.as_ref().is_some_and(|c| c.conn.db == 5) && !a.loading
    })
    .await;
    assert_eq!(app.sort, SortMode::Ttl, "status: {}", app.status);
    setup.execute_raw("FLUSHALL").await.unwrap();
}

/// Wait for the reconnect to `db` and the key listing that follows it.
async fn pump_db(app: &mut App, rx: &mut Rx, db: i64) {
    pump(app, rx, &format!("db{db}"), |a| {
        !a.connecting && a.client.as_ref().is_some_and(|c| c.conn.db == db) && !a.loading
    })
    .await;
}

fn sorted_key_names(app: &App) -> Vec<String> {
    let mut names = key_names(app);
    names.sort();
    names
}

/// The session is kept per profile, not per database: the sort, the search
/// pattern and the open folders go along to each database switched to, and
/// every switch writes the view it leaves.
#[tokio::test]
async fn ctrl_d_there_and_back_keeps_the_sort_pattern_and_folders() {
    let Some(server) = server().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let eleven = Client::connect(profile(&server, 11)).await.unwrap();
    let twelve = Client::connect(profile(&server, 12)).await.unwrap();
    eleven.execute_raw("FLUSHDB").await.unwrap();
    twelve.execute_raw("FLUSHDB").await.unwrap();
    for key in ["a:1", "a:2", "b:1"] {
        eleven.set_string(key, "x").await.unwrap();
    }
    for key in ["a:9", "c:1"] {
        twelve.set_string(key, "x").await.unwrap();
    }

    let (mut app, mut rx) = browser_app(Store {
        connections: vec![profile(&server, 11)],
        ..Default::default()
    });
    open_browser(&mut app, &mut rx, profile(&server, 11)).await;
    assert_eq!(sorted_key_names(&app), ["a:1", "a:2", "b:1"]);

    press(&mut app, KeyCode::Char('/'));
    for c in "a*".chars() {
        press(&mut app, KeyCode::Char(c));
    }
    press(&mut app, KeyCode::Enter);
    pump(&mut app, &mut rx, "the search", |a| {
        !a.loading && a.pattern == "a*" && a.keys.len() == 2
    })
    .await;
    press(&mut app, KeyCode::Char('o'));
    press(&mut app, KeyCode::Char('o'));
    assert_eq!(app.sort, SortMode::Type);
    assert!(app.expanded.contains("a"), "{:?}", app.expanded);

    switch_db(&mut app, 12);
    pump_db(&mut app, &mut rx, 12).await;
    assert_eq!(app.sort, SortMode::Type, "status: {}", app.status);
    assert_eq!(app.pattern, "a*", "the pattern is the profile's");
    assert_eq!(sorted_key_names(&app), ["a:9"]);
    assert!(app.expanded.contains("a"), "{:?}", app.expanded);
    let saved = &app.store.sessions["socket"];
    assert_eq!(
        (saved.db, saved.pattern.as_str(), saved.sort),
        (11, "a*", SortMode::Type),
        "the view left behind was saved"
    );
    let text = std::fs::read_to_string(rediscope::config::config_file()).unwrap();
    assert!(text.contains(r#""sort": "type""#), "{text}");

    // Change the order here, then go back.
    press(&mut app, KeyCode::Char('o'));
    assert_eq!(app.sort, SortMode::Name);
    switch_db(&mut app, 11);
    pump_db(&mut app, &mut rx, 11).await;
    assert_eq!(
        app.sort,
        SortMode::Name,
        "the order set in db12 comes along"
    );
    assert_eq!(app.pattern, "a*");
    assert_eq!(sorted_key_names(&app), ["a:1", "a:2"]);
    let saved = &app.store.sessions["socket"];
    assert_eq!((saved.db, saved.sort), (12, SortMode::Name));
    let text = std::fs::read_to_string(rediscope::config::config_file()).unwrap();
    assert!(
        !text.contains(r#""sort""#),
        "name order is the default and is not written: {text}"
    );

    press(&mut app, KeyCode::Char('o'));
    switch_db(&mut app, 12);
    pump_db(&mut app, &mut rx, 12).await;
    assert_eq!(app.sort, SortMode::Ttl);
    eleven.execute_raw("FLUSHDB").await.unwrap();
    twelve.execute_raw("FLUSHDB").await.unwrap();
}

/// A connections.json that could not be read is left alone: switching the
/// database still saves the session in memory, so the order survives, but
/// nothing is written over the file.
#[tokio::test]
async fn ctrl_d_on_a_store_that_failed_to_load_does_not_write_the_file() {
    let Some(server) = server().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let setup = Client::connect(profile(&server, 11)).await.unwrap();
    setup.execute_raw("FLUSHDB").await.unwrap();
    setup.set_string("k:1", "x").await.unwrap();

    let path = rediscope::config::config_file();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let original = "{ not json, and the user's profiles are in here }";
    std::fs::write(&path, original).unwrap();

    let (mut app, mut rx) = browser_app(Store {
        connections: vec![profile(&server, 11)],
        read_error: Some("permission denied".into()),
        ..Default::default()
    });
    open_browser(&mut app, &mut rx, profile(&server, 11)).await;
    press(&mut app, KeyCode::Char('o'));
    assert_eq!(app.sort, SortMode::Ttl);

    switch_db(&mut app, 12);
    pump_db(&mut app, &mut rx, 12).await;
    assert_eq!(std::fs::read_to_string(&path).unwrap(), original);
    assert_eq!(app.sort, SortMode::Ttl, "kept in memory: {}", app.status);
    assert_eq!(app.store.sessions["socket"].db, 11);

    switch_db(&mut app, 11);
    pump_db(&mut app, &mut rx, 11).await;
    assert_eq!(key_names(&app), ["k:1"]);
    ctrl(&mut app, 'n'); // back to the server list, which saves again
    assert_eq!(std::fs::read_to_string(&path).unwrap(), original);
    let _ = std::fs::remove_file(&path);
    setup.execute_raw("FLUSHDB").await.unwrap();
}

#[tokio::test]
async fn t_probes_a_socket_profile_from_the_server_list() {
    let Some(server) = server().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let setup = Client::connect(profile(&server, 6)).await.unwrap();
    setup.execute_raw("FLUSHDB").await.unwrap();
    setup.set_string("probe:1", "x").await.unwrap();

    let (mut app, mut rx) = browser_app(Store {
        connections: vec![profile(&server, 6)],
        ..Default::default()
    });
    app.conn_state.select(Some(0));
    press(&mut app, KeyCode::Char('T'));
    assert_eq!(app.status, format!("Testing {} ...", server.socket));
    pump(&mut app, &mut rx, "the probe", |a| a.testing.is_none()).await;
    assert!(app.status.starts_with("socket: PONG in "), "{}", app.status);
    assert!(app.status.contains("1 key(s) in db"), "{}", app.status);
    setup.execute_raw("FLUSHDB").await.unwrap();
}

#[tokio::test]
async fn the_sort_and_folders_are_saved_and_a_changed_separator_reopens_cleanly() {
    let Some(server) = server().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let setup = Client::connect(profile(&server, 7)).await.unwrap();
    setup.execute_raw("FLUSHDB").await.unwrap();
    for key in ["app/user/1", "app/user/2", "app/queue", "legacy:a:b"] {
        setup.set_string(key, "x").await.unwrap();
    }
    let slashes = Connection {
        separator: "/".into(),
        ..profile(&server, 7)
    };
    let (mut app, mut rx) = browser_app(Store {
        connections: vec![slashes.clone()],
        ..Default::default()
    });
    open_browser(&mut app, &mut rx, slashes.clone()).await;
    assert_eq!(app.separator, "/");
    assert!(app.expanded.contains("app/user"), "{:?}", app.expanded);
    press(&mut app, KeyCode::Char('o'));
    press(&mut app, KeyCode::Char('o'));
    assert_eq!(app.sort, SortMode::Type);
    ctrl(&mut app, 'n'); // back to the server list, which saves the session

    let text = std::fs::read_to_string(rediscope::config::config_file()).unwrap();
    assert!(text.contains(r#""sort": "type""#), "{text}");
    assert!(text.contains(r#""separator": "/""#), "{text}");
    let session = &app.store.sessions["socket"];
    assert!(session.expanded.contains(&"app/user".to_string()));

    // Reopening restores the order.
    open_browser(&mut app, &mut rx, slashes.clone()).await;
    assert_eq!(app.sort, SortMode::Type);
    ctrl(&mut app, 'n');

    // Now the profile splits on `:`; the saved folders name paths that no
    // longer exist.
    let colons = Connection {
        separator: ":".into(),
        ..slashes
    };
    app.store.connections = vec![colons.clone()];
    open_browser(&mut app, &mut rx, colons).await;
    assert_eq!(app.separator, ":");
    assert_eq!(app.sort, SortMode::Type);
    let labels: Vec<&str> = app.rows.iter().map(|r| r.label.as_str()).collect();
    assert!(labels.contains(&"app/user/1"), "{labels:?}");
    assert!(labels.contains(&"legacy"), "{labels:?}");
    for (w, h) in [(120, 40), (40, 12), (10, 5)] {
        let mut terminal =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(w, h)).unwrap();
        terminal.draw(|f| rediscope::ui::draw(f, &mut app)).unwrap();
    }
    press(&mut app, KeyCode::Char('q'));
    setup.execute_raw("FLUSHDB").await.unwrap();
}

fn monitor_state(app: &App) -> &PubSubState {
    match &app.modal {
        Some(Modal::PubSub(state)) => state,
        _ => panic!("the monitor is not open: {}", app.status),
    }
}

#[tokio::test]
async fn the_monitor_over_a_socket_tags_databases_and_follows_ctrl_d() {
    let Some(server) = server().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let other = Client::connect(profile(&server, 9)).await.unwrap();
    let (mut app, mut rx) = browser_app(Store::default());
    open_browser(&mut app, &mut rx, profile(&server, 8)).await;

    press(&mut app, KeyCode::Char('W'));
    assert_eq!(monitor_state(&app).current_db, 8);
    // Keep issuing commands until the monitor has seen one from each database;
    // MONITOR may attach a moment after it was asked for.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mine = app.client.clone().unwrap();
    loop {
        mine.execute_raw("GET mon:eight").await.unwrap();
        other.execute_raw("GET mon:nine").await.unwrap();
        if let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            app.on_msg(msg);
        }
        let state = monitor_state(&app);
        let seen = |key: &str| state.messages.iter().any(|m| m.payload.contains(key));
        if seen("mon:eight") && seen("mon:nine") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "monitor saw nothing"
        );
    }
    let state = monitor_state(&app);
    let nine = state
        .messages
        .iter()
        .find(|m| m.payload.contains("mon:nine"))
        .unwrap();
    assert_eq!(nine.db, Some(9));
    assert!(
        nine.payload.contains(&format!("unix:{}", server.socket)),
        "the client is the socket: {}",
        nine.payload
    );

    press(&mut app, KeyCode::Char('d'));
    let state = monitor_state(&app);
    assert_eq!(state.db_filter, Some(8));
    assert!(state.shown().iter().all(|m| m.db == Some(8)));
    assert!(!state.shown().is_empty());
    press(&mut app, KeyCode::Char('d'));
    assert_eq!(monitor_state(&app).db_filter, Some(9));
    press(&mut app, KeyCode::Esc);
    assert!(app.modal.is_none());

    // After switching database the monitor offers the new one first.
    switch_db(&mut app, 10);
    pump(&mut app, &mut rx, "db10", |a| {
        !a.connecting && a.client.as_ref().is_some_and(|c| c.conn.db == 10) && !a.loading
    })
    .await;
    press(&mut app, KeyCode::Char('W'));
    assert_eq!(monitor_state(&app).current_db, 10);
    press(&mut app, KeyCode::Char('d'));
    assert_eq!(monitor_state(&app).db_filter, Some(10));
    assert!(
        app.status.contains("the database this profile has open"),
        "{}",
        app.status
    );
    press(&mut app, KeyCode::Esc);
}

// ---- the command line ----------------------------------------------------------

fn cli(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rediscope"))
        .args(args)
        .env_remove("REDISCOPE_PASSWORD")
        .output()
        .unwrap()
}

fn ok(out: &std::process::Output) -> String {
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[tokio::test]
async fn headless_commands_work_through_a_socket() {
    let Some(server) = server().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let source = Client::connect(profile(&server, 15)).await.unwrap();
    let target = Client::connect(profile(&server, 14)).await.unwrap();
    source.execute_raw("FLUSHDB").await.unwrap();
    target.execute_raw("FLUSHDB").await.unwrap();
    source.set_string("cli:a", "1").await.unwrap();
    source.execute_raw("HSET cli:h f v").await.unwrap();
    source.execute_raw("EXPIRE cli:h 1000").await.unwrap();

    let sock = server.socket.as_str();
    let listed: serde_json::Value =
        serde_json::from_str(&ok(&cli(&["--socket", sock, "-n", "15", "keys", "--json"]))).unwrap();
    let mut names: Vec<&str> = listed
        .as_array()
        .unwrap()
        .iter()
        .map(|k| k["key"].as_str().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, ["cli:a", "cli:h"]);

    let url = format!("unix://{sock}?db=15");
    let text = ok(&cli(&["--url", &url, "keys"]));
    assert!(text.contains("cli:a") && text.contains("cli:h"), "{text}");

    let info = ok(&cli(&["-s", sock, "info"]));
    assert!(info.contains("redis_version:"), "{info}");

    let export =
        std::env::temp_dir().join(format!("rediscope-sock-export-{}.json", std::process::id()));
    let export_path = export.to_str().unwrap();
    ok(&cli(&[
        "-s",
        sock,
        "-n",
        "15",
        "export",
        "--out",
        export_path,
    ]));

    // A read-only session refuses the import and writes nothing.
    let refused = cli(&[
        "-s",
        sock,
        "-n",
        "14",
        "--read-only",
        "import",
        "--file",
        export_path,
    ]);
    assert!(!refused.status.success());
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("read-only"),
        "{}",
        String::from_utf8_lossy(&refused.stderr)
    );
    assert_eq!(target.dbsize().await.unwrap(), 0);

    ok(&cli(&[
        "-s",
        sock,
        "-n",
        "14",
        "import",
        "--file",
        export_path,
    ]));
    assert_eq!(target.execute_raw("GET cli:a").await.unwrap(), "1");
    assert_eq!(target.execute_raw("HGET cli:h f").await.unwrap(), "v");
    let ttl = target.execute_raw("TTL cli:h").await.unwrap();
    let seconds: i64 = ttl
        .trim_start_matches("(integer)")
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("TTL reply {ttl:?}"));
    assert!(seconds > 0, "the TTL came along: {ttl}");
    let _ = std::fs::remove_file(&export);

    // A socket path relative to the working directory.
    if let Some(dir) = &server.dir {
        let out = Command::new(env!("CARGO_BIN_EXE_rediscope"))
            .args(["--socket", "redis.sock", "-n", "15", "keys"])
            .current_dir(dir)
            .output()
            .unwrap();
        let text = ok(&out);
        assert!(text.contains("cli:a"), "{text}");
    }
    source.execute_raw("FLUSHDB").await.unwrap();
    target.execute_raw("FLUSHDB").await.unwrap();
}

#[tokio::test]
async fn mem_report_groups_by_the_saved_profile_separator() {
    let Some(server) = server().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let setup = Client::connect(profile(&server, 13)).await.unwrap();
    setup.execute_raw("FLUSHDB").await.unwrap();
    for key in ["svc/a/1", "svc/a/2", "svc/b/1", "other:x:y"] {
        setup.set_string(key, "some value").await.unwrap();
    }
    Store {
        connections: vec![Connection {
            name: "slashes".into(),
            separator: "/".into(),
            ..profile(&server, 13)
        }],
        ..Default::default()
    }
    .save()
    .unwrap();

    let report = |depth: &str| -> Vec<(String, u64)> {
        let out = ok(&cli(&[
            "--profile",
            "slashes",
            "mem-report",
            "--depth",
            depth,
            "--json",
        ]));
        let json: serde_json::Value = serde_json::from_str(&out).unwrap();
        let mut rows: Vec<(String, u64)> = json["prefixes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| {
                (
                    p["prefix"].as_str().unwrap().to_string(),
                    p["keys"].as_u64().unwrap(),
                )
            })
            .collect();
        rows.sort();
        rows
    };
    assert_eq!(
        report("1"),
        [("other:x:y".to_string(), 1), ("svc/".to_string(), 3)]
    );
    assert_eq!(
        report("2"),
        [
            ("other:x:y".to_string(), 1),
            ("svc/a/".to_string(), 2),
            ("svc/b/".to_string(), 1)
        ]
    );
    setup.execute_raw("FLUSHDB").await.unwrap();
}
