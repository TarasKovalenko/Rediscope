//! Socket profiles, key separators and saved sort orders without a server:
//! how they are read from and written to connections.json, parsed from URLs
//! and flags, and refused where a socket cannot work.
#![cfg(unix)]

mod common;

use std::process::Command;
use std::time::Duration;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use rediscope::app::{App, Modal, Msg, Screen};
use rediscope::config::{Connection, Deployment, Session, Store, config_file};
use rediscope::redis_client::Client;
use rediscope::tree::SortMode;

/// Every test that reads or writes connections.json holds this: the file is
/// one per test binary.
static FILE: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn lock_file() -> std::sync::MutexGuard<'static, ()> {
    common::isolate_config();
    FILE.lock().unwrap_or_else(|e| e.into_inner())
}

fn saved_text(store: &Store) -> String {
    store.save().unwrap();
    std::fs::read_to_string(config_file()).unwrap()
}

// ---- connections.json ---------------------------------------------------------

/// A file written before sockets, separators and sorting existed, in the
/// shape `save` writes it.
fn old_style_store() -> Store {
    let mut store = Store {
        connections: vec![
            Connection {
                name: "local".into(),
                ..Default::default()
            },
            Connection {
                name: "tls".into(),
                host: "cache.example".into(),
                port: 6380,
                tls: true,
                db: 3,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    store.sessions.insert(
        "local".into(),
        Session {
            db: 2,
            pattern: "user:*".into(),
            expanded: vec!["user".into(), "user:1".into()],
            selected_key: "user:1:name".into(),
            ..Default::default()
        },
    );
    store
}

#[test]
fn an_old_file_is_written_back_byte_for_byte() {
    let _file = lock_file();
    let original = saved_text(&old_style_store());
    for new_key in ["\"socket\"", "\"separator\"", "\"sort\""] {
        assert!(!original.contains(new_key), "{new_key} in {original}");
    }
    // Load and save it twice: nothing is added, reordered or reformatted.
    for _ in 0..2 {
        let (store, notice) = Store::load();
        assert!(notice.is_none(), "{notice:?}");
        assert_eq!(store.connections[0].key_separator(), ":");
        assert!(!store.connections[0].uses_socket());
        assert_eq!(store.sessions["local"].sort, SortMode::Name);
        assert_eq!(saved_text(&store), original);
    }
}

#[test]
fn defaults_spelled_out_in_the_file_are_not_written_back() {
    let _file = lock_file();
    let original = saved_text(&old_style_store());
    let mut json: serde_json::Value = serde_json::from_str(&original).unwrap();
    json["connections"][0]["socket"] = "".into();
    json["connections"][0]["separator"] = ":".into();
    json["connections"][1]["separator"] = "".into();
    json["sessions"]["local"]["sort"] = "name".into();
    std::fs::write(config_file(), serde_json::to_string(&json).unwrap()).unwrap();

    let (store, notice) = Store::load();
    assert!(notice.is_none(), "{notice:?}");
    assert_eq!(
        store.connections[1].key_separator(),
        ":",
        "empty reads as :"
    );
    assert_eq!(saved_text(&store), original);
}

#[test]
fn an_unknown_sort_reads_as_name_and_the_rest_of_the_file_survives() {
    let _file = lock_file();
    std::fs::write(
        config_file(),
        r#"{"connections":[{"name":"a"}],"sessions":{"a":{"db":4,"pattern":"p*","sort":"ttl_desc","expanded":["x"]}}}"#,
    )
    .unwrap();
    let (store, notice) = Store::load();
    assert!(notice.is_none(), "{notice:?}");
    let session = &store.sessions["a"];
    assert_eq!(session.sort, SortMode::Name);
    assert_eq!((session.db, session.pattern.as_str()), (4, "p*"));
    assert_eq!(session.expanded, ["x"]);

    for odd in ["null", "[]", "{}", "\"TTL\"", "\"\"", "true", "1.5"] {
        let text = format!(r#"{{"sessions":{{"a":{{"sort":{odd},"db":1}}}}}}"#);
        std::fs::write(config_file(), text).unwrap();
        let (store, notice) = Store::load();
        assert!(notice.is_none(), "{odd}: {notice:?}");
        assert_eq!(store.sessions["a"].sort, SortMode::Name, "{odd}");
        assert_eq!(store.sessions["a"].db, 1, "{odd}");
    }
}

#[test]
fn custom_separators_and_sorts_round_trip_exactly() {
    let _file = lock_file();
    let mut store = old_style_store();
    let separators = ["::", " ", "/", "*", "\\", "→", ": "];
    store.connections = separators
        .iter()
        .enumerate()
        .map(|(i, sep)| Connection {
            name: format!("p{i}"),
            separator: sep.to_string(),
            ..Default::default()
        })
        .collect();
    store.connections.push(Connection {
        name: "sock".into(),
        socket: "/var/run/redis/redis-server.sock".into(),
        db: 9,
        ..Default::default()
    });
    store.sessions.get_mut("local").unwrap().sort = SortMode::Type;
    let first = saved_text(&store);
    assert!(first.contains(r#""sort": "type""#), "{first}");

    let (loaded, _) = Store::load();
    for (conn, sep) in loaded.connections.iter().zip(separators) {
        assert_eq!(conn.separator, sep, "saved untrimmed");
        assert_eq!(conn.key_separator(), sep);
    }
    let sock = loaded.connections.last().unwrap();
    assert_eq!(sock.socket, "/var/run/redis/redis-server.sock");
    assert_eq!(
        sock.address(),
        "unix:///var/run/redis/redis-server.sock?db=9"
    );
    assert_eq!(saved_text(&loaded), first);
}

// ---- URLs -------------------------------------------------------------------------

#[test]
fn socket_urls_carry_database_and_credentials_in_the_query() {
    let c = Connection::from_url("unix:///tmp/r.sock?db=3&user=u&pass=p").unwrap();
    assert_eq!(
        (
            c.socket.as_str(),
            c.db,
            c.username.as_str(),
            c.password.as_str()
        ),
        ("/tmp/r.sock", 3, "u", "p")
    );
    assert_eq!(c.name, "/tmp/r.sock");
    assert!(!c.tls);
    assert_eq!(c.deployment, Deployment::Standalone);
    assert_eq!(c.address(), "unix:///tmp/r.sock?db=3");
    assert_eq!(c.endpoint(), "/tmp/r.sock");
    // Nothing from host and port leaks into the display.
    assert!(!c.address().contains("6379"));

    for url in [
        "redis+unix:///tmp/r.sock?db=3&user=u&pass=p",
        "valkey+unix:///tmp/r.sock?db=3&user=u&pass=p",
    ] {
        let d = Connection::from_url(url).unwrap();
        assert_eq!(
            (d.socket, d.db, d.username, d.password),
            (c.socket.clone(), 3, "u".into(), "p".into()),
            "{url}"
        );
    }
}

#[test]
fn percent_encoded_socket_paths_and_passwords_are_decoded() {
    let c = Connection::from_url("unix:///tmp/my%20dir/r.sock?user=a%2Bb&pass=p%40ss%26word%3D")
        .unwrap();
    assert_eq!(c.socket, "/tmp/my dir/r.sock");
    assert_eq!(c.username, "a+b");
    assert_eq!(c.password, "p@ss&word=");
    assert_eq!(c.endpoint(), "/tmp/my dir/r.sock");
}

#[test]
fn a_socket_url_without_an_absolute_path_is_refused() {
    for url in [
        "unix://tmp/r.sock",
        "unix:tmp/r.sock",
        "unix://",
        "redis+unix://relative",
    ] {
        let err = Connection::from_url(url).unwrap_err().to_string();
        assert!(err.contains(url), "{url}: {err}");
    }
    // localhost as the authority is the same absolute path.
    let c = Connection::from_url("unix://localhost/tmp/r.sock").unwrap();
    assert_eq!(c.socket, "/tmp/r.sock");
}

#[test]
fn a_database_that_is_not_a_number_is_refused_with_the_url() {
    for url in [
        "unix:///tmp/r.sock?db=abc",
        "unix:///tmp/r.sock?db=",
        "unix:///tmp/r.sock?db=1.5",
        "unix:///tmp/r.sock?db=99999999999999999999",
    ] {
        let err = Connection::from_url(url).unwrap_err();
        let text = format!("{err:#}");
        assert!(text.contains("invalid redis url"), "{url}: {text}");
        assert!(text.to_lowercase().contains("database"), "{url}: {text}");
    }
}

#[test]
fn tcp_urls_still_name_the_host() {
    let c = Connection::from_url("redis://u:p@cache.example:6380/2").unwrap();
    assert_eq!((c.name.as_str(), c.port, c.db), ("cache.example", 6380, 2));
    assert!(c.socket.is_empty() && !c.uses_socket());
    assert_eq!(c.address(), "redis://cache.example:6380/2");
    let s = Connection::from_url("rediss://cache.example/0").unwrap();
    assert_eq!(s.address(), "rediss://cache.example:6379/0");
    assert_eq!(s.endpoint(), "cache.example:6379");
}

// ---- refusals -----------------------------------------------------------------

fn socket(path: &str) -> Connection {
    Connection {
        name: "sock".into(),
        socket: path.into(),
        ..Default::default()
    }
}

/// Each setting a socket cannot use, and the words its refusal must contain.
fn refused() -> Vec<(Connection, &'static str)> {
    let base = || socket("/nonexistent/rediscope-refusal.sock");
    vec![
        (
            Connection {
                tls: true,
                ..base()
            },
            "TLS does not apply to a Unix socket",
        ),
        (
            Connection {
                tls: true,
                tls_insecure: true,
                tls_ca_file: "/tmp/ca.pem".into(),
                ..base()
            },
            "TLS does not apply to a Unix socket",
        ),
        (
            Connection {
                ssh_host: "bastion.example".into(),
                ..base()
            },
            "cannot go through an SSH tunnel",
        ),
        (
            Connection {
                deployment: Deployment::Cluster,
                ..base()
            },
            "Cluster and Sentinel need host and port",
        ),
        (
            Connection {
                deployment: Deployment::Sentinel,
                sentinel_master: "mymaster".into(),
                ..base()
            },
            "Cluster and Sentinel need host and port",
        ),
    ]
}

#[tokio::test]
async fn connect_refuses_socket_profiles_that_need_a_network_before_trying() {
    common::isolate_config();
    for (conn, words) in refused() {
        let err = Client::connect(conn.clone()).await.unwrap_err().to_string();
        assert!(err.contains(words), "{words}: {err}");
        // Refused up front, not after failing to open the path.
        assert!(!err.contains("No such file"), "{err}");
        let err = Client::probe(conn).await.unwrap_err().to_string();
        assert!(err.contains(words), "probe: {err}");
    }
}

fn app_with(store: Store) -> (App, tokio::sync::mpsc::UnboundedReceiver<Msg>) {
    common::isolate_config();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    (App::new(store, tx), rx)
}

async fn next_msg(app: &mut App, rx: &mut tokio::sync::mpsc::UnboundedReceiver<Msg>) {
    let msg = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("no message")
        .expect("channel closed");
    app.on_msg(msg);
}

#[tokio::test]
async fn the_browser_reports_a_refused_socket_profile() {
    for (conn, words) in refused() {
        let (mut app, mut rx) = app_with(Store::default());
        app.connect(conn);
        assert!(
            app.status.contains("/nonexistent/rediscope-refusal.sock"),
            "{}",
            app.status
        );
        while app.connecting {
            next_msg(&mut app, &mut rx).await;
        }
        assert!(app.status.contains(words), "{words}: {}", app.status);
        assert!(app.screen == Screen::Connections);
        assert!(app.client.is_none());
    }
}

#[tokio::test]
async fn t_on_a_socket_profile_names_the_path_and_the_failure() {
    let (mut app, mut rx) = app_with(Store {
        connections: vec![socket("/nonexistent/rediscope-probe.sock")],
        ..Default::default()
    });
    app.conn_state.select(Some(0));
    app.on_key(KeyEvent::new(KeyCode::Char('T'), KeyModifiers::NONE));
    assert_eq!(app.status, "Testing /nonexistent/rediscope-probe.sock ...");
    next_msg(&mut app, &mut rx).await;
    assert!(app.status.starts_with("Error: sock: "), "{}", app.status);
    assert!(
        app.status.contains("/nonexistent/rediscope-probe.sock"),
        "{}",
        app.status
    );

    let (mut app, mut rx) = app_with(Store {
        connections: vec![Connection {
            tls: true,
            ..socket("/nonexistent/rediscope-probe.sock")
        }],
        ..Default::default()
    });
    app.conn_state.select(Some(0));
    app.on_key(KeyEvent::new(KeyCode::Char('T'), KeyModifiers::NONE));
    next_msg(&mut app, &mut rx).await;
    assert!(
        app.status.contains("TLS does not apply to a Unix socket"),
        "{}",
        app.status
    );
}

// ---- the connection form ---------------------------------------------------------

mod field {
    pub const NAME: usize = 0;
    pub const HOST: usize = 2;
    pub const SOCKET: usize = 4;
    pub const TLS: usize = 10;
    pub const SSH_HOST: usize = 15;
    pub const DEPLOYMENT: usize = 19;
    pub const SENTINEL_MASTER: usize = 21;
    pub const SEPARATOR: usize = 25;
}

fn with_inputs(app: &mut App, edit: impl FnOnce(&mut Vec<&mut rediscope::app::Field>)) {
    let Some(Modal::Form { fields, .. }) = &mut app.modal else {
        panic!("expected the connection form")
    };
    let mut inputs: Vec<_> = fields.iter_mut().filter(|f| f.is_input()).collect();
    assert_eq!(inputs.len(), 26);
    edit(&mut inputs);
}

fn form_error(app: &App) -> Option<String> {
    match &app.modal {
        Some(Modal::Form { error, .. }) => error.clone(),
        _ => None,
    }
}

fn render(app: &mut App, w: u16, h: u16) -> String {
    let mut terminal = Terminal::new(TestBackend::new(w, h)).unwrap();
    terminal.draw(|f| rediscope::ui::draw(f, app)).unwrap();
    let buffer = terminal.backend().buffer();
    (0..h)
        .map(|y| (0..w).map(|x| buffer[(x, y)].symbol()).collect::<String>())
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_sizes(app: &mut App) {
    for (w, h) in [(160, 50), (120, 40), (80, 24), (40, 12), (20, 8), (10, 5)] {
        render(app, w, h);
    }
}

/// A change to the connection form's inputs.
type FormEdit = Box<dyn Fn(&mut Vec<&mut rediscope::app::Field>)>;

#[test]
fn the_form_refuses_ssh_cluster_and_sentinel_on_a_socket() {
    let _file = lock_file();
    let cases: Vec<(&str, FormEdit, &str)> = vec![
        (
            "ssh",
            Box::new(|i| i[field::SSH_HOST].input.set("bastion")),
            "A Unix socket is local; it cannot go through an SSH tunnel",
        ),
        (
            "cluster",
            Box::new(|i| i[field::DEPLOYMENT].choice = 1),
            "A Unix socket reaches one server; Cluster and Sentinel need host and port",
        ),
        (
            "sentinel",
            Box::new(|i| {
                i[field::DEPLOYMENT].choice = 2;
                i[field::SENTINEL_MASTER].input.set("mymaster");
            }),
            "A Unix socket reaches one server; Cluster and Sentinel need host and port",
        ),
        (
            "tls",
            Box::new(|i| i[field::TLS].flag = true),
            "TLS does not apply to a Unix socket",
        ),
    ];
    for (name, edit, message) in cases {
        let (mut app, _rx) = app_with(Store::default());
        app.on_key(KeyEvent::new(KeyCode::Char('n'), KeyModifiers::NONE));
        with_inputs(&mut app, |i| {
            i[field::NAME].input.set(name);
            i[field::SOCKET].input.set("/tmp/redis.sock");
            edit(i);
        });
        app.on_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert_eq!(form_error(&app).as_deref(), Some(message), "{name}");
        assert!(app.store.connections.is_empty(), "{name} saved anyway");
        render_sizes(&mut app);

        // Clearing the socket makes the same settings acceptable to the form.
        with_inputs(&mut app, |i| i[field::SOCKET].input.set("   "));
        app.on_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        if name != "sentinel" && name != "cluster" {
            assert!(app.modal.is_none(), "{name}: {:?}", form_error(&app));
            assert!(app.store.connections[0].socket.is_empty());
        }
    }
}

#[test]
fn editing_a_socket_profile_shows_its_path_and_the_effective_separator() {
    let _file = lock_file();
    let (mut app, _rx) = app_with(Store {
        connections: vec![Connection {
            separator: String::new(),
            ..socket("/run/redis.sock")
        }],
        ..Default::default()
    });
    app.conn_state.select(Some(0));
    app.on_key(KeyEvent::new(KeyCode::Char('e'), KeyModifiers::NONE));
    with_inputs(&mut app, |i| {
        assert_eq!(i[field::SOCKET].input.value(), "/run/redis.sock");
        assert_eq!(i[field::SEPARATOR].input.value(), ":");
        assert_eq!(i[field::HOST].input.value(), "127.0.0.1");
    });
    render_sizes(&mut app);
    // Back from the first field wraps to the last, and the form follows.
    app.on_key(KeyEvent::new(KeyCode::BackTab, KeyModifiers::SHIFT));
    let screen = render(&mut app, 160, 50);
    assert!(screen.contains("Key separator"), "{screen}");
    render_sizes(&mut app);

    with_inputs(&mut app, |i| i[field::SEPARATOR].input.set("::"));
    app.on_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    assert!(app.modal.is_none(), "{:?}", form_error(&app));
    let saved = &app.store.connections[0];
    assert_eq!(saved.separator, "::");
    assert_eq!(saved.socket, "/run/redis.sock");
    let text = std::fs::read_to_string(config_file()).unwrap();
    assert!(text.contains(r#""separator": "::""#), "{text}");
}

#[test]
fn the_server_list_shows_a_socket_profile_at_every_size() {
    let (mut app, _rx) = app_with(Store {
        connections: vec![
            Connection {
                db: 7,
                ..socket("/var/run/redis/a-rather-long-directory-name/redis-server.sock")
            },
            Connection {
                name: "tcp".into(),
                ..Default::default()
            },
        ],
        ..Default::default()
    });
    let wide = render(&mut app, 160, 20);
    assert!(
        wide.contains("unix:///var/run/redis/a-rather-long-directory-name/redis-server.sock?db=7"),
        "{wide}"
    );
    assert!(wide.contains("redis://127.0.0.1:6379/0"), "{wide}");
    render_sizes(&mut app);
}

// ---- the command line -----------------------------------------------------------

fn rediscope(args: &[&str]) -> std::process::Output {
    common::isolate_config();
    Command::new(env!("CARGO_BIN_EXE_rediscope"))
        .args(args)
        .env_remove("REDISCOPE_PASSWORD")
        .output()
        .unwrap()
}

fn stderr(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn socket_flag_conflicts_with_network_flags() {
    let sock = "/nonexistent/rediscope-cli.sock";
    for extra in [
        vec!["-H", "cache.example"],
        vec!["--host", "cache.example"],
        vec!["--url", "redis://cache.example"],
        vec!["--tls"],
        vec!["--tls-insecure"],
        vec!["--tls-ca", "/tmp/ca.pem"],
        vec!["--ssh", "bastion"],
    ] {
        let mut args = vec!["--socket", sock];
        args.extend(extra.iter().copied());
        args.push("keys");
        let out = rediscope(&args);
        assert_eq!(out.status.code(), Some(2), "{args:?}: {}", stderr(&out));
        assert!(
            stderr(&out).contains("cannot be used with"),
            "{args:?}: {}",
            stderr(&out)
        );
    }
}

#[test]
fn a_missing_socket_fails_the_subcommand_naming_the_path() {
    for args in [
        vec!["-s", "/nonexistent/rediscope-cli.sock", "keys"],
        vec![
            "--socket",
            "/nonexistent/rediscope-cli.sock",
            "-n",
            "3",
            "info",
        ],
        vec![
            "--socket",
            "/nonexistent/rediscope-cli.sock",
            "--read-only",
            "export",
        ],
        vec![
            "--url",
            "unix:///nonexistent/rediscope-cli.sock?db=2",
            "keys",
        ],
    ] {
        let out = rediscope(&args);
        assert!(!out.status.success(), "{args:?}");
        assert!(
            stderr(&out).contains("/nonexistent/rediscope-cli.sock"),
            "{args:?}: {}",
            stderr(&out)
        );
        assert!(!stderr(&out).contains("panicked"), "{}", stderr(&out));
    }
}

#[test]
fn a_socket_url_with_network_flags_is_refused_before_connecting() {
    for (extra, words) in [
        (vec!["--tls"], "TLS does not apply to a Unix socket"),
        (
            vec!["--tls-insecure"],
            "TLS does not apply to a Unix socket",
        ),
        (
            vec!["--ssh", "bastion.invalid"],
            "cannot go through an SSH tunnel",
        ),
    ] {
        let mut args = vec!["--url", "unix:///nonexistent/rediscope-cli.sock"];
        args.extend(extra.iter().copied());
        args.push("keys");
        let out = rediscope(&args);
        assert!(!out.status.success(), "{args:?}");
        assert!(stderr(&out).contains(words), "{args:?}: {}", stderr(&out));
    }
}

#[test]
fn bad_socket_urls_fail_on_the_command_line() {
    for (url, words) in [
        ("unix:///tmp/r.sock?db=abc", "Invalid database number"),
        ("unix://relative/r.sock", "invalid redis url"),
    ] {
        let out = rediscope(&["--url", url, "keys"]);
        assert!(!out.status.success(), "{url}");
        assert!(stderr(&out).contains(words), "{url}: {}", stderr(&out));
    }
}

#[test]
fn the_help_mentions_the_socket_flag_and_the_error_names_it() {
    let out = rediscope(&["--help"]);
    let help = String::from_utf8_lossy(&out.stdout);
    assert!(help.contains("-s, --socket <PATH>"), "{help}");
    assert!(help.contains("unix:///"), "{help}");
    let out = rediscope(&["keys"]);
    assert!(!out.status.success());
    assert!(stderr(&out).contains("--socket"), "{}", stderr(&out));
}

#[test]
fn a_saved_profile_wins_over_socket_flags() {
    let _file = lock_file();
    Store {
        connections: vec![Connection {
            name: "saved".into(),
            socket: "/nonexistent/rediscope-saved.sock".into(),
            ..Default::default()
        }],
        ..Default::default()
    }
    .save()
    .unwrap();
    let out = rediscope(&[
        "--socket",
        "/nonexistent/rediscope-flag.sock",
        "--profile",
        "saved",
        "keys",
    ]);
    assert!(!out.status.success());
    let err = stderr(&out);
    assert!(err.contains("/nonexistent/rediscope-saved.sock"), "{err}");
    assert!(!err.contains("rediscope-flag.sock"), "{err}");
}
