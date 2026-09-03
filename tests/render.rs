//! Rendering and key-handling smoke tests. These catch layout arithmetic that
//! panics on small terminals and modals that mis-index their fields.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::backend::TestBackend;
use ratatui::Terminal;
use rediscope::app::{App, Msg};
use rediscope::config::{Connection, Store};
use rediscope::redis_client::ServerInfo;
use rediscope::redis_client::{KeyInfo, KeyType, KeyValue, Row};
use rediscope::ui;

fn app() -> App {
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let store = Store {
        connections: vec![
            Connection {
                name: "local".into(),
                ..Default::default()
            },
            Connection {
                name: "prod".into(),
                host: "cache.internal".into(),
                tls: true,
                ..Default::default()
            },
        ],
    };
    App::new(store, tx)
}

fn key(name: &str, kind: KeyType, ttl: i64) -> KeyInfo {
    KeyInfo {
        name: name.into(),
        kind,
        ttl,
    }
}

fn populate(app: &mut App) {
    app.screen = rediscope::app::Screen::Browser;
    app.on_msg(Msg::Keys {
        keys: vec![
            key("app:user:1", KeyType::String, -1),
            key("app:user:2", KeyType::Hash, 3600),
            key("app:queue", KeyType::List, -1),
            key("flat", KeyType::Set, 45),
        ],
        truncated: false,
        dbsize: 4,
        pattern: "*".into(),
    });
    app.on_msg(Msg::Value {
        info: key("app:user:2", KeyType::Hash, 3600),
        value: KeyValue::Rows {
            headers: vec!["field", "value"],
            rows: vec![
                Row {
                    id: "name".into(),
                    cells: vec!["name".into(), "ada".into()],
                },
                Row {
                    id: "bio".into(),
                    cells: vec!["bio".into(), "multi\nline\tvalue".into()],
                },
            ],
            total: 2,
        },
    });
}

fn render_at(app: &mut App, w: u16, h: u16) {
    let mut terminal = Terminal::new(TestBackend::new(w, h)).unwrap();
    terminal.draw(|f| ui::draw(f, app)).unwrap();
}

fn render_all_sizes(app: &mut App) {
    for (w, h) in [(120, 40), (80, 24), (40, 12), (20, 8), (10, 5)] {
        render_at(app, w, h);
    }
}

fn press(app: &mut App, code: KeyCode) {
    app.on_key(KeyEvent::new(code, KeyModifiers::NONE));
}

fn type_str(app: &mut App, text: &str) {
    for c in text.chars() {
        press(app, KeyCode::Char(c));
    }
}

fn app_ctrl(app: &mut App, c: char) {
    app.on_key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL));
}

#[tokio::test]
async fn renders_every_screen_and_modal_at_any_size() {
    let mut a = app();
    render_all_sizes(&mut a); // connection list

    press(&mut a, KeyCode::Char('?')); // help
    render_all_sizes(&mut a);
    press(&mut a, KeyCode::Esc);

    press(&mut a, KeyCode::Char('n')); // connection form
    render_all_sizes(&mut a);
    // Walk the whole form so every field, heading and scroll position renders.
    for _ in 0..13 {
        press(&mut a, KeyCode::Tab);
        render_at(&mut a, 80, 24);
        render_at(&mut a, 40, 12);
    }
    press(&mut a, KeyCode::Esc);

    press(&mut a, KeyCode::Char('/')); // server-list filter
    press(&mut a, KeyCode::Char('p'));
    render_all_sizes(&mut a);
    press(&mut a, KeyCode::Esc);

    populate(&mut a);
    render_all_sizes(&mut a); // browser with a hash selected

    for opener in [':', 'n', 't', 'a', 'e', 'x', 'R'] {
        press(&mut a, KeyCode::Char(opener));
        render_all_sizes(&mut a);
        press(&mut a, KeyCode::Esc);
    }

    // Server info: every tab, at every size, plus scrolling past the end.
    a.on_msg(Msg::Info(Box::new(Ok(ServerInfo::parse(
        "# Server\nredis_version:7.2.4\nredis_mode:standalone\n\n# Memory\nused_memory_human:1.20M\n\n# Stats\nkeyspace_hits:9\nkeyspace_misses:1\n\n# Keyspace\ndb0:keys=4,expires=1,avg_ttl=0\n",
    )))));
    for _ in 0..rediscope::app::INFO_TABS.len() {
        render_all_sizes(&mut a);
        press(&mut a, KeyCode::Char('G'));
        render_all_sizes(&mut a);
        press(&mut a, KeyCode::Tab);
    }
    press(&mut a, KeyCode::Char('/')); // filter inside server info
    type_str(&mut a, "mem");
    render_all_sizes(&mut a);
    press(&mut a, KeyCode::Enter);
    render_all_sizes(&mut a);
    press(&mut a, KeyCode::Esc); // clears the filter, keeps the modal
    render_all_sizes(&mut a);
    press(&mut a, KeyCode::Esc);

    press(&mut a, KeyCode::Char('/')); // search line
    press(&mut a, KeyCode::Char('u'));
    render_all_sizes(&mut a);
    press(&mut a, KeyCode::Esc);

    // A string value renders through a different path than the table.
    a.on_msg(Msg::Value {
        info: key("app:user:1", KeyType::String, -1),
        value: KeyValue::Str("{\"a\":[1,2,3]}".into()),
    });
    render_all_sizes(&mut a);
    press(&mut a, KeyCode::Char('e')); // multi-line editor
    render_all_sizes(&mut a);
}

#[tokio::test]
async fn tree_navigation_expands_folders_and_tracks_selection() {
    let mut a = app();
    populate(&mut a);
    // Small result sets auto-expand, so every key is visible.
    assert_eq!(a.rows.len(), 6, "2 folders + 4 keys");

    // Folders sort before the leaves at the same depth.
    let labels: Vec<&str> = a.rows.iter().map(|r| r.label.as_str()).collect();
    assert_eq!(labels, ["app", "user", "1", "2", "queue", "flat"]);

    press(&mut a, KeyCode::Char('j'));
    press(&mut a, KeyCode::Char('j'));
    assert_eq!(
        a.selected_row().unwrap().key.as_ref().unwrap().name,
        "app:user:1"
    );

    // Collapse `app:user`, and its two children disappear.
    press(&mut a, KeyCode::Char('k'));
    press(&mut a, KeyCode::Char('h'));
    assert_eq!(a.rows.len(), 4);
    press(&mut a, KeyCode::Char('l'));
    assert_eq!(a.rows.len(), 6);

    press(&mut a, KeyCode::Char('G'));
    assert_eq!(a.selected_row().unwrap().label, "flat");
    press(&mut a, KeyCode::Char('g'));
    assert_eq!(a.selected_row().unwrap().label, "app");
    assert!(a.selected_row().unwrap().folder_path.is_some());
}

#[tokio::test]
async fn form_validation_blocks_submit_and_keeps_the_modal_open() {
    let mut a = app();
    press(&mut a, KeyCode::Char('n')); // new connection form
    press(&mut a, KeyCode::Enter); // name is empty
    assert!(a.modal.is_some(), "invalid form stays open");

    type_str(&mut a, "srv"); // focus starts on Name, not the section heading
    press(&mut a, KeyCode::Tab); // host
    press(&mut a, KeyCode::Tab); // port
    type_str(&mut a, "notaport");
    press(&mut a, KeyCode::Enter);
    assert!(a.modal.is_some(), "bad port stays open");

    app_ctrl(&mut a, 'u'); // clear the port field
    type_str(&mut a, "6380");
    press(&mut a, KeyCode::Enter);
    assert!(a.modal.is_none(), "valid form closes");
    let saved = a
        .store
        .connections
        .iter()
        .find(|c| c.name == "srv")
        .unwrap();
    assert_eq!(saved.port, 6380);
    assert_eq!(
        saved.host, "127.0.0.1",
        "an empty host falls back to loopback"
    );
}

/// Section headings carry no value, so every field after one would land in the
/// wrong slot if the form and the save action disagreed about indices.
#[tokio::test]
async fn connection_form_writes_every_field_to_the_right_slot() {
    let mut a = app();
    press(&mut a, KeyCode::Char('n'));

    let inputs = [
        "edge",           // Name
        "cache.example",  // Host
        "6380",           // Port
        "3",              // Database
        "reader",         // Username
        "s3cret",         // Password
        "",               // keychain switch, left off
        "",               // TLS switch, toggled below
        "~/certs/ca.pem", // CA certificate
        "",               // client certificate
        "",               // client key
        "",               // skip verification
    ];
    for (i, value) in inputs.iter().enumerate() {
        if i == 7 {
            press(&mut a, KeyCode::Char(' ')); // switch TLS on
        } else if !value.is_empty() {
            app_ctrl(&mut a, 'u');
            type_str(&mut a, value);
        }
        if i + 1 < inputs.len() {
            press(&mut a, KeyCode::Tab);
        }
    }
    press(&mut a, KeyCode::Enter);
    assert!(a.modal.is_none(), "form should have been accepted");

    let c = a
        .store
        .connections
        .iter()
        .find(|c| c.name == "edge")
        .unwrap();
    assert_eq!(c.host, "cache.example");
    assert_eq!(c.port, 6380);
    assert_eq!(c.db, 3);
    assert_eq!(c.username, "reader");
    assert_eq!(c.password, "s3cret");
    assert!(!c.use_keychain);
    assert!(c.tls);
    assert_eq!(c.tls_ca_file, "~/certs/ca.pem");
    assert!(c.tls_cert_file.is_empty());
    assert!(!c.tls_insecure);
}

#[tokio::test]
async fn certificate_files_require_tls() {
    let mut a = app();
    press(&mut a, KeyCode::Char('n'));
    type_str(&mut a, "certs-only");
    for _ in 0..8 {
        press(&mut a, KeyCode::Tab); // walk to the CA certificate field
    }
    type_str(&mut a, "/tmp/ca.pem");
    press(&mut a, KeyCode::Enter);
    assert!(a.modal.is_some(), "certificates without TLS are rejected");

    press(&mut a, KeyCode::BackTab); // back to the TLS switch
    press(&mut a, KeyCode::Char(' '));
    press(&mut a, KeyCode::Enter);
    assert!(a.modal.is_none());
    let c = a
        .store
        .connections
        .iter()
        .find(|c| c.name == "certs-only")
        .unwrap();
    assert!(c.tls);
    assert_eq!(c.tls_ca_file, "/tmp/ca.pem");
}

#[tokio::test]
async fn duplicate_reorder_and_filter_the_server_list() {
    let mut a = app();
    let names =
        |a: &App| -> Vec<String> { a.store.connections.iter().map(|c| c.name.clone()).collect() };

    press(&mut a, KeyCode::Char('c')); // duplicate "local"
    assert_eq!(names(&a), ["local", "local copy", "prod"]);
    assert_eq!(
        a.conn_state.selected(),
        Some(1),
        "the cursor follows the copy"
    );

    press(&mut a, KeyCode::Char('J')); // move it down
    assert_eq!(names(&a), ["local", "prod", "local copy"]);
    assert_eq!(a.conn_state.selected(), Some(2));
    press(&mut a, KeyCode::Char('J')); // already last, nothing moves
    assert_eq!(names(&a), ["local", "prod", "local copy"]);
    press(&mut a, KeyCode::Char('K'));
    assert_eq!(names(&a), ["local", "local copy", "prod"]);

    press(&mut a, KeyCode::Char('/'));
    type_str(&mut a, "prod");
    press(&mut a, KeyCode::Enter);
    assert_eq!(a.visible_connections().len(), 1);
    render_all_sizes(&mut a);

    // Reordering a filtered list would rewrite an order the user cannot see.
    press(&mut a, KeyCode::Char('J'));
    assert_eq!(names(&a), ["local", "local copy", "prod"]);
    assert!(a.status.contains("filter"));

    press(&mut a, KeyCode::Esc); // clears the filter, does not quit
    assert!(!a.should_quit);
    assert_eq!(a.visible_connections().len(), 3);
    press(&mut a, KeyCode::Esc); // nothing left to clear, so this quits
    assert!(a.should_quit);
}

#[tokio::test]
async fn escape_clears_an_active_search_pattern() {
    let mut a = app();
    populate(&mut a);
    a.pattern = "user:*".into();
    press(&mut a, KeyCode::Esc);
    assert_eq!(a.pattern, "*");
}
