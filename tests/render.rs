//! Rendering and key-handling smoke tests. These catch layout arithmetic that
//! panics on small terminals and modals that mis-index their fields.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::backend::TestBackend;
use ratatui::Terminal;
use rediscope::app::{App, Msg};
use rediscope::config::{Connection, Store};
use rediscope::redis_client::ServerInfo;
use rediscope::redis_client::{KeyInfo, KeyType, KeyValue, Row};
use rediscope::theme::Theme;
use rediscope::ui;

/// Point the config at a scratch directory. Several of these tests add,
/// reorder or delete profiles, which writes `connections.json` — without this
/// they would overwrite the real one belonging to whoever runs the suite.
fn isolate_config() {
    use std::sync::OnceLock;
    static HOME: OnceLock<std::path::PathBuf> = OnceLock::new();
    let dir = HOME.get_or_init(|| {
        let p = std::env::temp_dir().join(format!("rediscope-render-{}", std::process::id()));
        std::fs::create_dir_all(&p).unwrap();
        p
    });
    std::env::set_var("REDISCOPE_HOME", dir);
}

fn app() -> App {
    isolate_config();
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
        ..Default::default()
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

fn render_text(app: &mut App, w: u16, h: u16) -> String {
    let mut terminal = Terminal::new(TestBackend::new(w, h)).unwrap();
    terminal.draw(|f| ui::draw(f, app)).unwrap();
    let buffer = terminal.backend().buffer();
    (0..h)
        .map(|y| {
            let mut line = String::new();
            for x in 0..w {
                line.push_str(buffer[(x, y)].symbol());
            }
            line
        })
        .collect::<Vec<_>>()
        .join("\n")
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

    // Every built-in theme previews without breaking any supported layout.
    press(&mut a, KeyCode::Char('p'));
    for theme in Theme::ALL {
        assert_eq!(a.store.theme, theme);
        render_all_sizes(&mut a);
        press(&mut a, KeyCode::Down);
    }
    press(&mut a, KeyCode::Esc);
    assert_eq!(a.store.theme, Theme::Redis, "cancel restores the old theme");

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
        value: KeyValue::Str("{\"a\":[1,2,3],\"b\":null}".into()),
    });
    render_all_sizes(&mut a); // coloured JSON
    press(&mut a, KeyCode::Char('e')); // JSON editor, opened pretty-printed
    render_all_sizes(&mut a);
    type_str(&mut a, "{"); // break it, then fail a save to draw the error line
    app_ctrl(&mut a, 's');
    render_all_sizes(&mut a);
    app_ctrl(&mut a, 'f'); // reformat also reports the error
    render_all_sizes(&mut a);
    press(&mut a, KeyCode::Esc);

    // A plain string keeps the old editor, with no JSON footer.
    a.on_msg(Msg::Value {
        info: key("app:user:1", KeyType::String, -1),
        value: KeyValue::Str("hello".into()),
    });
    press(&mut a, KeyCode::Char('e'));
    render_all_sizes(&mut a);
    press(&mut a, KeyCode::Esc);

    // Keys expiring out from under the tree must not break the layout.
    a.age_ttls(4_000);
    render_all_sizes(&mut a);
}

#[tokio::test]
async fn previews_structured_list_values_and_handles_long_editor_titles() {
    let mut a = app();
    a.screen = rediscope::app::Screen::Browser;
    a.focus = rediscope::app::Focus::Value;
    a.on_msg(Msg::Value {
        info: key("medinsight:data-protection:keys", KeyType::List, -1),
        value: KeyValue::Rows {
            headers: vec!["index", "value"],
            rows: vec![Row {
                id: "0".into(),
                cells: vec![
                    "0".into(),
                    "<key id=\"abc\"><creationDate>2026-09-04</creationDate><descriptor><masterKey requiresEncryption=\"true\"><value>protected</value></masterKey></descriptor></key>".into(),
                ],
            }],
            total: 1,
        },
    });

    let screen = render_text(&mut a, 100, 30);
    assert!(screen.contains("Selected XML"), "{screen}");
    assert!(
        screen.contains("<creationDate>2026-09-04</creationDate>"),
        "{screen}"
    );
    press(&mut a, KeyCode::PageDown);
    assert_eq!(a.value_scroll, 10, "page keys scroll the preview");
    press(&mut a, KeyCode::Char('j'));
    assert_eq!(a.value_scroll, 0, "changing rows resets preview scroll");

    let long_name =
        "medinsight:data-protection:keys:with:a:key:name:that:is:far:longer:than:the:dialog";
    a.on_msg(Msg::Value {
        info: key(long_name, KeyType::String, -1),
        value: KeyValue::Str("{\"enabled\":true}".into()),
    });
    press(&mut a, KeyCode::Char('e'));
    render_all_sizes(&mut a);
    let editor = render_text(&mut a, 80, 24);
    let title_line = editor
        .lines()
        .find(|line| line.contains("Edit JSON"))
        .expect("the edit title is visible");
    assert!(!title_line.contains("ctrl+s"), "{title_line}");
    let controls_line = editor
        .lines()
        .find(|line| line.contains("ctrl+s saves"))
        .expect("editor controls are visible in the footer");
    assert!(controls_line.contains("ctrl+f formats"), "{controls_line}");
    assert!(controls_line.contains("esc cancels"), "{controls_line}");
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
