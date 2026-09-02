//! Rendering and key-handling smoke tests. These catch layout arithmetic that
//! panics on small terminals and modals that mis-index their fields.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::backend::TestBackend;
use ratatui::Terminal;
use rediscope::app::{App, Msg};
use rediscope::config::{Connection, Store};
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
    press(&mut a, KeyCode::Esc);

    populate(&mut a);
    render_all_sizes(&mut a); // browser with a hash selected

    for opener in [':', 'n', 't', 'a', 'e', 'x', 'R'] {
        press(&mut a, KeyCode::Char(opener));
        render_all_sizes(&mut a);
        press(&mut a, KeyCode::Esc);
    }

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

    for c in "srv".chars() {
        press(&mut a, KeyCode::Char(c));
    }
    press(&mut a, KeyCode::Tab); // host
    press(&mut a, KeyCode::Tab); // port
    for c in "notaport".chars() {
        press(&mut a, KeyCode::Char(c));
    }
    press(&mut a, KeyCode::Enter);
    assert!(a.modal.is_some(), "bad port stays open");

    app_ctrl(&mut a, 'u'); // clear the port field
    for c in "6380".chars() {
        press(&mut a, KeyCode::Char(c));
    }
    press(&mut a, KeyCode::Enter);
    assert!(a.modal.is_none(), "valid form closes");
    let saved = a
        .store
        .connections
        .iter()
        .find(|c| c.name == "srv")
        .unwrap();
    assert_eq!(saved.port, 6380);
}

#[tokio::test]
async fn escape_clears_an_active_search_pattern() {
    let mut a = app();
    populate(&mut a);
    a.pattern = "user:*".into();
    press(&mut a, KeyCode::Esc);
    assert_eq!(a.pattern, "*");
}
