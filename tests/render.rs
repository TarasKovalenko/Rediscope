//! Rendering and key-handling smoke tests. These catch layout arithmetic that
//! panics on small terminals and modals that mis-index their fields.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use rediscope::app::{App, Modal, Msg};
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
    HOME.get_or_init(|| {
        let p = std::env::temp_dir().join(format!("rediscope-render-{}", std::process::id()));
        std::fs::create_dir_all(&p).unwrap();
        // SAFETY: OnceLock runs this exactly once, before any test in this
        // binary has read the variable, and nothing here spawns a thread that
        // reads the environment.
        unsafe { std::env::set_var("REDISCOPE_HOME", &p) };
        p
    });
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
        warnings: vec![],
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
                    decoding: None,
                },
                Row {
                    id: "bio".into(),
                    cells: vec!["bio".into(), "multi\nline\tvalue".into()],
                    decoding: None,
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
    a.on_msg(Msg::Info(Box::new(Ok((
        ServerInfo::parse(
            "# Server\nredis_version:7.2.4\nredis_mode:standalone\n\n# Memory\nused_memory_human:1.20M\n\n# Stats\nkeyspace_hits:9\nkeyspace_misses:1\n\n# Keyspace\ndb0:keys=4,expires=1,avg_ttl=0\n",
        ),
        rediscope::redis_client::Diagnostics::default(),
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
                decoding: None,
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
        "",               // read-only switch, left off
        "reader",         // Username
        "s3cret",         // Password
        "",               // keychain switch, left off
        "",               // TLS switch, toggled below
        "~/certs/ca.pem", // CA certificate
        "",               // client certificate
        "",               // client key
        "",               // skip verification
        "",               // SSH host, left blank
        "",               // SSH user
        "",               // SSH port, keeps its default
        "",               // SSH key
    ];
    for (i, value) in inputs.iter().enumerate() {
        if i == 8 {
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
    assert!(!c.read_only);
    assert!(c.ssh_host.is_empty());
    assert_eq!(c.ssh_port, 22);
}

#[tokio::test]
async fn certificate_files_require_tls() {
    let mut a = app();
    press(&mut a, KeyCode::Char('n'));
    type_str(&mut a, "certs-only");
    for _ in 0..9 {
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

#[tokio::test]
async fn the_console_renders_its_reverse_search_at_every_size() {
    let mut a = app();
    populate(&mut a);
    press(&mut a, KeyCode::Char(':'));
    type_str(&mut a, "GET user:1");
    press(&mut a, KeyCode::Enter);
    app_ctrl(&mut a, 'r');
    type_str(&mut a, "get");
    render_all_sizes(&mut a);
    let screen = render_text(&mut a, 120, 40);
    assert!(screen.contains("reverse-i-search"), "{screen}");
}

#[tokio::test]
async fn the_memory_report_renders_while_it_is_still_scanning() {
    let mut a = app();
    populate(&mut a);
    // The scan itself needs a server; the report only needs its state.
    a.modal = Some(rediscope::app::Modal::Memory(
        rediscope::app::MemoryState::new(4_182_996),
    ));
    render_all_sizes(&mut a);
    let mid = render_text(&mut a, 120, 40);
    assert!(mid.contains("scanning"), "{mid}");

    let mut rollup = rediscope::memory::Rollup::default();
    for i in 0..40 {
        let key = format!("session:web:{i}");
        rollup.count(&key);
        rollup.measure(&key, 2_048);
    }
    a.on_msg(Msg::Memory {
        rollup: Box::new(rollup),
        done: true,
    });
    render_all_sizes(&mut a);
    let done = render_text(&mut a, 120, 40);
    assert!(done.contains("session:"), "{done}");
    assert!(done.contains("KB") || done.contains("MB"), "{done}");

    press(&mut a, KeyCode::Char('2'));
    let deep = render_text(&mut a, 120, 40);
    assert!(deep.contains("session:web:"), "{deep}");
}

#[tokio::test]
async fn the_new_panes_render_at_any_size() {
    let mut a = app();
    populate(&mut a);

    // The pub/sub feed, filled past the height of the smallest terminal.
    a.modal = Some(rediscope::app::Modal::PubSub(
        rediscope::app::PubSubState::new(vec!["news.*".into()], false),
    ));
    for i in 0..30 {
        a.on_msg(Msg::PubSub {
            channel: format!("news.{i}"),
            payload: format!("message {i} with a body long enough to need truncating"),
        });
    }
    render_all_sizes(&mut a);
    press(&mut a, KeyCode::Char('f')); // stop following
    press(&mut a, KeyCode::Char('G'));
    render_all_sizes(&mut a);
    press(&mut a, KeyCode::Char('c')); // clear
    render_all_sizes(&mut a);
    press(&mut a, KeyCode::Esc);
    assert!(a.modal.is_none());

    // Consumer groups, both empty and populated.
    let mut groups = rediscope::app::GroupsState::new("events".into());
    a.modal = Some(rediscope::app::Modal::Groups(
        rediscope::app::GroupsState::new("events".into()),
    ));
    render_all_sizes(&mut a);
    groups.set_groups(vec![rediscope::redis_client::StreamGroup {
        name: "workers".into(),
        consumers: 2,
        pending: 3,
        last_delivered: "1-1".into(),
        lag: "0".into(),
    }]);
    groups.detail = rediscope::redis_client::StreamGroupDetail {
        consumers: vec![rediscope::redis_client::StreamConsumer {
            name: "alice".into(),
            pending: 3,
            idle_ms: 4200,
        }],
        pending: vec![rediscope::redis_client::PendingEntry {
            id: "1-1".into(),
            consumer: "alice".into(),
            idle_ms: 4200,
            deliveries: 2,
        }],
    };
    a.modal = Some(rediscope::app::Modal::Groups(groups));
    render_all_sizes(&mut a);
    press(&mut a, KeyCode::Tab); // into the pending pane
    render_all_sizes(&mut a);
    press(&mut a, KeyCode::Esc);

    // A long reply scrolls inside the message box.
    a.modal = Some(rediscope::app::Modal::Message {
        title: "Result".into(),
        body: (0..60)
            .map(|i| format!("line {i}"))
            .collect::<Vec<_>>()
            .join("\n"),
        scroll: 0,
    });
    render_all_sizes(&mut a);
    press(&mut a, KeyCode::Char('G'));
    render_all_sizes(&mut a);
    press(&mut a, KeyCode::Esc);
    assert!(a.modal.is_none());
}

#[tokio::test]
async fn marking_keys_retargets_the_bulk_actions() {
    let mut a = app();
    populate(&mut a);
    a.focus = rediscope::app::Focus::Tree;

    // Marking the folder marks every key beneath it.
    press(&mut a, KeyCode::Char('g'));
    press(&mut a, KeyCode::Char('m'));
    assert_eq!(a.marked.len(), 3, "app:queue, app:user:1 and app:user:2");
    render_all_sizes(&mut a);

    press(&mut a, KeyCode::Char('D'));
    match &a.modal {
        Some(rediscope::app::Modal::Confirm { message, .. }) => {
            assert!(message.contains("3 marked key(s)"), "{message}");
        }
        _ => panic!("delete should confirm the marked set"),
    }
    press(&mut a, KeyCode::Esc);

    press(&mut a, KeyCode::Char('t'));
    match &a.modal {
        Some(rediscope::app::Modal::Form { title, .. }) => {
            assert!(title.contains("3 marked key(s)"), "{title}");
        }
        _ => panic!("ttl should target the marked set"),
    }
    press(&mut a, KeyCode::Esc);

    // Unmarking returns the actions to the selected key.
    press(&mut a, KeyCode::Char('u'));
    assert!(a.marked.is_empty());
    press(&mut a, KeyCode::Char('m'));
    assert_eq!(a.marked.len(), 3, "the folder toggles as a whole");
    press(&mut a, KeyCode::Char('m'));
    assert!(a.marked.is_empty(), "and toggles back off");
}

#[tokio::test]
async fn the_info_modal_reaches_the_diagnostics_tabs() {
    let mut a = app();
    populate(&mut a);
    let diag = rediscope::redis_client::Diagnostics {
        config: vec![
            ("maxmemory".into(), "0".into()),
            ("appendonly".into(), "no".into()),
        ],
        clients: vec![rediscope::redis_client::ClientEntry {
            id: "17".into(),
            addr: "127.0.0.1:6379".into(),
            db: "0".into(),
            command: "client|list".into(),
            ..Default::default()
        }],
        latency: vec![("ping (5 samples)".into(), "min 0.10 ms".into())],
        ..Default::default()
    };
    a.on_msg(Msg::Info(Box::new(Ok((
        ServerInfo::parse("# Server\nredis_version:7.2.4\n"),
        diag,
    )))));

    for _ in 0..rediscope::app::INFO_TABS.len() {
        render_all_sizes(&mut a);
        press(&mut a, KeyCode::Tab);
    }

    // The Config tab offers the selected parameter for editing.
    press(&mut a, KeyCode::Char('7'));
    press(&mut a, KeyCode::Char('j'));
    render_all_sizes(&mut a);
    press(&mut a, KeyCode::Char('e'));
    match &a.modal {
        Some(rediscope::app::Modal::Form { title, .. }) => {
            assert!(title.starts_with("CONFIG SET"), "{title}");
        }
        _ => panic!("e on the config tab edits the parameter"),
    }
}

#[tokio::test]
async fn sentinel_form_persists_discovery_fields() {
    use rediscope::app::Modal;
    let mut a = app();
    press(&mut a, KeyCode::Char('n'));
    let Some(Modal::Form { fields, .. }) = &mut a.modal else {
        panic!("expected form")
    };
    let mut inputs: Vec<_> = fields.iter_mut().filter(|f| f.is_input()).collect();
    inputs[0].input.set("sentinel-profile");
    inputs[2].input.set("26379");
    inputs[17].choice = 2;
    inputs[18].input.set("[::1]:26380,redis-b:26379");
    inputs[19].input.set("primary-service");
    inputs[20].input.set("sentinel-reader");
    inputs[21].input.set("${SENTINEL_PASSWORD}");
    press(&mut a, KeyCode::Enter);
    assert!(a.modal.is_none());
    let saved = a
        .store
        .connections
        .iter()
        .find(|c| c.name == "sentinel-profile")
        .unwrap();
    assert_eq!(saved.deployment, rediscope::config::Deployment::Sentinel);
    assert_eq!(saved.seeds, vec!["[::1]:26380", "redis-b:26379"]);
    assert_eq!(saved.sentinel_master, "primary-service");
    assert_eq!(saved.sentinel_username, "sentinel-reader");
    assert_eq!(saved.sentinel_password, "${SENTINEL_PASSWORD}");
}

#[tokio::test]
async fn partial_coverage_survives_status_changes_and_clears_on_complete_refresh() {
    let mut a = app();
    populate(&mut a);
    a.on_msg(Msg::Keys {
        keys: vec![],
        truncated: false,
        warnings: vec!["node unavailable".into()],
        dbsize: 0,
        pattern: "*".into(),
    });
    a.status = "another action".into();
    let mut terminal = Terminal::new(TestBackend::new(180, 30)).unwrap();
    terminal.draw(|f| ui::draw(f, &mut a)).unwrap();
    let text: String = terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect();
    assert!(text.contains("PARTIAL RESULTS"));
    assert!(!text.contains("No keys match"));
    a.on_msg(Msg::Keys {
        keys: vec![],
        truncated: false,
        warnings: vec![],
        dbsize: 0,
        pattern: "*".into(),
    });
    assert!(a.coverage_warnings.is_empty());
}

#[test]
fn conflict_preview_is_readable_and_survives_small_terminals() {
    use rediscope::redis_client::EditTarget;
    let mut app = app();
    populate(&mut app);
    app.modal = Some(Modal::EditConflict {
        target: EditTarget {
            key: "customer:1".into(),
            kind: KeyType::String,
            selector: String::new(),
            original: "original value".into(),
            decoded: None,
        },
        values: vec!["my unsaved draft".into()],
        current: Some("concurrent writer".into()),
        error: None,
    });
    let text = render_text(&mut app, 120, 40);
    for expected in [
        "ORIGINAL",
        "CURRENT",
        "YOUR DRAFT",
        "original value",
        "concurrent writer",
        "my unsaved draft",
    ] {
        assert!(text.contains(expected), "missing {expected}");
    }
    render_all_sizes(&mut app);
}

#[tokio::test]
async fn decoded_values_and_the_view_picker_render_at_any_size() {
    use rediscope::codec::{Builtin, Codec, Decoding};
    let mut a = app();
    populate(&mut a);
    let gzip = Decoding {
        codec: Codec::Builtin(Builtin::Gzip),
        raw: vec![0x1f, 0x8b, 0x08, 0x00],
        read_only: None,
    };
    a.on_msg(Msg::Value {
        info: key("app:user:1", KeyType::String, -1),
        value: KeyValue::Decoded {
            text: "{\"plan\":\"pro\"}".into(),
            decoding: gzip.clone(),
        },
    });
    render_all_sizes(&mut a);
    let screen = render_text(&mut a, 120, 40);
    assert!(screen.contains("gzip"), "the codec is named in the header");
    assert!(screen.contains("json"), "the decoded text is still JSON");
    assert!(screen.contains("\"plan\""), "the decoded text is shown");
    assert!(screen.contains("4 byte(s) stored"), "{screen}");

    // Decoded collection elements, one of them read-only.
    a.on_msg(Msg::Value {
        info: key("app:user:2", KeyType::Hash, 3600),
        value: KeyValue::Rows {
            headers: vec!["field", "value"],
            rows: vec![
                Row {
                    id: "doc".into(),
                    cells: vec!["doc".into(), "{\"a\":1}".into()],
                    decoding: Some(gzip.clone()),
                },
                Row {
                    id: "bin".into(),
                    cells: vec!["bin".into(), "<binary, 3 bytes>\n".into()],
                    decoding: Some(Decoding {
                        read_only: Some("not text".into()),
                        ..gzip.clone()
                    }),
                },
            ],
            total: 2,
        },
    });
    render_all_sizes(&mut a);
    assert!(render_text(&mut a, 120, 40).contains("gzip (read-only)"));

    press(&mut a, KeyCode::Char('v'));
    assert!(matches!(a.modal, Some(Modal::ViewPicker { .. })));
    render_all_sizes(&mut a);
    let screen = render_text(&mut a, 120, 40);
    for label in ["auto", "plain", "gzip", "zstd", "msgpack", "hex"] {
        assert!(screen.contains(label), "{label} missing from the picker");
    }
    press(&mut a, KeyCode::Char('j'));
    press(&mut a, KeyCode::Enter);
    assert!(a.modal.is_none());
    assert!(render_text(&mut a, 120, 40).contains("plain"));
}

#[tokio::test]
async fn the_monitor_feed_and_filtered_collections_render_at_any_size() {
    let mut a = app();
    populate(&mut a);

    // A filtered hash that has not been searched to the end.
    a.value_window.filter = Some("user:*".into());
    a.on_msg(Msg::Value {
        info: key("app:user:2", KeyType::Hash, 3600),
        value: KeyValue::Rows {
            headers: vec!["field", "value"],
            rows: vec![Row {
                id: "user:1".into(),
                cells: vec!["user:1".into(), "ada".into()],
                decoding: None,
            }],
            total: 250_000,
        },
    });
    a.value_coverage = rediscope::redis_client::Coverage {
        filtered: true,
        complete: false,
        examined: 100_000,
    };
    render_all_sizes(&mut a);
    let screen = render_text(&mut a, 140, 40);
    assert!(screen.contains("filter user:*: 1 match(es)"), "{screen}");
    assert!(screen.contains("searched 100000 of 250000"), "{screen}");

    // An unfiltered first page offers more.
    a.value_window.filter = None;
    a.value_coverage = rediscope::redis_client::Coverage::default();
    render_all_sizes(&mut a);
    assert!(render_text(&mut a, 140, 40).contains("showing 1 of 250000 · + loads more"));

    // The tree says more keys can be loaded.
    a.truncated = true;
    a.pattern = "*".into();
    assert!(render_text(&mut a, 160, 40).contains("TRUNCATED (+ more)"));

    // The command monitor, with a burst it could not keep up with.
    let mut feed = rediscope::app::PubSubState::monitor(vec!["user:*".into()]);
    feed.push(
        "SET".into(),
        "db0 10.0.0.7:51234  \"user:1\" \"ada\"".into(),
    );
    feed.push("GET".into(), "db0 10.0.0.7:51234  \"user:1\"".into());
    feed.push_dropped(1_234);
    a.modal = Some(Modal::PubSub(feed));
    render_all_sizes(&mut a);
    let screen = render_text(&mut a, 140, 40);
    assert!(screen.contains("Command monitor"), "{screen}");
    assert!(screen.contains("cmd/s"), "{screen}");
    assert!(screen.contains("1234 too fast to show"), "{screen}");
    assert!(screen.contains("Commands"), "{screen}");

    // An empty monitor explains what it is waiting for.
    a.modal = Some(Modal::PubSub(rediscope::app::PubSubState::monitor(vec![])));
    render_all_sizes(&mut a);
    assert!(render_text(&mut a, 140, 40).contains("waiting for commands"));
}
