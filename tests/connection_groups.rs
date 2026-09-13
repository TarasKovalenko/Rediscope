//! Connection groups on the server list, driven through the keyboard and the
//! renderer the way a user would meet them. Each test names the acceptance
//! criterion (AC n) of `docs/plans/connection-groups.md` it checks.

use std::sync::{Mutex, MutexGuard};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use rediscope::app::{App, Modal, Msg};
use rediscope::config::{Connection, ConnectionView, Environment, Store};
use rediscope::conn_list::ConnRow;
use rediscope::ui;

/// Holds the turn of one test, and removes the scratch config directory when
/// the test ends, so a run leaves nothing behind in the temp dir.
struct Serial {
    _guard: MutexGuard<'static, ()>,
    home: &'static std::path::Path,
}

impl Drop for Serial {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(self.home);
    }
}

/// Every test here reads or writes the one `connections.json`, and several
/// assert what `Store::load` gives back, so they take turns.
fn serial() -> Serial {
    use std::sync::OnceLock;
    static LOCK: Mutex<()> = Mutex::new(());
    static HOME: OnceLock<std::path::PathBuf> = OnceLock::new();
    let guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let home = HOME.get_or_init(|| {
        let p = std::env::temp_dir().join(format!("rediscope-conn-groups-{}", std::process::id()));
        // SAFETY: OnceLock runs this once, under the lock, before any test in
        // this binary reads the variable.
        unsafe {
            std::env::set_var("REDISCOPE_HOME", &p);
            std::env::set_var("REDISCOPE_AUDIT_FILE", p.join("audit.jsonl"));
        }
        p
    });
    // Start every test from an empty directory; the previous test's guard
    // removed it.
    let _ = std::fs::remove_dir_all(home);
    std::fs::create_dir_all(home).unwrap();
    Serial {
        _guard: guard,
        home,
    }
}

fn conn(name: &str, group: Option<&str>) -> Connection {
    Connection {
        name: name.into(),
        group: group.map(str::to_string),
        ..Default::default()
    }
}

/// `checkout` {dev, prod}, `billing` {prod} and ungrouped `local`, stored
/// interleaved so grouping has to reorder them.
fn grouped_store() -> Store {
    Store {
        connections: vec![
            conn("checkout-dev", Some("checkout")),
            conn("local", None),
            conn("billing-prod", Some("billing")),
            conn("checkout-prod", Some("checkout")),
        ],
        ..Default::default()
    }
}

fn app_with(store: Store) -> App {
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    App::new(store, tx)
}

/// The visible rows in a readable form: `▾ name (n)` / `▸ name (n)` for a
/// header, the profile name indented two spaces per level otherwise.
fn rows(a: &App) -> Vec<String> {
    a.connection_rows()
        .iter()
        .map(|r| match r {
            ConnRow::Group {
                name,
                count,
                expanded,
            } => format!("{} {name} ({count})", if *expanded { "▾" } else { "▸" }),
            ConnRow::Connection { index, depth } => format!(
                "{}{}",
                "  ".repeat(usize::from(*depth)),
                a.store.connections[*index].name
            ),
        })
        .collect()
}

/// The row under the cursor, in the same form as [`rows`].
fn selected(a: &App) -> Option<String> {
    a.conn_state
        .selected()
        .and_then(|i| rows(a).get(i).cloned())
}

/// Move the cursor to the row reading exactly `label`.
fn select(a: &mut App, label: &str) {
    let at = rows(a)
        .iter()
        .position(|r| r == label)
        .unwrap_or_else(|| panic!("no row {label:?} in {:?}", rows(a)));
    a.conn_state.select(Some(at));
}

fn names(a: &App) -> Vec<String> {
    a.store.connections.iter().map(|c| c.name.clone()).collect()
}

fn find<'a>(a: &'a App, name: &str) -> &'a Connection {
    a.store
        .connections
        .iter()
        .find(|c| c.name == name)
        .unwrap_or_else(|| panic!("no profile {name}"))
}

fn press(a: &mut App, code: KeyCode) {
    a.on_key(KeyEvent::new(code, KeyModifiers::NONE));
}

fn type_str(a: &mut App, text: &str) {
    for c in text.chars() {
        press(a, KeyCode::Char(c));
    }
}

fn ctrl(a: &mut App, c: char) {
    a.on_key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL));
}

fn render_text(a: &mut App, w: u16, h: u16) -> String {
    let mut terminal = Terminal::new(TestBackend::new(w, h)).unwrap();
    terminal.draw(|f| ui::draw(f, a)).unwrap();
    let buffer = terminal.backend().buffer();
    (0..h)
        .map(|y| (0..w).map(|x| buffer[(x, y)].symbol()).collect::<String>())
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_all_sizes(a: &mut App) {
    for (w, h) in [
        (120, 40),
        (80, 24),
        (40, 12),
        (20, 8),
        (10, 5),
        (5, 3),
        (1, 1),
    ] {
        render_text(a, w, h);
    }
}

/// The screen line holding `needle`, and how many cells of padding sit
/// between the panel border and it.
fn indent_of(screen: &str, needle: &str) -> usize {
    let line = screen
        .lines()
        .find(|l| l.contains(needle))
        .unwrap_or_else(|| panic!("{needle:?} not on screen:\n{screen}"));
    let chars: Vec<char> = line.chars().collect();
    let border = chars.iter().position(|c| *c == '│').expect("panel border");
    let text: String = chars[border + 1..].iter().collect();
    text.chars().take_while(|c| *c == ' ').count()
}

/// Values of the open form's input fields, in order.
fn form_values(a: &App) -> Vec<String> {
    match &a.modal {
        Some(Modal::Form { fields, .. }) => fields
            .iter()
            .filter(|f| f.is_input())
            .map(|f| f.input.value().to_string())
            .collect(),
        _ => panic!("expected the connection form"),
    }
}

fn form_labels(a: &App) -> Vec<String> {
    match &a.modal {
        Some(Modal::Form { fields, .. }) => fields
            .iter()
            .filter(|f| f.is_input())
            .map(|f| f.label.clone())
            .collect(),
        _ => panic!("expected the connection form"),
    }
}

// ---- AC1, AC2, AC4, AC5: loading and drawing ------------------------------

/// AC1 + AC2 through the real file: a 0.12 file opens grouped with nothing
/// collapsed, shows the flat list, and a save adds no new keys.
#[tokio::test]
async fn a_legacy_file_opens_unchanged_and_saves_without_group_keys() {
    let _g = serial();
    std::fs::write(
        rediscope::config::config_file(),
        r#"{
  "theme": "redis",
  "connections": [
    {"name": "local", "host": "127.0.0.1", "port": 6379},
    {"name": "prod", "host": "cache.internal", "port": 6380, "environment": "production"}
  ]
}"#,
    )
    .unwrap();
    let (store, notice) = Store::load();
    assert!(notice.is_none(), "{notice:?}");
    assert!(store.connections.iter().all(|c| c.group.is_none()));
    assert_eq!(store.connection_view, ConnectionView::Grouped);
    assert!(store.collapsed_groups.is_empty());

    let mut a = app_with(store);
    assert_eq!(rows(&a), ["local", "prod"]);
    assert_eq!(selected(&a).as_deref(), Some("local"));
    let screen = render_text(&mut a, 120, 40);
    assert!(screen.contains("Saved connections (2)"), "{screen}");
    assert!(!screen.contains('▾') && !screen.contains('▸'), "{screen}");

    // Moving a profile saves the file; nothing new may appear in it.
    press(&mut a, KeyCode::Char('J'));
    assert_eq!(names(&a), ["prod", "local"]);
    let text = std::fs::read_to_string(rediscope::config::config_file()).unwrap();
    for key in ["\"group\"", "connection_view", "collapsed_groups"] {
        assert!(!text.contains(key), "{key} leaked into {text}");
    }
}

/// AC4: with no profile in a group, the grouped and flat lists draw the same
/// cells, filtered or not, including a whitespace-only group.
#[tokio::test]
async fn with_no_groups_grouped_and_flat_render_identically() {
    let _g = serial();
    let store = |view| Store {
        connections: vec![
            conn("local", None),
            Connection {
                name: "prod".into(),
                host: "cache.internal".into(),
                tls: true,
                environment: Environment::Production,
                ..Default::default()
            },
            conn("blank-group", Some("   ")),
        ],
        connection_view: view,
        ..Default::default()
    };
    let mut grouped = app_with(store(ConnectionView::Grouped));
    let mut flat = app_with(store(ConnectionView::Flat));
    for (w, h) in [(120, 40), (80, 24)] {
        assert_eq!(
            render_text(&mut grouped, w, h),
            render_text(&mut flat, w, h),
            "{w}x{h}"
        );
    }
    press(&mut grouped, KeyCode::Char('j'));
    press(&mut flat, KeyCode::Char('j'));
    for a in [&mut grouped, &mut flat] {
        press(a, KeyCode::Char('/'));
        type_str(a, "o");
        press(a, KeyCode::Enter);
    }
    for (w, h) in [(120, 40), (80, 24)] {
        assert_eq!(
            render_text(&mut grouped, w, h),
            render_text(&mut flat, w, h),
            "filtered {w}x{h}"
        );
    }
}

/// AC5: headers in case-insensitive order, members indented in stored order
/// under them, ungrouped profiles last at the root.
#[tokio::test]
async fn grouped_list_draws_sorted_headers_then_indented_members_then_root() {
    let _g = serial();
    let mut a = app_with(grouped_store());
    assert_eq!(
        rows(&a),
        [
            "▾ billing (1)",
            "  billing-prod",
            "▾ checkout (2)",
            "  checkout-dev",
            "  checkout-prod",
            "local",
        ]
    );

    let screen = render_text(&mut a, 120, 40);
    assert!(
        screen.contains("Saved connections (4 · 2 groups)"),
        "{screen}"
    );
    let line = |needle: &str| {
        screen
            .lines()
            .position(|l| l.contains(needle))
            .unwrap_or_else(|| panic!("{needle:?} missing:\n{screen}"))
    };
    assert!(line("▾ billing  (1)") < line("billing-prod"));
    assert!(line("billing-prod") < line("▾ checkout  (2)"));
    assert!(line("▾ checkout  (2)") < line("checkout-dev"));
    assert!(line("checkout-dev") < line("checkout-prod"));
    assert!(line("checkout-prod") < line("local "));
    // Members sit two cells deeper than headers and root rows.
    let root = indent_of(&screen, "local ");
    assert_eq!(indent_of(&screen, "▾ billing"), root);
    assert_eq!(indent_of(&screen, "checkout-dev"), root + 2);
    assert_eq!(indent_of(&screen, "billing-prod"), root + 2);

    // The same fixture in flat view: stored order, no headers, no indent.
    press(&mut a, KeyCode::Char('v'));
    assert_eq!(
        rows(&a),
        ["checkout-dev", "local", "billing-prod", "checkout-prod"]
    );
    let screen = render_text(&mut a, 120, 40);
    assert!(screen.contains("Saved connections (4)"), "{screen}");
    assert!(!screen.contains('▾'), "{screen}");
    assert_eq!(
        indent_of(&screen, "checkout-dev"),
        indent_of(&screen, "local ")
    );
}

#[tokio::test]
async fn groups_sort_ignoring_case_and_the_title_counts_them() {
    let _g = serial();
    let mut a = app_with(Store {
        connections: vec![
            conn("z1", Some("Zeta")),
            conn("root", None),
            conn("a1", Some("alpha")),
            conn("b1", Some("Beta")),
            conn("a2", Some("alpha")),
        ],
        ..Default::default()
    });
    let headers: Vec<String> = rows(&a)
        .into_iter()
        .filter(|r| r.starts_with('▾'))
        .collect();
    assert_eq!(headers, ["▾ alpha (2)", "▾ Beta (1)", "▾ Zeta (1)"]);
    assert_eq!(rows(&a).last().map(String::as_str), Some("root"));
    assert!(render_text(&mut a, 120, 40).contains("(5 · 3 groups)"));

    let mut one = app_with(Store {
        connections: vec![conn("x", Some("only"))],
        ..Default::default()
    });
    let screen = render_text(&mut one, 120, 40);
    assert!(
        screen.contains("Saved connections (1 · 1 group)"),
        "{screen}"
    );
}

/// A group is its trimmed name: padding in the file must not split it.
#[tokio::test]
async fn padded_group_names_are_one_group() {
    let _g = serial();
    let mut a = app_with(Store {
        connections: vec![
            conn("a", Some(" checkout")),
            conn("b", Some("checkout ")),
            conn("c", Some("checkout")),
        ],
        ..Default::default()
    });
    assert_eq!(rows(&a), ["▾ checkout (3)", "  a", "  b", "  c"]);
    select(&mut a, "▾ checkout (3)");
    press(&mut a, KeyCode::Char('h'));
    assert_eq!(rows(&a), ["▸ checkout (3)"]);
    let (loaded, _) = Store::load();
    assert_eq!(loaded.collapsed_groups, ["checkout"]);
    render_all_sizes(&mut a);
}

// ---- AC6, AC7, AC10: header keys and the cursor ---------------------------

/// AC6: Enter and Space toggle, l and → only expand, h and ← only collapse.
#[tokio::test]
async fn header_keys_toggle_expand_and_collapse() {
    let _g = serial();
    let mut a = app_with(grouped_store());
    select(&mut a, "▾ checkout (2)");
    let checkout = |a: &App| {
        rows(a)
            .into_iter()
            .find(|r| r.contains("checkout ("))
            .unwrap()
    };

    press(&mut a, KeyCode::Enter);
    assert_eq!(checkout(&a), "▸ checkout (2)");
    assert!(!a.connecting, "Enter on a header must not connect");
    press(&mut a, KeyCode::Enter);
    assert_eq!(checkout(&a), "▾ checkout (2)");
    press(&mut a, KeyCode::Char(' '));
    assert_eq!(checkout(&a), "▸ checkout (2)");
    press(&mut a, KeyCode::Char(' '));
    assert_eq!(checkout(&a), "▾ checkout (2)");

    for (collapse, expand) in [
        (KeyCode::Char('h'), KeyCode::Char('l')),
        (KeyCode::Left, KeyCode::Right),
    ] {
        press(&mut a, collapse);
        assert_eq!(checkout(&a), "▸ checkout (2)");
        press(&mut a, collapse); // already shut: stays shut
        assert_eq!(checkout(&a), "▸ checkout (2)");
        assert_eq!(selected(&a).as_deref(), Some("▸ checkout (2)"));
        press(&mut a, expand);
        assert_eq!(checkout(&a), "▾ checkout (2)");
        press(&mut a, expand); // already open: stays open
        assert_eq!(checkout(&a), "▾ checkout (2)");
        assert_eq!(selected(&a).as_deref(), Some("▾ checkout (2)"));
    }
    assert!(a.store.collapsed_groups.is_empty());
    assert!(a.modal.is_none());
}

/// AC6: h and ← on a member jump to its header without folding anything;
/// on a root row they do nothing. Space, l and → on a profile do nothing.
#[tokio::test]
async fn left_on_a_member_selects_its_header() {
    let _g = serial();
    let mut a = app_with(grouped_store());
    for key in [KeyCode::Char('h'), KeyCode::Left] {
        select(&mut a, "  checkout-prod");
        press(&mut a, key);
        assert_eq!(selected(&a).as_deref(), Some("▾ checkout (2)"));
        assert!(a.store.collapsed_groups.is_empty(), "only moved");

        select(&mut a, "local");
        press(&mut a, key);
        assert_eq!(selected(&a).as_deref(), Some("local"));
    }

    let before = rows(&a);
    for key in [KeyCode::Char(' '), KeyCode::Char('l'), KeyCode::Right] {
        select(&mut a, "  checkout-dev");
        press(&mut a, key);
        assert_eq!(selected(&a).as_deref(), Some("  checkout-dev"));
        assert_eq!(rows(&a), before);
        assert!(a.modal.is_none() && !a.connecting);
    }
}

/// AC7: h, h from a member folds the group with the cursor on its header,
/// and the fold is on disk.
#[tokio::test]
async fn collapsing_the_cursors_group_leaves_it_on_the_header_and_persists() {
    let _g = serial();
    let mut a = app_with(grouped_store());
    select(&mut a, "  checkout-prod");
    press(&mut a, KeyCode::Char('h'));
    press(&mut a, KeyCode::Char('h'));
    assert_eq!(
        rows(&a),
        ["▾ billing (1)", "  billing-prod", "▸ checkout (2)", "local"]
    );
    assert_eq!(a.conn_state.selected(), Some(2));
    assert_eq!(a.store.collapsed_groups, ["checkout"]);
    let (loaded, _) = Store::load();
    assert_eq!(loaded.collapsed_groups, ["checkout"]);

    // Opening it again keeps the cursor on the header, and saves that too.
    press(&mut a, KeyCode::Char('l'));
    assert_eq!(selected(&a).as_deref(), Some("▾ checkout (2)"));
    let (loaded, _) = Store::load();
    assert!(loaded.collapsed_groups.is_empty());
}

/// j/k walk the visible rows, headers included, and stop at the ends.
#[tokio::test]
async fn j_and_k_move_over_visible_rows_and_clamp() {
    let _g = serial();
    let mut a = app_with(grouped_store());
    select(&mut a, "▾ billing (1)");
    press(&mut a, KeyCode::Char('k'));
    assert_eq!(a.conn_state.selected(), Some(0));
    let mut seen = vec![selected(&a).unwrap()];
    for _ in 0..10 {
        press(&mut a, KeyCode::Char('j'));
        let now = selected(&a).unwrap();
        if seen.last() != Some(&now) {
            seen.push(now);
        }
    }
    assert_eq!(seen, rows(&a));
    assert_eq!(a.conn_state.selected(), Some(rows(&a).len() - 1));

    select(&mut a, "▾ checkout (2)");
    press(&mut a, KeyCode::Char('h'));
    press(&mut a, KeyCode::Down);
    assert_eq!(
        selected(&a).as_deref(),
        Some("local"),
        "skips hidden members"
    );
    press(&mut a, KeyCode::Up);
    assert_eq!(selected(&a).as_deref(), Some("▸ checkout (2)"));
}

/// AC10: the cursor starts on a profile so Enter connects at once, even when
/// the first groups are folded; on a header only when nothing else shows.
#[tokio::test]
async fn startup_selects_the_first_connection_row() {
    let _g = serial();
    let a = app_with(grouped_store());
    assert_eq!(selected(&a).as_deref(), Some("  billing-prod"));

    let mut store = grouped_store();
    store.collapsed_groups = vec!["billing".into()];
    let a = app_with(store);
    assert_eq!(selected(&a).as_deref(), Some("  checkout-dev"));

    let mut store = grouped_store();
    store.collapsed_groups = vec!["billing".into(), "checkout".into()];
    let a = app_with(store);
    assert_eq!(selected(&a).as_deref(), Some("local"));

    let a = app_with(Store {
        connections: vec![conn("x", Some("g"))],
        collapsed_groups: vec!["g".into()],
        ..Default::default()
    });
    assert_eq!(selected(&a).as_deref(), Some("▸ g (1)"));

    let a = app_with(Store::default());
    assert_eq!(a.conn_state.selected(), None);

    let mut store = grouped_store();
    store.connection_view = ConnectionView::Flat;
    let a = app_with(store);
    assert_eq!(selected(&a).as_deref(), Some("checkout-dev"));

    // And Enter there starts connecting rather than toggling anything.
    let mut a = app_with(grouped_store());
    press(&mut a, KeyCode::Enter);
    assert!(a.connecting);
    assert!(a.status.contains("Connecting"), "{}", a.status);
}

// ---- AC8, AC9: view mode --------------------------------------------------

/// AC9: v flips the view with the same profile selected, both ways, and
/// from a header lands on that group's first member.
#[tokio::test]
async fn v_toggles_the_view_and_keeps_the_selection() {
    let _g = serial();
    let mut a = app_with(grouped_store());
    for member in ["  checkout-prod", "  billing-prod", "local"] {
        select(&mut a, member);
        press(&mut a, KeyCode::Char('v'));
        assert_eq!(a.store.connection_view, ConnectionView::Flat);
        assert_eq!(selected(&a).as_deref(), Some(member.trim()));
        press(&mut a, KeyCode::Char('v'));
        assert_eq!(a.store.connection_view, ConnectionView::Grouped);
        assert_eq!(selected(&a).as_deref(), Some(member));
    }

    select(&mut a, "▾ checkout (2)");
    press(&mut a, KeyCode::Char('v'));
    assert_eq!(selected(&a).as_deref(), Some("checkout-dev"));

    let (loaded, _) = Store::load();
    assert_eq!(loaded.connection_view, ConnectionView::Flat);
    let text = std::fs::read_to_string(rediscope::config::config_file()).unwrap();
    assert!(text.contains("\"connection_view\": \"flat\""), "{text}");

    press(&mut a, KeyCode::Char('v'));
    let (loaded, _) = Store::load();
    assert_eq!(loaded.connection_view, ConnectionView::Grouped);
    let text = std::fs::read_to_string(rediscope::config::config_file()).unwrap();
    assert!(!text.contains("connection_view"), "default is not written");
}

/// Flat view ignores the group keys: h/l/Space do nothing and n does not
/// prefill a group.
#[tokio::test]
async fn flat_view_has_no_group_keys() {
    let _g = serial();
    let mut store = grouped_store();
    store.connection_view = ConnectionView::Flat;
    let mut a = app_with(store);
    press(&mut a, KeyCode::Char('j')); // local
    press(&mut a, KeyCode::Char('j')); // billing-prod
    for key in [KeyCode::Char('h'), KeyCode::Char('l'), KeyCode::Char(' ')] {
        press(&mut a, key);
        assert_eq!(selected(&a).as_deref(), Some("billing-prod"));
        assert!(a.store.collapsed_groups.is_empty());
        assert!(a.modal.is_none());
    }
    press(&mut a, KeyCode::Char('n'));
    assert_eq!(form_values(&a)[1], "", "no prefill in flat view");
}

/// AC8: folds and the view mode come back in a fresh app after a reload.
#[tokio::test]
async fn folds_and_view_mode_survive_a_restart() {
    let _g = serial();
    let mut a = app_with(grouped_store());
    a.store.save().unwrap();
    select(&mut a, "▾ billing (1)");
    press(&mut a, KeyCode::Char('h'));
    press(&mut a, KeyCode::Char('v'));
    drop(a);

    let (loaded, notice) = Store::load();
    assert!(notice.is_none());
    assert_eq!(loaded.connection_view, ConnectionView::Flat);
    assert_eq!(loaded.collapsed_groups, ["billing"]);
    let mut b = app_with(loaded);
    assert_eq!(
        rows(&b),
        ["checkout-dev", "local", "billing-prod", "checkout-prod"]
    );
    press(&mut b, KeyCode::Char('v'));
    assert_eq!(
        rows(&b),
        [
            "▸ billing (1)",
            "▾ checkout (2)",
            "  checkout-dev",
            "  checkout-prod",
            "local"
        ]
    );
}

/// Regression: `v` from a folded header used to open the group on the way
/// to flat view (focus_connection expanded it unconditionally), so v, v lost
/// the fold. Flat view has nothing to open, so nothing in it may unfold.
#[tokio::test]
async fn flat_view_never_forgets_a_fold() {
    let _g = serial();
    let mut a = app_with(grouped_store());
    let on_disk = || Store::load().0.collapsed_groups;
    select(&mut a, "▾ checkout (2)");
    press(&mut a, KeyCode::Char('h'));
    press(&mut a, KeyCode::Char('v'));
    assert_eq!(selected(&a).as_deref(), Some("checkout-dev"));
    assert_eq!(a.store.collapsed_groups, ["checkout"]);
    assert_eq!(on_disk(), ["checkout"]);

    // v, v: back in grouped view the fold is intact, in memory and on disk,
    // and the cursor waits on the folded header rather than opening it.
    press(&mut a, KeyCode::Char('v'));
    assert_eq!(a.store.connection_view, ConnectionView::Grouped);
    assert_eq!(a.store.collapsed_groups, ["checkout"]);
    assert_eq!(on_disk(), ["checkout"]);
    assert_eq!(selected(&a).as_deref(), Some("▸ checkout (2)"));

    // Starting in flat view on a member of the folded group lands on the
    // header too.
    press(&mut a, KeyCode::Char('v'));
    select(&mut a, "checkout-prod");
    press(&mut a, KeyCode::Char('v'));
    assert_eq!(selected(&a).as_deref(), Some("▸ checkout (2)"));
    assert_eq!(on_disk(), ["checkout"]);
    press(&mut a, KeyCode::Char('v'));
    select(&mut a, "checkout-dev");

    // Duplicating and a palette jump in flat view keep it too.
    press(&mut a, KeyCode::Char('c'));
    assert_eq!(selected(&a).as_deref(), Some("checkout-dev copy"));
    ctrl(&mut a, 'p');
    type_str(&mut a, "checkout-prod");
    press(&mut a, KeyCode::Enter);
    assert_eq!(selected(&a).as_deref(), Some("checkout-prod"));
    assert_eq!(a.store.collapsed_groups, ["checkout"]);
    let (loaded, _) = Store::load();
    assert_eq!(loaded.collapsed_groups, ["checkout"]);

    // Back to grouped from an ungrouped profile: the fold is still drawn.
    select(&mut a, "local");
    press(&mut a, KeyCode::Char('v'));
    assert_eq!(selected(&a).as_deref(), Some("local"));
    assert!(rows(&a).contains(&"▸ checkout (3)".to_string()));
}

/// Editing or duplicating while a filter is applied must not reopen a folded
/// group behind the user's back: the filter already shows the profile, and
/// the fold is theirs. Saving says "Connection saved", not something a second
/// write replaced it with.
#[tokio::test]
async fn saving_under_a_filter_keeps_folds_and_the_saved_status() {
    let _g = serial();
    let mut store = grouped_store();
    store.collapsed_groups = vec!["checkout".into()];
    let mut a = app_with(store);
    a.store.save().unwrap();
    press(&mut a, KeyCode::Char('/'));
    type_str(&mut a, "checkout-dev");
    press(&mut a, KeyCode::Enter);
    assert_eq!(rows(&a), ["▾ checkout (1)", "  checkout-dev"]);

    select(&mut a, "  checkout-dev");
    press(&mut a, KeyCode::Char('e'));
    press(&mut a, KeyCode::Enter);
    assert!(a.modal.is_none(), "form accepted");
    assert_eq!(a.status, "Connection saved");
    assert_eq!(selected(&a).as_deref(), Some("  checkout-dev"));
    assert_eq!(a.store.collapsed_groups, ["checkout"]);

    press(&mut a, KeyCode::Char('c'));
    assert_eq!(selected(&a).as_deref(), Some("  checkout-dev copy"));
    assert_eq!(a.store.collapsed_groups, ["checkout"]);
    assert_eq!(Store::load().0.collapsed_groups, ["checkout"]);

    // Without a filter, saving into a folded group opens it, in one write
    // that still reports the save.
    press(&mut a, KeyCode::Esc);
    select(&mut a, "▸ checkout (3)");
    press(&mut a, KeyCode::Char('n'));
    type_str(&mut a, "checkout-stg");
    press(&mut a, KeyCode::Enter);
    assert_eq!(a.status, "Connection saved");
    assert!(a.store.collapsed_groups.is_empty());
    assert!(Store::load().0.collapsed_groups.is_empty());
    assert_eq!(selected(&a).as_deref(), Some("  checkout-stg"));
}

// ---- AC11: filtering ------------------------------------------------------

/// Groups whose names share nothing with their members, so a hit can only
/// come from the group name.
fn team_store() -> Store {
    Store {
        connections: vec![
            conn("alpha", Some("team-a")),
            conn("beta", Some("team-a")),
            conn("gamma", None),
            conn("delta", Some("ops")),
        ],
        collapsed_groups: vec!["ops".into(), "team-a".into()],
        ..Default::default()
    }
}

#[tokio::test]
async fn the_filter_matches_group_names_and_opens_folded_groups() {
    let _g = serial();
    let mut a = app_with(team_store());
    a.store.save().unwrap();
    assert_eq!(rows(&a), ["▸ ops (1)", "▸ team-a (2)", "gamma"]);

    press(&mut a, KeyCode::Char('/'));
    type_str(&mut a, "TEAM");
    // Live while typing, and still after Enter keeps the query.
    assert_eq!(rows(&a), ["▾ team-a (2)", "  alpha", "  beta"]);
    press(&mut a, KeyCode::Enter);
    assert_eq!(rows(&a), ["▾ team-a (2)", "  alpha", "  beta"]);
    let screen = render_text(&mut a, 120, 40);
    assert!(screen.contains("2 of 4 match 'TEAM'"), "{screen}");
    assert!(screen.contains("▾ team-a  (2)"), "{screen}");

    // The fold on disk and in memory is untouched.
    assert_eq!(a.store.collapsed_groups, ["ops", "team-a"]);
    // Header keys cannot fold a group the filter holds open.
    select(&mut a, "▾ team-a (2)");
    for key in [
        KeyCode::Char('h'),
        KeyCode::Left,
        KeyCode::Char('l'),
        KeyCode::Right,
        KeyCode::Char(' '),
        KeyCode::Enter,
    ] {
        press(&mut a, key);
        assert_eq!(rows(&a), ["▾ team-a (2)", "  alpha", "  beta"], "{key:?}");
        assert_eq!(a.store.collapsed_groups, ["ops", "team-a"], "{key:?}");
        assert!(!a.connecting && a.modal.is_none(), "{key:?}");
    }
    let (loaded, _) = Store::load();
    assert_eq!(loaded.collapsed_groups, ["ops", "team-a"]);

    press(&mut a, KeyCode::Esc);
    assert!(a.conn_query.is_empty());
    assert!(!a.should_quit);
    assert_eq!(rows(&a), ["▸ ops (1)", "▸ team-a (2)", "gamma"]);
    assert!(a.conn_state.selected().unwrap() < rows(&a).len());
}

/// A member-name hit shows only the matching members and counts them.
#[tokio::test]
async fn a_filter_on_member_names_counts_only_matches() {
    let _g = serial();
    let mut a = app_with(team_store());
    press(&mut a, KeyCode::Char('/'));
    type_str(&mut a, "ta"); // beta, delta
    press(&mut a, KeyCode::Enter);
    assert_eq!(rows(&a), ["▾ ops (1)", "  delta", "▾ team-a (1)", "  beta"]);
    // A host hit still counts too.
    a.store.connections[2].host = "cache.team.internal".into();
    press(&mut a, KeyCode::Char('/'));
    ctrl(&mut a, 'u');
    type_str(&mut a, "cache.team");
    press(&mut a, KeyCode::Enter);
    assert_eq!(rows(&a), ["gamma"]);
    // Nothing matching draws the empty-filter message, not a panic.
    press(&mut a, KeyCode::Char('/'));
    ctrl(&mut a, 'u');
    type_str(&mut a, "zzz");
    press(&mut a, KeyCode::Enter);
    assert!(rows(&a).is_empty());
    assert!(render_text(&mut a, 120, 40).contains("Nothing matches this filter"));
    render_all_sizes(&mut a);
    for key in ['h', 'l', ' ', 'J', 'K', 'e', 'd', 'c', 'T', 'v', 'v'] {
        press(&mut a, KeyCode::Char(key));
    }
    assert!(a.modal.is_none());
}

// ---- AC3, AC12, AC16: the form and the header no-ops ----------------------

/// AC12: e, d, c and T on a header touch nothing and say why.
#[tokio::test]
async fn profile_keys_on_a_header_do_nothing() {
    let _g = serial();
    let mut a = app_with(grouped_store());
    select(&mut a, "▾ checkout (2)");
    let before = names(&a);
    for key in ['e', 'd', 'c', 'T', 'J', 'K'] {
        a.status.clear();
        press(&mut a, KeyCode::Char(key));
        assert!(a.modal.is_none(), "{key} opened a modal");
        assert_eq!(names(&a), before, "{key} changed the profiles");
        assert!(a.testing.is_none(), "{key} started a test");
        assert!(!a.connecting);
        assert_eq!(selected(&a).as_deref(), Some("▾ checkout (2)"));
        let expected = if matches!(key, 'J' | 'K') {
            "Groups are sorted by name"
        } else {
            "Select a connection"
        };
        assert_eq!(a.status, expected, "{key}");
    }
}

/// AC12: n prefills Group from a header or a member, and not from a root row.
#[tokio::test]
async fn n_prefills_the_group_of_the_cursor() {
    let _g = serial();
    let mut a = app_with(grouped_store());
    assert_eq!(
        form_labels_after_n(&mut a, "▾ checkout (2)")[1],
        "Group (optional)"
    );
    for (row, group) in [
        ("▾ checkout (2)", "checkout"),
        ("  checkout-dev", "checkout"),
        ("  billing-prod", "billing"),
        ("local", ""),
    ] {
        select(&mut a, row);
        press(&mut a, KeyCode::Char('n'));
        let values = form_values(&a);
        assert_eq!(values[0], "", "name is still blank");
        assert_eq!(values[1], group, "from {row}");
        press(&mut a, KeyCode::Esc);
    }
}

fn form_labels_after_n(a: &mut App, row: &str) -> Vec<String> {
    select(a, row);
    press(a, KeyCode::Char('n'));
    let labels = form_labels(a);
    press(a, KeyCode::Esc);
    labels
}

/// AC3 + AC12: saving a prefilled form lands in that group, opened, with the
/// new profile selected; even a group the user had folded.
#[tokio::test]
async fn a_new_profile_from_a_folded_group_is_saved_into_it_and_shown() {
    let _g = serial();
    let mut a = app_with(grouped_store());
    select(&mut a, "▾ checkout (2)");
    press(&mut a, KeyCode::Char('h'));
    press(&mut a, KeyCode::Char('n'));
    type_str(&mut a, "checkout-stg");
    press(&mut a, KeyCode::Enter);
    assert!(a.modal.is_none(), "form accepted");
    assert_eq!(find(&a, "checkout-stg").group.as_deref(), Some("checkout"));
    assert_eq!(selected(&a).as_deref(), Some("  checkout-stg"));
    assert!(rows(&a).contains(&"▾ checkout (3)".to_string()));

    let (loaded, _) = Store::load();
    let saved = loaded
        .connections
        .iter()
        .find(|c| c.name == "checkout-stg")
        .unwrap();
    assert_eq!(saved.group.as_deref(), Some("checkout"));
}

/// AC3 + AC16: editing moves a profile between groups; a blank or
/// whitespace group ungroups it; padding is trimmed.
#[tokio::test]
async fn editing_the_group_field_moves_and_ungroups_a_profile() {
    let _g = serial();
    let mut a = app_with(grouped_store());
    let set_group = |a: &mut App, row: &str, group: &str| {
        select(a, row);
        press(a, KeyCode::Char('e'));
        assert!(a.modal.is_some(), "e opens the form on {row}");
        press(a, KeyCode::Tab); // Name -> Group
        ctrl(a, 'u');
        type_str(a, group);
        press(a, KeyCode::Enter);
        assert!(a.modal.is_none(), "form accepted");
    };

    select(&mut a, "  checkout-dev");
    press(&mut a, KeyCode::Char('e'));
    assert_eq!(form_values(&a)[1], "checkout", "edit shows the group");
    press(&mut a, KeyCode::Esc);

    set_group(&mut a, "  checkout-dev", "  billing  ");
    assert_eq!(find(&a, "checkout-dev").group.as_deref(), Some("billing"));
    assert_eq!(selected(&a).as_deref(), Some("  checkout-dev"));
    assert_eq!(
        rows(&a),
        [
            "▾ billing (2)",
            "  checkout-dev",
            "  billing-prod",
            "▾ checkout (1)",
            "  checkout-prod",
            "local",
        ]
    );

    set_group(&mut a, "  checkout-dev", "   ");
    assert_eq!(find(&a, "checkout-dev").group, None);
    assert_eq!(selected(&a).as_deref(), Some("checkout-dev"));

    set_group(&mut a, "  checkout-prod", "");
    assert_eq!(find(&a, "checkout-prod").group, None);
    assert!(
        !rows(&a).iter().any(|r| r.contains("checkout (")),
        "empty group vanished: {:?}",
        rows(&a)
    );
    let text = std::fs::read_to_string(rediscope::config::config_file()).unwrap();
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    for c in json["connections"].as_array().unwrap() {
        if c["name"] == "billing-prod" {
            assert_eq!(c["group"], "billing");
        } else if c["name"] != "local" {
            assert!(c.get("group").is_none(), "{c}");
        }
    }
}

/// AC16: opening a fully populated profile and saving it untouched writes
/// back exactly the same profile, so no field shifted a slot.
#[tokio::test]
async fn resaving_an_untouched_profile_keeps_every_field() {
    let _g = serial();
    let full = Connection {
        name: "everything".into(),
        group: Some("checkout".into()),
        environment: Environment::Staging,
        host: "cache.example".into(),
        port: 6380,
        db: 5,
        read_only: true,
        username: "reader".into(),
        password: "s3cret".into(),
        tls: true,
        tls_ca_file: "/ca.pem".into(),
        tls_cert_file: "/cert.pem".into(),
        tls_key_file: "/key.pem".into(),
        tls_insecure: true,
        ssh_host: "bastion".into(),
        ssh_user: "ops".into(),
        ssh_port: 2222,
        ssh_key_file: "~/.ssh/id".into(),
        ..Default::default()
    };
    let mut a = app_with(Store {
        connections: vec![full.clone()],
        ..Default::default()
    });
    assert_eq!(selected(&a).as_deref(), Some("  everything"));
    press(&mut a, KeyCode::Char('e'));
    press(&mut a, KeyCode::Enter);
    assert!(a.modal.is_none(), "untouched form is valid");
    assert_eq!(
        serde_json::to_value(&a.store.connections[0]).unwrap(),
        serde_json::to_value(&full).unwrap()
    );
}

// ---- AC13: reordering -----------------------------------------------------

#[tokio::test]
async fn reorder_in_grouped_view_stays_inside_the_group() {
    let _g = serial();
    let mut store = grouped_store();
    store.connections.push(conn("scratch", None));
    let mut a = app_with(store);

    select(&mut a, "  checkout-dev");
    press(&mut a, KeyCode::Char('J'));
    // It swapped with checkout-prod, stepping over local and billing-prod.
    assert_eq!(
        names(&a),
        [
            "checkout-prod",
            "local",
            "billing-prod",
            "checkout-dev",
            "scratch"
        ]
    );
    assert_eq!(selected(&a).as_deref(), Some("  checkout-dev"));
    assert_eq!(
        rows(&a)[2..5],
        ["▾ checkout (2)", "  checkout-prod", "  checkout-dev"]
    );
    press(&mut a, KeyCode::Char('J')); // last member: no move
    assert_eq!(names(&a)[3], "checkout-dev");
    assert_eq!(selected(&a).as_deref(), Some("  checkout-dev"));
    press(&mut a, KeyCode::Char('K'));
    assert_eq!(
        names(&a),
        [
            "checkout-dev",
            "local",
            "billing-prod",
            "checkout-prod",
            "scratch"
        ]
    );
    assert_eq!(selected(&a).as_deref(), Some("  checkout-dev"));
    press(&mut a, KeyCode::Char('K')); // first member: no move
    assert_eq!(names(&a)[0], "checkout-dev");

    // The only member of billing has nowhere to go.
    select(&mut a, "  billing-prod");
    press(&mut a, KeyCode::Char('K'));
    press(&mut a, KeyCode::Char('J'));
    assert_eq!(names(&a)[2], "billing-prod");

    // Root rows move among the ungrouped profiles only.
    select(&mut a, "scratch");
    press(&mut a, KeyCode::Char('K'));
    assert_eq!(
        names(&a),
        [
            "checkout-dev",
            "scratch",
            "billing-prod",
            "checkout-prod",
            "local"
        ]
    );
    assert_eq!(selected(&a).as_deref(), Some("scratch"));
    assert_eq!(rows(&a)[5..], ["scratch", "local"]);

    let (loaded, _) = Store::load();
    let on_disk: Vec<String> = loaded.connections.iter().map(|c| c.name.clone()).collect();
    assert_eq!(on_disk, names(&a));
}

/// Flat view keeps today's behaviour: J/K swap with the stored neighbour,
/// whatever its group, and the cursor follows the row index.
#[tokio::test]
async fn reorder_in_flat_view_uses_stored_neighbours() {
    let _g = serial();
    let mut store = grouped_store();
    store.connection_view = ConnectionView::Flat;
    let mut a = app_with(store);
    press(&mut a, KeyCode::Char('J'));
    assert_eq!(
        names(&a),
        ["local", "checkout-dev", "billing-prod", "checkout-prod"]
    );
    assert_eq!(a.conn_state.selected(), Some(1));
    press(&mut a, KeyCode::Char('J'));
    assert_eq!(a.conn_state.selected(), Some(2));
    assert_eq!(names(&a)[2], "checkout-dev");
}

#[tokio::test]
async fn duplicating_a_member_keeps_the_copy_in_its_group() {
    let _g = serial();
    let mut a = app_with(grouped_store());
    select(&mut a, "  checkout-dev");
    press(&mut a, KeyCode::Char('c'));
    assert_eq!(
        find(&a, "checkout-dev copy").group.as_deref(),
        Some("checkout")
    );
    assert_eq!(selected(&a).as_deref(), Some("  checkout-dev copy"));
    assert!(rows(&a).contains(&"▾ checkout (3)".to_string()));
}

// ---- AC14: palette --------------------------------------------------------

#[tokio::test]
async fn the_palette_labels_grouped_servers_and_toggles_the_view() {
    let _g = serial();
    let mut a = app_with(grouped_store());
    ctrl(&mut a, 'p');
    type_str(&mut a, "checkout");
    let Some(Modal::Palette(p)) = &a.modal else {
        panic!("palette open");
    };
    let texts: Vec<&str> = p.hits.iter().map(|h| h.text.as_str()).collect();
    assert!(texts.contains(&"checkout › checkout-dev"), "{texts:?}");
    assert!(texts.contains(&"checkout › checkout-prod"), "{texts:?}");
    let screen = render_text(&mut a, 120, 40);
    assert!(screen.contains("checkout › checkout-prod"), "{screen}");
    render_all_sizes(&mut a);
    press(&mut a, KeyCode::Esc);

    // A group-only query finds every member, ungrouped rows stay plain.
    ctrl(&mut a, 'p');
    type_str(&mut a, "billing");
    let Some(Modal::Palette(p)) = &a.modal else {
        panic!("palette open");
    };
    assert!(p.hits.iter().any(|h| h.text == "billing › billing-prod"));
    press(&mut a, KeyCode::Esc);
    ctrl(&mut a, 'p');
    type_str(&mut a, "local");
    let Some(Modal::Palette(p)) = &a.modal else {
        panic!("palette open");
    };
    assert!(
        p.hits.iter().any(|h| h.text == "local"),
        "no › on root rows"
    );
    press(&mut a, KeyCode::Esc);

    ctrl(&mut a, 'p');
    type_str(&mut a, "toggle grouped");
    let Some(Modal::Palette(p)) = &a.modal else {
        panic!("palette open");
    };
    assert!(
        p.selected_hit()
            .is_some_and(|h| h.text.contains("Toggle grouped / flat")),
        "{:?}",
        p.selected_hit()
    );
    select_hit_and_enter(&mut a);
    assert_eq!(a.store.connection_view, ConnectionView::Flat);
}

fn select_hit_and_enter(a: &mut App) {
    press(a, KeyCode::Enter);
    assert!(a.modal.is_none());
}

/// AC14: jumping to a server inside a folded group opens the group, selects
/// the server, and connects.
#[tokio::test]
async fn a_palette_jump_into_a_folded_group_opens_it_and_connects() {
    let _g = serial();
    let mut store = grouped_store();
    store.collapsed_groups = vec!["checkout".into()];
    let mut a = app_with(store);
    a.store.save().unwrap();
    // Even with a filter hiding it.
    press(&mut a, KeyCode::Char('/'));
    type_str(&mut a, "billing");
    press(&mut a, KeyCode::Enter);

    ctrl(&mut a, 'p');
    type_str(&mut a, "checkout-prod");
    let Some(Modal::Palette(p)) = &a.modal else {
        panic!("palette open");
    };
    assert_eq!(
        p.selected_hit().map(|h| &h.target),
        Some(&rediscope::palette::Target::Server("checkout-prod".into()))
    );
    press(&mut a, KeyCode::Enter);
    assert!(a.conn_query.is_empty(), "the jump clears the filter");
    assert!(a.store.collapsed_groups.is_empty());
    assert_eq!(selected(&a).as_deref(), Some("  checkout-prod"));
    assert!(a.connecting, "Enter from the palette connects");
    let (loaded, _) = Store::load();
    assert!(loaded.collapsed_groups.is_empty(), "the open is saved");
}

// ---- AC15: deleting -------------------------------------------------------

#[tokio::test]
async fn deleting_the_last_member_removes_its_header_and_clamps() {
    let _g = serial();
    let mut a = app_with(grouped_store());
    select(&mut a, "  billing-prod");
    press(&mut a, KeyCode::Char('d'));
    assert!(matches!(a.modal, Some(Modal::Confirm { .. })));
    press(&mut a, KeyCode::Enter);
    assert!(a.modal.is_none());
    assert_eq!(
        rows(&a),
        [
            "▾ checkout (2)",
            "  checkout-dev",
            "  checkout-prod",
            "local"
        ]
    );
    let sel = a.conn_state.selected().expect("still a selection");
    assert!(sel < rows(&a).len());
    assert!(render_text(&mut a, 120, 40).contains("(3 · 1 group)"));

    // The bottom row going clamps the cursor onto the new last row.
    select(&mut a, "local");
    press(&mut a, KeyCode::Char('d'));
    press(&mut a, KeyCode::Enter);
    assert_eq!(
        rows(&a),
        ["▾ checkout (2)", "  checkout-dev", "  checkout-prod"]
    );
    assert_eq!(a.conn_state.selected(), Some(2));

    // Down to nothing at all.
    for member in ["  checkout-dev", "  checkout-prod"] {
        select(&mut a, member);
        press(&mut a, KeyCode::Char('d'));
        press(&mut a, KeyCode::Enter);
    }
    assert!(a.store.connections.is_empty());
    assert!(rows(&a).is_empty());
    assert_eq!(a.conn_state.selected(), None);
    let screen = render_text(&mut a, 120, 40);
    assert!(screen.contains("No saved connections yet"), "{screen}");
}

/// A folded group can only lose its last member through a filter. The fold
/// must not outlive the group and swallow a future group of the same name.
#[tokio::test]
async fn deleting_the_last_member_of_a_folded_group_forgets_the_fold() {
    let _g = serial();
    let mut store = grouped_store();
    store.collapsed_groups = vec!["billing".into()];
    let mut a = app_with(store);
    press(&mut a, KeyCode::Char('/'));
    type_str(&mut a, "billing-prod");
    press(&mut a, KeyCode::Enter);
    select(&mut a, "  billing-prod");
    press(&mut a, KeyCode::Char('d'));
    press(&mut a, KeyCode::Enter);
    assert!(rows(&a).is_empty());
    press(&mut a, KeyCode::Esc);
    assert!(!rows(&a).iter().any(|r| r.contains("billing")));
    assert!(a.conn_state.selected().unwrap() < rows(&a).len());
    let (loaded, _) = Store::load();
    assert!(loaded.collapsed_groups.is_empty(), "pruned on save");

    // A new "billing" group starts open.
    select(&mut a, "local");
    press(&mut a, KeyCode::Char('n'));
    type_str(&mut a, "billing-new");
    press(&mut a, KeyCode::Tab);
    type_str(&mut a, "billing");
    press(&mut a, KeyCode::Enter);
    assert!(a.modal.is_none());
    assert!(
        rows(&a).contains(&"▾ billing (1)".to_string()),
        "{:?}",
        rows(&a)
    );
    assert_eq!(selected(&a).as_deref(), Some("  billing-new"));
}

// ---- AC17: sizes ----------------------------------------------------------

#[tokio::test]
async fn the_grouped_server_list_renders_at_any_size() {
    let _g = serial();
    let long = "a-group-name-that-is-far-too-long-for-any-small-terminal-xxx";
    assert_eq!(long.len(), 60);
    let mut store = grouped_store();
    store.connections.push(conn("long-member", Some(long)));
    store.connections.push(conn(
        "ünïcödé-名前-member-with-a-long-name",
        Some("名前グループ"),
    ));
    let mut a = app_with(store);
    for view in [ConnectionView::Grouped, ConnectionView::Flat] {
        assert_eq!(a.store.connection_view, view);
        render_all_sizes(&mut a); // cursor on a member
        let first = rows(&a)[0].clone();
        select(&mut a, &first);
        render_all_sizes(&mut a); // cursor on a header (grouped)
        press(&mut a, KeyCode::Char('/'));
        type_str(&mut a, "group");
        render_all_sizes(&mut a); // filter being typed
        press(&mut a, KeyCode::Enter);
        render_all_sizes(&mut a);
        press(&mut a, KeyCode::Esc);
        ctrl(&mut a, 'p');
        render_all_sizes(&mut a);
        press(&mut a, KeyCode::Esc);
        press(&mut a, KeyCode::Char('?'));
        render_all_sizes(&mut a);
        press(&mut a, KeyCode::Esc);
        press(&mut a, KeyCode::Char('n'));
        render_all_sizes(&mut a);
        press(&mut a, KeyCode::Esc);
        press(&mut a, KeyCode::Char('v'));
    }
    // Every group folded.
    for header in rows(&a).into_iter().filter(|r| r.starts_with('▾')).rev() {
        select(&mut a, &header);
        press(&mut a, KeyCode::Char('h'));
    }
    assert!(rows(&a).iter().all(|r| !r.starts_with('▾')));
    render_all_sizes(&mut a);

    // The long header is cut to the panel, keeping its glyph.
    let screen = render_text(&mut a, 40, 20);
    assert!(!screen.contains(long), "{screen}");
    assert!(screen.contains("▸ a-group-name"), "{screen}");
}

/// One group holding every profile, then folded: headers are rows, so the
/// list is not "empty" and Enter reopens it.
#[tokio::test]
async fn a_single_folded_group_is_not_an_empty_list() {
    let _g = serial();
    let mut a = app_with(Store {
        connections: vec![conn("a", Some("all")), conn("b", Some("all"))],
        ..Default::default()
    });
    assert_eq!(rows(&a), ["▾ all (2)", "  a", "  b"]);
    assert_eq!(selected(&a).as_deref(), Some("  a"));
    press(&mut a, KeyCode::Char('h'));
    press(&mut a, KeyCode::Char('h'));
    assert_eq!(rows(&a), ["▸ all (2)"]);
    assert_eq!(a.conn_state.selected(), Some(0));
    let screen = render_text(&mut a, 80, 24);
    assert!(screen.contains("▸ all  (2)"), "{screen}");
    assert!(!screen.contains("No saved connections yet"), "{screen}");
    assert!(!screen.contains("Nothing matches"), "{screen}");
    render_all_sizes(&mut a);
    for key in ['j', 'k', 'J', 'K', 'e', 'd', 'c', 'T'] {
        press(&mut a, KeyCode::Char(key));
    }
    assert_eq!(rows(&a), ["▸ all (2)"]);
    press(&mut a, KeyCode::Enter);
    assert_eq!(rows(&a), ["▾ all (2)", "  a", "  b"]);
}

/// With no profiles at all, the group keys are harmless.
#[tokio::test]
async fn an_empty_list_ignores_the_group_keys() {
    let _g = serial();
    let mut a = app_with(Store::default());
    for key in [
        KeyCode::Char('v'),
        KeyCode::Char('h'),
        KeyCode::Char('l'),
        KeyCode::Left,
        KeyCode::Right,
        KeyCode::Char(' '),
        KeyCode::Char('J'),
        KeyCode::Char('K'),
        KeyCode::Char('e'),
        KeyCode::Char('d'),
        KeyCode::Char('c'),
        KeyCode::Char('T'),
        KeyCode::Char('j'),
        KeyCode::Char('v'),
    ] {
        press(&mut a, key);
        assert!(a.modal.is_none(), "{key:?}");
        assert_eq!(a.conn_state.selected(), None, "{key:?}");
    }
    let screen = render_text(&mut a, 120, 40);
    assert!(screen.contains("No saved connections yet"), "{screen}");
    render_all_sizes(&mut a);
    press(&mut a, KeyCode::Char('n'));
    assert_eq!(form_values(&a)[1], "");
}

// ---- AC18: --profile ------------------------------------------------------

#[tokio::test]
async fn profile_lookup_ignores_groups() {
    let _g = serial();
    grouped_store().save().unwrap();
    let c = rediscope::headless::resolve(Some("checkout-prod"), None).unwrap();
    assert_eq!(c.name, "checkout-prod");
    assert_eq!(c.group.as_deref(), Some("checkout"));
    assert!(
        rediscope::headless::resolve(Some("checkout"), None).is_err(),
        "a group name is not a profile"
    );
    assert!(rediscope::headless::resolve(Some("checkout/checkout-prod"), None).is_err());
}
