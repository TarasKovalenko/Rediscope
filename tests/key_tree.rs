//! The key tree's separator and sort order, away from any server: edge cases
//! in how names split, how natural order compares them, and what the browser,
//! palette and memory report do with both.

mod common;

use std::cmp::Ordering;
use std::collections::HashSet;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use rediscope::app::{App, Modal, Msg, Screen};
use rediscope::config::Store;
use rediscope::memory::Rollup;
use rediscope::redis_client::{KeyInfo, KeyType, encode_key};
use rediscope::tree::{SortMode, Tree, VisibleRow, natural_cmp};

fn key(name: &str) -> KeyInfo {
    typed(name, KeyType::String, -1)
}

fn typed(name: &str, kind: KeyType, ttl: i64) -> KeyInfo {
    KeyInfo {
        name: name.into(),
        kind,
        ttl,
    }
}

fn keys(names: &[&str]) -> Vec<KeyInfo> {
    names.iter().map(|n| key(n)).collect()
}

/// Every row with every folder open.
fn open_rows(tree: &Tree) -> Vec<VisibleRow> {
    let all: HashSet<String> = tree.all_folder_paths().into_iter().collect();
    tree.visible(&all)
}

fn labels(tree: &Tree) -> Vec<String> {
    open_rows(tree).into_iter().map(|r| r.label).collect()
}

fn sorted_paths(tree: &Tree) -> Vec<String> {
    let mut paths = tree.all_folder_paths();
    paths.sort();
    paths
}

/// Each leaf, as (the folder path it is drawn in, its label, its key name).
fn placements(tree: &Tree) -> Vec<(Option<String>, String, String)> {
    let mut stack: Vec<String> = Vec::new();
    let mut out = Vec::new();
    for row in open_rows(tree) {
        stack.truncate(row.depth);
        match (&row.folder_path, &row.key) {
            (Some(path), _) => stack.push(path.clone()),
            (None, Some(k)) => out.push((stack.last().cloned(), row.label.clone(), k.name.clone())),
            _ => {}
        }
    }
    out
}

/// The folder a key is drawn in, joined to its label by the separator, has to
/// give the key name back; and no two folders may share a path, since the
/// path is what opens, marks and remembers a folder.
fn assert_consistent(tree: &Tree, separator: &str) {
    for (folder, label, name) in placements(tree) {
        let rebuilt = match &folder {
            None => label.clone(),
            Some(path) => format!("{path}{separator}{label}"),
        };
        assert_eq!(rebuilt, name, "drawn in {folder:?} as {label:?}");
    }
    let paths = tree.all_folder_paths();
    let unique: HashSet<&String> = paths.iter().collect();
    assert_eq!(
        unique.len(),
        paths.len(),
        "duplicate folder paths: {paths:?}"
    );
}

// ---- how names split --------------------------------------------------------

#[test]
fn keys_starting_ending_or_doubling_the_separator_keep_every_segment() {
    let tree = Tree::build(
        &keys(&[":lead", "trail:", "a::b", "a:b", "::", "plain"]),
        ":",
        SortMode::Name,
    );
    // Empty segments are folders with empty names; nothing is lost or merged.
    assert_consistent(&tree, ":");
    assert_eq!(sorted_paths(&tree), ["", ":", "a", "a:", "trail"]);
    assert_eq!(placements(&tree).len(), 6, "every key is drawn once");
}

/// `::x` puts `x` two empty-named folders deep. The inner folder's path is
/// `:`, not the outer one's `""`, or opening and marking one acts on both.
#[test]
fn nested_empty_folders_at_the_root_have_distinct_paths() {
    let mut app = browser(":", &["::x", ":y"]);
    let inner = app
        .rows
        .iter()
        .position(|r| r.depth == 1 && r.folder_path.is_some())
        .expect("an inner folder");
    app.tree_state.select(Some(inner));
    press(&mut app, KeyCode::Char('m'));
    let mut marked: Vec<&str> = app.marked.iter().map(String::as_str).collect();
    marked.sort();
    assert_eq!(marked, ["::x"], "only the key inside the inner folder");

    let tree = Tree::build(&keys(&["::x", ":y"]), ":", SortMode::Name);
    assert_consistent(&tree, ":");
}

#[test]
fn a_single_colon_separator_nests_double_colon_keys_one_level_deeper() {
    let tree = Tree::build(&keys(&["ns:a", "ns::b"]), ":", SortMode::Name);
    assert_consistent(&tree, ":");
    assert_eq!(sorted_paths(&tree), ["ns", "ns:"]);
    let place = placements(&tree);
    assert!(
        place.contains(&(Some("ns:".into()), "b".into(), "ns::b".into())),
        "{place:?}"
    );
    assert!(
        place.contains(&(Some("ns".into()), "a".into(), "ns:a".into())),
        "{place:?}"
    );
}

#[test]
fn a_double_colon_separator_leaves_single_colons_inside_segments() {
    let tree = Tree::build(
        &keys(&["mod::fn:inner", "mod::other", "mod:flat", "::root"]),
        "::",
        SortMode::Name,
    );
    assert_consistent(&tree, "::");
    assert_eq!(sorted_paths(&tree), ["", "mod"]);
    let place = placements(&tree);
    assert!(place.contains(&(
        Some("mod".into()),
        "fn:inner".into(),
        "mod::fn:inner".into()
    )));
    assert!(place.contains(&(None, "mod:flat".into(), "mod:flat".into())));
    assert!(place.contains(&(Some("".into()), "root".into(), "::root".into())));
}

/// A separator that can overlap itself (`::` in `a:::b`) splits left to
/// right, so the key sits in folder `a` with `:b` left over. The label and
/// the new-key prefix have to agree with where the key is drawn.
#[test]
fn an_overlapping_separator_labels_the_leaf_with_what_follows_its_folder() {
    let tree = Tree::build(&keys(&["a:::b"]), "::", SortMode::Name);
    assert_eq!(sorted_paths(&tree), ["a"]);
    let place = placements(&tree);
    assert_eq!(
        place,
        [(Some("a".to_string()), ":b".to_string(), "a:::b".to_string())],
        "folder a joined by :: to the label must give the key back"
    );
}

#[test]
fn an_overlapping_separator_starts_a_new_key_in_the_folder_the_key_is_drawn_in() {
    let mut app = browser("::", &["a:::b", "a::c"]);
    select_key(&mut app, "a:::b");
    assert_eq!(
        app.new_key_prefix(),
        "a::",
        "the folder the key is drawn in, then the separator"
    );
}

#[test]
fn glob_and_regex_characters_are_plain_separators() {
    for sep in ["*", "?", "[", "]", ".", "|", "\\d", "^", "$", "(", "+"] {
        // Names as the server hands them over, so `\d` in a stored name is
        // `\\d` by the time the tree sees it.
        let names: Vec<String> = [
            format!("one{sep}two{sep}x"),
            format!("one{sep}two{sep}y"),
            format!("one{sep}z"),
            "nosep".to_string(),
        ]
        .iter()
        .map(|n| encode_key(n.as_bytes()))
        .collect();
        let shown = encode_key(sep.as_bytes());
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let tree = Tree::build(&keys(&refs), sep, SortMode::Name);
        assert_consistent(&tree, &shown);
        assert_eq!(
            sorted_paths(&tree),
            ["one".to_string(), format!("one{shown}two")],
            "{sep}"
        );
        assert_eq!(
            labels(&tree),
            ["one", "two", "x", "y", "z", "nosep"],
            "{sep}"
        );
    }
}

#[test]
fn a_separator_longer_than_every_key_leaves_the_tree_flat() {
    let tree = Tree::build(
        &keys(&["a", "ab", "user:1"]),
        "-->separator<--",
        SortMode::Name,
    );
    assert!(tree.all_folder_paths().is_empty());
    assert_eq!(labels(&tree), ["a", "ab", "user:1"]);
}

#[test]
fn unicode_separators_split_on_characters_not_bytes() {
    // `→` is three bytes; `é` shares none of them but a byte split would still
    // be wrong for a separator such as `·` whose bytes appear in other text.
    let tree = Tree::build(
        &keys(&["café→menu→1", "café→menu→2", "naïve·x"]),
        "→",
        SortMode::Name,
    );
    assert_eq!(sorted_paths(&tree), ["café", "café→menu"]);
    let tree = Tree::build(&keys(&["a·b", "aÂ·"]), "·", SortMode::Name);
    assert_eq!(sorted_paths(&tree), ["a", "aÂ"]);
}

/// Binary key names reach the tree through `encode_key`, which writes bytes
/// that are not UTF-8 as `\xNN` and doubles a literal backslash. Splitting on
/// `:` must leave those escapes whole.
#[test]
fn escaped_binary_names_split_without_breaking_their_escapes() {
    let raw: [&[u8]; 3] = [b"bin:\xff\xfe:1", b"bin:\xff\xfe:2", b"bin:plain"];
    let names: Vec<String> = raw.iter().map(|b| encode_key(b)).collect();
    assert_eq!(names[0], "bin:\\xff\\xfe:1");
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let tree = Tree::build(&keys(&refs), ":", SortMode::Name);
    assert_consistent(&tree, ":");
    assert_eq!(sorted_paths(&tree), ["bin", "bin:\\xff\\xfe"]);
    assert_eq!(
        labels(&tree),
        ["bin", "\\xff\\xfe", "1", "2", "plain"],
        "the escape is one folder name"
    );
}

/// With a backslash as the separator, a key holding one literal backslash is
/// carried as `dir\\file`. The tree should still show `dir` holding `file`,
/// not an extra empty folder between them.
#[test]
fn a_backslash_separator_splits_names_that_hold_one_literal_backslash() {
    let names: Vec<String> = [b"dir\\file".as_slice(), b"dir\\other"]
        .iter()
        .map(|b| encode_key(b))
        .collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let tree = Tree::build(&keys(&refs), "\\", SortMode::Name);
    assert_eq!(sorted_paths(&tree), ["dir"], "{:?}", labels(&tree));
    assert_eq!(labels(&tree), ["dir", "file", "other"]);
}

#[test]
fn a_space_is_a_usable_separator() {
    let tree = Tree::build(&keys(&["a b c", "a b d", " lead"]), " ", SortMode::Name);
    assert_consistent(&tree, " ");
    assert_eq!(sorted_paths(&tree), ["", "a", "a b"]);
}

#[test]
fn leaves_count_every_key_below_a_folder() {
    let tree = Tree::build(
        &keys(&["x/1", "x/y/2", "x/y/3", "x/y/z/4", "w"]),
        "/",
        SortMode::Name,
    );
    let rows = open_rows(&tree);
    let count = |label: &str| rows.iter().find(|r| r.label == label).unwrap().leaves;
    assert_eq!((count("x"), count("y"), count("z")), (4, 3, 1));
}

// ---- natural order ----------------------------------------------------------

#[test]
fn leading_zeros_group_by_value_and_break_ties_by_bytes() {
    let mut names = vec!["a1", "a01", "a001", "a2", "a010", "a10", "a0", "a00", "a"];
    names.sort_by(|a, b| natural_cmp(a, b));
    assert_eq!(
        names,
        ["a", "a0", "a00", "a001", "a01", "a1", "a2", "a010", "a10"]
    );
    assert_eq!(natural_cmp("a01", "a1"), Ordering::Less);
    assert_eq!(natural_cmp("a1", "a01"), Ordering::Greater);
    // Leading zeros decide nothing while a later character still differs.
    assert_eq!(natural_cmp("a01b", "a1a"), Ordering::Greater);
}

#[test]
fn digit_runs_far_past_u64_compare_by_value_without_panicking() {
    let big = "9".repeat(400);
    let bigger = format!("1{}", "0".repeat(400));
    assert_eq!(natural_cmp(&big, &bigger), Ordering::Less);
    let padded = format!("{}{}", "0".repeat(1000), big);
    assert_eq!(
        natural_cmp(&padded, &big),
        Ordering::Less,
        "bytes tie-break"
    );
    assert_eq!(
        natural_cmp(&format!("id:{big}:x"), &format!("id:{big}:y")),
        Ordering::Less
    );
    // u64::MAX and one past it.
    assert_eq!(
        natural_cmp("k18446744073709551615", "k18446744073709551616"),
        Ordering::Less
    );
    // A run of nothing but zeros is zero, however long.
    assert_eq!(
        natural_cmp(&"0".repeat(5000), "1"),
        Ordering::Less,
        "no overflow"
    );
    let tree = Tree::build(
        &keys(&[&format!("n:{bigger}"), &format!("n:{big}"), "n:7"]),
        ":",
        SortMode::Name,
    );
    let order: Vec<String> = placements(&tree).into_iter().map(|(_, _, k)| k).collect();
    assert_eq!(
        order,
        ["n:7".to_string(), format!("n:{big}"), format!("n:{bigger}")]
    );
}

#[test]
fn case_is_ignored_for_unicode_letters_too() {
    assert_eq!(natural_cmp("Éclair", "éclair").then(Ordering::Equal), {
        // Equal ignoring case, so byte order decides, and deterministically.
        "Éclair".cmp("éclair")
    });
    let mut names = vec!["ÖL", "öl2", "Öl10", "apfel", "Zebra", "äpfel"];
    names.sort_by(|a, b| natural_cmp(a, b));
    // Lowercased: apfel, zebra, äpfel, öl, öl2, öl10 by code point.
    assert_eq!(names, ["apfel", "Zebra", "äpfel", "ÖL", "öl2", "Öl10"]);
    // Kelvin sign lowercases to k.
    assert_eq!(natural_cmp("\u{212A}ey", "key2"), Ordering::Less);
}

#[test]
fn names_equal_but_for_case_sort_the_same_way_every_time() {
    let base = ["Key", "KEY", "key", "kEy", "key0", "Key00"];
    let mut forward = base.to_vec();
    let mut backward: Vec<&str> = base.iter().rev().copied().collect();
    forward.sort_by(|a, b| natural_cmp(a, b));
    backward.sort_by(|a, b| natural_cmp(a, b));
    assert_eq!(forward, backward);
    for a in base {
        for b in base {
            assert_eq!(natural_cmp(a, b) == Ordering::Equal, a == b, "{a} {b}");
        }
    }
}

/// Rust's sort may panic when a comparison is not a total order, so check it
/// is one over names built to stress it: digits against letters and
/// punctuation, leading zeros, case, and characters whose lowercase form is
/// longer than one character.
#[test]
fn natural_order_is_a_total_order() {
    let atoms = [
        "", "0", "00", "01", "1", "9", "10", "a", "A", "b", ":", "/", "-", " ", "_", "~", "ß",
        "SS", "İ", "i\u{307}", "\u{212A}", "k", "é", "٣",
    ];
    let mut names: Vec<String> = Vec::new();
    for a in atoms {
        for b in ["", "0", "1", "a", "B", ":", "İ"] {
            names.push(format!("{a}{b}"));
        }
    }
    names.sort();
    names.dedup();
    for a in &names {
        for b in &names {
            let ab = natural_cmp(a, b);
            assert_eq!(ab, natural_cmp(b, a).reverse(), "antisymmetry {a:?} {b:?}");
            if ab != Ordering::Less {
                continue;
            }
            for c in &names {
                if natural_cmp(b, c) == Ordering::Less {
                    assert_eq!(
                        natural_cmp(a, c),
                        Ordering::Less,
                        "transitivity {a:?} < {b:?} < {c:?}"
                    );
                }
            }
        }
    }
    // And a sort over all of them agrees with itself from any starting order.
    let mut forward = names.clone();
    let mut backward: Vec<String> = names.iter().rev().cloned().collect();
    forward.sort_by(|a, b| natural_cmp(a, b));
    backward.sort_by(|a, b| natural_cmp(a, b));
    assert_eq!(forward, backward);
}

#[test]
fn mixed_digits_and_letters_compare_run_by_run() {
    let mut names = vec![
        "v1.10.2", "v1.9.10", "v1.9.9", "v1.10", "v10", "v2a", "v2", "vA",
    ];
    names.sort_by(|a, b| natural_cmp(a, b));
    assert_eq!(
        names,
        [
            "v1.9.9", "v1.9.10", "v1.10", "v1.10.2", "v2", "v2a", "v10", "vA"
        ]
    );
    // A digit against a letter compares the characters.
    assert_eq!(natural_cmp("x9", "xa"), Ordering::Less);
}

// ---- sort modes ---------------------------------------------------------------

fn leaf_names(tree: &Tree) -> Vec<String> {
    placements(tree).into_iter().map(|(_, _, k)| k).collect()
}

#[test]
fn ttl_order_puts_no_expiry_and_gone_keys_last_by_name() {
    let tree = Tree::build(
        &[
            typed("gone2", KeyType::String, -2),
            typed("forever10", KeyType::String, -1),
            typed("zero", KeyType::String, 0),
            typed("soon", KeyType::String, 1),
            typed("gone1", KeyType::String, -2),
            typed("later", KeyType::String, 86_400),
            typed("forever9", KeyType::String, -1),
            typed("huge", KeyType::String, i64::MAX),
            typed("weird", KeyType::String, i64::MIN),
        ],
        ":",
        SortMode::Ttl,
    );
    assert_eq!(
        leaf_names(&tree),
        [
            "zero",
            "soon",
            "later",
            "huge",
            "forever9",
            "forever10",
            "gone1",
            "gone2",
            "weird"
        ]
    );
}

#[test]
fn type_order_groups_by_type_name_then_natural_name() {
    let kinds = [
        KeyType::String,
        KeyType::Hash,
        KeyType::List,
        KeyType::Set,
        KeyType::ZSet,
        KeyType::Stream,
        KeyType::Json,
        KeyType::TimeSeries,
        KeyType::VectorSet,
        KeyType::Other,
    ];
    let mut input = Vec::new();
    for (i, kind) in kinds.iter().enumerate() {
        input.push(typed(&format!("k{}", 20 - i), *kind, -1));
        input.push(typed(&format!("k{}", 100 + i), *kind, -1));
    }
    let tree = Tree::build(&input, ":", SortMode::Type);
    let order: Vec<(String, String)> = open_rows(&tree)
        .into_iter()
        .filter_map(|r| r.key)
        .map(|k| (k.kind.name().to_string(), k.name))
        .collect();
    let mut expected = order.clone();
    expected.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| natural_cmp(&a.1, &b.1)));
    assert_eq!(order, expected);
    assert_eq!(order.first().unwrap().0, "hash");
    assert_eq!(order.last().unwrap().0, "zset");
}

#[test]
fn folders_keep_their_place_whichever_the_mode() {
    let input = [
        typed("b:1", KeyType::Hash, 5),
        typed("a:1", KeyType::String, -1),
        typed("c", KeyType::List, 1),
        typed("a0", KeyType::Set, 2),
    ];
    let folders = |mode| -> Vec<String> {
        open_rows(&Tree::build(&input, ":", mode))
            .into_iter()
            .filter(|r| r.folder_path.is_some())
            .map(|r| r.label)
            .collect()
    };
    for mode in [SortMode::Name, SortMode::Ttl, SortMode::Type] {
        assert_eq!(folders(mode), ["a", "b"], "{mode:?}");
        let rows = open_rows(&Tree::build(&input, ":", mode));
        let first_root_leaf = rows
            .iter()
            .position(|r| r.depth == 0 && r.key.is_some())
            .unwrap();
        assert!(
            rows[..first_root_leaf]
                .iter()
                .all(|r| r.depth > 0 || r.folder_path.is_some()),
            "folders first at the root in {mode:?}"
        );
    }
}

// ---- the browser ------------------------------------------------------------

fn browser(separator: &str, names: &[&str]) -> App {
    browser_with(separator, names.iter().map(|n| key(n)).collect())
}

fn browser_with(separator: &str, keys: Vec<KeyInfo>) -> App {
    common::isolate_config();
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(Store::default(), tx);
    app.screen = Screen::Browser;
    app.separator = separator.into();
    app.on_msg(Msg::Keys {
        warnings: vec![],
        dbsize: keys.len() as u64,
        keys,
        truncated: false,
        pattern: "*".into(),
    });
    app
}

fn press(app: &mut App, code: KeyCode) {
    app.on_key(KeyEvent::new(code, KeyModifiers::NONE));
}

fn ctrl(app: &mut App, c: char) {
    app.on_key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL));
}

fn type_str(app: &mut App, text: &str) {
    for c in text.chars() {
        press(app, KeyCode::Char(c));
    }
}

fn select_key(app: &mut App, name: &str) {
    let index = app
        .rows
        .iter()
        .position(|r| r.key.as_ref().is_some_and(|k| k.name == name))
        .unwrap_or_else(|| panic!("no key {name}"));
    app.tree_state.select(Some(index));
}

fn select_folder(app: &mut App, path: &str) {
    let index = app
        .rows
        .iter()
        .position(|r| r.folder_path.as_deref() == Some(path))
        .unwrap_or_else(|| panic!("no folder {path}"));
    app.tree_state.select(Some(index));
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
    for (w, h) in [(120, 40), (80, 24), (40, 12), (20, 8), (10, 5), (1, 1)] {
        render(app, w, h);
    }
}

#[test]
fn o_keeps_the_cursor_on_a_folder_and_on_a_nested_key() {
    let mut app = browser_with(
        ":",
        vec![
            typed("q:z", KeyType::String, 1),
            typed("q:a", KeyType::Hash, -1),
            typed("q:m", KeyType::List, 50),
            typed("top", KeyType::Set, 3),
        ],
    );
    select_folder(&mut app, "q");
    for _ in 0..3 {
        press(&mut app, KeyCode::Char('o'));
        assert_eq!(
            app.selected_row().unwrap().folder_path.as_deref(),
            Some("q"),
            "{:?}",
            app.sort
        );
    }
    select_key(&mut app, "q:m");
    for want in [SortMode::Ttl, SortMode::Type, SortMode::Name] {
        press(&mut app, KeyCode::Char('o'));
        assert_eq!(app.sort, want);
        let row = app.selected_row().unwrap();
        assert_eq!(row.key.as_ref().unwrap().name, "q:m", "{want:?}");
    }
}

#[test]
fn o_with_no_keys_or_a_cursor_past_the_end_does_not_panic() {
    let mut app = browser(":", &[]);
    for _ in 0..4 {
        press(&mut app, KeyCode::Char('o'));
        render_sizes(&mut app);
    }
    assert_eq!(app.sort, SortMode::Ttl);

    let mut app = browser(":", &["a", "b"]);
    app.tree_state.select(Some(99));
    press(&mut app, KeyCode::Char('o'));
    render_sizes(&mut app);
}

#[test]
fn the_sort_label_fits_or_is_cut_at_every_size() {
    let mut app = browser_with(
        "/",
        vec![
            typed("svc/b", KeyType::Hash, 9),
            typed("svc/a", KeyType::String, -1),
        ],
    );
    press(&mut app, KeyCode::Char('o'));
    assert!(render(&mut app, 140, 12).contains("by ttl"));
    press(&mut app, KeyCode::Char('o'));
    assert!(render(&mut app, 140, 12).contains("by type"));
    // A tiny terminal still draws.
    let tiny = render(&mut app, 10, 5);
    assert_eq!(tiny.lines().count(), 5);
    render_sizes(&mut app);
}

#[test]
fn folders_left_open_under_another_separator_are_ignored() {
    // More keys than the browser auto-expands, so only restored paths open.
    let names: Vec<String> = (0..250).map(|i| format!("app/user/{i}")).collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let mut app = browser("/", &[]);
    app.expanded.extend(
        [
            "app:user",
            "app:",
            "",
            ":",
            "app/user/does/not/exist",
            "\u{0}",
        ]
        .iter()
        .map(|s| s.to_string()),
    );
    app.on_msg(Msg::Keys {
        warnings: vec![],
        dbsize: refs.len() as u64,
        keys: keys(&refs),
        truncated: false,
        pattern: "*".into(),
    });
    assert_eq!(app.rows.len(), 1, "only the collapsed app folder");
    assert_eq!(app.rows[0].folder_path.as_deref(), Some("app"));
    render_sizes(&mut app);
    // Opening it by hand still works alongside the stale paths.
    app.tree_state.select(Some(0));
    press(&mut app, KeyCode::Right);
    assert!(app.rows.len() > 1, "{:?}", app.rows.len());
    press(&mut app, KeyCode::Char('o'));
    render_sizes(&mut app);
}

#[test]
fn marking_a_folder_with_a_glob_separator_takes_only_its_keys() {
    let mut app = browser("*", &["a*1", "a*2", "ab*3", "a", "a1"]);
    select_folder(&mut app, "a");
    press(&mut app, KeyCode::Char('m'));
    let mut marked: Vec<&str> = app.marked.iter().map(String::as_str).collect();
    marked.sort();
    // `a` is the folder's own name as a key, which marking a folder includes.
    assert_eq!(marked, ["a", "a*1", "a*2"]);
    press(&mut app, KeyCode::Char('m'));
    assert!(app.marked.is_empty(), "a second m unmarks them");
}

#[test]
fn marking_an_empty_named_folder_takes_the_doubled_separator_keys() {
    let mut app = browser(":", &["ns::x", "ns::y", "ns:z"]);
    select_folder(&mut app, "ns:");
    press(&mut app, KeyCode::Char('m'));
    let mut marked: Vec<&str> = app.marked.iter().map(String::as_str).collect();
    marked.sort();
    assert_eq!(marked, ["ns::x", "ns::y"]);
}

#[test]
fn a_new_key_prefix_uses_multi_character_and_empty_segment_folders() {
    let mut app = browser("::", &["crate::mod::Item", "crate::Top"]);
    select_folder(&mut app, "crate::mod");
    assert_eq!(app.new_key_prefix(), "crate::mod::");
    select_key(&mut app, "crate::Top");
    assert_eq!(app.new_key_prefix(), "crate::");

    let mut app = browser(":", &["ns::x"]);
    select_key(&mut app, "ns::x");
    assert_eq!(app.new_key_prefix(), "ns::");
    select_folder(&mut app, "ns:");
    assert_eq!(app.new_key_prefix(), "ns::");

    let mut app = browser(":", &[]);
    assert_eq!(app.new_key_prefix(), "", "nothing selected");
    press(&mut app, KeyCode::Char('n'));
    let Some(Modal::Form { fields, .. }) = &app.modal else {
        panic!("no new-key form")
    };
    assert_eq!(fields[0].input.value(), "");
}

#[test]
fn the_palette_reveals_keys_under_a_double_colon_or_glob_separator() {
    for (sep, target, folders) in [
        (
            "::",
            "crate::net::tcp::Listener",
            vec!["crate", "crate::net", "crate::net::tcp"],
        ),
        ("*", "one*two*three", vec!["one", "one*two"]),
        (":", "lead::x", vec!["lead", "lead:"]),
    ] {
        // Over the auto-expand limit, so the palette has to open the folders.
        let mut names: Vec<String> = (0..210).map(|i| format!("filler{sep}{i}")).collect();
        names.push(target.to_string());
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let mut app = browser(sep, &refs);
        assert!(app.expanded.is_empty(), "{sep}");
        ctrl(&mut app, 'p');
        type_str(&mut app, target);
        press(&mut app, KeyCode::Enter);
        assert_eq!(
            app.current.as_ref().map(|k| k.name.as_str()),
            Some(target),
            "{sep}: {}",
            app.status
        );
        for folder in folders {
            assert!(app.expanded.contains(folder), "{sep}: {folder}");
        }
        assert_eq!(
            app.selected_row()
                .and_then(|r| r.key.as_ref())
                .map(|k| k.name.as_str()),
            Some(target)
        );
    }
}

#[test]
fn the_palette_ranks_a_word_after_the_profile_separator_first() {
    let mut app = browser("~", &["xcart", "user~cart"]);
    ctrl(&mut app, 'p');
    type_str(&mut app, "cart");
    let Some(Modal::Palette(state)) = &app.modal else {
        panic!("no palette")
    };
    let first_key = state
        .hits
        .iter()
        .find(|h| matches!(h.target, rediscope::palette::Target::Key { .. }))
        .unwrap();
    assert_eq!(first_key.text, "user~cart");
    render_sizes(&mut app);
}

// ---- memory report ------------------------------------------------------------

fn prefixes(rollup: &Rollup, depth: usize) -> Vec<String> {
    let mut rows: Vec<String> = rollup.rows(depth).into_iter().map(|r| r.prefix).collect();
    rows.sort();
    rows
}

#[test]
fn memory_prefixes_follow_glob_and_multi_character_separators() {
    let mut r = Rollup::with_separator("*");
    for k in ["a*b*1", "a*b*2", "a*c", "a:b:c"] {
        r.count(k);
        r.measure(k, 10);
    }
    assert_eq!(prefixes(&r, 1), ["a*", "a:b:c"]);
    assert_eq!(prefixes(&r, 2), ["a*b*", "a*c", "a:b:c"]);

    let mut r = Rollup::with_separator("::");
    for k in ["x::y::z", "x:::w", "::lead", "trail::"] {
        r.count(k);
        r.measure(k, 1);
    }
    assert_eq!(prefixes(&r, 1), ["::", "trail::", "x::"]);
}

#[test]
fn memory_prefixes_keep_empty_segments_and_survive_odd_depths() {
    let mut r = Rollup::with_separator(":");
    for k in ["a::b", "a:b", ":x", "", "a:"] {
        r.count(k);
        r.measure(k, 3);
    }
    assert_eq!(
        prefixes(&r, 1),
        ["", ":", "a:"],
        "the empty name is its own prefix"
    );
    assert_eq!(prefixes(&r, 2), ["", ":x", "a:", "a::", "a:b"]);
    for depth in [0, 1, 7, 100, usize::MAX] {
        let rows = r.rows(depth);
        let keys: u64 = rows.iter().map(|row| row.keys).sum();
        assert_eq!(keys, 5, "depth {depth}");
    }
    // An empty separator reads as `:`, like the tree.
    let mut blank = Rollup::with_separator("");
    blank.count("u:1");
    blank.measure("u:1", 1);
    blank.count("solo");
    blank.measure("solo", 1);
    assert_eq!(prefixes(&blank, 1), ["solo", "u:"]);
}

// ---- escapes against separators that share their characters ------------------

/// Encode each raw name the way the server's bytes reach the tree.
fn encoded(raw: &[&[u8]]) -> Vec<String> {
    raw.iter().map(|b| encode_key(b)).collect()
}

fn tree_of(names: &[String], separator: &str) -> Tree {
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    Tree::build(&keys(&refs), separator, SortMode::Name)
}

/// A backslash separator next to escapes and doubled backslashes: the split
/// always lands on a whole doubled backslash, never on the one that starts
/// an escape or on half of a pair.
#[test]
fn a_backslash_separator_never_splits_an_escape_or_half_a_pair() {
    let names = encoded(&[
        b"a\\\xff",    // `a\\\xff`: a literal backslash, then a byte
        b"\xff\\b",    // `\xff\\b`
        b"\\\xfe",     // `\\\xfe`: the separator first
        b"a\xffb",     // `a\xffb`: an escape and no separator at all
        b"a\\\\b",     // two literal backslashes: an empty folder between
        b"\x0a\\\x0b", // control bytes are valid UTF-8, so not escaped
    ]);
    assert_eq!(names[0], "a\\\\\\xff");
    assert_eq!(names[3], "a\\xffb");
    let tree = tree_of(&names, "\\");
    assert_consistent(&tree, "\\\\");
    let mut drawn: Vec<(Option<String>, String)> = placements(&tree)
        .into_iter()
        .map(|(folder, label, _)| (folder, label))
        .collect();
    drawn.sort();
    let s = |t: &str| t.to_string();
    let mut want = vec![
        (Some(s("a")), s("\\xff")),
        (Some(s("\\xff")), s("b")),
        (Some(s("")), s("\\xfe")),
        (None, s("a\\xffb")),
        (Some(s("a\\\\")), s("b")),
        (Some(s("\n")), s("\u{b}")),
    ];
    want.sort();
    assert_eq!(drawn, want);
    for row in open_rows(&tree) {
        if let Some(path) = &row.folder_path {
            // A path never ends in the lone backslash of an escape or half of
            // a doubled one.
            let trailing = path.chars().rev().take_while(|c| *c == '\\').count();
            assert_eq!(trailing % 2, 0, "{path:?}");
        }
    }
}

/// With `\x` as the separator the tree matches `\\x`, a literal backslash
/// and an `x`. A literal backslash followed by an escaped byte, `\\\xff`,
/// also holds `\\x` from its second character, but the raw name has no `\x`
/// in it.
#[test]
fn a_backslash_x_separator_is_not_found_across_a_pair_and_an_escape() {
    let names = encoded(&[b"dir\\xfile", b"dir\\xother", b"\\\xff", b"k\\\xfe\\xv"]);
    assert_eq!(names[2], "\\\\\\xff");
    let tree = tree_of(&names, "\\x");
    assert_consistent(&tree, "\\\\x");
    let drawn: Vec<(Option<String>, String, String)> = placements(&tree);
    assert!(
        drawn.contains(&(Some("dir".into()), "file".into(), names[0].clone())),
        "{drawn:?}"
    );
    assert!(
        drawn.contains(&(Some("k\\\\\\xfe".into()), "v".into(), names[3].clone())),
        "only the real `\\x` splits: {drawn:?}"
    );
    // And the folder it is drawn in is one folder, not `k\` holding `fe`.
    assert!(
        !tree.all_folder_paths().contains(&"k\\".to_string()),
        "{:?}",
        tree.all_folder_paths()
    );
    assert!(
        drawn.contains(&(None, names[2].clone(), names[2].clone())),
        "the raw name holds no `\\x`, so it stays at the root: {drawn:?}"
    );
}

/// `x`, `0` and `f` all appear inside `\xNN` escapes. A raw name without the
/// separator in it should not be cut into a folder ending in half an escape.
#[test]
fn separators_made_of_escape_characters_do_not_cut_escapes() {
    let mut wrong = Vec::new();
    for (separator, raw, want_folder, want_label) in [
        ("x", b"a\xffb".as_slice(), None, "a\\xffb"),
        ("x", b"box\xff", Some("bo"), "\\xff"),
        ("0", b"a\xf0b", None, "a\\xf0b"),
        ("0", b"v0\xf0", Some("v"), "\\xf0"),
        ("f", b"a\xffb", None, "a\\xffb"),
        ("f", b"of\xfe", Some("o"), "\\xfe"),
    ] {
        let names = encoded(&[raw]);
        let tree = tree_of(&names, separator);
        let drawn = placements(&tree);
        let want = [(
            want_folder.map(str::to_string),
            want_label.to_string(),
            names[0].clone(),
        )];
        if drawn != want {
            wrong.push(format!(
                "separator {separator:?}, name {:?}: drawn {drawn:?}, want {want:?}",
                names[0]
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

// ---- multibyte separators and names -------------------------------------------

#[test]
fn split_last_cuts_on_whole_multibyte_separators() {
    use rediscope::tree::split_last;
    assert_eq!(split_last("a→b→c", "→"), Some(("a→b", "c")));
    assert_eq!(split_last("🙂🙂x", "🙂"), Some(("🙂", "x")));
    assert_eq!(split_last("x🙂", "🙂"), Some(("x", "")));
    assert_eq!(split_last("🙂", "🙂"), Some(("", "")));
    assert_eq!(
        split_last("🙃x", "🙂"),
        None,
        "one byte short of the separator"
    );
    // An emoji whose bytes start like the separator's.
    assert_eq!(split_last("😀a🙂b", "🙂"), Some(("😀a", "b")));
    assert_eq!(split_last("家👨‍👩‍👧→🍕", "→"), Some(("家👨‍👩‍👧", "🍕")));
    assert_eq!(split_last("", "→"), None);
    // Overlapping emoji separators split from the left.
    assert_eq!(split_last("a🙂🙂🙂b", "🙂🙂"), Some(("a", "🙂b")));
}

#[test]
fn emoji_and_arrow_separators_build_trees_with_whole_labels() {
    for separator in ["→", "🙂", "🙂🙂"] {
        let names: Vec<String> = [
            "🍕{s}topping{s}1",
            "🍕{s}topping{s}2",
            "{s}lead",
            "trail{s}",
            "日本{s}東京",
            "plain🙃",
        ]
        .iter()
        .map(|n| n.replace("{s}", separator))
        .collect();
        let tree = tree_of(&names, separator);
        assert_consistent(&tree, separator);
        let mut paths = sorted_paths(&tree);
        paths.sort();
        let mut want: Vec<String> = ["", "🍕", "🍕{s}topping", "trail", "日本"]
            .iter()
            .map(|p| p.replace("{s}", separator))
            .collect();
        want.sort();
        assert_eq!(paths, want, "{separator}");
        for row in open_rows(&tree) {
            // Labels are whole characters and never hold the separator.
            assert!(
                !row.label.contains(separator),
                "{separator}: {:?}",
                row.label
            );
        }
    }
}

#[test]
fn a_new_key_prefix_under_an_emoji_separator_ends_with_it() {
    let mut app = browser("🙂", &["🍕🙂topping🙂1", "🍕🙂solo", "flat😀"]);
    select_folder(&mut app, "🍕🙂topping");
    assert_eq!(app.new_key_prefix(), "🍕🙂topping🙂");
    select_key(&mut app, "🍕🙂topping🙂1");
    assert_eq!(app.new_key_prefix(), "🍕🙂topping🙂");
    select_key(&mut app, "🍕🙂solo");
    assert_eq!(app.new_key_prefix(), "🍕🙂");
    select_key(&mut app, "flat😀");
    assert_eq!(app.new_key_prefix(), "", "a root key starts at the root");
    select_folder(&mut app, "🍕");
    press(&mut app, KeyCode::Char('n'));
    let Some(Modal::Form { fields, .. }) = &app.modal else {
        panic!("no new-key form")
    };
    assert_eq!(fields[0].input.value(), "🍕🙂");
    render_sizes(&mut app);

    let mut app = browser("→", &["a→b→c"]);
    select_key(&mut app, "a→b→c");
    assert_eq!(app.new_key_prefix(), "a→b→");
    render_sizes(&mut app);
}

#[test]
fn the_palette_ranks_a_word_after_an_emoji_or_arrow_separator_first() {
    for separator in ["→", "🙂"] {
        let names = ["xcart".to_string(), format!("user{separator}cart")];
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let mut app = browser(separator, &refs);
        ctrl(&mut app, 'p');
        type_str(&mut app, "cart");
        let Some(Modal::Palette(state)) = &app.modal else {
            panic!("no palette")
        };
        let first_key = state
            .hits
            .iter()
            .find(|h| matches!(h.target, rediscope::palette::Target::Key { .. }))
            .unwrap();
        assert_eq!(first_key.text, names[1], "{separator}");
        render_sizes(&mut app);
        // Typing the separator itself into the query does not panic.
        type_str(&mut app, separator);
        render_sizes(&mut app);
        press(&mut app, KeyCode::Esc);
    }
}
