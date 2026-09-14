//! Namespace tree over key names split on a separator (`:` unless the profile
//! says otherwise), plus a flattened view for rendering. Expansion state lives
//! outside the tree (keyed by folder path) so it survives a rescan.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::redis_client::KeyInfo;

/// How the keys inside each folder are ordered. Folders themselves always sort
/// by name, so the shape of the tree does not move when the mode changes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortMode {
    /// Natural order: `2` before `11`, ignoring case.
    #[default]
    Name,
    /// Soonest expiry first, keys without a TTL last.
    Ttl,
    /// Grouped by type name, then by name.
    Type,
}

impl SortMode {
    /// The mode `o` moves on to.
    pub fn next(self) -> Self {
        match self {
            Self::Name => Self::Ttl,
            Self::Ttl => Self::Type,
            Self::Type => Self::Name,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::Ttl => "ttl",
            Self::Type => "type",
        }
    }

    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }

    fn compare(self, a: &KeyInfo, b: &KeyInfo) -> Ordering {
        let first = match self {
            Self::Name => Ordering::Equal,
            // A negative TTL is no expiry (-1) or a key already gone (-2):
            // either way it sorts after every key that has one.
            Self::Ttl => match (a.ttl >= 0, b.ttl >= 0) {
                (true, true) => a.ttl.cmp(&b.ttl),
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                (false, false) => Ordering::Equal,
            },
            Self::Type => a.kind.name().cmp(b.kind.name()),
        };
        first.then_with(|| natural_cmp(&a.name, &b.name))
    }
}

/// Compare the way a person reads names: runs of digits by their value, so
/// `user:2` comes before `user:11`, and everything else ignoring case. Names
/// that only differ in case or leading zeros fall back to plain byte order,
/// so the result is still a total order and a sort is stable across runs.
pub fn natural_cmp(a: &str, b: &str) -> Ordering {
    natural_folded(a, b).then_with(|| a.cmp(b))
}

/// The case-folded, digit-aware part of [`natural_cmp`]. It runs for every
/// comparison of a sort over tens of thousands of names on the UI thread, so
/// it allocates nothing: digit runs are compared as slices of the names, and
/// ASCII, the common case, is folded byte by byte.
fn natural_folded(a: &str, b: &str) -> Ordering {
    let (x, y) = (a.as_bytes(), b.as_bytes());
    let start = shared_start(x, y);
    let (mut i, mut j) = (start, start);
    loop {
        let (c, d) = match (x.get(i), y.get(j)) {
            (None, None) => return Ordering::Equal,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (Some(&c), Some(&d)) => (c, d),
        };
        if c.is_ascii_digit() && d.is_ascii_digit() {
            let (left, next_i) = digit_run(a, i);
            let (right, next_j) = digit_run(b, j);
            let (left, right) = (left.trim_start_matches('0'), right.trim_start_matches('0'));
            // No parsing, so a run longer than any integer still compares.
            let order = left.len().cmp(&right.len()).then_with(|| left.cmp(right));
            if order != Ordering::Equal {
                return order;
            }
            (i, j) = (next_i, next_j);
        } else if c.is_ascii() && d.is_ascii() {
            let order = c.to_ascii_lowercase().cmp(&d.to_ascii_lowercase());
            if order != Ordering::Equal {
                return order;
            }
            (i, j) = (i + 1, j + 1);
        } else {
            // Both indices sit on a character boundary: every step before
            // this one moved over whole characters.
            let c = a[i..].chars().next().expect("not at the end");
            let d = b[j..].chars().next().expect("not at the end");
            // Lowercasing may give more than one character (`İ` is `i` and
            // a combining dot), and those compare in turn.
            let order = c.to_lowercase().cmp(d.to_lowercase());
            if order != Ordering::Equal {
                return order;
            }
            (i, j) = (i + c.len_utf8(), j + d.len_utf8());
        }
    }
}

/// Where comparing `x` and `y` has to start: past the bytes they share, since
/// identical text folds and compares equal, but backed up to the start of
/// the character and of the digit run that the first difference falls in.
/// Keys in one folder tend to share a long prefix, so this skips most of
/// the work.
fn shared_start(x: &[u8], y: &[u8]) -> usize {
    let n = x.len().min(y.len());
    let mut k = 0;
    while k + 8 <= n && x[k..k + 8] == y[k..k + 8] {
        k += 8;
    }
    while k < n && x[k] == y[k] {
        k += 1;
    }
    // A continuation byte in one means the same lead byte in both, so the
    // character boundary is the same for both.
    while k < x.len() && (x[k] & 0xC0) == 0x80 {
        k -= 1;
    }
    while k > 0 && x[k - 1].is_ascii_digit() {
        k -= 1;
    }
    k
}

/// The run of ASCII digits starting at byte `at`, and the byte after it.
fn digit_run(s: &str, at: usize) -> (&str, usize) {
    let len = s.as_bytes()[at..]
        .iter()
        .take_while(|b| b.is_ascii_digit())
        .count();
    (&s[at..at + len], at + len)
}

#[derive(Debug)]
pub enum Node {
    Folder {
        name: String,
        path: String,
        children: Vec<Node>,
        leaves: usize,
    },
    Leaf {
        label: String,
        key: KeyInfo,
    },
}

#[derive(Debug, Clone)]
pub struct VisibleRow {
    pub depth: usize,
    pub label: String,
    /// `Some(path)` for folders, used to toggle expansion.
    pub folder_path: Option<String>,
    pub expanded: bool,
    pub leaves: usize,
    pub key: Option<KeyInfo>,
}

#[derive(Debug, Default)]
pub struct Tree {
    roots: Vec<Node>,
}

/// Intermediate builder node so children keep insertion-independent sort order.
#[derive(Default)]
struct Builder {
    folders: HashMap<String, Builder>,
    leaves: Vec<KeyInfo>,
}

impl Tree {
    /// Group `keys` into folders on `separator`. A folder's path is its
    /// segments joined with the same separator, so it reads as a key prefix.
    /// Folders come before keys at each level and sort by name; the keys
    /// follow `sort`.
    pub fn build(keys: &[KeyInfo], separator: &str, sort: SortMode) -> Self {
        let separator = effective(separator);
        let separator = separator.as_ref();
        let mut root = Builder::default();
        for k in keys {
            let mut cur = &mut root;
            if let Some((folders, _)) = split_last(&k.name, separator) {
                for part in folders.split(separator) {
                    // Looked up before inserting, so a folder already seen
                    // costs no allocation for its name.
                    if !cur.folders.contains_key(part) {
                        cur.folders.insert(part.to_string(), Builder::default());
                    }
                    cur = cur.folders.get_mut(part).expect("inserted above");
                }
            }
            cur.leaves.push(k.clone());
        }
        Self {
            roots: finish(root, None, separator, sort),
        }
    }

    /// Depth-first walk honouring `expanded`, producing render-ready rows.
    pub fn visible(&self, expanded: &HashSet<String>) -> Vec<VisibleRow> {
        let mut out = Vec::new();
        walk(&self.roots, 0, expanded, &mut out);
        out
    }

    /// Every folder path in the tree — used by "expand all".
    pub fn all_folder_paths(&self) -> Vec<String> {
        let mut out = Vec::new();
        collect_paths(&self.roots, &mut out);
        out
    }
}

/// The separator as it appears in a key name. An empty one would split
/// between every character, so it means the default. Names reach the tree
/// through [`encode_key`](crate::redis_client::encode_key), which doubles a
/// literal backslash, so a separator holding one is matched in that same
/// doubled form.
pub fn effective(separator: &str) -> Cow<'_, str> {
    if separator.is_empty() {
        Cow::Borrowed(":")
    } else if separator.contains('\\') {
        Cow::Owned(separator.replace('\\', "\\\\"))
    } else {
        Cow::Borrowed(separator)
    }
}

/// `name` cut at the last separator a left-to-right split finds, as the path
/// of the folder the key is drawn in and the key's label. `None` for a name
/// at the root. `separator` is already [`effective`].
///
/// This is not `rsplit_once`: a separator that can overlap itself, such as
/// `::` in `a:::b`, splits into `a` and `:b` from the left but `a:` and `b`
/// from the right, and the tree splits from the left.
pub fn split_last<'a>(name: &'a str, separator: &str) -> Option<(&'a str, &'a str)> {
    let (at, _) = name.match_indices(separator).last()?;
    Some((&name[..at], &name[at + separator.len()..]))
}

/// `parent` is the path of the folder these nodes sit in, `None` at the root.
/// It cannot be an empty string for the root, since a folder may have an
/// empty name (a key starting with the separator).
fn finish(b: Builder, parent: Option<&str>, separator: &str, sort: SortMode) -> Vec<Node> {
    let mut out: Vec<Node> = Vec::new();
    let mut folders: Vec<(String, Builder)> = b.folders.into_iter().collect();
    folders.sort_by(|(a, _), (b, _)| natural_cmp(a, b));
    for (name, child) in folders {
        let path = match parent {
            None => name.clone(),
            Some(parent) => format!("{parent}{separator}{name}"),
        };
        let children = finish(child, Some(&path), separator, sort);
        let leaves = children
            .iter()
            .map(|c| match c {
                Node::Folder { leaves, .. } => *leaves,
                Node::Leaf { .. } => 1,
            })
            .sum();
        out.push(Node::Folder {
            name,
            path,
            children,
            leaves,
        });
    }
    let mut leaves: Vec<KeyInfo> = b.leaves;
    leaves.sort_by(|a, b| sort.compare(a, b));
    for k in leaves {
        let label = split_last(&k.name, separator)
            .map_or(k.name.as_str(), |(_, label)| label)
            .to_string();
        out.push(Node::Leaf { label, key: k });
    }
    out
}

fn walk(nodes: &[Node], depth: usize, expanded: &HashSet<String>, out: &mut Vec<VisibleRow>) {
    for n in nodes {
        match n {
            Node::Folder {
                name,
                path,
                children,
                leaves,
            } => {
                let is_open = expanded.contains(path);
                out.push(VisibleRow {
                    depth,
                    label: name.clone(),
                    folder_path: Some(path.clone()),
                    expanded: is_open,
                    leaves: *leaves,
                    key: None,
                });
                if is_open {
                    walk(children, depth + 1, expanded, out);
                }
            }
            Node::Leaf { label, key } => out.push(VisibleRow {
                depth,
                label: label.clone(),
                folder_path: None,
                expanded: false,
                leaves: 0,
                key: Some(key.clone()),
            }),
        }
    }
}

fn collect_paths(nodes: &[Node], out: &mut Vec<String>) {
    for n in nodes {
        if let Node::Folder { path, children, .. } = n {
            out.push(path.clone());
            collect_paths(children, out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redis_client::KeyType;

    fn key(name: &str) -> KeyInfo {
        KeyInfo {
            name: name.into(),
            kind: KeyType::String,
            ttl: -1,
        }
    }

    #[test]
    fn groups_by_namespace_and_counts_leaves() {
        let tree = Tree::build(
            &[key("user:1"), key("user:2"), key("session:a"), key("flat")],
            ":",
            SortMode::Name,
        );
        let mut open = HashSet::new();
        let rows = tree.visible(&open);
        // Two collapsed folders plus the flat key.
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].label, "session");
        assert_eq!(rows[1].label, "user");
        assert_eq!(rows[1].leaves, 2);
        assert_eq!(rows[2].label, "flat");

        open.insert("user".to_string());
        let rows = tree.visible(&open);
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[2].key.as_ref().unwrap().name, "user:1");
        assert_eq!(rows[2].depth, 1);
    }

    #[test]
    fn nests_deeply_and_lists_folder_paths() {
        let tree = Tree::build(&[key("a:b:c:1")], ":", SortMode::Name);
        let mut paths = tree.all_folder_paths();
        paths.sort();
        assert_eq!(paths, vec!["a", "a:b", "a:b:c"]);
    }

    fn labels(tree: &Tree) -> Vec<String> {
        let all: HashSet<String> = tree.all_folder_paths().into_iter().collect();
        tree.visible(&all).into_iter().map(|r| r.label).collect()
    }

    #[test]
    fn any_separator_splits_the_tree_and_joins_the_paths() {
        let names = ["app/user/1", "app/user/2", "app/queue", "a:b"];
        let keys: Vec<KeyInfo> = names.iter().map(|n| key(n)).collect();
        let tree = Tree::build(&keys, "/", SortMode::Name);
        let mut paths = tree.all_folder_paths();
        paths.sort();
        assert_eq!(paths, ["app", "app/user"]);
        // A colon is just a character once the separator is something else.
        assert_eq!(labels(&tree), ["app", "user", "1", "2", "queue", "a:b"]);

        for (sep, names, want) in [
            (".", ["com.example.api", "com.example.web"], "com.example"),
            (
                "::",
                ["crate::mod::Item", "crate::mod::Other"],
                "crate::mod",
            ),
            ("|", ["tenant|42|cart", "tenant|42|seen"], "tenant|42"),
            // A glob character is only a character here.
            ("*", ["a*b*c", "a*b*d"], "a*b"),
        ] {
            let keys: Vec<KeyInfo> = names.iter().map(|n| key(n)).collect();
            let tree = Tree::build(&keys, sep, SortMode::Name);
            assert!(tree.all_folder_paths().contains(&want.to_string()), "{sep}");
            let rows = tree.visible(&tree.all_folder_paths().into_iter().collect());
            let leaf = rows.iter().find(|r| r.key.is_some()).unwrap();
            assert_eq!(leaf.key.as_ref().unwrap().name, names[0], "{sep}");
            assert!(!leaf.label.contains(sep), "{sep}: {}", leaf.label);
        }
    }

    fn typed(name: &str, kind: KeyType, ttl: i64) -> KeyInfo {
        KeyInfo {
            name: name.into(),
            kind,
            ttl,
        }
    }

    #[test]
    fn names_sort_naturally_ignoring_case() {
        let mut names = vec![
            "user:11", "user:2", "User:3", "user:1", "user:10", "user:02", "b", "A", "a",
        ];
        names.sort_by(|a, b| natural_cmp(a, b));
        assert_eq!(
            names,
            [
                "A", "a", "b", "user:1", "user:02", "user:2", "User:3", "user:10", "user:11"
            ]
        );
        assert_eq!(natural_cmp("x9", "x10"), Ordering::Less);
        assert_eq!(natural_cmp("v1.10", "v1.9"), Ordering::Greater);
        // Longer than any integer type, still compared by value.
        assert_eq!(
            natural_cmp("id:99999999999999999999999", "id:100000000000000000000000"),
            Ordering::Less
        );
        assert_eq!(natural_cmp("same", "same"), Ordering::Equal);
    }

    #[test]
    fn a_shared_prefix_ending_inside_a_number_or_a_character_still_compares_whole() {
        // The bytes first differ inside the run of digits.
        assert_eq!(natural_cmp("a123", "a13"), Ordering::Greater);
        assert_eq!(natural_cmp("a0099", "a0100"), Ordering::Less);
        assert_eq!(natural_cmp("x:12:y", "x:12:Y"), "x:12:y".cmp("x:12:Y"));
        // `é` and `è` share their first byte; `É` folds to `é`.
        assert_eq!(natural_cmp("cafè", "café"), Ordering::Less);
        assert_eq!(natural_cmp("cafÉ", "cafè"), Ordering::Greater);
        assert_eq!(natural_cmp("k\u{212A}2", "kk10"), Ordering::Less);
        assert_eq!(super::shared_start(b"ab12", b"ab13"), 2);
        assert_eq!(super::shared_start("é".as_bytes(), "è".as_bytes()), 0);
        assert_eq!(super::shared_start(b"abc", b"abc"), 3);
        assert_eq!(super::shared_start(b"", b"x"), 0);
    }

    #[test]
    fn leaves_follow_the_sort_mode_and_folders_stay_in_name_order() {
        let keys = [
            typed("job:10", KeyType::String, -1),
            typed("job:9", KeyType::Hash, 30),
            typed("job:2", KeyType::List, 5),
            typed("job:1", KeyType::Hash, -1),
            typed("job:z10:x", KeyType::String, 1),
            typed("job:z9:x", KeyType::String, 99),
        ];
        let order = |sort: SortMode| labels(&Tree::build(&keys, ":", sort));
        // Folders first, `z9` before `z10`, whatever the mode.
        assert_eq!(
            order(SortMode::Name),
            ["job", "z9", "x", "z10", "x", "1", "2", "9", "10"]
        );
        assert_eq!(
            order(SortMode::Ttl),
            ["job", "z9", "x", "z10", "x", "2", "9", "1", "10"],
            "soonest first, no TTL last, then by name"
        );
        assert_eq!(
            order(SortMode::Type),
            ["job", "z9", "x", "z10", "x", "1", "9", "2", "10"],
            "hash, list, string"
        );
    }

    #[test]
    fn sort_modes_cycle_and_have_names() {
        let mut mode = SortMode::default();
        let mut seen = Vec::new();
        for _ in 0..3 {
            seen.push(mode.name());
            mode = mode.next();
        }
        assert_eq!(seen, ["name", "ttl", "type"]);
        assert_eq!(mode, SortMode::Name);
    }

    #[test]
    fn a_multi_character_separator_is_matched_whole() {
        let tree = Tree::build(&[key("a:b::c"), key("a:b::d")], "::", SortMode::Name);
        assert_eq!(tree.all_folder_paths(), ["a:b"]);
        assert_eq!(labels(&tree), ["a:b", "c", "d"]);
    }

    #[test]
    fn an_empty_separator_means_the_default() {
        let tree = Tree::build(&[key("user:1")], "", SortMode::Name);
        assert_eq!(tree.all_folder_paths(), ["user"]);
    }
}
