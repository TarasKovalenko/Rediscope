//! Namespace tree over key names split on a separator (`:` unless the profile
//! says otherwise), plus a flattened view for rendering. Expansion state lives
//! outside the tree (keyed by folder path) so it survives a rescan.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::HashSet;

use crate::redis_client::KeyInfo;

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
    folders: BTreeMap<String, Builder>,
    leaves: Vec<KeyInfo>,
}

impl Tree {
    /// Group `keys` into folders on `separator`. A folder's path is its
    /// segments joined with the same separator, so it reads as a key prefix.
    pub fn build(keys: &[KeyInfo], separator: &str) -> Self {
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
            roots: finish(root, None, separator),
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
fn finish(b: Builder, parent: Option<&str>, separator: &str) -> Vec<Node> {
    let mut out: Vec<Node> = Vec::new();
    for (name, child) in b.folders {
        let path = match parent {
            None => name.clone(),
            Some(parent) => format!("{parent}{separator}{name}"),
        };
        let children = finish(child, Some(&path), separator);
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
    leaves.sort_by(|a, b| a.name.cmp(&b.name));
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
        let tree = Tree::build(&[key("a:b:c:1")], ":");
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
        let tree = Tree::build(&keys, "/");
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
            let tree = Tree::build(&keys, sep);
            assert!(tree.all_folder_paths().contains(&want.to_string()), "{sep}");
            let rows = tree.visible(&tree.all_folder_paths().into_iter().collect());
            let leaf = rows.iter().find(|r| r.key.is_some()).unwrap();
            assert_eq!(leaf.key.as_ref().unwrap().name, names[0], "{sep}");
            assert!(!leaf.label.contains(sep), "{sep}: {}", leaf.label);
        }
    }

    #[test]
    fn a_multi_character_separator_is_matched_whole() {
        let tree = Tree::build(&[key("a:b::c"), key("a:b::d")], "::");
        assert_eq!(tree.all_folder_paths(), ["a:b"]);
        assert_eq!(labels(&tree), ["a:b", "c", "d"]);
    }

    #[test]
    fn an_empty_separator_means_the_default() {
        let tree = Tree::build(&[key("user:1")], "");
        assert_eq!(tree.all_folder_paths(), ["user"]);
    }
}
