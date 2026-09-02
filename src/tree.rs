//! Namespace tree over key names split on `:`, plus a flattened view for
//! rendering. Expansion state lives outside the tree (keyed by folder path) so
//! it survives a rescan.

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
    pub fn build(keys: &[KeyInfo]) -> Self {
        let mut root = Builder::default();
        for k in keys {
            let parts: Vec<&str> = k.name.split(':').collect();
            let mut cur = &mut root;
            for part in &parts[..parts.len().saturating_sub(1)] {
                cur = cur.folders.entry((*part).to_string()).or_default();
            }
            cur.leaves.push(k.clone());
        }
        Self {
            roots: finish(root, ""),
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

fn finish(b: Builder, prefix: &str) -> Vec<Node> {
    let mut out: Vec<Node> = Vec::new();
    for (name, child) in b.folders {
        let path = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}:{name}")
        };
        let children = finish(child, &path);
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
        let label = k.name.rsplit(':').next().unwrap_or(&k.name).to_string();
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
        let tree = Tree::build(&[key("user:1"), key("user:2"), key("session:a"), key("flat")]);
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
        let tree = Tree::build(&[key("a:b:c:1")]);
        let mut paths = tree.all_folder_paths();
        paths.sort();
        assert_eq!(paths, vec!["a", "a:b", "a:b:c"]);
    }
}
