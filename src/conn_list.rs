//! The server list as rows: saved profiles, optionally under one level of
//! collapsible group headers. Pure, so the ordering and the selection rules can
//! be tested without a terminal.
//!
//! The list is small, so it is rebuilt on demand rather than cached, and the
//! cursor is a row index into whatever [`build_rows`] returns right now.

use std::collections::{BTreeMap, HashSet};

use crate::config::{Connection, ConnectionView};

/// One line of the server list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnRow {
    /// A group header. `count` is the members shown under it when expanded:
    /// every member, or only the matching ones while a filter is applied.
    Group {
        name: String,
        count: usize,
        expanded: bool,
    },
    /// A profile, by index into `Store::connections`. `depth` is 1 under a
    /// header and 0 at the root.
    Connection { index: usize, depth: u8 },
}

impl ConnRow {
    pub fn connection_index(&self) -> Option<usize> {
        match self {
            Self::Connection { index, .. } => Some(*index),
            Self::Group { .. } => None,
        }
    }

    pub fn group(&self) -> Option<&str> {
        match self {
            Self::Group { name, .. } => Some(name),
            Self::Connection { .. } => None,
        }
    }
}

/// Lay out `matches` (indices into `conns`, in stored order) as rows.
///
/// Flat, or grouped with no grouped match, lists the matches as they are.
/// Grouped lists the headers sorted by name case-insensitively, each followed
/// by its members in stored order when expanded, then the ungrouped matches:
/// folders before leaves, as in the key tree. `filtering` opens every group
/// that has a match, whatever `collapsed` says, so a filter can find a server
/// inside a folded group.
pub fn build_rows(
    conns: &[Connection],
    matches: &[usize],
    view: ConnectionView,
    collapsed: &HashSet<String>,
    filtering: bool,
) -> Vec<ConnRow> {
    let flat = || {
        matches
            .iter()
            .map(|&index| ConnRow::Connection { index, depth: 0 })
            .collect()
    };
    if view == ConnectionView::Flat {
        return flat();
    }
    // Keyed by (folded, exact) so "Billing" and "billing" sort together but
    // stay distinct groups, in the same order on every run.
    let mut groups: BTreeMap<(String, &str), Vec<usize>> = BTreeMap::new();
    let mut root = Vec::new();
    for &index in matches {
        let Some(conn) = conns.get(index) else {
            continue;
        };
        match conn.group_name() {
            Some(name) => groups
                .entry((name.to_lowercase(), name))
                .or_default()
                .push(index),
            None => root.push(index),
        }
    }
    if groups.is_empty() {
        return flat();
    }

    let mut rows = Vec::with_capacity(matches.len() + groups.len());
    for ((_, name), members) in groups {
        let expanded = filtering || !collapsed.contains(name);
        rows.push(ConnRow::Group {
            name: name.to_string(),
            count: members.len(),
            expanded,
        });
        if expanded {
            rows.extend(
                members
                    .into_iter()
                    .map(|index| ConnRow::Connection { index, depth: 1 }),
            );
        }
    }
    rows.extend(
        root.into_iter()
            .map(|index| ConnRow::Connection { index, depth: 0 }),
    );
    rows
}

/// How many distinct groups the profiles name.
pub fn group_count(conns: &[Connection]) -> usize {
    conns
        .iter()
        .filter_map(Connection::group_name)
        .collect::<HashSet<_>>()
        .len()
}

/// The row showing profile `index`, if it is visible.
pub fn row_of_connection(rows: &[ConnRow], index: usize) -> Option<usize> {
    rows.iter()
        .position(|r| r.connection_index() == Some(index))
}

/// The header row of group `name`, if it is shown.
pub fn row_of_group(rows: &[ConnRow], name: &str) -> Option<usize> {
    rows.iter().position(|r| r.group() == Some(name))
}

/// Where the cursor starts: the first profile, so `Enter` connects straight
/// away, or the first header when every group is folded.
pub fn first_connection_row(rows: &[ConnRow]) -> Option<usize> {
    rows.iter()
        .position(|r| r.connection_index().is_some())
        .or_else(|| (!rows.is_empty()).then_some(0))
}

/// The header above the row at `row`: itself for a header, the enclosing
/// header for a member, `None` at the root.
pub fn header_of(rows: &[ConnRow], row: usize) -> Option<usize> {
    match rows.get(row)? {
        ConnRow::Group { .. } => Some(row),
        ConnRow::Connection { depth: 0, .. } => None,
        ConnRow::Connection { .. } => rows[..row]
            .iter()
            .rposition(|r| matches!(r, ConnRow::Group { .. })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn(name: &str, group: Option<&str>) -> Connection {
        Connection {
            name: name.into(),
            group: group.map(str::to_string),
            ..Default::default()
        }
    }

    /// `checkout` {dev, prod}, `billing` {prod}, ungrouped `local`, stored
    /// interleaved so the ordering has something to do.
    fn fixture() -> Vec<Connection> {
        vec![
            conn("checkout-dev", Some("checkout")),
            conn("local", None),
            conn("billing-prod", Some("billing")),
            conn("checkout-prod", Some("checkout")),
        ]
    }

    fn all(conns: &[Connection]) -> Vec<usize> {
        (0..conns.len()).collect()
    }

    fn header(name: &str, count: usize, expanded: bool) -> ConnRow {
        ConnRow::Group {
            name: name.into(),
            count,
            expanded,
        }
    }

    fn member(index: usize) -> ConnRow {
        ConnRow::Connection { index, depth: 1 }
    }

    fn root(index: usize) -> ConnRow {
        ConnRow::Connection { index, depth: 0 }
    }

    #[test]
    fn flat_view_lists_matches_in_order() {
        let conns = fixture();
        let rows = build_rows(
            &conns,
            &[0, 2, 3],
            ConnectionView::Flat,
            &HashSet::new(),
            false,
        );
        assert_eq!(rows, [root(0), root(2), root(3)]);
    }

    #[test]
    fn grouped_view_sorts_groups_then_root_connections() {
        let conns = fixture();
        let rows = build_rows(
            &conns,
            &all(&conns),
            ConnectionView::Grouped,
            &HashSet::new(),
            false,
        );
        assert_eq!(
            rows,
            [
                header("billing", 1, true),
                member(2),
                header("checkout", 2, true),
                member(0),
                member(3),
                root(1),
            ]
        );
    }

    #[test]
    fn groups_sort_case_insensitively_but_stay_distinct() {
        let conns = vec![
            conn("x", Some("beta")),
            conn("y", Some("Alpha")),
            conn("z", Some("alpha")),
        ];
        let rows = build_rows(
            &conns,
            &all(&conns),
            ConnectionView::Grouped,
            &HashSet::new(),
            false,
        );
        let names: Vec<&str> = rows.iter().filter_map(ConnRow::group).collect();
        assert_eq!(names, ["Alpha", "alpha", "beta"]);
        assert_eq!(group_count(&conns), 3);
    }

    #[test]
    fn collapsed_group_hides_members_but_keeps_header_count() {
        let conns = fixture();
        let collapsed = HashSet::from(["checkout".to_string()]);
        let rows = build_rows(
            &conns,
            &all(&conns),
            ConnectionView::Grouped,
            &collapsed,
            false,
        );
        assert_eq!(
            rows,
            [
                header("billing", 1, true),
                member(2),
                header("checkout", 2, false),
                root(1),
            ]
        );
    }

    #[test]
    fn filtering_forces_matching_groups_open() {
        let conns = fixture();
        let collapsed = HashSet::from(["checkout".to_string(), "billing".to_string()]);
        // The filter kept one checkout member and nothing from billing.
        let rows = build_rows(&conns, &[3], ConnectionView::Grouped, &collapsed, true);
        assert_eq!(rows, [header("checkout", 1, true), member(3)]);
        // Without the filter the same collapsed set folds it again.
        let rows = build_rows(&conns, &[3], ConnectionView::Grouped, &collapsed, false);
        assert_eq!(rows, [header("checkout", 1, false)]);
    }

    #[test]
    fn no_groups_grouped_equals_flat() {
        let conns = vec![conn("a", None), conn("b", Some("   ")), conn("c", None)];
        let collapsed = HashSet::new();
        let grouped = build_rows(
            &conns,
            &all(&conns),
            ConnectionView::Grouped,
            &collapsed,
            false,
        );
        let flat = build_rows(
            &conns,
            &all(&conns),
            ConnectionView::Flat,
            &collapsed,
            false,
        );
        assert_eq!(grouped, flat);
        assert_eq!(group_count(&conns), 0);
    }

    #[test]
    fn blank_group_is_ungrouped() {
        let conns = vec![conn("blank", Some("")), conn("spaces", Some("  \t"))];
        let rows = build_rows(
            &conns,
            &all(&conns),
            ConnectionView::Grouped,
            &HashSet::new(),
            false,
        );
        assert_eq!(rows, [root(0), root(1)]);
    }

    #[test]
    fn selection_maps_between_rows_and_connections() {
        let conns = fixture();
        let rows = build_rows(
            &conns,
            &all(&conns),
            ConnectionView::Grouped,
            &HashSet::new(),
            false,
        );
        assert_eq!(first_connection_row(&rows), Some(1), "skips the header");
        assert_eq!(row_of_connection(&rows, 3), Some(4));
        assert_eq!(row_of_connection(&rows, 9), None);
        assert_eq!(row_of_group(&rows, "checkout"), Some(2));
        assert_eq!(header_of(&rows, 4), Some(2), "a member's header");
        assert_eq!(header_of(&rows, 2), Some(2), "a header is its own");
        assert_eq!(header_of(&rows, 5), None, "root rows have none");
        assert_eq!(header_of(&rows, 99), None);
    }

    #[test]
    fn the_cursor_starts_on_a_header_only_when_every_group_is_folded() {
        let conns = vec![conn("a", Some("g"))];
        let collapsed = HashSet::from(["g".to_string()]);
        let rows = build_rows(&conns, &[0], ConnectionView::Grouped, &collapsed, false);
        assert_eq!(first_connection_row(&rows), Some(0));
        assert_eq!(first_connection_row(&[]), None);
    }
}
