//! The `ctrl+p` palette: every action on the current screen, and every loaded
//! key or saved server, behind one fuzzy-matched input line.
//!
//! An action is not a second implementation of what its key does. Choosing one
//! replays the keystroke it is bound to, so the palette can never drift from
//! the real bindings and every check behind a key still applies.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::input::InputBuf;
use crate::redis_client::KeyType;

/// Most rows the palette ranks and shows. Typing narrows the rest.
pub const RESULT_LIMIT: usize = 200;

/// One palette action: the key it replays, and how it reads in the list.
#[derive(Clone, Copy, Debug)]
pub struct Command {
    /// The binding as the help screen writes it (`M`, `ctrl+d`).
    pub keys: &'static str,
    pub label: &'static str,
    code: KeyCode,
    ctrl: bool,
}

impl Command {
    const fn key(keys: &'static str, c: char, label: &'static str) -> Self {
        Self {
            keys,
            label,
            code: KeyCode::Char(c),
            ctrl: false,
        }
    }

    const fn ctrl(keys: &'static str, c: char, label: &'static str) -> Self {
        Self {
            keys,
            label,
            code: KeyCode::Char(c),
            ctrl: true,
        }
    }

    /// The keystroke that runs this action.
    pub fn event(&self) -> KeyEvent {
        let mods = if self.ctrl {
            KeyModifiers::CONTROL
        } else {
            KeyModifiers::NONE
        };
        KeyEvent::new(self.code, mods)
    }
}

/// The key browser's actions, in the order an empty query lists them.
pub const BROWSER: &[Command] = &[
    Command::key("/", '/', "Search keys by pattern"),
    Command::key("F", 'F', "Find keys whose value contains some text"),
    Command::key("r", 'r', "Refresh keys and the open value"),
    Command::key("n", 'n', "New key"),
    Command::key("e", 'e', "Edit the value or the selected element"),
    Command::key("a", 'a', "Add an element to the open collection"),
    Command::key("x", 'x', "Delete the selected element"),
    Command::key("f", 'f', "Filter the open collection's elements"),
    Command::key("+", '+', "Load more keys or elements"),
    Command::key("v", 'v', "View the value as plain, gzip, msgpack, hex …"),
    Command::key("t", 't', "Set or clear a TTL"),
    Command::key("R", 'R', "Rename key"),
    Command::key("D", 'D', "Delete key"),
    Command::key("y", 'y', "Copy the key name to the clipboard"),
    Command::key("m", 'm', "Mark the key, or every key under the folder"),
    Command::key("u", 'u', "Clear every mark"),
    Command::key("C", 'C', "Copy the key to another name, database or server"),
    Command::key("w", 'w', "Export the marked keys to a file"),
    Command::key("I", 'I', "Import keys from a file"),
    Command::key("L", 'L', "Run a Lua script"),
    Command::key("i", 'i', "Server info"),
    Command::key("M", 'M', "Namespace memory report"),
    Command::key("P", 'P', "Pub/sub feed"),
    Command::key("N", 'N', "Keyspace event feed"),
    Command::key("W", 'W', "Command monitor"),
    Command::key(
        "S",
        'S',
        "Consumer groups of the stream · similar vector set elements",
    ),
    Command::key("Q", 'Q', "Run a RediSearch query"),
    Command::key(":", ':', "Raw command console"),
    Command::key("p", 'p', "Colour theme"),
    Command::ctrl("ctrl+d", 'd', "Switch database"),
    Command::ctrl("ctrl+w", 'w', "Unlock or lock production writes"),
    Command::ctrl("ctrl+n", 'n', "Back to the server list"),
    Command::key("?", '?', "Keybindings"),
    Command::key("q", 'q', "Quit"),
];

/// The server list's actions.
pub const CONNECTIONS: &[Command] = &[
    Command::key("n", 'n', "New connection"),
    Command::key("e", 'e', "Edit connection"),
    Command::key("c", 'c', "Duplicate connection"),
    Command::key("d", 'd', "Delete connection"),
    Command::key("T", 'T', "Test the connection without opening it"),
    Command::key("J", 'J', "Move the connection down"),
    Command::key("K", 'K', "Move the connection up"),
    Command::key("/", '/', "Filter servers by name or host"),
    Command::key("p", 'p', "Colour theme"),
    Command::key("?", '?', "Keybindings"),
    Command::key("q", 'q', "Quit"),
];

/// What a palette row does when chosen.
#[derive(Clone, Debug, PartialEq)]
pub enum Target {
    /// Index into the screen's command table.
    Action(usize),
    /// A loaded key, by name.
    Key { name: String, kind: KeyType },
    /// A saved server, by profile name.
    Server(String),
}

/// A ranked row, with the characters of its text the query matched.
#[derive(Clone, Debug)]
pub struct Hit {
    pub target: Target,
    pub text: String,
    /// Char indices into `text`, for highlighting.
    pub matched: Vec<usize>,
    score: i64,
}

pub struct PaletteState {
    pub input: InputBuf,
    pub selected: usize,
    pub hits: Vec<Hit>,
    /// How many candidates matched, including those past [`RESULT_LIMIT`].
    pub matches: usize,
    pub commands: &'static [Command],
}

impl PaletteState {
    pub fn new(commands: &'static [Command]) -> Self {
        Self {
            input: InputBuf::new(""),
            selected: 0,
            hits: Vec::new(),
            matches: 0,
            commands,
        }
    }

    pub fn selected_hit(&self) -> Option<&Hit> {
        self.hits.get(self.selected)
    }

    /// Re-rank against the current query. `keys` are the loaded key names and
    /// types, `servers` the saved profile names.
    pub fn refresh<'a>(
        &mut self,
        keys: impl IntoIterator<Item = (&'a str, KeyType)>,
        servers: impl IntoIterator<Item = &'a str>,
    ) {
        let query = self.input.value();
        let query = query.trim();
        let mut hits: Vec<Hit> = Vec::new();
        for (i, c) in self.commands.iter().enumerate() {
            // The binding itself is searchable, so `M` finds the memory report.
            let text = format!("{}  {}", c.label, c.keys);
            if let Some((score, matched)) = fuzzy(query, &text) {
                // Keys outnumber actions by thousands, so a query that
                // matches an action as well as a key means the action. A long
                // label is not a worse match, either.
                let score = score + ACTION_LEAD + text.chars().count() as i64 / 8;
                hits.push(Hit {
                    target: Target::Action(i),
                    text,
                    matched,
                    score,
                });
            }
        }
        // An empty query lists the actions; keys only appear once asked for.
        if !query.is_empty() {
            for (name, kind) in keys {
                if let Some((score, matched)) = fuzzy(query, name) {
                    hits.push(Hit {
                        target: Target::Key {
                            name: name.to_string(),
                            kind,
                        },
                        text: name.to_string(),
                        matched,
                        score,
                    });
                }
            }
        }
        for name in servers {
            if let Some((score, matched)) = fuzzy(query, name) {
                hits.push(Hit {
                    target: Target::Server(name.to_string()),
                    text: name.to_string(),
                    matched,
                    score,
                });
            }
        }
        self.matches = hits.len();
        if !query.is_empty() {
            // Stable, so equal scores keep actions first and keys in order.
            hits.sort_by_key(|h| std::cmp::Reverse(h.score));
        }
        hits.truncate(RESULT_LIMIT);
        self.hits = hits;
        self.selected = 0;
    }
}

/// Score `text` against `query`, case-insensitively: every query character
/// has to appear in order. Runs of consecutive characters, and characters
/// right after a separator (`:` `_` `-` `.` `/` space) or at the very start,
/// score well; gaps and a long text cost a little. An empty query matches
/// everything with score 0. Returns the score and the matched char indices.
pub fn fuzzy(query: &str, text: &str) -> Option<(i64, Vec<usize>)> {
    if query.is_empty() {
        return Some((0, Vec::new()));
    }
    let text: Vec<char> = text.chars().collect();
    let lower: Vec<char> = text.iter().map(|c| fold(*c)).collect();
    let query: Vec<char> = query.chars().map(fold).collect();
    if query.len() > text.len() {
        return None;
    }

    // The best alignment ending at each text position, per query character.
    // `best[j][i]`: highest score with query[..=j] matched and query[j] at i.
    const NONE: i64 = i64::MIN / 2;
    let n = text.len();
    let mut best = vec![vec![NONE; n]; query.len()];
    let mut from = vec![vec![usize::MAX; n]; query.len()];
    for (j, &qc) in query.iter().enumerate() {
        // Best of the previous row strictly before i - 1: every way to reach
        // i with a gap, kept as a running maximum so the walk stays linear.
        let mut gap_best = NONE;
        let mut gap_at = usize::MAX;
        for i in 0..n {
            if j > 0 && i >= 2 && best[j - 1][i - 2] > gap_best {
                gap_best = best[j - 1][i - 2];
                gap_at = i - 2;
            }
            if lower[i] != qc {
                continue;
            }
            let bonus = position_bonus(&text, i);
            if j == 0 {
                // Where the match starts matters, less so at a word boundary.
                let late = (i as i64).min(15);
                best[j][i] = 16 + bonus - if bonus > 0 { late / 4 } else { late };
                continue;
            }
            let mut score = NONE;
            if i > 0 && best[j - 1][i - 1] > NONE {
                score = best[j - 1][i - 1] + 16 + bonus.max(8);
                from[j][i] = i - 1;
            }
            if gap_best > NONE {
                let gapped = gap_best + 16 + bonus - GAP;
                if gapped > score {
                    score = gapped;
                    from[j][i] = gap_at;
                }
            }
            best[j][i] = score;
        }
    }

    let last = query.len() - 1;
    let (mut end, &top) = best[last]
        .iter()
        .enumerate()
        .max_by_key(|&(i, s)| (*s, std::cmp::Reverse(i)))?;
    if top <= NONE {
        return None;
    }
    let mut matched = vec![0; query.len()];
    for j in (0..=last).rev() {
        matched[j] = end;
        if j > 0 {
            end = from[j][end];
        }
    }
    // A shorter text is a closer match when the characters were the same.
    let score = top - (n as i64 / 8);
    Some((score, matched))
}

fn fold(c: char) -> char {
    c.to_lowercase().next().unwrap_or(c)
}

fn position_bonus(text: &[char], i: usize) -> i64 {
    if i == 0 {
        return 12;
    }
    let before = text[i - 1];
    if matches!(before, ':' | '_' | '-' | '.' | '/' | ' ' | '|') {
        10
    } else if before.is_lowercase() && text[i].is_uppercase() {
        // camelCase boundary.
        8
    } else {
        0
    }
}

/// How far an action is ranked ahead of a key matching as well.
const ACTION_LEAD: i64 = 8;

/// What skipping over characters between two matches costs.
const GAP: i64 = 6;

#[cfg(test)]
mod tests {
    use super::*;

    fn score(q: &str, t: &str) -> i64 {
        fuzzy(q, t)
            .unwrap_or_else(|| panic!("{q:?} should match {t:?}"))
            .0
    }

    #[test]
    fn every_query_character_has_to_appear_in_order() {
        assert!(fuzzy("usr", "user:42").is_some());
        assert!(fuzzy("USR", "user:42").is_some(), "case-insensitive");
        assert!(fuzzy("rsu", "user:42").is_none());
        assert!(fuzzy("userx", "user").is_none());
        assert_eq!(fuzzy("", "anything").unwrap().0, 0);
    }

    #[test]
    fn reports_the_characters_it_matched() {
        let (_, at) = fuzzy("u42", "user:42").unwrap();
        assert_eq!(at, vec![0, 5, 6]);
    }

    #[test]
    fn consecutive_runs_beat_scattered_characters() {
        assert!(score("sess", "session:1") > score("sess", "s:e:s:s"));
    }

    #[test]
    fn characters_after_a_separator_beat_ones_mid_word() {
        // `up` as u·ser:p·rofile beats `up` buried in "cup".
        assert!(score("up", "user:profile") > score("up", "cupboard"));
        assert!(score("sp", "user:session:profile") > score("sp", "wasp"));
    }

    #[test]
    fn picks_the_best_alignment_not_the_first() {
        // The first `p` is mid-word; the one after the colon scores better.
        let (_, at) = fuzzy("pro", "app:profile").unwrap();
        assert_eq!(at, vec![4, 5, 6]);
    }

    #[test]
    fn a_shorter_text_wins_a_tie() {
        assert!(score("user", "user") > score("user", "user:with:a:long:tail"));
    }

    #[test]
    fn handles_multibyte_text() {
        let (_, at) = fuzzy("čš", "ключ:čaj:šum").unwrap();
        assert_eq!(at, vec![5, 9]);
    }

    fn keys<'a>(names: &'a [&'a str]) -> impl Iterator<Item = (&'a str, KeyType)> {
        names.iter().map(|n| (*n, KeyType::String))
    }

    #[test]
    fn an_empty_query_lists_every_action_in_order_and_no_keys() {
        let mut p = PaletteState::new(BROWSER);
        p.refresh(keys(&["user:1"]), []);
        assert_eq!(p.hits.len(), BROWSER.len());
        assert_eq!(p.hits[0].target, Target::Action(0));
        assert!(p.hits.iter().all(|h| matches!(h.target, Target::Action(_))));
    }

    #[test]
    fn a_query_ranks_keys_and_actions_together() {
        let mut p = PaletteState::new(BROWSER);
        p.input.set("memory");
        p.refresh(keys(&["cache:memory:1", "other"]), []);
        let first = p.selected_hit().unwrap();
        let label = match first.target {
            Target::Action(i) => BROWSER[i].label,
            _ => panic!("expected the memory report first, got {first:?}"),
        };
        assert_eq!(label, "Namespace memory report");
        assert!(p.hits.iter().any(|h| h.text == "cache:memory:1"));
        assert!(!p.hits.iter().any(|h| h.text == "other"));
    }

    #[test]
    fn results_are_capped_but_the_match_count_is_not() {
        let many: Vec<String> = (0..RESULT_LIMIT + 50).map(|i| format!("k:{i}")).collect();
        let mut p = PaletteState::new(BROWSER);
        p.input.set("k:");
        p.refresh(many.iter().map(|n| (n.as_str(), KeyType::String)), []);
        assert_eq!(p.hits.len(), RESULT_LIMIT);
        assert!(p.matches >= RESULT_LIMIT + 50);
    }

    #[test]
    fn saved_servers_are_listed_on_the_server_screen() {
        let mut p = PaletteState::new(CONNECTIONS);
        p.input.set("prd");
        p.refresh([], ["local", "production"]);
        assert_eq!(
            p.selected_hit().unwrap().target,
            Target::Server("production".into())
        );
    }

    #[test]
    fn every_action_replays_a_distinct_keystroke() {
        for table in [BROWSER, CONNECTIONS] {
            let mut seen = std::collections::HashSet::new();
            for c in table {
                let e = c.event();
                assert!(seen.insert((e.code, e.modifiers)), "{} twice", c.keys);
            }
        }
    }
}
