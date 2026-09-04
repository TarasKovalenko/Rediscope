//! A minimal single-line text buffer with a character-indexed cursor.
//! Unicode-safe: all movement is by `char`, never by byte.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

#[derive(Debug, Clone, Default)]
pub struct InputBuf {
    chars: Vec<char>,
    cursor: usize,
}

impl InputBuf {
    pub fn new(initial: &str) -> Self {
        let chars: Vec<char> = initial.chars().collect();
        let cursor = chars.len();
        Self { chars, cursor }
    }

    pub fn value(&self) -> String {
        self.chars.iter().collect()
    }

    pub fn cursor(&self) -> usize {
        self.cursor
    }

    pub fn clear(&mut self) {
        self.chars.clear();
        self.cursor = 0;
    }

    pub fn set(&mut self, text: &str) {
        *self = Self::new(text);
    }

    /// Returns true when the key was consumed as editing input.
    pub fn handle(&mut self, key: KeyEvent) -> bool {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Char(c) if ctrl => match c {
                'u' => {
                    self.chars.drain(..self.cursor);
                    self.cursor = 0;
                    true
                }
                'k' => {
                    self.chars.truncate(self.cursor);
                    true
                }
                'a' => {
                    self.cursor = 0;
                    true
                }
                'e' => {
                    self.cursor = self.chars.len();
                    true
                }
                'w' => {
                    let mut i = self.cursor;
                    while i > 0 && self.chars[i - 1].is_whitespace() {
                        i -= 1;
                    }
                    while i > 0 && !self.chars[i - 1].is_whitespace() {
                        i -= 1;
                    }
                    self.chars.drain(i..self.cursor);
                    self.cursor = i;
                    true
                }
                _ => false,
            },
            KeyCode::Char(c) => {
                self.chars.insert(self.cursor, c);
                self.cursor += 1;
                true
            }
            KeyCode::Backspace => {
                if self.cursor > 0 {
                    self.cursor -= 1;
                    self.chars.remove(self.cursor);
                }
                true
            }
            KeyCode::Delete => {
                if self.cursor < self.chars.len() {
                    self.chars.remove(self.cursor);
                }
                true
            }
            KeyCode::Left => {
                self.cursor = self.cursor.saturating_sub(1);
                true
            }
            KeyCode::Right => {
                if self.cursor < self.chars.len() {
                    self.cursor += 1;
                }
                true
            }
            KeyCode::Home => {
                self.cursor = 0;
                true
            }
            KeyCode::End => {
                self.cursor = self.chars.len();
                true
            }
            _ => false,
        }
    }
}

/// The outcome of pressing `Tab` on a partly typed word.
#[derive(Debug, PartialEq, Eq)]
pub enum Completion {
    /// Nothing starts with the word; leave it as it is.
    None,
    /// Replace the word with this. Either the single match, or the longest
    /// prefix every match shares.
    Extend(String),
    /// The word is already everything the matches share, so show them instead.
    Choices(Vec<String>),
}

/// Complete `word` against `candidates`, ignoring case in the comparison but
/// keeping the candidate's own spelling in the result.
pub fn complete(word: &str, candidates: &[String]) -> Completion {
    let lower = word.to_ascii_lowercase();
    let hits: Vec<&String> = candidates
        .iter()
        .filter(|c| c.to_ascii_lowercase().starts_with(&lower))
        .collect();
    match hits.as_slice() {
        [] => Completion::None,
        [only] => Completion::Extend((*only).clone()),
        _ => {
            let shared = shared_prefix(&hits);
            if shared.chars().count() > word.chars().count() {
                Completion::Extend(shared)
            } else {
                Completion::Choices(hits.into_iter().cloned().collect())
            }
        }
    }
}

/// The longest prefix every candidate shares, compared without case but
/// returned as the first candidate spells it.
fn shared_prefix(hits: &[&String]) -> String {
    let first: Vec<char> = hits[0].chars().collect();
    let mut len = first.len();
    for hit in &hits[1..] {
        let common = first
            .iter()
            .zip(hit.chars())
            .take_while(|(a, b)| a.eq_ignore_ascii_case(b))
            .count();
        len = len.min(common);
    }
    first[..len].iter().collect()
}

/// Incremental backwards search over the console history, as `ctrl+r` does it
/// in a shell.
#[derive(Debug, Default, Clone)]
pub struct ReverseSearch {
    query: String,
    /// How many matches back from the newest one to show.
    skip: usize,
}

impl ReverseSearch {
    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn push_char(&mut self, c: char, history: &[String]) {
        self.query.push(c);
        self.settle(history);
    }

    pub fn pop_char(&mut self, history: &[String]) {
        self.query.pop();
        self.settle(history);
    }

    /// Step to the next match further back in time, staying put at the oldest.
    pub fn older(&mut self, history: &[String]) {
        if self.matches(history).nth(self.skip + 1).is_some() {
            self.skip += 1;
        }
    }

    /// The history line currently under the search, if any.
    pub fn hit<'a>(&self, history: &'a [String]) -> Option<&'a str> {
        self.matches(history).nth(self.skip).map(String::as_str)
    }

    /// Newest match first.
    fn matches<'a>(&self, history: &'a [String]) -> impl Iterator<Item = &'a String> {
        let needle = self.query.to_ascii_lowercase();
        history
            .iter()
            .rev()
            .filter(move |line| line.to_ascii_lowercase().contains(&needle))
    }

    /// Narrowing or widening the query invalidates a position counted against
    /// the old set of matches.
    fn settle(&mut self, history: &[String]) {
        let count = self.matches(history).count();
        self.skip = self.skip.min(count.saturating_sub(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn press(buf: &mut InputBuf, code: KeyCode) {
        buf.handle(KeyEvent::new(code, KeyModifiers::NONE));
    }

    #[test]
    fn edits_multibyte_text_without_panicking() {
        let mut b = InputBuf::new("héllo");
        press(&mut b, KeyCode::Left);
        press(&mut b, KeyCode::Backspace);
        assert_eq!(b.value(), "hélo");
        assert_eq!(b.cursor(), 3);
    }

    #[test]
    fn ctrl_w_deletes_previous_word() {
        let mut b = InputBuf::new("GET user:1");
        b.handle(KeyEvent::new(KeyCode::Char('w'), KeyModifiers::CONTROL));
        assert_eq!(b.value(), "GET ");
    }
    fn commands() -> Vec<String> {
        ["GET", "GETDEL", "GETRANGE", "SET", "DBSIZE"]
            .iter()
            .map(|s| (*s).to_string())
            .collect()
    }

    #[test]
    fn a_prefix_only_one_command_answers_completes_to_all_of_it() {
        assert_eq!(
            complete("db", &commands()),
            Completion::Extend("DBSIZE".into())
        );
    }

    #[test]
    fn a_prefix_several_commands_share_completes_to_what_they_share() {
        assert_eq!(
            complete("ge", &commands()),
            Completion::Extend("GET".into())
        );
    }

    #[test]
    fn a_word_that_is_already_the_shared_prefix_offers_the_choices() {
        assert_eq!(
            complete("get", &commands()),
            Completion::Choices(vec!["GET".into(), "GETDEL".into(), "GETRANGE".into()])
        );
    }

    #[test]
    fn a_prefix_nothing_matches_leaves_the_word_alone() {
        assert_eq!(complete("zzz", &commands()), Completion::None);
    }

    fn history() -> Vec<String> {
        ["PING", "GET user:1", "DBSIZE", "GET user:2"]
            .iter()
            .map(|s| (*s).to_string())
            .collect()
    }

    #[test]
    fn reverse_search_finds_the_most_recent_match_first() {
        let mut r = ReverseSearch::default();
        r.push_char('g', &history());
        r.push_char('e', &history());
        assert_eq!(r.hit(&history()), Some("GET user:2"));
    }

    #[test]
    fn searching_again_steps_back_to_the_older_match() {
        let h = history();
        let mut r = ReverseSearch::default();
        for c in "get".chars() {
            r.push_char(c, &h);
        }
        r.older(&h);
        assert_eq!(r.hit(&h), Some("GET user:1"));
        // Nothing older matches, so it stays where it is.
        r.older(&h);
        assert_eq!(r.hit(&h), Some("GET user:1"));
    }

    #[test]
    fn a_backspace_widens_the_search_again() {
        let h = history();
        let mut r = ReverseSearch::default();
        for c in "getx".chars() {
            r.push_char(c, &h);
        }
        assert_eq!(r.hit(&h), None);
        r.pop_char(&h);
        assert_eq!(r.hit(&h), Some("GET user:2"));
    }

    #[test]
    fn the_search_ignores_case() {
        let h = history();
        let mut r = ReverseSearch::default();
        for c in "DBS".chars() {
            r.push_char(c, &h);
        }
        assert_eq!(r.hit(&h), Some("DBSIZE"));
    }
}
