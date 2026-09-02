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
}
