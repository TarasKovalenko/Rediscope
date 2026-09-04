//! JSON helpers shared by the value pane and the string editor. Values that
//! happen to hold JSON get pretty-printed, coloured and validated; everything
//! else is left exactly as the server returned it.

use serde_json::Value;

/// How a string value was stored, so an edit can be written back in the same
/// shape the key already had.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsonMode {
    /// Not JSON. The buffer is written verbatim.
    None,
    /// JSON stored on one line: minify on save.
    Compact,
    /// JSON already spread over several lines: save what the user typed.
    Pretty,
}

impl JsonMode {
    pub fn is_json(self) -> bool {
        self != Self::None
    }
}

/// Cheap pre-filter: only `{...}` and `[...]` are worth parsing. Bare numbers
/// and quoted words are valid JSON too, but treating every string value as a
/// JSON document would be more surprising than useful.
fn looks_like_json(s: &str) -> bool {
    let t = s.trim_start();
    t.starts_with('{') || t.starts_with('[')
}

pub fn parse(s: &str) -> Option<Value> {
    if !looks_like_json(s) {
        return None;
    }
    serde_json::from_str(s).ok()
}

/// Classify a stored value so the editor knows how to write it back.
pub fn mode(s: &str) -> JsonMode {
    match parse(s) {
        None => JsonMode::None,
        Some(_) if s.trim().contains('\n') => JsonMode::Pretty,
        Some(_) => JsonMode::Compact,
    }
}

/// Indented JSON, or the input untouched when it is not JSON.
pub fn pretty(s: &str) -> String {
    parse(s)
        .and_then(|v| serde_json::to_string_pretty(&v).ok())
        .unwrap_or_else(|| s.to_string())
}

/// One-line JSON, or the input untouched when it is not JSON.
pub fn minify(s: &str) -> String {
    parse(s)
        .and_then(|v| serde_json::to_string(&v).ok())
        .unwrap_or_else(|| s.to_string())
}

/// Validate an edited buffer, reporting where it broke.
pub fn check(s: &str) -> Result<(), String> {
    match serde_json::from_str::<Value>(s) {
        Ok(_) => Ok(()),
        // serde repeats the position at the end of its message; say it once.
        Err(e) => {
            let msg = e.to_string();
            let msg = msg.split(" at line ").next().unwrap_or(&msg).to_string();
            Err(format!("line {}, column {}: {msg}", e.line(), e.column()))
        }
    }
}

/// One coloured piece of a JSON document.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Token {
    Key,
    Str,
    Number,
    Literal,
    Punct,
}

/// Split pretty-printed JSON into coloured spans, one `Vec` per line. A string
/// followed by `:` is an object key; every other string is a value.
pub fn highlight(text: &str) -> Vec<Vec<(Token, String)>> {
    text.lines()
        .map(|line| {
            let mut spans: Vec<(Token, String)> = Vec::new();
            let chars: Vec<char> = line.chars().collect();
            let mut i = 0;
            while i < chars.len() {
                let c = chars[i];
                match c {
                    '"' => {
                        let start = i;
                        i += 1;
                        while i < chars.len() {
                            match chars[i] {
                                '\\' => i += 2,
                                '"' => {
                                    i += 1;
                                    break;
                                }
                                _ => i += 1,
                            }
                        }
                        let text: String = chars[start..i.min(chars.len())].iter().collect();
                        // A key is a string whose next non-space character is a colon.
                        let is_key = chars[i..].iter().find(|c| !c.is_whitespace()) == Some(&':');
                        spans.push((if is_key { Token::Key } else { Token::Str }, text));
                    }
                    '-' | '0'..='9' => {
                        let start = i;
                        while i < chars.len()
                            && matches!(chars[i], '-' | '+' | '.' | 'e' | 'E' | '0'..='9')
                        {
                            i += 1;
                        }
                        spans.push((Token::Number, chars[start..i].iter().collect()));
                    }
                    't' | 'f' | 'n' => {
                        let start = i;
                        while i < chars.len() && chars[i].is_ascii_alphabetic() {
                            i += 1;
                        }
                        spans.push((Token::Literal, chars[start..i].iter().collect()));
                    }
                    _ => {
                        let start = i;
                        while i < chars.len()
                            && !matches!(chars[i], '"' | '-' | '0'..='9' | 't' | 'f' | 'n')
                        {
                            i += 1;
                        }
                        spans.push((Token::Punct, chars[start..i].iter().collect()));
                    }
                }
            }
            spans
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_stored_values() {
        assert_eq!(mode(r#"{"a":1}"#), JsonMode::Compact);
        assert_eq!(mode("{\n  \"a\": 1\n}"), JsonMode::Pretty);
        assert_eq!(mode("plain text"), JsonMode::None);
        assert_eq!(mode("{not json"), JsonMode::None);
        // A bare scalar is valid JSON but stays a plain string here.
        assert_eq!(mode("42"), JsonMode::None);
    }

    #[test]
    fn round_trips_between_shapes() {
        // Key order is the author's, not alphabetical (serde_json preserve_order).
        let compact = r#"{"b":[1,2],"a":null}"#;
        let spread = pretty(compact);
        assert!(spread.contains("\n  \"b\""));
        assert_eq!(minify(&spread), compact);
        assert_eq!(pretty("nope"), "nope");
    }

    #[test]
    fn reports_where_parsing_failed() {
        assert!(check(r#"{"a":1}"#).is_ok());
        let err = check("{\n  \"a\": ,\n}").unwrap_err();
        assert_eq!(err, "line 2, column 8: expected value", "{err}");
    }

    #[test]
    fn highlights_keys_apart_from_string_values() {
        let lines = highlight("{\n  \"a\": \"b\",\n  \"n\": 1.5e3,\n  \"t\": true\n}");
        let kinds: Vec<Token> = lines[1].iter().map(|(t, _)| *t).collect();
        assert_eq!(
            kinds,
            vec![
                Token::Punct,
                Token::Key,
                Token::Punct,
                Token::Str,
                Token::Punct
            ]
        );
        assert!(
            lines[2]
                .iter()
                .any(|(t, s)| *t == Token::Number && s == "1.5e3")
        );
        assert!(
            lines[3]
                .iter()
                .any(|(t, s)| *t == Token::Literal && s == "true")
        );
    }

    #[test]
    fn highlighting_preserves_every_character() {
        let text = pretty(r#"{"a":[1,"two",false,null],"b":{"c":-3.5}}"#);
        for (line, spans) in text.lines().zip(highlight(&text)) {
            let joined: String = spans.iter().map(|(_, s)| s.as_str()).collect();
            assert_eq!(joined, line);
        }
    }
}
