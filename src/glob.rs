//! Redis glob patterns, matched locally.
//!
//! `SCAN`, `HSCAN`, `SSCAN` and `ZSCAN` take a `MATCH` pattern, but lists,
//! streams and the command monitor have nothing like it. Filtering those
//! happens here, and it follows the rules of Redis's own `stringmatchlen` so a
//! pattern means the same thing wherever it is typed: `*` any run of bytes,
//! `?` one byte, `[abc]`, `[^abc]` and `[a-z]` classes, and `\` to take the
//! next character literally.

/// Whether `text` matches `pattern`, byte for byte.
pub fn matches(pattern: &[u8], text: &[u8]) -> bool {
    let (mut p, mut t) = (0, 0);
    // Where to resume after the last `*`: the pattern just past it, and the
    // text position it is currently standing in for.
    let mut star: Option<(usize, usize)> = None;
    while t < text.len() {
        if p < pattern.len() {
            if pattern[p] == b'*' {
                while p < pattern.len() && pattern[p] == b'*' {
                    p += 1;
                }
                if p == pattern.len() {
                    return true;
                }
                star = Some((p, t));
                continue;
            }
            if let Some(next) = one(pattern, p, text[t]) {
                p = next;
                t += 1;
                continue;
            }
        }
        // No match here: let the last `*` swallow one more byte and retry.
        match star {
            Some((after, at)) => {
                p = after;
                t = at + 1;
                star = Some((after, at + 1));
            }
            None => return false,
        }
    }
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}

/// [`matches`], ignoring ASCII case.
pub fn matches_nocase(pattern: &[u8], text: &[u8]) -> bool {
    matches(&pattern.to_ascii_lowercase(), &text.to_ascii_lowercase())
}

/// Match the single-byte element at `pattern[p]` against `c`, returning the
/// position just past the element when it matches.
fn one(pattern: &[u8], p: usize, c: u8) -> Option<usize> {
    match pattern[p] {
        b'?' => Some(p + 1),
        b'\\' if p + 1 < pattern.len() => (pattern[p + 1] == c).then_some(p + 2),
        b'[' => class(pattern, p + 1, c),
        literal => (literal == c).then_some(p + 1),
    }
}

/// A `[...]` class starting just past the `[`. An unterminated class runs to
/// the end of the pattern, as it does in Redis.
fn class(pattern: &[u8], mut p: usize, c: u8) -> Option<usize> {
    let negate = pattern.get(p) == Some(&b'^');
    if negate {
        p += 1;
    }
    let mut matched = false;
    let end = loop {
        let Some(&b) = pattern.get(p) else {
            break pattern.len();
        };
        if b == b'\\' && p + 1 < pattern.len() {
            p += 1;
            matched |= pattern[p] == c;
        } else if b == b']' {
            break p + 1;
        } else if p + 2 < pattern.len() && pattern[p + 1] == b'-' {
            let (mut lo, mut hi) = (b, pattern[p + 2]);
            if lo > hi {
                std::mem::swap(&mut lo, &mut hi);
            }
            matched |= (lo..=hi).contains(&c);
            p += 2;
        } else {
            matched |= b == c;
        }
        p += 1;
    };
    (matched != negate).then_some(end)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(p: &str, t: &str) -> bool {
        matches(p.as_bytes(), t.as_bytes())
    }

    #[test]
    fn stars_and_questions() {
        assert!(m("*", ""));
        assert!(m("*", "anything"));
        assert!(m("user:*", "user:42"));
        assert!(!m("user:*", "session:42"));
        assert!(m("*:42", "user:42"));
        assert!(m("*er*", "user"));
        assert!(m("a*b*c", "aXXbYYc"));
        assert!(!m("a*b*c", "aXXbYY"));
        assert!(m("a**b", "ab"));
        assert!(m("h?llo", "hello"));
        assert!(!m("h?llo", "hllo"));
        assert!(!m("", "x"));
        assert!(m("", ""));
        assert!(m("*a", "aaa"));
        assert!(!m("?", ""));
    }

    #[test]
    fn classes() {
        assert!(m("h[ae]llo", "hallo"));
        assert!(!m("h[ae]llo", "hillo"));
        assert!(m("h[^e]llo", "hallo"));
        assert!(!m("h[^e]llo", "hello"));
        assert!(m("h[a-c]llo", "hbllo"));
        assert!(m("h[c-a]llo", "hbllo"), "a reversed range still works");
        assert!(!m("h[a-c]llo", "hdllo"));
        assert!(m("[\\]]", "]"));
        assert!(!m("[]", "a"), "an empty class matches nothing");
        assert!(m("[abc", "b"), "an unterminated class runs to the end");
    }

    #[test]
    fn escapes() {
        assert!(m("a\\*b", "a*b"));
        assert!(!m("a\\*b", "aXb"));
        assert!(m("a\\?", "a?"));
        assert!(m("end\\", "end\\"), "a trailing backslash is literal");
    }

    #[test]
    fn bytes_and_case() {
        assert!(matches(b"\x80*", b"\x80\xff"));
        assert!(!m("USER*", "user:1"));
        assert!(matches_nocase(b"USER*", b"user:1"));
    }

    #[test]
    fn pathological_patterns_finish_quickly() {
        let text = "a".repeat(10_000);
        let pattern = format!("{}b", "*a".repeat(50));
        let started = std::time::Instant::now();
        assert!(!m(&pattern, &text));
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }
}
