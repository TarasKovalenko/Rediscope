//! Small XML helpers used for read-only structured value previews.

use quick_xml::events::Event;
use quick_xml::{Reader, Writer};

fn looks_like_xml(text: &str) -> bool {
    let text = text.trim_start();
    text.starts_with('<') && !text.starts_with("<!--")
}

/// Whether `text` is a complete XML document with one root element.
pub fn is_xml(text: &str) -> bool {
    pretty(text).is_some()
}

/// Indent a complete XML document without changing text or attribute content.
/// Invalid XML and fragments are left to the normal plain-text renderer.
pub fn pretty(text: &str) -> Option<String> {
    if !looks_like_xml(text) {
        return None;
    }

    let mut reader = Reader::from_str(text);
    reader.config_mut().trim_text(false);
    let mut writer = Writer::new_with_indent(Vec::with_capacity(text.len() + 64), b' ', 2);
    let mut depth = 0usize;
    let mut roots = 0usize;

    loop {
        let event = reader.read_event().ok()?;
        match &event {
            Event::Start(_) => {
                if depth == 0 {
                    roots += 1;
                }
                depth += 1;
            }
            Event::Empty(_) if depth == 0 => roots += 1,
            Event::End(_) => depth = depth.checked_sub(1)?,
            Event::Text(text) if text.bytes().all(|b| b.is_ascii_whitespace()) => continue,
            Event::Text(_) | Event::CData(_) if depth == 0 => return None,
            Event::Eof => break,
            _ => {}
        }
        writer.write_event(event.into_owned()).ok()?;
    }

    if roots != 1 || depth != 0 {
        return None;
    }
    String::from_utf8(writer.into_inner()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_compact_data_protection_xml() {
        let compact = r#"<key id="abc"><creationDate>2026-09-04</creationDate><descriptor><masterKey requiresEncryption="true"><value>secret</value></masterKey></descriptor></key>"#;
        let formatted = pretty(compact).unwrap();
        assert_eq!(
            formatted,
            "<key id=\"abc\">\n  <creationDate>2026-09-04</creationDate>\n  <descriptor>\n    <masterKey requiresEncryption=\"true\">\n      <value>secret</value>\n    </masterKey>\n  </descriptor>\n</key>"
        );
    }

    #[test]
    fn rejects_plain_text_fragments_and_broken_xml() {
        assert!(pretty("plain text").is_none());
        assert!(pretty("<one/><two/>").is_none());
        assert!(pretty("<one/>trailing text").is_none());
        assert!(pretty("<one><two></one>").is_none());
    }

    #[test]
    fn preserves_text_and_cdata_content() {
        let input = "<root><text> a &amp; b </text><![CDATA[<not-a-tag>]]></root>";
        let formatted = pretty(input).unwrap();
        assert!(formatted.contains("<text> a &amp; b </text>"));
        assert!(formatted.contains("<![CDATA[<not-a-tag>]]>"));
    }
}
