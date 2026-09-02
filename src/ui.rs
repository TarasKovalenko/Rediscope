//! All rendering. Reads `App` and draws; never mutates state except the
//! scroll/selection bookkeeping ratatui's stateful widgets require.

use ratatui::prelude::*;
use ratatui::widgets::{
    Block, BorderType, Borders, Cell, Clear, List, ListItem, Paragraph, Row, Table, Wrap,
};

use crate::app::{App, ConsoleState, Field, FieldKind, Focus, Modal, Screen};
use crate::redis_client::{KeyType, KeyValue};

const ACCENT: Color = Color::Rgb(220, 56, 44);
const DIM: Color = Color::Rgb(130, 130, 140);
const PANEL: Color = Color::Rgb(60, 60, 70);

pub fn draw(f: &mut Frame, app: &mut App) {
    let chunks = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(3),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .split(f.area());

    title_bar(f, chunks[0], app);
    match app.screen {
        Screen::Connections => connections(f, chunks[1], app),
        Screen::Browser => browser(f, chunks[1], app),
    }
    status_bar(f, chunks[2], app);
    footer(f, chunks[3], app);

    if app.modal.is_some() {
        modal(f, f.area(), app);
    }
}

fn title_bar(f: &mut Frame, area: Rect, app: &App) {
    let mut spans = vec![
        Span::styled(
            " rediscope ",
            Style::new().bg(ACCENT).fg(Color::White).bold(),
        ),
        Span::raw(" "),
    ];
    if let Some(client) = &app.client {
        let c = &client.conn;
        let scheme = if c.tls { "rediss" } else { "redis" };
        spans.push(Span::styled(
            format!("{}  {scheme}://{}:{}/{}", c.name, c.host, c.port, c.db),
            Style::new().fg(Color::White),
        ));
        if !app.server_line.is_empty() {
            spans.push(Span::styled(
                format!("  ·  {}", app.server_line),
                Style::new().fg(DIM),
            ));
        }
    } else {
        spans.push(Span::styled(
            "a terminal Redis client",
            Style::new().fg(DIM),
        ));
    }
    f.render_widget(Line::from(spans), area);
}

fn connections(f: &mut Frame, area: Rect, app: &mut App) {
    let cols = Layout::horizontal([
        Constraint::Percentage(20),
        Constraint::Min(40),
        Constraint::Percentage(20),
    ])
    .split(area);
    let items: Vec<ListItem> = app
        .store
        .connections
        .iter()
        .map(|c| {
            ListItem::new(Line::from(vec![
                Span::styled(format!("{:<18}", c.name), Style::new().bold()),
                Span::styled(
                    format!(
                        "{}://{}:{}/{}",
                        if c.tls { "rediss" } else { "redis" },
                        c.host,
                        c.port,
                        c.db
                    ),
                    Style::new().fg(DIM),
                ),
            ]))
        })
        .collect();
    let empty = items.is_empty();
    let list = List::new(items)
        .block(panel("Saved connections", true))
        .highlight_style(Style::new().bg(ACCENT).fg(Color::White).bold())
        .highlight_symbol(" ");
    f.render_stateful_widget(list, cols[1], &mut app.conn_state);
    if empty {
        let inner = cols[1].inner(Margin::new(2, 2));
        f.render_widget(
            Paragraph::new("No saved connections yet — press 'n' to add one.")
                .style(Style::new().fg(DIM)),
            inner,
        );
    }
}

fn browser(f: &mut Frame, area: Rect, app: &mut App) {
    let cols =
        Layout::horizontal([Constraint::Percentage(38), Constraint::Percentage(62)]).split(area);
    key_panel(f, cols[0], app);
    value_panel(f, cols[1], app);
}

fn key_panel(f: &mut Frame, area: Rect, app: &mut App) {
    let rows = if app.search.is_some() {
        Layout::vertical([Constraint::Length(3), Constraint::Min(1)]).split(area)
    } else {
        Layout::vertical([Constraint::Length(0), Constraint::Min(1)]).split(area)
    };
    if let Some(buf) = &app.search {
        let text = buf.value();
        f.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled("/", Style::new().fg(ACCENT).bold()),
                Span::raw(text.clone()),
            ]))
            .block(panel("Search pattern (Enter applies, Esc cancels)", true)),
            rows[0],
        );
        f.set_cursor_position((rows[0].x + 2 + buf.cursor() as u16, rows[0].y + 1));
    }

    let focused = app.focus == Focus::Tree;
    let items: Vec<ListItem> = app
        .rows
        .iter()
        .map(|r| {
            let indent = "  ".repeat(r.depth);
            let line = match (&r.folder_path, &r.key) {
                (Some(_), _) => Line::from(vec![
                    Span::raw(indent),
                    Span::styled(if r.expanded { "▾ " } else { "▸ " }, Style::new().fg(DIM)),
                    Span::styled(r.label.clone(), Style::new().fg(Color::White).bold()),
                    Span::styled(format!("  ({})", r.leaves), Style::new().fg(DIM)),
                ]),
                (None, Some(k)) => {
                    let mut spans = vec![
                        Span::raw(indent),
                        Span::styled(
                            format!("{} ", k.kind.badge()),
                            Style::new().fg(type_color(k.kind)).bold(),
                        ),
                        Span::raw(r.label.clone()),
                    ];
                    if k.ttl >= 0 {
                        spans.push(Span::styled(
                            format!("  {}", human_ttl(k.ttl)),
                            Style::new().fg(Color::Yellow),
                        ));
                    }
                    Line::from(spans)
                }
                _ => Line::raw(r.label.clone()),
            };
            ListItem::new(line)
        })
        .collect();

    let mut title = if app.loading {
        "Keys — scanning…".to_string()
    } else {
        format!("Keys — {} shown / {} in db", app.key_count, app.dbsize)
    };
    if app.pattern != "*" {
        title.push_str(&format!("  ·  {}", app.pattern));
    }
    if app.truncated {
        title.push_str("  ·  TRUNCATED");
    }

    let list = List::new(items)
        .block(panel(&title, focused))
        .highlight_style(if focused {
            Style::new().bg(ACCENT).fg(Color::White).bold()
        } else {
            Style::new().bg(PANEL)
        });
    f.render_stateful_widget(list, rows[1], &mut app.tree_state);

    if app.rows.is_empty() && !app.loading {
        f.render_widget(
            Paragraph::new("No keys match. Press / to change the pattern.")
                .style(Style::new().fg(DIM)),
            rows[1].inner(Margin::new(2, 1)),
        );
    }
}

fn value_panel(f: &mut Frame, area: Rect, app: &mut App) {
    let rows = Layout::vertical([Constraint::Length(4), Constraint::Min(1)]).split(area);

    let header = match &app.current {
        Some(k) => {
            let ttl = if k.ttl == -1 {
                "no expiry".to_string()
            } else if k.ttl == -2 {
                "missing".to_string()
            } else {
                human_ttl(k.ttl)
            };
            let size = match &app.value {
                Some(KeyValue::Rows { rows, total, .. }) if (*total as usize) > rows.len() => {
                    format!("   showing {} of {}", rows.len(), total)
                }
                Some(KeyValue::Rows { total, .. }) => format!("   {total} element(s)"),
                Some(KeyValue::Str(s)) => format!("   {} byte(s)", s.len()),
                _ => String::new(),
            };
            Paragraph::new(vec![
                Line::from(Span::styled(k.name.clone(), Style::new().bold())),
                Line::from(vec![
                    Span::styled(k.kind.name(), Style::new().fg(type_color(k.kind)).bold()),
                    Span::styled(format!("   ttl: {ttl}"), Style::new().fg(DIM)),
                    Span::styled(size, Style::new().fg(DIM)),
                ]),
            ])
        }
        None => Paragraph::new(Line::from(Span::styled(
            "Select a key on the left.",
            Style::new().fg(DIM),
        ))),
    };
    f.render_widget(header.block(panel("Key", false)), rows[0]);

    let focused = app.focus == Focus::Value;
    let block = panel("Value", focused);
    match &app.value {
        None => f.render_widget(
            Paragraph::new(if app.current.is_some() {
                "Loading…"
            } else {
                ""
            })
            .style(Style::new().fg(DIM))
            .block(block),
            rows[1],
        ),
        Some(KeyValue::Str(s)) => {
            let text = pretty_json(s);
            f.render_widget(
                Paragraph::new(text)
                    .wrap(Wrap { trim: false })
                    .scroll((app.value_scroll, 0))
                    .block(block),
                rows[1],
            );
        }
        Some(KeyValue::Unsupported(msg)) => f.render_widget(
            Paragraph::new(msg.clone())
                .wrap(Wrap { trim: true })
                .style(Style::new().fg(DIM))
                .block(block),
            rows[1],
        ),
        Some(KeyValue::Rows {
            headers,
            rows: data,
            ..
        }) => {
            let widths: Vec<Constraint> = match headers.len() {
                1 => vec![Constraint::Percentage(100)],
                _ => vec![Constraint::Percentage(35), Constraint::Percentage(65)],
            };
            let header_row = Row::new(
                headers
                    .iter()
                    .map(|h| Cell::from(Span::styled(*h, Style::new().fg(ACCENT).bold())))
                    .collect::<Vec<_>>(),
            );
            let body: Vec<Row> = data
                .iter()
                .map(|r| {
                    Row::new(
                        r.cells
                            .iter()
                            .map(|c| Cell::from(one_line(c)))
                            .collect::<Vec<_>>(),
                    )
                })
                .collect();
            let table = Table::new(body, widths)
                .header(header_row)
                .block(block)
                .row_highlight_style(if focused {
                    Style::new().bg(ACCENT).fg(Color::White).bold()
                } else {
                    Style::new().bg(PANEL)
                });
            f.render_stateful_widget(table, rows[1], &mut app.value_state);
        }
    }
}

fn status_bar(f: &mut Frame, area: Rect, app: &App) {
    let style = if app.status.starts_with("Error") || app.status.contains("failed") {
        Style::new().fg(Color::White).bg(ACCENT)
    } else {
        Style::new().fg(Color::Cyan)
    };
    f.render_widget(
        Paragraph::new(Span::styled(format!(" {}", app.status), style)),
        area,
    );
}

fn footer(f: &mut Frame, area: Rect, app: &App) {
    let keys: &[(&str, &str)] = match (app.screen, app.modal.is_some()) {
        (_, true) => &[("esc", "close"), ("enter", "confirm")],
        (Screen::Connections, _) => &[
            ("↑↓", "move"),
            ("enter", "connect"),
            ("n", "new"),
            ("e", "edit"),
            ("d", "delete"),
            ("?", "help"),
            ("q", "quit"),
        ],
        (Screen::Browser, _) => &[
            ("/", "search"),
            ("tab", "pane"),
            ("n", "new key"),
            ("e", "edit"),
            ("a", "add"),
            ("x", "del item"),
            ("D", "del key"),
            ("t", "ttl"),
            (":", "console"),
            ("?", "help"),
            ("q", "quit"),
        ],
    };
    let mut spans = Vec::new();
    for (k, label) in keys {
        spans.push(Span::styled(
            format!(" {k} "),
            Style::new().bg(PANEL).fg(Color::White).bold(),
        ));
        spans.push(Span::styled(format!(" {label}  "), Style::new().fg(DIM)));
    }
    f.render_widget(Line::from(spans), area);
}

// ---- modals -------------------------------------------------------------

fn modal(f: &mut Frame, area: Rect, app: &mut App) {
    let Some(m) = &app.modal else { return };
    match m {
        Modal::Help => {
            let rect = centered(area, 74, 24);
            f.render_widget(Clear, rect);
            f.render_widget(
                Paragraph::new(help_text())
                    .block(panel("Keybindings — esc closes", true))
                    .wrap(Wrap { trim: false }),
                rect,
            );
        }
        Modal::Message { title, body } => {
            let rect = centered(area, 64, 9);
            f.render_widget(Clear, rect);
            f.render_widget(
                Paragraph::new(body.clone())
                    .wrap(Wrap { trim: true })
                    .block(panel(title, true)),
                rect,
            );
        }
        Modal::Confirm { message, .. } => {
            let rect = centered(area, 66, 8);
            f.render_widget(Clear, rect);
            f.render_widget(
                Paragraph::new(vec![
                    Line::raw(""),
                    Line::from(Span::styled(message.clone(), Style::new().bold())),
                    Line::raw(""),
                    Line::from(Span::styled(
                        "y = yes    n / esc = no",
                        Style::new().fg(DIM),
                    )),
                ])
                .wrap(Wrap { trim: true })
                .block(panel("Confirm", true).border_style(Style::new().fg(ACCENT))),
                rect,
            );
        }
        Modal::Form {
            title,
            hint,
            fields,
            focus,
            error,
            ..
        } => form(f, area, title, hint, fields, *focus, error.as_deref()),
        Modal::Editor {
            title, textarea, ..
        } => {
            let rect = centered(area, 90, 24);
            f.render_widget(Clear, rect);
            let editor_title = format!("{title} — ctrl+s saves, esc cancels");
            let block = panel(&editor_title, true);
            let inner = block.inner(rect);
            f.render_widget(block, rect);
            f.render_widget(&**textarea, inner);
        }
        Modal::Console(state) => console(f, area, state),
    }
}

fn form(
    f: &mut Frame,
    area: Rect,
    title: &str,
    hint: &str,
    fields: &[Field],
    focus: usize,
    error: Option<&str>,
) {
    let height = (fields.len() as u16 * 3) + 5;
    let rect = centered(area, 68, height.min(area.height.saturating_sub(2)));
    f.render_widget(Clear, rect);
    let block = panel(title, true);
    let inner = block.inner(rect);
    f.render_widget(block, rect);

    let mut constraints: Vec<Constraint> = fields.iter().map(|_| Constraint::Length(3)).collect();
    constraints.push(Constraint::Length(1));
    constraints.push(Constraint::Min(0));
    let rows = Layout::vertical(constraints).split(inner);

    for (i, field) in fields.iter().enumerate() {
        let active = i == focus;
        let border = if active { ACCENT } else { PANEL };
        let content = match &field.kind {
            FieldKind::Secret => "•".repeat(field.value().chars().count()),
            FieldKind::Bool => {
                if field.flag {
                    "[x] on   (space toggles)".into()
                } else {
                    "[ ] off  (space toggles)".into()
                }
            }
            FieldKind::Choice(opts) => opts
                .iter()
                .enumerate()
                .map(|(j, o)| {
                    if j == field.choice {
                        format!("[{o}]")
                    } else {
                        format!(" {o} ")
                    }
                })
                .collect::<Vec<_>>()
                .join(" "),
            FieldKind::Text => field.value(),
        };
        f.render_widget(
            Paragraph::new(content).block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_type(BorderType::Rounded)
                    .border_style(Style::new().fg(border))
                    .title(Span::styled(
                        format!(" {} ", field.label),
                        if active {
                            Style::new().fg(ACCENT).bold()
                        } else {
                            Style::new().fg(DIM)
                        },
                    )),
            ),
            rows[i],
        );
        if active && matches!(field.kind, FieldKind::Text | FieldKind::Secret) {
            f.set_cursor_position((rows[i].x + 1 + field.input.cursor() as u16, rows[i].y + 1));
        }
    }
    let footer_line = match error {
        Some(e) => Line::from(Span::styled(
            format!(" {e}"),
            Style::new().fg(Color::White).bg(ACCENT).bold(),
        )),
        None => Line::from(Span::styled(format!(" {hint}"), Style::new().fg(DIM))),
    };
    f.render_widget(Paragraph::new(footer_line), rows[fields.len()]);
}

fn console(f: &mut Frame, area: Rect, state: &ConsoleState) {
    let rect = centered(area, 92, 26);
    f.render_widget(Clear, rect);
    let block = panel("Command console — esc closes, ↑↓ history", true);
    let inner = block.inner(rect);
    f.render_widget(block, rect);
    let rows = Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).split(inner);

    let visible = rows[0].height as usize;
    let start = state.log.len().saturating_sub(visible);
    let lines: Vec<Line> = state.log[start..]
        .iter()
        .map(|l| {
            if l.starts_with("> ") {
                Line::from(Span::styled(l.clone(), Style::new().fg(Color::Cyan).bold()))
            } else if l.starts_with("(error)") {
                Line::from(Span::styled(l.clone(), Style::new().fg(ACCENT)))
            } else {
                Line::raw(l.clone())
            }
        })
        .collect();
    f.render_widget(Paragraph::new(lines), rows[0]);

    let prompt = format!("> {}", state.input.value());
    f.render_widget(
        Paragraph::new(Span::styled(prompt, Style::new().fg(Color::White))),
        rows[1],
    );
    f.set_cursor_position((rows[1].x + 2 + state.input.cursor() as u16, rows[1].y));
}

// ---- helpers ------------------------------------------------------------

fn panel(title: &str, focused: bool) -> Block<'_> {
    Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(if focused { ACCENT } else { PANEL }))
        .title(Span::styled(
            format!(" {title} "),
            if focused {
                Style::new().fg(Color::White).bold()
            } else {
                Style::new().fg(DIM)
            },
        ))
}

fn centered(area: Rect, width: u16, height: u16) -> Rect {
    let w = width.min(area.width.saturating_sub(2));
    let h = height.min(area.height.saturating_sub(2));
    Rect {
        x: area.x + (area.width.saturating_sub(w)) / 2,
        y: area.y + (area.height.saturating_sub(h)) / 2,
        width: w,
        height: h,
    }
}

fn type_color(kind: KeyType) -> Color {
    match kind {
        KeyType::String => Color::Cyan,
        KeyType::Hash => Color::Green,
        KeyType::List => Color::Yellow,
        KeyType::Set => Color::Magenta,
        KeyType::ZSet => Color::Blue,
        KeyType::Stream => Color::LightRed,
        KeyType::Other => DIM,
    }
}

/// Collapse control characters so a multi-line value cannot break the table.
fn one_line(s: &str) -> String {
    let flat: String = s
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    if flat.chars().count() > 400 {
        flat.chars().take(400).collect::<String>() + "…"
    } else {
        flat
    }
}

/// Indent a value that parses as JSON; return it untouched otherwise.
pub fn pretty_json(s: &str) -> String {
    let t = s.trim_start();
    if !t.starts_with('{') && !t.starts_with('[') {
        return s.to_string();
    }
    serde_json::from_str::<serde_json::Value>(t)
        .ok()
        .and_then(|v| serde_json::to_string_pretty(&v).ok())
        .unwrap_or_else(|| s.to_string())
}

pub fn human_ttl(seconds: i64) -> String {
    let s = seconds;
    if s < 60 {
        format!("{s}s")
    } else if s < 3600 {
        format!("{}m{}s", s / 60, s % 60)
    } else if s < 86_400 {
        format!("{}h{}m", s / 3600, (s % 3600) / 60)
    } else {
        format!("{}d{}h", s / 86_400, (s % 86_400) / 3600)
    }
}

fn help_text() -> Vec<Line<'static>> {
    let head = |t: &'static str| Line::from(Span::styled(t, Style::new().fg(ACCENT).bold()));
    let row = |k: &'static str, d: &'static str| {
        Line::from(vec![
            Span::styled(format!("  {k:<12}"), Style::new().fg(Color::White).bold()),
            Span::raw(d),
        ])
    };
    vec![
        head("Navigation"),
        row("j / k  ↑↓", "move        g / G  jump to top / bottom"),
        row("h / l  ←→", "collapse / expand folder"),
        row("enter", "toggle folder, or jump into the value pane"),
        row("tab", "switch between the key tree and the value pane"),
        head("Keys"),
        row("/", "search by pattern (bare words become *word*)"),
        row("esc", "clear the search pattern"),
        row("n", "new key       D  delete key      R  rename key"),
        row("t", "set or clear TTL"),
        row("y", "copy the selected key name to the clipboard"),
        row("r", "refresh"),
        head("Values"),
        row("e", "edit — string opens an editor, rows open a form"),
        row("a", "add an element (hash / list / set / zset / stream)"),
        row("x", "delete the selected element"),
        head("Server"),
        row(":", "raw command console"),
        row("ctrl+d", "switch database (reconnects)"),
        row("ctrl+n", "back to the server list"),
        row("q", "quit"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_ttl_by_magnitude() {
        assert_eq!(human_ttl(45), "45s");
        assert_eq!(human_ttl(90), "1m30s");
        assert_eq!(human_ttl(7_200), "2h0m");
        assert_eq!(human_ttl(90_000), "1d1h");
    }

    #[test]
    fn pretty_prints_json_only_when_valid() {
        assert_eq!(pretty_json(r#"{"a":1}"#), "{\n  \"a\": 1\n}");
        assert_eq!(pretty_json("{not json"), "{not json");
        assert_eq!(pretty_json("plain"), "plain");
    }

    #[test]
    fn flattens_control_characters_in_cells() {
        assert_eq!(one_line("a\nb\tc"), "a b c");
    }
}
