//! All rendering. Reads `App` and draws; never mutates state except the
//! scroll/selection bookkeeping ratatui's stateful widgets require.

use ratatui::prelude::*;
use ratatui::widgets::{
    Block, BorderType, Borders, Cell, Clear, List, ListItem, Paragraph, Row, Scrollbar,
    ScrollbarOrientation, ScrollbarState, Table, Wrap,
};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::app::{
    App, ConsoleState, Field, FieldKind, Focus, GroupPane, GroupsState, INFO_TABS, InfoRow,
    InfoState, MemoryState, Modal, PubSubState, Screen,
};
use crate::json::{self, Token};
use crate::memory::human_bytes;
use crate::redis_client::{KeyType, KeyValue};
use crate::theme::{Palette, Theme};

pub fn draw(f: &mut Frame, app: &mut App) {
    let palette = app.store.theme.palette();
    f.render_widget(
        Block::default().style(Style::new().bg(palette.background).fg(palette.foreground)),
        f.area(),
    );
    let chunks = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(3),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .split(f.area());

    title_bar(f, chunks[0], app, palette);
    match app.screen {
        Screen::Connections => connections(f, chunks[1], app, palette),
        Screen::Browser => browser(f, chunks[1], app, palette),
    }
    status_bar(f, chunks[2], app, palette);
    footer(f, chunks[3], app, palette);

    if app.modal.is_some() {
        modal(f, f.area(), app, palette);
    }
}

fn title_bar(f: &mut Frame, area: Rect, app: &App, palette: Palette) {
    let mut spans = vec![
        Span::styled(
            " rediscope ",
            Style::new()
                .bg(palette.accent)
                .fg(palette.highlight_foreground)
                .bold(),
        ),
        Span::raw(" "),
    ];
    if let Some(client) = &app.client {
        let c = &client.conn;
        let scheme = if c.tls { "rediss" } else { "redis" };
        spans.push(Span::styled(
            format!("{}  {scheme}://{}:{}/{}", c.name, c.host, c.port, c.db),
            Style::new().fg(palette.foreground),
        ));
        if !app.server_line.is_empty() {
            spans.push(Span::styled(
                format!("  ·  {}", app.server_line),
                Style::new().fg(palette.dim),
            ));
        }
        // A read-only session says so where it cannot be missed: every write
        // is refused, and that should never come as a surprise.
        if c.read_only {
            spans.push(Span::raw("  "));
            spans.push(Span::styled(
                " READ-ONLY ",
                Style::new()
                    .bg(palette.warning)
                    .fg(palette.highlight_foreground)
                    .bold(),
            ));
        }
    } else {
        spans.push(Span::styled(
            "a terminal Redis client",
            Style::new().fg(palette.dim),
        ));
    }
    f.render_widget(Line::from(spans), area);
}

fn connections(f: &mut Frame, area: Rect, app: &mut App, palette: Palette) {
    let cols = Layout::horizontal([
        Constraint::Percentage(15),
        Constraint::Min(44),
        Constraint::Percentage(15),
    ])
    .split(area);

    let filtering = app.conn_filter.is_some();
    let rows = Layout::vertical([
        Constraint::Length(if filtering { 3 } else { 0 }),
        Constraint::Min(1),
    ])
    .split(cols[1]);

    if let Some(buf) = &app.conn_filter {
        f.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled("/", Style::new().fg(palette.accent).bold()),
                Span::raw(buf.value()),
            ]))
            .block(panel(
                "Filter by name or host (Enter keeps it, Esc closes)",
                true,
                palette,
            )),
            rows[0],
        );
        f.set_cursor_position((
            cursor_col(rows[0], rows[0].x + 2, buf.cursor()),
            rows[0].y + 1,
        ));
    }

    let visible = app.visible_connections();
    let items: Vec<ListItem> = visible
        .iter()
        .map(|i| {
            let c = &app.store.connections[*i];
            let mut spans = vec![
                Span::styled(
                    format!("{:<16}", truncate(&c.name, 16)),
                    Style::new().bold(),
                ),
                Span::styled(
                    format!(
                        "{}://{}:{}/{}",
                        if c.tls { "rediss" } else { "redis" },
                        c.host,
                        c.port,
                        c.db
                    ),
                    Style::new().fg(palette.dim),
                ),
            ];
            if c.tls {
                spans.push(Span::styled("  TLS", Style::new().fg(palette.success)));
            }
            if c.tls_insecure {
                spans.push(Span::styled(
                    "  no-verify",
                    Style::new().fg(palette.warning),
                ));
            }
            if c.use_keychain {
                spans.push(Span::styled("  keychain", Style::new().fg(palette.info)));
            }
            if c.read_only {
                spans.push(Span::styled(
                    "  read-only",
                    Style::new().fg(palette.warning),
                ));
            }
            if c.uses_ssh() {
                spans.push(Span::styled("  ssh", Style::new().fg(palette.magenta)));
            }
            if app.testing.as_deref() == Some(c.name.as_str()) {
                spans.push(Span::styled("  testing…", Style::new().fg(palette.accent)));
            }
            ListItem::new(Line::from(spans))
        })
        .collect();

    let total = app.store.connections.len();
    let title = if app.conn_query.is_empty() {
        format!("Saved connections ({total})")
    } else {
        format!(
            "Saved connections — {} of {total} match '{}'",
            visible.len(),
            app.conn_query
        )
    };
    let empty = items.is_empty();
    let list = List::new(items)
        .block(panel(&title, !filtering, palette))
        .highlight_style(
            Style::new()
                .bg(palette.accent)
                .fg(palette.highlight_foreground)
                .bold(),
        )
        .highlight_symbol(" ");
    f.render_stateful_widget(list, rows[1], &mut app.conn_state);

    if empty {
        let message = if total == 0 {
            "No saved connections yet — press 'n' to add one."
        } else {
            "Nothing matches this filter. Press esc to clear it."
        };
        f.render_widget(
            Paragraph::new(message).style(Style::new().fg(palette.dim)),
            rows[1].inner(Margin::new(2, 1)),
        );
    }
}

fn browser(f: &mut Frame, area: Rect, app: &mut App, palette: Palette) {
    let cols =
        Layout::horizontal([Constraint::Percentage(38), Constraint::Percentage(62)]).split(area);
    key_panel(f, cols[0], app, palette);
    value_panel(f, cols[1], app, palette);
}

fn key_panel(f: &mut Frame, area: Rect, app: &mut App, palette: Palette) {
    let rows = if app.search.is_some() {
        Layout::vertical([Constraint::Length(3), Constraint::Min(1)]).split(area)
    } else {
        Layout::vertical([Constraint::Length(0), Constraint::Min(1)]).split(area)
    };
    if let Some(buf) = &app.search {
        let text = buf.value();
        f.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled("/", Style::new().fg(palette.accent).bold()),
                Span::raw(text.clone()),
            ]))
            .block(panel(
                "Search pattern (Enter applies, Esc cancels)",
                true,
                palette,
            )),
            rows[0],
        );
        f.set_cursor_position((
            cursor_col(rows[0], rows[0].x + 2, buf.cursor()),
            rows[0].y + 1,
        ));
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
                    Span::styled(
                        if r.expanded { "▾ " } else { "▸ " },
                        Style::new().fg(palette.dim),
                    ),
                    Span::styled(r.label.clone(), Style::new().fg(palette.foreground).bold()),
                    Span::styled(format!("  ({})", r.leaves), Style::new().fg(palette.dim)),
                ]),
                (None, Some(k)) => {
                    let marked = app.marked.contains(&k.name);
                    let mut spans = vec![
                        Span::raw(indent),
                        Span::styled(
                            if marked { "✓" } else { " " },
                            Style::new().fg(palette.success).bold(),
                        ),
                        Span::styled(
                            format!("{} ", k.kind.badge()),
                            Style::new().fg(type_color(k.kind, palette)).bold(),
                        ),
                        Span::raw(r.label.clone()),
                    ];
                    if k.ttl >= 0 {
                        spans.push(Span::styled(
                            format!("  {}", human_ttl(k.ttl)),
                            Style::new().fg(palette.warning),
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
    if !app.marked.is_empty() {
        title.push_str(&format!("  ·  {} marked", app.marked.len()));
    }

    let list = List::new(items)
        .block(panel(&title, focused, palette))
        .highlight_style(if focused {
            Style::new()
                .bg(palette.accent)
                .fg(palette.highlight_foreground)
                .bold()
        } else {
            Style::new().bg(palette.panel)
        });
    f.render_stateful_widget(list, rows[1], &mut app.tree_state);

    if app.rows.is_empty() && !app.loading {
        f.render_widget(
            Paragraph::new("No keys match. Press / to change the pattern.")
                .style(Style::new().fg(palette.dim)),
            rows[1].inner(Margin::new(2, 1)),
        );
    }
}

fn value_panel(f: &mut Frame, area: Rect, app: &mut App, palette: Palette) {
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
                    Span::styled(
                        k.kind.name(),
                        Style::new().fg(type_color(k.kind, palette)).bold(),
                    ),
                    Span::styled(json_badge(&app.value), Style::new().fg(palette.info).bold()),
                    Span::styled(format!("   ttl: {ttl}"), Style::new().fg(palette.dim)),
                    Span::styled(size, Style::new().fg(palette.dim)),
                ]),
            ])
        }
        None => Paragraph::new(Line::from(Span::styled(
            "Select a key on the left.",
            Style::new().fg(palette.dim),
        ))),
    };
    f.render_widget(header.block(panel("Key", false, palette)), rows[0]);

    let focused = app.focus == Focus::Value;
    match &app.value {
        None => f.render_widget(
            Paragraph::new(if app.current.is_some() {
                "Loading…"
            } else {
                ""
            })
            .style(Style::new().fg(palette.dim))
            .block(panel("Value", focused, palette)),
            rows[1],
        ),
        Some(KeyValue::Str(s)) => {
            let text = json_text(s, palette);
            f.render_widget(
                Paragraph::new(text)
                    .wrap(Wrap { trim: false })
                    .scroll((app.value_scroll, 0))
                    .block(panel("Value", focused, palette)),
                rows[1],
            );
        }
        Some(KeyValue::Unsupported(msg)) => f.render_widget(
            Paragraph::new(msg.clone())
                .wrap(Wrap { trim: true })
                .style(Style::new().fg(palette.dim))
                .block(panel("Value", focused, palette)),
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
                    .map(|h| Cell::from(Span::styled(*h, Style::new().fg(palette.accent).bold())))
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

            let preview = app
                .selected_value_row()
                .and_then(|row| structured_document(&row.cells, palette));
            let show_preview = preview.is_some() && rows[1].height >= 10;
            let panes = show_preview.then(|| {
                Layout::vertical([Constraint::Percentage(38), Constraint::Percentage(62)])
                    .split(rows[1])
            });
            let table_area = panes.as_ref().map_or(rows[1], |areas| areas[0]);
            let table_title = if show_preview { "Elements" } else { "Value" };
            let table = Table::new(body, widths)
                .header(header_row)
                .block(panel(table_title, focused, palette))
                .row_highlight_style(if focused {
                    Style::new()
                        .bg(palette.accent)
                        .fg(palette.highlight_foreground)
                        .bold()
                } else {
                    Style::new().bg(palette.panel)
                });
            f.render_stateful_widget(table, table_area, &mut app.value_state);

            if let (Some((kind, text)), Some(areas)) = (preview, panes) {
                let title = format!("Selected {kind} · PgUp/PgDn scroll");
                f.render_widget(
                    Paragraph::new(text)
                        .wrap(Wrap { trim: false })
                        .scroll((app.value_scroll, 0))
                        .block(panel(&title, false, palette)),
                    areas[1],
                );
            }
        }
    }
}

fn status_bar(f: &mut Frame, area: Rect, app: &App, palette: Palette) {
    let style = if app.status.starts_with("Error")
        || app.status.starts_with("Could not")
        || app.status.contains("failed")
    {
        Style::new()
            .fg(palette.highlight_foreground)
            .bg(palette.red)
    } else {
        Style::new().fg(palette.info)
    };
    f.render_widget(
        Paragraph::new(Span::styled(format!(" {}", app.status), style)),
        area,
    );
}

fn footer(f: &mut Frame, area: Rect, app: &App, palette: Palette) {
    let keys: &[(&str, &str)] = match (app.screen, app.modal.is_some()) {
        (_, true) => &[("esc", "close"), ("enter", "confirm")],
        (Screen::Connections, _) => &[
            ("↑↓", "move"),
            ("enter", "connect"),
            ("n", "new"),
            ("e", "edit"),
            ("c", "copy"),
            ("d", "delete"),
            ("J/K", "reorder"),
            ("T", "test"),
            ("/", "filter"),
            ("p", "theme"),
            ("?", "help"),
            ("q", "quit"),
        ],
        (Screen::Browser, _) => &[
            ("/", "search"),
            ("F", "find value"),
            ("tab", "pane"),
            ("n", "new key"),
            ("e", "edit"),
            ("a", "add"),
            ("m", "mark"),
            ("D", "del"),
            ("C", "copy"),
            ("t", "ttl"),
            (":", "console"),
            ("i", "info"),
            ("P", "pub/sub"),
            ("?", "help"),
            ("q", "quit"),
        ],
    };
    let mut spans = Vec::new();
    for (k, label) in keys {
        spans.push(Span::styled(
            format!(" {k} "),
            Style::new().bg(palette.panel).fg(palette.foreground).bold(),
        ));
        spans.push(Span::styled(
            format!(" {label}  "),
            Style::new().fg(palette.dim),
        ));
    }
    f.render_widget(Line::from(spans), area);
}

// ---- modals -------------------------------------------------------------

fn modal(f: &mut Frame, area: Rect, app: &mut App, palette: Palette) {
    let Some(m) = &app.modal else { return };
    match m {
        Modal::Help => {
            let rect = centered(area, 74, 32);
            clear_area(f, rect, palette);
            f.render_widget(
                Paragraph::new(help_text(palette))
                    .block(panel("Keybindings — esc closes", true, palette))
                    .wrap(Wrap { trim: false }),
                rect,
            );
        }
        Modal::Message {
            title,
            body,
            scroll,
        } => {
            // A one-line notice gets a small box; a command reply gets room to
            // be read, and scrolls when it still does not fit.
            let lines = body.lines().count() as u16;
            let height = (lines + 4).clamp(9, area.height.saturating_sub(2).max(9));
            let width = if lines > 4 { 96 } else { 64 };
            let rect = centered(area, width, height);
            clear_area(f, rect, palette);
            let scrollable = lines + 2 > rect.height;
            let heading = if scrollable {
                format!("{title} — ↑↓ scrolls · y copies · esc closes")
            } else {
                title.clone()
            };
            f.render_widget(
                Paragraph::new(body.clone())
                    .wrap(Wrap { trim: true })
                    .scroll((*scroll, 0))
                    .block(panel(&heading, true, palette)),
                rect,
            );
        }
        Modal::Confirm { message, .. } => {
            let rect = centered(area, 66, 8);
            clear_area(f, rect, palette);
            f.render_widget(
                Paragraph::new(vec![
                    Line::raw(""),
                    Line::from(Span::styled(message.clone(), Style::new().bold())),
                    Line::raw(""),
                    Line::from(Span::styled(
                        "y = yes    n / esc = no",
                        Style::new().fg(palette.dim),
                    )),
                ])
                .wrap(Wrap { trim: true })
                .block(
                    panel("Confirm", true, palette).border_style(Style::new().fg(palette.accent)),
                ),
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
        } => form(
            f,
            area,
            FormView {
                title,
                hint,
                fields,
                focus: *focus,
                error: error.as_deref(),
            },
            palette,
        ),
        Modal::Editor {
            title,
            textarea,
            json: mode,
            error,
            ..
        } => {
            let rect = centered(area, 90, 26);
            clear_area(f, rect, palette);
            let editor_title = truncate(title, rect.width.saturating_sub(4) as usize);
            let controls = if mode.is_json() {
                "ctrl+s saves · ctrl+f formats · esc cancels"
            } else {
                "ctrl+s saves · esc cancels"
            };
            let block = panel(&editor_title, true, palette);
            let inner = block.inner(rect);
            f.render_widget(block, rect);
            let rows = Layout::vertical([
                Constraint::Min(1),
                Constraint::Length(1),
                Constraint::Length(1),
            ])
            .split(inner);
            f.render_widget(&**textarea, rows[0]);
            let status = match (error, mode.is_json()) {
                (Some(e), _) => Span::styled(
                    format!("invalid JSON — {e}"),
                    Style::new().fg(palette.red).bold(),
                ),
                (None, true) => Span::styled(
                    "JSON — checked before it is saved".to_string(),
                    Style::new().fg(palette.dim),
                ),
                (None, false) => Span::raw(""),
            };
            f.render_widget(Line::from(status), rows[1]);
            f.render_widget(
                Line::from(Span::styled(controls, Style::new().fg(palette.dim))).right_aligned(),
                rows[2],
            );
        }
        Modal::Console(state) => console(f, area, state, palette),
        Modal::PubSub(state) => pubsub_feed(f, area, state, palette),
        Modal::Groups(state) => consumer_groups(f, area, state, palette),
        Modal::Memory(state) => memory_report(f, area, state, palette),
        Modal::Info(state) => server_info(f, area, state, palette),
        Modal::ThemePicker { selected, .. } => theme_picker(f, area, *selected, palette),
    }
}

struct FormView<'a> {
    title: &'a str,
    hint: &'a str,
    fields: &'a [Field],
    focus: usize,
    error: Option<&'a str>,
}

fn form(f: &mut Frame, area: Rect, view: FormView<'_>, palette: Palette) {
    let FormView {
        title,
        hint,
        fields,
        focus,
        error,
    } = view;
    let content_height: u16 = fields.iter().map(|f| f.height()).sum();
    // +2 borders, +1 hint line.
    let rect = centered(area, 68, content_height + 3);
    clear_area(f, rect, palette);
    let title = truncate(title, rect.width.saturating_sub(4) as usize);
    let block = panel(&title, true, palette);
    let inner = block.inner(rect);
    f.render_widget(block, rect);

    let rows = Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).split(inner);
    let viewport = rows[0];

    // A long form (the connection editor is 14 rows tall) will not fit a short
    // terminal, so scroll the field list to keep the focused input on screen.
    let offsets: Vec<u16> = fields
        .iter()
        .scan(0u16, |acc, fld| {
            let at = *acc;
            *acc += fld.height();
            Some(at)
        })
        .collect();
    let focus_top = offsets.get(focus).copied().unwrap_or(0);
    let focus_bottom = focus_top + fields.get(focus).map_or(0, |f| f.height());
    let mut scroll = 0u16;
    if focus_bottom > viewport.height {
        scroll = focus_bottom - viewport.height;
    }
    if focus_top < scroll {
        scroll = focus_top;
    }

    for (i, field) in fields.iter().enumerate() {
        let top = offsets[i];
        let height = field.height();
        // Skip anything scrolled out, and anything only partly visible.
        if top < scroll || top + height > scroll + viewport.height {
            continue;
        }
        let row = Rect {
            x: viewport.x,
            y: viewport.y + (top - scroll),
            width: viewport.width,
            height,
        };
        if let FieldKind::Section = field.kind {
            f.render_widget(
                Paragraph::new(vec![
                    Line::raw(""),
                    Line::from(Span::styled(
                        field.label.to_uppercase(),
                        Style::new().fg(palette.accent).bold(),
                    )),
                ]),
                row,
            );
            continue;
        }
        let active = i == focus;
        let border = if active {
            palette.accent
        } else {
            palette.panel
        };
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
            FieldKind::Section => unreachable!("handled above"),
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
                            Style::new().fg(palette.accent).bold()
                        } else {
                            Style::new().fg(palette.dim)
                        },
                    )),
            ),
            row,
        );
        if active && matches!(field.kind, FieldKind::Text | FieldKind::Secret) {
            f.set_cursor_position((
                cursor_col(row, row.x + 1, field.input.cursor())
                    .min(row.x + row.width.saturating_sub(2)),
                row.y + 1,
            ));
        }
    }

    // The hint and error text must never spill past the dialog border.
    let width = rows[1].width as usize;
    let footer_line = match error {
        Some(e) => Line::from(Span::styled(
            truncate(&format!(" {e}"), width),
            Style::new()
                .fg(palette.highlight_foreground)
                .bg(palette.red)
                .bold(),
        )),
        None => {
            let text = if content_height > viewport.height {
                format!(" {hint}  ·  ↑↓ scrolls")
            } else {
                format!(" {hint}")
            };
            Line::from(Span::styled(
                truncate(&text, width),
                Style::new().fg(palette.dim),
            ))
        }
    };
    f.render_widget(Paragraph::new(footer_line), rows[1]);
}

fn theme_picker(f: &mut Frame, area: Rect, selected: usize, palette: Palette) {
    let rect = centered(area, 58, Theme::ALL.len() as u16 + 4);
    clear_area(f, rect, palette);
    let block = panel(
        "Theme — ↑↓ previews, Enter saves, Esc cancels",
        true,
        palette,
    );
    let inner = block.inner(rect);
    f.render_widget(block, rect);

    let items = Theme::ALL.iter().enumerate().map(|(i, theme)| {
        let sample = theme.palette();
        let marker = if i == selected { "›" } else { " " };
        let line = Line::from(vec![
            Span::styled(
                format!(" {marker} {:<19}", theme.name()),
                if i == selected {
                    Style::new().fg(palette.foreground).bold()
                } else {
                    Style::new().fg(palette.dim)
                },
            ),
            Span::styled("●", Style::new().fg(sample.accent)),
            Span::styled(" ●", Style::new().fg(sample.info)),
            Span::styled(" ●", Style::new().fg(sample.success)),
            Span::styled(" ●", Style::new().fg(sample.warning)),
            Span::styled(
                format!("  {}", theme.description()),
                Style::new().fg(palette.dim),
            ),
        ]);
        ListItem::new(line)
    });
    f.render_widget(List::new(items), inner);
}

/// The `INFO` viewer: a tab strip over a scrolling, filterable field list.
fn server_info(f: &mut Frame, area: Rect, state: &InfoState, palette: Palette) {
    // Fill most of the terminal — INFO is long, and a taller list means less
    // scrolling on the big sections.
    let rect = centered(area, area.width.saturating_sub(6).min(110), area.height);
    clear_area(f, rect, palette);
    let block = panel(
        "Server info — tab/1-0 section · / filter · e edit config · x kill/reset · y copy · r refresh",
        true,
        palette,
    );
    let inner = block.inner(rect);
    f.render_widget(block, rect);
    let rows = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Min(1),
    ])
    .split(inner);

    let mut tabs = Vec::new();
    for (i, name) in INFO_TABS.iter().enumerate() {
        tabs.push(Span::styled(
            format!(" {} {name} ", i + 1),
            if i == state.tab {
                Style::new()
                    .bg(palette.accent)
                    .fg(palette.highlight_foreground)
                    .bold()
            } else {
                Style::new().fg(palette.dim)
            },
        ));
        tabs.push(Span::raw(" "));
    }
    f.render_widget(Line::from(tabs), rows[0]);

    let all = state.rows();
    let body = rows[2];
    let visible = body.height as usize;
    let start = state.view_start(visible).min(all.len().saturating_sub(1));
    let width = body.width.saturating_sub(1) as usize; // leave the scrollbar column
    let key_width = 30.min(width.saturating_sub(4));

    // Left of the second line: the filter box or the applied query. Right: the
    // position within the section.
    let filter_span = match (&state.filter, state.query.as_str()) {
        (Some(buf), _) => Span::styled(
            format!("/{}", buf.value()),
            Style::new().fg(palette.foreground).bold(),
        ),
        (None, "") => Span::styled("/ filters this section", Style::new().fg(palette.dim)),
        (None, q) => Span::styled(
            format!("filter: {q}   (esc clears)"),
            Style::new().fg(palette.accent),
        ),
    };
    let counter = if all.len() > visible {
        format!(
            "{}–{} of {}",
            start + 1,
            (start + visible).min(all.len()),
            all.len()
        )
    } else {
        format!("{} row(s)", all.len())
    };
    f.render_widget(Line::from(filter_span), rows[1]);
    f.render_widget(
        Line::from(Span::styled(counter, Style::new().fg(palette.dim))).right_aligned(),
        rows[1],
    );
    if let Some(buf) = &state.filter {
        f.set_cursor_position((cursor_col(rows[1], rows[1].x + 1, buf.cursor()), rows[1].y));
    }

    let value_width = width.saturating_sub(key_width + 3);
    // The Config and Clients tabs act on one row, so that row is highlighted.
    let cursor = crate::app::tab_is_actionable(state.tab).then_some(state.cursor);
    let lines: Vec<Line> = all
        .iter()
        .enumerate()
        .skip(start)
        .take(visible)
        .map(|(index, row)| {
            let line = match row {
                InfoRow::Head(name) => Line::from(Span::styled(
                    format!("{name} "),
                    Style::new().fg(palette.accent).bold(),
                )),
                InfoRow::Field(k, v) => Line::from(vec![
                    Span::styled(
                        format!("  {:<key_width$}", truncate(k, key_width.saturating_sub(1))),
                        Style::new().fg(palette.dim),
                    ),
                    Span::styled(
                        truncate(&one_line(v), value_width),
                        Style::new().fg(palette.foreground),
                    ),
                ]),
                InfoRow::Gauge {
                    label,
                    ratio,
                    text,
                    alarm_high,
                } => {
                    let bar_width = 24.min(value_width.saturating_sub(text.len() + 2));
                    let filled = (ratio * bar_width as f64).round() as usize;
                    Line::from(vec![
                        Span::styled(
                            format!(
                                "  {:<key_width$}",
                                truncate(label, key_width.saturating_sub(1))
                            ),
                            Style::new().fg(palette.dim),
                        ),
                        Span::styled(
                            "█".repeat(filled),
                            Style::new().fg(gauge_color(*ratio, *alarm_high, palette)),
                        ),
                        Span::styled(
                            "░".repeat(bar_width - filled),
                            Style::new().fg(palette.panel),
                        ),
                        Span::styled(
                            format!(" {text}"),
                            Style::new().fg(palette.foreground).bold(),
                        ),
                    ])
                }
            };
            if cursor == Some(index) {
                line.style(
                    Style::new()
                        .bg(palette.accent)
                        .fg(palette.highlight_foreground),
                )
            } else {
                line
            }
        })
        .collect();
    f.render_widget(Paragraph::new(lines), body);

    if all.len() > visible {
        let mut sb = ScrollbarState::new(all.len().saturating_sub(visible)).position(start);
        f.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .begin_symbol(None)
                .end_symbol(None)
                .thumb_style(Style::new().fg(palette.accent))
                .track_style(Style::new().fg(palette.panel)),
            body,
            &mut sb,
        );
    }
}

/// Green while a ratio is healthy, red once it is alarming. Which end is
/// alarming depends on the metric: a full memory bar is bad, a full hit-rate
/// bar is good.
fn gauge_color(ratio: f64, alarm_high: bool, palette: Palette) -> Color {
    let bad = if alarm_high { ratio } else { 1.0 - ratio };
    match bad {
        r if r >= 0.9 => palette.red,
        r if r >= 0.75 => palette.warning,
        _ => palette.success,
    }
}

fn console(f: &mut Frame, area: Rect, state: &ConsoleState, palette: Palette) {
    let rect = centered(area, 92, 26);
    clear_area(f, rect, palette);
    let block = panel(
        "Command console — esc closes, ↑↓ history, ctrl+r search, tab completes",
        true,
        palette,
    );
    let inner = block.inner(rect);
    f.render_widget(block, rect);
    let rows = Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).split(inner);

    let visible = rows[0].height as usize;
    let start = state.log.len().saturating_sub(visible);
    let lines: Vec<Line> = state.log[start..]
        .iter()
        .map(|l| {
            if l.starts_with("> ") {
                Line::from(Span::styled(
                    l.clone(),
                    Style::new().fg(palette.info).bold(),
                ))
            } else if l.starts_with("(error)") {
                Line::from(Span::styled(l.clone(), Style::new().fg(palette.red)))
            } else {
                Line::raw(l.clone())
            }
        })
        .collect();
    f.render_widget(Paragraph::new(lines), rows[0]);

    // While ctrl+r is open the prompt shows the search and its current hit
    // instead of the line being edited, the way a shell does it.
    if let Some(search) = &state.search {
        let hit = search.hit(state.history.entries()).unwrap_or("");
        let label = format!("(reverse-i-search)`{}\u{27}: ", search.query());
        let offset = UnicodeWidthStr::width(label.as_str());
        f.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled(label, Style::new().fg(palette.dim)),
                Span::styled(hit.to_string(), Style::new().fg(palette.foreground)),
            ])),
            rows[1],
        );
        f.set_cursor_position((
            cursor_col(rows[1], rows[1].x, offset + UnicodeWidthStr::width(hit)),
            rows[1].y,
        ));
        return;
    }

    let prompt = format!("> {}", state.input.value());
    f.render_widget(
        Paragraph::new(Span::styled(prompt, Style::new().fg(palette.foreground))),
        rows[1],
    );
    f.set_cursor_position((
        cursor_col(rows[1], rows[1].x + 2, state.input.cursor()),
        rows[1].y,
    ));
}

// ---- helpers ------------------------------------------------------------

/// Screen column for a cursor `offset` characters into a field drawn inside
/// `area` at `x`. A pasted line can be longer than the screen - or than `u16`
/// itself - so the column is clamped instead of wrapping around.
fn cursor_col(area: Rect, x: u16, offset: usize) -> u16 {
    let right = area.x.saturating_add(area.width.saturating_sub(1));
    x.saturating_add(u16::try_from(offset).unwrap_or(u16::MAX))
        .min(right)
}

/// The namespace memory report: which key prefix is holding the RAM, and how
/// much of the keyspace that answer is based on.
fn memory_report(f: &mut Frame, area: Rect, state: &MemoryState, palette: Palette) {
    let rect = centered(area, 88, 28);
    clear_area(f, rect, palette);
    let block = panel(
        "Namespace memory — 1/2/3 depth · t biggest keys · r rescans · y copies · esc closes",
        true,
        palette,
    );
    let inner = block.inner(rect);
    f.render_widget(block, rect);
    let rows = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Min(1),
    ])
    .split(inner);

    // Say what the estimate rests on. An extrapolated number without its
    // sample size invites more trust than it has earned.
    let total = state.dbsize.max(state.rollup.scanned());
    let headline = if state.running {
        format!(
            "scanning … {} of {} keys, {} measured",
            state.rollup.scanned(),
            total,
            state.rollup.sampled()
        )
    } else {
        format!(
            "{} keys, {} measured, {} estimated in total",
            state.rollup.scanned(),
            state.rollup.sampled(),
            human_bytes(state.rollup.total_bytes())
        )
    };
    f.render_widget(
        Line::from(Span::styled(headline, Style::new().fg(palette.dim))),
        rows[0],
    );

    let header = if state.show_keys {
        format!("{:<52}{:>12}{:>10}", "key", "size", "freq")
    } else {
        format!(
            "{:<38}{:>10}{:>12}{:>9}",
            format!("prefix (depth {})", state.depth),
            "keys",
            "est. size",
            "share"
        )
    };
    f.render_widget(
        Line::from(Span::styled(header, Style::new().fg(palette.accent).bold())),
        rows[1],
    );

    // The biggest individual keys the sample measured, rather than prefixes.
    if state.show_keys {
        let keys = state.rollup.top_keys();
        if keys.is_empty() {
            let note = if state.running {
                "measuring …"
            } else {
                "no keys were measured"
            };
            f.render_widget(
                Paragraph::new(Span::styled(note, Style::new().fg(palette.dim))),
                rows[2],
            );
            return;
        }
        let height = rows[2].height as usize;
        let start = state.scroll.min(keys.len().saturating_sub(1));
        let lines: Vec<Line> = keys
            .iter()
            .skip(start)
            .take(height)
            .map(|key| {
                Line::from(vec![
                    Span::raw(format!("{:<52}", truncate(&key.key, 51))),
                    Span::styled(
                        format!("{:>12}", human_bytes(key.bytes)),
                        Style::new().fg(palette.foreground),
                    ),
                    Span::styled(
                        match key.freq {
                            Some(f) => format!("{f:>10}"),
                            None => format!("{:>10}", "—"),
                        },
                        Style::new().fg(palette.dim),
                    ),
                ])
            })
            .collect();
        f.render_widget(Paragraph::new(lines), rows[2]);
        return;
    }

    let all = state.rows();
    if all.is_empty() {
        let note = if state.running {
            "measuring …"
        } else {
            "no keys in this database"
        };
        f.render_widget(
            Paragraph::new(Span::styled(note, Style::new().fg(palette.dim))),
            rows[2],
        );
        return;
    }

    let height = rows[2].height as usize;
    let start = state.scroll.min(all.len().saturating_sub(1));
    let lines: Vec<Line> = all
        .iter()
        .skip(start)
        .take(height)
        .map(|row| {
            let bar_width = 8usize;
            let filled = ((row.share / 100.0) * bar_width as f64).round() as usize;
            Line::from(vec![
                Span::raw(format!("{:<38}", truncate(&row.prefix, 37))),
                Span::styled(format!("{:>10}", row.keys), Style::new().fg(palette.dim)),
                Span::styled(
                    format!("{:>12}", human_bytes(row.est_bytes)),
                    Style::new().fg(palette.foreground),
                ),
                Span::styled(
                    format!("{:>6.1}% ", row.share),
                    Style::new().fg(palette.dim),
                ),
                Span::styled(
                    "\u{2588}".repeat(filled.min(bar_width)),
                    Style::new().fg(palette.accent),
                ),
            ])
        })
        .collect();
    f.render_widget(Paragraph::new(lines), rows[2]);
}

/// The live message feed. Newest at the bottom, like a log.
fn pubsub_feed(f: &mut Frame, area: Rect, state: &PubSubState, palette: Palette) {
    let rect = centered(area, area.width.saturating_sub(6).min(110), area.height);
    clear_area(f, rect, palette);
    let title = format!(
        "{} — s resubscribe · w publish · f follow · c clear · y copy · esc closes",
        state.title()
    );
    let block = panel(&title, true, palette);
    let inner = block.inner(rect);
    f.render_widget(block, rect);
    let rows = Layout::vertical([Constraint::Length(1), Constraint::Min(1)]).split(inner);

    let headline = if state.messages.is_empty() {
        if state.keyspace {
            "waiting … keyspace events need notify-keyspace-events set on the server".to_string()
        } else {
            "waiting for messages …".to_string()
        }
    } else {
        format!(
            "{} message(s){}",
            state.messages.len(),
            if state.follow { " · following" } else { "" }
        )
    };
    f.render_widget(
        Line::from(Span::styled(headline, Style::new().fg(palette.dim))),
        rows[0],
    );

    let body = rows[1];
    let height = body.height as usize;
    // Follow mode keeps the newest message in view; otherwise the cursor
    // decides which window to show.
    let last = state.messages.len().saturating_sub(1);
    let anchor = state.scroll.min(last);
    let start = anchor.saturating_sub(height.saturating_sub(1));
    // A 10-column terminal leaves nothing for a channel column; clamp rather
    // than letting the arithmetic wrap.
    let channel_width = 28.min(body.width.saturating_sub(4) as usize).max(1);
    let payload_width = (body.width as usize).saturating_sub(channel_width + 3);
    let lines: Vec<Line> = state
        .messages
        .iter()
        .skip(start)
        .take(height)
        .map(|(channel, payload)| {
            Line::from(vec![
                Span::styled(
                    format!(
                        "{:<channel_width$} ",
                        truncate(channel, channel_width.saturating_sub(1))
                    ),
                    Style::new().fg(palette.accent),
                ),
                Span::styled(
                    truncate(&one_line(payload), payload_width),
                    Style::new().fg(palette.foreground),
                ),
            ])
        })
        .collect();
    f.render_widget(Paragraph::new(lines), body);
}

/// Consumer groups on the left, the selected group's consumers and unacked
/// entries on the right.
fn consumer_groups(f: &mut Frame, area: Rect, state: &GroupsState, palette: Palette) {
    let rect = centered(area, area.width.saturating_sub(6).min(120), area.height);
    clear_area(f, rect, palette);
    let title = format!(
        "Consumer groups of '{}' — n new · d destroy · a ack · c claim · tab pane · esc closes",
        truncate(&state.key, 40)
    );
    let block = panel(&title, true, palette);
    let inner = block.inner(rect);
    f.render_widget(block, rect);
    let columns =
        Layout::horizontal([Constraint::Percentage(40), Constraint::Percentage(60)]).split(inner);

    let group_items: Vec<ListItem> = state
        .groups
        .iter()
        .map(|g| {
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{:<20}", truncate(&g.name, 19)),
                    Style::new().fg(palette.foreground).bold(),
                ),
                Span::styled(
                    format!(
                        "{:>4} pend  {:>3} cons  lag {}",
                        g.pending, g.consumers, g.lag
                    ),
                    Style::new().fg(palette.dim),
                ),
            ]))
        })
        .collect();
    let mut group_state = ratatui::widgets::ListState::default();
    if !state.groups.is_empty() {
        group_state.select(Some(state.selected.min(state.groups.len() - 1)));
    }
    let groups_focused = state.pane == GroupPane::Groups;
    f.render_stateful_widget(
        List::new(group_items)
            .block(panel("Groups", groups_focused, palette))
            .highlight_style(if groups_focused {
                Style::new()
                    .bg(palette.accent)
                    .fg(palette.highlight_foreground)
                    .bold()
            } else {
                Style::new().bg(palette.panel)
            }),
        columns[0],
        &mut group_state,
    );

    let right = Layout::vertical([Constraint::Length(7), Constraint::Min(1)]).split(columns[1]);
    let consumers: Vec<Line> = if state.detail.consumers.is_empty() {
        vec![Line::from(Span::styled(
            "no consumers",
            Style::new().fg(palette.dim),
        ))]
    } else {
        state
            .detail
            .consumers
            .iter()
            .map(|c| {
                Line::from(vec![
                    Span::styled(
                        format!("{:<24}", truncate(&c.name, 23)),
                        Style::new().fg(palette.foreground),
                    ),
                    Span::styled(
                        format!("{:>4} pending · idle {}s", c.pending, c.idle_ms / 1000),
                        Style::new().fg(palette.dim),
                    ),
                ])
            })
            .collect()
    };
    f.render_widget(
        Paragraph::new(consumers).block(panel("Consumers", false, palette)),
        right[0],
    );

    let pending_focused = state.pane == GroupPane::Pending;
    let pending_items: Vec<ListItem> = state
        .detail
        .pending
        .iter()
        .map(|e| {
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{:<20}", truncate(&e.id, 19)),
                    Style::new().fg(palette.foreground),
                ),
                Span::styled(
                    format!(
                        "{:<18} idle {}s · delivered {}",
                        truncate(&e.consumer, 17),
                        e.idle_ms / 1000,
                        e.deliveries
                    ),
                    Style::new().fg(palette.dim),
                ),
            ]))
        })
        .collect();
    let pending_title = format!("Pending ({})", state.detail.pending.len());
    let mut pending_state = ratatui::widgets::ListState::default();
    if !state.detail.pending.is_empty() {
        pending_state.select(Some(state.pending_sel.min(state.detail.pending.len() - 1)));
    }
    f.render_stateful_widget(
        List::new(pending_items)
            .block(panel(&pending_title, pending_focused, palette))
            .highlight_style(if pending_focused {
                Style::new()
                    .bg(palette.accent)
                    .fg(palette.highlight_foreground)
                    .bold()
            } else {
                Style::new().bg(palette.panel)
            }),
        right[1],
        &mut pending_state,
    );
}

fn clear_area(f: &mut Frame, area: Rect, palette: Palette) {
    f.render_widget(Clear, area);
    f.render_widget(
        Block::default().style(Style::new().bg(palette.background).fg(palette.foreground)),
        area,
    );
}

fn panel(title: &str, focused: bool, palette: Palette) -> Block<'_> {
    Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(if focused {
            palette.accent
        } else {
            palette.panel
        }))
        .title(Span::styled(
            format!(" {title} "),
            if focused {
                Style::new().fg(palette.foreground).bold()
            } else {
                Style::new().fg(palette.dim)
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

fn truncate(s: &str, width: usize) -> String {
    if UnicodeWidthStr::width(s) <= width {
        s.to_string()
    } else if width == 0 {
        String::new()
    } else {
        let mut used = 0;
        let mut result = String::new();
        for ch in s.chars() {
            let ch_width = UnicodeWidthChar::width(ch).unwrap_or(0);
            if used + ch_width > width - 1 {
                break;
            }
            used += ch_width;
            result.push(ch);
        }
        result.push('…');
        result
    }
}

fn structured_document(
    cells: &[String],
    palette: Palette,
) -> Option<(&'static str, Text<'static>)> {
    for cell in cells {
        if json::mode(cell).is_json() {
            return Some(("JSON", json_text(cell, palette)));
        }
        if let Some(pretty) = crate::xml::pretty(cell) {
            return Some(("XML", Text::raw(pretty)));
        }
    }
    None
}

fn type_color(kind: KeyType, palette: Palette) -> Color {
    match kind {
        KeyType::String => palette.info,
        KeyType::Hash => palette.success,
        KeyType::List => palette.warning,
        KeyType::Set => palette.magenta,
        KeyType::ZSet => palette.blue,
        KeyType::Stream => palette.red,
        KeyType::Json => palette.accent,
        KeyType::TimeSeries => palette.warning,
        KeyType::Other => palette.dim,
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
/// " · json" when a string value parses as a JSON document.
fn json_badge(value: &Option<KeyValue>) -> String {
    match value {
        Some(KeyValue::Str(s)) if json::mode(s).is_json() => "  ·  json".into(),
        _ => String::new(),
    }
}

/// A string value as renderable text: coloured and indented when it is JSON,
/// otherwise exactly what the server returned.
fn json_text(s: &str, palette: Palette) -> Text<'static> {
    if !json::mode(s).is_json() {
        return Text::raw(s.to_string());
    }
    let pretty = json::pretty(s);
    Text::from(
        json::highlight(&pretty)
            .into_iter()
            .map(|spans| {
                Line::from(
                    spans
                        .into_iter()
                        .map(|(kind, text)| Span::styled(text, token_style(kind, palette)))
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>(),
    )
}

fn token_style(kind: Token, palette: Palette) -> Style {
    match kind {
        Token::Key => Style::new().fg(palette.info).bold(),
        Token::Str => Style::new().fg(palette.success),
        Token::Number => Style::new().fg(palette.warning),
        Token::Literal => Style::new().fg(palette.magenta),
        Token::Punct => Style::new().fg(palette.dim),
    }
}

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

fn help_text(palette: Palette) -> Vec<Line<'static>> {
    let head =
        |t: &'static str| Line::from(Span::styled(t, Style::new().fg(palette.accent).bold()));
    let row = |k: &'static str, d: &'static str| {
        Line::from(vec![
            Span::styled(
                format!("  {k:<12}"),
                Style::new().fg(palette.foreground).bold(),
            ),
            Span::raw(d),
        ])
    };
    vec![
        head("Server list"),
        row(
            "enter",
            "connect        n  new        e  edit        c  duplicate",
        ),
        row("d", "delete         J / K  move the profile down / up"),
        row("T", "test the connection without opening it"),
        row("/", "filter by name or host · esc clears the filter"),
        row("p", "choose a colour theme (saved for the next run)"),
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
        row("m", "mark the key, or every key under the folder"),
        row("u", "clear every mark · D and t then act on the marked set"),
        row("F", "find keys whose value contains some text"),
        row("C", "copy a key elsewhere (name, database or server)"),
        row(
            "w / I",
            "export the marked keys to a file · import one back",
        ),
        row(
            "r",
            "refresh — TTLs count down live, expired keys leave the tree",
        ),
        head("Values"),
        row("e", "edit — string opens an editor, rows open a form"),
        row(
            "ctrl+f",
            "reformat JSON in the editor · ctrl+s validates before saving",
        ),
        row("a", "add an element (hash / list / set / zset / stream)"),
        row("x", "delete the selected element"),
        row("PgUp / PgDn", "scroll the selected JSON / XML preview"),
        head("Server"),
        row(
            "i",
            "server info — INFO, slow log, clients, config, latency, cluster",
        ),
        row(
            "M",
            "namespace memory — which prefix holds the RAM · t big keys",
        ),
        row("P", "pub/sub feed        N  keyspace event feed"),
        row("S", "consumer groups of the selected stream"),
        row("L", "run a Lua script — marked keys become KEYS[1..]"),
        row(":", "raw command console — tab completes, ctrl+r searches"),
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

    #[test]
    fn long_modal_titles_fit_inside_the_border() {
        let title = truncate(
            "Edit JSON 'medinsight:data-protection:keys:with:a:very:long:suffix'",
            56,
        );
        assert!(UnicodeWidthStr::width(title.as_str()) <= 56, "{title}");
        assert!(title.contains('…'), "{title}");
    }

    #[test]
    fn truncation_uses_terminal_columns_not_character_count() {
        let text = truncate("keys:🔑🔑🔑", 9);
        assert!(UnicodeWidthStr::width(text.as_str()) <= 9, "{text}");
        assert!(text.ends_with('…'));
    }
    #[test]
    fn a_cursor_past_the_edge_stays_inside_the_area() {
        let area = Rect::new(4, 0, 20, 1);
        assert_eq!(cursor_col(area, 6, 3), 9);
        // Longer than the field: pinned to its last column, never wrapped.
        assert_eq!(cursor_col(area, 6, 100), 23);
        // Longer than `u16` itself: the same, rather than an overflow.
        assert_eq!(cursor_col(area, 6, 70_000), 23);
    }
}
