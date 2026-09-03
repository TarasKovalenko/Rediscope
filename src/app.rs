//! Application state and the update half of the loop. Rendering lives in `ui`.
//!
//! Every Redis call runs on a spawned task and reports back over an mpsc
//! channel, so the UI thread never awaits the network.

use std::collections::HashSet;

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::widgets::{ListState, TableState};
use tokio::sync::mpsc::UnboundedSender;
use tui_textarea::TextArea;

use crate::config::{Connection, Store};
use crate::input::InputBuf;
use crate::json::{self, JsonMode};
use crate::redis_client::{
    is_destructive, Client, KeyInfo, KeyType, KeyValue, ServerInfo, KEY_LIMIT,
};
use crate::theme::Theme;
use crate::tree::{Tree, VisibleRow};

pub const NEW_KEY_TYPES: [KeyType; 6] = [
    KeyType::String,
    KeyType::Hash,
    KeyType::List,
    KeyType::Set,
    KeyType::ZSet,
    KeyType::Stream,
];

pub enum Msg {
    Connected(Box<Result<Client, String>>),
    Server(String),
    Keys {
        keys: Vec<KeyInfo>,
        truncated: bool,
        dbsize: u64,
        pattern: String,
    },
    Value {
        info: KeyInfo,
        value: KeyValue,
    },
    /// A write finished; `Ok` carries the status line to show.
    Mutated(Result<String, String>),
    Console(String),
    /// A connection test finished: profile name, then the result.
    Probe(String, Box<Result<crate::redis_client::Probe, String>>),
    /// An `INFO` read finished, for the server-info modal.
    Info(Box<Result<ServerInfo, String>>),
    Status(String),
    Error(String),
    Noop,
}

#[derive(PartialEq, Eq, Clone, Copy)]
pub enum Screen {
    Connections,
    Browser,
}

#[derive(PartialEq, Eq, Clone, Copy)]
pub enum Focus {
    Tree,
    Value,
}

/// What a modal does when submitted.
#[derive(Clone, Debug)]
pub enum Action {
    SaveConnection { replacing: Option<String> },
    DeleteConnection(String),
    NewKey,
    DeleteKey(String),
    RenameKey(String),
    SetTtl(String),
    EditString(String),
    HashSet { key: String, field: Option<String> },
    HashDel { key: String, field: String },
    ListAdd(String),
    ListSet { key: String, index: isize },
    ListDel { key: String, index: isize },
    SetAdd(String),
    SetReplace { key: String, old: String },
    SetDel { key: String, member: String },
    ZsetSet { key: String, old: Option<String> },
    ZsetDel { key: String, member: String },
    StreamAdd(String),
    StreamDel { key: String, id: String },
    SelectDb,
    RunCommand(String),
}

#[derive(Clone, Debug)]
pub enum FieldKind {
    Text,
    Secret,
    Bool,
    Choice(Vec<String>),
    /// A heading. Not focusable, and contributes no value on submit.
    Section,
}

#[derive(Clone, Debug)]
pub struct Field {
    pub label: String,
    pub kind: FieldKind,
    pub input: InputBuf,
    pub flag: bool,
    pub choice: usize,
}

impl Field {
    pub fn text(label: &str, initial: &str) -> Self {
        Self {
            label: label.into(),
            kind: FieldKind::Text,
            input: InputBuf::new(initial),
            flag: false,
            choice: 0,
        }
    }
    pub fn secret(label: &str, initial: &str) -> Self {
        Self {
            kind: FieldKind::Secret,
            ..Self::text(label, initial)
        }
    }
    pub fn boolean(label: &str, value: bool) -> Self {
        Self {
            kind: FieldKind::Bool,
            flag: value,
            ..Self::text(label, "")
        }
    }
    pub fn section(label: &str) -> Self {
        Self {
            kind: FieldKind::Section,
            ..Self::text(label, "")
        }
    }
    pub fn is_input(&self) -> bool {
        !matches!(self.kind, FieldKind::Section)
    }
    /// Height in terminal rows when rendered.
    pub fn height(&self) -> u16 {
        if self.is_input() {
            3
        } else {
            2
        }
    }
    pub fn choice(label: &str, options: &[&str], selected: usize) -> Self {
        Self {
            kind: FieldKind::Choice(options.iter().map(|s| s.to_string()).collect()),
            choice: selected,
            ..Self::text(label, "")
        }
    }
    pub fn value(&self) -> String {
        self.input.value()
    }
}

/// Tabs of the server-info modal, in display order.
pub const INFO_TABS: [&str; 5] = ["Server", "Memory", "Stats", "Key Statistics", "All"];

/// One rendered line of the server-info modal.
#[derive(Clone, Debug, PartialEq)]
pub enum InfoRow {
    Head(String),
    Field(String, String),
    /// A proportion worth seeing as a bar: memory used, cache hit rate.
    Gauge {
        label: String,
        ratio: f64,
        text: String,
        /// True when a full bar is the bad outcome (memory), false when it is
        /// the good one (hit rate).
        alarm_high: bool,
    },
}

impl InfoRow {
    /// The text a filter matches against.
    fn haystack(&self) -> String {
        match self {
            Self::Head(h) => h.clone(),
            Self::Field(k, v) => format!("{k} {v}"),
            Self::Gauge { label, text, .. } => format!("{label} {text}"),
        }
    }
}

pub struct InfoState {
    pub info: ServerInfo,
    pub tab: usize,
    pub scroll: u16,
    /// Live text of the field filter, while it has focus.
    pub filter: Option<InputBuf>,
    /// The applied filter. Empty means "show everything".
    pub query: String,
}

impl InfoState {
    pub fn new(info: ServerInfo) -> Self {
        Self {
            info,
            tab: 0,
            scroll: 0,
            filter: None,
            query: String::new(),
        }
    }

    /// Rows for the selected tab, after the filter. Curated tabs mirror one
    /// `INFO` section; "Key Statistics" is assembled from Keyspace plus the
    /// hit/miss counters.
    pub fn rows(&self) -> Vec<InfoRow> {
        let rows = match INFO_TABS[self.tab.min(INFO_TABS.len() - 1)] {
            "Server" => self.server(),
            "Memory" => self.memory(),
            "Stats" => section_rows(&self.info, "Stats"),
            "Key Statistics" => self.key_statistics(),
            _ => self
                .info
                .sections
                .iter()
                .flat_map(|s| {
                    std::iter::once(InfoRow::Head(s.name.clone())).chain(
                        s.fields
                            .iter()
                            .map(|(k, v)| InfoRow::Field(k.clone(), v.clone())),
                    )
                })
                .collect(),
        };
        filter_rows(rows, &self.query)
    }

    /// Headline facts first, then the whole Server section.
    fn server(&self) -> Vec<InfoRow> {
        let field = |k: &str| self.info.field(k).unwrap_or("?").to_string();
        let mut rows = vec![
            InfoRow::Head("Overview".into()),
            InfoRow::Field(
                "version".into(),
                format!("redis {} · {}", field("redis_version"), field("redis_mode")),
            ),
            InfoRow::Field("uptime".into(), human_uptime(&field("uptime_in_seconds"))),
            InfoRow::Field("os".into(), field("os")),
        ];
        if let Some(v) = self.info.field("connected_clients") {
            rows.push(InfoRow::Field("clients".into(), v.to_string()));
        }
        if let Some(v) = self.info.field("used_memory_human") {
            rows.push(InfoRow::Field("memory".into(), v.to_string()));
        }
        let keys: u64 = self.info.keyspace().iter().map(|(_, k, _)| k).sum();
        rows.push(InfoRow::Field("keys".into(), keys.to_string()));
        rows.push(InfoRow::Head("Server".into()));
        rows.extend(section_rows(&self.info, "Server"));
        rows
    }

    /// The Memory section, led by a used / maxmemory bar when one is set.
    fn memory(&self) -> Vec<InfoRow> {
        let used: f64 = self
            .info
            .field("used_memory")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.0);
        let max: f64 = self
            .info
            .field("maxmemory")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.0);
        let mut rows = Vec::new();
        if max > 0.0 {
            rows.push(InfoRow::Head("Usage".into()));
            rows.push(InfoRow::Gauge {
                label: "used / max".into(),
                ratio: (used / max).clamp(0.0, 1.0),
                text: format!(
                    "{} of {}",
                    self.info.field("used_memory_human").unwrap_or("?"),
                    self.info.field("maxmemory_human").unwrap_or("?")
                ),
                alarm_high: true,
            });
            if let Some(policy) = self.info.field("maxmemory_policy") {
                rows.push(InfoRow::Field("eviction policy".into(), policy.into()));
            }
            rows.push(InfoRow::Head("Memory".into()));
        }
        rows.extend(section_rows(&self.info, "Memory"));
        rows
    }

    fn key_statistics(&self) -> Vec<InfoRow> {
        let mut rows = vec![InfoRow::Head("Keyspace".into())];
        let keyspace = self.info.keyspace();
        if keyspace.is_empty() {
            rows.push(InfoRow::Field("(empty)".into(), "no keys in any db".into()));
        }
        let mut total_keys = 0u64;
        let mut total_expires = 0u64;
        for (db, keys, expires) in &keyspace {
            total_keys += keys;
            total_expires += expires;
            rows.push(InfoRow::Field(db.clone(), key_count(*keys, *expires)));
        }
        if keyspace.len() > 1 {
            rows.push(InfoRow::Field(
                "total".into(),
                key_count(total_keys, total_expires),
            ));
        }

        rows.push(InfoRow::Head("Lookups".into()));
        let field = |k: &str| self.info.field(k).unwrap_or("0").to_string();
        let hits: f64 = field("keyspace_hits").parse().unwrap_or(0.0);
        let misses: f64 = field("keyspace_misses").parse().unwrap_or(0.0);
        if hits + misses > 0.0 {
            rows.push(InfoRow::Gauge {
                label: "hit rate".into(),
                ratio: hits / (hits + misses),
                text: format!("{:.1}%", hits / (hits + misses) * 100.0),
                alarm_high: false,
            });
        } else {
            rows.push(InfoRow::Field("hit rate".into(), "n/a".into()));
        }
        rows.push(InfoRow::Field(
            "keyspace_hits".into(),
            field("keyspace_hits"),
        ));
        rows.push(InfoRow::Field(
            "keyspace_misses".into(),
            field("keyspace_misses"),
        ));

        rows.push(InfoRow::Head("Churn".into()));
        for k in [
            "expired_keys",
            "evicted_keys",
            "total_reads_processed",
            "total_writes_processed",
        ] {
            if let Some(v) = self.info.field(k) {
                rows.push(InfoRow::Field(k.into(), v.to_string()));
            }
        }
        let limits: Vec<InfoRow> = ["used_memory_human", "maxmemory_human", "maxmemory_policy"]
            .iter()
            .filter_map(|k| {
                self.info
                    .field(k)
                    .map(|v| InfoRow::Field((*k).into(), v.to_string()))
            })
            .collect();
        if !limits.is_empty() {
            rows.push(InfoRow::Head("Limits".into()));
            rows.extend(limits);
        }
        rows
    }

    /// The current tab as plain text, for the clipboard.
    pub fn text(&self) -> String {
        self.rows()
            .iter()
            .map(|r| match r {
                InfoRow::Head(h) => format!("# {h}"),
                InfoRow::Field(k, v) => format!("{k}: {v}"),
                InfoRow::Gauge { label, text, .. } => format!("{label}: {text}"),
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// Keep the rows a filter matches, and any heading that still has children.
fn filter_rows(rows: Vec<InfoRow>, query: &str) -> Vec<InfoRow> {
    let needle = query.trim().to_lowercase();
    if needle.is_empty() {
        return rows;
    }
    let mut out: Vec<InfoRow> = Vec::new();
    for row in rows {
        match row {
            InfoRow::Head(_) => {
                // Drop the previous heading if nothing under it matched.
                if matches!(out.last(), Some(InfoRow::Head(_))) {
                    out.pop();
                }
                out.push(row);
            }
            _ if row.haystack().to_lowercase().contains(&needle) => out.push(row),
            _ => {}
        }
    }
    if matches!(out.last(), Some(InfoRow::Head(_))) {
        out.pop();
    }
    out
}

fn section_rows(info: &ServerInfo, name: &str) -> Vec<InfoRow> {
    let fields = info.section(name);
    if fields.is_empty() {
        return vec![InfoRow::Field(
            "(empty)".into(),
            format!("this server reports no {name} section"),
        )];
    }
    fields
        .iter()
        .map(|(k, v)| InfoRow::Field(k.clone(), v.clone()))
        .collect()
}

/// "12 keys · 3 with a TTL", singular where it matters.
fn key_count(keys: u64, expires: u64) -> String {
    let plural = if keys == 1 { "key" } else { "keys" };
    format!("{keys} {plural} · {expires} with a TTL")
}

/// `uptime_in_seconds` as something a human reads at a glance.
fn human_uptime(secs: &str) -> String {
    let Ok(s) = secs.parse::<u64>() else {
        return secs.to_string();
    };
    let (d, h, m) = (s / 86_400, (s % 86_400) / 3600, (s % 3600) / 60);
    match (d, h) {
        (0, 0) => format!("{m}m{}s", s % 60),
        (0, _) => format!("{h}h{m}m"),
        _ => format!("{d}d{h}h"),
    }
}

pub struct ConsoleState {
    pub input: InputBuf,
    pub log: Vec<String>,
    pub history: Vec<String>,
    pub hist_idx: Option<usize>,
}

pub enum Modal {
    Confirm {
        message: String,
        action: Action,
    },
    Form {
        title: String,
        hint: String,
        fields: Vec<Field>,
        focus: usize,
        error: Option<String>,
        action: Action,
    },
    Editor {
        title: String,
        textarea: Box<TextArea<'static>>,
        action: Action,
        /// Whether the value being edited is JSON, and how it was stored.
        json: JsonMode,
        error: Option<String>,
    },
    Message {
        title: String,
        body: String,
    },
    Console(ConsoleState),
    Info(Box<InfoState>),
    ThemePicker {
        selected: usize,
        original: Theme,
    },
    Help,
}

pub struct App {
    pub store: Store,
    pub screen: Screen,
    pub should_quit: bool,
    pub status: String,
    pub tx: UnboundedSender<Msg>,

    pub conn_state: ListState,
    pub connecting: bool,
    /// Live text of the server-list filter box, while it has focus.
    pub conn_filter: Option<InputBuf>,
    /// The applied filter. Empty means "show everything".
    pub conn_query: String,
    /// Name of the profile currently being tested, if any.
    pub testing: Option<String>,

    pub client: Option<Client>,
    pub server_line: String,
    /// The keys behind the tree, kept so a TTL can expire one locally.
    pub keys: Vec<KeyInfo>,
    pub tree: Tree,
    pub expanded: HashSet<String>,
    pub rows: Vec<VisibleRow>,
    pub tree_state: ListState,
    pub pattern: String,
    pub truncated: bool,
    pub dbsize: u64,
    pub key_count: usize,
    pub loading: bool,

    pub search: Option<InputBuf>,
    pub focus: Focus,
    pub current: Option<KeyInfo>,
    pub value: Option<KeyValue>,
    pub value_state: TableState,
    pub value_scroll: u16,

    pub modal: Option<Modal>,

    /// When the TTL clock last advanced.
    last_tick: std::time::Instant,
}

impl App {
    pub fn new(store: Store, tx: UnboundedSender<Msg>) -> Self {
        let mut conn_state = ListState::default();
        if !store.connections.is_empty() {
            conn_state.select(Some(0));
        }
        Self {
            store,
            screen: Screen::Connections,
            should_quit: false,
            status: String::new(),
            tx,
            conn_state,
            connecting: false,
            conn_filter: None,
            conn_query: String::new(),
            testing: None,
            client: None,
            server_line: String::new(),
            keys: Vec::new(),
            tree: Tree::default(),
            expanded: HashSet::new(),
            rows: Vec::new(),
            tree_state: ListState::default(),
            pattern: "*".into(),
            truncated: false,
            dbsize: 0,
            key_count: 0,
            loading: false,
            search: None,
            focus: Focus::Tree,
            current: None,
            value: None,
            value_state: TableState::default(),
            value_scroll: 0,
            modal: None,
            last_tick: std::time::Instant::now(),
        }
    }

    // ---- async plumbing -------------------------------------------------

    fn spawn<F>(&self, fut: F)
    where
        F: std::future::Future<Output = Msg> + Send + 'static,
    {
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let _ = tx.send(fut.await);
        });
    }

    pub fn connect(&mut self, conn: Connection) {
        self.connecting = true;
        self.status = format!("Connecting to {}:{} ...", conn.host, conn.port);
        self.spawn(async move {
            Msg::Connected(Box::new(
                Client::connect(conn).await.map_err(|e| e.to_string()),
            ))
        });
    }

    pub fn reload_keys(&mut self) {
        let Some(client) = self.client.clone() else {
            return;
        };
        let pattern = self.pattern.clone();
        self.loading = true;
        self.spawn(async move {
            match client.scan_keys(&pattern, KEY_LIMIT).await {
                Ok((keys, truncated)) => {
                    let dbsize = client.dbsize().await.unwrap_or(0);
                    Msg::Keys {
                        keys,
                        truncated,
                        dbsize,
                        pattern,
                    }
                }
                Err(e) => Msg::Error(format!("scan failed: {e}")),
            }
        });
    }

    pub fn reload_value(&mut self) {
        let (Some(client), Some(name)) = (
            self.client.clone(),
            self.current.as_ref().map(|k| k.name.clone()),
        ) else {
            return;
        };
        self.spawn(async move {
            match client.key_info(&name).await {
                Ok(info) => match client.read_value(&info.name, info.kind).await {
                    Ok(value) => Msg::Value { info, value },
                    Err(e) => Msg::Error(format!("read failed: {e}")),
                },
                Err(e) => Msg::Error(format!("read failed: {e}")),
            }
        });
    }

    fn mutate<F, Fut>(&mut self, ok_status: &str, f: F)
    where
        F: FnOnce(Client) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = anyhow::Result<()>> + Send,
    {
        let Some(client) = self.client.clone() else {
            return;
        };
        let ok = ok_status.to_string();
        self.spawn(async move {
            match f(client).await {
                Ok(()) => Msg::Mutated(Ok(ok)),
                Err(e) => Msg::Mutated(Err(e.to_string())),
            }
        });
    }

    // ---- the TTL clock ----------------------------------------------------

    /// Called from the event loop. Advances the cached TTLs by however much
    /// wall clock has passed since the last call.
    pub fn on_tick(&mut self) {
        let secs = self.last_tick.elapsed().as_secs() as i64;
        if secs < 1 {
            return;
        }
        self.last_tick += std::time::Duration::from_secs(secs as u64);
        self.age_ttls(secs);
    }

    /// Count the cached TTLs down and drop whatever just expired. The server
    /// expires keys on its own schedule; this only keeps the view honest
    /// between scans, and the next scan is still the truth.
    pub fn age_ttls(&mut self, secs: i64) {
        if self.screen != Screen::Browser || secs <= 0 {
            return;
        }
        if let Some(cur) = &mut self.current {
            if cur.ttl > 0 {
                cur.ttl = (cur.ttl - secs).max(0);
            }
        }
        let mut expired: Vec<String> = Vec::new();
        for k in &mut self.keys {
            if k.ttl > 0 {
                k.ttl -= secs;
                if k.ttl <= 0 {
                    expired.push(k.name.clone());
                }
            }
        }
        if expired.is_empty() {
            return;
        }
        self.keys.retain(|k| k.ttl > 0 || k.ttl == -1);
        self.key_count = self.keys.len();
        self.dbsize = self.dbsize.saturating_sub(expired.len() as u64);
        self.tree = Tree::build(&self.keys);
        self.rebuild_rows();
        if self.rows.is_empty() {
            self.tree_state.select(None);
        } else if let Some(idx) = self.tree_state.selected() {
            self.tree_state.select(Some(idx.min(self.rows.len() - 1)));
        }
        // The open key going away is worth saying out loud.
        if let Some(name) = self.current.as_ref().map(|c| c.name.clone()) {
            if expired.contains(&name) {
                self.current = None;
                self.value = None;
                self.focus = Focus::Tree;
                self.status = format!("'{name}' expired");
                return;
            }
        }
        self.status = match expired.len() {
            1 => format!("'{}' expired", expired[0]),
            n => format!("{n} keys expired"),
        };
    }

    // ---- message handling -------------------------------------------------

    pub fn on_msg(&mut self, msg: Msg) {
        match msg {
            Msg::Connected(result) => {
                self.connecting = false;
                match *result {
                    Ok(client) => {
                        let probe = client.clone();
                        self.spawn(async move {
                            Msg::Server(probe.server_line().await.unwrap_or_default())
                        });
                        self.client = Some(client);
                        self.screen = Screen::Browser;
                        self.pattern = "*".into();
                        self.status.clear();
                        self.current = None;
                        self.value = None;
                        self.expanded.clear();
                        self.reload_keys();
                    }
                    Err(e) => self.status = format!("Connection failed: {e}"),
                }
            }
            Msg::Server(line) => self.server_line = line,
            Msg::Keys {
                keys,
                truncated,
                dbsize,
                pattern,
            } => {
                self.loading = false;
                self.key_count = keys.len();
                self.truncated = truncated;
                self.dbsize = dbsize;
                self.pattern = pattern;
                self.tree = Tree::build(&keys);
                self.keys = keys.clone();
                // A narrow result set is more useful expanded than collapsed.
                if keys.len() <= 200 {
                    self.expanded.extend(self.tree.all_folder_paths());
                }
                self.rebuild_rows();
                if self.rows.is_empty() {
                    self.tree_state.select(None);
                } else {
                    let idx = self
                        .tree_state
                        .selected()
                        .unwrap_or(0)
                        .min(self.rows.len() - 1);
                    self.tree_state.select(Some(idx));
                }
            }
            Msg::Value { info, value } => {
                let same_key = self.current.as_ref().is_some_and(|c| c.name == info.name);
                self.current = Some(info);
                self.value = Some(value);
                if !same_key {
                    self.value_state.select(Some(0));
                    self.value_scroll = 0;
                } else if let Some(KeyValue::Rows { rows, .. }) = &self.value {
                    let idx = self.value_state.selected().unwrap_or(0);
                    self.value_state
                        .select(Some(idx.min(rows.len().saturating_sub(1))));
                }
            }
            Msg::Mutated(Ok(status)) => {
                self.status = status;
                self.reload_keys();
                self.reload_value();
            }
            Msg::Mutated(Err(e)) => self.status = format!("Error: {e}"),
            Msg::Console(text) => {
                if let Some(Modal::Console(c)) = &mut self.modal {
                    c.log.extend(text.lines().map(|l| l.to_string()));
                }
            }
            Msg::Probe(name, result) => {
                self.testing = None;
                self.status = match *result {
                    Ok(p) => format!(
                        "{name}: PONG in {:.1} ms · redis {} {} · {} key(s) in db",
                        p.latency_ms, p.version, p.mode, p.dbsize
                    ),
                    Err(e) => format!("Error: {name}: {e}"),
                };
            }
            Msg::Info(result) => match *result {
                Ok(info) => {
                    let (tab, scroll) = match &self.modal {
                        Some(Modal::Info(state)) => (state.tab, state.scroll),
                        _ => (0, 0),
                    };
                    self.status.clear();
                    self.modal = Some(Modal::Info(Box::new(InfoState {
                        tab,
                        scroll,
                        ..InfoState::new(info)
                    })));
                }
                Err(e) => self.status = format!("Error: INFO failed: {e}"),
            },
            Msg::Status(text) => self.status = text,
            Msg::Error(e) => {
                self.loading = false;
                self.testing = None;
                self.status = format!("Error: {e}");
            }
            Msg::Noop => {}
        }
    }

    pub fn rebuild_rows(&mut self) {
        self.rows = self.tree.visible(&self.expanded);
    }

    pub fn selected_row(&self) -> Option<&VisibleRow> {
        self.rows.get(self.tree_state.selected()?)
    }

    pub fn selected_value_row(&self) -> Option<&crate::redis_client::Row> {
        match self.value.as_ref()? {
            KeyValue::Rows { rows, .. } => rows.get(self.value_state.selected()?),
            _ => None,
        }
    }

    // ---- key events -------------------------------------------------------

    pub fn on_key(&mut self, key: KeyEvent) {
        if key.kind == KeyEventKind::Release {
            return;
        }
        if self.modal.is_some() {
            self.modal_key(key);
            return;
        }
        if let Some(buf) = &mut self.conn_filter {
            match key.code {
                KeyCode::Esc => self.conn_filter = None,
                KeyCode::Enter => {
                    self.conn_query = buf.value().trim().to_string();
                    self.conn_filter = None;
                    self.clamp_connection_selection();
                }
                _ => {
                    buf.handle(key);
                    // Filtering as you type keeps the list honest about what
                    // Enter will leave behind.
                    self.conn_query = buf.value().trim().to_string();
                    self.clamp_connection_selection();
                }
            }
            return;
        }
        if let Some(buf) = &mut self.search {
            match key.code {
                KeyCode::Esc => {
                    self.search = None;
                    self.status.clear();
                }
                KeyCode::Enter => {
                    let raw = buf.value().trim().to_string();
                    self.search = None;
                    self.pattern = normalize_pattern(&raw);
                    self.expanded.clear();
                    self.reload_keys();
                }
                _ => {
                    buf.handle(key);
                }
            }
            return;
        }
        match self.screen {
            Screen::Connections => self.connections_key(key),
            Screen::Browser => self.browser_key(key),
        }
    }

    fn connections_key(&mut self, key: KeyEvent) {
        let len = self.visible_connections().len();
        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Esc => {
                if self.conn_query.is_empty() {
                    self.should_quit = true;
                } else {
                    self.conn_query.clear();
                    self.clamp_connection_selection();
                }
            }
            KeyCode::Char('?') => self.modal = Some(Modal::Help),
            KeyCode::Char('p') => self.open_theme_picker(),
            KeyCode::Char('/') => self.conn_filter = Some(InputBuf::new(&self.conn_query)),
            KeyCode::Down | KeyCode::Char('j') => move_sel(&mut self.conn_state, len, 1),
            KeyCode::Up | KeyCode::Char('k') => move_sel(&mut self.conn_state, len, -1),
            KeyCode::Char('J') => self.reorder_connection(1),
            KeyCode::Char('K') => self.reorder_connection(-1),
            KeyCode::Char('c') => self.duplicate_connection(),
            KeyCode::Char('T') => self.test_connection(),
            KeyCode::Char('n') => self.open_connection_form(None),
            KeyCode::Char('e') => {
                if let Some(c) = self.selected_connection() {
                    self.open_connection_form(Some(c));
                }
            }
            KeyCode::Char('d') => {
                if let Some(c) = self.selected_connection() {
                    self.modal = Some(Modal::Confirm {
                        message: format!("Delete saved connection '{}'?", c.name),
                        action: Action::DeleteConnection(c.name),
                    });
                }
            }
            KeyCode::Enter => {
                if let Some(c) = self.selected_connection() {
                    self.connect(c);
                } else {
                    self.open_connection_form(None);
                }
            }
            _ => {}
        }
    }

    /// Indices into `store.connections` that pass the current filter, in order.
    pub fn visible_connections(&self) -> Vec<usize> {
        if self.conn_query.is_empty() {
            return (0..self.store.connections.len()).collect();
        }
        let needle = self.conn_query.to_lowercase();
        self.store
            .connections
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                c.name.to_lowercase().contains(&needle) || c.host.to_lowercase().contains(&needle)
            })
            .map(|(i, _)| i)
            .collect()
    }

    fn selected_index(&self) -> Option<usize> {
        self.visible_connections()
            .get(self.conn_state.selected()?)
            .copied()
    }

    fn selected_connection(&self) -> Option<Connection> {
        self.store.connections.get(self.selected_index()?).cloned()
    }

    fn clamp_connection_selection(&mut self) {
        let len = self.visible_connections().len();
        self.conn_state.select(if len == 0 {
            None
        } else {
            Some(self.conn_state.selected().unwrap_or(0).min(len - 1))
        });
    }

    /// Put the cursor back on a profile by name after a save or duplicate.
    fn focus_connection(&mut self, name: &str) {
        let pos = self
            .visible_connections()
            .iter()
            .position(|i| self.store.connections[*i].name == name);
        match pos {
            Some(p) => self.conn_state.select(Some(p)),
            None => self.clamp_connection_selection(),
        }
    }

    fn duplicate_connection(&mut self) {
        let Some(index) = self.selected_index() else {
            return;
        };
        if let Some(new_index) = self.store.duplicate(index) {
            let name = self.store.connections[new_index].name.clone();
            if let Err(e) = self.store.save() {
                self.status = format!("Could not save connections: {e}");
                return;
            }
            self.focus_connection(&name);
            self.status = format!("Duplicated as '{name}'");
        }
    }

    /// Reordering rewrites the stored order, so it only makes sense against the
    /// unfiltered list.
    fn reorder_connection(&mut self, delta: isize) {
        if !self.conn_query.is_empty() {
            self.status = "Clear the filter (esc) before reordering".into();
            return;
        }
        let Some(index) = self.selected_index() else {
            return;
        };
        let moved = self.store.move_by(index, delta);
        if moved != index {
            if let Err(e) = self.store.save() {
                self.status = format!("Could not save connections: {e}");
                return;
            }
            self.conn_state.select(Some(moved));
        }
    }

    fn test_connection(&mut self) {
        let Some(conn) = self.selected_connection() else {
            return;
        };
        let name = conn.name.clone();
        self.testing = Some(name.clone());
        self.status = format!("Testing {}:{} ...", conn.host, conn.port);
        self.spawn(async move {
            let result = Client::probe(conn).await.map_err(|e| e.to_string());
            Msg::Probe(name, Box::new(result))
        });
    }

    /// Reconcile the OS keychain with a profile that was just saved.
    fn sync_keychain(
        &mut self,
        previous: Option<Connection>,
        name: String,
        typed_password: String,
        use_keychain: bool,
    ) {
        let old_name = previous.as_ref().map(|p| p.name.clone());
        let was_keychain = previous.as_ref().is_some_and(|p| p.use_keychain);
        let renamed = old_name.clone().filter(|o| *o != name);

        if !use_keychain {
            // Opted out: drop any secret we were holding for this profile.
            if was_keychain {
                if let Some(old) = old_name {
                    self.spawn(async move {
                        keychain_task(move || {
                            crate::secrets::delete(&old)?;
                            Ok(None)
                        })
                        .await
                    });
                }
            }
            return;
        }

        self.spawn(async move {
            keychain_task(move || {
                let secret = if !typed_password.is_empty() {
                    Some(typed_password)
                } else if was_keychain {
                    // No new password typed. Migrate the stored one if the
                    // profile was renamed, otherwise leave it alone.
                    match &renamed {
                        Some(_) => Some(crate::secrets::get(old_name.as_deref().unwrap_or(""))?),
                        None => None,
                    }
                } else {
                    None
                };
                let stored = secret.is_some();
                if let Some(s) = secret {
                    crate::secrets::set(&name, &s)?;
                }
                if was_keychain {
                    if let Some(old) = renamed {
                        crate::secrets::delete(&old)?;
                    }
                }
                Ok(Some(if stored {
                    format!("Password for '{name}' stored in the OS keychain")
                } else if was_keychain {
                    format!("'{name}' keeps its existing keychain entry")
                } else {
                    format!("'{name}' uses the keychain, but no password was entered")
                }))
            })
            .await
        });
    }

    fn open_connection_form(&mut self, existing: Option<Connection>) {
        let c = existing.clone().unwrap_or_default();
        let keychain_label = match crate::secrets::unavailable_reason() {
            None => "Store the password in the OS keychain".to_string(),
            Some(_) => "Store in the OS keychain (unavailable on this machine)".to_string(),
        };
        // A stored secret is never read back just to populate a form; an empty
        // password field on save means "keep whatever is already there".
        let password_label = if c.use_keychain {
            "Password (blank keeps the stored secret)"
        } else {
            "Password (optional, ${ENV_VAR} works)"
        };
        self.modal = Some(Modal::Form {
            title: if existing.is_some() {
                "Edit connection".into()
            } else {
                "New connection".into()
            },
            hint: "Tab/↑↓ move · Space toggles · Enter saves".into(),
            fields: vec![
                Field::section("Server"),
                Field::text("Name", &c.name),
                Field::text("Host", &c.host),
                Field::text("Port", &c.port.to_string()),
                Field::text("Database", &c.db.to_string()),
                Field::section("Authentication"),
                Field::text("Username (optional)", &c.username),
                Field::secret(
                    password_label,
                    if c.use_keychain { "" } else { &c.password },
                ),
                Field::boolean(&keychain_label, c.use_keychain),
                Field::section("TLS"),
                Field::boolean("Use TLS", c.tls),
                Field::text("CA certificate file (optional)", &c.tls_ca_file),
                Field::text("Client certificate file (optional)", &c.tls_cert_file),
                Field::text("Client key file (optional)", &c.tls_key_file),
                Field::boolean("Skip certificate verification (unsafe)", c.tls_insecure),
            ],
            focus: 1,
            error: None,
            action: Action::SaveConnection {
                replacing: existing.map(|e| e.name),
            },
        });
    }

    fn browser_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Char('d') if ctrl => self.prompt_select_db(),
            KeyCode::Char('n') if ctrl => self.back_to_connections(),
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Esc => {
                if self.pattern != "*" {
                    self.pattern = "*".into();
                    self.reload_keys();
                }
            }
            KeyCode::Char('?') => self.modal = Some(Modal::Help),
            KeyCode::Char('p') => self.open_theme_picker(),
            KeyCode::Char('/') => self.search = Some(InputBuf::new("")),
            KeyCode::Char(':') => self.open_console(),
            KeyCode::Char('r') => {
                self.reload_keys();
                self.reload_value();
            }
            KeyCode::Tab => {
                self.focus = match self.focus {
                    Focus::Tree => Focus::Value,
                    Focus::Value => Focus::Tree,
                }
            }
            KeyCode::Char('y') => self.yank_key_name(),
            KeyCode::Char('i') => self.load_info(),
            KeyCode::Char('n') => self.prompt_new_key(),
            KeyCode::Char('t') => self.prompt_ttl(),
            KeyCode::Char('R') => self.prompt_rename(),
            KeyCode::Char('D') => self.confirm_delete_key(),
            KeyCode::Char('a') => self.prompt_add_item(),
            KeyCode::Char('e') | KeyCode::Char('E') => self.prompt_edit(),
            KeyCode::Char('x') => self.confirm_delete_row(),
            _ => match self.focus {
                Focus::Tree => self.tree_key(key),
                Focus::Value => self.value_key(key),
            },
        }
    }

    fn tree_key(&mut self, key: KeyEvent) {
        let len = self.rows.len();
        match key.code {
            KeyCode::Down | KeyCode::Char('j') => {
                move_sel(&mut self.tree_state, len, 1);
                self.on_tree_move();
            }
            KeyCode::Up | KeyCode::Char('k') => {
                move_sel(&mut self.tree_state, len, -1);
                self.on_tree_move();
            }
            KeyCode::PageDown => {
                move_sel(&mut self.tree_state, len, 10);
                self.on_tree_move();
            }
            KeyCode::PageUp => {
                move_sel(&mut self.tree_state, len, -10);
                self.on_tree_move();
            }
            KeyCode::Char('g') | KeyCode::Home => {
                if len > 0 {
                    self.tree_state.select(Some(0));
                    self.on_tree_move();
                }
            }
            KeyCode::Char('G') | KeyCode::End => {
                if len > 0 {
                    self.tree_state.select(Some(len - 1));
                    self.on_tree_move();
                }
            }
            KeyCode::Enter | KeyCode::Char(' ') | KeyCode::Right | KeyCode::Char('l') => {
                self.toggle_or_open()
            }
            KeyCode::Left | KeyCode::Char('h') => {
                if let Some(path) = self.selected_row().and_then(|r| r.folder_path.clone()) {
                    self.expanded.remove(&path);
                    self.rebuild_rows();
                }
            }
            _ => {}
        }
    }

    /// Moving the tree cursor onto a key loads it; folders clear the pane.
    fn on_tree_move(&mut self) {
        match self.selected_row().and_then(|r| r.key.clone()) {
            Some(k) => {
                if self.current.as_ref().map(|c| &c.name) != Some(&k.name) {
                    self.current = Some(k);
                    self.value = None;
                    self.reload_value();
                }
            }
            None => {
                self.current = None;
                self.value = None;
            }
        }
    }

    fn toggle_or_open(&mut self) {
        let Some(row) = self.selected_row().cloned() else {
            return;
        };
        if let Some(path) = row.folder_path {
            if !self.expanded.remove(&path) {
                self.expanded.insert(path);
            }
            self.rebuild_rows();
        } else {
            self.focus = Focus::Value;
        }
    }

    fn value_key(&mut self, key: KeyEvent) {
        let len = match &self.value {
            Some(KeyValue::Rows { rows, .. }) => rows.len(),
            _ => 0,
        };
        match key.code {
            KeyCode::Down | KeyCode::Char('j') => {
                if len > 0 {
                    move_sel(&mut self.value_state, len, 1)
                } else {
                    self.value_scroll = self.value_scroll.saturating_add(1)
                }
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if len > 0 {
                    move_sel(&mut self.value_state, len, -1)
                } else {
                    self.value_scroll = self.value_scroll.saturating_sub(1)
                }
            }
            KeyCode::PageDown => {
                if len > 0 {
                    move_sel(&mut self.value_state, len, 10)
                } else {
                    self.value_scroll = self.value_scroll.saturating_add(10)
                }
            }
            KeyCode::PageUp => {
                if len > 0 {
                    move_sel(&mut self.value_state, len, -10)
                } else {
                    self.value_scroll = self.value_scroll.saturating_sub(10)
                }
            }
            KeyCode::Char('g') | KeyCode::Home => {
                self.value_scroll = 0;
                if len > 0 {
                    self.value_state.select(Some(0))
                }
            }
            KeyCode::Char('G') | KeyCode::End => {
                if len > 0 {
                    self.value_state.select(Some(len - 1))
                }
            }
            KeyCode::Left | KeyCode::Char('h') => self.focus = Focus::Tree,
            _ => {}
        }
    }

    // ---- browser commands --------------------------------------------------

    fn back_to_connections(&mut self) {
        self.client = None;
        self.screen = Screen::Connections;
        self.current = None;
        self.value = None;
        self.rows.clear();
        self.status.clear();
    }

    fn yank_key_name(&mut self) {
        let Some(name) = self.current.as_ref().map(|k| k.name.clone()) else {
            return;
        };
        crate::osc52::copy(&name);
        self.status = format!("Copied '{name}' to clipboard");
    }

    fn prompt_new_key(&mut self) {
        let types: Vec<&str> = NEW_KEY_TYPES.iter().map(|t| t.name()).collect();
        self.modal = Some(Modal::Form {
            title: "New key".into(),
            hint: "Tab/↑↓ move · ←→ pick type · Enter creates · Esc cancels".into(),
            fields: vec![
                Field::text("Key name", ""),
                Field::choice("Type", &types, 0),
            ],
            focus: 0,
            error: None,
            action: Action::NewKey,
        });
    }

    fn confirm_delete_key(&mut self) {
        let Some(k) = self.current.clone() else {
            self.status = "No key selected".into();
            return;
        };
        self.modal = Some(Modal::Confirm {
            message: format!("Delete key '{}'? This cannot be undone.", k.name),
            action: Action::DeleteKey(k.name),
        });
    }

    fn prompt_rename(&mut self) {
        let Some(k) = self.current.clone() else {
            return;
        };
        self.modal = Some(Modal::Form {
            title: format!("Rename '{}'", k.name),
            hint: "Enter renames · Esc cancels".into(),
            fields: vec![Field::text("New name", &k.name)],
            focus: 0,
            error: None,
            action: Action::RenameKey(k.name),
        });
    }

    fn prompt_ttl(&mut self) {
        let Some(k) = self.current.clone() else {
            return;
        };
        let current = if k.ttl < 0 {
            String::new()
        } else {
            k.ttl.to_string()
        };
        self.modal = Some(Modal::Form {
            title: format!("TTL for '{}'", k.name),
            hint: "Seconds, or empty to remove the expiry · Enter applies".into(),
            fields: vec![Field::text("Seconds", &current)],
            focus: 0,
            error: None,
            action: Action::SetTtl(k.name),
        });
    }

    fn prompt_select_db(&mut self) {
        let db = self.client.as_ref().map(|c| c.conn.db).unwrap_or(0);
        self.modal = Some(Modal::Form {
            title: "Switch database".into(),
            hint: "Reconnects on the chosen index · Enter applies".into(),
            fields: vec![Field::text("DB index", &db.to_string())],
            focus: 0,
            error: None,
            action: Action::SelectDb,
        });
    }

    fn prompt_add_item(&mut self) {
        let Some(k) = self.current.clone() else {
            return;
        };
        let name = k.name.clone();
        self.modal = Some(match k.kind {
            KeyType::Hash => Modal::Form {
                title: format!("Add field to '{name}'"),
                hint: "Enter saves · Esc cancels".into(),
                fields: vec![Field::text("Field", ""), Field::text("Value", "")],
                focus: 0,
                error: None,
                action: Action::HashSet {
                    key: name,
                    field: None,
                },
            },
            KeyType::List => Modal::Form {
                title: format!("Append to '{name}' (RPUSH)"),
                hint: "Enter saves · Esc cancels".into(),
                fields: vec![Field::text("Value", "")],
                focus: 0,
                error: None,
                action: Action::ListAdd(name),
            },
            KeyType::Set => Modal::Form {
                title: format!("Add member to '{name}' (SADD)"),
                hint: "Enter saves · Esc cancels".into(),
                fields: vec![Field::text("Member", "")],
                focus: 0,
                error: None,
                action: Action::SetAdd(name),
            },
            KeyType::ZSet => Modal::Form {
                title: format!("Add member to '{name}' (ZADD)"),
                hint: "Enter saves · Esc cancels".into(),
                fields: vec![Field::text("Member", ""), Field::text("Score", "0")],
                focus: 0,
                error: None,
                action: Action::ZsetSet {
                    key: name,
                    old: None,
                },
            },
            KeyType::Stream => Modal::Form {
                title: format!("Add entry to '{name}' (XADD *)"),
                hint: "Enter saves · Esc cancels".into(),
                fields: vec![Field::text("Field", ""), Field::text("Value", "")],
                focus: 0,
                error: None,
                action: Action::StreamAdd(name),
            },
            KeyType::String | KeyType::Other => Modal::Message {
                title: "Nothing to add".into(),
                body: "Strings hold a single value — press 'e' to edit it.".into(),
            },
        });
    }

    fn prompt_edit(&mut self) {
        let Some(k) = self.current.clone() else {
            return;
        };
        let name = k.name.clone();
        if k.kind == KeyType::String {
            let current = match &self.value {
                Some(KeyValue::Str(s)) => s.clone(),
                _ => String::new(),
            };
            // JSON opens indented, however it was stored.
            let mode = json::mode(&current);
            let text = if mode.is_json() {
                json::pretty(&current)
            } else {
                current
            };
            let mut ta = TextArea::from(text.lines().collect::<Vec<_>>());
            ta.set_cursor_line_style(ratatui::style::Style::default());
            let title = if mode.is_json() {
                format!("Edit JSON '{name}'")
            } else {
                format!("Edit string '{name}'")
            };
            self.modal = Some(Modal::Editor {
                title,
                textarea: Box::new(ta),
                action: Action::EditString(name),
                json: mode,
                error: None,
            });
            return;
        }
        let Some(row) = self.selected_value_row().cloned() else {
            self.status = "No element selected".into();
            return;
        };
        self.modal = Some(match k.kind {
            KeyType::Hash => Modal::Form {
                title: format!("Edit field '{}'", row.id),
                hint: "The field name is fixed; rename via delete + add".into(),
                fields: vec![Field::text("Value", row.cells.get(1).map_or("", |v| v))],
                focus: 0,
                error: None,
                action: Action::HashSet {
                    key: name,
                    field: Some(row.id),
                },
            },
            KeyType::List => {
                let index: isize = row.id.parse().unwrap_or(0);
                Modal::Form {
                    title: format!("Edit item [{index}] (LSET)"),
                    hint: "Enter saves · Esc cancels".into(),
                    fields: vec![Field::text("Value", row.cells.get(1).map_or("", |v| v))],
                    focus: 0,
                    error: None,
                    action: Action::ListSet { key: name, index },
                }
            }
            KeyType::Set => Modal::Form {
                title: "Replace set member".into(),
                hint: "Removes the old member and adds the new one".into(),
                fields: vec![Field::text("Member", &row.id)],
                focus: 0,
                error: None,
                action: Action::SetReplace {
                    key: name,
                    old: row.id,
                },
            },
            KeyType::ZSet => Modal::Form {
                title: "Edit sorted-set member".into(),
                hint: "Enter saves · Esc cancels".into(),
                fields: vec![
                    Field::text("Member", &row.id),
                    Field::text("Score", row.cells.get(1).map_or("0", |v| v)),
                ],
                focus: 0,
                error: None,
                action: Action::ZsetSet {
                    key: name,
                    old: Some(row.id),
                },
            },
            KeyType::Stream | KeyType::String | KeyType::Other => Modal::Message {
                title: "Not editable".into(),
                body: "Stream entries are immutable. Add a new entry with 'a', or delete this one with 'x'.".into(),
            },
        });
    }

    fn confirm_delete_row(&mut self) {
        let Some(k) = self.current.clone() else {
            return;
        };
        let Some(row) = self.selected_value_row().cloned() else {
            self.status = "No element selected".into();
            return;
        };
        let name = k.name.clone();
        let (message, action) = match k.kind {
            KeyType::Hash => (
                format!("Delete field '{}' from '{name}'?", row.id),
                Action::HashDel {
                    key: name,
                    field: row.id,
                },
            ),
            KeyType::List => {
                let index: isize = row.id.parse().unwrap_or(0);
                (
                    format!("Delete item [{index}] from '{name}'?"),
                    Action::ListDel { key: name, index },
                )
            }
            KeyType::Set => (
                format!("Remove member '{}' from '{name}'?", row.id),
                Action::SetDel {
                    key: name,
                    member: row.id,
                },
            ),
            KeyType::ZSet => (
                format!("Remove member '{}' from '{name}'?", row.id),
                Action::ZsetDel {
                    key: name,
                    member: row.id,
                },
            ),
            KeyType::Stream => (
                format!("Delete entry '{}' from '{name}'?", row.id),
                Action::StreamDel {
                    key: name,
                    id: row.id,
                },
            ),
            KeyType::String | KeyType::Other => {
                self.status = "Use 'D' to delete the whole key".into();
                return;
            }
        };
        self.modal = Some(Modal::Confirm { message, action });
    }

    /// Fetch `INFO` and show it in the server-info modal.
    fn load_info(&mut self) {
        let Some(client) = self.client.clone() else {
            return;
        };
        self.status = "Loading server info ...".into();
        self.spawn(
            async move { Msg::Info(Box::new(client.info().await.map_err(|e| e.to_string()))) },
        );
    }

    fn open_console(&mut self) {
        self.modal = Some(Modal::Console(ConsoleState {
            input: InputBuf::new(""),
            log: vec![
                "Type a Redis command and press Enter. Esc closes.".into(),
                "Destructive commands (FLUSHALL, FLUSHDB, ...) ask first.".into(),
                String::new(),
            ],
            history: Vec::new(),
            hist_idx: None,
        }));
    }

    fn open_theme_picker(&mut self) {
        let original = self.store.theme;
        self.modal = Some(Modal::ThemePicker {
            selected: original.index(),
            original,
        });
    }

    // ---- modal input ------------------------------------------------------

    fn modal_key(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        // Theme navigation updates the store for an immediate preview. Escape
        // restores the opening value; Enter keeps it and persists it.
        if let Some(Modal::ThemePicker { selected, original }) = &mut self.modal {
            let mut close = false;
            let mut save = false;
            match key.code {
                KeyCode::Esc | KeyCode::Char('q') => {
                    self.store.theme = *original;
                    close = true;
                }
                KeyCode::Enter => {
                    save = true;
                    close = true;
                }
                KeyCode::Down | KeyCode::Char('j') | KeyCode::Right | KeyCode::Char('l') => {
                    *selected = (*selected + 1) % Theme::ALL.len();
                    self.store.theme = Theme::ALL[*selected];
                }
                KeyCode::Up | KeyCode::Char('k') | KeyCode::Left | KeyCode::Char('h') => {
                    *selected = (*selected + Theme::ALL.len() - 1) % Theme::ALL.len();
                    self.store.theme = Theme::ALL[*selected];
                }
                KeyCode::Home | KeyCode::Char('g') => {
                    *selected = 0;
                    self.store.theme = Theme::ALL[0];
                }
                KeyCode::End | KeyCode::Char('G') => {
                    *selected = Theme::ALL.len() - 1;
                    self.store.theme = Theme::ALL[*selected];
                }
                _ => {}
            }
            if save {
                self.status = match self.store.save() {
                    Ok(()) => format!("Theme set to {}", self.store.theme.name()),
                    Err(e) => format!("Could not save theme: {e}"),
                };
            }
            if close {
                self.modal = None;
            }
            return;
        }
        match self.modal.as_mut().expect("modal_key with no modal") {
            Modal::Help | Modal::Message { .. } => {
                if matches!(key.code, KeyCode::Esc | KeyCode::Enter | KeyCode::Char('q')) {
                    self.modal = None;
                }
            }
            Modal::Confirm { action, .. } => match key.code {
                KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
                    let action = action.clone();
                    self.modal = None;
                    self.run_action(action, Vec::new());
                }
                KeyCode::Esc | KeyCode::Char('n') | KeyCode::Char('N') => self.modal = None,
                _ => {}
            },
            Modal::Editor {
                textarea,
                action,
                json: mode,
                error,
                ..
            } => match key.code {
                KeyCode::Esc => self.modal = None,
                // Reformat without saving, so a hand-typed edit can be tidied.
                KeyCode::Char('f') if ctrl && mode.is_json() => {
                    let text = textarea.lines().join("\n");
                    match json::check(&text) {
                        Ok(()) => {
                            let mut ta =
                                TextArea::from(json::pretty(&text).lines().collect::<Vec<_>>());
                            ta.set_cursor_line_style(ratatui::style::Style::default());
                            **textarea = ta;
                            *error = None;
                        }
                        Err(e) => *error = Some(e),
                    }
                }
                KeyCode::Char('s') if ctrl => {
                    let text = textarea.lines().join("\n");
                    // A key that held JSON keeps holding JSON: refuse a broken
                    // edit rather than overwriting the document with garbage.
                    if mode.is_json() {
                        if let Err(e) = json::check(&text) {
                            *error = Some(e);
                            return;
                        }
                    }
                    // Written back in the shape the key already had.
                    let text = if *mode == JsonMode::Compact {
                        json::minify(&text)
                    } else {
                        text
                    };
                    let action = action.clone();
                    self.modal = None;
                    self.run_action(action, vec![text]);
                }
                _ => {
                    textarea.input(key);
                    *error = None;
                }
            },
            Modal::Form {
                fields,
                focus,
                action,
                error,
                ..
            } => match key.code {
                KeyCode::Esc => self.modal = None,
                KeyCode::Tab | KeyCode::Down => *focus = next_focus(fields, *focus, 1),
                KeyCode::BackTab | KeyCode::Up => *focus = next_focus(fields, *focus, -1),
                KeyCode::Enter => {
                    let values: Vec<String> = fields
                        .iter()
                        .filter(|f| f.is_input())
                        .map(|f| match &f.kind {
                            FieldKind::Bool => f.flag.to_string(),
                            FieldKind::Choice(opts) => {
                                opts.get(f.choice).cloned().unwrap_or_default()
                            }
                            _ => f.value(),
                        })
                        .collect();
                    let action = action.clone();
                    if let Some(err) = validate(&action, &values) {
                        *error = Some(err);
                        return;
                    }
                    self.modal = None;
                    self.run_action(action, values);
                }
                _ => {
                    let field = &mut fields[*focus];
                    match (&field.kind, key.code) {
                        (FieldKind::Bool, KeyCode::Char(' ') | KeyCode::Left | KeyCode::Right) => {
                            field.flag = !field.flag
                        }
                        (FieldKind::Choice(opts), KeyCode::Right | KeyCode::Char(' ')) => {
                            field.choice = (field.choice + 1) % opts.len()
                        }
                        (FieldKind::Choice(opts), KeyCode::Left) => {
                            field.choice = (field.choice + opts.len() - 1) % opts.len()
                        }
                        (FieldKind::Choice(_) | FieldKind::Bool | FieldKind::Section, _) => {}
                        _ => {
                            field.input.handle(key);
                            *error = None;
                        }
                    }
                }
            },
            Modal::Info(state) => {
                // While the filter box has focus every printable key edits it.
                if let Some(buf) = &mut state.filter {
                    match key.code {
                        KeyCode::Esc => {
                            state.filter = None;
                            state.query.clear();
                            state.scroll = 0;
                        }
                        KeyCode::Enter => state.filter = None,
                        _ => {
                            buf.handle(key);
                            state.query = buf.value();
                            state.scroll = 0;
                        }
                    }
                    return;
                }
                let last = state.rows().len().saturating_sub(1) as u16;
                match key.code {
                    KeyCode::Esc | KeyCode::Char('q') => {
                        if state.query.is_empty() {
                            self.modal = None;
                        } else {
                            state.query.clear();
                            state.scroll = 0;
                        }
                    }
                    KeyCode::Char('/') => state.filter = Some(InputBuf::new(&state.query)),
                    KeyCode::Tab | KeyCode::Right | KeyCode::Char('l') => {
                        state.tab = (state.tab + 1) % INFO_TABS.len();
                        state.scroll = 0;
                    }
                    KeyCode::BackTab | KeyCode::Left | KeyCode::Char('h') => {
                        state.tab = (state.tab + INFO_TABS.len() - 1) % INFO_TABS.len();
                        state.scroll = 0;
                    }
                    KeyCode::Char(c @ '1'..='5') => {
                        state.tab = c as usize - '1' as usize;
                        state.scroll = 0;
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        state.scroll = (state.scroll + 1).min(last)
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        state.scroll = state.scroll.saturating_sub(1)
                    }
                    KeyCode::PageDown => state.scroll = (state.scroll + 10).min(last),
                    KeyCode::PageUp => state.scroll = state.scroll.saturating_sub(10),
                    KeyCode::Char('g') | KeyCode::Home => state.scroll = 0,
                    KeyCode::Char('G') | KeyCode::End => state.scroll = last,
                    KeyCode::Char('y') => {
                        let tab = INFO_TABS[state.tab];
                        crate::osc52::copy(&state.text());
                        self.status = format!("Copied the {tab} tab to the clipboard");
                    }
                    KeyCode::Char('r') => self.load_info(),
                    _ => {}
                }
            }
            Modal::Console(state) => match key.code {
                KeyCode::Esc => self.modal = None,
                KeyCode::Up if !state.history.is_empty() => {
                    let idx = match state.hist_idx {
                        Some(0) | None if state.hist_idx.is_none() => state.history.len() - 1,
                        Some(i) => i.saturating_sub(1),
                        None => state.history.len() - 1,
                    };
                    state.hist_idx = Some(idx);
                    state.input.set(&state.history[idx]);
                }
                KeyCode::Down if !state.history.is_empty() => match state.hist_idx {
                    Some(i) if i + 1 < state.history.len() => {
                        state.hist_idx = Some(i + 1);
                        state.input.set(&state.history[i + 1]);
                    }
                    _ => {
                        state.hist_idx = None;
                        state.input.clear();
                    }
                },
                KeyCode::Enter => {
                    let line = state.input.value().trim().to_string();
                    if line.is_empty() {
                        return;
                    }
                    state.input.clear();
                    state.history.push(line.clone());
                    state.hist_idx = None;
                    state.log.push(format!("> {line}"));
                    if is_destructive(&line) {
                        state.log.push(
                            "  ^ destructive — press y to confirm, any other key to abort".into(),
                        );
                        let action = Action::RunCommand(line);
                        self.modal = Some(Modal::Confirm {
                            message: "Run this destructive command?".into(),
                            action,
                        });
                    } else {
                        self.run_console(line);
                    }
                }
                _ => {
                    state.input.handle(key);
                }
            },
            Modal::ThemePicker { .. } => unreachable!("handled above"),
        }
    }

    fn run_console(&mut self, line: String) {
        let Some(client) = self.client.clone() else {
            return;
        };
        // `SELECT` through the console would desync our idea of the current db.
        let select_target = parse_select(&line);
        self.spawn(async move {
            match client.execute_raw(&line).await {
                Ok(out) => Msg::Console(if out.is_empty() { "OK".into() } else { out }),
                Err(e) => Msg::Console(format!("(error) {e}")),
            }
        });
        if let Some(db) = select_target {
            self.switch_db(db);
        }
    }

    fn switch_db(&mut self, db: i64) {
        let Some(client) = self.client.clone() else {
            return;
        };
        let mut conn = client.conn.clone();
        conn.db = db;
        self.connecting = true;
        self.status = format!("Switching to db{db} ...");
        self.spawn(async move {
            Msg::Connected(Box::new(
                Client::connect(conn).await.map_err(|e| e.to_string()),
            ))
        });
    }

    fn run_action(&mut self, action: Action, values: Vec<String>) {
        let v = |i: usize| values.get(i).cloned().unwrap_or_default();
        match action {
            Action::SaveConnection { replacing } => {
                let previous = replacing
                    .as_deref()
                    .and_then(|n| self.store.connections.iter().find(|c| c.name == n))
                    .cloned();
                let typed_password = v(5);
                let use_keychain = v(6) == "true";
                let conn = Connection {
                    name: v(0).trim().to_string(),
                    host: {
                        let h = v(1).trim().to_string();
                        if h.is_empty() {
                            "127.0.0.1".into()
                        } else {
                            h
                        }
                    },
                    port: v(2).trim().parse().unwrap_or(6379),
                    db: v(3).trim().parse().unwrap_or(0),
                    username: v(4).trim().to_string(),
                    password: if use_keychain {
                        String::new()
                    } else {
                        typed_password.clone()
                    },
                    use_keychain,
                    tls: v(7) == "true",
                    tls_ca_file: v(8).trim().to_string(),
                    tls_cert_file: v(9).trim().to_string(),
                    tls_key_file: v(10).trim().to_string(),
                    tls_insecure: v(11) == "true",
                };
                let new_name = conn.name.clone();
                self.store.upsert(conn, replacing.as_deref());
                if let Err(e) = self.store.save() {
                    self.status = format!("Could not save connections: {e}");
                } else {
                    self.status = "Connection saved".into();
                }
                self.sync_keychain(previous, new_name.clone(), typed_password, use_keychain);
                self.focus_connection(&new_name);
            }
            Action::DeleteConnection(name) => {
                let had_keychain = self
                    .store
                    .connections
                    .iter()
                    .any(|c| c.name == name && c.use_keychain);
                self.store.remove(&name);
                let _ = self.store.save();
                if had_keychain {
                    self.spawn(async move {
                        match tokio::task::spawn_blocking(move || crate::secrets::delete(&name))
                            .await
                        {
                            Ok(Err(e)) => Msg::Error(e.to_string()),
                            _ => Msg::Noop,
                        }
                    });
                }
                let len = self.visible_connections().len();
                self.conn_state.select(if len == 0 {
                    None
                } else {
                    Some(self.conn_state.selected().unwrap_or(0).min(len - 1))
                });
            }
            Action::NewKey => {
                let name = v(0).trim().to_string();
                let kind = NEW_KEY_TYPES
                    .iter()
                    .copied()
                    .find(|t| t.name() == v(1))
                    .unwrap_or(KeyType::String);
                self.mutate("Key created", move |c| async move {
                    c.create_key(&name, kind).await
                });
            }
            Action::DeleteKey(name) => {
                self.current = None;
                self.value = None;
                self.mutate(
                    "Key deleted",
                    move |c| async move { c.delete_key(&name).await },
                );
            }
            Action::RenameKey(old) => {
                let new = v(0).trim().to_string();
                self.current = None;
                self.value = None;
                self.mutate("Key renamed", move |c| async move {
                    c.rename_key(&old, &new).await
                });
            }
            Action::SetTtl(name) => {
                let raw = v(0).trim().to_string();
                let seconds = if raw.is_empty() {
                    None
                } else {
                    raw.parse::<i64>().ok()
                };
                self.mutate("TTL updated", move |c| async move {
                    c.set_ttl(&name, seconds).await
                });
            }
            Action::EditString(name) => {
                let value = v(0);
                self.mutate("Value saved", move |c| async move {
                    c.set_string(&name, &value).await
                });
            }
            Action::HashSet { key, field } => {
                let (f, val) = match field {
                    Some(f) => (f, v(0)),
                    None => (v(0), v(1)),
                };
                self.mutate("Field saved", move |c| async move {
                    c.hash_set(&key, &f, &val).await
                });
            }
            Action::HashDel { key, field } => self.mutate("Field deleted", move |c| async move {
                c.hash_del(&key, &field).await
            }),
            Action::ListAdd(key) => {
                let val = v(0);
                self.mutate("Item appended", move |c| async move {
                    c.list_push(&key, &val).await
                });
            }
            Action::ListSet { key, index } => {
                let val = v(0);
                self.mutate("Item saved", move |c| async move {
                    c.list_set(&key, index, &val).await
                });
            }
            Action::ListDel { key, index } => self.mutate("Item deleted", move |c| async move {
                c.list_remove_at(&key, index).await
            }),
            Action::SetAdd(key) => {
                let m = v(0);
                self.mutate(
                    "Member added",
                    move |c| async move { c.set_add(&key, &m).await },
                );
            }
            Action::SetReplace { key, old } => {
                let new = v(0);
                self.mutate("Member replaced", move |c| async move {
                    if new != old {
                        c.set_remove(&key, &old).await?;
                    }
                    c.set_add(&key, &new).await
                });
            }
            Action::SetDel { key, member } => self.mutate("Member removed", move |c| async move {
                c.set_remove(&key, &member).await
            }),
            Action::ZsetSet { key, old } => {
                let member = v(0);
                let score: f64 = v(1).trim().parse().unwrap_or(0.0);
                self.mutate("Member saved", move |c| async move {
                    if let Some(old) = old {
                        if old != member {
                            c.zset_remove(&key, &old).await?;
                        }
                    }
                    c.zset_add(&key, &member, score).await
                });
            }
            Action::ZsetDel { key, member } => self.mutate("Member removed", move |c| async move {
                c.zset_remove(&key, &member).await
            }),
            Action::StreamAdd(key) => {
                let (f, val) = (v(0), v(1));
                self.mutate("Entry added", move |c| async move {
                    c.stream_add(&key, &f, &val).await
                });
            }
            Action::StreamDel { key, id } => self.mutate("Entry deleted", move |c| async move {
                c.stream_delete(&key, &id).await
            }),
            Action::SelectDb => {
                if let Ok(db) = v(0).trim().parse::<i64>() {
                    self.switch_db(db);
                } else {
                    self.status = "DB index must be a number".into();
                }
            }
            Action::RunCommand(line) => {
                self.open_console();
                if let Some(Modal::Console(c)) = &mut self.modal {
                    c.log.push(format!("> {line}"));
                }
                self.run_console(line);
            }
        }
    }
}

/// Field-level validation that must happen before the modal closes.
fn validate(action: &Action, values: &[String]) -> Option<String> {
    let get = |i: usize| values.get(i).map(|s| s.trim()).unwrap_or("");
    match action {
        Action::SaveConnection { .. } => {
            if get(0).is_empty() {
                return Some("Name is required".into());
            }
            if get(2).parse::<u16>().is_err() {
                return Some("Port must be a number between 0 and 65535".into());
            }
            if get(3).parse::<i64>().is_err() {
                return Some("Database must be a number".into());
            }
            // Opting into the keychain on a machine without one would silently
            // lose the password.
            if get(6) == "true" {
                if let Some(reason) = crate::secrets::unavailable_reason() {
                    return Some(format!("No OS keychain available here: {reason}"));
                }
            }
            if get(7) != "true" && [8, 9, 10].iter().any(|i| !get(*i).is_empty()) {
                return Some("Certificate files need TLS switched on".into());
            }
            if get(9).is_empty() != get(10).is_empty() {
                return Some("Mutual TLS needs both a client certificate and a key".into());
            }
            None
        }
        Action::NewKey | Action::RenameKey(_) => {
            (get(0).is_empty()).then(|| "Key name is required".into())
        }
        Action::SetTtl(_) => {
            let raw = get(0);
            (!raw.is_empty() && raw.parse::<i64>().is_err())
                .then(|| "TTL must be a whole number of seconds".into())
        }
        Action::SelectDb => {
            (get(0).parse::<i64>().is_err()).then(|| "DB index must be a number".into())
        }
        Action::ZsetSet { .. } => {
            if get(0).is_empty() {
                return Some("Member is required".into());
            }
            (get(1).parse::<f64>().is_err()).then(|| "Score must be a number".into())
        }
        Action::HashSet { field: None, .. } | Action::StreamAdd(_) => {
            (get(0).is_empty()).then(|| "Field is required".into())
        }
        Action::SetAdd(_) | Action::SetReplace { .. } => {
            (get(0).is_empty()).then(|| "Member cannot be empty".into())
        }
        _ => None,
    }
}

/// A bare word with no glob characters is treated as a substring search.
pub fn normalize_pattern(raw: &str) -> String {
    if raw.is_empty() {
        return "*".into();
    }
    if raw.contains(['*', '?', '[']) {
        raw.to_string()
    } else {
        format!("*{raw}*")
    }
}

fn parse_select(line: &str) -> Option<i64> {
    let mut parts = line.split_whitespace();
    let head = parts.next()?;
    if !head.eq_ignore_ascii_case("select") {
        return None;
    }
    parts.next()?.parse().ok()
}

trait Selectable {
    fn get(&self) -> Option<usize>;
    fn set(&mut self, i: Option<usize>);
}
impl Selectable for ListState {
    fn get(&self) -> Option<usize> {
        self.selected()
    }
    fn set(&mut self, i: Option<usize>) {
        self.select(i)
    }
}
impl Selectable for TableState {
    fn get(&self) -> Option<usize> {
        self.selected()
    }
    fn set(&mut self, i: Option<usize>) {
        self.select(i)
    }
}

/// Run a blocking keychain operation off the runtime and turn it into a message.
async fn keychain_task<F>(job: F) -> Msg
where
    F: FnOnce() -> anyhow::Result<Option<String>> + Send + 'static,
{
    match tokio::task::spawn_blocking(job).await {
        Ok(Ok(Some(text))) => Msg::Status(text),
        Ok(Ok(None)) => Msg::Noop,
        Ok(Err(e)) => Msg::Error(e.to_string()),
        Err(e) => Msg::Error(e.to_string()),
    }
}

/// Step focus to the next real input, wrapping around and skipping headings.
fn next_focus(fields: &[Field], from: usize, dir: isize) -> usize {
    let n = fields.len();
    if n == 0 {
        return 0;
    }
    let mut i = from;
    for _ in 0..n {
        i = ((i as isize + dir).rem_euclid(n as isize)) as usize;
        if fields[i].is_input() {
            return i;
        }
    }
    from
}

fn move_sel<S: Selectable>(state: &mut S, len: usize, delta: isize) {
    if len == 0 {
        state.set(None);
        return;
    }
    let cur = state.get().unwrap_or(0) as isize;
    let next = (cur + delta).clamp(0, len as isize - 1);
    state.set(Some(next as usize));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_search_terms_become_substring_globs() {
        assert_eq!(normalize_pattern("session"), "*session*");
        assert_eq!(normalize_pattern("user:*"), "user:*");
        assert_eq!(normalize_pattern(""), "*");
    }

    #[test]
    fn detects_select_in_console_input() {
        assert_eq!(parse_select("SELECT 4"), Some(4));
        assert_eq!(parse_select("select  0"), Some(0));
        assert_eq!(parse_select("GET select"), None);
    }

    #[test]
    fn connection_form_rejects_bad_port() {
        let action = Action::SaveConnection { replacing: None };
        let vals = ["srv".into(), "h".into(), "nope".into(), "0".into()];
        assert!(validate(&action, &vals).unwrap().contains("Port"));
        let vals = ["".into(), "h".into(), "6379".into(), "0".into()];
        assert!(validate(&action, &vals).unwrap().contains("Name"));
    }

    #[test]
    fn key_statistics_tab_summarises_the_keyspace() {
        let info = crate::redis_client::ServerInfo::parse(
            "# Memory\nused_memory_human:1.20M\n\n# Stats\nkeyspace_hits:75\nkeyspace_misses:25\nexpired_keys:4\n\n# Keyspace\ndb0:keys=10,expires=2,avg_ttl=0\ndb1:keys=5,expires=1,avg_ttl=0\n",
        );
        let mut state = InfoState::new(info);
        state.tab = INFO_TABS
            .iter()
            .position(|t| *t == "Key Statistics")
            .unwrap();
        let text = state.text();
        assert!(text.contains("db0: 10 keys · 2 with a TTL"), "{text}");
        assert!(text.contains("# Limits"), "{text}");
        assert!(text.contains("total: 15 keys · 3 with a TTL"), "{text}");
        assert!(text.contains("hit rate: 75.0%"), "{text}");
        assert!(text.contains("expired_keys: 4"), "{text}");
    }

    #[test]
    fn server_tab_leads_with_an_overview_and_missing_sections_say_so() {
        let mut state = InfoState::new(crate::redis_client::ServerInfo::parse(
            "# Server\nredis_version:7.2.4\nredis_mode:standalone\nuptime_in_seconds:90061\nos:Linux\n",
        ));
        let text = state.text();
        assert!(text.contains("version: redis 7.2.4 · standalone"), "{text}");
        assert!(text.contains("uptime: 1d1h"), "{text}");
        assert!(text.contains("keys: 0"), "{text}");
        state.tab = 1;
        assert!(state.text().contains("no Memory section"));
    }

    #[test]
    fn memory_tab_gauges_usage_against_maxmemory() {
        let mut state = InfoState::new(crate::redis_client::ServerInfo::parse(
            "# Memory\nused_memory:750\nused_memory_human:750B\nmaxmemory:1000\nmaxmemory_human:1000B\nmaxmemory_policy:allkeys-lru\n",
        ));
        state.tab = 1;
        let gauge = state
            .rows()
            .into_iter()
            .find(|r| matches!(r, InfoRow::Gauge { .. }))
            .expect("a usage gauge");
        let InfoRow::Gauge {
            ratio,
            text,
            alarm_high,
            ..
        } = gauge
        else {
            unreachable!()
        };
        assert!((ratio - 0.75).abs() < f64::EPSILON);
        assert_eq!(text, "750B of 1000B");
        assert!(alarm_high);
    }

    #[test]
    fn the_filter_keeps_matching_fields_and_their_headings() {
        let mut state = InfoState::new(crate::redis_client::ServerInfo::parse(
            "# Server\nredis_version:7.2.4\n\n# Memory\nused_memory_human:1.20M\nmem_allocator:libc\n",
        ));
        state.tab = INFO_TABS.iter().position(|t| *t == "All").unwrap();
        state.query = "mem_alloc".into();
        assert_eq!(
            state.rows(),
            vec![
                InfoRow::Head("Memory".into()),
                InfoRow::Field("mem_allocator".into(), "libc".into()),
            ],
            "the empty Server heading is dropped"
        );
        state.query = "nothing matches".into();
        assert!(state.rows().is_empty());
    }

    fn tick_app(keys: Vec<KeyInfo>) -> App {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
        let mut app = App::new(crate::config::Store::default(), tx);
        app.screen = Screen::Browser;
        app.on_msg(Msg::Keys {
            dbsize: keys.len() as u64,
            keys,
            truncated: false,
            pattern: "*".into(),
        });
        app
    }

    fn info(name: &str, ttl: i64) -> KeyInfo {
        KeyInfo {
            name: name.into(),
            kind: KeyType::String,
            ttl,
        }
    }

    #[test]
    fn expired_keys_leave_the_tree_and_persistent_ones_stay() {
        let mut app = tick_app(vec![info("gone", 3), info("stays", -1), info("later", 90)]);
        app.age_ttls(2);
        assert_eq!(app.keys.len(), 3, "nothing has expired yet");
        assert_eq!(app.keys[0].ttl, 1);

        app.age_ttls(1);
        assert_eq!(
            app.keys.iter().map(|k| k.name.as_str()).collect::<Vec<_>>(),
            ["stays", "later"]
        );
        assert_eq!(app.key_count, 2);
        assert_eq!(app.dbsize, 2);
        assert_eq!(app.rows.len(), 2);
        assert_eq!(app.status, "'gone' expired");
    }

    #[test]
    fn the_open_key_expiring_clears_the_value_pane() {
        let mut app = tick_app(vec![info("session:1", 5), info("other", -1)]);
        app.current = Some(info("session:1", 5));
        app.value = Some(KeyValue::Str("hello".into()));
        app.focus = Focus::Value;

        app.age_ttls(2);
        assert_eq!(
            app.current.as_ref().unwrap().ttl,
            3,
            "the header counts down"
        );

        app.age_ttls(3);
        assert!(app.current.is_none());
        assert!(app.value.is_none());
        assert!(matches!(app.focus, Focus::Tree));
        assert_eq!(app.status, "'session:1' expired");
    }

    #[test]
    fn the_ttl_clock_only_runs_in_the_browser() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
        let mut app = App::new(crate::config::Store::default(), tx);
        app.keys = vec![info("k", 1)];
        app.age_ttls(5);
        assert_eq!(app.keys.len(), 1, "the server list has no keyspace to age");
    }

    #[test]
    fn editing_json_opens_indented_and_saves_in_the_stored_shape() {
        let mut app = tick_app(vec![info("doc", -1)]);
        app.current = Some(info("doc", -1));
        app.value = Some(KeyValue::Str(r#"{"b":2,"a":[1,2]}"#.into()));
        app.on_key(KeyEvent::from(KeyCode::Char('e')));

        let Some(Modal::Editor {
            title,
            textarea,
            json: mode,
            ..
        }) = &app.modal
        else {
            panic!("expected the editor")
        };
        assert_eq!(title, "Edit JSON 'doc'");
        assert_eq!(*mode, JsonMode::Compact);
        assert!(textarea.lines().len() > 1, "opened pretty-printed");
    }

    #[test]
    fn a_broken_json_edit_is_refused_and_keeps_the_editor_open() {
        let mut app = tick_app(vec![info("doc", -1)]);
        app.current = Some(info("doc", -1));
        app.value = Some(KeyValue::Str(r#"{"a":1}"#.into()));
        app.on_key(KeyEvent::from(KeyCode::Char('e')));
        // Break the buffer, then try to save.
        app.on_key(KeyEvent::from(KeyCode::Char('{')));
        app.on_key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL));

        let Some(Modal::Editor { error, .. }) = &app.modal else {
            panic!("the editor should still be open")
        };
        assert!(
            error.as_deref().unwrap_or_default().starts_with("line 1"),
            "{error:?}"
        );
    }

    #[test]
    fn a_plain_string_edit_is_never_json_checked() {
        let mut app = tick_app(vec![info("greeting", -1)]);
        app.current = Some(info("greeting", -1));
        app.value = Some(KeyValue::Str("hello".into()));
        app.on_key(KeyEvent::from(KeyCode::Char('e')));

        let Some(Modal::Editor { title, json, .. }) = &app.modal else {
            panic!("expected the editor")
        };
        assert_eq!(title, "Edit string 'greeting'");
        assert_eq!(*json, JsonMode::None);
        app.on_key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL));
        assert!(app.modal.is_none(), "a plain string saves straight away");
    }
}
