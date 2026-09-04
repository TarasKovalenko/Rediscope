//! Application state and the update half of the loop. Rendering lives in `ui`.
//!
//! Every Redis call runs on a spawned task and reports back over an mpsc
//! channel, so the UI thread never awaits the network.

use std::collections::HashSet;

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::widgets::{ListState, TableState};
use ratatui_textarea::TextArea;
use tokio::sync::mpsc::UnboundedSender;

use crate::config::{Connection, Store};
use crate::history::History;
use crate::input::{Completion, InputBuf, ReverseSearch, complete};
use crate::json::{self, JsonMode};
use crate::memory::{PrefixRow, Rollup};
use crate::redis_client::{
    Client, CommandTable, Diagnostics, ExportEntry, KEY_LIMIT, KeyInfo, KeyType, KeyValue,
    ServerInfo, StreamGroup, StreamGroupDetail, is_destructive,
};
use crate::theme::Theme;
use crate::tree::{Tree, VisibleRow};

/// The "copy to" target that means the connection already open.
pub const THIS_CONNECTION: &str = "(this connection)";

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
    /// The server's command names, for console completion, and which of them
    /// write — a read-only profile refuses those.
    Commands(Box<CommandTable>),
    /// A value search finished: the keys whose contents matched.
    Found {
        keys: Vec<KeyInfo>,
        truncated: bool,
        needle: String,
    },
    /// One message from the pub/sub feed.
    PubSub {
        channel: String,
        payload: String,
    },
    /// Consumer groups of the open stream.
    Groups(Box<Result<Vec<StreamGroup>, String>>),
    /// Consumers and pending entries of the selected group.
    GroupDetail(Box<Result<StreamGroupDetail, String>>),
    /// A Lua script finished; the text is its reply.
    Script(Result<String, String>),
    /// The RediSearch indexes this server has.
    Indexes(Vec<String>),
    /// A batch of the namespace memory scan; `done` on the last one.
    Memory {
        rollup: Box<Rollup>,
        done: bool,
    },
    /// A connection test finished: profile name, then the result.
    Probe(String, Box<Result<crate::redis_client::Probe, String>>),
    /// An `INFO` read finished, for the server-info modal, together with the
    /// diagnostics its other tabs show.
    Info(Box<Result<(ServerInfo, Diagnostics), String>>),
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
    SaveConnection {
        replacing: Option<String>,
    },
    DeleteConnection(String),
    NewKey,
    DeleteKey(String),
    RenameKey(String),
    SetTtl(String),
    EditString(String),
    EditJson(String),
    TsAdd(String),
    TsDel {
        key: String,
        timestamp: String,
    },
    HashSet {
        key: String,
        field: Option<String>,
    },
    HashDel {
        key: String,
        field: String,
    },
    ListAdd(String),
    ListSet {
        key: String,
        index: isize,
    },
    ListDel {
        key: String,
        index: isize,
    },
    SetAdd(String),
    SetReplace {
        key: String,
        old: String,
    },
    SetDel {
        key: String,
        member: String,
    },
    ZsetSet {
        key: String,
        old: Option<String>,
    },
    ZsetDel {
        key: String,
        member: String,
    },
    StreamAdd(String),
    StreamDel {
        key: String,
        id: String,
    },
    SelectDb,
    RunCommand(String),
    /// Subscribe the pub/sub feed to the typed patterns.
    Subscribe,
    /// Publish a message from the pub/sub feed.
    Publish,
    /// Delete every marked key.
    DeleteMarked(Vec<String>),
    /// Set or clear the TTL on every marked key.
    TtlMarked(Vec<String>),
    /// Copy one key elsewhere: another name, database or server.
    CopyKey(String),
    /// Search key values for a substring.
    GrepValues,
    /// Write keys to a file, and read them back.
    Export(Vec<String>),
    Import,
    /// Run the Lua script in the editor.
    RunLua,
    /// Run a RediSearch query.
    Search,
    /// `CONFIG SET` the parameter the info modal has selected.
    SetConfig(String),
    /// Disconnect a client by id.
    KillClient(String),
    ResetSlowlog,
    CreateGroup(String),
    DestroyGroup {
        key: String,
        group: String,
    },
    AckPending {
        key: String,
        group: String,
        id: String,
    },
    ClaimPending {
        key: String,
        group: String,
        id: String,
    },
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
        if self.is_input() { 3 } else { 2 }
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

/// Tabs of the server-info modal, in display order. The first four read
/// `INFO`; the rest come from the diagnostics fetched alongside it.
pub const INFO_TABS: [&str; 10] = [
    "Server",
    "Memory",
    "Stats",
    "Key Statistics",
    "Slowlog",
    "Clients",
    "Config",
    "Latency",
    "Cluster",
    "All",
];

/// Tabs whose rows can be acted on, so the modal knows when to show a cursor.
pub fn tab_is_actionable(tab: usize) -> bool {
    matches!(INFO_TABS.get(tab), Some(&"Config") | Some(&"Clients"))
}

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
    /// Slow log, clients, config, latency and cluster state.
    pub diag: Diagnostics,
    pub tab: usize,
    pub scroll: u16,
    /// Selected row on the tabs that can be acted on (Config, Clients).
    pub cursor: usize,
    /// Live text of the field filter, while it has focus.
    pub filter: Option<InputBuf>,
    /// The applied filter. Empty means "show everything".
    pub query: String,
}

impl InfoState {
    pub fn new(info: ServerInfo, diag: Diagnostics) -> Self {
        Self {
            info,
            diag,
            tab: 0,
            scroll: 0,
            cursor: 0,
            filter: None,
            query: String::new(),
        }
    }

    /// The `param = value` pair under the cursor on the Config tab.
    pub fn selected_config(&self) -> Option<(String, String)> {
        if INFO_TABS.get(self.tab) != Some(&"Config") {
            return None;
        }
        match self.rows().get(self.cursor) {
            Some(InfoRow::Field(k, v)) => Some((k.clone(), v.clone())),
            _ => None,
        }
    }

    /// The client id under the cursor on the Clients tab.
    pub fn selected_client(&self) -> Option<(String, String)> {
        if INFO_TABS.get(self.tab) != Some(&"Clients") {
            return None;
        }
        match self.rows().get(self.cursor) {
            Some(InfoRow::Field(id, rest)) => Some((id.clone(), rest.clone())),
            _ => None,
        }
    }

    /// First row to draw, given how many fit. Keeps the cursor on screen on
    /// the tabs that have one without the update half knowing the height.
    pub fn view_start(&self, height: usize) -> usize {
        let scroll = self.scroll as usize;
        if !tab_is_actionable(self.tab) || height == 0 {
            return scroll;
        }
        if self.cursor < scroll {
            self.cursor
        } else if self.cursor >= scroll + height {
            self.cursor + 1 - height
        } else {
            scroll
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
            "Slowlog" => self.slowlog(),
            "Clients" => self.clients(),
            "Config" => self.config(),
            "Latency" => self.latency(),
            "Cluster" => self.cluster(),
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

    /// Slowest commands first: the log is what explains a latency spike.
    fn slowlog(&self) -> Vec<InfoRow> {
        if self.diag.slowlog.is_empty() {
            return vec![
                InfoRow::Head("Slow log".into()),
                InfoRow::Field(
                    "empty".into(),
                    "nothing has crossed slowlog-log-slower-than".into(),
                ),
            ];
        }
        let mut entries = self.diag.slowlog.clone();
        entries.sort_by_key(|e| std::cmp::Reverse(e.micros));
        let mut rows = vec![InfoRow::Head(format!(
            "Slow log — {} entries, slowest first",
            entries.len()
        ))];
        rows.extend(entries.iter().map(|e| {
            InfoRow::Field(
                format!("{:.2} ms", e.micros as f64 / 1000.0),
                format!("#{} · {} · {}", e.id, e.command, e.client),
            )
        }));
        rows
    }

    /// Connected clients, longest idle first — an idle client holding a
    /// connection is the one worth seeing.
    fn clients(&self) -> Vec<InfoRow> {
        if self.diag.clients.is_empty() {
            return vec![
                InfoRow::Head("Clients".into()),
                InfoRow::Field("unavailable".into(), "CLIENT LIST was refused".into()),
            ];
        }
        let mut clients = self.diag.clients.clone();
        clients.sort_by_key(|c| std::cmp::Reverse(c.idle_secs));
        let mut rows = vec![InfoRow::Head(format!(
            "{} connected client(s) — x disconnects the selected one",
            clients.len()
        ))];
        rows.extend(clients.iter().map(|c| {
            let name = if c.name.is_empty() {
                String::new()
            } else {
                format!(" · {}", c.name)
            };
            InfoRow::Field(
                c.id.clone(),
                format!(
                    "{} · db{} · idle {}s · age {}s · last {}{name}",
                    c.addr, c.db, c.idle_secs, c.age_secs, c.command
                ),
            )
        }));
        rows
    }

    fn config(&self) -> Vec<InfoRow> {
        if self.diag.config.is_empty() {
            return vec![
                InfoRow::Head("Config".into()),
                InfoRow::Field("unavailable".into(), "CONFIG GET was refused".into()),
            ];
        }
        let mut rows = vec![InfoRow::Head(format!(
            "{} running parameters — e edits the selected one",
            self.diag.config.len()
        ))];
        rows.extend(
            self.diag
                .config
                .iter()
                .map(|(k, v)| InfoRow::Field(k.clone(), v.clone())),
        );
        rows
    }

    fn latency(&self) -> Vec<InfoRow> {
        let mut rows = vec![InfoRow::Head("Latency".into())];
        rows.extend(
            self.diag
                .latency
                .iter()
                .map(|(k, v)| InfoRow::Field(k.clone(), v.clone())),
        );
        rows
    }

    fn cluster(&self) -> Vec<InfoRow> {
        let mut rows = Vec::new();
        if self.diag.cluster.is_empty() {
            rows.push(InfoRow::Head("Cluster".into()));
            rows.push(InfoRow::Field(
                "cluster_enabled".into(),
                "0 — this server is not in cluster mode".into(),
            ));
        } else {
            rows.push(InfoRow::Head("Cluster".into()));
            rows.extend(
                self.diag
                    .cluster
                    .iter()
                    .map(|(k, v)| InfoRow::Field(k.clone(), v.clone())),
            );
        }
        if !self.diag.modules.is_empty() {
            rows.push(InfoRow::Head("Modules".into()));
            rows.extend(
                self.diag
                    .modules
                    .iter()
                    .enumerate()
                    .map(|(i, m)| InfoRow::Field(format!("module {}", i + 1), m.clone())),
            );
        }
        rows
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

/// How many keys the report is willing to measure. Beyond this the scan takes
/// every nth key instead, which is what keeps it usable on a big server.
const MEMORY_SAMPLE_LIMIT: u64 = 20_000;

/// The namespace memory report and the scan filling it in.
pub struct MemoryState {
    pub rollup: Rollup,
    /// How many segments of the key name each row groups by.
    pub depth: usize,
    /// `DBSIZE` when the scan started, for the progress bar and the stride.
    pub dbsize: u64,
    pub running: bool,
    /// False shows prefixes, true shows the biggest individual keys.
    pub show_keys: bool,
    /// Set when the report closes, so the scan task stops at the next batch.
    pub cancel: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub scroll: usize,
}

impl MemoryState {
    pub fn new(dbsize: u64) -> Self {
        Self {
            rollup: Rollup::default(),
            depth: 1,
            dbsize,
            running: true,
            show_keys: false,
            cancel: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            scroll: 0,
        }
    }

    /// Measure one key in every `stride`, so a big keyspace still finishes.
    pub fn stride(&self) -> u64 {
        (self.dbsize / MEMORY_SAMPLE_LIMIT).max(1)
    }

    pub fn rows(&self) -> Vec<PrefixRow> {
        self.rollup.rows(self.depth)
    }
}

/// The live pub/sub feed: what it is subscribed to, and what has arrived.
pub struct PubSubState {
    /// The patterns passed to `PSUBSCRIBE`, as typed.
    pub patterns: Vec<String>,
    /// True when the feed is following keyspace notifications rather than
    /// application channels, which only changes how it is labelled.
    pub keyspace: bool,
    /// Newest last. Capped, so a busy channel cannot grow without bound.
    pub messages: Vec<(String, String)>,
    /// Stopped when the modal closes, so no task outlives the view.
    pub task: Option<tokio::task::JoinHandle<()>>,
    pub scroll: usize,
    /// Set by `f`: keep the view pinned to the newest message.
    pub follow: bool,
}

/// How many messages the feed keeps.
pub const PUBSUB_LIMIT: usize = 2_000;

impl PubSubState {
    pub fn new(patterns: Vec<String>, keyspace: bool) -> Self {
        Self {
            patterns,
            keyspace,
            messages: Vec::new(),
            task: None,
            scroll: 0,
            follow: true,
        }
    }

    pub fn push(&mut self, channel: String, payload: String) {
        self.messages.push((channel, payload));
        if self.messages.len() > PUBSUB_LIMIT {
            let overflow = self.messages.len() - PUBSUB_LIMIT;
            self.messages.drain(..overflow);
            self.scroll = self.scroll.saturating_sub(overflow);
        }
        if self.follow {
            self.scroll = self.messages.len().saturating_sub(1);
        }
    }

    pub fn title(&self) -> String {
        let what = if self.keyspace {
            "Keyspace events"
        } else {
            "Pub/Sub"
        };
        format!("{what} — {}", self.patterns.join(" "))
    }

    /// Stop the subscription. Called when the modal closes and before it is
    /// re-subscribed, so a feed never keeps running behind the browser.
    pub fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Which half of the consumer-group view has the cursor.
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum GroupPane {
    Groups,
    Pending,
}

/// Consumer groups of one stream, and the entries they have not acked.
pub struct GroupsState {
    pub key: String,
    pub groups: Vec<StreamGroup>,
    pub selected: usize,
    pub detail: StreamGroupDetail,
    pub pane: GroupPane,
    pub pending_sel: usize,
}

impl GroupsState {
    pub fn new(key: String) -> Self {
        Self {
            key,
            groups: Vec::new(),
            selected: 0,
            detail: StreamGroupDetail::default(),
            pane: GroupPane::Groups,
            pending_sel: 0,
        }
    }

    /// Replace the group list, keeping the cursor on the same group where it
    /// still exists so a refresh does not move the selection.
    pub fn set_groups(&mut self, groups: Vec<StreamGroup>) {
        let previous = self.selected_group();
        self.groups = groups;
        self.selected = previous
            .and_then(|name| self.groups.iter().position(|g| g.name == name))
            .unwrap_or(0)
            .min(self.groups.len().saturating_sub(1));
    }

    pub fn selected_group(&self) -> Option<String> {
        self.groups.get(self.selected).map(|g| g.name.clone())
    }

    pub fn selected_pending(&self) -> Option<crate::redis_client::PendingEntry> {
        self.detail.pending.get(self.pending_sel).cloned()
    }
}

pub struct ConsoleState {
    pub input: InputBuf,
    pub log: Vec<String>,
    pub history: History,
    pub hist_idx: Option<usize>,
    /// Live `ctrl+r` search, and the line it interrupted.
    pub search: Option<ReverseSearch>,
    pub interrupted: String,
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
        /// First line shown, so a long reply can be read past the box.
        scroll: u16,
    },
    Console(ConsoleState),
    Memory(MemoryState),
    PubSub(PubSubState),
    Groups(GroupsState),
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
    /// The server's command table, read once per connection: console
    /// completion, and which commands a read-only profile has to refuse.
    pub commands: CommandTable,
    /// Keys marked with `m`, for the bulk operations.
    pub marked: HashSet<String>,
    /// The pub/sub feed while a dialog is on top of it, so publishing does not
    /// throw away the subscription and everything it has collected.
    pub held_feed: Option<PubSubState>,
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

    /// A key named by a restored session, selected once the tree is built.
    pending_selection: Option<String>,

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
            commands: CommandTable::default(),
            marked: HashSet::new(),
            held_feed: None,
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
            pending_selection: None,
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
        if client.read_only() {
            self.status = "This connection is read-only — no writes are sent".into();
            return;
        }
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
        if let Some(cur) = &mut self.current
            && cur.ttl > 0
        {
            cur.ttl = (cur.ttl - secs).max(0);
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
        if let Some(name) = self.current.as_ref().map(|c| c.name.clone())
            && expired.contains(&name)
        {
            self.current = None;
            self.value = None;
            self.focus = Focus::Tree;
            self.status = format!("'{name}' expired");
            return;
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
                        // Console completion wants the command list; a server
                        // that will not give it just means no completion.
                        let names = client.clone();
                        self.spawn(async move {
                            Msg::Commands(Box::new(names.command_names().await.unwrap_or_default()))
                        });
                        let session = self.store.sessions.get(&client.conn.name).cloned();
                        self.client = Some(client);
                        self.screen = Screen::Browser;
                        self.status.clear();
                        self.current = None;
                        self.value = None;
                        self.marked.clear();
                        self.expanded.clear();
                        // Reopen where this profile was left: same filter, same
                        // folders, same key.
                        self.pattern = "*".into();
                        self.pending_selection = None;
                        if let Some(session) = session {
                            if !session.pattern.is_empty() {
                                self.pattern = session.pattern;
                            }
                            self.expanded.extend(session.expanded);
                            if !session.selected_key.is_empty() {
                                self.pending_selection = Some(session.selected_key);
                            }
                        }
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
                // A restored session names the key it was on; find it now that
                // the tree exists.
                if let Some(wanted) = self.pending_selection.take()
                    && let Some(index) = self
                        .rows
                        .iter()
                        .position(|r| r.key.as_ref().is_some_and(|k| k.name == wanted))
                {
                    self.tree_state.select(Some(index));
                    self.on_tree_move();
                }
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
            Msg::Commands(table) => self.commands = *table,
            Msg::Found {
                keys,
                truncated,
                needle,
            } => {
                self.loading = false;
                self.status = if keys.is_empty() {
                    format!("No values contain '{needle}'")
                } else {
                    format!(
                        "{} key(s) contain '{needle}'{}",
                        keys.len(),
                        if truncated { " (search truncated)" } else { "" }
                    )
                };
                let pattern = self.pattern.clone();
                self.on_msg(Msg::Keys {
                    keys,
                    truncated,
                    dbsize: self.dbsize,
                    pattern,
                });
            }
            Msg::PubSub { channel, payload } => match &mut self.modal {
                Some(Modal::PubSub(state)) => state.push(channel, payload),
                // The feed is behind a dialog: keep collecting for it.
                _ => match &mut self.held_feed {
                    Some(feed) => feed.push(channel, payload),
                    // Nothing is listening any more, so neither is the task.
                    None => self.stop_feeds(),
                },
            },
            Msg::Groups(result) => match *result {
                Ok(groups) => {
                    if let Some(Modal::Groups(state)) = &mut self.modal {
                        state.set_groups(groups);
                        let want = state.selected_group();
                        if let Some(group) = want {
                            self.load_group_detail(&group);
                        }
                    }
                }
                Err(e) => self.status = format!("Error: {e}"),
            },
            Msg::GroupDetail(result) => match *result {
                Ok(detail) => {
                    if let Some(Modal::Groups(state)) = &mut self.modal {
                        state.detail = detail;
                        state.pending_sel = 0;
                    }
                }
                Err(e) => self.status = format!("Error: {e}"),
            },
            Msg::Indexes(indexes) => {
                self.status.clear();
                let options: Vec<&str> = indexes.iter().map(String::as_str).collect();
                self.modal = Some(Modal::Form {
                    title: "Search".into(),
                    hint: "←→ picks the index · Enter runs FT.SEARCH".into(),
                    fields: vec![
                        Field::choice("Index", &options, 0),
                        Field::text("Query", "*"),
                    ],
                    focus: 1,
                    error: None,
                    action: Action::Search,
                });
            }
            Msg::Script(Ok(out)) => {
                self.modal = Some(Modal::Message {
                    title: "Result".into(),
                    body: if out.trim().is_empty() {
                        "(no reply)".into()
                    } else {
                        out
                    },
                    scroll: 0,
                });
            }
            Msg::Script(Err(e)) => {
                self.modal = Some(Modal::Message {
                    title: "Error".into(),
                    body: e,
                    scroll: 0,
                });
            }
            Msg::Memory { rollup, done } => {
                if let Some(Modal::Memory(state)) = &mut self.modal {
                    state.rollup = *rollup;
                    state.running = !done;
                }
            }
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
                Ok((info, diag)) => {
                    let (tab, scroll, cursor) = match &self.modal {
                        Some(Modal::Info(state)) => (state.tab, state.scroll, state.cursor),
                        _ => (0, 0, 0),
                    };
                    self.status.clear();
                    self.modal = Some(Modal::Info(Box::new(InfoState {
                        tab,
                        scroll,
                        cursor,
                        ..InfoState::new(info, diag)
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

    fn selected_value_is_structured(&self) -> bool {
        self.selected_value_row().is_some_and(|row| {
            row.cells
                .iter()
                .any(|cell| json::mode(cell).is_json() || crate::xml::is_xml(cell))
        })
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
                if let Some(mut c) = self.selected_connection() {
                    // Reconnecting from the list returns to the database this
                    // profile was last browsing.
                    if let Some(session) = self.store.sessions.get(&c.name) {
                        c.db = session.db;
                    }
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
            if was_keychain && let Some(old) = old_name {
                self.spawn(async move {
                    keychain_task(move || {
                        crate::secrets::delete(&old)?;
                        Ok(None)
                    })
                    .await
                });
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
                if was_keychain && let Some(old) = renamed {
                    crate::secrets::delete(&old)?;
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
                Field::boolean(
                    "Read-only (refuse every write from this profile)",
                    c.read_only,
                ),
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
                Field::section("SSH tunnel"),
                Field::text("SSH host (blank connects directly)", &c.ssh_host),
                Field::text("SSH user (optional)", &c.ssh_user),
                Field::text("SSH port", &c.ssh_port.to_string()),
                Field::text("SSH private key file (optional)", &c.ssh_key_file),
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
            KeyCode::Char('q') => {
                self.save_session();
                self.stop_feeds();
                self.should_quit = true;
            }
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
            KeyCode::Char('M') => self.start_memory_report(),
            KeyCode::Char('m') => self.toggle_mark(),
            KeyCode::Char('u') => {
                let count = self.marked.len();
                self.marked.clear();
                self.status = format!("Cleared {count} mark(s)");
            }
            KeyCode::Char('C') => self.prompt_copy_key(),
            KeyCode::Char('F') => self.prompt_grep(),
            KeyCode::Char('w') => self.prompt_export(),
            KeyCode::Char('I') => self.prompt_import(),
            KeyCode::Char('L') => self.prompt_lua(),
            KeyCode::Char('P') => self.prompt_pubsub(),
            KeyCode::Char('N') => self.watch_keyspace(),
            KeyCode::Char('S') => self.open_stream_groups(),
            KeyCode::Char('Q') => self.prompt_search(),
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
                    move_sel(&mut self.value_state, len, 1);
                    self.value_scroll = 0;
                } else {
                    self.value_scroll = self.value_scroll.saturating_add(1)
                }
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if len > 0 {
                    move_sel(&mut self.value_state, len, -1);
                    self.value_scroll = 0;
                } else {
                    self.value_scroll = self.value_scroll.saturating_sub(1)
                }
            }
            KeyCode::PageDown => {
                if self.selected_value_is_structured() {
                    self.value_scroll = self.value_scroll.saturating_add(10)
                } else if len > 0 {
                    move_sel(&mut self.value_state, len, 10)
                } else {
                    self.value_scroll = self.value_scroll.saturating_add(10)
                }
            }
            KeyCode::PageUp => {
                if self.selected_value_is_structured() {
                    self.value_scroll = self.value_scroll.saturating_sub(10)
                } else if len > 0 {
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
                    self.value_state.select(Some(len - 1));
                    self.value_scroll = 0;
                }
            }
            KeyCode::Left | KeyCode::Char('h') => self.focus = Focus::Tree,
            _ => {}
        }
    }

    // ---- browser commands --------------------------------------------------

    /// Remember where this profile was left, so reconnecting lands here again.
    /// Best effort: a session we cannot write is not worth a message.
    fn save_session(&mut self) {
        let Some(client) = &self.client else {
            return;
        };
        let session = crate::config::Session {
            db: client.conn.db,
            pattern: self.pattern.clone(),
            expanded: {
                let mut folders: Vec<String> = self.expanded.iter().cloned().collect();
                folders.sort();
                folders
            },
            selected_key: self
                .current
                .as_ref()
                .map(|k| k.name.clone())
                .unwrap_or_default(),
        };
        self.store
            .sessions
            .insert(client.conn.name.clone(), session);
        let _ = self.store.save();
    }

    fn back_to_connections(&mut self) {
        self.save_session();
        self.stop_feeds();
        self.client = None;
        self.screen = Screen::Connections;
        self.current = None;
        self.value = None;
        self.rows.clear();
        self.status.clear();
    }

    /// Mark or unmark the selected key, or every key under the selected
    /// folder — marking a whole namespace one key at a time is no use.
    fn toggle_mark(&mut self) {
        let Some(row) = self.selected_row().cloned() else {
            return;
        };
        match (row.key, row.folder_path) {
            (Some(k), _) => {
                if !self.marked.remove(&k.name) {
                    self.marked.insert(k.name);
                }
            }
            (None, Some(path)) => {
                let prefix = format!("{path}:");
                let under: Vec<String> = self
                    .keys
                    .iter()
                    .filter(|k| k.name == path || k.name.starts_with(&prefix))
                    .map(|k| k.name.clone())
                    .collect();
                let all_marked = under.iter().all(|n| self.marked.contains(n));
                for name in under {
                    if all_marked {
                        self.marked.remove(&name);
                    } else {
                        self.marked.insert(name);
                    }
                }
            }
            _ => {}
        }
        self.status = format!("{} key(s) marked", self.marked.len());
    }

    /// The keys a bulk action applies to: the marked set, or the selected key
    /// when nothing is marked.
    fn target_keys(&self) -> Vec<String> {
        if !self.marked.is_empty() {
            let mut names: Vec<String> = self.marked.iter().cloned().collect();
            names.sort();
            return names;
        }
        self.current.iter().map(|k| k.name.clone()).collect()
    }

    fn prompt_copy_key(&mut self) {
        let Some(k) = self.current.clone() else {
            self.status = "Select a key first".into();
            return;
        };
        if self.refuse_write() {
            return;
        }
        let db = self.client.as_ref().map_or(0, |c| c.conn.db);
        let mut targets = vec![THIS_CONNECTION.to_string()];
        targets.extend(self.store.connections.iter().map(|c| c.name.clone()));
        let options: Vec<&str> = targets.iter().map(String::as_str).collect();
        self.modal = Some(Modal::Form {
            title: format!("Copy '{}'", k.name),
            hint: "←→ picks the target server · Enter copies".into(),
            fields: vec![
                Field::text("New key name", &k.name),
                Field::choice("Target server", &options, 0),
                Field::text("Target database", &db.to_string()),
                Field::boolean("Overwrite the target if it exists", false),
            ],
            focus: 0,
            error: None,
            action: Action::CopyKey(k.name),
        });
    }

    fn prompt_grep(&mut self) {
        self.modal = Some(Modal::Form {
            title: "Find in values".into(),
            hint: format!(
                "Searches the values of keys matching '{}' · case-insensitive",
                self.pattern
            ),
            fields: vec![Field::text("Text to find", "")],
            focus: 0,
            error: None,
            action: Action::GrepValues,
        });
    }

    fn prompt_export(&mut self) {
        let names = if self.marked.is_empty() {
            self.keys.iter().map(|k| k.name.clone()).collect()
        } else {
            self.target_keys()
        };
        if names.is_empty() {
            self.status = "Nothing to export".into();
            return;
        }
        self.modal = Some(Modal::Form {
            title: format!("Export {} key(s)", names.len()),
            hint: "DUMP payloads and TTLs, as JSON · Enter writes the file".into(),
            fields: vec![Field::text("File", "rediscope-export.json")],
            focus: 0,
            error: None,
            action: Action::Export(names),
        });
    }

    fn prompt_import(&mut self) {
        if self.refuse_write() {
            return;
        }
        self.modal = Some(Modal::Form {
            title: "Import keys".into(),
            hint: "Reads a file written by the export above".into(),
            fields: vec![
                Field::text("File", "rediscope-export.json"),
                Field::boolean("Overwrite keys that already exist", false),
            ],
            focus: 0,
            error: None,
            action: Action::Import,
        });
    }

    /// Query a RediSearch index. The index list comes from the server, so a
    /// server without the module says so rather than offering an empty form.
    fn prompt_search(&mut self) {
        let Some(client) = self.client.clone() else {
            return;
        };
        self.status = "Reading the search indexes ...".into();
        self.spawn(async move {
            match client.search_indexes().await {
                Ok(indexes) if indexes.is_empty() => {
                    Msg::Status("No search indexes on this server".into())
                }
                Ok(indexes) => Msg::Indexes(indexes),
                Err(e) => Msg::Error(format!(
                    "FT._LIST failed: {e} — is the search module loaded?"
                )),
            }
        });
    }

    fn prompt_lua(&mut self) {
        let keys = self.target_keys();
        let mut ta = TextArea::from(vec!["return redis.call('PING')"]);
        ta.set_cursor_line_style(ratatui::style::Style::default());
        let title = if keys.is_empty() {
            "Lua script — no KEYS".to_string()
        } else {
            format!("Lua script — KEYS: {}", keys.join(", "))
        };
        self.modal = Some(Modal::Editor {
            title,
            textarea: Box::new(ta),
            action: Action::RunLua,
            json: JsonMode::None,
            error: None,
        });
    }

    fn yank_key_name(&mut self) {
        let Some(name) = self.current.as_ref().map(|k| k.name.clone()) else {
            return;
        };
        crate::osc52::copy(&name);
        self.status = format!("Copied '{name}' to clipboard");
    }

    fn prompt_new_key(&mut self) {
        if self.refuse_write() {
            return;
        }
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
        if self.refuse_write() {
            return;
        }
        // Marks win over the cursor: having marked a set, `D` is about that set.
        if !self.marked.is_empty() {
            let names = self.target_keys();
            self.modal = Some(Modal::Confirm {
                message: format!(
                    "Delete {} marked key(s)? This cannot be undone.",
                    names.len()
                ),
                action: Action::DeleteMarked(names),
            });
            return;
        }
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
        if self.refuse_write() {
            return;
        }
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
        if self.refuse_write() {
            return;
        }
        if !self.marked.is_empty() {
            let names = self.target_keys();
            self.modal = Some(Modal::Form {
                title: format!("TTL for {} marked key(s)", names.len()),
                hint: "Seconds, or blank to clear the expiry".into(),
                fields: vec![Field::text("Seconds", "")],
                focus: 0,
                error: None,
                action: Action::TtlMarked(names),
            });
            return;
        }
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
        if self.refuse_write() {
            return;
        }
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
            KeyType::TimeSeries => Modal::Form {
                title: format!("Add a sample to '{name}'"),
                hint: "Timestamp * means now · Enter saves · Esc cancels".into(),
                fields: vec![Field::text("Timestamp", "*"), Field::text("Value", "0")],
                focus: 0,
                error: None,
                action: Action::TsAdd(name),
            },
            KeyType::String | KeyType::Json | KeyType::Other => Modal::Message {
                title: "Nothing to add".into(),
                body: "This key holds a single document — press 'e' to edit it.".into(),
                scroll: 0,
            },
        });
    }

    fn prompt_edit(&mut self) {
        if self.refuse_write() {
            return;
        }
        let Some(k) = self.current.clone() else {
            return;
        };
        let name = k.name.clone();
        if matches!(k.kind, KeyType::String | KeyType::Json) {
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
            let title = if k.kind == KeyType::Json {
                format!("Edit document '{name}'")
            } else if mode.is_json() {
                format!("Edit JSON '{name}'")
            } else {
                format!("Edit string '{name}'")
            };
            // A RedisJSON document is always JSON, whatever the text looks like
            // right now, so it is checked before it is written back.
            let mode = if k.kind == KeyType::Json {
                JsonMode::Pretty
            } else {
                mode
            };
            let action = if k.kind == KeyType::Json {
                Action::EditJson(name)
            } else {
                Action::EditString(name)
            };
            self.modal = Some(Modal::Editor {
                title,
                textarea: Box::new(ta),
                action,
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
            KeyType::TimeSeries => Modal::Message {
                title: "Not editable".into(),
                body: "A time-series sample is written by timestamp. Add one with 'a'.".into(),
                scroll: 0,
            },
            KeyType::Stream | KeyType::String | KeyType::Json | KeyType::Other => Modal::Message {
                title: "Not editable".into(),
                body: "Stream entries are immutable. Add a new entry with 'a', or delete this one with 'x'.".into(),
                scroll: 0,
            },
        });
    }

    fn confirm_delete_row(&mut self) {
        if self.refuse_write() {
            return;
        }
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
            KeyType::TimeSeries => (
                format!("Delete the sample at {} from '{name}'?", row.id),
                Action::TsDel {
                    key: name,
                    timestamp: row.id,
                },
            ),
            KeyType::String | KeyType::Json | KeyType::Other => {
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
        self.spawn(async move {
            let result = async {
                let info = client.info().await.map_err(|e| e.to_string())?;
                // Diagnostics are best effort: a provider that blocks CONFIG
                // or CLIENT LIST should still get the INFO tabs.
                let diag = client.diagnostics().await.unwrap_or_default();
                Ok((info, diag))
            }
            .await;
            Msg::Info(Box::new(result))
        });
    }

    // ---- pub/sub -----------------------------------------------------------

    /// Subscribe to `patterns` and stream what arrives into the feed modal.
    /// Pub/sub needs a connection of its own, so this opens one and drops it
    /// when the modal closes.
    fn start_pubsub(&mut self, patterns: Vec<String>, keyspace: bool) {
        let Some(client) = self.client.clone() else {
            return;
        };
        if let Some(Modal::PubSub(old)) = &mut self.modal {
            old.stop();
        }
        let mut state = PubSubState::new(patterns.clone(), keyspace);
        let tx = self.tx.clone();
        state.task = Some(tokio::spawn(async move {
            let mut pubsub = match client.pubsub().await {
                Ok(p) => p,
                Err(e) => {
                    let _ = tx.send(Msg::Error(format!("cannot subscribe: {e}")));
                    return;
                }
            };
            for pattern in &patterns {
                if let Err(e) = pubsub.psubscribe(pattern).await {
                    let _ = tx.send(Msg::Error(format!("cannot subscribe to {pattern}: {e}")));
                    return;
                }
            }
            let mut stream = pubsub.on_message();
            while let Some(msg) = futures_util::StreamExt::next(&mut stream).await {
                let channel = msg.get_channel_name().to_string();
                let payload: String = msg.get_payload().unwrap_or_default();
                if tx.send(Msg::PubSub { channel, payload }).is_err() {
                    return;
                }
            }
        }));
        self.modal = Some(Modal::PubSub(state));
    }

    /// Abort any subscription still running with nothing to show it in.
    fn stop_feeds(&mut self) {
        if let Some(Modal::PubSub(state)) = &mut self.modal {
            state.stop();
        }
        if let Some(feed) = &mut self.held_feed {
            feed.stop();
        }
        self.held_feed = None;
    }

    fn prompt_pubsub(&mut self) {
        self.modal = Some(Modal::Form {
            title: "Subscribe".into(),
            hint: "Space-separated patterns, e.g. news.* jobs · Enter subscribes".into(),
            fields: vec![Field::text("Channel patterns", "*")],
            focus: 0,
            error: None,
            action: Action::Subscribe,
        });
    }

    /// Follow keyspace notifications for the current database. Needs
    /// `notify-keyspace-events` to be configured on the server; the feed says
    /// so when nothing arrives.
    fn watch_keyspace(&mut self) {
        let db = self.client.as_ref().map_or(0, |c| c.conn.db);
        self.start_pubsub(vec![format!("__keyevent@{db}__:*")], true);
    }

    // ---- consumer groups ---------------------------------------------------

    fn open_stream_groups(&mut self) {
        let Some(key) = self.current.clone() else {
            self.status = "Select a stream first".into();
            return;
        };
        if key.kind != KeyType::Stream {
            self.status = "Consumer groups belong to streams".into();
            return;
        }
        self.modal = Some(Modal::Groups(GroupsState::new(key.name.clone())));
        self.load_groups();
    }

    fn load_groups(&mut self) {
        let (Some(client), Some(Modal::Groups(state))) = (self.client.clone(), &self.modal) else {
            return;
        };
        let key = state.key.clone();
        self.spawn(async move {
            Msg::Groups(Box::new(
                client.stream_groups(&key).await.map_err(|e| e.to_string()),
            ))
        });
    }

    fn load_group_detail(&mut self, group: &str) {
        let (Some(client), Some(Modal::Groups(state))) = (self.client.clone(), &self.modal) else {
            return;
        };
        let (key, group) = (state.key.clone(), group.to_string());
        self.spawn(async move {
            Msg::GroupDetail(Box::new(
                client
                    .stream_group_detail(&key, &group)
                    .await
                    .map_err(|e| e.to_string()),
            ))
        });
    }

    // ---- read-only guard ---------------------------------------------------

    /// True when this profile refuses writes. Every path that changes data
    /// asks first, so the guard cannot be walked around through a form, the
    /// console, or a bulk action.
    pub fn read_only(&self) -> bool {
        self.client.as_ref().is_some_and(Client::read_only)
    }

    fn refuse_write(&mut self) -> bool {
        if self.read_only() {
            self.status = "This connection is read-only — no writes are sent".into();
            return true;
        }
        false
    }

    /// Open the namespace memory report and start the scan behind it.
    fn start_memory_report(&mut self) {
        let Some(client) = self.client.clone() else {
            return;
        };
        let state = MemoryState::new(self.dbsize);
        let stride = state.stride();
        let cancel = state.cancel.clone();
        self.modal = Some(Modal::Memory(state));

        // The scan reports as it goes, so the table fills in rather than the
        // report sitting empty until the last batch lands.
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let mut scan = crate::redis_client::MemoryScan::default();
            let mut rollup = Rollup::default();
            let mut last = std::time::Instant::now();
            loop {
                if cancel.load(std::sync::atomic::Ordering::Relaxed) {
                    return;
                }
                let done = match client.memory_batch(&mut scan, stride, &mut rollup).await {
                    Ok(done) => done,
                    Err(e) => {
                        let _ = tx.send(Msg::Error(format!("memory scan failed: {e}")));
                        return;
                    }
                };
                // Cloning the rollup is only worth it a few times a second.
                if done || last.elapsed() >= std::time::Duration::from_millis(200) {
                    last = std::time::Instant::now();
                    let _ = tx.send(Msg::Memory {
                        rollup: Box::new(rollup.clone()),
                        done,
                    });
                }
                if done {
                    return;
                }
            }
        });
    }

    fn open_console(&mut self) {
        self.modal = Some(Modal::Console(ConsoleState {
            input: InputBuf::new(""),
            log: vec![
                "Type a Redis command and press Enter. Esc closes.".into(),
                "Destructive commands (FLUSHALL, FLUSHDB, ...) ask first.".into(),
                String::new(),
            ],
            history: History::load(),
            hist_idx: None,
            search: None,
            interrupted: String::new(),
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
        // Completion needs the key list and the command list alongside the
        // console, which the borrow below rules out. Handle it up here.
        if key.code == KeyCode::Tab
            && matches!(self.modal, Some(Modal::Console(ref c)) if c.search.is_none())
        {
            let commands = self.commands.names.clone();
            let keys: Vec<String> = self.keys.iter().map(|k| k.name.clone()).collect();
            if let Some(Modal::Console(state)) = &mut self.modal {
                complete_console(state, &commands, &keys);
            }
            return;
        }
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
        // The consumer-group view issues follow-up requests as it is driven,
        // which the borrow of `self.modal` below would rule out.
        if matches!(self.modal, Some(Modal::Groups(_))) {
            self.groups_key(key);
            return;
        }
        match self.modal.as_mut().expect("modal_key with no modal") {
            Modal::Help => {
                if matches!(key.code, KeyCode::Esc | KeyCode::Enter | KeyCode::Char('q')) {
                    self.modal = None;
                }
            }
            Modal::Message { body, scroll, .. } => {
                let last = body.lines().count().saturating_sub(1) as u16;
                match key.code {
                    KeyCode::Esc | KeyCode::Enter | KeyCode::Char('q') => self.modal = None,
                    KeyCode::Down | KeyCode::Char('j') => *scroll = (*scroll + 1).min(last),
                    KeyCode::Up | KeyCode::Char('k') => *scroll = scroll.saturating_sub(1),
                    KeyCode::PageDown => *scroll = (*scroll + 10).min(last),
                    KeyCode::PageUp => *scroll = scroll.saturating_sub(10),
                    KeyCode::Home | KeyCode::Char('g') => *scroll = 0,
                    KeyCode::End | KeyCode::Char('G') => *scroll = last,
                    KeyCode::Char('y') => {
                        crate::osc52::copy(body);
                        self.status = "Copied to the clipboard".into();
                    }
                    _ => {}
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
                    if mode.is_json()
                        && let Err(e) = json::check(&text)
                    {
                        *error = Some(e);
                        return;
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
                KeyCode::Esc => {
                    self.modal = self.held_feed.take().map(Modal::PubSub);
                }
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
                        state.cursor = 0;
                    }
                    KeyCode::BackTab | KeyCode::Left | KeyCode::Char('h') => {
                        state.tab = (state.tab + INFO_TABS.len() - 1) % INFO_TABS.len();
                        state.scroll = 0;
                        state.cursor = 0;
                    }
                    // 1-9 pick a tab, 0 is the tenth ("All").
                    KeyCode::Char(c @ '0'..='9') => {
                        let index = (c as usize + 9 - '0' as usize) % 10;
                        state.tab = index.min(INFO_TABS.len() - 1);
                        state.scroll = 0;
                        state.cursor = 0;
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        state.scroll = (state.scroll + 1).min(last);
                        state.cursor = (state.cursor + 1).min(last as usize);
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        state.scroll = state.scroll.saturating_sub(1);
                        state.cursor = state.cursor.saturating_sub(1);
                    }
                    KeyCode::PageDown => {
                        state.scroll = (state.scroll + 10).min(last);
                        state.cursor = (state.cursor + 10).min(last as usize);
                    }
                    KeyCode::PageUp => {
                        state.scroll = state.scroll.saturating_sub(10);
                        state.cursor = state.cursor.saturating_sub(10);
                    }
                    KeyCode::Char('g') | KeyCode::Home => {
                        state.scroll = 0;
                        state.cursor = 0;
                    }
                    KeyCode::Char('G') | KeyCode::End => {
                        state.scroll = last;
                        state.cursor = last as usize;
                    }
                    KeyCode::Char('y') => {
                        let tab = INFO_TABS[state.tab];
                        crate::osc52::copy(&state.text());
                        self.status = format!("Copied the {tab} tab to the clipboard");
                    }
                    KeyCode::Char('r') => self.load_info(),
                    // `e` on the Config tab edits the selected parameter.
                    KeyCode::Char('e') => {
                        if let Some((param, value)) = state.selected_config() {
                            self.modal = Some(Modal::Form {
                                title: format!("CONFIG SET {param}"),
                                hint: "Applies to the running server, not its config file".into(),
                                fields: vec![Field::text(&param, &value)],
                                focus: 0,
                                error: None,
                                action: Action::SetConfig(param),
                            });
                        }
                    }
                    // `x` disconnects a client, or clears the slow log.
                    KeyCode::Char('x') => match INFO_TABS.get(state.tab) {
                        Some(&"Clients") => {
                            if let Some((id, addr)) = state.selected_client() {
                                self.modal = Some(Modal::Confirm {
                                    message: format!("Disconnect client {id} ({addr})?"),
                                    action: Action::KillClient(id),
                                });
                            }
                        }
                        Some(&"Slowlog") => {
                            self.modal = Some(Modal::Confirm {
                                message: "Reset the slow log?".into(),
                                action: Action::ResetSlowlog,
                            });
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
            Modal::Console(state) if state.search.is_some() => {
                let entries = state.history.entries().to_vec();
                let search = state.search.as_mut().expect("guarded above");
                match key.code {
                    // A second ctrl+r steps further back through the matches.
                    KeyCode::Char('r') if ctrl => search.older(&entries),
                    KeyCode::Char(c) if !ctrl => search.push_char(c, &entries),
                    KeyCode::Backspace => search.pop_char(&entries),
                    KeyCode::Enter => {
                        let hit = search.hit(&entries).map(str::to_string);
                        state.search = None;
                        if let Some(line) = hit {
                            state.input.set(&line);
                        }
                    }
                    // Esc abandons the search and restores the interrupted line.
                    KeyCode::Esc => {
                        state.search = None;
                        let restored = state.interrupted.clone();
                        state.input.set(&restored);
                    }
                    _ => {}
                }
            }
            Modal::Console(state) => match key.code {
                KeyCode::Esc => self.modal = None,
                KeyCode::Char('r') if ctrl => {
                    state.interrupted = state.input.value();
                    state.search = Some(ReverseSearch::default());
                }
                KeyCode::Up if !state.history.entries().is_empty() => {
                    let entries = state.history.entries();
                    let idx = match state.hist_idx {
                        Some(i) => i.saturating_sub(1),
                        None => entries.len() - 1,
                    };
                    state.hist_idx = Some(idx);
                    let line = entries[idx].clone();
                    state.input.set(&line);
                }
                KeyCode::Down if !state.history.entries().is_empty() => {
                    let entries = state.history.entries();
                    match state.hist_idx {
                        Some(i) if i + 1 < entries.len() => {
                            state.hist_idx = Some(i + 1);
                            let line = entries[i + 1].clone();
                            state.input.set(&line);
                        }
                        _ => {
                            state.hist_idx = None;
                            state.input.clear();
                        }
                    }
                }
                KeyCode::Enter => {
                    let line = state.input.value().trim().to_string();
                    if line.is_empty() {
                        return;
                    }
                    state.input.clear();
                    state.history.push(&line);
                    // Best effort: a history we cannot write is not worth
                    // interrupting the session over.
                    let _ = state.history.save();
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
            Modal::Memory(state) => match key.code {
                KeyCode::Esc | KeyCode::Char('q') => {
                    state
                        .cancel
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    self.modal = None;
                }
                KeyCode::Char(c @ '1'..='3') => {
                    state.depth = c.to_digit(10).unwrap_or(1) as usize;
                    state.show_keys = false;
                    state.scroll = 0;
                }
                // The same scan already measured individual keys; `t` shows
                // them rather than their prefixes.
                KeyCode::Char('t') => {
                    state.show_keys = !state.show_keys;
                    state.scroll = 0;
                }
                KeyCode::Down | KeyCode::Char('j') => state.scroll = state.scroll.saturating_add(1),
                KeyCode::Up | KeyCode::Char('k') => state.scroll = state.scroll.saturating_sub(1),
                KeyCode::Home | KeyCode::Char('g') => state.scroll = 0,
                KeyCode::Char('y') => {
                    crate::osc52::copy(&memory_report_text(state));
                    self.status = "Copied the memory report to the clipboard".into();
                }
                KeyCode::Char('r') => {
                    state
                        .cancel
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    self.start_memory_report();
                }
                _ => {}
            },
            Modal::PubSub(state) => match key.code {
                KeyCode::Esc | KeyCode::Char('q') => {
                    state.stop();
                    self.modal = None;
                }
                KeyCode::Char('c') => {
                    state.messages.clear();
                    state.scroll = 0;
                }
                KeyCode::Char('f') => {
                    state.follow = !state.follow;
                    if state.follow {
                        state.scroll = state.messages.len().saturating_sub(1);
                    }
                }
                KeyCode::Char('s') => {
                    state.stop();
                    let current = state.patterns.join(" ");
                    self.modal = Some(Modal::Form {
                        title: "Subscribe".into(),
                        hint: "Space-separated patterns · Enter subscribes".into(),
                        fields: vec![Field::text("Channel patterns", &current)],
                        focus: 0,
                        error: None,
                        action: Action::Subscribe,
                    });
                }
                KeyCode::Char('w') => {
                    let channel = state
                        .messages
                        .last()
                        .map(|(c, _)| c.clone())
                        .unwrap_or_default();
                    // Set the feed aside rather than dropping it: the
                    // subscription and its history come back after publishing.
                    if let Some(Modal::PubSub(feed)) = self.modal.take() {
                        self.held_feed = Some(feed);
                    }
                    self.modal = Some(Modal::Form {
                        title: "Publish".into(),
                        hint: "Enter publishes · Esc cancels".into(),
                        fields: vec![Field::text("Channel", &channel), Field::text("Message", "")],
                        focus: 0,
                        error: None,
                        action: Action::Publish,
                    });
                }
                KeyCode::Char('y') => {
                    let text = state
                        .messages
                        .iter()
                        .map(|(c, p)| format!("{c}  {p}"))
                        .collect::<Vec<_>>()
                        .join("\n");
                    crate::osc52::copy(&text);
                    self.status = "Copied the feed to the clipboard".into();
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    state.follow = false;
                    state.scroll = (state.scroll + 1).min(state.messages.len().saturating_sub(1));
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    state.follow = false;
                    state.scroll = state.scroll.saturating_sub(1);
                }
                KeyCode::PageDown => {
                    state.follow = false;
                    state.scroll = (state.scroll + 10).min(state.messages.len().saturating_sub(1));
                }
                KeyCode::PageUp => {
                    state.follow = false;
                    state.scroll = state.scroll.saturating_sub(10);
                }
                KeyCode::Home | KeyCode::Char('g') => {
                    state.follow = false;
                    state.scroll = 0;
                }
                KeyCode::End | KeyCode::Char('G') => {
                    state.scroll = state.messages.len().saturating_sub(1);
                    state.follow = true;
                }
                _ => {}
            },
            Modal::Groups(_) | Modal::ThemePicker { .. } => unreachable!("handled above"),
        }
    }

    /// Drive the consumer-group view. Split out because most of its keys start
    /// another request, which cannot happen while the modal is borrowed.
    fn groups_key(&mut self, key: KeyEvent) {
        let Some(Modal::Groups(state)) = &mut self.modal else {
            return;
        };
        let stream = state.key.clone();
        let mut follow_up: Option<String> = None;
        let mut refresh = false;
        let mut next_modal: Option<Modal> = None;
        let mut close = false;
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => close = true,
            KeyCode::Tab => {
                state.pane = match state.pane {
                    GroupPane::Groups => GroupPane::Pending,
                    GroupPane::Pending => GroupPane::Groups,
                }
            }
            KeyCode::Down | KeyCode::Char('j') => match state.pane {
                GroupPane::Groups => {
                    if !state.groups.is_empty() {
                        state.selected = (state.selected + 1).min(state.groups.len() - 1);
                        follow_up = state.selected_group();
                    }
                }
                GroupPane::Pending => {
                    state.pending_sel =
                        (state.pending_sel + 1).min(state.detail.pending.len().saturating_sub(1));
                }
            },
            KeyCode::Up | KeyCode::Char('k') => match state.pane {
                GroupPane::Groups => {
                    state.selected = state.selected.saturating_sub(1);
                    follow_up = state.selected_group();
                }
                GroupPane::Pending => state.pending_sel = state.pending_sel.saturating_sub(1),
            },
            KeyCode::Enter => follow_up = state.selected_group(),
            KeyCode::Char('r') => refresh = true,
            KeyCode::Char('n') => {
                next_modal = Some(Modal::Form {
                    title: format!("New consumer group on '{stream}'"),
                    hint: "Start at $ for new entries, 0 for the whole stream".into(),
                    fields: vec![Field::text("Group", ""), Field::text("Start at", "$")],
                    focus: 0,
                    error: None,
                    action: Action::CreateGroup(stream.clone()),
                });
            }
            KeyCode::Char('d') => {
                if let Some(group) = state.selected_group() {
                    next_modal = Some(Modal::Confirm {
                        message: format!("Destroy group '{group}' on '{stream}'?"),
                        action: Action::DestroyGroup {
                            key: stream.clone(),
                            group,
                        },
                    });
                }
            }
            KeyCode::Char('a') => {
                if let (Some(group), Some(entry)) =
                    (state.selected_group(), state.selected_pending())
                {
                    next_modal = Some(Modal::Confirm {
                        message: format!("Ack entry {} for group '{group}'?", entry.id),
                        action: Action::AckPending {
                            key: stream.clone(),
                            group,
                            id: entry.id,
                        },
                    });
                }
            }
            KeyCode::Char('c') => {
                if let (Some(group), Some(entry)) =
                    (state.selected_group(), state.selected_pending())
                {
                    next_modal = Some(Modal::Form {
                        title: format!("Claim {} in '{group}'", entry.id),
                        hint: "The consumer that should own this entry next".into(),
                        fields: vec![Field::text("Consumer", &entry.consumer)],
                        focus: 0,
                        error: None,
                        action: Action::ClaimPending {
                            key: stream.clone(),
                            group,
                            id: entry.id,
                        },
                    });
                }
            }
            _ => {}
        }
        if close {
            self.modal = None;
            return;
        }
        if let Some(modal) = next_modal {
            self.modal = Some(modal);
            return;
        }
        if refresh {
            self.load_groups();
        } else if let Some(group) = follow_up {
            self.load_group_detail(&group);
        }
    }

    fn run_console(&mut self, line: String) {
        let Some(client) = self.client.clone() else {
            return;
        };
        if client.read_only() && self.commands.is_write(&line) {
            let refusal = "(error) this connection is read-only".to_string();
            if let Some(Modal::Console(c)) = &mut self.modal {
                c.log.push(refusal);
            } else {
                self.status = "This connection is read-only — no writes are sent".into();
            }
            return;
        }
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
                let typed_password = v(6);
                let use_keychain = v(7) == "true";
                let conn = Connection {
                    name: v(0).trim().to_string(),
                    host: {
                        let h = v(1).trim().to_string();
                        if h.is_empty() { "127.0.0.1".into() } else { h }
                    },
                    port: v(2).trim().parse().unwrap_or(6379),
                    db: v(3).trim().parse().unwrap_or(0),
                    read_only: v(4) == "true",
                    username: v(5).trim().to_string(),
                    password: if use_keychain {
                        String::new()
                    } else {
                        typed_password.clone()
                    },
                    use_keychain,
                    tls: v(8) == "true",
                    tls_ca_file: v(9).trim().to_string(),
                    tls_cert_file: v(10).trim().to_string(),
                    tls_key_file: v(11).trim().to_string(),
                    tls_insecure: v(12) == "true",
                    ssh_host: v(13).trim().to_string(),
                    ssh_user: v(14).trim().to_string(),
                    ssh_port: v(15).trim().parse().unwrap_or(22),
                    ssh_key_file: v(16).trim().to_string(),
                };
                let new_name = conn.name.clone();
                self.store.upsert(conn, replacing.as_deref());
                match self.store.save() {
                    Err(e) => {
                        self.status = format!("Could not save connections: {e}");
                    }
                    _ => {
                        self.status = "Connection saved".into();
                    }
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
            Action::EditJson(name) => {
                let value = v(0);
                self.mutate("Document saved", move |c| async move {
                    c.json_set(&name, "$", &value).await
                });
            }
            Action::TsAdd(name) => {
                let (ts, value) = (v(0), v(1));
                self.mutate("Sample added", move |c| async move {
                    c.ts_add(&name, &ts, &value).await
                });
            }
            Action::TsDel { key, timestamp } => {
                self.mutate("Sample deleted", move |c| async move {
                    c.ts_del(&key, &timestamp).await
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
                    if let Some(old) = old
                        && old != member
                    {
                        c.zset_remove(&key, &old).await?;
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
            Action::Subscribe => {
                let patterns: Vec<String> = v(0).split_whitespace().map(str::to_string).collect();
                if patterns.is_empty() {
                    self.status = "No pattern given".into();
                    return;
                }
                self.start_pubsub(patterns, false);
            }
            Action::Publish => {
                // Whatever happens, the feed underneath comes back.
                if let Some(feed) = self.held_feed.take() {
                    self.modal = Some(Modal::PubSub(feed));
                }
                if self.refuse_write() {
                    return;
                }
                let (channel, payload) = (v(0), v(1));
                let Some(client) = self.client.clone() else {
                    return;
                };
                self.spawn(async move {
                    match client
                        .execute_raw(&format!("PUBLISH {channel} {payload:?}"))
                        .await
                    {
                        Ok(out) => Msg::Status(format!("Published to {channel} — {}", out.trim())),
                        Err(e) => Msg::Error(format!("publish failed: {e}")),
                    }
                });
            }
            Action::DeleteMarked(names) => {
                if self.refuse_write() {
                    return;
                }
                let count = names.len();
                self.marked.clear();
                self.current = None;
                self.value = None;
                self.mutate(&format!("{count} key(s) deleted"), move |c| async move {
                    c.delete_keys(&names).await.map(|_| ())
                });
            }
            Action::TtlMarked(names) => {
                if self.refuse_write() {
                    return;
                }
                let raw = v(0).trim().to_string();
                let seconds = if raw.is_empty() {
                    None
                } else {
                    raw.parse::<i64>().ok()
                };
                let count = names.len();
                self.mutate(&format!("TTL set on {count} key(s)"), move |c| async move {
                    c.expire_keys(&names, seconds).await.map(|_| ())
                });
            }
            Action::CopyKey(source) => {
                let target_name = v(0).trim().to_string();
                let profile = v(1);
                let db: i64 = v(2).trim().parse().unwrap_or(0);
                let replace = v(3) == "true";
                let Some(client) = self.client.clone() else {
                    return;
                };
                // Copying onto this same connection is refused when it would
                // overwrite the source with itself.
                let same_server = profile == THIS_CONNECTION;
                if same_server && target_name == source && db == client.conn.db {
                    self.status = "Error: the target is the source".into();
                    return;
                }
                let mut target_conn = if same_server {
                    client.conn.clone()
                } else {
                    match self.store.connections.iter().find(|c| c.name == profile) {
                        Some(c) => c.clone(),
                        None => {
                            self.status = format!("Error: no saved profile called '{profile}'");
                            return;
                        }
                    }
                };
                target_conn.db = db;
                if target_conn.read_only {
                    self.status = format!("Error: '{}' is read-only", target_conn.name);
                    return;
                }
                self.status = format!("Copying '{source}' ...");
                self.spawn(async move {
                    // A copy inside the same database needs no second
                    // connection; anything else opens one for the target.
                    let target = if same_server && db == client.conn.db {
                        client.clone()
                    } else {
                        match Client::connect(target_conn).await {
                            Ok(c) => c,
                            Err(e) => return Msg::Error(format!("cannot reach the target: {e}")),
                        }
                    };
                    match client
                        .copy_key(&source, &target_name, &target, replace)
                        .await
                    {
                        Ok(()) => Msg::Mutated(Ok(format!("Copied '{source}' to '{target_name}'"))),
                        Err(e) => Msg::Mutated(Err(e.to_string())),
                    }
                });
            }
            Action::GrepValues => {
                let needle = v(0);
                if needle.is_empty() {
                    return;
                }
                let Some(client) = self.client.clone() else {
                    return;
                };
                let pattern = self.pattern.clone();
                self.loading = true;
                self.status = format!("Searching values for '{needle}' ...");
                self.spawn(async move {
                    match client.grep_values(&pattern, &needle, KEY_LIMIT).await {
                        Ok((keys, truncated)) => Msg::Found {
                            keys,
                            truncated,
                            needle,
                        },
                        Err(e) => Msg::Error(format!("search failed: {e}")),
                    }
                });
            }
            Action::Export(names) => {
                let path = crate::config::expand_home(v(0).trim());
                let Some(client) = self.client.clone() else {
                    return;
                };
                if names.is_empty() {
                    self.status = "Nothing to export".into();
                    return;
                }
                self.status = format!("Exporting {} key(s) ...", names.len());
                self.spawn(async move {
                    let entries = match client.export_keys(&names).await {
                        Ok(entries) => entries,
                        Err(e) => return Msg::Error(format!("export failed: {e}")),
                    };
                    let count = entries.len();
                    let text = match serde_json::to_string_pretty(&entries) {
                        Ok(text) => text,
                        Err(e) => return Msg::Error(format!("export failed: {e}")),
                    };
                    match tokio::task::spawn_blocking(move || std::fs::write(&path, text)).await {
                        Ok(Ok(())) => Msg::Status(format!("Exported {count} key(s)")),
                        Ok(Err(e)) => Msg::Error(format!("cannot write the export: {e}")),
                        Err(e) => Msg::Error(e.to_string()),
                    }
                });
            }
            Action::Import => {
                if self.refuse_write() {
                    return;
                }
                let path = crate::config::expand_home(v(0).trim());
                let replace = v(1) == "true";
                let Some(client) = self.client.clone() else {
                    return;
                };
                self.status = "Importing ...".into();
                self.spawn(async move {
                    let text =
                        match tokio::task::spawn_blocking(move || std::fs::read_to_string(&path))
                            .await
                        {
                            Ok(Ok(text)) => text,
                            Ok(Err(e)) => return Msg::Error(format!("cannot read the file: {e}")),
                            Err(e) => return Msg::Error(e.to_string()),
                        };
                    let entries: Vec<ExportEntry> = match serde_json::from_str(&text) {
                        Ok(entries) => entries,
                        Err(e) => return Msg::Error(format!("not a rediscope export: {e}")),
                    };
                    match client.import_entries(&entries, replace).await {
                        Ok(n) => Msg::Mutated(Ok(format!("Imported {n} key(s)"))),
                        Err(e) => Msg::Mutated(Err(e.to_string())),
                    }
                });
            }
            Action::RunLua => {
                let script = v(0);
                let Some(client) = self.client.clone() else {
                    return;
                };
                // Marked keys become KEYS[1..], which is how a script is meant
                // to name what it touches.
                let mut keys: Vec<String> = self.marked.iter().cloned().collect();
                keys.sort();
                if keys.is_empty()
                    && let Some(current) = &self.current
                {
                    keys.push(current.name.clone());
                }
                if self.read_only() && self.commands.is_write("EVAL") {
                    self.status = "This connection is read-only — no writes are sent".into();
                    return;
                }
                self.status = "Running the script ...".into();
                self.spawn(async move {
                    Msg::Script(
                        client
                            .eval(&script, &keys, &[])
                            .await
                            .map_err(|e| e.to_string()),
                    )
                });
            }
            Action::Search => {
                let (index, query) = (v(0), v(1));
                let Some(client) = self.client.clone() else {
                    return;
                };
                self.status = format!("Searching {index} ...");
                self.spawn(async move {
                    match client
                        .search(&index, &query, crate::redis_client::VALUE_LIMIT)
                        .await
                    {
                        Ok(out) => Msg::Script(Ok(out)),
                        Err(e) => Msg::Script(Err(e.to_string())),
                    }
                });
            }
            Action::SetConfig(param) => {
                if self.refuse_write() {
                    return;
                }
                let value = v(0);
                let Some(client) = self.client.clone() else {
                    return;
                };
                self.spawn(async move {
                    match client.config_set(&param, &value).await {
                        Ok(()) => Msg::Status(format!("{param} set to '{value}'")),
                        Err(e) => Msg::Error(format!("CONFIG SET failed: {e}")),
                    }
                });
                self.load_info();
            }
            Action::KillClient(id) => {
                let Some(client) = self.client.clone() else {
                    return;
                };
                self.spawn(async move {
                    match client.client_kill(&id).await {
                        Ok(()) => Msg::Status(format!("Client {id} disconnected")),
                        Err(e) => Msg::Error(format!("CLIENT KILL failed: {e}")),
                    }
                });
                self.load_info();
            }
            Action::ResetSlowlog => {
                let Some(client) = self.client.clone() else {
                    return;
                };
                self.spawn(async move {
                    match client.slowlog_reset().await {
                        Ok(()) => Msg::Status("Slow log reset".into()),
                        Err(e) => Msg::Error(format!("SLOWLOG RESET failed: {e}")),
                    }
                });
                self.load_info();
            }
            Action::CreateGroup(key) => {
                if self.refuse_write() {
                    return;
                }
                let (group, start) = (v(0).trim().to_string(), v(1).trim().to_string());
                let Some(client) = self.client.clone() else {
                    return;
                };
                self.spawn(async move {
                    match client.stream_group_create(&key, &group, &start).await {
                        Ok(()) => Msg::Status(format!("Group '{group}' created")),
                        Err(e) => Msg::Error(format!("XGROUP CREATE failed: {e}")),
                    }
                });
                self.load_groups();
            }
            Action::DestroyGroup { key, group } => {
                if self.refuse_write() {
                    return;
                }
                let Some(client) = self.client.clone() else {
                    return;
                };
                self.spawn(async move {
                    match client.stream_group_destroy(&key, &group).await {
                        Ok(()) => Msg::Status(format!("Group '{group}' destroyed")),
                        Err(e) => Msg::Error(format!("XGROUP DESTROY failed: {e}")),
                    }
                });
                self.load_groups();
            }
            Action::AckPending { key, group, id } => {
                if self.refuse_write() {
                    return;
                }
                let Some(client) = self.client.clone() else {
                    return;
                };
                self.spawn(async move {
                    match client.stream_ack(&key, &group, &id).await {
                        Ok(()) => Msg::Status(format!("Acked {id}")),
                        Err(e) => Msg::Error(format!("XACK failed: {e}")),
                    }
                });
                self.load_groups();
            }
            Action::ClaimPending { key, group, id } => {
                if self.refuse_write() {
                    return;
                }
                let consumer = v(0).trim().to_string();
                let Some(client) = self.client.clone() else {
                    return;
                };
                self.spawn(async move {
                    match client.stream_claim(&key, &group, &consumer, &id).await {
                        Ok(()) => Msg::Status(format!("{id} claimed by {consumer}")),
                        Err(e) => Msg::Error(format!("XCLAIM failed: {e}")),
                    }
                });
                self.load_groups();
            }
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
            if get(7) == "true"
                && let Some(reason) = crate::secrets::unavailable_reason()
            {
                return Some(format!("No OS keychain available here: {reason}"));
            }
            if get(8) != "true" && [9, 10, 11].iter().any(|i| !get(*i).is_empty()) {
                return Some("Certificate files need TLS switched on".into());
            }
            if get(10).is_empty() != get(11).is_empty() {
                return Some("Mutual TLS needs both a client certificate and a key".into());
            }
            if get(15).parse::<u16>().is_err() {
                return Some("SSH port must be a number between 0 and 65535".into());
            }
            if get(13).is_empty() && [14, 16].iter().any(|i| !get(*i).is_empty()) {
                return Some("SSH settings need an SSH host".into());
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

/// The report as plain text, for `y`.
fn memory_report_text(state: &MemoryState) -> String {
    let mut out = format!(
        "namespace memory - sampled {} of {} keys\n",
        state.rollup.sampled(),
        state.dbsize.max(state.rollup.scanned())
    );
    if state.show_keys {
        for key in state.rollup.top_keys() {
            out.push_str(&format!(
                "{:<52} {:>12}{}\n",
                key.key,
                crate::memory::human_bytes(key.bytes),
                key.freq.map(|f| format!("  freq {f}")).unwrap_or_default()
            ));
        }
        return out;
    }
    for row in state.rows() {
        out.push_str(&format!(
            "{:<40} {:>12} {:>12} {:>6.1}%\n",
            row.prefix,
            row.keys,
            crate::memory::human_bytes(row.est_bytes),
            row.share
        ));
    }
    out
}

/// Complete the word under the console cursor: the first word against Redis
/// command names, anything after it against the keys already on screen.
fn complete_console(state: &mut ConsoleState, commands: &[String], keys: &[String]) {
    let line = state.input.value();
    let Some(word) = line.split_whitespace().next_back() else {
        return;
    };
    // The word is only being completed while the cursor sits at its end.
    if !line.ends_with(word) {
        return;
    }
    let first_word = line.trim_start().len() == word.len();
    let candidates = if first_word { commands } else { keys };
    match complete(word, candidates) {
        Completion::None => {}
        Completion::Extend(full) => {
            let head = &line[..line.len() - word.len()];
            // A completed word is finished, so leave a space to type the next.
            let done = full.chars().count() > word.chars().count()
                || candidates.iter().any(|c| c.eq_ignore_ascii_case(word));
            state
                .input
                .set(&format!("{head}{full}{}", if done { " " } else { "" }));
        }
        Completion::Choices(all) => {
            state.log.push(format!("  {}", all.join("  ")));
        }
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
        let mut state = InfoState::new(info, Diagnostics::default());
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
        let mut state = InfoState::new(
            crate::redis_client::ServerInfo::parse(
                "# Server\nredis_version:7.2.4\nredis_mode:standalone\nuptime_in_seconds:90061\nos:Linux\n",
            ),
            Diagnostics::default(),
        );
        let text = state.text();
        assert!(text.contains("version: redis 7.2.4 · standalone"), "{text}");
        assert!(text.contains("uptime: 1d1h"), "{text}");
        assert!(text.contains("keys: 0"), "{text}");
        state.tab = 1;
        assert!(state.text().contains("no Memory section"));
    }

    #[test]
    fn memory_tab_gauges_usage_against_maxmemory() {
        let mut state = InfoState::new(
            crate::redis_client::ServerInfo::parse(
                "# Memory\nused_memory:750\nused_memory_human:750B\nmaxmemory:1000\nmaxmemory_human:1000B\nmaxmemory_policy:allkeys-lru\n",
            ),
            Diagnostics::default(),
        );
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
        let mut state = InfoState::new(
            crate::redis_client::ServerInfo::parse(
                "# Server\nredis_version:7.2.4\n\n# Memory\nused_memory_human:1.20M\nmem_allocator:libc\n",
            ),
            Diagnostics::default(),
        );
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
    fn console_app() -> App {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
        let mut app = App::new(crate::config::Store::default(), tx);
        app.screen = Screen::Browser;
        app.commands = CommandTable {
            names: ["GET", "GETDEL", "DBSIZE", "SET"]
                .iter()
                .map(|s| (*s).to_string())
                .collect(),
            writes: ["SET", "GETDEL"].iter().map(|s| (*s).to_string()).collect(),
        };
        app.keys = vec![info("user:1", -1), info("user:2", -1)];
        app.open_console();
        app
    }

    fn console(app: &App) -> &ConsoleState {
        match app.modal.as_ref() {
            Some(Modal::Console(c)) => c,
            _ => panic!("the console is not open"),
        }
    }

    fn type_line(app: &mut App, text: &str) {
        for c in text.chars() {
            app.on_key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE));
        }
    }

    #[test]
    fn tab_completes_a_command_name() {
        let mut app = console_app();
        type_line(&mut app, "dbs");
        app.on_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        assert_eq!(console(&app).input.value(), "DBSIZE ");
    }

    #[test]
    fn tab_over_an_ambiguous_command_lists_the_choices() {
        let mut app = console_app();
        type_line(&mut app, "get");
        app.on_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        assert_eq!(console(&app).input.value(), "get", "the line is left alone");
        let log = console(&app).log.join(" ");
        assert!(log.contains("GET") && log.contains("GETDEL"), "{log}");
    }

    #[test]
    fn tab_completes_an_argument_from_the_loaded_keys() {
        let mut app = console_app();
        type_line(&mut app, "GET user:2");
        // Trim it back to an unambiguous prefix of the second key.
        for _ in 0..1 {
            app.on_key(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
        }
        type_line(&mut app, "2");
        app.on_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        assert_eq!(console(&app).input.value(), "GET user:2 ");
    }

    #[test]
    fn ctrl_r_pulls_an_earlier_command_back_into_the_line() {
        let mut app = console_app();
        if let Some(Modal::Console(c)) = app.modal.as_mut() {
            c.history.push("GET user:1");
            c.history.push("DBSIZE");
        }
        app.on_key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::CONTROL));
        type_line(&mut app, "get");
        assert_eq!(
            console(&app)
                .search
                .as_ref()
                .unwrap()
                .hit(console(&app).history.entries()),
            Some("GET user:1")
        );
        app.on_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert_eq!(console(&app).input.value(), "GET user:1");
        assert!(
            console(&app).search.is_none(),
            "the search closes on accept"
        );
    }

    #[test]
    fn escape_leaves_the_search_without_touching_what_was_typed() {
        let mut app = console_app();
        type_line(&mut app, "PIN");
        if let Some(Modal::Console(c)) = app.modal.as_mut() {
            c.history.push("GET user:1");
        }
        app.on_key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::CONTROL));
        type_line(&mut app, "get");
        app.on_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert_eq!(console(&app).input.value(), "PIN");
        assert!(
            app.modal.is_some(),
            "escape closes the search, not the console"
        );
    }
    fn memory_app() -> App {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
        let mut app = App::new(crate::config::Store::default(), tx);
        app.screen = Screen::Browser;
        app.modal = Some(Modal::Memory(MemoryState::new(1_000)));
        app
    }

    fn memory(app: &App) -> &MemoryState {
        match app.modal.as_ref() {
            Some(Modal::Memory(m)) => m,
            _ => panic!("the report is not open"),
        }
    }

    fn seeded_rollup() -> crate::memory::Rollup {
        let mut r = crate::memory::Rollup::default();
        for i in 0..10 {
            let key = format!("session:web:{i}");
            r.count(&key);
            r.measure(&key, 100);
        }
        r
    }

    #[test]
    fn a_progress_message_fills_the_table_in_as_it_goes() {
        let mut app = memory_app();
        assert!(memory(&app).running);
        app.on_msg(Msg::Memory {
            rollup: Box::new(seeded_rollup()),
            done: false,
        });
        assert_eq!(memory(&app).rollup.scanned(), 10);
        assert!(memory(&app).running, "still going");

        app.on_msg(Msg::Memory {
            rollup: Box::new(seeded_rollup()),
            done: true,
        });
        assert!(!memory(&app).running, "the last message ends the scan");
    }

    #[test]
    fn changing_the_depth_regroups_what_was_already_scanned() {
        let mut app = memory_app();
        app.on_msg(Msg::Memory {
            rollup: Box::new(seeded_rollup()),
            done: true,
        });
        assert_eq!(memory(&app).rows()[0].prefix, "session:");
        app.on_key(KeyEvent::new(KeyCode::Char('2'), KeyModifiers::NONE));
        assert_eq!(memory(&app).depth, 2);
        assert_eq!(memory(&app).rows()[0].prefix, "session:web:");
    }

    #[test]
    fn escape_stops_the_scan_rather_than_leaving_it_running() {
        let mut app = memory_app();
        let cancel = memory(&app).cancel.clone();
        app.on_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert!(app.modal.is_none());
        assert!(
            cancel.load(std::sync::atomic::Ordering::Relaxed),
            "the task is told to stop"
        );
    }

    #[test]
    fn a_scan_of_an_empty_keyspace_measures_every_key() {
        // One key in a thousand would be a useless sample of a small server.
        assert_eq!(MemoryState::new(0).stride(), 1);
        assert_eq!(MemoryState::new(1_000).stride(), 1);
        assert_eq!(MemoryState::new(2_000_000).stride(), 100);
    }
}
