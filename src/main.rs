//! rediscope — a terminal UI Redis client.

use std::io::{self, Stdout};

use anyhow::Result;
use clap::{Parser, Subcommand};
use crossterm::event::{Event, EventStream, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use futures_util::StreamExt;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

use rediscope::app::{App, Msg};
use rediscope::config::{self, Connection, Store};
use rediscope::{headless, ui};

/// A terminal UI Redis client: browse keys as a tree, edit every value type,
/// run raw commands.
#[derive(Parser, Debug)]
#[command(name = "rediscope", version, about, long_about = None)]
struct Cli {
    /// Redis host. Given, rediscope connects straight away and skips the server list.
    #[arg(short = 'H', long)]
    host: Option<String>,

    /// Redis port.
    #[arg(short = 'p', long, default_value_t = 6379)]
    port: u16,

    /// Database index.
    #[arg(short = 'n', long, default_value_t = 0)]
    db: i64,

    /// Username (Redis 6+ ACL).
    #[arg(short = 'u', long, default_value = "")]
    username: String,

    /// Password. Prefer the env var: a flag is visible in `ps` output.
    #[arg(
        short = 'a',
        long,
        env = "REDISCOPE_PASSWORD",
        default_value = "",
        hide_env_values = true
    )]
    password: String,

    /// Connect over TLS.
    #[arg(long)]
    tls: bool,

    /// PEM root certificate, when the server is not signed by a public CA.
    #[arg(long, value_name = "FILE")]
    tls_ca: Option<String>,

    /// PEM client certificate, for mutual TLS. Requires --tls-key.
    #[arg(long, value_name = "FILE", requires = "tls_key")]
    tls_cert: Option<String>,

    /// PEM client key, for mutual TLS. Requires --tls-cert.
    #[arg(long, value_name = "FILE", requires = "tls_cert")]
    tls_key: Option<String>,

    /// Accept any server certificate. Only sensible against a dev server.
    #[arg(long)]
    tls_insecure: bool,

    /// Connect via URL, e.g. redis://user:pass@host:6379/2. Overrides the other flags.
    #[arg(long)]
    url: Option<String>,

    /// Refuse every write for this session.
    #[arg(long)]
    read_only: bool,

    /// Reach the server through `ssh -L` on this jump host.
    #[arg(long, value_name = "HOST")]
    ssh: Option<String>,

    /// SSH user for --ssh.
    #[arg(long, value_name = "USER")]
    ssh_user: Option<String>,

    /// SSH port for --ssh.
    #[arg(long, value_name = "PORT", default_value_t = 22)]
    ssh_port: u16,

    /// SSH private key for --ssh.
    #[arg(long, value_name = "FILE")]
    ssh_key: Option<String>,

    /// Print the path of the connections file and exit.
    #[arg(long)]
    config_path: bool,

    /// Connect straight to a saved profile by name.
    #[arg(long, value_name = "NAME")]
    profile: Option<String>,

    #[command(subcommand)]
    command: Option<Command>,
}

/// Subcommands print to stdout and never open the TUI, so rediscope can be
/// used from a script as well as a terminal.
#[derive(Subcommand, Debug)]
enum Command {
    /// List keys matching a pattern.
    Keys {
        /// Glob pattern, as SCAN MATCH takes it.
        #[arg(long, default_value = "*")]
        pattern: String,
        #[arg(long)]
        json: bool,
    },
    /// Write keys and their TTLs to a file, as DUMP payloads.
    Export {
        #[arg(long, default_value = "*")]
        pattern: String,
        /// Output file, or `-` for stdout.
        #[arg(long, default_value = "-")]
        out: String,
    },
    /// Restore keys from a file written by `export`.
    Import {
        #[arg(long)]
        file: String,
        /// Overwrite keys that already exist.
        #[arg(long)]
        replace: bool,
    },
    /// Print the server's INFO reply.
    Info {
        #[arg(long)]
        json: bool,
    },
    /// Estimate which key prefixes hold the memory.
    MemReport {
        /// How many `:`-separated segments to group by.
        #[arg(long, default_value_t = 1)]
        depth: usize,
        #[arg(long)]
        json: bool,
    },
}

impl Cli {
    fn quick_connect(&self) -> Result<Option<Connection>> {
        let tls_ca_file = self.tls_ca.clone().unwrap_or_default();
        let tls_cert_file = self.tls_cert.clone().unwrap_or_default();
        let tls_key_file = self.tls_key.clone().unwrap_or_default();
        // Naming any certificate implies TLS; requiring both flags is noise.
        let tls =
            self.tls || self.tls_insecure || !tls_ca_file.is_empty() || !tls_cert_file.is_empty();

        if let Some(url) = &self.url {
            let mut conn = Connection::from_url(url)?;
            conn.tls = conn.tls || tls;
            conn.tls_ca_file = tls_ca_file;
            conn.tls_cert_file = tls_cert_file;
            conn.tls_key_file = tls_key_file;
            conn.tls_insecure = self.tls_insecure;
            self.apply_extras(&mut conn);
            return Ok(Some(conn));
        }
        let Some(host) = &self.host else {
            return Ok(None);
        };
        Ok(Some(Connection {
            name: host.clone(),
            host: host.clone(),
            port: self.port,
            db: self.db,
            username: self.username.clone(),
            password: self.password.clone(),
            tls,
            tls_ca_file,
            tls_cert_file,
            tls_key_file,
            tls_insecure: self.tls_insecure,
            use_keychain: false,
            read_only: self.read_only,
            ssh_host: self.ssh.clone().unwrap_or_default(),
            ssh_user: self.ssh_user.clone().unwrap_or_default(),
            ssh_port: self.ssh_port,
            ssh_key_file: self.ssh_key.clone().unwrap_or_default(),
        }))
    }

    /// Flags that apply whether the profile came from --url or the host flags.
    fn apply_extras(&self, conn: &mut Connection) {
        conn.read_only = self.read_only;
        conn.ssh_host = self.ssh.clone().unwrap_or_default();
        conn.ssh_user = self.ssh_user.clone().unwrap_or_default();
        conn.ssh_port = self.ssh_port;
        conn.ssh_key_file = self.ssh_key.clone().unwrap_or_default();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.config_path {
        println!("{}", config::config_file().display());
        return Ok(());
    }
    let quick = cli.quick_connect()?;

    // A subcommand is a one-shot: resolve the server, print, and exit without
    // ever touching the alternate screen.
    if let Some(command) = &cli.command {
        let conn = headless::resolve(cli.profile.as_deref(), quick)?;
        return match command {
            Command::Keys { pattern, json } => headless::keys(conn, pattern, *json).await,
            Command::Export { pattern, out } => headless::export(conn, pattern, out).await,
            Command::Import { file, replace } => headless::import(conn, file, *replace).await,
            Command::Info { json } => headless::info(conn, *json).await,
            Command::MemReport { depth, json } => headless::mem_report(conn, *depth, *json).await,
        };
    }

    // `--profile name` opens that saved server directly, the same as picking
    // it from the list.
    let quick = match (&cli.profile, quick) {
        (Some(name), _) => Some(headless::resolve(Some(name), None)?),
        (None, quick) => quick,
    };

    let mut terminal = setup()?;
    let result = run(&mut terminal, quick).await;
    restore(&mut terminal)?;
    result
}

type Term = Terminal<CrosstermBackend<Stdout>>;

fn setup() -> Result<Term> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    // A panic in raw mode leaves the terminal unusable; always undo it first.
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        hook(info);
    }));
    Ok(Terminal::new(CrosstermBackend::new(stdout))?)
}

fn restore(terminal: &mut Term) -> Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

async fn run(terminal: &mut Term, quick: Option<Connection>) -> Result<()> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let (store, notice) = Store::load();
    let mut app = App::new(store, tx);
    if let Some(notice) = notice {
        app.status = notice;
    }
    if let Some(conn) = quick {
        app.connect(conn);
    }

    let mut events = EventStream::new();
    // Drives the TTL countdown; keys drop out of the tree as they expire.
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        terminal.draw(|f| ui::draw(f, &mut app))?;
        tokio::select! {
            maybe_event = events.next() => match maybe_event {
                Some(Ok(Event::Key(key))) if key.kind != KeyEventKind::Release => app.on_key(key),
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e.into()),
                None => break,
            },
            Some(msg) = rx.recv() => app.on_msg(msg),
            _ = ticker.tick() => app.on_tick(),
        }
        if app.should_quit {
            break;
        }
    }
    Ok(())
}
