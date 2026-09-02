//! rediscope — a terminal UI Redis client.

use std::io::{self, Stdout};

use anyhow::Result;
use clap::Parser;
use crossterm::event::{Event, EventStream, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use futures::StreamExt;
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

use rediscope::app::{App, Msg};
use rediscope::config::{self, Connection, Store};
use rediscope::ui;

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

    /// Connect via URL, e.g. redis://user:pass@host:6379/2. Overrides the other flags.
    #[arg(long)]
    url: Option<String>,

    /// Print the path of the connections file and exit.
    #[arg(long)]
    config_path: bool,
}

impl Cli {
    fn quick_connect(&self) -> Result<Option<Connection>> {
        if let Some(url) = &self.url {
            return Ok(Some(Connection::from_url(url)?));
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
            tls: self.tls,
        }))
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
    let mut app = App::new(Store::load(), tx);
    if let Some(conn) = quick {
        app.connect(conn);
    }

    let mut events = EventStream::new();
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
        }
        if app.should_quit {
            break;
        }
    }
    Ok(())
}
