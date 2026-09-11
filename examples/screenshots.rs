//! Regenerate the README screenshots.
//!
//!     redis-server --port 6379 --daemonize yes      # Redis 8, for the vector set
//!     cargo run --example screenshots
//!
//! Every screen is rendered through ratatui's test backend and written out as
//! SVG, so the pictures are exactly what the terminal draws and can be
//! regenerated after any layout change. The data is synthetic: profiles that
//! point nowhere, keys invented for the occasion, and a scratch database on a
//! local server. Nothing here should ever carry a real host or a secret.

use std::fmt::Write as _;
use std::path::Path;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::style::{Color, Modifier};
use rediscope::app::{App, InfoState, Modal, Msg, PubSubState};
use rediscope::config::{Connection, Store};
use rediscope::redis_client::{ClientEntry, Diagnostics, KeyValue, ServerInfo, SlowEntry};
use rediscope::theme::Theme;
use rediscope::ui;

const WIDTH: u16 = 132;
const HEIGHT: u16 = 34;
const OUT: &str = "docs/screenshots";

/// The scratch database the demo keys are written to. High enough that a
/// developer's own data is never touched.
const DEMO_DB: i64 = 9;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Never touch the config of whoever runs this.
    let home = std::env::temp_dir().join("rediscope-screenshots");
    std::fs::create_dir_all(&home)?;
    // SAFETY: single-threaded start-up, before anything reads the variable.
    unsafe { std::env::set_var("REDISCOPE_HOME", &home) };
    std::fs::create_dir_all(OUT)?;

    let url = std::env::var("REDISCOPE_DEMO_URL")
        .unwrap_or_else(|_| format!("redis://127.0.0.1:6379/{DEMO_DB}"));
    seed(&url).await?;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(demo_store(&url), tx);

    // 1. The server list, before anything is connected.
    shot(&mut app, "connections")?;

    app.connect(demo_connection(&url));
    pump(&mut app, &mut rx, |a| a.client.is_some()).await;
    app.reload_keys();
    pump(&mut app, &mut rx, |a| !a.rows.is_empty()).await;

    // 2. The browser, on a JSON string value. Walking the tree only schedules
    //    a read, so anything already in flight is drained before the key is
    //    selected again and its own value awaited.
    collapse_all(&mut app);
    open_path(&mut app, &["user", "1042"]);
    drain(&mut app, &mut rx);
    press(&mut app, KeyCode::Up);
    press(&mut app, KeyCode::Down);
    pump(&mut app, &mut rx, |a| {
        a.value.is_some() && a.current.as_ref().is_some_and(|k| k.name == "user:1042")
    })
    .await;
    shot(&mut app, "browser")?;

    // 3. The value editor over that JSON.
    press(&mut app, KeyCode::Char('e'));
    shot(&mut app, "editor")?;
    press(&mut app, KeyCode::Esc);

    // 4. Server info. The picture uses an invented INFO reply rather than the
    //    local server's, so it shows a machine worth looking at and leaks
    //    nothing about the one it was generated on.
    app.modal = Some(Modal::Info(Box::new(InfoState::new(
        ServerInfo::parse(DEMO_INFO),
        demo_diagnostics(),
    ))));
    press(&mut app, KeyCode::Char('2')); // the Memory tab, with its usage bar
    shot(&mut app, "server-info")?;
    press(&mut app, KeyCode::Esc);

    // 5. The namespace memory report, once the scan has finished.
    press(&mut app, KeyCode::Char('M'));
    pump(
        &mut app,
        &mut rx,
        |a| matches!(&a.modal, Some(Modal::Memory(m)) if !m.running),
    )
    .await;
    shot(&mut app, "memory")?;
    press(&mut app, KeyCode::Esc);

    // 6. The pub/sub feed. The traffic is fabricated rather than published, so
    //    the picture is the same every time it is regenerated.
    app.modal = Some(Modal::PubSub(demo_feed()));
    shot(&mut app, "pubsub")?;

    // 7. The command monitor, fabricated the same way.
    app.modal = Some(Modal::PubSub(demo_monitor()));
    shot(&mut app, "monitor")?;

    // 8. A gzipped JSON document, recognised by its header and decoded on the
    //    way to the screen. Taken last, so the folders it opens never show
    //    behind the dialogs above.
    app.modal = None;
    collapse_all(&mut app);
    open_path(&mut app, &["cache", "profile", "1042"]);
    drain(&mut app, &mut rx);
    press(&mut app, KeyCode::Up);
    press(&mut app, KeyCode::Down);
    pump(&mut app, &mut rx, |a| {
        matches!(a.value, Some(KeyValue::Decoded { .. }))
            && a.current
                .as_ref()
                .is_some_and(|k| k.name == "cache:profile:1042")
    })
    .await;
    shot(&mut app, "codecs")?;

    // 9. The ctrl+p palette, part way through typing a key it will jump to.
    app.on_key(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::CONTROL));
    for c in "1042".chars() {
        press(&mut app, KeyCode::Char(c));
    }
    shot(&mut app, "palette")?;
    press(&mut app, KeyCode::Esc);

    // 10. A vector set, searched for the elements most like one of them.
    collapse_all(&mut app);
    open_path(&mut app, &["embeddings", "products"]);
    drain(&mut app, &mut rx);
    press(&mut app, KeyCode::Up);
    press(&mut app, KeyCode::Down);
    pump(&mut app, &mut rx, |a| {
        a.value.is_some()
            && a.current
                .as_ref()
                .is_some_and(|k| k.name == "embeddings:products")
    })
    .await;
    press(&mut app, KeyCode::Tab);
    for _ in 0..2 {
        press(&mut app, KeyCode::Down);
    }
    press(&mut app, KeyCode::Char('S'));
    pump(
        &mut app,
        &mut rx,
        |a| matches!(&a.value, Some(KeyValue::Rows { headers, .. }) if headers.len() == 3),
    )
    .await;
    shot(&mut app, "vectors")?;

    println!("wrote {} screenshots to {OUT}/", 10);
    Ok(())
}

fn demo_store(_url: &str) -> Store {
    Store {
        theme: Theme::TokyoNight,
        connections: vec![
            // Pictured at the address the docs tell people to use, whichever
            // server `REDISCOPE_DEMO_URL` actually points the run at.
            demo_connection(&format!("redis://127.0.0.1:6379/{DEMO_DB}")),
            Connection {
                name: "staging".into(),
                host: "cache-01.staging.example".into(),
                port: 6380,
                db: 0,
                tls: true,
                ..Default::default()
            },
            Connection {
                name: "production".into(),
                host: "cache-01.prod.example".into(),
                port: 6380,
                db: 0,
                tls: true,
                read_only: true,
                use_keychain: true,
                ..Default::default()
            },
            Connection {
                name: "analytics".into(),
                host: "cache-02.prod.example".into(),
                port: 6380,
                db: 2,
                tls: true,
                read_only: true,
                ..Default::default()
            },
            Connection {
                name: "eu-replica".into(),
                host: "replica.eu.example".into(),
                port: 6379,
                tls: true,
                ssh_host: "bastion.eu.example".into(),
                ssh_user: "deploy".into(),
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

/// The same server `seed` filled. `REDISCOPE_DEMO_URL` has to steer both, or
/// the pictures come out of an empty database.
fn demo_connection(url: &str) -> Connection {
    Connection {
        name: "local-dev".into(),
        ..Connection::from_url(url).expect("REDISCOPE_DEMO_URL is not a redis url")
    }
}

fn demo_feed() -> PubSubState {
    let mut feed = PubSubState::new(vec!["orders:*".into(), "news:*".into()], false);
    let start = feed.started;
    let channels = ["orders:eu", "orders:us", "news:release", "metrics:cpu"];
    for second in 0..40u64 {
        // A rising and falling rate, so the sparkline has a shape.
        let burst = 1.0 + (second as f64 * 0.4).sin().abs() * 6.0;
        for n in 0..burst as usize {
            let channel = channels[(second as usize + n) % channels.len()];
            let payload = format!(
                r#"{{"id":{},"channel":"{channel}","level":"{}","body":"event {}"}}"#,
                second * 10 + n as u64,
                if n % 5 == 0 { "warn" } else { "info" },
                second * 10 + n as u64
            );
            feed.push_at(
                channel.into(),
                payload,
                start + std::time::Duration::from_secs(second),
            );
        }
    }
    feed.follow = false;
    feed.scroll = feed.messages.len() - 3;
    feed
}

/// A `MONITOR` feed as a busy cache would fill it: the commands, the rate, and
/// the share it could not keep up with. Fabricated like the pub/sub feed, so
/// the picture never changes between runs and shows no real client.
fn demo_monitor() -> PubSubState {
    let mut feed = PubSubState::monitor(Vec::new());
    let start = feed.started;
    let clients = ["10.4.19.31:52110", "10.4.19.44:40912", "10.4.20.7:61208"];
    for second in 0..40u64 {
        let burst = 2.0 + (second as f64 * 0.3).cos().abs() * 7.0;
        for n in 0..burst as usize {
            let id = 1042 + (second * 7 + n as u64) % 400;
            let client = clients[(second as usize + n) % clients.len()];
            let (command, args) = match (second as usize + n) % 6 {
                0 | 1 => ("GET", format!(r#""user:{id}""#)),
                2 => ("HGETALL", format!(r#""orders:2026:{:04}""#, id % 420)),
                3 => (
                    "SETEX",
                    format!(r#""session:web:{id:05}" "1800" "{{"user":{id}}}""#),
                ),
                4 => ("ZINCRBY", format!(r#""leaderboard:eu" "5" "user:{id}""#)),
                _ => ("EXPIRE", format!(r#""cache:page:{:03}" "120""#, id % 48)),
            };
            feed.push_at(
                command.into(),
                format!("db0 {client}  {args}"),
                start + std::time::Duration::from_secs(second),
            );
        }
        // The rest of that second's traffic: counted in the rate and the
        // total, never queued, which is how the feed survives a busy server.
        let rate = 3_000.0 + (second as f64 * 0.21).sin().abs() * 6_500.0;
        feed.push_dropped_at(
            rate as u64 - burst as u64,
            start + std::time::Duration::from_secs(second),
        );
    }
    feed.follow = false;
    feed.scroll = feed.messages.len() - 3;
    feed
}

/// A plausible `INFO` reply for a mid-sized production cache. Invented from end
/// to end: no field here came off a real server.
const DEMO_INFO: &str = "# Server\r
redis_version:8.2.1\r
redis_mode:standalone\r
os:Linux 6.8.0-51-generic x86_64\r
arch_bits:64\r
process_id:1\r
tcp_port:6380\r
uptime_in_seconds:1904400\r
uptime_in_days:22\r
executable:/usr/local/bin/redis-server\r
config_file:/etc/redis/redis.conf\r
\r
# Clients\r
connected_clients:184\r
cluster_connections:0\r
maxclients:10000\r
blocked_clients:3\r
\r
# Memory\r
used_memory:6871947673\r
used_memory_human:6.40G\r
used_memory_rss_human:6.71G\r
used_memory_peak_human:7.02G\r
used_memory_dataset_human:6.02G\r
maxmemory:8589934592\r
maxmemory_human:8.00G\r
maxmemory_policy:allkeys-lru\r
mem_fragmentation_ratio:1.05\r
mem_allocator:jemalloc-5.3.0\r
\r
# Persistence\r
rdb_last_bgsave_status:ok\r
aof_enabled:1\r
aof_last_write_status:ok\r
\r
# Stats\r
total_connections_received:9482113\r
total_commands_processed:41822904115\r
instantaneous_ops_per_sec:38412\r
keyspace_hits:38911204471\r
keyspace_misses:1204118342\r
expired_keys:882401173\r
evicted_keys:1904822\r
rejected_connections:0\r
\r
# Replication\r
role:master\r
connected_slaves:2\r
slave0:ip=10.4.19.22,port=6380,state=online,offset=88213904712,lag=0\r
slave1:ip=10.4.20.14,port=6380,state=online,offset=88213904102,lag=1\r
master_repl_offset:88213904712\r
\r
# CPU\r
used_cpu_sys:184402.19\r
used_cpu_user:392018.44\r
\r
# Keyspace\r
db0:keys=41892204,expires=41112904,avg_ttl=1794000\r
db2:keys=180422,expires=0,avg_ttl=0\r
";

/// Slow queries, clients and running config to match, so every tab of the
/// picture has something in it. Also invented.
fn demo_diagnostics() -> Diagnostics {
    Diagnostics {
        slowlog: vec![
            SlowEntry {
                id: 4821,
                at: 1_774_000_000,
                micros: 41_902,
                command: "KEYS session:web:*".into(),
                client: "10.4.18.9:52114".into(),
            },
            SlowEntry {
                id: 4820,
                at: 1_773_999_400,
                micros: 18_774,
                command: "SMEMBERS features:beta".into(),
                client: "10.4.18.11:41220".into(),
            },
        ],
        clients: vec![
            ClientEntry {
                id: "91422".into(),
                addr: "10.4.18.9:52114".into(),
                name: "checkout-api".into(),
                age_secs: 88_204,
                idle_secs: 0,
                db: "0".into(),
                command: "hgetall".into(),
            },
            ClientEntry {
                id: "91423".into(),
                addr: "10.4.18.11:41220".into(),
                name: "sessions-worker".into(),
                age_secs: 41_002,
                idle_secs: 2,
                db: "0".into(),
                command: "setex".into(),
            },
        ],
        config: vec![
            ("maxmemory".into(), "8589934592".into()),
            ("maxmemory-policy".into(), "allkeys-lru".into()),
            ("appendonly".into(), "yes".into()),
            ("timeout".into(), "300".into()),
        ],
        latency: vec![("expire-cycle".into(), "14".into())],
        cluster: vec![("cluster_enabled".into(), "0".into())],
        modules: Vec::new(),
    }
}

/// Write the demo keyspace. Flushed first, so a rerun cannot leave stale keys
/// behind in the picture. The volumes are made up but shaped like a real cache:
/// a lot of short-lived sessions, fewer orders, a handful of everything else.
async fn seed(url: &str) -> anyhow::Result<()> {
    use redis::AsyncCommands;
    let client = redis::Client::open(url)?;
    let mut c = client.get_multiplexed_async_connection().await?;
    let _: () = redis::cmd("FLUSHDB").query_async(&mut c).await?;

    let profile = r#"{"id":1042,"name":"Ada Lovelace","plan":"team","seats":12,"regions":["eu-west","us-east"],"createdAt":"2025-11-02T09:14:00Z"}"#;
    let _: () = c.set("user:1042", profile).await?;

    // The same kind of document the way many apps actually cache it: gzipped.
    // The value pane recognises the header and shows the JSON inside.
    let cached = r#"{"id":1042,"name":"Ada Lovelace","plan":"team","seats":12,"features":["sso","audit-log","priority-support"],"limits":{"projects":50,"storageGb":200},"renewsAt":"2026-11-02T00:00:00Z"}"#;
    let gzipped = rediscope::codec::encode(
        &rediscope::codec::Codec::Builtin(rediscope::codec::Builtin::Gzip),
        cached,
    )?;
    let _: () = c.set_ex("cache:profile:1042", gzipped, 3_600).await?;

    let mut pipe = redis::pipe();
    for n in 0..1_400 {
        pipe.set_ex(
            format!("session:web:{n:05}"),
            format!("{{\"user\":{},\"csrf\":\"synthetic\"}}", 1000 + n % 400),
            1_800 + n % 900,
        )
        .ignore();
    }
    for n in 0..420 {
        pipe.hset_multiple(
            format!("orders:2026:{n:04}"),
            &[
                ("total", format!("{}.{:02}", 20 + n % 300, n % 100)),
                (
                    "status",
                    if n % 7 == 0 {
                        "pending".into()
                    } else {
                        "paid".to_string()
                    },
                ),
                ("items", format!("{}", 1 + n % 5)),
            ],
        )
        .ignore();
    }
    for n in 0..260 {
        let id = 1043 + n;
        pipe.set(
            format!("user:{id}"),
            format!(
                r#"{{"id":{id},"plan":"{}"}}"#,
                if n % 3 == 0 { "team" } else { "solo" }
            ),
        )
        .ignore();
    }
    for n in 0..48 {
        pipe.set_ex(
            format!("cache:page:{n:03}"),
            "<html>…</html>".repeat(4),
            120 + n,
        )
        .ignore();
    }
    let _: () = pipe.query_async(&mut c).await?;

    let _: () = c
        .rpush(
            "queue:emails",
            &["welcome:1042", "digest:1043", "receipt:1044"],
        )
        .await?;
    let _: () = c.expire("queue:emails", 600).await?;
    let _: () = c.sadd("features:beta", &["1042", "1044", "1121"]).await?;
    for (member, score) in [("ada", 940), ("grace", 880), ("alan", 815)] {
        let _: () = c.zadd("leaderboard:eu", member, score).await?;
    }
    // A vector set of product embeddings (Redis 8). Eight dimensions is
    // nothing like a real model, but it searches the same way.
    let products = [
        (
            "trail-runner",
            "shoes",
            129,
            [0.9, 0.1, 0.8, 0.2, 0.1, 0.0, 0.3, 0.1],
        ),
        (
            "road-racer",
            "shoes",
            149,
            [0.8, 0.2, 0.9, 0.1, 0.2, 0.1, 0.2, 0.0],
        ),
        (
            "hiking-boot",
            "shoes",
            189,
            [0.9, 0.0, 0.5, 0.6, 0.1, 0.1, 0.5, 0.2],
        ),
        (
            "rain-shell",
            "jackets",
            220,
            [0.2, 0.9, 0.1, 0.7, 0.3, 0.1, 0.4, 0.1],
        ),
        (
            "down-parka",
            "jackets",
            340,
            [0.1, 0.8, 0.0, 0.9, 0.2, 0.2, 0.1, 0.3],
        ),
        (
            "wool-beanie",
            "hats",
            35,
            [0.0, 0.5, 0.1, 0.8, 0.9, 0.1, 0.0, 0.2],
        ),
        (
            "sun-cap",
            "hats",
            29,
            [0.3, 0.2, 0.4, 0.0, 0.9, 0.0, 0.6, 0.1],
        ),
        (
            "day-pack",
            "bags",
            95,
            [0.5, 0.3, 0.3, 0.3, 0.1, 0.9, 0.4, 0.2],
        ),
        (
            "trail-socks",
            "socks",
            18,
            [0.8, 0.1, 0.7, 0.3, 0.2, 0.1, 0.2, 0.9],
        ),
    ];
    for (name, category, price, vector) in products {
        let mut add = redis::cmd("VADD");
        add.arg("embeddings:products")
            .arg("VALUES")
            .arg(vector.len());
        for v in vector {
            add.arg(v);
        }
        add.arg(name).arg("SETATTR").arg(format!(
            r#"{{"category":"{category}","price":{price},"in_stock":{}}}"#,
            price < 300
        ));
        let _: i64 = add
            .query_async(&mut c)
            .await
            .map_err(|e| anyhow::anyhow!("the demo needs Redis 8 for its vector set: {e}"))?;
    }
    for n in 0..6 {
        let _: () = c
            .xadd(
                "events:signup",
                "*",
                &[("user", format!("{}", 1042 + n).as_str()), ("plan", "team")],
            )
            .await?;
    }
    Ok(())
}

fn press(app: &mut App, code: KeyCode) {
    app.on_key(KeyEvent::new(code, KeyModifiers::NONE));
}

/// Fold every top-level folder, so the tree can be walked without loading a
/// value for each of two thousand keys on the way past.
fn collapse_all(app: &mut App) {
    press(app, KeyCode::Char('g'));
    for _ in 0..64 {
        press(app, KeyCode::Left);
        if app.tree_state.selected() == Some(app.rows.len().saturating_sub(1)) {
            break;
        }
        press(app, KeyCode::Down);
    }
    press(app, KeyCode::Char('g'));
}

/// Open a folder path and land on the key at the end of it, e.g.
/// `["user", "1042", "profile"]` for `user:1042:profile`.
fn open_path(app: &mut App, path: &[&str]) {
    for (depth, segment) in path.iter().enumerate() {
        for _ in 0..512 {
            if app.selected_row().is_some_and(|r| r.label == *segment) {
                break;
            }
            press(app, KeyCode::Down);
        }
        // Every segment but the last names a folder, which has to be opened
        // before the next one is on screen.
        if depth + 1 < path.len() {
            press(app, KeyCode::Right);
            press(app, KeyCode::Down);
        }
    }
}

/// Handle whatever has already arrived, without waiting for more.
fn drain(app: &mut App, rx: &mut tokio::sync::mpsc::UnboundedReceiver<Msg>) {
    while let Ok(msg) = rx.try_recv() {
        app.on_msg(msg);
    }
}

/// Drive the message loop until `done`, or until the server has clearly
/// answered everything it is going to. Mirrors the real event loop, including
/// the timer that reads the value once the cursor has stopped moving.
async fn pump(
    app: &mut App,
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Msg>,
    done: impl Fn(&App) -> bool,
) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while !done(app) {
        let value_due = app.value_deadline().map(tokio::time::Instant::from_std);
        tokio::select! {
            biased;
            () = async move {
                match value_due {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            } => app.on_value_deadline(),
            msg = tokio::time::timeout_at(deadline, rx.recv()) => match msg {
                Ok(Some(msg)) => app.on_msg(msg),
                _ => break,
            },
        }
    }
}

fn shot(app: &mut App, name: &str) -> anyhow::Result<()> {
    // The banner in the title bar reports whichever server generated the
    // pictures. Pin it, so a developer's local build never ends up in the
    // README, and every screenshot agrees with the invented INFO reply.
    if let Some(client) = app.client.as_mut() {
        app.server_line = "redis 8.2.1 · standalone".into();
        // Likewise the address: whatever `REDISCOPE_DEMO_URL` points at, the
        // pictures show the default one. Only the label changes; the
        // connection itself is already open.
        client.conn.host = "127.0.0.1".into();
        client.conn.port = 6379;
    }
    let mut terminal = Terminal::new(TestBackend::new(WIDTH, HEIGHT))?;
    terminal.draw(|f| ui::draw(f, app))?;
    let svg = svg(terminal.backend().buffer(), app.store.theme);
    let path = Path::new(OUT).join(format!("{name}.svg"));
    std::fs::write(&path, svg)?;
    println!("{}", path.display());
    Ok(())
}

// ---- the SVG writer -----------------------------------------------------

/// Character cell size in the output, in pixels. The text is stretched to fit
/// exactly, so the picture does not depend on which monospace font a reader's
/// browser picks.
const CW: f64 = 9.6;
const CH: f64 = 17.6;
const FONT: &str =
    "ui-monospace, SFMono-Regular, 'SF Mono', Menlo, Consolas, 'DejaVu Sans Mono', monospace";

fn svg(buffer: &ratatui::buffer::Buffer, theme: Theme) -> String {
    let palette = theme.palette();
    let default_bg = hex(palette.background, "#12131c");
    let default_fg = hex(palette.foreground, "#c8d0f0");
    let width = WIDTH as f64 * CW;
    let height = HEIGHT as f64 * CH;

    let mut out = String::new();
    let _ = write!(
        out,
        r#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width:.0} {height:.0}" width="{width:.0}" height="{height:.0}" font-family="{FONT}" font-size="16">"#
    );
    let _ = write!(
        out,
        r#"<rect width="{width:.0}" height="{height:.0}" rx="6" fill="{default_bg}"/>"#
    );

    // Backgrounds first, as one rectangle per run of equal colour.
    for y in 0..HEIGHT {
        let mut run: Option<(u16, String)> = None;
        for x in 0..WIDTH {
            let bg = hex(
                buffer[(x, y)].style().bg.unwrap_or(Color::Reset),
                &default_bg,
            );
            match &mut run {
                Some((start, colour)) if *colour == bg => {}
                Some((start, colour)) => {
                    emit_bg(&mut out, *start, x, y, colour, &default_bg);
                    run = Some((x, bg));
                }
                None => run = Some((x, bg)),
            }
        }
        if let Some((start, colour)) = run {
            emit_bg(&mut out, start, WIDTH, y, &colour, &default_bg);
        }
    }

    // Then the text, one span per run of equal style.
    for y in 0..HEIGHT {
        let mut start = 0u16;
        let mut text = String::new();
        let mut style = buffer[(0, y)].style();
        for x in 0..=WIDTH {
            let cell_style = (x < WIDTH).then(|| buffer[(x, y)].style());
            if cell_style != Some(style) || x == WIDTH {
                emit_text(&mut out, start, y, &text, style, &default_fg);
                text.clear();
                start = x;
                if let Some(next) = cell_style {
                    style = next;
                }
            }
            if x < WIDTH {
                text.push_str(buffer[(x, y)].symbol());
            }
        }
    }

    out.push_str("</svg>");
    out
}

fn emit_bg(out: &mut String, start: u16, end: u16, y: u16, colour: &str, default: &str) {
    if colour == default || end <= start {
        return;
    }
    let _ = write!(
        out,
        r#"<rect x="{:.1}" y="{:.1}" width="{:.1}" height="{CH:.1}" fill="{colour}"/>"#,
        start as f64 * CW,
        y as f64 * CH,
        (end - start) as f64 * CW
    );
}

fn emit_text(
    out: &mut String,
    start: u16,
    y: u16,
    text: &str,
    style: ratatui::style::Style,
    default_fg: &str,
) {
    if text.trim().is_empty() {
        return;
    }
    let fill = hex(style.fg.unwrap_or(Color::Reset), default_fg);
    let weight = if style.add_modifier.contains(Modifier::BOLD) {
        r#" font-weight="600""#
    } else {
        ""
    };
    let columns = text.chars().count() as f64;
    let _ = write!(
        out,
        r#"<text x="{:.1}" y="{:.1}" fill="{fill}"{weight} textLength="{:.1}" lengthAdjust="spacingAndGlyphs" xml:space="preserve">{}</text>"#,
        start as f64 * CW,
        y as f64 * CH + CH * 0.78,
        columns * CW,
        escape(text)
    );
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// A ratatui colour as CSS. `Reset` means "whatever the terminal uses", which
/// in a picture has to become a concrete colour.
fn hex(color: Color, default: &str) -> String {
    match color {
        Color::Reset => default.to_string(),
        Color::Rgb(r, g, b) => format!("#{r:02x}{g:02x}{b:02x}"),
        Color::Black => "#1b1d2b".into(),
        Color::Red => "#f7768e".into(),
        Color::Green => "#9ece6a".into(),
        Color::Yellow => "#e0af68".into(),
        Color::Blue => "#7aa2f7".into(),
        Color::Magenta => "#bb9af7".into(),
        Color::Cyan => "#7dcfff".into(),
        Color::Gray => "#a9b1d6".into(),
        Color::DarkGray => "#565f89".into(),
        Color::LightRed => "#ff9ba8".into(),
        Color::LightGreen => "#b9f27c".into(),
        Color::LightYellow => "#f2d5a0".into(),
        Color::LightBlue => "#9cb8ff".into(),
        Color::LightMagenta => "#d3b4ff".into(),
        Color::LightCyan => "#a4e0ff".into(),
        Color::White => "#c0caf5".into(),
        Color::Indexed(i) => indexed(i),
    }
}

/// The xterm-256 cube, for a theme that reaches for an indexed colour.
fn indexed(index: u8) -> String {
    let level = |v: u8| -> u8 { if v == 0 { 0 } else { 55 + v * 40 } };
    match index {
        0..=15 => hex(
            [
                Color::Black,
                Color::Red,
                Color::Green,
                Color::Yellow,
                Color::Blue,
                Color::Magenta,
                Color::Cyan,
                Color::Gray,
                Color::DarkGray,
                Color::LightRed,
                Color::LightGreen,
                Color::LightYellow,
                Color::LightBlue,
                Color::LightMagenta,
                Color::LightCyan,
                Color::White,
            ][index as usize],
            "#c0caf5",
        ),
        16..=231 => {
            let i = index - 16;
            format!(
                "#{:02x}{:02x}{:02x}",
                level(i / 36),
                level((i % 36) / 6),
                level(i % 6)
            )
        }
        _ => {
            let v = 8 + (index - 232) * 10;
            format!("#{v:02x}{v:02x}{v:02x}")
        }
    }
}
