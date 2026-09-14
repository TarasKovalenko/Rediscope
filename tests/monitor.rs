//! The command monitor against a real redis-server.
//!
//! Skipped unless REDISCOPE_TEST_PORT is set:
//!   redis-server --port 7799 --daemonize yes
//!   REDISCOPE_TEST_PORT=7799 cargo test --test monitor

mod common;

use crossterm::event::{KeyCode, KeyEvent};
use futures_util::StreamExt;
use rediscope::app::{Action, App, Modal, Msg, Screen};
use rediscope::config::{Connection, Environment, Store};
use rediscope::redis_client::{Client, parse_monitor_line};

/// These tests count MONITOR clients on the shared server, so they run one
/// at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn monitors(client: &Client) -> usize {
    let list = client.execute_raw("CLIENT LIST").await.unwrap();
    // Dragonfly's CLIENT LIST has no cmd= field, so a MONITOR connection
    // cannot be told apart there. Every test here holds SERIAL, which leaves
    // the connection count moving only with the monitor.
    if !list.contains(" cmd=") {
        return list.lines().count();
    }
    list.lines().filter(|l| l.contains("cmd=monitor")).count()
}

fn conn(environment: Environment) -> Option<Connection> {
    common::isolate_config();
    let port: u16 = std::env::var("REDISCOPE_TEST_PORT").ok()?.parse().ok()?;
    Some(Connection {
        name: "monitored".into(),
        host: "127.0.0.1".into(),
        port,
        environment,
        ..Default::default()
    })
}

#[tokio::test]
async fn monitor_sees_other_clients_commands() {
    let Some(conn) = conn(Environment::Development) else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let client = Client::connect(conn).await.unwrap();
    let monitor = client.monitor().await.unwrap();
    let mut stream = monitor.into_on_message::<String>();
    client.execute_raw("SET monitor:probe hello").await.unwrap();
    let seen = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(line) = stream.next().await {
            if let Some(parsed) = parse_monitor_line(&line)
                && parsed.command == "SET"
                && parsed.detail.contains("monitor:probe")
            {
                return Some(parsed);
            }
        }
        None
    })
    .await
    .expect("MONITOR delivered nothing within 5 s")
    .expect("stream ended");
    assert!(seen.detail.starts_with("db0 "), "{}", seen.detail);
    assert!(
        seen.detail.ends_with(r#""monitor:probe" "hello""#),
        "{}",
        seen.detail
    );
    client.execute_raw("DEL monitor:probe").await.unwrap();
}

#[tokio::test]
async fn the_feed_fills_in_batches_and_honours_its_filter() {
    let Some(conn) = conn(Environment::Development) else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let client = Client::connect(conn).await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(Store::default(), tx);
    app.screen = Screen::Browser;
    app.client = Some(client.clone());

    // Development profiles start straight away, with a filter set through
    // the feed's own form.
    app.on_key(KeyEvent::from(KeyCode::Char('W')));
    assert!(matches!(app.modal, Some(Modal::PubSub(ref s)) if s.monitor));
    app.on_key(KeyEvent::from(KeyCode::Char('s')));
    for c in "mon:keep".chars() {
        app.on_key(KeyEvent::from(KeyCode::Char(c)));
    }
    app.on_key(KeyEvent::from(KeyCode::Enter));
    assert!(
        matches!(app.modal, Some(Modal::PubSub(ref s)) if s.monitor && s.patterns == ["mon:keep"])
    );
    // Let the MONITOR connection come up before issuing commands.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    for i in 0..20 {
        client
            .execute_raw(&format!("SET mon:keep:{i} x"))
            .await
            .unwrap();
        client
            .execute_raw(&format!("SET mon:skip:{i} x"))
            .await
            .unwrap();
    }
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if let Ok(Some(msg)) =
            tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await
        {
            app.on_msg(msg);
        }
        let Some(Modal::PubSub(state)) = &app.modal else {
            panic!("the feed closed")
        };
        if state.total >= 20 || tokio::time::Instant::now() > deadline {
            break;
        }
    }
    let Some(Modal::PubSub(state)) = &app.modal else {
        panic!("the feed closed")
    };
    assert_eq!(state.total, 20, "only the kept commands arrive");
    assert!(state.messages.iter().all(|m| m.channel == "SET"));
    assert!(
        state
            .messages
            .iter()
            .all(|m| m.payload.contains("mon:keep"))
    );

    // Esc stops the monitor: the task is gone with the modal.
    app.on_key(KeyEvent::from(KeyCode::Esc));
    assert!(app.modal.is_none());
    let mut pipe = String::from("DEL");
    for i in 0..20 {
        pipe.push_str(&format!(" mon:keep:{i} mon:skip:{i}"));
    }
    client.execute_raw(&pipe).await.unwrap();
}

#[tokio::test]
async fn a_flood_is_counted_rather_than_queued() {
    let Some(conn) = conn(Environment::Development) else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let client = Client::connect(conn).await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(Store::default(), tx);
    app.screen = Screen::Browser;
    app.client = Some(client.clone());
    app.on_key(KeyEvent::from(KeyCode::Char('W')));
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Far more than one batch keeps, in one burst.
    let mut raw = redis::Client::open(format!(
        "redis://127.0.0.1:{}",
        std::env::var("REDISCOPE_TEST_PORT").unwrap()
    ))
    .unwrap()
    .get_multiplexed_async_connection()
    .await
    .unwrap();
    let mut pipe = redis::pipe();
    for _ in 0..5_000 {
        pipe.cmd("PING").ignore();
    }
    let _: () = pipe.query_async(&mut raw).await.unwrap();

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut largest_batch = 0;
    loop {
        if let Ok(Some(msg)) =
            tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await
        {
            if let Msg::MonitorBatch { lines, .. } = &msg {
                largest_batch = largest_batch.max(lines.len());
            }
            app.on_msg(msg);
        }
        let Some(Modal::PubSub(state)) = &app.modal else {
            panic!("the feed closed")
        };
        if state.total >= 5_000 || tokio::time::Instant::now() > deadline {
            break;
        }
    }
    let Some(Modal::PubSub(state)) = &app.modal else {
        panic!("the feed closed")
    };
    assert!(
        state.total >= 5_000,
        "every command counted: {}",
        state.total
    );
    assert!(largest_batch <= rediscope::app::MONITOR_BATCH);
    assert!(state.messages.len() <= rediscope::app::PUBSUB_LIMIT);
    app.on_key(KeyEvent::from(KeyCode::Esc));
}

#[tokio::test]
async fn production_asks_before_monitoring() {
    let Some(conn) = conn(Environment::Production) else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let client = Client::connect(conn).await.unwrap();
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(Store::default(), tx);
    app.screen = Screen::Browser;
    app.client = Some(client);
    app.on_key(KeyEvent::from(KeyCode::Char('W')));
    let Some(Modal::Confirm { message, action }) = &app.modal else {
        panic!("expected a confirmation")
    };
    assert!(matches!(action, Action::Monitor));
    assert!(message.contains("throughput"), "{message}");
    app.on_key(KeyEvent::from(KeyCode::Char('n')));
    assert!(app.modal.is_none(), "declining starts nothing");
    app.on_key(KeyEvent::from(KeyCode::Char('W')));
    app.on_key(KeyEvent::from(KeyCode::Char('y')));
    assert!(matches!(app.modal, Some(Modal::PubSub(ref s)) if s.monitor));
    app.on_key(KeyEvent::from(KeyCode::Esc));
}

#[tokio::test]
async fn a_feed_replaced_by_another_dialog_stops_its_monitor() {
    let Some(conn) = conn(Environment::Development) else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let client = Client::connect(conn).await.unwrap();
    let before = monitors(&client).await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(Store::default(), tx);
    app.screen = Screen::Browser;
    app.client = Some(client.clone());
    app.on_key(KeyEvent::from(KeyCode::Char('W')));
    let started = tokio::time::Instant::now();
    while monitors(&client).await == before {
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "MONITOR never started"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // A Lua reply arriving now replaces the feed with its message box.
    app.on_msg(Msg::Script(Ok("done".into())));
    assert!(!matches!(app.modal, Some(Modal::PubSub(_))));
    let started = tokio::time::Instant::now();
    while monitors(&client).await != before {
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "the MONITOR connection outlived its feed"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn the_filter_sees_arguments_past_the_part_the_feed_keeps() {
    let Some(conn) = conn(Environment::Development) else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let client = Client::connect(conn).await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(Store::default(), tx);
    app.screen = Screen::Browser;
    app.client = Some(client.clone());
    // MONITOR sees every client of the server, so the filter names this run's
    // own key and needle: no other test, and no earlier run, can match it.
    let run = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let key = format!("monitor:far:{run}");
    let needle = format!("deepneedle{run}");
    app.on_key(KeyEvent::from(KeyCode::Char('W')));
    app.on_key(KeyEvent::from(KeyCode::Char('s')));
    // One glob: the filter splits on spaces.
    for c in format!("*{key}*{needle}*").chars() {
        app.on_key(KeyEvent::from(KeyCode::Char(c)));
    }
    app.on_key(KeyEvent::from(KeyCode::Enter));
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let far = format!(
        "{}{needle}",
        "x".repeat(3 * rediscope::redis_client::MONITOR_DETAIL_LIMIT)
    );
    let mut raw = redis::Client::open(format!(
        "redis://127.0.0.1:{}",
        std::env::var("REDISCOPE_TEST_PORT").unwrap()
    ))
    .unwrap()
    .get_multiplexed_async_connection()
    .await
    .unwrap();
    let _: () = redis::cmd("SET")
        .arg(&key)
        .arg(&far)
        .query_async(&mut raw)
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if let Ok(Some(msg)) =
            tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await
        {
            app.on_msg(msg);
        }
        let Some(Modal::PubSub(state)) = &app.modal else {
            panic!("the feed closed")
        };
        if state.total >= 1 || tokio::time::Instant::now() > deadline {
            break;
        }
    }
    let Some(Modal::PubSub(state)) = &app.modal else {
        panic!("the feed closed")
    };
    assert_eq!(state.total, 1, "the match past the cut was kept");
    let kept = &state.messages[0].payload;
    assert!(
        kept.ends_with("more bytes"),
        "and stored cut: {} bytes",
        kept.len()
    );
    app.on_key(KeyEvent::from(KeyCode::Esc));
    let _: () = redis::cmd("DEL")
        .arg(&key)
        .query_async(&mut raw)
        .await
        .unwrap();
}

#[tokio::test]
async fn d_narrows_the_feed_to_one_database() {
    let Some(conn) = conn(Environment::Development) else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let client = Client::connect(conn.clone()).await.unwrap();
    // Database 4 holds only keys other suites clean up by prefix: 6 belongs
    // to the memory scan, which flushes it and counts every key it finds.
    let elsewhere = Client::connect(Connection { db: 4, ..conn }).await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(Store::default(), tx);
    app.screen = Screen::Browser;
    app.client = Some(client.clone());
    app.on_key(KeyEvent::from(KeyCode::Char('W')));
    app.on_key(KeyEvent::from(KeyCode::Char('s')));
    for c in "mondb:".chars() {
        app.on_key(KeyEvent::from(KeyCode::Char(c)));
    }
    app.on_key(KeyEvent::from(KeyCode::Enter));
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    for i in 0..3 {
        client
            .execute_raw(&format!("SET mondb:zero:{i} x"))
            .await
            .unwrap();
        elsewhere
            .execute_raw(&format!("SET mondb:six:{i} x"))
            .await
            .unwrap();
    }
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if let Ok(Some(msg)) =
            tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await
        {
            app.on_msg(msg);
        }
        let Some(Modal::PubSub(state)) = &app.modal else {
            panic!("the feed closed")
        };
        if state.total >= 6 || tokio::time::Instant::now() > deadline {
            break;
        }
    }
    let payloads = |app: &App| -> Vec<String> {
        let Some(Modal::PubSub(state)) = &app.modal else {
            panic!("the feed closed")
        };
        state.shown().iter().map(|m| m.payload.clone()).collect()
    };
    assert_eq!(payloads(&app).len(), 6, "every database to start with");

    // This profile's own database first.
    app.on_key(KeyEvent::from(KeyCode::Char('d')));
    let zero = payloads(&app);
    assert_eq!(zero.len(), 3, "{zero:?}");
    assert!(
        zero.iter()
            .all(|p| p.starts_with("db0 ") && p.contains("mondb:zero"))
    );

    // Then the other database the feed has seen.
    app.on_key(KeyEvent::from(KeyCode::Char('d')));
    let six = payloads(&app);
    assert_eq!(six.len(), 3, "{six:?}");
    assert!(
        six.iter()
            .all(|p| p.starts_with("db4 ") && p.contains("mondb:six"))
    );

    // A new text filter keeps the database it was showing.
    app.on_key(KeyEvent::from(KeyCode::Char('s')));
    app.on_key(KeyEvent::from(KeyCode::Enter));
    let Some(Modal::PubSub(state)) = &app.modal else {
        panic!("the feed closed")
    };
    assert_eq!(state.db_filter, Some(4));

    app.on_key(KeyEvent::from(KeyCode::Esc));
    let (mut zero_keys, mut six_keys) = (String::from("DEL"), String::from("DEL"));
    for i in 0..3 {
        zero_keys.push_str(&format!(" mondb:zero:{i}"));
        six_keys.push_str(&format!(" mondb:six:{i}"));
    }
    client.execute_raw(&zero_keys).await.unwrap();
    elsewhere.execute_raw(&six_keys).await.unwrap();
}

// ---- parsing and the database filter, without a server ------------------------

use rediscope::app::PubSubState;
use rediscope::redis_client::MonitorLine;

#[test]
fn monitor_lines_from_every_kind_of_client_name_their_database() {
    for (line, command, db, detail) in [
        (
            r#"1700000000.123456 [0 127.0.0.1:1234] "SET" "k" "v""#,
            "SET",
            Some(0),
            r#"db0 127.0.0.1:1234  "k" "v""#,
        ),
        (
            r#"1700000000.123456 [0 lua] "set" "k""#,
            "SET",
            Some(0),
            r#"db0 lua  "k""#,
        ),
        (
            r#"1700000000.123456 [0 unix:/tmp/r.sock] "GET" "k""#,
            "GET",
            Some(0),
            r#"db0 unix:/tmp/r.sock  "k""#,
        ),
        (
            r#"1700000000.123456 [7 unix:/tmp/my dir/r.sock] "GET" "k""#,
            "GET",
            Some(7),
            r#"db7 unix:/tmp/my dir/r.sock  "k""#,
        ),
        (
            r#"1700000000.123456 [3 [::1]:52100] "HGET" "h" "f""#,
            "HGET",
            Some(3),
            r#"db3 [::1]:52100  "h" "f""#,
        ),
        (
            r#"1700000000.123456 [15 10.0.0.1:6000] "PING""#,
            "PING",
            Some(15),
            "db15 10.0.0.1:6000",
        ),
        (
            r#"1700000000.123456 [1000 10.0.0.1:6000] "get" "k""#,
            "GET",
            Some(1000),
            r#"db1000 10.0.0.1:6000  "k""#,
        ),
        // An argument holding the characters that end the source part.
        (
            r#"1.0 [2 10.0.0.1:6000] "SET" "a] [b" "\"q\"""#,
            "SET",
            Some(2),
            r#"db2 10.0.0.1:6000  "a] [b" "\"q\"""#,
        ),
        (
            r#"1.0 [x 10.0.0.1:6000] "GET" "k""#,
            "GET",
            None,
            r#"dbx 10.0.0.1:6000  "k""#,
        ),
    ] {
        let parsed = parse_monitor_line(line).unwrap_or_else(|| panic!("{line}"));
        assert_eq!(parsed.command, command, "{line}");
        assert_eq!(parsed.db, db, "{line}");
        assert_eq!(parsed.detail, detail, "{line}");
    }
}

#[test]
fn malformed_monitor_lines_are_skipped_without_panicking() {
    for line in [
        "",
        "OK",
        "1700000000.1",
        "1700000000.1 ",
        "1700000000.1 [0 127.0.0.1:1]",
        "1700000000.1 [0 127.0.0.1:1] ",
        r#"1700000000.1 [0127.0.0.1:1] "GET""#,
        "1700000000.1 [0 127.0.0.1:1] GET k",
        r#"1700000000.1 [0 127.0.0.1:1] "GET"#,
        r#"[0 127.0.0.1:1] "GET" "k""#,
        r#"1700000000.1 0 127.0.0.1:1] "GET""#,
    ] {
        assert!(parse_monitor_line(line).is_none(), "{line:?}");
    }
    // Odd but readable: never a panic on multibyte text.
    for line in [
        r#"1.0 [0 127.0.0.1:1] "ГЕТ" "ключ""#,
        "1.0 [0 127.0.0.1:1] \"\u{1F600}\"",
        r#"1.0 [٣ 127.0.0.1:1] "GET""#,
    ] {
        let parsed = parse_monitor_line(line).unwrap();
        assert!(parsed.db.is_none() || parsed.db == Some(0), "{line}");
    }
    let long = format!(r#"1.0 [4 127.0.0.1:1] "SET" "k" "{}""#, "ж".repeat(10_000));
    let short = parse_monitor_line(&long).unwrap().shortened();
    assert_eq!(short.db, Some(4));
    assert!(short.detail.len() < long.len());
}

fn line(db: Option<i64>, text: &str) -> MonitorLine {
    MonitorLine {
        command: "GET".into(),
        detail: format!("db{} 1.2.3.4:5  \"{text}\"", db.unwrap_or(-1)),
        db,
    }
}

#[test]
fn d_with_no_other_database_seen_toggles_between_all_and_this_one() {
    let mut state = PubSubState::monitor(vec![]);
    state.current_db = 4;
    // Nothing seen at all yet.
    let mut filters = Vec::new();
    for _ in 0..4 {
        state.cycle_db();
        filters.push(state.db_filter);
    }
    assert_eq!(filters, [Some(4), None, Some(4), None]);

    // Only this database seen.
    state.push_command(line(Some(4), "a"));
    state.push_command(line(Some(4), "b"));
    assert_eq!(state.dbs, [4]);
    state.cycle_db();
    assert_eq!(state.db_filter, Some(4));
    assert_eq!(state.shown().len(), 2);
    state.cycle_db();
    assert_eq!(state.db_filter, None);
}

#[test]
fn commands_with_no_database_only_show_unfiltered() {
    let mut state = PubSubState::monitor(vec![]);
    state.push_command(line(None, "mystery"));
    state.push_command(line(Some(0), "zero"));
    assert_eq!(state.dbs, [0], "an unknown database is not offered");
    assert_eq!(state.shown().len(), 2);
    state.cycle_db();
    let shown: Vec<&str> = state.shown().iter().map(|m| m.payload.as_str()).collect();
    assert_eq!(shown.len(), 1);
    assert!(shown[0].contains("zero"));
}

#[test]
fn a_filter_on_a_database_that_went_quiet_lists_nothing_and_still_draws() {
    common::isolate_config();
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(Store::default(), tx);
    app.screen = Screen::Browser;
    let mut state = PubSubState::monitor(vec![]);
    state.current_db = 9;
    for i in 0..30 {
        state.push_command(line(Some(i % 3), &format!("cmd{i}")));
    }
    app.modal = Some(Modal::PubSub(state));
    app.on_key(KeyEvent::from(KeyCode::Char('d')));
    let Some(Modal::PubSub(state)) = &app.modal else {
        panic!("feed closed")
    };
    assert_eq!(state.db_filter, Some(9));
    assert!(state.shown().is_empty());
    assert_eq!(state.scroll, 0);
    for (w, h) in [(140, 40), (80, 24), (40, 12), (20, 8), (10, 5)] {
        let mut terminal =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(w, h)).unwrap();
        terminal.draw(|f| rediscope::ui::draw(f, &mut app)).unwrap();
    }
    // Moving about an empty list is harmless.
    for code in [
        KeyCode::Down,
        KeyCode::Up,
        KeyCode::PageDown,
        KeyCode::PageUp,
        KeyCode::End,
        KeyCode::Home,
        KeyCode::Char('f'),
        KeyCode::Char('f'),
    ] {
        app.on_key(KeyEvent::from(code));
    }
    let Some(Modal::PubSub(state)) = &app.modal else {
        panic!("feed closed")
    };
    assert_eq!(state.scroll, 0);
}

#[test]
fn moving_through_a_filtered_feed_stays_within_what_is_listed() {
    common::isolate_config();
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(Store::default(), tx);
    app.screen = Screen::Browser;
    let mut state = PubSubState::monitor(vec![]);
    for i in 0..40 {
        state.push_command(line(Some(if i % 4 == 0 { 1 } else { 0 }), &format!("c{i}")));
    }
    state.cycle_db(); // db0: 30 listed
    state.cycle_db(); // db1: 10 listed
    assert_eq!(state.db_filter, Some(1));
    app.modal = Some(Modal::PubSub(state));
    let scroll = |app: &App| match &app.modal {
        Some(Modal::PubSub(state)) => (state.scroll, state.shown().len()),
        _ => panic!("feed closed"),
    };
    assert_eq!(scroll(&app), (9, 10), "following the newest listed");
    app.on_key(KeyEvent::from(KeyCode::PageDown));
    assert_eq!(scroll(&app).0, 9);
    app.on_key(KeyEvent::from(KeyCode::Home));
    app.on_key(KeyEvent::from(KeyCode::PageDown));
    assert_eq!(scroll(&app).0, 9, "clamped to the ten listed");
    app.on_key(KeyEvent::from(KeyCode::Up));
    app.on_key(KeyEvent::from(KeyCode::Up));
    assert_eq!(scroll(&app).0, 7);
    // More commands for another database do not move a cursor that stopped.
    if let Some(Modal::PubSub(state)) = &mut app.modal {
        for _ in 0..50 {
            state.push_command(line(Some(0), "noise"));
        }
    }
    assert_eq!(scroll(&app), (7, 10));
    app.on_key(KeyEvent::from(KeyCode::End));
    assert_eq!(scroll(&app).0, 9);
    let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(140, 30)).unwrap();
    terminal.draw(|f| rediscope::ui::draw(f, &mut app)).unwrap();
    let text: String = terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect();
    assert!(text.contains("db1 only"), "{text}");
    assert!(text.contains("10 shown"), "{text}");
    assert!(text.contains("90 total"), "{text}");
    assert!(!text.contains("noise"), "{text}");
}

// ---- the listed count across everything that changes it ------------------------

use rediscope::app::PUBSUB_LIMIT;

fn feed_app() -> (App, tokio::sync::mpsc::UnboundedReceiver<Msg>) {
    common::isolate_config();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(Store::default(), tx);
    app.screen = Screen::Browser;
    (app, rx)
}

fn feed(app: &App) -> &PubSubState {
    match &app.modal {
        Some(Modal::PubSub(state)) => state,
        _ => panic!("the feed is not open: {}", app.status),
    }
}

/// The cached count matches a recount, the cursor is on a listed message
/// (the newest while following), and the feed draws small and large.
fn check_feed(app: &mut App, when: &str) {
    let state = feed(app);
    let listed = state.shown().len();
    assert_eq!(state.shown_len(), listed, "{when}");
    assert!(
        state.scroll < listed.max(1),
        "{when}: scroll {} of {listed}",
        state.scroll
    );
    if state.follow {
        assert_eq!(state.scroll, listed.saturating_sub(1), "{when}");
    }
    for (w, h) in [(10, 5), (80, 24)] {
        let mut terminal =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(w, h)).unwrap();
        terminal
            .draw(|f| rediscope::ui::draw(f, app))
            .unwrap_or_else(|e| panic!("{when}: {e}"));
    }
}

fn batch(app: &mut App, from: usize, count: usize, dbs: i64) {
    let lines = (from..from + count)
        .map(|i| line(Some(i as i64 % dbs), &format!("c{i}")))
        .collect();
    app.on_msg(Msg::MonitorBatch { lines, dropped: 3 });
}

#[test]
fn the_monitor_count_holds_through_overflow_d_clear_and_pause() {
    let (mut app, _rx) = feed_app();
    let mut state = PubSubState::monitor(vec![]);
    state.current_db = 1;
    app.modal = Some(Modal::PubSub(state));
    check_feed(&mut app, "empty");

    let mut next = 0;
    let mut push = |app: &mut App, count: usize, when: &str| {
        batch(app, next, count, 3);
        next += count;
        check_feed(app, when);
    };
    push(&mut app, 120, "a first batch");
    app.on_key(KeyEvent::from(KeyCode::Char('d')));
    check_feed(&mut app, "d: db1");
    assert_eq!(feed(&app).db_filter, Some(1));

    // Past the buffer while filtered, following.
    for round in 0..6 {
        push(&mut app, 500, &format!("filtered batch {round}"));
    }
    assert_eq!(feed(&app).messages.len(), PUBSUB_LIMIT);

    // Paused at the bottom: pushes past the cap drop old listed commands and
    // the cursor stays within the list.
    app.on_key(KeyEvent::from(KeyCode::End));
    app.on_key(KeyEvent::from(KeyCode::Char('f')));
    assert!(!feed(&app).follow);
    check_feed(&mut app, "paused at the bottom");
    for round in 0..3 {
        push(&mut app, 500, &format!("paused batch {round}"));
    }
    for code in ['d', 'd', 'd', 'd'] {
        app.on_key(KeyEvent::from(KeyCode::Char(code)));
        let when = format!("d while paused, now {:?}", feed(&app).db_filter);
        check_feed(&mut app, &when);
        push(&mut app, 250, &format!("{when}, then a push"));
    }
    app.on_key(KeyEvent::from(KeyCode::Char('f')));
    check_feed(&mut app, "following again");

    app.on_key(KeyEvent::from(KeyCode::Char('c')));
    check_feed(&mut app, "cleared");
    assert_eq!(feed(&app).shown_len(), 0);
    push(&mut app, 7, "a push after clearing");

    // Cursor at the bottom, then everything at once.
    app.on_key(KeyEvent::from(KeyCode::End));
    for i in 0..(PUBSUB_LIMIT * 2) / 500 {
        app.on_key(KeyEvent::from(KeyCode::Char('d')));
        push(&mut app, 500, &format!("d and a full batch {i}"));
        if i % 2 == 0 {
            app.on_key(KeyEvent::from(KeyCode::Char('f')));
            check_feed(&mut app, &format!("pause toggled {i}"));
        }
    }
    app.on_key(KeyEvent::from(KeyCode::Char('c')));
    app.on_key(KeyEvent::from(KeyCode::Char('d')));
    check_feed(&mut app, "cleared, then d");
    push(&mut app, 1, "one more");
}

/// `s` closes the feed for its form; the monitor it starts is a new feed
/// with its own count, and clearing the filter starts another.
#[tokio::test]
async fn setting_and_clearing_the_monitor_filter_keeps_the_count() {
    let Some(conn) = conn(Environment::Development) else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let client = Client::connect(conn).await.unwrap();
    let (mut app, _rx) = feed_app();
    app.client = Some(client);

    app.on_key(KeyEvent::from(KeyCode::Char('W')));
    check_feed(&mut app, "opened");
    batch(&mut app, 0, PUBSUB_LIMIT + 300, 2);
    check_feed(&mut app, "past the cap");
    app.on_key(KeyEvent::from(KeyCode::Char('d')));
    check_feed(&mut app, "d");
    let db = feed(&app).db_filter;
    assert_eq!(db, Some(0));

    app.on_key(KeyEvent::from(KeyCode::Char('s')));
    assert!(matches!(app.modal, Some(Modal::Form { .. })));
    // A batch arriving while the form is open goes nowhere and panics nothing.
    batch(&mut app, 0, 10, 2);
    for c in "get*".chars() {
        app.on_key(KeyEvent::from(KeyCode::Char(c)));
    }
    app.on_key(KeyEvent::from(KeyCode::Enter));
    check_feed(&mut app, "filter set");
    assert_eq!(feed(&app).patterns, ["get*"]);
    assert_eq!(feed(&app).db_filter, db, "the database filter carries over");
    batch(&mut app, 0, 900, 2);
    check_feed(&mut app, "filtered pushes");
    app.on_key(KeyEvent::from(KeyCode::End));
    app.on_key(KeyEvent::from(KeyCode::Char('f')));
    batch(&mut app, 900, PUBSUB_LIMIT, 2);
    check_feed(&mut app, "paused past the cap");

    app.on_key(KeyEvent::from(KeyCode::Char('s')));
    for _ in 0.."get*".len() {
        app.on_key(KeyEvent::from(KeyCode::Backspace));
    }
    app.on_key(KeyEvent::from(KeyCode::Enter));
    check_feed(&mut app, "filter cleared");
    assert!(feed(&app).patterns.is_empty());
    batch(&mut app, 0, 50, 2);
    check_feed(&mut app, "a push after clearing the filter");
    app.on_key(KeyEvent::from(KeyCode::Char('c')));
    check_feed(&mut app, "cleared");
    app.on_key(KeyEvent::from(KeyCode::Esc));
    assert!(app.modal.is_none());
}

/// The pub/sub and keyspace feeds share the state: they never filter by
/// database, so everything they receive is listed, including what arrives
/// while the feed is set aside for the publish form.
#[test]
fn pubsub_and_keyspace_feeds_count_every_message() {
    for (patterns, keyspace) in [
        (vec!["news.*".to_string()], false),
        (vec!["__keyevent@0__:*".to_string()], true),
    ] {
        let (mut app, _rx) = feed_app();
        app.modal = Some(Modal::PubSub(PubSubState::new(patterns, keyspace)));
        let what = if keyspace { "keyspace" } else { "pubsub" };
        check_feed(&mut app, what);
        let send = |app: &mut App, from: usize, count: usize| {
            for i in from..from + count {
                app.on_msg(Msg::PubSub {
                    channel: format!("news.{}", i % 5),
                    payload: format!("m{i} ключ 🙂"),
                });
            }
        };
        send(&mut app, 0, PUBSUB_LIMIT + 123);
        check_feed(&mut app, &format!("{what}: past the cap"));
        assert_eq!(feed(&app).shown_len(), PUBSUB_LIMIT);

        // `d` belongs to the monitor only.
        app.on_key(KeyEvent::from(KeyCode::Char('d')));
        assert_eq!(feed(&app).db_filter, None, "{what}");
        check_feed(&mut app, &format!("{what}: d does nothing"));

        app.on_key(KeyEvent::from(KeyCode::End));
        app.on_key(KeyEvent::from(KeyCode::Char('f')));
        send(&mut app, 0, 40);
        check_feed(&mut app, &format!("{what}: paused"));

        // Held behind the publish form, still counting.
        app.on_key(KeyEvent::from(KeyCode::Char('w')));
        assert!(matches!(app.modal, Some(Modal::Form { .. })), "{what}");
        send(&mut app, 0, 500);
        assert_eq!(
            app.held_feed.as_ref().map(|f| f.shown_len()),
            app.held_feed.as_ref().map(|f| f.shown().len()),
            "{what}"
        );
        app.on_key(KeyEvent::from(KeyCode::Esc));
        check_feed(&mut app, &format!("{what}: back from the form"));
        assert_eq!(feed(&app).shown_len(), PUBSUB_LIMIT);

        app.on_key(KeyEvent::from(KeyCode::Char('c')));
        check_feed(&mut app, &format!("{what}: cleared"));
        send(&mut app, 0, 3);
        check_feed(&mut app, &format!("{what}: after clearing"));
        assert_eq!(feed(&app).shown_len(), 3);
    }
}
