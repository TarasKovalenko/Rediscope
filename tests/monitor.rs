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
    client
        .execute_raw("CLIENT LIST")
        .await
        .unwrap()
        .lines()
        .filter(|l| l.contains("cmd=monitor"))
        .count()
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
    app.on_key(KeyEvent::from(KeyCode::Char('W')));
    app.on_key(KeyEvent::from(KeyCode::Char('s')));
    for c in "deepneedle".chars() {
        app.on_key(KeyEvent::from(KeyCode::Char(c)));
    }
    app.on_key(KeyEvent::from(KeyCode::Enter));
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let far = format!(
        "{}deepneedle",
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
        .arg("monitor:far")
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
        .arg("monitor:far")
        .query_async(&mut raw)
        .await
        .unwrap();
}
