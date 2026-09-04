//! End-to-end checks against a real redis-server.
//!
//! Skipped unless REDISCOPE_TEST_PORT is set:
//!   redis-server --port 7799 --daemonize yes
//!   REDISCOPE_TEST_PORT=7799 cargo test --test integration

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use rediscope::app::{App, Modal, Msg};
use rediscope::config::Connection;
use rediscope::config::Store;
use rediscope::memory::Rollup;
use rediscope::redis_client::{Client, KeyType, KeyValue, MemoryScan};

/// Each test owns a database index so the suite can run in parallel.
fn conn(db: i64) -> Option<Connection> {
    let port: u16 = std::env::var("REDISCOPE_TEST_PORT").ok()?.parse().ok()?;
    Some(Connection {
        name: "test".into(),
        host: "127.0.0.1".into(),
        port,
        db,
        ..Default::default()
    })
}

macro_rules! client {
    ($db:expr_2021) => {
        match conn($db) {
            Some(c) => Client::connect(c).await.expect("connect"),
            None => return,
        }
    };
}

#[tokio::test]
async fn scans_types_and_reads_every_value_shape() {
    let c = client!(9);
    c.execute_raw("FLUSHDB").await.unwrap();
    c.create_key("app:user:1", KeyType::String).await.unwrap();
    c.set_string("app:user:1", r#"{"id":1}"#).await.unwrap();
    c.create_key("app:user:2", KeyType::Hash).await.unwrap();
    c.create_key("app:queue", KeyType::List).await.unwrap();
    c.create_key("tags", KeyType::Set).await.unwrap();
    c.create_key("scores", KeyType::ZSet).await.unwrap();
    c.create_key("events", KeyType::Stream).await.unwrap();

    let (keys, truncated) = c.scan_keys("*", 5000).await.unwrap();
    assert!(!truncated);
    assert_eq!(keys.len(), 6);
    let kind = |n: &str| keys.iter().find(|k| k.name == n).unwrap().kind;
    assert_eq!(kind("app:user:1"), KeyType::String);
    assert_eq!(kind("app:user:2"), KeyType::Hash);
    assert_eq!(kind("app:queue"), KeyType::List);
    assert_eq!(kind("tags"), KeyType::Set);
    assert_eq!(kind("scores"), KeyType::ZSet);
    assert_eq!(kind("events"), KeyType::Stream);

    for k in &keys {
        let v = c.read_value(&k.name, k.kind).await.unwrap();
        match v {
            KeyValue::Str(s) => assert_eq!(s, r#"{"id":1}"#),
            KeyValue::Rows { rows, total, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(total, 1);
            }
            KeyValue::Unsupported(_) => panic!("unexpected type for {}", k.name),
        }
    }

    let (filtered, _) = c.scan_keys("app:*", 5000).await.unwrap();
    assert_eq!(filtered.len(), 3);
}

#[tokio::test]
async fn mutates_elements_of_each_collection_type() {
    let c = client!(10);
    c.execute_raw("FLUSHDB").await.unwrap();

    c.hash_set("h", "a", "1").await.unwrap();
    c.hash_set("h", "b", "2").await.unwrap();
    c.hash_del("h", "a").await.unwrap();
    let KeyValue::Rows { rows, .. } = c.read_value("h", KeyType::Hash).await.unwrap() else {
        panic!()
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cells, vec!["b", "2"]);

    for v in ["x", "y", "z"] {
        c.list_push("l", v).await.unwrap();
    }
    c.list_set("l", 1, "Y").await.unwrap();
    // Deleting by index is a sentinel swap-and-trim; make sure it removes
    // exactly the requested slot.
    c.list_remove_at("l", 0).await.unwrap();
    let KeyValue::Rows { rows, total, .. } = c.read_value("l", KeyType::List).await.unwrap() else {
        panic!()
    };
    assert_eq!(total, 2);
    assert_eq!(
        rows.iter().map(|r| r.cells[1].clone()).collect::<Vec<_>>(),
        vec!["Y", "z"]
    );

    c.set_add("s", "one").await.unwrap();
    c.set_add("s", "two").await.unwrap();
    c.set_remove("s", "one").await.unwrap();
    let KeyValue::Rows { rows, .. } = c.read_value("s", KeyType::Set).await.unwrap() else {
        panic!()
    };
    assert_eq!(rows[0].id, "two");

    c.zset_add("z", "m", 1.5).await.unwrap();
    let KeyValue::Rows { rows, .. } = c.read_value("z", KeyType::ZSet).await.unwrap() else {
        panic!()
    };
    assert_eq!(rows[0].cells, vec!["m", "1.5"]);

    c.stream_add("st", "f", "v").await.unwrap();
    let KeyValue::Rows { rows, .. } = c.read_value("st", KeyType::Stream).await.unwrap() else {
        panic!()
    };
    let id = rows[0].id.clone();
    c.stream_delete("st", &id).await.unwrap();
    let KeyValue::Rows { total, .. } = c.read_value("st", KeyType::Stream).await.unwrap() else {
        panic!()
    };
    assert_eq!(total, 0);
}

#[tokio::test]
async fn ttl_rename_and_delete_round_trip() {
    let c = client!(11);
    c.execute_raw("FLUSHDB").await.unwrap();
    c.set_string("k", "v").await.unwrap();

    assert_eq!(c.key_info("k").await.unwrap().ttl, -1);
    c.set_ttl("k", Some(120)).await.unwrap();
    assert!(c.key_info("k").await.unwrap().ttl > 100);
    c.set_ttl("k", None).await.unwrap();
    assert_eq!(c.key_info("k").await.unwrap().ttl, -1);

    c.rename_key("k", "k2").await.unwrap();
    assert_eq!(c.key_info("k").await.unwrap().kind, KeyType::Other);
    assert_eq!(c.key_info("k2").await.unwrap().kind, KeyType::String);

    c.delete_key("k2").await.unwrap();
    assert_eq!(c.dbsize().await.unwrap(), 0);
}

#[tokio::test]
async fn value_reads_are_bounded_on_large_collections() {
    let c = client!(12);
    c.execute_raw("FLUSHDB").await.unwrap();
    let mut args = String::from("RPUSH big");
    for i in 0..2500 {
        args.push_str(&format!(" item{i}"));
    }
    c.execute_raw(&args).await.unwrap();

    let KeyValue::Rows { rows, total, .. } = c.read_value("big", KeyType::List).await.unwrap()
    else {
        panic!()
    };
    assert_eq!(total, 2500, "the true length is still reported");
    assert_eq!(rows.len(), 1000, "but only a bounded window is fetched");
}

#[tokio::test]
async fn raw_console_formats_replies() {
    let c = client!(13);
    c.execute_raw("FLUSHDB").await.unwrap();
    assert_eq!(c.execute_raw("SET greet hello").await.unwrap(), "OK");
    assert_eq!(c.execute_raw("GET greet").await.unwrap(), "hello");
    assert_eq!(c.execute_raw("GET missing").await.unwrap(), "(nil)");
    assert_eq!(c.execute_raw("DBSIZE").await.unwrap(), "(integer) 1");
    let out = c.execute_raw(r#"SET "spaced key" "a b""#).await.unwrap();
    assert_eq!(out, "OK");
    assert_eq!(c.execute_raw(r#"GET "spaced key""#).await.unwrap(), "a b");
    assert!(c.execute_raw("NOTACOMMAND").await.is_err());
}

#[tokio::test]
async fn info_reports_sections_and_key_counts() {
    let c = client!(14);
    c.execute_raw("FLUSHDB").await.unwrap();
    c.execute_raw("SET info:probe 1").await.unwrap();

    let info = c.info().await.unwrap();
    assert!(info.field("redis_version").is_some());
    assert!(!info.section("Memory").is_empty());
    assert!(!info.section("Stats").is_empty());
    let (_, keys, _) = info
        .keyspace()
        .into_iter()
        .find(|(db, ..)| db == "db14")
        .expect("db14 in the keyspace section");
    assert!(keys >= 1);
}

#[tokio::test]
async fn a_json_string_round_trips_through_the_editor_shape() {
    let c = client!(15);
    c.execute_raw("FLUSHDB").await.unwrap();
    c.set_string("doc", r#"{"b":2,"a":[1,2]}"#).await.unwrap();

    let KeyValue::Str(stored) = c.read_value("doc", KeyType::String).await.unwrap() else {
        panic!()
    };
    assert_eq!(
        rediscope::json::mode(&stored),
        rediscope::json::JsonMode::Compact
    );

    // What the editor shows, and what it writes back for a compact document.
    let shown = rediscope::json::pretty(&stored);
    assert!(shown.contains("\n  \"b\": 2"));
    c.set_string("doc", &rediscope::json::minify(&shown))
        .await
        .unwrap();

    let KeyValue::Str(after) = c.read_value("doc", KeyType::String).await.unwrap() else {
        panic!()
    };
    assert_eq!(after, stored, "the stored shape survives an edit");
}

#[tokio::test]
async fn the_command_list_is_read_for_console_completion() {
    let c = client!(8);
    let table = c.command_names().await.unwrap();
    let names = table.names;
    assert!(
        names.len() > 100,
        "a server knows more than {}",
        names.len()
    );
    assert!(names.iter().any(|n| n == "GET"), "uppercased for display");
    assert!(names.iter().any(|n| n == "DBSIZE"));
    assert!(
        table.writes.contains("SET"),
        "the server flags SET as a write"
    );
    assert!(!table.writes.contains("GET"), "GET is a read");
    assert!(
        names.windows(2).all(|w| w[0] <= w[1]),
        "sorted for completion"
    );
}

#[tokio::test]
async fn the_memory_scan_finds_the_prefix_holding_the_most() {
    let c = client!(7);
    c.execute_raw("FLUSHDB").await.unwrap();
    for i in 0..50 {
        c.set_string(&format!("big:{i}"), &"x".repeat(400))
            .await
            .unwrap();
    }
    for i in 0..10 {
        c.set_string(&format!("small:{i}"), "x").await.unwrap();
    }

    let mut scan = MemoryScan::default();
    let mut rollup = Rollup::default();
    // stride 1: measure every key, so the totals are exact rather than sampled.
    while !c.memory_batch(&mut scan, 1, &mut rollup).await.unwrap() {}

    assert_eq!(rollup.scanned(), 60);
    assert_eq!(rollup.sampled(), 60);
    let rows = rollup.rows(1);
    assert_eq!(rows[0].prefix, "big:");
    assert_eq!(rows[0].keys, 50);
    assert!(rows[0].share > 90.0, "{:?}", rows[0]);
    assert!(rows[0].est_bytes > 50 * 400, "{:?}", rows[0]);
}

#[tokio::test]
async fn sampling_one_key_in_ten_still_lands_near_the_real_size() {
    let c = client!(6);
    c.execute_raw("FLUSHDB").await.unwrap();
    for i in 0..500 {
        c.set_string(&format!("app:{i}"), &"x".repeat(300))
            .await
            .unwrap();
    }

    let mut exact = Rollup::default();
    let mut scan = MemoryScan::default();
    while !c.memory_batch(&mut scan, 1, &mut exact).await.unwrap() {}

    let mut sampled = Rollup::default();
    let mut scan = MemoryScan::default();
    while !c.memory_batch(&mut scan, 10, &mut sampled).await.unwrap() {}

    assert_eq!(sampled.scanned(), 500);
    assert!(sampled.sampled() <= 60, "{} measured", sampled.sampled());
    let error = (sampled.total_bytes() as f64 - exact.total_bytes() as f64).abs()
        / exact.total_bytes() as f64;
    assert!(error < 0.05, "off by {:.1}%", error * 100.0);
}

/// The report end to end: the key press starts the scan, the messages it sends
/// land in the modal, and the table ends up ranked.
#[tokio::test]
async fn pressing_m_fills_the_namespace_report_from_a_live_server() {
    let c = client!(5);
    c.execute_raw("FLUSHDB").await.unwrap();
    for i in 0..30 {
        c.set_string(&format!("big:{i}"), &"x".repeat(500))
            .await
            .unwrap();
    }
    for i in 0..5 {
        c.set_string(&format!("tiny:{i}"), "x").await.unwrap();
    }

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(Store::default(), tx);
    app.on_msg(Msg::Connected(Box::new(Ok(c))));
    app.dbsize = 35;
    app.on_key(KeyEvent::new(KeyCode::Char('M'), KeyModifiers::NONE));

    let finish = async {
        while let Some(msg) = rx.recv().await {
            app.on_msg(msg);
            if let Some(Modal::Memory(state)) = &app.modal
                && !state.running
            {
                return;
            }
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), finish)
        .await
        .expect("the scan finishes");

    let Some(Modal::Memory(state)) = &app.modal else {
        panic!("the report closed itself")
    };
    assert_eq!(state.rollup.scanned(), 35);
    let rows = state.rows();
    assert_eq!(rows[0].prefix, "big:");
    assert_eq!(rows[0].keys, 30);
}

// ---- bulk operations, copy and search ------------------------------------
//
// The databases up to 15 are taken by the tests above, so these share the low
// ones and clean up after themselves by prefix rather than with FLUSHDB.

/// Remove every key under a prefix, so a shared database stays usable.
async fn clear_prefix(c: &Client, prefix: &str) {
    let (keys, _) = c.scan_keys(&format!("{prefix}*"), 5000).await.unwrap();
    let names: Vec<String> = keys.into_iter().map(|k| k.name).collect();
    if !names.is_empty() {
        c.delete_keys(&names).await.unwrap();
    }
}

#[tokio::test]
async fn bulk_delete_and_expire_cover_every_named_key() {
    let c = client!(0);
    clear_prefix(&c, "bulk:").await;
    let names: Vec<String> = (0..5).map(|i| format!("bulk:{i}")).collect();
    for name in &names {
        c.set_string(name, "v").await.unwrap();
    }

    assert_eq!(c.expire_keys(&names, Some(600)).await.unwrap(), 5);
    assert_eq!(c.key_info("bulk:0").await.unwrap().ttl, 600);
    assert_eq!(c.expire_keys(&names, None).await.unwrap(), 5);
    assert_eq!(c.key_info("bulk:0").await.unwrap().ttl, -1);

    assert_eq!(c.delete_keys(&names).await.unwrap(), 5);
    assert_eq!(c.key_info("bulk:0").await.unwrap().kind, KeyType::Other);
}

#[tokio::test]
async fn copying_a_key_carries_its_type_and_ttl() {
    let c = client!(1);
    clear_prefix(&c, "cp:").await;
    c.create_key("cp:src", KeyType::Hash).await.unwrap();
    c.hash_set("cp:src", "a", "1").await.unwrap();
    c.set_ttl("cp:src", Some(120)).await.unwrap();

    c.copy_key("cp:src", "cp:dst", &c, false).await.unwrap();
    let copied = c.key_info("cp:dst").await.unwrap();
    assert_eq!(copied.kind, KeyType::Hash);
    assert!(copied.ttl > 0, "the expiry travels with the key");
    match c.read_value("cp:dst", KeyType::Hash).await.unwrap() {
        KeyValue::Rows { rows, .. } => assert!(rows.iter().any(|r| r.cells == ["a", "1"])),
        other => panic!("unexpected value: {other:?}"),
    }

    // Without REPLACE the target must not be clobbered silently.
    assert!(c.copy_key("cp:src", "cp:dst", &c, false).await.is_err());
    assert!(c.copy_key("cp:src", "cp:dst", &c, true).await.is_ok());
    clear_prefix(&c, "cp:").await;
}

#[tokio::test]
async fn searching_values_looks_inside_every_type() {
    let c = client!(2);
    clear_prefix(&c, "grep:").await;
    c.set_string("grep:plain", "hello WORLD").await.unwrap();
    c.create_key("grep:h", KeyType::Hash).await.unwrap();
    c.hash_set("grep:h", "greeting", "world domination")
        .await
        .unwrap();
    c.set_string("grep:other", "nothing here").await.unwrap();

    let (hits, _) = c.grep_values("grep:*", "world", 5000).await.unwrap();
    let mut names: Vec<String> = hits.into_iter().map(|k| k.name).collect();
    names.sort();
    assert_eq!(
        names,
        ["grep:h", "grep:plain"],
        "case-insensitive, across types"
    );
    clear_prefix(&c, "grep:").await;
}

#[tokio::test]
async fn an_export_round_trips_through_a_file() {
    let c = client!(3);
    clear_prefix(&c, "keep:").await;
    c.set_string("keep:1", "one").await.unwrap();
    c.create_key("keep:2", KeyType::ZSet).await.unwrap();
    c.zset_add("keep:2", "member", 1.5).await.unwrap();

    let names = vec!["keep:1".to_string(), "keep:2".to_string()];
    let entries = c.export_keys(&names).await.unwrap();
    assert_eq!(entries.len(), 2);
    let text = serde_json::to_string(&entries).unwrap();

    c.delete_keys(&names).await.unwrap();
    assert_eq!(c.key_info("keep:1").await.unwrap().kind, KeyType::Other);

    let parsed: Vec<rediscope::redis_client::ExportEntry> = serde_json::from_str(&text).unwrap();
    assert_eq!(c.import_entries(&parsed, false).await.unwrap(), 2);
    assert_eq!(c.key_info("keep:2").await.unwrap().kind, KeyType::ZSet);
    match c.read_value("keep:1", KeyType::String).await.unwrap() {
        KeyValue::Str(s) => assert_eq!(s, "one"),
        other => panic!("unexpected value: {other:?}"),
    }

    // A second import of the same keys needs REPLACE.
    assert!(c.import_entries(&parsed, false).await.is_err());
    assert!(c.import_entries(&parsed, true).await.is_ok());
    clear_prefix(&c, "keep:").await;
}

#[tokio::test]
async fn consumer_groups_are_listed_acked_and_destroyed() {
    let c = client!(4);
    clear_prefix(&c, "xg:").await;
    c.create_key("xg:stream", KeyType::Stream).await.unwrap();
    c.stream_group_create("xg:stream", "workers", "0")
        .await
        .unwrap();
    // Read the entry into the group so it becomes pending.
    c.execute_raw("XREADGROUP GROUP workers alice COUNT 10 STREAMS xg:stream >")
        .await
        .unwrap();

    let groups = c.stream_groups("xg:stream").await.unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].name, "workers");
    assert_eq!(groups[0].pending, 1);

    let detail = c.stream_group_detail("xg:stream", "workers").await.unwrap();
    assert_eq!(detail.consumers.len(), 1);
    assert_eq!(detail.consumers[0].name, "alice");
    let pending = detail.pending.first().expect("one pending entry").clone();
    assert_eq!(pending.consumer, "alice");

    c.stream_claim("xg:stream", "workers", "bob", &pending.id)
        .await
        .unwrap();
    let detail = c.stream_group_detail("xg:stream", "workers").await.unwrap();
    assert_eq!(detail.pending[0].consumer, "bob", "claimed by bob");

    c.stream_ack("xg:stream", "workers", &pending.id)
        .await
        .unwrap();
    assert_eq!(c.stream_groups("xg:stream").await.unwrap()[0].pending, 0);

    c.stream_group_destroy("xg:stream", "workers")
        .await
        .unwrap();
    assert!(c.stream_groups("xg:stream").await.unwrap().is_empty());
    clear_prefix(&c, "xg:").await;
}

#[tokio::test]
async fn diagnostics_read_the_slow_log_clients_and_config() {
    let c = client!(6);
    let diag = c.diagnostics().await.unwrap();
    assert!(
        diag.config.iter().any(|(k, _)| k == "maxmemory"),
        "CONFIG GET * reaches the running config"
    );
    assert!(
        diag.clients.iter().any(|c| !c.addr.is_empty()),
        "our own connection is in CLIENT LIST"
    );
    assert!(
        diag.latency.iter().any(|(k, _)| k.starts_with("ping")),
        "a ping sample is always available"
    );
    // A server built without cluster support refuses CLUSTER INFO outright,
    // which has to read as "no cluster" rather than as a failed fetch.
    assert!(
        diag.cluster.is_empty() || diag.cluster.iter().any(|(k, _)| k == "cluster_enabled"),
        "cluster state is either absent or parsed: {:?}",
        diag.cluster
    );
}

#[tokio::test]
async fn a_lua_script_sees_the_keys_it_was_given() {
    // db7 belongs to the memory scan, which flushes and counts what it finds.
    let c = client!(0);
    c.set_string("script:key", "value").await.unwrap();
    let out = c
        .eval(
            "return redis.call('GET', KEYS[1])",
            &["script:key".to_string()],
            &[],
        )
        .await
        .unwrap();
    assert_eq!(out.trim(), "value");
    c.delete_key("script:key").await.unwrap();
}

#[tokio::test]
async fn a_read_only_profile_refuses_every_write() {
    let Some(base) = conn(1) else { return };
    let writable = Client::connect(base.clone()).await.expect("connect");
    writable.set_string("ro:guarded", "before").await.unwrap();

    let guarded = Client::connect(Connection {
        read_only: true,
        ..base
    })
    .await
    .expect("connect");
    assert!(guarded.read_only());

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(Store::default(), tx);
    app.on_msg(Msg::Connected(Box::new(Ok(guarded))));
    app.current = Some(writable.key_info("ro:guarded").await.unwrap());
    // Every write key is refused before a modal ever opens.
    for key in ['n', 'D', 'R', 't', 'a', 'e', 'x'] {
        app.on_key(KeyEvent::new(KeyCode::Char(key), KeyModifiers::NONE));
        assert!(app.modal.is_none(), "'{key}' opened a write modal");
        assert!(app.status.contains("read-only"), "'{key}': {}", app.status);
    }

    match writable
        .read_value("ro:guarded", KeyType::String)
        .await
        .unwrap()
    {
        KeyValue::Str(s) => assert_eq!(s, "before", "nothing was written"),
        other => panic!("unexpected value: {other:?}"),
    }
    writable.delete_key("ro:guarded").await.unwrap();
}

#[tokio::test]
async fn the_write_table_separates_reads_from_writes() {
    let c = client!(9);
    let table = c.command_names().await.unwrap();
    assert!(table.is_write("SET k v"));
    assert!(table.is_write("del k"));
    assert!(table.is_write("FLUSHALL"), "destructive counts as a write");
    assert!(!table.is_write("GET k"));
    assert!(!table.is_write("INFO"));
    assert!(
        table.is_write("NOTACOMMAND"),
        "an unknown command is treated as a write"
    );
}

#[tokio::test]
async fn the_pubsub_feed_receives_what_is_published() {
    let c = client!(5);
    let publisher = client!(5);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(Store::default(), tx);
    app.on_msg(Msg::Connected(Box::new(Ok(c))));
    // Drain the messages the connection itself queues (server line, commands).
    app.on_key(KeyEvent::new(KeyCode::Char('P'), KeyModifiers::NONE));
    app.on_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)); // subscribe to *
    assert!(matches!(app.modal, Some(Modal::PubSub(_))));

    let received = async {
        loop {
            // Publishing repeatedly: the subscription is set up on its own
            // task, so the first message may go out before it is listening.
            publisher
                .execute_raw("PUBLISH chat hello")
                .await
                .expect("publish");
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(200);
            while let Ok(Some(msg)) = tokio::time::timeout_at(deadline, rx.recv()).await {
                app.on_msg(msg);
                if let Some(Modal::PubSub(state)) = &app.modal
                    && !state.messages.is_empty()
                {
                    return state.messages[0].clone();
                }
            }
        }
    };
    let (channel, payload) = tokio::time::timeout(std::time::Duration::from_secs(10), received)
        .await
        .expect("a message arrives");
    assert_eq!(channel, "chat");
    assert_eq!(payload, "hello");
}
