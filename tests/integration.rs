//! End-to-end checks against a real redis-server.
//!
//! Skipped unless REDISCOPE_TEST_PORT is set:
//!   redis-server --port 7799 --daemonize yes
//!   REDISCOPE_TEST_PORT=7799 cargo test --test integration

use rediscope::config::Connection;
use rediscope::redis_client::{Client, KeyType, KeyValue};

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
