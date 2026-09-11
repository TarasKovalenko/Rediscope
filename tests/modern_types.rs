//! Hash field expiry (Redis 7.4+) and vector sets (Redis 8), against a real
//! server. Each test skips itself, saying why, when the server predates the
//! feature, so the suite still passes against older Redis and other servers.
//!
//! Skipped unless REDISCOPE_TEST_PORT is set:
//!   redis-server --port 7799 --daemonize yes
//!   REDISCOPE_TEST_PORT=7799 cargo test --test modern_types

mod common;

use redis::AsyncCommands;
use rediscope::codec::View;
use rediscope::config::Connection;
use rediscope::redis_client::{
    Client, EditOutcome, EditTarget, KeyType, KeyValue, Row, Similar, SimilarTo, Window,
};

/// Every key this suite writes lives under this prefix, in db 12, and is
/// removed first: nothing here flushes a database.
const PREFIX: &str = "rediscope-modern:";
const DB: i64 = 12;

async fn clients(read_only: bool) -> Option<(Client, redis::aio::MultiplexedConnection)> {
    common::isolate_config();
    let port: u16 = std::env::var("REDISCOPE_TEST_PORT").ok()?.parse().ok()?;
    let client = Client::connect(Connection {
        name: "modern".into(),
        host: "127.0.0.1".into(),
        port,
        db: DB,
        read_only,
        ..Default::default()
    })
    .await
    .expect("connect");
    let raw = redis::Client::open(format!("redis://127.0.0.1:{port}/{DB}"))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    Some((client, raw))
}

/// Whether the server knows `command`, judged from `COMMAND INFO`.
async fn supports(raw: &mut redis::aio::MultiplexedConnection, command: &str) -> bool {
    let info: redis::Value = redis::cmd("COMMAND")
        .arg("INFO")
        .arg(command)
        .query_async(raw)
        .await
        .unwrap_or(redis::Value::Nil);
    matches!(info, redis::Value::Array(ref items) if items.iter().any(|i| *i != redis::Value::Nil))
}

async fn fresh(raw: &mut redis::aio::MultiplexedConnection, key: &str) -> String {
    let name = format!("{PREFIX}{key}");
    let _: () = raw.del(&name).await.unwrap();
    name
}

fn rows(value: KeyValue) -> Vec<Row> {
    match value {
        KeyValue::Rows { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

#[tokio::test]
async fn hash_fields_carry_their_own_ttls() {
    let Some((c, mut raw)) = clients(false).await else {
        return;
    };
    if !supports(&mut raw, "HEXPIRE").await {
        eprintln!("skipped: this server has no hash field expiry (Redis 7.4+, Valkey 9)");
        return;
    }
    let key = fresh(&mut raw, "fields").await;
    let _: () = raw
        .hset_multiple(&key, &[("a", "1"), ("b", "2"), ("c", "3")])
        .await
        .unwrap();
    let _: Vec<i64> = redis::cmd("HEXPIRE")
        .arg(&key)
        .arg(100)
        .arg("FIELDS")
        .arg(1)
        .arg("a")
        .query_async(&mut raw)
        .await
        .unwrap();

    let ttl_of = |rows: &[Row], field: &str| rows.iter().find(|r| r.id == field).unwrap().ttl;
    let read = rows(c.read_value(&key, KeyType::Hash).await.unwrap());
    assert!(matches!(ttl_of(&read, "a"), Some(t) if (95..=100).contains(&t)));
    assert_eq!(ttl_of(&read, "b"), None);

    c.set_field_ttl(&key, "b", Some(50)).await.unwrap();
    if supports(&mut raw, "HPERSIST").await {
        c.set_field_ttl(&key, "a", None).await.unwrap();
    } else {
        // Dragonfly expires fields but cannot persist one again; say so.
        let e = c.set_field_ttl(&key, "a", None).await.unwrap_err();
        assert!(e.to_string().contains("no HPERSIST"), "{e}");
        let _: () = raw.hdel(&key, "a").await.unwrap();
        let _: () = raw.hset(&key, "a", "1").await.unwrap();
    }
    let read = rows(c.read_value(&key, KeyType::Hash).await.unwrap());
    assert_eq!(ttl_of(&read, "a"), None);
    assert!(matches!(ttl_of(&read, "b"), Some(t) if (45..=50).contains(&t)));
    assert_eq!(ttl_of(&read, "c"), None);

    // The key keeps no expiry of its own.
    let key_ttl: i64 = raw.ttl(&key).await.unwrap();
    assert_eq!(key_ttl, -1);

    let gone = c.set_field_ttl(&key, "nope", Some(10)).await.unwrap_err();
    assert!(gone.to_string().contains("no longer"), "{gone}");

    // Zero seconds deletes the field, the way Redis does.
    c.set_field_ttl(&key, "c", Some(0)).await.unwrap();
    let exists: bool = raw.hexists(&key, "c").await.unwrap();
    assert!(!exists);

    // Read-only profiles still see the TTLs, and cannot change them.
    let (ro, _) = clients(true).await.unwrap();
    let read = rows(ro.read_value(&key, KeyType::Hash).await.unwrap());
    assert!(ttl_of(&read, "b").is_some());
    assert!(ro.set_field_ttl(&key, "b", Some(5)).await.is_err());
}

#[tokio::test]
async fn a_hash_without_field_ttls_has_no_ttl_column() {
    let Some((c, mut raw)) = clients(false).await else {
        return;
    };
    let key = fresh(&mut raw, "plain-hash").await;
    let _: () = raw.hset(&key, "f", "v").await.unwrap();
    let read = rows(c.read_value(&key, KeyType::Hash).await.unwrap());
    assert_eq!(read.len(), 1);
    assert!(read.iter().all(|r| r.ttl.is_none()));
}

/// A small vector set under its own name, since the tests run in parallel.
async fn movie_set(raw: &mut redis::aio::MultiplexedConnection, name: &str) -> String {
    let key = fresh(raw, name).await;
    for (element, vector, attrs) in [
        ("heat", [1.0, 0.0, 0.0], r#"{"year":1995}"#),
        ("ronin", [0.9, 0.1, 0.0], r#"{"year":1998}"#),
        ("amelie", [0.0, 0.0, 1.0], ""),
    ] {
        let mut cmd = redis::cmd("VADD");
        cmd.arg(&key).arg("VALUES").arg(3);
        for v in vector {
            cmd.arg(v);
        }
        cmd.arg(element);
        if !attrs.is_empty() {
            cmd.arg("SETATTR").arg(attrs);
        }
        let _: i64 = cmd.query_async(raw).await.unwrap();
    }
    key
}

#[tokio::test]
async fn vector_sets_are_browsed_with_their_attributes() {
    let Some((c, mut raw)) = clients(false).await else {
        return;
    };
    if !supports(&mut raw, "VADD").await {
        eprintln!("skipped: this server has no vector sets (Redis 8)");
        return;
    }
    let key = movie_set(&mut raw, "movies-browse").await;

    let (keys, _) = c
        .scan_keys(&format!("{PREFIX}movies-browse"), 100)
        .await
        .unwrap();
    assert_eq!(keys[0].kind, KeyType::VectorSet);

    let read = c
        .read_window(&key, KeyType::VectorSet, &View::Auto, &Window::default())
        .await
        .unwrap();
    let detail = read.detail.clone().unwrap_or_default();
    assert!(detail.contains("dim 3"), "{detail}");
    let KeyValue::Rows {
        headers,
        rows,
        total,
    } = read.value
    else {
        panic!("rows");
    };
    assert_eq!(headers, vec!["element", "attributes"]);
    assert_eq!(total, 3);
    let ids: Vec<&str> = rows.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(ids, vec!["amelie", "heat", "ronin"]);
    assert_eq!(rows[1].cells[1], r#"{"year":1995}"#);
    assert_eq!(rows[0].cells[1], "");
    assert!(read.coverage.complete);

    let element = c.vset_element(&key, "heat").await.unwrap().unwrap();
    assert_eq!(element.vector.len(), 3);
    assert_eq!(element.attributes.as_deref(), Some(r#"{"year":1995}"#));
    assert!(c.vset_element(&key, "nope").await.unwrap().is_none());
}

#[tokio::test]
async fn similarity_search_ranks_filters_and_takes_a_typed_vector() {
    let Some((c, mut raw)) = clients(false).await else {
        return;
    };
    if !supports(&mut raw, "VSIM").await {
        eprintln!("skipped: this server has no vector sets (Redis 8)");
        return;
    }
    let key = movie_set(&mut raw, "movies-similar").await;
    let search = |to: SimilarTo, filter: Option<&str>| Window {
        similar: Some(Similar {
            to,
            filter: filter.map(str::to_string),
            count: 10,
        }),
        ..Window::default()
    };

    let read = c
        .read_window(
            &key,
            KeyType::VectorSet,
            &View::Auto,
            &search(SimilarTo::Element("heat".into()), None),
        )
        .await
        .unwrap();
    let KeyValue::Rows { headers, rows, .. } = read.value else {
        panic!("rows");
    };
    assert_eq!(headers, vec!["element", "similarity", "attributes"]);
    let ids: Vec<&str> = rows.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(ids, vec!["heat", "ronin", "amelie"]);
    assert_eq!(rows[0].cells[1], "1.0000");
    assert_eq!(rows[1].cells[2], r#"{"year":1998}"#);
    assert!(read.coverage.filtered);

    let filtered = rows_of(
        c.read_window(
            &key,
            KeyType::VectorSet,
            &View::Auto,
            &search(SimilarTo::Element("heat".into()), Some(".year > 1996")),
        )
        .await
        .unwrap()
        .value,
    );
    assert_eq!(filtered, vec!["ronin"]);

    let typed = rows_of(
        c.read_window(
            &key,
            KeyType::VectorSet,
            &View::Auto,
            &search(SimilarTo::Vector(vec![0.0, 0.1, 0.9]), None),
        )
        .await
        .unwrap()
        .value,
    );
    assert_eq!(typed.first().map(String::as_str), Some("amelie"));

    // A read-only profile can search.
    let (ro, _) = clients(true).await.unwrap();
    ro.read_window(
        &key,
        KeyType::VectorSet,
        &View::Auto,
        &search(SimilarTo::Element("heat".into()), None),
    )
    .await
    .unwrap();
}

fn rows_of(value: KeyValue) -> Vec<String> {
    rows(value).into_iter().map(|r| r.id).collect()
}

#[tokio::test]
async fn vector_set_elements_are_added_edited_and_removed() {
    let Some((c, mut raw)) = clients(false).await else {
        return;
    };
    if !supports(&mut raw, "VADD").await {
        eprintln!("skipped: this server has no vector sets (Redis 8)");
        return;
    }
    let key = movie_set(&mut raw, "movies-edit").await;

    c.vset_add(&key, "drive", &[0.8, 0.2, 0.1], Some(r#"{"year":2011}"#))
        .await
        .unwrap();
    let card: i64 = redis::cmd("VCARD")
        .arg(&key)
        .query_async(&mut raw)
        .await
        .unwrap();
    assert_eq!(card, 4);
    let wrong_dim = c.vset_add(&key, "x", &[1.0], None).await;
    assert!(wrong_dim.is_err());

    // Attributes save through the conflict check.
    let target = |original: &str| EditTarget {
        key: key.clone(),
        kind: KeyType::VectorSet,
        selector: "heat".into(),
        original: original.into(),
        decoded: None,
    };
    let saved = c
        .save_edit(
            &target(r#"{"year":1995}"#),
            &[r#"{"year":1995,"cast":["pacino"]}"#.into()],
            false,
        )
        .await
        .unwrap();
    assert_eq!(saved, EditOutcome::Saved);
    let attr: String = redis::cmd("VGETATTR")
        .arg(&key)
        .arg("heat")
        .query_async(&mut raw)
        .await
        .unwrap();
    assert_eq!(attr, r#"{"year":1995,"cast":["pacino"]}"#);

    // What the user saw is stale: nothing is written.
    let stale = c
        .save_edit(
            &target(r#"{"year":1995}"#),
            &[r#"{"year":1}"#.into()],
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        stale,
        EditOutcome::Conflict {
            current: Some(r#"{"year":1995,"cast":["pacino"]}"#.into())
        }
    );

    // An element with no attributes compares as empty, and gains some.
    let amelie = EditTarget {
        selector: "amelie".into(),
        ..target("")
    };
    assert_eq!(
        c.save_edit(&amelie, &[r#"{"year":2001}"#.into()], false)
            .await
            .unwrap(),
        EditOutcome::Saved
    );
    // Broken JSON never reaches the server.
    assert!(c.save_edit(&amelie, &["{".into()], false).await.is_err());

    // Empty attributes are removed.
    let cleared = EditTarget {
        selector: "amelie".into(),
        ..target(r#"{"year":2001}"#)
    };
    assert_eq!(
        c.save_edit(&cleared, &[String::new()], false)
            .await
            .unwrap(),
        EditOutcome::Saved
    );
    let attr: Option<String> = redis::cmd("VGETATTR")
        .arg(&key)
        .arg("amelie")
        .query_async(&mut raw)
        .await
        .unwrap();
    assert!(attr.as_deref().is_none_or(str::is_empty), "{attr:?}");

    // An element removed since it was read is a conflict, not a recreation.
    c.vset_remove(&key, "ronin").await.unwrap();
    let missing = EditTarget {
        selector: "ronin".into(),
        ..target(r#"{"year":1998}"#)
    };
    assert_eq!(
        c.save_edit(&missing, &[r#"{"year":2}"#.into()], true)
            .await
            .unwrap(),
        EditOutcome::Conflict { current: None }
    );
    assert!(c.vset_remove(&key, "ronin").await.is_err());

    // Read-only profiles cannot write.
    let (ro, _) = clients(true).await.unwrap();
    assert!(
        ro.vset_add(&key, "y", &[0.0, 0.0, 1.0], None)
            .await
            .is_err()
    );
    assert!(ro.vset_remove(&key, "heat").await.is_err());
}
