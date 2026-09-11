//! Reading past the first page of a collection, filtering its elements, and
//! loading more keys, against a real redis-server.
//!
//! Skipped unless REDISCOPE_TEST_PORT is set:
//!   redis-server --port 7799 --daemonize yes
//!   REDISCOPE_TEST_PORT=7799 cargo test --test windows

mod common;

use redis::AsyncCommands;
use rediscope::codec::View;
use rediscope::config::Connection;
use rediscope::redis_client::{
    Client, EditOutcome, EditTarget, FILTER_BUDGET, KeyType, KeyValue, VALUE_LIMIT, Window,
};

fn port() -> Option<u16> {
    common::isolate_config();
    std::env::var("REDISCOPE_TEST_PORT").ok()?.parse().ok()
}

async fn clients(db: i64) -> Option<(Client, redis::aio::MultiplexedConnection)> {
    let port = port()?;
    let client = Client::connect(Connection {
        name: "windows".into(),
        host: "127.0.0.1".into(),
        port,
        db,
        ..Default::default()
    })
    .await
    .expect("connect");
    // The byte-budget test pushes 80 MiB in one pipeline. The crate's default
    // 500 ms reply timeout is too short for that on some servers (KeyDB in
    // Docker takes longer), and the fixture should not be what fails.
    let raw = redis::Client::open(format!("redis://127.0.0.1:{port}/{db}"))
        .unwrap()
        .get_multiplexed_async_connection_with_config(
            &redis::AsyncConnectionConfig::new()
                .set_response_timeout(Some(std::time::Duration::from_secs(30))),
        )
        .await
        .unwrap();
    Some((client, raw))
}

macro_rules! clients {
    ($db:expr) => {
        match clients($db).await {
            Some(pair) => pair,
            None => return,
        }
    };
}

async fn clear(raw: &mut redis::aio::MultiplexedConnection, prefix: &str) {
    let keys: Vec<Vec<u8>> = raw.keys(format!("{prefix}*")).await.unwrap();
    for k in keys {
        let _: i64 = raw.del(k).await.unwrap();
    }
}

fn window(limit: usize, filter: Option<&str>) -> Window {
    Window {
        limit,
        filter: filter.map(str::to_string),
        similar: None,
    }
}

fn rows(value: KeyValue) -> Vec<rediscope::redis_client::Row> {
    match value {
        KeyValue::Rows { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

#[tokio::test]
async fn the_default_window_reads_exactly_what_it_always_did() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "win:same:").await;
    let fields: Vec<(String, String)> = (0..2500)
        .map(|i| (format!("f{i}"), format!("v{i}")))
        .collect();
    let _: () = raw.hset_multiple("win:same:h", &fields).await.unwrap();
    let before = rows(c.read_value("win:same:h", KeyType::Hash).await.unwrap());
    let read = c
        .read_window(
            "win:same:h",
            KeyType::Hash,
            &View::Plain,
            &Window::default(),
        )
        .await
        .unwrap();
    let after = rows(read.value);
    assert_eq!(before.len(), VALUE_LIMIT);
    assert_eq!(
        before.iter().map(|r| &r.cells).collect::<Vec<_>>(),
        after.iter().map(|r| &r.cells).collect::<Vec<_>>()
    );
    assert!(!read.coverage.complete);
    assert!(!read.coverage.filtered);
    clear(&mut raw, "win:same:").await;
}

#[tokio::test]
async fn loading_more_reads_past_the_first_page_of_every_type() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "win:more:").await;
    let n = 2500;
    let fields: Vec<(String, String)> = (0..n).map(|i| (format!("f{i}"), "v".into())).collect();
    let _: () = raw.hset_multiple("win:more:h", &fields).await.unwrap();
    let items: Vec<String> = (0..n).map(|i| format!("i{i}")).collect();
    let _: () = raw.rpush("win:more:l", &items).await.unwrap();
    let _: () = raw.sadd("win:more:s", &items).await.unwrap();
    let scored: Vec<(f64, String)> = (0..n).map(|i| (i as f64, format!("m{i}"))).collect();
    let _: () = raw.zadd_multiple("win:more:z", &scored).await.unwrap();
    let mut pipe = redis::pipe();
    for i in 0..n {
        pipe.cmd("XADD")
            .arg("win:more:x")
            .arg("*")
            .arg("i")
            .arg(i)
            .ignore();
    }
    let _: () = pipe.query_async(&mut raw).await.unwrap();

    for (key, kind) in [
        ("win:more:h", KeyType::Hash),
        ("win:more:l", KeyType::List),
        ("win:more:s", KeyType::Set),
        ("win:more:z", KeyType::ZSet),
        ("win:more:x", KeyType::Stream),
    ] {
        let first = c
            .read_window(key, kind, &View::Plain, &Window::default())
            .await
            .unwrap();
        assert!(!first.coverage.complete, "{key}");
        let more = c
            .read_window(key, kind, &View::Plain, &window(2000, None))
            .await
            .unwrap();
        assert_eq!(rows(more.value).len(), 2000, "{key}");
        assert!(!more.coverage.complete, "{key}");
        let all = c
            .read_window(key, kind, &View::Plain, &window(3000, None))
            .await
            .unwrap();
        assert_eq!(rows(all.value).len(), n, "{key}");
        assert!(all.coverage.complete, "{key}");
    }
    clear(&mut raw, "win:more:").await;
}

#[tokio::test]
async fn filters_match_fields_and_members_on_the_server() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "win:flt:").await;
    let fields: Vec<(String, String)> = (0..3000)
        .map(|i| {
            (
                format!("{}:{i}", if i % 3 == 0 { "user" } else { "job" }),
                "v".into(),
            )
        })
        .collect();
    let _: () = raw.hset_multiple("win:flt:h", &fields).await.unwrap();
    let read = c
        .read_window(
            "win:flt:h",
            KeyType::Hash,
            &View::Plain,
            &window(2000, Some("user:*")),
        )
        .await
        .unwrap();
    let found = rows(read.value);
    assert_eq!(found.len(), 1000);
    assert!(found.iter().all(|r| r.id.starts_with("user:")));
    assert!(read.coverage.filtered && read.coverage.complete);

    let members: Vec<String> = (0..50).map(|i| format!("tag{i}")).collect();
    let _: () = raw.sadd("win:flt:s", &members).await.unwrap();
    let read = c
        .read_window(
            "win:flt:s",
            KeyType::Set,
            &View::Plain,
            &window(1000, Some("tag1?")),
        )
        .await
        .unwrap();
    assert_eq!(rows(read.value).len(), 10);

    // A filtered sorted set comes back in score order, like the plain view.
    let _: () = raw
        .zadd_multiple(
            "win:flt:z",
            &[(3.0, "b:3"), (1.0, "b:1"), (2.0, "a:2"), (0.5, "b:0")],
        )
        .await
        .unwrap();
    let read = c
        .read_window(
            "win:flt:z",
            KeyType::ZSet,
            &View::Plain,
            &window(1000, Some("b:*")),
        )
        .await
        .unwrap();
    let found = rows(read.value);
    assert_eq!(
        found.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
        ["b:0", "b:1", "b:3"]
    );
    assert_eq!(found[0].cells[1], "0.5");
    clear(&mut raw, "win:flt:").await;
}

#[tokio::test]
async fn a_filtered_list_keeps_real_indexes_so_edits_hit_the_right_item() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "win:lst:").await;
    let items: Vec<String> = (0..3000)
        .map(|i| {
            if [5, 1500, 2999].contains(&i) {
                format!("needle-{i}")
            } else {
                format!("hay-{i}")
            }
        })
        .collect();
    let _: () = raw.rpush("win:lst:l", &items).await.unwrap();
    let read = c
        .read_window(
            "win:lst:l",
            KeyType::List,
            &View::Plain,
            &window(1000, Some("*needle*")),
        )
        .await
        .unwrap();
    assert!(read.coverage.complete);
    let found = rows(read.value);
    assert_eq!(
        found.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
        ["5", "1500", "2999"]
    );
    assert_eq!(found[1].cells, ["1500", "needle-1500"]);

    let target = EditTarget {
        key: "win:lst:l".into(),
        kind: KeyType::List,
        selector: found[1].id.clone(),
        original: found[1].cells[1].clone(),
        decoded: None,
    };
    assert_eq!(
        c.save_edit(&target, &["edited".into()], false)
            .await
            .unwrap(),
        EditOutcome::Saved
    );
    let at: String = raw.lindex("win:lst:l", 1500).await.unwrap();
    assert_eq!(at, "edited");
    let neighbour: String = raw.lindex("win:lst:l", 1499).await.unwrap();
    assert_eq!(neighbour, "hay-1499");
    clear(&mut raw, "win:lst:").await;
}

#[tokio::test]
async fn a_filtered_read_stops_at_its_budget_and_says_how_far_it_got() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "win:budget:").await;
    let total = FILTER_BUDGET + 5_000;
    let mut pipe = redis::pipe();
    for chunk in (0..total).collect::<Vec<_>>().chunks(10_000) {
        let items: Vec<String> = chunk.iter().map(|i| format!("x{i}")).collect();
        pipe.rpush("win:budget:l", items).ignore();
    }
    let _: () = pipe.query_async(&mut raw).await.unwrap();
    let _: () = raw.rpush("win:budget:l", "needle").await.unwrap();

    let first = c
        .read_window(
            "win:budget:l",
            KeyType::List,
            &View::Plain,
            &window(1000, Some("needle")),
        )
        .await
        .unwrap();
    assert!(!first.coverage.complete);
    assert_eq!(first.coverage.examined, FILTER_BUDGET as u64);
    assert!(
        rows(first.value).is_empty(),
        "the match lies past the budget"
    );

    // Loading more widens the budget with the limit.
    let second = c
        .read_window(
            "win:budget:l",
            KeyType::List,
            &View::Plain,
            &window(2000, Some("needle")),
        )
        .await
        .unwrap();
    assert!(second.coverage.complete);
    assert_eq!(rows(second.value).len(), 1);
    clear(&mut raw, "win:budget:").await;
}

#[tokio::test]
async fn a_filtered_stream_is_searched_past_its_first_chunk() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "win:xs:").await;
    let mut pipe = redis::pipe();
    for i in 0..2500 {
        let kind = if i % 1000 == 7 { "needle" } else { "hay" };
        pipe.cmd("XADD")
            .arg("win:xs:x")
            .arg(format!("1-{}", i + 1))
            .arg("kind")
            .arg(kind)
            .arg("n")
            .arg(i)
            .ignore();
    }
    let _: () = pipe.query_async(&mut raw).await.unwrap();
    let read = c
        .read_window(
            "win:xs:x",
            KeyType::Stream,
            &View::Plain,
            &window(1000, Some("needle")),
        )
        .await
        .unwrap();
    assert!(read.coverage.complete);
    let found = rows(read.value);
    // Newest first, across all three chunks.
    assert_eq!(
        found.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
        ["1-2008", "1-1008", "1-8"]
    );
    assert!(found[0].cells[1].contains("kind=needle"));
    clear(&mut raw, "win:xs:").await;
}

#[tokio::test]
async fn local_glob_matching_agrees_with_the_server() {
    let (_, mut raw) = clients!(0);
    clear(&mut raw, "win:glob").await;
    let members = [
        "hello", "hallo", "hillo", "h]llo", "-", "a", "b", "]", "a*b", "aXb", "a?", "ab", "abc",
        "", "\\", "end\\", "x-y", "[abc", "caT", "CAT",
    ];
    let _: () = raw.sadd("win:glob", &members).await.unwrap();
    let patterns = [
        "h[ae]llo",
        "h[^e]llo",
        "h[a-c]llo",
        "[a-]",
        "[\\]]",
        "a\\*b",
        "a?",
        "*",
        "a*",
        "[]",
        "[abc",
        "*\\\\",
        "x[-]y",
        "[^a-z]*",
        "?",
        "c[a-z]T",
        "\\[abc",
    ];
    for pattern in patterns {
        let (_, server): (u64, Vec<String>) = redis::cmd("SSCAN")
            .arg("win:glob")
            .arg(0)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(1000)
            .query_async(&mut raw)
            .await
            .unwrap();
        let mut server = server;
        server.sort();
        let mut local: Vec<String> = members
            .iter()
            .filter(|m| rediscope::glob::matches(pattern.as_bytes(), m.as_bytes()))
            .map(|m| m.to_string())
            .collect();
        local.sort();
        assert_eq!(local, server, "pattern {pattern:?}");
    }
    clear(&mut raw, "win:glob").await;
}

#[tokio::test]
async fn the_key_limit_follows_the_pattern_it_was_raised_for() {
    use rediscope::app::{App, Focus, Msg, Screen};
    use rediscope::config::Store;
    let (c, mut raw) = clients!(0);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(Store::default(), tx);
    app.screen = Screen::Browser;
    app.client = Some(c.clone());
    app.pattern = "win:keys:*".into();
    app.truncated = true;
    app.focus = Focus::Tree;
    app.on_key(crossterm::event::KeyEvent::from(
        crossterm::event::KeyCode::Char('+'),
    ));
    assert_eq!(app.key_limit, 10_000);
    // A different pattern starts again from one page.
    app.pattern = "other:*".into();
    app.reload_keys();
    assert_eq!(app.key_limit, 5_000);
    while rx.try_recv().is_ok() {}
    clear(&mut raw, "win:keys:").await;
}

#[tokio::test]
async fn scans_load_past_five_thousand_keys_when_asked() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "win:many:").await;
    let mut pipe = redis::pipe();
    for i in 0..7000 {
        pipe.set(format!("win:many:{i}"), i).ignore();
    }
    let _: () = pipe.query_async(&mut raw).await.unwrap();
    let first = c.scan_report("win:many:*", 5_000).await.unwrap();
    assert_eq!(first.keys.len(), 5_000);
    assert!(first.truncated);
    let more = c.scan_report("win:many:*", 10_000).await.unwrap();
    assert_eq!(more.keys.len(), 7_000);
    assert!(!more.truncated);
    clear(&mut raw, "win:many:").await;
}

#[tokio::test]
async fn a_list_delete_only_removes_the_item_that_was_read() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "win:del:").await;
    let _: () = raw.rpush("win:del:q", &["a", "b", "c"]).await.unwrap();
    // Someone pops the head: index 1 now holds "c", not the "b" we saw.
    let _: String = raw.lpop("win:del:q", None).await.unwrap();
    assert!(!c.list_remove_checked("win:del:q", 1, b"b").await.unwrap());
    let now: Vec<String> = raw.lrange("win:del:q", 0, -1).await.unwrap();
    assert_eq!(now, ["b", "c"], "nothing was removed");
    assert!(c.list_remove_checked("win:del:q", 0, b"b").await.unwrap());
    let now: Vec<String> = raw.lrange("win:del:q", 0, -1).await.unwrap();
    assert_eq!(now, ["c"]);
    clear(&mut raw, "win:del:").await;
}

#[tokio::test]
async fn a_filtered_sorted_set_shows_the_lowest_scores_it_found() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "win:zlow:").await;
    // 3,000 matches in scan order that has nothing to do with score.
    let scored: Vec<(f64, String)> = (0..3000)
        .map(|i| (((i * 7919) % 3000) as f64, format!("m:{i}")))
        .collect();
    let _: () = raw.zadd_multiple("win:zlow:z", &scored).await.unwrap();
    let read = c
        .read_window(
            "win:zlow:z",
            KeyType::ZSet,
            &View::Plain,
            &window(1000, Some("m:*")),
        )
        .await
        .unwrap();
    assert!(!read.coverage.complete, "more matches than one page");
    let found = rows(read.value);
    assert_eq!(found.len(), 1000);
    let scores: Vec<f64> = found.iter().map(|r| r.cells[1].parse().unwrap()).collect();
    assert_eq!(scores[0], 0.0);
    assert_eq!(scores[999], 999.0, "the lowest thousand, in order");
    clear(&mut raw, "win:zlow:").await;
}

#[tokio::test]
async fn a_filtered_walk_of_large_items_stops_at_its_byte_budget() {
    let (c, mut raw) = clients!(0);
    clear(&mut raw, "win:bytes:").await;
    let mib = vec![b'x'; 1024 * 1024];
    let items = rediscope::redis_client::FILTER_BYTES / mib.len() + 16;
    let mut pipe = redis::pipe();
    for _ in 0..items {
        pipe.rpush("win:bytes:l", &mib).ignore();
    }
    let _: () = pipe.query_async(&mut raw).await.unwrap();
    let read = c
        .read_window(
            "win:bytes:l",
            KeyType::List,
            &View::Plain,
            &window(1000, Some("needle")),
        )
        .await
        .unwrap();
    assert!(!read.coverage.complete);
    assert!(
        read.coverage.examined < items as u64,
        "examined {} of {items}",
        read.coverage.examined
    );
    clear(&mut raw, "win:bytes:").await;
}
