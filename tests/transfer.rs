//! Export and import in every format, against a real server: every type out
//! of one database and back into another, compared value by value.
//!
//! Skipped unless REDISCOPE_TEST_PORT is set:
//!   redis-server --port 7799 --daemonize yes
//!   REDISCOPE_TEST_PORT=7799 cargo test --test transfer

mod common;

use common::Flavor;
use redis::AsyncCommands;
use rediscope::config::{Connection, Environment};
use rediscope::redis_client::{Client, encode_key};
use rediscope::transfer::{self, Format, Parsed, Record, Value};

/// Keys are read from one database and written into the other. Every test
/// works under its own prefix and removes only that; nothing is flushed.
const SOURCE: i64 = 13;
const TARGET: i64 = 14;

fn port() -> Option<u16> {
    common::isolate_config();
    std::env::var("REDISCOPE_TEST_PORT").ok()?.parse().ok()
}

fn profile(db: i64) -> Connection {
    Connection {
        name: format!("transfer-{db}"),
        host: "127.0.0.1".into(),
        port: port().unwrap(),
        db,
        ..Default::default()
    }
}

struct Pair {
    source: Client,
    target: Client,
    raw: redis::aio::MultiplexedConnection,
    raw_target: redis::aio::MultiplexedConnection,
}

async fn raw(db: i64) -> redis::aio::MultiplexedConnection {
    redis::Client::open(format!("redis://127.0.0.1:{}/{db}", port().unwrap()))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

/// Both databases, with `prefix` cleared in each.
async fn pair(prefix: &str) -> Option<Pair> {
    port()?;
    let pair = Pair {
        source: Client::connect(profile(SOURCE)).await.expect("connect"),
        target: Client::connect(profile(TARGET)).await.expect("connect"),
        raw: raw(SOURCE).await,
        raw_target: raw(TARGET).await,
    };
    clear(&pair.source, prefix).await;
    clear(&pair.target, prefix).await;
    Some(pair)
}

async fn clear(c: &Client, prefix: &str) {
    let names = names(c, prefix).await;
    if !names.is_empty() {
        c.delete_keys(&names).await.unwrap();
    }
}

async fn names(c: &Client, prefix: &str) -> Vec<String> {
    let (keys, _) = c.scan_keys(&format!("{prefix}*"), 50_000).await.unwrap();
    keys.into_iter().map(|k| k.name).collect()
}

async fn supports(raw: &mut redis::aio::MultiplexedConnection, command: &str) -> bool {
    let info: redis::Value = redis::cmd("COMMAND")
        .arg("INFO")
        .arg(command)
        .query_async(raw)
        .await
        .unwrap_or(redis::Value::Nil);
    matches!(info, redis::Value::Array(ref items) if items.iter().any(|i| *i != redis::Value::Nil))
}

async fn export(c: &Client, names: &[String], format: Format, replace: bool) -> Vec<u8> {
    let (report, bytes) = c
        .export_to(names, format, replace, Vec::new())
        .await
        .unwrap();
    assert!(report.skipped.is_empty(), "{:?}", report.skipped);
    bytes
}

/// Every key under `prefix`, read back through the JSON Lines writer.
async fn snapshot(c: &Client, prefix: &str) -> Vec<Record> {
    let names = names(c, prefix).await;
    let bytes = export(c, &names, Format::Jsonl, false).await;
    match transfer::parse(&bytes).unwrap() {
        Parsed::Records(records, _) => records,
        Parsed::Dump(d) if d.is_empty() => Vec::new(),
        other => panic!("unexpected {other:?}"),
    }
}

/// Values must match exactly, except vectors, which a server quantizes on
/// the way in, and TTLs, which run down while the test runs.
fn assert_same(expected: &[Record], actual: &[Record], what: &str) {
    assert_eq!(
        expected
            .iter()
            .map(|r| encode_key(&r.key))
            .collect::<Vec<_>>(),
        actual
            .iter()
            .map(|r| encode_key(&r.key))
            .collect::<Vec<_>>(),
        "{what}: key names"
    );
    for (e, a) in expected.iter().zip(actual) {
        let name = encode_key(&e.key);
        match (e.ttl_ms, a.ttl_ms) {
            (None, None) => {}
            (Some(x), Some(y)) => assert!((x - y).abs() < 10_000, "{what} {name}: ttl {x} vs {y}"),
            other => panic!("{what} {name}: ttl {other:?}"),
        }
        match (&e.value, &a.value) {
            (Value::VectorSet(x), Value::VectorSet(y)) => {
                assert_eq!(x.elements.len(), y.elements.len(), "{what} {name}");
                for (ex, ey) in x.elements.iter().zip(&y.elements) {
                    assert_eq!(ex.element, ey.element, "{what} {name}");
                    assert_eq!(ex.attributes, ey.attributes, "{what} {name}");
                    for (vx, vy) in ex.vector.iter().zip(&ey.vector) {
                        assert!(
                            (vx - vy).abs() <= 0.05 * vx.abs().max(1.0),
                            "{what} {name}: vector {:?} vs {:?}",
                            ex.vector,
                            ey.vector
                        );
                    }
                }
            }
            (x, y) => assert_eq!(x, y, "{what} {name}"),
        }
    }
}

/// One key of every type the server has, including binary names and values
/// and an expiry. Returns how many keys it wrote.
async fn seed_every_type(p: &mut Pair, prefix: &str) -> usize {
    let r = &mut p.raw;
    let bin: &[u8] = &[0x80, 0xfe, 0x00, b'A'];
    let key = |s: &str| format!("{prefix}{s}");
    let _: () = r
        .set(key("string"), "hello, \"world\"\nline two")
        .await
        .unwrap();
    let _: () = r.pset_ex(key("expiring"), "soon", 600_000).await.unwrap();
    let mut bin_key = key("bin:").into_bytes();
    bin_key.extend_from_slice(bin);
    let _: () = r.set(&bin_key, bin).await.unwrap();
    let _: () = r
        .hset_multiple(key("hash"), &[("name", b"ada".as_slice()), ("blob", bin)])
        .await
        .unwrap();
    let _: () = r.hset(key("hash:binfield"), bin, "value").await.unwrap();
    let _: () = r
        .rpush(key("list"), &[b"a".as_slice(), b"a", b"", bin])
        .await
        .unwrap();
    let _: () = r.sadd(key("set"), &[b"x".as_slice(), bin]).await.unwrap();
    let _: () = redis::cmd("ZADD")
        .arg(key("zset"))
        .arg("-inf")
        .arg("low")
        .arg(1.5)
        .arg("mid")
        .arg("0.1")
        .arg("tenth")
        .arg("+inf")
        .arg(bin)
        .query_async(r)
        .await
        .unwrap();
    for (id, fields) in [
        ("1-1", vec![("f", b"v".as_slice()), ("f", b"w")]),
        ("2-0", vec![("a:b", bin)]),
    ] {
        let mut cmd = redis::cmd("XADD");
        cmd.arg(key("stream")).arg(id);
        for (f, v) in fields {
            cmd.arg(f).arg(v);
        }
        let _: String = cmd.query_async(r).await.unwrap();
    }
    let _: i64 = r.expire(key("stream"), 900).await.unwrap();
    let mut count = 9;
    if !common::skip_on(&[Flavor::Dragonfly], "HyperLogLog payloads are its own") {
        let _: () = r.pfadd(key("hll"), &["a", "b", "c"]).await.unwrap();
        count += 1;
    }
    if supports(r, "VADD").await {
        for (el, v, attr) in [
            ("e1", [1.0, 2.0], Some(r#"{"year":1950}"#)),
            ("e2", [0.5, -0.25], None),
        ] {
            let mut cmd = redis::cmd("VADD");
            cmd.arg(key("vset"))
                .arg("VALUES")
                .arg(2)
                .arg(v[0])
                .arg(v[1])
                .arg(el);
            if let Some(a) = attr {
                cmd.arg("SETATTR").arg(a);
            }
            let _: i64 = cmd.query_async(r).await.unwrap();
        }
        count += 1;
    } else {
        eprintln!("skipped vector sets: the server has no VADD");
    }
    if supports(r, "JSON.SET").await {
        let _: () = redis::cmd("JSON.SET")
            .arg(key("json"))
            .arg("$")
            .arg(r#"{"b":[1,2.5,"x"],"a":null}"#)
            .query_async(r)
            .await
            .unwrap();
        count += 1;
    } else {
        eprintln!("skipped RedisJSON: the module is not loaded");
    }
    if supports(r, "TS.CREATE").await {
        let _: () = redis::cmd("TS.CREATE")
            .arg(key("ts"))
            .arg("LABELS")
            .arg("sensor")
            .arg("t1")
            .query_async(r)
            .await
            .unwrap();
        for (t, v) in [(1000, 1.25), (2000, -3.0)] {
            let _: i64 = redis::cmd("TS.ADD")
                .arg(key("ts"))
                .arg(t)
                .arg(v)
                .query_async(r)
                .await
                .unwrap();
        }
        count += 1;
    } else {
        eprintln!("skipped RedisTimeSeries: the module is not loaded");
    }
    count
}

#[tokio::test]
async fn every_type_round_trips_through_every_format() {
    let prefix = "xfer:types:";
    let Some(mut p) = pair(prefix).await else {
        return;
    };
    let count = seed_every_type(&mut p, prefix).await;
    let expected = snapshot(&p.source, prefix).await;
    assert_eq!(expected.len(), count);
    let names = names(&p.source, prefix).await;
    for format in Format::ALL {
        let bytes = export(&p.source, &names, format, false).await;
        clear(&p.target, prefix).await;
        let parsed = transfer::parse(&bytes).unwrap();
        assert_eq!(parsed.format(), format, "detected from the content");
        let report = p.target.import_parsed(&parsed, false).await.unwrap();
        assert_eq!(report.keys as usize, count, "{format:?}");
        let mut actual = snapshot(&p.target, prefix).await;
        let mut wanted = expected.clone();
        if format == Format::Csv {
            // CSV has no column for series labels or retention.
            for r in wanted.iter_mut().chain(actual.iter_mut()) {
                if let Value::TimeSeries(s) = &mut r.value {
                    s.labels.clear();
                    s.retention_ms = None;
                }
            }
        }
        assert_same(&wanted, &actual, format.name());
    }
    // HyperLogLogs arrive as working HyperLogLogs.
    if expected.iter().any(|r| r.key.ends_with(b"hll")) {
        let n: i64 = p.raw_target.pfcount(format!("{prefix}hll")).await.unwrap();
        assert_eq!(n, 3);
    }
    clear(&p.source, prefix).await;
    clear(&p.target, prefix).await;
}

#[tokio::test]
async fn big_collections_cross_every_chunk_boundary() {
    let prefix = "xfer:big:";
    let Some(mut p) = pair(prefix).await else {
        return;
    };
    let r = &mut p.raw;
    let key = |s: &str| format!("{prefix}{s}");
    let fields: Vec<(String, String)> = (0..1203)
        .map(|i| (format!("f{i}"), format!("v{i}")))
        .collect();
    let _: () = r.hset_multiple(key("hash"), &fields).await.unwrap();
    let items: Vec<String> = (0..2505).map(|i| format!("item{i}")).collect();
    let _: () = r.rpush(key("list"), &items).await.unwrap();
    let _: () = r.sadd(key("set"), &items[..1201]).await.unwrap();
    let scored: Vec<(f64, String)> = (0..2100)
        .map(|i| (i as f64 / 7.0, format!("m{i}")))
        .collect();
    let _: () = r.zadd_multiple(key("zset"), &scored).await.unwrap();
    let mut pipe = redis::pipe();
    for i in 0..1150 {
        pipe.cmd("XADD")
            .arg(key("stream"))
            .arg(format!("{}-{}", 1 + i / 3, i % 3))
            .arg("n")
            .arg(i);
    }
    let _: Vec<String> = pipe.query_async(r).await.unwrap();

    let expected = snapshot(&p.source, prefix).await;
    let lens: Vec<usize> = expected
        .iter()
        .map(|r| match &r.value {
            Value::Hash(v) => v.len(),
            Value::List(v) | Value::Set(v) => v.len(),
            Value::ZSet(v) => v.len(),
            Value::Stream(v) => v.len(),
            other => panic!("{other:?}"),
        })
        .collect();
    // hash, list, set, stream, zset: name order.
    assert_eq!(lens, [1203, 2505, 1201, 1150, 2100]);

    let names = names(&p.source, prefix).await;
    for format in [Format::Jsonl, Format::Commands, Format::Csv] {
        let bytes = export(&p.source, &names, format, false).await;
        if format == Format::Commands {
            let text = String::from_utf8(bytes.clone()).unwrap();
            // 1203 fields in chunks of 500, 2505 items, and so on.
            assert_eq!(text.lines().filter(|l| l.starts_with("HSET ")).count(), 3);
            assert_eq!(text.lines().filter(|l| l.starts_with("RPUSH ")).count(), 6);
            assert_eq!(
                text.lines().filter(|l| l.starts_with("XADD ")).count(),
                1150
            );
        }
        clear(&p.target, prefix).await;
        let parsed = transfer::parse(&bytes).unwrap();
        p.target.import_parsed(&parsed, false).await.unwrap();
        assert_same(&expected, &snapshot(&p.target, prefix).await, format.name());
    }
    clear(&p.source, prefix).await;
    clear(&p.target, prefix).await;
}

#[tokio::test]
async fn redis_cli_loads_a_commands_file() {
    let prefix = "xfer:cli:";
    let Some(mut p) = pair(prefix).await else {
        return;
    };
    let Ok(version) = std::process::Command::new("redis-cli")
        .arg("--version")
        .output()
    else {
        eprintln!("skipped: redis-cli is not on PATH");
        return;
    };
    assert!(version.status.success());
    seed_every_type(&mut p, prefix).await;
    let expected = snapshot(&p.source, prefix).await;
    let names = names(&p.source, prefix).await;
    let file = std::env::temp_dir().join(format!("rediscope-xfer-{}.redis", std::process::id()));
    std::fs::write(
        &file,
        export(&p.source, &names, Format::Commands, true).await,
    )
    .unwrap();

    // Something in the way, which DEL clears.
    let _: () = p
        .raw_target
        .rpush(format!("{prefix}list"), "stale")
        .await
        .unwrap();
    let output = std::process::Command::new("redis-cli")
        .args([
            "-p",
            &port().unwrap().to_string(),
            "-n",
            &TARGET.to_string(),
        ])
        .stdin(std::fs::File::open(&file).unwrap())
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains("ERR"), "{stdout}");
    assert_same(&expected, &snapshot(&p.target, prefix).await, "redis-cli");
    std::fs::remove_file(file).unwrap();
    clear(&p.source, prefix).await;
    clear(&p.target, prefix).await;
}

#[tokio::test]
async fn old_dump_files_still_import() {
    let prefix = "xfer:old:";
    let Some(mut p) = pair(prefix).await else {
        return;
    };
    let _: () = p.raw.set(format!("{prefix}s"), "kept").await.unwrap();
    let _: () = p.raw.zadd(format!("{prefix}z"), "m", 2.5).await.unwrap();
    let names = names(&p.source, prefix).await;
    // Exactly what `export` wrote before it had formats.
    let entries = p.source.export_keys(&names).await.unwrap();
    let old = serde_json::to_string_pretty(&entries).unwrap();
    let parsed = transfer::parse(old.as_bytes()).unwrap();
    assert!(matches!(parsed, Parsed::Dump(ref e) if e.len() == 2));
    assert_eq!(
        p.target.import_parsed(&parsed, false).await.unwrap().keys,
        2
    );
    assert_same(
        &snapshot(&p.source, prefix).await,
        &snapshot(&p.target, prefix).await,
        "dump",
    );
    // And the default export is still that format.
    let file = std::env::temp_dir().join(format!("rediscope-xfer-old-{}.json", std::process::id()));
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_rediscope"))
        .args([
            "-H",
            "127.0.0.1",
            "-p",
            &port().unwrap().to_string(),
            "-n",
            &SOURCE.to_string(),
        ])
        .args([
            "export",
            "--pattern",
            &format!("{prefix}*"),
            "--out",
            file.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
    let text = std::fs::read_to_string(&file).unwrap();
    assert!(
        text.trim_start().starts_with('[') && text.contains("\"dump\""),
        "{text}"
    );
    std::fs::remove_file(file).unwrap();
    clear(&p.source, prefix).await;
    clear(&p.target, prefix).await;
}

#[tokio::test]
async fn replace_decides_what_happens_to_existing_keys() {
    let prefix = "xfer:replace:";
    let Some(mut p) = pair(prefix).await else {
        return;
    };
    let list = format!("{prefix}list");
    let _: () = p.raw.rpush(&list, &["a", "b"]).await.unwrap();
    let _: () = p.raw.set(format!("{prefix}s"), "one").await.unwrap();
    let names = names(&p.source, prefix).await;
    let expected = snapshot(&p.source, prefix).await;
    let bytes = export(&p.source, &names, Format::Json, false).await;
    let parsed = transfer::parse(&bytes).unwrap();

    let _: () = p.raw_target.rpush(&list, "stale").await.unwrap();
    let err = p
        .target
        .import_parsed(&parsed, false)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("already exists"), "{err}");
    let stale: Vec<String> = p.raw_target.lrange(&list, 0, -1).await.unwrap();
    assert_eq!(stale, ["stale"], "without replace nothing is merged in");

    let report = p.target.import_parsed(&parsed, true).await.unwrap();
    assert_eq!(report.keys, 2);
    assert_same(&expected, &snapshot(&p.target, prefix).await, "replace");

    // A commands file without DEL adds to what is there; with DEL it replaces.
    let plain = export(&p.source, &names, Format::Commands, false).await;
    p.target
        .import_parsed(&transfer::parse(&plain).unwrap(), false)
        .await
        .unwrap();
    let merged: Vec<String> = p.raw_target.lrange(&list, 0, -1).await.unwrap();
    assert_eq!(merged, ["a", "b", "a", "b"]);
    let with_del = export(&p.source, &names, Format::Commands, true).await;
    assert!(String::from_utf8_lossy(&with_del).contains(&format!("DEL {list}\n")));
    p.target
        .import_parsed(&transfer::parse(&with_del).unwrap(), false)
        .await
        .unwrap();
    assert_same(
        &expected,
        &snapshot(&p.target, prefix).await,
        "commands with DEL",
    );
    clear(&p.source, prefix).await;
    clear(&p.target, prefix).await;
}

#[tokio::test]
async fn a_failing_commands_file_names_its_line() {
    let prefix = "xfer:lines:";
    let Some(mut p) = pair(prefix).await else {
        return;
    };
    let text = format!("SET {prefix}a 1\n\nSET {prefix}b 2\nRPUSH {prefix}a x\nSET {prefix}c 3\n");
    let parsed = transfer::parse(text.as_bytes()).unwrap();
    let err = p
        .target
        .import_parsed(&parsed, false)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.starts_with("line 4: ") && err.contains("WRONGTYPE"),
        "{err}"
    );
    assert!(err.contains("2 command(s) before it ran"), "{err}");
    let c: Option<String> = p.raw.get(format!("{prefix}c")).await.unwrap();
    assert_eq!(c, None, "the import stops at the failing line");

    // A command that is not a data command refuses the whole file up front.
    let text = format!("SET {prefix}d 1\nFLUSHDB\n");
    let err = transfer::parse(text.as_bytes()).unwrap_err().to_string();
    assert!(err.starts_with("line 2: FLUSHDB"), "{err}");
    clear(&p.target, prefix).await;
}

#[tokio::test]
async fn a_read_only_profile_refuses_every_import() {
    let prefix = "xfer:ro:";
    let Some(mut p) = pair(prefix).await else {
        return;
    };
    let _: () = p.raw.set(format!("{prefix}s"), "v").await.unwrap();
    let names = names(&p.source, prefix).await;
    let mut ro = profile(TARGET);
    ro.read_only = true;
    let client = Client::connect(ro.clone()).await.unwrap();
    // Export is a read, and a read-only profile may do it.
    let file = std::env::temp_dir().join(format!("rediscope-xfer-ro-{}.jsonl", std::process::id()));
    for format in [Format::Jsonl, Format::Commands, Format::Dump] {
        let bytes = export(&p.source, &names, format, false).await;
        std::fs::write(&file, &bytes).unwrap();
        let parsed = transfer::parse(&bytes).unwrap();
        assert!(
            client.import_parsed(&parsed, true).await.is_err(),
            "{format:?}"
        );
        assert!(
            rediscope::headless::import(ro.clone(), file.to_str().unwrap(), false)
                .await
                .is_err()
        );
    }
    let exists: bool = p.raw_target.exists(format!("{prefix}s")).await.unwrap();
    assert!(!exists);
    std::fs::remove_file(file).unwrap();
    clear(&p.source, prefix).await;
}

fn production(name: &str) -> Connection {
    Connection {
        name: name.into(),
        environment: Environment::Production,
        ..profile(TARGET)
    }
}

#[tokio::test]
async fn a_production_import_needs_the_write_lease() {
    let prefix = "xfer:lease:";
    let Some(mut p) = pair(prefix).await else {
        return;
    };
    let _: () = p.raw.sadd(format!("{prefix}s"), "m").await.unwrap();
    let names = names(&p.source, prefix).await;
    let client = Client::connect(production("xfer-lease")).await.unwrap();
    for format in [Format::Json, Format::Commands] {
        let bytes = export(&p.source, &names, format, false).await;
        let parsed = transfer::parse(&bytes).unwrap();
        let err = client.import_parsed(&parsed, false).await.unwrap_err();
        assert!(err.to_string().contains("rejected"), "{format:?}: {err}");
        let exists: bool = p.raw_target.exists(format!("{prefix}s")).await.unwrap();
        assert!(!exists, "{format:?}");
    }
    client.unlock_writes("xfer-lease").unwrap();
    let bytes = export(&p.source, &names, Format::Json, false).await;
    let parsed = transfer::parse(&bytes).unwrap();
    assert_eq!(client.import_parsed(&parsed, false).await.unwrap().keys, 1);
    client.lock_writes().unwrap();
    assert!(client.import_parsed(&parsed, true).await.is_err());
    clear(&p.source, prefix).await;
    clear(&p.target, prefix).await;
}
