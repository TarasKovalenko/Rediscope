//! Export and import edge cases: awkward bytes in every format, a second
//! disposable server as the target, `redis-cli` as an independent reader of
//! commands files, and the safety checks an import passes.
//!
//! The live tests read from REDISCOPE_TEST_PORT (database 2, under a prefix)
//! and write into a `redis-server` they start themselves, so they need
//! `redis-server` and `redis-cli` on the PATH. Without the port they skip.
//!   REDISCOPE_TEST_PORT=7799 cargo test --test transfer_edges

mod common;

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use rediscope::config::{Connection, Environment};
use rediscope::redis_client::{Client, encode_key};
use rediscope::transfer::{
    self, CHUNK, CSV_HEADER, Format, Parsed, Record, Value, command_line, commands, csv_records,
    parse, parse_csv, quote_arg, split_line,
};

/// Shared only with suites that clean up by prefix too: nothing flushes it
/// or counts every key in it, even with every suite running at once.
const SOURCE_DB: i64 = 2;

fn source_port() -> Option<u16> {
    common::isolate_config();
    std::env::var("REDISCOPE_TEST_PORT").ok()?.parse().ok()
}

fn scratch(name: &str) -> PathBuf {
    static N: AtomicU32 = AtomicU32::new(0);
    let dir = std::env::temp_dir().join(format!(
        "rediscope-edges-{}-{}-{name}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// ---- a disposable target server ----------------------------------------------

struct Target {
    child: Child,
    port: u16,
    dir: PathBuf,
}

impl Drop for Target {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Target {
    /// A fresh `redis-server` on a free port, or `None` when live tests are
    /// off or the binary is missing.
    async fn start() -> Option<Self> {
        source_port()?;
        let dir = scratch("server");
        for _ in 0..5 {
            let port = TcpListener::bind(("127.0.0.1", 0))
                .unwrap()
                .local_addr()
                .unwrap()
                .port();
            let child = match Command::new("redis-server")
                .args(["--port", &port.to_string()])
                .args(["--bind", "127.0.0.1", "--save", "", "--appendonly", "no"])
                .arg("--dir")
                .arg(&dir)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
            {
                Ok(child) => child,
                Err(e) => {
                    eprintln!("skipped: cannot start redis-server: {e}");
                    return None;
                }
            };
            let mut target = Self {
                child,
                port,
                dir: dir.clone(),
            };
            for _ in 0..250 {
                if let Ok(mut c) = redis::Client::open(("127.0.0.1", port))
                    .unwrap()
                    .get_multiplexed_async_connection()
                    .await
                    && redis::cmd("PING")
                        .query_async::<String>(&mut c)
                        .await
                        .is_ok()
                {
                    return Some(target);
                }
                if let Ok(Some(_)) = target.child.try_wait() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            // Lost the port to someone else: try another. Keep the directory.
            let _ = target.child.kill();
            let _ = target.child.wait();
            target.dir = scratch("unused");
        }
        panic!("redis-server did not start");
    }

    async fn raw(&self) -> MultiplexedConnection {
        redis::Client::open(("127.0.0.1", self.port))
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap()
    }

    fn profile(&self, name: &str) -> Connection {
        Connection {
            name: name.into(),
            host: "127.0.0.1".into(),
            port: self.port,
            ..Default::default()
        }
    }

    async fn client(&self, name: &str) -> Client {
        Client::connect(self.profile(name)).await.unwrap()
    }

    async fn flush(&self) {
        let _: () = redis::cmd("FLUSHALL")
            .query_async(&mut self.raw().await)
            .await
            .unwrap();
    }

    /// Load a file with the real `redis-cli`, returning what it printed.
    fn redis_cli(&self, file: &Path) -> String {
        let out = Command::new("redis-cli")
            .args(["-p", &self.port.to_string()])
            .stdin(std::fs::File::open(file).unwrap())
            .output()
            .expect("redis-cli must be installed");
        assert!(out.status.success(), "{out:?}");
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    /// Calls per command since the last `CONFIG RESETSTAT`.
    async fn command_calls(&self) -> BTreeMap<String, u64> {
        let info: String = redis::cmd("INFO")
            .arg("commandstats")
            .query_async(&mut self.raw().await)
            .await
            .unwrap();
        info.lines()
            .filter_map(|l| {
                let (name, rest) = l.strip_prefix("cmdstat_")?.split_once(':')?;
                let calls = rest
                    .strip_prefix("calls=")?
                    .split(',')
                    .next()?
                    .parse()
                    .ok()?;
                Some((name.to_string(), calls))
            })
            .collect()
    }

    async fn reset_stats(&self) {
        let _: () = redis::cmd("CONFIG")
            .arg("RESETSTAT")
            .query_async(&mut self.raw().await)
            .await
            .unwrap();
    }
}

/// Every command that can change data, as `INFO commandstats` names them.
const WRITES: &[&str] = &[
    "set",
    "hset",
    "hmset",
    "rpush",
    "lpush",
    "sadd",
    "zadd",
    "xadd",
    "pfadd",
    "del",
    "unlink",
    "pexpire",
    "expire",
    "restore",
    "vadd",
    "mset",
    "flushall",
    "flushdb",
    "config|set",
    "eval",
    "multi",
    "exec",
    "select",
];

async fn assert_no_writes(target: &Target, what: &str) {
    let calls = target.command_calls().await;
    for w in WRITES {
        assert!(
            !calls.contains_key(*w),
            "{what}: the server ran {w}: {calls:?}"
        );
    }
}

async fn source() -> MultiplexedConnection {
    redis::Client::open(format!(
        "redis://127.0.0.1:{}/{SOURCE_DB}",
        source_port().unwrap()
    ))
    .unwrap()
    .get_multiplexed_async_connection()
    .await
    .unwrap()
}

async fn source_client() -> Client {
    Client::connect(Connection {
        name: "edges-source".into(),
        host: "127.0.0.1".into(),
        port: source_port().unwrap(),
        db: SOURCE_DB,
        ..Default::default()
    })
    .await
    .unwrap()
}

async fn scan_all(raw: &mut MultiplexedConnection, pattern: &str) -> Vec<Vec<u8>> {
    let mut cursor = 0u64;
    let mut keys = std::collections::BTreeSet::new();
    loop {
        let (next, batch): (u64, Vec<Vec<u8>>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(1000)
            .query_async(raw)
            .await
            .unwrap();
        keys.extend(batch);
        cursor = next;
        if cursor == 0 {
            return keys.into_iter().collect();
        }
    }
}

async fn clear_source(prefix: &str) {
    let mut raw = source().await;
    for key in scan_all(&mut raw, &format!("{prefix}*")).await {
        let _: i64 = raw.del(key).await.unwrap();
    }
}

async fn supports(raw: &mut MultiplexedConnection, command: &str) -> bool {
    let info: redis::Value = redis::cmd("COMMAND")
        .arg("INFO")
        .arg(command)
        .query_async(raw)
        .await
        .unwrap_or(redis::Value::Nil);
    matches!(info, redis::Value::Array(ref items) if items.iter().any(|i| *i != redis::Value::Nil))
}

async fn rdb_compatible(a: &mut MultiplexedConnection, b: &mut MultiplexedConnection) -> bool {
    async fn version(c: &mut MultiplexedConnection) -> String {
        let info: String = redis::cmd("INFO")
            .arg("server")
            .query_async(c)
            .await
            .unwrap();
        info.lines()
            .find_map(|l| l.strip_prefix("redis_version:"))
            .unwrap_or_default()
            .trim()
            .to_string()
    }
    version(a).await == version(b).await
}

// ---- comparing what two servers hold, without rediscope's reader ----------------

#[derive(Debug, PartialEq)]
struct Held {
    kind: String,
    value: Vec<u8>,
    pttl: i64,
}

fn frame(parts: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for p in parts {
        out.extend_from_slice(&(p.len() as u64).to_le_bytes());
        out.extend_from_slice(p);
    }
    out
}

/// Every key matching `pattern`, read with plain Redis commands.
async fn holdings(raw: &mut MultiplexedConnection, pattern: &str) -> BTreeMap<Vec<u8>, Held> {
    holdings_with(raw, pattern, true).await
}

/// `quantization: false` leaves out a vector set's quantization and vectors,
/// which a CSV file does not carry.
async fn holdings_with(
    raw: &mut MultiplexedConnection,
    pattern: &str,
    quantization: bool,
) -> BTreeMap<Vec<u8>, Held> {
    let mut out = BTreeMap::new();
    for key in scan_all(raw, pattern).await {
        let kind: String = redis::cmd("TYPE").arg(&key).query_async(raw).await.unwrap();
        let pttl: i64 = raw.pttl(&key).await.unwrap();
        let value = match kind.as_str() {
            "string" => {
                let v: Vec<u8> = raw.get(&key).await.unwrap();
                // PFCOUNT rewrites a HyperLogLog's cached count, so its bytes
                // are not stable: compare what it counts.
                if v.starts_with(b"HYLL") {
                    let n: i64 = raw.pfcount(&key).await.unwrap();
                    format!("hll:{n}").into_bytes()
                } else {
                    v
                }
            }
            "hash" => {
                let flat: Vec<Vec<u8>> = redis::cmd("HGETALL")
                    .arg(&key)
                    .query_async(raw)
                    .await
                    .unwrap();
                let mut pairs: Vec<Vec<u8>> = flat.chunks(2).map(frame).collect();
                pairs.sort();
                frame(&pairs)
            }
            "list" => frame(&raw.lrange::<_, Vec<Vec<u8>>>(&key, 0, -1).await.unwrap()),
            "set" => {
                let mut m: Vec<Vec<u8>> = raw.smembers(&key).await.unwrap();
                m.sort();
                frame(&m)
            }
            // Scores as the server prints them, so -0 and 1e-300 compare exactly.
            "zset" => frame(
                &redis::cmd("ZRANGE")
                    .arg(&key)
                    .arg(0)
                    .arg(-1)
                    .arg("WITHSCORES")
                    .query_async::<Vec<Vec<u8>>>(raw)
                    .await
                    .unwrap(),
            ),
            "stream" => {
                let v: redis::Value = redis::cmd("XRANGE")
                    .arg(&key)
                    .arg("-")
                    .arg("+")
                    .query_async(raw)
                    .await
                    .unwrap();
                format!("{v:?}").into_bytes()
            }
            "vectorset" => {
                let members: Vec<Vec<u8>> = redis::cmd("VRANGE")
                    .arg(&key)
                    .arg("-")
                    .arg("+")
                    .arg(-1)
                    .query_async(raw)
                    .await
                    .unwrap();
                let info: redis::Value = redis::cmd("VINFO")
                    .arg(&key)
                    .query_async(raw)
                    .await
                    .unwrap();
                let quant = quantization && format!("{info:?}").contains("\"f32\"");
                let mut parts = vec![vec![u8::from(quant)]];
                for m in members {
                    let emb: redis::Value = redis::cmd("VEMB")
                        .arg(&key)
                        .arg(&m)
                        .query_async(raw)
                        .await
                        .unwrap();
                    let attr: Option<Vec<u8>> = redis::cmd("VGETATTR")
                        .arg(&key)
                        .arg(&m)
                        .query_async(raw)
                        .await
                        .unwrap();
                    parts.push(m);
                    // Only a full-precision set keeps its vectors exactly.
                    if quant {
                        parts.push(format!("{emb:?}").into_bytes());
                    }
                    parts.push(attr.unwrap_or_default());
                }
                frame(&parts)
            }
            _ => redis::cmd("DUMP")
                .arg(&key)
                .query_async::<Vec<u8>>(raw)
                .await
                .unwrap(),
        };
        out.insert(key, Held { kind, value, pttl });
    }
    out
}

fn assert_same_holdings(
    expected: &BTreeMap<Vec<u8>, Held>,
    actual: &BTreeMap<Vec<u8>, Held>,
    what: &str,
) {
    let problems = diff_holdings(expected, actual, what);
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// Every difference between two servers' keys, TTLs within 15 seconds.
fn diff_holdings(
    expected: &BTreeMap<Vec<u8>, Held>,
    actual: &BTreeMap<Vec<u8>, Held>,
    what: &str,
) -> Vec<String> {
    let names = |m: &BTreeMap<Vec<u8>, Held>| m.keys().map(|k| encode_key(k)).collect::<Vec<_>>();
    if names(expected) != names(actual) {
        return vec![format!(
            "{what}: key names\n expected {:?}\n   actual {:?}",
            names(expected),
            names(actual)
        )];
    }
    let mut problems = Vec::new();
    for (key, e) in expected {
        let a = &actual[key];
        let name = encode_key(key);
        if e.kind != a.kind {
            problems.push(format!("{what} {name}: type {} vs {}", e.kind, a.kind));
            continue;
        }
        if e.value != a.value {
            let show = |v: &[u8]| {
                if v.len() < 4096 {
                    format!("{:?}", String::from_utf8_lossy(v))
                } else {
                    format!("{} bytes", v.len())
                }
            };
            problems.push(format!(
                "{what} {name}: {} value differs\n expected {}\n   actual {}",
                e.kind,
                show(&e.value),
                show(&a.value)
            ));
        }
        match (e.pttl, a.pttl) {
            (-1, -1) => {}
            (x, y) if x >= 0 && y >= 0 && (x - y).abs() < 15_000 => {}
            other => problems.push(format!("{what} {name}: pttl {other:?}")),
        }
    }
    problems
}

// ---- seeding ----------------------------------------------------------------------

fn k(prefix: &str, rest: impl AsRef<[u8]>) -> Vec<u8> {
    [prefix.as_bytes(), rest.as_ref()].concat()
}

/// Keys with every awkward name, value and size. Returns how many.
async fn seed(raw: &mut MultiplexedConnection, prefix: &str) -> usize {
    let mut n = 0;
    let bin: Vec<u8> = (0..=255u8).collect();
    let mut set = |key: Vec<u8>, value: Vec<u8>| {
        n += 1;
        (key, value)
    };
    let strings = vec![
        set(k(prefix, "str:empty"), Vec::new()),
        set(
            k(prefix, "str:huge"),
            (0..1_048_576u32).map(|i| (i * 7 % 256) as u8).collect(),
        ),
        set(k(prefix, "str:bin"), bin.clone()),
        set(k(prefix, "str:b64"), b"base64:QUJD".to_vec()),
        set(k(prefix, "str:b64-bare"), b"base64:".to_vec()),
        set(
            k(prefix, "str:json-marker"),
            br#"{"base64":"QUJD"}"#.to_vec(),
        ),
        set(
            k(prefix, "str:quotes"),
            br#"say "hi" 'there' back\slash \x41 \n literal"#.to_vec(),
        ),
        set(
            k(prefix, "str:ctrl"),
            b"\0\t\r\n\x07\x08\x0b\x0c\x1b\x7f".to_vec(),
        ),
        set(k(prefix, "str:hash"), b"# not a comment".to_vec()),
        set(k(prefix, "str:csv-header"), CSV_HEADER.as_bytes().to_vec()),
        set(k(prefix, "str:neg-zero"), b"-0".to_vec()),
        set(k(prefix, "name\nwith newline"), b"v".to_vec()),
        set(k(prefix, "name \"double\" 'single'"), b"v".to_vec()),
        set(k(prefix, "name\\back\\slash"), b"v".to_vec()),
        set(
            k(prefix, "name \u{43a}\u{43b}\u{44e}\u{447} \u{1F511}"),
            b"v".to_vec(),
        ),
        set(k(prefix, "name with spaces"), b"v".to_vec()),
        set(k(prefix, "name\\x41 literal"), b"v".to_vec()),
        set(k(prefix, [0x00, 0xff, 0x80, b'\r']), b"v".to_vec()),
        set(k(prefix, "#name"), b"v".to_vec()),
        set(k(prefix, "base64:name"), b"v".to_vec()),
        set(k(prefix, "[bracket"), b"v".to_vec()),
        set(k(prefix, "{brace}"), b"v".to_vec()),
        set(k(prefix, "a,b,\"c\""), b"x,y".to_vec()),
    ];
    for (key, value) in strings {
        let _: () = raw.set(key, value).await.unwrap();
    }

    let _: () = redis::cmd("SET")
        .arg(k(prefix, "ttl:string"))
        .arg("expires")
        .arg("PX")
        .arg(3_600_000)
        .query_async(raw)
        .await
        .unwrap();
    n += 1;

    // Hashes.
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (vec![0xff], b"non-utf8 field".to_vec()),
        (vec![0xfe, 0x00], vec![0x80]),
        (b"a".to_vec(), b"1".to_vec()),
        (b"a ".to_vec(), b"2".to_vec()),
        (b"A".to_vec(), b"3".to_vec()),
        (Vec::new(), b"empty field name".to_vec()),
        (b"base64:x".to_vec(), bin.clone()),
        (b"key,type".to_vec(), b"line\nbreak".to_vec()),
    ];
    let _: () = raw
        .hset_multiple(k(prefix, "hash:odd"), &pairs)
        .await
        .unwrap();
    let big: Vec<(String, Vec<u8>)> = (0..10_000)
        .map(|i| {
            let v = if i % 97 == 0 {
                vec![0xff, i as u8]
            } else {
                format!("v{i}").into_bytes()
            };
            (format!("f{i}"), v)
        })
        .collect();
    for chunk in big.chunks(2_000) {
        let _: () = raw
            .hset_multiple(k(prefix, "hash:big"), chunk)
            .await
            .unwrap();
    }
    let _: i64 = raw.expire(k(prefix, "hash:big"), 7200).await.unwrap();
    n += 2;

    // Lists: duplicates and order, one exactly at the reader's step, one over.
    let _: () = raw
        .rpush(
            k(prefix, "list:dups"),
            &[
                b"a".to_vec(),
                b"a".to_vec(),
                Vec::new(),
                b"b".to_vec(),
                b"a".to_vec(),
                bin.clone(),
            ],
        )
        .await
        .unwrap();
    for (name, len) in [("list:2000", 2000), ("list:2001", 2001)] {
        let items: Vec<String> = (0..len).map(|i| format!("{}", i % 7)).collect();
        let _: () = raw.rpush(k(prefix, name), &items).await.unwrap();
    }
    n += 3;

    // Sets.
    let _: () = raw
        .sadd(
            k(prefix, "set:odd"),
            &[Vec::new(), bin.clone(), b"base64:x".to_vec(), b"x".to_vec()],
        )
        .await
        .unwrap();
    let members: Vec<String> = (0..1501).map(|i| format!("m{i}")).collect();
    let _: () = raw.sadd(k(prefix, "set:big"), &members).await.unwrap();
    n += 2;

    // Sorted sets: infinities, negative zero, tiny and huge scores, ties.
    let mut z = redis::cmd("ZADD");
    z.arg(k(prefix, "zset:odd"));
    for (score, member) in [
        ("inf", b"plus-inf".to_vec()),
        ("-inf", b"minus-inf".to_vec()),
        ("-0", b"neg-zero".to_vec()),
        ("0", b"zero".to_vec()),
        ("1e-300", b"tiny".to_vec()),
        ("5e-324", b"subnormal".to_vec()),
        ("1.7976931348623157e308", b"max".to_vec()),
        ("0.1", b"tenth".to_vec()),
        ("1", b"tie-b".to_vec()),
        ("1", b"tie-a".to_vec()),
        ("1", b"tie-c".to_vec()),
        ("-2.5", vec![0xff, 0x00]),
        ("123456789.123456789", b"long".to_vec()),
    ] {
        z.arg(score).arg(member);
    }
    let _: i64 = z.query_async(raw).await.unwrap();
    for (name, len) in [("zset:1000", 1000), ("zset:ties", 2501)] {
        let scored: Vec<(f64, String)> = (0..len).map(|i| (7.0, format!("m{i:05}"))).collect();
        let _: () = raw.zadd_multiple(k(prefix, name), &scored).await.unwrap();
    }
    n += 3;

    // Streams: several and repeated fields, binary names, sequences over zero,
    // the largest id there is, and more entries than one read.
    for (id, fields) in [
        (
            "1-1",
            vec![
                (b"f".to_vec(), b"v".to_vec()),
                (b"f".to_vec(), b"w".to_vec()),
            ],
        ),
        (
            "1-2",
            vec![(vec![0xff, b':'], bin.clone()), (Vec::new(), Vec::new())],
        ),
        ("5-3", vec![(b"a:b".to_vec(), b"c,d\n\"e\"".to_vec())]),
        (
            "18446744073709551615-18446744073709551615",
            vec![(b"last".to_vec(), b"id".to_vec())],
        ),
    ] {
        let mut cmd = redis::cmd("XADD");
        cmd.arg(k(prefix, "stream:odd")).arg(id);
        for (f, v) in fields {
            cmd.arg(f).arg(v);
        }
        let _: String = cmd.query_async(raw).await.unwrap();
    }
    for (name, len) in [("stream:2000", 2000u64), ("stream:2500", 2500)] {
        let mut pipe = redis::pipe();
        for i in 0..len {
            pipe.cmd("XADD")
                .arg(k(prefix, name))
                .arg(format!("{}-{}", 1 + i / 600, 1 + i % 600))
                .arg("n")
                .arg(i)
                .arg("bin")
                .arg(&[0xfe, i as u8][..]);
        }
        let _: Vec<String> = pipe.query_async(raw).await.unwrap();
    }
    let _: i64 = raw
        .pexpire(k(prefix, "stream:odd"), 5_000_000)
        .await
        .unwrap();
    n += 3;

    // A HyperLogLog is a string to Redis.
    let elements: Vec<String> = (0..1000).map(|i| format!("e{i}")).collect();
    let _: () = raw.pfadd(k(prefix, "hll"), &elements).await.unwrap();
    n += 1;

    if supports(raw, "VRANGE").await {
        let mut pipe = redis::pipe();
        for i in 0..1100u32 {
            let mut element = format!("el{i}").into_bytes();
            if i % 50 == 0 {
                element.push(0xff);
            }
            pipe.cmd("VADD")
                .arg(k(prefix, "vset:f32"))
                .arg("VALUES")
                .arg(2)
                .arg(f64::from(i) * 0.5)
                .arg(-0.25)
                .arg(element)
                .arg("NOQUANT");
            if i % 3 == 0 {
                pipe.arg("SETATTR")
                    .arg(format!(r#"{{"i":{i},"s":"a,\"b\""}}"#));
            }
        }
        let _: Vec<i64> = pipe.query_async(raw).await.unwrap();
        for (el, v) in [("x", [1.0, 2.0]), ("y", [0.5, -0.25]), ("-", [3.0, 4.0])] {
            let _: i64 = redis::cmd("VADD")
                .arg(k(prefix, "vset:q8"))
                .arg("VALUES")
                .arg(2)
                .arg(v[0])
                .arg(v[1])
                .arg(el)
                .query_async(raw)
                .await
                .unwrap();
        }
        n += 2;
    } else {
        eprintln!("skipped vector sets: the server has no VRANGE");
    }
    n
}

async fn export_names(c: &Client, prefix: &str) -> Vec<String> {
    let (keys, truncated) = c.scan_keys(&format!("{prefix}*"), 50_000).await.unwrap();
    assert!(!truncated);
    keys.into_iter().map(|k| k.name).collect()
}

async fn export(c: &Client, names: &[String], format: Format, replace: bool) -> Vec<u8> {
    let (report, bytes) = c
        .export_to(names, format, replace, Vec::new())
        .await
        .unwrap();
    assert!(report.skipped.is_empty(), "{:?}", report.skipped);
    bytes
}

// ---- round trips ----------------------------------------------------------------

#[tokio::test]
async fn every_awkward_value_round_trips_into_another_server_in_every_format() {
    let prefix = "edge:rt:";
    let Some(target) = Target::start().await else {
        return;
    };
    clear_source(prefix).await;
    let mut raw = source().await;
    let count = seed(&mut raw, prefix).await;
    let expected = holdings(&mut raw, &format!("{prefix}*")).await;
    assert_eq!(expected.len(), count);
    let pfcount: i64 = raw.pfcount(k(prefix, "hll")).await.unwrap();

    let src = source_client().await;
    let names = export_names(&src, prefix).await;
    assert_eq!(names.len(), count);
    let dir = scratch("rt");
    let mut target_raw = target.raw().await;
    let same_version = rdb_compatible(&mut raw, &mut target_raw).await;
    let client = target.client("edges-rt").await;
    let pattern = format!("{prefix}*");
    let loose = holdings_with(&mut raw, &pattern, false).await;
    // Every format is tried before failing, so one report shows them all.
    let mut problems = Vec::new();
    for format in Format::ALL {
        if format == Format::Dump && !same_version {
            eprintln!("skipped dump: the servers have different versions");
            continue;
        }
        let bytes = export(&src, &names, format, false).await;
        target.flush().await;
        let parsed = parse(&bytes).unwrap();
        assert_eq!(parsed.format(), format);
        let report = client.import_parsed(&parsed, false).await.unwrap();
        // A commands file counts the distinct keys its lines write.
        assert_eq!(report.keys as usize, count, "{format:?}");
        if format == Format::Csv {
            // CSV has no column for a vector set's quantization.
            let actual = holdings_with(&mut target_raw, &pattern, false).await;
            problems.extend(diff_holdings(&loose, &actual, format.name()));
        } else {
            let actual = holdings(&mut target_raw, &pattern).await;
            problems.extend(diff_holdings(&expected, &actual, format.name()));
        }
        let n: i64 = target_raw.pfcount(k(prefix, "hll")).await.unwrap();
        assert_eq!(n, pfcount, "{format:?}: the HyperLogLog still counts");

        // The same file through real redis-cli, when it is one it can read.
        if format == Format::Commands {
            for replace in [false, true] {
                let bytes = export(&src, &names, format, replace).await;
                let file = dir.join(format!("all-{replace}.redis"));
                std::fs::write(&file, &bytes).unwrap();
                target.flush().await;
                let printed = target.redis_cli(&file);
                assert!(
                    !printed.contains("ERR") && !printed.contains("Invalid argument"),
                    "{printed}"
                );
                let actual = holdings(&mut target_raw, &pattern).await;
                problems.extend(diff_holdings(&expected, &actual, "redis-cli"));
            }
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
    let _ = std::fs::remove_dir_all(dir);
    clear_source(prefix).await;
}

#[tokio::test]
async fn keys_expiring_during_an_export_never_leave_a_broken_entry() {
    let prefix = "edge:expiring:";
    let Some(target) = Target::start().await else {
        return;
    };
    clear_source(prefix).await;
    let src = source_client().await;
    let client = target.client("edges-expiring").await;
    for round in 0..6u64 {
        let mut raw = source().await;
        let mut pipe = redis::pipe();
        for i in 0..150u64 {
            let ms = 1 + (i * 7 + round * 13) % 40;
            let key = format!("{prefix}{i}");
            match i % 5 {
                0 => {
                    pipe.cmd("SET").arg(&key).arg("v").arg("PX").arg(ms);
                }
                1 => {
                    pipe.cmd("HSET").arg(&key).arg("f").arg("v");
                    pipe.cmd("PEXPIRE").arg(&key).arg(ms);
                }
                2 => {
                    pipe.cmd("RPUSH").arg(&key).arg("a").arg("b");
                    pipe.cmd("PEXPIRE").arg(&key).arg(ms);
                }
                3 => {
                    pipe.cmd("ZADD").arg(&key).arg(1).arg("m");
                    pipe.cmd("PEXPIRE").arg(&key).arg(ms);
                }
                _ => {
                    pipe.cmd("XADD").arg(&key).arg("*").arg("f").arg("v");
                    pipe.cmd("PEXPIRE").arg(&key).arg(ms);
                }
            }
        }
        let _: redis::Value = pipe.query_async(&mut raw).await.unwrap();
        let names: Vec<String> = (0..150).map(|i| format!("{prefix}{i}")).collect();
        let format = [
            Format::Json,
            Format::Jsonl,
            Format::Csv,
            Format::Commands,
            Format::Dump,
        ][round as usize % 5];
        let bytes = export(&src, &names, format, false).await;
        let parsed = parse(&bytes).unwrap_or_else(|e| panic!("{format:?}: {e}"));
        if let Parsed::Records(records, _) = &parsed {
            for r in records {
                let empty = match &r.value {
                    Value::Hash(v) => v.is_empty(),
                    Value::List(v) | Value::Set(v) => v.is_empty(),
                    Value::ZSet(v) => v.is_empty(),
                    _ => false,
                };
                assert!(
                    !empty,
                    "{format:?}: {} was written empty",
                    encode_key(&r.key)
                );
                assert!(r.ttl_ms.is_none_or(|t| t >= 0), "{format:?}: {r:?}");
            }
        }
        target.flush().await;
        // Whatever made it into the file loads; a key that runs out on the way
        // simply is not there.
        let result = client.import_parsed(&parsed, false).await;
        if format != Format::Dump {
            result.unwrap_or_else(|e| panic!("{format:?}: {e}"));
        }
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    clear_source(prefix).await;
}

// ---- commands files ------------------------------------------------------------

#[test]
fn commands_split_collections_at_exactly_the_chunk_size() {
    let key = b"k".to_vec();
    let items = |n: usize| {
        (0..n)
            .map(|i| i.to_string().into_bytes())
            .collect::<Vec<_>>()
    };
    let cases = [
        (Value::List(items(CHUNK)), 1, 2 + CHUNK),
        (Value::List(items(CHUNK + 1)), 2, 3),
        (Value::Set(items(2 * CHUNK)), 2, 2 + CHUNK),
        (
            Value::Hash(items(CHUNK).into_iter().map(|i| (i.clone(), i)).collect()),
            1,
            2 + 2 * CHUNK,
        ),
        (
            Value::Hash(
                items(CHUNK + 1)
                    .into_iter()
                    .map(|i| (i.clone(), i))
                    .collect(),
            ),
            2,
            4,
        ),
        (
            Value::ZSet(items(CHUNK + 1).into_iter().map(|i| (i, 1.0)).collect()),
            2,
            4,
        ),
    ];
    for (value, writes, last_len) in cases {
        let name = value.type_name();
        let record = Record {
            key: key.clone(),
            ttl_ms: Some(5000),
            value,
        };
        let plain = commands(&record, false);
        assert_eq!(plain.len(), writes + 1, "{name}");
        assert_eq!(plain[writes - 1].len(), last_len, "{name}");
        assert_eq!(
            plain[writes],
            [b"PEXPIRE".to_vec(), key.clone(), b"5000".to_vec()]
        );
        let replaced = commands(&record, true);
        // DEL once, first, and nothing else changes.
        assert_eq!(replaced[0], [b"DEL".to_vec(), key.clone()]);
        assert_eq!(&replaced[1..], &plain[..]);
        assert_eq!(
            replaced.iter().filter(|c| c[0] == b"DEL").count(),
            1,
            "{name}"
        );
    }
    // An empty collection writes nothing, not even the DEL or the TTL.
    let empty = Record {
        key: key.clone(),
        ttl_ms: Some(5),
        value: Value::List(Vec::new()),
    };
    assert!(commands(&empty, false).is_empty());
    assert_eq!(commands(&empty, true), [vec![b"DEL".to_vec(), key]]);
}

#[tokio::test]
async fn redis_cli_and_rediscope_agree_at_the_chunk_boundaries() {
    let prefix = "edge:chunk:";
    let Some(target) = Target::start().await else {
        return;
    };
    let mut records = Vec::new();
    for n in [CHUNK - 1, CHUNK, CHUNK + 1, 2 * CHUNK, 2 * CHUNK + 1] {
        let items: Vec<Vec<u8>> = (0..n).map(|i| format!("i{i}").into_bytes()).collect();
        records.push(Record {
            key: k(prefix, format!("list:{n}")),
            ttl_ms: None,
            value: Value::List(items.clone()),
        });
        records.push(Record {
            key: k(prefix, format!("hash:{n}")),
            ttl_ms: Some(100_000),
            value: Value::Hash(items.iter().map(|i| (i.clone(), i.clone())).collect()),
        });
        records.push(Record {
            key: k(prefix, format!("zset:{n}")),
            ttl_ms: None,
            value: Value::ZSet(items.iter().map(|i| (i.clone(), 2.0)).collect()),
        });
    }
    let mut w = transfer::Writer::new(Vec::new(), Format::Commands, true).unwrap();
    for r in &records {
        w.write(r).unwrap();
    }
    let bytes = w.finish().unwrap();
    let text = String::from_utf8(bytes.clone()).unwrap();
    assert_eq!(
        text.lines().filter(|l| l.starts_with("RPUSH ")).count(),
        1 + 1 + 2 + 2 + 3
    );
    // Each key: DEL, its writes, then the TTL, never interleaved with another key.
    let mut seen = Vec::new();
    for line in text.lines() {
        let args = split_line(line.as_bytes()).unwrap();
        if args[0] == b"DEL" {
            assert!(!seen.contains(&args[1]), "DEL comes once, first");
            seen.push(args[1].clone());
        } else {
            assert_eq!(seen.last(), Some(&args[1]), "{line:.40}");
        }
    }

    let dir = scratch("chunk");
    let file = dir.join("chunk.redis");
    std::fs::write(&file, &bytes).unwrap();
    let mut raw = target.raw().await;
    let printed = target.redis_cli(&file);
    assert!(!printed.contains("ERR"), "{printed}");
    let by_cli = holdings(&mut raw, &format!("{prefix}*")).await;
    target.flush().await;
    let report = target
        .client("edges-chunk")
        .await
        .import_parsed(&parse(&bytes).unwrap(), false)
        .await
        .unwrap();
    assert_eq!(report.keys, records.len() as u64);
    let by_rediscope = holdings(&mut raw, &format!("{prefix}*")).await;
    assert_same_holdings(&by_cli, &by_rediscope, "chunks");
    assert_eq!(by_cli.len(), records.len());
    let len: i64 = raw.llen(k(prefix, "list:1001")).await.unwrap();
    assert_eq!(len, 1001);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn quoting_survives_every_awkward_argument() {
    let samples: Vec<&[u8]> = vec![
        b"\"",
        b"\\",
        b"\\\"",
        b"line\nbreak",
        b"tab\there",
        b"nul\0byte",
        b"\\x41",
        b"\\x4",
        b"\\xZZ",
        b"#comment",
        b"",
        b" ",
        b"'",
        b"'single'",
        b"base64:QUJD",
        b"\r",
        b"\x0b\x0c",
        "\u{85}\u{2028}".as_bytes(),
        &[0xc3],
        &[0xed, 0xa0, 0x80],
        &[0xf0, 0x9f, 0x98],
    ];
    for s in samples {
        let quoted = quote_arg(s);
        assert!(!quoted.contains(['\n', '\r']), "{quoted}");
        assert!(
            !quoted.bytes().any(|b| b.is_ascii_control()),
            "{quoted:?} holds a raw control byte"
        );
        let line = format!("SET {quoted} {quoted}");
        assert_eq!(
            split_line(line.as_bytes()).unwrap(),
            [b"SET".to_vec(), s.to_vec(), s.to_vec()],
            "{quoted}"
        );
    }
    assert_eq!(quote_arg(b""), "\"\"");
    assert_eq!(quote_arg(b"#x"), "#x");
}

#[tokio::test]
async fn hand_written_lines_load_the_same_through_redis_cli_and_rediscope() {
    let prefix = "edge:lines:";
    let Some(target) = Target::start().await else {
        return;
    };
    let p = prefix;
    let lines = [
        format!(r#"SET {p}1 "a\x4""#),
        format!(r#"SET {p}2 "\xZZ""#),
        format!(r#"SET {p}3 'it\'s'"#),
        format!(r#"SET {p}4 'no \x41 or \n escape'"#),
        format!(r#"SET {p}5 """#),
        format!("SET {p}6 #not-a-comment"),
        format!(r#"SET {p}7 "tab\there nul\x00 bell\a bs\b cr\r""#),
        format!(r#"SET {p}8 un"quoted mid""#),
        format!("   SET   {p}9   spaced   "),
        format!("set {p}10 lower"),
        format!(r#"SET {p}11 "\\" "#),
        format!(r#"SET {p}12 "\"""#),
        format!("SET {p}13 \"\u{e9} unicode \u{1F600}\""),
        format!(r#"SET "{p}14 key with space" v"#),
        format!("SET {p}15 crlf\r"),
        format!(r"SET {p}16 \xff-raw"),
        format!(r#"SET {p}17 "\X41 \x4a\x4A""#),
        format!(r#"SET {p}18 "\q\z""#),
        format!("SET {p}19 raw\ttab-splits"),
        format!(r#"HSET {p}20 "" "" "f" 'v'"#),
        format!(r#"RPUSH {p}21 "" '' """#),
        String::new(),
        "   ".into(),
    ];
    let text = lines.join("\n");
    let dir = scratch("lines");
    let file = dir.join("hand.redis");
    std::fs::write(&file, &text).unwrap();
    let mut raw = target.raw().await;
    let printed = target.redis_cli(&file);
    let by_cli = holdings(&mut raw, &format!("{prefix}*")).await;
    target.flush().await;

    let parsed = parse(text.as_bytes()).unwrap();
    let client = target.client("edges-lines").await;
    let result = client.import_parsed(&parsed, false).await;
    let by_rediscope = holdings(&mut raw, &format!("{prefix}*")).await;
    // `SET k raw tab-splits` is a syntax error for both.
    assert!(printed.contains("ERR syntax error"), "{printed}");
    let err = result.unwrap_err().to_string();
    assert!(
        err.starts_with("line 19: ") && err.contains("syntax"),
        "{err}"
    );
    // Everything before the failing line matches, byte for byte.
    let before: BTreeMap<_, _> = by_cli
        .into_iter()
        .filter(|(key, _)| {
            let n: usize = String::from_utf8_lossy(&key[p.len()..])
                .split(' ')
                .next()
                .unwrap()
                .parse()
                .unwrap();
            n < 19
        })
        .collect();
    assert_same_holdings(&before, &by_rediscope, "hand-written");
    let v: Vec<u8> = raw.get(k(p, "7")).await.unwrap();
    assert_eq!(v, b"tab\there nul\0 bell\x07 bs\x08 cr\r");
    let v: Vec<u8> = raw.get(k(p, "1")).await.unwrap();
    assert_eq!(v, b"ax4");
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn form_feeds_and_vertical_tabs_split_like_redis_cli() {
    let prefix = "edge:spaces:";
    let Some(target) = Target::start().await else {
        return;
    };
    let p = prefix;
    let text = format!(
        "\x0b\x0cSET {p}1 a\x0cb\x0b\nSET {p}2 \"x\"\x0c\x0b\nSET\t{p}3\x0b 'y'\x0b\nRPUSH {p}4 \x0c\"a\"\x0b\x0c'b' c\x0bd\n"
    );
    let dir = scratch("spaces");
    let file = dir.join("spaces.redis");
    std::fs::write(&file, &text).unwrap();
    let mut raw = target.raw().await;
    let printed = target.redis_cli(&file);
    assert!(!printed.contains("ERR"), "{printed}");
    let by_cli = holdings(&mut raw, &format!("{prefix}*")).await;
    assert_eq!(by_cli.len(), 4);
    target.flush().await;
    let client = target.client("edges-spaces").await;
    client
        .import_parsed(&parse(text.as_bytes()).unwrap(), false)
        .await
        .unwrap();
    let by_rediscope = holdings(&mut raw, &format!("{prefix}*")).await;
    assert_same_holdings(&by_cli, &by_rediscope, "spaces");
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn non_data_commands_are_refused_before_anything_is_sent() {
    let Some(target) = Target::start().await else {
        return;
    };
    let mut raw = target.raw().await;
    let _: () = raw.set("keep", "me").await.unwrap();
    let client = target.client("edges-refuse").await;
    let valid: String = (0..600).map(|i| format!("SET k{i} v\n")).collect();
    for bad in [
        "FLUSHALL",
        "flushall",
        "  FlushDB  ",
        "\"FLUSHALL\"",
        "CONFIG SET maxmemory 1",
        "EVAL \"return redis.call('FLUSHALL')\" 0",
        "FCALL f 0",
        "MULTI",
        "EXEC",
        "SELECT 1",
        "SWAPDB 0 1",
        "RENAME keep other",
        "COPY keep other",
        "SORT keep STORE other",
        "SHUTDOWN",
        "DEBUG SLEEP 0",
        "MIGRATE 127.0.0.1 1 keep 0 1",
        "# a comment",
        "SET\0 k v",
    ] {
        target.reset_stats().await;
        for file in [
            format!("{bad}\n{valid}"),
            format!("{valid}{bad}"),
            format!("{valid}{bad}\r\n{valid}"),
        ] {
            let err = parse(file.as_bytes()).unwrap_err().to_string();
            assert!(
                err.contains("is not a data command, so nothing was imported"),
                "{bad}: {err}"
            );
            let line = if file.starts_with(bad) { 1 } else { 601 };
            assert!(err.starts_with(&format!("line {line}: ")), "{bad}: {err}");
        }
        // The CLI refuses the same way, and the server never hears of it.
        let dir = scratch("refuse");
        let path = dir.join("bad.redis");
        std::fs::write(&path, format!("{valid}{bad}\n")).unwrap();
        let out = cli(
            &["-p", &target.port.to_string()],
            &["import", path.to_str().unwrap()],
        );
        assert_eq!(out.status.code(), Some(1), "{bad}: {out:?}");
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("not a data command"),
            "{out:?}"
        );
        let _ = std::fs::remove_dir_all(dir);
        assert_no_writes(&target, bad).await;
    }
    let size: i64 = redis::cmd("DBSIZE").query_async(&mut raw).await.unwrap();
    assert_eq!(size, 1);
    let kept: String = raw.get("keep").await.unwrap();
    assert_eq!(kept, "me");
    drop(client);
}

#[tokio::test]
async fn a_failing_line_is_named_exactly_and_stops_the_import() {
    let Some(target) = Target::start().await else {
        return;
    };
    let mut raw = target.raw().await;
    let client = target.client("edges-batch").await;
    let file = "SET a 1\n\
                RPUSH l x\n\
                \n\
                RPUSH l y\n\
                HSET l f v\n\
                RPUSH l w\n\
                SET b 2\n\
                SET c 3\n";
    let err = client
        .import_parsed(&parse(file.as_bytes()).unwrap(), true)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.starts_with("line 5: "), "{err}");
    assert!(err.contains("WRONGTYPE"), "{err}");
    assert!(err.contains("3 command(s) before it ran"), "{err}");
    // The lines for one key go out together, so the one after the error in
    // that batch ran too, and the error says so.
    assert!(err.contains("and 1 command(s) after it ran too"), "{err}");
    let b: Option<String> = raw.get("b").await.unwrap();
    assert_eq!(b, None, "nothing after the failing batch runs");
    let l: Vec<String> = raw.lrange("l", 0, -1).await.unwrap();
    assert_eq!(l, ["x", "y", "w"]);

    // A single failing line is named alone, with the count before it.
    target.flush().await;
    let file = "SET a 1\nSET b 2\nRPUSH a x\nSET c 3\n";
    let err = client
        .import_parsed(&parse(file.as_bytes()).unwrap(), false)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.starts_with("line 3: "), "{err}");
    assert!(err.contains("2 command(s) before it ran"), "{err}");
    assert!(!err.contains("after it"), "{err}");

    // Many lines for one key go out in bounded batches, and the failing line
    // is still named exactly.
    target.flush().await;
    let mut file: String = (0..1500).map(|i| format!("RPUSH big {i}\n")).collect();
    file.push_str("HSET big f v\nSET after 1\n");
    let err = client
        .import_parsed(&parse(file.as_bytes()).unwrap(), false)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.starts_with("line 1501: "), "{err}");
    assert!(err.contains("1500 command(s) before it ran"), "{err}");
    let len: i64 = raw.llen("big").await.unwrap();
    assert_eq!(len, 1500);

    // A failure in the middle of a batch names its line, and the lines after
    // it that ran and failed.
    target.flush().await;
    let file = "RPUSH m a\nHSET m f v\nRPUSH m b\nHSET m g w\nSET after 1\n";
    let err = client
        .import_parsed(&parse(file.as_bytes()).unwrap(), false)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.starts_with("line 2: "), "{err}");
    assert!(err.contains("1 command(s) before it ran"), "{err}");
    assert!(err.contains("and 1 command(s) after it ran too"), "{err}");
    assert!(
        err.contains("1 more line(s) in that batch failed as well"),
        "{err}"
    );

    // A clean file reports its commands and the distinct keys they wrote.
    target.flush().await;
    let file = "SET a 1\nPEXPIRE a 100000\nRPUSH l x y\nSET a 2\nMSET b 1 c 2\n";
    let parsed = parse(file.as_bytes()).unwrap();
    let report = client.import_parsed(&parsed, false).await.unwrap();
    assert_eq!((report.commands, report.keys), (5, 4));
    assert_eq!(
        transfer::import_summary(parsed.format(), &report),
        "Imported 5 command(s) for 4 key(s) from redis-cli commands"
    );
    // SET clears the TTL the earlier line gave, exactly as redis-cli would.
    let ttl: i64 = raw.pttl("a").await.unwrap();
    assert_eq!(ttl, -1);
}

// ---- CSV ----------------------------------------------------------------------

#[test]
fn csv_cells_follow_rfc_4180() {
    let text = "a,\"b,c\",\"say \"\"hi\"\"\",\"two\nlines\",\"cr\r\nlf\"\r\n\
                \"\",,\"\"\"\",x\"y,\"\"\"a,b\"\"\"\n";
    let rows = parse_csv(text).unwrap();
    assert_eq!(
        rows,
        vec![
            (
                1,
                vec![
                    "a".into(),
                    "b,c".into(),
                    "say \"hi\"".into(),
                    "two\nlines".into(),
                    "cr\r\nlf".into()
                ]
            ),
            (
                4,
                vec![
                    String::new(),
                    String::new(),
                    "\"".into(),
                    "x\"y".into(),
                    "\"a,b\"".into()
                ]
            ),
        ]
    );
    // A trailing comma is an empty last cell; a last line without a newline counts.
    assert_eq!(
        parse_csv("a,").unwrap(),
        vec![(1, vec!["a".into(), String::new()])]
    );
    assert!(
        parse_csv("a,\"open\nstill open")
            .unwrap_err()
            .to_string()
            .contains("line 1")
    );
}

fn csv_import(text: &str) -> anyhow::Result<Vec<Record>> {
    match parse(text.as_bytes())? {
        Parsed::Records(records, Format::Csv) => Ok(records),
        other => panic!("not csv: {other:?}"),
    }
}

#[test]
fn csv_written_cells_read_back_exactly() {
    let awkward: Vec<Vec<u8>> = vec![
        b"a,b".to_vec(),
        b"\"quoted\"".to_vec(),
        b"line\nbreak".to_vec(),
        b"cr\rlf\r\n".to_vec(),
        b"base64:".to_vec(),
        b"base64:QUJD".to_vec(),
        b"base64".to_vec(),
        b"BASE64:QUJD".to_vec(),
        b" padded ".to_vec(),
        Vec::new(),
        vec![0xff, b','],
        CSV_HEADER.as_bytes().to_vec(),
    ];
    let records: Vec<Record> = awkward
        .iter()
        .enumerate()
        .map(|(i, v)| Record {
            key: [v.as_slice(), format!("#{i}").as_bytes()].concat(),
            ttl_ms: if i % 2 == 0 { None } else { Some(i as i64) },
            value: Value::Hash(vec![(v.clone(), v.clone())]),
        })
        .chain(std::iter::once(Record {
            key: b"list".to_vec(),
            ttl_ms: None,
            value: Value::List(awkward.clone()),
        }))
        .collect();
    let mut w = transfer::Writer::new(Vec::new(), Format::Csv, false).unwrap();
    for r in &records {
        w.write(r).unwrap();
    }
    let bytes = w.finish().unwrap();
    let text = String::from_utf8(bytes).unwrap();
    assert_eq!(csv_import(&text).unwrap(), records);
    // A file saved with CRLF row endings reads the same, as long as no cell
    // holds a line break of its own.
    let plain: Vec<Record> = records
        .iter()
        .filter(|r| !r.key.contains(&b'\n') && !r.key.contains(&b'\r'))
        .filter(|r| match &r.value {
            Value::Hash(p) => !p[0].1.contains(&b'\n') && !p[0].1.contains(&b'\r'),
            _ => false,
        })
        .cloned()
        .collect();
    let mut w = transfer::Writer::new(Vec::new(), Format::Csv, false).unwrap();
    for r in &plain {
        w.write(r).unwrap();
    }
    let lf = String::from_utf8(w.finish().unwrap()).unwrap();
    assert_eq!(csv_import(&lf.replace('\n', "\r\n")).unwrap(), plain);
    assert_eq!(
        csv_import(&format!("\u{feff}{}", lf.replace('\n', "\r\n"))).unwrap(),
        plain
    );
}

#[test]
fn csv_base64_cells_are_never_ambiguous() {
    assert_eq!(transfer::csv_cell_bytes("base64:").unwrap(), b"");
    assert_eq!(transfer::csv_cell_bytes("base64").unwrap(), b"base64");
    assert_eq!(
        transfer::csv_cell_bytes("Base64:QUJD").unwrap(),
        b"Base64:QUJD"
    );
    assert_eq!(transfer::csv_cell(b"base64:"), "base64:YmFzZTY0Og==");
    let err = csv_import("key,type,ttl_ms,field,value\nk,string,,,base64:!!!\n")
        .unwrap_err()
        .to_string();
    assert!(
        err.starts_with("line 2: ") && err.contains("base64"),
        "{err}"
    );
    let err = csv_import("key,type,ttl_ms,field,value\nk,string,,,ok\nbase64:%%,string,,,v\n")
        .unwrap_err()
        .to_string();
    assert!(err.starts_with("line 3: "), "{err}");
}

#[test]
fn csv_header_problems_are_clear_errors() {
    // Straight to the CSV reader: the header is named.
    for bad in [
        "",
        "k,string,,,v\n",
        "key,type,ttl,field,value\nk,string,,,v\n",
        "Key,Type,TTL_ms,Field,Value\n",
        "key,type,ttl_ms,field\n",
        "\"key\",type,ttl_ms,field,value,extra\n",
    ] {
        let err = csv_records(bad).unwrap_err().to_string();
        assert!(err.contains(CSV_HEADER), "{bad:?}: {err}");
    }
    // A quoted header is still the header.
    assert!(
        csv_records("\"key\",\"type\",\"ttl_ms\",\"field\",\"value\"\n")
            .unwrap()
            .is_empty()
    );
    // Through detection, a file without the header is not CSV at all, and
    // the commands reader refuses its first line.
    let err = parse(b"key,type,ttl,field,value\nk,string,,,v\n")
        .unwrap_err()
        .to_string();
    assert!(
        err.starts_with("line 1: ") && err.contains("not a data command"),
        "{err}"
    );
    let err = parse(b"key,type,ttl_ms,field,value,extra\n")
        .unwrap_err()
        .to_string();
    assert!(err.starts_with("line 1: "), "{err}");
    // A header with CRLF, or with nothing after it, is an empty import.
    for empty in [
        "key,type,ttl_ms,field,value",
        "key,type,ttl_ms,field,value\r\n",
        "\n\nkey,type,ttl_ms,field,value\n\n",
    ] {
        assert!(csv_import(empty).unwrap().is_empty(), "{empty:?}");
    }
    // Rows with the wrong number of cells name their line.
    for (row, found) in [("k,string,,v", 4), ("k,string,,,v,", 6), ("\"a,b\"", 1)] {
        let err = csv_import(&format!("{CSV_HEADER}\n{row}\n"))
            .unwrap_err()
            .to_string();
        assert_eq!(err, format!("line 2: expected 5 cells, found {found}"));
    }
    let err = csv_import(&format!("{CSV_HEADER}\nk,widget,,,v\n"))
        .unwrap_err()
        .to_string();
    assert_eq!(err, "line 2: the type 'widget' cannot be imported");
}

#[test]
fn csv_scores_and_ttls_are_checked() {
    let ok = csv_import(&format!(
        "{CSV_HEADER}\nz,zset,,a,inf\nz,zset,,b,-inf\nz,zset,,c, 2.5 \nz,zset,,d,1e-300\nz,zset,,e,-0\n\
         t,string, 1500 ,,v\nu,string,,,v\nw,string,+7,,v\n"
    ))
    .unwrap();
    let Value::ZSet(scores) = &ok[0].value else {
        panic!("{ok:?}");
    };
    let bits: Vec<u64> = scores.iter().map(|(_, s)| s.to_bits()).collect();
    assert_eq!(
        bits,
        [f64::INFINITY, f64::NEG_INFINITY, 2.5, 1e-300, -0.0].map(f64::to_bits)
    );
    assert_eq!(
        ok[1..].iter().map(|r| r.ttl_ms).collect::<Vec<_>>(),
        [Some(1500), None, Some(7)]
    );
    for (row, message) in [
        ("z,zset,,a,abc", "line 2: score 'abc' is not a number"),
        ("z,zset,,a,", "line 2: score '' is not a number"),
        ("z,zset,,a,\"1,5\"", "line 2: score '1,5' is not a number"),
        (
            "t,string,abc,,v",
            "line 2: ttl_ms 'abc' is not a whole number",
        ),
        (
            "t,string,1.5,,v",
            "line 2: ttl_ms '1.5' is not a whole number",
        ),
        (
            "t,string,99999999999999999999,,v",
            "line 2: ttl_ms '99999999999999999999' is not a whole number",
        ),
        (
            "s,stream,,no-colon,v",
            "line 2: a stream field must be id:field",
        ),
        (
            "s,string,,,a\ns,string,,,b",
            "line 3: a string key has one row",
        ),
    ] {
        let err = csv_import(&format!("{CSV_HEADER}\n{row}\n"))
            .unwrap_err()
            .to_string();
        assert_eq!(err, message, "{row}");
    }
}

#[tokio::test]
async fn a_negative_ttl_never_makes_an_imported_key_vanish() {
    // PTTL says -1 for "no expiry", and so do the `pttl` fields of a dump
    // file, so a hand-written -1 must not quietly delete the key it imports.
    let Some(target) = Target::start().await else {
        return;
    };
    let client = target.client("edges-negative-ttl").await;
    let mut raw = target.raw().await;
    for (name, file) in [
        ("csv", format!("{CSV_HEADER}\ncsv,string,-1,,v\n")),
        (
            "jsonl",
            r#"{"key":"jsonl","type":"string","ttl_ms":-1,"value":"v"}"#.to_string(),
        ),
        (
            "json",
            r#"[{"key":"json","type":"list","ttl_ms":-5000,"value":["a"]}]"#.to_string(),
        ),
    ] {
        // Refused while reading the file, or at import: either way, loudly.
        let imported = match parse(file.as_bytes()) {
            Ok(parsed) => client.import_parsed(&parsed, false).await,
            Err(e) => Err(e),
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        let exists: bool = raw.exists(name).await.unwrap();
        assert!(
            imported.is_err() || exists,
            "{name}: the import reported {imported:?}, and the key is gone"
        );
    }
}

#[tokio::test]
async fn csv_rows_for_one_key_that_are_not_together_lose_nothing() {
    let Some(target) = Target::start().await else {
        return;
    };
    let client = target.client("edges-csv-rows").await;
    let mut raw = target.raw().await;
    // Sorted by value in a spreadsheet: the rows of `l` are split by `s`.
    let file = format!("{CSV_HEADER}\nl,list,,0,a\ns,string,,,x\nl,list,,1,b\n");
    let parsed = parse(file.as_bytes()).unwrap();
    for replace in [false, true] {
        target.flush().await;
        let result = client.import_parsed(&parsed, replace).await;
        let l: Vec<String> = raw.lrange("l", 0, -1).await.unwrap();
        assert!(
            result.is_err() || l == ["a", "b"],
            "replace={replace}: the import reported {result:?}, and the list holds {l:?}"
        );
    }
}

#[tokio::test]
async fn a_nan_score_is_refused_without_touching_the_target() {
    let Some(target) = Target::start().await else {
        return;
    };
    let client = target.client("edges-nan").await;
    let mut raw = target.raw().await;
    for file in [
        format!("{CSV_HEADER}\nz,zset,,a,nan\n"),
        r#"[{"key":"z","type":"zset","value":[["a","NaN"]]}]"#.to_string(),
    ] {
        let _: () = raw.zadd("z", "kept", 1).await.unwrap();
        let err = match parse(file.as_bytes()) {
            Err(e) => e,
            Ok(parsed) => client.import_parsed(&parsed, false).await.unwrap_err(),
        };
        assert!(!err.to_string().is_empty());
        let kept: Vec<String> = raw.zrange("z", 0, -1).await.unwrap();
        assert_eq!(kept, ["kept"], "{file}");
    }
}

// ---- JSON and JSON Lines ---------------------------------------------------------

fn parse_err(text: &str) -> String {
    format!("{:#}", parse(text.as_bytes()).unwrap_err())
}

#[test]
fn json_shapes_that_do_not_fit_the_type_are_refused_by_entry() {
    for (text, needle) in [
        (
            r#"[{"key":"k","type":"string","value":{"base64":"!!!"}}]"#,
            "entry 1: a value in 'k' has invalid base64",
        ),
        (
            r#"[{"key":{"base64":"@@"},"type":"string","value":"v"}]"#,
            "entry 1: the key name has invalid base64",
        ),
        (
            r#"[{"key":"k","type":"string","value":{"base64":"QQ==","x":1}}]"#,
            "must be a string or {\"base64\"",
        ),
        (
            r#"[{"key":"k","type":"string","value":{"b64":"QQ=="}}]"#,
            "must be a string or {\"base64\"",
        ),
        (
            r#"[{"key":"k","type":"string","value":null}]"#,
            "must be a string",
        ),
        (
            r#"[{"key":"k","type":"string","value":true}]"#,
            "must be a string",
        ),
        (
            r#"[{"key":"a","type":"string","value":"v"},{"type":"string","value":"v"}]"#,
            "entry 2: an entry has no \"key\"",
        ),
        (
            r#"[{"key":"k","value":"v"}]"#,
            "entry 1: 'k' has no \"type\"",
        ),
        (
            r#"[{"key":"k","type":7,"value":"v"}]"#,
            "'k' has no \"type\"",
        ),
        (
            r#"[{"key":"k","type":"string"}]"#,
            "entry 1: 'k' has no \"value\"",
        ),
        (
            r#"[{"key":"k","type":"string","ttl_ms":1.5,"value":"v"}]"#,
            "ttl_ms that is not a whole number",
        ),
        (
            r#"[{"key":"k","type":"string","ttl_ms":"100","value":"v"}]"#,
            "ttl_ms that is not a whole number",
        ),
        (
            r#"[{"key":"k","type":"hash","value":"v"}]"#,
            "must be an object or an array of [field, value] pairs",
        ),
        (
            r#"[{"key":"k","type":"hash","value":[["f","v","extra"]]}]"#,
            "[field, value] pairs",
        ),
        (
            r#"[{"key":"k","type":"hash","value":[["f"]]}]"#,
            "[field, value] pairs",
        ),
        (
            r#"[{"key":"k","type":"hash","value":{"f":null}}]"#,
            "must be a string",
        ),
        (
            r#"[{"key":"k","type":"list","value":{"0":"a"}}]"#,
            "the value of 'k' must be an array",
        ),
        (
            r#"[{"key":"k","type":"set","value":"a"}]"#,
            "must be an array",
        ),
        (
            r#"[{"key":"k","type":"zset","value":{"m":1}}]"#,
            "must be an array",
        ),
        (
            r#"[{"key":"k","type":"zset","value":[["m"]]}]"#,
            "'k' must hold [member, score] pairs",
        ),
        (
            r#"[{"key":"k","type":"zset","value":[["m","abc"]]}]"#,
            "is not a number: abc",
        ),
        (
            r#"[{"key":"k","type":"zset","value":[["m",true]]}]"#,
            "must be a number",
        ),
        (
            r#"[{"key":"k","type":"stream","value":[{"fields":{}}]}]"#,
            "a stream entry in 'k' has no id",
        ),
        (
            r#"[{"key":"k","type":"stream","value":[{"id":5,"fields":{}}]}]"#,
            "has no id",
        ),
        (
            r#"[{"key":"k","type":"stream","value":[{"id":"1-1"}]}]"#,
            "stream entry 1-1 in 'k' has no fields",
        ),
        (
            r#"[{"key":"k","type":"module-thing","value":1}]"#,
            "'k' has the type 'module-thing', which import cannot write",
        ),
        (
            r#"[{"key":{"base64":"/w=="},"type":"nope","value":1}]"#,
            "'\\xff' has the type 'nope'",
        ),
        (r#"[5]"#, "an entry must be a JSON object"),
        (
            "[{\"key\":\"k\",\"type\":\"string\",\"value\":\"v\"}",
            "not a valid JSON export",
        ),
    ] {
        let err = parse_err(text);
        assert!(err.contains(needle), "{text}\n  gave: {err}");
    }
}

#[test]
fn json_documents_check_their_tag_and_version() {
    for (text, needle) in [
        (
            r#"{"format":"rediscope-export","version":2,"keys":[]}"#,
            "this export is version 2; this rediscope reads version 1",
        ),
        (
            r#"{"format":"rediscope-export","version":"1","keys":[]}"#,
            "version 0",
        ),
        (r#"{"format":"rediscope-export","keys":[]}"#, "version 0"),
        (
            r#"{"format":"rediscope-export","version":1.0,"keys":[]}"#,
            "version 0",
        ),
        (
            r#"{"format":"rediscope-export","version":1}"#,
            "the export has no \"keys\" array",
        ),
        (
            r#"{"format":"rediscope-export","version":1,"keys":{}}"#,
            "no \"keys\" array",
        ),
        (
            r#"{"version":1,"keys":[]}"#,
            "must have \"format\": \"rediscope-export\"",
        ),
        (
            r#"{"format":"other","version":1,"keys":[]}"#,
            "must have \"format\"",
        ),
        (
            r#"{"format":"rediscope-export","version":1,"keys":[{"key":"k","type":"string"}]}"#,
            "entry 1: 'k' has no \"value\"",
        ),
    ] {
        let err = parse_err(text);
        assert!(err.contains(needle), "{text}\n  gave: {err}");
    }
    // Whitespace, a byte order mark and key order do not matter.
    let doc = "\u{feff}\n  {\"keys\":[{\"value\":\"v\",\"type\":\"string\",\"key\":\"k\"}],\"version\":1,\"format\":\"rediscope-export\"}\n\n";
    let Parsed::Records(records, Format::Json) = parse(doc.as_bytes()).unwrap() else {
        panic!("not a JSON document");
    };
    assert_eq!(records[0].value, Value::String(b"v".to_vec()));
}

#[test]
fn json_lines_skip_blank_lines_and_name_the_bad_one() {
    let text = "\n{\"key\":\"a\",\"type\":\"string\",\"value\":\"1\"}   \r\n\
                \t\r\n\
                {\"key\":\"b\",\"type\":\"list\",\"ttl_ms\":null,\"value\":[\"x\"]}\t\n\n";
    let Parsed::Records(records, Format::Jsonl) = parse(text.as_bytes()).unwrap() else {
        panic!("not JSON Lines");
    };
    assert_eq!(records.len(), 2);
    assert_eq!(records[1].value, Value::List(vec![b"x".to_vec()]));

    let text = "{\"key\":\"a\",\"type\":\"string\",\"value\":\"1\"}\n\n\
                {\"key\":\"b\",\"type\":\"string\",\"value\":\"2\"}\n\
                {\"key\":\"c\",\"type\":\"string\"}\n";
    assert_eq!(parse_err(text), "line 4: 'c' has no \"value\"");
    let text = "{\"key\":\"a\",\"type\":\"string\",\"value\":\"1\"}\n{not json}\n";
    assert!(
        parse_err(text).starts_with("line 2: not a JSON object"),
        "{}",
        parse_err(text)
    );
    // A single entry on its own is JSON Lines of one.
    let Parsed::Records(one, Format::Jsonl) =
        parse(b"  {\"key\":\"a\",\"type\":\"set\",\"value\":[\"m\"]}  ").unwrap()
    else {
        panic!("not JSON Lines");
    };
    assert_eq!(one.len(), 1);
}

#[test]
fn json_hash_values_accept_objects_and_pairs() {
    let text = r#"[
        {"key":"obj","type":"hash","value":{"f":"v","n":12,"b":{"base64":"/w=="}}},
        {"key":"pairs","type":"hash","value":[["f","v"],[{"base64":"/w=="},"x"],["f","again"]]},
        {"key":"s","type":"stream","value":[{"id":"1-1","fields":[["f","1"],["f","2"]]},{"id":"2-5","fields":{"g":"3"}}]}
    ]"#;
    let Parsed::Records(r, Format::Json) = parse(text.as_bytes()).unwrap() else {
        panic!("not records");
    };
    assert_eq!(
        r[0].value,
        Value::Hash(vec![
            (b"f".to_vec(), b"v".to_vec()),
            (b"n".to_vec(), b"12".to_vec()),
            (b"b".to_vec(), vec![0xff]),
        ])
    );
    assert_eq!(
        r[1].value,
        Value::Hash(vec![
            (b"f".to_vec(), b"v".to_vec()),
            (vec![0xff], b"x".to_vec()),
            (b"f".to_vec(), b"again".to_vec()),
        ])
    );
    let Value::Stream(entries) = &r[2].value else {
        panic!("not a stream");
    };
    assert_eq!(entries[0].fields.len(), 2);
    assert_eq!(entries[1].id, "2-5");
    // What the writer produces for a hash with a field named `base64`.
    let record = Record {
        key: b"h".to_vec(),
        ttl_ms: None,
        value: Value::Hash(vec![(b"base64".to_vec(), b"QUJD".to_vec())]),
    };
    let json = transfer::record_json(&record);
    assert_eq!(json["value"], serde_json::json!({"base64": "QUJD"}));
    assert_eq!(transfer::json_record(&json).unwrap(), record);
}

#[test]
fn redis_json_numbers_keep_what_a_double_can_hold() {
    let text = r#"[{"key":"doc","type":"json","value":{
        "u64max": 18446744073709551615,
        "i64min": -9223372036854775808,
        "above_2_53": 9007199254740993,
        "tenth": 0.1,
        "tiny": 5e-324,
        "big": 123456789012345678901234567890
    }}]"#;
    let Parsed::Records(r, _) = parse(text.as_bytes()).unwrap() else {
        panic!("not records");
    };
    let set = commands(&r[0], false);
    assert_eq!(set.len(), 1);
    assert_eq!(
        &set[0][..3],
        [b"JSON.SET".to_vec(), b"doc".to_vec(), b"$".to_vec()]
    );
    let doc = String::from_utf8(set[0][3].clone()).unwrap();
    for exact in [
        "\"u64max\":18446744073709551615",
        "\"i64min\":-9223372036854775808",
        "\"above_2_53\":9007199254740993",
        "\"tenth\":0.1",
        "\"tiny\":5e-324",
    ] {
        assert!(doc.contains(exact), "{exact} in {doc}");
    }
    // Past 64 bits a number is a double, as the README says.
    assert!(doc.contains("\"big\":1.2345678901234568e+29"), "{doc}");
    // Out of a double's range it is not a number at all.
    assert!(parse(br#"[{"key":"d","type":"json","value":1e400}]"#).is_err());
}

#[test]
fn empty_and_old_files_still_parse() {
    assert!(matches!(parse(b"[]").unwrap(), Parsed::Dump(e) if e.is_empty()));
    assert!(matches!(parse(b"  [ ]\n").unwrap(), Parsed::Dump(e) if e.is_empty()));
    assert!(matches!(parse(b"").unwrap(), Parsed::Commands(c) if c.is_empty()));
    assert!(matches!(parse(b"\n\r\n  \n").unwrap(), Parsed::Commands(c) if c.is_empty()));
    let doc = br#"{"format":"rediscope-export","version":1,"keys":[]}"#;
    assert!(matches!(parse(doc).unwrap(), Parsed::Records(r, Format::Json) if r.is_empty()));
    // The shape `export` wrote before it had formats, `kind` and `pttl` optional.
    let old = "[\n  {\n    \"key\": \"a\\\\x00b\",\n    \"kind\": \"string\",\n    \"pttl\": 1500,\n    \"dump\": \"00\"\n  },\n  {\n    \"key\": \"c\",\n    \"dump\": \"00\"\n  }\n]\n";
    let Parsed::Dump(entries) = parse(old.as_bytes()).unwrap() else {
        panic!("not a dump");
    };
    assert_eq!(entries[0].key, "a\\x00b");
    assert_eq!((entries[0].pttl, entries[1].pttl), (1500, 0));
    assert!(entries[1].kind.is_empty());
    // A dump file with one entry missing its payload is refused whole.
    let err = parse_err(r#"[{"key":"a","dump":"00"},{"key":"b"}]"#);
    assert!(err.contains("not a rediscope DUMP export"), "{err}");
    // Mixing a record into a dump file, or a dump into records, is refused.
    let err = parse_err(r#"[{"key":"a","type":"string","value":"v"},{"key":"b","dump":"00"}]"#);
    assert!(err.contains("entry 2: 'b' has no \"type\""), "{err}");
}

#[tokio::test]
async fn old_dump_files_restore_into_another_server() {
    let prefix = "edge:old:";
    let Some(target) = Target::start().await else {
        return;
    };
    let mut raw = source().await;
    let mut target_raw = target.raw().await;
    if !rdb_compatible(&mut raw, &mut target_raw).await {
        eprintln!("skipped: the servers have different versions");
        return;
    }
    clear_source(prefix).await;
    let _: () = raw.set(k(prefix, "plain"), "kept").await.unwrap();
    let _: () = raw
        .pset_ex(k(prefix, "ttl"), "soon", 600_000)
        .await
        .unwrap();
    let _: () = raw
        .rpush(k(prefix, [0xff, b'\n']), &["a", "b"])
        .await
        .unwrap();
    let src = source_client().await;
    let names = export_names(&src, prefix).await;
    // Byte for byte what the old `export --out -` printed.
    let entries = src.export_keys(&names).await.unwrap();
    let old = format!("{}\n", serde_json::to_string_pretty(&entries).unwrap());
    let client = target.client("edges-old").await;
    let parsed = parse(old.as_bytes()).unwrap();
    assert_eq!(client.import_parsed(&parsed, false).await.unwrap().keys, 3);
    assert_same_holdings(
        &holdings(&mut raw, &format!("{prefix}*")).await,
        &holdings(&mut target_raw, &format!("{prefix}*")).await,
        "old dump",
    );
    // Without overwrite the first existing key stops it, named.
    let err = client
        .import_parsed(&parsed, false)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("cannot restore"), "{err}");
    // A dump with a TTL in the target and none in the file loses the TTL.
    let _: i64 = target_raw
        .pexpire(k(prefix, "plain"), 900_000)
        .await
        .unwrap();
    client.import_parsed(&parsed, true).await.unwrap();
    let ttl: i64 = target_raw.pttl(k(prefix, "plain")).await.unwrap();
    assert_eq!(ttl, -1);
    // A corrupt payload is named, and nothing after it is written.
    target.flush().await;
    let bad = r#"[{"key":"first","dump":"zz"},{"key":"second","dump":"00"}]"#;
    let err = client
        .import_parsed(&parse(bad.as_bytes()).unwrap(), false)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("'first' has a corrupt payload"), "{err}");
    let size: i64 = redis::cmd("DBSIZE")
        .query_async(&mut target_raw)
        .await
        .unwrap();
    assert_eq!(size, 0);
    clear_source(prefix).await;
}

// ---- safety ------------------------------------------------------------------------

/// One small file per format, all holding the same two keys.
async fn files_in_every_format(prefix: &str) -> Vec<(Format, Vec<u8>)> {
    let mut raw = source().await;
    clear_source(prefix).await;
    let _: () = raw.set(k(prefix, "s"), "v").await.unwrap();
    let _: () = raw.hset(k(prefix, "h"), "f", "v").await.unwrap();
    let src = source_client().await;
    let names = export_names(&src, prefix).await;
    let mut out = Vec::new();
    for format in Format::ALL {
        out.push((
            format,
            export(&src, &names, format, format == Format::Commands).await,
        ));
    }
    clear_source(prefix).await;
    out
}

#[tokio::test]
async fn a_read_only_profile_sends_no_write_in_any_format() {
    let prefix = "edge:ro:";
    let Some(target) = Target::start().await else {
        return;
    };
    let files = files_in_every_format(prefix).await;
    let dir = scratch("ro");
    let mut ro = target.profile("edges-read-only");
    ro.read_only = true;
    let client = Client::connect(ro.clone()).await.unwrap();
    let mut raw = target.raw().await;
    // Something already there, so the replace path has a key to delete.
    let _: () = raw.set(k(prefix, "s"), "old").await.unwrap();
    target.reset_stats().await;
    for (format, bytes) in &files {
        let parsed = parse(bytes).unwrap();
        for replace in [false, true] {
            let err = client.import_parsed(&parsed, replace).await.unwrap_err();
            assert!(
                err.to_string().to_lowercase().contains("read-only"),
                "{format:?}: {err}"
            );
        }
        let path = dir.join(format!("file.{}", format.extension()));
        std::fs::write(&path, bytes).unwrap();
        for replace in [false, true] {
            assert!(
                rediscope::headless::import(ro.clone(), path.to_str().unwrap(), replace)
                    .await
                    .is_err()
            );
        }
        let out = cli(
            &["-p", &target.port.to_string(), "--read-only"],
            &["import", path.to_str().unwrap(), "--replace"],
        );
        assert_eq!(out.status.code(), Some(1), "{format:?}: {out:?}");
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("read-only"),
            "{out:?}"
        );
        assert_no_writes(&target, format.name()).await;
    }
    let old: String = raw.get(k(prefix, "s")).await.unwrap();
    assert_eq!(old, "old");
    let size: i64 = redis::cmd("DBSIZE").query_async(&mut raw).await.unwrap();
    assert_eq!(size, 1);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn a_production_import_needs_both_flags_in_every_format() {
    let prefix = "edge:prod:";
    let Some(target) = Target::start().await else {
        return;
    };
    let files = files_in_every_format(prefix).await;
    let dir = scratch("prod");
    let prod = Connection {
        environment: Environment::Production,
        ..target.profile("edges-production")
    };
    let mut raw = target.raw().await;
    for (format, bytes) in &files {
        let path = dir.join(format!("file.{}", format.extension()));
        std::fs::write(&path, bytes).unwrap();
        let path = path.to_str().unwrap();
        target.flush().await;
        target.reset_stats().await;
        let name = prod.name.as_str();
        for (unlock, confirm) in [
            (None, None),
            (Some(name), None),
            (None, Some(name)),
            (Some("wrong"), Some(name)),
            (Some(name), Some("wrong")),
        ] {
            assert!(
                rediscope::headless::import_confirmed(prod.clone(), path, false, unlock, confirm)
                    .await
                    .is_err(),
                "{format:?} {unlock:?} {confirm:?}"
            );
        }
        // A locked client refuses too, before it sends a thing.
        let client = Client::connect(prod.clone()).await.unwrap();
        assert!(
            client
                .import_parsed(&parse(bytes).unwrap(), true)
                .await
                .is_err()
        );
        assert_no_writes(&target, format.name()).await;

        rediscope::headless::import_confirmed(prod.clone(), path, false, Some(name), Some(name))
            .await
            .unwrap_or_else(|e| panic!("{format:?}: {e}"));
        let size: i64 = redis::cmd("DBSIZE").query_async(&mut raw).await.unwrap();
        assert_eq!(size, 2, "{format:?}");
    }
    let _ = std::fs::remove_dir_all(dir);
}

fn audit_events(profile: &str) -> Vec<(String, String, Option<u64>)> {
    common::isolate_config();
    let path = std::env::var_os("REDISCOPE_AUDIT_FILE").expect("isolate_config sets it");
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|e| e["profile"] == profile)
        .filter(|e| e["action"] == "PIPELINE" || e["action"] == "RESTORE")
        .map(|e| {
            (
                e["action"].as_str().unwrap().to_string(),
                e["outcome"].as_str().unwrap().to_string(),
                e["target_key_count"].as_u64(),
            )
        })
        .collect()
}

fn ev(action: &str, outcome: &str, count: Option<u64>) -> (String, String, Option<u64>) {
    (action.into(), outcome.into(), count)
}

#[tokio::test]
async fn imports_are_audited_once_per_batch_with_their_outcome() {
    let Some(target) = Target::start().await else {
        return;
    };
    let profile = format!("edges-audit-{}", std::process::id());
    let client = target.client(&profile).await;

    // Records: one batch per key; DEL, the write and the TTL count as targets.
    let jsonl = "{\"key\":\"a\",\"type\":\"string\",\"ttl_ms\":5000,\"value\":\"1\"}\n\
                 {\"key\":\"b\",\"type\":\"list\",\"value\":[\"x\"]}\n\
                 {\"key\":\"e\",\"type\":\"set\",\"value\":[]}\n";
    let report = client
        .import_parsed(&parse(jsonl.as_bytes()).unwrap(), true)
        .await
        .unwrap();
    assert_eq!((report.keys, report.skipped, report.commands), (2, 1, 5));
    assert_eq!(
        audit_events(&profile),
        [
            ev("PIPELINE", "started", Some(3)),
            ev("PIPELINE", "success", Some(3)),
            ev("PIPELINE", "started", Some(2)),
            ev("PIPELINE", "success", Some(2)),
        ]
    );

    // Commands: a batch the server rejects is logged with an unknown outcome,
    // and nothing after it is logged at all.
    let before = audit_events(&profile).len();
    let file = "SET c 1\nSET d 2\nHSET c f v\nSET z 9\n";
    assert!(
        client
            .import_parsed(&parse(file.as_bytes()).unwrap(), false)
            .await
            .is_err()
    );
    assert_eq!(
        audit_events(&profile)[before..],
        [
            ev("PIPELINE", "started", Some(1)),
            ev("PIPELINE", "success", Some(1)),
            ev("PIPELINE", "started", Some(1)),
            ev("PIPELINE", "success", Some(1)),
            ev("PIPELINE", "started", Some(1)),
            ev("PIPELINE", "unknown", Some(1)),
        ]
    );

    // Without overwrite, an existing key stops the import before its batch.
    let before = audit_events(&profile).len();
    assert!(
        client
            .import_parsed(&parse(jsonl.as_bytes()).unwrap(), false)
            .await
            .is_err()
    );
    assert_eq!(audit_events(&profile).len(), before);

    // DUMP files restore key by key.
    let mut raw = target.raw().await;
    let dump: Vec<u8> = redis::cmd("DUMP")
        .arg("a")
        .query_async(&mut raw)
        .await
        .unwrap();
    let hex: String = dump.iter().map(|b| format!("{b:02x}")).collect();
    let file = format!(
        r#"[{{"key":"r1","pttl":-1,"dump":"{hex}"}},{{"key":"a","pttl":-1,"dump":"{hex}"}}]"#
    );
    let before = audit_events(&profile).len();
    assert!(
        client
            .import_parsed(&parse(file.as_bytes()).unwrap(), false)
            .await
            .is_err()
    );
    assert_eq!(
        audit_events(&profile)[before..],
        [
            ev("RESTORE", "started", Some(1)),
            ev("RESTORE", "success", Some(1)),
            ev("RESTORE", "started", Some(1)),
            ev("RESTORE", "failure", Some(1)),
        ]
    );

    // A read-only profile's refusal is logged as denied, once.
    let ro_name = format!("{profile}-ro");
    let mut ro = target.profile(&ro_name);
    ro.read_only = true;
    let ro = Client::connect(ro).await.unwrap();
    assert!(
        ro.import_parsed(&parse(file.as_bytes()).unwrap(), true)
            .await
            .is_err()
    );
    assert!(
        ro.import_parsed(&parse(b"SET q 1\n").unwrap(), true)
            .await
            .is_err()
    );
    let events = audit_events(&ro_name);
    assert!(
        events
            .iter()
            .all(|(_, outcome, _)| outcome == "started" || outcome == "denied"),
        "{events:?}"
    );
    assert_eq!(
        events.iter().filter(|(_, o, _)| o == "denied").count(),
        2,
        "{events:?}"
    );
}

#[tokio::test]
async fn overwrite_decides_existing_keys_and_their_ttls_in_every_format() {
    let prefix = "edge:replace:";
    let Some(target) = Target::start().await else {
        return;
    };
    let mut raw = source().await;
    clear_source(prefix).await;
    let _: () = raw.set(k(prefix, "a"), "file-a").await.unwrap();
    let _: () = raw
        .rpush(k(prefix, [b'b', 0xff]), &["file", "list"])
        .await
        .unwrap();
    let _: () = raw
        .pset_ex(k(prefix, "c"), "file-c", 500_000)
        .await
        .unwrap();
    let src = source_client().await;
    let names = export_names(&src, prefix).await;
    let client = target.client("edges-replace").await;
    let mut t = target.raw().await;
    let same_version = rdb_compatible(&mut raw, &mut t).await;
    for format in Format::ALL {
        if format == Format::Dump && !same_version {
            continue;
        }
        let with_del = format == Format::Commands;
        let bytes = export(&src, &names, format, with_del).await;
        let parsed = parse(&bytes).unwrap();

        // In the target: `b` is in the way with a TTL, `c` has none.
        let reset = async |t: &mut MultiplexedConnection| {
            target.flush().await;
            let _: () = t
                .hset(k(prefix, [b'b', 0xff]), "stale", "hash")
                .await
                .unwrap();
            let _: i64 = t.pexpire(k(prefix, [b'b', 0xff]), 800_000).await.unwrap();
            let _: () = t.set(k(prefix, "c"), "stale").await.unwrap();
        };
        reset(&mut t).await;
        if !with_del {
            let err = client
                .import_parsed(&parsed, false)
                .await
                .unwrap_err()
                .to_string();
            if format == Format::Dump {
                // A dump restores in the file's order, which is the scan's.
                assert!(
                    err.contains("cannot restore '") && err.contains("BUSYKEY"),
                    "{err}"
                );
                reset(&mut t).await;
                client.import_parsed(&parsed, true).await.unwrap();
                let b: Vec<String> = t.lrange(k(prefix, [b'b', 0xff]), 0, -1).await.unwrap();
                assert_eq!(b, ["file", "list"]);
                let ttl: i64 = t.pttl(k(prefix, [b'b', 0xff])).await.unwrap();
                assert_eq!(ttl, -1, "RESTORE REPLACE with no TTL clears the old one");
                continue;
            }
            let shown = format!("cannot import '{prefix}b\\xff': the key already exists");
            assert!(err.contains(&shown), "{format:?}: {err}");
            assert!(err.contains("1 key(s) were written before it"), "{err}");
            let a: Option<String> = t.get(k(prefix, "a")).await.unwrap();
            assert_eq!(
                a.as_deref(),
                Some("file-a"),
                "{format:?}: keys before it are written"
            );
            let c: String = t.get(k(prefix, "c")).await.unwrap();
            assert_eq!(c, "stale", "{format:?}: keys after it are not");
            let ttl: i64 = t.pttl(k(prefix, [b'b', 0xff])).await.unwrap();
            assert!(ttl > 0, "{format:?}: the key in the way keeps its TTL");
            reset(&mut t).await;
        }
        client.import_parsed(&parsed, true).await.unwrap();
        let b: Vec<String> = t.lrange(k(prefix, [b'b', 0xff]), 0, -1).await.unwrap();
        assert_eq!(b, ["file", "list"], "{format:?}");
        let ttl: i64 = t.pttl(k(prefix, [b'b', 0xff])).await.unwrap();
        assert_eq!(ttl, -1, "{format:?}: no TTL in the file clears the old one");
        let ttl: i64 = t.pttl(k(prefix, "c")).await.unwrap();
        assert!(ttl > 400_000 && ttl <= 500_000, "{format:?}: {ttl}");
        let c: String = t.get(k(prefix, "c")).await.unwrap();
        assert_eq!(c, "file-c");
    }
    clear_source(prefix).await;
}

#[tokio::test]
async fn a_big_key_goes_out_in_bounded_pipelines_and_replaces_the_old_one_whole() {
    let Some(target) = Target::start().await else {
        return;
    };
    let profile = format!("edges-big-{}", std::process::id());
    let client = target.client(&profile).await;
    let mut raw = target.raw().await;
    let _: () = raw.set("big:list", "old").await.unwrap();
    let _: i64 = raw.pexpire("big:list", 900_000).await.unwrap();
    let _: () = raw.hset("big:hash", "old", "1").await.unwrap();
    let mut records = vec![
        Record {
            key: b"big:list".to_vec(),
            ttl_ms: None,
            value: Value::List((0..300_000).map(|i| format!("i{i}").into_bytes()).collect()),
        },
        Record {
            key: b"big:hash".to_vec(),
            ttl_ms: Some(600_000),
            value: Value::Hash(
                (0..100_000)
                    .map(|i| (format!("field-{i:010}").into_bytes(), vec![b'v'; 20]))
                    .collect(),
            ),
        },
        Record {
            key: b"big:zset".to_vec(),
            ttl_ms: None,
            value: Value::ZSet(
                (0..50_000)
                    .map(|i| (format!("m{i}").into_bytes(), f64::from(i) / 3.0))
                    .collect(),
            ),
        },
    ];
    let vectors = supports(&mut raw, "VADD").await;
    if vectors {
        records.push(Record {
            key: b"big:vset".to_vec(),
            ttl_ms: None,
            value: Value::VectorSet(transfer::VectorSet {
                quant: Some("f32".into()),
                elements: (0..3_000)
                    .map(|i| transfer::VectorElement {
                        element: format!("e{i}").into_bytes(),
                        vector: (0..8).map(|d| f64::from(i * 8 + d)).collect(),
                        attributes: (i % 2 == 0).then(|| format!(r#"{{"i":{i}}}"#)),
                    })
                    .collect(),
            }),
        });
    }
    let report = client.import_records(&records, true).await.unwrap();
    assert_eq!(report.keys as usize, records.len());

    let list: Vec<String> = raw.lrange("big:list", 299_998, -1).await.unwrap();
    assert_eq!(list, ["i299998", "i299999"]);
    assert_eq!(raw.llen::<_, i64>("big:list").await.unwrap(), 300_000);
    let ttl: i64 = raw.pttl("big:list").await.unwrap();
    assert_eq!(ttl, -1, "the old key's TTL went with it");
    assert_eq!(raw.hlen::<_, i64>("big:hash").await.unwrap(), 100_000);
    let old: Option<String> = raw.hget("big:hash", "old").await.unwrap();
    assert_eq!(old, None, "the old hash was replaced, not merged into");
    let ttl: i64 = raw.pttl("big:hash").await.unwrap();
    assert!(ttl > 500_000, "{ttl}");
    assert_eq!(raw.zcard::<_, i64>("big:zset").await.unwrap(), 50_000);
    if vectors {
        let n: i64 = redis::cmd("VCARD")
            .arg("big:vset")
            .query_async(&mut raw)
            .await
            .unwrap();
        assert_eq!(n, 3_000);
    }
    // Nothing temporary is left behind.
    let size: i64 = redis::cmd("DBSIZE").query_async(&mut raw).await.unwrap();
    assert_eq!(size as usize, records.len());

    // Every pipeline stayed small, and the big keys took many of them.
    let started: Vec<Option<u64>> = audit_events(&profile)
        .into_iter()
        .filter(|(_, outcome, _)| outcome == "started")
        .map(|(_, _, count)| count)
        .collect();
    assert!(
        started.iter().all(|c| c.is_some_and(|c| c <= 256)),
        "{started:?}"
    );
    // Without vector sets (older servers) the list, hash and zset alone
    // still need well over one pipeline each.
    assert!(started.len() >= 10, "{}", started.len());
}

#[tokio::test]
async fn a_replace_that_fails_leaves_the_existing_key_as_it_was() {
    let Some(target) = Target::start().await else {
        return;
    };
    let mut raw = target.raw().await;
    // A user who may write, but not add set members or rename keys.
    let _: () = redis::cmd("ACL")
        .arg(
            &[
                "SETUSER", "limited", "on", ">pw", "~*", "&*", "+@all", "-sadd", "-rename",
            ][..],
        )
        .query_async(&mut raw)
        .await
        .unwrap();
    let profile = format!("edges-failing-{}", std::process::id());
    let client = Client::connect(Connection {
        username: "limited".into(),
        password: "pw".into(),
        ..target.profile(&profile)
    })
    .await
    .unwrap();
    let keep = async |raw: &mut MultiplexedConnection| {
        let _: () = raw.set("kept", "old").await.unwrap();
        let _: i64 = raw.pexpire("kept", 900_000).await.unwrap();
    };
    let still_kept = async |raw: &mut MultiplexedConnection, what: &str| {
        let v: Option<String> = raw.get("kept").await.unwrap();
        assert_eq!(v.as_deref(), Some("old"), "{what}");
        let ttl: i64 = raw.pttl("kept").await.unwrap();
        assert!(ttl > 0, "{what}: {ttl}");
        let size: i64 = redis::cmd("DBSIZE").query_async(raw).await.unwrap();
        assert_eq!(size, 1, "{what}: nothing temporary is left");
    };

    // A small key is one transaction: the refused SADD discards the DEL too.
    keep(&mut raw).await;
    let small = Record {
        key: b"kept".to_vec(),
        ttl_ms: None,
        value: Value::Set(vec![b"a".to_vec(), b"b".to_vec()]),
    };
    let err = client
        .import_records(std::slice::from_ref(&small), true)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.starts_with("cannot import 'kept': "), "{err}");
    assert!(err.contains("no permissions"), "{err}");
    assert!(err.contains("the key was not changed"), "{err}");
    still_kept(&mut raw, "transaction").await;
    let events = audit_events(&profile);
    assert_eq!(
        events[events.len() - 2..],
        [
            ev("PIPELINE", "started", Some(2)),
            ev("PIPELINE", "failure", Some(2))
        ]
    );

    // A big key is written aside; the refused RENAME leaves the key alone.
    let big = Record {
        key: b"kept".to_vec(),
        ttl_ms: Some(5_000),
        value: Value::List((0..100_000).map(|i| i.to_string().into_bytes()).collect()),
    };
    let err = client
        .import_records(std::slice::from_ref(&big), true)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("no permissions"), "{err}");
    assert!(err.contains("the key was not changed"), "{err}");
    still_kept(&mut raw, "rename").await;

    // A type the server has no commands for is refused before anything is sent.
    if !supports(&mut raw, "JSON.SET").await {
        target.reset_stats().await;
        let doc = r#"[{"key":"kept","type":"json","value":{"a":1}},{"key":"other","type":"string","value":"v"}]"#;
        let err = client
            .import_parsed(&parse(doc.as_bytes()).unwrap(), true)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("the server has no JSON.SET") && err.contains("nothing was imported"),
            "{err}"
        );
        still_kept(&mut raw, "no RedisJSON").await;
        assert_no_writes(&target, "no RedisJSON").await;
    }
}

#[tokio::test]
async fn an_import_by_a_user_without_transactions_never_says_the_key_was_unchanged() {
    let Some(target) = Target::start().await else {
        return;
    };
    let mut raw = target.raw().await;
    // MULTI and EXEC are refused, and every command in between runs at once.
    let _: () = redis::cmd("ACL")
        .arg(
            &[
                "SETUSER",
                "nomulti",
                "on",
                ">pw",
                "~*",
                "&*",
                "+@all",
                "-@transaction",
            ][..],
        )
        .query_async(&mut raw)
        .await
        .unwrap();
    let profile = format!("edges-nomulti-{}", std::process::id());
    let client = Client::connect(Connection {
        username: "nomulti".into(),
        password: "pw".into(),
        ..target.profile(&profile)
    })
    .await
    .unwrap();
    let _: () = raw.set("small", "old").await.unwrap();
    let small = Record {
        key: b"small".to_vec(),
        ttl_ms: None,
        value: Value::Set(vec![b"a".to_vec(), b"b".to_vec()]),
    };
    let err = client
        .import_records(std::slice::from_ref(&small), true)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("no permissions"), "{err}");
    assert!(!err.contains("not changed"), "{err}");
    assert!(
        err.contains("may have been partly or fully applied"),
        "{err}"
    );
    // It did run: the old string is gone and the set is there.
    let kind: String = redis::cmd("TYPE")
        .arg("small")
        .query_async(&mut raw)
        .await
        .unwrap();
    assert_eq!(kind, "set");
    let events = audit_events(&profile);
    assert_eq!(
        events[events.len() - 2..],
        [
            ev("PIPELINE", "started", Some(2)),
            ev("PIPELINE", "unknown", Some(2))
        ]
    );

    let _: () = raw.set("big", "old").await.unwrap();
    let big = Record {
        key: b"big".to_vec(),
        ttl_ms: Some(900_000),
        value: Value::List((0..100_000).map(|i| i.to_string().into_bytes()).collect()),
    };
    let err = client
        .import_records(std::slice::from_ref(&big), true)
        .await
        .unwrap_err()
        .to_string();
    assert!(!err.contains("not changed"), "{err}");
    assert!(
        err.contains("may have been partly or fully applied"),
        "{err}"
    );
    assert!(err.contains("TTL and rename"), "written aside: {err}");
    let len: i64 = raw.llen("big").await.unwrap();
    assert_eq!(len, 100_000);
    let size: i64 = redis::cmd("DBSIZE").query_async(&mut raw).await.unwrap();
    assert_eq!(size, 2, "nothing temporary is left");
    let events = audit_events(&profile);
    assert_eq!(events.last().map(|e| e.1.as_str()), Some("unknown"));
}

#[tokio::test]
async fn an_import_by_a_user_limited_to_a_key_pattern_writes_aside_under_that_pattern() {
    let Some(target) = Target::start().await else {
        return;
    };
    let mut raw = target.raw().await;
    let _: () = redis::cmd("ACL")
        .arg(&["SETUSER", "apponly", "on", ">pw", "~app:*", "&*", "+@all"][..])
        .query_async(&mut raw)
        .await
        .unwrap();
    let client = Client::connect(Connection {
        username: "apponly".into(),
        password: "pw".into(),
        ..target.profile(&format!("edges-pattern-{}", std::process::id()))
    })
    .await
    .unwrap();
    let list = |key: &str, n: usize| Record {
        key: key.as_bytes().to_vec(),
        ttl_ms: Some(900_000),
        value: Value::List((0..n).map(|i| i.to_string().into_bytes()).collect()),
    };
    // Big with overwrite, and small without it: both go through a temporary key.
    let _: () = raw.set("app:big", "old").await.unwrap();
    let report = client
        .import_records(&[list("app:big", 100_000), list("app:small", 3)], true)
        .await
        .unwrap();
    assert_eq!(report.keys, 2);
    let report = client
        .import_records(&[list("app:fresh", 3)], false)
        .await
        .unwrap();
    assert_eq!(report.keys, 1);
    let len: i64 = raw.llen("app:big").await.unwrap();
    assert_eq!(len, 100_000);
    let len: i64 = raw.llen("app:fresh").await.unwrap();
    assert_eq!(len, 3);
    let size: i64 = redis::cmd("DBSIZE").query_async(&mut raw).await.unwrap();
    assert_eq!(size, 3, "nothing temporary is left");

    // A key outside the pattern is refused with its name and what came before.
    let err = client
        .import_records(&[list("app:first", 3), list("other", 100_000)], true)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.starts_with("cannot import 'other': "), "{err}");
    assert!(err.contains("1 key(s) were written before it"), "{err}");
}

#[tokio::test]
async fn an_export_names_what_a_format_cannot_hold_and_is_audited_once() {
    let Some(target) = Target::start().await else {
        return;
    };
    let mut raw = target.raw().await;
    // A stream whose entries were all deleted still exists, empty.
    let _: String = raw.xadd("empty", "1-1", &[("f", "v")]).await.unwrap();
    let _: i64 = raw.xdel("empty", &["1-1"]).await.unwrap();
    let _: () = raw.set("s", "v").await.unwrap();
    let profile = format!("edges-export-audit-{}", std::process::id());
    let client = target.client(&profile).await;
    let names = vec!["empty".to_string(), "s".to_string()];
    for format in Format::ALL {
        let (report, bytes) = client
            .export_to(&names, format, false, Vec::new())
            .await
            .unwrap();
        match format {
            Format::Csv | Format::Commands => {
                assert_eq!(report.written, 1, "{format:?}");
                assert_eq!(report.skipped.len(), 1, "{format:?}");
                assert!(
                    report.skipped[0].starts_with("empty: an empty stream"),
                    "{:?}",
                    report.skipped
                );
            }
            _ => {
                assert_eq!(report.written, 2, "{format:?}");
                assert!(report.skipped.is_empty(), "{:?}", report.skipped);
            }
        }
        assert!(parse(&bytes).is_ok(), "{format:?}");
    }
    common::isolate_config();
    let path = std::env::var_os("REDISCOPE_AUDIT_FILE").unwrap();
    let events: Vec<(String, Option<u64>)> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|e| e["profile"] == profile.as_str() && e["action"] == "EXPORT")
        .map(|e| {
            (
                e["outcome"].as_str().unwrap().to_string(),
                e["target_key_count"].as_u64(),
            )
        })
        .collect();
    let expected: Vec<(String, Option<u64>)> = Format::ALL
        .iter()
        .flat_map(|_| [("started".into(), Some(2)), ("success".into(), Some(2))])
        .collect();
    assert_eq!(events, expected);
}

// ---- the command line ---------------------------------------------------------------

fn cli(connection: &[&str], args: &[&str]) -> std::process::Output {
    common::isolate_config();
    Command::new(env!("CARGO_BIN_EXE_rediscope"))
        .args(["-H", "127.0.0.1"])
        .args(connection)
        .args(args)
        .env(
            "REDISCOPE_HOME",
            std::env::var_os("REDISCOPE_HOME").unwrap(),
        )
        .env(
            "REDISCOPE_AUDIT_FILE",
            std::env::var_os("REDISCOPE_AUDIT_FILE").unwrap(),
        )
        .env_remove("REDISCOPE_PASSWORD")
        .stdin(Stdio::null())
        .output()
        .unwrap()
}

#[test]
fn export_flags_are_checked_before_connecting() {
    // Port 1 has no server: these fail on the flags, not the connection.
    let out = cli(&["-p", "1"], &["export", "--format", "xml"]);
    assert_eq!(out.status.code(), Some(2), "{out:?}");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("expected one of dump, json, jsonl, csv, commands"),
        "{err}"
    );
    for format in ["dump", "json", "jsonl", "csv"] {
        let out = cli(&["-p", "1"], &["export", "--format", format, "--replace"]);
        assert_eq!(out.status.code(), Some(1), "{out:?}");
        assert!(
            String::from_utf8_lossy(&out.stderr)
                .contains("--replace only applies to --format commands"),
            "{out:?}"
        );
    }
    for args in [
        &["import"][..],
        &["import", "a.json", "--file", "b.json"],
        &["import", "a.json", "b.json"],
    ] {
        let out = cli(&["-p", "1"], args);
        assert_eq!(out.status.code(), Some(2), "{args:?}: {out:?}");
    }
}

#[tokio::test]
async fn the_cli_exports_to_stdout_or_a_file_and_imports_either_way() {
    let prefix = "edge:cli:";
    let Some(target) = Target::start().await else {
        return;
    };
    let mut raw = source().await;
    clear_source(prefix).await;
    let _: () = raw.set(k(prefix, "s"), "hello, \"cli\"").await.unwrap();
    let _: () = raw.sadd(k(prefix, "set"), &["a", "b"]).await.unwrap();
    let source_args = [
        "-p".to_string(),
        source_port().unwrap().to_string(),
        "-n".into(),
        SOURCE_DB.to_string(),
    ];
    let source_args: Vec<&str> = source_args.iter().map(String::as_str).collect();
    let pattern = format!("{prefix}*");
    let dir = scratch("cli");
    let port = target.port.to_string();
    let mut t = target.raw().await;

    for (format, name) in [
        ("JSON", "JSON"),
        ("jsonl", "JSON Lines"),
        (" csv ", "CSV"),
        ("commands", "redis-cli commands"),
        ("dump", "DUMP payloads"),
    ] {
        let file = dir.join(format!("out-{}", format.trim()));
        let path = file.to_str().unwrap();
        let to_file = cli(
            &source_args,
            &[
                "export",
                "--pattern",
                &pattern,
                "--format",
                format,
                "--out",
                path,
            ],
        );
        assert!(to_file.status.success(), "{to_file:?}");
        assert!(
            to_file.stdout.is_empty(),
            "{format}: a file export prints nothing to stdout"
        );
        let err = String::from_utf8_lossy(&to_file.stderr);
        assert_eq!(err.trim(), format!("exported 2 key(s) to {path} as {name}"));
        let to_stdout = cli(
            &source_args,
            &["export", "--pattern", &pattern, "--format", format],
        );
        assert!(to_stdout.status.success(), "{to_stdout:?}");
        assert!(to_stdout.stderr.is_empty(), "{to_stdout:?}");
        if format != "dump" {
            assert_eq!(to_stdout.stdout, std::fs::read(&file).unwrap(), "{format}");
        }

        target.flush().await;
        let out = cli(&["-p", &port], &["import", path]);
        assert!(out.status.success(), "{out:?}");
        let said = String::from_utf8_lossy(&out.stderr);
        assert!(said.contains(name), "{said}");
        let size: i64 = redis::cmd("DBSIZE").query_async(&mut t).await.unwrap();
        assert_eq!(size, 2, "{format}");

        // Again, without overwrite: the first existing key is an error, exit 1.
        let out = cli(&["-p", &port], &["import", "--file", path]);
        if format.trim() == "commands" {
            assert!(
                out.status.success(),
                "a commands file runs as written: {out:?}"
            );
        } else {
            assert_eq!(out.status.code(), Some(1), "{out:?}");
        }
        let out = cli(&["-p", &port], &["import", "--file", path, "--replace"]);
        assert!(out.status.success(), "{out:?}");
        if format.trim() == "commands" {
            assert!(
                String::from_utf8_lossy(&out.stderr)
                    .contains("--replace does not apply to a commands file"),
                "{out:?}"
            );
        }
    }
    let got: String = t.get(k(prefix, "s")).await.unwrap();
    assert_eq!(got, "hello, \"cli\"");

    // A missing file and a file that is nothing at all.
    let out = cli(
        &["-p", &port],
        &["import", dir.join("missing").to_str().unwrap()],
    );
    assert_eq!(out.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("cannot read"),
        "{out:?}"
    );
    let junk = dir.join("junk.json");
    std::fs::write(&junk, "{\"not\": \"an export\"").unwrap();
    let out = cli(&["-p", &port], &["import", junk.to_str().unwrap()]);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("not a rediscope export"),
        "{out:?}"
    );
    let _ = std::fs::remove_dir_all(dir);
    clear_source(prefix).await;
}

#[tokio::test]
async fn a_failed_export_leaves_the_old_file_alone() {
    let prefix = "edge:keepfile:";
    let Some(target) = Target::start().await else {
        return;
    };
    let mut raw = target.raw().await;
    let _: () = raw.set(k(prefix, "s"), "v").await.unwrap();
    let _: () = raw.hset(k(prefix, "h"), "f", "v").await.unwrap();
    // A user who can list and read strings, but not scan a hash.
    let _: () = redis::cmd("ACL")
        .arg(&["SETUSER", "noscan", "on", ">pw", "~*", "+@all", "-hscan"][..])
        .query_async(&mut raw)
        .await
        .unwrap();
    let dir = scratch("keepfile");
    let path = dir.join("export.json");
    std::fs::write(&path, "the export from yesterday").unwrap();
    let profile = Connection {
        username: "noscan".into(),
        password: "pw".into(),
        ..target.profile("edges-keepfile")
    };
    for format in Format::ALL.into_iter().filter(|f| *f != Format::Dump) {
        let err = rediscope::headless::export(
            profile.clone(),
            &format!("{prefix}*"),
            path.to_str().unwrap(),
            format,
            false,
        )
        .await
        .unwrap_err();
        assert!(format!("{err:#}").contains("cannot read"), "{err:#}");
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "the export from yesterday",
            "{format:?}"
        );
        let names: Vec<_> = std::fs::read_dir(&dir).unwrap().collect();
        assert_eq!(names.len(), 1, "{format:?}: no temporary file is left");
    }
    // A good export replaces it.
    rediscope::headless::export(
        target.profile("edges-keepfile-ok"),
        &format!("{prefix}*"),
        path.to_str().unwrap(),
        Format::Jsonl,
        false,
    )
    .await
    .unwrap();
    assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 2);
    let _ = std::fs::remove_dir_all(dir);
}

// ---- the export form ------------------------------------------------------------

#[tokio::test]
async fn the_export_form_names_the_file_after_the_format_only_while_it_is_the_default() {
    use crossterm::event::{KeyCode, KeyEvent};
    use rediscope::app::{App, EXPORT_FILE, Modal, Msg, Screen};
    use rediscope::config::Store;
    use rediscope::input::InputBuf;
    use rediscope::redis_client::{KeyInfo, KeyType};

    let prefix = "edge:form:";
    let Some(target) = Target::start().await else {
        return;
    };
    let mut raw = target.raw().await;
    let _: () = raw.set(k(prefix, "s"), "v").await.unwrap();
    let client = target.client("edges-form").await;
    let dir = scratch("form");

    // `file` is what the File field holds when Enter is pressed; `rights` how
    // far the format moves.
    async fn run(client: &Client, prefix: &str, file: Option<&str>, rights: usize) -> String {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
        let mut app = App::new(Store::default(), tx);
        app.screen = Screen::Browser;
        app.client = Some(client.clone());
        app.on_msg(Msg::Keys {
            keys: vec![KeyInfo {
                name: format!("{prefix}s"),
                kind: KeyType::String,
                ttl: -1,
            }],
            truncated: false,
            warnings: vec![],
            dbsize: 1,
            pattern: "*".into(),
        });
        app.on_key(KeyEvent::from(KeyCode::Char('w')));
        let Some(Modal::Form { fields, .. }) = &mut app.modal else {
            panic!("w opens the export form");
        };
        assert_eq!(fields[0].input.value(), EXPORT_FILE);
        if let Some(file) = file {
            fields[0].input = InputBuf::new(file);
        }
        app.on_key(KeyEvent::from(KeyCode::Tab));
        for _ in 0..rights {
            app.on_key(KeyEvent::from(KeyCode::Right));
        }
        app.on_key(KeyEvent::from(KeyCode::Enter));
        let msg = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .unwrap()
            .unwrap();
        match msg {
            Msg::Status(text) => text,
            Msg::Error(e) => panic!("{e}"),
            _ => panic!("the export did not report"),
        }
    }

    // A typed name is kept whatever the format.
    let typed = dir.join("rediscope-export.json.txt");
    let status = run(&client, prefix, Some(typed.to_str().unwrap()), 3).await;
    assert!(
        status.ends_with(&format!("as CSV to {}", typed.display())),
        "{status}"
    );
    assert!(
        std::fs::read_to_string(&typed)
            .unwrap()
            .starts_with(CSV_HEADER)
    );
    // The import form then offers the default name in the format just used.
    {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
        let mut app = App::new(Store::default(), tx);
        app.screen = Screen::Browser;
        app.client = Some(client.clone());
        let form_file = |app: &App| match &app.modal {
            Some(Modal::Form { fields, .. }) => fields[0].input.value(),
            _ => panic!("no form"),
        };
        app.on_key(KeyEvent::from(KeyCode::Char('I')));
        assert_eq!(form_file(&app), EXPORT_FILE);
        app.on_key(KeyEvent::from(KeyCode::Esc));
        app.on_msg(Msg::Keys {
            keys: vec![KeyInfo {
                name: format!("{prefix}s"),
                kind: KeyType::String,
                ttl: -1,
            }],
            truncated: false,
            warnings: vec![],
            dbsize: 1,
            pattern: "*".into(),
        });
        app.on_key(KeyEvent::from(KeyCode::Char('w')));
        if let Some(Modal::Form { fields, .. }) = &mut app.modal {
            fields[0].input = InputBuf::new(dir.join("typed.jsonl").to_str().unwrap());
        }
        app.on_key(KeyEvent::from(KeyCode::Tab));
        app.on_key(KeyEvent::from(KeyCode::Right));
        app.on_key(KeyEvent::from(KeyCode::Right));
        app.on_key(KeyEvent::from(KeyCode::Enter));
        app.on_key(KeyEvent::from(KeyCode::Char('I')));
        assert_eq!(form_file(&app), "rediscope-export.jsonl");
    }
    // A typed name that differs from the default only by folder is kept too.
    let folder = dir.join(EXPORT_FILE);
    let status = run(&client, prefix, Some(folder.to_str().unwrap()), 4).await;
    assert!(
        status.ends_with(&format!("to {}", folder.display())),
        "{status}"
    );
    assert!(
        std::fs::read_to_string(&folder)
            .unwrap()
            .starts_with("SET ")
    );

    // The untouched default follows the format. It is relative, so it lands in
    // the working directory: every name it can take is removed afterwards.
    let defaults: Vec<String> = Format::ALL
        .iter()
        .map(|f| format!("rediscope-export.{}", f.extension()))
        .collect();
    let existed: Vec<bool> = defaults.iter().map(|d| Path::new(d).exists()).collect();
    if existed.iter().any(|e| *e) {
        eprintln!(
            "skipped the default name: a rediscope-export file is already in the working directory"
        );
    } else {
        let mut results = Vec::new();
        for (rights, ext) in [(0, "json"), (2, "jsonl"), (3, "csv"), (4, "redis")] {
            let status = run(&client, prefix, None, rights).await;
            let name = format!("rediscope-export.{ext}");
            let body = std::fs::read_to_string(&name).ok();
            let _ = std::fs::remove_file(&name);
            results.push((status, name, body));
        }
        for d in &defaults {
            let _ = std::fs::remove_file(d);
        }
        for (status, name, body) in results {
            assert!(status.ends_with(&format!("to {name}")), "{status}");
            assert!(
                body.is_some_and(|b| !b.is_empty()),
                "{name} was not written"
            );
        }
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn every_written_line_is_one_redis_cli_line() {
    // Nothing a record holds can break a line of a commands file or a JSON Lines file.
    let nasty: Vec<u8> = (0..=255u8).chain(b"\r\n\n\r".iter().copied()).collect();
    let record = Record {
        key: nasty.clone(),
        ttl_ms: Some(1),
        value: Value::Stream(vec![transfer::StreamEntry {
            id: "1-1".into(),
            fields: vec![(nasty.clone(), nasty.clone())],
        }]),
    };
    for format in [Format::Commands, Format::Jsonl] {
        let mut w = transfer::Writer::new(Vec::new(), format, true).unwrap();
        w.write(&record).unwrap();
        let bytes = w.finish().unwrap();
        let lines = bytes
            .split(|b| *b == b'\n')
            .filter(|l| !l.is_empty())
            .count();
        let expected = if format == Format::Commands { 3 } else { 1 };
        assert_eq!(lines, expected, "{format:?}");
    }
    assert_eq!(
        command_line(&[b"XADD".to_vec(), nasty.clone()])
            .lines()
            .count(),
        1
    );
}

#[test]
fn json_keeps_every_bit_of_a_score() {
    // Doubles that print with 17 significant digits are where a best-effort
    // float parser lands on the neighbouring value.
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut scores = vec![123_456_789.123_456_79, 0.1 + 0.2, 1e-300, 5e-324, f64::MAX];
    while scores.len() < 2000 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let f = f64::from_bits(state);
        if f.is_finite() {
            scores.push(f);
        }
    }
    let record = Record {
        key: b"z".to_vec(),
        ttl_ms: None,
        value: Value::ZSet(
            scores
                .iter()
                .enumerate()
                .map(|(i, s)| (i.to_string().into_bytes(), *s))
                .collect(),
        ),
    };
    let mut failures = Vec::new();
    for format in [Format::Json, Format::Jsonl, Format::Csv, Format::Commands] {
        let mut w = transfer::Writer::new(Vec::new(), format, false).unwrap();
        w.write(&record).unwrap();
        let bytes = w.finish().unwrap();
        let back: Vec<u64> = match parse(&bytes).unwrap() {
            Parsed::Records(r, _) => match &r[0].value {
                Value::ZSet(items) => items.iter().map(|(_, s)| s.to_bits()).collect(),
                other => panic!("{other:?}"),
            },
            Parsed::Commands(lines) => lines
                .iter()
                .flat_map(|l| l.args[2..].chunks(2).map(|p| p[0].clone()))
                .map(|s| {
                    String::from_utf8(s)
                        .unwrap()
                        .parse::<f64>()
                        .unwrap()
                        .to_bits()
                })
                .collect(),
            other => panic!("{other:?}"),
        };
        let wrong: Vec<String> = scores
            .iter()
            .zip(&back)
            .filter(|(s, b)| s.to_bits() != **b)
            .map(|(s, b)| format!("{s:e} came back as {:e}", f64::from_bits(*b)))
            .collect();
        if !wrong.is_empty() {
            failures.push(format!(
                "{format:?}: {} of {} scores changed, e.g. {:?}",
                wrong.len(),
                scores.len(),
                &wrong[..wrong.len().min(3)]
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
