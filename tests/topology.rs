//! Deterministic RESP peers exercise redirects and failures without a Redis daemon.

mod common;
use rediscope::{
    config::{Connection, Deployment},
    redis_client::{Client, KeyType, key_slot},
};
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

type Handler = dyn Fn(usize, &[String]) -> Option<String> + Send + Sync;
struct Peer {
    port: u16,
    stopped: Arc<AtomicBool>,
}
impl Peer {
    fn start(handler: impl Fn(usize, &[String]) -> Option<String> + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let stopped = Arc::new(AtomicBool::new(false));
        let stop = stopped.clone();
        let handler: Arc<Handler> = Arc::new(handler);
        thread::spawn(move || {
            let mut id = 0;
            while !stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((socket, _)) => {
                        id += 1;
                        let handler = handler.clone();
                        thread::spawn(move || serve(socket, id, handler));
                    }
                    Err(_) => thread::sleep(Duration::from_millis(2)),
                }
            }
        });
        Self { port, stopped }
    }
    fn profile(&self, deployment: Deployment) -> Connection {
        common::isolate_config();
        Connection {
            name: "protocol-test".into(),
            host: "127.0.0.1".into(),
            port: self.port,
            deployment,
            ..Default::default()
        }
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::SeqCst);
    }
}
fn serve(socket: TcpStream, id: usize, handler: Arc<Handler>) {
    socket.set_nonblocking(false).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut reader = BufReader::new(socket);
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        let Some(n) = line
            .trim()
            .strip_prefix('*')
            .and_then(|s| s.parse::<usize>().ok())
        else {
            return;
        };
        let mut args = Vec::new();
        for _ in 0..n {
            line.clear();
            if reader.read_line(&mut line).is_err() {
                return;
            }
            let Some(len) = line
                .trim()
                .strip_prefix('$')
                .and_then(|s| s.parse::<usize>().ok())
            else {
                return;
            };
            let mut bytes = vec![0; len + 2];
            if reader.read_exact(&mut bytes).is_err() {
                return;
            }
            args.push(String::from_utf8_lossy(&bytes[..len]).to_string());
        }
        // Connection setup (`CLIENT SETINFO`) is answered here; the
        // diagnostics commands `CLIENT LIST` and `CLIENT KILL` reach the handler.
        let response = if args[0].eq_ignore_ascii_case("CLIENT")
            && !args
                .get(1)
                .is_some_and(|s| s.eq_ignore_ascii_case("LIST") || s.eq_ignore_ascii_case("KILL"))
        {
            Some("+OK\r\n".into())
        } else {
            handler(id, &args)
        };
        let Some(response) = response else {
            return;
        };
        // A reply ending in `CLOSE` is written, then the connection dropped:
        // a subscriber or monitor socket that the server lets go of.
        let (response, close) = match response.strip_suffix(CLOSE) {
            Some(rest) => (rest.to_string(), true),
            None => (response, false),
        };
        if reader.get_mut().write_all(response.as_bytes()).is_err() || close {
            return;
        }
    }
}
/// Appended to a scripted reply, closes the connection once it is written.
const CLOSE: &str = "\0CLOSE";
fn bulk(s: &str) -> String {
    format!("${}\r\n{s}\r\n", s.len())
}
fn slots(nodes: &[(u16, u16, u16)]) -> String {
    let mut reply = format!("*{}\r\n", nodes.len());
    for (start, end, port) in nodes {
        reply.push_str(&format!(
            "*3\r\n:{start}\r\n:{end}\r\n*3\r\n{}:{port}\r\n{}",
            bulk("127.0.0.1"),
            bulk(&format!("node-{port}"))
        ));
    }
    reply
}

#[tokio::test]
async fn moved_is_cached_and_ask_is_one_request_on_a_dedicated_socket() {
    for ask in [false, true] {
        let asking = Arc::new(Mutex::new(Vec::new()));
        let seen = asking.clone();
        let target = Peer::start(move |id, args| match args[0].as_str() {
            "ASKING" => {
                seen.lock().unwrap().push(id);
                Some("+OK\r\n".into())
            }
            "GET" => {
                if ask {
                    assert!(
                        seen.lock().unwrap().contains(&id),
                        "ASKING must precede GET on the same socket"
                    );
                }
                Some(bulk("routed"))
            }
            _ => Some("+OK\r\n".into()),
        });
        let port = Arc::new(AtomicUsize::new(0));
        let own = port.clone();
        let gets = Arc::new(AtomicUsize::new(0));
        let calls = gets.clone();
        let target_port = target.port;
        let source = Peer::start(move |_, args| match args[0].as_str() {
            "CLUSTER" => Some(slots(&[(0, 16383, own.load(Ordering::SeqCst) as u16)])),
            "GET" => {
                calls.fetch_add(1, Ordering::SeqCst);
                Some(format!(
                    "-{} {} 127.0.0.1:{target_port}\r\n",
                    if ask { "ASK" } else { "MOVED" },
                    key_slot(b"key")
                ))
            }
            _ => Some("+OK\r\n".into()),
        });
        port.store(source.port as usize, Ordering::SeqCst);
        let client = Client::connect(source.profile(Deployment::Cluster))
            .await
            .unwrap();
        for _ in 0..2 {
            assert!(
                client
                    .execute_raw("GET key")
                    .await
                    .unwrap()
                    .contains("routed")
            );
        }
        assert_eq!(gets.load(Ordering::SeqCst), if ask { 2 } else { 1 });
        assert!(client.set_string("key", "written").await.is_ok());
        assert!(client.execute_raw("CONFIG SET maxmemory 1").await.is_ok());
        assert!(
            client
                .execute_raw("FLUSHDB")
                .await
                .unwrap_err()
                .to_string()
                .contains("only one primary")
        );
    }
}

#[tokio::test]
async fn scan_keeps_available_keys_and_reports_unavailable_primary() {
    let port = Arc::new(AtomicUsize::new(0));
    let own = port.clone();
    let dead = TcpListener::bind("127.0.0.1:0").unwrap();
    let unavailable = dead.local_addr().unwrap().port();
    drop(dead);
    let key = (0..100)
        .map(|n| format!("key{n}"))
        .find(|k| key_slot(k.as_bytes()) < 8192)
        .unwrap();
    let expected = key.clone();
    let source = Peer::start(move |_, args| match args[0].as_str() {
        "CLUSTER" => Some(slots(&[
            (0, 8191, own.load(Ordering::SeqCst) as u16),
            (8192, 16383, unavailable),
        ])),
        "SCAN" => Some(format!(
            "*2\r\n$1\r\n0\r\n*2\r\n{}{}",
            bulk(&key),
            bulk(&key)
        )),
        "TYPE" => Some("+string\r\n".into()),
        "TTL" => Some(":-1\r\n".into()),
        _ => Some("+OK\r\n".into()),
    });
    port.store(source.port as usize, Ordering::SeqCst);
    let client = Client::connect(source.profile(Deployment::Cluster))
        .await
        .unwrap();
    let report = client.scan_report("*", 100).await.unwrap();
    assert_eq!(report.keys.len(), 1);
    assert_eq!(report.keys[0].name, expected);
    assert!(!report.truncated);
    assert!(
        report
            .warnings
            .iter()
            .any(|s| s.contains(&unavailable.to_string()))
    );
}

#[tokio::test]
async fn sentinel_rediscovers_primary_after_disconnect_and_verifies_role() {
    let second = Peer::start(|_, args| match args[0].as_str() {
        "ROLE" => Some("*1\r\n$6\r\nmaster\r\n".into()),
        "GET" => Some(bulk("new primary")),
        _ => Some("+OK\r\n".into()),
    });
    let active = Arc::new(AtomicUsize::new(0));
    let switch = active.clone();
    let next_port = second.port;
    let first = Peer::start(move |_, args| match args[0].as_str() {
        "ROLE" => Some("*1\r\n$6\r\nmaster\r\n".into()),
        "GET" => {
            switch.store(next_port as usize, Ordering::SeqCst);
            None
        }
        _ => Some("+OK\r\n".into()),
    });
    active.store(first.port as usize, Ordering::SeqCst);
    let master = active.clone();
    let sentinel = Peer::start(move |_, args| match args[0].as_str() {
        "SENTINEL" => Some(format!(
            "*2\r\n{}{}",
            bulk("127.0.0.1"),
            bulk(&master.load(Ordering::SeqCst).to_string())
        )),
        _ => Some("+OK\r\n".into()),
    });
    let mut profile = sentinel.profile(Deployment::Sentinel);
    profile.sentinel_master = "service".into();
    let client = Client::connect(profile).await.unwrap();
    assert!(
        client
            .execute_raw("GET key")
            .await
            .unwrap()
            .contains("new primary")
    );
    assert_eq!(
        client.refresh_topology().await.unwrap()[0].port,
        second.port
    );
}

#[tokio::test]
async fn disconnected_reads_reconnect_but_unknown_writes_are_never_replayed() {
    let writes = Arc::new(AtomicUsize::new(0));
    let observed = writes.clone();
    let reads = Arc::new(AtomicUsize::new(0));
    let fetched = reads.clone();
    let server = Peer::start(move |_, args| match args[0].as_str() {
        "SET" => {
            observed.fetch_add(1, Ordering::SeqCst);
            None
        }
        "GET" => {
            if fetched.fetch_add(1, Ordering::SeqCst) == 0 {
                None
            } else {
                Some(bulk("ok"))
            }
        }
        _ => Some("+OK\r\n".into()),
    });
    let client = Client::connect(server.profile(Deployment::Standalone))
        .await
        .unwrap();
    assert!(client.read_value("key", KeyType::String).await.is_ok());
    let err = client
        .set_string("key", "value")
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("outcome unknown"));
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    assert!(client.read_value("key", KeyType::String).await.is_ok());
}

#[test]
fn old_profiles_default_to_standalone_and_topology_profiles_round_trip() {
    let old: Connection = serde_json::from_str(r#"{"name":"old"}"#).unwrap();
    assert_eq!(old.deployment, Deployment::Standalone);
    let c = Connection {
        deployment: Deployment::Sentinel,
        seeds: vec!["[::1]:26379".into()],
        sentinel_master: "service".into(),
        ..old
    };
    let decoded: Connection = serde_json::from_str(&serde_json::to_string(&c).unwrap()).unwrap();
    assert_eq!(decoded.deployment, Deployment::Sentinel);
    assert_eq!(decoded.seeds, c.seeds);
}

#[tokio::test]
async fn sentinel_rejects_a_replica_candidate_and_tries_another_seed() {
    let replica = Peer::start(|_, args| match args[0].as_str() {
        "ROLE" => Some("*1\r\n$5\r\nslave\r\n".into()),
        _ => Some("+OK\r\n".into()),
    });
    let primary = Peer::start(|_, args| match args[0].as_str() {
        "ROLE" => Some("*1\r\n$6\r\nmaster\r\n".into()),
        _ => Some("+OK\r\n".into()),
    });
    let candidate = replica.port;
    let stale = Peer::start(move |_, _| {
        Some(format!(
            "*2\r\n{}{}",
            bulk("127.0.0.1"),
            bulk(&candidate.to_string())
        ))
    });
    let candidate = primary.port;
    let current = Peer::start(move |_, _| {
        Some(format!(
            "*2\r\n{}{}",
            bulk("127.0.0.1"),
            bulk(&candidate.to_string())
        ))
    });
    let mut profile = stale.profile(Deployment::Sentinel);
    profile.sentinel_master = "service".into();
    assert!(Client::connect(profile.clone()).await.is_err());
    profile.seeds.push(format!("127.0.0.1:{}", current.port));
    let client = Client::connect(profile).await.unwrap();
    assert_eq!(
        client.refresh_topology().await.unwrap()[0].port,
        primary.port
    );
}

#[tokio::test]
async fn sentinel_and_data_authentication_are_separate() {
    let data_auth = Arc::new(Mutex::new(Vec::new()));
    let seen = data_auth.clone();
    let data = Peer::start(move |_, args| match args[0].as_str() {
        "AUTH" => {
            seen.lock().unwrap().push(args.to_vec());
            Some("+OK\r\n".into())
        }
        "ROLE" => Some("*1\r\n$6\r\nmaster\r\n".into()),
        _ => Some("+OK\r\n".into()),
    });
    let sentinel_auth = Arc::new(Mutex::new(Vec::new()));
    let seen = sentinel_auth.clone();
    let port = data.port;
    let sentinel = Peer::start(move |_, args| match args[0].as_str() {
        "AUTH" => {
            seen.lock().unwrap().push(args.to_vec());
            Some("+OK\r\n".into())
        }
        "SENTINEL" => Some(format!(
            "*2\r\n{}{}",
            bulk("127.0.0.1"),
            bulk(&port.to_string())
        )),
        _ => Some("+OK\r\n".into()),
    });
    let mut profile = sentinel.profile(Deployment::Sentinel);
    profile.sentinel_master = "service".into();
    profile.username = "data-user".into();
    profile.password = "data-password".into();
    profile.sentinel_username = "sentinel-user".into();
    profile.sentinel_password = "sentinel-password".into();
    Client::connect(profile).await.unwrap();
    assert_eq!(
        *data_auth.lock().unwrap(),
        vec![vec!["AUTH", "data-user", "data-password"]]
    );
    assert_eq!(
        *sentinel_auth.lock().unwrap(),
        vec![vec!["AUTH", "sentinel-user", "sentinel-password"]]
    );
}

#[tokio::test]
async fn redirect_loops_stop_and_cluster_database_is_validated() {
    let port = Arc::new(AtomicUsize::new(0));
    let own = port.clone();
    let gets = Arc::new(AtomicUsize::new(0));
    let seen = gets.clone();
    let source = Peer::start(move |_, args| match args[0].as_str() {
        "CLUSTER" => Some(slots(&[(0, 16383, own.load(Ordering::SeqCst) as u16)])),
        "GET" => {
            seen.fetch_add(1, Ordering::SeqCst);
            Some(format!(
                "-ASK {} 127.0.0.1:{}\r\n",
                key_slot(b"key"),
                own.load(Ordering::SeqCst)
            ))
        }
        _ => Some("+OK\r\n".into()),
    });
    port.store(source.port as usize, Ordering::SeqCst);
    let mut profile = source.profile(Deployment::Cluster);
    profile.db = 1;
    assert!(
        Client::connect(profile.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("database 0")
    );
    profile.db = 0;
    let client = Client::connect(profile).await.unwrap();
    assert!(
        client
            .execute_raw("GET key")
            .await
            .unwrap_err()
            .to_string()
            .contains("Redirect limit")
    );
    assert_eq!(gets.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn a_lost_write_pipeline_is_not_replayed() {
    let deletes = Arc::new(AtomicUsize::new(0));
    let seen = deletes.clone();
    let source = Peer::start(move |_, args| match args[0].as_str() {
        "UNLINK" => {
            seen.fetch_add(1, Ordering::SeqCst);
            None
        }
        _ => Some("+OK\r\n".into()),
    });
    let client = Client::connect(source.profile(Deployment::Standalone))
        .await
        .unwrap();
    let error = client
        .delete_keys(&["one".into(), "two".into()])
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("outcome unknown"));
    assert_eq!(deletes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn scan_metadata_follows_redirects_after_a_key_moves() {
    let target = Peer::start(|_, args| match args[0].as_str() {
        "TYPE" => Some("+string\r\n".into()),
        "TTL" => Some(":-1\r\n".into()),
        _ => Some("+OK\r\n".into()),
    });
    let port = Arc::new(AtomicUsize::new(0));
    let own = port.clone();
    let destination = target.port;
    let source = Peer::start(move |_, args| match args[0].as_str() {
        "CLUSTER" => Some(slots(&[(0, 16383, own.load(Ordering::SeqCst) as u16)])),
        "SCAN" => Some(format!("*2\r\n$1\r\n0\r\n*1\r\n{}", bulk("key"))),
        "TYPE" | "TTL" => Some(format!(
            "-MOVED {} 127.0.0.1:{destination}\r\n",
            key_slot(b"key")
        )),
        _ => Some("+OK\r\n".into()),
    });
    port.store(source.port as usize, Ordering::SeqCst);
    let client = Client::connect(source.profile(Deployment::Cluster))
        .await
        .unwrap();
    let report = client.scan_report("*", 100).await.unwrap();
    assert!(report.warnings.is_empty(), "{:?}", report.warnings);
    assert_eq!(report.keys[0].kind, KeyType::String);
}

// ---- cluster and sentinel writes ---------------------------------------

/// Every command a fake node received (except `CLUSTER` and `CLIENT`), with
/// the id of the socket it arrived on.
type Log = Arc<Mutex<Vec<(usize, Vec<String>)>>>;
/// A scripted reply for node `'a'` or `'b'`: `Some(Some(reply))` answers,
/// `Some(None)` drops the connection, `None` falls back to the default reply.
type Script = dyn Fn(char, usize, &[String]) -> Option<Option<String>> + Send + Sync;

/// Two primaries: `a` owns slots 0-8191 and is the seed, `b` owns 8192-16383.
struct TwoNodes {
    a: Peer,
    b: Peer,
    a_log: Log,
    b_log: Log,
    /// `CLUSTER SLOTS` requests each node answered, `[a, b]`.
    discoveries: Arc<[AtomicUsize; 2]>,
}
impl TwoNodes {
    fn start(
        script: impl Fn(char, usize, &[String]) -> Option<Option<String>> + Send + Sync + 'static,
    ) -> Self {
        Self::start_merging(Arc::default(), script)
    }
    /// Like `start`, but once `merged` is set both nodes report that `a`
    /// owns every slot.
    fn start_merging(
        merged: Arc<AtomicBool>,
        script: impl Fn(char, usize, &[String]) -> Option<Option<String>> + Send + Sync + 'static,
    ) -> Self {
        let ports = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
        let discoveries: Arc<[AtomicUsize; 2]> = Arc::default();
        let script: Arc<Script> = Arc::new(script);
        let node = |name: char| {
            let log: Log = Arc::default();
            let seen = log.clone();
            let ports = ports.clone();
            let script = script.clone();
            let merged = merged.clone();
            let asked = discoveries.clone();
            let peer = Peer::start(move |id, args| {
                let head = args[0].to_ascii_uppercase();
                if head == "CLUSTER" {
                    asked[usize::from(name == 'b')].fetch_add(1, Ordering::SeqCst);
                    let (a, b) = (
                        ports[0].load(Ordering::SeqCst) as u16,
                        ports[1].load(Ordering::SeqCst) as u16,
                    );
                    return Some(if merged.load(Ordering::SeqCst) {
                        slots(&[(0, 16383, a)])
                    } else {
                        slots(&[(0, 8191, a), (8192, 16383, b)])
                    });
                }
                seen.lock().unwrap().push((id, args.to_vec()));
                if let Some(reply) = script(name, id, args) {
                    return reply;
                }
                Some(match head.as_str() {
                    "UNLINK" | "DEL" | "EXPIRE" | "PERSIST" | "PUBLISH" => ":1\r\n".into(),
                    _ => "+OK\r\n".into(),
                })
            });
            (peer, log)
        };
        let (a, a_log) = node('a');
        let (b, b_log) = node('b');
        ports[0].store(a.port as usize, Ordering::SeqCst);
        ports[1].store(b.port as usize, Ordering::SeqCst);
        Self {
            a,
            b,
            a_log,
            b_log,
            discoveries,
        }
    }
    fn profile(&self) -> Connection {
        self.a.profile(Deployment::Cluster)
    }
    async fn client(&self) -> Client {
        Client::connect(self.profile()).await.unwrap()
    }
}
/// How many times a log saw `head`, optionally with `key` as its first argument.
fn count(log: &Log, head: &str, key: Option<&str>) -> usize {
    log.lock()
        .unwrap()
        .iter()
        .filter(|(_, args)| {
            args[0].eq_ignore_ascii_case(head)
                && key.is_none_or(|k| args.get(1).is_some_and(|a| a == k))
        })
        .count()
}
fn heads(log: &Log) -> Vec<String> {
    log.lock()
        .unwrap()
        .iter()
        .map(|(_, args)| args[0].to_ascii_uppercase())
        .collect()
}
/// The `n`th key with `prefix` whose slot is on node `a` (low) or `b` (high).
fn key_on(low: bool, prefix: &str, n: usize) -> String {
    (0..)
        .map(|i| format!("{prefix}{i}"))
        .filter(|k| (key_slot(k.as_bytes()) < 8192) == low)
        .nth(n)
        .unwrap()
}
fn tag_on(low: bool) -> String {
    let tag = key_on(low, "tag", 0);
    format!("{{{tag}}}")
}
fn moved(key: &str, port: u16) -> String {
    format!("-MOVED {} 127.0.0.1:{port}\r\n", key_slot(key.as_bytes()))
}

#[tokio::test]
async fn cluster_single_key_write_goes_to_the_owning_primary() {
    let nodes = TwoNodes::start(|_, _, _| None);
    let client = nodes.client().await;
    let (ka, kb) = (key_on(true, "w", 0), key_on(false, "w", 0));
    client.set_string(&kb, "value").await.unwrap();
    assert_eq!(count(&nodes.b_log, "SET", Some(&kb)), 1);
    assert_eq!(count(&nodes.a_log, "SET", None), 0);
    client.set_string(&ka, "value").await.unwrap();
    assert_eq!(count(&nodes.a_log, "SET", Some(&ka)), 1);
    assert_eq!(count(&nodes.b_log, "SET", None), 1);
    client.execute_raw(&format!("HSET {kb} f v")).await.unwrap();
    assert_eq!(count(&nodes.b_log, "HSET", Some(&kb)), 1);
    assert_eq!(count(&nodes.a_log, "HSET", None), 0);
}

#[tokio::test]
async fn cluster_cross_slot_writes_are_refused_before_sending() {
    let nodes = TwoNodes::start(|_, _, _| None);
    let client = nodes.client().await;
    let (ka, kb) = (key_on(true, "x", 0), key_on(false, "x", 0));
    let errors = [
        client.rename_key(&ka, &kb).await.unwrap_err().to_string(),
        client
            .execute_raw(&format!("RENAME {kb} {ka}"))
            .await
            .unwrap_err()
            .to_string(),
        client
            .execute_raw(&format!("MSET {ka} 1 {kb} 2"))
            .await
            .unwrap_err()
            .to_string(),
        client
            .execute_raw(&format!("DEL {ka} {kb}"))
            .await
            .unwrap_err()
            .to_string(),
        client
            .eval("return 1", &[ka.clone(), kb.clone()], &[])
            .await
            .unwrap_err()
            .to_string(),
    ];
    for e in &errors {
        assert!(
            e.contains("different cluster slots") && e.contains("CROSSSLOT"),
            "{e}"
        );
    }
    for log in [&nodes.a_log, &nodes.b_log] {
        let heads = heads(log);
        for refused in ["RENAME", "MSET", "DEL", "EVAL"] {
            assert!(
                !heads.iter().any(|h| h == refused),
                "{refused} was sent: {heads:?}"
            );
        }
    }
}

#[tokio::test]
async fn cluster_same_hash_tag_rename_goes_to_the_owning_primary() {
    let nodes = TwoNodes::start(|_, _, _| None);
    let client = nodes.client().await;
    let tag = tag_on(false);
    let (old, new) = (format!("{tag}:old"), format!("{tag}:new"));
    assert!(key_slot(old.as_bytes()) >= 8192);
    client.rename_key(&old, &new).await.unwrap();
    assert_eq!(count(&nodes.b_log, "RENAME", Some(&old)), 1);
    assert_eq!(count(&nodes.a_log, "RENAME", None), 0);
    // Two same-slot keys in a script also route by that slot.
    client
        .eval("return 1", &[old.clone(), new.clone()], &["arg".into()])
        .await
        .unwrap();
    assert_eq!(count(&nodes.b_log, "EVAL", None), 1);
    assert_eq!(count(&nodes.a_log, "EVAL", None), 0);
}

#[tokio::test]
async fn cluster_keyless_writes_flushdb_refused_config_and_publish_allowed() {
    let nodes = TwoNodes::start(|_, _, _| None);
    let client = nodes.client().await;
    for refused in ["FLUSHDB", "FLUSHALL", "SCRIPT FLUSH", "FUNCTION FLUSH"] {
        let e = client.execute_raw(refused).await.unwrap_err().to_string();
        assert!(e.contains("only one primary"), "{refused}: {e}");
    }
    for log in [&nodes.a_log, &nodes.b_log] {
        let heads = heads(log);
        assert!(
            !heads
                .iter()
                .any(|h| matches!(h.as_str(), "FLUSHDB" | "FLUSHALL" | "SCRIPT" | "FUNCTION")),
            "{heads:?}"
        );
    }
    client.execute_raw("CONFIG SET maxmemory 1").await.unwrap();
    assert_eq!(count(&nodes.a_log, "CONFIG", None), 1);
    assert_eq!(count(&nodes.b_log, "CONFIG", None), 0);
    client.config_set("maxmemory", "2").await.unwrap();
    assert_eq!(count(&nodes.a_log, "CONFIG", None), 2);
    assert_eq!(
        client.execute_raw("PUBLISH channel hello").await.unwrap(),
        "(integer) 1"
    );
    assert_eq!(count(&nodes.a_log, "PUBLISH", None), 1);
    assert_eq!(count(&nodes.b_log, "PUBLISH", None), 0);
    // A keyless script can write any key on the node it runs on.
    let e = client
        .eval("return 1", &[], &[])
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("only one primary"), "{e}");
    assert_eq!(count(&nodes.a_log, "EVAL", None), 0);
    assert_eq!(count(&nodes.b_log, "EVAL", None), 0);
}

#[tokio::test]
async fn cluster_unknown_commands_are_routed_by_command_getkeys() {
    let kb = key_on(false, "mod", 0);
    let routed = kb.clone();
    let nodes = TwoNodes::start(move |_, _, args| {
        if !args[0].eq_ignore_ascii_case("COMMAND") {
            return None;
        }
        Some(Some(match args.get(2).map(String::as_str) {
            Some("MYMOD.WRITE") => format!("*1\r\n{}", bulk(&routed)),
            Some("MYMOD.NOKEYS") => "-ERR The command has no key arguments\r\n".into(),
            _ => "-ERR Invalid command specified\r\n".into(),
        }))
    });
    let client = nodes.client().await;
    client
        .execute_raw(&format!("MYMOD.WRITE {kb} value"))
        .await
        .unwrap();
    let asked: Vec<Vec<String>> = nodes
        .a_log
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, a)| a[0] == "COMMAND")
        .map(|(_, a)| a.clone())
        .collect();
    assert_eq!(
        asked,
        vec![vec![
            "COMMAND".to_string(),
            "GETKEYS".into(),
            "MYMOD.WRITE".into(),
            kb.clone(),
            "value".into()
        ]]
    );
    assert_eq!(count(&nodes.b_log, "MYMOD.WRITE", Some(&kb)), 1);
    assert_eq!(count(&nodes.a_log, "MYMOD.WRITE", None), 0);

    let e = client
        .execute_raw("MYMOD.NOKEYS arg")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("only one primary"), "{e}");
    let e = client
        .execute_raw("MYMOD.BROKEN arg")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("nothing was sent"), "{e}");
    for log in [&nodes.a_log, &nodes.b_log] {
        assert_eq!(count(log, "MYMOD.NOKEYS", None), 0);
        assert_eq!(count(log, "MYMOD.BROKEN", None), 0);
    }
}

#[tokio::test]
async fn cluster_write_follows_moved_once_and_caches_the_new_owner() {
    let ka = key_on(true, "moved", 0);
    let target = Arc::new(AtomicUsize::new(0));
    let port = target.clone();
    let key = ka.clone();
    let nodes = TwoNodes::start(move |node, _, args| {
        (node == 'a' && args[0] == "SET" && args[1] == key)
            .then(|| Some(moved(&key, port.load(Ordering::SeqCst) as u16)))
    });
    target.store(nodes.b.port as usize, Ordering::SeqCst);
    let client = nodes.client().await;
    client.set_string(&ka, "one").await.unwrap();
    assert_eq!(count(&nodes.a_log, "SET", Some(&ka)), 1);
    assert_eq!(count(&nodes.b_log, "SET", Some(&ka)), 1);
    client.set_string(&ka, "two").await.unwrap();
    assert_eq!(
        count(&nodes.a_log, "SET", Some(&ka)),
        1,
        "MOVED was not cached"
    );
    assert_eq!(count(&nodes.b_log, "SET", Some(&ka)), 2);
}

#[tokio::test]
async fn cluster_write_ask_sends_asking_and_the_write_on_one_socket() {
    let ka = key_on(true, "ask", 0);
    let target = Arc::new(AtomicUsize::new(0));
    let port = target.clone();
    let key = ka.clone();
    let asked = Arc::new(Mutex::new(Vec::new()));
    let sockets = asked.clone();
    let nodes = TwoNodes::start(move |node, id, args| match (node, args[0].as_str()) {
        ('a', "SET") => Some(Some(format!(
            "-ASK {} 127.0.0.1:{}\r\n",
            key_slot(key.as_bytes()),
            port.load(Ordering::SeqCst)
        ))),
        ('b', "ASKING") => {
            sockets.lock().unwrap().push(id);
            Some(Some("+OK\r\n".into()))
        }
        ('b', "SET") if !sockets.lock().unwrap().contains(&id) => Some(Some(
            "-ERR SET arrived without ASKING on its socket\r\n".into(),
        )),
        _ => None,
    });
    target.store(nodes.b.port as usize, Ordering::SeqCst);
    let client = nodes.client().await;
    for round in 1..=2 {
        client.set_string(&ka, "value").await.unwrap();
        // ASK is not cached: every write asks the source again.
        assert_eq!(count(&nodes.a_log, "SET", Some(&ka)), round);
        assert_eq!(count(&nodes.b_log, "SET", Some(&ka)), round);
        assert_eq!(count(&nodes.b_log, "ASKING", None), round);
    }
}

#[tokio::test]
async fn cluster_write_refused_unrun_is_retried_until_it_lands() {
    for kind in [
        "READONLY You can't write against a read only replica.",
        "TRYAGAIN Multiple keys request during rehashing of slot",
        "CLUSTERDOWN The cluster is down",
        "MASTERDOWN Link with MASTER is down",
        "LOADING Redis is loading the dataset in memory",
    ] {
        let refusals = Arc::new(AtomicUsize::new(0));
        let seen = refusals.clone();
        let nodes = TwoNodes::start(move |node, _, args| {
            (node == 'b' && args[0] == "SET" && seen.fetch_add(1, Ordering::SeqCst) == 0)
                .then(|| Some(format!("-{kind}\r\n")))
        });
        let client = nodes.client().await;
        let kb = key_on(false, "retry", 0);
        client
            .set_string(&kb, "value")
            .await
            .unwrap_or_else(|e| panic!("{kind}: {e}"));
        assert_eq!(count(&nodes.b_log, "SET", Some(&kb)), 2, "{kind}");
        assert_eq!(count(&nodes.a_log, "SET", None), 0, "{kind}");
    }
}

#[tokio::test]
async fn cluster_write_refused_every_time_stops_after_bounded_attempts() {
    let nodes = TwoNodes::start(|node, _, args| {
        (node == 'b' && args[0] == "SET").then(|| Some("-TRYAGAIN still migrating\r\n".into()))
    });
    let client = nodes.client().await;
    let kb = key_on(false, "stuck", 0);
    let e = client
        .set_string(&kb, "value")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("still migrating"), "{e}");
    assert_eq!(count(&nodes.b_log, "SET", Some(&kb)), 4);
}

#[tokio::test]
async fn cluster_write_on_a_dropped_connection_is_unknown_and_not_replayed() {
    let nodes = TwoNodes::start(|node, _, args| (node == 'b' && args[0] == "SET").then_some(None));
    let client = nodes.client().await;
    let kb = key_on(false, "lost", 0);
    let e = client
        .set_string(&kb, "value")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("outcome unknown"), "{e}");
    assert_eq!(count(&nodes.b_log, "SET", Some(&kb)), 1);
    assert_eq!(count(&nodes.a_log, "SET", None), 0);
    // The node is still usable afterwards, and the lost write stays lost.
    client.set_string(&key_on(false, "lost", 1), "v").await.ok();
    assert_eq!(count(&nodes.b_log, "SET", Some(&kb)), 1);
}

#[tokio::test]
async fn cluster_delete_keys_splits_per_node_and_sums_counts() {
    let nodes = TwoNodes::start(|_, _, _| None);
    let client = nodes.client().await;
    let (a1, a2) = (key_on(true, "del", 0), key_on(true, "del", 1));
    let (b1, b2) = (key_on(false, "del", 0), key_on(false, "del", 1));
    let names = vec![a1.clone(), b1.clone(), a2.clone(), b2.clone()];
    assert_eq!(client.delete_keys(&names).await.unwrap(), 4);
    for k in [&a1, &a2] {
        assert_eq!(count(&nodes.a_log, "UNLINK", Some(k)), 1);
    }
    for k in [&b1, &b2] {
        assert_eq!(count(&nodes.b_log, "UNLINK", Some(k)), 1);
    }
    assert_eq!(count(&nodes.a_log, "UNLINK", None), 2);
    assert_eq!(count(&nodes.b_log, "UNLINK", None), 2);
    // Each node's commands keep their original order.
    let order: Vec<String> = nodes
        .a_log
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, a)| a[0] == "UNLINK")
        .map(|(_, a)| a[1].clone())
        .collect();
    assert_eq!(order, vec![a1, a2]);
}

#[tokio::test]
async fn cluster_delete_keys_resends_a_moved_or_refused_key_once() {
    let a2 = key_on(true, "mv", 1);
    let b1 = key_on(false, "mv", 0);
    let target = Arc::new(AtomicUsize::new(0));
    let port = target.clone();
    let (moved_key, busy_key) = (a2.clone(), b1.clone());
    let busy = Arc::new(AtomicUsize::new(0));
    let nodes = TwoNodes::start(move |node, _, args| match (node, args[0].as_str()) {
        ('a', "UNLINK") if args[1] == moved_key => {
            Some(Some(moved(&moved_key, port.load(Ordering::SeqCst) as u16)))
        }
        ('b', "UNLINK") if args[1] == busy_key && busy.fetch_add(1, Ordering::SeqCst) == 0 => {
            Some(Some("-TRYAGAIN rehashing\r\n".into()))
        }
        _ => None,
    });
    target.store(nodes.b.port as usize, Ordering::SeqCst);
    let client = nodes.client().await;
    let a1 = key_on(true, "mv", 0);
    let b2 = key_on(false, "mv", 1);
    let names = vec![a1.clone(), b1.clone(), a2.clone(), b2.clone()];
    assert_eq!(client.delete_keys(&names).await.unwrap(), 4);
    assert_eq!(
        count(&nodes.b_log, "UNLINK", Some(&a2)),
        1,
        "moved key reached its target once"
    );
    assert_eq!(count(&nodes.a_log, "UNLINK", Some(&a1)), 1);
    assert_eq!(
        count(&nodes.b_log, "UNLINK", Some(&b1)),
        2,
        "refused once, then sent again"
    );
    assert_eq!(count(&nodes.b_log, "UNLINK", Some(&b2)), 1);
}

#[tokio::test]
async fn cluster_delete_keys_lost_connection_on_second_node_is_not_replayed() {
    let nodes =
        TwoNodes::start(|node, _, args| (node == 'b' && args[0] == "UNLINK").then_some(None));
    let client = nodes.client().await;
    let (a1, a2) = (key_on(true, "gone", 0), key_on(true, "gone", 1));
    let (b1, b2) = (key_on(false, "gone", 0), key_on(false, "gone", 1));
    let e = client
        .delete_keys(&[a1.clone(), b1.clone(), a2.clone(), b2.clone()])
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("outcome unknown"), "{e}");
    assert!(
        e.contains("1 other node(s)") && e.contains("applied"),
        "{e}"
    );
    assert_eq!(count(&nodes.a_log, "UNLINK", None), 2);
    assert_eq!(count(&nodes.b_log, "UNLINK", None), 1);
}

#[tokio::test]
async fn cluster_delete_keys_lost_connection_on_first_node_sends_nothing_else() {
    let nodes =
        TwoNodes::start(|node, _, args| (node == 'a' && args[0] == "UNLINK").then_some(None));
    let client = nodes.client().await;
    let (a1, b1) = (key_on(true, "first", 0), key_on(false, "first", 0));
    let e = client.delete_keys(&[a1, b1]).await.unwrap_err().to_string();
    assert!(
        e.contains("outcome unknown") && !e.contains("other node"),
        "{e}"
    );
    assert_eq!(count(&nodes.a_log, "UNLINK", None), 1);
    assert_eq!(count(&nodes.b_log, "UNLINK", None), 0);
}

#[tokio::test]
async fn cluster_expire_keys_across_nodes() {
    let nodes = TwoNodes::start(|_, _, args| {
        (args[0] == "EXPIRE" && args[1].ends_with('1')).then(|| Some(":0\r\n".into()))
    });
    let client = nodes.client().await;
    let names = vec![
        key_on(true, "ttl", 0),
        key_on(false, "ttl", 0),
        key_on(true, "ttl", 1),
        key_on(false, "ttl", 1),
    ];
    let expected = 4 - names.iter().filter(|n| n.ends_with('1')).count() as u64;
    assert_eq!(
        client.expire_keys(&names, Some(30)).await.unwrap(),
        expected
    );
    for (i, name) in names.iter().enumerate() {
        let (own, other) = if i % 2 == 0 {
            (&nodes.a_log, &nodes.b_log)
        } else {
            (&nodes.b_log, &nodes.a_log)
        };
        assert_eq!(count(own, "EXPIRE", Some(name)), 1);
        assert_eq!(count(other, "EXPIRE", Some(name)), 0);
    }
    assert_eq!(client.expire_keys(&names, None).await.unwrap(), 4);
    assert_eq!(count(&nodes.a_log, "PERSIST", None), 2);
    assert_eq!(count(&nodes.b_log, "PERSIST", None), 2);
}

#[tokio::test]
async fn cluster_read_only_and_production_profiles_still_guard_writes() {
    let nodes = TwoNodes::start(|_, _, _| None);
    let kb = key_on(false, "guard", 0);

    let mut hard = nodes.profile();
    hard.read_only = true;
    let client = Client::connect(hard.clone()).await.unwrap();
    assert!(client.read_only());
    assert!(client.set_string(&kb, "v").await.is_err());
    assert!(client.delete_keys(std::slice::from_ref(&kb)).await.is_err());
    assert!(client.execute_raw("CONFIG SET maxmemory 1").await.is_err());
    assert!(client.unlock_writes(&hard.name).is_err());
    assert!(client.set_string(&kb, "v").await.is_err());

    let mut production = nodes.profile();
    production.name = "cluster-production".into();
    production.environment = rediscope::config::Environment::Production;
    let client = Client::connect(production.clone()).await.unwrap();
    assert!(client.read_only());
    let e = client.set_string(&kb, "v").await.unwrap_err().to_string();
    assert!(e.contains("Read-only"), "{e}");
    assert!(client.delete_keys(std::slice::from_ref(&kb)).await.is_err());
    for log in [&nodes.a_log, &nodes.b_log] {
        let heads = heads(log);
        assert!(
            !heads
                .iter()
                .any(|h| matches!(h.as_str(), "SET" | "UNLINK" | "CONFIG")),
            "{heads:?}"
        );
    }
    assert!(client.unlock_writes("wrong").is_err());
    client.unlock_writes(&production.name).unwrap();
    assert!(!client.read_only());
    client.set_string(&kb, "v").await.unwrap();
    assert_eq!(
        client.delete_keys(std::slice::from_ref(&kb)).await.unwrap(),
        1
    );
    assert_eq!(count(&nodes.b_log, "SET", Some(&kb)), 1);
    assert_eq!(count(&nodes.b_log, "UNLINK", Some(&kb)), 1);
    client.lock_writes().unwrap();
    assert!(client.set_string(&kb, "v").await.is_err());
    assert_eq!(count(&nodes.b_log, "SET", Some(&kb)), 1);
}

#[tokio::test]
async fn cluster_writes_are_audited_with_their_outcome() {
    let lost = key_on(false, "audit-lost", 0);
    let drop_key = lost.clone();
    let nodes = TwoNodes::start(move |_, _, args| {
        (args[0] == "SET" && args[1] == drop_key).then_some(None)
    });
    let mut profile = nodes.profile();
    profile.name = format!("audit-cluster-{}-{}", std::process::id(), nodes.a.port);
    let client = Client::connect(profile.clone()).await.unwrap();
    client
        .set_string(&key_on(true, "audit", 0), "v")
        .await
        .unwrap();
    assert!(client.set_string(&lost, "v").await.is_err());
    client
        .delete_keys(&[key_on(true, "audit", 1), key_on(false, "audit", 1)])
        .await
        .unwrap();

    let path =
        std::env::var_os("REDISCOPE_AUDIT_FILE").expect("isolate_config sets the audit file");
    let log = std::fs::read_to_string(path).unwrap();
    assert!(!log.contains("audit-lost"), "keys must never be logged");
    let events: Vec<serde_json::Value> = log
        .lines()
        .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap())
        .filter(|e| e["profile"] == profile.name)
        .collect();
    let outcomes = |action: &str| -> Vec<(u64, String)> {
        events
            .iter()
            .filter(|e| e["action"] == action)
            .map(|e| {
                (
                    e["operation_id"].as_u64().unwrap(),
                    e["outcome"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    };
    let sets = outcomes("SET");
    assert_eq!(sets.len(), 4, "{sets:?}");
    assert_eq!(sets[0].0, sets[1].0);
    assert_eq!(
        (sets[0].1.as_str(), sets[1].1.as_str()),
        ("started", "success")
    );
    assert_eq!(sets[2].0, sets[3].0);
    assert_eq!(
        (sets[2].1.as_str(), sets[3].1.as_str()),
        ("started", "unknown")
    );
    let pipelines = outcomes("PIPELINE");
    assert_eq!(
        pipelines
            .iter()
            .map(|(_, o)| o.as_str())
            .collect::<Vec<_>>(),
        vec!["started", "success"]
    );
    assert!(
        events
            .iter()
            .any(|e| e["action"] == "PIPELINE" && e["target_key_count"] == 2)
    );
}

/// A Sentinel whose primary answers from `active`, and two primaries.
struct Failover {
    sentinel: Peer,
    first: Peer,
    second: Peer,
    first_log: Log,
    second_log: Log,
}
impl Failover {
    /// `first` accepts `healthy` write commands, then turns into a replica:
    /// it points the Sentinel at `second` and answers `READONLY`.
    fn start(healthy: usize) -> Self {
        let active = Arc::new(AtomicUsize::new(0));
        let second_log: Log = Arc::default();
        let seen = second_log.clone();
        let second = Peer::start(move |id, args| {
            seen.lock().unwrap().push((id, args.to_vec()));
            Some(match args[0].as_str() {
                "ROLE" => "*1\r\n$6\r\nmaster\r\n".into(),
                "UNLINK" => ":1\r\n".into(),
                _ => "+OK\r\n".into(),
            })
        });
        let first_log: Log = Arc::default();
        let seen = first_log.clone();
        let switch = active.clone();
        let next = second.port as usize;
        let writes = AtomicUsize::new(0);
        let first = Peer::start(move |id, args| {
            seen.lock().unwrap().push((id, args.to_vec()));
            Some(match args[0].as_str() {
                "ROLE" if switch.load(Ordering::SeqCst) == next => "*1\r\n$5\r\nslave\r\n".into(),
                "ROLE" => "*1\r\n$6\r\nmaster\r\n".into(),
                "SET" | "UNLINK" => {
                    if writes.fetch_add(1, Ordering::SeqCst) < healthy {
                        if args[0] == "UNLINK" {
                            ":1\r\n"
                        } else {
                            "+OK\r\n"
                        }
                        .into()
                    } else {
                        switch.store(next, Ordering::SeqCst);
                        "-READONLY You can't write against a read only replica.\r\n".into()
                    }
                }
                _ => "+OK\r\n".into(),
            })
        });
        active.store(first.port as usize, Ordering::SeqCst);
        let master = active.clone();
        let sentinel = Peer::start(move |_, args| match args[0].as_str() {
            "SENTINEL" => Some(format!(
                "*2\r\n{}{}",
                bulk("127.0.0.1"),
                bulk(&master.load(Ordering::SeqCst).to_string())
            )),
            _ => Some("+OK\r\n".into()),
        });
        Self {
            sentinel,
            first,
            second,
            first_log,
            second_log,
        }
    }
    async fn client(&self) -> Client {
        let mut profile = self.sentinel.profile(Deployment::Sentinel);
        profile.sentinel_master = "service".into();
        Client::connect(profile).await.unwrap()
    }
}

#[tokio::test]
async fn sentinel_write_follows_failover_after_readonly() {
    let failover = Failover::start(1);
    let client = failover.client().await;
    client.set_string("before", "v").await.unwrap();
    assert_eq!(count(&failover.first_log, "SET", Some("before")), 1);
    client.set_string("during", "v").await.unwrap();
    assert_eq!(count(&failover.first_log, "SET", Some("during")), 1);
    assert_eq!(count(&failover.second_log, "SET", Some("during")), 1);
    assert_eq!(
        client.refresh_topology().await.unwrap()[0].port,
        failover.second.port
    );
    client.set_string("after", "v").await.unwrap();
    assert_eq!(count(&failover.first_log, "SET", Some("after")), 0);
    assert_eq!(count(&failover.second_log, "SET", Some("after")), 1);
    let _ = &failover.first;
}

#[tokio::test]
async fn sentinel_pipeline_refused_by_a_demoted_primary_rediscovers_for_the_retry() {
    let failover = Failover::start(0);
    let client = failover.client().await;
    let names = vec!["one".to_string(), "two".to_string()];
    let e = client.delete_keys(&names).await.unwrap_err().to_string();
    assert!(e.contains("try again"), "{e}");
    assert_eq!(count(&failover.second_log, "UNLINK", None), 0);
    assert_eq!(client.delete_keys(&names).await.unwrap(), 2);
    assert_eq!(count(&failover.second_log, "UNLINK", None), 2);
    assert!(count(&failover.first_log, "UNLINK", None) <= 2);
}

#[tokio::test]
async fn standalone_write_refused_as_readonly_is_returned_without_retrying() {
    let writes = Arc::new(AtomicUsize::new(0));
    let seen = writes.clone();
    let server = Peer::start(move |_, args| match args[0].as_str() {
        "SET" => {
            seen.fetch_add(1, Ordering::SeqCst);
            Some("-READONLY You can't write against a read only replica.\r\n".into())
        }
        _ => Some("+OK\r\n".into()),
    });
    let client = Client::connect(server.profile(Deployment::Standalone))
        .await
        .unwrap();
    let e = client.set_string("key", "v").await.unwrap_err().to_string();
    assert!(e.contains("read only") || e.contains("READONLY"), "{e}");
    // One attempt is the proof there was no retry. A wall-clock bound on top
    // failed under a loaded parallel run.
    assert_eq!(writes.load(Ordering::SeqCst), 1);
}

/// Wait, bounded, until nothing accepts connections on `port`.
fn wait_until_closed(port: u16) {
    for _ in 0..500 {
        if TcpStream::connect(("127.0.0.1", port)).is_err() {
            return;
        }
        thread::sleep(Duration::from_millis(2));
    }
    panic!("port {port} still accepts connections");
}

#[tokio::test]
async fn cluster_cross_slot_pfcount_is_refused_before_sending() {
    // PFCOUNT takes any number of keys, like MGET.
    let nodes = TwoNodes::start(|_, _, args| (args[0] == "PFCOUNT").then(|| Some(":0\r\n".into())));
    let client = nodes.client().await;
    let (ka, kb) = (key_on(true, "hll", 0), key_on(false, "hll", 0));
    let result = client.execute_raw(&format!("PFCOUNT {ka} {kb}")).await;
    let sent = count(&nodes.a_log, "PFCOUNT", None) + count(&nodes.b_log, "PFCOUNT", None);
    assert!(
        matches!(&result, Err(e) if e.to_string().contains("different cluster slots")) && sent == 0,
        "cross-slot PFCOUNT was routed by its first key and sent {sent} time(s): {result:?}"
    );
}

#[tokio::test]
async fn sentinel_write_after_the_primary_dies_reaches_the_new_primary() {
    let second_log: Log = Arc::default();
    let seen = second_log.clone();
    let second = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        Some(match args[0].as_str() {
            "ROLE" => "*1\r\n$6\r\nmaster\r\n".into(),
            _ => "+OK\r\n".into(),
        })
    });
    let active = Arc::new(AtomicUsize::new(0));
    let switch = active.clone();
    let next = second.port as usize;
    let first = Peer::start(move |_, args| match args[0].as_str() {
        "ROLE" => Some("*1\r\n$6\r\nmaster\r\n".into()),
        // The primary dies while the write is in flight; Sentinel promotes.
        "SET" => {
            switch.store(next, Ordering::SeqCst);
            None
        }
        _ => Some("+OK\r\n".into()),
    });
    let first_port = first.port;
    active.store(first_port as usize, Ordering::SeqCst);
    let master = active.clone();
    let sentinel = Peer::start(move |_, args| match args[0].as_str() {
        "SENTINEL" => Some(format!(
            "*2\r\n{}{}",
            bulk("127.0.0.1"),
            bulk(&master.load(Ordering::SeqCst).to_string())
        )),
        _ => Some("+OK\r\n".into()),
    });
    let mut profile = sentinel.profile(Deployment::Sentinel);
    profile.sentinel_master = "service".into();
    let client = Client::connect(profile).await.unwrap();
    let e = client
        .set_string("lost", "v")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("outcome unknown"), "{e}");
    drop(first);
    wait_until_closed(first_port);
    // The old primary refuses connections, so this write was never sent
    // anywhere; it should find the promoted primary.
    let result = client.set_string("next", "v").await;
    assert!(
        result.is_ok() && count(&second_log, "SET", Some("next")) == 1,
        "write after failover did not reach the new primary: {result:?}"
    );
}

#[tokio::test]
async fn cluster_write_after_a_primary_dies_reaches_the_promoted_node() {
    let ka = key_on(true, "failover", 0);
    let failed = Arc::new(AtomicBool::new(false));
    let ports = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
    let b_log: Log = Arc::default();
    let (seen, view, down) = (b_log.clone(), ports.clone(), failed.clone());
    let layout = move || {
        let (a, b) = (
            view[0].load(Ordering::SeqCst) as u16,
            view[1].load(Ordering::SeqCst) as u16,
        );
        if down.load(Ordering::SeqCst) {
            slots(&[(0, 16383, b)])
        } else {
            slots(&[(0, 8191, a), (8192, 16383, b)])
        }
    };
    let b_layout = layout.clone();
    let b = Peer::start(move |id, args| match args[0].as_str() {
        "CLUSTER" => Some(b_layout()),
        _ => {
            seen.lock().unwrap().push((id, args.to_vec()));
            Some("+OK\r\n".into())
        }
    });
    let die = failed.clone();
    let a = Peer::start(move |_, args| match args[0].as_str() {
        "CLUSTER" => Some(layout()),
        "SET" => {
            die.store(true, Ordering::SeqCst);
            None
        }
        _ => Some("+OK\r\n".into()),
    });
    ports[0].store(a.port as usize, Ordering::SeqCst);
    ports[1].store(b.port as usize, Ordering::SeqCst);
    let mut profile = a.profile(Deployment::Cluster);
    profile.seeds = vec![format!("127.0.0.1:{}", b.port)];
    let client = Client::connect(profile).await.unwrap();
    let e = client
        .set_string(&ka, "lost")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("outcome unknown"), "{e}");
    let a_port = a.port;
    drop(a);
    wait_until_closed(a_port);
    let result = client.set_string(&ka, "next").await;
    assert!(
        result.is_ok() && count(&b_log, "SET", Some(&ka)) == 1,
        "write to a slot whose primary died did not reach the promoted node: {result:?}"
    );
}

// ---- round 2: scripts, forged redirects, unsent writes, diagnostics -----

/// A port nothing listens on.
fn closed_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

#[tokio::test]
async fn cluster_script_error_looking_like_a_refusal_is_never_resent() {
    for (reply, head, line) in [
        ("-TRYAGAIN fake", "EVAL", "EVAL x 1 {key}"),
        ("-READONLY fake", "EVAL", "EVAL x 1 {key}"),
        ("-CLUSTERDOWN fake", "EVALSHA", "EVALSHA abc 1 {key}"),
        ("-MASTERDOWN fake", "FCALL", "FCALL f 1 {key}"),
        ("-LOADING fake", "EVAL", "EVAL x 1 {key}"),
    ] {
        let nodes = TwoNodes::start(move |node, _, args| {
            (node == 'b' && args[0] == head).then(|| Some(format!("{reply}\r\n")))
        });
        let client = nodes.client().await;
        let kb = key_on(false, "script", 0);
        let e = client
            .execute_raw(&line.replace("{key}", &kb))
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("fake"), "{reply}: {e}");
        assert_eq!(count(&nodes.b_log, head, None), 1, "{reply}");
        assert_eq!(count(&nodes.a_log, head, None), 0, "{reply}");
    }
    // The script API too.
    let nodes = TwoNodes::start(|node, _, args| {
        (node == 'b' && args[0] == "EVAL").then(|| Some("-TRYAGAIN fake\r\n".into()))
    });
    let client = nodes.client().await;
    let kb = key_on(false, "script", 1);
    assert!(client.eval("return 1", &[kb], &[]).await.is_err());
    assert_eq!(count(&nodes.b_log, "EVAL", None), 1);
}

#[tokio::test]
async fn cluster_script_error_looking_like_a_redirect_is_not_followed() {
    for kind in ["MOVED", "ASK"] {
        let kb = key_on(false, "redir", 0);
        let target = Arc::new(AtomicUsize::new(0));
        let port = target.clone();
        let key = kb.clone();
        let nodes = TwoNodes::start(move |node, _, args| {
            (node == 'b' && args[0] == "EVAL").then(|| {
                Some(format!(
                    "-{kind} {} 127.0.0.1:{}\r\n",
                    key_slot(key.as_bytes()),
                    port.load(Ordering::SeqCst)
                ))
            })
        });
        target.store(nodes.a.port as usize, Ordering::SeqCst);
        let client = nodes.client().await;
        let result = client
            .eval("return 1", std::slice::from_ref(&kb), &[])
            .await;
        assert!(result.is_err(), "{kind}: {result:?}");
        assert_eq!(count(&nodes.b_log, "EVAL", None), 1, "{kind}");
        assert_eq!(
            count(&nodes.a_log, "EVAL", None),
            0,
            "{kind}: redirect followed"
        );
        assert_eq!(count(&nodes.a_log, "ASKING", None), 0, "{kind}");
    }
}

#[tokio::test]
async fn cluster_read_only_script_refused_unrun_is_retried() {
    let refusals = Arc::new(AtomicUsize::new(0));
    let seen = refusals.clone();
    let nodes = TwoNodes::start(move |node, _, args| {
        (node == 'b' && args[0] == "EVAL_RO" && seen.fetch_add(1, Ordering::SeqCst) == 0)
            .then(|| Some("-TRYAGAIN rehashing\r\n".into()))
    });
    let client = nodes.client().await;
    let kb = key_on(false, "ro", 0);
    client
        .execute_raw(&format!("EVAL_RO x 1 {kb}"))
        .await
        .unwrap();
    assert_eq!(count(&nodes.b_log, "EVAL_RO", None), 2);
}

#[tokio::test]
async fn cluster_write_redirect_for_another_slot_is_returned_not_followed() {
    for kind in ["MOVED", "ASK"] {
        let ka = key_on(true, "forged", 0);
        let target = Arc::new(AtomicUsize::new(0));
        let port = target.clone();
        let key = ka.clone();
        let nodes = TwoNodes::start(move |node, _, args| {
            (node == 'a' && args[0] == "SET").then(|| {
                let other = (key_slot(key.as_bytes()) + 1) % 16384;
                Some(format!(
                    "-{kind} {other} 127.0.0.1:{}\r\n",
                    port.load(Ordering::SeqCst)
                ))
            })
        });
        target.store(nodes.b.port as usize, Ordering::SeqCst);
        let client = nodes.client().await;
        let e = client.set_string(&ka, "v").await.unwrap_err().to_string();
        assert!(e.to_ascii_uppercase().contains(kind), "{kind}: {e}");
        assert_eq!(count(&nodes.a_log, "SET", Some(&ka)), 1, "{kind}");
        assert!(
            heads(&nodes.b_log)
                .iter()
                .all(|h| h != "SET" && h != "ASKING"),
            "{kind}: {:?}",
            heads(&nodes.b_log)
        );
    }
}

#[tokio::test]
async fn cluster_write_to_a_dead_owner_is_unsent_and_lands_after_rediscovery() {
    let dead = closed_port();
    let healed = Arc::new(AtomicBool::new(false));
    let b_log: Log = Arc::default();
    let seen = b_log.clone();
    let b = Peer::start(move |id, args| {
        if args[0] != "CLUSTER" {
            seen.lock().unwrap().push((id, args.to_vec()));
        }
        Some("+OK\r\n".into())
    });
    let a_log: Log = Arc::default();
    let (seen, own, live, fixed) = (
        a_log.clone(),
        Arc::new(AtomicUsize::new(0)),
        b.port,
        healed.clone(),
    );
    let own_port = own.clone();
    let a = Peer::start(move |id, args| {
        if args[0] == "CLUSTER" {
            let a = own_port.load(Ordering::SeqCst) as u16;
            let high = if fixed.load(Ordering::SeqCst) {
                live
            } else {
                dead
            };
            return Some(slots(&[(0, 8191, a), (8192, 16383, high)]));
        }
        seen.lock().unwrap().push((id, args.to_vec()));
        Some("+OK\r\n".into())
    });
    own.store(a.port as usize, Ordering::SeqCst);
    let client = Client::connect(a.profile(Deployment::Cluster))
        .await
        .unwrap();
    healed.store(true, Ordering::SeqCst);
    let kb = key_on(false, "unsent", 0);
    client.set_string(&kb, "v").await.unwrap();
    assert_eq!(count(&b_log, "SET", Some(&kb)), 1);
    assert_eq!(count(&a_log, "SET", None), 0);
}

#[tokio::test]
async fn standalone_write_to_a_server_that_stopped_listening_fails_cleanly() {
    let writes = Arc::new(AtomicUsize::new(0));
    let seen = writes.clone();
    let server = Peer::start(move |_, args| match args[0].as_str() {
        "SET" => {
            seen.fetch_add(1, Ordering::SeqCst);
            Some("+OK\r\n".into())
        }
        // Closes the cached socket, so the next command must reconnect.
        "GET" => None,
        _ => Some("+OK\r\n".into()),
    });
    let port = server.port;
    let client = Client::connect(server.profile(Deployment::Standalone))
        .await
        .unwrap();
    drop(server);
    wait_until_closed(port);
    assert!(client.read_value("key", KeyType::String).await.is_err());
    let started = std::time::Instant::now();
    let e = client.set_string("key", "v").await.unwrap_err().to_string();
    assert!(!e.contains("outcome unknown"), "never sent: {e}");
    assert_eq!(writes.load(Ordering::SeqCst), 0);
    assert!(started.elapsed() < Duration::from_secs(10));
    // Standalone connect to nothing is an error too, not a panic.
    let mut profile = Connection {
        name: "nothing".into(),
        host: "127.0.0.1".into(),
        port: closed_port(),
        ..Default::default()
    };
    assert!(Client::connect(profile.clone()).await.is_err());
    profile.deployment = Deployment::Cluster;
    assert!(Client::connect(profile).await.is_err());
}

#[tokio::test]
async fn sentinel_sent_write_lost_rediscovers_immediately_for_the_next_write() {
    let second_log: Log = Arc::default();
    let seen = second_log.clone();
    let second = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        Some(match args[0].as_str() {
            "ROLE" => "*1\r\n$6\r\nmaster\r\n".into(),
            _ => "+OK\r\n".into(),
        })
    });
    let active = Arc::new(AtomicUsize::new(0));
    let switch = active.clone();
    let next = second.port as usize;
    let first_log: Log = Arc::default();
    let seen = first_log.clone();
    let dropped = AtomicBool::new(false);
    // The old primary stays up and would still accept the next write.
    let first = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        match args[0].as_str() {
            "ROLE" => Some("*1\r\n$6\r\nmaster\r\n".into()),
            "SET" if !dropped.swap(true, Ordering::SeqCst) => {
                switch.store(next, Ordering::SeqCst);
                None
            }
            _ => Some("+OK\r\n".into()),
        }
    });
    active.store(first.port as usize, Ordering::SeqCst);
    let master = active.clone();
    let sentinel = Peer::start(move |_, args| match args[0].as_str() {
        "SENTINEL" => Some(format!(
            "*2\r\n{}{}",
            bulk("127.0.0.1"),
            bulk(&master.load(Ordering::SeqCst).to_string())
        )),
        _ => Some("+OK\r\n".into()),
    });
    let mut profile = sentinel.profile(Deployment::Sentinel);
    profile.sentinel_master = "service".into();
    let client = Client::connect(profile).await.unwrap();
    let e = client
        .set_string("lost", "v")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("outcome unknown"), "{e}");
    assert_eq!(count(&first_log, "SET", Some("lost")), 1);
    assert_eq!(count(&second_log, "SET", Some("lost")), 0, "resent");
    client.set_string("next", "v").await.unwrap();
    assert_eq!(count(&second_log, "SET", Some("next")), 1);
    assert_eq!(
        count(&first_log, "SET", Some("next")),
        0,
        "next write went to the old primary: no rediscovery after the lost write"
    );
}

#[tokio::test]
async fn cluster_sent_write_lost_rediscovers_immediately_for_the_next_write() {
    let ka = key_on(true, "gone", 0);
    let failed = Arc::new(AtomicBool::new(false));
    let ports = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
    let (a_log, b_log): (Log, Log) = (Arc::default(), Arc::default());
    let (view, down) = (ports.clone(), failed.clone());
    let layout = move || {
        let (a, b) = (
            view[0].load(Ordering::SeqCst) as u16,
            view[1].load(Ordering::SeqCst) as u16,
        );
        if down.load(Ordering::SeqCst) {
            slots(&[(0, 16383, b)])
        } else {
            slots(&[(0, 8191, a), (8192, 16383, b)])
        }
    };
    let (b_layout, seen) = (layout.clone(), b_log.clone());
    let b = Peer::start(move |id, args| match args[0].as_str() {
        "CLUSTER" => Some(b_layout()),
        _ => {
            seen.lock().unwrap().push((id, args.to_vec()));
            Some("+OK\r\n".into())
        }
    });
    let (die, seen) = (failed.clone(), a_log.clone());
    // `a` stays up and would still accept the next write.
    let a = Peer::start(move |id, args| match args[0].as_str() {
        "CLUSTER" => Some(layout()),
        _ => {
            seen.lock().unwrap().push((id, args.to_vec()));
            if args[0] == "SET" && !die.swap(true, Ordering::SeqCst) {
                None
            } else {
                Some("+OK\r\n".into())
            }
        }
    });
    ports[0].store(a.port as usize, Ordering::SeqCst);
    ports[1].store(b.port as usize, Ordering::SeqCst);
    let client = Client::connect(a.profile(Deployment::Cluster))
        .await
        .unwrap();
    let e = client
        .set_string(&ka, "lost")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("outcome unknown"), "{e}");
    assert_eq!(count(&a_log, "SET", Some(&ka)), 1);
    assert_eq!(count(&b_log, "SET", Some(&ka)), 0, "resent");
    client.set_string(&ka, "next").await.unwrap();
    assert_eq!(count(&b_log, "SET", Some(&ka)), 1);
    assert_eq!(
        count(&a_log, "SET", Some(&ka)),
        1,
        "next write went to the old owner: no rediscovery after the lost write"
    );
}

#[tokio::test]
async fn cluster_keyless_writing_scripts_refused_read_only_scripts_allowed() {
    let nodes =
        TwoNodes::start(|_, _, args| args[0].ends_with("_RO").then(|| Some(":1\r\n".into())));
    let client = nodes.client().await;
    for (line, head) in [
        ("EVAL x 0", "EVAL"),
        ("EVALSHA abc 0", "EVALSHA"),
        ("FCALL f 0", "FCALL"),
        ("eval x 0 argv", "EVAL"),
    ] {
        let e = client.execute_raw(line).await.unwrap_err().to_string();
        assert!(e.contains("only one primary"), "{line}: {e}");
        assert_eq!(count(&nodes.a_log, head, None), 0, "{line}");
        assert_eq!(count(&nodes.b_log, head, None), 0, "{line}");
    }
    for (line, head) in [
        ("EVAL_RO x 0", "EVAL_RO"),
        ("EVALSHA_RO abc 0", "EVALSHA_RO"),
        ("FCALL_RO f 0", "FCALL_RO"),
    ] {
        assert_eq!(client.execute_raw(line).await.unwrap(), "(integer) 1");
        assert_eq!(count(&nodes.a_log, head, None), 1, "{line}");
        assert_eq!(count(&nodes.b_log, head, None), 0, "{line}");
    }
}

#[tokio::test]
async fn cluster_spublish_goes_to_the_channel_slot_owner() {
    let nodes =
        TwoNodes::start(|_, _, args| (args[0] == "SPUBLISH").then(|| Some(":2\r\n".into())));
    let client = nodes.client().await;
    let (ca, cb) = (key_on(true, "chan", 0), key_on(false, "chan", 0));
    assert_eq!(
        client
            .execute_raw(&format!("SPUBLISH {cb} hello"))
            .await
            .unwrap(),
        "(integer) 2"
    );
    assert_eq!(count(&nodes.b_log, "SPUBLISH", Some(&cb)), 1);
    assert_eq!(count(&nodes.a_log, "SPUBLISH", None), 0);
    client
        .execute_raw(&format!("SPUBLISH {ca} hello"))
        .await
        .unwrap();
    assert_eq!(count(&nodes.a_log, "SPUBLISH", Some(&ca)), 1);
    assert_eq!(count(&nodes.b_log, "SPUBLISH", None), 1);
}

const DIAG_HEADS: [&str; 5] = ["SLOWLOG", "CLIENT", "CONFIG", "LATENCY", "MODULE"];

#[tokio::test]
async fn cluster_diagnostics_read_from_the_default_node_only() {
    let nodes = TwoNodes::start(|_, _, args| match args[0].as_str() {
        "CLIENT" => Some(Some(bulk(
            "id=7 addr=127.0.0.1:5000 name= age=1 idle=2 db=0 cmd=client|list\n",
        ))),
        "CONFIG" => Some(Some(format!("*2\r\n{}{}", bulk("maxmemory"), bulk("0")))),
        "SLOWLOG" | "MODULE" | "LATENCY" => Some(Some("*0\r\n".into())),
        _ => None,
    });
    let client = nodes.client().await;
    let d = client.diagnostics().await.unwrap();
    let a = ("127.0.0.1".to_string(), nodes.a.port);
    assert_eq!(d.node, Some(a.clone()));
    assert!(
        d.cluster
            .iter()
            .any(|(k, v)| k == "diagnostics_node" && *v == format!("127.0.0.1:{}", nodes.a.port)),
        "{:?}",
        d.cluster
    );
    assert_eq!(d.clients.len(), 1, "{:?}", d.clients);
    assert_eq!(d.clients[0].id, "7");
    assert!(d.config.iter().any(|(k, _)| k == "maxmemory"));
    for head in ["SLOWLOG", "CLIENT", "CONFIG"] {
        assert_eq!(count(&nodes.a_log, head, None), 1, "{head}");
    }
    for head in DIAG_HEADS {
        assert_eq!(count(&nodes.b_log, head, None), 0, "{head} reached b");
    }
}

#[tokio::test]
async fn cluster_diagnostics_changes_go_to_the_named_node_guarded_and_audited() {
    let nodes = TwoNodes::start(|_, _, _| None);
    let b = Some(("127.0.0.1".to_string(), nodes.b.port));

    let mut hard = nodes.profile();
    hard.read_only = true;
    let client = Client::connect(hard).await.unwrap();
    assert!(client.client_kill_on(&b, "5").await.is_err());
    assert!(client.config_set_on(&b, "maxmemory", "1").await.is_err());
    assert!(client.slowlog_reset_on(&b).await.is_err());

    let mut production = nodes.profile();
    production.name = format!("diag-production-{}-{}", std::process::id(), nodes.a.port);
    production.environment = rediscope::config::Environment::Production;
    let client = Client::connect(production.clone()).await.unwrap();
    let e = client
        .client_kill_on(&b, "5")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("Read-only"), "{e}");
    assert!(client.config_set_on(&b, "maxmemory", "1").await.is_err());
    assert!(client.slowlog_reset_on(&b).await.is_err());
    for log in [&nodes.a_log, &nodes.b_log] {
        for head in DIAG_HEADS {
            assert_eq!(count(log, head, None), 0, "{head} sent while locked");
        }
    }

    client.unlock_writes(&production.name).unwrap();
    client.client_kill_on(&b, "5").await.unwrap();
    client.config_set_on(&b, "maxmemory", "1").await.unwrap();
    client.slowlog_reset_on(&b).await.unwrap();
    let sent: Vec<Vec<String>> = nodes
        .b_log
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, a)| DIAG_HEADS.contains(&a[0].as_str()))
        .map(|(_, a)| a.clone())
        .collect();
    assert_eq!(
        sent,
        vec![
            vec!["CLIENT".to_string(), "KILL".into(), "ID".into(), "5".into()],
            vec![
                "CONFIG".into(),
                "SET".into(),
                "maxmemory".into(),
                "1".into()
            ],
            vec!["SLOWLOG".into(), "RESET".into()],
        ]
    );
    for head in DIAG_HEADS {
        assert_eq!(count(&nodes.a_log, head, None), 0, "{head} reached a");
    }
    // The plain forms still go to the default node.
    client.client_kill("6").await.unwrap();
    assert_eq!(count(&nodes.a_log, "CLIENT", None), 1);

    let path = std::env::var_os("REDISCOPE_AUDIT_FILE").unwrap();
    let events: Vec<serde_json::Value> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap())
        .filter(|e| e["profile"] == production.name)
        .collect();
    for action in ["CLIENT", "CONFIG", "SLOWLOG"] {
        let outcomes: Vec<&str> = events
            .iter()
            .filter(|e| e["action"] == action)
            .map(|e| e["outcome"].as_str().unwrap())
            .collect();
        assert!(
            outcomes.contains(&"denied")
                && outcomes.windows(2).any(|w| w == ["started", "success"]),
            "{action}: {outcomes:?}"
        );
    }
}

#[tokio::test]
async fn standalone_diagnostics_have_no_node_and_plain_changes_work() {
    let log: Log = Arc::default();
    let seen = log.clone();
    let server = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        Some(match args[0].as_str() {
            "CLIENT" if args[1] == "LIST" => {
                bulk("id=3 addr=127.0.0.1:1 age=1 idle=1 db=0 cmd=x\n")
            }
            "SLOWLOG" | "MODULE" | "LATENCY" => "*0\r\n".into(),
            "CONFIG" if args[1] == "GET" => "*0\r\n".into(),
            "CLUSTER" => "-ERR This instance has cluster support disabled\r\n".into(),
            _ => "+OK\r\n".into(),
        })
    });
    let client = Client::connect(server.profile(Deployment::Standalone))
        .await
        .unwrap();
    let d = client.diagnostics().await.unwrap();
    assert_eq!(d.node, None);
    assert!(!d.cluster.iter().any(|(k, _)| k == "diagnostics_node"));
    assert_eq!(d.clients.len(), 1);
    client.client_kill("3").await.unwrap();
    client.client_kill_on(&None, "4").await.unwrap();
    client.config_set("maxmemory", "1").await.unwrap();
    client.slowlog_reset().await.unwrap();
    let heads = heads(&log);
    assert_eq!(heads.iter().filter(|h| *h == "CLIENT").count(), 3);
    assert_eq!(heads.iter().filter(|h| *h == "SLOWLOG").count(), 2);
    assert_eq!(heads.iter().filter(|h| *h == "CONFIG").count(), 2);
}

#[tokio::test]
async fn cluster_pipeline_first_node_loss_names_no_other_node() {
    let nodes =
        TwoNodes::start(|node, _, args| (node == 'a' && args[0] == "UNLINK").then_some(None));
    let client = nodes.client().await;
    let e = client
        .delete_keys(&[key_on(true, "p1", 0), key_on(false, "p1", 0)])
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("outcome unknown"), "{e}");
    assert!(!e.contains("other node"), "{e}");
    assert!(e.contains(&format!("127.0.0.1:{}", nodes.a.port)), "{e}");
    assert_eq!(count(&nodes.b_log, "UNLINK", None), 0);
}

#[tokio::test]
async fn cluster_pipeline_second_node_loss_says_earlier_commands_may_have_applied() {
    let nodes =
        TwoNodes::start(|node, _, args| (node == 'b' && args[0] == "UNLINK").then_some(None));
    let client = nodes.client().await;
    let e = client
        .delete_keys(&[key_on(true, "p2", 0), key_on(false, "p2", 0)])
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("outcome unknown"), "{e}");
    assert!(e.contains("1 other node(s)"), "{e}");
    assert!(e.contains("may have been applied"), "{e}");
    assert!(e.contains(&format!("127.0.0.1:{}", nodes.b.port)), "{e}");
}

#[tokio::test]
async fn cluster_pipeline_unreachable_second_node_says_its_commands_were_not_sent() {
    let TwoNodes {
        a, b, a_log, b_log, ..
    } = TwoNodes::start(|_, _, _| None);
    let client = Client::connect(a.profile(Deployment::Cluster))
        .await
        .unwrap();
    let b_port = b.port;
    drop(b);
    wait_until_closed(b_port);
    let (ka, kb) = (key_on(true, "p3", 0), key_on(false, "p3", 0));
    let e = client
        .delete_keys(&[ka.clone(), kb.clone()])
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("its commands were not sent"), "{e}");
    assert!(e.contains("1 other node(s)"), "{e}");
    assert!(e.contains(&format!("127.0.0.1:{b_port}")), "{e}");
    assert_eq!(count(&a_log, "UNLINK", Some(&ka)), 1);
    assert_eq!(count(&b_log, "UNLINK", None), 0);
    // Unreachable first node: nothing is sent anywhere.
    let e = client
        .delete_keys(&[key_on(false, "p3", 1), key_on(true, "p3", 1)])
        .await
        .unwrap_err()
        .to_string();
    assert!(!e.contains("other node"), "{e}");
    assert_eq!(count(&a_log, "UNLINK", None), 1);
}

#[tokio::test]
async fn sentinel_pipeline_demoted_mid_batch_may_have_applied_and_says_try_again() {
    let failover = Failover::start(1);
    let client = failover.client().await;
    let e = client
        .delete_keys(&["one".into(), "two".into()])
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("may have been applied"), "{e}");
    assert!(e.contains("try again"), "{e}");
    assert_eq!(count(&failover.first_log, "UNLINK", None), 2);
    assert_eq!(count(&failover.second_log, "UNLINK", None), 0);
    assert_eq!(
        client.refresh_topology().await.unwrap()[0].port,
        failover.second.port
    );
}

// ---- round 3: opaque commands, partial batches, lease mid-batch ----------

/// `COMMAND GETKEYS` for `MYMOD.*` commands: their first argument is the key.
fn getkeys(args: &[String]) -> Option<Option<String>> {
    (args[0] == "COMMAND" && args.get(2).is_some_and(|c| c.starts_with("MYMOD.")))
        .then(|| Some(format!("*1\r\n{}", bulk(&args[3]))))
}

/// Outcomes the audit log recorded for `action` under `profile`, in order.
fn audit_outcomes(profile: &str, action: &str) -> Vec<String> {
    let path =
        std::env::var_os("REDISCOPE_AUDIT_FILE").expect("isolate_config sets the audit file");
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap())
        .filter(|e| e["profile"] == profile && e["action"] == action)
        .map(|e| e["outcome"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn cluster_unknown_command_refused_after_sending_is_never_resent() {
    for reply in ["-TRYAGAIN x", "-READONLY x", "-CLUSTERDOWN x", "-LOADING x"] {
        let nodes = TwoNodes::start(move |node, _, args| {
            getkeys(args).or_else(|| {
                (node == 'b' && args[0] == "MYMOD.WRITE").then(|| Some(format!("{reply}\r\n")))
            })
        });
        let client = nodes.client().await;
        let kb = key_on(false, "opaque", 0);
        let e = client
            .execute_raw(&format!("MYMOD.WRITE {kb} v"))
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains('x'), "{reply}: {e}");
        assert_eq!(count(&nodes.b_log, "MYMOD.WRITE", Some(&kb)), 1, "{reply}");
        assert_eq!(count(&nodes.a_log, "MYMOD.WRITE", None), 0, "{reply}");
    }
}

#[tokio::test]
async fn cluster_unknown_command_to_an_unreachable_owner_is_unsent_and_retried() {
    let dead = closed_port();
    let healed = Arc::new(AtomicBool::new(false));
    let b_log: Log = Arc::default();
    let seen = b_log.clone();
    let b = Peer::start(move |id, args| {
        if args[0] != "CLUSTER" {
            seen.lock().unwrap().push((id, args.to_vec()));
        }
        Some("+OK\r\n".into())
    });
    let a_log: Log = Arc::default();
    let (seen, own, live, fixed) = (
        a_log.clone(),
        Arc::new(AtomicUsize::new(0)),
        b.port,
        healed.clone(),
    );
    let own_port = own.clone();
    let a = Peer::start(move |id, args| {
        if args[0] == "CLUSTER" {
            let a = own_port.load(Ordering::SeqCst) as u16;
            let high = if fixed.load(Ordering::SeqCst) {
                live
            } else {
                dead
            };
            return Some(slots(&[(0, 8191, a), (8192, 16383, high)]));
        }
        seen.lock().unwrap().push((id, args.to_vec()));
        if let Some(reply) = getkeys(args) {
            return reply;
        }
        Some("+OK\r\n".into())
    });
    own.store(a.port as usize, Ordering::SeqCst);
    let client = Client::connect(a.profile(Deployment::Cluster))
        .await
        .unwrap();
    healed.store(true, Ordering::SeqCst);
    let kb = key_on(false, "unsent-mod", 0);
    client
        .execute_raw(&format!("MYMOD.WRITE {kb} v"))
        .await
        .unwrap();
    assert_eq!(count(&b_log, "MYMOD.WRITE", Some(&kb)), 1);
    assert_eq!(count(&a_log, "MYMOD.WRITE", None), 0);
}

#[tokio::test]
async fn cluster_opaque_command_genuine_redirect_refreshes_but_is_not_followed() {
    for kind in ["MOVED", "ASK"] {
        for (head, line) in [
            ("EVAL", "EVAL x 1 {key}"),
            ("MYMOD.WRITE", "MYMOD.WRITE {key} v"),
        ] {
            let kb = key_on(false, "genuine", 0);
            let merged = Arc::new(AtomicBool::new(false));
            let target = Arc::new(AtomicUsize::new(0));
            let (port, key, flip) = (target.clone(), kb.clone(), merged.clone());
            let nodes = TwoNodes::start_merging(merged, move |node, _, args| {
                getkeys(args).or_else(|| {
                    (node == 'b' && args[0] == head).then(|| {
                        // The slot really moved to `a`.
                        flip.store(true, Ordering::SeqCst);
                        Some(format!(
                            "-{kind} {} 127.0.0.1:{}\r\n",
                            key_slot(key.as_bytes()),
                            port.load(Ordering::SeqCst)
                        ))
                    })
                })
            });
            target.store(nodes.a.port as usize, Ordering::SeqCst);
            let client = nodes.client().await;
            let before = nodes.discoveries[0].load(Ordering::SeqCst);
            let e = client
                .execute_raw(&line.replace("{key}", &kb))
                .await
                .unwrap_err()
                .to_string();
            let case = format!("{kind} {head}");
            assert!(e.contains("not sent again"), "{case}: {e}");
            assert!(e.contains("try again"), "{case}: {e}");
            assert_eq!(count(&nodes.b_log, head, None), 1, "{case}");
            assert_eq!(count(&nodes.a_log, head, None), 0, "{case}: followed");
            assert_eq!(count(&nodes.a_log, "ASKING", None), 0, "{case}");
            assert!(
                nodes.discoveries[0].load(Ordering::SeqCst) > before,
                "{case}: topology was not refreshed"
            );
            client.set_string(&kb, "v").await.unwrap();
            assert_eq!(count(&nodes.a_log, "SET", Some(&kb)), 1, "{case}");
            assert_eq!(count(&nodes.b_log, "SET", None), 0, "{case}");
        }
    }
}

#[tokio::test]
async fn cluster_pipeline_lease_locked_while_first_node_replies_stops_before_the_next() {
    let release = Arc::new(AtomicBool::new(false));
    let wait = release.clone();
    let nodes = TwoNodes::start(move |node, _, args| {
        if node == 'a' && args[0] == "UNLINK" {
            for _ in 0..5000 {
                if wait.load(Ordering::SeqCst) {
                    break;
                }
                thread::sleep(Duration::from_millis(2));
            }
        }
        None
    });
    let mut production = nodes.profile();
    production.name = format!("lease-mid-batch-{}-{}", std::process::id(), nodes.a.port);
    production.environment = rediscope::config::Environment::Production;
    let client = Client::connect(production.clone()).await.unwrap();
    client.unlock_writes(&production.name).unwrap();
    let (ka, kb) = (key_on(true, "lease", 0), key_on(false, "lease", 0));
    let batch = {
        let (client, names) = (client.clone(), vec![ka.clone(), kb.clone()]);
        tokio::spawn(async move { client.delete_keys(&names).await })
    };
    // Node a has the command and is holding its reply.
    for _ in 0..2500 {
        if count(&nodes.a_log, "UNLINK", None) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(count(&nodes.a_log, "UNLINK", Some(&ka)), 1);
    client.lock_writes().unwrap();
    release.store(true, Ordering::SeqCst);
    let e = tokio::time::timeout(Duration::from_secs(20), batch)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err()
        .to_string();
    assert!(e.contains("its commands were not sent"), "{e}");
    assert!(e.contains("1 other node(s)"), "{e}");
    assert!(e.contains(&format!("127.0.0.1:{}", nodes.b.port)), "{e}");
    assert_eq!(count(&nodes.b_log, "UNLINK", None), 0);
    assert_eq!(
        audit_outcomes(&production.name, "PIPELINE"),
        vec!["started", "unknown"]
    );

    // Locked before the call: nothing is sent, and the audit says denied.
    let e = client
        .delete_keys(&[key_on(true, "lease", 1), key_on(false, "lease", 1)])
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("Read-only"), "{e}");
    assert_eq!(count(&nodes.a_log, "UNLINK", None), 1);
    assert_eq!(count(&nodes.b_log, "UNLINK", None), 0);
    assert_eq!(
        audit_outcomes(&production.name, "PIPELINE"),
        vec!["started", "unknown", "started", "denied"]
    );
}

#[tokio::test]
async fn cluster_pipeline_partly_failed_reports_what_ran_and_audits_unknown() {
    let (a1, a2) = (key_on(true, "part", 0), key_on(true, "part", 1));
    let (b1, b2) = (key_on(false, "part", 0), key_on(false, "part", 1));
    let boom = b1.clone();
    let nodes = TwoNodes::start(move |node, _, args| {
        (node == 'b' && args[0] == "UNLINK" && args[1] == boom)
            .then(|| Some("-ERR boom\r\n".into()))
    });
    let mut profile = nodes.profile();
    profile.name = format!("partial-{}-{}", std::process::id(), nodes.a.port);
    let client = Client::connect(profile.clone()).await.unwrap();
    let e = client
        .delete_keys(&[a1.clone(), b1.clone(), a2.clone(), b2.clone()])
        .await
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("Pipeline partially applied: 3 of 4 commands ran"),
        "{e}"
    );
    assert!(e.contains("boom"), "{e}");
    assert_eq!(count(&nodes.a_log, "UNLINK", None), 2);
    assert_eq!(count(&nodes.b_log, "UNLINK", Some(&b1)), 1, "retried");
    assert_eq!(count(&nodes.b_log, "UNLINK", Some(&b2)), 1);
    assert_eq!(
        audit_outcomes(&profile.name, "PIPELINE"),
        vec!["started", "unknown"]
    );

    // Every command failed: a plain error.
    let nodes =
        TwoNodes::start(|_, _, args| (args[0] == "UNLINK").then(|| Some("-ERR boom\r\n".into())));
    let client = nodes.client().await;
    let e = client
        .delete_keys(&[key_on(true, "part", 2), key_on(false, "part", 2)])
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("boom"), "{e}");
    assert!(!e.contains("partially applied"), "{e}");
    assert_eq!(count(&nodes.a_log, "UNLINK", None), 1);
    assert_eq!(count(&nodes.b_log, "UNLINK", None), 1);
}

#[tokio::test]
async fn sentinel_pipeline_lost_connection_is_unknown_and_rediscovers_for_the_next() {
    let second_log: Log = Arc::default();
    let seen = second_log.clone();
    let second = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        Some(match args[0].as_str() {
            "ROLE" => "*1\r\n$6\r\nmaster\r\n".into(),
            "UNLINK" => ":1\r\n".into(),
            _ => "+OK\r\n".into(),
        })
    });
    let active = Arc::new(AtomicUsize::new(0));
    let (switch, next) = (active.clone(), second.port as usize);
    let first_log: Log = Arc::default();
    let seen = first_log.clone();
    let dropped = AtomicBool::new(false);
    // The old primary stays up and would accept the next batch.
    let first = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        match args[0].as_str() {
            "ROLE" => Some("*1\r\n$6\r\nmaster\r\n".into()),
            "UNLINK" if !dropped.swap(true, Ordering::SeqCst) => {
                switch.store(next, Ordering::SeqCst);
                None
            }
            "UNLINK" => Some(":1\r\n".into()),
            _ => Some("+OK\r\n".into()),
        }
    });
    active.store(first.port as usize, Ordering::SeqCst);
    let master = active.clone();
    let sentinel = Peer::start(move |_, args| match args[0].as_str() {
        "SENTINEL" => Some(format!(
            "*2\r\n{}{}",
            bulk("127.0.0.1"),
            bulk(&master.load(Ordering::SeqCst).to_string())
        )),
        _ => Some("+OK\r\n".into()),
    });
    let mut profile = sentinel.profile(Deployment::Sentinel);
    profile.sentinel_master = "service".into();
    let client = Client::connect(profile).await.unwrap();
    let names = vec!["one".to_string(), "two".to_string()];
    let e = client.delete_keys(&names).await.unwrap_err().to_string();
    assert!(e.contains("outcome unknown"), "{e}");
    assert_eq!(count(&first_log, "UNLINK", None), 1);
    assert_eq!(count(&second_log, "UNLINK", None), 0, "replayed");
    assert_eq!(client.delete_keys(&names).await.unwrap(), 2);
    assert_eq!(count(&second_log, "UNLINK", None), 2);
    assert_eq!(
        count(&first_log, "UNLINK", None),
        1,
        "next batch went to the old primary: no rediscovery after the lost batch"
    );
}

#[tokio::test]
async fn cluster_getkeys_lost_connection_sends_nothing_and_refreshes() {
    let nodes =
        TwoNodes::start(|node, _, args| (node == 'a' && args[0] == "COMMAND").then_some(None));
    let client = nodes.client().await;
    let before = nodes.discoveries[0].load(Ordering::SeqCst);
    let kb = key_on(false, "probe", 0);
    let e = client
        .execute_raw(&format!("MYMOD.WRITE {kb} v"))
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("nothing was sent"), "{e}");
    assert!(e.contains("try again"), "{e}");
    assert_eq!(count(&nodes.a_log, "COMMAND", None), 1);
    for log in [&nodes.a_log, &nodes.b_log] {
        assert_eq!(count(log, "MYMOD.WRITE", None), 0);
    }
    assert!(
        nodes.discoveries[0].load(Ordering::SeqCst) > before,
        "topology was not refreshed"
    );
}

#[tokio::test]
async fn cluster_node_command_server_error_keeps_the_socket() {
    let nodes = TwoNodes::start(|node, _, args| {
        (node == 'b' && args[0] == "CONFIG").then(|| Some("-NOPERM x\r\n".into()))
    });
    let client = nodes.client().await;
    let b = Some(("127.0.0.1".to_string(), nodes.b.port));
    for _ in 0..2 {
        let e = client
            .config_set_on(&b, "maxmemory", "1")
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("NOPERM") || e.contains('x'), "{e}");
    }
    let sockets: std::collections::HashSet<usize> = nodes
        .b_log
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, a)| a[0] == "CONFIG")
        .map(|(id, _)| *id)
        .collect();
    assert_eq!(count(&nodes.b_log, "CONFIG", None), 2);
    assert_eq!(sockets.len(), 1, "reconnected after a server error");
}

#[tokio::test]
async fn sentinel_diagnostics_name_the_primary_and_changes_follow_it_after_failover() {
    let failover = Failover::start(0);
    let client = failover.client().await;
    let d = client.diagnostics().await.unwrap();
    let first = Some(("127.0.0.1".to_string(), failover.first.port));
    assert_eq!(d.node, first);
    assert!(count(&failover.first_log, "SLOWLOG", None) >= 1);
    for head in DIAG_HEADS {
        assert_eq!(count(&failover.second_log, head, None), 0, "{head}");
    }
    // A write moves the client to the promoted primary; `first` stays up.
    client.set_string("move", "v").await.unwrap();
    assert_eq!(count(&failover.second_log, "SET", Some("move")), 1);
    client.client_kill_on(&d.node, "5").await.unwrap();
    assert_eq!(count(&failover.first_log, "CLIENT", Some("KILL")), 1);
    assert_eq!(count(&failover.second_log, "CLIENT", None), 0);
    let d = client.diagnostics().await.unwrap();
    assert_eq!(
        d.node,
        Some(("127.0.0.1".to_string(), failover.second.port))
    );
}

// ---- round 4: typed audit outcomes, opaque refusals, chunked batches -----

const SPOOF: &str =
    "-ERR Read-only profile or production write lease expired; command rejected\r\n";

fn unique(tag: &str, port: u16) -> String {
    format!("{tag}-{}-{port}", std::process::id())
}

#[tokio::test]
async fn cluster_audit_outcomes_come_from_the_error_type_not_its_text() {
    let nodes = TwoNodes::start(|node, _, args| {
        if let Some(reply) = getkeys(args) {
            return Some(reply);
        }
        if node != 'b' {
            return None;
        }
        let reply = match (args[0].as_str(), args.get(1).map(String::as_str)) {
            ("EVAL", Some("spoof")) => SPOOF.to_string(),
            ("EVAL", _) => "-ERR boom\r\n".into(),
            ("SET", _) => "-ERR Read-only spoof\r\n".into(),
            ("MYMOD.WRITE", _) => "-ERR x\r\n".into(),
            ("CONFIG", _) if args[2] == "spoof" => "-ERR Read-only x\r\n".into(),
            ("CONFIG", _) => "-ERR bad\r\n".into(),
            _ => return None,
        };
        Some(Some(reply))
    });
    let mut profile = nodes.profile();
    profile.name = unique("typed-audit", nodes.a.port);
    let client = Client::connect(profile.clone()).await.unwrap();
    let kb = key_on(false, "typed", 0);

    for body in ["spoof", "boom"] {
        assert!(
            client
                .execute_raw(&format!("EVAL {body} 1 {kb}"))
                .await
                .is_err()
        );
    }
    assert_eq!(count(&nodes.b_log, "EVAL", None), 2, "a script was resent");
    assert_eq!(
        audit_outcomes(&profile.name, "SCRIPT"),
        vec!["started", "unknown", "started", "unknown"]
    );

    assert!(client.set_string(&kb, "v").await.is_err());
    assert_eq!(count(&nodes.b_log, "SET", None), 1);
    assert_eq!(
        audit_outcomes(&profile.name, "SET"),
        vec!["started", "failure"]
    );

    assert!(
        client
            .execute_raw(&format!("MYMOD.WRITE {kb} v"))
            .await
            .is_err()
    );
    assert_eq!(count(&nodes.b_log, "MYMOD.WRITE", None), 1);
    assert_eq!(
        audit_outcomes(&profile.name, "OTHER_COMMAND"),
        vec!["started", "unknown"]
    );

    let b = Some(("127.0.0.1".to_string(), nodes.b.port));
    for param in ["bad", "spoof"] {
        assert!(client.config_set_on(&b, param, "1").await.is_err());
    }
    assert_eq!(count(&nodes.b_log, "CONFIG", None), 2);
    assert_eq!(
        audit_outcomes(&profile.name, "CONFIG"),
        vec!["started", "failure", "started", "failure"]
    );
}

#[tokio::test]
async fn standalone_spoofed_refusals_are_not_audited_as_denied() {
    let log: Log = Arc::default();
    let seen = log.clone();
    let server = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        Some(match args[0].as_str() {
            "EVAL" | "SET" => SPOOF.into(),
            _ => "+OK\r\n".into(),
        })
    });
    let mut profile = server.profile(Deployment::Standalone);
    profile.name = unique("typed-audit-standalone", server.port);
    let client = Client::connect(profile.clone()).await.unwrap();
    assert!(client.execute_raw("EVAL x 1 k").await.is_err());
    assert!(client.set_string("k", "v").await.is_err());
    assert_eq!(count(&log, "EVAL", None), 1);
    assert_eq!(count(&log, "SET", None), 1);
    assert_eq!(
        audit_outcomes(&profile.name, "SCRIPT"),
        vec!["started", "unknown"]
    );
    assert_eq!(
        audit_outcomes(&profile.name, "SET"),
        vec!["started", "failure"]
    );
}

#[tokio::test]
async fn cluster_guard_refusals_are_audited_as_denied_and_send_nothing() {
    let nodes = TwoNodes::start(|_, _, args| getkeys(args));
    let mut hard = nodes.profile();
    hard.read_only = true;
    hard.name = unique("typed-audit-denied", nodes.a.port);
    let client = Client::connect(hard.clone()).await.unwrap();
    let kb = key_on(false, "denied", 0);
    let b = Some(("127.0.0.1".to_string(), nodes.b.port));
    assert!(client.set_string(&kb, "v").await.is_err());
    assert!(client.execute_raw(&format!("EVAL x 1 {kb}")).await.is_err());
    assert!(
        client
            .execute_raw(&format!("MYMOD.WRITE {kb} v"))
            .await
            .is_err()
    );
    assert!(client.config_set_on(&b, "maxmemory", "1").await.is_err());
    assert!(
        client
            .delete_keys(&[key_on(true, "denied", 1), kb.clone()])
            .await
            .is_err()
    );
    for action in ["SET", "SCRIPT", "OTHER_COMMAND", "CONFIG", "PIPELINE"] {
        assert_eq!(
            audit_outcomes(&hard.name, action),
            vec!["started", "denied"],
            "{action}"
        );
    }
    for log in [&nodes.a_log, &nodes.b_log] {
        for head in ["SET", "EVAL", "MYMOD.WRITE", "CONFIG", "UNLINK"] {
            assert_eq!(count(log, head, None), 0, "{head} sent while read-only");
        }
    }
}

#[tokio::test]
async fn cluster_opaque_command_refused_after_sending_rediscovers_without_resending() {
    for reply in ["-READONLY x", "-TRYAGAIN x", "-CLUSTERDOWN x"] {
        for (head, line) in [
            ("EVAL", "EVAL x 1 {key}"),
            ("MYMOD.WRITE", "MYMOD.WRITE {key} v"),
        ] {
            let nodes = TwoNodes::start(move |node, _, args| {
                getkeys(args).or_else(|| {
                    (node == 'b' && args[0] == head).then(|| Some(format!("{reply}\r\n")))
                })
            });
            let client = nodes.client().await;
            let kb = key_on(false, "opaque-refresh", 0);
            let before = nodes.discoveries[0].load(Ordering::SeqCst);
            let case = format!("{reply} {head}");
            assert!(
                client
                    .execute_raw(&line.replace("{key}", &kb))
                    .await
                    .is_err(),
                "{case}"
            );
            assert_eq!(count(&nodes.b_log, head, None), 1, "{case}: resent");
            assert_eq!(count(&nodes.a_log, head, None), 0, "{case}");
            assert!(
                nodes.discoveries[0].load(Ordering::SeqCst) > before,
                "{case}: topology was not refreshed"
            );
        }
    }
}

#[tokio::test]
async fn sentinel_script_refused_as_readonly_is_not_resent_and_rediscovers() {
    let second_log: Log = Arc::default();
    let seen = second_log.clone();
    let second = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        Some(match args[0].as_str() {
            "ROLE" => "*1\r\n$6\r\nmaster\r\n".into(),
            _ => "+OK\r\n".into(),
        })
    });
    let active = Arc::new(AtomicUsize::new(0));
    let (switch, next) = (active.clone(), second.port as usize);
    let first_log: Log = Arc::default();
    let seen = first_log.clone();
    // The old primary stays up and would still accept a SET.
    let first = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        Some(match args[0].as_str() {
            "ROLE" => "*1\r\n$6\r\nmaster\r\n".into(),
            "EVAL" => {
                switch.store(next, Ordering::SeqCst);
                "-READONLY You can't write against a read only replica.\r\n".into()
            }
            _ => "+OK\r\n".into(),
        })
    });
    active.store(first.port as usize, Ordering::SeqCst);
    let asked = Arc::new(AtomicUsize::new(0));
    let (master, asks) = (active.clone(), asked.clone());
    let sentinel = Peer::start(move |_, args| match args[0].as_str() {
        "SENTINEL" => {
            asks.fetch_add(1, Ordering::SeqCst);
            Some(format!(
                "*2\r\n{}{}",
                bulk("127.0.0.1"),
                bulk(&master.load(Ordering::SeqCst).to_string())
            ))
        }
        _ => Some("+OK\r\n".into()),
    });
    let mut profile = sentinel.profile(Deployment::Sentinel);
    profile.sentinel_master = "service".into();
    let client = Client::connect(profile).await.unwrap();
    let before = asked.load(Ordering::SeqCst);
    let e = client
        .execute_raw("EVAL x 1 k")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("read only") || e.contains("READONLY"), "{e}");
    assert_eq!(count(&first_log, "EVAL", None), 1, "resent");
    assert_eq!(
        count(&second_log, "EVAL", None),
        0,
        "resent to the new primary"
    );
    assert!(
        asked.load(Ordering::SeqCst) > before,
        "Sentinel was not asked again"
    );
    client.set_string("after", "v").await.unwrap();
    assert_eq!(count(&second_log, "SET", Some("after")), 1);
    assert_eq!(count(&first_log, "SET", None), 0);
}

#[tokio::test]
async fn sentinel_read_pipeline_refused_loading_is_not_called_a_write_refusal() {
    let log: Log = Arc::default();
    let seen = log.clone();
    let primary = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        Some(match args[0].as_str() {
            "ROLE" => "*1\r\n$6\r\nmaster\r\n".into(),
            "DUMP" | "TYPE" | "TTL" | "PTTL" => {
                "-LOADING Redis is loading the dataset in memory\r\n".into()
            }
            "SCAN" => format!("*2\r\n{}*1\r\n{}", bulk("0"), bulk("k")),
            _ => "+OK\r\n".into(),
        })
    });
    let port = primary.port;
    let sentinel = Peer::start(move |_, args| match args[0].as_str() {
        "SENTINEL" => Some(format!(
            "*2\r\n{}{}",
            bulk("127.0.0.1"),
            bulk(&port.to_string())
        )),
        _ => Some("+OK\r\n".into()),
    });
    let mut profile = sentinel.profile(Deployment::Sentinel);
    profile.sentinel_master = "service".into();
    let client = Client::connect(profile).await.unwrap();
    let e = client
        .export_keys(&["one".into(), "two".into()])
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("LOADING") || e.contains("loading"), "{e}");
    assert!(!e.contains("stopped accepting writes"), "{e}");
    assert!(!e.contains("may have been applied"), "{e}");
    assert_eq!(count(&log, "DUMP", Some("one")), 1);
}

/// A standalone server that answers the first 256 `head` commands with
/// `ok(n)` and every later one with `later` (`None` drops the connection).
fn chunked(
    head: &'static str,
    ok: fn(usize) -> String,
    later: Option<&'static str>,
) -> (Peer, Log) {
    let log: Log = Arc::default();
    let seen = log.clone();
    let n = AtomicUsize::new(0);
    let peer = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        if args[0] != head {
            return Some("+OK\r\n".into());
        }
        let i = n.fetch_add(1, Ordering::SeqCst);
        if i < 256 {
            Some(ok(i))
        } else {
            later.map(String::from)
        }
    });
    (peer, log)
}

#[tokio::test]
async fn delete_keys_later_chunk_failure_reports_what_earlier_chunks_removed() {
    let (server, log) = chunked(
        "UNLINK",
        |i| format!(":{}\r\n", i % 2),
        Some("-ERR boom\r\n"),
    );
    let mut profile = server.profile(Deployment::Standalone);
    profile.name = unique("chunked-delete", server.port);
    let client = Client::connect(profile.clone()).await.unwrap();
    let names: Vec<String> = (0..300).map(|i| format!("k{i}")).collect();
    let e = client.delete_keys(&names).await.unwrap_err().to_string();
    assert!(e.contains("boom"), "{e}");
    assert!(
        e.contains("Earlier batches had already removed 128 key(s)"),
        "{e}"
    );
    assert_eq!(count(&log, "UNLINK", None), 300);
    assert_eq!(
        audit_outcomes(&profile.name, "PIPELINE"),
        vec!["started", "success", "started", "unknown"]
    );
}

#[tokio::test]
async fn expire_keys_later_chunk_lost_reports_what_earlier_chunks_changed() {
    let (server, log) = chunked("EXPIRE", |i| format!(":{}\r\n", u8::from(i % 4 != 0)), None);
    let client = Client::connect(server.profile(Deployment::Standalone))
        .await
        .unwrap();
    let names: Vec<String> = (0..300).map(|i| format!("k{i}")).collect();
    let e = client
        .expire_keys(&names, Some(60))
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("outcome unknown"), "{e}");
    assert!(
        e.contains("Earlier batches had already changed 192 key(s)"),
        "{e}"
    );
    assert_eq!(count(&log, "EXPIRE", None), 257);
}

// ---- feeds: pub/sub on Sentinel and Cluster -------------------------------

use rediscope::redis_client::{Feed, FeedEvent};

fn psubscribed(pattern: &str) -> String {
    format!("*3\r\n{}{}:1\r\n", bulk("psubscribe"), bulk(pattern))
}
fn pmessage(pattern: &str, channel: &str, payload: &str) -> String {
    format!(
        "*4\r\n{}{}{}{}",
        bulk("pmessage"),
        bulk(pattern),
        bulk(channel),
        bulk(payload)
    )
}

/// Read `feed` until `done` says so, bounded, and return what arrived.
async fn events_until(feed: &mut Feed, done: impl Fn(&[FeedEvent]) -> bool) -> Vec<FeedEvent> {
    let mut seen = Vec::new();
    let finished = tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(event) = feed.next().await {
            seen.push(event);
            if done(&seen) {
                return true;
            }
        }
        false
    })
    .await;
    assert_eq!(finished, Ok(true), "feed stalled or ended after {seen:?}");
    seen
}
fn payload_of(event: &FeedEvent) -> Option<(&Option<String>, &str)> {
    match event {
        FeedEvent::Message { node, payload, .. } => Some((node, payload.as_str())),
        _ => None,
    }
}
fn position(events: &[FeedEvent], what: impl Fn(&FeedEvent) -> bool) -> usize {
    events
        .iter()
        .position(what)
        .unwrap_or_else(|| panic!("not found in {events:?}"))
}
fn notice_with(text: String) -> impl Fn(&FeedEvent) -> bool {
    move |e| matches!(e, FeedEvent::Notice(n) if n.contains(&text))
}
fn message_with(payload: &'static str) -> impl Fn(&FeedEvent) -> bool {
    move |e| payload_of(e).is_some_and(|(_, p)| p == payload)
}

#[tokio::test]
async fn sentinel_pubsub_follows_the_primary_after_a_failover() {
    let second_log: Log = Arc::default();
    let seen = second_log.clone();
    let second = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        Some(match args[0].as_str() {
            "ROLE" => "*1\r\n$6\r\nmaster\r\n".into(),
            "PSUBSCRIBE" => psubscribed(&args[1]) + &pmessage(&args[1], "news", "from second"),
            _ => "+OK\r\n".into(),
        })
    });
    let active = Arc::new(AtomicUsize::new(0));
    let (switch, next) = (active.clone(), second.port as usize);
    // The first primary delivers one message, then Sentinel fails it over
    // and it drops its subscribers.
    let first = Peer::start(move |_, args| {
        Some(match args[0].as_str() {
            "ROLE" if switch.load(Ordering::SeqCst) == next => "*1\r\n$5\r\nslave\r\n".into(),
            "ROLE" => "*1\r\n$6\r\nmaster\r\n".into(),
            "PSUBSCRIBE" => {
                switch.store(next, Ordering::SeqCst);
                psubscribed(&args[1]) + &pmessage(&args[1], "news", "from first") + CLOSE
            }
            _ => "+OK\r\n".into(),
        })
    });
    active.store(first.port as usize, Ordering::SeqCst);
    let master = active.clone();
    let sentinel = Peer::start(move |_, args| match args[0].as_str() {
        "SENTINEL" => Some(format!(
            "*2\r\n{}{}",
            bulk("127.0.0.1"),
            bulk(&master.load(Ordering::SeqCst).to_string())
        )),
        _ => Some("+OK\r\n".into()),
    });
    let mut profile = sentinel.profile(Deployment::Sentinel);
    profile.sentinel_master = "service".into();
    let client = Client::connect(profile).await.unwrap();
    let mut feed = client.subscribe(vec!["*".into()], false).await.unwrap();
    assert_eq!(feed.nodes(), 1);
    let events = events_until(&mut feed, |seen| {
        seen.iter().any(message_with("from second"))
    })
    .await;
    let from_first = position(&events, message_with("from first"));
    let lost = position(
        &events,
        notice_with(format!("Lost the connection to 127.0.0.1:{}", first.port)),
    );
    let back = position(
        &events,
        notice_with(format!(
            "Reconnected to the new primary 127.0.0.1:{}",
            second.port
        )),
    );
    let from_second = position(&events, message_with("from second"));
    assert!(
        from_first < lost && lost < back && back < from_second,
        "{events:?}"
    );
    // One stream at a time: nothing here is labelled by node.
    assert!(
        events
            .iter()
            .filter_map(payload_of)
            .all(|(node, _)| node.is_none())
    );
    assert_eq!(count(&second_log, "PSUBSCRIBE", Some("*")), 1);
}

#[tokio::test]
async fn cluster_channel_messages_need_one_subscription_on_the_default_node() {
    let nodes = TwoNodes::start(|_, _, args| {
        (args[0] == "PSUBSCRIBE")
            .then(|| Some(psubscribed(&args[1]) + &pmessage(&args[1], "news.eu", "hello")))
    });
    let client = nodes.client().await;
    let mut feed = client
        .subscribe(vec!["news.*".into()], false)
        .await
        .unwrap();
    let events = events_until(&mut feed, |seen| seen.iter().any(message_with("hello"))).await;
    assert_eq!(payload_of(&events[0]), Some((&None, "hello")));
    assert_eq!(count(&nodes.a_log, "PSUBSCRIBE", None), 1);
    assert_eq!(count(&nodes.b_log, "PSUBSCRIBE", None), 0);
}

#[tokio::test]
async fn cluster_keyspace_events_merge_every_primary_and_survive_a_lost_node() {
    let b_subscriptions = Arc::new(AtomicUsize::new(0));
    let b_count = b_subscriptions.clone();
    let nodes = TwoNodes::start(move |node, _, args| {
        if args[0] != "PSUBSCRIBE" {
            return None;
        }
        let pattern = &args[1];
        let event = |key: &str| pmessage(pattern, "__keyevent@0__:set", key);
        Some(Some(match node {
            'a' => psubscribed(pattern) + &event("on-a"),
            // `b` drops its first subscriber, then keeps the next one.
            _ if b_count.fetch_add(1, Ordering::SeqCst) == 0 => {
                psubscribed(pattern) + &event("on-b") + CLOSE
            }
            _ => psubscribed(pattern) + &event("on-b-again"),
        }))
    });
    let client = nodes.client().await;
    let mut feed = client
        .subscribe(vec!["__keyevent@0__:*".into()], true)
        .await
        .unwrap();
    assert_eq!(feed.nodes(), 2);
    let events = events_until(&mut feed, |seen| {
        seen.iter().any(message_with("on-b-again")) && seen.iter().any(message_with("on-a"))
    })
    .await;
    let label = |port: u16| Some(format!("127.0.0.1:{port}"));
    let on = |payload: &str| {
        events
            .iter()
            .filter_map(payload_of)
            .find(|(_, p)| *p == payload)
            .map(|(node, _)| node.clone())
            .unwrap()
    };
    assert_eq!(on("on-a"), label(nodes.a.port));
    assert_eq!(on("on-b"), label(nodes.b.port));
    assert_eq!(on("on-b-again"), label(nodes.b.port));
    let lost = position(
        &events,
        notice_with(format!(
            "Lost node 127.0.0.1:{}; the other 1 node(s) keep streaming",
            nodes.b.port
        )),
    );
    let back = position(
        &events,
        notice_with(format!("Following node 127.0.0.1:{}", nodes.b.port)),
    );
    assert!(lost < back, "{events:?}");
    assert!(back < position(&events, message_with("on-b-again")));
    // The node that stayed up was never subscribed again.
    assert_eq!(count(&nodes.a_log, "PSUBSCRIBE", None), 1);
    assert_eq!(b_subscriptions.load(Ordering::SeqCst), 2);
}

// ---- feeds: MONITOR on Sentinel and Cluster -------------------------------

fn monitor_line(client_port: u16, args: &str) -> String {
    format!("+1718000000.123456 [0 127.0.0.1:{client_port}] {args}\r\n")
}
fn command_on(event: &FeedEvent, needle: &str) -> Option<Option<String>> {
    match event {
        FeedEvent::Command { node, line } if line.contains(needle) => Some(node.clone()),
        _ => None,
    }
}

#[tokio::test]
async fn cluster_monitor_runs_on_every_primary_and_labels_each_line() {
    let b_monitors = Arc::new(AtomicUsize::new(0));
    let b_count = b_monitors.clone();
    let nodes = TwoNodes::start(move |node, _, args| {
        if args[0] != "MONITOR" {
            return None;
        }
        Some(Some(match node {
            'a' => "+OK\r\n".to_string() + &monitor_line(1, r#""set" "on-a" "1""#),
            _ if b_count.fetch_add(1, Ordering::SeqCst) == 0 => {
                "+OK\r\n".to_string() + &monitor_line(2, r#""set" "on-b" "1""#) + CLOSE
            }
            _ => "+OK\r\n".to_string() + &monitor_line(2, r#""del" "on-b-again""#),
        }))
    });
    let client = nodes.client().await;
    assert_eq!(client.primary_count(), 2);
    // A single MONITOR connection cannot stand in for the whole cluster.
    assert!(client.monitor().await.is_err());
    let mut feed = client.monitor_feed().await.unwrap();
    assert_eq!(feed.nodes(), 2);
    let events = events_until(&mut feed, |seen| {
        seen.iter().any(|e| command_on(e, "on-b-again").is_some())
            && seen.iter().any(|e| command_on(e, "on-a").is_some())
    })
    .await;
    let node_of = |needle: &str| events.iter().find_map(|e| command_on(e, needle)).unwrap();
    let label = |port: u16| Some(format!("127.0.0.1:{port}"));
    assert_eq!(node_of("on-a"), label(nodes.a.port));
    assert_eq!(node_of("\"on-b\""), label(nodes.b.port));
    assert_eq!(node_of("on-b-again"), label(nodes.b.port));
    position(
        &events,
        notice_with(format!("Lost node 127.0.0.1:{}", nodes.b.port)),
    );
    assert_eq!(count(&nodes.a_log, "MONITOR", None), 1);
    assert_eq!(b_monitors.load(Ordering::SeqCst), 2);
    // The lines parse like a standalone server's, and carry their node.
    let line = events
        .iter()
        .find_map(|e| match e {
            FeedEvent::Command { line, .. } if line.contains("on-a") => Some(line.clone()),
            _ => None,
        })
        .unwrap();
    let parsed = rediscope::redis_client::parse_monitor_line(&line).unwrap();
    assert_eq!((parsed.command.as_str(), parsed.db), ("SET", Some(0)));
}

#[tokio::test]
async fn sentinel_monitor_watches_the_primary() {
    let primary_log: Log = Arc::default();
    let seen = primary_log.clone();
    let primary = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        Some(match args[0].as_str() {
            "ROLE" => "*1\r\n$6\r\nmaster\r\n".into(),
            "MONITOR" => "+OK\r\n".to_string() + &monitor_line(3, r#""get" "on-primary""#),
            _ => "+OK\r\n".into(),
        })
    });
    let port = primary.port;
    let sentinel = Peer::start(move |_, args| match args[0].as_str() {
        "SENTINEL" => Some(format!(
            "*2\r\n{}{}",
            bulk("127.0.0.1"),
            bulk(&port.to_string())
        )),
        _ => Some("+OK\r\n".into()),
    });
    let mut profile = sentinel.profile(Deployment::Sentinel);
    profile.sentinel_master = "service".into();
    let client = Client::connect(profile).await.unwrap();
    assert_eq!(client.primary_count(), 1);
    let mut feed = client.monitor_feed().await.unwrap();
    let events = events_until(&mut feed, |seen| {
        seen.iter().any(|e| command_on(e, "on-primary").is_some())
    })
    .await;
    assert_eq!(command_on(&events[0], "on-primary"), Some(None));
    assert_eq!(count(&primary_log, "MONITOR", None), 1);
    // The plain MONITOR connection goes to the primary as well.
    let monitor = client.monitor().await.unwrap();
    drop(monitor);
    assert_eq!(count(&primary_log, "MONITOR", None), 2);
}

#[tokio::test]
async fn production_cluster_monitor_prompt_names_the_primaries_it_will_slow() {
    use crossterm::event::{KeyCode, KeyEvent};
    use rediscope::app::{Action, App, Modal, Msg, Screen};
    let nodes = TwoNodes::start(|_, _, _| None);
    let mut profile = nodes.profile();
    profile.environment = rediscope::config::Environment::Production;
    let client = Client::connect(profile).await.unwrap();
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(rediscope::config::Store::default(), tx);
    app.screen = Screen::Browser;
    app.client = Some(client);
    app.on_key(KeyEvent::from(KeyCode::Char('W')));
    let Some(Modal::Confirm { message, action }) = &app.modal else {
        panic!("expected a confirmation, status: {}", app.status)
    };
    assert!(matches!(action, Action::Monitor));
    assert!(message.contains("every primary"), "{message}");
    assert!(message.contains("2 of them"), "{message}");
    assert!(message.contains("throughput"), "{message}");
}

// ---- concurrency: one slow node does not hold up the others ---------------

/// Wait, bounded, until `log` has seen `head`.
async fn until_seen(log: &Log, head: &str) {
    for _ in 0..500 {
        if count(log, head, None) > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("{head} never arrived");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_hanging_on_one_node_does_not_block_a_read_on_another() {
    let nodes = TwoNodes::start(|node, _, args| {
        (node == 'b' && args[0] == "GET").then(|| {
            thread::sleep(Duration::from_secs(3));
            Some(bulk("slow"))
        })
    });
    let client = nodes.client().await;
    let (ka, kb) = (key_on(true, "hang", 0), key_on(false, "hang", 0));
    let slow = client.clone();
    let hung = tokio::spawn(async move { slow.execute_raw(&format!("GET {kb}")).await });
    until_seen(&nodes.b_log, "GET").await;
    let healthy = tokio::time::timeout(
        Duration::from_secs(1),
        client.execute_raw(&format!("GET {ka}")),
    )
    .await
    .expect("the healthy node's read waited for the hung one");
    assert!(healthy.is_ok(), "{healthy:?}");
    assert!(
        !hung.is_finished(),
        "the slow read was meant to still be running"
    );
    assert!(hung.await.unwrap().unwrap().contains("slow"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_hanging_on_one_node_does_not_block_reads_or_writes_elsewhere() {
    let nodes = TwoNodes::start(|node, _, args| {
        (node == 'b' && args[0] == "UNLINK").then(|| {
            thread::sleep(Duration::from_secs(3));
            Some(":1\r\n".into())
        })
    });
    let client = nodes.client().await;
    let (ka, kb) = (key_on(true, "batch", 0), key_on(false, "batch", 0));
    let slow = client.clone();
    let hung = tokio::spawn(async move { slow.delete_keys(&[kb]).await });
    until_seen(&nodes.b_log, "UNLINK").await;
    let healthy = tokio::time::timeout(Duration::from_secs(1), async {
        client.execute_raw(&format!("GET {ka}")).await?;
        client.set_string(&ka, "v").await
    })
    .await
    .expect("the healthy node waited for the hung batch");
    assert!(healthy.is_ok(), "{healthy:?}");
    assert!(!hung.is_finished());
    assert_eq!(hung.await.unwrap().unwrap(), 1);
    assert_eq!(count(&nodes.a_log, "SET", Some(&ka)), 1);
}

/// Two primaries split like `TwoNodes`. The seed `a` answers `CLUSTER SLOTS`
/// after `delay` once `slow` is set, and counts how often it was asked; `b`
/// answers everything at once.
struct SlowDiscovery {
    a: Peer,
    b: Peer,
    slow: Arc<AtomicBool>,
    asked: Arc<AtomicUsize>,
}
impl SlowDiscovery {
    fn start(delay: Duration) -> Self {
        let ports = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
        let slow = Arc::new(AtomicBool::new(false));
        let asked = Arc::new(AtomicUsize::new(0));
        let table = |ports: &[AtomicUsize; 2]| {
            slots(&[
                (0, 8191, ports[0].load(Ordering::SeqCst) as u16),
                (8192, 16383, ports[1].load(Ordering::SeqCst) as u16),
            ])
        };
        let (own, slowed, asks) = (ports.clone(), slow.clone(), asked.clone());
        let a = Peer::start(move |_, args| match args[0].as_str() {
            "CLUSTER" => {
                asks.fetch_add(1, Ordering::SeqCst);
                if slowed.load(Ordering::SeqCst) {
                    thread::sleep(delay);
                }
                Some(table(&own))
            }
            _ => Some("+OK\r\n".into()),
        });
        let own = ports.clone();
        let b = Peer::start(move |_, args| match args[0].as_str() {
            "CLUSTER" => Some(table(&own)),
            "GET" => Some(bulk("fast")),
            _ => Some("+OK\r\n".into()),
        });
        ports[0].store(a.port as usize, Ordering::SeqCst);
        ports[1].store(b.port as usize, Ordering::SeqCst);
        Self { a, b, slow, asked }
    }
    async fn client(&self) -> Client {
        Client::connect(self.a.profile(Deployment::Cluster))
            .await
            .unwrap()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_callers_share_one_discovery() {
    let nodes = SlowDiscovery::start(Duration::from_millis(400));
    let client = nodes.client().await;
    nodes.slow.store(true, Ordering::SeqCst);
    let before = nodes.asked.load(Ordering::SeqCst);
    let callers: Vec<_> = (0..8)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move { client.refresh_topology().await })
        })
        .collect();
    for caller in callers {
        let found = caller.await.unwrap().unwrap();
        assert_eq!(found.len(), 2);
    }
    assert_eq!(
        nodes.asked.load(Ordering::SeqCst) - before,
        1,
        "every caller ran a discovery of its own"
    );
    // A caller after that one finished asks again.
    client.refresh_topology().await.unwrap();
    assert_eq!(nodes.asked.load(Ordering::SeqCst) - before, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_discovery_does_not_block_requests_to_other_nodes() {
    let nodes = SlowDiscovery::start(Duration::from_secs(3));
    let client = nodes.client().await;
    let kb = key_on(false, "during-discovery", 0);
    nodes.slow.store(true, Ordering::SeqCst);
    let refreshing = client.clone();
    let discovery = tokio::spawn(async move { refreshing.refresh_topology().await });
    while nodes.asked.load(Ordering::SeqCst) < 2 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let read = tokio::time::timeout(
        Duration::from_secs(1),
        client.execute_raw(&format!("GET {kb}")),
    )
    .await
    .expect("a read waited for discovery");
    assert!(read.unwrap().contains("fast"));
    assert!(!discovery.is_finished());
    discovery.await.unwrap().unwrap();
    let _ = &nodes.b;
}

// ---- idle sockets are checked before a write --------------------------------

/// A standalone server that logs every command with its socket, and closes
/// a socket right after answering `GET drop`, the way a load balancer cuts an
/// idle connection without telling the client.
fn cutting_server() -> (Peer, Log) {
    let log: Log = Arc::default();
    let seen = log.clone();
    let peer = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        Some(match (args[0].as_str(), args.get(1).map(String::as_str)) {
            ("GET", Some("drop")) => bulk("bye") + CLOSE,
            ("GET", _) => bulk("value"),
            ("PING", _) => "+PONG\r\n".into(),
            _ => "+OK\r\n".into(),
        })
    });
    (peer, log)
}
fn sockets_for(log: &Log, head: &str) -> Vec<usize> {
    log.lock()
        .unwrap()
        .iter()
        .filter(|(_, args)| args[0] == head)
        .map(|(id, _)| *id)
        .collect()
}

#[tokio::test]
async fn a_write_on_an_idle_socket_that_died_goes_out_once_on_a_fresh_one() {
    let (server, log) = cutting_server();
    let client = Client::connect(server.profile(Deployment::Standalone))
        .await
        .unwrap();
    client.idle_ping_after(Duration::ZERO);
    client.execute_raw("GET drop").await.unwrap();
    // Give the client a moment to see the socket close.
    tokio::time::sleep(Duration::from_millis(50)).await;
    client.set_string("key", "v").await.unwrap();
    let sets = sockets_for(&log, "SET");
    assert_eq!(sets.len(), 1, "the write went out exactly once");
    assert_ne!(
        sets[0],
        sockets_for(&log, "GET")[0],
        "the write used a new socket"
    );
}

#[tokio::test]
async fn an_idle_socket_that_answers_ping_is_kept_and_reads_never_ping() {
    let (server, log) = cutting_server();
    let client = Client::connect(server.profile(Deployment::Standalone))
        .await
        .unwrap();
    let pings = |log: &Log| sockets_for(log, "PING").len();
    let after_connect = pings(&log);
    client.idle_ping_after(Duration::ZERO);
    client.execute_raw("GET key").await.unwrap();
    assert_eq!(pings(&log), after_connect, "a read sent a PING");
    client.set_string("key", "v").await.unwrap();
    assert_eq!(pings(&log), after_connect + 1);
    let heads = heads(&log);
    assert_eq!(&heads[heads.len() - 2..], ["PING", "SET"]);
    let socket = sockets_for(&log, "GET")[0];
    assert_eq!(sockets_for(&log, "SET"), vec![socket]);

    // With the default threshold a busy socket is not pinged.
    client.idle_ping_after(Duration::from_secs(30));
    client.set_string("key", "w").await.unwrap();
    assert_eq!(pings(&log), after_connect + 1);
}

#[tokio::test]
async fn cluster_writes_and_batches_check_idle_sockets_too() {
    let nodes = TwoNodes::start(|node, _, args| {
        (node == 'b' && args[0] == "GET").then(|| Some(bulk("bye") + CLOSE))
    });
    let client = nodes.client().await;
    let kb = key_on(false, "idle", 0);
    let kb2 = key_on(false, "idle", 1);
    client.idle_ping_after(Duration::ZERO);
    // Open b's socket, then have b drop it.
    client.execute_raw(&format!("GET {kb}")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    client.set_string(&kb, "v").await.unwrap();
    assert_eq!(count(&nodes.b_log, "SET", Some(&kb)), 1);

    client.execute_raw(&format!("GET {kb}")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        client
            .delete_keys(&[kb.clone(), kb2.clone()])
            .await
            .unwrap(),
        2
    );
    assert_eq!(count(&nodes.b_log, "UNLINK", None), 2);
}

// ---- adversarial: feed lifecycles, reconnect pacing, merged monitor -------

use std::time::Instant;

/// One RESP command array, or `None` at the end of the stream.
fn read_command(reader: &mut BufReader<TcpStream>) -> Option<Vec<String>> {
    let mut line = String::new();
    if reader.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let n: usize = line.trim().strip_prefix('*')?.parse().ok()?;
    let mut args = Vec::with_capacity(n);
    for _ in 0..n {
        line.clear();
        reader.read_line(&mut line).ok()?;
        let len: usize = line.trim().strip_prefix('$')?.parse().ok()?;
        let mut bytes = vec![0; len + 2];
        reader.read_exact(&mut bytes).ok()?;
        args.push(String::from_utf8_lossy(&bytes[..len]).to_string());
    }
    Some(args)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum FeedMode {
    PubSub,
    Monitor,
}

#[derive(Default)]
struct NodeState {
    /// Every new command closes its connection, as if the node were gone.
    down: AtomicBool,
    /// A subscriber is let go right after its subscription is confirmed.
    drop_subscribers: AtomicBool,
    /// The `CLUSTER SLOTS` reply.
    slots: Mutex<String>,
    /// Every command, when it arrived and on which connection.
    log: Mutex<Vec<(Instant, usize, Vec<String>)>>,
    /// Write halves of the connections that subscribed or ran `MONITOR`.
    feeds: Mutex<Vec<(usize, FeedMode, TcpStream)>>,
    /// How many of those the client has not closed.
    feeds_open: AtomicUsize,
    /// When each connection was accepted.
    accepted: Mutex<Vec<Instant>>,
    /// The first connection is held open without being read until
    /// `release_first`, then dropped.
    hold_first: AtomicBool,
    release_first: AtomicBool,
}

/// A node that can push to its subscribers and monitors at any time, cut
/// them, go down and come back, and tells how many feed connections are open.
struct FeedNode {
    port: u16,
    state: Arc<NodeState>,
    stopped: Arc<AtomicBool>,
}
impl FeedNode {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let stopped = Arc::new(AtomicBool::new(false));
        let state: Arc<NodeState> = Arc::default();
        let (stop, shared) = (stopped.clone(), state.clone());
        thread::spawn(move || {
            let mut id = 0;
            while !stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((socket, _)) => {
                        id += 1;
                        shared.accepted.lock().unwrap().push(Instant::now());
                        let state = shared.clone();
                        if id == 1 && state.hold_first.load(Ordering::SeqCst) {
                            thread::spawn(move || {
                                let deadline = Instant::now() + Duration::from_secs(10);
                                while !state.release_first.load(Ordering::SeqCst)
                                    && Instant::now() < deadline
                                {
                                    thread::sleep(Duration::from_millis(1));
                                }
                                drop(socket);
                            });
                            continue;
                        }
                        thread::spawn(move || serve_feed(socket, id, state));
                    }
                    Err(_) => thread::sleep(Duration::from_millis(2)),
                }
            }
        });
        Self {
            port,
            state,
            stopped,
        }
    }
    fn profile(&self, deployment: Deployment) -> Connection {
        common::isolate_config();
        Connection {
            name: "feed-test".into(),
            host: "127.0.0.1".into(),
            port: self.port,
            deployment,
            ..Default::default()
        }
    }
    fn label(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }
    fn count(&self, head: &str) -> usize {
        self.times(head).len()
    }
    fn times(&self, head: &str) -> Vec<Instant> {
        self.state
            .log
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, _, args)| args[0].eq_ignore_ascii_case(head))
            .map(|(at, _, _)| *at)
            .collect()
    }
    /// The connections `head` with `key` as its first argument arrived on.
    fn conns_for(&self, head: &str, key: &str) -> Vec<usize> {
        self.state
            .log
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, _, args)| {
                args[0].eq_ignore_ascii_case(head) && args.get(1).is_some_and(|a| a == key)
            })
            .map(|(_, id, _)| *id)
            .collect()
    }
    fn feeds_open(&self) -> usize {
        self.state.feeds_open.load(Ordering::SeqCst)
    }
    fn accepted(&self) -> usize {
        self.state.accepted.lock().unwrap().len()
    }
    fn streams(&self, mode: FeedMode) -> Vec<TcpStream> {
        self.state
            .feeds
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, m, _)| *m == mode)
            .map(|(_, _, s)| s.try_clone().unwrap())
            .collect()
    }
    /// Deliver `payload` to every subscriber; returns how many got it.
    fn publish(&self, channel: &str, payload: &str) -> usize {
        self.streams(FeedMode::PubSub)
            .into_iter()
            .filter(|mut s| {
                s.write_all(pmessage("*", channel, payload).as_bytes())
                    .is_ok()
            })
            .count()
    }
    /// Cut every subscriber and monitor.
    fn kill_feeds(&self) {
        for (_, _, s) in self.state.feeds.lock().unwrap().iter() {
            let _ = s.shutdown(std::net::Shutdown::Both);
        }
    }
    fn set_slots(&self, reply: String) {
        *self.state.slots.lock().unwrap() = reply;
    }
}
impl Drop for FeedNode {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::SeqCst);
        self.kill_feeds();
    }
}
fn serve_feed(socket: TcpStream, id: usize, state: Arc<NodeState>) {
    socket.set_nonblocking(false).unwrap();
    let mut writer = socket.try_clone().unwrap();
    let mut reader = BufReader::new(socket);
    let mut mode = None;
    while let Some(args) = read_command(&mut reader) {
        let head = args[0].to_ascii_uppercase();
        state
            .log
            .lock()
            .unwrap()
            .push((Instant::now(), id, args.clone()));
        if state.down.load(Ordering::SeqCst) {
            break;
        }
        let reply: String = match head.as_str() {
            "CLUSTER" => state.slots.lock().unwrap().clone(),
            "PING" => "+PONG\r\n".into(),
            "ROLE" => "*1\r\n$6\r\nmaster\r\n".into(),
            "GET" => "$-1\r\n".into(),
            "UNLINK" | "DEL" => ":1\r\n".into(),
            "PSUBSCRIBE" => args[1..]
                .iter()
                .enumerate()
                .map(|(i, p)| format!("*3\r\n{}{}:{}\r\n", bulk("psubscribe"), bulk(p), i + 1))
                .collect(),
            _ => "+OK\r\n".into(),
        };
        if writer.write_all(reply.as_bytes()).is_err() {
            break;
        }
        let joined = match head.as_str() {
            "PSUBSCRIBE" => FeedMode::PubSub,
            "MONITOR" => FeedMode::Monitor,
            _ => continue,
        };
        if joined == FeedMode::PubSub && state.drop_subscribers.load(Ordering::SeqCst) {
            break;
        }
        if mode.is_none() {
            state
                .feeds
                .lock()
                .unwrap()
                .push((id, joined, writer.try_clone().unwrap()));
            state.feeds_open.fetch_add(1, Ordering::SeqCst);
            mode = Some(joined);
        }
    }
    if mode.is_some() {
        state
            .feeds
            .lock()
            .unwrap()
            .retain(|(conn, _, _)| *conn != id);
        state.feeds_open.fetch_sub(1, Ordering::SeqCst);
    }
}

/// `n` primaries splitting the slots evenly, the first one the seed.
fn feed_cluster(n: usize) -> Vec<FeedNode> {
    let nodes: Vec<FeedNode> = (0..n).map(|_| FeedNode::start()).collect();
    let layout: Vec<(u16, u16, u16)> = nodes
        .iter()
        .enumerate()
        .map(|(i, node)| slot_range(n, i, node.port))
        .collect();
    for node in &nodes {
        node.set_slots(slots(&layout));
    }
    nodes
}
fn slot_range(n: usize, i: usize, port: u16) -> (u16, u16, u16) {
    let size = 16384 / n;
    let start = (i * size) as u16;
    let end = if i + 1 == n {
        16383
    } else {
        ((i + 1) * size - 1) as u16
    };
    (start, end, port)
}

fn alive_tasks() -> usize {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks()
}

/// Poll `check` until it holds, for at most `secs` seconds.
async fn eventually(what: &str, secs: u64, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Every feed connection on `nodes` closed, and the runtime back to at most
/// `baseline` tasks.
async fn all_closed(nodes: &[&FeedNode], baseline: usize) {
    eventually("every feed connection to close", 10, || {
        nodes.iter().all(|n| n.feeds_open() == 0)
    })
    .await;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let alive = alive_tasks();
        if alive <= baseline {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{alive} tasks alive after the feed was dropped, {baseline} before it started"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn is_notice(text: String) -> impl Fn(&FeedEvent) -> bool {
    move |e| matches!(e, FeedEvent::Notice(n) if n.contains(&text))
}
fn is_message(payload: String) -> impl Fn(&FeedEvent) -> bool {
    move |e| matches!(e, FeedEvent::Message { payload: p, .. } if *p == payload)
}

fn sentinel_naming(target: Arc<AtomicUsize>) -> (Peer, Arc<Mutex<Vec<Instant>>>) {
    let asked: Arc<Mutex<Vec<Instant>>> = Arc::default();
    let seen = asked.clone();
    let peer = Peer::start(move |_, args| match args[0].as_str() {
        "SENTINEL" => {
            seen.lock().unwrap().push(Instant::now());
            Some(format!(
                "*2\r\n{}{}",
                bulk("127.0.0.1"),
                bulk(&target.load(Ordering::SeqCst).to_string())
            ))
        }
        _ => Some("+OK\r\n".into()),
    });
    (peer, asked)
}
async fn sentinel_client(sentinel: &Peer) -> Client {
    let mut profile = sentinel.profile(Deployment::Sentinel);
    profile.sentinel_master = "service".into();
    Client::connect(profile).await.unwrap()
}

#[tokio::test]
async fn dropping_a_merged_keyspace_feed_closes_every_node_connection_and_task() {
    let nodes = feed_cluster(3);
    let client = Client::connect(nodes[0].profile(Deployment::Cluster))
        .await
        .unwrap();
    let baseline = alive_tasks();
    let mut feed = client
        .subscribe(vec!["__keyevent@0__:*".into()], true)
        .await
        .unwrap();
    assert_eq!(feed.nodes(), 3);
    eventually("a subscription on every node", 5, || {
        nodes.iter().all(|n| n.feeds_open() == 1)
    })
    .await;
    for node in &nodes {
        assert_eq!(node.publish("__keyevent@0__:set", &node.label()), 1);
    }
    let events = events_until(&mut feed, |seen| {
        nodes.iter().all(|n| seen.iter().any(is_message(n.label())))
    })
    .await;
    for node in &nodes {
        let from = events
            .iter()
            .find_map(|e| match e {
                FeedEvent::Message {
                    node: from,
                    payload,
                    ..
                } if *payload == node.label() => Some(from.clone()),
                _ => None,
            })
            .unwrap();
        assert_eq!(from, Some(node.label()));
    }
    drop(feed);
    all_closed(&nodes.iter().collect::<Vec<_>>(), baseline).await;
    for node in &nodes {
        assert_eq!(node.count("PSUBSCRIBE"), 1);
        assert_eq!(node.publish("__keyevent@0__:set", "late"), 0);
    }
}

#[tokio::test]
async fn dropping_a_merged_monitor_while_it_waits_to_reconnect_leaves_nothing_behind() {
    let nodes = feed_cluster(3);
    let client = Client::connect(nodes[0].profile(Deployment::Cluster))
        .await
        .unwrap();
    let baseline = alive_tasks();
    let mut feed = client.monitor_feed().await.unwrap();
    assert_eq!(feed.nodes(), 3);
    eventually("MONITOR on every node", 5, || {
        nodes.iter().all(|n| n.feeds_open() == 1)
    })
    .await;
    nodes[1].kill_feeds();
    events_until(&mut feed, |seen| {
        seen.iter()
            .any(is_notice(format!("Lost node {}", nodes[1].label())))
    })
    .await;
    // The feed now waits before it looks for the node again.
    drop(feed);
    all_closed(&nodes.iter().collect::<Vec<_>>(), baseline).await;
    assert_eq!(nodes[0].count("MONITOR"), 1);
    assert_eq!(nodes[2].count("MONITOR"), 1);
}

#[tokio::test]
async fn dropping_a_cluster_channel_feed_while_it_reconnects_leaves_nothing_behind() {
    let nodes = feed_cluster(3);
    let client = Client::connect(nodes[0].profile(Deployment::Cluster))
        .await
        .unwrap();
    let baseline = alive_tasks();
    let mut feed = client.subscribe(vec!["*".into()], false).await.unwrap();
    assert_eq!(feed.nodes(), 1);
    eventually("the one subscription", 5, || nodes[0].feeds_open() == 1).await;
    assert_eq!(nodes[1].feeds_open() + nodes[2].feeds_open(), 0);
    nodes[0].kill_feeds();
    events_until(&mut feed, |seen| {
        seen.iter().any(is_notice(format!(
            "Lost the connection to {}",
            nodes[0].label()
        )))
    })
    .await;
    drop(feed);
    all_closed(&nodes.iter().collect::<Vec<_>>(), baseline).await;
}

#[tokio::test]
async fn dropping_sentinel_feeds_closes_their_primary_connections_even_mid_reconnect() {
    let primary = FeedNode::start();
    let (sentinel, _) = sentinel_naming(Arc::new(AtomicUsize::new(primary.port as usize)));
    let client = sentinel_client(&sentinel).await;
    let baseline = alive_tasks();
    let mut feed = client.subscribe(vec!["*".into()], false).await.unwrap();
    let monitor = client.monitor_feed().await.unwrap();
    eventually("a subscriber and a monitor", 5, || {
        primary.feeds_open() == 2
    })
    .await;
    drop(monitor);
    eventually("the monitor to close", 5, || primary.feeds_open() == 1).await;
    primary.kill_feeds();
    events_until(&mut feed, |seen| {
        seen.iter().any(is_notice(format!(
            "Lost the connection to {}; waiting for Sentinel",
            primary.label()
        )))
    })
    .await;
    drop(feed);
    all_closed(&[&primary], baseline).await;
}

#[tokio::test]
async fn standalone_feeds_close_on_drop_and_end_when_the_server_lets_go() {
    let server = FeedNode::start();
    let client = Client::connect(server.profile(Deployment::Standalone))
        .await
        .unwrap();
    let baseline = alive_tasks();
    let feed = client.subscribe(vec!["*".into()], false).await.unwrap();
    let monitor = client.monitor_feed().await.unwrap();
    eventually("a subscriber and a monitor", 5, || server.feeds_open() == 2).await;
    drop((feed, monitor));
    all_closed(&[&server], baseline).await;

    // A standalone feed has nowhere else to go: it ends, and so does its task.
    let mut feed = client.subscribe(vec!["*".into()], false).await.unwrap();
    eventually("the subscriber", 5, || server.feeds_open() == 1).await;
    server.kill_feeds();
    let ended = tokio::time::timeout(Duration::from_secs(10), async {
        while feed.next().await.is_some() {}
    })
    .await;
    assert!(
        ended.is_ok(),
        "the feed kept running after the server closed it"
    );
    all_closed(&[&server], baseline).await;
    drop(feed);
    assert_eq!(server.count("PSUBSCRIBE"), 2);
}

#[tokio::test]
async fn quitting_the_feed_modal_closes_every_node_connection() {
    use crossterm::event::{KeyCode, KeyEvent};
    use rediscope::app::{App, Modal, Msg, Screen};
    let nodes = feed_cluster(3);
    let client = Client::connect(nodes[0].profile(Deployment::Cluster))
        .await
        .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = App::new(rediscope::config::Store::default(), tx);
    app.screen = Screen::Browser;
    app.client = Some(client);
    let baseline = alive_tasks();
    let every = nodes.iter().collect::<Vec<_>>();
    let press = |app: &mut App, c: char| app.on_key(KeyEvent::from(KeyCode::Char(c)));
    // `W` merges MONITOR from every primary, `N` keyspace events.
    for (open, monitor) in [('W', true), ('N', false)] {
        press(&mut app, open);
        assert!(
            matches!(&app.modal, Some(Modal::PubSub(s)) if s.monitor == monitor),
            "{open}: {}",
            app.status
        );
        eventually("a feed connection on every node", 5, || {
            nodes.iter().all(|n| n.feeds_open() == 1)
        })
        .await;
        press(&mut app, 'q');
        assert!(app.modal.is_none());
        all_closed(&every, baseline).await;
    }
    // Set aside behind the publish form and brought back, then quit.
    press(&mut app, 'N');
    eventually("keyspace subscriptions", 5, || {
        nodes.iter().all(|n| n.feeds_open() == 1)
    })
    .await;
    press(&mut app, 'w');
    assert!(matches!(&app.modal, Some(Modal::Form { .. })));
    app.on_key(KeyEvent::from(KeyCode::Esc));
    assert!(matches!(&app.modal, Some(Modal::PubSub(_))));
    assert!(nodes.iter().all(|n| n.feeds_open() == 1));
    press(&mut app, 'q');
    all_closed(&every, baseline).await;
    while let Ok(msg) = rx.try_recv() {
        if let Msg::Error(e) = msg {
            panic!("{e}");
        }
    }
}

// ---- reconnect pacing -------------------------------------------------------

#[tokio::test]
async fn a_default_node_that_drops_every_subscriber_is_not_hammered() {
    let nodes = feed_cluster(2);
    let client = Client::connect(nodes[0].profile(Deployment::Cluster))
        .await
        .unwrap();
    let mut feed = client.subscribe(vec!["*".into()], false).await.unwrap();
    eventually("the subscription", 5, || nodes[0].feeds_open() == 1).await;
    nodes[0]
        .state
        .drop_subscribers
        .store(true, Ordering::SeqCst);
    nodes[0].kill_feeds();
    let lost = format!(
        "Lost the connection to {}; reconnecting through another node",
        nodes[0].label()
    );
    let events = events_until(&mut feed, |seen| {
        seen.iter().filter(|e| is_notice(lost.clone())(e)).count() >= 4
    })
    .await;
    assert!(
        events.iter().all(|e| matches!(e, FeedEvent::Notice(_))),
        "{events:?}"
    );
    let attempts: Vec<Instant> = nodes
        .iter()
        .flat_map(|n| n.times("PSUBSCRIBE"))
        .skip(1)
        .collect();
    assert!(attempts.len() >= 3, "{attempts:?}");
    for pair in attempts.windows(2) {
        let gap = pair[1].saturating_duration_since(pair[0]);
        assert!(
            gap >= Duration::from_millis(200),
            "subscriptions {gap:?} apart: more than 5 a second"
        );
    }
}

#[tokio::test]
async fn a_sentinel_feed_backs_off_while_the_primary_is_unreachable_then_follows_it() {
    let first = FeedNode::start();
    let second = FeedNode::start();
    let target = Arc::new(AtomicUsize::new(first.port as usize));
    let (sentinel, asked) = sentinel_naming(target.clone());
    let client = sentinel_client(&sentinel).await;
    let mut feed = client.subscribe(vec!["*".into()], false).await.unwrap();
    eventually("the subscription", 5, || first.feeds_open() == 1).await;
    let before = asked.lock().unwrap().len();
    target.store(closed_port() as usize, Ordering::SeqCst);
    first.state.down.store(true, Ordering::SeqCst);
    first.kill_feeds();
    events_until(&mut feed, |seen| {
        seen.iter().any(is_notice(format!(
            "Lost the connection to {}; waiting for Sentinel to name the primary",
            first.label()
        )))
    })
    .await;
    eventually("four asks while the primary is unreachable", 15, || {
        asked.lock().unwrap().len() >= before + 4
    })
    .await;
    let times = asked.lock().unwrap()[before..before + 4].to_vec();
    for (i, pair) in times.windows(2).enumerate() {
        let gap = pair[1].saturating_duration_since(pair[0]);
        let floor = Duration::from_millis(500 << i) * 4 / 5;
        assert!(
            gap >= floor,
            "attempt {i}: {gap:?} after the last, under {floor:?}"
        );
    }
    target.store(second.port as usize, Ordering::SeqCst);
    events_until(&mut feed, |seen| {
        seen.iter().any(is_notice(format!(
            "Reconnected to the new primary {}",
            second.label()
        )))
    })
    .await;
    eventually("the subscription on the new primary", 5, || {
        second.feeds_open() == 1
    })
    .await;
    assert_eq!(second.publish("news", "after"), 1);
    events_until(&mut feed, |seen| {
        seen.iter().any(is_message("after".into()))
    })
    .await;
    assert_eq!(first.feeds_open(), 0);
}

/// A primary Sentinel demoted without closing its connections.
async fn quiet_failover() -> (FeedNode, FeedNode, Peer, Arc<AtomicUsize>, Client, Feed) {
    let first = FeedNode::start();
    let second = FeedNode::start();
    let target = Arc::new(AtomicUsize::new(first.port as usize));
    let (sentinel, _) = sentinel_naming(target.clone());
    let client = sentinel_client(&sentinel).await;
    let feed = client.subscribe(vec!["*".into()], false).await.unwrap();
    eventually("the subscription", 5, || first.feeds_open() == 1).await;
    target.store(second.port as usize, Ordering::SeqCst);
    (first, second, sentinel, target, client, feed)
}

async fn moves_within(feed: &mut Feed, first: &FeedNode, second: &FeedNode, limit: Duration) {
    let wanted = format!(
        "Sentinel now names {} as the primary instead of {}",
        second.label(),
        first.label()
    );
    let started = Instant::now();
    let mut seen = Vec::new();
    let moved = tokio::time::timeout(limit, async {
        while let Some(event) = feed.next().await {
            let hit = is_notice(wanted.clone())(&event);
            seen.push(event);
            if hit {
                return;
            }
        }
    })
    .await;
    assert!(
        moved.is_ok(),
        "the feed stayed on the demoted primary for {:?}: {seen:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn a_sentinel_feed_follows_a_quiet_failover_once_a_refresh_sees_it() {
    let (first, second, _sentinel, _target, client, mut feed) = quiet_failover().await;
    // Any write or refresh that asks Sentinel again updates the topology.
    assert_eq!(
        client.refresh_topology().await.unwrap()[0].port,
        second.port
    );
    moves_within(&mut feed, &first, &second, Duration::from_secs(8)).await;
    eventually("the subscription to move", 10, || {
        second.feeds_open() == 1 && first.feeds_open() == 0
    })
    .await;
    assert_eq!(second.publish("news", "moved"), 1);
    events_until(&mut feed, |seen| {
        seen.iter().any(is_message("moved".into()))
    })
    .await;
}

#[tokio::test]
async fn a_sentinel_feed_notices_a_quiet_failover_within_one_poll_on_its_own() {
    let (first, second, _sentinel, _target, _client, mut feed) = quiet_failover().await;
    // Nothing else talks to Sentinel. The feed checks every 5 seconds.
    moves_within(&mut feed, &first, &second, Duration::from_secs(8)).await;
}

#[tokio::test]
async fn a_merged_feed_streams_from_survivors_while_a_node_is_down_and_takes_it_back() {
    let nodes = feed_cluster(3);
    let client = Client::connect(nodes[0].profile(Deployment::Cluster))
        .await
        .unwrap();
    let mut feed = client
        .subscribe(vec!["__keyevent@0__:*".into()], true)
        .await
        .unwrap();
    eventually("a subscription on every node", 5, || {
        nodes.iter().all(|n| n.feeds_open() == 1)
    })
    .await;
    let down = &nodes[1];
    down.state.down.store(true, Ordering::SeqCst);
    down.kill_feeds();
    events_until(&mut feed, |seen| {
        seen.iter().any(is_notice(format!(
            "Lost node {}; the other 2 node(s) keep streaming",
            down.label()
        )))
    })
    .await;
    let accepted = down.accepted();
    // Through two attempts to reach the node again, the others keep streaming.
    for round in 0..2 {
        eventually("another attempt on the lost node", 15, || {
            down.accepted() > accepted + round
        })
        .await;
        for survivor in [&nodes[0], &nodes[2]] {
            let payload = format!("{}-{round}", survivor.label());
            assert_eq!(survivor.publish("__keyevent@0__:set", &payload), 1);
            events_until(&mut feed, |seen| {
                seen.iter().any(is_message(payload.clone()))
            })
            .await;
        }
    }
    let tries = down.state.accepted.lock().unwrap()[accepted..].to_vec();
    for pair in tries.windows(2) {
        let gap = pair[1].saturating_duration_since(pair[0]);
        assert!(gap >= Duration::from_secs(4), "retried {gap:?} apart");
    }
    down.state.down.store(false, Ordering::SeqCst);
    events_until(&mut feed, |seen| {
        seen.iter()
            .any(is_notice(format!("Following node {}", down.label())))
    })
    .await;
    eventually("the node back in the feed", 5, || down.feeds_open() == 1).await;
    assert_eq!(down.publish("__keyevent@0__:set", "back"), 1);
    let events = events_until(&mut feed, |seen| seen.iter().any(is_message("back".into()))).await;
    assert!(events.iter().any(|e| matches!(
        e,
        FeedEvent::Message { node: Some(n), payload, .. } if payload == "back" && *n == down.label()
    )));
    assert_eq!(nodes[0].count("PSUBSCRIBE"), 1);
    assert_eq!(nodes[2].count("PSUBSCRIBE"), 1);
}

// ---- merged MONITOR under a flood -------------------------------------------

/// What the monitor task handed the UI so far.
#[derive(Default)]
struct Tally {
    kept: usize,
    dropped: u64,
    notices: usize,
    biggest: usize,
}
impl Tally {
    /// Pass messages to `app` until `until` commands were kept or dropped.
    async fn collect(
        &mut self,
        app: &mut rediscope::app::App,
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<rediscope::app::Msg>,
        until: usize,
    ) {
        use rediscope::app::Msg;
        let deadline = Instant::now() + Duration::from_secs(60);
        while self.kept + (self.dropped as usize) < until {
            let left = deadline.saturating_duration_since(Instant::now());
            let msg = tokio::time::timeout(left, rx.recv())
                .await
                .expect("the monitor stopped delivering")
                .unwrap();
            match &msg {
                Msg::MonitorBatch { lines, dropped, .. } => {
                    self.biggest = self.biggest.max(lines.len());
                    self.kept += lines.len();
                    self.dropped += dropped;
                }
                Msg::FeedNotice { .. } => self.notices += 1,
                Msg::Error(e) => panic!("{e}"),
                _ => {}
            }
            app.on_msg(msg);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_merged_monitor_flood_is_capped_per_batch_counted_and_filterable() {
    use crossterm::event::{KeyCode, KeyEvent};
    use rediscope::app::{MONITOR_BATCH, MONITOR_FLUSH, Modal, Msg, PUBSUB_LIMIT, Screen};
    const PER_NODE: usize = 20_000;
    const TAIL: usize = 5;
    let nodes = feed_cluster(3);
    let client = Client::connect(nodes[0].profile(Deployment::Cluster))
        .await
        .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = rediscope::app::App::new(rediscope::config::Store::default(), tx);
    app.screen = Screen::Browser;
    app.client = Some(client);
    let started = Instant::now();
    app.on_key(KeyEvent::from(KeyCode::Char('W')));
    eventually("MONITOR on every node", 5, || {
        nodes.iter().all(|n| n.feeds_open() == 1)
    })
    .await;
    // Node `i` runs its commands in database `i`, on keys named after it.
    let flood = |i: usize, name: &str, count: usize| {
        let mut text = String::new();
        for j in 0..count {
            text.push_str(&format!(
                "+1718000000.{j:06} [{i} 127.0.0.1:5000] \"set\" \"{name}{i}-{j}\" \"v\"\r\n"
            ));
        }
        let streams = nodes[i].streams(FeedMode::Monitor);
        thread::spawn(move || {
            for mut s in streams {
                s.write_all(text.as_bytes()).unwrap();
            }
        })
    };
    let mut tally = Tally::default();
    let writers: Vec<_> = (0..3).map(|i| flood(i, "n", PER_NODE)).collect();
    tally.collect(&mut app, &mut rx, 3 * PER_NODE).await;
    for w in writers {
        w.join().unwrap();
    }
    let flooded = started.elapsed();
    for i in 0..3 {
        flood(i, "tail", TAIL).join().unwrap();
    }
    tally
        .collect(&mut app, &mut rx, 3 * (PER_NODE + TAIL))
        .await;
    let Tally {
        kept,
        dropped,
        notices,
        biggest,
    } = tally;

    assert!(biggest <= MONITOR_BATCH, "a batch of {biggest}");
    assert!(dropped > 0, "the flood never outran the cap");
    // Batches flush on the tick, and before a notice; the tick can never run
    // more often than its period allows.
    let ticks = flooded.as_millis() as usize / MONITOR_FLUSH.as_millis() as usize + 1;
    assert!(
        kept - 3 * TAIL <= MONITOR_BATCH * (ticks + notices + 1),
        "{kept} kept in {flooded:?}"
    );
    let Some(Modal::PubSub(state)) = &app.modal else {
        panic!("the monitor closed: {}", app.status);
    };
    assert_eq!(state.total, (3 * (PER_NODE + TAIL)) as u64);
    assert_eq!(state.dropped, dropped);
    assert!(state.messages.len() <= PUBSUB_LIMIT);
    for m in state.messages.iter().filter(|m| !m.notice) {
        let i = (0..3)
            .find(|i| {
                m.payload.contains(&format!("\"n{i}-"))
                    || m.payload.contains(&format!("\"tail{i}-"))
            })
            .unwrap_or_else(|| panic!("{}", m.payload));
        assert_eq!(m.node, Some(nodes[i].label()), "{}", m.payload);
        assert_eq!(m.db, Some(i as i64));
    }

    let text = {
        let mut terminal =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(200, 40)).unwrap();
        terminal.draw(|f| rediscope::ui::draw(f, &mut app)).unwrap();
        let buffer = terminal.backend().buffer().clone();
        buffer
            .content()
            .iter()
            .map(|c| c.symbol())
            .collect::<String>()
    };
    assert!(text.contains("too fast to show"), "{text}");

    // `d`: every database, then db0 (the profile's), db1, db2, and back.
    for filter in [Some(0), Some(1), Some(2), None] {
        app.on_key(KeyEvent::from(KeyCode::Char('d')));
        let Some(Modal::PubSub(state)) = &app.modal else {
            panic!("the monitor closed");
        };
        assert_eq!(state.db_filter, filter);
        let shown = state.shown();
        assert_eq!(shown.len(), state.shown_len());
        if let Some(db) = filter {
            assert!(shown.iter().all(|m| m.notice || m.db == Some(db)));
            let tail = format!("\"tail{db}-");
            assert_eq!(
                shown.iter().filter(|m| m.payload.contains(&tail)).count(),
                TAIL
            );
        } else {
            assert_eq!(shown.len(), state.messages.len());
        }
    }
}

// ---- routing invariants under concurrency -----------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retries_redirects_batches_discoveries_and_reads_at_once_keep_exact_counts() {
    const ROUNDS: usize = 10;
    const READS: usize = 5;
    let key = |low: bool, what: &str, round: usize| key_on(low, &format!("mix-{what}-"), round);
    let tried: Arc<Mutex<std::collections::HashSet<String>>> = Arc::default();
    let refused = tried.clone();
    let b_port = Arc::new(AtomicUsize::new(0));
    let b_seen = b_port.clone();
    let nodes = TwoNodes::start(move |node, _, args| {
        let b = b_seen.load(Ordering::SeqCst);
        let arg = args.get(1).cloned().unwrap_or_default();
        match (node, args[0].as_str()) {
            // Refused unrun once, then accepted.
            ('b', "SET") if arg.starts_with("mix-try-") => refused
                .lock()
                .unwrap()
                .insert(arg.clone())
                .then(|| Some("-TRYAGAIN rehashing\r\n".into())),
            ('a', "SET") if arg.starts_with("mix-moved-") => Some(Some(moved(&arg, b as u16))),
            ('a', "SET") if arg.starts_with("mix-ask-") => Some(Some(format!(
                "-ASK {} 127.0.0.1:{b}\r\n",
                key_slot(arg.as_bytes())
            ))),
            (_, "GET") => Some(Some(bulk("v"))),
            _ => None,
        }
    });
    b_port.store(nodes.b.port as usize, Ordering::SeqCst);
    let client = nodes.client().await;
    let mut tasks = tokio::task::JoinSet::new();
    for round in 0..ROUNDS {
        let c = client.clone();
        tasks.spawn(async move { c.set_string(&key(false, "try", round), "v").await });
        let c = client.clone();
        tasks.spawn(async move { c.set_string(&key(true, "moved", round), "v").await });
        let c = client.clone();
        tasks.spawn(async move { c.set_string(&key(true, "ask", round), "v").await });
        let c = client.clone();
        tasks.spawn(async move {
            c.delete_keys(&[key(true, "del", round), key(false, "del", round)])
                .await
                .map(|n| assert_eq!(n, 2))
        });
        let c = client.clone();
        tasks.spawn(async move { c.refresh_topology().await.map(|n| assert_eq!(n.len(), 2)) });
        for low in [true, false] {
            let c = client.clone();
            tasks.spawn(async move {
                for _ in 0..READS {
                    c.execute_raw(&format!("GET {}", key(low, "read", round)))
                        .await?;
                }
                Ok(())
            });
        }
    }
    while let Some(done) = tasks.join_next().await {
        done.unwrap().unwrap();
    }
    for round in 0..ROUNDS {
        let (a, b) = (&nodes.a_log, &nodes.b_log);
        let k = key(false, "try", round);
        assert_eq!(
            (count(a, "SET", Some(&k)), count(b, "SET", Some(&k))),
            (0, 2),
            "{k}"
        );
        let k = key(true, "moved", round);
        assert_eq!(
            (count(a, "SET", Some(&k)), count(b, "SET", Some(&k))),
            (1, 1),
            "{k}"
        );
        let k = key(true, "ask", round);
        assert_eq!(
            (count(a, "SET", Some(&k)), count(b, "SET", Some(&k))),
            (1, 1),
            "{k}"
        );
        for (low, log, other) in [(true, a, b), (false, b, a)] {
            let k = key(low, "del", round);
            assert_eq!(
                (
                    count(log, "UNLINK", Some(&k)),
                    count(other, "UNLINK", Some(&k))
                ),
                (1, 0),
                "{k}"
            );
            let k = key(low, "read", round);
            assert_eq!(
                (count(log, "GET", Some(&k)), count(other, "GET", Some(&k))),
                (READS, 0),
                "{k}"
            );
        }
    }
    assert_eq!(count(&nodes.b_log, "ASKING", None), ROUNDS);
    assert_eq!(count(&nodes.a_log, "ASKING", None), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_discovery_that_started_before_a_moved_leaves_at_most_one_more_redirect() {
    // `a` owns the low half and `b` the high half, and `a` keeps saying so.
    // Then the slot of `key` moves to `a`, and `b` redirects it there.
    let key = key_on(false, "migrating", 0);
    let slot = key_slot(key.as_bytes());
    let tagged = format!("{{{key}}}:again");
    let ports = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
    let (gate, asked, migrated) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicBool::new(false)),
    );
    let layout = {
        let ports = ports.clone();
        move || {
            slots(&[
                (0, 8191, ports[0].load(Ordering::SeqCst) as u16),
                (8192, 16383, ports[1].load(Ordering::SeqCst) as u16),
            ])
        }
    };
    let a_log: Log = Arc::default();
    let (seen, stale, held, asks) = (a_log.clone(), layout.clone(), gate.clone(), asked.clone());
    let a = Peer::start(move |id, args| match args[0].as_str() {
        "CLUSTER" => {
            asks.fetch_add(1, Ordering::SeqCst);
            let deadline = Instant::now() + Duration::from_secs(10);
            while held.load(Ordering::SeqCst) && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
            Some(stale())
        }
        _ => {
            seen.lock().unwrap().push((id, args.to_vec()));
            Some("+OK\r\n".into())
        }
    });
    let b_log: Log = Arc::default();
    let (seen, own, moving) = (b_log.clone(), ports.clone(), migrated.clone());
    let b = Peer::start(move |id, args| match args[0].as_str() {
        "CLUSTER" => Some(layout()),
        _ => {
            seen.lock().unwrap().push((id, args.to_vec()));
            Some(if moving.load(Ordering::SeqCst) {
                format!(
                    "-MOVED {slot} 127.0.0.1:{}\r\n",
                    own[0].load(Ordering::SeqCst)
                )
            } else {
                "+OK\r\n".into()
            })
        }
    });
    ports[0].store(a.port as usize, Ordering::SeqCst);
    ports[1].store(b.port as usize, Ordering::SeqCst);
    let client = Client::connect(a.profile(Deployment::Cluster))
        .await
        .unwrap();
    let redirects = || count(&b_log, "SET", None);

    // A discovery is under way when the MOVED arrives.
    gate.store(true, Ordering::SeqCst);
    let before = asked.load(Ordering::SeqCst);
    let refreshing = client.clone();
    let discovery = tokio::spawn(async move { refreshing.refresh_topology().await });
    while asked.load(Ordering::SeqCst) == before {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    migrated.store(true, Ordering::SeqCst);
    let writer = client.clone();
    let write_key = key.clone();
    let write = tokio::spawn(async move { writer.set_string(&write_key, "v").await });
    until_seen(&b_log, "SET").await;
    gate.store(false, Ordering::SeqCst);
    discovery.await.unwrap().unwrap();
    write.await.unwrap().unwrap();
    assert_eq!(count(&a_log, "SET", Some(&key)), 1);
    assert_eq!(redirects(), 1);

    // Both are done: the slot stays with its new owner.
    client.set_string(&tagged, "v").await.unwrap();
    assert_eq!(count(&a_log, "SET", Some(&tagged)), 1);
    assert_eq!(redirects(), 1, "the stale discovery undid the MOVED");

    // A later stale discovery costs one redirect, and the next write none.
    client.refresh_topology().await.unwrap();
    client.set_string(&tagged, "w").await.unwrap();
    client.set_string(&tagged, "x").await.unwrap();
    assert_eq!(count(&a_log, "SET", Some(&tagged)), 3);
    assert_eq!(redirects(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_connect_does_not_evict_the_socket_another_request_just_opened() {
    let nodes = feed_cluster(2);
    let client = Client::connect(nodes[0].profile(Deployment::Cluster))
        .await
        .unwrap();
    let key = |n| {
        (0..)
            .map(|i| format!("evict{i}"))
            .filter(|k| key_slot(k.as_bytes()) >= 8192)
            .nth(n)
            .unwrap()
    };
    let node = &nodes[1];
    assert_eq!(node.accepted(), 0, "nothing has talked to node 1 yet");
    // Node 1 holds its first connection without a word, then drops it.
    node.state.hold_first.store(true, Ordering::SeqCst);
    let first = client.clone();
    let k1 = key(0);
    let a = tokio::spawn(async move { first.execute_raw(&format!("GET {k1}")).await });
    eventually("request A's connection", 5, || node.accepted() == 1).await;
    // Meanwhile B connects and caches a socket of its own.
    client
        .execute_raw(&format!("GET {}", key(1)))
        .await
        .unwrap();
    let b_socket = node.conns_for("GET", &key(1));
    assert_eq!(b_socket.len(), 1);
    // A's connection fails without sending anything; A tries again.
    node.state.release_first.store(true, Ordering::SeqCst);
    a.await.unwrap().unwrap();
    client
        .execute_raw(&format!("GET {}", key(2)))
        .await
        .unwrap();
    assert_eq!(
        (
            node.conns_for("GET", &key(0)),
            node.conns_for("GET", &key(2))
        ),
        (b_socket.clone(), b_socket),
        "A's failed connect evicted B's socket, so later requests opened another ({} accepted)",
        node.accepted()
    );
    assert_eq!(node.accepted(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sentinel_writes_never_reach_the_old_primary_once_discovery_names_the_new_one() {
    let second = FeedNode::start();
    let demoted = Arc::new(AtomicBool::new(false));
    let first_log: Log = Arc::default();
    let (seen, refusing) = (first_log.clone(), demoted.clone());
    let first = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        Some(match args[0].as_str() {
            "ROLE" if refusing.load(Ordering::SeqCst) => "*1\r\n$5\r\nslave\r\n".into(),
            "ROLE" => "*1\r\n$6\r\nmaster\r\n".into(),
            "SET" if refusing.load(Ordering::SeqCst) => {
                "-READONLY You can't write against a read only replica.\r\n".into()
            }
            _ => "+OK\r\n".into(),
        })
    });
    let target = Arc::new(AtomicUsize::new(first.port as usize));
    let (gate, asked) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicUsize::new(0)),
    );
    let (named, held, asks) = (target.clone(), gate.clone(), asked.clone());
    let sentinel = Peer::start(move |_, args| match args[0].as_str() {
        "SENTINEL" => {
            asks.fetch_add(1, Ordering::SeqCst);
            let deadline = Instant::now() + Duration::from_secs(8);
            while held.load(Ordering::SeqCst) && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
            Some(format!(
                "*2\r\n{}{}",
                bulk("127.0.0.1"),
                bulk(&named.load(Ordering::SeqCst).to_string())
            ))
        }
        _ => Some("+OK\r\n".into()),
    });
    let client = sentinel_client(&sentinel).await;

    // Failover: the old primary refuses writes, and a discovery is held up
    // while writes pile into it.
    gate.store(true, Ordering::SeqCst);
    demoted.store(true, Ordering::SeqCst);
    target.store(second.port as usize, Ordering::SeqCst);
    let before = asked.load(Ordering::SeqCst);
    let refreshing = client.clone();
    let discovery = tokio::spawn(async move { refreshing.refresh_topology().await });
    while asked.load(Ordering::SeqCst) == before {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let during: Vec<String> = (0..10).map(|i| format!("during-{i}")).collect();
    let mut writes = tokio::task::JoinSet::new();
    for k in during.clone() {
        let c = client.clone();
        writes.spawn(async move { c.set_string(&k, "v").await });
    }
    eventually("every write refused by the old primary", 10, || {
        count(&first_log, "SET", None) == during.len()
    })
    .await;
    gate.store(false, Ordering::SeqCst);
    assert_eq!(discovery.await.unwrap().unwrap()[0].port, second.port);
    while let Some(done) = writes.join_next().await {
        done.unwrap().unwrap();
    }
    for k in &during {
        assert_eq!(count(&first_log, "SET", Some(k)), 1, "{k}");
        assert_eq!(second.conns_for("SET", k).len(), 1, "{k}");
    }

    // After discovery returned the new primary, writes and more discoveries
    // at once: nothing goes to the old one.
    let after: Vec<String> = (0..30).map(|i| format!("after-{i}")).collect();
    let mut tasks = tokio::task::JoinSet::new();
    for (i, k) in after.clone().into_iter().enumerate() {
        let c = client.clone();
        tasks.spawn(async move { c.set_string(&k, "v").await });
        if i % 6 == 0 {
            let c = client.clone();
            tasks.spawn(async move { c.refresh_topology().await.map(|_| ()) });
        }
    }
    while let Some(done) = tasks.join_next().await {
        done.unwrap().unwrap();
    }
    for k in &after {
        assert_eq!(count(&first_log, "SET", Some(k)), 0, "{k}");
        assert_eq!(second.conns_for("SET", k).len(), 1, "{k}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fifty_mixed_requests_against_a_flapping_node_apply_each_write_at_most_once() {
    const REQUESTS: usize = 50;
    // Every 7th command `b` receives drops the connection instead of running.
    let received = Arc::new(AtomicUsize::new(0));
    let lost: Arc<Mutex<Vec<String>>> = Arc::default();
    let (seen, cut) = (received.clone(), lost.clone());
    let nodes = TwoNodes::start(move |node, _, args| {
        if node != 'b' {
            return (args[0] == "GET").then(|| Some(bulk("v")));
        }
        if seen.fetch_add(1, Ordering::SeqCst) % 7 == 6 {
            cut.lock()
                .unwrap()
                .push(args.get(1).cloned().unwrap_or_default());
            return Some(None);
        }
        (args[0] == "GET").then(|| Some(bulk("v")))
    });
    let mut profile = nodes.profile();
    profile.name = unique("flapping", nodes.a.port);
    let client = Client::connect(profile.clone()).await.unwrap();
    client.idle_ping_after(Duration::ZERO);
    let mut tasks = tokio::task::JoinSet::new();
    let mut writes = Vec::new();
    for i in 0..REQUESTS {
        let low = i % 4 < 2;
        let k = key_on(low, &format!("flap-{}-", nodes.a.port), i);
        let c = client.clone();
        if i % 2 == 0 {
            writes.push((low, k.clone()));
            tasks.spawn(async move { (k.clone(), true, c.set_string(&k, "v").await.map(|_| ())) });
        } else {
            tasks.spawn(async move {
                let read = c.execute_raw(&format!("GET {k}")).await.map(|_| ());
                (k, false, read)
            });
        }
    }
    let mut outcomes = std::collections::HashMap::new();
    while let Some(done) = tasks.join_next().await {
        let (k, write, result) = done.expect("a request panicked");
        if write {
            outcomes.insert(k, result);
        }
    }
    let lost = lost.lock().unwrap().clone();
    for (low, k) in &writes {
        let (owner, other) = if *low {
            (&nodes.a_log, &nodes.b_log)
        } else {
            (&nodes.b_log, &nodes.a_log)
        };
        assert_eq!(
            count(other, "SET", Some(k)),
            0,
            "{k} reached the wrong node"
        );
        let received = count(owner, "SET", Some(k));
        assert!(received <= 1, "{k} was sent {received} times");
        let applied = received - lost.iter().filter(|l| *l == k).count();
        match &outcomes[k] {
            Ok(()) => assert_eq!(applied, 1, "{k}"),
            // Run, but its reply lost with the connection: reported unknown.
            Err(e) if e.to_string().contains("outcome unknown") => {}
            Err(e) => assert_eq!(applied, 0, "{k} failed but ran: {e}"),
        }
    }
    // One started event and one outcome per write, under one operation id.
    let path = std::env::var_os("REDISCOPE_AUDIT_FILE").unwrap();
    let mut ops: std::collections::HashMap<u64, Vec<String>> = Default::default();
    for event in std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
        .filter(|e| e["profile"] == profile.name && e["action"] == "SET")
    {
        ops.entry(event["operation_id"].as_u64().unwrap())
            .or_default()
            .push(event["outcome"].as_str().unwrap().to_string());
    }
    assert_eq!(ops.len(), writes.len(), "{ops:?}");
    let succeeded = outcomes.values().filter(|r| r.is_ok()).count();
    for events in ops.values() {
        assert_eq!(events.len(), 2, "{events:?}");
        assert_eq!(events[0], "started");
    }
    assert_eq!(
        ops.values().filter(|e| e[1] == "success").count(),
        succeeded
    );
}

// ---- idle PING before a write ---------------------------------------------

/// A standalone server logging each command with its connection. Its
/// `cut`th `PING` (counting the one at connect) closes the connection, and
/// every `PING` waits `slow` first.
fn pinging_server(cut: Option<usize>, slow: Duration) -> (Peer, Log) {
    let log: Log = Arc::default();
    let seen = log.clone();
    let pings = AtomicUsize::new(0);
    let peer = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        match args[0].as_str() {
            "PING" if Some(pings.fetch_add(1, Ordering::SeqCst) + 1) == cut => None,
            "PING" => {
                thread::sleep(slow);
                Some("+PONG\r\n".into())
            }
            "UNLINK" => Some(":1\r\n".into()),
            "GET" => Some(bulk("v")),
            _ => Some("+OK\r\n".into()),
        }
    });
    (peer, log)
}

#[tokio::test]
async fn an_idle_ping_that_loses_the_connection_sends_the_write_once_on_a_new_one() {
    // The first PING is discovery's at connect; the second is the idle check.
    let (server, log) = pinging_server(Some(2), Duration::ZERO);
    let client = Client::connect(server.profile(Deployment::Standalone))
        .await
        .unwrap();
    client.idle_ping_after(Duration::ZERO);
    client.execute_raw("GET key").await.unwrap();
    let socket = sockets_for(&log, "GET")[0];
    assert_eq!(sockets_for(&log, "PING"), vec![socket], "reads do not PING");
    client.set_string("key", "v").await.unwrap();
    assert_eq!(
        sockets_for(&log, "PING"),
        vec![socket, socket],
        "one PING, on the idle socket"
    );
    let sets = sockets_for(&log, "SET");
    assert_eq!(sets.len(), 1, "the write went out exactly once");
    assert_ne!(sets[0], socket);
    // The new socket is the one kept.
    client.execute_raw("GET key").await.unwrap();
    assert_eq!(sockets_for(&log, "GET"), vec![socket, sets[0]]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_sharing_an_idle_socket_during_its_ping_ping_once_and_keep_it() {
    let (server, log) = pinging_server(None, Duration::from_millis(200));
    let client = Client::connect(server.profile(Deployment::Standalone))
        .await
        .unwrap();
    client.execute_raw("GET key").await.unwrap();
    let socket = sockets_for(&log, "GET")[0];
    let at_connect = sockets_for(&log, "PING").len();
    client.idle_ping_after(Duration::from_millis(300));
    // Idle for longer than the threshold.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let keys: Vec<String> = (0..10).map(|i| format!("shared-{i}")).collect();
    let results =
        futures_util::future::join_all(keys.iter().map(|k| client.set_string(k, "v"))).await;
    assert!(results.iter().all(Result::is_ok), "{results:?}");
    assert_eq!(
        sockets_for(&log, "PING")[at_connect..],
        [socket],
        "{:?}",
        heads(&log)
    );
    assert_eq!(sockets_for(&log, "SET"), vec![socket; keys.len()]);
    client.execute_raw("GET key").await.unwrap();
    assert_eq!(sockets_for(&log, "GET"), vec![socket, socket]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_queued_behind_a_ping_that_kills_the_socket_are_unknown_and_never_resent() {
    let (server, log) = pinging_server(Some(2), Duration::ZERO);
    let client = Client::connect(server.profile(Deployment::Standalone))
        .await
        .unwrap();
    client.execute_raw("GET key").await.unwrap();
    let socket = sockets_for(&log, "GET")[0];
    client.idle_ping_after(Duration::from_millis(300));
    tokio::time::sleep(Duration::from_millis(400)).await;
    let keys: Vec<String> = (0..10).map(|i| format!("queued-{i}")).collect();
    let results =
        futures_util::future::join_all(keys.iter().map(|k| client.set_string(k, "v"))).await;
    assert_eq!(
        sockets_for(&log, "PING"),
        vec![socket, socket],
        "one idle PING"
    );
    // The write that pinged goes out once on a new socket. A write that had
    // already gone out on the socket the PING lost is unknown and not resent;
    // the rest share the new socket.
    let fresh = sockets_for(&log, "SET");
    assert!(
        !fresh.is_empty() && fresh.iter().all(|s| *s == fresh[0] && *s != socket),
        "{fresh:?}"
    );
    for (k, result) in keys.iter().zip(&results) {
        let sent = sockets_for_key(&log, "SET", k).len();
        match result {
            Ok(()) => assert_eq!(sent, 1, "{k}"),
            Err(e) => {
                assert!(e.to_string().contains("outcome unknown"), "{k}: {e}");
                assert_eq!(sent, 0, "{k}");
            }
        }
    }
    // Those failures did not throw away the socket the pinging write opened.
    client.execute_raw("GET key").await.unwrap();
    assert_eq!(sockets_for(&log, "GET"), vec![socket, fresh[0]]);
}

fn sockets_for_key(log: &Log, head: &str, key: &str) -> Vec<usize> {
    log.lock()
        .unwrap()
        .iter()
        .filter(|(_, args)| args[0] == head && args.get(1).is_some_and(|a| a == key))
        .map(|(id, _)| *id)
        .collect()
}

#[tokio::test]
async fn reads_never_ping_and_batches_ping_once_per_node_socket() {
    let nodes = TwoNodes::start(|_, _, args| (args[0] == "GET").then(|| Some(bulk("v"))));
    let client = nodes.client().await;
    client.idle_ping_after(Duration::ZERO);
    let (ka, kb) = (
        key_on(true, "idle-batch", 0),
        key_on(false, "idle-batch", 0),
    );
    for _ in 0..3 {
        client.execute_raw(&format!("GET {ka}")).await.unwrap();
        client.execute_raw(&format!("GET {kb}")).await.unwrap();
    }
    assert_eq!(
        count(&nodes.a_log, "PING", None) + count(&nodes.b_log, "PING", None),
        0
    );
    let names: Vec<String> = (0..6)
        .map(|i| key_on(i % 2 == 0, "idle-batch-del", i))
        .collect();
    assert_eq!(client.delete_keys(&names).await.unwrap(), 6);
    assert_eq!(count(&nodes.a_log, "PING", None), 1);
    assert_eq!(count(&nodes.b_log, "PING", None), 1);
    for log in [&nodes.a_log, &nodes.b_log] {
        let h = heads(log);
        let ping = h.iter().position(|x| x == "PING").unwrap();
        assert!(h[ping + 1..].iter().all(|x| x == "UNLINK"), "{h:?}");
    }

    // Standalone: one PING per chunk of the batch, each before its chunk.
    let (server, log) = pinging_server(None, Duration::ZERO);
    let client = Client::connect(server.profile(Deployment::Standalone))
        .await
        .unwrap();
    client.idle_ping_after(Duration::ZERO);
    let at_connect = heads(&log).len();
    let names: Vec<String> = (0..600).map(|i| format!("chunked-{i}")).collect();
    assert_eq!(client.delete_keys(&names).await.unwrap(), 600);
    let h = heads(&log)[at_connect..].to_vec();
    let pings: Vec<usize> = h
        .iter()
        .enumerate()
        .filter(|(_, x)| *x == "PING")
        .map(|(i, _)| i)
        .collect();
    assert_eq!(pings.len(), 3, "{h:?}");
    for (chunk, at) in pings.iter().enumerate() {
        let unlinks = h[at + 1..].iter().take_while(|x| *x == "UNLINK").count();
        assert_eq!(unlinks, [256, 256, 88][chunk]);
    }
}

// ---- discovery generations, Sentinel staleness, shared sockets ----------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_lost_during_a_discovery_that_started_before_it_discovers_again() {
    // `a` owns the low slots and is the seed; its CLUSTER SLOTS waits while
    // `gate` is set. `b` owns the high slots and drops the connection on SET.
    let (gate, asked) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicUsize::new(0)),
    );
    let ports = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
    let layout = {
        let ports = ports.clone();
        move || {
            slots(&[
                (0, 8191, ports[0].load(Ordering::SeqCst) as u16),
                (8192, 16383, ports[1].load(Ordering::SeqCst) as u16),
            ])
        }
    };
    let (held, asks, table) = (gate.clone(), asked.clone(), layout.clone());
    let a = Peer::start(move |_, args| match args[0].as_str() {
        "CLUSTER" => {
            asks.fetch_add(1, Ordering::SeqCst);
            let deadline = Instant::now() + Duration::from_secs(10);
            while held.load(Ordering::SeqCst) && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
            Some(table())
        }
        _ => Some("+OK\r\n".into()),
    });
    let log: Log = Arc::default();
    let seen = log.clone();
    let b = Peer::start(move |id, args| match args[0].as_str() {
        "CLUSTER" => Some(layout()),
        "SET" => {
            seen.lock().unwrap().push((id, args.to_vec()));
            None
        }
        _ => Some("+OK\r\n".into()),
    });
    ports[0].store(a.port as usize, Ordering::SeqCst);
    ports[1].store(b.port as usize, Ordering::SeqCst);
    let client = Client::connect(a.profile(Deployment::Cluster))
        .await
        .unwrap();
    let key = key_on(false, "lost-during-discovery", 0);

    gate.store(true, Ordering::SeqCst);
    let before = asked.load(Ordering::SeqCst);
    let refreshing = client.clone();
    let discovery = tokio::spawn(async move { refreshing.refresh_topology().await });
    while asked.load(Ordering::SeqCst) == before {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    // The discovery has asked. Now a write is lost, and waits to rediscover.
    let writer = client.clone();
    let write_key = key.clone();
    let write = tokio::spawn(async move { writer.set_string(&write_key, "v").await });
    until_seen(&log, "SET").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!write.is_finished(), "the write did not wait for discovery");
    gate.store(false, Ordering::SeqCst);
    discovery.await.unwrap().unwrap();
    let err = write.await.unwrap().unwrap_err().to_string();
    assert!(err.contains("outcome unknown"), "{err}");
    assert_eq!(count(&log, "SET", Some(&key)), 1);
    assert_eq!(
        asked.load(Ordering::SeqCst) - before,
        2,
        "the lost write took the result of a discovery that asked before it failed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_a_failed_sentinel_discovery_the_next_write_asks_again_and_never_uses_the_old_address()
 {
    let first = FeedNode::start();
    let second = FeedNode::start();
    let target = Arc::new(AtomicUsize::new(first.port as usize));
    let broken = Arc::new(AtomicBool::new(false));
    let asked = Arc::new(AtomicUsize::new(0));
    let (named, failing, asks) = (target.clone(), broken.clone(), asked.clone());
    let sentinel = Peer::start(move |_, args| match args[0].as_str() {
        "SENTINEL" => {
            asks.fetch_add(1, Ordering::SeqCst);
            if failing.load(Ordering::SeqCst) {
                return None;
            }
            Some(format!(
                "*2\r\n{}{}",
                bulk("127.0.0.1"),
                bulk(&named.load(Ordering::SeqCst).to_string())
            ))
        }
        _ => Some("+OK\r\n".into()),
    });
    let client = sentinel_client(&sentinel).await;
    client.set_string("before", "v").await.unwrap();
    assert_eq!(first.conns_for("SET", "before").len(), 1);

    // Sentinel stops answering, and meanwhile the primary moves.
    broken.store(true, Ordering::SeqCst);
    target.store(second.port as usize, Ordering::SeqCst);
    assert!(client.refresh_topology().await.is_err());
    let at = asked.load(Ordering::SeqCst);
    let err = client.set_string("during", "v").await.unwrap_err();
    assert!(
        asked.load(Ordering::SeqCst) > at,
        "the write did not ask Sentinel again: {err}"
    );
    assert!(first.conns_for("SET", "during").is_empty());
    assert!(second.conns_for("SET", "during").is_empty());

    // Sentinel answers again: the next write goes to the primary it names.
    broken.store(false, Ordering::SeqCst);
    client.set_string("after", "v").await.unwrap();
    assert!(first.conns_for("SET", "after").is_empty());
    assert_eq!(second.conns_for("SET", "after").len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sentinel_rediscovery_keeps_the_primary_socket_until_the_primary_changes() {
    let first = FeedNode::start();
    let second = FeedNode::start();
    let target = Arc::new(AtomicUsize::new(first.port as usize));
    let (sentinel, _) = sentinel_naming(target.clone());
    let client = sentinel_client(&sentinel).await;
    client.set_string("one", "v").await.unwrap();
    for _ in 0..3 {
        client.refresh_topology().await.unwrap();
    }
    client.set_string("two", "v").await.unwrap();
    let socket = first.conns_for("SET", "one");
    assert_eq!(socket.len(), 1);
    assert_eq!(
        first.conns_for("SET", "two"),
        socket,
        "a discovery that found the same primary replaced its socket"
    );
    // Every ROLE check went over that socket too.
    let roles: Vec<usize> = first
        .state
        .log
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, _, args)| args[0] == "ROLE")
        .map(|(_, id, _)| *id)
        .collect();
    assert_eq!(roles.len(), 4, "{roles:?}");
    assert!(roles.iter().all(|id| *id == socket[0]), "{roles:?}");

    target.store(second.port as usize, Ordering::SeqCst);
    client.refresh_topology().await.unwrap();
    client.set_string("three", "v").await.unwrap();
    assert_eq!(second.conns_for("SET", "three").len(), 1);
    assert!(first.conns_for("SET", "three").is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_writes_on_an_idle_dead_socket_both_land_once_on_one_new_socket() {
    let (server, log) = cutting_server();
    let client = Client::connect(server.profile(Deployment::Standalone))
        .await
        .unwrap();
    client.idle_ping_after(Duration::from_millis(300));
    client.execute_raw("GET drop").await.unwrap();
    let old = sockets_for(&log, "GET")[0];
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (a, b) = tokio::join!(client.set_string("a", "v"), client.set_string("b", "v"));
    assert!(a.is_ok() && b.is_ok(), "{a:?} {b:?}");
    let a = sockets_for_key(&log, "SET", "a");
    let b = sockets_for_key(&log, "SET", "b");
    assert_eq!(a.len(), 1, "{:?}", heads(&log));
    assert_eq!(a, b, "the writes used different sockets");
    assert_ne!(a[0], old);
}

// ---- merged feeds follow the set of primaries ------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merged_feeds_add_new_primaries_and_drop_demoted_ones() {
    let nodes: Vec<FeedNode> = (0..3).map(|_| FeedNode::start()).collect();
    let layout = |members: &[usize]| {
        let ranges: Vec<(u16, u16, u16)> = members
            .iter()
            .enumerate()
            .map(|(i, &m)| slot_range(members.len(), i, nodes[m].port))
            .collect();
        for node in &nodes {
            node.set_slots(slots(&ranges));
        }
    };
    layout(&[0, 1]);
    let client = Client::connect(nodes[0].profile(Deployment::Cluster))
        .await
        .unwrap();
    client.feed_check_every(Duration::from_millis(300));
    // Resharded onto a third node before any feed starts.
    layout(&[0, 1, 2]);
    let every = nodes.iter().collect::<Vec<_>>();
    let baseline = alive_tasks();
    for monitor in [false, true] {
        let mut feed = if monitor {
            client.monitor_feed().await.unwrap()
        } else {
            client
                .subscribe(vec!["__keyevent@0__:*".into()], true)
                .await
                .unwrap()
        };
        assert_eq!(feed.nodes(), 3, "monitor: {monitor}");
        eventually("a feed connection on every node", 5, || {
            nodes.iter().all(|n| n.feeds_open() == 1)
        })
        .await;

        // Node 1 is demoted.
        layout(&[0, 2]);
        events_until(&mut feed, |seen| {
            seen.iter().any(is_notice(format!(
                "Node {} is no longer a primary; stopped following it",
                nodes[1].label()
            )))
        })
        .await;
        eventually("the demoted node's connection to close", 5, || {
            nodes[1].feeds_open() == 0
        })
        .await;

        // And promoted again.
        layout(&[0, 1, 2]);
        events_until(&mut feed, |seen| {
            seen.iter()
                .any(is_notice(format!("Following node {}", nodes[1].label())))
        })
        .await;
        eventually("the promoted node back in the feed", 5, || {
            nodes[1].feeds_open() == 1
        })
        .await;
        if !monitor {
            assert_eq!(nodes[1].publish("__keyevent@0__:set", "promoted"), 1);
            let events = events_until(&mut feed, |seen| {
                seen.iter().any(is_message("promoted".into()))
            })
            .await;
            assert!(events.iter().any(|e| matches!(
                e,
                FeedEvent::Message { node: Some(n), payload, .. }
                    if payload == "promoted" && *n == nodes[1].label()
            )));
        }
        drop(feed);
        all_closed(&every, baseline).await;
    }
}

#[tokio::test]
async fn a_node_that_drops_every_subscriber_is_retried_less_and_less_often() {
    let nodes = feed_cluster(2);
    let client = Client::connect(nodes[0].profile(Deployment::Cluster))
        .await
        .unwrap();
    let _feed = client.subscribe(vec!["*".into()], false).await.unwrap();
    eventually("the subscription", 5, || nodes[0].feeds_open() == 1).await;
    nodes[0]
        .state
        .drop_subscribers
        .store(true, Ordering::SeqCst);
    nodes[0].kill_feeds();
    eventually("four more subscriptions", 15, || {
        nodes[0].count("PSUBSCRIBE") >= 5
    })
    .await;
    let attempts = nodes[0].times("PSUBSCRIBE")[1..5].to_vec();
    for (i, pair) in attempts.windows(2).enumerate() {
        let gap = pair[1].saturating_duration_since(pair[0]);
        let floor = Duration::from_millis(500 << i) * 4 / 5;
        assert!(
            gap >= floor,
            "attempt {i}: {gap:?} after the last, under {floor:?}"
        );
    }
}

// ---- a pub/sub flood through the app ---------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pubsub_flood_is_capped_per_batch_and_every_message_is_counted() {
    use crossterm::event::{KeyCode, KeyEvent};
    use rediscope::app::{MONITOR_BATCH, Modal, Msg, PUBSUB_LIMIT, Screen};
    const PER_NODE: usize = 20_000;
    let nodes = feed_cluster(3);
    let client = Client::connect(nodes[0].profile(Deployment::Cluster))
        .await
        .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut app = rediscope::app::App::new(rediscope::config::Store::default(), tx);
    app.screen = Screen::Browser;
    app.client = Some(client);
    app.on_key(KeyEvent::from(KeyCode::Char('N')));
    eventually("a subscription on every node", 5, || {
        nodes.iter().all(|n| n.feeds_open() == 1)
    })
    .await;
    let writers: Vec<_> = (0..3)
        .map(|i| {
            let mut text = String::new();
            for j in 0..PER_NODE {
                text.push_str(&pmessage(
                    "__keyevent@0__:*",
                    &format!("__keyevent@0__:set{i}"),
                    &format!("n{i}-{j}"),
                ));
            }
            let streams = nodes[i].streams(FeedMode::PubSub);
            thread::spawn(move || {
                for mut s in streams {
                    s.write_all(text.as_bytes()).unwrap();
                }
            })
        })
        .collect();
    let (mut kept, mut dropped, mut biggest) = (0usize, 0u64, 0usize);
    let deadline = Instant::now() + Duration::from_secs(60);
    while kept + (dropped as usize) < 3 * PER_NODE {
        let left = deadline.saturating_duration_since(Instant::now());
        let msg = tokio::time::timeout(left, rx.recv())
            .await
            .expect("the feed stopped delivering")
            .unwrap();
        match &msg {
            Msg::PubSubBatch {
                messages,
                dropped: lost,
                ..
            } => {
                biggest = biggest.max(messages.len());
                kept += messages.len();
                dropped += lost.iter().map(|(_, n)| n).sum::<u64>();
            }
            Msg::Error(e) => panic!("{e}"),
            _ => {}
        }
        app.on_msg(msg);
    }
    for w in writers {
        w.join().unwrap();
    }
    assert!(biggest <= MONITOR_BATCH, "a batch of {biggest}");
    assert!(dropped > 0, "the flood never outran the cap");
    let Some(Modal::PubSub(state)) = &app.modal else {
        panic!("the feed closed: {}", app.status);
    };
    assert_eq!(state.total, (3 * PER_NODE) as u64);
    assert_ne!(state.feed, 0, "the feed was not numbered");
    assert_eq!(state.dropped, dropped);
    assert!(state.messages.len() <= PUBSUB_LIMIT);
    let per_channel: std::collections::HashMap<&str, u64> = state
        .channels
        .iter()
        .map(|(c, n)| (c.as_str(), *n))
        .collect();
    for i in 0..3 {
        assert_eq!(
            per_channel[format!("__keyevent@0__:set{i}").as_str()],
            PER_NODE as u64
        );
    }
    let text = {
        let mut terminal =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(200, 40)).unwrap();
        terminal.draw(|f| rediscope::ui::draw(f, &mut app)).unwrap();
        let buffer = terminal.backend().buffer().clone();
        buffer
            .content()
            .iter()
            .map(|c| c.symbol())
            .collect::<String>()
    };
    assert!(text.contains("too fast to show"), "{text}");
}

// ---- node-addressed changes and lost writes during a failover ------------

/// A node that logs every command and answers like a primary.
fn primary_peer() -> (Peer, Log) {
    let log: Log = Arc::default();
    let seen = log.clone();
    let peer = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        Some(match args[0].as_str() {
            "ROLE" => "*1\r\n$6\r\nmaster\r\n".into(),
            "SLOWLOG" | "CONFIG" if args.get(1).is_some_and(|a| a == "GET") => "*0\r\n".into(),
            "CLIENT" if args.get(1).is_some_and(|a| a == "LIST") => bulk(""),
            _ => "+OK\r\n".into(),
        })
    });
    (peer, log)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sentinel_node_changes_after_a_failed_discovery_ask_again_and_never_reach_the_old_address()
{
    let (first, first_log) = primary_peer();
    let (second, second_log) = primary_peer();
    let target = Arc::new(AtomicUsize::new(first.port as usize));
    let broken = Arc::new(AtomicBool::new(false));
    let asked = Arc::new(AtomicUsize::new(0));
    let (named, failing, asks) = (target.clone(), broken.clone(), asked.clone());
    let sentinel = Peer::start(move |_, args| match args[0].as_str() {
        "SENTINEL" => {
            asks.fetch_add(1, Ordering::SeqCst);
            if failing.load(Ordering::SeqCst) {
                return None;
            }
            Some(format!(
                "*2\r\n{}{}",
                bulk("127.0.0.1"),
                bulk(&named.load(Ordering::SeqCst).to_string())
            ))
        }
        _ => Some("+OK\r\n".into()),
    });
    let client = sentinel_client(&sentinel).await;
    let old = Some(("127.0.0.1".to_string(), first.port));
    assert_eq!(client.diagnostics().await.unwrap().node, old);

    // Sentinel stops answering, and meanwhile the primary moves.
    broken.store(true, Ordering::SeqCst);
    target.store(second.port as usize, Ordering::SeqCst);
    assert!(client.refresh_topology().await.is_err());
    let changes = |log: &Log| {
        count(log, "CLIENT", Some("KILL"))
            + count(log, "CONFIG", Some("SET"))
            + count(log, "SLOWLOG", Some("RESET"))
    };
    let at = asked.load(Ordering::SeqCst);
    assert!(client.client_kill_on(&old, "5").await.is_err());
    assert!(
        asked.load(Ordering::SeqCst) > at,
        "no rediscovery was tried"
    );
    assert!(client.config_set_on(&old, "maxmemory", "1").await.is_err());
    assert!(client.slowlog_reset_on(&old).await.is_err());
    assert_eq!(changes(&first_log), 0, "a change reached the stale address");
    assert_eq!(changes(&second_log), 0);

    // Diagnostics say why instead of reading the stale address.
    let reads = first_log.lock().unwrap().len();
    let d = client.diagnostics().await.unwrap();
    assert_eq!(d.node, None);
    let error = d
        .cluster
        .iter()
        .find(|(k, _)| k == "diagnostics_error")
        .map(|(_, v)| v.clone())
        .expect("the discovery failure is shown");
    assert!(error.contains("Discovery failed"), "{error}");
    assert_eq!(first_log.lock().unwrap().len(), reads);

    // Sentinel answers again: a change addressed to a node goes to that node.
    broken.store(false, Ordering::SeqCst);
    client.client_kill_on(&old, "5").await.unwrap();
    assert_eq!(count(&first_log, "CLIENT", Some("KILL")), 1);
    assert_eq!(
        client.diagnostics().await.unwrap().node,
        Some(("127.0.0.1".to_string(), second.port))
    );
}

/// A Sentinel whose answer waits while `gate` is set, counting every ask.
fn held_sentinel(target: Arc<AtomicUsize>, gate: Arc<AtomicBool>, asked: Arc<AtomicUsize>) -> Peer {
    Peer::start(move |_, args| match args[0].as_str() {
        "SENTINEL" => {
            asked.fetch_add(1, Ordering::SeqCst);
            let deadline = Instant::now() + Duration::from_secs(8);
            while gate.load(Ordering::SeqCst) && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
            Some(format!(
                "*2\r\n{}{}",
                bulk("127.0.0.1"),
                bulk(&target.load(Ordering::SeqCst).to_string())
            ))
        }
        _ => Some("+OK\r\n".into()),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writers_arriving_while_a_lost_write_rediscovers_never_reach_the_old_primary() {
    let (second, second_log) = primary_peer();
    let target = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(AtomicBool::new(false));
    let asked = Arc::new(AtomicUsize::new(0));
    // The old primary loses the first write sent to it, names the new
    // primary, and holds Sentinel's next answer. It stays up and would take
    // any later write.
    let first_log: Log = Arc::default();
    let (seen, switch, hold) = (first_log.clone(), target.clone(), gate.clone());
    let next = second.port as usize;
    let first = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        match args[0].as_str() {
            "ROLE" => Some("*1\r\n$6\r\nmaster\r\n".into()),
            "SET" if args[1].starts_with("lost") => {
                hold.store(true, Ordering::SeqCst);
                switch.store(next, Ordering::SeqCst);
                None
            }
            _ => Some("+OK\r\n".into()),
        }
    });
    target.store(first.port as usize, Ordering::SeqCst);
    let sentinel = held_sentinel(target.clone(), gate.clone(), asked.clone());
    let client = sentinel_client(&sentinel).await;
    client.set_string("before", "v").await.unwrap();
    assert_eq!(count(&first_log, "SET", Some("before")), 1);

    // Writer A loses its write and starts rediscovering; B arrives meanwhile.
    let before = asked.load(Ordering::SeqCst);
    let a = client.clone();
    let lost = tokio::spawn(async move { a.set_string("lost-a", "v").await });
    eventually("A's rediscovery to ask Sentinel", 10, || {
        asked.load(Ordering::SeqCst) > before
    })
    .await;
    let b = client.clone();
    let arriving = tokio::spawn(async move { b.set_string("b", "v").await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        count(&first_log, "SET", Some("b")),
        0,
        "B was sent to the old primary while A rediscovered"
    );
    gate.store(false, Ordering::SeqCst);
    let e = lost.await.unwrap().unwrap_err().to_string();
    assert!(e.contains("outcome unknown"), "{e}");
    arriving.await.unwrap().unwrap();
    assert_eq!(count(&first_log, "SET", Some("b")), 0);
    assert_eq!(count(&second_log, "SET", Some("b")), 1);
    assert_eq!(count(&first_log, "SET", Some("lost-a")), 1);
    assert_eq!(count(&second_log, "SET", Some("lost-a")), 0, "resent");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_write_whose_task_is_cancelled_mid_rediscovery_still_moves_the_next_write() {
    let (second, second_log) = primary_peer();
    let target = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(AtomicBool::new(false));
    let asked = Arc::new(AtomicUsize::new(0));
    let first_log: Log = Arc::default();
    let (seen, switch, hold) = (first_log.clone(), target.clone(), gate.clone());
    let next = second.port as usize;
    let first = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        match args[0].as_str() {
            "ROLE" => Some("*1\r\n$6\r\nmaster\r\n".into()),
            "SET" if args[1] == "lost" => {
                hold.store(true, Ordering::SeqCst);
                switch.store(next, Ordering::SeqCst);
                None
            }
            _ => Some("+OK\r\n".into()),
        }
    });
    target.store(first.port as usize, Ordering::SeqCst);
    let sentinel = held_sentinel(target.clone(), gate.clone(), asked.clone());
    let client = sentinel_client(&sentinel).await;

    let before = asked.load(Ordering::SeqCst);
    let a = client.clone();
    let lost = tokio::spawn(async move { a.set_string("lost", "v").await });
    eventually("the rediscovery to ask Sentinel", 10, || {
        asked.load(Ordering::SeqCst) > before
    })
    .await;
    lost.abort();
    assert!(lost.await.unwrap_err().is_cancelled());
    gate.store(false, Ordering::SeqCst);

    client.set_string("next", "v").await.unwrap();
    assert_eq!(
        count(&first_log, "SET", Some("next")),
        0,
        "went to the old primary"
    );
    assert_eq!(count(&second_log, "SET", Some("next")), 1);
    assert_eq!(count(&first_log, "SET", Some("lost")), 1);
    assert_eq!(count(&second_log, "SET", Some("lost")), 0, "resent");
}

#[tokio::test]
async fn a_batch_answered_with_a_server_error_keeps_the_shared_socket() {
    let log: Log = Arc::default();
    let seen = log.clone();
    let server = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        Some(match (args[0].as_str(), args.get(1).map(String::as_str)) {
            ("UNLINK", Some("refused")) => "-NOPERM no\r\n".into(),
            ("UNLINK", _) => ":1\r\n".into(),
            ("PING", _) => "+PONG\r\n".into(),
            _ => "+OK\r\n".into(),
        })
    });
    let client = Client::connect(server.profile(Deployment::Standalone))
        .await
        .unwrap();
    client.set_string("before", "v").await.unwrap();
    assert!(client.delete_keys(&["refused".into()]).await.is_err());
    client.set_string("after", "v").await.unwrap();
    let socket = sockets_for(&log, "SET")[0];
    assert_eq!(sockets_for(&log, "UNLINK"), vec![socket]);
    assert_eq!(
        sockets_for(&log, "SET"),
        vec![socket, socket],
        "reconnected after a server error"
    );
}

#[tokio::test]
async fn a_standalone_discovery_whose_ping_fails_drops_that_socket() {
    let log: Log = Arc::default();
    let seen = log.clone();
    let failing = Arc::new(AtomicBool::new(false));
    let fails = failing.clone();
    let server = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        Some(match args[0].as_str() {
            "PING" if fails.load(Ordering::SeqCst) => "-ERR not now\r\n".into(),
            "PING" => "+PONG\r\n".into(),
            "GET" => bulk("value"),
            _ => "+OK\r\n".into(),
        })
    });
    let client = Client::connect(server.profile(Deployment::Standalone))
        .await
        .unwrap();
    client.execute_raw("GET before").await.unwrap();
    failing.store(true, Ordering::SeqCst);
    assert!(client.refresh_topology().await.is_err());
    failing.store(false, Ordering::SeqCst);
    client.execute_raw("GET after").await.unwrap();
    let gets = sockets_for(&log, "GET");
    assert_eq!(gets.len(), 2);
    assert_ne!(gets[0], gets[1], "kept the socket whose PING failed");
}

// ---- a discovery that succeeds while a write waits for its socket --------

/// A primary whose `PING` takes a second while `slow` is set, logging every
/// command.
fn slow_ping_primary(slow: Arc<AtomicBool>) -> (Peer, Log) {
    let log: Log = Arc::default();
    let seen = log.clone();
    let peer = Peer::start(move |id, args| {
        seen.lock().unwrap().push((id, args.to_vec()));
        Some(match args[0].as_str() {
            "ROLE" => "*1\r\n$6\r\nmaster\r\n".into(),
            "PING" => {
                if slow.load(Ordering::SeqCst) {
                    thread::sleep(Duration::from_secs(1));
                }
                "+PONG\r\n".into()
            }
            "UNLINK" => ":1\r\n".into(),
            _ => "+OK\r\n".into(),
        })
    });
    (peer, log)
}

/// A Sentinel naming `target`, or dropping the connection while `broken`.
fn switchable_sentinel(target: Arc<AtomicUsize>, broken: Arc<AtomicBool>) -> Peer {
    Peer::start(move |_, args| match args[0].as_str() {
        "SENTINEL" if broken.load(Ordering::SeqCst) => None,
        "SENTINEL" => Some(format!(
            "*2\r\n{}{}",
            bulk("127.0.0.1"),
            bulk(&target.load(Ordering::SeqCst).to_string())
        )),
        _ => Some("+OK\r\n".into()),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_waiting_on_an_idle_check_follows_a_discovery_that_moved_the_primary() {
    for batch in [false, true] {
        let slow = Arc::new(AtomicBool::new(false));
        let (first, first_log) = slow_ping_primary(slow.clone());
        let (second, second_log) = slow_ping_primary(Arc::default());
        let target = Arc::new(AtomicUsize::new(first.port as usize));
        let sentinel = switchable_sentinel(target.clone(), Arc::default());
        let client = sentinel_client(&sentinel).await;
        client.set_string("before", "v").await.unwrap();
        client.idle_ping_after(Duration::ZERO);
        slow.store(true, Ordering::SeqCst);

        let pings = count(&first_log, "PING", None);
        let c = client.clone();
        let write = tokio::spawn(async move {
            if batch {
                c.delete_keys(&["a".into()]).await.map(|_| ())
            } else {
                c.set_string("a", "v").await
            }
        });
        eventually("the idle check on the old primary", 10, || {
            count(&first_log, "PING", None) > pings
        })
        .await;
        // The primary moves, and a discovery finds it while the write waits.
        target.store(second.port as usize, Ordering::SeqCst);
        client.refresh_topology().await.unwrap();
        write.await.unwrap().unwrap();

        let head = if batch { "UNLINK" } else { "SET" };
        assert_eq!(
            count(&first_log, head, Some("a")),
            0,
            "batch={batch}: sent to the address from before the discovery"
        );
        assert_eq!(count(&second_log, head, Some("a")), 1, "batch={batch}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refusals_that_send_nothing_are_audited_as_failures_for_scripts_and_batches() {
    let slow = Arc::new(AtomicBool::new(false));
    let (first, first_log) = slow_ping_primary(slow.clone());
    let target = Arc::new(AtomicUsize::new(first.port as usize));
    let broken = Arc::new(AtomicBool::new(false));
    let sentinel = switchable_sentinel(target.clone(), broken.clone());
    let mut profile = sentinel.profile(Deployment::Sentinel);
    profile.sentinel_master = "service".into();
    profile.name = unique("unsent-audit", sentinel.port);
    let client = Client::connect(profile.clone()).await.unwrap();
    client.idle_ping_after(Duration::ZERO);
    slow.store(true, Ordering::SeqCst);

    for (action, head) in [("SCRIPT", "EVAL"), ("PIPELINE", "UNLINK")] {
        broken.store(false, Ordering::SeqCst);
        client.refresh_topology().await.unwrap();
        let pings = count(&first_log, "PING", None);
        let c = client.clone();
        let write = tokio::spawn(async move {
            if head == "EVAL" {
                c.execute_raw("EVAL return 1 0").await.map(|_| ())
            } else {
                c.delete_keys(&["a".into()]).await.map(|_| ())
            }
        });
        eventually("the idle check", 10, || {
            count(&first_log, "PING", None) > pings
        })
        .await;
        // Discovery fails while the write waits, and keeps failing.
        broken.store(true, Ordering::SeqCst);
        assert!(client.refresh_topology().await.is_err());
        let e = write.await.unwrap().unwrap_err().to_string();
        assert!(e.contains("othing was sent"), "{action}: {e}");
        assert_eq!(count(&first_log, head, None), 0, "{action} was sent");
        assert_eq!(
            audit_outcomes(&profile.name, action),
            vec!["started", "failure"],
            "{action}: {e}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sentinel_config_change_for_a_primary_that_moved_is_refused() {
    let (first, first_log) = primary_peer();
    let (second, second_log) = primary_peer();
    let target = Arc::new(AtomicUsize::new(first.port as usize));
    let sentinel = switchable_sentinel(target.clone(), Arc::default());
    let mut profile = sentinel.profile(Deployment::Sentinel);
    profile.sentinel_master = "service".into();
    profile.name = unique("moved-config", sentinel.port);
    let client = Client::connect(profile.clone()).await.unwrap();
    let old = client.diagnostics().await.unwrap().node;
    assert_eq!(old, Some(("127.0.0.1".to_string(), first.port)));

    target.store(second.port as usize, Ordering::SeqCst);
    client.refresh_topology().await.unwrap();
    let e = client
        .config_set_on(&old, "maxmemory", "1")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("othing was sent"), "{e}");
    assert!(e.contains(&second.port.to_string()), "{e}");
    assert_eq!(count(&first_log, "CONFIG", Some("SET")), 0);
    assert_eq!(count(&second_log, "CONFIG", Some("SET")), 0);
    assert_eq!(
        audit_outcomes(&profile.name, "CONFIG"),
        vec!["started", "failure"]
    );

    // The current primary takes it, and a client id still goes to the node
    // that listed it.
    let new = client.diagnostics().await.unwrap().node;
    client.config_set_on(&new, "maxmemory", "1").await.unwrap();
    assert_eq!(count(&second_log, "CONFIG", Some("SET")), 1);
    client.client_kill_on(&old, "5").await.unwrap();
    assert_eq!(count(&first_log, "CLIENT", Some("KILL")), 1);
}

// ---- import over scripted servers ---------------------------------------------

/// A standalone server that runs `MULTI` the way Redis does: queued commands
/// are answered `QUEUED`, and `EXEC` answers with each queued command's reply
/// from `reply`, which also answers every command outside a transaction.
fn transactional(log: Log, reply: impl Fn(&[String]) -> String + Send + Sync + 'static) -> Peer {
    let queued: Arc<Mutex<std::collections::HashMap<usize, Vec<Vec<String>>>>> = Arc::default();
    Peer::start(move |id, args| {
        log.lock().unwrap().push((id, args.to_vec()));
        let mut queued = queued.lock().unwrap();
        Some(match args[0].to_ascii_uppercase().as_str() {
            "MULTI" => {
                queued.insert(id, Vec::new());
                "+OK\r\n".into()
            }
            "EXEC" => {
                let cmds = queued.remove(&id).unwrap_or_default();
                let mut out = format!("*{}\r\n", cmds.len());
                for cmd in &cmds {
                    out.push_str(&reply(cmd));
                }
                out
            }
            _ => match queued.get_mut(&id) {
                Some(q) => {
                    q.push(args.to_vec());
                    "+QUEUED\r\n".into()
                }
                None => reply(args),
            },
        })
    })
}

/// Replies like an empty server that accepts every write, with `PEXPIRE`
/// failing the way Redis fails an expiry time it cannot hold.
fn expire_fails(args: &[String]) -> String {
    match args[0].to_ascii_uppercase().as_str() {
        "PEXPIRE" => "-ERR invalid expire time in 'pexpire' command\r\n".into(),
        "TYPE" => "+none\r\n".into(),
        "EXISTS" => ":0\r\n".into(),
        "DEL" | "RPUSH" | "RENAMENX" => ":1\r\n".into(),
        _ => "+OK\r\n".into(),
    }
}

#[tokio::test]
async fn import_transaction_that_ran_with_a_failed_command_says_so_and_audits_unknown() {
    use rediscope::transfer::{Record, Value};
    let log: Log = Arc::default();
    let peer = transactional(log.clone(), expire_fails);
    let mut profile = peer.profile(Deployment::Standalone);
    profile.name = unique("exec-error", peer.port);
    let client = Client::connect(profile.clone()).await.unwrap();
    let record = Record {
        key: b"k".to_vec(),
        ttl_ms: Some(60_000),
        value: Value::String(b"new".to_vec()),
    };
    let e = client
        .import_records(&[record], true)
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("invalid expire time"), "{e}");
    assert!(!e.contains("not changed"), "{e}");
    assert!(!e.contains("discarded"), "{e}");
    assert!(
        e.contains("its old value is gone") && e.contains("without its TTL"),
        "{e}"
    );
    assert_eq!(count(&log, "EXEC", None), 1);
    assert_eq!(
        audit_outcomes(&profile.name, "PIPELINE"),
        vec!["started", "unknown"]
    );
}

/// A standalone server whose ACL refuses `MULTI` and `EXEC`, the way Redis
/// does for a user without `@transaction`: every other command runs at once.
fn multi_refused(log: Log, reply: impl Fn(&[String]) -> String + Send + Sync + 'static) -> Peer {
    Peer::start(move |id, args| {
        log.lock().unwrap().push((id, args.to_vec()));
        Some(match args[0].to_ascii_uppercase().as_str() {
            name @ ("MULTI" | "EXEC") => format!(
                "-NOPERM User limited has no permissions to run the '{}' command\r\n",
                name.to_ascii_lowercase()
            ),
            _ => reply(args),
        })
    })
}

#[tokio::test]
async fn import_transaction_refused_at_multi_ran_its_commands_and_audits_unknown() {
    use rediscope::transfer::{Record, Value};
    let log: Log = Arc::default();
    let peer = multi_refused(log.clone(), |args| {
        match args[0].to_ascii_uppercase().as_str() {
            "DEL" | "PEXPIRE" => ":1\r\n".into(),
            _ => "+OK\r\n".into(),
        }
    });
    let mut profile = peer.profile(Deployment::Standalone);
    profile.name = unique("multi-refused", peer.port);
    let client = Client::connect(profile.clone()).await.unwrap();
    let record = Record {
        key: b"k".to_vec(),
        ttl_ms: Some(60_000),
        value: Value::String(b"new".to_vec()),
    };
    let e = client
        .import_records(&[record], true)
        .await
        .unwrap_err()
        .to_string();
    assert!(!e.contains("not changed"), "{e}");
    assert!(!e.contains("discarded"), "{e}");
    assert!(e.contains("may have been partly or fully applied"), "{e}");
    // The server ran them: they were not queued.
    assert_eq!(count(&log, "SET", None), 1);
    assert_eq!(
        audit_outcomes(&profile.name, "PIPELINE"),
        vec!["started", "unknown"]
    );
}

#[tokio::test]
async fn import_rename_transaction_refused_at_multi_does_not_say_the_key_was_unchanged() {
    use rediscope::transfer::{Record, Value};
    let log: Log = Arc::default();
    let peer = multi_refused(log.clone(), |args| {
        match args[0].to_ascii_uppercase().as_str() {
            "EXISTS" => ":0\r\n".into(),
            "RPUSH" | "DEL" | "PEXPIRE" => ":1\r\n".into(),
            _ => "+OK\r\n".into(),
        }
    });
    let mut profile = peer.profile(Deployment::Standalone);
    profile.name = unique("multi-refused-rename", peer.port);
    let client = Client::connect(profile.clone()).await.unwrap();
    let record = Record {
        key: b"big".to_vec(),
        ttl_ms: Some(60_000),
        value: Value::List((0..2_000).map(|_| vec![b'v'; 1_000]).collect()),
    };
    let e = client
        .import_records(&[record], true)
        .await
        .unwrap_err()
        .to_string();
    assert!(!e.contains("not changed"), "{e}");
    assert!(e.contains("may have been partly or fully applied"), "{e}");
    assert_eq!(count(&log, "RENAME", None), 1);
    let outcomes = audit_outcomes(&profile.name, "PIPELINE");
    assert_eq!(outcomes.last().map(String::as_str), Some("unknown"));
}

#[tokio::test]
async fn import_transaction_aborted_at_exec_is_still_not_changed_and_a_failure() {
    use rediscope::transfer::{Record, Value};
    // Queueing refuses SET, so EXEC answers EXECABORT and nothing ran.
    let open: Arc<Mutex<bool>> = Arc::default();
    let peer = Peer::start(move |_, args| {
        let mut open = open.lock().unwrap();
        Some(match args[0].to_ascii_uppercase().as_str() {
            "MULTI" => {
                *open = true;
                "+OK\r\n".into()
            }
            "EXEC" => {
                *open = false;
                "-EXECABORT Transaction discarded because of previous errors.\r\n".into()
            }
            "SET" if *open => "-NOPERM no permissions to run the 'set' command\r\n".into(),
            _ if *open => "+QUEUED\r\n".into(),
            _ => "+OK\r\n".into(),
        })
    });
    let mut profile = peer.profile(Deployment::Standalone);
    profile.name = unique("exec-abort", peer.port);
    let client = Client::connect(profile.clone()).await.unwrap();
    let record = Record {
        key: b"k".to_vec(),
        ttl_ms: Some(60_000),
        value: Value::String(b"new".to_vec()),
    };
    let e = client
        .import_records(&[record], true)
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("no permissions"), "{e}");
    assert!(e.contains("the key was not changed"), "{e}");
    assert_eq!(
        audit_outcomes(&profile.name, "PIPELINE"),
        vec!["started", "failure"]
    );
}

#[tokio::test]
async fn import_rename_that_ran_after_a_failed_ttl_does_not_say_the_key_was_unchanged() {
    use rediscope::transfer::{Record, Value};
    let log: Log = Arc::default();
    let peer = transactional(log.clone(), expire_fails);
    let mut profile = peer.profile(Deployment::Standalone);
    profile.name = unique("rename-ran", peer.port);
    let client = Client::connect(profile.clone()).await.unwrap();
    // Big enough for more than one pipeline, so it is written aside.
    let record = Record {
        key: b"big".to_vec(),
        ttl_ms: Some(60_000),
        value: Value::List((0..2_000).map(|_| vec![b'v'; 1_000]).collect()),
    };
    let e = client
        .import_records(&[record], true)
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("invalid expire time"), "{e}");
    assert!(!e.contains("not changed"), "{e}");
    assert!(e.contains("without its TTL"), "{e}");
    assert_eq!(count(&log, "RENAME", None), 1);
    // The temporary key was renamed away: there is nothing left to delete.
    assert_eq!(count(&log, "DEL", None), 0);
    let outcomes = audit_outcomes(&profile.name, "PIPELINE");
    assert_eq!(outcomes.last().map(String::as_str), Some("unknown"));
}

#[tokio::test]
async fn import_without_overwrite_never_writes_over_a_key_created_after_the_check() {
    use rediscope::transfer::{Record, Value};
    // The key does not exist when checked, and exists by the time it is written.
    let log: Log = Arc::default();
    let peer = transactional(log.clone(), |args| {
        match args[0].to_ascii_uppercase().as_str() {
            "TYPE" => "+none\r\n".into(),
            "EXISTS" => ":0\r\n".into(),
            // SET ... NX that finds the key: nothing set.
            "SET" => "$-1\r\n".into(),
            "RENAMENX" => ":0\r\n".into(),
            "RPUSH" | "DEL" | "PEXPIRE" => ":1\r\n".into(),
            _ => "+OK\r\n".into(),
        }
    });
    let client = Client::connect(peer.profile(Deployment::Standalone))
        .await
        .unwrap();
    let string = Record {
        key: b"s".to_vec(),
        ttl_ms: Some(60_000),
        value: Value::String(b"new".to_vec()),
    };
    let e = client
        .import_records(&[string], false)
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("created by someone else"), "{e}");
    let sets: Vec<Vec<String>> = log
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, a)| a[0] == "SET")
        .map(|(_, a)| a.clone())
        .collect();
    assert_eq!(sets, [["SET", "s", "new", "NX", "PX", "60000"]]);
    assert_eq!(count(&log, "PEXPIRE", None), 0);

    let list = Record {
        key: b"l".to_vec(),
        ttl_ms: None,
        value: Value::List(vec![b"a".to_vec()]),
    };
    let e = client
        .import_records(&[list], false)
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("created by someone else"), "{e}");
    let log = log.lock().unwrap();
    let pushed: Vec<&String> = log
        .iter()
        .filter(|(_, a)| a[0] == "RPUSH")
        .map(|(_, a)| &a[1])
        .collect();
    assert_eq!(pushed.len(), 1, "{pushed:?}");
    assert!(
        pushed[0].starts_with("l:rediscope-import-tmp:"),
        "{pushed:?}"
    );
    assert!(
        log.iter()
            .any(|(_, a)| a[0] == "RENAMENX" && a[1] == *pushed[0] && a[2] == "l")
    );
    assert!(log.iter().any(|(_, a)| a[0] == "DEL" && a[1] == *pushed[0]));
}

#[tokio::test]
async fn import_report_counts_every_command_it_sent() {
    use rediscope::transfer::{Record, Value};
    let log: Log = Arc::default();
    let peer = transactional(log.clone(), |args| {
        match args[0].to_ascii_uppercase().as_str() {
            "TYPE" => "+none\r\n".into(),
            "EXISTS" => ":0\r\n".into(),
            "RPUSH" | "DEL" | "PEXPIRE" | "RENAMENX" => ":1\r\n".into(),
            _ => "+OK\r\n".into(),
        }
    });
    let client = Client::connect(peer.profile(Deployment::Standalone))
        .await
        .unwrap();
    let list = |key: &[u8], ttl_ms| Record {
        key: key.to_vec(),
        ttl_ms,
        value: Value::List(vec![b"a".to_vec()]),
    };
    let string = Record {
        key: b"s".to_vec(),
        ttl_ms: Some(60_000),
        value: Value::String(b"v".to_vec()),
    };
    let writes = |log: &Log| {
        log.lock()
            .unwrap()
            .iter()
            .filter(|(_, a)| {
                !matches!(
                    a[0].to_ascii_uppercase().as_str(),
                    "TYPE" | "EXISTS" | "MULTI" | "EXEC" | "COMMAND" | "PING" | "SELECT" | "HELLO"
                )
            })
            .count() as u64
    };
    // Without overwrite: SET NX; RPUSH and RENAMENX; RPUSH, PEXPIRE and RENAMENX.
    let report = client
        .import_records(&[string, list(b"l", None), list(b"t", Some(60_000))], false)
        .await
        .unwrap();
    assert_eq!(report.commands, 6);
    assert_eq!(report.commands, writes(&log));
    // With overwrite: DEL, RPUSH and PEXPIRE in one transaction.
    log.lock().unwrap().clear();
    let report = client
        .import_records(&[list(b"t", Some(60_000))], true)
        .await
        .unwrap();
    assert_eq!(report.commands, 3);
    assert_eq!(report.commands, writes(&log));
}

/// A cluster whose node `b` refuses `head` once with TRYAGAIN and then drops
/// the connection on the resend, so its outcome is unknown.
fn lost_on_resend(head: &'static str) -> TwoNodes {
    let sent = Arc::new(AtomicUsize::new(0));
    TwoNodes::start(move |node, _, args| {
        if node != 'b' {
            return None;
        }
        match args[0].as_str() {
            "TYPE" => Some(Some("+none\r\n".into())),
            "EXISTS" => Some(Some(":0\r\n".into())),
            "RPUSH" => Some(Some(":1\r\n".into())),
            h if h == head => Some(match sent.fetch_add(1, Ordering::SeqCst) {
                0 => Some("-TRYAGAIN resharding\r\n".into()),
                _ => None,
            }),
            _ => None,
        }
    })
}

#[tokio::test]
async fn cluster_import_write_lost_on_resend_does_not_say_the_key_was_unchanged() {
    use rediscope::transfer::{Record, Value};
    let kb = key_on(false, "lost-import", 0);
    let nodes = lost_on_resend("SET");
    let client = nodes.client().await;
    let string = Record {
        key: kb.as_bytes().to_vec(),
        ttl_ms: None,
        value: Value::String(b"v".to_vec()),
    };
    let e = client
        .import_records(&[string], false)
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("outcome unknown"), "{e}");
    assert!(!e.contains("not changed"), "{e}");
    assert_eq!(count(&nodes.b_log, "SET", Some(&kb)), 2);

    let nodes = lost_on_resend("RENAMENX");
    let client = nodes.client().await;
    let list = Record {
        key: kb.as_bytes().to_vec(),
        ttl_ms: None,
        value: Value::List(vec![b"a".to_vec()]),
    };
    let e = client
        .import_records(&[list], false)
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("outcome unknown"), "{e}");
    assert!(!e.contains("not changed"), "{e}");
    assert_eq!(count(&nodes.b_log, "RENAMENX", None), 2);
}

#[tokio::test]
async fn cluster_import_resend_refused_by_an_expired_lease_is_not_audited_denied() {
    let release = Arc::new(AtomicBool::new(false));
    let refused = Arc::new(AtomicUsize::new(0));
    let (wait, once) = (release.clone(), refused.clone());
    let nodes = TwoNodes::start(move |node, _, args| {
        if node == 'b'
            && args[0] == "SET"
            && args[2] == "1"
            && once.fetch_add(1, Ordering::SeqCst) == 0
        {
            for _ in 0..5000 {
                if wait.load(Ordering::SeqCst) {
                    break;
                }
                thread::sleep(Duration::from_millis(2));
            }
            return Some(Some("-TRYAGAIN resharding\r\n".into()));
        }
        None
    });
    let mut production = nodes.profile();
    production.name = unique("lease-resend", nodes.a.port);
    production.environment = rediscope::config::Environment::Production;
    let client = Client::connect(production.clone()).await.unwrap();
    client.unlock_writes(&production.name).unwrap();
    let kb = key_on(false, "resend", 0);
    let file = rediscope::transfer::parse(format!("SET {kb} 1\nSET {kb} 2\n").as_bytes()).unwrap();
    let import = {
        let client = client.clone();
        tokio::spawn(async move { client.import_parsed(&file, false).await })
    };
    for _ in 0..2500 {
        if count(&nodes.b_log, "SET", None) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    client.lock_writes().unwrap();
    release.store(true, Ordering::SeqCst);
    let e = tokio::time::timeout(Duration::from_secs(20), import)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err()
        .to_string();
    assert!(e.contains("Pipeline partially applied: 1 of 2"), "{e}");
    assert!(e.contains("not sent again"), "{e}");
    assert_eq!(count(&nodes.b_log, "SET", Some(&kb)), 2, "nothing resent");
    assert_eq!(
        audit_outcomes(&production.name, "PIPELINE"),
        vec!["started", "unknown"]
    );
}

#[tokio::test]
async fn cluster_dump_export_is_one_export_event_without_per_key_reads() {
    let nodes = TwoNodes::start(|_, _, args| match args[0].as_str() {
        "DUMP" => Some(Some(bulk("payload"))),
        "PTTL" => Some(Some(":-1\r\n".into())),
        "TYPE" => Some(Some("+string\r\n".into())),
        _ => None,
    });
    let mut profile = nodes.profile();
    profile.name = unique("dump-export-audit", nodes.a.port);
    let client = Client::connect(profile.clone()).await.unwrap();
    let names = vec![key_on(true, "dump", 0), key_on(false, "dump", 0)];
    let (report, _) = client
        .export_to(&names, rediscope::transfer::Format::Dump, false, Vec::new())
        .await
        .unwrap();
    assert_eq!(report.written, 2);
    assert_eq!(client.export_keys(&names).await.unwrap().len(), 2);
    assert_eq!(count(&nodes.a_log, "DUMP", None), 2);
    assert!(audit_outcomes(&profile.name, "EXPORT_READ").is_empty());
    assert_eq!(
        audit_outcomes(&profile.name, "EXPORT"),
        vec!["started", "success", "started", "success"]
    );
}
