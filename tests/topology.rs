//! Deterministic RESP peers exercise redirects and failures without a Redis daemon.
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
        let response = if args[0].eq_ignore_ascii_case("CLIENT") {
            Some("+OK\r\n".into())
        } else {
            handler(id, &args)
        };
        let Some(response) = response else {
            return;
        };
        if reader.get_mut().write_all(response.as_bytes()).is_err() {
            return;
        }
    }
}
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
        assert!(client.set_string("key", "bad").await.is_err());
        assert!(client.execute_raw("CONFIG SET maxmemory 1").await.is_err());
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
