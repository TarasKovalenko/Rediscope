//! Opt-in real Redis smoke test: cargo test --test live_topology -- --ignored
//! Starts only disposable local redis-server children and removes their files.

mod common;
use rediscope::{
    config::{Connection, Deployment},
    redis_client::{Client, KeyType, key_slot},
};
use std::{
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};
struct Server {
    child: Child,
    port: u16,
    dir: PathBuf,
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
impl Server {
    async fn start(cluster: bool, sentinel: Option<u16>) -> Self {
        let port = (18000..28000)
            .find(|p| {
                TcpListener::bind(("127.0.0.1", *p)).is_ok()
                    && TcpListener::bind(("127.0.0.1", *p + 10000)).is_ok()
            })
            .unwrap();
        let dir =
            std::env::temp_dir().join(format!("rediscope-topology-{}-{port}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut config = format!(
            "bind 127.0.0.1\nport {port}\nsave \"\"\nappendonly no\ndir {}\n",
            dir.display()
        );
        if cluster {
            config.push_str("cluster-enabled yes\ncluster-config-file nodes.conf\ncluster-node-timeout 500\ncluster-require-full-coverage no\n");
        }
        if let Some(primary) = sentinel {
            config.push_str(&format!("sentinel monitor test-primary 127.0.0.1 {primary} 1\nsentinel down-after-milliseconds test-primary 500\n"));
        }
        let file = dir.join("redis.conf");
        std::fs::write(&file, config).unwrap();
        let mut cmd = Command::new("redis-server");
        cmd.arg(file);
        if sentinel.is_some() {
            cmd.arg("--sentinel");
        }
        let child = cmd
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("redis-server must be installed");
        let server = Self { child, port, dir };
        for _ in 0..100 {
            if let Ok(mut c) = redis::Client::open(("127.0.0.1", port))
                .unwrap()
                .get_multiplexed_async_connection()
                .await
                && redis::cmd("PING")
                    .query_async::<String>(&mut c)
                    .await
                    .is_ok()
            {
                return server;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("redis-server did not start")
    }
    async fn raw(&self) -> redis::aio::MultiplexedConnection {
        redis::Client::open(("127.0.0.1", self.port))
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap()
    }
    fn profile(&self, deployment: Deployment) -> Connection {
        common::isolate_config();
        Connection {
            name: "live topology".into(),
            host: "127.0.0.1".into(),
            port: self.port,
            deployment,
            ..Default::default()
        }
    }
}

/// Live tests share port ranges and scratch directories, so they run one at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Three primaries joined with `redis-cli --cluster create`, once converged.
async fn start_cluster() -> Vec<Server> {
    let mut servers = Vec::new();
    for _ in 0..3 {
        servers.push(Server::start(true, None).await);
    }
    let addresses: Vec<_> = servers
        .iter()
        .map(|s| format!("127.0.0.1:{}", s.port))
        .collect();
    let output = Command::new("redis-cli")
        .args(["--cluster", "create"])
        .args(&addresses)
        .arg("--cluster-yes")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    for server in &servers {
        let mut ready = false;
        for _ in 0..100 {
            let info: String = redis::cmd("CLUSTER")
                .arg("INFO")
                .query_async(&mut server.raw().await)
                .await
                .unwrap();
            if info.contains("cluster_state:ok") {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(ready, "cluster did not converge");
    }
    servers
}

#[tokio::test]
#[ignore = "requires redis-server and redis-cli; starts disposable local instances"]
async fn real_cluster_browsing_partial_coverage_and_sentinel_discovery() {
    let _serial = SERIAL.lock().await;
    let mut servers = start_cluster().await;
    let client = Client::connect(servers[0].profile(Deployment::Cluster))
        .await
        .unwrap();
    let nodes = client.refresh_topology().await.unwrap();
    assert_eq!(nodes.iter().filter(|n| n.primary).count(), 3);
    let mut names = Vec::new();
    for node in &nodes {
        let key = (0..10000)
            .map(|i| format!("live:key:{i}"))
            .find(|s| {
                node.slots
                    .iter()
                    .any(|(a, b)| (*a..=*b).contains(&key_slot(s.as_bytes())))
            })
            .unwrap();
        let server = servers.iter().find(|s| s.port == node.port).unwrap();
        redis::cmd("SET")
            .arg(&key)
            .arg("value")
            .query_async::<()>(&mut server.raw().await)
            .await
            .unwrap();
        names.push(key);
    }
    let report = client.scan_report("live:*", 100).await.unwrap();
    assert!(report.warnings.is_empty(), "{:?}", report.warnings);
    assert_eq!(report.keys.len(), 3);
    assert_eq!(client.dbsize().await.unwrap(), 3);
    for name in &names {
        assert!(client.read_value(name, KeyType::String).await.is_ok());
    }
    client.set_string(&names[0], "written").await.unwrap();
    assert!(matches!(
        client.read_value(&names[0], KeyType::String).await.unwrap(),
        rediscope::redis_client::KeyValue::Str(s) if s == "written"
    ));
    assert_eq!(client.delete_keys(&names).await.unwrap(), 3);
    for name in &names {
        redis::cmd("SET")
            .arg(name)
            .arg("value")
            .query_async::<()>(
                &mut servers
                    .iter()
                    .find(|s| {
                        nodes.iter().any(|n| {
                            n.port == s.port
                                && n.slots
                                    .iter()
                                    .any(|(a, b)| (*a..=*b).contains(&key_slot(name.as_bytes())))
                        })
                    })
                    .unwrap()
                    .raw()
                    .await,
            )
            .await
            .unwrap();
    }
    assert!(client.info().await.unwrap().raw.contains("slots=["));
    servers[2].child.kill().unwrap();
    servers[2].child.wait().unwrap();
    let report = client.scan_report("live:*", 100).await.unwrap();
    assert!(!report.warnings.is_empty());
    assert_eq!(report.keys.len(), 2);

    let primary = Server::start(false, None).await;
    redis::cmd("SET")
        .arg("sentinel:key")
        .arg("value")
        .query_async::<()>(&mut primary.raw().await)
        .await
        .unwrap();
    let sentinel = Server::start(false, Some(primary.port)).await;
    let mut profile = sentinel.profile(Deployment::Sentinel);
    profile.sentinel_master = "test-primary".into();
    let client = Client::connect(profile).await.unwrap();
    assert_eq!(client.scan_report("*", 100).await.unwrap().keys.len(), 1);
    assert_eq!(
        client.refresh_topology().await.unwrap()[0].port,
        primary.port
    );
}

/// The server owning `key`'s slot.
fn owner<'a>(
    servers: &'a [Server],
    nodes: &[rediscope::redis_client::Node],
    key: &str,
) -> &'a Server {
    let slot = key_slot(key.as_bytes());
    let node = nodes
        .iter()
        .find(|n| n.primary && n.slots.iter().any(|(a, b)| (*a..=*b).contains(&slot)))
        .unwrap();
    servers.iter().find(|s| s.port == node.port).unwrap()
}
async fn raw_get(server: &Server, key: &str) -> Option<String> {
    redis::cmd("GET")
        .arg(key)
        .query_async(&mut server.raw().await)
        .await
        .unwrap()
}
async fn raw_exists(server: &Server, key: &str) -> bool {
    redis::cmd("EXISTS")
        .arg(key)
        .query_async::<i64>(&mut server.raw().await)
        .await
        .unwrap()
        == 1
}

#[tokio::test]
#[ignore = "requires redis-server and redis-cli; starts disposable local instances"]
async fn real_cluster_and_sentinel_accept_routed_writes() {
    use rediscope::redis_client::{EditOutcome, EditTarget, KeyValue};
    let _serial = SERIAL.lock().await;
    let servers = start_cluster().await;
    let client = Client::connect(servers[0].profile(Deployment::Cluster))
        .await
        .unwrap();
    assert!(!client.read_only());
    let nodes = client.refresh_topology().await.unwrap();
    let primaries: Vec<_> = nodes.iter().filter(|n| n.primary).collect();
    assert_eq!(primaries.len(), 3);

    // One key per primary, written through the client, read back raw from its owner.
    let names: Vec<String> = primaries
        .iter()
        .map(|node| {
            (0..10000)
                .map(|i| format!("write:key:{i}"))
                .find(|k| {
                    let slot = key_slot(k.as_bytes());
                    node.slots.iter().any(|(a, b)| (*a..=*b).contains(&slot))
                })
                .unwrap()
        })
        .collect();
    for name in &names {
        client.set_string(name, "routed").await.unwrap();
        assert_eq!(
            raw_get(owner(&servers, &nodes, name), name)
                .await
                .as_deref(),
            Some("routed")
        );
    }
    let owners: std::collections::HashSet<u16> = names
        .iter()
        .map(|n| owner(&servers, &nodes, n).port)
        .collect();
    assert_eq!(owners.len(), 3, "the keys cover every primary");

    // Rename within a hash tag works; across slots it is refused and nothing changes.
    let (old, new) = ("{write}:old".to_string(), "{write}:new".to_string());
    client.set_string(&old, "tagged").await.unwrap();
    client.rename_key(&old, &new).await.unwrap();
    let tagged = owner(&servers, &nodes, &new);
    assert!(!raw_exists(tagged, &old).await);
    assert_eq!(raw_get(tagged, &new).await.as_deref(), Some("tagged"));
    let e = client
        .rename_key(&names[0], &names[1])
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("different cluster slots"), "{e}");
    assert!(raw_exists(owner(&servers, &nodes, &names[0]), &names[0]).await);

    // A script over two same-slot keys runs on their owner.
    let reply = client
        .eval(
            "redis.call('SET', KEYS[1], ARGV[1]); redis.call('SET', KEYS[2], ARGV[1]); return redis.call('GET', KEYS[2])",
            &["{write}:s1".into(), "{write}:s2".into()],
            &["scripted".into()],
        )
        .await
        .unwrap();
    assert!(reply.contains("scripted"), "{reply}");
    assert_eq!(
        raw_get(owner(&servers, &nodes, "{write}:s1"), "{write}:s1")
            .await
            .as_deref(),
        Some("scripted")
    );

    // Keyless writes that would touch one primary of three are refused.
    let before = client.dbsize().await.unwrap();
    let e = client.execute_raw("FLUSHDB").await.unwrap_err().to_string();
    assert!(e.contains("only one primary"), "{e}");
    assert_eq!(client.dbsize().await.unwrap(), before);
    assert!(
        client
            .execute_raw("CONFIG SET slowlog-max-len 128")
            .await
            .is_ok()
    );

    // Bulk expiry and persistence across nodes.
    assert_eq!(client.expire_keys(&names, Some(1000)).await.unwrap(), 3);
    for name in &names {
        let ttl: i64 = redis::cmd("TTL")
            .arg(name)
            .query_async(&mut owner(&servers, &nodes, name).raw().await)
            .await
            .unwrap();
        assert!(ttl > 0 && ttl <= 1000, "{name}: {ttl}");
    }
    assert_eq!(client.expire_keys(&names, None).await.unwrap(), 3);

    // Export, delete across nodes, import back.
    // One of the three becomes a hash, so the round trip carries more than strings.
    client
        .delete_keys(std::slice::from_ref(&names[2]))
        .await
        .unwrap();
    client.hash_set(&names[2], "field", "value").await.unwrap();
    let entries = client.export_keys(&names).await.unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(client.delete_keys(&names).await.unwrap(), 3);
    for name in &names {
        assert!(!raw_exists(owner(&servers, &nodes, name), name).await);
    }
    assert_eq!(client.delete_keys(&names).await.unwrap(), 0);
    assert_eq!(client.import_entries(&entries, false).await.unwrap(), 3);
    assert!(client.import_entries(&entries, false).await.is_err());
    assert_eq!(client.import_entries(&entries, true).await.unwrap(), 3);
    assert_eq!(
        raw_get(owner(&servers, &nodes, &names[0]), &names[0])
            .await
            .as_deref(),
        Some("routed")
    );
    assert!(matches!(
        client.read_value(&names[2], KeyType::Hash).await.unwrap(),
        KeyValue::Rows { .. }
    ));

    // An in-place edit is one EVAL on the key's owner, with its conflict check.
    let edit = |original: &str| EditTarget {
        key: names[0].clone(),
        kind: KeyType::String,
        selector: String::new(),
        original: original.into(),
        decoded: None,
    };
    assert_eq!(
        client
            .save_edit(&edit("routed"), &["edited".into()], false)
            .await
            .unwrap(),
        EditOutcome::Saved
    );
    assert_eq!(
        raw_get(owner(&servers, &nodes, &names[0]), &names[0])
            .await
            .as_deref(),
        Some("edited")
    );
    assert_eq!(
        client
            .save_edit(&edit("stale"), &["lost".into()], false)
            .await
            .unwrap(),
        EditOutcome::Conflict {
            current: Some("edited".into())
        }
    );

    // Readable export and import: each key read from and written to its owner.
    use rediscope::transfer::{self, Format};
    for format in [Format::Jsonl, Format::Commands] {
        let (report, bytes) = client
            .export_to(&names, format, false, Vec::new())
            .await
            .unwrap();
        assert_eq!(report.written, 3);
        assert_eq!(client.delete_keys(&names).await.unwrap(), 3);
        let parsed = transfer::parse(&bytes).unwrap();
        assert_eq!(client.import_parsed(&parsed, false).await.unwrap().keys, 3);
        assert_eq!(
            raw_get(owner(&servers, &nodes, &names[0]), &names[0])
                .await
                .as_deref(),
            Some("edited")
        );
        assert!(raw_exists(owner(&servers, &nodes, &names[2]), &names[2]).await);
    }
    // A command file line over keys in different slots is refused unsent.
    let line = format!("MSET {} 1 {} 2\n", names[0], names[1]);
    let parsed = transfer::parse(line.as_bytes()).unwrap();
    let err = client.import_parsed(&parsed, false).await.unwrap_err();
    assert!(err.to_string().contains("different cluster slots"), "{err}");
    assert_eq!(
        raw_get(owner(&servers, &nodes, &names[0]), &names[0])
            .await
            .as_deref(),
        Some("edited")
    );

    // Sentinel: writes land on the primary it names.
    let primary = Server::start(false, None).await;
    let sentinel = Server::start(false, Some(primary.port)).await;
    let mut profile = sentinel.profile(Deployment::Sentinel);
    profile.sentinel_master = "test-primary".into();
    let client = Client::connect(profile).await.unwrap();
    assert!(!client.read_only());
    client.set_string("sentinel:write", "landed").await.unwrap();
    assert_eq!(
        raw_get(&primary, "sentinel:write").await.as_deref(),
        Some("landed")
    );
    assert_eq!(
        client
            .delete_keys(&["sentinel:write".into(), "sentinel:missing".into()])
            .await
            .unwrap(),
        1
    );
    assert!(!raw_exists(&primary, "sentinel:write").await);
    drop(servers);
}
