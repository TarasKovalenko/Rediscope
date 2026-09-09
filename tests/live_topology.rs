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

#[tokio::test]
#[ignore = "requires redis-server and redis-cli; starts disposable local instances"]
async fn real_cluster_browsing_partial_coverage_and_sentinel_discovery() {
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
    assert!(client.set_string(&names[0], "forbidden").await.is_err());
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
