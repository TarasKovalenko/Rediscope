//! Discovery and read routing. Writes are sent at most once on standalone;
//! discovered deployments deliberately expose only browsing in this release.
use super::*;
use crate::config::Deployment;
use redis::{Cmd, RedisError, RedisFuture, RedisResult, Value, aio::ConnectionLike};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

type Endpoint = (String, u16);

#[derive(Clone, Debug)]
pub struct Node {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub primary: bool,
    pub slots: Vec<(u16, u16)>,
}
impl Node {
    fn endpoint(&self) -> Endpoint {
        (self.host.clone(), self.port)
    }
}

#[derive(Clone)]
pub(super) struct Transport {
    profile: Connection,
    safety: crate::safety::Safety,
    audit: crate::audit::Audit,
    state: Arc<Mutex<State>>,
}
struct State {
    clients: HashMap<Endpoint, redis::Client>,
    sockets: HashMap<Endpoint, MultiplexedConnection>,
    nodes: Vec<Node>,
    default: Endpoint,
    refreshed: Instant,
    warning: Option<String>,
}
fn error(message: impl Into<String>) -> RedisError {
    RedisError::from((
        redis::ErrorKind::InvalidClientConfig,
        "Rediscope topology",
        message.into(),
    ))
}
use crate::config::parse_endpoint as endpoint;

pub(super) async fn socket(client: &redis::Client) -> RedisResult<MultiplexedConnection> {
    client
        .get_multiplexed_async_connection_with_config(
            &redis::AsyncConnectionConfig::new()
                .set_connection_timeout(Some(Duration::from_secs(3)))
                .set_response_timeout(Some(Duration::from_secs(5))),
        )
        .await
}

impl Transport {
    pub async fn new(profile: Connection, raw: redis::Client) -> Result<Self> {
        profile.validate_topology()?;
        let default = (profile.host.clone(), profile.port);
        let mut clients = HashMap::new();
        if profile.deployment == Deployment::Standalone {
            clients.insert(default.clone(), raw);
        }
        let audit = crate::audit::Audit::open(&profile)?;
        let connect_id = audit.id();
        audit.record(connect_id, "CONNECT", "started", Some(0))?;
        let this = Self {
            safety: crate::safety::Safety::default(),
            audit,
            profile,
            state: Arc::new(Mutex::new(State {
                clients,
                sockets: HashMap::new(),
                nodes: vec![],
                default,
                refreshed: Instant::now(),
                warning: None,
            })),
        };
        let result = this.refresh().await;
        this.audit.record(
            connect_id,
            "CONNECT",
            if result.is_ok() { "success" } else { "failure" },
            Some(0),
        )?;
        result?;
        Ok(this)
    }

    pub fn read_only(&self) -> bool {
        self.safety.read_only(&self.profile)
    }
    pub fn remaining(&self) -> u64 {
        self.safety.remaining()
    }
    pub fn unlock(&self, confirmation: &str) -> Result<()> {
        // Persist intent before granting the lease, and revoke if completion cannot be recorded.
        let id = self.audit.id();
        self.audit.record(id, "WRITE_UNLOCK", "started", Some(0))?;
        let result = self.safety.unlock(&self.profile, confirmation);
        if let Err(e) = self.audit.record(
            id,
            "WRITE_UNLOCK",
            if result.is_ok() { "success" } else { "denied" },
            Some(0),
        ) {
            self.safety.lock();
            return Err(e);
        }
        result
    }
    pub fn lock(&self) -> Result<()> {
        self.safety.lock();
        self.audit
            .record(self.audit.id(), "WRITE_LOCK", "success", Some(0))
    }
    fn guard(&self, read: bool) -> RedisResult<()> {
        if !read && self.read_only() {
            return Err(error(
                "Read-only profile or production write lease expired; command rejected",
            ));
        }
        Ok(())
    }
    fn audit_result<T>(
        &self,
        id: u64,
        action: &str,
        count: Option<usize>,
        result: &RedisResult<T>,
        pipeline: bool,
    ) -> RedisResult<()> {
        let outcome = match result {
            Ok(_) => "success",
            Err(e) if e.to_string().contains("Read-only") => "denied",
            Err(e) if pipeline || e.is_io_error() || e.to_string().contains("outcome unknown") => {
                "unknown"
            }
            Err(_) => "failure",
        };
        self.audit.record(id, action, outcome, count).map_err(|_| {
            error(format!(
                "Operation outcome: {outcome}; audit completion failed. Do not automatically retry."
            ))
        })
    }

    async fn connection(
        &self,
        state: &mut State,
        ep: &Endpoint,
    ) -> RedisResult<MultiplexedConnection> {
        if let Some(c) = state.sockets.get(ep) {
            return Ok(c.clone());
        }
        if !state.clients.contains_key(ep) {
            let mut profile = self.profile.clone();
            profile.host = ep.0.clone();
            profile.port = ep.1;
            let raw = build_client(&profile, None)
                .await
                .map_err(|e| error(e.to_string()))?;
            state.clients.insert(ep.clone(), raw);
        }
        let c = socket(&state.clients[ep]).await?;
        state.sockets.insert(ep.clone(), c.clone());
        Ok(c)
    }

    async fn discover(&self, state: &mut State) -> RedisResult<()> {
        // A long seed list cannot leave a caller waiting indefinitely.
        tokio::time::timeout(Duration::from_secs(10), self.discover_inner(state))
            .await
            .map_err(|_| error("Topology discovery timed out"))?
    }

    async fn discover_inner(&self, state: &mut State) -> RedisResult<()> {
        if self.profile.deployment == Deployment::Standalone {
            let ep = state.default.clone();
            let mut c = self.connection(state, &ep).await?;
            redis::cmd("PING").query_async::<()>(&mut c).await?;
            return Ok(());
        }
        let mut seeds = vec![(self.profile.host.clone(), self.profile.port)];
        seeds.extend(self.profile.seeds.iter().filter_map(|s| endpoint(s).ok()));
        if self.profile.deployment == Deployment::Cluster {
            seeds.extend(state.nodes.iter().map(Node::endpoint));
        }
        let mut failures = Vec::new();
        let mut visited = std::collections::HashSet::new();
        for ep in seeds {
            if !visited.insert(ep.clone()) {
                continue;
            }
            let result: RedisResult<Vec<Node>> = async {
                if self.profile.deployment == Deployment::Sentinel {
                    let mut sentinel = self.profile.clone();
                    sentinel.host = ep.0.clone();
                    sentinel.port = ep.1;
                    sentinel.db = 0;
                    sentinel.username = self.profile.sentinel_username.clone();
                    sentinel.password = self.profile.sentinel_password.clone();
                    sentinel.use_keychain = false;
                    let raw = build_client(&sentinel, None)
                        .await
                        .map_err(|e| error(e.to_string()))?;
                    let mut c = socket(&raw).await?;
                    let address: Option<(String, u16)> = redis::cmd("SENTINEL")
                        .arg("get-master-addr-by-name")
                        .arg(&self.profile.sentinel_master)
                        .query_async(&mut c)
                        .await?;
                    let address =
                        address.ok_or_else(|| error("Sentinel does not know this service"))?;
                    // Always establish a fresh socket on rediscovery and verify the role.
                    state.sockets.remove(&address);
                    let mut primary = self.connection(state, &address).await?;
                    let role: Vec<Value> = redis::cmd("ROLE").query_async(&mut primary).await?;
                    if role.first().map(scalar).as_deref() != Some("master") {
                        return Err(error("Sentinel candidate is not a primary"));
                    }
                    Ok(vec![Node {
                        id: self.profile.sentinel_master.clone(),
                        host: address.0,
                        port: address.1,
                        primary: true,
                        slots: vec![],
                    }])
                } else {
                    let mut c = self.connection(state, &ep).await?;
                    let reply: Value = redis::cmd("CLUSTER")
                        .arg("SLOTS")
                        .query_async(&mut c)
                        .await?;
                    parse_slots(reply, &ep.0)
                }
            }
            .await;
            match result {
                Ok(nodes) if !nodes.is_empty() => {
                    state.default = nodes
                        .iter()
                        .find(|n| n.primary && n.endpoint() == ep)
                        .or_else(|| nodes.iter().find(|n| n.primary))
                        .ok_or_else(|| error("No primary discovered"))?
                        .endpoint();
                    state.nodes = nodes;
                    state.refreshed = Instant::now();
                    state.warning = None;
                    return Ok(());
                }
                Ok(_) => failures.push(format!("{}:{}: empty topology", ep.0, ep.1)),
                Err(e) => {
                    state.sockets.remove(&ep);
                    failures.push(format!("{}:{}: {e}", ep.0, ep.1));
                }
            }
        }
        Err(error(format!("Discovery failed: {}", failures.join("; "))))
    }

    pub async fn refresh(&self) -> RedisResult<()> {
        let mut state = self.state.lock().await;
        let result = self.discover(&mut state).await;
        if let Err(e) = &result {
            state.warning = Some(e.to_string());
        }
        result
    }
    pub async fn nodes(&self) -> Vec<Node> {
        self.state.lock().await.nodes.clone()
    }
    pub async fn warning(&self) -> Option<String> {
        self.state.lock().await.warning.clone()
    }
    pub async fn primaries(&self) -> Vec<Endpoint> {
        let state = self.state.lock().await;
        if state.nodes.is_empty() {
            vec![state.default.clone()]
        } else {
            state
                .nodes
                .iter()
                .filter(|n| n.primary)
                .map(Node::endpoint)
                .collect()
        }
    }
    /// Node-addressed command. Callers bypass slot routing, never the safety
    /// guard or the audit log: a future write here is enforced like any other.
    pub async fn direct(&self, ep: &Endpoint, cmd: &Cmd) -> RedisResult<Value> {
        let (action, count) = crate::audit::command(cmd);
        let audited = !read_route(cmd).0 || action == "EXPORT_READ";
        if !audited {
            return self.direct_inner(ep, cmd).await;
        }
        let id = self.audit.id();
        self.audit
            .record(id, action, "started", count)
            .map_err(|_| error("Audit unavailable; command was not sent"))?;
        let result = self.direct_inner(ep, cmd).await;
        self.audit_result(id, action, count, &result, false)?;
        result
    }
    async fn direct_inner(&self, ep: &Endpoint, cmd: &Cmd) -> RedisResult<Value> {
        self.guard(read_route(cmd).0)?;
        let mut state = self.state.lock().await;
        let mut c = self.connection(&mut state, ep).await?;
        drop(state);
        let result = c
            .req_packed_command(cmd)
            .await
            .and_then(Value::extract_error);
        if result.is_err() {
            self.state.lock().await.sockets.remove(ep);
        }
        result
    }
    pub async fn metadata(&self, ep: &Endpoint, names: &[String]) -> RedisResult<Vec<Value>> {
        let mut pipeline = redis::pipe();
        for name in names {
            pipeline.cmd("TYPE").arg(name);
            pipeline.cmd("TTL").arg(name);
        }
        // Metadata is read-only by construction; the guard keeps that true if
        // the command list ever grows.
        if pipeline.cmd_iter().any(|c| !read_route(c).0) {
            self.guard(false)?;
        }
        let mut state = self.state.lock().await;
        let mut c = self.connection(&mut state, ep).await?;
        drop(state);
        let result = c.req_packed_commands(&pipeline, 0, names.len() * 2).await;
        if result.is_err() {
            self.state.lock().await.sockets.remove(ep);
        }
        result
    }

    async fn request(&self, cmd: &Cmd) -> RedisResult<Value> {
        let (action, count) = crate::audit::command(cmd);
        let audited = !read_route(cmd).0 || action == "EXPORT_READ";
        if !audited {
            return self.request_inner(cmd).await;
        }
        let id = self.audit.id();
        self.audit
            .record(id, action, "started", count)
            .map_err(|_| error("Audit unavailable; command was not sent"))?;
        let result = self.request_inner(cmd).await;
        self.audit_result(id, action, count, &result, false)?;
        result
    }
    async fn request_inner(&self, cmd: &Cmd) -> RedisResult<Value> {
        let (read, key) = read_route(cmd);
        self.guard(read)?;
        let mut state = self.state.lock().await;
        if self.profile.deployment != Deployment::Standalone
            && state.refreshed.elapsed() >= Duration::from_secs(30)
            && let Err(e) = self.discover(&mut state).await
        {
            state.warning = Some(e.to_string());
            // A Sentinel primary may have changed: never use its stale address.
            if self.profile.deployment == Deployment::Sentinel {
                return Err(e);
            }
        }
        let route = |state: &State| {
            key.as_ref()
                .and_then(|k| {
                    let slot = key_slot(k);
                    state
                        .nodes
                        .iter()
                        .find(|n| {
                            n.primary && n.slots.iter().any(|(a, b)| (*a..=*b).contains(&slot))
                        })
                        .map(Node::endpoint)
                })
                .unwrap_or_else(|| state.default.clone())
        };
        let mut ep = route(&state);
        let mut asking = false;
        for attempt in 0..4 {
            let result = async {
                if asking {
                    // Dedicated socket keeps ASKING and the redirected command adjacent.
                    let raw = state.clients.get(&ep).cloned();
                    let raw = match raw {
                        Some(raw) => raw,
                        None => {
                            let mut p = self.profile.clone();
                            p.host = ep.0.clone();
                            p.port = ep.1;
                            build_client(&p, None)
                                .await
                                .map_err(|e| error(e.to_string()))?
                        }
                    };
                    let mut c = socket(&raw).await?;
                    redis::cmd("ASKING").query_async::<()>(&mut c).await?;
                    c.req_packed_command(cmd)
                        .await
                        .and_then(Value::extract_error)
                } else {
                    let mut c = self.connection(&mut state, &ep).await?;
                    self.guard(read)?;
                    c.req_packed_command(cmd)
                        .await
                        .and_then(Value::extract_error)
                }
            }
            .await;
            match result {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if self.profile.deployment == Deployment::Cluster
                        && let Some((address, slot)) = e.redirect_node()
                    {
                        if slot >= 16384 {
                            return Err(error("Invalid redirect slot"));
                        }
                        let address = if address.starts_with(':') {
                            format!("{}{address}", ep.0)
                        } else {
                            address.to_string()
                        };
                        let target = endpoint(&address).map_err(|e| error(e.to_string()))?;
                        asking = e.kind() == redis::ErrorKind::Server(redis::ServerErrorKind::Ask);
                        if !asking {
                            if let Err(refresh) = self.discover(&mut state).await {
                                state.warning = Some(refresh.to_string());
                            }
                            // A MOVED is authoritative for this slot even if discovery is unavailable.
                            for n in &mut state.nodes {
                                let mut ranges = Vec::new();
                                for &(a, b) in &n.slots {
                                    if slot < a || slot > b {
                                        ranges.push((a, b));
                                    } else {
                                        if a < slot {
                                            ranges.push((a, slot - 1));
                                        }
                                        if slot < b {
                                            ranges.push((slot + 1, b));
                                        }
                                    }
                                }
                                n.slots = ranges;
                            }
                            if let Some(n) = state.nodes.iter_mut().find(|n| n.endpoint() == target)
                            {
                                n.primary = true;
                                n.slots.push((slot, slot));
                            } else {
                                state.nodes.push(Node {
                                    id: "MOVED".into(),
                                    host: target.0.clone(),
                                    port: target.1,
                                    primary: true,
                                    slots: vec![(slot, slot)],
                                });
                            }
                        }
                        ep = target;
                        continue;
                    }
                    let transient = e.is_io_error()
                        || matches!(
                            e.kind(),
                            redis::ErrorKind::Server(
                                redis::ServerErrorKind::ReadOnly
                                    | redis::ServerErrorKind::ClusterDown
                                    | redis::ServerErrorKind::TryAgain
                            )
                        );
                    if !transient {
                        return Err(e);
                    }
                    state.sockets.remove(&ep);
                    if !read {
                        return Err(error(format!(
                            "Write outcome unknown; command was not retried: {e}"
                        )));
                    }
                    if attempt == 3 {
                        return Err(e);
                    }
                    tokio::time::sleep(Duration::from_millis(50 * (1 << attempt))).await;
                    if self.profile.deployment != Deployment::Standalone
                        && let Err(refresh) = self.discover(&mut state).await
                    {
                        state.warning = Some(refresh.to_string());
                        if self.profile.deployment == Deployment::Sentinel {
                            return Err(refresh);
                        }
                    }
                    ep = route(&state);
                    asking = false;
                }
            }
        }
        Err(error(
            "Redirect limit exceeded; refresh topology and try again",
        ))
    }
}

impl Transport {
    async fn pipeline_inner(
        &self,
        pipeline: &redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        if self.profile.deployment != Deployment::Standalone {
            if pipeline.is_transaction() || pipeline.cmd_iter().any(|c| !read_route(c).0) {
                return Err(error(
                    "Transactions and writes are disabled for discovered deployments",
                ));
            }
            let mut values = Vec::new();
            for cmd in pipeline.cmd_iter() {
                values.push(self.request(cmd).await?);
            }
            return Ok(values.into_iter().skip(offset).take(count).collect());
        }
        if self.read_only() && pipeline.cmd_iter().any(|c| !read_route(c).0) {
            return Err(error("Read-only profile: pipeline rejected"));
        }
        let mut state = self.state.lock().await;
        let ep = state.default.clone();
        let mut c = self.connection(&mut state, &ep).await?;
        self.guard(pipeline.cmd_iter().all(|cmd| read_route(cmd).0))?;
        let result = c
            .req_packed_commands(pipeline, offset, count)
            .await
            .and_then(|values| values.into_iter().map(Value::extract_error).collect());
        if let Err(e) = &result {
            state.sockets.remove(&ep);
            if e.is_io_error() {
                if !pipeline.is_transaction() && pipeline.cmd_iter().all(|cmd| read_route(cmd).0) {
                    drop(state);
                    let mut values = Vec::new();
                    for cmd in pipeline.cmd_iter() {
                        values.push(self.request(cmd).await?);
                    }
                    return Ok(values.into_iter().skip(offset).take(count).collect());
                }
                return Err(error(format!(
                    "Pipeline outcome unknown; no commands retried: {e}"
                )));
            }
        }
        result
    }
}

impl ConnectionLike for Transport {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        Box::pin(async move { self.request(cmd).await })
    }
    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        Box::pin(async move {
            let writes = pipeline.cmd_iter().any(|c| !read_route(c).0);
            if !writes {
                return self.pipeline_inner(pipeline, offset, count).await;
            }
            let id = self.audit.id();
            let targets = pipeline.cmd_iter().try_fold(0usize, |sum, c| {
                crate::audit::command(c).1.and_then(|n| sum.checked_add(n))
            });
            self.audit
                .record(id, "PIPELINE", "started", targets)
                .map_err(|_| error("Audit unavailable; pipeline was not sent"))?;
            let result = self.pipeline_inner(pipeline, offset, count).await;
            self.audit_result(id, "PIPELINE", targets, &result, true)?;
            result
        })
    }
    fn get_db(&self) -> i64 {
        self.profile.db
    }
}

// An explicit allowlist prevents module/administrative writes from bypassing
// read-only enforcement. Unknown commands are never automatically replayed.
pub(super) fn read_route(cmd: &Cmd) -> (bool, Option<Vec<u8>>) {
    let args: Vec<&[u8]> = cmd
        .args_iter()
        .filter_map(|a| match a {
            redis::Arg::Simple(a) => Some(a),
            _ => None,
        })
        .collect();
    let head = args
        .first()
        .map(|a| String::from_utf8_lossy(a).to_ascii_uppercase())
        .unwrap_or_default();
    let sub = args
        .get(1)
        .map(|a| String::from_utf8_lossy(a).to_ascii_uppercase())
        .unwrap_or_default();
    let index = match head.as_str() {
        "GET" | "TYPE" | "TTL" | "PTTL" | "DUMP" | "STRLEN" | "GETRANGE" | "HLEN" | "HSCAN"
        | "HGET" | "HGETALL" | "LLEN" | "LRANGE" | "SCARD" | "SSCAN" | "SMEMBERS" | "ZCARD"
        | "ZRANGE" | "ZSCAN" | "XLEN" | "XRANGE" | "XREVRANGE" | "XPENDING" | "JSON.GET"
        | "JSON.TYPE" | "TS.GET" | "TS.RANGE" | "TS.INFO" => Some(1),
        "MEMORY" if sub == "USAGE" => Some(2),
        "OBJECT" if matches!(sub.as_str(), "FREQ" | "IDLETIME" | "ENCODING" | "REFCOUNT") => {
            Some(2)
        }
        "XINFO" if matches!(sub.as_str(), "GROUPS" | "CONSUMERS" | "STREAM") => Some(2),
        "PING" | "INFO" | "DBSIZE" | "SCAN" | "COMMAND" | "ROLE" | "TIME" => None,
        "CLUSTER"
            if matches!(
                sub.as_str(),
                "INFO" | "SLOTS" | "SHARDS" | "NODES" | "KEYSLOT"
            ) =>
        {
            None
        }
        "CONFIG" if sub == "GET" => None,
        "CLIENT" if matches!(sub.as_str(), "LIST" | "INFO" | "ID") => None,
        "SLOWLOG" if matches!(sub.as_str(), "GET" | "LEN") => None,
        "LATENCY" if matches!(sub.as_str(), "LATEST" | "HISTORY" | "DOCTOR") => None,
        "MODULE" if sub == "LIST" => None,
        _ => return (false, None),
    };
    (true, index.and_then(|i| args.get(i).map(|a| a.to_vec())))
}

pub fn key_slot(key: &[u8]) -> u16 {
    let mut hash = key;
    if let Some(start) = key.iter().position(|b| *b == b'{')
        && let Some(end) = key[start + 1..].iter().position(|b| *b == b'}')
        && end > 0
    {
        hash = &key[start + 1..start + 1 + end];
    }
    let mut crc = 0u16;
    for byte in hash {
        crc ^= (*byte as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc % 16384
}

fn parse_slots(value: Value, source: &str) -> RedisResult<Vec<Node>> {
    let Value::Array(rows) = value else {
        return Err(error("Invalid CLUSTER SLOTS response"));
    };
    let mut nodes: Vec<Node> = Vec::new();
    for row in rows {
        let Value::Array(fields) = row else {
            return Err(error("Invalid slot row"));
        };
        if fields.len() < 3 {
            return Err(error("Missing slot primary"));
        }
        let start: u16 = redis::from_redis_value(fields[0].clone())?;
        let end: u16 = redis::from_redis_value(fields[1].clone())?;
        if start > end || end >= 16384 {
            return Err(error("Invalid slot range"));
        }
        for (i, value) in fields[2..].iter().enumerate() {
            let Value::Array(parts) = value else {
                return Err(error("Invalid node"));
            };
            if parts.len() < 2 {
                return Err(error("Missing node address"));
            }
            let host = match &parts[0] {
                Value::Nil => source.to_string(),
                v => {
                    let s = scalar(v);
                    if s.is_empty() { source.to_string() } else { s }
                }
            };
            if host == "?" {
                return Err(error("Unknown advertised node endpoint"));
            }
            let port = redis::from_redis_value(parts[1].clone())?;
            if port == 0 {
                return Err(error("Invalid node port"));
            }
            let id = parts.get(2).map(scalar).unwrap_or_default();
            if let Some(n) = nodes.iter_mut().find(|n| n.host == host && n.port == port) {
                if i == 0 {
                    n.primary = true;
                    n.slots.push((start, end));
                }
            } else {
                nodes.push(Node {
                    id,
                    host,
                    port,
                    primary: i == 0,
                    slots: if i == 0 { vec![(start, end)] } else { vec![] },
                });
            }
        }
    }
    Ok(nodes)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn redis_hash_tags_and_crc() {
        assert_eq!(key_slot(b"123456789"), 0x31c3);
        assert_eq!(key_slot(b"a{user}1"), key_slot(b"b{user}2"));
        assert_eq!(key_slot(b"a{user}1"), key_slot(b"user"));
        assert_ne!(key_slot(b"a{}x{user}"), key_slot(b"user"));
    }
    #[test]
    fn writes_and_unknown_commands_are_not_retryable() {
        for (head, sub) in [
            ("SET", "k"),
            ("EVAL", "return 1"),
            ("CONFIG", "SET"),
            ("CLUSTER", "FAILOVER"),
            ("XREADGROUP", "GROUP"),
            ("MODULE", "LOAD"),
        ] {
            assert!(!read_route(redis::cmd(head).arg(sub)).0);
        }
        assert_eq!(
            read_route(redis::cmd("MEMORY").arg("USAGE").arg("k")),
            (true, Some(b"k".to_vec()))
        );
    }
    #[test]
    fn addresses_support_ipv6() {
        assert_eq!(endpoint("[::1]:6379").unwrap(), ("::1".into(), 6379));
        assert!(endpoint("bad").is_err());
        assert!(endpoint("host:0").is_err());
    }
}
