//! Discovery and routing. A write is only sent again when the server proves it
//! never ran: a redirect, or a rejection such as `READONLY` or `TRYAGAIN`. A
//! lost connection leaves a write's outcome unknown, and it is never replayed.
use super::*;
use crate::config::Deployment;
use redis::{Cmd, RedisError, RedisFuture, RedisResult, Value, aio::ConnectionLike};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

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

/// Shared routing state and the network calls made with it.
///
/// The state lock is a plain mutex and is never held across an `.await`, so a
/// request to a node that hangs cannot hold up requests to the others. Each
/// call copies what it needs out of the state (a socket, the routing table),
/// releases it, talks to the network, then takes it again to record what it
/// learned. Discovery is the one thing callers wait on each other for: it runs
/// one at a time. A caller that saw a request fail only takes the result of a
/// discovery that started after it arrived; any other caller also takes one
/// that was already running.
#[derive(Clone)]
pub(super) struct Transport {
    profile: Connection,
    safety: crate::safety::Safety,
    audit: crate::audit::Audit,
    state: Arc<Mutex<State>>,
    /// Held for as long as one discovery runs.
    discovery: Arc<tokio::sync::Mutex<()>>,
    /// Primaries the last discovery found, readable without the lock.
    primary_count: Arc<std::sync::atomic::AtomicUsize>,
    /// Socket activity is measured in milliseconds from here.
    epoch: Instant,
    /// How long, in milliseconds, a cached socket may go without proving it
    /// is alive before a write on it is preceded by a `PING`.
    idle_ping_ms: Arc<AtomicU64>,
    /// How often, in milliseconds, a feed that follows every primary checks
    /// the topology for primaries that came or went.
    feed_check_ms: Arc<AtomicU64>,
}
/// Load balancers and firewalls drop connections that sit quiet for a few
/// minutes without telling either end. A write sent into such a socket is
/// lost with an unknown outcome, so a socket unused for this long is checked
/// with a `PING` first.
const IDLE_PING: Duration = Duration::from_secs(30);
/// How often a merged feed looks for primaries that were added or demoted.
const FEED_CHECK: Duration = Duration::from_secs(20);
struct State {
    clients: HashMap<Endpoint, redis::Client>,
    sockets: HashMap<Endpoint, Socket>,
    nodes: Vec<Node>,
    default: Endpoint,
    refreshed: Instant,
    warning: Option<String>,
    /// Sockets opened so far; the latest one's number is its id.
    opened: u64,
    /// Discoveries started so far; the latest one's number is its generation.
    started: u64,
    /// Discoveries finished so far, the generation of the last one and how
    /// it went.
    discovered: u64,
    last_generation: u64,
    last_discovery: RedisResult<()>,
    /// The last discovery failed. A Sentinel profile then discovers again
    /// before its next request instead of trusting the address it had.
    must_rediscover: bool,
    /// A write was lost after it was sent, when `started` stood at this
    /// number. Set before the writer rediscovers, so writers that arrive
    /// meanwhile, or come after a writer that was cancelled, wait for a
    /// discovery that started after the loss instead of routing by the
    /// address the write was lost on.
    lost_at: Option<u64>,
}
/// A cached multiplexed connection. The id lets a caller whose request failed
/// on it drop exactly that socket, not a newer one another caller opened.
#[derive(Clone)]
struct Socket {
    id: u64,
    conn: MultiplexedConnection,
    /// Shared by every copy.
    health: Arc<Health>,
}
/// What is known about whether a socket still works.
struct Health {
    /// When it last proved alive, in milliseconds from the transport's epoch:
    /// opened, or answered a `PING` or a command.
    last: AtomicU64,
    /// Bumped each time it proves alive, so a writer that waited for another
    /// writer's `PING` can tell it succeeded.
    proofs: AtomicU64,
    /// A `PING` on it lost the connection.
    dead: std::sync::atomic::AtomicBool,
    /// Held while one writer checks it, so writers that take an idle socket
    /// at once share a single `PING`.
    check: tokio::sync::Mutex<()>,
}
impl Socket {
    /// Record that the socket just answered.
    fn alive(&self, now: u64) {
        self.health.last.store(now, Ordering::Relaxed);
        self.health.proofs.fetch_add(1, Ordering::Relaxed);
    }
}
/// The guard's refusals. Audit events match them exactly, by kind and text,
/// so no server reply or script error can pass for one.
const DENIED: &str = "Read-only profile or production write lease expired; command rejected";
const PIPELINE_DENIED: &str = "Read-only profile: pipeline rejected";

fn own_error(e: &RedisError) -> bool {
    e.kind() == redis::ErrorKind::InvalidClientConfig
}

fn denied(e: &RedisError) -> bool {
    own_error(e) && matches!(e.detail(), Some(DENIED | PIPELINE_DENIED))
}

/// Refusals made before anything was sent, because the route could not be
/// trusted. They are audited as failures even for a batch or a script, whose
/// other errors leave the outcome unknown. Matched like the guard's refusals:
/// by kind, and by exact text or the `UNSENT` prefix.
const WRITE_UNSETTLED: &str =
    "The topology kept changing while this write waited; nothing was sent, so try again";
const BATCH_UNSETTLED: &str =
    "The topology kept changing while this batch waited; nothing was sent, so try again";
const SENTINEL_UNSETTLED: &str =
    "The Sentinel primary is being rediscovered after a failure; nothing was sent, so try again";
const UNSENT: &str = "Nothing was sent: ";

fn unsent(e: &RedisError) -> bool {
    own_error(e)
        && e.detail().is_some_and(|d| {
            matches!(d, WRITE_UNSETTLED | BATCH_UNSETTLED | SENTINEL_UNSETTLED)
                || d.starts_with(UNSENT)
        })
}

/// `e`, a failure to confirm the route, as a refusal that sent nothing.
fn not_sent(e: RedisError) -> RedisError {
    if unsent(&e) {
        return e;
    }
    let reason = match e.detail() {
        Some(detail) if own_error(&e) => detail.to_string(),
        _ => e.to_string(),
    };
    error(format!("{UNSENT}{reason}"))
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
                opened: 0,
                started: 0,
                discovered: 0,
                last_generation: 0,
                last_discovery: Ok(()),
                must_rediscover: false,
                lost_at: None,
            })),
            discovery: Arc::default(),
            primary_count: Arc::new(std::sync::atomic::AtomicUsize::new(1)),
            epoch: Instant::now(),
            idle_ping_ms: Arc::new(AtomicU64::new(IDLE_PING.as_millis() as u64)),
            feed_check_ms: Arc::new(AtomicU64::new(FEED_CHECK.as_millis() as u64)),
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

    /// The shared state, for a lookup or an update. Never held across an
    /// `.await`: the guard is not `Send`, so the compiler refuses that.
    fn state(&self) -> MutexGuard<'_, State> {
        // Nothing panics while holding the lock, but a poisoned one must not
        // take every later request down with it.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
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
            return Err(error(DENIED));
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
        opaque: bool,
    ) -> RedisResult<()> {
        let outcome = match result {
            Ok(_) => "success",
            Err(e) if denied(e) => "denied",
            Err(e) if unsent(e) => "failure",
            // A batch that fails may have run part of itself, and a script or
            // module command may have written before its error.
            Err(_) if pipeline || opaque => "unknown",
            Err(e)
                if e.is_io_error()
                    || (own_error(e) && e.to_string().contains("outcome unknown")) =>
            {
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

    /// The cached socket for `ep`, or a new one. Two callers connecting at
    /// once both open one; the first to finish is cached and the other one's
    /// is closed, so both use the same socket.
    async fn connection(&self, ep: &Endpoint) -> RedisResult<Socket> {
        if let Some(socket) = self.cached(ep) {
            return Ok(socket);
        }
        let client = self.node_client(ep).await?;
        let conn = socket(&client).await?;
        let mut state = self.state();
        if let Some(existing) = state.sockets.get(ep) {
            return Ok(existing.clone());
        }
        state.opened += 1;
        let socket = Socket {
            id: state.opened,
            conn,
            health: Arc::new(Health {
                last: AtomicU64::new(self.now_ms()),
                proofs: AtomicU64::new(0),
                dead: Default::default(),
                check: Default::default(),
            }),
        };
        state.sockets.insert(ep.clone(), socket.clone());
        Ok(socket)
    }

    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    fn cached(&self, ep: &Endpoint) -> Option<Socket> {
        self.state().sockets.get(ep).cloned()
    }

    /// Record the reply `result` got on `socket`: any reply but a lost
    /// connection shows the socket works.
    fn answered<T>(&self, socket: &Socket, result: &RedisResult<T>) {
        if !result.as_ref().is_err_and(RedisError::is_io_error) {
            socket.alive(self.now_ms());
        }
    }

    /// A socket a write can be sent on. One that has not proved alive for a
    /// while is asked for a `PING` first: if that fails, the connection was
    /// already dead and the write has not been sent, so it goes out once on a
    /// fresh socket instead of being lost with an unknown outcome. Any other
    /// reply means the connection is alive. Writers that take the same idle
    /// socket at once wait for one `PING` and all go by its answer.
    async fn for_write(&self, ep: &Endpoint, socket: Socket) -> RedisResult<Socket> {
        let health = socket.health.clone();
        let proofs = health.proofs.load(Ordering::Relaxed);
        let quiet = self
            .now_ms()
            .saturating_sub(health.last.load(Ordering::Relaxed));
        if quiet < self.idle_ping_ms.load(Ordering::Relaxed) && !health.dead.load(Ordering::Relaxed)
        {
            return Ok(socket);
        }
        let checking = health.check.lock().await;
        if health.dead.load(Ordering::Relaxed) {
            drop(checking);
            return self.connection(ep).await;
        }
        if health.proofs.load(Ordering::Relaxed) != proofs {
            // It answered someone while this writer waited.
            return Ok(socket);
        }
        let mut c = socket.conn.clone();
        match redis::cmd("PING").query_async::<Value>(&mut c).await {
            Err(e) if e.is_io_error() => {
                health.dead.store(true, Ordering::Relaxed);
                self.forget(ep, socket.id);
                drop(checking);
                self.connection(ep).await
            }
            _ => {
                socket.alive(self.now_ms());
                Ok(socket)
            }
        }
    }

    /// Check sockets idle for longer than `after` before writing on them.
    pub fn idle_ping_after(&self, after: Duration) {
        self.idle_ping_ms
            .store(after.as_millis() as u64, Ordering::Relaxed);
    }

    /// How often a feed on every primary checks for primaries that came or went.
    pub fn feed_check_every(&self, every: Duration) {
        self.feed_check_ms
            .store(every.as_millis() as u64, Ordering::Relaxed);
    }
    pub(super) fn feed_check(&self) -> Duration {
        Duration::from_millis(self.feed_check_ms.load(Ordering::Relaxed))
    }

    /// Drop the cached socket for `ep` after a failure on it, if it is still
    /// the socket `used` names. A request that failed before it had a socket
    /// drops nothing: whatever is cached belongs to someone else.
    fn forget(&self, ep: &Endpoint, used: u64) {
        let mut state = self.state();
        if state.sockets.get(ep).is_some_and(|s| s.id == used) {
            state.sockets.remove(ep);
        }
    }

    /// Discover the topology, or wait for the discovery already running and
    /// take its result. Discoveries run one at a time, so a result can only
    /// ever be replaced by one that started after it.
    ///
    /// With `since`, a discovery that was already under way may have asked
    /// before something failed. Only a result from a discovery numbered
    /// above `since` (one that started later) will do then. Otherwise any
    /// discovery that finished after the caller arrived will.
    async fn discover(&self, since: Option<u64>) -> RedisResult<()> {
        let finished = self.state().discovered;
        let _running = self.discovery.lock().await;
        let generation = {
            let mut state = self.state();
            let fresh = match since {
                Some(since) => state.last_generation > since,
                None => state.discovered != finished,
            };
            if fresh {
                return state.last_discovery.clone();
            }
            state.started += 1;
            state.started
        };
        // A long seed list cannot leave a caller waiting indefinitely.
        let result = tokio::time::timeout(Duration::from_secs(10), self.discover_inner())
            .await
            .unwrap_or_else(|_| Err(error("Topology discovery timed out")));
        let mut state = self.state();
        if let Ok(Some((nodes, default))) = &result {
            let primaries = nodes.iter().filter(|n| n.primary).count();
            self.primary_count
                .store(primaries.max(1), std::sync::atomic::Ordering::Relaxed);
            state.default = default.clone();
            state.nodes = nodes.clone();
            state.refreshed = Instant::now();
            state.warning = None;
        }
        let result = result.map(|_| ());
        state.discovered += 1;
        state.last_generation = generation;
        state.must_rediscover = result.is_err();
        // A discovery that started after the loss settles it. If it failed,
        // `must_rediscover` keeps a Sentinel profile asking again.
        if state.lost_at.is_some_and(|lost| generation > lost) {
            state.lost_at = None;
        }
        state.last_discovery = result.clone();
        result
    }

    /// Discover after a request failed, and keep a failure as the topology
    /// warning.
    async fn rediscover(&self) -> RedisResult<()> {
        let started = self.state().started;
        self.discover_noting(Some(started)).await
    }

    /// Record that a write was lost after it was sent, then rediscover. The
    /// mark is set before anything is awaited, so it holds even if the
    /// caller is cancelled while discovery runs.
    async fn rediscover_after_lost_write(&self) {
        if self.profile.deployment == Deployment::Standalone {
            return;
        }
        let started = {
            let mut state = self.state();
            let started = state.started;
            state.lost_at = Some(state.lost_at.map_or(started, |l| l.max(started)));
            started
        };
        let _ = self.discover_noting(Some(started)).await;
    }

    /// A write was lost and no discovery that started after it has finished
    /// yet, or, on a Sentinel profile, the last discovery failed. A write
    /// must not go by the current address then.
    fn unsettled(&self) -> bool {
        let state = self.state();
        state.lost_at.is_some()
            || (state.must_rediscover && self.profile.deployment == Deployment::Sentinel)
    }

    async fn discover_noting(&self, since: Option<u64>) -> RedisResult<()> {
        let result = self.discover(since).await;
        if let Err(e) = &result {
            self.state().warning = Some(e.to_string());
        }
        result
    }

    /// The nodes and the default endpoint, for everything but a standalone
    /// profile, which only checks that its server answers.
    async fn discover_inner(&self) -> RedisResult<Option<(Vec<Node>, Endpoint)>> {
        if self.profile.deployment == Deployment::Standalone {
            let ep = self.state().default.clone();
            let socket = self.connection(&ep).await?;
            let mut c = socket.conn.clone();
            let result = redis::cmd("PING").query_async::<()>(&mut c).await;
            self.answered(&socket, &result);
            if result.is_err() {
                self.forget(&ep, socket.id);
            }
            result?;
            return Ok(None);
        }
        let mut seeds = self.seeds();
        if self.profile.deployment == Deployment::Cluster {
            seeds.extend(self.state().nodes.iter().map(Node::endpoint));
        }
        let mut failures = Vec::new();
        let mut visited = std::collections::HashSet::new();
        for ep in seeds {
            if !visited.insert(ep.clone()) {
                continue;
            }
            let mut used = None;
            let result: RedisResult<Vec<Node>> = async {
                if self.profile.deployment == Deployment::Sentinel {
                    let address = self.sentinel_names(&ep).await?;
                    self.verify_primary(&address).await?;
                    Ok(vec![Node {
                        id: self.profile.sentinel_master.clone(),
                        host: address.0,
                        port: address.1,
                        primary: true,
                        slots: vec![],
                    }])
                } else {
                    let socket = self.connection(&ep).await?;
                    used = Some(socket.id);
                    let mut c = socket.conn.clone();
                    let reply = redis::cmd("CLUSTER")
                        .arg("SLOTS")
                        .query_async::<Value>(&mut c)
                        .await;
                    self.answered(&socket, &reply);
                    parse_slots(reply?, &ep.0)
                }
            }
            .await;
            match result {
                Ok(nodes) if !nodes.is_empty() => {
                    let default = nodes
                        .iter()
                        .find(|n| n.primary && n.endpoint() == ep)
                        .or_else(|| nodes.iter().find(|n| n.primary))
                        .ok_or_else(|| error("No primary discovered"))?
                        .endpoint();
                    return Ok(Some((nodes, default)));
                }
                Ok(_) => failures.push(format!("{}:{}: empty topology", ep.0, ep.1)),
                Err(e) => {
                    if let Some(id) = used {
                        self.forget(&ep, id);
                    }
                    failures.push(format!("{}:{}: {e}", ep.0, ep.1));
                }
            }
        }
        Err(error(format!("Discovery failed: {}", failures.join("; "))))
    }

    /// The profile's address, then its other seeds.
    fn seeds(&self) -> Vec<Endpoint> {
        let mut seeds = vec![(self.profile.host.clone(), self.profile.port)];
        seeds.extend(self.profile.seeds.iter().filter_map(|s| endpoint(s).ok()));
        seeds
    }

    /// The primary the Sentinel at `ep` names for the profile's service.
    async fn sentinel_names(&self, ep: &Endpoint) -> RedisResult<Endpoint> {
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
        address.ok_or_else(|| error("Sentinel does not know this service"))
    }

    /// The primary Sentinel names right now, from the first seed that
    /// answers. Nothing is verified or recorded: this only tells a caller
    /// whether a discovery is worth running.
    pub(super) async fn sentinel_primary(&self) -> RedisResult<Endpoint> {
        let ask = async {
            let mut failures = Vec::new();
            for ep in self.seeds() {
                match self.sentinel_names(&ep).await {
                    Ok(address) => return Ok(address),
                    Err(e) => failures.push(format!("{}:{}: {e}", ep.0, ep.1)),
                }
            }
            Err(error(format!(
                "No Sentinel answered: {}",
                failures.join("; ")
            )))
        };
        tokio::time::timeout(Duration::from_secs(10), ask)
            .await
            .unwrap_or_else(|_| Err(error("Sentinel did not answer in time")))
    }

    /// Check with `ROLE` that the node Sentinel named is a primary. The
    /// cached socket to it is kept when it is the primary already in use, so
    /// a periodic discovery does not cut the connection every request
    /// shares. A newly named primary, or a failed check, starts over on a
    /// fresh socket.
    async fn verify_primary(&self, address: &Endpoint) -> RedisResult<()> {
        {
            let mut state = self.state();
            if state.default != *address {
                state.sockets.remove(address);
            }
        }
        let mut retried = false;
        loop {
            let socket = self.connection(address).await?;
            let mut c = socket.conn.clone();
            let result = redis::cmd("ROLE").query_async::<Vec<Value>>(&mut c).await;
            self.answered(&socket, &result);
            match result {
                Ok(role) if role.first().map(scalar).as_deref() == Some("master") => return Ok(()),
                Ok(_) => {
                    self.forget(address, socket.id);
                    return Err(error("Sentinel candidate is not a primary"));
                }
                // A cached socket that died since it was last used says
                // nothing about the node: ask once more on a new one.
                Err(e) if e.is_io_error() && !retried => {
                    self.forget(address, socket.id);
                    retried = true;
                }
                Err(e) => {
                    self.forget(address, socket.id);
                    return Err(e);
                }
            }
        }
    }

    /// Rediscover when the topology is older than 30 seconds, or, on a
    /// Sentinel profile, when the last discovery failed. After a lost write,
    /// wait for a discovery that started after the loss. Only a Sentinel
    /// failure is an error: its primary may have moved, and a stale address
    /// must never be written to. A cluster keeps routing on what it knows.
    async fn refresh_if_stale(&self) -> RedisResult<()> {
        if self.profile.deployment == Deployment::Standalone {
            return Ok(());
        }
        let (due, lost) = {
            let state = self.state();
            (
                state.refreshed.elapsed() >= Duration::from_secs(30)
                    || (state.must_rediscover && self.profile.deployment == Deployment::Sentinel),
                state.lost_at,
            )
        };
        if !due && lost.is_none() {
            return Ok(());
        }
        if let Err(e) = self.discover_noting(lost).await
            && self.profile.deployment == Deployment::Sentinel
        {
            return Err(e);
        }
        Ok(())
    }

    /// Where a command for `slot` goes, by the topology as it is now.
    fn route(&self, slot: Option<u16>) -> Endpoint {
        route_slot(&self.state(), slot)
    }

    /// Record a `MOVED`: it is authoritative for its slot even when discovery
    /// is unavailable.
    fn moved(&self, slot: u16, target: &Endpoint) {
        let mut state = self.state();
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
        if let Some(n) = state.nodes.iter_mut().find(|n| n.endpoint() == *target) {
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

    /// The slot a command must be sent to on a cluster, or `None` when it has
    /// no keys and can go to any primary. Refuses, before anything is sent,
    /// keys in different slots and keyless writes that would silently touch
    /// only one primary of many.
    async fn cluster_slot(&self, cmd: &Cmd) -> RedisResult<Option<u16>> {
        let keys = match command_keys(cmd) {
            Keys::Known(keys) => keys,
            Keys::Unknown => self.server_keys(cmd).await?,
        };
        let Some(first) = keys.first() else {
            if !read_route(cmd).0 && !keyless_on_any_node(cmd) {
                return Err(error(format!(
                    "{} has no key to route by, so on a cluster it would reach only one primary; run it against each node from a standalone profile",
                    command_name(cmd)
                )));
            }
            return Ok(None);
        };
        let slot = key_slot(first);
        if keys.iter().any(|k| key_slot(k) != slot) {
            return Err(error(
                "Keys in this command hash to different cluster slots, so Redis would refuse it (CROSSSLOT); nothing was sent. Give the keys a shared hash tag, like {user:42}:a and {user:42}:b",
            ));
        }
        Ok(Some(slot))
    }

    /// Ask the server which arguments are keys, for a command the local
    /// table does not know: a module command, or a newer one.
    async fn server_keys(&self, cmd: &Cmd) -> RedisResult<Vec<Vec<u8>>> {
        let mut probe = redis::cmd("COMMAND");
        probe.arg("GETKEYS");
        // The connect below can fail without anything being sent.
        for arg in simple_args(cmd) {
            probe.arg(arg);
        }
        let ep = self.state().default.clone();
        let socket = self.connection(&ep).await?;
        let mut c = socket.conn.clone();
        let reply = c.req_packed_command(&probe).await;
        self.answered(&socket, &reply);
        match reply.and_then(Value::extract_error) {
            Ok(v) => Ok(redis::from_redis_value(v)?),
            Err(e)
                if e.to_string()
                    .to_ascii_lowercase()
                    .contains("no key arguments") =>
            {
                Ok(Vec::new())
            }
            Err(e) if e.is_io_error() => {
                self.forget(&ep, socket.id);
                let _ = self.rediscover().await;
                Err(error(format!(
                    "Cannot look up the keys of {} on {}:{}; nothing was sent, and the topology was refreshed, so try again: {e}",
                    command_name(cmd),
                    ep.0,
                    ep.1
                )))
            }
            Err(e) => Err(error(format!(
                "Cannot tell which cluster node owns the keys of {}; nothing was sent: {e}",
                command_name(cmd)
            ))),
        }
    }

    pub async fn refresh(&self) -> RedisResult<()> {
        self.discover_noting(None).await
    }
    /// Refresh after something was lost, such as a feed's connection: a
    /// discovery already under way when that happened does not count.
    pub(super) async fn refresh_after_loss(&self) -> RedisResult<()> {
        self.rediscover().await
    }
    /// Where keyless commands go: the node diagnostics describe. Refreshed
    /// first when stale. If that refresh fails, a cluster uses the last known
    /// node, and a Sentinel profile gets the error: its primary may have moved.
    pub async fn default_endpoint(&self) -> RedisResult<Endpoint> {
        self.refresh_if_stale().await?;
        Ok(self.state().default.clone())
    }
    /// The default node as last discovered, without refreshing first.
    pub async fn known_default(&self) -> Endpoint {
        self.state().default.clone()
    }
    pub fn deployment(&self) -> Deployment {
        self.profile.deployment
    }
    pub fn primary_count(&self) -> usize {
        self.primary_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }
    /// A client for one node, with the profile's data credentials and TLS,
    /// for a connection that cannot be shared: pub/sub or `MONITOR`. On a
    /// standalone profile this is the client the profile connected with, so a
    /// tunnel or a Unix socket still applies.
    pub async fn node_client(&self, ep: &Endpoint) -> RedisResult<redis::Client> {
        if let Some(client) = self.state().clients.get(ep) {
            return Ok(client.clone());
        }
        let mut profile = self.profile.clone();
        profile.host = ep.0.clone();
        profile.port = ep.1;
        let raw = build_client(&profile, None)
            .await
            .map_err(|e| error(e.to_string()))?;
        Ok(self
            .state()
            .clients
            .entry(ep.clone())
            .or_insert(raw)
            .clone())
    }
    pub async fn nodes(&self) -> Vec<Node> {
        self.state().nodes.clone()
    }
    pub async fn warning(&self) -> Option<String> {
        self.state().warning.clone()
    }
    pub async fn primaries(&self) -> Vec<Endpoint> {
        let state = self.state();
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
        self.audit_result(id, action, count, &result, false, never_resent(cmd))?;
        result
    }
    async fn direct_inner(&self, ep: &Endpoint, cmd: &Cmd) -> RedisResult<Value> {
        let read = read_route(cmd).0;
        self.guard(read)?;
        // On a Sentinel profile, `ep` came from a discovery. If a later one
        // failed, or a write was lost since, it may name a demoted primary:
        // a change is only sent once discovery has succeeded again.
        let checked = !read && self.profile.deployment == Deployment::Sentinel;
        // A client id or a slow log belongs to the node that listed it, even
        // once that node is no longer the primary. Any other change was meant
        // for the primary, and must not reach one that was replaced.
        let primary_only = checked && !node_bound(cmd);
        let moved = || {
            let now = self.state().default.clone();
            (primary_only && now != *ep).then(|| {
                error(format!(
                    "{UNSENT}the Sentinel primary moved from {}:{} to {}:{} since this node was listed; refresh and try again",
                    ep.0, ep.1, now.0, now.1
                ))
            })
        };
        if checked {
            self.refresh_if_stale().await.map_err(not_sent)?;
            if let Some(e) = moved() {
                return Err(e);
            }
        }
        let mut socket = self.connection(ep).await?;
        if !read {
            socket = self.for_write(ep, socket).await?;
        }
        if checked && self.unsettled() {
            return Err(error(SENTINEL_UNSETTLED));
        }
        if let Some(e) = moved() {
            return Err(e);
        }
        // Again at the moment of sending: connecting can take a while.
        self.guard(read)?;
        let mut c = socket.conn.clone();
        let result = c.req_packed_command(cmd).await;
        self.answered(&socket, &result);
        let result = result.and_then(Value::extract_error);
        // A server error (NOPERM from a managed service) leaves the socket good.
        if result.as_ref().is_err_and(RedisError::is_io_error) {
            self.forget(ep, socket.id);
            if !read {
                self.rediscover_after_lost_write().await;
            }
        }
        result
    }
    pub async fn metadata(&self, ep: &Endpoint, names: &[String]) -> RedisResult<Vec<Value>> {
        let mut pipeline = redis::pipe();
        for name in names {
            pipeline.cmd("TYPE").arg(super::decode_key(name));
            pipeline.cmd("TTL").arg(super::decode_key(name));
        }
        // Metadata is read-only by construction; the guard keeps that true if
        // the command list ever grows.
        if pipeline.cmd_iter().any(|c| !read_route(c).0) {
            self.guard(false)?;
        }
        let socket = self.connection(ep).await?;
        let mut c = socket.conn.clone();
        let result = c.req_packed_commands(&pipeline, 0, names.len() * 2).await;
        self.answered(&socket, &result);
        if result.is_err() {
            self.forget(ep, socket.id);
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
        self.audit_result(id, action, count, &result, false, never_resent(cmd))?;
        result
    }
    async fn request_inner(&self, cmd: &Cmd) -> RedisResult<Value> {
        let read = read_route(cmd).0;
        self.guard(read)?;
        self.refresh_if_stale().await.map_err(not_sent)?;
        let slot = if self.profile.deployment == Deployment::Cluster {
            self.cluster_slot(cmd).await?
        } else {
            None
        };
        // A script's own error reply can look like `TRYAGAIN` or `MOVED`
        // after it has already written, and so can a module command's. Only
        // commands whose behaviour is known are ever sent twice.
        let opaque = never_resent(cmd);
        let mut ep = self.route(slot);
        // `ep` came from a redirect rather than from the topology. A
        // `MOVED` outranks a discovery that has not caught up with it.
        let mut redirected = false;
        let mut asking = false;
        for attempt in 0..4 {
            // Set once the command is on the wire. A failure before that, such
            // as a node that refuses the connection, proves nothing ran.
            let mut sent = false;
            // The cached socket the attempt used, if any.
            let mut used = None;
            let result = async {
                if asking {
                    // Dedicated socket keeps ASKING and the redirected command adjacent.
                    let raw = self.state().clients.get(&ep).cloned();
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
                    self.guard(read)?;
                    sent = true;
                    c.req_packed_command(cmd)
                        .await
                        .and_then(Value::extract_error)
                } else {
                    let mut socket = self.connection(&ep).await?;
                    if !read {
                        socket = self.for_write(&ep, socket).await?;
                        // A write lost while this one waited for its socket
                        // leaves the route in doubt: nothing is sent until a
                        // discovery after that loss has finished. A discovery
                        // that finished meanwhile may have moved the route:
                        // the write follows it.
                        let mut waits = 0;
                        while self.profile.deployment != Deployment::Standalone
                            && (self.unsettled() || (!redirected && self.route(slot) != ep))
                        {
                            if waits == 3 {
                                return Err(error(WRITE_UNSETTLED));
                            }
                            waits += 1;
                            self.refresh_if_stale().await.map_err(not_sent)?;
                            ep = self.route(slot);
                            redirected = false;
                            socket = self.connection(&ep).await?;
                            socket = self.for_write(&ep, socket).await?;
                        }
                    }
                    used = Some(socket.id);
                    let mut c = socket.conn.clone();
                    self.guard(read)?;
                    sent = true;
                    let reply = c.req_packed_command(cmd).await;
                    self.answered(&socket, &reply);
                    reply.and_then(Value::extract_error)
                }
            }
            .await;
            match result {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if self.profile.deployment == Deployment::Cluster
                        && let Some((address, redirect)) = e.redirect_node()
                    {
                        if redirect >= 16384 {
                            return Err(error("Invalid redirect slot"));
                        }
                        // The server redirects a write before running it, and
                        // only for the slot of its keys. Anything else is a
                        // reply that merely looks like a redirect.
                        if !read && (opaque || slot != Some(redirect)) {
                            if slot == Some(redirect) {
                                // Likely genuine: refresh so the next attempt
                                // goes to the right node, without resending.
                                let _ = self.rediscover().await;
                                return Err(error(format!(
                                    "{e}; the command was not sent again, because its reply cannot prove it never ran. The topology was refreshed, so try again"
                                )));
                            }
                            return Err(e);
                        }
                        let slot = redirect;
                        let address = if address.starts_with(':') {
                            format!("{}{address}", ep.0)
                        } else {
                            address.to_string()
                        };
                        let target = endpoint(&address).map_err(|e| error(e.to_string()))?;
                        asking = e.kind() == redis::ErrorKind::Server(redis::ServerErrorKind::Ask);
                        if !asking {
                            let _ = self.rediscover().await;
                            // A MOVED is authoritative for this slot even if discovery is unavailable.
                            self.moved(slot, &target);
                        }
                        ep = target;
                        redirected = true;
                        continue;
                    }
                    let refused = rejected_unrun(&e) && !(opaque && sent);
                    if !e.is_io_error() && !refused {
                        // Not resent, but a refusal still says the topology
                        // moved, and the next attempt should find the new owner.
                        if opaque
                            && rejected_unrun(&e)
                            && self.profile.deployment != Deployment::Standalone
                        {
                            let _ = self.rediscover().await;
                        }
                        return Err(e);
                    }
                    // A failure before a socket was in hand, such as a refused
                    // connection, leaves another caller's socket alone.
                    if let Some(id) = used {
                        self.forget(&ep, id);
                    }
                    if !read && sent && !refused {
                        // Never resent, but the next command should not be
                        // aimed at a primary that is gone.
                        self.rediscover_after_lost_write().await;
                        return Err(error(format!(
                            "Write outcome unknown; command was not retried: {e}"
                        )));
                    }
                    // A standalone server that refuses a write will refuse it
                    // again: there is no other node to find.
                    if !read && refused && self.profile.deployment == Deployment::Standalone {
                        return Err(e);
                    }
                    if attempt == 3 {
                        return Err(e);
                    }
                    tokio::time::sleep(Duration::from_millis(50 * (1 << attempt))).await;
                    if self.profile.deployment != Deployment::Standalone
                        && let Err(refresh) = self.rediscover().await
                        && self.profile.deployment == Deployment::Sentinel
                    {
                        // Every earlier attempt was refused unrun, so nothing
                        // from this command has reached a server.
                        return Err(not_sent(refresh));
                    }
                    ep = self.route(slot);
                    redirected = false;
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
        let writes = pipeline.cmd_iter().any(|c| !read_route(c).0);
        if self.read_only() && writes {
            return Err(error(PIPELINE_DENIED));
        }
        if self.profile.deployment == Deployment::Cluster {
            if pipeline.is_transaction() {
                return Err(error(
                    "Transactions are not supported on a cluster; nothing was sent",
                ));
            }
            if writes {
                return self.cluster_pipeline(pipeline, offset, count).await;
            }
            let mut values = Vec::new();
            for cmd in pipeline.cmd_iter() {
                values.push(self.request(cmd).await?);
            }
            return Ok(values.into_iter().skip(offset).take(count).collect());
        }
        self.refresh_if_stale().await.map_err(not_sent)?;
        let mut ep = self.state().default.clone();
        let mut socket = self.connection(&ep).await?;
        if writes {
            socket = self.for_write(&ep, socket).await?;
            // As for a single write: wait out a lost write, and follow a
            // discovery that moved the primary while the socket was checked.
            let mut waits = 0;
            while self.profile.deployment != Deployment::Standalone
                && (self.unsettled() || self.state().default != ep)
            {
                if waits == 3 {
                    return Err(error(BATCH_UNSETTLED));
                }
                waits += 1;
                self.refresh_if_stale().await.map_err(not_sent)?;
                ep = self.state().default.clone();
                socket = self.connection(&ep).await?;
                socket = self.for_write(&ep, socket).await?;
            }
        }
        self.guard(!writes)?;
        let mut c = socket.conn.clone();
        let result = c.req_packed_commands(pipeline, offset, count).await;
        self.answered(&socket, &result);
        let result =
            result.and_then(|values| values.into_iter().map(Value::extract_error).collect());
        if let Err(e) = &result {
            // A server error leaves the shared socket working.
            if e.is_io_error() {
                self.forget(&ep, socket.id);
            }
            // A Sentinel primary demoted during failover refuses writes as a
            // replica. Find the new primary now, so a retry reaches it.
            if self.profile.deployment == Deployment::Sentinel && writes && rejected_unrun(e) {
                let _ = self.rediscover().await;
                return Err(error(format!(
                    "The primary stopped accepting writes partway through this batch ({e}); commands before that point may have been applied. The topology was refreshed; try again to reach the new primary"
                )));
            }
            if e.is_io_error() {
                if writes {
                    self.rediscover_after_lost_write().await;
                } else if self.profile.deployment == Deployment::Sentinel {
                    let _ = self.rediscover().await;
                }
                if !pipeline.is_transaction() && pipeline.cmd_iter().all(|cmd| read_route(cmd).0) {
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

    /// A non-transactional pipeline with writes, on a cluster. Every command's
    /// node is worked out before anything is sent, so one command that cannot
    /// be routed fails the whole batch unsent. Each node then gets one
    /// sub-pipeline of its own commands, in their original order. A command
    /// the server redirected or refused unrun is sent again on its own; a
    /// lost connection leaves the batch's outcome unknown and nothing is
    /// retried.
    async fn cluster_pipeline(
        &self,
        pipeline: &redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        let cmds: Vec<&Cmd> = pipeline.cmd_iter().collect();
        self.refresh_if_stale().await?;
        let mut slots = Vec::with_capacity(cmds.len());
        for cmd in &cmds {
            slots.push(self.cluster_slot(cmd).await?);
        }
        // Every command is routed by the same view of the topology.
        let mut groups: Vec<(Endpoint, Vec<usize>)> = Vec::new();
        {
            let state = self.state();
            for (i, slot) in slots.iter().enumerate() {
                let ep = route_slot(&state, *slot);
                match groups.iter_mut().find(|(e, _)| *e == ep) {
                    Some((_, indices)) => indices.push(i),
                    None => groups.push((ep, vec![i])),
                }
            }
        }
        let mut replies: Vec<Option<RedisResult<Value>>> = cmds.iter().map(|_| None).collect();
        let mut again = Vec::new();
        for (sent, (ep, indices)) in groups.iter().enumerate() {
            let mut sub = redis::pipe();
            for &i in indices {
                sub.add_command(cmds[i].clone());
            }
            // Commands earlier nodes ran are not undone. Commands those nodes
            // redirected or refused are dropped with the rest of the batch.
            let earlier = |this: &str, e: &RedisError| {
                let before = if sent == 0 {
                    String::new()
                } else {
                    format!(" Commands already sent to {sent} other node(s) may have been applied.")
                };
                error(format!(
                    "Pipeline stopped at {}:{}: {this}.{before} Nothing was retried: {e}",
                    ep.0, ep.1
                ))
            };
            let socket = match async { self.for_write(ep, self.connection(ep).await?).await }.await
            {
                Ok(socket) => socket,
                Err(e) if sent == 0 => return Err(e),
                Err(e) => return Err(earlier("its commands were not sent", &e)),
            };
            // Checked as each node's commands go out: the lease can run out
            // while the batch waits for the connection or a slower node.
            if let Err(e) = self.guard(false) {
                if sent == 0 {
                    return Err(e);
                }
                return Err(earlier(
                    "its commands were not sent",
                    &error("writes were locked or the write lease expired mid-batch"),
                ));
            }
            let mut c = socket.conn.clone();
            let reply = c.req_packed_commands(&sub, 0, indices.len()).await;
            self.answered(&socket, &reply);
            match reply {
                Ok(values) => {
                    for (&i, value) in indices.iter().zip(values) {
                        let opaque = never_resent(cmds[i]);
                        match Value::extract_error(value) {
                            Err(e)
                                if !opaque
                                    && (e
                                        .redirect_node()
                                        .is_some_and(|(_, r)| slots[i] == Some(r))
                                        || rejected_unrun(&e)) =>
                            {
                                again.push(i)
                            }
                            reply => replies[i] = Some(reply),
                        }
                    }
                }
                Err(e) => {
                    self.forget(ep, socket.id);
                    self.rediscover_after_lost_write().await;
                    return Err(earlier("outcome unknown for its commands", &e));
                }
            }
        }
        // Redirected and refused commands never ran, so each is safe to send
        // once more, through the single-command path that follows redirects.
        // Among them the batch's order is kept; on one key it can only differ
        // from the batch as a whole if the slot changed owner mid-batch.
        again.sort_unstable();
        let resent = !again.is_empty();
        for i in again {
            replies[i] = Some(self.request_inner(cmds[i]).await);
        }
        let total = replies.len();
        let applied = replies.iter().filter(|r| matches!(r, Some(Ok(_)))).count();
        let values: RedisResult<Vec<Value>> = replies
            .into_iter()
            .map(|r| r.unwrap_or_else(|| Err(error("Pipeline reply missing"))))
            .collect();
        match values {
            Ok(values) => Ok(values.into_iter().skip(offset).take(count).collect()),
            // One failure must not hide the commands that did run.
            Err(e) if applied > 0 => Err(error(format!(
                "Pipeline partially applied: {applied} of {total} commands ran, and nothing was retried. First failure: {e}"
            ))),
            // A refusal met only while resending still followed sends that may
            // have run: it must not read as a batch refused before sending.
            Err(e) if denied(&e) && (resent || groups.len() > 1) => Err(error(format!(
                "Pipeline stopped: writes were locked or the write lease expired before every command was sent; commands already sent may have been applied. {e}"
            ))),
            // Likewise a resend refused because the route was in doubt.
            Err(e) if unsent(&e) && (resent || groups.len() > 1) => Err(error(format!(
                "Pipeline stopped before every command was sent; commands already sent may have been applied. {e}"
            ))),
            Err(e) => Err(e),
        }
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
            self.audit_result(id, "PIPELINE", targets, &result, true, false)?;
            result
        })
    }
    fn get_db(&self) -> i64 {
        self.profile.db
    }
}

fn route_slot(state: &State, slot: Option<u16>) -> Endpoint {
    slot.and_then(|slot| {
        state
            .nodes
            .iter()
            .find(|n| n.primary && n.slots.iter().any(|(a, b)| (*a..=*b).contains(&slot)))
            .map(Node::endpoint)
    })
    .unwrap_or_else(|| state.default.clone())
}

/// A server error that proves the command never ran, so even a write can be
/// sent again: a replica refusing a write, a cluster slot mid-migration or
/// without a primary, a server still loading its data.
fn rejected_unrun(e: &RedisError) -> bool {
    matches!(
        e.kind(),
        redis::ErrorKind::Server(
            redis::ServerErrorKind::ReadOnly
                | redis::ServerErrorKind::ClusterDown
                | redis::ServerErrorKind::TryAgain
                | redis::ServerErrorKind::MasterDown
                | redis::ServerErrorKind::BusyLoading
        )
    )
}

/// A node-addressed change that only means something on the node that listed
/// it: `CLIENT KILL` for a client id, `SLOWLOG RESET` for that node's log.
fn node_bound(cmd: &Cmd) -> bool {
    let args = simple_args(cmd);
    let word = |i: usize| {
        args.get(i)
            .map(|a| String::from_utf8_lossy(a).to_ascii_uppercase())
    };
    matches!(
        (word(0).as_deref(), word(1).as_deref()),
        (Some("CLIENT"), Some("KILL")) | (Some("SLOWLOG"), Some("RESET"))
    )
}

/// A script that may write. Its reply is whatever the script returns, so an
/// error in it proves nothing about whether its writes happened.
fn writing_script(cmd: &Cmd) -> bool {
    matches!(command_name(cmd).as_str(), "EVAL" | "EVALSHA" | "FCALL")
}

/// Commands whose error replies cannot be trusted to mean "never ran": writing
/// scripts, and anything outside the key table — a module can run scripts of
/// its own (`TFCALL`, `RG.PYEXECUTE`) that answer however they like.
fn never_resent(cmd: &Cmd) -> bool {
    writing_script(cmd) || command_keys(cmd) == Keys::Unknown
}

fn simple_args(cmd: &Cmd) -> Vec<&[u8]> {
    cmd.args_iter()
        .filter_map(|a| match a {
            redis::Arg::Simple(a) => Some(a),
            _ => None,
        })
        .collect()
}

fn command_name(cmd: &Cmd) -> String {
    simple_args(cmd)
        .first()
        .map(|a| String::from_utf8_lossy(a).to_ascii_uppercase())
        .unwrap_or_default()
}

/// Where a command's keys are, as far as the client can tell on its own.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Keys {
    /// The key arguments; empty for a command that takes none.
    Known(Vec<Vec<u8>>),
    /// Not in the table: the server is asked with `COMMAND GETKEYS`.
    Unknown,
}

/// The key arguments of a command, for slot routing. Covers every command
/// rediscope sends itself and the common console ones, so they need no extra
/// round trip; anything else is `Unknown`.
pub(super) fn command_keys(cmd: &Cmd) -> Keys {
    let args = simple_args(cmd);
    let (read, key) = read_route(cmd);
    if read {
        return Keys::Known(key.into_iter().collect());
    }
    let word = |i: usize| {
        args.get(i)
            .map(|a| String::from_utf8_lossy(a).to_ascii_uppercase())
            .unwrap_or_default()
    };
    let (head, sub) = (word(0), word(1));
    let at = |positions: &[usize]| -> Keys {
        if positions.iter().any(|&i| i >= args.len()) {
            // Too few arguments: the server will say so, and routing by a
            // guess must not happen first.
            return Keys::Unknown;
        }
        Keys::Known(positions.iter().map(|&i| args[i].to_vec()).collect())
    };
    let every = |start: usize, step: usize| -> Keys {
        Keys::Known(
            args.iter()
                .skip(start)
                .step_by(step)
                .map(|a| a.to_vec())
                .collect(),
        )
    };
    // `numkeys` at `count`, then that many keys; `extra` keys come first.
    let counted = |extra: &[usize], count: usize| -> Keys {
        let Some(n) = args
            .get(count)
            .and_then(|a| std::str::from_utf8(a).ok())
            .and_then(|a| a.parse::<usize>().ok())
        else {
            return Keys::Unknown;
        };
        let Some(end) = (count + 1).checked_add(n).filter(|&end| end <= args.len()) else {
            return Keys::Unknown;
        };
        let mut keys: Vec<Vec<u8>> = extra
            .iter()
            .filter_map(|&i| args.get(i))
            .map(|a| a.to_vec())
            .collect();
        keys.extend(args[count + 1..end].iter().map(|a| a.to_vec()));
        Keys::Known(keys)
    };
    match head.as_str() {
        "SET" | "SETNX" | "SETEX" | "PSETEX" | "APPEND" | "INCR" | "DECR" | "INCRBY" | "DECRBY"
        | "INCRBYFLOAT" | "GETSET" | "GETDEL" | "GETEX" | "SETRANGE" | "SETBIT" | "HSET"
        | "HSETNX" | "HMSET" | "HDEL" | "HINCRBY" | "HINCRBYFLOAT" | "HEXPIRE" | "HPEXPIRE"
        | "HEXPIREAT" | "HPEXPIREAT" | "HPERSIST" | "HGETDEL" | "HGETEX" | "HSETEX" | "LPUSH"
        | "RPUSH" | "LPUSHX" | "RPUSHX" | "LSET" | "LREM" | "LTRIM" | "LINSERT" | "LPOP"
        | "RPOP" | "SADD" | "SREM" | "SPOP" | "ZADD" | "ZREM" | "ZINCRBY" | "ZREMRANGEBYSCORE"
        | "ZREMRANGEBYRANK" | "ZREMRANGEBYLEX" | "ZPOPMIN" | "ZPOPMAX" | "XADD" | "XDEL"
        | "XTRIM" | "XACK" | "XCLAIM" | "XAUTOCLAIM" | "XSETID" | "EXPIRE" | "PEXPIRE"
        | "EXPIREAT" | "PEXPIREAT" | "PERSIST" | "RESTORE" | "PFADD" | "GEOADD" | "VADD"
        | "VREM" | "VSETATTR" | "HMGET" | "HEXISTS" | "HKEYS" | "HVALS" | "HSTRLEN"
        | "HRANDFIELD" | "LINDEX" | "LPOS" | "SISMEMBER" | "SMISMEMBER" | "SRANDMEMBER"
        | "ZSCORE" | "ZMSCORE" | "ZRANK" | "ZREVRANK" | "ZCOUNT" | "ZRANGEBYSCORE"
        | "ZREVRANGEBYSCORE" | "ZRANDMEMBER" | "GETBIT" | "BITCOUNT" | "BITPOS" | "EXPIRETIME"
        | "PEXPIRETIME" | "HPTTL" | "OBJECT" => {
            // `OBJECT` subcommands name their key second.
            if head == "OBJECT" { at(&[2]) } else { at(&[1]) }
        }
        "XGROUP"
            if matches!(
                sub.as_str(),
                "CREATE" | "DESTROY" | "SETID" | "CREATECONSUMER" | "DELCONSUMER"
            ) =>
        {
            at(&[2])
        }
        "DEL" | "UNLINK" | "TOUCH" | "EXISTS" | "MGET" | "SINTERSTORE" | "SUNIONSTORE"
        | "SDIFFSTORE" | "SINTER" | "SUNION" | "SDIFF" | "PFMERGE" | "PFCOUNT" => every(1, 1),
        "MSET" | "MSETNX" => every(1, 2),
        "SPUBLISH" => at(&[1]),
        "RENAME" | "RENAMENX" | "SMOVE" | "LMOVE" | "RPOPLPUSH" | "COPY" | "ZRANGESTORE"
        | "GEOSEARCHSTORE" => at(&[1, 2]),
        "BITOP" => every(2, 1),
        "ZUNIONSTORE" | "ZINTERSTORE" | "ZDIFFSTORE" => counted(&[1], 2),
        "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" | "FCALL" | "FCALL_RO" => counted(&[], 2),
        "TS.CREATERULE" | "TS.DELETERULE" => at(&[1, 2]),
        "JSON.MGET" | "JSON.MSET" | "TS.MADD" | "TS.MGET" | "TS.MRANGE" | "TS.MREVRANGE"
        | "TS.QUERYINDEX" => Keys::Unknown,
        h if h.starts_with("JSON.") || h.starts_with("TS.") => at(&[1]),
        "FLUSHALL" | "FLUSHDB" | "SWAPDB" | "CONFIG" | "CLIENT" | "SLOWLOG" | "LATENCY"
        | "PUBLISH" | "SCRIPT" | "FUNCTION" | "ACL" | "MEMORY" | "SAVE" | "BGSAVE"
        | "BGREWRITEAOF" | "SHUTDOWN" | "DEBUG" | "MODULE" | "CLUSTER" | "FAILOVER"
        | "REPLICAOF" | "SLAVEOF" | "RANDOMKEY" | "KEYS" | "LASTSAVE" | "ECHO" => {
            Keys::Known(Vec::new())
        }
        _ => Keys::Unknown,
    }
}

/// Keyless writes whose meaning on a cluster is one node's business, sent to
/// the node the diagnostics tabs read from. `PUBLISH` reaches every node's
/// subscribers from any of them, and a read-only script cannot change data.
/// A keyless `EVAL` or `FCALL` is refused: a script can reach any key on the
/// node it runs on, so it would change one primary of many. So would
/// `FLUSHDB`, `SCRIPT FLUSH` or `FUNCTION LOAD`. `RANDOMKEY` and `KEYS` are
/// refused too: one node's answer would pass for the cluster's.
fn keyless_on_any_node(cmd: &Cmd) -> bool {
    matches!(
        command_name(cmd).as_str(),
        "CONFIG"
            | "CLIENT"
            | "SLOWLOG"
            | "LATENCY"
            | "MEMORY"
            | "PUBLISH"
            | "EVAL_RO"
            | "EVALSHA_RO"
            | "FCALL_RO"
            | "ECHO"
            | "LASTSAVE"
    )
}

// An explicit allowlist prevents module/administrative writes from bypassing
// read-only enforcement. Unknown commands are never automatically replayed.
pub(super) fn read_route(cmd: &Cmd) -> (bool, Option<Vec<u8>>) {
    let args = simple_args(cmd);
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
        | "HTTL" | "HGET" | "HGETALL" | "LLEN" | "LRANGE" | "SCARD" | "SSCAN" | "SMEMBERS"
        | "ZCARD" | "ZRANGE" | "ZSCAN" | "XLEN" | "XRANGE" | "XREVRANGE" | "XPENDING"
        | "JSON.GET" | "JSON.TYPE" | "TS.GET" | "TS.RANGE" | "TS.INFO" | "VCARD" | "VDIM"
        | "VINFO" | "VRANGE" | "VRANDMEMBER" | "VEMB" | "VGETATTR" | "VSIM" | "VLINKS"
        | "VISMEMBER" => Some(1),
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
    fn line(parts: &[&str]) -> Cmd {
        let mut cmd = redis::cmd(parts[0]);
        for p in &parts[1..] {
            cmd.arg(*p);
        }
        cmd
    }
    fn known(parts: &[&str]) -> Vec<Vec<u8>> {
        match command_keys(&line(parts)) {
            Keys::Known(keys) => keys,
            Keys::Unknown => panic!("{parts:?} should have known keys"),
        }
    }
    fn keys(list: &[&str]) -> Vec<Vec<u8>> {
        list.iter().map(|k| k.as_bytes().to_vec()).collect()
    }
    #[test]
    fn command_keys_single_key_writes_and_reads() {
        assert_eq!(known(&["SET", "k", "v"]), keys(&["k"]));
        assert_eq!(known(&["set", "k", "v", "KEEPTTL"]), keys(&["k"]));
        assert_eq!(known(&["HSET", "h", "f", "v"]), keys(&["h"]));
        assert_eq!(known(&["EXPIRE", "k", "10"]), keys(&["k"]));
        assert_eq!(known(&["RESTORE", "k", "0", "payload"]), keys(&["k"]));
        assert_eq!(known(&["GET", "r"]), keys(&["r"]));
        assert_eq!(known(&["MEMORY", "USAGE", "m"]), keys(&["m"]));
        assert_eq!(known(&["OBJECT", "FREQ", "o"]), keys(&["o"]));
        assert_eq!(known(&["XGROUP", "CREATE", "s", "g", "$"]), keys(&["s"]));
        assert_eq!(known(&["xgroup", "destroy", "s", "g"]), keys(&["s"]));
    }
    #[test]
    fn command_keys_multi_key_shapes() {
        assert_eq!(known(&["DEL", "a", "b", "c"]), keys(&["a", "b", "c"]));
        assert_eq!(known(&["UNLINK", "a"]), keys(&["a"]));
        assert_eq!(
            known(&["MSET", "a", "1", "b", "2", "c", "3"]),
            keys(&["a", "b", "c"])
        );
        assert_eq!(known(&["MSETNX", "a", "1"]), keys(&["a"]));
        assert_eq!(known(&["RENAME", "old", "new"]), keys(&["old", "new"]));
        assert_eq!(
            known(&["COPY", "src", "dst", "REPLACE"]),
            keys(&["src", "dst"])
        );
        assert_eq!(
            known(&["BITOP", "AND", "d", "a", "b"]),
            keys(&["d", "a", "b"])
        );
        assert_eq!(
            known(&["ZUNIONSTORE", "dest", "2", "a", "b", "WEIGHTS", "1", "2"]),
            keys(&["dest", "a", "b"])
        );
        assert_eq!(
            known(&["ZINTERSTORE", "dest", "1", "a"]),
            keys(&["dest", "a"])
        );
    }
    #[test]
    fn command_keys_scripts_count_their_keys() {
        assert_eq!(known(&["EVAL", "return 1", "0"]), Vec::<Vec<u8>>::new());
        assert_eq!(
            known(&["EVAL", "return 1", "0", "arg"]),
            Vec::<Vec<u8>>::new()
        );
        assert_eq!(
            known(&["EVAL", "return 1", "2", "k1", "k2", "argv1"]),
            keys(&["k1", "k2"])
        );
        assert_eq!(known(&["EVALSHA", "abc", "1", "k"]), keys(&["k"]));
        assert_eq!(known(&["FCALL", "f", "1", "k", "a"]), keys(&["k"]));
        for bad in [
            &["EVAL", "return 1", "x", "k"][..],
            &["EVAL", "return 1", "-1", "k"],
            &["EVAL", "return 1", "3", "k1", "k2"],
            &["EVAL", "return 1"],
            &["ZUNIONSTORE", "dest", "5", "a"],
            &["ZUNIONSTORE", "dest", "many", "a"],
        ] {
            assert_eq!(command_keys(&line(bad)), Keys::Unknown, "{bad:?}");
        }
    }
    #[test]
    fn command_keys_modules_admin_and_unknown() {
        assert_eq!(known(&["JSON.SET", "j", "$", "{}"]), keys(&["j"]));
        assert_eq!(known(&["TS.ADD", "t", "*", "1"]), keys(&["t"]));
        assert_eq!(
            known(&["TS.CREATERULE", "a", "b", "AGGREGATION"]),
            keys(&["a", "b"])
        );
        assert_eq!(
            command_keys(&line(&["JSON.MGET", "a", "b", "$"])),
            Keys::Unknown
        );
        assert_eq!(
            command_keys(&line(&["TS.MADD", "a", "1", "2"])),
            Keys::Unknown
        );
        for admin in [
            &["FLUSHDB"][..],
            &["FLUSHALL", "ASYNC"],
            &["CONFIG", "SET", "maxmemory", "1"],
            &["CLIENT", "KILL", "ID", "1"],
            &["SLOWLOG", "RESET"],
            &["PUBLISH", "chan", "msg"],
            &["SCRIPT", "FLUSH"],
            &["FUNCTION", "LOAD", "code"],
            &["ECHO", "x"],
            &["LASTSAVE"],
        ] {
            assert_eq!(known(admin), Vec::<Vec<u8>>::new(), "{admin:?}");
        }
        assert_eq!(command_keys(&line(&["MYMODULE.WRITE", "k"])), Keys::Unknown);
        assert_eq!(
            command_keys(&line(&["XREADGROUP", "GROUP", "g"])),
            Keys::Unknown
        );
        assert_eq!(command_keys(&line(&["XGROUP", "HELP"])), Keys::Unknown);
    }
    #[test]
    fn command_keys_too_few_arguments_is_unknown() {
        for short in [
            &["SET"][..],
            &["HSET"],
            &["RENAME", "only"],
            &["COPY", "only"],
            &["XGROUP", "CREATE"],
            &["TS.CREATERULE", "a"],
            &["JSON.SET"],
        ] {
            assert_eq!(command_keys(&line(short)), Keys::Unknown, "{short:?}");
        }
    }
    #[test]
    fn keyless_writes_allowed_on_one_node_are_an_explicit_list() {
        for ok in [
            &["CONFIG", "SET", "a", "b"][..],
            &["client", "kill", "id", "1"],
            &["SLOWLOG", "RESET"],
            &["LATENCY", "RESET"],
            &["MEMORY", "PURGE"],
            &["PUBLISH", "c", "m"],
            &["EVAL_RO", "return 1", "0"],
            &["EVALSHA_RO", "abc", "0"],
            &["FCALL_RO", "f", "0"],
            &["ECHO", "x"],
            &["LASTSAVE"],
        ] {
            assert!(keyless_on_any_node(&line(ok)), "{ok:?}");
        }
        for refused in [
            // A keyless script can still write any key on its node.
            &["EVAL", "return 1", "0"][..],
            &["EVALSHA", "abc", "0"],
            &["FCALL", "f", "0"],
            &["FLUSHDB"],
            &["FLUSHALL"],
            &["SCRIPT", "FLUSH"],
            &["FUNCTION", "LOAD", "x"],
            &["SWAPDB", "0", "1"],
            &["RANDOMKEY"],
            &["KEYS", "*"],
            &["SHUTDOWN"],
            &["CLUSTER", "FAILOVER"],
            &["DEBUG", "SLEEP", "0"],
            &["SAVE"],
        ] {
            assert!(!keyless_on_any_node(&line(refused)), "{refused:?}");
        }
    }
    fn server_error(reply: &str) -> RedisError {
        redis::parse_redis_value(reply.as_bytes())
            .unwrap()
            .extract_error()
            .unwrap_err()
    }
    #[test]
    fn only_the_guards_own_refusals_count_as_denied() {
        assert!(denied(&error(DENIED)));
        assert!(denied(&error(PIPELINE_DENIED)));
        for spoof in [
            format!("-ERR {DENIED}\r\n"),
            format!("-READONLY {DENIED}\r\n"),
            format!("-ERR {PIPELINE_DENIED}\r\n"),
        ] {
            assert!(!denied(&server_error(&spoof)), "{spoof}");
        }
        assert!(!denied(&error(format!("{DENIED}; and more"))));
        assert!(!denied(&error(
            "Write outcome unknown; command was not retried"
        )));
    }
    #[test]
    fn only_errors_proving_the_command_never_ran_are_resendable() {
        for unrun in [
            "-READONLY You can't write against a read only replica.\r\n",
            "-CLUSTERDOWN The cluster is down\r\n",
            "-TRYAGAIN Multiple keys request during rehashing of slot\r\n",
            "-MASTERDOWN Link with MASTER is down\r\n",
            "-LOADING Redis is loading the dataset in memory\r\n",
        ] {
            assert!(rejected_unrun(&server_error(unrun)), "{unrun}");
        }
        for ran_or_unknown in [
            "-ERR something\r\n",
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
            "-CROSSSLOT Keys in request don't hash to the same slot\r\n",
            "-NOSCRIPT No matching script\r\n",
            "-BUSY Redis is busy running a script\r\n",
            "-MOVED 1 127.0.0.1:7000\r\n",
            "-ASK 1 127.0.0.1:7000\r\n",
            "-NOPERM no permission\r\n",
        ] {
            assert!(
                !rejected_unrun(&server_error(ran_or_unknown)),
                "{ran_or_unknown}"
            );
        }
        let io = RedisError::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ));
        assert!(!rejected_unrun(&io));
    }
    #[test]
    fn only_scripts_that_may_write_are_never_resent() {
        for script in [
            &["EVAL", "return 1", "0"][..],
            &["eval", "return 1", "1", "k"],
            &["EVALSHA", "abc", "0"],
            &["FCALL", "f", "1", "k"],
            &["fcall", "f", "0"],
        ] {
            assert!(writing_script(&line(script)), "{script:?}");
        }
        for other in [
            &["EVAL_RO", "return 1", "0"][..],
            &["EVALSHA_RO", "abc", "0"],
            &["FCALL_RO", "f", "0"],
            &["SET", "k", "v"],
            &["SCRIPT", "LOAD", "return 1"],
            &["FUNCTION", "LOAD", "code"],
        ] {
            assert!(!writing_script(&line(other)), "{other:?}");
        }
    }
    #[test]
    fn unknown_commands_and_writing_scripts_are_never_resent() {
        for opaque in [
            &["EVAL", "return 1", "1", "k"][..],
            &["FCALL", "f", "1", "k"],
            &["MYMOD.WRITE", "k", "v"],
            &["TFCALL", "lib.f", "1", "k"],
        ] {
            assert!(never_resent(&line(opaque)), "{opaque:?}");
        }
        for known in [
            &["SET", "k", "v"][..],
            &["UNLINK", "k"],
            &["EVAL_RO", "return 1", "1", "k"],
            &["HSET", "k", "f", "v"],
        ] {
            assert!(!never_resent(&line(known)), "{known:?}");
        }
    }
    #[test]
    fn sharded_publish_routes_by_its_channel() {
        assert_eq!(known(&["SPUBLISH", "chan", "msg"]), keys(&["chan"]));
        assert_eq!(known(&["PFCOUNT", "a", "b"]), keys(&["a", "b"]));
        assert_eq!(command_keys(&line(&["SPUBLISH"])), Keys::Unknown);
    }
    #[test]
    fn addresses_support_ipv6() {
        assert_eq!(endpoint("[::1]:6379").unwrap(), ("::1".into(), 6379));
        assert!(endpoint("bad").is_err());
        assert!(endpoint("host:0").is_err());
    }
}
