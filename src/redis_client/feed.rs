//! Live feeds on every kind of profile: pub/sub, keyspace events and
//! `MONITOR`.
//!
//! A feed needs connections of its own, because a subscribed connection can
//! do nothing else. Where they go depends on the deployment:
//!
//! - A standalone profile uses the server it connected to.
//! - A Sentinel profile uses the primary Sentinel names, and follows it to a
//!   new primary after a failover.
//! - A cluster broadcasts `PUBLISH` to every node, so channel messages need one
//!   subscription on the default node, followed to another node if it goes.
//! - Keyspace notifications never leave the node that raised them, and
//!   `MONITOR` only sees the node it runs on, so on a cluster those feeds open
//!   a connection on every primary and merge what they send, labelled by node.
//!   A lost node is reported and the others go on. The feed checks the
//!   topology now and then, so a primary added by a reshard or promoted by a
//!   failover joins it, and a node that is no longer a primary leaves it.
use super::topology::Transport;
use crate::config::Deployment;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::{
    sync::mpsc,
    task::{AbortHandle, JoinSet},
};

type Endpoint = (String, u16);

/// What a feed hands to its reader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FeedEvent {
    /// A pub/sub message. `node` names the node it came from when the feed
    /// merges several.
    Message {
        node: Option<String>,
        channel: String,
        payload: String,
    },
    /// One raw `MONITOR` line. `node` names the node that ran the command
    /// when the feed merges several.
    Command { node: Option<String>, line: String },
    /// Something the viewer should know about the feed itself: a node lost,
    /// a reconnection.
    Notice(String),
}

/// A running feed. Dropping it closes every connection it opened.
pub struct Feed {
    rx: mpsc::Receiver<FeedEvent>,
    _task: AbortOnDrop,
    nodes: usize,
}

impl Feed {
    /// The next event, or `None` once the feed has ended for good.
    pub async fn next(&mut self) -> Option<FeedEvent> {
        self.rx.recv().await
    }
    /// How many nodes the feed started on.
    pub fn nodes(&self) -> usize {
        self.nodes
    }
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Events waiting for the reader. This only evens out bursts: the redis crate
/// queues whatever a subscribed or monitoring socket receives without limit,
/// so the reader has to keep draining the feed and drop what it cannot show.
/// The app's readers do, a batch at a time.
const BUFFER: usize = 4096;
/// How long opening one node's connection and subscribing may take.
const OPEN_TIMEOUT: Duration = Duration::from_secs(5);
/// The first wait before reaching a node again, doubled on each failure.
const RETRY_MIN: Duration = Duration::from_millis(250);
/// The longest wait between attempts to reach a node again.
const RETRY_MAX: Duration = Duration::from_secs(5);
/// How long a connection has to stay up before a loss starts the waits over
/// from `RETRY_MIN`. A node that accepts subscribers and drops them at once
/// is retried less and less often instead of several times a second.
const HEALTHY: Duration = Duration::from_secs(10);

/// The connection a feed reads from.
enum Link {
    PubSub(redis::aio::PubSub),
    Monitor(redis::aio::Monitor),
}

/// What a feed is made of: `PSUBSCRIBE` to `patterns`, or `MONITOR` when
/// there are none.
#[derive(Clone)]
struct Spec {
    monitor: bool,
    patterns: Vec<String>,
}

pub fn label(ep: &Endpoint) -> String {
    if ep.0.contains(':') {
        format!("[{}]:{}", ep.0, ep.1)
    } else {
        format!("{}:{}", ep.0, ep.1)
    }
}

async fn open(transport: &Transport, ep: &Endpoint, spec: &Spec) -> Result<Link> {
    let work = async {
        let client = transport.node_client(ep).await?;
        if spec.monitor {
            return anyhow::Ok(Link::Monitor(client.get_async_monitor().await?));
        }
        let mut pubsub = client.get_async_pubsub().await?;
        for pattern in &spec.patterns {
            pubsub.psubscribe(pattern).await?;
        }
        anyhow::Ok(Link::PubSub(pubsub))
    };
    tokio::time::timeout(OPEN_TIMEOUT, work)
        .await
        .map_err(|_| anyhow::anyhow!("timed out"))?
        .with_context(|| label(ep))
}

/// Forward everything `link` receives. True when the connection ended, false
/// when nobody reads the feed any more.
async fn pump(link: Link, node: Option<String>, tx: &mpsc::Sender<FeedEvent>) -> bool {
    match link {
        Link::PubSub(pubsub) => {
            let mut stream = pubsub.into_on_message();
            while let Some(msg) = stream.next().await {
                let event = FeedEvent::Message {
                    node: node.clone(),
                    channel: msg.get_channel_name().to_string(),
                    payload: msg.get_payload().unwrap_or_default(),
                };
                if tx.send(event).await.is_err() {
                    return false;
                }
            }
            true
        }
        Link::Monitor(monitor) => {
            let mut stream = monitor.into_on_message::<String>();
            while let Some(line) = stream.next().await {
                let event = FeedEvent::Command {
                    node: node.clone(),
                    line,
                };
                if tx.send(event).await.is_err() {
                    return false;
                }
            }
            true
        }
    }
}

/// Subscribe to `patterns`. `node_local` marks keyspace notifications, which
/// a cluster only delivers on the node that raised them.
pub(super) async fn subscribe(
    transport: Transport,
    patterns: Vec<String>,
    node_local: bool,
) -> Result<Feed> {
    let spec = Spec {
        monitor: false,
        patterns,
    };
    let every_primary = node_local && transport.deployment() == Deployment::Cluster;
    start(transport, spec, every_primary).await
}

/// `MONITOR`, on every primary of a cluster.
pub(super) async fn monitor(transport: Transport) -> Result<Feed> {
    let spec = Spec {
        monitor: true,
        patterns: Vec::new(),
    };
    let every_primary = transport.deployment() == Deployment::Cluster;
    start(transport, spec, every_primary).await
}

async fn start(transport: Transport, spec: Spec, every_primary: bool) -> Result<Feed> {
    let (tx, rx) = mpsc::channel(BUFFER);
    if !every_primary {
        let ep = transport.default_endpoint().await?;
        let link = open(&transport, &ep, &spec).await?;
        let task = tokio::spawn(follow(transport, spec, ep, link, tx));
        return Ok(Feed {
            rx,
            _task: AbortOnDrop(task),
            nodes: 1,
        });
    }
    // A reshard or failover since the last discovery would otherwise leave a
    // primary out, or follow a demoted one, until the first check.
    let _ = transport.refresh().await;
    let primaries = transport.primaries().await;
    let attempts = futures_util::future::join_all(primaries.iter().map(|ep| {
        let (transport, spec) = (&transport, &spec);
        async move { (ep.clone(), open(transport, ep, spec).await) }
    }))
    .await;
    let mut links = Vec::new();
    let mut failed = Vec::new();
    for (ep, attempt) in attempts {
        match attempt {
            Ok(link) => links.push((ep, link)),
            Err(e) => failed.push((ep, format!("{e:#}"))),
        }
    }
    if links.is_empty() {
        let reasons: Vec<String> = failed.iter().map(|(_, e)| e.clone()).collect();
        anyhow::bail!("no primary could be reached: {}", reasons.join("; "));
    }
    for (ep, e) in &failed {
        let _ = tx.try_send(FeedEvent::Notice(format!(
            "Cannot reach node {}; it is missing from the feed until it answers: {e}",
            label(ep)
        )));
    }
    let nodes = links.len();
    let missing = failed.into_iter().map(|(ep, _)| ep).collect();
    let task = tokio::spawn(every(transport, spec, links, missing, tx));
    Ok(Feed {
        rx,
        _task: AbortOnDrop(task),
        nodes,
    })
}

/// One connection, on the node the profile routes keyless commands to. When
/// it drops, the topology is discovered again and the feed reconnects to
/// whichever node is the default now. A standalone feed simply ends.
async fn follow(
    transport: Transport,
    spec: Spec,
    mut ep: Endpoint,
    mut link: Link,
    tx: mpsc::Sender<FeedEvent>,
) {
    let deployment = transport.deployment();
    let mut delay = RETRY_MIN;
    let mut connected = Instant::now();
    loop {
        let reason = if deployment == Deployment::Sentinel {
            // A demoted primary can keep a connection open. Checking what
            // Sentinel names now and then moves the feed anyway.
            tokio::select! {
                ended = pump(link, None, &tx) => {
                    if !ended {
                        return;
                    }
                    format!("Lost the connection to {}", label(&ep))
                }
                moved = primary_moved(&transport, &ep) => {
                    format!("Sentinel now names {} as the primary instead of {}", label(&moved), label(&ep))
                }
            }
        } else {
            if !pump(link, None, &tx).await {
                return;
            }
            if deployment == Deployment::Standalone {
                return;
            }
            format!("Lost the connection to {}", label(&ep))
        };
        if connected.elapsed() >= HEALTHY {
            delay = RETRY_MIN;
        }
        let wait = if deployment == Deployment::Sentinel {
            "; waiting for Sentinel to name the primary"
        } else {
            "; reconnecting through another node"
        };
        if tx
            .send(FeedEvent::Notice(format!("{reason}{wait}")))
            .await
            .is_err()
        {
            return;
        }
        loop {
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(RETRY_MAX);
            if tx.is_closed() {
                return;
            }
            // Only an answer from Sentinel names the primary; a stale
            // address is never followed.
            if transport.refresh_after_loss().await.is_err() && deployment == Deployment::Sentinel {
                continue;
            }
            let target = transport.known_default().await;
            let Ok(next) = open(&transport, &target, &spec).await else {
                continue;
            };
            let note = match (deployment, target == ep) {
                (Deployment::Sentinel, false) => {
                    format!("Reconnected to the new primary {}", label(&target))
                }
                (_, true) => format!("Reconnected to {}", label(&target)),
                _ => format!("Reconnected through node {}", label(&target)),
            };
            if tx.send(FeedEvent::Notice(note)).await.is_err() {
                return;
            }
            ep = target;
            link = next;
            connected = Instant::now();
            break;
        }
    }
}

/// Resolves once the primary the profile routes to is no longer `ep`. Every
/// few seconds it asks Sentinel which node it names, which is one cheap
/// command. Only when that differs does a full discovery run, and only a
/// discovery, which checks the new node's role, moves the feed.
async fn primary_moved(transport: &Transport, ep: &Endpoint) -> Endpoint {
    loop {
        tokio::time::sleep(RETRY_MAX).await;
        let known = transport.known_default().await;
        if known != *ep {
            // Someone else's discovery already found the new primary.
            return known;
        }
        // Sentinel naming another node is only a hint; the discovery checks
        // the new node's role before the feed moves.
        if transport
            .sentinel_primary()
            .await
            .is_ok_and(|named| named != *ep)
            && transport.refresh_after_loss().await.is_ok()
        {
            let now = transport.known_default().await;
            if now != *ep {
                return now;
            }
        }
    }
}

/// One connection per primary, merged. A node that drops is reported, and the
/// topology is checked again so a node that comes back, or a replica promoted
/// in its place, joins the feed. The topology is also checked on a timer, so
/// primaries added or demoted while every connection stays up are noticed.
async fn every(
    transport: Transport,
    spec: Spec,
    links: Vec<(Endpoint, Link)>,
    mut missing: Vec<Endpoint>,
    tx: mpsc::Sender<FeedEvent>,
) {
    let mut readers: JoinSet<bool> = JoinSet::new();
    // The node each reader follows, by task, and each followed node's reader.
    let mut nodes: HashMap<tokio::task::Id, Endpoint> = HashMap::new();
    let mut active: HashMap<Endpoint, AbortHandle> = HashMap::new();
    let spawn = |readers: &mut JoinSet<bool>,
                 nodes: &mut HashMap<tokio::task::Id, Endpoint>,
                 active: &mut HashMap<Endpoint, AbortHandle>,
                 ep: Endpoint,
                 link: Link| {
        let tx = tx.clone();
        let node = label(&ep);
        let handle = readers.spawn(async move { pump(link, Some(node), &tx).await });
        nodes.insert(handle.id(), ep.clone());
        active.insert(ep, handle);
    };
    for (ep, link) in links {
        spawn(&mut readers, &mut nodes, &mut active, ep, link);
    }
    let next_check = |missing: &[Endpoint]| {
        Instant::now()
            + if missing.is_empty() {
                transport.feed_check()
            } else {
                RETRY_MAX
            }
    };
    let mut next = next_check(&missing);
    loop {
        let mut lost = false;
        tokio::select! {
            Some(done) = readers.join_next_with_id() => {
                let (id, ended) = match done {
                    Ok((id, ended)) => (id, Some(ended)),
                    Err(e) => (e.id(), None),
                };
                // A reader this loop stopped itself is already forgotten.
                let Some(ep) = nodes.remove(&id) else {
                    continue;
                };
                active.remove(&ep);
                let note = match ended {
                    Some(false) => return,
                    Some(true) => format!(
                        "Lost node {}; the other {} node(s) keep streaming",
                        label(&ep),
                        active.len()
                    ),
                    // It panicked. Forgetting it lets the node join again.
                    None => format!(
                        "The reader for node {} failed; the other {} node(s) keep streaming",
                        label(&ep),
                        active.len()
                    ),
                };
                if tx.send(FeedEvent::Notice(note)).await.is_err() {
                    return;
                }
                lost = true;
                // A node that closes connections as fast as they open
                // must not flood the feed with notices.
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            _ = tokio::time::sleep_until(next.into()) => {}
        }
        if tx.is_closed() {
            return;
        }
        let refreshed = if lost {
            transport.refresh_after_loss().await
        } else {
            transport.refresh().await
        };
        let primaries = transport.primaries().await;
        if refreshed.is_ok() {
            let gone: Vec<Endpoint> = active
                .keys()
                .filter(|ep| !primaries.contains(ep))
                .cloned()
                .collect();
            for ep in gone {
                if let Some(reader) = active.remove(&ep) {
                    nodes.remove(&reader.id());
                    reader.abort();
                }
                let note = format!(
                    "Node {} is no longer a primary; stopped following it",
                    label(&ep)
                );
                if tx.send(FeedEvent::Notice(note)).await.is_err() {
                    return;
                }
            }
        }
        missing.clear();
        for ep in primaries {
            if active.contains_key(&ep) {
                continue;
            }
            match open(&transport, &ep, &spec).await {
                Ok(link) => {
                    let note = format!("Following node {}", label(&ep));
                    spawn(&mut readers, &mut nodes, &mut active, ep, link);
                    if tx.send(FeedEvent::Notice(note)).await.is_err() {
                        return;
                    }
                }
                Err(_) => missing.push(ep),
            }
        }
        next = next_check(&missing);
    }
}
