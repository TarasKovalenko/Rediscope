//! Live feeds on every kind of profile: pub/sub and keyspace events.
//!
//! A feed needs connections of its own, because a subscribed connection can
//! do nothing else. Where they go depends on the deployment:
//!
//! - A standalone profile uses the server it connected to.
//! - A Sentinel profile uses the primary Sentinel names, and follows it to a
//!   new primary after a failover.
//! - A cluster broadcasts `PUBLISH` to every node, so channel messages need one
//!   subscription on the default node, followed to another node if it goes.
//! - Keyspace notifications never leave the node that raised them, so on a
//!   cluster a keyspace feed subscribes on every primary and merges what they
//!   send, labelled by node. A lost node is reported and the others go on.
use super::topology::Transport;
use crate::config::Deployment;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::{collections::HashSet, time::Duration};
use tokio::{sync::mpsc, task::JoinSet};

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

/// Events waiting for the reader. A reader slower than the server pushes back
/// on the sockets instead of queueing without bound.
const BUFFER: usize = 4096;
/// How long opening one node's connection and subscribing may take.
const OPEN_TIMEOUT: Duration = Duration::from_secs(5);
/// The longest wait between attempts to reach a node again.
const RETRY_MAX: Duration = Duration::from_secs(5);

/// The connection a feed reads from.
enum Link {
    PubSub(redis::aio::PubSub),
}

/// What a feed is made of.
#[derive(Clone)]
struct Spec {
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
    }
}

/// Subscribe to `patterns`. `node_local` marks keyspace notifications, which
/// a cluster only delivers on the node that raised them.
pub(super) async fn subscribe(
    transport: Transport,
    patterns: Vec<String>,
    node_local: bool,
) -> Result<Feed> {
    let spec = Spec { patterns };
    let every_primary = node_local && transport.deployment() == Deployment::Cluster;
    start(transport, spec, every_primary).await
}

async fn start(transport: Transport, spec: Spec, every_primary: bool) -> Result<Feed> {
    let (tx, rx) = mpsc::channel(BUFFER);
    if !every_primary {
        let ep = transport.default_endpoint().await;
        let link = open(&transport, &ep, &spec).await?;
        let task = tokio::spawn(follow(transport, spec, ep, link, tx));
        return Ok(Feed {
            rx,
            _task: AbortOnDrop(task),
            nodes: 1,
        });
    }
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
            "Cannot reach node {}; its events are missing until it answers: {e}",
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
        let mut delay = Duration::from_millis(250);
        loop {
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(RETRY_MAX);
            if tx.is_closed() {
                return;
            }
            // Only an answer from Sentinel names the primary; a stale
            // address is never followed.
            if transport.refresh().await.is_err() && deployment == Deployment::Sentinel {
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
            break;
        }
    }
}

/// Resolves once the primary the profile routes to is no longer `ep`.
async fn primary_moved(transport: &Transport, ep: &Endpoint) -> Endpoint {
    loop {
        tokio::time::sleep(RETRY_MAX).await;
        let now = transport.default_endpoint().await;
        if now != *ep {
            return now;
        }
    }
}

/// One connection per primary, merged. A node that drops is reported, and the
/// topology is checked again so a node that comes back, or a replica promoted
/// in its place, joins the feed.
async fn every(
    transport: Transport,
    spec: Spec,
    links: Vec<(Endpoint, Link)>,
    mut missing: Vec<Endpoint>,
    tx: mpsc::Sender<FeedEvent>,
) {
    let mut readers: JoinSet<(Endpoint, bool)> = JoinSet::new();
    let mut active = HashSet::new();
    let spawn = |readers: &mut JoinSet<(Endpoint, bool)>, ep: Endpoint, link: Link| {
        let tx = tx.clone();
        readers.spawn(async move {
            let ended = pump(link, Some(label(&ep)), &tx).await;
            (ep, ended)
        });
    };
    for (ep, link) in links {
        active.insert(ep.clone());
        spawn(&mut readers, ep, link);
    }
    loop {
        tokio::select! {
            Some(done) = readers.join_next() => {
                let Ok((ep, ended)) = done else {
                    continue;
                };
                if !ended {
                    return;
                }
                active.remove(&ep);
                let note = format!(
                    "Lost node {}; the other {} node(s) keep streaming",
                    label(&ep),
                    active.len()
                );
                if tx.send(FeedEvent::Notice(note)).await.is_err() {
                    return;
                }
                // A node that closes connections as fast as they open
                // must not flood the feed with notices.
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            _ = tokio::time::sleep(RETRY_MAX), if !missing.is_empty() => {}
            else => tokio::time::sleep(RETRY_MAX).await,
        }
        if tx.is_closed() {
            return;
        }
        let _ = transport.refresh().await;
        missing.clear();
        for ep in transport.primaries().await {
            if active.contains(&ep) {
                continue;
            }
            match open(&transport, &ep, &spec).await {
                Ok(link) => {
                    active.insert(ep.clone());
                    let note = format!("Following node {}", label(&ep));
                    spawn(&mut readers, ep, link);
                    if tx.send(FeedEvent::Notice(note)).await.is_err() {
                        return;
                    }
                }
                Err(_) => missing.push(ep),
            }
        }
    }
}
