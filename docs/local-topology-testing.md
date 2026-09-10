Local Cluster and Sentinel testing
==================================

These steps target macOS with zsh/bash, matching this workspace. Commands run
from the repository root. Keep the same terminal for setup so its variables
remain available. All servers bind to loopback and use temporary directories.
Cluster/Sentinel profiles are read-only in this first release; seed data with
`redis-cli`.

1. Check prerequisites and build

```sh
cd /Users/taras/Projects/Github/Rediscope
rustc --version
cargo --version
redis-server --version
redis-cli --version
```

The project requires Rust 1.88 or newer. If Redis is missing and you use
Homebrew, install it with `brew install redis`. You do not need to start a
Homebrew Redis service for these tests.

Build the modified source, then use its binary explicitly:

```sh
cargo build --release
./target/release/rediscope --help
```

2. Run automated checks

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo test --test topology -- --nocapture
cargo test --test live_topology -- --ignored --nocapture
```

Expected results:

- `topology`: 10 passing tests covering MOVED, ASK, metadata redirects,
  partial results, separate Sentinel credentials, replica rejection,
  discovery after disconnect, and no replay of uncertain writes/pipelines.
- `live_topology`: one passing test. It starts three real cluster primaries,
  checks browsing, stops one node, checks partial results, and tests real
  Sentinel primary discovery. It cleans up its own processes and files.
- Existing standalone/TLS integration tests return early without their
  environment variables. Step 3 enables the standalone suite explicitly.

The real topology fixture tests Sentinel discovery; its failover/recovery
protocol test uses simulated peers. Step 7 below tests a real promotion.

3. Create an isolated manual lab and test standalone Redis

The manual lab uses ports 17300–17302, 17400–17401, 17500, and 27400;
cluster bus ports are 27300–27302. Check that these ports are unused first.
On macOS, this prints listeners if any conflict exists:

```sh
lsof -nP -iTCP -sTCP:LISTEN | grep -E ':(1730[0-2]|1740[01]|17500|2730[0-2]|27400) '
```

No output means no matching listeners. If ports are occupied, change the lab
ports consistently before proceeding; do not stop unrelated Redis instances.

```sh
export REDISCOPE_LAB=$(mktemp -d /tmp/rediscope-lab.XXXXXX)
export REDISCOPE_HOME="$REDISCOPE_LAB/profiles"
mkdir -p "$REDISCOPE_HOME" "$REDISCOPE_LAB/standalone"
printf 'Lab directory: %s\n' "$REDISCOPE_LAB"

redis-server --bind 127.0.0.1 --port 17500 \
  --daemonize yes --save '' --appendonly no \
  --dir "$REDISCOPE_LAB/standalone" \
  --pidfile "$REDISCOPE_LAB/standalone/redis.pid" \
  --logfile "$REDISCOPE_LAB/standalone/redis.log"

redis-cli -p 17500 PING
REDISCOPE_TEST_PORT=17500 cargo test --test integration
```

Expect `PONG` and 21 passing integration tests. They write/delete test data in
several databases on this disposable instance.

4. Start three cluster primaries

```sh
for port in 17300 17301 17302; do
  mkdir -p "$REDISCOPE_LAB/cluster-$port"
  cat > "$REDISCOPE_LAB/cluster-$port/redis.conf" <<EOF_CONFIG
bind 127.0.0.1
port $port
daemonize yes
dir "$REDISCOPE_LAB/cluster-$port"
pidfile "$REDISCOPE_LAB/cluster-$port/redis.pid"
logfile "$REDISCOPE_LAB/cluster-$port/redis.log"
save ""
appendonly no
cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 1000
cluster-require-full-coverage no
EOF_CONFIG
  redis-server "$REDISCOPE_LAB/cluster-$port/redis.conf"
done

redis-cli --cluster create \
  127.0.0.1:17300 127.0.0.1:17301 127.0.0.1:17302 \
  --cluster-replicas 0 --cluster-yes
```

Check each node before seeding data:

```sh
for port in 17300 17301 17302; do
  redis-cli -p "$port" CLUSTER INFO
done
```

Each should report `cluster_state:ok`. If it reports `fail` immediately after
creation, wait a few seconds and repeat the check.

```sh
for i in $(seq 1 90); do
  redis-cli -c -p 17300 SET "demo:item:$i" "value-$i" >/dev/null
done
redis-cli -c -p 17300 HSET 'demo:{user}:1' name Alice role reader
redis-cli -c -p 17300 RPUSH 'demo:{queue}:jobs' job-1 job-2
```

This creates 92 keys across the cluster. The three-primary lab has no replicas:
it tests partial availability and node restart, rather than cluster replica
promotion. `cluster-require-full-coverage no` permits healthy slots to remain
usable when another primary is unavailable. See the [Redis Cluster guide](https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/).

5. Add profiles and browse the cluster

Write profiles only inside the temporary `REDISCOPE_HOME`:

```sh
cat > "$REDISCOPE_HOME/connections.json" <<'EOF_PROFILES'
{
  "connections": [
    {
      "name": "local-cluster",
      "deployment": "cluster",
      "host": "127.0.0.1",
      "port": 17300,
      "db": 0,
      "seeds": ["127.0.0.1:17301", "127.0.0.1:17302"]
    },
    {
      "name": "local-sentinel",
      "deployment": "sentinel",
      "host": "127.0.0.1",
      "port": 27400,
      "sentinel_master": "rediscope-local"
    }
  ]
}
EOF_PROFILES

./target/release/rediscope --profile local-cluster keys --pattern 'demo:*'
./target/release/rediscope --profile local-cluster info
./target/release/rediscope --profile local-cluster
```

In the TUI:

- Browse the `demo` folder. Expect 92 keys in total.
- Open keys and check their values and displayed hash slots.
- Press `i`, then navigate to the Cluster tab: expect three primaries and
  slot ranges covering 0–16383. The All view includes per-primary INFO.
- Close the modal with Escape. Press `r` to refresh keys and topology.
- Try editing/deleting a key: Rediscope should refuse because the deployment
  is read-only. A standalone profile aimed at a cluster seed does not enable
  discovery; use the Cluster profile above.

6. Test cluster partial results and recovery

Quit the TUI with `q`, or use another terminal for the Redis commands.
Stop only the lab's third node, saving its keys for the restart test:

```sh
redis-cli -p 17302 SHUTDOWN SAVE
./target/release/rediscope --profile local-cluster keys --pattern 'demo:*'
./target/release/rediscope --profile local-cluster
```

Expect reachable keys to remain visible, a `PARTIAL RESULTS` indicator, and
an unavailable-node warning. Values on the stopped node are unavailable.
The CLI prints warnings on stderr. Refresh may take several seconds while
connection attempts time out.

Restart the node using the same directory/configuration:

```sh
redis-server "$REDISCOPE_LAB/cluster-17302/redis.conf"
redis-cli -p 17302 PING
```

Wait for cluster convergence, then press `r` in Rediscope. Expect all 92 keys
to return and the partial-results indicator to clear. The saved RDB keeps
this test's keys across restart.

For deterministic MOVED/ASK coverage, run:

```sh
cargo test --test topology moved_is_cached_and_ask_is_one_request_on_a_dedicated_socket
cargo test --test topology scan_metadata_follows_redirects_after_a_key_moves
```

7. Start Sentinel with a primary and replica; test failover

```sh
for port in 17400 17401; do
  mkdir -p "$REDISCOPE_LAB/sentinel-data-$port"
  redis-server --bind 127.0.0.1 --port "$port" \
    --daemonize yes --save '' --appendonly no \
    --dir "$REDISCOPE_LAB/sentinel-data-$port" \
    --pidfile "$REDISCOPE_LAB/sentinel-data-$port/redis.pid" \
    --logfile "$REDISCOPE_LAB/sentinel-data-$port/redis.log"
done

redis-cli -p 17401 REPLICAOF 127.0.0.1 17400
redis-cli -p 17400 SET sentinel:hello world
redis-cli -p 17401 GET sentinel:hello
```

Wait and repeat the final GET until the replica returns `world`.

```sh
mkdir -p "$REDISCOPE_LAB/sentinel"
cat > "$REDISCOPE_LAB/sentinel/sentinel.conf" <<EOF_SENTINEL
bind 127.0.0.1
port 27400
daemonize yes
dir "$REDISCOPE_LAB/sentinel"
pidfile "$REDISCOPE_LAB/sentinel/redis.pid"
logfile "$REDISCOPE_LAB/sentinel/redis.log"
sentinel monitor rediscope-local 127.0.0.1 17400 1
sentinel down-after-milliseconds rediscope-local 1000
sentinel failover-timeout rediscope-local 5000
sentinel parallel-syncs rediscope-local 1
EOF_SENTINEL

redis-server "$REDISCOPE_LAB/sentinel/sentinel.conf" --sentinel
redis-cli -p 27400 SENTINEL get-master-addr-by-name rediscope-local
redis-cli -p 27400 SENTINEL replicas rediscope-local
./target/release/rediscope --profile local-sentinel
```

The primary address should initially be port 17400. Confirm Sentinel's replica
list includes 17401 before testing failure. In Rediscope, open `sentinel:hello`
and verify `world`.

Keep Rediscope open. In a second terminal, run:

```sh
redis-cli -p 17400 SHUTDOWN NOSAVE
redis-cli -p 27400 SENTINEL get-master-addr-by-name rediscope-local
```

Repeat the address query until it reports port 17401. In Rediscope, press `r`
and reopen `sentinel:hello`. A transient error during election is expected;
after promotion, refreshing should recover without recreating the profile.
The Cluster diagnostics tab also shows the discovered Sentinel primary.

This is a one-Sentinel local failover exercise. A robust deployment uses
multiple independent Sentinels; see the [Redis Sentinel guide](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/).

8. Check write safety and stop the lab

Run the deterministic lost-reply tests:

```sh
cargo test --test topology disconnected_reads_reconnect_but_unknown_writes_are_never_replayed
cargo test --test topology a_lost_write_pipeline_is_not_replayed
```

Both should pass. They simulate receiving a write and closing the connection
before replying, then verify Rediscope does not resend it.

Quit Rediscope. From the original setup terminal, stop the lab servers:

```sh
for port in 27400 17300 17301 17302 17400 17401 17500; do
  redis-cli -p "$port" SHUTDOWN NOSAVE 2>/dev/null || true
done
printf 'Temporary files and logs remain at: %s\n' "$REDISCOPE_LAB"
unset REDISCOPE_HOME
unset REDISCOPE_LAB
```

A node already stopped during testing may report a connection failure; that is
normal. Keep the temporary directory if you want to inspect logs. Unsetting
`REDISCOPE_HOME` restores your usual Rediscope profile location.

Current limits to expect

- Cluster/Sentinel writes and imports are disabled, even with `read_only: false`.
- Cluster accepts database 0 only.
- Cluster namespace-memory rollups and discovered-profile pub/sub are unavailable.
- Browser discovery spans primaries; raw node-local commands such as SCAN
  still describe one diagnostic endpoint.
- The automated real smoke test and protocol tests passed during implementation.
  The extended manual Sentinel promotion exercise is a walkthrough to run,
  not a claim that every manual step has already been executed.
