#!/usr/bin/env bash
# Publish bursty test traffic so the pub/sub feed has something to draw.
#
#   scripts/pubsub-traffic.sh [seconds] [host] [port]
#   scripts/pubsub-traffic.sh --keyspace [seconds] [host] [port]
#
# Default: 60 seconds against 127.0.0.1:6379.
# In rediscope: press P and subscribe to '*' (or 'news:*'), or press N for the
# keyspace feed and run this with --keyspace.
set -euo pipefail

keyspace=false
if [[ "${1:-}" == "--keyspace" ]]; then
  keyspace=true
  shift
fi

seconds="${1:-60}"
host="${2:-127.0.0.1}"
port="${3:-6379}"
cli=(redis-cli -h "$host" -p "$port")

command -v redis-cli >/dev/null || { echo "redis-cli not found" >&2; exit 1; }
"${cli[@]}" ping >/dev/null || { echo "cannot reach $host:$port" >&2; exit 1; }

if $keyspace; then
  echo "Enabling keyspace notifications (notify-keyspace-events KEA)"
  "${cli[@]}" config set notify-keyspace-events KEA >/dev/null
fi

channels=(news:eu news:us orders metrics:cpu)
end=$((SECONDS + seconds))
count=0

cleanup() { echo; echo "published $count message(s)"; }
trap cleanup EXIT

echo "publishing to ${channels[*]} for ${seconds}s — ctrl+c stops"
while (( SECONDS < end )); do
  # Every few rounds, send a burst so the sparkline spikes and one channel
  # pulls ahead in the breakdown.
  burst=1
  (( RANDOM % 8 == 0 )) && burst=$(( 15 + RANDOM % 25 ))

  for ((i = 0; i < burst; i++)); do
    channel="${channels[$((RANDOM % ${#channels[@]}))]}"
    if $keyspace; then
      key="traffic:$((RANDOM % 50))"
      "${cli[@]}" set "$key" "$RANDOM" ex 30 >/dev/null
      (( RANDOM % 4 == 0 )) && "${cli[@]}" del "$key" >/dev/null
    else
      payload=$(printf '{"id":%d,"channel":"%s","level":"%s","body":"event %d"}' \
        "$count" "$channel" "$([[ $((RANDOM % 5)) == 0 ]] && echo warn || echo info)" "$count")
      "${cli[@]}" publish "$channel" "$payload" >/dev/null
    fi
    count=$((count + 1))
  done

  sleep "0.$(( 1 + RANDOM % 6 ))"
done
