#!/usr/bin/env bash
# Headline saturation table at the canonical workload.
#
# Defaults: embedded backends only.
# DOCKER=1                        include networked backends (Redis, Valkey,
#                                 Dragonfly, shardcache RESP+SCNP).
# SERVER_CPUSET=0-3               pin shardcache via taskset.
# SCNP=1                          include fc-server-scnp alongside fc-server-resp.
# PIPELINE_DEPTH=16               enable network request pipelining where supported.

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$root/.." && pwd)"
# shellcheck source=_lib.sh
. "$here/_lib.sh"

cd "$ws_root"

cargo build --release -p shardcache-benchmarks
report_pinning

backends="fc-embed,dashmap,moka,lru,rwlock-hashmap"
server_pid_arg=()

if [[ "${DOCKER:-0}" == "1" ]]; then
  docker compose -f "$root/docker/compose.yml" up -d
  cargo build --release -p shardcache --features server --bin shardcache
  pinned_exec ./target/release/shardcache --bind-addr 127.0.0.1:6383 --shard-count 4 \
    >/tmp/shardcache.headline.log 2>&1 &
  fc_server_pid=$!
  trap 'kill $fc_server_pid 2>/dev/null || true; docker compose -f "$root/docker/compose.yml" down' EXIT
  sleep 1
  backends="$backends,fc-server-resp"
  if [[ "${SCNP:-0}" == "1" ]]; then
    backends="$backends,fc-server-scnp"
  fi
  backends="$backends,redis,valkey,dragonfly"
  server_pid_arg=(--server-pid "$fc_server_pid")
fi

mkdir -p "$root/results"

"$ws_root/target/release/saturation" \
  --backends "$backends" \
  --addr 127.0.0.1:6383 \
  --value-size 512 \
  --mix 80-20 \
  --vcpu-budget 4 \
  --clients 16 \
  --pipeline-depth "${PIPELINE_DEPTH:-1}" \
  --key-count 100000 \
  --warmup 2 \
  --duration 10 \
  --csv "$root/results/headline.csv" \
  ${server_pid_arg[@]+"${server_pid_arg[@]}"}
