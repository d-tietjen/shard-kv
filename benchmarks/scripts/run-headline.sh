#!/usr/bin/env bash
# Headline saturation table at the canonical workload.
#
# Defaults: embedded backends only.
# DOCKER=1                        include networked backends (Redis, Valkey,
#                                 Dragonfly, fast-cache-server RESP+FCNP).
# SERVER_CPUSET=0-3               pin fast-cache-server via taskset.
# FCNP=1                          include fc-server-fcnp alongside fc-server-resp.
# PIPELINE_DEPTH=16               enable network request pipelining where supported.

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$root/.." && pwd)"
# shellcheck source=_lib.sh
. "$here/_lib.sh"

cd "$ws_root"

cargo build --release -p fast-cache-benchmarks
report_pinning

backends="fc-embed,dashmap,moka,lru,rwlock-hashmap"
server_pid_arg=()

if [[ "${DOCKER:-0}" == "1" ]]; then
  docker compose -f "$root/docker/compose.yml" up -d
  cargo build --release -p fast-cache --features server --bin fast-cache-server
  pinned_exec ./target/release/fast-cache-server --bind-addr 127.0.0.1:6383 --shard-count 4 \
    >/tmp/fast-cache-server.headline.log 2>&1 &
  fc_server_pid=$!
  trap 'kill $fc_server_pid 2>/dev/null || true; docker compose -f "$root/docker/compose.yml" down' EXIT
  sleep 1
  backends="$backends,fc-server-resp"
  if [[ "${FCNP:-0}" == "1" ]]; then
    backends="$backends,fc-server-fcnp"
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
