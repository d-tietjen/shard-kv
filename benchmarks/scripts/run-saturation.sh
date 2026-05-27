#!/usr/bin/env bash
# Full saturation matrix: value_size x mix x vcpu_budget.
# Embedded by default; pass DOCKER=1 to include networked.
# SERVER_CPUSET=0-3 pins shardcache via taskset (Linux).
# PIPELINE_DEPTHS="1 4 16 64" adds network request pipelining to the matrix.

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
    >/tmp/shardcache.saturation.log 2>&1 &
  fc_server_pid=$!
  trap 'kill $fc_server_pid 2>/dev/null || true; docker compose -f "$root/docker/compose.yml" down' EXIT
  sleep 1
  backends="$backends,fc-server-resp,fc-server-scnp,redis,valkey,dragonfly"
  server_pid_arg=(--server-pid "$fc_server_pid")
fi

ts="$(date +%Y%m%d_%H%M%S)"
out="$root/results/saturation_${ts}.csv"
mkdir -p "$(dirname "$out")"

for value_size in 64 512 4096 65536 1048576; do
  for mix in 100-0 0-100 80-20; do
    for vcpu in 1 2 4 8 16; do
      for pipeline_depth in ${PIPELINE_DEPTHS:-${PIPELINE_DEPTH:-1}}; do
        "$ws_root/target/release/saturation" \
          --backends "$backends" \
          --addr 127.0.0.1:6383 \
          --value-size "$value_size" \
          --mix "$mix" \
          --vcpu-budget "$vcpu" \
          --clients 16 \
          --pipeline-depth "$pipeline_depth" \
          --key-count "$(python3 -c "print(min(100000, max(64, 4_000_000_000 // $value_size)))")" \
          --warmup 2 \
          --duration 10 \
          --csv "$out" \
          ${server_pid_arg[@]+"${server_pid_arg[@]}"}
      done
    done
  done
done

echo "wrote $out"
