#!/usr/bin/env bash
# CPU-vs-load curve at the headline workload.
#
#   ./scripts/run-curve.sh                                # embedded
#   DOCKER=1 ./scripts/run-curve.sh                       # include networked
#   BACKENDS=fc-embed,dashmap ./scripts/run-curve.sh      # focused subset
#   SERVER_CPUSET=0-3 DOCKER=1 ./scripts/run-curve.sh     # pin server cores

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$root/.." && pwd)"
# shellcheck source=_lib.sh
. "$here/_lib.sh"

cd "$ws_root"
cargo build --release -p shardcache-benchmarks
report_pinning

backends="${BACKENDS:-fc-embed,dashmap,moka,lru,rwlock-hashmap}"
server_pid_arg=()

if [[ "${DOCKER:-0}" == "1" ]]; then
  docker compose -f "$root/docker/compose.yml" up -d
  cargo build --release -p shardcache --features server --bin shardcache
  pinned_exec ./target/release/shardcache --bind-addr 127.0.0.1:6383 --shard-count 4 \
    >/tmp/shardcache.curve.log 2>&1 &
  fc_server_pid=$!
  trap 'kill $fc_server_pid 2>/dev/null || true; docker compose -f "$root/docker/compose.yml" down' EXIT
  sleep 1
  backends="$backends,fc-server-resp,fc-server-scnp,redis,valkey,dragonfly"
  server_pid_arg=(--server-pid "$fc_server_pid")
fi

ts="$(date +%Y%m%d_%H%M%S)"
out="$root/results/curve_${ts}.csv"
mkdir -p "$(dirname "$out")"

"$ws_root/target/release/curve" \
  --backends "$backends" \
  --addr 127.0.0.1:6383 \
  --value-size "${VALUE_SIZE:-512}" \
  --mix "${MIX:-80-20}" \
  --vcpu-budget "${VCPU_BUDGET:-4}" \
  --submitters "${SUBMITTERS:-16}" \
  --key-count "${KEY_COUNT:-100000}" \
  --target-rates "${TARGET_RATES:-100K,250K,500K,1M,2M,4M,8M}" \
  --warmup 2 \
  --duration 10 \
  --csv "$out" \
  ${server_pid_arg[@]+"${server_pid_arg[@]}"}

echo "wrote $out"
