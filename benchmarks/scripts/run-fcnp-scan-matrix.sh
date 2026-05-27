#!/usr/bin/env bash
# Run the FCNP SCAN benchmark against fast-cache's native protocol paths.

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$root/.." && pwd)"
# shellcheck source=_lib.sh
. "$here/_lib.sh"

cd "$ws_root"

fanout_addr="${FCNP_FANOUT_ADDR:-127.0.0.1:6383}"
direct_addr="${FCNP_DIRECT_ADDR:-127.0.0.1:6384}"
direct_base_port="${direct_addr##*:}"
shard_count="${SHARD_COUNT:-4}"

cargo build --release -p fast-cache --features redis-server --bin fast-cache-server
cargo build --release -p fast-cache-benchmarks --bin fcnp_scan_matrix
report_pinning

if [[ -n "${SERVER_CPUSET:-}" ]] && command -v taskset >/dev/null 2>&1; then
  env FAST_CACHE_DIRECT_SHARD_PORTS=1 FAST_CACHE_DIRECT_SHARD_BASE_PORT="$direct_base_port" \
    taskset -c "$SERVER_CPUSET" "$ws_root/target/release/fast-cache-server" \
    --bind-addr "$fanout_addr" \
    --shard-count "$shard_count" \
    --disable-persistence \
    --server-mode direct \
    >/tmp/fast-cache-server.fcnp-scan-matrix.log 2>&1 &
else
  env FAST_CACHE_DIRECT_SHARD_PORTS=1 FAST_CACHE_DIRECT_SHARD_BASE_PORT="$direct_base_port" \
    "$ws_root/target/release/fast-cache-server" \
    --bind-addr "$fanout_addr" \
    --shard-count "$shard_count" \
    --disable-persistence \
    --server-mode direct \
    >/tmp/fast-cache-server.fcnp-scan-matrix.log 2>&1 &
fi
fc_server_pid=$!

cleanup() {
  if [[ -n "${fc_server_pid:-}" ]]; then
    kill "$fc_server_pid" 2>/dev/null || true
    for _ in 1 2 3 4 5; do
      if ! kill -0 "$fc_server_pid" 2>/dev/null; then
        break
      fi
      sleep 0.2
    done
    kill -9 "$fc_server_pid" 2>/dev/null || true
    wait "$fc_server_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

sleep "${STARTUP_SLEEP:-1}"
mkdir -p "$root/results"

"$ws_root/target/release/fcnp_scan_matrix" \
  --fanout-addr "$fanout_addr" \
  --direct-addr "$direct_addr" \
  --shard-count "$shard_count" \
  --modes "${MODES:-generic,shard-fanout,shard-direct}" \
  --clients "${CLIENTS:-1}" \
  --warmup "${WARMUP:-1}" \
  --duration "${DURATION:-5}" \
  --key-count "${KEY_COUNT:-65536}" \
  --count "${SCAN_COUNT:-1000}" \
  --value-size "${VALUE_SIZE:-1}" \
  --csv "${CSV:-$root/results/fcnp-scan-matrix.csv}"
