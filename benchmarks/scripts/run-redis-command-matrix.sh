#!/usr/bin/env bash
# Run the RESP command matrix against fast-cache, Redis, and Valkey.
#
# Defaults:
#   - starts Redis and Valkey via benchmarks/docker/compose.yml
#   - starts fast-cache-server on 127.0.0.1:6383 with redis-server features
#   - writes benchmarks/results/redis-command-matrix.csv
#
# Useful overrides:
#   TARGETS=fast-cache=127.0.0.1:6383,redis=127.0.0.1:6379,valkey=127.0.0.1:6381
#   CASES=hash,zset
#   CASES=large
#   CLIENTS=4
#   WARMUP=2
#   DURATION=10
#   DOCKER=0

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$root/.." && pwd)"
# shellcheck source=_lib.sh
. "$here/_lib.sh"

cd "$ws_root"

fc_addr="${FAST_CACHE_ADDR:-127.0.0.1:6383}"
targets="${TARGETS:-fast-cache=$fc_addr,redis=127.0.0.1:6379,valkey=127.0.0.1:6381}"

cargo build --release -p fast-cache --features redis-server --bin fast-cache-server
cargo build --release -p fast-cache-benchmarks --bin redis_command_matrix
report_pinning

if [[ "${DOCKER:-1}" == "1" ]]; then
  docker compose -f "$root/docker/compose.yml" up -d redis valkey
fi

if [[ -n "${SERVER_CPUSET:-}" ]] && command -v taskset >/dev/null 2>&1; then
  taskset -c "$SERVER_CPUSET" "$ws_root/target/release/fast-cache-server" \
    --bind-addr "$fc_addr" \
    --shard-count "${SHARD_COUNT:-4}" \
    --disable-persistence \
    --server-mode direct \
    >/tmp/fast-cache-server.redis-command-matrix.log 2>&1 &
else
  "$ws_root/target/release/fast-cache-server" \
    --bind-addr "$fc_addr" \
    --shard-count "${SHARD_COUNT:-4}" \
    --disable-persistence \
    --server-mode direct \
    >/tmp/fast-cache-server.redis-command-matrix.log 2>&1 &
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
  if [[ "${DOCKER:-1}" == "1" ]]; then
    docker compose -f "$root/docker/compose.yml" down
  fi
}
trap cleanup EXIT

sleep "${STARTUP_SLEEP:-1}"
mkdir -p "$root/results"

"$ws_root/target/release/redis_command_matrix" \
  --targets "$targets" \
  --cases "${CASES:-all}" \
  --clients "${CLIENTS:-1}" \
  --warmup "${WARMUP:-1}" \
  --duration "${DURATION:-5}" \
  --csv "${CSV:-$root/results/redis-command-matrix.csv}" \
  ${FAIL_ON_ERROR:+--fail-on-error}
