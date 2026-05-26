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
#   CASES=extended-no-keyspace
#   CASES=profile:keyspace
#   CASES=profile:destructive
#   SKIP_CASES=OBJECT,COPY
#   FIXTURE_SCOPE=shared-keyspace
#   CLIENTS=4
#   KEY_SHARDS=4
#   PIPELINE_DEPTH=16
#   WARMUP=2
#   DURATION=10
#   START_FAST_CACHE=0
#   DOCKER=0
#   DOCKER_SERVICES="redis valkey dragonfly"
#   DOCKER_CPUSET=0
#   SERVER_DIRECT_SHARD_PORTS=1
#   FAST_CACHE_DIRECT_SHARD_BASE_PORT=6384

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$root/.." && pwd)"
# shellcheck source=_lib.sh
. "$here/_lib.sh"

cd "$ws_root"

fc_addr="${FAST_CACHE_ADDR:-127.0.0.1:6383}"
targets="${TARGETS:-fast-cache=$fc_addr,redis=127.0.0.1:6379,valkey=127.0.0.1:6381}"
start_fast_cache="${START_FAST_CACHE:-1}"
server_env=()
if [[ "${SERVER_DIRECT_SHARD_PORTS:-0}" != "0" ]]; then
  server_env+=(FAST_CACHE_DIRECT_SHARD_PORTS=1)
  if [[ -n "${FAST_CACHE_DIRECT_SHARD_BASE_PORT:-}" ]]; then
    server_env+=(FAST_CACHE_DIRECT_SHARD_BASE_PORT="$FAST_CACHE_DIRECT_SHARD_BASE_PORT")
  fi
fi

if [[ "$start_fast_cache" == "1" ]]; then
  cargo build --release -p fast-cache --features redis-server --bin fast-cache-server
fi
cargo build --release -p fast-cache-benchmarks --bin redis_command_matrix
report_pinning

if [[ "${DOCKER:-1}" == "1" ]]; then
  # shellcheck disable=SC2206
  docker_services=(${DOCKER_SERVICES:-redis valkey})
  docker compose -f "$root/docker/compose.yml" up -d "${docker_services[@]}"
  if [[ -n "${DOCKER_CPUSET:-}" ]]; then
    for service in "${docker_services[@]}"; do
      docker update --cpuset-cpus "$DOCKER_CPUSET" "bench-$service" >/dev/null
    done
  fi
fi

fc_server_pid=""
if [[ "$start_fast_cache" == "1" ]]; then
  server_cmd=("$ws_root/target/release/fast-cache-server")
  if [[ -n "${SERVER_CPUSET:-}" ]] && command -v taskset >/dev/null 2>&1; then
    server_cmd=(taskset -c "$SERVER_CPUSET" "${server_cmd[@]}")
  fi
  server_cmd+=(
    --bind-addr "$fc_addr"
    --shard-count "${SHARD_COUNT:-4}"
    --disable-persistence
    --server-mode direct
  )
  if [[ "${#server_env[@]}" -gt 0 ]]; then
    env "${server_env[@]}" "${server_cmd[@]}" \
      >/tmp/fast-cache-server.redis-command-matrix.log 2>&1 &
  else
    "${server_cmd[@]}" \
      >/tmp/fast-cache-server.redis-command-matrix.log 2>&1 &
  fi
  fc_server_pid=$!
fi

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

extra_args=()
if [[ -n "${SKIP_CASES:-}" ]]; then
  extra_args+=(--skip-cases "$SKIP_CASES")
fi
if [[ -n "${FIXTURE_SCOPE:-}" ]]; then
  extra_args+=(--fixture-scope "$FIXTURE_SCOPE")
fi
if [[ -n "${FAIL_ON_ERROR:-}" ]]; then
  extra_args+=(--fail-on-error)
fi

matrix_cmd=(
  "$ws_root/target/release/redis_command_matrix"
  --targets "$targets"
  --cases "${CASES:-all}"
  --clients "${CLIENTS:-1}"
  --key-shards "${KEY_SHARDS:-1}"
  --pipeline-depth "${PIPELINE_DEPTH:-1}"
  --warmup "${WARMUP:-1}"
  --duration "${DURATION:-5}"
  --csv "${CSV:-$root/results/redis-command-matrix.csv}"
)
if [[ "${#extra_args[@]}" -gt 0 ]]; then
  matrix_cmd+=("${extra_args[@]}")
fi

"${matrix_cmd[@]}"
