#!/usr/bin/env bash
# Start local TCP servers and run a pinned client-count sweep.
#
# This is intended for benchmark hosts such as server where the server and client
# harness run on the same machine. It starts fast-cache FCNP direct, fast-cache
# RESP, and Redis in turn, pins each server to SERVER_CPUSET, and appends all
# rows to one CSV.

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$root/.." && pwd)"

server_cpuset="${SERVER_CPUSET:-0}"
shard_count="${SHARD_COUNT:-1}"
server_runtime="${SERVER_RUNTIME:-tokio}"
server_direct_shard_ports="${SERVER_DIRECT_SHARD_PORTS:-1}"
client_counts="${CLIENT_COUNTS:-1 16 64 256}"
pipeline_depths="${PIPELINE_DEPTHS:-1 64}"
vcpu_budget="${VCPU_BUDGET:-1}"
redis_image="${REDIS_IMAGE:-redis:7-alpine}"
fcnp_addr="${FCNP_ADDR:-127.0.0.1:6500}"
resp_addr="${RESP_ADDR:-127.0.0.1:6383}"
redis_addr="${REDIS_ADDR:-127.0.0.1:6379}"
redis_container="${REDIS_CONTAINER:-fc-client-sweep-redis}"
ts="$(date +%Y%m%d_%H%M%S)"
csv="${CSV:-$root/results/tcp_client_sweep_${ts}.csv}"

cd "$ws_root"

case "$server_runtime" in
  tokio)
    default_server_features="server"
    fcnp_server_env=()
    resp_server_env=()
    fcnp_backend="fc-server-fcnp"
    ;;
  monoio)
    default_server_features="server,monoio"
    fcnp_server_env=(FAST_CACHE_USE_MONOIO=1)
    resp_server_env=(FAST_CACHE_USE_MONOIO=1)
    fcnp_backend="fc-server-fcnp"
    ;;
  *)
    echo "SERVER_RUNTIME must be tokio or monoio" >&2
    exit 2
    ;;
esac

append_server_env_if_set() {
  local name="$1"
  local value="${!name:-}"
  if [[ -n "$value" ]]; then
    fcnp_server_env+=("$name=$value")
    resp_server_env+=("$name=$value")
  fi
}

if [[ "$server_runtime" == "monoio" ]]; then
  for name in \
    FAST_CACHE_MONOIO_DRIVER \
    FAST_CACHE_MONOIO_ENTRIES \
    FAST_CACHE_MONOIO_SAFE_WRITER \
    FAST_CACHE_MONOIO_SPLIT_WRITER \
    FAST_CACHE_MONOIO_WRITEV \
    FAST_CACHE_TCP_BUFFER_BYTES
  do
    append_server_env_if_set "$name"
  done
fi
append_server_env_if_set FAST_CACHE_ROUTE_MODE

server_features="${SERVER_FEATURES:-$default_server_features}"
if [[ "${SERVER_UNSAFE:-0}" != "0" && ",$server_features," != *",unsafe,"* ]]; then
  server_features="$server_features,unsafe"
fi
if [[ "$server_direct_shard_ports" != "0" ]]; then
  fcnp_server_env+=(FAST_CACHE_DIRECT_SHARD_PORTS=1)
  fcnp_backend="fc-server-fcnp-direct"
fi

if [[ "${BUILD:-1}" != "0" ]]; then
  cargo build --release -p fast-cache --features "$server_features" --bin fast-cache-server
  cargo build --release -p fast-cache-benchmarks --bin saturation
fi

mkdir -p "$(dirname "$csv")"

port_from_addr() {
  local addr="$1"
  echo "${addr##*:}"
}

host_from_addr() {
  local addr="$1"
  echo "${addr%:*}"
}

addr_with_port() {
  local addr="$1"
  local port="$2"
  echo "$(host_from_addr "$addr"):${port}"
}

fcnp_client_addr="$fcnp_addr"
if [[ "$server_direct_shard_ports" != "0" ]]; then
  direct_base_port="${FCNP_DIRECT_BASE_PORT:-$(( $(port_from_addr "$fcnp_addr") + 1 ))}"
  fcnp_server_env+=(FAST_CACHE_DIRECT_SHARD_BASE_PORT="$direct_base_port")
  fcnp_client_addr="${FCNP_DIRECT_ADDR:-$(addr_with_port "$fcnp_addr" "$direct_base_port")}"
fi

wait_port() {
  local port="$1"
  for _ in $(seq 1 100); do
    if ss -ltn | grep -Eq "127\\.0\\.0\\.1:${port}\\b|\\[::1\\]:${port}\\b"; then
      return 0
    fi
    sleep 0.1
  done
  echo "port ${port} did not open" >&2
  return 1
}

stop_pid() {
  local pid="${1:-}"
  if [[ -n "$pid" ]]; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
}

cleanup() {
  stop_pid "${fcnp_pid:-}"
  stop_pid "${fcnp_launcher_pid:-}"
  stop_pid "${resp_pid:-}"
  stop_pid "${resp_launcher_pid:-}"
  docker rm -f "$redis_container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

resolve_fast_cache_pid() {
  local addr="$1"
  pgrep -nf "fast-cache-server --bind-addr ${addr}" || true
}

run_depths() {
  local backend="$1"
  local addr="$2"
  local pid="$3"
  echo "client sweep backend=${backend} vcpu_budget=${vcpu_budget} clients=${client_counts} depths=${pipeline_depths}"
  CSV="$csv" \
    BACKENDS="$backend" \
    ADDR="$addr" \
    SERVER_PID="$pid" \
    VCPU_BUDGET="$vcpu_budget" \
    CLIENT_COUNTS="$client_counts" \
    PIPELINE_DEPTHS="$pipeline_depths" \
    VALUE_SIZE="${VALUE_SIZE:-64}" \
    MIX="${MIX:-80-20}" \
    KEY_COUNT="${KEY_COUNT:-100000}" \
    WARMUP="${WARMUP:-2}" \
    DURATION="${DURATION:-10}" \
    LATENCY_SAMPLE_RATE="${LATENCY_SAMPLE_RATE:-0}" \
    "$here/run-client-sweep.sh"
}

cleanup
echo "fast-cache server runtime=${server_runtime} features=${server_features} shards=${shard_count} fcnp_backend=${fcnp_backend} fcnp_client_addr=${fcnp_client_addr}"

if command -v taskset >/dev/null 2>&1; then
  env "${fcnp_server_env[@]}" \
    taskset -c "$server_cpuset" ./target/release/fast-cache-server \
    --server-mode direct --disable-persistence \
    --bind-addr "$fcnp_addr" --shard-count "$shard_count" \
    >/tmp/fc-fcnp-direct-client-sweep.log 2>&1 &
else
  env "${fcnp_server_env[@]}" \
    ./target/release/fast-cache-server \
    --server-mode direct --disable-persistence \
    --bind-addr "$fcnp_addr" --shard-count "$shard_count" \
    >/tmp/fc-fcnp-direct-client-sweep.log 2>&1 &
fi
fcnp_launcher_pid=$!
wait_port "$(port_from_addr "$fcnp_addr")"
if [[ "$server_direct_shard_ports" != "0" ]]; then
  wait_port "$(port_from_addr "$fcnp_client_addr")"
fi
fcnp_pid="$(resolve_fast_cache_pid "$fcnp_addr")"
run_depths "$fcnp_backend" "$fcnp_client_addr" "$fcnp_pid"
stop_pid "$fcnp_pid"
stop_pid "$fcnp_launcher_pid"
unset fcnp_pid
unset fcnp_launcher_pid

if command -v taskset >/dev/null 2>&1; then
  env "${resp_server_env[@]}" \
    taskset -c "$server_cpuset" ./target/release/fast-cache-server \
    --server-mode direct --disable-persistence \
    --bind-addr "$resp_addr" --shard-count "$shard_count" \
    >/tmp/fc-resp-client-sweep.log 2>&1 &
else
  env "${resp_server_env[@]}" \
    ./target/release/fast-cache-server \
    --server-mode direct --disable-persistence \
    --bind-addr "$resp_addr" --shard-count "$shard_count" \
    >/tmp/fc-resp-client-sweep.log 2>&1 &
fi
resp_launcher_pid=$!
wait_port "$(port_from_addr "$resp_addr")"
resp_pid="$(resolve_fast_cache_pid "$resp_addr")"
run_depths fc-server-resp "$resp_addr" "$resp_pid"
stop_pid "$resp_pid"
stop_pid "$resp_launcher_pid"
unset resp_pid
unset resp_launcher_pid

docker rm -f "$redis_container" >/dev/null 2>&1 || true
docker run -d --rm \
  --name "$redis_container" \
  --cpuset-cpus="$server_cpuset" \
  -p "$redis_addr:6379" \
  "$redis_image" \
  redis-server --save "" --appendonly no >/tmp/redis-client-sweep.cid
wait_port "$(port_from_addr "$redis_addr")"
redis_pid="$(docker inspect --format '{{.State.Pid}}' "$redis_container")"
run_depths redis "$redis_addr" "$redis_pid"
docker rm -f "$redis_container" >/dev/null 2>&1 || true

echo "wrote $csv"
sed -n "1,240p" "$csv"
