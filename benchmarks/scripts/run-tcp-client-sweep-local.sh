#!/usr/bin/env bash
# Start local TCP servers and run a pinned client-count sweep.
#
# This is intended for benchmark hosts such as server where the server and client
# harness run on the same machine. It starts shardcache SCNP direct, shardcache
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
scnp_addr="${SCNP_ADDR:-127.0.0.1:6500}"
resp_addr="${RESP_ADDR:-127.0.0.1:6383}"
redis_addr="${REDIS_ADDR:-127.0.0.1:6379}"
redis_container="${REDIS_CONTAINER:-fc-client-sweep-redis}"
ts="$(date +%Y%m%d_%H%M%S)"
csv="${CSV:-$root/results/tcp_client_sweep_${ts}.csv}"

cd "$ws_root"

case "$server_runtime" in
  tokio)
    default_server_features="server"
    scnp_server_env=()
    resp_server_env=()
    scnp_backend="fc-server-scnp"
    ;;
  monoio)
    default_server_features="server,monoio"
    scnp_server_env=(SHARDCACHE_USE_MONOIO=1)
    resp_server_env=(SHARDCACHE_USE_MONOIO=1)
    scnp_backend="fc-server-scnp"
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
    scnp_server_env+=("$name=$value")
    resp_server_env+=("$name=$value")
  fi
}

if [[ "$server_runtime" == "monoio" ]]; then
  for name in \
    SHARDCACHE_MONOIO_DRIVER \
    SHARDCACHE_MONOIO_ENTRIES \
    SHARDCACHE_MONOIO_SAFE_WRITER \
    SHARDCACHE_MONOIO_SPLIT_WRITER \
    SHARDCACHE_MONOIO_WRITEV \
    SHARDCACHE_TCP_BUFFER_BYTES
  do
    append_server_env_if_set "$name"
  done
fi
append_server_env_if_set SHARDCACHE_ROUTE_MODE

server_features="${SERVER_FEATURES:-$default_server_features}"
if [[ "${SERVER_UNSAFE:-0}" != "0" && ",$server_features," != *",unsafe,"* ]]; then
  server_features="$server_features,unsafe"
fi
if [[ "$server_direct_shard_ports" != "0" ]]; then
  scnp_server_env+=(SHARDCACHE_DIRECT_SHARD_PORTS=1)
  scnp_backend="fc-server-scnp-direct"
fi

if [[ "${BUILD:-1}" != "0" ]]; then
  cargo build --release -p shardcache --features "$server_features" --bin shardcache
  cargo build --release -p shardcache-benchmarks --bin saturation
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

scnp_client_addr="$scnp_addr"
if [[ "$server_direct_shard_ports" != "0" ]]; then
  direct_base_port="${SCNP_DIRECT_BASE_PORT:-$(( $(port_from_addr "$scnp_addr") + 1 ))}"
  scnp_server_env+=(SHARDCACHE_DIRECT_SHARD_BASE_PORT="$direct_base_port")
  scnp_client_addr="${SCNP_DIRECT_ADDR:-$(addr_with_port "$scnp_addr" "$direct_base_port")}"
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
  stop_pid "${scnp_pid:-}"
  stop_pid "${scnp_launcher_pid:-}"
  stop_pid "${resp_pid:-}"
  stop_pid "${resp_launcher_pid:-}"
  docker rm -f "$redis_container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

resolve_shardcache_pid() {
  local addr="$1"
  pgrep -nf "shardcache --bind-addr ${addr}" || true
}

run_depths() {
  local backend="$1"
  local addr="$2"
  local pid="$3"
  # The SCNP direct-shard client must open one connection per server shard and
  # route each key to its owner, independent of vcpu_budget. Pass the server's
  # shard_count only for that backend; leave SCNP_SHARDS empty otherwise.
  local scnp_shards=""
  if [[ "$backend" == fc-server-scnp-direct* ]]; then
    scnp_shards="$shard_count"
  fi
  echo "client sweep backend=${backend} vcpu_budget=${vcpu_budget} scnp_shards=${scnp_shards:-n/a} clients=${client_counts} depths=${pipeline_depths}"
  CSV="$csv" \
    BACKENDS="$backend" \
    ADDR="$addr" \
    SERVER_PID="$pid" \
    VCPU_BUDGET="$vcpu_budget" \
    SCNP_SHARDS="$scnp_shards" \
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
echo "shardcache server runtime=${server_runtime} features=${server_features} shards=${shard_count} scnp_backend=${scnp_backend} scnp_client_addr=${scnp_client_addr}"

if command -v taskset >/dev/null 2>&1; then
  env "${scnp_server_env[@]}" \
    taskset -c "$server_cpuset" ./target/release/shardcache \
    --server-mode direct --disable-persistence \
    --bind-addr "$scnp_addr" --shard-count "$shard_count" \
    >/tmp/fc-scnp-direct-client-sweep.log 2>&1 &
else
  env "${scnp_server_env[@]}" \
    ./target/release/shardcache \
    --server-mode direct --disable-persistence \
    --bind-addr "$scnp_addr" --shard-count "$shard_count" \
    >/tmp/fc-scnp-direct-client-sweep.log 2>&1 &
fi
scnp_launcher_pid=$!
wait_port "$(port_from_addr "$scnp_addr")"
if [[ "$server_direct_shard_ports" != "0" ]]; then
  wait_port "$(port_from_addr "$scnp_client_addr")"
fi
scnp_pid="$(resolve_shardcache_pid "$scnp_addr")"
run_depths "$scnp_backend" "$scnp_client_addr" "$scnp_pid"
stop_pid "$scnp_pid"
stop_pid "$scnp_launcher_pid"
unset scnp_pid
unset scnp_launcher_pid

if command -v taskset >/dev/null 2>&1; then
  env "${resp_server_env[@]}" \
    taskset -c "$server_cpuset" ./target/release/shardcache \
    --server-mode direct --disable-persistence \
    --bind-addr "$resp_addr" --shard-count "$shard_count" \
    >/tmp/fc-resp-client-sweep.log 2>&1 &
else
  env "${resp_server_env[@]}" \
    ./target/release/shardcache \
    --server-mode direct --disable-persistence \
    --bind-addr "$resp_addr" --shard-count "$shard_count" \
    >/tmp/fc-resp-client-sweep.log 2>&1 &
fi
resp_launcher_pid=$!
wait_port "$(port_from_addr "$resp_addr")"
resp_pid="$(resolve_shardcache_pid "$resp_addr")"
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
