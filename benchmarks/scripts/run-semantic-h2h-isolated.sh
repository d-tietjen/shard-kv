#!/usr/bin/env bash
# Run the semantic-cache head-to-head with explicit CPU isolation.
#
# Default topology for Adam:
#   - SUT_CPUSET=0-15: Redis/Qdrant server containers, or embedded-only process.
#   - LOAD_CPUSET=16-31: Python load process for networked/server-backed rows.
#   - WORKERS=16: one load worker per logical CPU in the active budget.
#
# ShardCache, FAISS, hnswlib, and GPTCache are embedded/in-process in the
# current harness. For those rows there is no separate server PID to pin, so the
# benchmark process itself is pinned to SUT_CPUSET and CPU is reported as
# process vCPU.

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
bench_root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$bench_root/.." && pwd)"

cd "$ws_root"

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${OUT_DIR:-$bench_root/results/adam-semantic-h2h-isolated-$stamp}"

pairs_csv="${PAIRS_CSV:-}"
dataset="${DATASET:-semantic-fixture}"
entries="${ENTRIES:-100000}"
dims="${DIMS:-384}"
threshold="${THRESHOLD:-0.35}"
seconds="${LOAD_SECONDS:-10}"
workers="${WORKERS:-16}"
sut_cpuset="${SUT_CPUSET:-0-15}"
load_cpuset="${LOAD_CPUSET:-16-31}"
cache_shards="${CACHE_SHARDS:-64}"
redis_port="${REDIS_PORT:-6384}"
qdrant_port="${QDRANT_PORT:-6333}"
shardcache_port="${SHARDCACHE_PORT:-6390}"
redis_url="${REDIS_URL:-redis://127.0.0.1:$redis_port}"
qdrant_url="${QDRANT_URL:-http://127.0.0.1:$qdrant_port}"
shardcache_url="${SHARDCACHE_URL:-redis://127.0.0.1:$shardcache_port}"
redis_container="${REDIS_CONTAINER:-bench-semantic-redis}"
qdrant_container="${QDRANT_CONTAINER:-bench-semantic-qdrant}"
redis_image="${REDIS_IMAGE:-redis/redis-stack-server:latest}"
qdrant_image="${QDRANT_IMAGE:-qdrant/qdrant:latest}"
keep_services="${KEEP_SERVICES:-0}"
python_bin="${PYTHON:-python3}"
semantic_server_shards="${SEMANTIC_SERVER_SHARDS:-1}"
shardcache_pid=""

if [[ -z "$pairs_csv" ]]; then
  echo "PAIRS_CSV is required; pass the shared semantic fixture CSV" >&2
  exit 2
fi

if [[ ! -f "$pairs_csv" ]]; then
  echo "PAIRS_CSV does not exist: $pairs_csv" >&2
  exit 2
fi

if ! command -v taskset >/dev/null 2>&1; then
  echo "taskset is required for isolated CPU runs" >&2
  exit 2
fi

mkdir -p "$out_dir"

cargo build --release -p shardcache-benchmarks --bin semantic_cache_matrix
cargo build --release -p shardcache --bin shardcache

cleanup() {
  stop_shardcache_server
  if [[ "$keep_services" != "1" ]]; then
    docker rm -f "$redis_container" >/dev/null 2>&1 || true
    docker rm -f "$qdrant_container" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

stop_shardcache_server() {
  if [[ -n "$shardcache_pid" ]]; then
    kill "$shardcache_pid" >/dev/null 2>&1 || true
    wait "$shardcache_pid" >/dev/null 2>&1 || true
    shardcache_pid=""
  fi
}

start_redis() {
  docker rm -f "$redis_container" >/dev/null 2>&1 || true
  docker run -d --rm \
    --name "$redis_container" \
    --cpuset-cpus "$sut_cpuset" \
    -p "127.0.0.1:$redis_port:6379" \
    "$redis_image" >/dev/null
  for _ in $(seq 1 60); do
    if docker exec "$redis_container" redis-cli ping >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "Redis Stack did not become ready" >&2
  return 1
}

start_qdrant() {
  docker rm -f "$qdrant_container" >/dev/null 2>&1 || true
  docker run -d --rm \
    --name "$qdrant_container" \
    --cpuset-cpus "$sut_cpuset" \
    -p "127.0.0.1:$qdrant_port:6333" \
    "$qdrant_image" >/dev/null
  for _ in $(seq 1 60); do
    if curl -fsS "$qdrant_url/readyz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "Qdrant did not become ready" >&2
  return 1
}

start_shardcache_server() {
  stop_shardcache_server
  local log="$out_dir/shardcache-server.log"
  SHARDCACHE_DIRECT_SHARD_PORTS=0 taskset -c "$sut_cpuset" \
    "$ws_root/target/release/shardcache" \
    --bind-addr "127.0.0.1:$shardcache_port" \
    --disable-persistence \
    --server-mode direct \
    --shard-count "$semantic_server_shards" \
    >"$log" 2>&1 &
  shardcache_pid="$!"
  for _ in $(seq 1 60); do
    if "$python_bin" -c 'import socket, sys; s=socket.create_connection(("127.0.0.1", int(sys.argv[1])), 1); s.close()' "$shardcache_port" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$shardcache_pid" >/dev/null 2>&1; then
      echo "ShardCache server exited before becoming ready; see $log" >&2
      return 1
    fi
    sleep 1
  done
  echo "ShardCache server did not become ready; see $log" >&2
  return 1
}

container_pid() {
  docker inspect --format '{{.State.Pid}}' "$1"
}

write_metadata() {
  {
    echo "created_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "host=$(hostname)"
    echo "uname=$(uname -a)"
    echo "git_sha=$(git rev-parse HEAD 2>/dev/null || echo unknown)"
    echo "git_branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
    echo "pairs_csv=$pairs_csv"
    echo "dataset=$dataset"
    echo "entries=$entries"
    echo "dims=$dims"
    echo "threshold=$threshold"
    echo "seconds=$seconds"
    echo "workers=$workers"
    echo "sut_cpuset=$sut_cpuset"
    echo "load_cpuset=$load_cpuset"
    echo "redis_image=$redis_image"
    echo "qdrant_image=$qdrant_image"
    echo "shardcache_url=$shardcache_url"
    echo "semantic_server_shards=$semantic_server_shards"
    echo "python=$python_bin"
    echo "redis_image_id=$(docker image inspect "$redis_image" --format '{{.Id}}' 2>/dev/null || echo unavailable)"
    echo "qdrant_image_id=$(docker image inspect "$qdrant_image" --format '{{.Id}}' 2>/dev/null || echo unavailable)"
    echo "notes=Networked rows pin Redis/Qdrant/ShardCache servers to SUT_CPUSET and Python load processes to LOAD_CPUSET. Embedded rows pin the benchmark process to SUT_CPUSET because there is no separate server process."
  } >"$out_dir/metadata.txt"
}

run_shardcache() {
  local scenario="$1"
  local out="$out_dir/shardcache-$scenario.csv"
  shift
  taskset -c "$sut_cpuset" "$ws_root/target/release/semantic_cache_matrix" \
    --pairs-csv "$pairs_csv" \
    --dataset "$dataset" \
    --mode load \
    --cache-shards "$cache_shards" \
    --index-entries "$entries" \
    --latency-threshold "$threshold" \
    --load-workers "$workers" \
    --load-seconds "$seconds" \
    --load-csv "$out" \
    "$@"
}

run_peer() {
  local adapter="$1"
  local scenario="$2"
  local cpuset="$3"
  local external_pids="$4"
  local out="$out_dir/peers-$adapter-$scenario.csv"
  shift 4
  taskset -c "$cpuset" "$python_bin" "$here/semantic-peer-load.py" \
    --redis-url "$redis_url" \
    --qdrant-url "$qdrant_url" \
    --shardcache-url "$shardcache_url" \
    --adapters "$adapter" \
    --scenario "$scenario" \
    --entries "$entries" \
    --dims "$dims" \
    --pairs-csv "$pairs_csv" \
    --workers "$workers" \
    --seconds "$seconds" \
    --threshold "$threshold" \
    --process-cpuset "$cpuset" \
    --external-pids "$external_pids" \
    --output "$out" \
    "$@"
}

run_networked_redis_adapter() {
  local adapter="$1"
  local scenario="$2"
  shift 2
  start_redis
  local redis_pid
  redis_pid="$(container_pid "$redis_container")"
  run_peer "$adapter" "$scenario" "$load_cpuset" "$redis_pid" "$@"
  docker rm -f "$redis_container" >/dev/null
}

run_qdrant_adapter() {
  local scenario="$1"
  shift
  start_qdrant
  local qdrant_pid
  qdrant_pid="$(container_pid "$qdrant_container")"
  run_peer qdrant "$scenario" "$load_cpuset" "$qdrant_pid" "$@"
  docker rm -f "$qdrant_container" >/dev/null
}

run_shardcache_server_adapter() {
  local scenario="$1"
  shift
  start_shardcache_server
  run_peer shardcache-server "$scenario" "$load_cpuset" "$shardcache_pid" "$@"
  stop_shardcache_server
}

run_embedded_adapter() {
  local adapter="$1"
  local scenario="$2"
  shift 2
  run_peer "$adapter" "$scenario" "$sut_cpuset" "" "$@"
}

write_metadata

echo "Running ShardCache isolated rows into $out_dir"
run_shardcache miss-cold \
  --load-query-pool "$entries" \
  --load-warmup-queries 0 \
  --load-unique-queries \
  --load-miss-random \
  --disable-semantic-query-cache
run_shardcache hit-cold-unique \
  --load-query-pool "$entries" \
  --load-warmup-queries 0 \
  --load-unique-queries \
  --disable-semantic-query-cache
run_shardcache hit-hot-cached \
  --load-query-pool 1024 \
  --load-warmup-queries 1024 \
  --load-exact-hits

echo "Running ShardCache server semantic rows"
run_shardcache_server_adapter miss-cold \
  --query-source miss-random \
  --query-pool "$entries" \
  --warmup-queries 0 \
  --unique-queries
run_shardcache_server_adapter hit-cold-unique \
  --query-source fixture \
  --query-pool "$entries" \
  --warmup-queries 0 \
  --unique-queries
run_shardcache_server_adapter hit-hot-cached \
  --query-source exact \
  --query-pool 1024 \
  --warmup-queries 1024

echo "Running Redis-backed semantic-cache rows"
for adapter in betterdb redisvl langchain-redis redis-flat redis-hnsw; do
  run_networked_redis_adapter "$adapter" miss-cold \
    --query-source miss-random \
    --query-pool "$entries" \
    --warmup-queries 0 \
    --unique-queries
  run_networked_redis_adapter "$adapter" hit-cold-unique \
    --query-source fixture \
    --query-pool "$entries" \
    --warmup-queries 0 \
    --unique-queries
  run_networked_redis_adapter "$adapter" hit-hot-cached \
    --query-source exact \
    --query-pool 1024 \
    --warmup-queries 1024
done

echo "Running embedded/vector rows"
for adapter in faiss-flat faiss-hnsw hnswlib; do
  OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}" run_embedded_adapter "$adapter" miss-cold \
    --query-source miss-random \
    --query-pool "$entries" \
    --warmup-queries 0 \
    --unique-queries
  OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}" run_embedded_adapter "$adapter" hit-cold-unique \
    --query-source fixture \
    --query-pool "$entries" \
    --warmup-queries 0 \
    --unique-queries
  OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}" run_embedded_adapter "$adapter" hit-hot-cached \
    --query-source exact \
    --query-pool 1024 \
    --warmup-queries 1024
done

echo "Running Qdrant rows"
run_qdrant_adapter miss-cold \
  --query-source miss-random \
  --query-pool "$entries" \
  --warmup-queries 0 \
  --unique-queries
run_qdrant_adapter hit-cold-unique \
  --query-source fixture \
  --query-pool "$entries" \
  --warmup-queries 0 \
  --unique-queries
run_qdrant_adapter hit-hot-cached \
  --query-source exact \
  --query-pool 1024 \
  --warmup-queries 1024

cat <<EOF
isolated semantic head-to-head complete:
  $out_dir/metadata.txt
  $out_dir
EOF
