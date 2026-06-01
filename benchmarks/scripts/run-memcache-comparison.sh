#!/usr/bin/env bash
# Isolated Docker comparison for shardcache server modes versus Memcached.

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
bench_root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$bench_root/.." && pwd)"
compose_file="$bench_root/docker/compose.yml"

usage() {
  cat <<'USAGE'
Usage:
  benchmarks/scripts/run-memcache-comparison.sh [options]

Options:
  --targets LIST          memcached,shardcache-resp,shardcache-scnp,shardcache-scnp-direct,all
  --vcpus LIST            Comma-separated vCPU matrix (default: 1,2,4,8,16)
  --clients LIST          Comma-separated client counts (default: 1,16)
  --pipeline-depth LIST   Comma-separated pipeline depths (default: 1,16)
  --value-size LIST       Comma-separated value sizes in bytes (default: 64,512,4096)
  --mix LIST              Comma-separated workload mixes (default: 100-0,80-20,0-100)
  --key-count N           Key cardinality (default: 100000)
  --warmup SECONDS        Warmup duration (default: 2)
  --duration SECONDS      Measurement duration (default: 10)
  --out-dir PATH          Output directory or parent results directory

Environment:
  SERVER_MEMORY_LIMIT     Optional Docker memory limit, e.g. 4g.
  CPUSET_CPUS             Optional explicit cpuset. Defaults to first N Linux CPUs.
  MEMCACHED_MEMORY_MB     Memcached item memory in MiB (default: 1024).
  MEMCACHED_PORT          Host port for Memcached (default: 11211).
  SHARDCACHE_FEATURES     Docker build features for shardcache (default: redis-server).
  SHARDCACHE_PORT         Host port for shardcache shared RESP/SCNP listener (default: 6383).

Examples:
  ./benchmarks/scripts/run-memcache-comparison.sh --vcpus 1 --clients 1 --pipeline-depth 1
  ./benchmarks/scripts/run-memcache-comparison.sh --targets memcached,shardcache-scnp-direct --vcpus 1,2,4,8,16
USAGE
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

expand_targets() {
  local raw="$1"
  local expanded=()
  local parts=()
  IFS=',' read -r -a parts <<< "$raw"
  for target in "${parts[@]}"; do
    target="$(trim "$target")"
    case "$target" in
      all)
        expanded+=(memcached shardcache-resp shardcache-scnp shardcache-scnp-direct)
        ;;
      shardcache)
        expanded+=(shardcache-resp shardcache-scnp shardcache-scnp-direct)
        ;;
      "")
        ;;
      *)
        expanded+=("$target")
        ;;
    esac
  done
  local IFS=,
  printf '%s' "${expanded[*]}"
}

split_csv() {
  local raw="$1"
  local -n out_ref="$2"
  local parts=()
  out_ref=()
  IFS=',' read -r -a parts <<< "$raw"
  for part in "${parts[@]}"; do
    part="$(trim "$part")"
    if [[ -n "$part" ]]; then
      out_ref+=("$part")
    fi
  done
}

target_service() {
  case "$1" in
    memcached) printf 'memcached' ;;
    shardcache-resp|shardcache-scnp|shardcache-scnp-direct) printf 'shardcache' ;;
    *)
      echo "unknown memcache comparison target: $1" >&2
      exit 2
      ;;
  esac
}

target_port() {
  case "$1" in
    memcached) printf '%s' "${MEMCACHED_PORT:-11211}" ;;
    shardcache-resp|shardcache-scnp) printf '%s' "${SHARDCACHE_PORT:-6383}" ;;
    shardcache-scnp-direct) printf '6501' ;;
  esac
}

target_addr() {
  printf '127.0.0.1:%s' "$(target_port "$1")"
}

target_backend() {
  case "$1" in
    memcached) printf 'memcached' ;;
    shardcache-resp) printf 'fc-server-resp' ;;
    shardcache-scnp) printf 'fc-server-scnp' ;;
    shardcache-scnp-direct) printf 'fc-server-scnp-direct' ;;
  esac
}

docker_cpuset_for_vcpus() {
  local vcpus="$1"
  if [[ -n "${CPUSET_CPUS:-}" ]]; then
    printf '%s' "$CPUSET_CPUS"
  elif [[ "$(uname -s)" == "Linux" ]]; then
    if [[ "$vcpus" == "1" ]]; then
      printf '0'
    else
      printf '0-%s' "$((vcpus - 1))"
    fi
  fi
}

effective_shard_count() {
  local vcpus="$1"
  if [[ -n "${shard_count_override:-}" ]]; then
    printf '%s' "$shard_count_override"
  else
    printf '%s' "$vcpus"
  fi
}

wait_for_port() {
  local port="$1"
  local label="$2"
  for _ in {1..100}; do
    if (echo >"/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
      return
    fi
    sleep 0.1
  done
  echo "$label did not open port $port" >&2
  exit 1
}

stop_targets() {
  docker compose -f "$compose_file" --profile servers down --remove-orphans >/dev/null 2>&1 || true
  docker rm -f bench-memcached bench-shardcache >/dev/null 2>&1 || true
}

start_target() {
  local target="$1"
  local vcpus="$2"
  local service cpuset override_file shard_count
  service="$(target_service "$target")"
  cpuset="$(docker_cpuset_for_vcpus "$vcpus")"
  override_file="$out_dir/tmp/compose-$target-v${vcpus}.yml"

  export MEMCACHED_THREADS="$vcpus"
  export SHARDCACHE_FEATURES="${SHARDCACHE_FEATURES:-redis-server}"
  shard_count="$(effective_shard_count "$vcpus")"
  if [[ "$target" == "shardcache-scnp-direct" && "$shard_count" -gt 16 ]]; then
    echo "shardcache-scnp-direct supports up to 16 mapped direct shard ports; got shard_count=$shard_count" >&2
    exit 2
  fi
  export SHARD_COUNT="$shard_count"
  export SHARDCACHE_DIRECT_SHARD_BASE_PORT=6501

  {
    echo "services:"
    echo "  $service:"
    echo "    cpus: \"$vcpus\""
    if [[ -n "$cpuset" ]]; then
      echo "    cpuset: \"$cpuset\""
    fi
    if [[ -n "${SERVER_MEMORY_LIMIT:-}" ]]; then
      echo "    mem_limit: \"$SERVER_MEMORY_LIMIT\""
    fi
  } > "$override_file"

  docker compose -f "$compose_file" -f "$override_file" up -d --build "$service"
  wait_for_port "$(target_port "$target")" "$target"
}

targets_raw="${TARGETS:-memcached,shardcache-resp,shardcache-scnp,shardcache-scnp-direct}"
vcpus_raw="${VCPUS:-1,2,4,8,16}"
clients_raw="${CLIENTS:-1,16}"
pipeline_depths_raw="${PIPELINE_DEPTHS:-${PIPELINE_DEPTH:-1,16}}"
value_sizes_raw="${VALUE_SIZES:-${VALUE_SIZE:-64,512,4096}}"
mixes_raw="${MIXES:-${MIX:-100-0,80-20,0-100}}"
key_count="${KEY_COUNT:-100000}"
warmup="${WARMUP:-2}"
duration="${DURATION:-10}"
latency_sample_rate="${LATENCY_SAMPLE_RATE:-1}"
out_base="${OUT_DIR:-$bench_root/results}"
shard_count_override="${SHARD_COUNT:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --targets) targets_raw="$2"; shift 2 ;;
    --targets=*) targets_raw="${1#*=}"; shift ;;
    --vcpus) vcpus_raw="$2"; shift 2 ;;
    --vcpus=*) vcpus_raw="${1#*=}"; shift ;;
    --clients) clients_raw="$2"; shift 2 ;;
    --clients=*) clients_raw="${1#*=}"; shift ;;
    --pipeline-depth|--pipeline-depths) pipeline_depths_raw="$2"; shift 2 ;;
    --pipeline-depth=*|--pipeline-depths=*) pipeline_depths_raw="${1#*=}"; shift ;;
    --value-size|--value-sizes) value_sizes_raw="$2"; shift 2 ;;
    --value-size=*|--value-sizes=*) value_sizes_raw="${1#*=}"; shift ;;
    --mix|--mixes) mixes_raw="$2"; shift 2 ;;
    --mix=*|--mixes=*) mixes_raw="${1#*=}"; shift ;;
    --key-count) key_count="$2"; shift 2 ;;
    --key-count=*) key_count="${1#*=}"; shift ;;
    --warmup) warmup="$2"; shift 2 ;;
    --warmup=*) warmup="${1#*=}"; shift ;;
    --duration) duration="$2"; shift 2 ;;
    --duration=*) duration="${1#*=}"; shift ;;
    --out-dir) out_base="$2"; shift 2 ;;
    --out-dir=*) out_base="${1#*=}"; shift ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

targets_raw="$(expand_targets "$targets_raw")"
split_csv "$targets_raw" targets
split_csv "$vcpus_raw" vcpus
split_csv "$clients_raw" clients_list
split_csv "$pipeline_depths_raw" pipeline_depths
split_csv "$value_sizes_raw" value_sizes
split_csv "$mixes_raw" mixes

if [[ "$out_base" != /* ]]; then
  out_base="$ws_root/$out_base"
fi
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
run_id="${RUN_ID:-memcache-comparison-$stamp}"
if [[ "$out_base" == */memcache-comparison-* ]]; then
  out_dir="$out_base"
else
  out_dir="$out_base/$run_id"
fi

mkdir -p "$out_dir/tmp"
trap stop_targets EXIT

if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon is not reachable. Start Docker, then rerun the benchmark." >&2
  exit 1
fi

cd "$ws_root"
cargo build --release -p shardcache-benchmarks --bin saturation

git_sha="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
git_branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
csv="$out_dir/cache-comparison.csv"

cat > "$out_dir/metadata.json" <<EOF
{
  "schema_version": 1,
  "created_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "run_id": "$run_id",
  "git_sha": "$git_sha",
  "git_branch": "$git_branch",
  "compose_file": "$compose_file",
  "targets": "$targets_raw",
  "vcpus": "$vcpus_raw",
  "clients": "$clients_raw",
  "pipeline_depths": "$pipeline_depths_raw",
  "value_sizes": "$value_sizes_raw",
  "mixes": "$mixes_raw",
  "key_count": $key_count,
  "warmup": $warmup,
  "duration": $duration,
  "latency_sample_rate": $latency_sample_rate,
  "cpu_pinning": "${CPUSET_CPUS:-first-n-linux-cpus}",
  "shard_count_override": "${shard_count_override:-}",
  "server_memory_limit": "${SERVER_MEMORY_LIMIT:-}",
  "memcached_memory_mb": "${MEMCACHED_MEMORY_MB:-1024}"
}
EOF

for vcpu in "${vcpus[@]}"; do
  for target in "${targets[@]}"; do
    stop_targets
    start_target "$target" "$vcpu"
    service="$(target_service "$target")"
    backend="$(target_backend "$target")"
    addr="$(target_addr "$target")"
    server_pid="$(docker inspect --format '{{.State.Pid}}' "bench-$service")"
    scnp_shards_arg=()
    if [[ "$target" == "shardcache-scnp-direct" ]]; then
      scnp_shards_arg=(--scnp-shards "$(effective_shard_count "$vcpu")")
    fi

    for clients in "${clients_list[@]}"; do
      for pipeline_depth in "${pipeline_depths[@]}"; do
        for value_size in "${value_sizes[@]}"; do
          for mix in "${mixes[@]}"; do
            echo "benchmark target=$target backend=$backend vcpus=$vcpu clients=$clients pipeline_depth=$pipeline_depth value_size=$value_size mix=$mix"
            "$ws_root/target/release/saturation" \
              --backends "$backend" \
              --addr "$addr" \
              --value-size "$value_size" \
              --mix "$mix" \
              --key-pattern point \
              --vcpu-budget "$vcpu" \
              --clients "$clients" \
              --pipeline-depth "$pipeline_depth" \
              --key-count "$key_count" \
              --warmup "$warmup" \
              --duration "$duration" \
              --latency-sample-rate "$latency_sample_rate" \
              --csv "$csv" \
              --server-pid "$server_pid" \
              "${scnp_shards_arg[@]}"
          done
        done
      done
    done
  done
done

stop_targets

cat > "$out_dir/report.md" <<EOF
# Memcached Comparison $run_id

This bundle compares Memcached against shardcache server targets using the
\`saturation\` cache workload driver. Each target is started in Docker in
isolation and receives the same vCPU limit, client counts, pipeline depths,
value sizes, key count, warmup, and measurement duration.

- Targets: \`$targets_raw\`
- vCPUs: \`$vcpus_raw\`
- Clients: \`$clients_raw\`
- Pipeline depths: \`$pipeline_depths_raw\`
- Value sizes: \`$value_sizes_raw\`
- Mixes: \`$mixes_raw\`
- Key count: \`$key_count\`
- CSV: \`cache-comparison.csv\`

Use the CSV for detailed comparisons. Rows with the same value size, mix,
vCPU count, client count, and pipeline depth are directly comparable.
EOF

cat <<EOF
memcache comparison written:
  $out_dir/metadata.json
  $out_dir/report.md
  $csv
EOF
