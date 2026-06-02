#!/usr/bin/env bash
# Run standardized Docker server benchmarks for Redis-compatible targets.

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
bench_root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$bench_root/.." && pwd)"
compose_file="$bench_root/docker/compose.yml"
config_file="$bench_root/bench.toml"

usage() {
  cat <<'USAGE'
Usage:
  benchmarks/scripts/run-benchmark-suite.sh [options]

Options:
  --config PATH              Benchmark config TOML (default: benchmarks/bench.toml)
  --suite LIST               Suites: redis-core,redis-v6-v7,redis-v8,redis-v8-vector,redis-keyspace,redis-destructive,redis-modules,all
  --targets LIST             Targets: redis,redis-cluster,redis-stack,valkey,dragonfly,shardcache-resp,shardcache-scnp,shardcache-scnp-direct,shardcache,all
  --vcpus LIST               Comma-separated vCPU matrix, for example 1,2,4,8,16
  --clients N                Concurrent clients
  --pipeline-depth LIST      Comma-separated pipeline depths
  --warmup SECONDS           Warmup duration
  --duration SECONDS         Measurement duration
  --key-shards N|vcpus       Logical key lanes, or vcpus to match each vCPU step
  --fixture-scope MODE       per-client or shared-keyspace
  --memory-budget-mib N      Total precomposed command memory budget
  --command-budget N         Total precomposed command count budget; 0 means memory-bounded
  --out-dir PATH             Output directory or parent results directory

Examples:
  benchmarks/scripts/run-benchmark-suite.sh --targets redis,redis-cluster,valkey,dragonfly,shardcache-resp,shardcache-scnp --suite redis-core --vcpus 1,2,4,8,16 --key-shards vcpus
  benchmarks/scripts/run-benchmark-suite.sh --targets redis-stack,shardcache-resp,shardcache-scnp --suite redis-modules --vcpus 1,2,4,8,16
  benchmarks/scripts/run-benchmark-suite.sh --targets all --suite all --vcpus 1,2,4,8,16 --memory-budget-mib 512
USAGE
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

toml_value() {
  local key="$1"
  local default="$2"
  local line value
  line="$(grep -E "^[[:space:]]*$key[[:space:]]*=" "$config_file" | tail -n 1 || true)"
  if [[ -z "$line" ]]; then
    printf '%s' "$default"
    return
  fi
  value="${line#*=}"
  value="${value%%#*}"
  value="$(trim "$value")"
  value="${value%\"}"
  value="${value#\"}"
  value="${value%\'}"
  value="${value#\'}"
  printf '%s' "$value"
}

setting() {
  local override="$1"
  local key="$2"
  local default="$3"
  if [[ -n "$override" ]]; then
    printf '%s' "$override"
  else
    toml_value "$key" "$default"
  fi
}

join_csv() {
  local IFS=,
  printf '%s' "$*"
}

expand_suites_raw() {
  local raw="$1"
  local suites_ref=()
  local expanded=()
  IFS=',' read -r -a suites_ref <<< "$raw"
  for suite in "${suites_ref[@]}"; do
    suite="$(trim "$suite")"
    case "$suite" in
      all)
        expanded+=(redis-core redis-v6-v7 redis-v8 redis-keyspace redis-destructive redis-modules)
        ;;
      "")
        ;;
      *)
        expanded+=("$suite")
        ;;
    esac
  done
  join_csv "${expanded[@]}"
}

expand_targets_raw() {
  local raw="$1"
  local targets_ref=()
  local expanded=()
  IFS=',' read -r -a targets_ref <<< "$raw"
  for target in "${targets_ref[@]}"; do
    target="$(trim "$target")"
    case "$target" in
      all)
        expanded+=(redis redis-cluster redis-stack valkey dragonfly shardcache-resp shardcache-scnp)
        ;;
      shardcache)
        expanded+=(shardcache-resp shardcache-scnp)
        ;;
      "")
        ;;
      *)
        expanded+=("$target")
        ;;
    esac
  done
  join_csv "${expanded[@]}"
}

suite_value() {
  local suite="$1"
  local key="$2"
  local default="$3"
  local suite_file="$bench_root/suites/$suite.toml"
  if [[ ! -f "$suite_file" ]]; then
    echo "unknown benchmark suite: $suite" >&2
    exit 2
  fi
  local line value
  line="$(grep -E "^[[:space:]]*$key[[:space:]]*=" "$suite_file" | tail -n 1 || true)"
  if [[ -z "$line" ]]; then
    printf '%s' "$default"
    return
  fi
  value="${line#*=}"
  value="${value%%#*}"
  value="$(trim "$value")"
  value="${value%\"}"
  value="${value#\"}"
  value="${value%\'}"
  value="${value#\'}"
  printf '%s' "$value"
}

target_service() {
  case "$1" in
    redis|redis-cluster|redis-stack|valkey|dragonfly) printf '%s' "$1" ;;
    shardcache|shardcache-resp|shardcache-scnp|shardcache-scnp-direct) printf 'shardcache' ;;
    *)
      echo "unknown benchmark target: $1" >&2
      exit 2
      ;;
  esac
}

target_port() {
  case "$1" in
    redis|redis-stack) printf '6379' ;;
    redis-cluster) printf '7000' ;;
    valkey) printf '6381' ;;
    dragonfly) printf '6382' ;;
    shardcache|shardcache-resp|shardcache-scnp) printf '6383' ;;
    shardcache-scnp-direct) printf '6501' ;;
  esac
}

effective_key_shards() {
  local vcpus="$1"
  case "$key_shards" in
    vcpu|vcpus|auto) printf '%s' "$vcpus" ;;
    *) printf '%s' "$key_shards" ;;
  esac
}

effective_shard_count() {
  local vcpus="$1"
  local lanes
  lanes="$(effective_key_shards "$vcpus")"
  if [[ -z "$lanes" || "$lanes" == *[!0-9]* ]]; then
    echo "--key-shards must be a positive integer or one of: vcpus, vcpu, auto" >&2
    exit 2
  fi
  if [[ -n "${user_shard_count:-}" ]]; then
    printf '%s' "$user_shard_count"
  elif [[ "$lanes" -gt "$vcpus" ]]; then
    printf '%s' "$lanes"
  else
    printf '%s' "$vcpus"
  fi
}

redis_cluster_nodes() {
  local vcpus="$1"
  local nodes
  nodes="$(effective_shard_count "$vcpus")"
  if [[ "$nodes" -lt 3 ]]; then
    nodes=3
  fi
  if [[ "$nodes" -gt 16 ]]; then
    echo "redis-cluster supports up to 16 mapped node ports in benchmarks/docker/compose.yml; got nodes=$nodes" >&2
    exit 2
  fi
  printf '%s' "$nodes"
}

target_spec() {
  local target="$1"
  local vcpus="$2"
  case "$target" in
    shardcache-scnp)
      printf 'shardcache-scnp=scnp:127.0.0.1:6383'
      ;;
    shardcache-scnp-direct)
      printf 'shardcache-scnp-direct=scnp:127.0.0.1:6501+%s' "$(effective_shard_count "$vcpus")"
      ;;
    redis-cluster)
      printf 'redis-cluster=resp-cluster:127.0.0.1:7000+%s' "$(redis_cluster_nodes "$vcpus")"
      ;;
    shardcache|shardcache-resp)
      printf 'shardcache-resp=127.0.0.1:6383'
      ;;
    *)
      printf '%s=127.0.0.1:%s' "$target" "$(target_port "$target")"
      ;;
  esac
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

configure_started_target() {
  local target="$1"
  local vcpus="${2:-1}"
  case "$target" in
    redis-cluster)
      local nodes
      nodes="$(redis_cluster_nodes "$vcpus")"
      for _ in {1..100}; do
        local ok=1
        for offset in $(seq 0 "$((nodes - 1))"); do
          local port info
          port="$((7000 + offset))"
          info="$(docker exec bench-redis-cluster redis-cli -p "$port" CLUSTER INFO 2>/dev/null || true)"
          if ! grep -q '^cluster_state:ok' <<<"$info" \
            || ! grep -q '^cluster_slots_assigned:16384' <<<"$info" \
            || ! grep -q "^cluster_known_nodes:$nodes" <<<"$info"; then
            ok=0
            break
          fi
        done
        if [[ "$ok" == "1" ]]; then
          return
        fi
        sleep 0.1
      done
      echo "redis-cluster did not reach a fully assigned healthy state on all nodes" >&2
      exit 1
      ;;
    redis-stack)
      for _ in {1..100}; do
        if docker exec bench-redis-stack redis-cli CONFIG SET save "" >/dev/null 2>&1; then
          docker exec bench-redis-stack redis-cli CONFIG SET appendonly no >/dev/null
          docker exec bench-redis-stack redis-cli MODULE LIST >/dev/null
          return
        fi
        sleep 0.1
      done
      echo "redis-stack did not accept benchmark configuration" >&2
      exit 1
      ;;
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

start_target() {
  local target="$1"
  local vcpus="$2"
  local service cpuset override_file shard_count cluster_nodes cluster_slot_lanes
  service="$(target_service "$target")"
  export DRAGONFLY_THREADS="$vcpus"
  shard_count="$(effective_shard_count "$vcpus")"
  if [[ "$target" == "shardcache-scnp-direct" && "$shard_count" -gt 16 ]]; then
    echo "shardcache-scnp-direct supports up to 16 mapped direct shard ports in benchmarks/docker/compose.yml; got shard_count=$shard_count" >&2
    exit 2
  fi
  cluster_nodes="$(redis_cluster_nodes "$vcpus")"
  cluster_slot_lanes="$(effective_key_shards "$vcpus")"
  export REDIS_CLUSTER_NODES="$cluster_nodes"
  export REDIS_CLUSTER_SLOT_LANES="$cluster_slot_lanes"
  export SHARD_COUNT="$shard_count"
  export SHARDCACHE_DIRECT_SHARD_BASE_PORT=6501

  cpuset="$(docker_cpuset_for_vcpus "$vcpus")"
  override_file="$out_dir/tmp/compose-$target-v${vcpus}.yml"
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
  configure_started_target "$target" "$vcpus"
}

stop_targets() {
  docker compose -f "$compose_file" --profile servers down --remove-orphans >/dev/null 2>&1 || true
  docker rm -f bench-redis bench-redis-cluster bench-redis-stack bench-valkey bench-dragonfly bench-shardcache >/dev/null 2>&1 || true
}

append_csv() {
  local src="$1"
  local dst="$2"
  if [[ ! -f "$dst" ]]; then
    cp "$src" "$dst"
  else
    tail -n +2 "$src" >> "$dst"
  fi
}

config_override=""
suites_override=""
targets_override=""
vcpus_override=""
clients_override=""
pipeline_depths_override=""
warmup_override=""
duration_override=""
key_shards_override=""
fixture_scope_override=""
memory_budget_override=""
command_budget_override=""
out_dir_override=""
user_shard_count="${SHARD_COUNT:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config) config_override="$2"; shift 2 ;;
    --config=*) config_override="${1#*=}"; shift ;;
    --suite|--suites) suites_override="$2"; shift 2 ;;
    --suite=*|--suites=*) suites_override="${1#*=}"; shift ;;
    --targets) targets_override="$2"; shift 2 ;;
    --targets=*) targets_override="${1#*=}"; shift ;;
    --vcpus) vcpus_override="$2"; shift 2 ;;
    --vcpus=*) vcpus_override="${1#*=}"; shift ;;
    --clients) clients_override="$2"; shift 2 ;;
    --clients=*) clients_override="${1#*=}"; shift ;;
    --pipeline-depth|--pipeline-depths) pipeline_depths_override="$2"; shift 2 ;;
    --pipeline-depth=*|--pipeline-depths=*) pipeline_depths_override="${1#*=}"; shift ;;
    --warmup) warmup_override="$2"; shift 2 ;;
    --warmup=*) warmup_override="${1#*=}"; shift ;;
    --duration) duration_override="$2"; shift 2 ;;
    --duration=*) duration_override="${1#*=}"; shift ;;
    --key-shards) key_shards_override="$2"; shift 2 ;;
    --key-shards=*) key_shards_override="${1#*=}"; shift ;;
    --fixture-scope) fixture_scope_override="$2"; shift 2 ;;
    --fixture-scope=*) fixture_scope_override="${1#*=}"; shift ;;
    --memory-budget-mib) memory_budget_override="$2"; shift 2 ;;
    --memory-budget-mib=*) memory_budget_override="${1#*=}"; shift ;;
    --command-budget) command_budget_override="$2"; shift 2 ;;
    --command-budget=*) command_budget_override="${1#*=}"; shift ;;
    --out-dir) out_dir_override="$2"; shift 2 ;;
    --out-dir=*) out_dir_override="${1#*=}"; shift ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -n "$config_override" ]]; then
  config_file="$config_override"
fi
if [[ ! -f "$config_file" ]]; then
  echo "benchmark config not found: $config_file" >&2
  exit 2
fi

suites_raw="$(setting "$suites_override" suites redis-core)"
targets_raw="$(setting "$targets_override" targets redis,valkey,dragonfly,shardcache-resp,shardcache-scnp)"
vcpus_raw="$(setting "$vcpus_override" vcpus 1,2,4,8,16)"
clients="$(setting "$clients_override" clients 1)"
pipeline_depths_raw="$(setting "$pipeline_depths_override" pipeline_depths 1)"
warmup="$(setting "$warmup_override" warmup 1)"
duration="$(setting "$duration_override" duration 5)"
key_shards="$(setting "$key_shards_override" key_shards 1)"
fixture_scope="$(setting "$fixture_scope_override" fixture_scope per-client)"
memory_budget_mib="$(setting "$memory_budget_override" memory_budget_mib 256)"
command_budget="$(setting "$command_budget_override" command_budget 0)"
out_base="$(setting "$out_dir_override" out_dir "$bench_root/results")"
if [[ "$out_base" != /* ]]; then
  out_base="$ws_root/$out_base"
fi

suites_raw="$(expand_suites_raw "$suites_raw")"
targets_raw="$(expand_targets_raw "$targets_raw")"
IFS=',' read -r -a suites <<< "$suites_raw"
IFS=',' read -r -a targets <<< "$targets_raw"
IFS=',' read -r -a vcpus <<< "$vcpus_raw"
IFS=',' read -r -a pipeline_depths <<< "$pipeline_depths_raw"

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
run_id="${RUN_ID:-server-suite-$stamp}"
if [[ "$out_base" == */server-suite-* ]]; then
  out_dir="$out_base"
else
  out_dir="$out_base/$run_id"
fi

mkdir -p "$out_dir/tmp" "$out_dir/plans"
trap stop_targets EXIT

if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon is not reachable. Start Docker, then rerun the benchmark suite." >&2
  exit 1
fi

cd "$ws_root"
cargo build --release -p shardcache-benchmarks \
  --bin redis_command_matrix \
  --bin redis_command_report \
  --bin redis_command_manifest

git_sha="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
git_branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"

cat > "$out_dir/metadata.json" <<EOF
{
  "schema_version": 1,
  "created_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "run_id": "$run_id",
  "git_sha": "$git_sha",
  "git_branch": "$git_branch",
  "config_file": "$config_file",
  "compose_file": "$compose_file",
  "suites": "$suites_raw",
  "targets": "$targets_raw",
  "redis_module_target": "redis-stack",
  "redis_cluster_target": "redis-cluster",
  "shardcache_protocol_targets": "shardcache-resp,shardcache-scnp",
  "vcpus": "$vcpus_raw",
  "clients": $clients,
  "pipeline_depths": "$pipeline_depths_raw",
  "warmup": $warmup,
  "duration": $duration,
  "key_shards": "$key_shards",
  "redis_cluster_slot_lanes": "$key_shards",
  "fixture_scope": "$fixture_scope",
  "memory_budget_mib": $memory_budget_mib,
  "command_budget": $command_budget,
  "cpu_pinning": "${CPUSET_CPUS:-first-n-linux-cpus}",
  "server_memory_limit": "${SERVER_MEMORY_LIMIT:-}"
}
EOF

plan_files=()
target_csvs=()

for suite in "${suites[@]}"; do
  cases="$(suite_value "$suite" cases all)"
  suite_fixture_scope="$(suite_value "$suite" fixture_scope "$fixture_scope")"
  scenario="$(suite_value "$suite" title "$suite")"
  for vcpu in "${vcpus[@]}"; do
    vcpu="$(trim "$vcpu")"
    lane_count="$(effective_key_shards "$vcpu")"
    for pipeline_depth in "${pipeline_depths[@]}"; do
      pipeline_depth="$(trim "$pipeline_depth")"
      plan_id="$suite-v${vcpu}-p${pipeline_depth}-c${clients}-k${lane_count}-m${memory_budget_mib}-b${command_budget}"

      for target in "${targets[@]}"; do
        target="$(trim "$target")"
        plan_file="$out_dir/plans/$target-$suite-v${vcpu}-p${pipeline_depth}.json"
        target_csv="$out_dir/$target.csv"
        tmp_csv="$out_dir/tmp/$target-$suite-v${vcpu}-p${pipeline_depth}.csv"
        plan_files+=("$plan_file")
        target_csvs+=("$target_csv")

        echo "benchmark target=$target suite=$suite vcpus=$vcpu pipeline_depth=$pipeline_depth"
        stop_targets
        start_target "$target" "$vcpu"

        "$ws_root/target/release/redis_command_matrix" \
          --targets "$(target_spec "$target" "$vcpu")" \
          --cases "$cases" \
          --fixture-scope "$suite_fixture_scope" \
          --clients "$clients" \
          --key-shards "$lane_count" \
          --pipeline-depth "$pipeline_depth" \
          --warmup "$warmup" \
          --duration "$duration" \
          --memory-budget-mib "$memory_budget_mib" \
          --command-budget "$command_budget" \
          --run-id "$run_id" \
          --resolved-plan-id "$plan_id" \
          --suite "$suite" \
          --scenario "$scenario" \
          --vcpus "$vcpu" \
          --plan-json "$plan_file" \
          --csv "$tmp_csv"

        append_csv "$tmp_csv" "$target_csv"
      done
    done
  done
done

stop_targets

{
  echo '{'
  echo '  "schema_version": 1,'
  echo "  \"run_id\": \"$run_id\","
  echo '  "plans": ['
  for index in "${!plan_files[@]}"; do
    comma=','
    if [[ "$index" == "$((${#plan_files[@]} - 1))" ]]; then
      comma=''
    fi
    echo "    \"${plan_files[$index]#$out_dir/}\"$comma"
  done
  echo '  ]'
  echo '}'
} > "$out_dir/resolved-plan.json"

unique_csvs=()
for path in "${target_csvs[@]}"; do
  [[ -f "$path" ]] || continue
  seen=0
  for existing in "${unique_csvs[@]:-}"; do
    if [[ "$existing" == "$path" ]]; then
      seen=1
      break
    fi
  done
  if [[ "$seen" == "0" ]]; then
    unique_csvs+=("$path")
  fi
done

if [[ "${#unique_csvs[@]}" -gt 0 ]]; then
  report_args=(
    --csv "${unique_csvs[0]}"
    --markdown "$out_dir/report.md"
    --json "$out_dir/summary.json"
    --label "standardized server benchmark $run_id"
  )
  for csv in "${unique_csvs[@]:1}"; do
    report_args+=(--reference-csv "$csv")
  done
  "$ws_root/target/release/redis_command_report" "${report_args[@]}"
fi

"$ws_root/target/release/redis_command_manifest" \
  --format json \
  --output "$out_dir/redis-compatibility.json"

cat <<EOF
benchmark suite written:
  $out_dir/metadata.json
  $out_dir/resolved-plan.json
  $out_dir/report.md
  $out_dir/summary.json
  $out_dir/redis-compatibility.json
  target CSVs: ${unique_csvs[*]:-none}
EOF
