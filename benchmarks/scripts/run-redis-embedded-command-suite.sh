#!/usr/bin/env bash
# Run Redis-compatible command benchmarks through the embedded shardcache API.

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
bench_root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$bench_root/.." && pwd)"
config_file="$bench_root/bench.toml"

usage() {
  cat <<'USAGE'
Usage:
  benchmarks/scripts/run-redis-embedded-command-suite.sh [options]

Options:
  --config PATH              Benchmark config TOML (default: benchmarks/bench.toml)
  --suite LIST               Suites: redis-core,redis-v6-v7,redis-v8,redis-v8-vector,redis-keyspace,redis-destructive,redis-modules,all
  --vcpus LIST               Comma-separated vCPU matrix, for example 1,2,4,8,16
  --clients N                Concurrent embedded client threads
  --pipeline-depth LIST      Comma-separated loop-unroll depths
  --warmup SECONDS           Warmup duration
  --duration SECONDS         Measurement duration
  --key-shards N             Logical key lanes
  --store-shards N           Embedded store shards; default matches vCPU count
  --fixture-scope MODE       per-client or shared-keyspace
  --memory-budget-mib N      Total prepared command memory budget
  --command-budget N         Total prepared command count budget; 0 means memory-bounded
  --out-dir PATH             Output directory or parent results directory

Examples:
  benchmarks/scripts/run-redis-embedded-command-suite.sh --suite redis-core --vcpus 1,2,4,8,16
  benchmarks/scripts/run-redis-embedded-command-suite.sh --suite all --vcpus 1,2,4,8,16 --memory-budget-mib 512
USAGE
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

toml_value_from_file() {
  local file="$1"
  local key="$2"
  local default="$3"
  local line value
  line="$(grep -E "^[[:space:]]*$key[[:space:]]*=" "$file" | tail -n 1 || true)"
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

toml_value() {
  toml_value_from_file "$config_file" "$1" "$2"
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

suite_value() {
  local suite="$1"
  local key="$2"
  local default="$3"
  local suite_file="$bench_root/suites/$suite.toml"
  if [[ ! -f "$suite_file" ]]; then
    echo "unknown benchmark suite: $suite" >&2
    exit 2
  fi
  toml_value_from_file "$suite_file" "$key" "$default"
}

cpuset_for_vcpu() {
  local vcpu="$1"
  if [[ -n "${CPUSET_CPUS:-}" ]]; then
    printf '%s' "$CPUSET_CPUS"
  elif [[ "$(uname -s)" == "Linux" ]]; then
    if [[ "$vcpu" == "1" ]]; then
      printf '0'
    else
      printf '0-%s' "$((vcpu - 1))"
    fi
  fi
}

run_pinned() {
  local vcpu="$1"
  shift
  local cpuset
  cpuset="$(cpuset_for_vcpu "$vcpu")"
  if [[ -n "$cpuset" && "$(command -v taskset || true)" != "" ]]; then
    taskset -c "$cpuset" "$@"
  else
    "$@"
  fi
}

effective_store_shards() {
  local vcpu="$1"
  if [[ -n "$store_shards" ]]; then
    printf '%s' "$store_shards"
  elif [[ -n "${SHARD_COUNT:-}" ]]; then
    printf '%s' "$SHARD_COUNT"
  else
    printf '%s' "$vcpu"
  fi
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
vcpus_override=""
clients_override=""
pipeline_depths_override=""
warmup_override=""
duration_override=""
key_shards_override=""
store_shards=""
fixture_scope_override=""
memory_budget_override=""
command_budget_override=""
out_dir_override=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config) config_override="$2"; shift 2 ;;
    --config=*) config_override="${1#*=}"; shift ;;
    --suite|--suites) suites_override="$2"; shift 2 ;;
    --suite=*|--suites=*) suites_override="${1#*=}"; shift ;;
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
    --store-shards) store_shards="$2"; shift 2 ;;
    --store-shards=*) store_shards="${1#*=}"; shift ;;
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
IFS=',' read -r -a suites <<< "$suites_raw"
IFS=',' read -r -a vcpus <<< "$vcpus_raw"
IFS=',' read -r -a pipeline_depths <<< "$pipeline_depths_raw"

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
run_id="${RUN_ID:-redis-embedded-command-suite-$stamp}"
if [[ "$out_base" == */redis-embedded-command-suite-* ]]; then
  out_dir="$out_base"
else
  out_dir="$out_base/$run_id"
fi

mkdir -p "$out_dir/tmp" "$out_dir/plans"

cd "$ws_root"
cargo build --release -p shardcache-benchmarks \
  --features redis-embedded-commands \
  --bin redis_embedded_command_matrix \
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
  "suites": "$suites_raw",
  "target": "shardcache-embedded",
  "protocol": "embedded",
  "vcpus": "$vcpus_raw",
  "clients": $clients,
  "pipeline_depths": "$pipeline_depths_raw",
  "warmup": $warmup,
  "duration": $duration,
  "key_shards": $key_shards,
  "store_shards": "${store_shards:-match-vcpus}",
  "fixture_scope": "$fixture_scope",
  "memory_budget_mib": $memory_budget_mib,
  "command_budget": $command_budget,
  "cpu_pinning": "${CPUSET_CPUS:-first-n-linux-cpus}"
}
EOF

plan_files=()
csv="$out_dir/shardcache-embedded.csv"

for suite in "${suites[@]}"; do
  suite="$(trim "$suite")"
  cases="$(suite_value "$suite" cases all)"
  suite_fixture_scope="$(suite_value "$suite" fixture_scope "$fixture_scope")"
  scenario="$(suite_value "$suite" title "$suite")"
  for vcpu in "${vcpus[@]}"; do
    vcpu="$(trim "$vcpu")"
    run_store_shards="$(effective_store_shards "$vcpu")"
    for pipeline_depth in "${pipeline_depths[@]}"; do
      pipeline_depth="$(trim "$pipeline_depth")"
      plan_id="$suite-v${vcpu}-p${pipeline_depth}-c${clients}-k${key_shards}-m${memory_budget_mib}-b${command_budget}"
      plan_file="$out_dir/plans/shardcache-embedded-$suite-v${vcpu}-p${pipeline_depth}.json"
      tmp_csv="$out_dir/tmp/shardcache-embedded-$suite-v${vcpu}-p${pipeline_depth}.csv"
      plan_files+=("$plan_file")

      echo "benchmark target=shardcache-embedded suite=$suite vcpus=$vcpu pipeline_depth=$pipeline_depth store_shards=$run_store_shards"
      run_pinned "$vcpu" "$ws_root/target/release/redis_embedded_command_matrix" \
        --cases "$cases" \
        --fixture-scope "$suite_fixture_scope" \
        --clients "$clients" \
        --key-shards "$key_shards" \
        --store-shards "$run_store_shards" \
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

      append_csv "$tmp_csv" "$csv"
    done
  done
done

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

if [[ -f "$csv" ]]; then
  "$ws_root/target/release/redis_command_report" \
    --csv "$csv" \
    --markdown "$out_dir/report.md" \
    --json "$out_dir/summary.json" \
    --label "embedded Redis command benchmark $run_id"
fi

"$ws_root/target/release/redis_command_manifest" \
  --format json \
  --output "$out_dir/redis-compatibility.json"

cat <<EOF
embedded Redis command suite written:
  $out_dir/metadata.json
  $out_dir/resolved-plan.json
  $out_dir/report.md
  $out_dir/summary.json
  $out_dir/redis-compatibility.json
  CSV: $csv
EOF
