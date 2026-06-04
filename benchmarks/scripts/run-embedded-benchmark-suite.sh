#!/usr/bin/env bash
# Run standardized embedded cache benchmarks with the saturation driver.

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
bench_root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$bench_root/.." && pwd)"
config_file="$bench_root/embedded.toml"

usage() {
  cat <<'USAGE'
Usage:
  benchmarks/scripts/run-embedded-benchmark-suite.sh [options]

Options:
  --config PATH              Embedded benchmark config TOML (default: benchmarks/embedded.toml)
  --suite LIST               Suites: embedded-core,embedded-ttl,embedded-lru,embedded-copy,all
  --backends LIST            Override suite backend list
  --vcpus LIST               Comma-separated vCPU matrix, for example 1,2,4,8,16
  --clients N|match-vcpus    Client threads per run (default: match-vcpus)
  --value-sizes LIST         Comma-separated value sizes in bytes
  --mixes LIST               Comma-separated mixes: 100-0,0-100,80-20
  --warmup SECONDS           Warmup duration
  --duration SECONDS         Measurement duration
  --latency-sample-rate N    Record one latency sample every N measured ops; 0 disables timing
  --key-memory-cap-bytes N   Key-count memory budget for generated fixture cardinality
  --large-value-key-floor N  Minimum key count for large values
  --key-pattern MODE         point or session
  --key-distribution MODE    uniform, zipf[:theta], or hot:<keys>[:pct]
  --out-dir PATH             Output directory or parent results directory

Examples:
  benchmarks/scripts/run-embedded-benchmark-suite.sh --suite embedded-core --vcpus 1,2,4,8,16
  benchmarks/scripts/run-embedded-benchmark-suite.sh --suite all --vcpus 1,2,4,8,16 --duration 10

Environment:
  BENCH_FEATURES              Extra shardcache-benchmarks features for cargo build,
                              for example BENCH_FEATURES=telemetry
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
        expanded+=(embedded-core embedded-ttl embedded-lru embedded-copy)
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
  local suite_file="$bench_root/embedded-suites/$suite.toml"
  if [[ ! -f "$suite_file" ]]; then
    echo "unknown embedded benchmark suite: $suite" >&2
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

key_count_for_value_size() {
  local value_size="$1"
  local count
  count=$((key_memory_cap_bytes / value_size))
  if [[ "$count" -gt 100000 ]]; then
    count=100000
  fi
  if [[ "$count" -lt "$large_value_key_floor" ]]; then
    count="$large_value_key_floor"
  fi
  if [[ "$count" -lt 1 ]]; then
    count=1
  fi
  printf '%s' "$count"
}

capacity_keys_for_pct() {
  local key_count="$1"
  local pct="$2"
  local count
  count=$((key_count * pct / 100))
  if [[ "$count" -lt 1 ]]; then
    count=1
  fi
  printf '%s' "$count"
}

memory_bytes_for_capacity() {
  local value_size="$1"
  local capacity_keys="$2"
  printf '%s' "$((capacity_keys * (value_size + 64)))"
}

client_count_for_vcpu() {
  local vcpu="$1"
  if [[ "$clients" == "match-vcpus" ]]; then
    printf '%s' "$vcpu"
  else
    printf '%s' "$clients"
  fi
}

append_summary_row() {
  local csv="$1"
  local suite="$2"
  if [[ ! -f "$csv" ]]; then
    return
  fi
  awk -F, -v suite="$suite" '
    NR == 1 { next }
    {
      key = $1 "," $9 "," $10 "," $11 "," $12
      ops[key] += $16
      count[key] += 1
    }
    END {
      for (key in ops) {
        printf("| %s | %s | %.0f |\n", suite, key, ops[key] / count[key])
      }
    }
  ' "$csv" >> "$out_dir/report.md"
}

config_override=""
suites_override=""
backends_override=""
vcpus_override=""
clients_override=""
value_sizes_override=""
mixes_override=""
warmup_override=""
duration_override=""
latency_sample_rate_override=""
key_memory_cap_override=""
large_value_key_floor_override=""
key_pattern_override=""
key_distribution_override=""
out_dir_override=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config) config_override="$2"; shift 2 ;;
    --config=*) config_override="${1#*=}"; shift ;;
    --suite|--suites) suites_override="$2"; shift 2 ;;
    --suite=*|--suites=*) suites_override="${1#*=}"; shift ;;
    --backends) backends_override="$2"; shift 2 ;;
    --backends=*) backends_override="${1#*=}"; shift ;;
    --vcpus) vcpus_override="$2"; shift 2 ;;
    --vcpus=*) vcpus_override="${1#*=}"; shift ;;
    --clients) clients_override="$2"; shift 2 ;;
    --clients=*) clients_override="${1#*=}"; shift ;;
    --value-sizes) value_sizes_override="$2"; shift 2 ;;
    --value-sizes=*) value_sizes_override="${1#*=}"; shift ;;
    --mixes) mixes_override="$2"; shift 2 ;;
    --mixes=*) mixes_override="${1#*=}"; shift ;;
    --warmup) warmup_override="$2"; shift 2 ;;
    --warmup=*) warmup_override="${1#*=}"; shift ;;
    --duration) duration_override="$2"; shift 2 ;;
    --duration=*) duration_override="${1#*=}"; shift ;;
    --latency-sample-rate) latency_sample_rate_override="$2"; shift 2 ;;
    --latency-sample-rate=*) latency_sample_rate_override="${1#*=}"; shift ;;
    --key-memory-cap-bytes) key_memory_cap_override="$2"; shift 2 ;;
    --key-memory-cap-bytes=*) key_memory_cap_override="${1#*=}"; shift ;;
    --large-value-key-floor) large_value_key_floor_override="$2"; shift 2 ;;
    --large-value-key-floor=*) large_value_key_floor_override="${1#*=}"; shift ;;
    --key-pattern) key_pattern_override="$2"; shift 2 ;;
    --key-pattern=*) key_pattern_override="${1#*=}"; shift ;;
    --key-distribution) key_distribution_override="$2"; shift 2 ;;
    --key-distribution=*) key_distribution_override="${1#*=}"; shift ;;
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
  echo "embedded benchmark config not found: $config_file" >&2
  exit 2
fi

suites_raw="$(setting "$suites_override" suites embedded-core)"
vcpus_raw="$(setting "$vcpus_override" vcpus 1,2,4,8,16)"
clients="$(setting "$clients_override" clients match-vcpus)"
value_sizes_raw="$(setting "$value_sizes_override" value_sizes 64,512,4096,65536,1048576)"
mixes_raw="$(setting "$mixes_override" mixes 100-0,0-100,80-20)"
warmup="$(setting "$warmup_override" warmup 2)"
duration="$(setting "$duration_override" duration 10)"
latency_sample_rate="$(setting "$latency_sample_rate_override" latency_sample_rate 1)"
key_memory_cap_bytes="$(setting "$key_memory_cap_override" key_memory_cap_bytes 4000000000)"
large_value_key_floor="$(setting "$large_value_key_floor_override" large_value_key_floor 64)"
key_pattern="$(setting "$key_pattern_override" key_pattern point)"
key_distribution="$(setting "$key_distribution_override" key_distribution uniform)"
out_base="$(setting "$out_dir_override" out_dir "$bench_root/results")"
if [[ "$out_base" != /* ]]; then
  out_base="$ws_root/$out_base"
fi

suites_raw="$(expand_suites_raw "$suites_raw")"
IFS=',' read -r -a suites <<< "$suites_raw"
IFS=',' read -r -a vcpus <<< "$vcpus_raw"
IFS=',' read -r -a value_sizes <<< "$value_sizes_raw"
IFS=',' read -r -a mixes <<< "$mixes_raw"

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
run_id="${RUN_ID:-embedded-suite-$stamp}"
if [[ "$out_base" == */embedded-suite-* ]]; then
  out_dir="$out_base"
else
  out_dir="$out_base/$run_id"
fi
mkdir -p "$out_dir"

cd "$ws_root"
if [[ -n "${BENCH_FEATURES:-}" ]]; then
  cargo build --release -p shardcache-benchmarks --features "$BENCH_FEATURES" --bin saturation
else
  cargo build --release -p shardcache-benchmarks --bin saturation
fi

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
  "vcpus": "$vcpus_raw",
  "clients": "$clients",
  "value_sizes": "$value_sizes_raw",
  "mixes": "$mixes_raw",
  "warmup": $warmup,
  "duration": $duration,
  "latency_sample_rate": $latency_sample_rate,
  "key_memory_cap_bytes": $key_memory_cap_bytes,
  "large_value_key_floor": $large_value_key_floor,
  "key_pattern": "$key_pattern",
  "key_distribution": "$key_distribution",
  "bench_features": "${BENCH_FEATURES:-}",
  "cpu_pinning": "${CPUSET_CPUS:-first-n-linux-cpus}"
}
EOF

cat > "$out_dir/report.md" <<EOF
# Embedded Benchmark Suite $run_id

This run compares in-process cache backends with the same generated workload,
same vCPU budget, same client count policy, same value sizes, and same key
distribution.

| Suite | Backend,Value,Mix,vCPU,Clients | Mean Ops/sec |
| --- | --- | ---: |
EOF

for suite in "${suites[@]}"; do
  suite="$(trim "$suite")"
  suite_title="$(suite_value "$suite" title "$suite")"
  suite_backends="$(suite_value "$suite" backends "")"
  if [[ -n "$backends_override" ]]; then
    suite_backends="$backends_override"
  fi
  ttl_ms="$(suite_value "$suite" ttl_ms "")"
  eviction_policy="$(suite_value "$suite" eviction_policy none)"
  capacity_resident_pct="$(suite_value "$suite" capacity_resident_pct "")"
  read_mode="$(suite_value "$suite" read_mode ref)"
  csv="$out_dir/$suite.csv"

  echo "embedded suite=$suite title=$suite_title backends=$suite_backends"
  for value_size in "${value_sizes[@]}"; do
    value_size="$(trim "$value_size")"
    key_count="$(key_count_for_value_size "$value_size")"
    capacity_args=()
    if [[ -n "$capacity_resident_pct" ]]; then
      capacity_keys="$(capacity_keys_for_pct "$key_count" "$capacity_resident_pct")"
      memory_bytes="$(memory_bytes_for_capacity "$value_size" "$capacity_keys")"
      capacity_args+=(--cache-capacity-keys "$capacity_keys" --cache-memory-bytes "$memory_bytes")
    fi
    for mix in "${mixes[@]}"; do
      mix="$(trim "$mix")"
      for vcpu in "${vcpus[@]}"; do
        vcpu="$(trim "$vcpu")"
        run_clients="$(client_count_for_vcpu "$vcpu")"
        args=(
          "$ws_root/target/release/saturation"
          --backends "$suite_backends"
          --value-size "$value_size"
          --mix "$mix"
          --vcpu-budget "$vcpu"
          --clients "$run_clients"
          --pipeline-depth 1
          --key-count "$key_count"
          --warmup "$warmup"
          --duration "$duration"
          --latency-sample-rate "$latency_sample_rate"
          --eviction-policy "$eviction_policy"
          --read-mode "$read_mode"
          --key-pattern "$key_pattern"
          --key-distribution "$key_distribution"
          --csv "$csv"
        )
        if [[ -n "$ttl_ms" ]]; then
          args+=(--ttl-ms "$ttl_ms")
        fi
        if [[ "${#capacity_args[@]}" -gt 0 ]]; then
          args+=("${capacity_args[@]}")
        fi

        echo "benchmark suite=$suite value_size=$value_size mix=$mix vcpus=$vcpu clients=$run_clients"
        run_pinned "$vcpu" "${args[@]}"
      done
    done
  done
  append_summary_row "$csv" "$suite"
done

cat <<EOF
embedded benchmark suite written:
  $out_dir/metadata.json
  $out_dir/report.md
  CSVs: $(printf '%s.csv ' "${suites[@]}")
EOF
