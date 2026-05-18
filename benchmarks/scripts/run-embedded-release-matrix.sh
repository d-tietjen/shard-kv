#!/usr/bin/env bash
# Linux embedded release benchmark matrix.
#
# Default phase is LRU only so the capacity-bounded path is reviewed before the
# full release matrix is allowed to run.
#
# Examples:
#   PHASE=lru ./benchmarks/scripts/run-embedded-release-matrix.sh
#   PHASE=full ./benchmarks/scripts/run-embedded-release-matrix.sh
#   PHASE=tcp ./benchmarks/scripts/run-embedded-release-matrix.sh
#   PHASE=report DURATION=6 WARMUP=1 ./benchmarks/scripts/run-embedded-release-matrix.sh
#   PHASE=all CONTINUE_AFTER_LRU=1 ./benchmarks/scripts/run-embedded-release-matrix.sh

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$root/.." && pwd)"

phase="${PHASE:-lru}"
repeats="${REPEATS:-3}"
warmup="${WARMUP:-2}"
duration="${DURATION:-10}"
latency_sample_rate="${LATENCY_SAMPLE_RATE:-0}"
ttl_ms="${TTL_MS:-60000}"
addr="${ADDR:-}"
server_pid="${SERVER_PID:-}"
value_sizes="${VALUE_SIZES:-64 512 4096 16384 65536 1048576}"
mixes="${MIXES:-100-0 0-100 80-20}"
lru_cpu_client_pairs="${LRU_CPU_CLIENT_PAIRS:-1:1 4:4 16:16}"
full_cpu_client_pairs="${FULL_CPU_CLIENT_PAIRS:-1:1 2:2 4:4 8:8 16:16}"
lru_resident_pcts="${LRU_RESIDENT_PCTS:-25 50}"
report_value_sizes="${REPORT_VALUE_SIZES:-64 4096 65536 1048576}"
report_cpu_client_pairs="${REPORT_CPU_CLIENT_PAIRS:-1:1 16:16}"
report_lru_resident_pct="${REPORT_LRU_RESIDENT_PCT:-25}"
full_key_memory_cap_bytes="${FULL_KEY_MEMORY_CAP_BYTES:-4000000000}"
large_value_key_floor="${LARGE_VALUE_KEY_FLOOR:-64}"
safe_target="${SAFE_TARGET_DIR:-target/bench-safe}"
unsafe_target="${UNSAFE_TARGET_DIR:-target/bench-unsafe}"

cd "$ws_root"

ts="$(date +%Y%m%d_%H%M%S)"
out_dir="${OUT_DIR:-$root/results/embedded_release_${ts}}"
mkdir -p "$out_dir"

safe_bin="$ws_root/$safe_target/release/saturation"
unsafe_bin="$ws_root/$unsafe_target/release/saturation"

build_binaries() {
  CARGO_TARGET_DIR="$safe_target" cargo native-bench --bin saturation
  CARGO_TARGET_DIR="$unsafe_target" cargo native-bench --features unsafe --bin saturation
}

cpuset_for_vcpu() {
  local vcpu="$1"
  case "$vcpu" in
    1) echo "0" ;;
    2) echo "0-1" ;;
    4) echo "0-3" ;;
    8) echo "0-7" ;;
    16) echo "0-15" ;;
    *) echo "0-$((vcpu - 1))" ;;
  esac
}

run_pinned() {
  local vcpu="$1"
  shift
  if command -v taskset >/dev/null 2>&1; then
    taskset -c "$(cpuset_for_vcpu "$vcpu")" "$@"
  else
    "$@"
  fi
}

log_progress() {
  printf '[%s] %s\n' "$(date -Is)" "$*"
}

key_count_for_value_size() {
  local value_size="$1"
  python3 - "$value_size" "$full_key_memory_cap_bytes" "$large_value_key_floor" <<'PY'
import sys
value_size = int(sys.argv[1])
cap_bytes = int(sys.argv[2])
floor = int(sys.argv[3])
print(min(100_000, max(floor, cap_bytes // max(value_size, 1))))
PY
}

memory_bytes_for_capacity() {
  local value_size="$1"
  local capacity_keys="$2"
  python3 - "$value_size" "$capacity_keys" <<'PY'
import sys
value_size = int(sys.argv[1])
capacity_keys = int(sys.argv[2])
print(capacity_keys * (value_size + 64))
PY
}

capacity_keys_for_pressure() {
  local key_count="$1"
  local resident_pct="$2"
  python3 - "$key_count" "$resident_pct" <<'PY'
import sys
key_count = int(sys.argv[1])
resident_pct = int(sys.argv[2])
print(max(1, key_count * resident_pct // 100))
PY
}

run_saturation() {
  local bin="$1"
  local csv="$2"
  local backends="$3"
  local value_size="$4"
  local mix="$5"
  local vcpu="$6"
  local clients="$7"
  local key_count="$8"
  local ttl_arg="$9"
  local eviction_policy="${10}"
  local capacity_keys="${11}"
  local memory_bytes="${12}"

  local args=(
    --backends "$backends"
    --value-size "$value_size"
    --mix "$mix"
    --vcpu-budget "$vcpu"
    --clients "$clients"
    --pipeline-depth 1
    --key-count "$key_count"
    --warmup "$warmup"
    --duration "$duration"
    --latency-sample-rate "$latency_sample_rate"
    --eviction-policy "$eviction_policy"
    --csv "$csv"
  )

  if [[ -n "$addr" ]]; then
    args+=(--addr "$addr")
  fi
  if [[ -n "$server_pid" ]]; then
    args+=(--server-pid "$server_pid")
  fi
  if [[ "$ttl_arg" != "none" ]]; then
    args+=(--ttl-ms "$ttl_arg")
  fi
  if [[ "$capacity_keys" != "none" ]]; then
    args+=(--cache-capacity-keys "$capacity_keys")
  fi
  if [[ "$memory_bytes" != "none" ]]; then
    args+=(--cache-memory-bytes "$memory_bytes")
  fi

  log_progress "start bin=$(basename "$bin") backends=$backends value=$value_size mix=$mix vcpu=$vcpu clients=$clients ttl=$ttl_arg eviction=$eviction_policy capacity_keys=$capacity_keys memory_bytes=$memory_bytes"
  run_pinned "$vcpu" "$bin" "${args[@]}"
  log_progress "done  bin=$(basename "$bin") backends=$backends value=$value_size mix=$mix vcpu=$vcpu clients=$clients ttl=$ttl_arg eviction=$eviction_policy"
}

run_lru_phase() {
  local csv="$out_dir/lru_first.csv"
  for repeat in $(seq 1 "$repeats"); do
    for value_size in $value_sizes; do
      local key_count
      key_count="$(key_count_for_value_size "$value_size")"
      for resident_pct in $lru_resident_pcts; do
        local capacity_keys
        capacity_keys="$(capacity_keys_for_pressure "$key_count" "$resident_pct")"
        local memory_bytes
        memory_bytes="$(memory_bytes_for_capacity "$value_size" "$capacity_keys")"
        for mix in $mixes; do
          for pair in $lru_cpu_client_pairs; do
            local vcpu="${pair%%:*}"
            local clients="${pair##*:}"
            log_progress "lru repeat=$repeat value=$value_size mix=$mix vcpu=$vcpu clients=$clients resident=${resident_pct}% ttl=none"
            run_saturation "$safe_bin" "$csv" "fc-embed,fc-shared,moka,lru" "$value_size" "$mix" "$vcpu" "$clients" "$key_count" "none" "lru" "$capacity_keys" "$memory_bytes"
            run_saturation "$unsafe_bin" "$csv" "fc-embed,fc-shared" "$value_size" "$mix" "$vcpu" "$clients" "$key_count" "none" "lru" "$capacity_keys" "$memory_bytes"

            log_progress "lru repeat=$repeat value=$value_size mix=$mix vcpu=$vcpu clients=$clients resident=${resident_pct}% ttl=${ttl_ms}ms"
            run_saturation "$safe_bin" "$csv" "fc-embed,fc-shared,moka,lru" "$value_size" "$mix" "$vcpu" "$clients" "$key_count" "$ttl_ms" "lru" "$capacity_keys" "$memory_bytes"
            run_saturation "$unsafe_bin" "$csv" "fc-embed,fc-shared" "$value_size" "$mix" "$vcpu" "$clients" "$key_count" "$ttl_ms" "lru" "$capacity_keys" "$memory_bytes"
          done
        done
      done
    done
  done
  echo "wrote $csv"
}

run_full_phase() {
  local csv="$out_dir/full_embedded.csv"
  local safe_backends="fc-embed,fc-shared,fc-shared-ref,fc-shared-x4,dashmap,dashmap-worker-shards,dashmap-ref,moka,rwlock-hashmap"
  local unsafe_backends="fc-embed,fc-shared,fc-shared-ref,fc-shared-x4"
  for repeat in $(seq 1 "$repeats"); do
    for value_size in $value_sizes; do
      local key_count
      key_count="$(key_count_for_value_size "$value_size")"
      for mix in $mixes; do
        for pair in $full_cpu_client_pairs; do
          local vcpu="${pair%%:*}"
          local clients="${pair##*:}"
          log_progress "full repeat=$repeat value=$value_size mix=$mix vcpu=$vcpu clients=$clients ttl=none"
          run_saturation "$safe_bin" "$csv" "$safe_backends" "$value_size" "$mix" "$vcpu" "$clients" "$key_count" "none" "none" "none" "none"
          run_saturation "$unsafe_bin" "$csv" "$unsafe_backends" "$value_size" "$mix" "$vcpu" "$clients" "$key_count" "none" "none" "none" "none"

          log_progress "full repeat=$repeat value=$value_size mix=$mix vcpu=$vcpu clients=$clients ttl=${ttl_ms}ms"
          run_saturation "$safe_bin" "$csv" "$safe_backends" "$value_size" "$mix" "$vcpu" "$clients" "$key_count" "$ttl_ms" "none" "none" "none"
          run_saturation "$unsafe_bin" "$csv" "$unsafe_backends" "$value_size" "$mix" "$vcpu" "$clients" "$key_count" "$ttl_ms" "none" "none" "none"
        done
      done
    done
  done
  echo "wrote $csv"
}

run_report_phase() {
  local csv="$out_dir/embedded_report.csv"
  local baseline_safe_backends="${REPORT_BASELINE_SAFE_BACKENDS:-fc-embed,fc-shared,dashmap,moka}"
  local baseline_unsafe_backends="${REPORT_BASELINE_UNSAFE_BACKENDS:-fc-embed,fc-shared}"
  local lru_safe_backends="${REPORT_LRU_SAFE_BACKENDS:-fc-embed,fc-shared,moka,lru}"
  local lru_ttl_safe_backends="${REPORT_LRU_TTL_SAFE_BACKENDS:-fc-embed,fc-shared,moka}"
  local lru_unsafe_backends="${REPORT_LRU_UNSAFE_BACKENDS:-fc-embed,fc-shared}"

  for repeat in $(seq 1 "$repeats"); do
    for value_size in $report_value_sizes; do
      local key_count
      key_count="$(key_count_for_value_size "$value_size")"
      local capacity_keys
      capacity_keys="$(capacity_keys_for_pressure "$key_count" "$report_lru_resident_pct")"
      local memory_bytes
      memory_bytes="$(memory_bytes_for_capacity "$value_size" "$capacity_keys")"

      for mix in $mixes; do
        for pair in $report_cpu_client_pairs; do
          local vcpu="${pair%%:*}"
          local clients="${pair##*:}"

          log_progress "report baseline repeat=$repeat value=$value_size mix=$mix vcpu=$vcpu clients=$clients ttl=none"
          run_saturation "$safe_bin" "$csv" "$baseline_safe_backends" "$value_size" "$mix" "$vcpu" "$clients" "$key_count" "none" "none" "none" "none"
          run_saturation "$unsafe_bin" "$csv" "$baseline_unsafe_backends" "$value_size" "$mix" "$vcpu" "$clients" "$key_count" "none" "none" "none" "none"

          log_progress "report ttl repeat=$repeat value=$value_size mix=$mix vcpu=$vcpu clients=$clients ttl=${ttl_ms}ms"
          run_saturation "$safe_bin" "$csv" "$baseline_safe_backends" "$value_size" "$mix" "$vcpu" "$clients" "$key_count" "$ttl_ms" "none" "none" "none"
          run_saturation "$unsafe_bin" "$csv" "$baseline_unsafe_backends" "$value_size" "$mix" "$vcpu" "$clients" "$key_count" "$ttl_ms" "none" "none" "none"

          log_progress "report lru repeat=$repeat value=$value_size mix=$mix vcpu=$vcpu clients=$clients resident=${report_lru_resident_pct}% ttl=none"
          run_saturation "$safe_bin" "$csv" "$lru_safe_backends" "$value_size" "$mix" "$vcpu" "$clients" "$key_count" "none" "lru" "$capacity_keys" "$memory_bytes"
          run_saturation "$unsafe_bin" "$csv" "$lru_unsafe_backends" "$value_size" "$mix" "$vcpu" "$clients" "$key_count" "none" "lru" "$capacity_keys" "$memory_bytes"

          log_progress "report lru+ttl repeat=$repeat value=$value_size mix=$mix vcpu=$vcpu clients=$clients resident=${report_lru_resident_pct}% ttl=${ttl_ms}ms"
          run_saturation "$safe_bin" "$csv" "$lru_ttl_safe_backends" "$value_size" "$mix" "$vcpu" "$clients" "$key_count" "$ttl_ms" "lru" "$capacity_keys" "$memory_bytes"
          run_saturation "$unsafe_bin" "$csv" "$lru_unsafe_backends" "$value_size" "$mix" "$vcpu" "$clients" "$key_count" "$ttl_ms" "lru" "$capacity_keys" "$memory_bytes"
        done
      done
    done
  done
  echo "wrote $csv"
}

run_tcp_phase() {
  local csv="$out_dir/tcp_routing_appendix.csv"
  echo "tcp phase expects fast-cache-server direct mode already running; set ADDR to shard port base/fanout as needed"
  for repeat in $(seq 1 "$repeats"); do
    for value_size in 64 512 4096 16384; do
      local key_count
      key_count="$(key_count_for_value_size "$value_size")"
      for mix in $mixes; do
        run_saturation "$safe_bin" "$csv" "fc-server-fcnp,fc-server-fcnp-direct" "$value_size" "$mix" 16 64 "$key_count" "none" "none" "none" "none"
      done
    done
  done
  echo "wrote $csv"
}

build_binaries

case "$phase" in
  lru)
    run_lru_phase
    ;;
  full)
    run_full_phase
    ;;
  report)
    run_report_phase
    ;;
  tcp)
    run_tcp_phase
    ;;
  all)
    run_lru_phase
    if [[ "${CONTINUE_AFTER_LRU:-0}" != "1" ]]; then
      echo "LRU gate complete. Review $out_dir/lru_first.csv before running PHASE=full."
      exit 0
    fi
    run_full_phase
    run_tcp_phase
    ;;
  *)
    echo "unknown PHASE=$phase; use lru, full, tcp, or all" >&2
    exit 2
    ;;
esac

echo "results directory: $out_dir"
