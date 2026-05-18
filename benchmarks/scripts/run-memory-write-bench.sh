#!/usr/bin/env bash
# Linux/server microbench for value write and materialization strategies.

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$root/.." && pwd)"

value_sizes="${VALUE_SIZES:-4096,16384,65536,1048576}"
modes="${MODES:-slice-copy,bytes-copy,vec-bytes,bytes-reuse,aligned-copy,nt-sse2,nt-avx2}"
warmup="${WARMUP_SECONDS:-1}"
duration="${DURATION_SECONDS:-5}"
pool_len="${POOL_LEN:-8}"
cpuset="${CPUSET:-0}"
target_dir="${TARGET_DIR:-target/bench-memory-write}"

cd "$ws_root"

ts="$(date +%Y%m%d_%H%M%S)"
out_dir="${OUT_DIR:-$root/results/memory_write_cost_${ts}}"
mkdir -p "$out_dir"

CARGO_TARGET_DIR="$target_dir" cargo native-bench --bin memory_write_cost

bin="$ws_root/$target_dir/release/memory_write_cost"
csv="$out_dir/memory_write_cost.csv"

cmd=(
  "$bin"
  --value-sizes "$value_sizes"
  --modes "$modes"
  --warmup-seconds "$warmup"
  --duration-seconds "$duration"
  --pool-len "$pool_len"
  --csv "$csv"
)

if command -v taskset >/dev/null 2>&1; then
  taskset -c "$cpuset" "${cmd[@]}" | tee "$out_dir/run.log"
else
  "${cmd[@]}" | tee "$out_dir/run.log"
fi

echo "results directory: $out_dir"
