#!/usr/bin/env bash
# Compare independently collected redis_command_matrix-compatible CSV files.

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
bench_root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$bench_root/.." && pwd)"

if [[ "$#" -lt 2 ]]; then
  cat >&2 <<'USAGE'
Usage:
  benchmarks/scripts/compare-benchmark-csvs.sh primary.csv reference.csv [reference2.csv ...]

Environment:
  OUT_DIR=benchmarks/results/compare-<timestamp>
  LABEL="benchmark comparison"
  BASELINE=redis
USAGE
  exit 2
fi

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${OUT_DIR:-$bench_root/results/compare-$stamp}"
if [[ "$out_dir" != /* ]]; then
  out_dir="$ws_root/$out_dir"
fi
mkdir -p "$out_dir"

cd "$ws_root"
cargo build --release -p shardcache-benchmarks --bin redis_command_report

primary="$1"
shift

args=(
  --csv "$primary"
  --markdown "$out_dir/report.md"
  --json "$out_dir/summary.json"
  --label "${LABEL:-benchmark comparison $stamp}"
)
if [[ -n "${BASELINE:-}" ]]; then
  args+=(--baseline "$BASELINE")
fi
for csv in "$@"; do
  args+=(--reference-csv "$csv")
done

"$ws_root/target/release/redis_command_report" "${args[@]}"

cat <<EOF
benchmark comparison written:
  $out_dir/report.md
  $out_dir/summary.json
EOF
