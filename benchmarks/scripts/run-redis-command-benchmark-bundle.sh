#!/usr/bin/env bash
# Run redis_command_matrix and package CSV, JSON, Markdown, and environment metadata.

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
bench_root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$bench_root/.." && pwd)"

cd "$ws_root"

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${OUT_DIR:-$bench_root/results/redis-command-matrix-$stamp}"
csv_path="$out_dir/redis-command-matrix.csv"
label="${LABEL:-redis command matrix $stamp}"

mkdir -p "$out_dir"

cargo build --release -p shardcache-benchmarks \
  --bin redis_command_manifest \
  --bin redis_command_report

{
  echo "created_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_sha=$(git rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "git_branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
  echo "git_status_short_begin"
  git status --short 2>/dev/null || true
  echo "git_status_short_end"
  echo "uname=$(uname -a)"
  echo "rustc_begin"
  rustc -Vv 2>/dev/null || true
  echo "rustc_end"
  echo "cargo=$(cargo -V 2>/dev/null || true)"
  echo "docker=$(docker --version 2>/dev/null || true)"
  echo "targets=${TARGETS:-shardcache=127.0.0.1:6383,redis=127.0.0.1:6379,valkey=127.0.0.1:6381}"
  echo "cases=${CASES:-all}"
  echo "skip_cases=${SKIP_CASES:-}"
  echo "fixture_scope=${FIXTURE_SCOPE:-per-client}"
  echo "clients=${CLIENTS:-1}"
  echo "key_shards=${KEY_SHARDS:-1}"
  echo "pipeline_depth=${PIPELINE_DEPTH:-1}"
  echo "warmup=${WARMUP:-1}"
  echo "duration=${DURATION:-5}"
  echo "server_cpuset=${SERVER_CPUSET:-}"
  echo "shard_count=${SHARD_COUNT:-4}"
  echo "server_direct_shard_ports=${SERVER_DIRECT_SHARD_PORTS:-0}"
  echo "shardcache_direct_shard_base_port=${SHARDCACHE_DIRECT_SHARD_BASE_PORT:-}"
  echo "start_shardcache=${START_SHARDCACHE:-1}"
  echo "docker=${DOCKER:-1}"
  echo "docker_services=${DOCKER_SERVICES:-redis valkey}"
  echo "reference_csvs=${REFERENCE_CSVS:-}"
} >"$out_dir/metadata.txt"

CSV="$csv_path" "$here/run-redis-command-matrix.sh"

report_args=(
  --csv "$csv_path"
  --markdown "$out_dir/report.md"
  --json "$out_dir/summary.json"
  --label "$label"
)
if [[ -n "${BASELINE:-}" ]]; then
  report_args+=(--baseline "$BASELINE")
fi
if [[ -n "${REFERENCE_CSVS:-}" ]]; then
  IFS=',' read -r -a reference_csvs <<< "$REFERENCE_CSVS"
  for reference_csv in "${reference_csvs[@]}"; do
    if [[ -n "$reference_csv" ]]; then
      report_args+=(--reference-csv "$reference_csv")
    fi
  done
fi

"$ws_root/target/release/redis_command_report" "${report_args[@]}"
"$ws_root/target/release/redis_command_manifest" \
  --format json \
  --output "$out_dir/redis-compatibility.json"

cat <<EOF
redis command benchmark bundle written:
  $out_dir/metadata.txt
  $out_dir/redis-command-matrix.csv
  $out_dir/report.md
  $out_dir/summary.json
  $out_dir/redis-compatibility.json
EOF
