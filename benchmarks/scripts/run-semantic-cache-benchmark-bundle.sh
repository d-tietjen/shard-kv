#!/usr/bin/env bash
# Run shardmap semantic-cache benchmark rows and package Adam-ready artifacts.

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
bench_root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$bench_root/.." && pwd)"

cd "$ws_root"

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
out_dir="${OUT_DIR:-$bench_root/results/semantic-cache-$stamp}"
label="${LABEL:-semantic cache matrix $stamp}"

pairs="${PAIRS:-5000}"
dims="${DIMS:-384}"
index_entries="${INDEX_ENTRIES:-5000}"
measured_queries="${MEASURED_QUERIES:-200}"
warmup_queries="${WARMUP_QUERIES:-50}"
cycling_queries="${CYCLING_QUERIES:-50}"
thresholds="${THRESHOLDS:-0.05,0.10,0.15,0.20,0.25,0.30,0.35,0.40,0.45}"
latency_threshold="${LATENCY_THRESHOLD:-0.35}"
scale_index_entries="${SCALE_INDEX_ENTRIES:-50000,100000}"
scale_measured_queries="${SCALE_MEASURED_QUERIES:-200}"
scale_warmup_queries="${SCALE_WARMUP_QUERIES:-64}"
scale_cycling_queries="${SCALE_CYCLING_QUERIES:-64}"
load_workers="${LOAD_WORKERS:-16}"
load_seconds="${LOAD_SECONDS:-5}"
load_query_pool="${LOAD_QUERY_POOL:-64}"
load_index_entries="${LOAD_INDEX_ENTRIES:-100000}"

quality_csv="$out_dir/shardmap-quality.csv"
latency_csv="$out_dir/shardmap-latency-${index_entries}.csv"
scale_latency_csv="$out_dir/shardmap-latency-scale.csv"
load_csv="$out_dir/shardmap-load.csv"
report_md="$out_dir/report.md"

mkdir -p "$out_dir"
rm -f "$quality_csv" "$latency_csv" "$scale_latency_csv" "$load_csv" "$report_md"

cargo build --release -p shardcache-benchmarks --bin semantic_cache_matrix

{
  echo "created_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "label=$label"
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
  echo "pairs=$pairs"
  echo "dims=$dims"
  echo "index_entries=$index_entries"
  echo "measured_queries=$measured_queries"
  echo "warmup_queries=$warmup_queries"
  echo "cycling_queries=$cycling_queries"
  echo "thresholds=$thresholds"
  echo "latency_threshold=$latency_threshold"
  echo "scale_index_entries=$scale_index_entries"
  echo "scale_measured_queries=$scale_measured_queries"
  echo "scale_warmup_queries=$scale_warmup_queries"
  echo "scale_cycling_queries=$scale_cycling_queries"
  echo "load_workers=$load_workers"
  echo "load_seconds=$load_seconds"
  echo "load_query_pool=$load_query_pool"
  echo "load_index_entries=$load_index_entries"
  echo "pairs_csv=${PAIRS_CSV:-}"
  echo "dataset=${DATASET:-synthetic}"
  echo "peer_csvs=${PEER_CSVS:-}"
  echo "betterdb_article=https://www.betterdb.com/blog/benchmark-semantic-cache-vs-redisvl"
  echo "betterdb_harness=https://github.com/BetterDB-inc/monitor/tree/main/packages/cache-benchmark"
} >"$out_dir/metadata.txt"

base_args=(
  --thresholds "$thresholds"
  --latency-threshold "$latency_threshold"
)
if [[ -n "${PAIRS_CSV:-}" ]]; then
  base_args+=(--pairs-csv "$PAIRS_CSV" --dataset "${DATASET:-semantic-fixture}")
else
  base_args+=(--pairs "$pairs" --dims "$dims" --dataset "${DATASET:-synthetic}")
fi

"$ws_root/target/release/semantic_cache_matrix" \
  "${base_args[@]}" \
  --index-entries "$index_entries" \
  --measured-queries "$measured_queries" \
  --warmup-queries "$warmup_queries" \
  --cycling-queries "$cycling_queries" \
  --quality-csv "$quality_csv" \
  --latency-csv "$latency_csv"

IFS=',' read -r -a scale_entries <<<"$scale_index_entries"
for entries in "${scale_entries[@]}"; do
  entries="$(echo "$entries" | xargs)"
  if [[ -z "$entries" ]]; then
    continue
  fi
  "$ws_root/target/release/semantic_cache_matrix" \
    "${base_args[@]}" \
    --mode latency \
    --index-entries "$entries" \
    --measured-queries "$scale_measured_queries" \
    --warmup-queries "$scale_warmup_queries" \
    --cycling-queries "$scale_cycling_queries" \
    --latency-csv "$scale_latency_csv"
done

if [[ "$load_workers" != "0" ]]; then
  "$ws_root/target/release/semantic_cache_matrix" \
    "${base_args[@]}" \
    --mode load \
    --index-entries "$load_index_entries" \
    --load-workers "$load_workers" \
    --load-seconds "$load_seconds" \
    --load-query-pool "$load_query_pool" \
    --load-csv "$load_csv"
fi

{
  echo "# $label"
  echo
  echo "- Created: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "- Dataset: ${DATASET:-synthetic}"
  echo "- Pairs: $pairs"
  echo "- Dimensions: $dims"
  echo "- Thresholds: $thresholds"
  echo "- BetterDB reference article: https://www.betterdb.com/blog/benchmark-semantic-cache-vs-redisvl"
  echo
  echo "## Quality"
  echo
  echo '| dataset | mode | pairs | distance | precision | recall | F1 | FPR |'
  echo '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |'
  awk -F, 'NR > 1 { printf "| %s | %s | %s | %.2f | %.4f | %.4f | %.4f | %.4f |\n", $1, $3, $4, $5, $11, $12, $13, $15 }' "$quality_csv"
  echo
  echo "## Latency"
  echo
  echo '| source | mode | entries | dims | queries | warmup | p50 ms | p95 ms | p99 ms | hits |'
  echo '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |'
  awk -F, 'NR > 1 { printf "| shardmap | %s | %s | %s | %s | %s | %.4f | %.4f | %.4f | %s |\n", $3, $4, $5, $7, $8, $11, $12, $13, $9 }' "$latency_csv"
  if [[ -f "$scale_latency_csv" ]]; then
    awk -F, 'NR > 1 { printf "| shardmap-scale | %s | %s | %s | %s | %s | %.4f | %.4f | %.4f | %s |\n", $3, $4, $5, $7, $8, $11, $12, $13, $9 }' "$scale_latency_csv"
  fi
  if [[ -f "$load_csv" ]]; then
    echo
    echo "## Hot Load"
    echo
    echo '| source | mode | workers | entries | ops/sec | ops/sut-cpu | p50 ms | p95 ms | p99 ms | process vCPU | hits |'
    echo '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |'
    awk -F, 'NR > 1 { printf "| shardmap | %s | %s | %s | %.0f | %.0f | %.4f | %.4f | %.4f | %.2f | %s |\n", $3, $4, $5, $11, $12, $13, $14, $15, $17, $10 }' "$load_csv"
  fi
  if [[ -n "${PEER_CSVS:-}" ]]; then
    echo
    echo "## Peer CSVs"
    echo
    IFS=',' read -r -a peer_csvs <<<"$PEER_CSVS"
    for peer_csv in "${peer_csvs[@]}"; do
      if [[ -n "$peer_csv" ]]; then
        echo "- $peer_csv"
      fi
    done
  fi
} >"$report_md"

cat <<EOF
semantic cache benchmark bundle written:
  $out_dir/metadata.txt
  $quality_csv
  $latency_csv
  $scale_latency_csv
  $load_csv
  $report_md
EOF
