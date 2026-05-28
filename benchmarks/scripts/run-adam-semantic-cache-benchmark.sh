#!/usr/bin/env bash
# Sync this checkout to Adam, run the semantic-cache bundle there, and fetch artifacts.

set -euo pipefail

if [[ -z "${ADAM_HOST:-}" ]]; then
  cat >&2 <<'EOF'
ADAM_HOST is required.

Example:
  ADAM_HOST=adam.example.com ./benchmarks/scripts/run-adam-semantic-cache-benchmark.sh
EOF
  exit 2
fi

here="$(cd "$(dirname "$0")" && pwd)"
bench_root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$bench_root/.." && pwd)"
stamp="${STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
remote_dir="${REMOTE_DIR:-shard-kv-semantic-$stamp}"
remote_out="benchmarks/results/adam-semantic-cache-$stamp"
local_out="${LOCAL_OUT:-$bench_root/results/adam-semantic-cache-$stamp}"

cd "$ws_root"

ssh_cmd=(ssh)
rsync_cmd=(rsync -az)
if [[ -n "${ADAM_PROXY_COMMAND:-}" ]]; then
  ssh_cmd=(ssh -o "ProxyCommand=$ADAM_PROXY_COMMAND")
  rsync_cmd=(rsync -az -e "ssh -o ProxyCommand=$ADAM_PROXY_COMMAND")
fi
if [[ -n "${ADAM_SSH_OPTS:-}" ]]; then
  # shellcheck disable=SC2206
  ssh_cmd=(ssh $ADAM_SSH_OPTS)
fi

echo "creating remote directory: $ADAM_HOST:$remote_dir"
"${ssh_cmd[@]}" "$ADAM_HOST" "mkdir -p $(printf '%q' "$remote_dir")"

echo "syncing workspace to Adam"
"${rsync_cmd[@]}" \
  --exclude target \
  --exclude benchmarks/results \
  "$ws_root/" \
  "$ADAM_HOST:$remote_dir/"

remote_env=(
  "OUT_DIR=$remote_out"
  "LABEL=adam semantic cache $stamp"
  "PAIRS=${PAIRS:-5000}"
  "DIMS=${DIMS:-384}"
  "INDEX_ENTRIES=${INDEX_ENTRIES:-5000}"
  "MEASURED_QUERIES=${MEASURED_QUERIES:-200}"
  "WARMUP_QUERIES=${WARMUP_QUERIES:-50}"
  "CYCLING_QUERIES=${CYCLING_QUERIES:-50}"
  "SCALE_INDEX_ENTRIES=${SCALE_INDEX_ENTRIES:-50000,100000}"
  "SCALE_MEASURED_QUERIES=${SCALE_MEASURED_QUERIES:-200}"
  "SCALE_WARMUP_QUERIES=${SCALE_WARMUP_QUERIES:-64}"
  "SCALE_CYCLING_QUERIES=${SCALE_CYCLING_QUERIES:-64}"
  "LOAD_INDEX_ENTRIES=${LOAD_INDEX_ENTRIES:-100000}"
  "LOAD_WORKERS=${LOAD_WORKERS:-16}"
  "LOAD_SECONDS=${LOAD_SECONDS:-5}"
  "LOAD_QUERY_POOL=${LOAD_QUERY_POOL:-64}"
  "THRESHOLDS=${THRESHOLDS:-0.05,0.10,0.15,0.20,0.25,0.30,0.35,0.40,0.45}"
  "LATENCY_THRESHOLD=${LATENCY_THRESHOLD:-0.35}"
)
if [[ -n "${PAIRS_CSV:-}" ]]; then
  remote_env+=("PAIRS_CSV=$PAIRS_CSV")
fi
if [[ -n "${DATASET:-}" ]]; then
  remote_env+=("DATASET=$DATASET")
fi
if [[ -n "${PEER_CSVS:-}" ]]; then
  remote_env+=("PEER_CSVS=$PEER_CSVS")
fi

remote_env_cmd="$(printf '%q ' "${remote_env[@]}")"
remote_cd="$(printf '%q' "$remote_dir")"

echo "running semantic bundle on Adam"
remote_run="cd $remote_cd && $remote_env_cmd ./benchmarks/scripts/run-semantic-cache-benchmark-bundle.sh"
"${ssh_cmd[@]}" "$ADAM_HOST" "bash -lc $(printf '%q' "$remote_run")"

if [[ "${FETCH_RESULTS:-1}" == "1" ]]; then
  mkdir -p "$local_out"
  echo "fetching artifacts to $local_out"
  "${rsync_cmd[@]}" "$ADAM_HOST:$remote_dir/$remote_out/" "$local_out/"
fi

cat <<EOF
Adam semantic benchmark complete:
  remote: $ADAM_HOST:$remote_dir/$remote_out
  local:  $local_out
EOF
