#!/usr/bin/env bash
# Client-pressure sweep at a fixed workload.
#
# This answers: with a fixed server CPU budget, how does throughput change as
# we increase the number of parallel benchmark clients / TCP connections?
#
# Defaults model the single-core sanity check:
#   VCPU_BUDGET=1
#   CLIENT_COUNTS="1 4 16 64 256"
#
# Examples:
#   BACKENDS=fc-embed,dashmap ./scripts/run-client-sweep.sh
#   BACKENDS=fc-server-resp,redis ADDR=127.0.0.1:6383 SERVER_PID=$pid ./scripts/run-client-sweep.sh
#   BACKENDS=fc-server-scnp-direct ADDR=127.0.0.1:6500 SERVER_PID=$pid PIPELINE_DEPTHS="1 64 128 256" ./scripts/run-client-sweep.sh
#
# SERVER_CPUSET=0 is reported when set, but this script does not launch a
# server itself. Start/pin the server first and pass SERVER_PID for server CPU
# accounting.

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$root/.." && pwd)"
# shellcheck source=_lib.sh
. "$here/_lib.sh"

backends="${BACKENDS:-fc-embed,dashmap,moka,lru,rwlock-hashmap}"
addr="${ADDR:-}"
server_pid="${SERVER_PID:-}"
client_counts="${CLIENT_COUNTS:-1 4 16 64 256}"
pipeline_depths="${PIPELINE_DEPTHS:-${PIPELINE_DEPTH:-1}}"

cd "$ws_root"
cargo build --release -p shardcache-benchmarks
report_pinning

addr_arg=()
if [[ -n "$addr" ]]; then
  addr_arg=(--addr "$addr")
fi

server_pid_arg=()
if [[ -n "$server_pid" ]]; then
  server_pid_arg=(--server-pid "$server_pid")
fi

ts="$(date +%Y%m%d_%H%M%S)"
out="${CSV:-$root/results/client_sweep_${ts}.csv}"
mkdir -p "$(dirname "$out")"

for clients in $client_counts; do
  for pipeline_depth in $pipeline_depths; do
    "$ws_root/target/release/saturation" \
      --backends "$backends" \
      --value-size "${VALUE_SIZE:-64}" \
      --mix "${MIX:-80-20}" \
      --key-pattern "${KEY_PATTERN:-point}" \
      --vcpu-budget "${VCPU_BUDGET:-1}" \
      ${SCNP_SHARDS:+--scnp-shards "$SCNP_SHARDS"} \
      --clients "$clients" \
      --pipeline-depth "$pipeline_depth" \
      --key-count "${KEY_COUNT:-100000}" \
      --warmup "${WARMUP:-2}" \
      --duration "${DURATION:-10}" \
      --latency-sample-rate "${LATENCY_SAMPLE_RATE:-0}" \
      --csv "$out" \
      ${addr_arg[@]+"${addr_arg[@]}"} \
      ${server_pid_arg[@]+"${server_pid_arg[@]}"}
  done
done

echo "wrote $out"
