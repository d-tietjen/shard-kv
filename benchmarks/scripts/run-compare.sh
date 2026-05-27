#!/usr/bin/env bash
# Focused two-backend (or N-backend) saturation comparison.
#
#   BACKENDS=fc-embed,dashmap ./scripts/run-compare.sh
#   BACKENDS=fc-server-resp,redis ADDR=127.0.0.1:6379 ./scripts/run-compare.sh
#   BACKENDS=fc-server-scnp,fc-server-resp ADDR=127.0.0.1:6383 ./scripts/run-compare.sh
#
# SERVER_CPUSET=0-3 pins the server via taskset (Linux).
# PIPELINE_DEPTH=16 enables network request pipelining where supported.
# No CSV writes by default; prints only to stdout.

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$root/.." && pwd)"
# shellcheck source=_lib.sh
. "$here/_lib.sh"

backends="${BACKENDS:?set BACKENDS=a,b (comma-separated)}"
addr="${ADDR:-}"

cd "$ws_root"
cargo build --release -p shardcache-benchmarks
report_pinning

addr_arg=()
if [[ -n "$addr" ]]; then
  addr_arg=(--addr "$addr")
fi

"$ws_root/target/release/saturation" \
  --backends "$backends" \
  --value-size "${VALUE_SIZE:-512}" \
  --mix "${MIX:-80-20}" \
  --vcpu-budget "${VCPU_BUDGET:-4}" \
  --clients "${CLIENTS:-16}" \
  --pipeline-depth "${PIPELINE_DEPTH:-1}" \
  --key-count "${KEY_COUNT:-100000}" \
  --warmup "${WARMUP:-2}" \
  --duration "${DURATION:-10}" \
  ${addr_arg[@]+"${addr_arg[@]}"}
