#!/usr/bin/env bash
# run-resp-saturation.sh — fair, saturating head-to-head: shardcache vs Redis.
#
# Fairness model (the things the old matrix got wrong):
#   * EQUAL CPU: both servers run on the same cpuset (SERVER_CORES). One core
#     each = per-core efficiency; N cores = scaling. shard-count defaults to the
#     server core count so shardcache gets one shard per core.
#   * ISOLATED CLIENT: the load generator runs on a DISJOINT cpuset
#     (CLIENT_CORES) so it never steals CPU from the server under test.
#   * ONE SERVER AT A TIME: the other server is stopped, so it cannot contend
#     for cores, cache, or memory bandwidth.
#   * CLIENT NEVER THE BOTTLENECK: resp_blast pre-encodes every request before
#     the timed window (zero runtime command generation) and runs one pinned
#     thread per connection. The printed thread_ops[min..max] must stay tight —
#     if it does, the client had headroom and the number is the server ceiling.
#
# Build the load generator first (linux container arch):
#   rm -f target-linux/release/resp_blast \
#         target-linux/release/deps/resp_blast-* \
#         target-linux/release/.fingerprint/shardcache-benchmarks-*
#   docker run --rm -v "$PWD":/app -w /app -e CARGO_TARGET_DIR=/app/target-linux \
#     rust:1.90-slim-bookworm cargo build --release -p shardcache-benchmarks --bin resp_blast
#
# Usage:
#   SERVER_CORES=0   CLIENT_CORES=2-9 ./run-resp-saturation.sh   # 1:1 per-core
#   SERVER_CORES=0-3 CLIENT_CORES=4-9 ./run-resp-saturation.sh   # 4-core scaling
set -euo pipefail
cd "$(dirname "$0")/../.."

SERVER_CORES="${SERVER_CORES:-0}"
CLIENT_CORES="${CLIENT_CORES:-2-9}"
NETWORK="${NETWORK:-bridge}"
WARMUP="${WARMUP:-2}"
DURATION="${DURATION:-5}"
CLIENTS="${CLIENTS:-16}"
PIPELINES="${PIPELINES:-1 8 32}"

# Each line: label|argv|populate(';'-separated setup commands)
COMMANDS_DEFAULT="\
GET|GET k|SET k v
LPOS|LPOS lb b RANK 1 COUNT 0|RPUSH lb a b c b b
LCS_LEN|LCS sa sb LEN|SET sa hello_world_foobar; SET sb hello_there_foobaz
RESET|RESET|
ACL_WHOAMI|ACL WHOAMI|
OBJECT_ENCODING|OBJECT ENCODING lb|RPUSH lb a b c
CLIENT_GETNAME|CLIENT GETNAME|"
COMMANDS="${COMMANDS:-$COMMANDS_DEFAULT}"

BLAST_HOST_BIN="$PWD/target-linux/release/resp_blast"

core_count() { # "0-3"->4  "0,2,4"->3  "0"->1
  local spec="$1" total=0 part lo hi; local -a parts
  IFS=',' read -ra parts <<<"$spec"
  for part in "${parts[@]}"; do
    if [[ "$part" == *-* ]]; then lo="${part%-*}"; hi="${part#*-}"; total=$((total+hi-lo+1));
    else total=$((total+1)); fi
  done
  echo "$total"
}
SHARD_COUNT="${SHARD_COUNT:-$(core_count "$SERVER_CORES")}"

[[ -x "$BLAST_HOST_BIN" ]] || { echo "missing $BLAST_HOST_BIN — build it (see header)" >&2; exit 1; }
# Stale-binary guard: the docker bind-mount build can skip edits on mtime ties.
if [[ "$BLAST_HOST_BIN" -ot benchmarks/src/bin/resp_blast.rs ]]; then
  echo "WARNING: $BLAST_HOST_BIN older than its source; force-clean and rebuild (see header)" >&2
  exit 1
fi

ip_of() { docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1"; }

blast() { # ip port label argv populate pipeline
  docker run --rm --network "$NETWORK" --cpuset-cpus "$CLIENT_CORES" \
    -v "$BLAST_HOST_BIN:/b:ro" debian:bookworm-slim \
    /b --target "$1:$2" --label "$3" --command "$4" --populate "$5" \
       --clients "$CLIENTS" --pipeline "$6" --warmup "$WARMUP" --duration "$DURATION" \
    | sed 's/\t/  /g'
}

sweep() { # display ip port
  local name="$1" ip="$2" port="$3"
  while IFS='|' read -r label argv populate; do
    [[ -z "${label// }" ]] && continue
    for pd in $PIPELINES; do
      printf '%-8s P%-3s ' "$name" "$pd"
      blast "$ip" "$port" "$label" "$argv" "${populate:- }" "$pd"
    done
  done <<<"$COMMANDS"
}

echo "# resp saturation: SERVER_CORES=$SERVER_CORES (shard-count=$SHARD_COUNT) CLIENT_CORES=$CLIENT_CORES"
echo "# clients=$CLIENTS warmup=${WARMUP}s duration=${DURATION}s pipelines=[$PIPELINES]"
echo

echo "=== shardcache (cpuset $SERVER_CORES, shard-count $SHARD_COUNT) ==="
docker rm -f bench-sc >/dev/null 2>&1 || true
docker run -d --name bench-sc --network "$NETWORK" --cpuset-cpus "$SERVER_CORES" \
  shardcache-bench:latest --bind-addr 0.0.0.0:6380 --disable-persistence \
  --server-mode direct --shard-count "$SHARD_COUNT" >/dev/null
sleep 1
sweep sc "$(ip_of bench-sc)" 6380
docker rm -f bench-sc >/dev/null 2>&1 || true

echo
echo "=== redis (cpuset $SERVER_CORES, single-threaded) ==="
docker rm -f bench-rd >/dev/null 2>&1 || true
docker run -d --name bench-rd --network "$NETWORK" --cpuset-cpus "$SERVER_CORES" \
  redis:7.4-alpine redis-server --save "" --appendonly no --port 6379 >/dev/null
sleep 1
sweep redis "$(ip_of bench-rd)" 6379
docker rm -f bench-rd >/dev/null 2>&1 || true

echo
echo "# done"
