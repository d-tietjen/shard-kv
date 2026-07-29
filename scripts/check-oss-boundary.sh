#!/usr/bin/env bash
# Keep proprietary availability implementations out of the public workspace.

set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

failed=0

for path in \
  benchmarks/src/bin/active_sync_conflict_cost.rs \
  benchmarks/src/bin/active_sync_cost.rs \
  benchmarks/src/bin/active_sync_tls_latency.rs \
  benchmarks/src/bin/replication_cost.rs \
  benchmarks/src/bin/replication_tcp_cost.rs \
  crates/deterministic-test-env \
  crates/shardcache-formal/src/active_sync_conflict.rs \
  crates/shardmap-blossom-bridge \
  crates/shardmap/examples/active_sync.rs \
  crates/shardmap/src/active_sync.rs \
  crates/shardmap/src/active_sync \
  crates/shardmap/src/replication.rs \
  crates/shardmap/src/replication \
  docs/ACTIVE_ACTIVE_REPLICATION.md
do
  if [[ -e "$path" ]]; then
    echo "private availability path must not exist in the OSS repository: $path" >&2
    failed=1
  fi
done

manifest_files=(Cargo.toml Cargo.lock)
while IFS= read -r manifest; do
  if [[ "$manifest" != "Cargo.toml" ]]; then
    manifest_files+=("$manifest")
  fi
done < <(
  git ls-files --cached --others --exclude-standard |
    rg '(^|/)Cargo\.toml$'
)

manifest_pattern='blossom-consensus|shard-kv-private|shard_kv_private|active-sync|active-active|active-passive|high-availability|(^|[^[:alnum:]_])fcrp([^[:alnum:]_]|$)'
if rg -n -i "$manifest_pattern" "${manifest_files[@]}"; then
  echo "OSS Cargo metadata contains a private availability package, dependency, or feature" >&2
  failed=1
fi

source_pattern='ActiveShardMap|ActiveSync|ActiveActive|ActivePassive|HighAvailability|ConflictOrderer|HaReplication|HaService|OpenRaft|BlossomConsensus|blossom_consensus|shard_kv_private|Replication(Config|Runtime|Transport|Backlog|Batcher)|mod[[:space:]]+replication'
if rg -n "$source_pattern" crates --glob '*.rs'; then
  echo "OSS Rust source contains a private availability implementation surface" >&2
  failed=1
fi

public_history_pattern='blossom|openraft|active-sync|active-active|active-passive|shard-kv-private|shardmap-blossom-bridge|(^|[^[:alnum:]_])fcrp([^[:alnum:]_]|$)'
if rg -n -i "$public_history_pattern" CHANGELOG.md; then
  echo "OSS release notes expose private implementation names" >&2
  failed=1
fi

if ((failed != 0)); then
  exit 1
fi

echo "OSS/private availability boundary is intact"
