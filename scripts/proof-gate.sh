#!/usr/bin/env bash
# Tiered local proof gates for contributors and release prep.
#
# Usage:
#   ./scripts/proof-gate.sh quick
#   ./scripts/proof-gate.sh redis
#   ./scripts/proof-gate.sh release

set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

tier="${1:-quick}"

quick() {
  ./scripts/check-tls-dependency-policy.sh
  cargo fmt --all -- --check
  cargo test -p shardcache-benchmarks --bin redis_command_matrix
  cargo test -p shardcache-benchmarks --bin redis_command_manifest
  cargo test -p shardcache-benchmarks --bin redis_command_report
  cargo test -p shardcache-benchmarks redis_command_cases::tests
  ./scripts/check-redis-compatibility-doc.sh
  ./scripts/check-feature-matrix.sh
  ./scripts/check-publish-set.sh
  ./scripts/check-publish-artifacts.sh
  git diff --check
}

redis() {
  quick
  cargo test -p shardmap --features redis
  cargo test -p shardmap --features redis,server raw_resp_
  SHARDCACHE_COMPAT_SERVER_BIN="${SHARDCACHE_COMPAT_SERVER_BIN:-redis-server}" \
    cargo test -p shardmap --features redis-server \
    --test redis_compat_differential_test -- --nocapture
}

release() {
  redis
  cargo test --workspace
  cargo test -p shardmap --features unsafe
  cargo test -p shardcache-formal
  cargo doc -p shardmap --no-deps --all-features
  cargo doc -p shardcache --no-deps --all-features
  cargo doc -p shardcache-client-rs --no-deps
}

case "$tier" in
  quick)
    quick
    ;;
  redis)
    redis
    ;;
  release)
    release
    ;;
  *)
    echo "unknown proof tier: $tier" >&2
    echo "expected one of: quick, redis, release" >&2
    exit 2
    ;;
esac
