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
  cargo fmt --all -- --check
  cargo test -p fast-cache-benchmarks --bin redis_command_matrix
  cargo test -p fast-cache-benchmarks --bin redis_command_manifest
  cargo test -p fast-cache-benchmarks --bin redis_command_report
  cargo test -p fast-cache-benchmarks redis_command_cases::tests
  ./scripts/check-redis-compatibility-doc.sh
  ./scripts/check-feature-matrix.sh
  git diff --check
}

redis() {
  quick
  cargo test -p fast-cache-core --features redis
  cargo test -p fast-cache-core --features redis,server raw_resp_
  FAST_CACHE_COMPAT_SERVER_BIN="${FAST_CACHE_COMPAT_SERVER_BIN:-redis-server}" \
    cargo test -p fast-cache-core --features redis-server \
    --test redis_compat_differential_test -- --nocapture
}

release() {
  redis
  cargo test --workspace
  cargo test -p fast-cache-core --features unsafe
  cargo test -p fast-cache-formal
  cargo doc -p fast-cache-core --no-deps --all-features
  cargo doc -p fast-cache --no-deps --all-features
  cargo doc -p fast-cache-redis --no-deps --all-features
  cargo doc -p fcnp-client-rs --no-deps
  cargo package -p fcnp-client-rs --locked
  cargo package -p fast-cache-core --locked
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
