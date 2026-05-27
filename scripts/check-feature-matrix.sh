#!/usr/bin/env bash
# Compile the public feature surfaces that define the 0.2.0 crate contract.

set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

cargo check -p fast-cache-core --no-default-features --features embedded
cargo check -p fast-cache-core --no-default-features --features sharded
cargo check -p fast-cache-core --no-default-features --features redis
cargo check -p fast-cache-core --no-default-features --features redis-server

cargo check -p fast-cache --no-default-features --features embedded
cargo check -p fast-cache --no-default-features --features sharded
cargo check -p fast-cache --no-default-features --features redis
cargo check -p fast-cache --no-default-features --features server
cargo check -p fast-cache --no-default-features --features redis-server

cargo check -p fast-cache-redis --no-default-features --features redis
cargo check -p fast-cache-redis --all-features

if cargo tree -p fast-cache --no-default-features --features embedded | grep -q 'fast-cache-redis'; then
  echo "embedded-only fast-cache unexpectedly depends on fast-cache-redis" >&2
  exit 1
fi

if cargo tree -p fast-cache-core --no-default-features --features embedded | grep -q 'fast-cache-redis'; then
  echo "embedded-only fast-cache-core unexpectedly depends on fast-cache-redis" >&2
  exit 1
fi
