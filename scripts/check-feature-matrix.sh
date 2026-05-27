#!/usr/bin/env bash
# Compile the public feature surfaces that define the 0.1.0 crate contract.

set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

cargo check -p shardmap --no-default-features --features embedded
cargo check -p shardmap --no-default-features --features sharded
cargo check -p shardmap --no-default-features --features redis
cargo check -p shardmap --no-default-features --features redis-server

cargo check -p shardcache --no-default-features --features server
cargo check -p shardcache --no-default-features --features redis-server
cargo check -p shardcache --features redis

cargo check -p shardcache-redis --no-default-features --features redis
cargo check -p shardcache-redis --all-features

if cargo tree -p shardmap --no-default-features --features embedded | grep -q 'shardcache-redis'; then
  echo "embedded-only shardmap unexpectedly depends on shardcache-redis" >&2
  exit 1
fi
