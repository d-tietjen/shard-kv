#!/usr/bin/env bash
# Compile the public feature surfaces that define the 0.1.0 crate contract.

set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

cargo check -p shardmap --no-default-features --features embedded
cargo check -p shardmap --no-default-features --features sharded
cargo check -p shardmap --no-default-features --features redis
cargo check -p shardmap --no-default-features --features redis-functions
cargo check -p shardmap --no-default-features --features redis-modules
cargo check -p shardmap --no-default-features --features redis-modules-all
cargo check -p shardmap --no-default-features --features redis-server

cargo check -p shardcache --no-default-features --features server
cargo check -p shardcache --no-default-features --features server,redis-functions
cargo check -p shardcache --no-default-features --features server,redis-modules
cargo check -p shardcache --no-default-features --features server,redis-modules-all
cargo check -p shardcache --no-default-features --features redis-server
cargo check -p shardcache --features redis

cargo check -p shardcache-redis --no-default-features --features redis
cargo check -p shardcache-redis --no-default-features --features redis-functions
cargo check -p shardcache-redis --no-default-features --features redis-modules
cargo check -p shardcache-redis --no-default-features --features redis-modules-all
cargo check -p shardcache-redis --all-features

redis_module_features=(
  redis-module-search
  redis-module-bloom
  redis-module-timeseries
  redis-module-graph
  redis-module-json
  redis-module-ai
  redis-module-gears
  redis-module-cell
  redis-module-neural
  redis-module-tdigest
  redis-module-cthulhu
  redis-module-snowflake
  redis-module-roaring
  redis-module-session-gate
  redis-module-rede
  redis-module-topk
  redis-module-cms
)

for feature in "${redis_module_features[@]}"; do
  cargo check -p shardmap --no-default-features --features "$feature"
  cargo check -p shardcache --no-default-features --features "server,$feature"
  cargo check -p shardcache-redis --no-default-features --features "$feature"
done

if cargo tree -p shardmap --no-default-features --features embedded | grep -q 'shardcache-redis'; then
  echo "embedded-only shardmap unexpectedly depends on shardcache-redis" >&2
  exit 1
fi
