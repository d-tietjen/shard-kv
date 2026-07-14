#!/usr/bin/env bash
# Compile the public feature surfaces that define the current crate contract.

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
cargo check -p shardmap --no-default-features --features kv-overflow
cargo check -p shardmap --no-default-features --features kv-overflow-redis
cargo check -p shardmap --no-default-features --features scnp-tls

cargo check -p shardcache --no-default-features --features server
cargo check -p shardcache --no-default-features --features server,redis-functions
cargo check -p shardcache --no-default-features --features server,redis-modules
cargo check -p shardcache --no-default-features --features server,redis-modules-all
cargo check -p shardcache --no-default-features --features redis-server
cargo check -p shardcache --features redis
cargo check -p shardcache --no-default-features --features kv-overflow
cargo check -p shardcache --no-default-features --features kv-overflow-redis
cargo check -p shardcache --no-default-features --features scnp-tls

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
done
