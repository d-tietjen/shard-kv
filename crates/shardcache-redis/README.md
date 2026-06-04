# shardcache-redis

Redis/Valkey command compatibility layer for `shardcache`.

This crate owns the Redis-only command families and Redis object storage
implementation. Its source root is `crates/shardcache-redis/src`.

## Install

Use the published crate from crates.io:

```toml
[dependencies]
shardcache-redis = "0.3.2"
```

From a workspace checkout, use a path dependency:

```toml
[dependencies]
shardcache-redis = { path = "crates/shardcache-redis" }
```

The crate defaults to `redis`, so `cargo check -p shardcache-redis` exercises
the Redis compatibility implementation.

Compatibility status is generated from the live command benchmark registry in
[`docs/REDIS_COMPATIBILITY.md`](../../docs/REDIS_COMPATIBILITY.md). The 0.3.x
target covers the Redis 5.0.14 command table plus Redis 6, 7, 8, and
feature-gated module command families tracked in the manifest, with standalone
expected-error behavior and semantic caveats documented there.

The intended long-term direction is to keep narrowing the remaining internal
extension points while preserving this crate as the public Redis-compatible
command surface for `shardcache`.

## Example Commands

Run the source-built server with Redis compatibility enabled, then use any
RESP client:

```bash
cargo run -p shardcache -- --bind-addr 127.0.0.1:6380 --disable-persistence

redis-cli -p 6380 HSET user:42 name Ada plan pro
redis-cli -p 6380 HGETALL user:42
redis-cli -p 6380 EXPIRE user:42 60
redis-cli -p 6380 TTL user:42
```

The Rust SCNP client can also send Redis command families without constructing
RESP frames:

```rust,no_run
use shardcache_client_rs::{RedisResponse, ShardCacheClient};

fn main() -> shardcache_client_rs::Result<()> {
    let mut client = ShardCacheClient::connect("127.0.0.1:6380")?;

    client.redis().hset("user:42", "name", "Ada")?;
    let response = client.redis().hgetall("user:42")?;
    assert!(matches!(response, RedisResponse::Array(_)));

    Ok(())
}
```
