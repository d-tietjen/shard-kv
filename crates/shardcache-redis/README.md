# shardcache-redis

Redis/Valkey compatibility source package for `shardcache`.

This crate owns the Redis-only command families and Redis object storage
implementation. Its source root is `crates/shardcache-redis/src`.

The crate defaults to `redis`, so `cargo check -p shardcache-redis`
exercises the Redis compatibility source instead of compiling an empty marker
package.

Compatibility status is generated from the live command benchmark registry in
[`docs/REDIS_COMPATIBILITY.md`](../../docs/REDIS_COMPATIBILITY.md). The 0.1.0
target covers the Redis 5.0.14 command table plus selected later cache-command
extensions, with standalone expected-error behavior and semantic caveats
documented there.

The intended long-term direction is to narrow the remaining internal extension
points until this package can become an ordinary optional dependency instead of
a source-owned compatibility package. During that transition, core still
includes these files by path behind its `redis` feature, but the files no
longer live inside the core crate tree.

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
