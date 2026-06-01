# shardcache-client-rs

Blocking Rust client for shardcache's native SCNP protocol.

SCNP is the native TCP protocol used by `shardcache` for Rust clients that want
lower overhead than RESP and, when configured, direct routing to shard-owned
server ports. This crate is intentionally small and synchronous: one client owns
one or more TCP connections and is meant to be used directly from worker
threads.

This is one of the two publishable crates in the 0.1.x release line. The
`shardcache` server itself is source-only and is built from the `shard-kv`
repository.

## Install

Use the published crate from crates.io:

```toml
[dependencies]
shardcache-client-rs = "0.1.0"
```

From a workspace checkout, use a path dependency:

```toml
[dependencies]
shardcache-client-rs = { path = "crates/shardcache-client-rs" }
```

## API Shape

- `ShardCacheClient`: one fanout connection to the ordinary server listener.
- `ShardCacheDirectClient`: one connection per direct shard listener, with client-side key routing.
- `ShardCacheDirectRouter`: route keys and connect individual shard-owned clients.
- `client.redis()`: optional first-party Redis command helpers behind the `redis` feature.
- `client.redis().command(...)`: generic Redis argv construction over SCNP.

## Server Shape

The client expects a running `shardcache` built with the `server`
feature:

```bash
cargo run -p shardcache --features server --bin shardcache -- \
  --bind-addr 127.0.0.1:6380 \
  --shard-count 4
```

For direct shard routing, enable shard-owned listeners on the server config:

```toml
bind_addr = "127.0.0.1:6380"
shard_count = 4
server_endpoint_mode = "direct_shard"
```

Then start the server with that config:

```bash
cargo run -p shardcache --features server --bin shardcache -- \
  --config shardcache.toml
```

For scripts and container runs that do not write a config file, the compatibility
environment switch exposes the same direct port shape:

```bash
SHARDCACHE_DIRECT_SHARD_PORTS=1 \
SHARDCACHE_DIRECT_SHARD_BASE_PORT=6501 \
cargo run -p shardcache --features server --bin shardcache -- \
  --bind-addr 127.0.0.1:6380 \
  --shard-count 4
```

With that layout, the fanout listener is `127.0.0.1:6380` and direct shard
listeners are `127.0.0.1:6501` through `127.0.0.1:6504`.

## Fanout Client

Use `ShardCacheClient` when you want one connection to the ordinary server listener.
The server routes each request internally.

```rust,no_run
use shardcache_client_rs::ShardCacheClient;

fn main() -> shardcache_client_rs::Result<()> {
    let mut client = ShardCacheClient::connect("127.0.0.1:6380")?;
    let mut value = Vec::new();

    client.set(b"user:42", b"ready")?;
    let hit = client.get_into(b"user:42", &mut value)?;

    assert!(hit);
    assert_eq!(value, b"ready");
    Ok(())
}
```

## Direct Shard Client

Use `ShardCacheDirectClient` when the server exposes one SCNP port per shard and the
client should route keys directly. The address is the first direct shard port,
not the fanout port.

```rust,no_run
use shardcache_client_rs::ShardCacheDirectClient;

fn main() -> shardcache_client_rs::Result<()> {
    let mut client = ShardCacheDirectClient::connect("127.0.0.1:6501", 4)?;
    let mut value = Vec::new();

    client.set(b"user:42", b"ready")?;
    let hit = client.get_into(b"user:42", &mut value)?;

    assert!(hit);
    assert_eq!(value, b"ready");
    Ok(())
}
```

Direct mode requires the client shard count to match the server shard count.
The shard count must be a non-zero power of two.

## Thread-Per-Shard Client

Use `ShardCacheDirectRouter` and `ShardCacheDirectShardClient` when your application already
partitions work by shard. A shard client refuses keys that route to a different
shard, which helps catch routing mistakes during development.

```rust,no_run
use shardcache_client_rs::ShardCacheDirectRouter;

fn main() -> shardcache_client_rs::Result<()> {
    let router = ShardCacheDirectRouter::new("127.0.0.1:6501", 4)?;
    let mut shard0 = router.connect_shard(0)?;

    if router.route_key(b"user:42").shard_id == shard0.shard_id() {
        shard0.set(b"user:42", b"ready")?;
    }

    Ok(())
}
```

## Pipelining

`ShardCacheClient` and `ShardCacheDirectShardClient` expose ordered pipelining helpers.
Write requests with `begin_pipeline_*`, flush once, then read responses in the
same order.

```rust,no_run
use shardcache_client_rs::ShardCacheClient;

fn main() -> shardcache_client_rs::Result<()> {
    let mut client = ShardCacheClient::connect("127.0.0.1:6380")?;

    client.begin_pipeline_set(b"k1", b"v1")?;
    client.begin_pipeline_set(b"k2", b"v2")?;
    client.flush_pipeline()?;
    client.finish_pipeline_set()?;
    client.finish_pipeline_set()?;

    let mut first = Vec::new();
    let mut second = Vec::new();
    client.begin_pipeline_get(b"k1")?;
    client.begin_pipeline_get(b"k2")?;
    client.flush_pipeline()?;
    assert!(client.finish_pipeline_get_into(&mut first)?);
    assert!(client.finish_pipeline_get_into(&mut second)?);

    Ok(())
}
```

## TTL And Delete

Use `set_ex` for cache entries that should expire on the server, and `del`
when the application invalidates a key explicitly.

```rust,no_run
use shardcache_client_rs::ShardCacheClient;

fn main() -> shardcache_client_rs::Result<()> {
    let mut client = ShardCacheClient::connect("127.0.0.1:6380")?;
    let mut value = Vec::new();

    client.set_ex(b"session:1", b"active", 30_000)?;
    assert!(client.get_into(b"session:1", &mut value)?);
    assert!(client.del(b"session:1")?);
    assert!(!client.get_into(b"session:1", &mut value)?);

    Ok(())
}
```

## Native Redis Commands

Enable the optional `redis` feature to use native SCNP Redis commands. Commands
with compact opcodes are sent as an SCNP opcode plus compact binary arguments.
Other Redis command names use the SCNP command-name wrapper, so the fanout
client can still cover the full Redis-compatible command surface without
building RESP request frames in user code.

```toml
[dependencies]
shardcache-client-rs = { version = "0.1.0", features = ["redis"] }
```

The primary API is the first-party Redis namespace on the client. Common
commands use Redis-client-style typed helpers, while less common commands can
still pass Redis argv through the generic command path.

```rust,ignore
use shardcache_client_rs::{RedisResponse, ShardCacheClient};

fn main() -> shardcache_client_rs::Result<()> {
    let mut client = ShardCacheClient::connect("127.0.0.1:6380")?;

    client.redis().hset("user:42", "name", "Ada")?;
    let response = client.redis().hgetall("user:42")?;

    if let RedisResponse::Array(fields) = response {
        assert!(!fields.is_empty());
    }

    Ok(())
}
```

Commands without compact opcodes still use native SCNP through the same
namespace on `ShardCacheClient`:

```rust,ignore
use shardcache_client_rs::{RedisResponse, ShardCacheClient};

fn main() -> shardcache_client_rs::Result<()> {
    let mut client = ShardCacheClient::connect("127.0.0.1:6380")?;

    let response = client
        .redis()
        .geoadd("places", -122.4194, 37.7749, "san-francisco")?;

    assert!(matches!(response, RedisResponse::Integer(1)));
    Ok(())
}
```

Use `client.redis().command(...)` for redis-rs-style generic command
construction:

```rust,ignore
use shardcache_client_rs::{RedisResponse, ShardCacheClient};

fn main() -> shardcache_client_rs::Result<()> {
    let mut client = ShardCacheClient::connect("127.0.0.1:6380")?;

    let response = client.redis().command("COMMAND").arg("COUNT").query()?;

    assert!(matches!(response, RedisResponse::Integer(_)));
    Ok(())
}
```

Use `redis_command`, `redis_command_by_name`, `client.redis().query_command(...)`,
or `client.redis().execute(...)` as lower-level escape hatches when you
explicitly want the compact-opcode path or already have borrowed command and
argv bytes.

`RedisResponse` decodes RESP2/RESP3 payloads returned by the command-name
wrapper into nested Rust values. Compact opcode commands return native SCNP
statuses directly; if a less-common compact command returns an embedded RESP
payload as `RedisResponse::Value`, call `decode_resp_value()` to decode it.

Direct shard clients route keyed compact-opcode commands to their owning shard
when all keys belong to one shard. Commands without compact opcodes, or commands
that require all shards, should use `ShardCacheClient` on the fanout listener.

## Routing Helpers

The crate exposes the same routing helpers used by the direct clients:

- `hash_key`
- `hash_key_tag`
- `shard_index`
- `ShardCacheDirectRouter`
- `ShardCacheRouteMode`

`ShardCacheRouteMode::FullKey` is the default. `ShardCacheRouteMode::SessionPrefix` routes
keys shaped like `s:<session>:c:<chunk>` by the session prefix while preserving
the full-key lookup hash.

## TCP Buffer Tuning

On Unix platforms the client can request larger socket buffers by setting:

```bash
SCNP_CLIENT_TCP_BUFFER_BYTES=4194304
```

`SHARDCACHE_TCP_BUFFER_BYTES` is also accepted for deployments that use one
shared environment knob for the server and client.

## Current Command Surface

This release supports the native hot-path commands currently implemented by
`shardcache`:

- `GET`
- `SET`
- `SETEX` with millisecond TTLs
- `GETEX` with millisecond TTLs
- `DEL`
- `EXISTS`
- `TTL`, returning Redis-compatible seconds
- `EXPIRE` with millisecond TTLs

With the `redis` feature enabled, it also supports the Redis-compatible command
surface through `client.redis()` on the fanout client. The low-level
`redis_command` and `redis_command_by_name` helpers remain available as escape
hatches.

The crate is organized internally with one command module per SCNP command so
future commands can be added without growing the transport and routing code.

## License

Licensed under Apache-2.0.
