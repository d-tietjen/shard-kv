# shardcache-client-rs

Blocking Rust client for shardcache's native SCNP protocol.

SCNP is the native TCP protocol used by `shardcache` for Rust clients that want
lower overhead than RESP and, when configured, direct routing to shard-owned
server ports. This crate is intentionally small and synchronous: one client owns
one or more TCP connections and is meant to be used directly from worker
threads.

This is one of the three publishable crates in the 0.8.x release line alongside
`shardmap` and `shardcache`.

## Install

Use the published crate from crates.io:

```toml
[dependencies]
shardcache-client-rs = "0.9.1"
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

## Typed Vector Commands

Enable `vector` for Object RAG clients that need native SCNP vector operations
without enabling the complete optional Redis command API:

```toml
[dependencies]
shardcache-client-rs = { version = "0.9.1", features = ["vector"] }
```

`VSIM` always requests `WITHSCORES WITHATTRIBS`, giving every successful query
one stable typed result shape. Add `.with_governance(true)` on 0.8.0 servers to
include opaque governance metadata as a fourth field. FP32 vectors are encoded
directly as little-endian binary values. Response bodies and result counts are
checked before allocation.

```rust,no_run
use std::time::Duration;
use shardcache_client_rs::{ShardCacheClient, VAddOptions, VSimOptions};

fn main() -> shardcache_client_rs::Result<()> {
    let timeout = Duration::from_secs(2);
    let mut client = ShardCacheClient::connect_with_timeouts_and_auth(
        "127.0.0.1:6380",
        timeout,
        timeout,
        std::env::var("SHARDCACHE_TOKEN").ok().as_deref().map(str::as_bytes),
    )?;

    client.ping()?;
    client.vadd(
        b"objects",
        b"doc:42",
        &[0.25, 0.5, 0.75],
        VAddOptions::new()
            .attributes(br#"{"source":"catalog"}"#)
            .governance_metadata(b"tenant=acme;classification=private"),
    )?;
    let matches = client.vsim(
        b"objects",
        &[0.25, 0.5, 0.75],
        VSimOptions::new()
            .count(10)
            .ef_search(64)
            .with_governance(true),
    )?;
    assert_eq!(matches[0].element, b"doc:42");
    assert_eq!(
        matches[0].governance.as_deref(),
        Some(b"tenant=acme;classification=private".as_slice())
    );
    assert!(client.vrem(b"objects", b"doc:42")?);
    Ok(())
}
```

The same methods are available on `ShardCacheDirectClient` and
`ShardCacheDirectShardClient`. Vector sets are pinned to direct shard 0; the
automatic direct client selects that connection regardless of the ordinary key
hash, and an individual shard client rejects vector calls unless it is attached
to shard 0.

Governance metadata is opaque and limited to 64 KiB per embedding. It is
distinct from filterable JSON attributes and is preserved by vector updates,
TTL changes, snapshots, key lifecycle commands, and native read-replica
replication. Shardcache returns the label with each typed match but does not
interpret authorization policy; applications must authorize the label before
releasing the corresponding document or generated context.

Leave `with_governance` disabled while a client can reach a 0.7.1 server; the
0.7.1 vector parser does not recognize the new option. Governed `VADD` requests
likewise require a 0.8.0 server.

Add the `tls` feature and use `connect_with_timeouts_auth_and_tls` on a fanout
client or router when the connection crosses a trust boundary. Token auth and
operation deadlines apply before any typed vector request is sent. The 0.8.0
client accepts native 0.8.0 responses and the RESP-enveloped vector responses
returned by 0.7.1 servers for rolling upgrades.

## Native Redis Commands

Enable the optional `redis` feature to use native SCNP Redis commands. Commands
with compact opcodes are sent as an SCNP opcode plus compact binary arguments.
Other Redis command names use the SCNP command-name wrapper, so the fanout
client can still cover the full Redis-compatible command surface without
building RESP request frames in user code.

```toml
[dependencies]
shardcache-client-rs = { version = "0.9.1", features = ["redis"] }
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
or `client.redis().execute(...)` as lower-level escape hatches when you need
borrowed command and argv bytes. `redis_command_by_name` and the first-party
Redis namespace use the native command-name SCNP wrapper; `redis_command` is
the explicit compact-opcode API.

`RedisResponse` decodes recursive native SCNP values, including RESP2/RESP3
arrays, maps, sets, pushes, attributes, and scalar types. The raw RESP command
wrapper remains available through `redis_resp_command` for compatibility tools.

Direct shard clients route keyed compact-opcode commands to their owning shard
when all keys belong to one shard. Command-name native routing and commands
that require all shards should use `ShardCacheClient` on the fanout listener.

### Native Redis wire format

Command-name APIs require a server revision that supports opcode `244`. For
older servers, select `redis_resp_command` explicitly. Compact-opcode APIs
remain available through `redis_command` and direct shard clients.

Command availability follows the selected server and storage mode. Native
requests use the engine's normal command execution on engine-backed servers and
the owning worker on routed embedded listeners. Commands such as `MGET` that
require embedded Redis storage retain that requirement. Routed embedded
transactions remain on the public connection and wait for earlier worker
requests before changing transaction state.

Native requests use the SCNP v2 header with opcode `244` and flag `0x08`.
After any flagged routing fields, the body contains a little-endian `u32`
command-name length, the command bytes, and compact argv: an unsigned `u32`
base-128 varint count, then a varint byte length and raw bytes for each argument.
Keys and values are binary-safe. The encoded request must fit the header's
`u32` body length.

Owner-routed embedded listeners retain their restriction on the legacy RESP
wrapper. Use native command-name requests on those listeners; the wrapper
remains available on server modes that accept it, including ordinary multiworker
embedded listeners.

Simple replies retain existing SCNP statuses: OK `0`, null `1`, error `2`,
integer `3`, blob `4`, boolean `5`, and float `7`. Recursive replies use status
`8` with the normal `u32` body length and one native value:

| Tag | Value and following bytes |
| --- | --- |
| 0 | Simple string: `u32le` byte length and bytes; `OK` becomes `RedisResponse::Ok`. |
| 1 | Blob string: `u32le` byte length and bytes. |
| 2 | Signed integer: `i64le`. |
| 3 | Array: `u32le` count and recursive values. |
| 4 | Map: `u32le` pair count and recursive key/value pairs. |
| 5 | Set: `u32le` count and recursive values. |
| 6 | Push: `u32le` count and recursive values. |
| 7 | Null: no following bytes. |
| 8 | Boolean: one byte, exactly `0` or `1`. |
| 9 | Double: IEEE 754 `f64le`. |
| 10 | Big number: `u32le` byte length and decimal bytes. |
| 11 | Verbatim: length-prefixed format bytes, then length-prefixed value bytes. |
| 12 | Attribute: `u32le` pair count, recursive pairs, then one data value. |
| 13 | Error: `u32le` byte length and message bytes. |

`MULTI`, queued commands, `EXEC`, `DISCARD`, and watch controls use the same
transaction state and shard checks as RESP. An error inside an `EXEC` array
remains `RedisResponse::Error`; a top-level error returns `Err`. Pipelines may
mix native command-name, compact-opcode, and explicit RESP wrappers when the
matching begin/finish APIs are used.

The client and server bound native bodies to 256 MiB, each collection to
65,536 entries, nesting to 128 levels, and each recursive response to 1,048,576
values. The decoder rejects truncated fields, invalid tags/booleans, impossible
counts, and trailing bytes before accepting a value. A fully consumed invalid
body leaves the next response aligned. An oversized unread body, invalid
header, or truncated response makes the connection unusable; reconnect before
sending more commands.

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
- typed `PING`, `VADD`, `VSIM`, and `VREM` with the `vector` feature

With the `redis` feature enabled, it also supports the Redis-compatible command
surface through `client.redis()` on the fanout client. The low-level
`redis_command` and `redis_command_by_name` helpers remain available as escape
hatches.

The crate is organized internally with one command module per SCNP command so
future commands can be added without growing the transport and routing code.

## License

Licensed under Apache-2.0.
