# fcnp-client-rs

Blocking Rust client for fast-cache's native FCNP protocol.

FCNP is the native TCP protocol used by `fast-cache` for Rust clients that want
lower overhead than RESP and, when configured, direct routing to shard-owned
server ports. This crate is intentionally small and synchronous: one client owns
one or more TCP connections and is meant to be used directly from worker
threads.

## Install

```toml
[dependencies]
fcnp-client-rs = "0.2"
```

## Server Shape

The client expects a running `fast-cache-server` built with the `server`
feature:

```bash
cargo install fast-cache --features server --locked
fast-cache-server --bind-addr 127.0.0.1:6380 --shard-count 4
```

For direct shard routing, enable shard-owned FCNP listeners on the server:

```bash
FAST_CACHE_DIRECT_SHARD_PORTS=1 \
FAST_CACHE_DIRECT_SHARD_BASE_PORT=6501 \
fast-cache-server --bind-addr 127.0.0.1:6380 --shard-count 4
```

With that layout, the fanout listener is `127.0.0.1:6380` and direct shard
listeners are `127.0.0.1:6501` through `127.0.0.1:6504`.

## Fanout Client

Use `FcnpClient` when you want one connection to the ordinary server listener.
The server routes each request internally.

```rust,no_run
use fcnp_client_rs::FcnpClient;

fn main() -> fcnp_client_rs::Result<()> {
    let mut client = FcnpClient::connect("127.0.0.1:6380")?;
    let mut value = Vec::new();

    client.set(b"user:42", b"ready")?;
    let hit = client.get_into(b"user:42", &mut value)?;

    assert!(hit);
    assert_eq!(value, b"ready");
    Ok(())
}
```

## Direct Shard Client

Use `FcnpDirectClient` when the server exposes one FCNP port per shard and the
client should route keys directly. The address is the first direct shard port,
not the fanout port.

```rust,no_run
use fcnp_client_rs::FcnpDirectClient;

fn main() -> fcnp_client_rs::Result<()> {
    let mut client = FcnpDirectClient::connect("127.0.0.1:6501", 4)?;
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

Use `FcnpDirectRouter` and `FcnpDirectShardClient` when your application already
partitions work by shard. A shard client refuses keys that route to a different
shard, which helps catch routing mistakes during development.

```rust,no_run
use fcnp_client_rs::FcnpDirectRouter;

fn main() -> fcnp_client_rs::Result<()> {
    let router = FcnpDirectRouter::new("127.0.0.1:6501", 4)?;
    let mut shard0 = router.connect_shard(0)?;

    if router.route_key(b"user:42").shard_id == shard0.shard_id() {
        shard0.set(b"user:42", b"ready")?;
    }

    Ok(())
}
```

## Pipelining

`FcnpClient` and `FcnpDirectShardClient` expose ordered pipelining helpers.
Write requests with `begin_pipeline_*`, flush once, then read responses in the
same order.

```rust,no_run
use fcnp_client_rs::FcnpClient;

fn main() -> fcnp_client_rs::Result<()> {
    let mut client = FcnpClient::connect("127.0.0.1:6380")?;

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

## Optimized Redis Commands

Enable the optional `redis` feature to use the compact opcode Redis wrapper.
This sends the Redis command as an FCNP opcode plus compact binary arguments,
instead of sending the command name in the generic `RESP` wrapper.

```toml
[dependencies]
fcnp-client-rs = { version = "0.2", features = ["redis"] }
```

```rust,ignore
use fcnp_client_rs::{FcnpClient, RedisCommandKind, RedisResponse};

fn main() -> fcnp_client_rs::Result<()> {
    let mut client = FcnpClient::connect("127.0.0.1:6380")?;

    let response = client.redis_command(
        RedisCommandKind::HGetAll,
        &[b"user:42"],
    )?;

    if let RedisResponse::Array(fields) = response {
        assert!(!fields.is_empty());
    }

    Ok(())
}
```

`RedisCommandKind` covers the full Redis-compatible opcode table exposed by the
server. Direct shard clients route keyed commands to their owning shard when
all keys belong to one shard; commands that require all shards should use
`FcnpClient` on the fanout listener.

## Routing Helpers

The crate exposes the same routing helpers used by the direct clients:

- `hash_key`
- `hash_key_tag`
- `shard_index`
- `FcnpDirectRouter`
- `FcnpRouteMode`

`FcnpRouteMode::FullKey` is the default. `FcnpRouteMode::SessionPrefix` routes
keys shaped like `s:<session>:c:<chunk>` by the session prefix while preserving
the full-key lookup hash.

## TCP Buffer Tuning

On Unix platforms the client can request larger socket buffers by setting:

```bash
FCNP_CLIENT_TCP_BUFFER_BYTES=4194304
```

`FAST_CACHE_TCP_BUFFER_BYTES` is also accepted for deployments that use one
shared environment knob for the server and client.

## Current Command Surface

This release supports the native hot-path commands currently implemented by
`fast-cache`:

- `GET`
- `SET`
- `SETEX` with millisecond TTLs
- `GETEX` with millisecond TTLs
- `DEL`
- `EXISTS`
- `TTL`, returning Redis-compatible seconds
- `EXPIRE` with millisecond TTLs

With the `redis` feature enabled, it also supports the full Redis-compatible
command opcode table through `redis_command` and `redis_command_by_name`.

The crate is organized internally with one command module per FCNP command so
future commands can be added without growing the transport and routing code.

## License

Licensed under Apache-2.0.
