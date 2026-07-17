# shardcache

`shardcache` is the Redis/Valkey-style server for `shard-kv`. It is built on
`shardmap` and exposes RESP plus the native SCNP protocol used by
`shardcache-client-rs`.

## Run From Source

```sh
cargo run -p shardcache -- --bind-addr 127.0.0.1:6380 --disable-persistence
```

Install the binary from crates.io:

```sh
cargo install shardcache --version 0.7.0 --locked
```

Install the binary from a checkout:

```sh
cargo install --path crates/shardcache --locked
```

Run with a config file:

```sh
shardcache --config shardcache.toml.example
```

## Basic Usage

The server accepts RESP clients on the configured bind address. For a quick
local check, start the server and use `redis-cli` or `valkey-cli`:

```sh
redis-cli -p 6380 SET user:42 ready
redis-cli -p 6380 GET user:42
```

The same server also exposes the native SCNP path used by
`shardcache-client-rs`:

```rust,no_run
use shardcache_client_rs::ShardCacheClient;

fn main() -> shardcache_client_rs::Result<()> {
    let mut client = ShardCacheClient::connect("127.0.0.1:6380")?;
    let mut value = Vec::new();

    client.set(b"user:42", b"ready")?;
    assert!(client.get_into(b"user:42", &mut value)?);
    assert_eq!(value, b"ready");
    Ok(())
}
```

## Endpoint Topology

`server_endpoint_mode = "fanout"` is the default. It binds one public RESP/SCNP
listener on `bind_addr` and routes each request internally.

Use `server_endpoint_mode = "direct_shard"` when shard-aware clients should
also connect to one shard-owned port per shard. Direct ports start at
`bind_addr + 1` unless `SHARDCACHE_DIRECT_SHARD_BASE_PORT` sets the first direct
port:

```toml
bind_addr = "127.0.0.1:6380"
shard_count = 4
server_endpoint_mode = "direct_shard"
```

With that config, the fanout listener is `127.0.0.1:6380` and the default
direct shard listeners are `127.0.0.1:6381` through `127.0.0.1:6384`.

Semantic cache commands are available through RESP when the server is used as a
networked semantic cache:

```text
SEMANTIC.SET prompt:refunds <answer-bytes> <embedding-f32le> <governance-bytes>
SEMANTIC.SEARCH <embedding-f32le> 0.75
```

## Docker

The repository Dockerfile builds the same `shardcache` binary into a local
image named `shardcache:local`.

```sh
docker compose up --build shardcache
```

See [`../../docs/SHARDCACHE_DOCKER.md`](../../docs/SHARDCACHE_DOCKER.md) for
the Docker/server runbook.

## Features

| Feature | Default | Purpose |
| --- | --- | --- |
| `redis-server` | Yes | RESP/SCNP server with Redis/Valkey compatibility. |
| `server` | No | Lean RESP/SCNP server without the Redis compatibility catalog. |
| `redis` | No | Redis/Valkey object and command behavior without the server feature. |
| `redis-functions` | Via `redis-server` | Redis 7 `FUNCTION`/`FCALL` compatibility stubs with an empty function registry. |
| `redis-modules` | Via `redis-server` | Redis `MODULE` compatibility stubs with an empty module registry and disabled loading. |
| `redis-modules-all` | No | Aggregate Redis Modules compatibility facades, concrete command discovery metadata, and embedded APIs; individual `redis-module-*` flags can enable one module family at a time. |
| `monoio` | No | Linux-only transport option for server experiments. |
| `object-overflow` | No | Filesystem-backed cold-value overflow for server storage. |
| `object-overflow-s3` | No | S3/RustFS-compatible object overflow adapter. |
| `kv-overflow` | No | Shardcache SCNP overflow-replica role and embedded primary support. |
| `kv-overflow-redis` | No | Redis/Valkey-compatible adapter for an embedded KV overflow primary. |
| `scnp-tls` | No | Rustls TLS 1.3 and mTLS for shard-owned SCNP overflow connections. |

Build the lean server with:

```sh
cargo run -p shardcache --no-default-features --features server -- \
  --bind-addr 127.0.0.1:6380 \
  --disable-persistence
```

Build an authenticated overflow replica with native SCNP TLS support using
`--features redis-server,kv-overflow,scnp-tls`. Configure the certificate,
client authentication, direct shard listeners, and encrypted persistence in
`shardcache.toml.example`; the complete deployment contract is in
[`../../docs/KV_OVERFLOW.md`](../../docs/KV_OVERFLOW.md).

## Operational Notes

- `--server-mode direct` requires `--disable-persistence`.
- The default Docker command uses direct, in-memory mode.
- Use a stable `--data-dir` or `--config` path for persistent source-built deployments.
- Rust clients should depend on `shardcache-client-rs`; server deployments use
  this crate's `shardcache` binary.
