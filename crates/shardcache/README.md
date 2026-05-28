# shardcache

`shardcache` is the source-only Redis/Valkey-style server for `shard-kv`. It is
built on `shardmap` and exposes RESP plus the native SCNP protocol used by
`shardcache-client-rs`.

`shardcache` is not published to crates.io in the 0.1.x release line. Build it
from this repository for local or private deployments.

## Run From Source

```sh
cargo run -p shardcache -- --bind-addr 127.0.0.1:6380 --disable-persistence
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
| `monoio` | No | Linux-only transport option for server experiments. |

Build the lean server with:

```sh
cargo run -p shardcache --no-default-features --features server -- \
  --bind-addr 127.0.0.1:6380 \
  --disable-persistence
```

## Operational Notes

- `--server-mode direct` requires `--disable-persistence`.
- The default Docker command uses direct, in-memory mode.
- Use a stable `--data-dir` or `--config` path for persistent source-built deployments.
- Publishable Rust clients should depend on `shardcache-client-rs`, not this source-only package.
