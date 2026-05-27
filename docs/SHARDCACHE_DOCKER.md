# shardcache Docker

This is the container runbook for the source-only `shardcache` server. The
repository builds a local image named `shardcache:local`; it does not publish a
Docker Hub image or remote registry image by default.

## Build And Run

```sh
docker compose up --build shardcache
```

The default Compose service:

- builds `cargo build --locked --release -p shardcache --features redis-server --bin shardcache`;
- publishes the fanout listener on `127.0.0.1:6380`;
- publishes direct shard ports `127.0.0.1:6501-6504`;
- starts `shardcache` with `--disable-persistence --server-mode direct`;
- names the local image `shardcache:local`.

Check the resolved Compose configuration before release runs:

```sh
docker compose config
```

## Smoke Test

Start the service in one terminal, then verify the RESP path from another:

```sh
redis-cli -p 6380 PING
redis-cli -p 6380 SET user:42 ready
redis-cli -p 6380 GET user:42
```

Expected output is `PONG`, `OK`, then `ready`.

## Build Variants

The default image includes Redis/Valkey compatibility:

```sh
SHARDCACHE_FEATURES=redis-server docker compose build shardcache
```

Build the lean RESP/SCNP server without the full Redis command catalog:

```sh
SHARDCACHE_FEATURES=server docker compose build shardcache
```

Optional native CPU tuning can be passed through the Docker build:

```sh
RUSTFLAGS="-C target-cpu=native" docker compose build shardcache
```

## Runtime Configuration

Compose exposes the main server knobs as environment variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `SHARDCACHE_HOST` | `127.0.0.1` | Host interface used for published ports. |
| `SHARDCACHE_PORT` | `6380` | Fanout RESP/SCNP listener port. |
| `SHARDCACHE_SHARD_COUNT` | `4` | Storage shard count. Must be a non-zero power of two. |
| `SHARDCACHE_DIRECT_SHARD_PORTS` | `1` | Enables one direct listener per shard. |
| `SHARDCACHE_DIRECT_SHARD_BASE_PORT` | `6501` | First direct shard listener port inside the container. |
| `SHARDCACHE_DIRECT_SHARD_PORT_RANGE` | `6501-6504` | Host/container direct port range published by Compose. |
| `SHARDCACHE_MAX_CONNECTIONS` | `4096` | Accepted connection limit. |
| `SHARDCACHE_HANDOFF_BUFFER_BYTES` | unset | Optional request handoff cap override for large SCNP payloads. |
| `SHARDCACHE_TOKIO_WRITER_MODE` | `inline` | Tokio writer mode. Set `split` for a separate per-connection writer task. |
| `SHARDCACHE_FEATURES` | `redis-server` | Cargo feature set used by the Docker build. |
| `RUST_LOG` | `info` | Runtime log filter. |

Keep `SHARDCACHE_DIRECT_SHARD_PORT_RANGE` aligned with
`SHARDCACHE_DIRECT_SHARD_BASE_PORT` and `SHARDCACHE_SHARD_COUNT`. For example,
an 8-shard server using base port 6501 should publish `6501-6508`.

## Direct Shard Ports

Direct shard ports are for clients that already route keys to their owning
shard. They accept RESP and SCNP, but they reject keyspace-wide operations,
transactions, or multi-key requests that do not belong entirely to that shard.
Use the fanout listener for ordinary Redis clients and cross-shard commands.

Rust clients can use direct routing with `shardcache-client-rs`:

```rust,no_run
use shardcache_client_rs::ShardCacheDirectClient;

fn main() -> shardcache_client_rs::Result<()> {
    let mut client = ShardCacheDirectClient::connect("127.0.0.1:6501", 4)?;
    client.set(b"user:42", b"ready")?;
    Ok(())
}
```

## Persistence

The default container command is disposable and in-memory:

```sh
shardcache --bind-addr 0.0.0.0:6380 --disable-persistence --server-mode direct
```

For durable deployments, run the source-built binary with an explicit config
or override the container command and mount a stable data directory. Persistent
engine-backed deployments should be validated against the command surface you
plan to expose before they are treated as durable Redis-compatible service
deployments.
