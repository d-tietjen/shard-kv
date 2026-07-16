# Operations

This page is the short operational contract for running `shardcache` in
0.6.x-style deployments.

For the container-specific runbook, see
[`SHARDCACHE_DOCKER.md`](SHARDCACHE_DOCKER.md).

## Build Selection

| Build | Command | Use When |
| --- | --- | --- |
| Embedded crate | `shardmap = "0.7.0"` | In-process Rust cache use. |
| Server crate | `shardcache = "0.7.0"` | Install or depend on the RESP/SCNP server package. |
| Native client crate | `shardcache-client-rs = "0.7.0"` | SCNP client access from Rust applications. |
| Server | `cargo run -p shardcache --features server --bin shardcache -- ...` | RESP/SCNP TCP access without the full Redis command catalog. |
| Redis-compatible server | `cargo run -p shardcache --features redis-server --bin shardcache -- ...` | Redis/Valkey-compatible command and object behavior. |

`shardmap`, `shardcache`, and `shardcache-client-rs` are the crates.io crates
for 0.6.x. Publish `shardcache-client-rs` first, then `shardmap`, then
`shardcache`; the optional `kv-overflow` adapter makes that order necessary.
Python, C ABI, runtime, benchmark, and
integration packages remain source-workspace packages.

`redis-server` implies `server`, `redis`, `redis-functions`, and
`redis-modules`. Embedded-only builds stay separate from the Redis-compatible
server feature set; guard this with `./scripts/check-feature-matrix.sh`.
Redis Modules command families stay behind `redis-modules-all` or the
individual `redis-module-*` features so production builds only compile module
facades they deliberately expose.

The Dockerfile builds the same `shardcache` binary into a local image.
Compose names that image `shardcache:local`; the repository does not push a
remote image by default. The default Docker build uses the
Redis/Valkey-compatible `redis-server` feature set and starts the direct
in-memory server path. Use `SHARDCACHE_FEATURES=server` for the lean server
build without the Redis compatibility catalog.

## Starting The Server

```bash
shardcache --bind-addr 127.0.0.1:6380 --data-dir ./var/shardcache
```

For disposable local runs:

```bash
shardcache \
  --bind-addr 127.0.0.1:6380 \
  --disable-persistence \
  --server-mode direct
```

The source checkout path is:

```bash
cargo run -p shardcache --features redis-server --bin shardcache -- \
  --bind-addr 127.0.0.1:6380 \
  --data-dir ./var/shardcache
```

The local Docker path is:

```bash
docker compose up --build shardcache
```

The default image and Compose command use
`--disable-persistence --server-mode direct`. That is the current container path
for Redis-compatible command execution. Engine-backed persistent deployment
should be configured explicitly and validated against the command surface before
it is treated as a durable Redis-compatible deployment.

## Ports

| Surface | Default Shape | Notes |
| --- | --- | --- |
| Fanout listener | `--bind-addr`, often `127.0.0.1:6380` | RESP and SCNP accepted on one socket. This is the default `server_endpoint_mode = "fanout"`. |
| Direct shard ports | `server_endpoint_mode = "direct_shard"` | Adds one route-checked RESP/SCNP listener per shard for direct routing clients. |

Direct shard ports start at `bind_addr + 1` unless
`SHARDCACHE_DIRECT_SHARD_BASE_PORT` sets the first direct port. When direct
shard ports are enabled, keep the published port range length equal to
`SHARDCACHE_SHARD_COUNT`. RESP requests on shard ports must route all keys to
that shard; keyspace-wide commands and RESP transactions are rejected there and
should use the fanout listener.

When a caller-owned embedded store is exposed as a server, fanout routes each
complete single-shard request to the shard owner. It should not lock across all
shards or force the embedded hot path through a separate memory copy. Use
`server_endpoint_mode = "direct_shard"` only when third-party clients can route
directly to shard-owned ports.

Direct server connections default to RESP2. `HELLO 3` switches that connection
to RESP3, `HELLO 2` switches it back, and `HELLO` without a protocol argument
returns the current connection protocol.

## Configuration

Start from `shardcache.toml.example` for file-based configuration. Docker
Compose exposes the main knobs as environment variables:

| Variable | Meaning |
| --- | --- |
| `SHARDCACHE_HOST` | Host interface used by Compose port publishing. Defaults to `127.0.0.1`. |
| `SHARDCACHE_PORT` | Host/container fanout port. |
| `SHARDCACHE_SHARD_COUNT` | Server shard count. |
| `SHARDCACHE_DIRECT_SHARD_PORTS` | Container/script compatibility switch that enables shard-owned direct listeners. Prefer `server_endpoint_mode = "direct_shard"` in config files. |
| `SHARDCACHE_DIRECT_SHARD_BASE_PORT` | First direct shard listener port. |
| `SHARDCACHE_DIRECT_SHARD_PORT_RANGE` | Host/container direct shard port range published by Compose. |
| `SHARDCACHE_MAX_CONNECTIONS` | Connection limit. |
| `SHARDCACHE_HANDOFF_BUFFER_BYTES` | Optional request handoff cap override for large SCNP/TCP payloads. |
| `SHARDCACHE_FEATURES` | Cargo features used by the Docker build. |
| `RUSTFLAGS` | Optional Docker build flags, for example native CPU tuning. |
| `RUST_LOG` | Runtime log filter. Defaults to `info` in Compose. |
| `SHARDCACHE_TOKIO_WRITER_MODE` | `inline` by default; set `split` to use a separate per-connection writer task. |

`SHARDCACHE_DIRECT_SHARD_PORT_RANGE` is only a Compose publishing setting. Keep
its length equal to `SHARDCACHE_SHARD_COUNT`, and keep its first port aligned
with `SHARDCACHE_DIRECT_SHARD_BASE_PORT`.

## Persistence

Use a stable `--data-dir` for persistent deployments. Benchmark and
compatibility proof runs often pass `--disable-persistence` so the storage path
does not dominate command measurements.

With persistence enabled, each storage shard exclusively owns a local WAL
block appender. Mutations accumulate until `wal_block_max_records`,
`wal_block_max_bytes`, or `fsync_interval_ms` seals the block. Per-shard bounded
queues feed one background merger, which preserves each shard's mutation order,
writes the existing canonical segment format, and calls `sync_data` at the
configured interval. A full shard queue applies backpressure only to that shard;
there is no producer lock shared by storage workers.

The defaults seal at 64 records or 256 KiB. Smaller blocks reduce the amount of
data exposed to process failure before handoff but increase channel and merger
overhead. The fsync interval remains the upper durability window; shutdown
flushes partial blocks and performs a final data sync before recovery can begin.

## Logs And Shutdown

The server writes startup and runtime logs through the configured Rust logging
subscriber. Use `RUST_LOG=info` for normal operation and a narrower module
filter for command-path investigations. The server handles process shutdown by
leaving cleanup to the owning supervisor; production deployments should run it
under systemd, Docker, Kubernetes, or another process manager.

## Benchmark Discipline

Do not publish ad hoc terminal numbers. Use the benchmark bundle script so
every run captures the git SHA, dirty status, command filters, fixture scope,
client count, and machine metadata alongside CSV and summary artifacts.
