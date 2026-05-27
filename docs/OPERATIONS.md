# Operations

This page is the short operational contract for running `fast-cache-server` in
0.2.0-style deployments.

## Build Selection

| Build | Command | Use When |
| --- | --- | --- |
| Embedded crate | `fast-cache = "0.2"` | In-process Rust cache use. |
| Server | `cargo install fast-cache --features server --locked` | RESP/FCNP TCP access without the full Redis command catalog. |
| Redis-compatible server | `cargo install fast-cache --features redis-server --locked` | Redis/Valkey-compatible command and object behavior. |

`redis-server` implies both `server` and `redis`. Embedded-only builds
are expected not to compile the Redis compatibility source package; guard this
with `./scripts/check-feature-matrix.sh`.

The Dockerfile builds the same `fast-cache-server` binary into a local image.
Compose names that image `fast-cache:local`; there is no Docker Hub or remote
registry publishing path in this repository yet. The default Docker build uses
the Redis/Valkey-compatible `redis-server` feature set and starts the direct
in-memory server path. Use `FAST_CACHE_FEATURES=server` for the lean server
build without the Redis compatibility catalog.

## Starting The Server

```bash
fast-cache-server --bind-addr 127.0.0.1:6380 --data-dir ./var/fast-cache
```

For disposable local runs:

```bash
fast-cache-server \
  --bind-addr 127.0.0.1:6380 \
  --disable-persistence \
  --server-mode direct
```

The source checkout path is:

```bash
cargo run -p fast-cache --features redis-server --bin fast-cache-server -- \
  --bind-addr 127.0.0.1:6380 \
  --data-dir ./var/fast-cache
```

The local Docker path is:

```bash
docker compose up --build fast-cache
```

The default image and Compose command use
`--disable-persistence --server-mode direct`. That is the current container path
for Redis-compatible command execution. Engine-backed persistent deployment
should be configured explicitly and validated against the command surface before
it is treated as a durable Redis-compatible deployment.

## Ports

| Surface | Default Shape | Notes |
| --- | --- | --- |
| Fanout listener | `--bind-addr`, often `127.0.0.1:6380` | RESP and FCNP accepted on one socket. |
| Direct shard ports | `FAST_CACHE_DIRECT_SHARD_PORTS=1` plus base port | One route-checked RESP/FCNP listener per shard for direct routing clients. |

When direct shard ports are enabled, keep the published port range length equal
to `FAST_CACHE_SHARD_COUNT`. RESP requests on shard ports must route all keys
to that shard; keyspace-wide commands and RESP transactions are rejected there
and should use the fanout listener.

Direct server connections default to RESP2. `HELLO 3` switches that connection
to RESP3, `HELLO 2` switches it back, and `HELLO` without a protocol argument
returns the current connection protocol.

## Configuration

Start from `fast-cache.toml.example` for file-based configuration. Docker
Compose exposes the main knobs as environment variables:

| Variable | Meaning |
| --- | --- |
| `FAST_CACHE_HOST` | Host interface used by Compose port publishing. Defaults to `127.0.0.1`. |
| `FAST_CACHE_PORT` | Host/container fanout port. |
| `FAST_CACHE_SHARD_COUNT` | Server shard count. |
| `FAST_CACHE_DIRECT_SHARD_PORTS` | Enables shard-owned direct FCNP listeners. |
| `FAST_CACHE_DIRECT_SHARD_BASE_PORT` | First direct shard listener port. |
| `FAST_CACHE_DIRECT_SHARD_PORT_RANGE` | Host/container direct shard port range published by Compose. |
| `FAST_CACHE_MAX_CONNECTIONS` | Connection limit. |
| `FAST_CACHE_HANDOFF_BUFFER_BYTES` | Optional request handoff cap override for large FCNP/TCP payloads. |
| `FAST_CACHE_FEATURES` | Cargo features used by the Docker build. |
| `RUSTFLAGS` | Optional Docker build flags, for example native CPU tuning. |
| `RUST_LOG` | Runtime log filter. Defaults to `info` in Compose. |
| `FAST_CACHE_TOKIO_WRITER_MODE` | `inline` by default; set `split` to use a separate per-connection writer task. |

`FAST_CACHE_DIRECT_SHARD_PORT_RANGE` is only a Compose publishing setting. Keep
its length equal to `FAST_CACHE_SHARD_COUNT`, and keep its first port aligned
with `FAST_CACHE_DIRECT_SHARD_BASE_PORT`.

## Persistence

Use a stable `--data-dir` for persistent deployments. Benchmark and
compatibility proof runs often pass `--disable-persistence` so the storage path
does not dominate command measurements.

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
