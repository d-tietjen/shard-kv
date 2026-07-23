# Operations

This page is the short operational contract for running `shardcache` 0.8.

For the container-specific runbook, see
[`SHARDCACHE_DOCKER.md`](SHARDCACHE_DOCKER.md).

## Build Selection

| Build | Command | Use When |
| --- | --- | --- |
| Embedded crate | `shardmap = "0.8.1"` | In-process Rust cache use. |
| Server crate | `shardcache = "0.8.1"` | Install or depend on the RESP/SCNP server package. |
| Native client crate | `shardcache-client-rs = "0.8.1"` | SCNP client access from Rust applications. |
| Active-active embedded map | `shardmap` with `active-sync-causal-eventual` or `active-sync-consensus-ordered-eventual` | Opt-in exact point-value synchronization; add `active-sync-tls` for network peers. |
| Vector read replica | `shardmap` with `redis` and `ReplicatedEmbeddedStore` | Single-writer FCRP replication of canonical vector-set state; fence promotion after catch-up. |
| Server | `cargo run -p shardcache --features server --bin shardcache -- ...` | RESP/SCNP TCP access without the full Redis command catalog. |
| Redis-compatible server | `cargo run -p shardcache --features redis-server --bin shardcache -- ...` | Redis/Valkey-compatible command and object behavior. |

`shardmap`, `shardcache`, and `shardcache-client-rs` are the crates.io crates
for 0.8. Publish `shardcache-client-rs` first, then `shardmap`, then
`shardcache`; the optional `kv-overflow` adapter makes that order necessary.
Python, C ABI, runtime, benchmark, and
integration packages remain source-workspace packages.

Active sync is absent from the default feature set. It is intended for
read-heavy exact-value deployments that need eventually consistent writable
replicas. Review [`RELEASE_0_8.md`](RELEASE_0_8.md) for measured overhead and
[`ACTIVE_ACTIVE_REPLICATION.md`](ACTIVE_ACTIVE_REPLICATION.md) for consistency,
membership, security, and recovery requirements before enabling it.

Native vector read-replica FCRP is a separate single-primary transport. FCRP
v3 requires both peers to run 0.8.0. On non-loopback addresses configure TLS
1.3 mTLS on both sides and a replication token. Prefer
`replication.auth_token_path`; during rotation, place the new token in that
file and the retiring token in `previous_auth_token_path`, reconnect replicas,
then remove the previous token. Certificate and CA files are loaded for each
new connection, so reconnect after atomically replacing them.

Size `replication.snapshot_receive_max_bytes` and
`snapshot_receive_max_entries` above the largest expected bootstrap while
leaving headroom for the resident replica. Exceeding either limit or receiving
reordered/mismatched chunks disconnects the peer without replacing local data.
The byte limit also caps the primary's per-shard retained key index during
streaming, so a bootstrap cannot create an unbounded key-only allocation.
`replication.receive_max_frame_bytes` limits mutation and snapshot-chunk frame
allocation (64 MiB by default); control frames remain capped at 128 KiB. The
replica advertises this value and the primary rejects it if it is smaller than
the primary's configured outbound ceiling. Keep the replica limit equal to or
greater than the primary limit, and ensure every individual mutation fits it.
`replication.read_timeout_ms` and `write_timeout_ms` are whole-frame deadlines,
not idle timeouts. When either expires, the connection closes rather than
continuing from a partial frame. The primary shard count is negotiated before
any mutation allocation and must remain unchanged for the connection lifetime.

Only one snapshot bootstrap is streamed by a primary at a time, and it
materializes one source shard at a time rather than cloning the complete cache.
The primary retains that shard's key index, one bounded materialization page,
and one wire chunk; remote overflow reads do not hold the shard lock. Other
stale replicas wait while backlog-only replicas remain independent.
`snapshot_bootstrap_timeout_ms` bounds gate wait, capture, cold-tier reads,
delivery, and buffered catch-up as one operation. Snapshot provider or
cold-tier failures close the connection without replacing the replica's last
valid state.

## Object Overflow Safety

Object overflow uses bounded background workers. Uploads retain the resident
value until the acknowledgement is observed, and upload, fault-in, and delete
requests never wait while a shard lock is held. A GET that reaches a remote
value materializes it before returning; server protocols report a backend
error rather than an ordinary cache miss when that read fails. Persistence
snapshots and entry visitors use fallible materialization and fail if a remote
value cannot be verified.

Each `SCOVF2` payload is compressed within an exact decoded-size bound and is
authenticated with a process-generation HMAC-SHA256 key bound to its object
key. Remote references are intentionally not durable recovery state, so a new
process receives a new authentication key and recovers authoritative values
from local WAL and snapshots. S3 bodies, generation markers, and cleanup key
listings are bounded before retention. Filesystem operations use
descriptor-relative, no-follow traversal on Unix, and each cleanup prefix is
traversed once with a total limit of 100,000 keys or 16 MiB of key names.
Generation markers are replaced with a synced same-directory rename, and
generation cleanup uses a heartbeat independent of the cleanup scan interval.
S3 credentials must be named explicitly as an access-key/secret-key environment
pair; the adapter never falls back to instance metadata. Private HTTPS
endpoints can add a bounded PEM trust bundle with `tls_ca_path` while retaining
certificate verification.

Configuration validation caps worker, queue, retry, timeout, cooldown, and
cleanup settings. Entropy acquisition and required worker/cleanup thread
creation are fallible startup operations and never intentionally panic.
Replacing or disabling a runtime is rejected while remote or pending entries
still depend on it. Cleanup scans fail closed above their total key or retained
name limits. `EmbeddedStore::try_batch_set`
preflights the complete batch; `batch_set` also rejects the whole batch rather
than partially applying invalid point mutations.

FCRP mutations are exact per-source-shard sequence streams. A gap, invalid
source shard, inconsistent key hash/tag, duplicate snapshot key, or topology
change is a protocol error and leaves the prior replica state intact. Operators
must restart bootstrap from a compatible primary after changing shard count;
0.8.0 does not perform online FCRP shard-topology migration.

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
