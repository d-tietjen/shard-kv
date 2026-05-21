# fast-cache

`fast-cache` is an embedded-first, in-memory key-value database. The default
crate build exposes a Rust API for direct in-process use. The optional `server`
feature builds `fast-cache-server` as a RESP/FCNP TCP server with WAL and
snapshot persistence, and `redis-compat` adds Redis/Valkey data-type semantics
as an extension for embedded or server deployments.

The default build uses conservative safe memory paths. Reviewed lower-overhead
paths are available only when the `unsafe` feature is enabled.

## Embedded Use

Use [`FastMap`] when several threads should share one cloneable map-like cache
handle.
Keys and values are byte strings. TTL arguments are milliseconds; pass `None`
for persistent values.

```rust
use fast_cache::FastMap;

let cache = FastMap::new();

cache.insert_slice(b"user:42", b"ready");
{
    let value = cache.get(b"user:42").expect("cache hit");
    assert_eq!(value.value(), b"ready");
}
assert_eq!(cache.get_owned(b"user:42").unwrap().as_ref(), b"ready");

cache.remove(b"user:42");
assert!(!cache.exists(b"user:42"));
```

For lower-level access, [`storage::EmbeddedStore`] is the richer sharded engine
used by server mode, and [`storage::LocalEmbeddedStore`] is the owner-local
sharded API for pinned workers.

The crate is intentionally one package with layered APIs:

| Layer | API | Use when |
| --- | --- | --- |
| Shared map/cache | `FastMap` / `FastCache` / `embedded::SharedCache` | You want a cloneable DashMap-like map, process-local lock table, or cache. |
| Sharded engine | `embedded::ShardedEngine` / `storage::EmbeddedStore` | You want the full embedded engine that also backs server mode. |
| Owner-local shards | `embedded::LocalEmbeddedStore` | Workers are pinned and each worker should skip shared lock traffic for its shards. |
| Server | `server` feature / `fast-cache-server` | Other processes or machines should access the same database over RESP or FCNP. |

Read APIs are split by ownership:

| API | Meaning |
| --- | --- |
| `get` / `get_ref` | Borrow/reference the stored value without copying bytes. The returned guard or slice must not outlive the store borrow. |
| `get_mut` | Borrow the routed point value for replacement or removal while preserving its existing TTL. |
| `get_owned` | Return a refcounted owned value handle for callers that need to keep the value independently. |

`FastMap` also exposes Redis-style token-lock helpers:

```rust,no_run
use fast_cache::FastMap;

let locks = FastMap::new();

assert!(locks.try_acquire_lock(b"lock:job:7", b"unique-token", 30_000));
assert!(!locks.try_acquire_lock(b"lock:job:7", b"other-token", 30_000));
assert!(!locks.release_lock(b"lock:job:7", b"other-token"));
assert!(locks.renew_lock(b"lock:job:7", b"unique-token", 30_000));
assert!(locks.release_lock(b"lock:job:7", b"unique-token"));
```

In embedded mode these locks coordinate threads in one process. Use server mode
when locks must coordinate multiple processes or machines.

`EmbeddedStore::get_ref` returns a shard guard, `SharedEmbeddedStore::get_ref`
returns a shared-handle guard, and `LocalEmbeddedStore::get_ref` returns a
slice tied to the worker-local `&mut` borrow. The corresponding `get_mut`
guards support `value`, `set`, `set_slice`, and `remove`.

The opt-in `mutable-value-slices` feature adds `value_mut_no_ttl()` to those
mutation guards. It exposes `&mut [u8]` only when the stored `bytes::Bytes`
buffer is uniquely owned and the entry has no TTL. TTL-backed values return
`None` because in-place mutation bypasses TTL-preserving replacement logic; use
`set_slice` for TTL entries.

The opt-in `no-ttl` feature specializes `SharedEmbeddedStore` point-key hot
paths for workloads that never create expiring entries. In that build,
shared-store TTL write helpers are unsupported and panic if called with a TTL.

Batch APIs group routing work and return results in key order:

```rust
use fast_cache::storage::EmbeddedStore;

let cache = EmbeddedStore::new(4);
cache.batch_set(
    vec![
        (b"alpha".to_vec(), b"one".to_vec()),
        (b"beta".to_vec(), b"two".to_vec()),
    ],
    None,
);

let values = cache.batch_get(vec![b"alpha".to_vec(), b"missing".to_vec()]);
assert_eq!(values[0], Some(b"one".to_vec()));
assert_eq!(values[1], None);
```

Use [`storage::LocalEmbeddedStore`] when each worker owns its shards and calls
the cache through an exclusive `&mut` handle. This is the lowest-overhead Rust
API and is enabled by the default `sharded` feature.

This ownership model is intentionally different from shared reference-counted
caches such as `DashMap`. Shared caches let every worker clone one map handle
and access any key through internal synchronization. `fast-cache` instead
routes each key to a shard and gives the owning worker exclusive access to that
shard's cache slab. Use `EmbeddedStore` to build and route the cache, then
split it with `into_local_stores` when workers are pinned.

Use [`storage::SharedEmbeddedStore`] when every worker needs to clone one
handle and reach every key. It is cache-padded and lock-striped like DashMap,
while still sharing the same embedded shard implementation. For shared-handle
deployments, prefer a stripe count around `4 * worker_count`; one stripe per
worker is usually under-striped for read-heavy shared access. `EmbeddedStore`,
`SharedEmbeddedStore`, and `LocalEmbeddedStore` all require power-of-two shard
counts so routing can use shift-based striping.

```rust
#[cfg(feature = "sharded")]
{
    use fast_cache::storage::{
        EmbeddedRouteMode, EmbeddedStore, LocalEmbeddedStoreBootstrap,
    };

    let shared = EmbeddedStore::with_route_mode(4, EmbeddedRouteMode::FullKey);
    let mut stores = LocalEmbeddedStoreBootstrap::from_embedded(shared, 1).into_stores();
    let mut local = stores.pop().expect("one worker store");

    local.set(b"local-key".to_vec(), b"value".to_vec(), None);
    assert_eq!(local.get_ref(b"local-key"), Some(b"value".as_slice()));
    assert_eq!(local.get(b"local-key"), Some(b"value".to_vec()));
}
```

Session APIs keep related KV-cache chunks on the same route and can pack values
into contiguous buffers:

```rust
use fast_cache::storage::{EmbeddedStore, PackedSessionWrite};

let cache = EmbeddedStore::new(4);
let mut write = PackedSessionWrite::with_capacity(b"session:1".to_vec(), 2, 16);

write.push_owned_record(b"session:1:layer:0".to_vec(), b"kv0".to_vec());
write.push_owned_record(b"session:1:layer:1".to_vec(), b"kv1".to_vec());
cache.batch_set_session_packed_no_ttl(write);

let keys = vec![
    b"session:1:layer:0".to_vec(),
    b"session:1:layer:1".to_vec(),
];
let batch = cache.batch_get_session_packed(b"session:1", &keys);

assert!(batch.all_hit());
assert_eq!(batch.total_bytes(), 6);
```

## Embedded Benchmark Highlights

The embedded API is optimized for thread-per-core deployments. The lowest
overhead path is [`storage::LocalEmbeddedStore`], where each worker owns its
shards and calls the cache through an exclusive `&mut` handle. For applications
that need a cloneable shared handle, [`storage::SharedEmbeddedStore`] provides
lock-striped access with the same embedded shard implementation.

Current Linux release benchmarks use pinned workers, `100k` keys for
small values, `10s` measured runs, and latency sampling disabled for
max-throughput rows. The `fc-embed` rows below are direct owner-local embedded
stores with no TTL and no eviction:

| Value | Mix | fast-cache direct | DashMap | Moka |
| ---: | --- | ---: | ---: | ---: |
| 64B | GET | 422.82M ops/s | 54.84M ops/s | 2.65M ops/s |
| 64B | SET | 114.85M ops/s | 35.40M ops/s | 1.51M ops/s |
| 64B | 80/20 | 253.88M ops/s | 45.23M ops/s | 4.52M ops/s |
| 4KiB | 80/20 | 21.03M ops/s | 6.90M ops/s | 3.77M ops/s |

Capacity-bounded rows stress a different path. With LRU enabled, `64B`,
read-only, `16` workers, and `25%` resident capacity, `fc-embed` reaches
`473.7M ops/s`. For `64KiB` LRU rows, the write-only case is the
worst fast-cache shape because value materialization and eviction bookkeeping
dominate; it measures around `19.1 GB/s` for fast-cache and `33.4 GB/s` for
Moka. That is not representative of normal LRU usage. On the same 64KiB LRU
setup, read-only reaches `756.93M ops/s` for fast-cache embedded direct versus
`3.31M ops/s` for Moka, and the `80/20` row is effectively tied with Moka
(`1.45M ops/s` versus `1.42M ops/s`). On a smaller `4KiB` LRU row at 16
workers, fast-cache embedded direct reaches `676.37M ops/s` read-only,
`26.70M ops/s` on `80/20`, and `5.41M ops/s` write-only.

The large-value embedded GET rows use reference-read mode by default. They
measure lookup plus borrowed value access, not copying the full value into a
new buffer. The resulting GB/s is a logical payload rate, not physical data
throughput. Use `--read-mode copy` in the benchmark harness when comparing
materialized read bandwidth against caches that copy into a caller buffer.

Treat these as workload-specific reference points, not universal constants.
The full embedded matrix, LRU/TTL rows, CSV artifact paths, and reproduction
commands live in
[`benchmarks/FAST_CACHE_EMBEDDED_RELEASE.md`](https://github.com/d-tietjen/fast-cache/blob/main/benchmarks/FAST_CACHE_EMBEDDED_RELEASE.md).

## API Map

The most commonly used Rust APIs live in [`storage`]:

- [`storage::EmbeddedStore`]: shared, sharded store for byte-string keys.
- [`storage::SharedEmbeddedStore`]: cloneable, lock-striped embedded store for
  cross-worker shared handles.
- [`storage::LocalEmbeddedStore`]: worker-local store for thread-per-core
  workers.
- `get_ref`, `get_mut`, and `get`: borrowed/reference reads, routed mutation
  guards, and owned materialized reads.
- [`storage::EmbeddedRouteMode`], [`storage::EmbeddedKeyRoute`], and
  [`storage::PreparedPointKey`]: routing and precomputed lookup helpers.
- [`storage::PackedBatch`] and [`storage::PackedSessionWrite`]: contiguous
  batch read/write payloads.
- [`storage::TierStatsSnapshot`], [`storage::ShardStatsSnapshot`], and
  [`storage::WalStatsSnapshot`]: runtime statistics.

Core key/value methods include `set`, `set_value_bytes`, `batch_set`, `get`,
`get_view`, `batch_get`, `batch_get_view`, `batch_get_packed`, `delete`,
`exists`, `ttl_seconds`, `pttl_millis`, `expire`, `persist`, `len`,
`key_snapshot`, `stored_bytes`, `stats_snapshot`, and
`process_maintenance`.

Session-oriented methods include `batch_set_session_owned_no_ttl`,
`batch_set_session_packed_no_ttl`, `batch_get_session`,
`batch_get_session_view`, `batch_get_session_packed`,
`prepare_point_key`, and their routed or prehashed variants.

With `redis-compat`, Redis object helpers are exposed on
[`storage::EmbeddedStore`] for hashes, lists, sets, and sorted sets. They use
Redis-style wrong-type behavior through [`storage::RedisObjectResult`]. The
public method families are `hset`/`hget` and related hash methods,
`lpush`/`rpush`/`lrange` and related list methods, `sadd`/`srem`/`smembers`
and related set methods, and `zadd`/`zrange`/`zscore` and related sorted-set
methods.

Other modules:

- [`config`]: `FastCacheConfig`, `EvictionPolicy`, tier sizing, persistence
  configuration, and TOML load/store helpers.
- [`protocol`]: RESP and native fast protocol codecs.
- [`persistence`]: snapshot loading/writing and WAL runtime support.
- [`cuda`]: GPU-facing configuration and transfer descriptors.
- [`server`]: TCP listener and connection handling, available with `server`.

## Server Use

Install and run the core server binary:

```bash
cargo install fast-cache --features server --locked
fast-cache-server --data-dir ./var/fast-cache
```

From a checkout:

```bash
cargo run -p fast-cache --features server --bin fast-cache-server -- --data-dir ./var/fast-cache
```

For a Redis/Valkey-compatible deployment, enable the compatibility extension:

```bash
cargo install fast-cache --features redis-server --locked
```

The server listens on `127.0.0.1:6380` by default and accepts RESP clients:

```bash
printf '*1\r\n$4\r\nPING\r\n' | nc 127.0.0.1 6380
printf '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n' | nc 127.0.0.1 6380
printf '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n' | nc 127.0.0.1 6380
```

The current server command catalog implements the redesigned string-key hot
path for `GET` and `SET`. Additional RESP commands should be added through the
per-command module pattern in `src/commands/README.md` so parser, storage, and
direct-server behavior stay local to the command.

## Configuration

Load configuration from TOML with [`config::FastCacheConfig`]:

```rust,no_run
use std::path::Path;

use fast_cache::config::FastCacheConfig;

fn main() -> fast_cache::Result<()> {
    let config = FastCacheConfig::load_from_path(Path::new("fast-cache.toml"))?;
    config.validate()?;
    Ok(())
}
```

The repository includes `fast-cache.toml.example` with the supported fields.
Redis-compatible transactions default to `transaction_mode = "shard_local"`.
Use `coordinated_cross_shard` only when the server/router should gate all
affected shards for cross-shard `EXEC`; use `disabled` to reject
`MULTI`/`EXEC`/`DISCARD`.

### WAL TCP Export

The server can stream live WAL frames in addition to writing segments to disk.
Two modes are supported:

- `connect`: fast-cache connects to one downstream collector.
- `listen`: fast-cache binds a subscription port and fans out live frames to
  authenticated subscribers.

```toml
[persistence.tcp_export]
enabled = true
mode = "listen"
addr = "127.0.0.1:7630"
auth_token = "replace-me"
channel_capacity = 16384
max_subscribers = 64
backpressure_on_full = false
```

The stream uses the same framed WAL bytes as disk segments: `FCW2` magic,
flags, payload length, payload, and CRC. This is a live export path, not a
catch-up or replay API; disk WAL segments and snapshots remain the recovery
source. With `backpressure_on_full = false`, a slow TCP exporter can drop live
export frames while disk WAL append continues. Set it to `true` only when the
export consumer is allowed to backpressure writes. Auth tokens are plaintext
inside the TCP stream, so use localhost, a private network, or a TLS tunnel
across trust boundaries.

### Native Replication

Native replication is separate from WAL and Redis PSYNC. It ships storage-level
mutation batches for async read replicas and service subscribers:

```toml
[replication]
enabled = true
role = "primary"
bind_addr = "127.0.0.1:7631"
auth_token = "replace-me"
compression = "none"
zstd_level = 3
send_policy = "batch"
batch_max_records = 64
batch_max_bytes = 262144
batch_max_delay_us = 250
backlog_bytes = 67108864
snapshot_chunk_bytes = 1048576
```

`send_policy = "immediate"` flushes every write as a one-record mutation
batch. `send_policy = "batch"` flushes by record count, byte size, or delay.
Replication defaults to `compression = "none"` because realistic write-sync
payloads usually compress poorly enough that zstd costs more CPU than it saves
on the hot path. Use `compression = "zstd"` only when bandwidth is the limiting
resource and benchmark data for the payload shape justifies it.
Primary export runs through shard-local queues and batchers. This keeps write
sync aligned with fast-cache's owned-shard architecture: a saturated replication
lane can backpressure its shard without centralizing all writes behind one
global replication queue. Subscribers still receive one FCRP frame stream, and
the per-shard sequence watermarks make replay idempotent.
Replicas track per-shard sequence watermarks so they can apply mutations
idempotently and catch up from backlog or snapshot-plus-delta.

## Feature Flags

- `embedded`: default embedded Rust database API.
- `sharded`: default sharded storage and owner-local embedded API.
- `redis-compat`: enables embedded Redis/Valkey data-type semantics and
  wrong-type behavior without requiring server networking.
- `server`: builds the RESP/FCNP `fast-cache-server` binary.
- `redis-server`: enables both `server` and `redis-compat` for Redis/Valkey
  compatibility deployments.
- `monoio`: enables the Linux-only server runtime selected with
  `FAST_CACHE_USE_MONOIO=1`. The server still uses `bytes-handoff` for
  connection read buffering, using its monoio adapter on Linux. With
  `FAST_CACHE_DIRECT_SHARD_PORTS=1`, the server also binds one listener per
  shard, starting at `FAST_CACHE_DIRECT_SHARD_BASE_PORT` or the fanout port + 1,
  so direct clients can route while fanout RESP/FCNP stays available. Monoio
  writer experiments are selected with
  `FAST_CACHE_MONOIO_SAFE_WRITER=inline|split|writev`; Tokio remains the
  portable default runtime.
- `telemetry`: integrates with `fast-telemetry`.
- `cuda`: exposes GPU-facing configuration and transfer descriptors.
- `experimental-no-ttl-point-hot-path`: experimental benchmark knob for the
  internal point-key-only hot path. It implies `no-ttl`, is only intended for
  TTL-free workloads, and falls back to the normal map when richer storage
  semantics are needed.
- `no-ttl`: specializes shared embedded point-key hot paths for TTL-free
  deployments.
- `unsafe`: opts into reviewed unsafe hot paths for lower overhead.

## Safety

The `unsafe` feature keeps the same public API while enabling reviewed hot
paths for server I/O, protocol codecs, flat-map indexing, and owner-local read
views. See `SAFETY.md` for the unsafe inventory, invariants, and safe
fallbacks.

## License

Apache-2.0. See the repository `LICENSE` file.
