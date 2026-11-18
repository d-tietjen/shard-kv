# shardmap

`shardmap` is the embedded Rust map/cache crate for `shard-kv`. It gives
applications a cloneable, sharded in-process `ShardMap<K, V>` handle, plus a
raw byte-oriented cache surface for TTLs, locks, prepared-key lookups, semantic
cache APIs, and server/protocol internals.

Use `shardmap` when you want an embedded Rust cache. Use the repository's
`shardcache` server package when you need a TCP service.

## Install

```toml
[dependencies]
shardmap = "0.10.0"
```

## Quick Start

```rust
use shardmap::ShardMap;

let users: ShardMap<String, String> = ShardMap::new();

users.insert("user:42".to_owned(), "ready".to_owned());
let value = users.get("user:42");

assert_eq!(value.as_deref(), Some("ready"));
```

`ShardMap<K, V>` is a cheap cloneable handle. Clones share the same underlying
sharded store and can be moved into worker threads. It stores Rust keys and
values directly, so custom structs work naturally when `K: Hash + Eq`. Use
`get` when you want an owned cloned value, or `get_ref` when a short borrowed
guard is enough.

## Feature Overview

| Area | What it gives you | Example |
| --- | --- | --- |
| Typed point-key map | Insert, get, borrow, remove, and route typed keys and values. | [`typed_map.rs`](examples/typed_map.rs) |
| Raw byte cache | Insert, get, mutate, remove, and entry-style access for byte keys. | [`basic_map.rs`](examples/basic_map.rs) |
| TTL cache | Relative TTL writes and memory-limit eviction. | [`ttl_and_locks.rs`](examples/ttl_and_locks.rs) |
| Prepared keys | Route metadata for repeated hot-key lookups. | [`prepared_keys_threads.rs`](examples/prepared_keys_threads.rs) |
| Entry API | Occupied/vacant mutation without a separate lookup. | [`entry_api.rs`](examples/entry_api.rs) |
| Route inspection | See which shard owns a key before sending work to a worker. | [`route_inspection.rs`](examples/route_inspection.rs) |
| Lock helpers | Process-local token locks built on `SET key token NX PX ttl` semantics. | [`ttl_and_locks.rs`](examples/ttl_and_locks.rs) |
| Configuration | Capacity hints, memory budgets, eviction policy, routing, and lock policy. | [`configured_cache.rs`](examples/configured_cache.rs) |
| Redis-compatible embedded API | Execute supported Redis commands directly in-process, including prepared commands and session state. | Enable the `redis` feature |
| Parent telemetry runtime | Record shardmap metrics into a parent-owned fast-telemetry runtime, or fall back to shardmap-owned metrics. | Enable the `parent-telemetry-runtime` feature |
| Semantic cache | Store embeddings with cached values and search by cosine similarity. | [`semantic_cache.rs`](examples/semantic_cache.rs) |
| Semantic TTL | Combine semantic reuse with freshness windows. | [`semantic_ttl.rs`](examples/semantic_ttl.rs) |
| Governance metadata | Enforce application-owned authorization context on semantic and exact point hits. | [`semantic_cache.rs`](examples/semantic_cache.rs), [`EXACT_GOVERNANCE.md`](../../docs/EXACT_GOVERNANCE.md) |
| Mini app | A small feature-flag cache combining TTL, prepared keys, and locks. | [`mini_feature_flags.rs`](examples/mini_feature_flags.rs) |

Run any example with:

```bash
cargo run -p shardmap --example basic_map
```


### SET KEEPTTL expiry

For TTL-enabled builds, `SET ... KEEPTTL` retains the live key's absolute
expiration at the storage write. A missing or already expired key is created
without a TTL. An intervening write supplies the expiry retained by KEEPTTL;
the existing NX/XX condition checks remain unchanged. Plain SET still clears
the TTL and keeps its existing prehashed writer.

The exact expiry regression tests run separately with the default server
features and with `experimental-compact-point-storage`. The `no-ttl` feature,
also enabled by `experimental-no-ttl-point-hot-path` and `--all-features`,
intentionally opts out of TTL behavior; those builds exclude the TTL regression
module and do not establish TTL correctness.

## Typed Map Operations

Use `ShardMap<K, V>` for the native typed embedded API. It stores Rust objects
directly, similar to DashMap, and shards by hashing `K`. Keys must implement
`Hash + Eq`; owned reads require `V: Clone`; snapshots require cloned keys or
values as needed.

```rust
use shardmap::{ShardMap, ShardMapOptions};

let users: ShardMap<String, String> = ShardMap::with_options(ShardMapOptions {
    capacity_hint: Some(1024),
    default_ttl_ms: None,
});

users.insert("user:42".to_owned(), "queued".to_owned());
assert!(users.contains_key("user:42"));

if let Some(value) = users.get_ref("user:42") {
    assert_eq!(value.value(), "queued");
}

users.insert("user:42".to_owned(), "running".to_owned());
assert_eq!(users.remove("user:42").as_deref(), Some("running"));
assert!(!users.contains_key("user:42"));
```

`ShardMapWithShards<N, K, V>` selects the stripe count at compile time. The
default is 64 stripes. `ShardMapOptions::default_ttl_ms` applies to plain
`insert` and `try_insert` calls; `insert_with_ttl` and `try_insert_with_ttl`
can override it for one write. Passing `None` to an explicit TTL write stores
that value without a TTL.

By default, shardmap behaves like a resident in-process map: entries remain
until the owning process removes them or the caller explicitly uses a TTL.
Memory-pressure eviction is opt-in through `CacheOptions` or server
configuration; it is not part of the default `ShardMap::new()` path.

### Resident And Cache Deployment Isolation

Service namespaces isolate keys, not eviction policy. Memory-pressure eviction
is configured on the embedded backing engine, so a resident deployment must not
share an engine with an LRU/LFU cache-style deployment. The Python `Store`
namespace view enforces this by refusing to change `resident_service` on an
existing engine; deployment registries should likewise key backing engines by
resident/cache mode and eviction settings.

Custom structs need no codec when they are used with the native `ShardMap`.

```rust
use shardmap::ShardMap;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct UserKey {
    tenant: String,
    id: u64,
}

#[derive(Clone, Debug, PartialEq)]
struct User {
    name: String,
    active: bool,
}

let map: ShardMap<UserKey, User> = ShardMap::new();
let key = UserKey { tenant: "acme".to_owned(), id: 7 };
map.insert(key.clone(), User { name: "Devon".to_owned(), active: true });

assert!(map.contains_key(&key));
```

### Parent Telemetry Runtime

Embedded applications that already own telemetry can pass that owner to
shardmap instead of creating a separate metrics owner. Today, enable
`parent-telemetry-runtime`, then pass `Some(Arc<TelemetryRuntime>)` to
`EmbeddedStore::with_parent_telemetry`,
`EmbeddedStore::with_route_mode_and_parent_telemetry`,
`SharedEmbeddedStore::with_parent_telemetry`, or
`EngineHandle::open_with_parent_telemetry`. Passing `None` keeps the same API
shape but lets shardmap allocate and own a private fast-telemetry runtime for
that store. Shardmap registers its `CacheTelemetry` metric group at
construction time and records through direct metric handles on the hot path.

## Codec Facades

Enable the `codec` feature when you want typed facades over the shared byte
engine used by the raw cache and protocol/server paths. This is for advanced
users who need multiple typed views of the same byte database or an
application-owned serialization format.

Each `CodecShardMap` facade must use a distinct non-empty namespace. Codec
iterator and snapshot APIs filter by that namespace and strip it before
decoding keys, so separate codecs over the same engine do not see each other's
data. When a namespaced facade is created with
`from_shared_engine_with_options`, `CacheOptions::default_ttl_ms` is applied
only to writes made through that namespace.

```rust,ignore
use bytes::Bytes;
use shardmap::{CodecShardMap, ShardCacheWithShards};

let engine = ShardCacheWithShards::<8>::new();
let strings: CodecShardMap<String, String, 8> =
    CodecShardMap::from_shared_engine(Bytes::from_static(b"strings"), engine.clone()).unwrap();
let numbers: CodecShardMap<u64, String, 8> =
    CodecShardMap::from_shared_engine(Bytes::from_static(b"numbers"), engine).unwrap();

strings.insert_ref("42", "string key");
numbers.insert(42, "numeric key".to_owned());

assert_eq!(strings.get("42").unwrap().as_deref(), Some("string key"));
assert_eq!(numbers.get(&42).unwrap().as_deref(), Some("numeric key"));
assert_eq!(strings.keys().unwrap(), vec!["42".to_owned()]);
assert_eq!(numbers.keys().unwrap(), vec![42]);
```

## Raw Byte Cache Operations

Use `ShardCache` when you want the lower-level byte-oriented cache surface:
entry guards, mutable byte values, raw prepared keys, Redis-style locks, TTLs,
and semantic-cache APIs.

```rust
use shardmap::ShardCache;

let cache = ShardCache::with_capacity(1024);

cache.insert_slice(b"job:1", b"queued");
assert!(cache.contains_key(b"job:1"));

if let Some(mut value) = cache.get_mut(b"job:1") {
    value.set_slice(b"running");
}

assert_eq!(cache.remove(b"job:1").unwrap().as_ref(), b"running");
assert!(!cache.contains_key(b"job:1"));
```

Use `get_owned` when you want refcounted bytes after the shard read lock has
been released. Use `get`/`get_ref` when a short borrowed guard is enough.

## TTL, Eviction, And Cache Configuration

TTL values are relative milliseconds. A `None` TTL means the value does not
expire because of time.

```rust,ignore
use shardmap::ShardCache;

let cache = ShardCache::new();
cache.insert_slice_with_ttl(b"session:1", b"active", Some(30_000));

assert!(cache.contains_key(b"session:1"));
```

`CacheOptions` configures the shared-handle cache. The default options are also
resident: no default TTL, no memory budget, and `EvictionPolicy::None`. Memory
limits are enforced inside each stripe only when you set a budget and select an
eviction policy. `default_ttl_ms` applies to plain cache writes, while explicit
TTL writes override it.

```rust
use shardmap::{CacheOptions, ShardCache};
use shardmap::config::EvictionPolicy;

let cache = ShardCache::with_options(CacheOptions {
    capacity_hint: Some(32_768),
    total_memory_bytes: Some(256 * 1024 * 1024),
    eviction_policy: EvictionPolicy::Lru,
    default_ttl_ms: Some(300_000),
    ..CacheOptions::default()
});

assert_eq!(cache.shard_count(), 64);
```

`EvictionPolicy::Lru` and `EvictionPolicy::Lfu` are available in the default
crate. `EvictionPolicy::Prefix` is available with the `prefix-eviction`
feature for prefix-group cache workloads.

## Prepared Keys And Concurrency

For repeated hot lookups, prepare the key once and reuse the route metadata.

```rust,ignore
use shardmap::ShardCache;

let cache = ShardCache::new();
cache.insert_slice(b"feature:alpha", b"enabled");

let prepared = cache.prepare_key(b"feature:alpha");
let value = cache.get_prepared_owned(&prepared).unwrap();

assert_eq!(value.as_ref(), b"enabled");
```

Cloned handles share the same storage, so applications can move a clone into
each worker thread and keep using normal map operations.

## Entry API And Routing

Use `entry` when the update naturally depends on whether the key is already
present.

```rust
use bytes::Bytes;
use shardmap::ShardCache;

let cache = ShardCache::new();
let value = cache.entry(Bytes::from_static(b"job:42"))
    .or_insert(Bytes::from_static(b"queued"));

assert_eq!(value.value().unwrap(), b"queued");
```

Use `route_key` when your application already partitions work by shard and
wants to send a key to its owning worker.

```rust
use shardmap::ShardCacheWithShards;

let cache = ShardCacheWithShards::<8>::new();
let route = cache.route_key(b"user:42");

assert!(route.shard_id < cache.shard_count());
```

## Lock Helpers

The lock helpers are useful for process-local coordination in embedded mode.
They acquire only when the key is absent or expired, release only when the
stored token matches, and renew by extending the TTL for the matching token.

```rust,ignore
use shardmap::ShardCache;

let cache = ShardCache::new();

assert!(cache
    .try_acquire_lock(b"lock:job:1", b"worker-a", 5_000)
    .expect("lock acquisition should be valid"));
assert!(!cache
    .try_acquire_lock(b"lock:job:1", b"worker-b", 5_000)
    .expect("second lock acquisition should be valid"));
assert!(cache
    .renew_lock(b"lock:job:1", b"worker-a", 5_000)
    .expect("lock renewal should be valid"));
assert!(cache.release_lock(b"lock:job:1", b"worker-a"));
```

Use the server surface when multiple processes or machines need to coordinate
through one lock table.

## Redis-Compatible Embedded API

Enable the `redis` feature when you want to call shardcache's supported Redis
command surface without opening a socket. The embedded Redis API uses the same
command implementations as the server path and supports prepared commands for
hot loops.

```rust,ignore
use shardmap::redis_embedded::EmbeddedRedis;
use shardmap::protocol::Frame;

let redis = EmbeddedRedis::new(4);

assert_eq!(
    redis.execute(&[b"SET".as_slice(), b"user:42", b"ready"]),
    Frame::SimpleString("OK".into())
);

let get = redis.prepare(&[b"GET".as_slice(), b"user:42"]).unwrap();
assert_eq!(
    redis.execute_prepared(&get),
    Frame::BlobString(b"ready".to_vec())
);
```

Use a session when a caller needs per-client Redis state such as transaction
queuing.

```rust,ignore
use shardmap::redis_embedded::EmbeddedRedis;
use shardmap::protocol::Frame;

let redis = EmbeddedRedis::new(4);
let mut session = redis.session();

assert_eq!(
    session.execute(&[b"MULTI".as_slice()]),
    Frame::SimpleString("OK".into())
);
assert_eq!(
    session.execute(&[b"SET".as_slice(), b"user:42", b"queued"]),
    Frame::SimpleString("QUEUED".into())
);
assert!(matches!(session.execute(&[b"EXEC".as_slice()]), Frame::Array(_)));
```

## Exposing Embedded Storage

Enable `redis-server` when an embedded process also needs to expose its live
store to third-party services over RESP/SCNP. `ShardCacheServer` can serve a
caller-owned `ShardedEngine`, so in-process code and remote clients observe the
same data.

```rust,ignore
use std::sync::Arc;

use shardmap::config::ShardCacheConfig;
use shardmap::embedded::{ShardCacheServer, ShardedEngine};

let store = Arc::new(ShardedEngine::new(4));
store.set(b"local-key".to_vec(), b"local-value".to_vec(), None);

let mut config = ShardCacheConfig::default();
config.bind_addr = "127.0.0.1:6380".into();
config.persistence.enabled = false;

ShardCacheServer::from_embedded_store(config, store.clone())
    .run()
    .await?;
# Ok::<(), shardmap::ShardCacheError>(())
```

When serving a caller-owned store, the store's shard count and routing mode are
authoritative. The server config still controls the bind address, connection
limit, transaction mode, and worker count, but it does not create replacement
storage or apply persistence to the embedded handle. Apply memory limits with
`ShardedEngine::configure_memory_policy` before handing the store to the
server. Multi-direct embedded serving is TCP-only today; Unix sockets still use
the engine-backed path.

Embedded server deployments expose two endpoint shapes:

- `ServerEndpointMode::Fanout` (default) binds one public listener. The listener
  parses each complete RESP/SCNP request, routes single-shard commands to the
  worker that owns the shard, and rejects cross-shard public commands instead of
  letting an arbitrary server thread lock across shards.
- `ServerEndpointMode::DirectShard` exposes shard-owned direct ports in
  addition to the fanout listener. Use this for shard-aware SCNP clients that
  can route directly to the owning shard.

```rust,ignore
use shardmap::config::ServerEndpointMode;

config.server_endpoint_mode = ServerEndpointMode::DirectShard;
```

If your embedded application is already using the owner-local hot path, serve
the `LocalEmbeddedStore` installed on that owner thread instead of wrapping a
shared `Arc<ShardedEngine>`. This keeps embedded calls and third-party protocol
requests on the same shard-owned memory and avoids introducing shared
`EmbeddedStore` locks into the hot path.

```rust,ignore
use shardmap::config::ShardCacheConfig;
use shardmap::embedded::{ShardCacheServer, ShardedEngine};

let store = ShardedEngine::new(1);
store.set(b"local-key".to_vec(), b"local-value".to_vec(), None);
let local_store = store.into_local_stores(1).into_iter().next().unwrap();
local_store.install_local()?;

let mut config = ShardCacheConfig::default();
config.bind_addr = "127.0.0.1:6380".into();
config.persistence.enabled = false;

ShardCacheServer::from_thread_local_embedded_store(config)
    .run_thread_local_with_shutdown(async {
        // Signal shutdown from your owner-thread runtime.
    })
    .await?;
# Ok::<(), shardmap::ShardCacheError>(())
```

The thread-local server future is intentionally `!Send`: run it on the same
current-thread runtime or `LocalSet` that owns the local embedded store. The
server leaves the local store installed when it stops so the embedding process
can continue using or reclaim it.

The thread-local server is still a fanout endpoint shape; the difference is
storage ownership. It serves the local store already installed on the owner
thread instead of introducing a shared `Arc<ShardedEngine>` handle.

For capacity scaling without full replica copies, enable `kv-overflow` and use
`KvOverflowStore`. It mirrors each fixed logical key slot to one shardcache
server through bounded ordered workers, evicts only acknowledged cold values
from primary memory, and faults remote values back on demand. Overflow servers
can apply their own LRU/LFU memory limits and cascade cold envelopes into
object overflow. Other services can read through the same `KvOverflowCluster`
without touching the primary. See
[`docs/KV_OVERFLOW.md`](../../docs/KV_OVERFLOW.md) for the API and operational
contract. Enable `kv-overflow-redis` to use Redis, Valkey, or a compatible
managed endpoint instead of shardcache SCNP servers.

## Semantic Cache

Semantic cache entries attach a normalized embedding to the same point-key
value. Lookups search live semantic entries and return the best match at or
above the requested score.

```rust
use shardmap::ShardCache;

let cache = ShardCache::new();
cache.insert_semantic_slice(b"prompt:cat", b"cached cat answer", &[1.0, 0.0])?;
cache.insert_semantic_slice(b"prompt:dog", b"cached dog answer", &[0.0, 1.0])?;

let matched = cache.semantic_search(&[0.9, 0.1], 0.75)?.unwrap();

assert_eq!(matched.key.as_slice(), b"prompt:cat");
assert_eq!(matched.value.as_ref(), b"cached cat answer");
# Ok::<(), shardmap::SemanticCacheError>(())
```

Plain writes to a key clear its semantic embedding, so semantic hits cannot
return a value whose embedding describes an older payload. Repeated exact
semantic queries use an internal query-result cache; call
`disable_semantic_query_cache` when benchmarking the cold vector path.

## Governance Metadata

Exact byte-key values can also carry opaque governance metadata. Protected
entries fail closed through ordinary GET, mutable, visitor, removal-return, and
Redis paths; use `EmbeddedStore::get_value_bytes_with_governance_filter` to
authorize borrowed metadata before the value handle is cloned. Governance is
preserved through persistence, object overflow, KV overflow, and the
runtime-neutral mutation/snapshot contracts used by external extensions.
See [`docs/EXACT_GOVERNANCE.md`](../../docs/EXACT_GOVERNANCE.md) for the full
security and compatibility contract.

Cross-user semantic cache entries can carry opaque governance metadata. Entries
written through the default semantic APIs return `None`; applications that need
cross-user authorization can opt into the governance API layer and pass a
predicate that must approve the metadata before the cached value is released.

```rust
use shardmap::ShardCache;

let cache = ShardCache::new();
cache.insert_semantic_slice_with_governance(
    b"prompt:cat",
    b"cached cat answer",
    &[1.0, 0.0],
    b"tenant=acme;doc=cat-faq;policy=v1",
)?;

let matched = cache
    .semantic_search_with_governance_filter(&[1.0, 0.0], 0.75, |metadata| {
        metadata == Some(b"tenant=acme;doc=cat-faq;policy=v1".as_slice())
    })?
    .unwrap();

assert_eq!(matched.value.as_ref(), b"cached cat answer");
assert_eq!(
    matched.governance.as_deref(),
    Some(b"tenant=acme;doc=cat-faq;policy=v1".as_slice())
);
# Ok::<(), shardmap::SemanticCacheError>(())
```

The intended data model is:

| Field | Example | Purpose |
| --- | --- | --- |
| `key` | `semantic:tenant/acme/faq/refund-policy` | Stable cache identity for the answer. |
| `value` | cached response bytes | The answer that may be reused. |
| `embedding` | normalized prompt embedding | Semantic lookup vector. |
| `governance` | `{tenant, policy_version, allowed_groups, source_docs}` | Opaque authorization context owned by the application. |
| `ttl` | `Some(300_000)` | Optional freshness bound for the cached answer. |

The cache does not parse governance bytes. Callers can encode tenant, group,
source document, policy version, retention tier, region, or audit context in
whatever format they already use, then decide whether a semantically close
candidate may release its cached value.

## Optional Server, Protocol, And Persistence Internals

The crate also contains the storage internals used by the `shardcache` server:
command parsing, RESP/SCNP protocol code, persistence, runtime-neutral
extension contracts, and server transport modules. Those surfaces are
feature-gated so embedded users do not compile server code by default.

Most applications should start with `ShardMap<K, V>`. Use `ShardCache` when you
need raw byte cache operations, semantic-cache APIs, lock helpers, or prepared
key APIs. Use lower-level modules only when you are building a custom server,
embedding the protocol layer, or wiring storage into a specialized runtime.

## API Shape

- `ShardMap<K, V>`: native typed embedded map handle for `K: Hash + Eq`.
- `ShardMapOptions`: native typed map capacity and default TTL options.
- `ShardMapWithShards<N, K, V>`: native typed embedded map with an explicit stripe count.
- `CodecShardMap<K, V>`: feature-gated typed facade over the shared byte engine.
- `ShardCache`: raw byte cache handle.
- `ShardCacheWithShards<N>`: raw byte cache with an explicit stripe count.
- `SharedCache`: lower-level raw byte shared-cache type.
- `CacheOptions`: embedded capacity, memory, routing, and lock options.
- `get_owned` and `get_prepared_owned`: return refcounted bytes after releasing the shard read lock.
- `entry`, `get_mut`, `try_insert_slice`, and lock helpers: DashMap-style mutation and coordination APIs.
- `insert_semantic_slice` and `semantic_search`: native semantic-cache APIs.
- `semantic_search_with_governance_filter`: semantic cache lookup with request-specific authorization.

## Features

| Feature | Default | Purpose |
| --- | --- | --- |
| `sharded` | Yes | Embedded sharded map/cache API. |
| `redis` | No | Redis/Valkey object and command behavior for shared internals. |
| `redis-functions` | Via `redis-server` | Redis 7 `FUNCTION`/`FCALL` compatibility stubs with an empty function registry. |
| `redis-modules` | Via `redis-server` | Redis `MODULE` compatibility stubs with an empty module registry and disabled loading. |
| `redis-modules-all` | No | Aggregate Redis Modules compatibility facades, concrete command discovery metadata, and embedded APIs; individual `redis-module-*` flags can enable one module family at a time. |
| `server` | No | TCP server internals used by the `shardcache` package. |
| `redis-server` | No | Server internals plus Redis/Valkey compatibility. |
| `codec` | No | `CodecShardMap` and codec traits for namespaced typed facades over one shared byte engine. |
| `telemetry` | No | Embedded operational metrics. |
| `parent-telemetry-runtime` | No | Embedded telemetry constructors that use a parent-provided fast-telemetry runtime or create a shardmap-owned fallback. |
| `monoio` | No | Linux-only server transport internals. |
| `prefix-eviction` | No | Enables `EvictionPolicy::Prefix` for prefix-group memory-limit eviction. |
| `object-overflow` | No | Cold-value overflow into filesystem object storage. |
| `object-overflow-s3` | No | S3/RustFS-compatible object overflow adapter. |
| `kv-overflow` | No | Fixed-slot overflow into shardcache SCNP servers. |
| `kv-overflow-redis` | No | Redis/Valkey-compatible endpoint adapter for fixed-slot KV overflow. |
| `scnp-tls` | No | Rustls TLS 1.3 and mTLS for shard-owned SCNP overflow connections. |

The internal `experimental-compact-point-storage` flag opts into an
unqualified layout for plain byte keys up to 64 bytes and values up to 256
bytes. Borrowed reads use fixed 4 KiB boxed payload chunks, with two-byte record
size classes. Fixed list heads track available chunks without separate class
allocations. Workloads with many distinct lengths can retain more partially
filled chunks. Small RESP GET responses encode those slices directly;
explicit shared/owned `Bytes` reads lazily copy
one owner per value version. Supplied `Bytes` buffers without unique ownership
use general storage; raw `value_mut_no_ttl` access rejects outstanding owned
aliases in either layout. Deletes and length changes reuse records, and
TTL/governance/semantic/overflow metadata migrates only the affected key into
general storage. Both layouts remain visible to counts, scans, snapshots,
recovery, and runtime memory policies. Payload allocations never relocate;
compact payload capacity is capped at 64 MiB per shard, after which new records
use general storage. Open read epochs retain replaced records and materialized
owners, with at most 32 compact records reclaimed per allocator call once readers
leave (at most 64 in a point mutation that falls back, and 32 in maintenance). See [SAFETY.md](SAFETY.md) and the
[memory-density qualification plan](../../benchmarks/REDIS_MEMORY_DENSITY.md)
before evaluating this feature for a workload.

## License

Licensed under Apache-2.0.

## Same-host service succession (opt-in)

Enable `succession` on 64-bit Linux and create `succession::Cache` **before
warming the service**. This experimental bounded point-cache profile retains
native `hashbrown::HashTable` indexes and fixed key/value allocations in a
size-sealed anonymous memfd. It uses the same hash-table family, randomized local
bucket hashing, XXH3 shard routing, cached TTL clock and cache-padded reader-biased
shard locks as the ordinary engine. It does not import an existing private-heap
`SharedCache`, or support the full engine's object, governance or overflow state.

A/B call the same public `get`, `insert` and `remove` implementation. Attachment
maps the existing native tables and payloads without traversing/rebuilding the
index or moving/copying cached bytes. Reads borrow from the mapping under one
shard lock. Ordinary operations perform no handoff admission/generation checks,
global cache lock or coordinator IPC. Inserts copy application input as usual.

```rust,ignore
use shardmap::succession::{protect_current_process, Cache, Options};
use std::time::Duration;

protect_current_process()?; // Explicit process-wide protection, described below.
let cache = Cache::create(Options {
    capacity: 4096, max_key_bytes: 32, max_value_bytes: 256,
    default_ttl_ms: None,
})?;
cache.insert(b"key", b"value", None)?;
// Check the supervisor-owned PID/UID before closing admission or preparing.
shardmap::succession::authenticate_peer(&private_channel, successor_peer)?;
// Workers may clone handles. First close service admission and return/drop all
// worker handles, borrowed views and mutations. Preparation consumes the cache.
let mut participant = match cache.prepare(operation, successor_peer, Duration::from_secs(2)) {
    Ok(participant) => participant,
    Err(failure) => {
        let still_active_cache = failure.cache;
        // Resume with this handle or return remaining workers and retry.
        return handle_prepare_failure(failure.error, still_active_cache);
    }
};
participant.send(&mut private_channel)?;
// cache is moved: stale entry access is unavailable, with no check on every GET.
// A recorded ABORT returns the original backing as participant.abort()?;
// B has no entry APIs until unsafe Staged::receive(...)? and activate(operation).
```

Preparation waits within the configured bound for every other active Cache handle
to drop. Borrowed guards cannot outlive their handle or be moved past prepare.
There is no implicit worker cancellation; the caller owns common request admission
and worker shutdown. A timeout returns `PrepareError { error, cache }` with the
original active handle and acknowledged writes. Dropping that error also drops
its handle, so recover it explicitly when resuming. On successful prepare, only
`Prepared` remains in A. `Prepared::abort` returns a new active Cache on the same
backing; dropping Prepared never guesses that the global decision was ABORT.

Entry/key/value bounds reserve at most 1 GiB. Small profiles have one shard;
profiles with at least 256 slots have 64. `capacity` is a total slot budget divided
into fixed shard quotas, exposed by `shard_count`/`shard_capacity`. A full routed
shard refuses insertion even if another shard has spare slots. There is no spill,
implicit eviction or cache growth. Deletes and expired entries release slots.
Indexes reserve at least twice each shard's quota so deletion churn can clean
hash-table tombstones in place without allocating another bucket array. This
headroom is part of the reserved memory budget and benchmark comparison.

TTL uses the ordinary engine's cached Unix milliseconds (approximately 1 ms
resolution, exact-clock fallback if its updater cannot start). Absolute deadlines
remain unchanged across custody transfer. Wall-clock adjustments have the same
effect as in the ordinary cache. `None` selects the configured default TTL; zero
expires immediately. `no-ttl` does not disable this profile's TTL checks. Expiring
matches check the clock; non-expiring reads skip it. Typed objects, Redis commands,
semantic/governance metadata, persistence and overflow are unsupported. This
feature does not make the existing server or an Eden deployment handoff-capable.

### Native compatibility and address requirements

Format 4 contains native Rust/hashbrown/allocator metadata and pointers.
`Staged::receive` requires the exact engine build. The fingerprint covers engine
Rust sources, compiler identity, target/features/build flags and locked inputs.
Both receivers validate the native geometry before accessing the table.

For a single A → B upgrade, `Staged::receive_qualified` accepts a locally
provisioned `QualifiedBuildPair` whose two exact artifacts have been qualified for
the same native layout, allocator, hashing and cache profile. The trusted
supervisor pins both executables and obtains peer PIDs from its own child
custody. B checks its own compiled digest before receiving a descriptor and
matches the expected source digest to A's authenticated receipt and immutable
backing header. A uses the original Format 4 protocol. Pair authorization must
come from the supervisor, independent of the offered receipt. Inherited backing
continues to serve through B after A exits. A later attempt to prepare that
backing returns B's active cache immediately with `Error::Profile`, before
parking it or waiting for workers.

A/B must map the **same available virtual range** so native index pointers and
payload addresses remain valid. `MAP_FIXED_NOREPLACE` refuses a conflict and never
overwrites another mapping. A can abort and resume its original cache after an
import refusal. There is no relocation fallback. This candidate does **not**
support different mapping bases; EDEN-3839 qualifies this fixed-address profile.
Compare store UUID, kernel device/inode, mapped size, offsets and generation as
well as addresses. Format 1/2/3 experimental backing is rejected without conversion.

### Protected custody and coordinated decisions

Both independently executed services must call `protect_current_process()` after
final credential changes and before holding backing, then retain non-dumpability
for the entire descriptor/mapping lifetime. Creation, prepare, send and activation
refuse an insecure setting. This explicit Linux process policy disables core dumps
and restricts debuggers/unprivileged `/proc` descriptor access; file mode 0600 and
a transfer token alone do not isolate same-UID processes. Privileged host
administrators and admitted native code remain trusted.

Every created/imported mapping also requires `MADV_DONTDUMP` and
`MADV_DONTFORK`; a failed protection call releases the local mapping and
descriptor. The cache mapping is excluded from ordinary core dumps and fork
inheritance. Descriptors are close-on-exec. Forked trusted helpers can still
inherit a descriptor until exec/close; do not fork active Cache handles or let
untrusted native helpers into this process. These controls do not sandbox root
or code already admitted to the service.

`Cache::create(options)` selects `MemoryPolicy::Pageable`. For caches that must
stay out of normal swap, use
`Cache::create_with_memory_policy(options, MemoryPolicy::Locked)`. Every owner
must successfully `mlock` the entire mapping before creation/readiness succeeds;
the authenticated receipt binds this policy, with no pageable fallback. Budget
RAM and `RLIMIT_MEMLOCK` for the full reserved backing rounded to kernel pages.
Locking applies to this
mapping, not input buffers, other application copies or hibernation images. No
process-wide `mlockall` or per-operation security syscall is used.

Before announcing readiness, B may explicitly call `staged.prefault()?` to
populate its read page tables. This optional Linux 5.14+ preparation leaves the
backing bytes unchanged; it is off unless called, and locked backing is already
populated. Unsupported kernels and other failures return an I/O error so the
caller can abort before commit. Budget its preparation time separately from the
first read batch: it moves page-fault work into preparation and adds no logic to
ordinary reads or writes.

Transfer requires the memfd and a random one-use credential bound to the complete
readiness receipt, including exact operation, recipient, generation, profile,
engine identity, mapping address and residency policy. HMAC-SHA256 binds that
receipt; verification uses the HMAC crate's constant-time comparison. Credential
buffers are RAII-zeroized, including partial-frame failures; never log them.
SO_PEERCRED authenticates the supervisor-owned child PID/UID at both ends.
Call `authenticate_peer` on each accepted connection **before** quiescing A;
reject/close wrong peers with a bounded accept deadline. Send/import recheck
identity. Use a private supervisor-controlled Unix channel with
bounded I/O. PIDs from an untrusted registration request are insufficient. A send
right is consumed even on partial I/O. Do not log credentials. Both unsafe
receivers require trusted builds and exclusive custody without raw competing
writes, forked active handles or untracked aliases. Withdrawing a credential
cannot revoke an admitted mapping.

The caller owns one common request/I/O admission gate and one recorded decision
covering the cache, **both TLS/socket legs and an embedded relational DB when
present**. Stop requests/background mutations/I/O, return all cache workers, and
prepare every participant while B stays inactive. Keep A recoverable until all
readiness receipts agree. Failed readiness before COMMIT aborts every participant;
`Prepared::abort` returns A's cache without an index rebuild or payload movement.

After recording global COMMIT, keep admission closed while B activates the entire
bundle, then retire A before reporting the upgrade effective. The cache CAS
prevents abort racing activation from exposing two owners; it is not the
cross-resource coordinator. Lost acknowledgements never permit stale A to resume.
Reconcile an uncertain decision using B's latest cache/TLS/SQL state and security
floor. Do not rewind TLS counters/SQL effects or retry ambiguous writes. In-place
mutation crashes and recovery after losing every descriptor/mapping are not
supported. A must not remain as a permanent cache relay.

### Reproduction

Run `cargo test --locked -p shardmap --features succession` for the public API,
borrow/worker drain, capacity/TTL/churn and authorization/ownership regressions.
`python3 scripts/prove-cache-succession.py /path/to/evidence` builds distinct
interface executables and an incompatible-feature artifact. It checks retained
index/payload bytes, same pointers, B after A exit, explicit/ambiguous decisions,
address/ABI refusal and crashes on both sides of activation. It deliberately does
not claim different-base attachment or full TLS/SQL atomicity.
Pass `--predecessor-root /path/to/eden-3839-checkout` to build A from the exact
EDEN-3839 merge and B from this revision. The evidence checks both engine
digests, retained index and payload bytes, CRUD and TTL after A exits, refusal
without a trusted pair, and continued service after B refuses to re-export.
`python3 scripts/prove-cache-security.py /path/to/evidence` separately checks
default heap behavior, mapping flags, fork/exec inheritance, pageable behavior
under a zero locking limit, and cleanup after required protection/locking fails.

`python3 scripts/benchmark-cache-succession.py /path/to/evidence` measures the
actual public implementation on matched physical-core affinity. Controls include
ordinary heap reservation, matched native bucket reservation, local native access
and a separately executed successor timed only after the old owner exits. Raw
trials, source/artifact identities, reserved backing and per-process resource
measurements are saved. Aggregate ns/op is a throughput reciprocal, not tail
latency or the cost of a complete application request. The bounded profile is
experimental; consuming services must qualify their exact engine/profile and
coordinated cache/TLS/SQL decision.
Use `--cold` to compare the first 8,192 reads with optional prefaulting after A
exits, before B warms or hashes any cache entry. This records preparation cost,
first-batch time and minor faults separately. `--binary` accepts an exact
prebuilt artifact, allowing compilation and target-host measurement to be
separated; preserve its source/compiler/hash manifest with the results.
