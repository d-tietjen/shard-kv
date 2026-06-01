# shardmap

`shardmap` is the embedded Rust map/cache crate for `shard-kv`. It gives
applications a cloneable, sharded in-process handle with byte-oriented keys and
values, TTL support, memory-limit eviction, lock helpers, prepared-key lookups,
and native semantic-cache APIs.

Use `shardmap` when you want an embedded Rust cache. Use the repository's
`shardcache` server package when you need a TCP service.

## Install

```toml
[dependencies]
shardmap = "0.2.1"
```

## Quick Start

```rust
use shardmap::ShardMap;

let cache = ShardMap::new();

cache.insert_slice(b"user:42", b"ready");
let value = cache.get_owned(b"user:42").unwrap();

assert_eq!(value.as_ref(), b"ready");
```

`ShardMap` is a cheap cloneable handle. Clones share the same underlying
sharded store and can be moved into worker threads.

## Feature Overview

| Area | What it gives you | Example |
| --- | --- | --- |
| Point-key map | Insert, get, mutate, remove, and entry-style access for byte keys. | [`basic_map.rs`](examples/basic_map.rs) |
| TTL cache | Relative TTL writes and memory-limit eviction. | [`ttl_and_locks.rs`](examples/ttl_and_locks.rs) |
| Prepared keys | Route metadata for repeated hot-key lookups. | [`prepared_keys_threads.rs`](examples/prepared_keys_threads.rs) |
| Entry API | Occupied/vacant mutation without a separate lookup. | [`entry_api.rs`](examples/entry_api.rs) |
| Route inspection | See which shard owns a key before sending work to a worker. | [`route_inspection.rs`](examples/route_inspection.rs) |
| Lock helpers | Process-local token locks built on `SET key token NX PX ttl` semantics. | [`ttl_and_locks.rs`](examples/ttl_and_locks.rs) |
| Configuration | Capacity hints, memory budgets, eviction policy, routing, and lock policy. | [`configured_cache.rs`](examples/configured_cache.rs) |
| Redis-compatible embedded API | Execute supported Redis commands directly in-process, including prepared commands and session state. | Enable the `redis` feature |
| Semantic cache | Store embeddings with cached values and search by cosine similarity. | [`semantic_cache.rs`](examples/semantic_cache.rs) |
| Semantic TTL | Combine semantic reuse with freshness windows. | [`semantic_ttl.rs`](examples/semantic_ttl.rs) |
| Governance metadata | Attach application-owned authorization context to semantic hits. | [`semantic_cache.rs`](examples/semantic_cache.rs) |
| Mini app | A small feature-flag cache combining TTL, prepared keys, and locks. | [`mini_feature_flags.rs`](examples/mini_feature_flags.rs) |

Run any example with:

```bash
cargo run -p shardmap --example basic_map
```

## Point-Key Map Operations

Use the default `ShardMap` for a 64-stripe shared embedded map, or
`ShardMapWithShards<N>` when you want to choose the stripe count at compile
time.

```rust
use shardmap::ShardMap;

let cache = ShardMap::with_capacity(1024);

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

```rust
use shardmap::ShardMap;

let cache = ShardMap::new();
cache.insert_slice_with_ttl(b"session:1", b"active", Some(30_000));

assert!(cache.contains_key(b"session:1"));
```

`CacheOptions` configures the shared-handle cache. Memory limits are enforced
inside each stripe, using the selected eviction policy.

```rust
use shardmap::{CacheOptions, ShardMap};
use shardmap::config::EvictionPolicy;

let cache = ShardMap::with_options(CacheOptions {
    capacity_hint: Some(32_768),
    total_memory_bytes: Some(256 * 1024 * 1024),
    eviction_policy: EvictionPolicy::Lru,
    ..CacheOptions::default()
});

assert_eq!(cache.shard_count(), 64);
```

`EvictionPolicy::Lru` and `EvictionPolicy::Lfu` are available in the default
crate. `EvictionPolicy::Prefix` is available with the `prefix-eviction`
feature for prefix-group cache workloads.

## Prepared Keys And Concurrency

For repeated hot lookups, prepare the key once and reuse the route metadata.

```rust
use shardmap::ShardMap;

let cache = ShardMap::new();
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
use shardmap::ShardMap;

let cache = ShardMap::new();
let value = cache.entry(Bytes::from_static(b"job:42"))
    .or_insert(Bytes::from_static(b"queued"));

assert_eq!(value.value().unwrap(), b"queued");
```

Use `route_key` when your application already partitions work by shard and
wants to send a key to its owning worker.

```rust
use shardmap::ShardMapWithShards;

let cache = ShardMapWithShards::<8>::new();
let route = cache.route_key(b"user:42");

assert!(route.shard_id < cache.shard_count());
```

## Lock Helpers

The lock helpers are useful for process-local coordination in embedded mode.
They acquire only when the key is absent or expired, release only when the
stored token matches, and renew by extending the TTL for the matching token.

```rust
use shardmap::ShardMap;

let cache = ShardMap::new();

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

For read replicas or service subscribers, wrap the embedded source in
`ReplicatedEmbeddedStore` and start its native replication listener.

```rust,ignore
use std::sync::Arc;

use shardmap::config::{ReplicationConfig, ReplicationRole};
use shardmap::embedded::{ReplicatedEmbeddedStore, ReplicationReplicaClient};

let mut primary_config = ReplicationConfig {
    enabled: true,
    role: ReplicationRole::Primary,
    bind_addr: "127.0.0.1:7631".into(),
    ..ReplicationConfig::default()
};

let primary = Arc::new(ReplicatedEmbeddedStore::new(4, primary_config.clone())?);
let _listener = primary.serve_replicas(primary_config)?;

let replica = ReplicationReplicaClient::start(ReplicationConfig {
    enabled: true,
    role: ReplicationRole::Replica,
    replica_of: Some("127.0.0.1:7631".into()),
    ..ReplicationConfig::default()
})?;
# Ok::<(), shardmap::ShardCacheError>(())
```

Native replication v1 streams byte-string cache mutations and consistent
snapshots. It is intended for read replicas, sidecar cache mirrors, and service
subscribers that consume shardcache's FCRP frames; Redis object-family
replication is outside this embedded replication surface.

## Semantic Cache

Semantic cache entries attach a normalized embedding to the same point-key
value. Lookups search live semantic entries and return the best match at or
above the requested score.

```rust
use shardmap::ShardMap;

let cache = ShardMap::new();
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

Cross-user semantic cache entries can carry opaque governance metadata. Entries
written through the default semantic APIs return `None`; applications that need
cross-user authorization can opt into the governance API layer and pass a
predicate that must approve the metadata before the cached value is released.

```rust
use shardmap::ShardMap;

let cache = ShardMap::new();
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
command parsing, RESP/SCNP protocol code, persistence, replication, and server
transport modules. Those surfaces are feature-gated so embedded users do not
compile server code by default.

Most applications should start with `ShardMap`. Use lower-level modules only
when you are building a custom server, embedding the protocol layer, or wiring
storage into a specialized runtime.

## API Shape

- `ShardMap`: default embedded map/cache handle.
- `ShardCache`: cache-flavored alias for `ShardMap`.
- `ShardMapWithShards<N>`: embedded handle with an explicit stripe count.
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
| `telemetry` | No | Embedded operational metrics. |
| `monoio` | No | Linux-only server transport internals. |
| `prefix-eviction` | No | Enables `EvictionPolicy::Prefix` for prefix-group memory-limit eviction. |

## License

Licensed under Apache-2.0.
