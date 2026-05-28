# shardmap

`shardmap` is the embedded Rust map/cache crate for `shard-kv`. It gives
applications a cloneable, sharded in-process handle with byte-oriented keys and
values, TTL support, memory-limit eviction, and route-aware lower-level APIs
for callers that already partition work by shard.

Use `shardmap` when you want the embedded surface. Use `shardcache` from the
repository when you need a TCP server.

## Install

```toml
[dependencies]
shardmap = "0.1.0"
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

## Common Operations

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

TTL values are expressed in milliseconds:

```rust
use shardmap::ShardMap;

let cache = ShardMap::new();
cache.insert_slice_with_ttl(b"session:1", b"active", Some(30_000));

assert!(cache.contains_key(b"session:1"));
```

Semantic cache entries attach a normalized embedding to the same point-key
value. Lookups perform an exact cosine search across live entries and return
the best match at or above the requested score:

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

Cross-user semantic cache entries can also carry opaque governance metadata.
Applications can return the metadata with a match and validate it themselves,
or pass a predicate that must approve the metadata before the cached value is
released:

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
        metadata == b"tenant=acme;doc=cat-faq;policy=v1"
    })?
    .unwrap();

assert_eq!(matched.value.as_ref(), b"cached cat answer");
assert_eq!(matched.governance.as_ref(), b"tenant=acme;doc=cat-faq;policy=v1");
# Ok::<(), shardmap::SemanticCacheError>(())
```

Plain writes to a key clear its semantic embedding, so semantic hits cannot
return a value whose embedding describes an older payload.

For repeated hot lookups, prepare the key once and reuse the route metadata:

```rust
use shardmap::ShardMap;

let cache = ShardMap::new();
cache.insert_slice(b"feature:alpha", b"enabled");

let prepared = cache.prepare_key(b"feature:alpha");
let value = cache.get_prepared_owned(&prepared).unwrap();

assert_eq!(value.as_ref(), b"enabled");
```

## Configuration

`CacheOptions` controls the shared-handle embedded cache. The default
`ShardMap` uses 64 stripes.

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

## API Shape

- `ShardMap`: default embedded map/cache handle.
- `ShardCache`: cache-flavored alias for `ShardMap`.
- `ShardMapWithShards<N>`: embedded handle with an explicit stripe count.
- `CacheOptions`: embedded capacity, memory, routing, and lock options.
- `get_owned` and `get_prepared_owned`: return refcounted bytes after releasing the shard read lock.
- `entry`, `get_mut`, `try_insert_slice`, and lock helpers: DashMap-style mutation and coordination APIs.

Lower-level modules expose the same storage engine used by the `shardcache`
server for direct shard ownership, SCNP/RESP protocol support, persistence,
and replication. Most embedded applications should start with `ShardMap`.

## Features

| Feature | Default | Purpose |
| --- | --- | --- |
| `sharded` | Yes | Embedded sharded map/cache API. |
| `redis` | No | Redis/Valkey object and command behavior for shared internals. |
| `server` | No | TCP server internals used by the source-only `shardcache` package. |
| `redis-server` | No | Server internals plus Redis/Valkey compatibility. |
| `telemetry` | No | Embedded operational metrics. |
| `monoio` | No | Linux-only server transport internals. |
| `prefix-eviction` | No | Enables `EvictionPolicy::Prefix` for prefix-group memory-limit eviction. |

## License

Licensed under Apache-2.0.
