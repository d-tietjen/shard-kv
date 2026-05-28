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
Entries written through the default semantic APIs return `None`; applications
that need cross-user authorization can opt into the governance API layer and
pass a predicate that must approve the metadata before the cached value is
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

The intended customer data model is:

| Field | Example | Purpose |
| --- | --- | --- |
| `key` | `semantic:tenant/acme/faq/refund-policy` | Stable cache identity for the answer. |
| `value` | cached response bytes | The answer that may be reused. |
| `embedding` | normalized prompt embedding | Semantic lookup vector. |
| `governance` | `{tenant, policy_version, allowed_groups, source_docs}` | Opaque authorization context owned by the application. |
| `ttl` | `Some(300_000)` | Optional freshness bound for the cached answer. |

The cross-user request flow is:

1. User A asks a question and the application generates an answer from source
   documents.
2. The application stores the answer with `insert_semantic_slice_with_governance`
   or `insert_semantic_slice_with_ttl_and_governance`, using governance bytes
   that identify the tenant, policy version, allowed groups, source documents,
   or any other application-specific access context.
3. User B asks a similar question. The application embeds the request and calls
   `semantic_search_with_governance_filter`.
4. ShardMap considers semantic candidates, but it returns a cached value only
   when the filter approves the candidate's `Option<&[u8]>` governance metadata
   for User B.
5. If no semantically close and authorized entry exists, the application treats
   the lookup as a miss and generates a fresh answer.

This keeps governance policy outside the cache engine while making the
authorization boundary explicit in the cache API.

Here is a complete in-process authorization example using compact metadata
bytes. A production application could use JSON, protobuf, bitsets, signed
policy claims, or any other application-owned format; ShardMap only stores and
returns the opaque bytes.

```rust
use shardmap::ShardMap;

struct RequestUser<'a> {
    tenant: &'a str,
    groups: &'a [&'a str],
    allowed_docs: &'a [&'a str],
    min_policy_version: u32,
}

fn csv_has_any(csv: &str, allowed: &[&str]) -> bool {
    csv.split(',').any(|value| allowed.contains(&value))
}

fn csv_all_allowed(csv: &str, allowed: &[&str]) -> bool {
    csv.split(',').all(|value| allowed.contains(&value))
}

fn can_use_cached_answer(user: &RequestUser<'_>, metadata: &[u8]) -> bool {
    let Ok(metadata) = std::str::from_utf8(metadata) else {
        return false;
    };

    let mut tenant_ok = false;
    let mut group_ok = false;
    let mut docs_ok = false;
    let mut policy_ok = false;

    for field in metadata.split(';') {
        let Some((name, value)) = field.split_once('=') else {
            continue;
        };
        match name {
            "tenant" => tenant_ok = value == user.tenant,
            "groups" => group_ok = csv_has_any(value, user.groups),
            "docs" => docs_ok = csv_all_allowed(value, user.allowed_docs),
            "policy" => {
                policy_ok = matches!(
                    value.parse::<u32>(),
                    Ok(version) if version >= user.min_policy_version
                );
            }
            _ => {}
        }
    }

    tenant_ok && group_ok && docs_ok && policy_ok
}

let cache = ShardMap::new();
cache.insert_semantic_slice_with_governance(
    b"semantic:tenant/acme/faq/refund-policy",
    b"Refunds are available within 30 days.",
    &[1.0, 0.0],
    b"tenant=acme;groups=support,billing;docs=doc_481,doc_902;policy=7",
)?;

let support_user = RequestUser {
    tenant: "acme",
    groups: &["support"],
    allowed_docs: &["doc_481", "doc_902"],
    min_policy_version: 7,
};

let hit = cache
    .semantic_search_with_governance_filter(&[0.95, 0.05], 0.75, |metadata| {
        metadata.is_some_and(|bytes| can_use_cached_answer(&support_user, bytes))
    })?
    .unwrap();

assert_eq!(hit.value.as_ref(), b"Refunds are available within 30 days.");

let sales_user = RequestUser {
    tenant: "acme",
    groups: &["sales"],
    allowed_docs: &["doc_481", "doc_902"],
    min_policy_version: 7,
};

let blocked = cache.semantic_search_with_governance_filter(
    &[0.95, 0.05],
    0.75,
    |metadata| metadata.is_some_and(|bytes| can_use_cached_answer(&sales_user, bytes)),
)?;

assert!(blocked.is_none());
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
