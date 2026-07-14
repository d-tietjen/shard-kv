# shard-kv

`shard-kv` is an embedded KV-cache path for LMCache/vLLM-style inference
offload, with a source-built server mode for general caching and compatibility
work.

The primary wedge is in-process storage: keep KV-cache payloads in the model
serving process, avoid Redis/TCP on the hot path, and expose memory bandwidth
through sharded, route-aware Rust storage. The TCP server, Redis/Valkey command
surface, persistence, and replication code are useful secondary surfaces, but
they are not the recommended path for GPU KV-cache restore.

Recorded proof artifacts live under [`benchmarks`](benchmarks/README.md). The
current LMCache report includes a Linux plugin run at `40.107 GB/s` for 1 MiB
GET payloads and local Apple M5 Max reruns above `77 GB/s` for 1 MiB embedded
GET payloads. Treat those as hardware-specific proof rows, not universal
numbers; the memory-ceiling report records the denominator for local
hardware-scaled claims.

This workspace contains three publishable Rust crate surfaces:

- `shardmap`: the published embedded Rust map/cache crate.
- `shardcache`: the Redis/Valkey-style server and Docker image, including
  Redis-compatible command support behind feature flags.
- `shardcache-client-rs`: the blocking Rust SCNP client.

Python, C ABI, runtime, benchmark, and integration packages remain built from
this repository for now.

## Start Here

| Surface | Doc | Use When |
| --- | --- | --- |
| Embedded Rust map/cache | [`crates/shardmap/README.md`](crates/shardmap/README.md) | You want an in-process, DashMap-like cache with sharding, resident-by-default storage, and optional TTL or memory-limit eviction. |
| Embedded C ABI | [`crates/shardcache-c/README.md`](crates/shardcache-c/README.md) | You want to embed shardcache from C, C++, Python, Go, Java, Node, .NET, or any FFI-capable runtime. |
| Native Rust client | [`crates/shardcache-client-rs/README.md`](crates/shardcache-client-rs/README.md) | You want a blocking Rust client for shardcache over SCNP, including optional Redis command helpers. |
| LMCache storage backend | [`integrations/lmcache_storage_backend/README.md`](integrations/lmcache_storage_backend/README.md) | You want LMCache to store KV-cache payloads in embedded shardcache or a shardcache TCP server. |
| vLLM/direct runtime | [`crates/shardcache-runtime/README.md`](crates/shardcache-runtime/README.md) | You want the experimental host/GPU restore path, including CUDA direct-DMA scaffolding. |
| shardcache Docker/server | [`docs/SHARDCACHE_DOCKER.md`](docs/SHARDCACHE_DOCKER.md) | You want to build and run the server locally or in a private container registry. |
| Benchmarks | [`benchmarks/README.md`](benchmarks/README.md) | You want reproducible head-to-head and hardware-ceiling artifacts. |
| Prefix-aware eviction | [`docs/PREFIX_AWARE_EVICTION.md`](docs/PREFIX_AWARE_EVICTION.md) | You want the feature-gated KV-cache hit-rate policy boundary beyond LRU/LFU. |
| Partitioned KV overflow | [`docs/KV_OVERFLOW.md`](docs/KV_OVERFLOW.md) | You want an in-memory primary whose acknowledged cold data and read traffic can scale across shard-owned SCNP or Redis/Valkey overflow capacity. |

## Quick Starts

Embedded `shardmap`:

```toml
[dependencies]
shardmap = "0.6.0"
```

```rust
use shardmap::ShardMap;

let cache: ShardMap<String, String> = ShardMap::new();
cache.insert("user:42".to_owned(), "ready".to_owned());
assert_eq!(cache.get("user:42").as_deref(), Some("ready"));
```

`shardcache` server:

```sh
cargo run -p shardcache -- --bind-addr 127.0.0.1:6380 --disable-persistence
```

For LMCache/vLLM KV-cache storage, prefer the embedded LMCache backend first.
Use the TCP server for shared/general caching or explicit networked comparisons.

Local Docker image:

```sh
docker compose up --build shardcache
```

## Examples

The embedded crate includes runnable examples for the major API areas:

```sh
cargo run -p shardmap --example basic_map
cargo run -p shardmap --example configured_cache
cargo run -p shardmap --example prepared_keys_threads
cargo run -p shardmap --example ttl_and_locks
cargo run -p shardmap --example semantic_cache
```

See [`crates/shardmap/examples`](crates/shardmap/examples/README.md) for the
full list, including entry API, route inspection, semantic TTL, and a small
feature-flag cache app.

## Semantic Cache Governance

The native semantic-cache path supports cross-user cache reuse without baking
authorization logic into the cache engine. Default semantic entries have
`governance: None`. Customers that need governed reuse opt into
`insert_semantic_slice_with_governance` or
`insert_semantic_slice_with_ttl_and_governance`, store opaque policy bytes with
the cached response, and then call `semantic_search_with_governance_filter`.

A typical application model is:

```text
key        = semantic:tenant/acme/faq/refund-policy
value      = cached response bytes
embedding  = normalized prompt embedding
governance = {tenant: acme, policy_version: 7,
              allowed_groups: [support, billing],
              source_docs: [doc_481, doc_902]}
ttl        = optional freshness window
```

The filter receives `Option<&[u8]>` for each semantic candidate and must approve
the metadata before ShardCache releases the cached value. If no candidate is
both semantically close and authorized for the requesting user, the application
treats the lookup as a miss and computes a fresh answer.

The metadata also makes semantic caching more useful operationally:

- It gives each cache hit an application-owned reason for reuse: tenant,
  groups, source documents, policy version, retention tier, or region.
- It lets applications reject stale hits after ACL, document, or policy changes
  without disabling semantic caching for everyone.
- It creates an audit trail for cross-user reuse, which is the common
  production semantic-cache case.
- It lets teams measure hit rate by tenant, policy, or document class instead
  of treating the semantic cache as a single opaque counter.

The default path stays lightweight. Entries written through the normal semantic
APIs have no governance metadata, and governed metadata is only checked when a
candidate is being considered for release.

Example usage:

```rust
cache.insert_semantic_slice_with_governance(
    b"semantic:tenant/acme/faq/refund-policy",
    response_bytes,
    &embedding,
    b"tenant=acme;groups=support;docs=doc_481;policy=7",
)?;

let hit = cache.semantic_search_with_governance_filter(
    &request_embedding,
    0.75,
    |metadata| {
        metadata.is_some_and(|bytes| {
            user.tenant == "acme"
                && user.groups.contains(&"support")
                && std::str::from_utf8(bytes)
                    .is_ok_and(|text| text.contains("docs=doc_481"))
        })
    },
)?;
```

Server deployments can use the RESP semantic commands when the cache is running
as `shardcache`:

```text
SEMANTIC.SET <key> <value> <embedding-f32le> [governance-bytes]
SEMANTIC.SEARCH <embedding-f32le> <min-score>
```

`embedding-f32le` is the normalized vector encoded as little-endian `f32`
bytes. `SEMANTIC.SEARCH` returns `nil` on miss or `[key, value, score,
governance]` on hit; `governance` is `nil` unless the entry was stored with the
optional metadata argument.

## Workspace

- `crates/shardmap`: published embedded sharded map/cache crate plus shared internals.
- `crates/shardcache`: published Redis/Valkey-compatible server package and binary.
- `crates/shardcache-client-rs`: published blocking Rust client for SCNP.
- `crates/shardcache-py`: source-only PyO3 bindings for Python integrations.
- `integrations`: LMCache and model-serving integration adapters.
- `benchmarks`: benchmark and compatibility harnesses.
