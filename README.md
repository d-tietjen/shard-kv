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

This workspace contains two related sharded key/value surfaces:

- `shardmap`: the published embedded Rust map/cache crate.
- `shardcache`: the source-only Redis/Valkey-style server and Docker image.

The 0.1.x crates.io release ships only `shardmap` and
`shardcache-client-rs`. The server, Docker image, Python bindings, and
integration packages are built from this repository.

## Start Here

| Surface | Doc | Use When |
| --- | --- | --- |
| Embedded Rust map/cache | [`crates/shardmap/README.md`](crates/shardmap/README.md) | You want an in-process, DashMap-like cache with sharding, TTL, and memory-limit eviction. |
| Embedded C ABI | [`crates/shardcache-c/README.md`](crates/shardcache-c/README.md) | You want to embed shardcache from C, C++, Python, Go, Java, Node, .NET, or any FFI-capable runtime. |
| Native Rust client | [`crates/shardcache-client-rs/README.md`](crates/shardcache-client-rs/README.md) | You want a blocking Rust client for shardcache over SCNP, including optional Redis command helpers. |
| LMCache storage backend | [`integrations/lmcache_storage_backend/README.md`](integrations/lmcache_storage_backend/README.md) | You want LMCache to store KV-cache payloads in embedded shardcache or a shardcache TCP server. |
| vLLM/direct runtime | [`crates/shardcache-runtime/README.md`](crates/shardcache-runtime/README.md) | You want the experimental host/GPU restore path, including CUDA direct-DMA scaffolding. |
| shardcache Docker/server | [`docs/SHARDCACHE_DOCKER.md`](docs/SHARDCACHE_DOCKER.md) | You want to build and run the source-only server locally or in a private container registry. |
| Benchmarks | [`benchmarks/README.md`](benchmarks/README.md) | You want reproducible head-to-head and hardware-ceiling artifacts. |
| Prefix-aware eviction | [`docs/PREFIX_AWARE_EVICTION.md`](docs/PREFIX_AWARE_EVICTION.md) | You want the feature-gated KV-cache hit-rate policy boundary beyond LRU/LFU. |

## Quick Starts

Embedded `shardmap`:

```toml
[dependencies]
shardmap = "0.1.0"
```

```rust
use shardmap::ShardMap;

let cache = ShardMap::new();
cache.insert_slice(b"user:42", b"ready");
assert_eq!(cache.get_owned(b"user:42").unwrap().as_ref(), b"ready");
```

Source-built `shardcache` server:

```sh
cargo run -p shardcache -- --bind-addr 127.0.0.1:6380 --disable-persistence
```

For LMCache/vLLM KV-cache storage, prefer the embedded LMCache backend first.
Use the TCP server for shared/general caching or explicit networked comparisons.

Local Docker image:

```sh
docker compose up --build shardcache
```

## Workspace

- `crates/shardmap`: published embedded sharded map/cache crate plus shared internals.
- `crates/shardcache-client-rs`: published blocking Rust client for SCNP.
- `crates/shardcache`: source-only server package and binary.
- `crates/shardcache-redis`: source-only Redis/Valkey compatibility package.
- `crates/shardcache-py`: source-only PyO3 bindings for Python integrations.
- `integrations`: LMCache and model-serving integration adapters.
- `benchmarks`: benchmark and compatibility harnesses.

## Release Checks

```sh
cargo fmt --check
cargo test -p shardmap
cargo test -p shardcache-client-rs
cargo check -p shardcache
cargo package -p shardmap --locked
cargo package -p shardcache-client-rs --locked
docker compose config
```
