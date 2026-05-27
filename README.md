# shard-kv

`shard-kv` is the workspace for two related sharded key/value surfaces:

- `shardmap`: the published embedded Rust map/cache crate.
- `shardcache`: the source-only Redis/Valkey-style server and Docker image.

The 0.1.x crates.io release ships only `shardmap` and
`shardcache-client-rs`. The server, Docker image, Python bindings, and
integration packages are built from this repository.

## Start Here

| Surface | Doc | Use When |
| --- | --- | --- |
| Embedded Rust map/cache | [`crates/shardmap/README.md`](crates/shardmap/README.md) | You want an in-process, DashMap-like cache with sharding, TTL, and memory-limit eviction. |
| Native Rust client | [`crates/shardcache-client-rs/README.md`](crates/shardcache-client-rs/README.md) | You want a blocking Rust client for shardcache over SCNP, including optional Redis command helpers. |
| shardcache Docker/server | [`docs/SHARDCACHE_DOCKER.md`](docs/SHARDCACHE_DOCKER.md) | You want to build and run the source-only server locally or in a private container registry. |
| LMCache storage backend | [`integrations/lmcache_storage_backend/README.md`](integrations/lmcache_storage_backend/README.md) | You want LMCache to store KV-cache payloads in embedded shardcache or a shardcache TCP server. |

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
