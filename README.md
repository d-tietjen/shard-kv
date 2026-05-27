# shard-kv

`shard-kv` is a Rust workspace for two related sharded key/value surfaces:

- `shardmap`: the embedded, DashMap-like Rust map/cache crate.
- `shardcache`: the source-only Redis/Valkey-style server binary built on the same sharded engine.

The split keeps the embedded API small while still letting the server reuse the
same storage, protocol, persistence, and Redis compatibility code.

## Embedded map

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

## Server

```sh
cargo run -p shardcache -- --bind-addr 127.0.0.1:6380 --disable-persistence
```

`shardcache` is not published to crates.io in this release. From a checkout,
use `cargo run -p shardcache` or `cargo install --path crates/shardcache
--locked` for local/private server deployments.

The Docker image builds the same `shardcache` binary:

```sh
docker compose up --build shardcache
```

## Workspace

- `crates/shardmap`: embedded sharded map/cache library plus shared server internals.
- `crates/shardcache`: source-only server package and binary.
- `crates/shardcache-redis`: unpublished Redis/Valkey compatibility source package.
- `crates/shardcache-client-rs`: blocking Rust client for the native SCNP protocol.
- `benchmarks`: benchmark and compatibility harnesses.

## Release Checks

```sh
cargo fmt --check
cargo test -p shardmap
cargo check -p shardcache
cargo package -p shardmap
cargo package -p shardcache-client-rs
```
