# shardcache

`shardcache` is the Redis/Valkey-style server for the `shard-kv` project. It is
built on `shardmap` and exposes RESP plus the native SCNP protocol.

```sh
cargo run -p shardcache -- --bind-addr 127.0.0.1:6380 --disable-persistence
```

`shardcache` is source-only for the 0.1.x release line and is not published to
crates.io. From a checkout, you can install the binary locally with:

```sh
cargo install --path crates/shardcache --locked
```

The default build includes Redis/Valkey compatibility. Build with
`--no-default-features --features server` for the lean RESP/SCNP server without
the full compatibility catalog.
