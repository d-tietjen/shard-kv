# shardmap

`shardmap` is the embedded Rust map/cache crate for `shard-kv`. It is designed
for in-process workloads that want a cloneable, sharded handle with TTL,
eviction, batch reads, and owner-local worker paths when callers can route work
to the correct shard.

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

`ShardMap` is the default shared-handle API. Lower-level modules expose the
same storage engine used by `shardcache` for direct shard ownership, SCNP/RESP
protocol support, persistence, and replication.

## Features

- `sharded` (default): embedded sharded map API.
- `redis`: Redis/Valkey object and command behavior.
- `server`: shared server internals used by `shardcache`.
- `redis-server`: server internals plus Redis/Valkey compatibility.
- `telemetry`: operational metrics for embedded use.
- `monoio`: Linux-only server transport internals.

Most applications should depend on `shardmap` for embedded use and install
`shardcache` when they need a TCP server.
