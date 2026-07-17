# shardmap examples

Run examples from the repository root:

```bash
cargo run -p shardmap --example basic_map
cargo run -p shardmap --example configured_cache
cargo run -p shardmap --example entry_api
cargo run -p shardmap --example mini_feature_flags
cargo run -p shardmap --example prepared_keys_threads
cargo run -p shardmap --example route_inspection
cargo run -p shardmap --example ttl_and_locks
cargo run -p shardmap --example semantic_cache
cargo run -p shardmap --example semantic_ttl
cargo run -p shardmap --example active_sync --features active-sync-causal-eventual
```

These examples are intentionally small. They are meant to show the embedded API
shape for each feature area without requiring a server process.
