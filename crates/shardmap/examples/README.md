# shardmap examples

Run examples from the repository root:

```bash
cargo run -p shardmap --example basic_map
cargo run -p shardmap --example configured_cache
cargo run -p shardmap --example prepared_keys_threads
cargo run -p shardmap --example ttl_and_locks
cargo run -p shardmap --example semantic_cache
```

These examples are intentionally small. They are meant to show the embedded API
shape for each feature area without requiring a server process.
