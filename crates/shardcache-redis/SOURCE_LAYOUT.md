# Redis Compatibility Source

Feature-gated Redis/Valkey command and object-storage source for
`shardcache`.

The workspace crate manifest is `crates/shardcache-redis/Cargo.toml`, and the
source now lives under `crates/shardcache-redis/src`. During the transition to a
fully external extension API, `shardmap` still includes these files by
path behind its `redis` feature. That keeps the runtime behavior stable
while making Redis source ownership explicit.

Use `docs/REDIS_COMPATIBILITY.md` for the generated command surface and
`crates/shardmap/src/commands/README.md` for the command-module pattern.
