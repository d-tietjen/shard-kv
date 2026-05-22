# Redis Compatibility Source

Feature-gated Redis/Valkey command and object-storage source for
`fast-cache`.

The workspace crate manifest is `crates/fast-cache-redis/Cargo.toml`, and the
source now lives under `crates/fast-cache-redis/src`. During the transition to a
fully external extension API, `fast-cache-core` still includes these files by
path behind its `redis-compat` feature. That keeps the runtime behavior stable
while making Redis source ownership explicit.
