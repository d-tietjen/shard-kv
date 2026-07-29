# Exact Point Governance

Shardmap 0.6 can associate opaque governance metadata with an exact byte-key
value. This is intended for durable, shared caches whose values may contain
state derived from private repositories, documents, tenants, or policy domains.
Shardmap stores the metadata but does not interpret it.

```rust
use bytes::Bytes;
use shardmap::embedded::EmbeddedStore;

let cache = EmbeddedStore::new(16);
cache.set_value_bytes_with_governance(
    b"kv-prefix",
    Bytes::from_static(b"model-state"),
    Some(300_000),
    Bytes::from_static(b"tenant-a/repo-private"),
);

assert!(cache.get_value_bytes(b"kv-prefix").is_none());
let value = cache.get_value_bytes_with_governance_filter(b"kv-prefix", |metadata| {
    metadata == Some(b"tenant-a/repo-private".as_slice())
});
assert_eq!(value.as_deref(), Some(b"model-state".as_slice()));
```

## Security Contract

- Metadata presence marks an entry as protected. This includes empty metadata.
- Ordinary point GET, borrowed GET, mutable guards, key/entry visitors, Redis
  GET, and value-returning removal paths do not release a protected value.
- A denied governed read is returned as a cache miss. The API does not
  distinguish a missing key from rejected metadata.
- The authorization callback receives `Option<&[u8]>`. It can reject an
  unlabelled entry by returning `false` for `None`.
- Resident authorization runs while the owning shard lock is held and before
  the value handle is cloned. Metadata and value therefore come from the same
  entry version during concurrent replacement.
- Callbacks must be bounded, nonblocking, and must not re-enter the same store.
- An ordinary write atomically replaces the value and TTL and clears prior
  governance. A governed write atomically replaces all three fields.

Persistence snapshots are an internal raw durability surface and include both
protected values and metadata. Treat snapshot and WAL access as privileged.

## Storage Tiers

Governance metadata follows the value through resident storage, TTL changes,
snapshot and WAL recovery, object overflow, and KV
overflow. Object-overflow references retain metadata locally and authorize
before fetching the object. KV-overflow envelopes store metadata with the
value, include it in the CRC32 integrity check, and preserve it through retry,
handoff, direct SCNP pipelines, Redis/Valkey storage, and promotion.

`KvOverflowStore::get` and `KvOverflowCluster::get` fail closed for protected
values. Use `get_with_governance_filter` for primary reads and
`get_remote_with_governance_filter` when a service should read the overflow
owner without promoting into primary memory.

```rust,ignore
overflow.set_with_governance(
    b"kv-prefix",
    payload,
    Some(300_000),
    b"tenant-a/repo-private",
)?;
overflow.flush_remote()?;

let value = overflow.get_with_governance_filter(b"kv-prefix", |metadata| {
    requester_can_read(metadata)
})?;
```

## Compatibility

Ungoverned point entries keep the existing in-memory entry shape and wire
encoding. Governed WAL SET records use an explicit opcode, snapshots use format
version 2 while retaining version 1 readers, replication mutation batches use
an explicit governed SET opcode, and replication snapshot chunks retain a
backward-decodable marker. KV overflow keeps v1/v2 envelopes readable and uses
v3/v4 only for governed raw/compressed values.

All replication peers that may receive governed entries must run a version that
understands governed SET records. No Redis command exposes governance in 0.6;
applications use the embedded Rust API.

This feature adds no third-party dependencies. The complete workspace
dependency inventory remains in [`DEPENDENCIES.md`](DEPENDENCIES.md).
