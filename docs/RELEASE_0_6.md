# Shardcache 0.6 Feature Guide

Shardcache 0.6 adds partitioned key-value overflow for deployments that need an
in-memory primary without forcing every read or every durable cache value
through that primary. It also makes the overflow path suitable for production
operation through deterministic placement, bounded shard-local networking,
online handoff, native TLS, explicit memory ceilings, and release benchmarks.

The complete configuration and operating contract is in
[`KV_OVERFLOW.md`](KV_OVERFLOW.md). This document summarizes the public 0.6
surface and the decisions an adopter needs to make.

## Feature Catalog

| Capability | Description | Enable With |
| --- | --- | --- |
| Partitioned KV overflow | Keeps the hot working set in the embedded primary and assigns each cold key to one remote owner. Remote capacity is partitioned, not fully copied to every replica. | `shardmap/kv-overflow` or `shardcache/kv-overflow` |
| Shard-owned networking | Gives every primary shard its own bounded drain, completion lane, assigned targets, and TCP connections. Primary shards do not share a network worker, queue, or connection mutex. | Included with `kv-overflow` |
| Direct SCNP placement | Validates replica identity, shard count, route mode, direct-port base, and protocol capabilities before use. `OverflowSlot` keys preserve the selected remote shard across WAL and snapshot recovery. | Included with `kv-overflow`; `transport = "direct_shard"` |
| Horizontal topology changes | Uses fixed logical slots and stable replica IDs. Exact rebalance supports replica additions, removals, and primary-shard-count changes while `previous_replicas` describes the old geometry during handoff. | Included with `kv-overflow` |
| Redis and Valkey overflow | Uses standalone Redis-compatible endpoints as logical owners with binary-safe namespaced keys, ordered pipelines, absolute TTL preservation, CRC32 envelopes, and environment-only credentials. | `kv-overflow-redis` |
| Acknowledged cold eviction | Keeps hot, dirty, pending, or failed generations resident. Only a remotely acknowledged cold generation can leave primary memory, and a miss can fault the value back in. | Included with `kv-overflow`; configure `eviction_policy` |
| Cascading storage tiers | Lets shardcache overflow replicas apply LRU/LFU and then move their own cold envelopes to filesystem, S3, or RustFS object storage. | `kv-overflow` plus `object-overflow` or `object-overflow-s3` on replicas |
| Native SCNP TLS | Adds Rustls TLS 1.3, optional mTLS, leaf-certificate fingerprints, bounded handshakes, and reloadable certificates, trust roots, and tokens without OpenSSL. | `scnp-tls` |
| Bounded resource use | Caps queues, pipelines, resident bytes, metadata bytes, key/value sizes, slot tables, response collections, protocol frames, handoff batches, and decompression expansion. Growth receives backpressure before local mutation. | Included with `kv-overflow`; configure the production limits |
| Failure isolation | Gives each target a circuit breaker, retry state, adaptive pipeline limit, and independent task when one shard owns multiple targets. A delayed target does not stall unrelated targets. | Included with `kv-overflow` |
| Fallible persistence | Materializes remote-only values into the authoritative local snapshot and fails loudly if a remote value cannot be fetched. Restore mirrors recovered values before enforcing the resident target. | Included with `kv-overflow` |
| Overflow health | Reports per-shard ownership, queue and completion depth, sockets, pipeline size and RTT, retries, circuit state, handoff progress, compression, TLS, resident bytes, and metadata bytes. | Included with `kv-overflow` |
| Exact point governance | Atomically attaches opaque policy metadata to exact values and fails closed across resident reads, persistence, replication, object overflow, and KV overflow. | Embedded Rust API; see [`EXACT_GOVERNANCE.md`](EXACT_GOVERNANCE.md) |

## Topology Examples

- **16 primary shards, one 16-shard replica:** primary shard `N` owns remote
  shard `N` and its direct connections.
- **16 primary shards, 16 replicas:** primary shard `N` owns complete replica
  `N`.
- **More than 16 replicas:** complete replicas are divided evenly and
  exclusively among primary shards.
- **Fewer replicas than primary shards:** the flattened set of remote shards is
  divided evenly, with every remote shard assigned to exactly one primary
  owner.

The configured SCNP tier must expose at least as many total remote shards as the
primary has shards. Every assigned target must receive at least one logical
slot. Redis and Valkey retain the same logical slot ownership, but standalone
servers cannot guarantee physical shard isolation.

## Write And Read Semantics

`KvOverflowStore::set` reserves shard-local queue capacity before changing the
primary. The call acknowledges local application and replication admission;
`flush_remote` is the explicit remote visibility boundary. A value remains
resident until the matching generation is acknowledged, so a failed upload
cannot cause data loss from primary memory.

Normal reads use the embedded primary first and fault in from the deterministic
remote owner on a local miss. Services that should not load the primary can use
`KvOverflowCluster::get` to read that owner directly. TTL starts when the
primary accepts the write and does not restart while a mutation waits or
retries.

Governed exact values use `set_value_bytes_with_governance` or
`KvOverflowStore::set_with_governance`. Ordinary GET cannot return a protected
entry. The governed filter receives borrowed opaque metadata and must approve
it before the value is cloned or promoted. KV overflow v3/v4 envelopes include
metadata in their integrity check and handoff verification.

## Topology Migration

0.6 uses exact rebalance. Adding a replica can move ranges between existing
replicas as well as onto the new replica; it is not an incremental-only ring.
Put the new membership in `replicas`, retain the old membership in
`previous_replicas`, and set `previous_primary_shard_count` when the primary
shard count changed. Keep removed nodes reachable.

Load the authoritative local snapshot, run `synchronize_resident`, and wait for
`flush_remote` to complete. Handoff reads the old owner, writes and verifies the
new owner, then deletes the old copy. This includes remote-only metadata. Remove
the previous membership only after handoff health reports completion.

## Security Defaults

Non-loopback SCNP requires TLS plus token authentication or mTLS. The explicit
`allow_insecure_scnp` option is intended only for a private encrypted overlay.
TLS uses Rustls with the `ring` provider and TLS 1.3 only; the all-feature graph
is checked to exclude OpenSSL and native-tls implementations.

Protocol and envelope lengths are validated before allocation or dispatch.
Response lengths come from initialized buffers rather than peer declarations,
which prevents Heartbleed-style memory disclosure. Durable replica files must
live on an encrypted filesystem or volume, and S3/RustFS deployments should
also configure server-side encryption.

See [`SECURITY.md`](../SECURITY.md) for reporting and dependency policy, and
[`DEPENDENCIES.md`](DEPENDENCIES.md) for the complete locked dependency
inventory.

## Upgrade Notes

The final topology model replaces address-only `endpoints` and
`previous_endpoints` with stable `replicas` and `previous_replicas`. Each SCNP
replica declares an ID, one or more equivalent addresses, a shard count, and a
direct-shard port base. Direct transport validates that all failover addresses
advertise the same identity.

Global overflow worker and connection settings are replaced by shard-local
controls such as `queue_capacity_per_shard`, `pipeline_max_items`,
`pipeline_max_bytes`, `pipeline_flush_micros`, and
`max_inflight_per_target`. Start from
[`crates/shardmap/shardcache.toml.example`](../crates/shardmap/shardcache.toml.example)
rather than carrying forward an earlier preview configuration.

`slot_count`, `cluster_id`, stable replica IDs, and replica persistence are
deployment invariants. Do not change `slot_count` while overflow data exists,
and do not reuse a cluster namespace for unrelated data.

## Release Evidence

The release gates include workspace formatting, all-target/all-feature clippy,
workspace tests, topology and fault-injection tests, live filesystem cascading
tests, TLS and malformed-frame tests, feature-matrix checks, and compilation of
external consumers from the packaged `.crate` files.

The final interleaved Adam A/B measured embedded, local/TCP replication, RESP,
SCNP, and bounded KV overflow paths. Median throughput changes stayed within
3.6% and the worst median p99 change was 4.9%. The fully bounded enqueue path
admitted a median 2.05 million writes/second. Hardware-specific details and
reproduction commands are recorded in [`KV_OVERFLOW.md`](KV_OVERFLOW.md).

## 0.6 Boundaries

- Each key has one overflow owner. This release provides aggregate capacity and
  read isolation, not a remotely replicated high-availability copy.
- Local WAL and snapshots remain the authoritative recovery boundary.
- Redis Cluster `MOVED` and `ASK` discovery is not included.
- The overflow primary is an embedded `KvOverflowStore`; standalone shardcache
  processes serve as overflow replicas.
- Live S3/RustFS smoke tests require endpoint credentials and remain
  environment-gated. Filesystem object overflow is covered in regular CI.
