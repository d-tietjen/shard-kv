# Shardcache 0.7 Feature Guide

Shardcache 0.7 adds opt-in active-active replication for read-heavy exact
point-value workloads. Multiple `ActiveShardMap` members may accept local reads
and writes, exchange bounded shard-local mutation blocks, and converge after
partitions without putting network round trips on local GET or SET calls.

Active sync is absent from the default Cargo feature set. Existing `ShardMap`
deployments retain their current layout and hot path unless they explicitly
select an active-sync feature.

## Feature Catalog

| Capability | Description | Enable With |
| --- | --- | --- |
| Causal eventual replication | Converges exact SET, DEL, expiry, and governed SET using causal dominance, remove-wins tombstones, and deterministic HLC ordering. | `active-sync-causal-eventual` |
| Consensus-ordered conflicts | Uses an external finalizer only for ambiguous concurrent conflicts. Values and WAL blocks stay on the active-sync data plane. | `active-sync-consensus-ordered-eventual` |
| Direct peer transport | Exchanges blocks over direct-shard Rustls connections with mandatory mTLS 1.3, ALPN, identity authorization, bounded frames, deadlines, and revocation. | Add `active-sync-tls` |
| Automatic membership | Reconciles revisioned desired peer sets with bounded join catch-up, draining, health reporting, and explicit forced retirement. | `ActiveSyncTlsMembership` |
| Independent cache residency | Lets each replica apply its own LRU/LFU decisions. Local eviction removes only the payload, not the logical version, and does not replicate read heat. | Included in active sync |
| Exact fault-in | Restores an evicted version from local overflow, durable state, or a peer and rejects stale promotion after a racing mutation. | Included in active sync |
| Persistence and repair | Preserves causal state in checksummed snapshots and repairs compacted block history through bounded state transfer. | Included in active sync |
| Conflict batching | Finalizes independent ambiguous conflicts in bounded batches while preserving repeated-key mutation order. | Included in consensus mode |

## Intended Workload And Performance

The release target is a read-heavy durable cache whose replicas absorb local
read traffic while accepting occasional writes. It is not intended to improve
write-heavy throughput.

Three release runs on Adam used commit `5d06a56`, CPUs `0-7`, eight shards,
eight clients, 100,000 keys, 1 KiB values, a 100 ms sync interval, two seconds
of warmup, and ten seconds of measurement per isolated process. Every
synchronized run drained pending blocks and verified all keys on both maps.
The table reports median throughput and median p99:

| Workload | Baseline | Causal local | Consensus local | Causal sync | Consensus sync |
| --- | ---: | ---: | ---: | ---: | ---: |
| 99% GET / 1% SET | 17.77M/s, 2.1us | 15.25M/s (85.8%), 2.2us | 15.08M/s (84.8%), 2.3us | 13.78M/s (77.5%), 2.3us | 13.34M/s (75.1%), 2.3us |
| 95% GET / 5% SET | 17.57M/s, 2.2us | 14.56M/s (82.9%), 2.3us | 14.09M/s (80.2%), 2.4us | 11.88M/s (67.6%), 2.7us | 11.61M/s (66.1%), 2.5us |

Pure GET reaches 15.05M/s in causal-sync and 14.70M/s in consensus-sync,
retaining 85.0% and 83.1% of the 17.70M/s embedded baseline. The important
production distinction is that a small write fraction also creates causal
records and background block traffic: use the 99/1 or 95/5 row for capacity
planning rather than extrapolating from pure GET.

Installing consensus ordering does not call the orderer on conflict-free
writes and changed local throughput by at most 0.5% in the canonical matrix.
Under guaranteed concurrent conflicts, causal convergence reached 333.9K
pairs/s. Bounded consensus reached 186.9K pairs/s with a zero-latency orderer
and 66.0K pairs/s with 100us synthetic external latency.

Raw runs and commands are preserved in
[`adam-active-sync-read-heavy-20260716`](../benchmarks/evidence/adam-active-sync-read-heavy-20260716/README.md).
The broader value-size, SET, 80/20, conflict, and feature-disabled regression
matrix is in
[`ACTIVE_ACTIVE_0_7_BASELINE.md`](../benchmarks/ACTIVE_ACTIVE_0_7_BASELINE.md).

## Consistency Selection

Use causal eventual mode unless externally finalized ordering of ambiguous
concurrent mutations is a requirement. Both modes provide local
read-your-writes and eventual convergence after finite writes and eventual
successful delivery. Neither mode provides linearizable reads, serializable
transactions, or immediate remote visibility.

Consensus mode orders only a conflict already observed during synchronization.
It does not send every write through consensus and does not create a second WAL
or value replication path.

## Deployment Contract

- Give every writer a stable unique `NodeId`, cluster identity, and persisted
  incarnation state.
- Use direct connectivity between every peer that must receive an origin's
  mutations. Unsigned multi-hop forwarding fails closed in 0.7.
- Use `active-sync-tls` for non-loopback peers. Configure TLS 1.3 mutual
  authentication, certificate-fingerprint authorization, bounded deadlines,
  and tested credential overlap before admitting traffic.
- Publish a monotonically increasing membership revision. Fence writes before
  removing a member, allow the member to drain, and revoke its certificate only
  after retirement.
- Monitor pending and retained blocks, peer failures, truncated rounds,
  state-transfer fallback, conflict counts, credential generation, and drain
  progress.
- Size memory for resident values plus causal metadata and retained mutation
  payloads. Active blocks currently retain payloads rather than durable WAL
  offsets.

## Release Boundaries

- Active sync supports exact byte point values, not arbitrary Redis command
  semantics or cross-key transactions.
- External topology discovery and persistence are application responsibilities.
- Automated writer fencing, multi-hop origin signatures, tombstone garbage
  collection, stronger acknowledgement modes, and quorum cold nomination are
  not 0.7 claims.
- Write-heavy active modes remain below the original 90% baseline-retention
  target. The feature therefore remains explicit and disabled by default.
- The standalone Blossom bridge is source-only because it pins a pre-release
  Blossom revision; published crates expose only the ordering interface.

See [`ACTIVE_ACTIVE_REPLICATION.md`](ACTIVE_ACTIVE_REPLICATION.md) for the full
protocol, conflict, eviction, topology, security, recovery, configuration, and
observability contract. See [`DEPENDENCIES.md`](DEPENDENCIES.md) for the locked
all-feature dependency and license inventory.

## Release Evidence

The release gate includes workspace formatting, all-target/all-feature Clippy,
workspace and documentation tests, causal-only and consensus-only builds,
feature-matrix checks, packaged-crate consumer builds, deterministic fault
injection, formal conflict laws, mTLS rotation and revocation tests, and the
real six-validator Blossom finality/restart integration test.

Feature-disabled raw-cache and native `ShardMap` GET, SET, and 80/20 rows stayed
within 2.4% of their recorded Adam baselines with zero errors. This verifies
that publishing 0.7 does not impose the active-sync path on default users.
