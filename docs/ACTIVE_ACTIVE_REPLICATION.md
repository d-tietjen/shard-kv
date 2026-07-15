# Eventually Consistent Active-Active Replication Plan

## Summary

Build active-active shardmap synchronization in which every member of a slot's
replica group may accept local reads and writes. Writes commit to the local WAL
and map first, then converge through shard-owned mutation streams and periodic
anti-entropy. Network partitions do not stop local writes. When peers reconnect,
they exchange causal summaries, fetch missing mutations or state, resolve
concurrent versions deterministically, and reach the same result.

The implementation should reuse the 0.6 fixed-slot topology, shard-owned
networking, bounded pipelines, FCRP frames, WAL, snapshots, TLS, governance
propagation, and overflow integration. It must add globally unique mutation
dots, hybrid logical clocks, explicit conflict policies, tombstones, per-origin
version summaries, range digests, bidirectional sync, and topology-safe replica
group changes.

This model prioritizes availability and eventual convergence. It does not claim
linearizability, serializability, or automatic lossless merging for every Redis
command. Those limits must be explicit in APIs, configuration, metrics, and
documentation.

## Goals

- Allow every member of a slot replica group to accept writes.
- Keep local write latency independent of network round trips by default.
- Stream mutations continuously when peers are connected.
- Let shardmaps explicitly synchronize on demand.
- Repair missed or compacted mutations through bounded anti-entropy.
- Converge concurrent point SET, DEL, EXPIRE, and governed SET operations.
- Preserve stable slot identities while nodes and local shards are added.
- Keep one networking runtime, queue set, and sync state owner per local shard.
- Preserve WAL, snapshot, TTL, governance, and overflow behavior.
- Bound logs, summaries, tombstones, digest trees, queues, and conflict state.

## Non-Goals

- Linearizable reads or writes in eventual mode.
- Cross-node serializable transactions.
- Transparent conflict-free behavior for arbitrary Redis commands.
- Silently selecting a conflict policy for protected or application-specific
  values.
- Counting LRU overflow copies toward the active replica count.
- Depending on a global worker, endpoint mutex, or per-operation `Arc` churn.
- Replacing a production membership system with a new consensus algorithm.

## Consistency Model

### Replica Groups

The cluster has a fixed power-of-two `slot_count`. Each slot maps to a stable
replica group:

```text
(slot, topology_epoch, ordered_member_node_ids)
```

Any current member may accept a mutation. Nodes outside the group route a
request to a member, preferring a local or low-latency path. There is no
authoritative writer or lease in the data path.

Adjacent slots with the same membership are stored as compact ranges in the
topology service and expanded into an O(1) local slot table. Membership order
does not affect slot identity. No request may derive placement by taking a node
list index modulo the current node count.

### Local Commit And Optional Sync Acknowledgement

Add these acknowledgement modes:

- `local`: acknowledge after local WAL admission and map application.
- `one_peer`: acknowledge after local commit and durable admission by one other
  current group member.
- `majority`: acknowledge after a majority of current group members, including
  the local writer, have durably admitted the mutation.
- `all_available`: acknowledge after every currently reachable member has
  durably admitted the mutation and return the observed membership revision.

`local` is the eventual-consistency default. Stronger acknowledgement improves
durability at the time of the write but does not make reads linearizable and
does not prevent a concurrent write on another member.

`local` never waits for or reserves a network queue entry. Its durable
shard-local mutation log is the source for later streaming and anti-entropy, so
a long partition does not consume one queued payload per write. Network lanes
carry coalesced wakeups and peer cursors rather than copied values.

Modes that wait for peers reserve bounded acknowledgement state before local
mutation. If that reservation fails, the write returns backpressure without
changing the local value. A timeout after local commit returns an explicit
ambiguous result with a mutation token so the caller can query or synchronize
it.

### Reads

Local reads return the locally converged version and never perform hidden
network I/O. Add explicit alternatives:

- `get_local`: return the local value immediately.
- `get_after(token)`: wait until the local shard has observed the token's causal
  context, or return a deadline error.
- `get_synced`: perform a bounded sync with the slot group, then read locally.
- `get_conflicts`: return concurrent versions when the configured policy keeps
  them instead of resolving to one value.

Ordinary APIs remain local. Applications choose network synchronization
explicitly rather than receiving an accidental latency increase.

### Mutation Identity

Every mutation has a globally unique dot:

```text
(cluster_id, slot, origin_node_id, origin_incarnation, origin_sequence)
```

- `origin_node_id` is stable across restarts.
- `origin_incarnation` is newly generated on restart and durably recorded before
  that incarnation accepts writes.
- `origin_sequence` is monotonic within an incarnation and local shard lane.
- `slot` prevents a mutation from being replayed into another placement range.

Each mutation also carries a hybrid logical clock (HLC), operation, key hash,
key, value, absolute expiry, governance metadata, and causal context. HLC is
used only by policies that explicitly select it. Mutation uniqueness and
anti-entropy correctness never depend on wall-clock ordering.

### Conflict Policies

Active replication must require an explicit policy per namespace or store:

- `lww_hlc`: choose the greatest `(hlc, origin_node_id, dot)` tuple. Reject or
  quarantine timestamps beyond `max_clock_skew_ms` so a bad clock cannot
  dominate indefinitely.
- `multi_value`: preserve causally concurrent versions and expose them through
  conflict-aware APIs. A later write that observes those versions supersedes
  them.
- `merge`: invoke a registered deterministic merge implementation identified by
  a stable policy ID and version. Every peer must advertise the same ID and
  version before joining the group. The merge operator must be associative,
  commutative, idempotent, resource-bounded, and free of network or storage I/O.

The value, TTL, and governance metadata form one atomic version. Conflict
resolution cannot combine a value from one version with metadata or expiry from
another.

`lww_hlc` is available for Redis-compatible point SET, DEL, and EXPIRE only when
configured explicitly. Commands such as INCR, list mutation, transactions, and
conditional updates need operation-specific CRDT or coordination semantics and
remain local-only or rejected in active mode until implemented.

### Deletes And TTL

DEL and expiration create versioned tombstones. A tombstone participates in the
same causal and conflict policy as a value so an old value cannot reappear after
sync. Expiration uses the accepted write's absolute deadline and produces a
tombstone when observed locally.

A tombstone may be collected only after every current member and every retained
previous member has acknowledged a causal summary that dominates it, and after
`tombstone_grace_seconds`. Offline members older than the configured retention
window require a full state sync before rejoining.

## Synchronization

### Streaming Fast Path

Each local storage shard owns an `ActiveSyncIo` runtime. It maintains one
bounded mutation lane per remote target shard, reuses direct-shard SCNP
connections, and pipelines ordered writes by origin. Workers advance durable
per-peer cursors through the local mutation log; coalesced wakeups avoid a
second payload allocation and queue entry for every local write. Independent
targets make progress concurrently so a slow peer cannot block healthy peers.

Peers exchange durable acknowledgement summaries, not only per-request replies.
Retries are idempotent. Duplicate dots are acknowledged without reapplication.
A sequence gap schedules range synchronization without blocking unrelated keys.

### Anti-Entropy

Streaming alone is insufficient because peers can be offline longer than the
retained mutation log. Add a bounded anti-entropy protocol:

1. Exchange cluster ID, topology epoch, slot range, protocol capabilities, and
   compact per-origin version summaries.
2. Use fixed-depth Merkle or range digest trees with a collision-resistant
   digest to locate divergent key ranges.
3. Exchange version metadata for only divergent ranges.
4. Transfer missing values, tombstones, or mutation segments in bounded chunks.
5. Apply the configured conflict policy and return an updated durable summary.

Digest construction runs incrementally from mutation application and snapshot
recovery. A sync request must not scan the entire map while holding a shard
lock. Snapshot fallback is range-scoped and uses the existing bounded,
fallible materialization path for remote-only values.

### Explicit Shardmap Sync APIs

Provide background and caller-driven synchronization:

```rust,ignore
pub struct SyncToken {
    pub slot: u32,
    pub origin_node_id: NodeId,
    pub origin_incarnation: IncarnationId,
    pub origin_sequence: u64,
}

pub struct SyncOptions {
    pub deadline: Duration,
    pub peers: SyncPeerSelection,
    pub durability: SyncDurability,
    pub max_bytes: usize,
}

impl ActiveShardMap {
    pub fn sync_with(
        &self,
        peer: &ActiveShardMap,
        options: SyncOptions,
    ) -> Result<BidirectionalSyncReport>;
    pub fn sync_once(&self, options: SyncOptions) -> Result<SyncReport>;
    pub fn sync_slot(&self, slot: u32, options: SyncOptions) -> Result<SyncReport>;
    pub fn sync_peer(&self, peer: NodeId, options: SyncOptions) -> Result<SyncReport>;
    pub fn wait_for(&self, token: &SyncToken, deadline: Instant) -> Result<()>;
    pub fn convergence_snapshot(&self) -> ConvergenceSnapshot;
}
```

`sync_with` provides direct bidirectional synchronization between embedded
shardmap instances without requiring a TCP server. It uses the same summaries,
chunk limits, and conflict engine as network sync and never holds locks from
both maps at the same time. A transport trait supports network and application
provided transports without duplicating convergence logic.

`sync_once` synchronizes bidirectionally with configured peers. It returns
partial progress and peer-specific errors rather than claiming full convergence
when a peer is unavailable. Reports describe convergence through the exchanged
frontier; writes concurrent with or after that frontier may still be pending.
`SyncReport` includes starting and ending causal summaries, transferred bytes,
conflicts, tombstones, and peers that still lag.

Background sync uses configurable intervals, jitter, byte budgets, and maximum
concurrency. It reuses the same shard-local runtimes and limits as explicit
sync. Explicit sync receives priority without bypassing memory ceilings.

## Topology And Membership

Data convergence does not require a leader. Topology still needs a consistent
membership revision so nodes agree which replica groups should retain data.
Support two modes:

- `static`: identical replica-group configuration on every node, suitable for
  embedded deployments and tests.
- `external`: a production topology adapter backed by etcd or another
  linearizable service.

The topology service stores node identities, addresses, capabilities, replica
groups, and monotonic topology epochs. It does not participate in each write.
The data path remains available during a topology-service outage using the last
durable membership view.

## Architecture

```mermaid
flowchart LR
    C["Client"] --> A["Any shardcache node"]
    A --> R["Fixed-slot replica-group router"]
    R --> N1["Writable shardmap A"]
    R --> N2["Writable shardmap B"]
    R --> N3["Writable shardmap C"]
    N1 <--> S1["Shard-local streaming and anti-entropy"]
    N2 <--> S1
    N3 <--> S1
    N1 --> O1["KV or object overflow"]
    N2 --> O2["KV or object overflow"]
    N3 --> O3["KV or object overflow"]
    T["Static or external topology"] --> R
```

## Reuse From 0.6

Extend these components instead of creating parallel implementations:

- fixed logical slots and precomputed O(1) placement
- one queue set, drain, and connection owner per local shard
- direct-shard SCNP and independent target progress
- ordered, byte-capped pipelines and bounded completion lanes
- TLS 1.3, mTLS, token authentication, deadlines, and reconnect handling
- bounded frame parsing and decompression limits
- FCRP mutation, acknowledgement, snapshot, and error frames
- WAL records, fallible snapshots, and backlog catch-up
- exact governance encoding and fail-closed reads
- KV overflow generations and acknowledged cold eviction
- topology identity validation and multi-address failover
- health snapshots and failure classifications

Active replicas and overflow remain separate layers. A replica-group member is
responsible for convergent state even when its resident cache evicts a value.
Its WAL, snapshot, or lower durable tier must reconstruct the version and
tombstone history needed for sync. Overflow copies do not satisfy `one_peer`,
`majority`, or convergence reporting.

## Configuration Surface

Add `[active_sync]` rather than overloading single-primary replication:

```toml
[active_sync]
enabled = true
cluster_id = "production-a"
node_id = "cache-us-east-1a-01"
slot_count = 16384
replica_count = 3
write_ack = "local"
conflict_policy = "lww_hlc"
max_clock_skew_ms = 5000
queue_capacity_per_shard = 4096
pipeline_max_items = 64
pipeline_max_bytes = 262144
pipeline_flush_micros = 50
max_inflight_per_target = 4
mutation_log_bytes_per_shard = 67108864
tombstone_grace_seconds = 86400
offline_member_retention_seconds = 604800
sync_interval_ms = 1000
sync_jitter_ms = 250
sync_max_bytes_per_round = 16777216
sync_max_concurrent_peers_per_shard = 2
digest_fanout = 16
digest_depth = 4

[active_sync.topology]
backend = "static"

[[active_sync.peers]]
id = "cache-us-east-1a-02"
addresses = ["cache-us-east-1a-02:6501"]

[[active_sync.peers]]
id = "cache-us-east-1a-03"
addresses = ["cache-us-east-1a-03:6501"]

[active_sync.tls]
enabled = true
require_client_auth = true
cert_path = "/run/secrets/node.crt"
key_path = "/run/secrets/node.key"
ca_path = "/run/secrets/cluster-ca.crt"
```

`replica_count` counts the complete current group, including the local member.
Static configuration validation requires every member to derive the same group
for every slot.

An external topology block may load credentials from configured environment
variables. Credentials, keys, values, governance metadata, and certificate
paths must be redacted from diagnostics. Non-loopback synchronization requires
authenticated TLS unless an explicit private encrypted-overlay exception is
enabled.

## Replica Group Changes

Adding nodes or local shards changes replica-group membership, never slot
identity. Reconfiguration uses overlapping membership:

1. **Expand**: publish a new topology epoch containing both old and new members.
2. **Seed**: new members receive range snapshots and mutation-log catch-up.
3. **Converge**: anti-entropy proves every new member dominates the retained
   summaries for the range.
4. **Contract**: publish a later epoch that removes old members.
5. **Retain**: old members keep tombstones and accept sync for a grace period,
   then securely purge the retired range.

There is no single-writer cutover. Writes may occur on any current member during
the move and converge normally. Previous membership remains configured until
the convergence proof is durable.

An isolated node with stale topology may continue accepting local writes. This
is a deliberate availability tradeoff. Operators must keep removed nodes
reachable through the retention period or explicitly accept that writes made
only on a permanently removed, isolated node cannot converge. A stale node that
reconnects synchronizes its accepted dots before retirement.

## Failure Semantics

### Peer Failure

Local mode continues accepting writes and reports the peer as lagging.
Acknowledgement modes requiring peers return unavailable or ambiguous according
to whether local commit occurred. Recovery uses mutation-log catch-up or range
anti-entropy.

### Network Partition

Every side continues local reads and writes. Concurrent versions are resolved
after reconnection by the configured policy. The system exposes partition age,
unseen dots, and conflicts; it does not describe either side as strongly
consistent.

### Clock Skew

Mutation identity and causal dominance do not use wall time. `lww_hlc` requires
bounded clock skew and quarantines future timestamps. `multi_value` and custom
causal merge policies can operate without selecting a wall-clock winner.

### Recovery

WAL and snapshots persist dots, HLC values, causal context, tombstones,
governance, and topology epoch. A restarted node serves recovered local state
and enters catch-up immediately. A node offline beyond retention must complete
a full range sync before it is considered converged.

## Governance And Security

- Governance metadata is part of the atomic version and integrity checksum.
- Governed reads fail closed before local return, promotion, or conflict
  exposure.
- Peer synchronization is a privileged replication surface authenticated by
  node identity; it does not interpret governance metadata as an ACL.
- A custom merge policy may not inspect protected values unless it is registered
  as governance-aware and runs inside the trusted store boundary.
- Protocol lengths, causal contexts, origin summaries, tombstones, digest
  nodes, conflict sets, pending acknowledgements, and snapshot staging are
  bounded before allocation.
- Unknown cluster IDs, node IDs, policy IDs, policy versions, or topology epochs
  fail closed.
- Sync logs and health output never expose keys, values, credentials, or
  governance metadata.

## Observability

Expose cluster and per-local-shard health without per-key metric cardinality:

- topology epoch and replica-group coverage
- local origin sequence and durable/applied summaries
- connected, lagging, offline, seeding, and retired peers
- streamed and anti-entropy bytes, entries, tombstones, and rounds
- digest comparisons, divergent ranges, snapshot fallbacks, and repair duration
- duplicate dots, sequence gaps, conflicts, resolutions, and quarantined clocks
- local, one-peer, majority, and all-available acknowledgement latency
- queue depth, pipeline bytes, RTT, retries, reconnects, and circuit state
- mutation-log bytes, compaction watermark, and oldest retained dot
- tombstone count, bytes, and oldest age
- explicit sync partial completions and deadline failures
- reconfiguration seeding and convergence progress

## Implementation Phases

### Phase 1: Versioned Point State

- Add dots, HLC, causal context, tombstones, and conflict-policy identifiers.
- Extend WAL and snapshots with backward-readable versioned records.
- Implement `lww_hlc` and `multi_value` for exact point SET, DEL, and EXPIRE.
- Preserve governance as part of each atomic version.

### Phase 2: Shard-Owned Mutation Streaming

- Add one `ActiveSyncIo` runtime per local shard.
- Extend FCRP capability negotiation and mutation frames.
- Implement bidirectional streams, per-origin summaries, idempotent apply, gap
  detection, and bounded acknowledgement tracking.
- Preserve independent target progress and no shared worker contention.

### Phase 3: Anti-Entropy And Explicit Sync

- Add incremental range digests and bounded divergent-range exchange.
- Add `sync_with`, `sync_once`, `sync_slot`, `sync_peer`, `wait_for`, and
  convergence health.
- Add range snapshot fallback for compacted or long-offline peers.
- Add background scheduling with jitter, budgets, and shard-local concurrency.

### Phase 4: Routing And Topology

- Add replica-group placement and connect-to-any-node routing.
- Implement static topology first and an external adapter behind a feature flag.
- Add overlapping membership changes, seeding, convergence proof, and retirement.
- Verify node and local-shard additions do not reorder slot identities.

### Phase 5: Redis And Merge Policies

- Expose explicitly configured LWW point commands over RESP.
- Reject unsupported active-active commands rather than giving false Redis
  atomicity guarantees.
- Define the deterministic custom merge registration and compatibility handshake.
- Add operation-specific CRDTs only as separately tested features.

### Phase 6: Overflow And Release Hardening

- Synchronize resident and remote-only values through fallible materialization.
- Verify replica members using filesystem, S3/RustFS, or KV overflow retain the
  history required for repair.
- Add rolling upgrades, fault injection, production benchmarks, and runbooks.

## Test Plan

### Unit And Model Tests

- Dot uniqueness across restart, sequence rollover, and shard reassignment.
- Causal dominance, concurrency detection, LWW tie-breaking, and multi-value
  convergence under every delivery permutation.
- Duplicate, reordered, dropped, delayed, and corrupted mutations.
- Delete and expiry tombstones cannot resurrect older values.
- Safe tombstone and mutation-log garbage collection.
- Governance, TTL, and value remain one atomic conflict unit.
- Digest equality implies equal version summaries for the covered range.
- Hard bounds for causal context, conflicts, digests, gaps, and sync chunks.

### Integration Tests

- Three writable shardmaps converge after concurrent writes on every node.
- Direct `sync_with` converges two embedded shardmaps without a server runtime.
- Bidirectional explicit sync works with background sync disabled.
- Partitions accept writes and converge after healing.
- Long-offline peers recover by anti-entropy after mutation-log compaction.
- Restart preserves dots, summaries, tombstones, and convergence.
- Node and shard additions use overlapping membership without slot reordering.
- Removed nodes reconcile retained writes before retirement.
- Governed values remain protected through conflicts and sync.
- Filesystem, S3/RustFS, SCNP, and Redis/Valkey overflow materialize correctly
  during anti-entropy.

Use deterministic network proxies and process-level faults for asymmetric
partitions, duplication, reordering, corruption, half-open sockets, and clock
skew. Validate final state on every peer, not only request return codes.

### Formal And History Tests

- Model causal delivery, tombstones, conflict policies, and membership overlap
  in the formal support crate.
- Prove convergence for each built-in policy under arbitrary finite delivery
  order and duplication.
- Check that causally newer writes dominate older values and that garbage
  collection cannot remove state needed by a retained peer.
- Run history tests for SET, GET, DEL, EXPIRE, governed SET, partitions, and
  reconfiguration. Verify eventual convergence, read-your-writes locally, and
  documented conflict outcomes rather than linearizability.

### Performance Tests

- Embedded baseline versus local active-sync metadata with networking disabled.
- Streaming throughput for replica counts 2, 3, and 5.
- Local, one-peer, and majority write latency at 64 B, 1 KiB, and 64 KiB.
- Explicit full-sync and one-percent divergence repair throughput.
- Digest maintenance overhead and memory per key, origin, and slot range.
- Healthy-target p99 while another peer is delayed or partitioned.
- Mutation-log compaction and tombstone collection under write churn.
- Reconfiguration seeding with foreground reads and writes.

Run production benchmarks on Adam with separate pinned client and peer CPU
sets. Preserve raw results and commands under `benchmarks/`.

## Acceptance Gates

- All connected peers converge after finite writes and eventual message delivery.
- Every built-in conflict policy is deterministic, associative, commutative,
  and idempotent over its supported operations.
- A deleted or expired value cannot be resurrected by an older peer.
- Explicit sync never reports complete while a selected peer or divergent range
  remains unresolved.
- Network partitions do not block local-mode writes.
- Node addition or removal does not change slot identity or move unaffected
  ranges.
- A delayed peer increases healthy-peer p99 by no more than 2x.
- Local-mode write throughput remains at least 90% of embedded-only throughput
  while queues have capacity.
- Local-mode p99 remains no more than 1.2x the embedded baseline.
- No global worker, endpoint mutex, or connection lock appears in the mutation
  or sync hot paths.
- All logs, summaries, conflicts, tombstones, digests, queues, and frames have
  tested hard limits.
- `cargo fmt --check`, workspace tests, feature-matrix checks, rustdoc, and
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` pass.

## Rollout

1. Ship version metadata and conflict policies disabled by default.
2. Enable shadow mutation streaming while reads remain local and unchanged.
3. Enable explicit sync for embedded shardmaps with background sync disabled.
4. Enable background anti-entropy and convergence monitoring.
5. Enable active writes for exact point operations under explicit policies.
6. Enable overlapping replica-group changes after retirement tests pass.
7. Add Redis command families only when their convergence semantics are
   documented and history-tested.

Existing single-primary replication and KV overflow remain supported. Active
sync uses a separate feature flag and configuration surface until its behavior
and operational costs are proven.

## Open Decisions

- Choose the causal-context representation after measuring per-key memory cost.
- Choose the digest tree shape and rebuild strategy.
- Define stable merge-policy registration and upgrade compatibility.
- Decide whether `lww_hlc` may be the server default or must always be explicit.
- Define Redis error behavior for unsupported active-active commands.
- Choose the external topology backend and minimum supported version.
- Set offline-member retention and clock quarantine defaults from production
  recovery measurements.
