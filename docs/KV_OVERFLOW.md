# Partitioned Key-Value Overflow

The `kv-overflow` feature turns an embedded `EmbeddedStore` into a bounded
in-memory primary backed by a horizontally scalable shardcache server tier.
Unlike a conventional read-replica set, overflow nodes do not each receive the
whole data set. Every primary shard exclusively owns a deterministic set of
remote shard targets and the TCP connections to those targets. With 16 primary
shards and one 16-shard replica, primary shard `N` owns remote shard `N`. With
16 replicas, primary shard `N` owns complete replica `N`. Larger memberships
divide complete replicas evenly across primary shards without sharing a target.
Each primary shard's contiguous logical slot range is subdivided only among its
owned targets.

Writes are applied to the local primary and admitted to a bounded asynchronous
replication pool. A precomputed fixed-slot table makes ownership lookup O(1).
Each embedded shard has one dedicated background I/O drain, an independent
bounded queue, and its own eviction-maintenance lock. A saturated shard does not
consume another shard's admission capacity. Deterministic shard-local lanes
preserve mutation order for each key without putting network latency on the
primary write path. SCNP and Redis workers drain ordered pipelines of up to 64
writes per target. A value becomes eligible for local
eviction only after its matching generation is acknowledged by the remote
owner. The configured memory target is divided deterministically across the
embedded shards; each shard chooses acknowledged victims from its own LRU or
LFU metadata. Pending or failed replications remain resident even when that
temporarily exceeds a shard's target.
Queue admission happens before the local mutation, so a full shard queue
returns `ShardCacheError::Backpressure` without changing the primary value.

On a local miss, `KvOverflowStore::get` fetches the deterministic owner and
promotes the value back into memory. Other services can call
`KvOverflowCluster::get` to read the same owner directly without sending read
traffic through the in-memory primary.

Protected exact values use `KvOverflowStore::set_with_governance`. Ordinary
primary and cluster GETs fail closed before network I/O when the primary knows
the remote entry is protected. `get_with_governance_filter` authorizes and may
promote a value; `get_remote_with_governance_filter` authorizes without
touching primary memory. Metadata is part of the versioned overflow envelope,
CRC32 integrity check, retry payload, handoff verification, snapshot, and
promotion. See [`EXACT_GOVERNANCE.md`](EXACT_GOVERNANCE.md).

```toml
[dependencies]
shardmap = { version = "0.7.2", features = ["kv-overflow"] }
```

```rust,ignore
use shardmap::config::{EvictionPolicy, KvOverflowConfig, KvOverflowReplica};
use shardmap::embedded::{EmbeddedStore, KvOverflowStore};

let config = KvOverflowConfig {
    enabled: true,
    cluster_id: "production-cache".into(),
    replicas: vec![KvOverflowReplica {
        id: "overflow-a".into(),
        addresses: vec!["10.0.0.11:6380".into()],
        shard_count: 16,
        direct_shard_base_port: 6381,
    }],
    slot_count: 16_384,
    max_memory_bytes: 1024 * 1024 * 1024,
    max_metadata_bytes: 256 * 1024 * 1024,
    max_key_bytes: 1024 * 1024,
    eviction_policy: EvictionPolicy::Lfu,
    ..KvOverflowConfig::default()
};

let cache = KvOverflowStore::from_config(EmbeddedStore::new(16), &config)?;
cache.set(b"model:7".to_vec(), payload, None)?;

// Optional durability/visibility boundary for all writes admitted so far.
cache.flush_remote()?;

// Hot-path primary read, with remote fault-in on a local miss.
let value = cache.get(b"model:7")?;

// A service-side read that bypasses the primary.
let remote = cache.cluster().get(b"model:7")?;
# Ok::<(), shardmap::ShardCacheError>(())
```

## Operational Contract

- Local WAL and snapshots remain authoritative. Use
  `KvOverflowStore::write_persistence_snapshot`, which atomically streams
  resident and remote-only values into a bounded-memory snapshot, syncs the
  directory, and prunes WAL only after the snapshot is durable. It fails if a
  remote value cannot be materialized. `restore_latest_snapshot` incrementally
  rebuilds the overflow tier while enforcing the resident target.
- `KvOverflowStore::from_config` mirrors recovered resident values before it
  starts enforcing the memory target.
- `KvOverflowStore::set` confirms local application and queue admission, not
  remote visibility. `flush_remote` waits for every mutation admitted before
  the call. Failed current generations remain resident and retry on each flush;
  the maintenance interval also retries them in the background. The flush
  cannot report success while an admitted generation is still dirty.
  A batch transport failure is retried per item so one ambiguous result does
  not mark unrelated keys as replicated or failed.
- Configured stores use one drain per primary shard and
  `queue_capacity_per_shard` admissions for each drain. Monitor
  `primary_shard_count`, `drains_per_shard`, `shard_queue_depths`,
  `shard_queue_capacities`, `completion_backlog`,
  `shard_completion_backlogs`, `pending_keys`, `failed_pending_keys`,
  `active_workers`, `enqueue_failures`, and `replication_failures`.
  Multi-target shards divide that capacity into bounded target lanes. A failed
  target can retain only its share and cannot consume every admission assigned
  to healthy targets on the shard.
- A shard's network drain owns only its queue, transport connections, reusable
  buffers, and network counters. It never receives or locks the embedded
  store, remote/pending metadata maps, key gates, or eviction state. Successful
  acknowledgements contain no value payload and return through a bounded
  single-producer completion lane owned by that primary shard. The lane shares
  no mutex with the worker. Its capacity is covered by the shard's in-flight
  reservation, so a worker can always publish an acknowledgement without
  blocking. The primary path applies bounded completions at admission pressure
  or while that shard is above its memory target; otherwise they are applied at
  `flush_remote` or by periodic maintenance.
- A shard with one SCNP target uses the lower-overhead blocking pipeline because
  there is no target-level head-of-line risk. A shard with multiple targets runs
  those target connections independently on one current-thread Tokio runtime;
  a delayed target therefore cannot stall healthy targets owned by the same
  shard. `max_inflight_per_target > 1` creates stable key-hash lanes so the same
  key remains ordered. Every target also has a separate lazy read socket.
  Direct shard transport is the default and never silently falls back to
  fanout. At startup every configured address must
  advertise the same node ID, shard count, `overflow_slot` route mode, direct
  port base, and `overflow_slot_v1` capability.
- Native TLS 1.3 is available through the `scnp-tls` feature. Each shard-owned
  mutation/read socket terminates its own Rustls session; there is no shared
  TLS proxy or connection lock. Replica certificates are verified against the
  configured CA and `tls_server_name` (the stable replica ID by default).
  The workspace disables default TLS features and selects Rustls with the
  `ring` provider; SCNP accepts TLS 1.3 only. CI rejects OpenSSL, native-tls,
  and OpenSSL-backed Rustls providers from the all-features dependency graph.
  Application frame decoders require the complete declared body before
  dispatch and response writers derive their lengths from initialized slices,
  preventing Heartbleed-style peer-length memory disclosure.
  Configuring a server client CA and matching primary client certificate/key
  enables mandatory mTLS. CA-valid clients must also match a configured
  `client_cert_sha256` leaf-certificate fingerprint; overlapping fingerprints
  support rotation. Server certificate, key, and CA files reload atomically on
  a dedicated thread at `reload_interval_ms`, while clients reload trust and
  identity files before a later reconnect. `max_concurrent_handshakes` rejects
  excess handshakes before
  they can occupy the connection pool. Non-loopback SCNP requires TLS plus token auth or
  mTLS. `allow_insecure_scnp = true` is the explicit private-overlay escape
  hatch. Tokens may be read from static environment variables or reloadable
  mounted files. Replica token rotation accepts the prior token for two reload
  intervals so primary connections can roll without downtime. Token values are
  redacted from debug output.
- Every owned target has one lock-free circuit breaker shared by its read,
  mutation, and pipeline lanes. Consecutive failures open only that target for
  `circuit_breaker_cooldown_ms`; healthy
  targets owned by the same primary shard continue draining. The first request
  after cooldown is the only permitted half-open probe.
- `adaptive_pipeline` uses target-local RTT feedback to reduce the item limit
  after slow or failed batches and grow it toward `pipeline_max_items` after
  healthy batches. `pipeline_max_bytes` remains a hard memory and tail-latency
  cap.
- `compression = "lz4"` optionally compresses values on the offloader thread.
  Values below `compression_min_value_bytes`, or values that do not clear
  `compression_min_savings_percent`, retain the original v1 envelope. The v2
  compressed envelope preserves expiry, original length, and CRC32 and remains
  readable alongside existing v1 data. Enabling compression also requires
  `allow_envelope_v2_writes = true`, and replicas must advertise
  `value_envelope_v2`. Set the acknowledgement only after every primary that
  may read the cluster has been upgraded; do not downgrade those readers while
  v2 values remain.
- Remote values are rejected before allocation when their decoded length exceeds
  `max_value_bytes` or their compression expansion exceeds
  `compression_max_expansion_ratio`.
- Primary key tracking is independently bounded by `max_metadata_bytes` and
  `max_key_bytes`. A zero metadata limit derives the larger of 25% of
  `max_memory_bytes` or one maximum-size key charge.
  New unique keys receive backpressure when the metadata budget is full;
  overwrites remain admitted, and confirmed deletes release their charge.
  Health snapshots expose the configured limits, current `metadata_bytes`, and
  `metadata_rejections`. Failed-mutation and expiry cleanup uses fixed-size
  stripe batches so maintenance cannot duplicate the complete key index.
- Resident admission is isolated per primary shard. Growth is rejected above
  the shard's share of `max_memory_bytes` plus one `max_key_bytes` and
  `max_value_bytes` envelope; non-growing overwrites remain admitted. This
  bounds replica-outage and stalled-pipeline memory without forcing healthy
  asynchronous writes to wait for each remote acknowledgement.
  Cold reads still return their remote value when the ceiling is full, but
  skip promotion into primary memory. `resident_backpressure` reports rejected
  or bypassed growth.
- `slot_count` is a persistent routing invariant and must not change while
  overflow data exists. It defaults to 16,384, must be at least the shard
  count, and is divided evenly into power-of-two shard ranges. Logical slots
  use the same stable hash prefix as embedded routing, so increasing the
  primary shard count subdivides existing ranges without renumbering keys.
  The maximum is 1,048,576 so the precomputed owner table has a bounded
  startup and memory cost.
- Topology changes perform an exact rebalance. This can move ranges between
  existing replicas as well as onto a newly added replica.
  Handoff runs independently across primary shards, with process concurrency
  bounded by `handoff_max_concurrency`, in-memory key batches bounded by
  `handoff_batch_items` per primary shard, and bandwidth bounded by
  `handoff_max_bytes_per_second` per shard. Source reads and throttling happen
  outside striped generation gates; the gate is reacquired and revalidated
  before writing the new owner.
- For online expansion, put the expanded membership in `replicas` and the old
  membership in `previous_replicas`. Writes establish the current-owner copy
  before deleting the old copy. A current-owner miss may read the previous
  owner, but fallback reads never mutate either node; this prevents an
  uncoordinated reader from overwriting a concurrent primary write. Current
  owner errors never fall back, avoiding stale reads after an acknowledged
  handoff.
- Changing the embedded primary shard count requires a restart because the
  local shard array is immutable. Keep the old membership in
  `previous_replicas` (it may equal the current membership) and set
  `previous_primary_shard_count` to the old count. Local WAL/snapshot recovery
  remains authoritative; the previous geometry lets re-offload and explicit
  synchronization find and remove copies placed by the old shard ownership.
- Keep `previous_replicas` configured until the authoritative local snapshot
  has been loaded and `synchronize_resident`/`flush_remote` has completed for
  the expanded membership. Then remove it. Node removal uses the same handoff
  mechanism, but the removed server must remain reachable during migration.
  `flush_remote` also migrates remote-only metadata by reading the old target,
  writing and verifying the new target, then deleting the old copy. Handoff
  records completion per key, so repeated flushes do not migrate completed keys.
  Network I/O holds only that key's striped mutation gate; unrelated primary
  writes continue while the caller waits for synchronization.
- Monitor `previous_node_count`, `handoff_reads`, `handoff_hits`, and
  `handoff_failures` while a membership handoff is active.
- Health snapshots report circuit state, suppressed requests, current adaptive
  item limits, raw/stored bytes, compressed values, retained-buffer limits,
  TLS, queue, pipeline, and ownership state without exposing certificate paths
  or credentials.
- Each key has one remote owner in 0.6.0. This provides aggregate capacity and
  read isolation, not remote-node high availability. Run overflow nodes with
  their own persistence or object overflow when remote loss must survive until
  the primary can rebuild them.
- Use `KvOverflowCluster` for direct reads. Values on the raw server include a
  versioned expiry, length, and CRC32 envelope that the cluster client checks.
- TTL starts when the primary accepts the write, not when a worker reaches the
  queued replication job. It is enforced from the absolute envelope deadline
  during every remote read. Redis also receives the remaining server-side TTL.
  Fault-ins coalesce concurrent reads for the same cold key without holding a
  striped mutation lock during network I/O. By default, a missing acknowledged
  remote value retains its metadata so persistence snapshots fail loudly.
  `forget_remote_misses = true` enables intentionally lossy cache semantics.
  The primary runs a configurable cleanup pass for expired envelopes.
  Cleanup deletes use the same ordered worker lanes as writes and retry in the
  background without blocking unrelated primary mutations on remote I/O.
- Deletes remove the remote copy before deleting the primary copy. A remote
  delete failure is returned and the primary value remains available.
- The wrapper supports byte-string entries. Redis object families and session
  slots are outside the 0.6.0 overflow surface.
- The wrapped primary is an embedded API in 0.6.0. Standalone shardcache
  processes can serve as overflow nodes, but enabling `[kv_overflow]` on a
  standalone primary is rejected so writes cannot silently bypass mirroring.

Overflow is most useful for durable or expensive cache state whose long tail
is worth retaining, while the working set still needs in-process latency. For
cheap disposable values, ordinary eviction and an origin refill remain the
simpler design.

## Redis And Valkey Endpoints

Enable `kv-overflow-redis` instead of `kv-overflow` to use Redis, Valkey, or a
compatible managed service as the overflow tier:

```toml
[dependencies]
shardmap = { version = "0.7.2", features = ["kv-overflow-redis"] }
```

```toml
[kv_overflow]
enabled = true
backend = "redis"
cluster_id = "production-cache"
slot_count = 16384
redis_key_prefix = "my-service:overflow:"
redis_username_env = "OVERFLOW_REDIS_USERNAME"
redis_password_env = "OVERFLOW_REDIS_PASSWORD"
max_memory_bytes = 1073741824
max_metadata_bytes = 268435456
max_key_bytes = 1048576
eviction_policy = "lfu"

[[kv_overflow.replicas]]
id = "redis-a"
addresses = ["rediss://cache-a.example.com:6380/0"]
shard_count = 1

[[kv_overflow.replicas]]
id = "redis-b"
addresses = ["rediss://cache-b.example.com:6380/0"]
shard_count = 1
```

Endpoint URLs may use `redis://`, `rediss://`, `valkey://`, or `valkeys://`.
Credentials in URLs are rejected so secrets cannot appear in configuration,
debug output, or ownership IDs; configure the names of credential environment
variables instead. The URL path selects the Redis database. All endpoints in a
membership use the same backend and key prefix.

Redis keys are binary-safe and namespaced with `redis_key_prefix`. Values use
the same versioned expiry, length, and CRC32 envelope as SCNP. TTL values also
use Redis `SET ... PX`, so the database removes expired envelopes without
waiting for a Shardmap cleanup pass. Ordered worker batches use Redis pipelines
to amortize network round trips. Connections are pooled per endpoint and use
the same connect timeout, operation deadline, retry count, and backoff settings
as SCNP nodes. TLS certificate verification is enabled for `rediss://` and
`valkeys://` endpoints.

The adapter targets standalone Redis-compatible endpoints. Redis Cluster
`MOVED`/`ASK` topology discovery is not part of 0.6.0; use one endpoint per
independent overflow owner or a managed endpoint that handles routing behind a
stable address.

## Replica LRU And Object Overflow

Durable replica WAL and snapshot files contain overflow envelopes. Put the
configured persistence directory on an encrypted filesystem or volume and set
`kv_overflow_replica.encrypted_persistence = true`; startup rejects durable
replica mode without that confirmation. This flag is an operational assertion,
not application-level cryptography. For S3/RustFS object overflow, configure
`server_side_encryption` as well. TLS protects network traffic, not files.

An overflow node can enforce its own `max_memory_bytes` with `eviction_policy =
"lru"` or `"lfu"`. Replica startup rejects this configuration unless object
overflow is enabled. Plain replica eviction can be selected explicitly with
`kv_overflow_replica.allow_lossy_eviction = true`; a later direct read may then
miss. Durable primaries retain the acknowledgment metadata by default, causing
the next snapshot to fail rather than silently omit the unavailable value.
Set `forget_remote_misses = true` only when origin refill defines correctness.

For a capacity-preserving cascade, run each overflow node with both a memory
limit and `[object_overflow]`. The node keeps its hot envelope working set in
RAM, moves cold envelopes to filesystem, S3, or RustFS object storage, and
faults them back through the same SCNP GET path. This composes the two distinct
features as:

`embedded primary RAM -> partitioned replica RAM -> object storage`

The primary still chooses only acknowledged cold values for its own eviction;
each replica independently applies LRU/LFU to its partition. The filesystem
integration test exercises this full path, including an SCNP read after the
replica has moved the envelope to object storage.

An overflow-node configuration can use the existing server memory policy and
object tier directly:

```toml
max_memory_bytes = 8589934592
eviction_policy = "lru"

[object_overflow]
enabled = true
backend = "s3"
endpoint = "http://rustfs:9000"
bucket = "shardcache-overflow"
prefix = "replica-tier"
node_id = "overflow-replica-a"
region = "us-east-1"
force_path_style = true
allow_http = true
access_key_env = "RUSTFS_ACCESS_KEY"
secret_key_env = "RUSTFS_SECRET_KEY"
min_value_bytes = 4096
offload_min_idle_ticks = 1024
compression = "zstd"
worker_threads = 2
queue_capacity = 1024
```

Do not enable `[kv_overflow]` on this server. It is an overflow node whose
local memory policy cascades to object storage, not another embedded primary.

## Primary Cost Benchmark

The benchmark can isolate local enqueue cost by holding in-process overflow
workers until the producers finish:

```bash
cargo run --release -p shardcache-benchmarks --features kv-overflow \
  --bin kv_overflow_primary_cost -- \
  --iterations 500000 --keys 65536 --value-size 1024 \
  --producers 4 --drain-mode blocked
```

Use `--drain-mode concurrent` to overlap primary writes with active drains. Live
SCNP or Redis runs use `--backend`, one or more repeated `--endpoint` arguments,
and the matching benchmark feature. The output separates primary enqueue time,
post-producer drain time, and end-to-end replication throughput.
Set `--read-iterations` to add embedded GET and direct overflow GET throughput
plus sampled p50/p95/p99 latency after all writes are remotely visible.
Use `--max-memory-bytes` and `--max-metadata-bytes` to benchmark the bounded
production path. Both default to `usize::MAX` so historical enqueue-only runs
continue to isolate queue and worker overhead.

To measure topology-validated direct ports and SCNP pipelines against a
16-shard replica:

```bash
cargo run --release -p shardcache-benchmarks --features kv-overflow \
  --bin kv_overflow_primary_cost -- \
  --backend scnp --transport direct --endpoint 10.0.0.11:6380 \
  --scnp-replica-id overflow-a --scnp-shard-count 16 \
  --pipeline-max-items 64 --value-size 1024 --drain-mode concurrent
```

Build with `--features kv-overflow,scnp-tls` and add
`--scnp-tls-ca`, `--scnp-tls-server-name`, and optional client certificate/key
arguments to compare plaintext, server-authenticated TLS, and mTLS. Use
`--compression lz4` for a separate compressible-value run; do not combine the
TLS and compression changes in the same A/B baseline.

Five release runs on Adam on 2026-07-13 used four producers, 1 KiB values,
131,072 keys, 400,000 writes, Valkey 8.1.8 containers pinned to dedicated CPUs,
and four workers per endpoint. Medians were:

| Valkey endpoints | Workers | Embedded SET | Overflow enqueue | End-to-end replicated writes/s |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 4 | 350 ns/op | 954 ns/op | 339K |
| 2 | 8 | 350 ns/op | 991 ns/op | 506K |
| 4 | 16 | 343 ns/op | 1,309 ns/op | 740K |

Redis pipelining raised the single-endpoint, four-worker median from 34.4K to
347K replicated writes/second in a separate 200,000-write run, approximately
10.1x. With a concurrent in-process no-op endpoint, four workers admitted a
median 1.93 million writes/second at 517 ns/op, isolating the local metadata and
queue path from network service time.

These results are regression and sizing references, not universal capacity
claims. The queue absorbs bursts but cannot create remote capacity: sustained
write throughput must remain below the aggregate endpoint drain rate, or the
bounded queue will eventually return `ShardCacheError::Backpressure` before
mutating the primary.

An interleaved release A/B on Adam on 2026-07-14 compared the memory-bound
hardening against commit `46a4ca8`. All cases used release builds pinned to
separate server/client CPU sets and three or more repetitions. Median results
stayed inside the release gates of 5% throughput and 10% p99 regression:

| Path | Workloads | Median throughput delta | Worst median p99 delta |
| --- | --- | ---: | ---: |
| KV enqueue, unlimited | 1 KiB, 4 producers, 2M writes | +7.0% | within gate |
| KV enqueue, resident + metadata limits | 1 KiB, 4 producers, 1M writes | -3.5% | within gate |
| Embedded map | GET, SET, 80/20; 64 B and 4 KiB | -1.4% to +2.3% | within timer resolution |
| Local/TCP replication | 64 B and 4 KiB mutation batches | -1.4% to +1.5% | within gate |
| RESP server | GET, SET, and 65-argument DEL; pipeline 1/16 | -3.3% to +2.6% | +3.5% |
| SCNP fanout | 80/20; 64 B and 4 KiB; pipeline 1/16 | -3.6% to +1.6% | +4.9% |

The bounded KV path admitted a median 2.05 million writes/second. Replication
runs reported no drops or backpressure, and server runs reported no protocol
errors.

### Shard-Owned Direct SCNP Results

The shard-local counter and bounded-queue implementation was measured on Adam
on 2026-07-13 with 16 primary shards, exactly 16 network drains, four producers,
1 KiB values, 131,072 keys, and 400,000 writes. Five in-process no-op runs
admitted a median 1.70 million writes/second at 589 ns/op after reusing the
already-computed xxh3 key hash for overflow metadata. Median end-to-end
throughput was 1.135 million writes/second and the median sampled enqueue p99
was 8.19 us. A longer run before the metadata optimization admitted 1.96
million writes/second with four producers and 3.05 million with sixteen. Before
the shard-local queue/counter work, the four-producer result was 1.68 million;
cache misses fell 19%, context switches 10%, and total cycles 11% under
`perf stat`.

The primary path now stores the queued generation in the resident entry, so a
successful SET does not allocate pending metadata. Five no-op runs after that
change admitted a median 1.90 million writes/second at 527 ns/op and completed
1.269 million writes/second end to end. Idle SCNP connection buffering is 16
KiB per socket pair rather than 128 KiB.

Five 1 KiB and three 64 KiB live runs used one 16-shard direct SCNP replica on
separately pinned CPUs, pipeline size 64, and a 256 KiB pipeline byte cap.
Medians were:

| Value size | Embedded SET | Overflow enqueue | End-to-end writes/s | Remote GET p50 | Remote GET p99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB | 358 ns/op | 680 ns/op | 1.044M | 49.5 us | 132 us |
| 64 KiB | 9.6 us/op | 16.9 us/op | 58.4K | not measured | not measured |

The 64 KiB byte cap limits pipelines to roughly four values. Compared with an
unbounded 64-item batch, it reduced median sampled enqueue p99 from about 1.07
ms to 229 us and raised end-to-end throughput by about 11%. Shards that own
multiple targets use independent target tasks; a regression test holds one
target for 300 ms and verifies a healthy target completes within 150 ms on the
same shard-owned runtime.

A synthetic no-network topology comparison used the benchmark's
`--noop-replicas` and `--noop-shards-per-replica` controls. Median end-to-end
throughput was 854K writes/second for `16x1` and 930K for `16x500`, confirming
that batch CPU and allocation cost no longer grows with all 8,000 cluster
targets. This does not test socket latency or slow-target isolation.

End-to-end throughput is effectively unchanged for the measured workload. The
enqueue path still has a small absolute latency premium from metadata and queue
admission; that overhead is retained in exchange for bounded asynchronous
replication and failure isolation.
