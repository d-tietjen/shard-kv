# Partitioned Key-Value Overflow

The `kv-overflow` feature turns an embedded `EmbeddedStore` into a bounded
in-memory primary backed by a horizontally scalable shardcache server tier.
Unlike a conventional read-replica set, overflow nodes do not each receive the
whole data set. Keys hash into a fixed logical slot space and rendezvous
ownership assigns each complete slot to one node, so usable remote capacity
grows with the number of nodes without coupling slots to the primary's local
shard count.

Writes are applied to the local primary and admitted to a bounded asynchronous
replication pool. Deterministic worker lanes preserve mutation order for each
key without putting network latency on the primary write path. A value becomes
eligible for local eviction only after its matching generation is acknowledged
by the remote owner. When resident bytes exceed the configured target, ShardMap
chooses acknowledged victims with its LRU or LFU metadata. Pending or failed
replications remain resident even when that temporarily exceeds the target.
Queue admission happens before the local mutation, so a full queue returns
`ShardCacheError::Backpressure` without changing the primary value.

On a local miss, `KvOverflowStore::get` fetches the deterministic owner and
promotes the value back into memory. Other services can call
`KvOverflowCluster::get` to read the same owner directly without sending read
traffic through the in-memory primary.

```toml
[dependencies]
shardmap = { version = "0.6.0", features = ["kv-overflow"] }
```

```rust,ignore
use shardmap::config::{EvictionPolicy, KvOverflowConfig};
use shardmap::embedded::{KvOverflowStore, ShardedEngine};

let config = KvOverflowConfig {
    enabled: true,
    endpoints: vec![
        "10.0.0.11:6380".into(),
        "10.0.0.12:6380".into(),
    ],
    slot_count: 16_384,
    max_memory_bytes: 1024 * 1024 * 1024,
    eviction_policy: EvictionPolicy::Lfu,
    ..KvOverflowConfig::default()
};

let cache = KvOverflowStore::from_config(ShardedEngine::new(16), &config)?;
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
  `KvOverflowStore::try_entry_snapshot`, which fails if a remote-only value
  cannot be materialized.
- `KvOverflowStore::from_config` mirrors recovered resident values before it
  starts enforcing the memory target.
- `KvOverflowStore::set` confirms local application and queue admission, not
  remote visibility. `flush_remote` waits for every mutation admitted before
  the call and returns an error if any worker lane reported a failure.
- `worker_threads` controls ordered replication lanes and `queue_capacity`
  bounds queued plus active jobs across all lanes. Size the queue for expected
  bursts and monitor `queue_depth`, `pending_keys`, `active_workers`,
  `enqueue_failures`, and `replication_failures`.
- `slot_count` is a persistent routing invariant and must not change while
  overflow data exists. It defaults to 16,384 and is independent of local
  `shard_count` and the number of overflow servers.
- Horizontal expansion moves complete logical slots. Rendezvous ownership
  guarantees that adding a node moves slots only from an old owner to the new
  node; slots that do not move retain their existing owner.
- For online expansion, put the expanded membership in `endpoints` and the old
  membership in `previous_endpoints`. Writes establish the current-owner copy
  before deleting the old copy. A current-owner miss may read the previous
  owner, but fallback reads never mutate either node; this prevents an
  uncoordinated reader from overwriting a concurrent primary write. Current
  owner errors never fall back, avoiding stale reads after an acknowledged
  handoff.
- Keep `previous_endpoints` configured until the authoritative local snapshot
  has been loaded and `synchronize_resident`/`flush_remote` has completed for
  the expanded membership. Then remove it. Node removal uses the same handoff
  mechanism, but the removed server must remain reachable during migration.
- Monitor `previous_node_count`, `handoff_reads`, `handoff_hits`, and
  `handoff_failures` while a membership handoff is active.
- Each key has one remote owner in 0.6.0. This provides aggregate capacity and
  read isolation, not remote-node high availability. Run overflow nodes with
  their own persistence or object overflow when remote loss must survive until
  the primary can rebuild them.
- Use `KvOverflowCluster` for direct reads. Values on the raw server include a
  versioned expiry, length, and CRC32 envelope that the cluster client checks.
- TTL starts when the primary accepts the write, not when a worker reaches the
  queued replication job. It is enforced from the absolute envelope deadline
  during every remote read. Redis also receives the remaining server-side TTL.
  The wrapped primary deletes an expired or missing remote copy on fault-in and
  runs a configurable cleanup pass for expired envelopes that are not read.
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
shardmap = { version = "0.6.0", features = ["kv-overflow-redis"] }
```

```toml
[kv_overflow]
enabled = true
backend = "redis"
endpoints = [
  "rediss://cache-a.example.com:6380/0",
  "rediss://cache-b.example.com:6380/0",
]
previous_endpoints = []
slot_count = 16384
redis_key_prefix = "my-service:overflow:"
redis_username_env = "OVERFLOW_REDIS_USERNAME"
redis_password_env = "OVERFLOW_REDIS_PASSWORD"
max_memory_bytes = 1073741824
eviction_policy = "lfu"
```

Endpoint URLs may use `redis://`, `rediss://`, `valkey://`, or `valkeys://`.
Credentials in URLs are rejected so secrets cannot appear in configuration,
debug output, or ownership IDs; configure the names of credential environment
variables instead. The URL path selects the Redis database. All endpoints in a
membership use the same backend and key prefix.

Redis keys are binary-safe and namespaced with `redis_key_prefix`. Values use
the same versioned expiry, length, and CRC32 envelope as SCNP. TTL values also
use Redis `SET ... PX`, so the database removes expired envelopes without
waiting for a Shardmap cleanup pass. Connections are pooled per endpoint and
use the same connect timeout, operation deadline, retry count, and backoff
settings as SCNP nodes. TLS certificate verification is enabled for `rediss://`
and `valkeys://` endpoints.

The adapter targets standalone Redis-compatible endpoints. Redis Cluster
`MOVED`/`ASK` topology discovery is not part of 0.6.0; use one endpoint per
independent overflow owner or a managed endpoint that handles routing behind a
stable address.

## Replica LRU And Object Overflow

An overflow node can enforce its own `max_memory_bytes` with `eviction_policy =
"lru"` or `"lfu"`. Plain replica eviction is intentionally lossy: if the node
drops an envelope, a later direct read misses. A wrapped-primary fault-in also
invalidates its remote acknowledgment metadata. The local WAL/snapshot remains
authoritative, but the value is unavailable through the live overflow tier
until it is restored from that authority or refilled from its origin.

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

The benchmark isolates local enqueue cost from remote drain time by holding
in-process overflow workers until the producer finishes:

```bash
cargo run --release -p shardcache-benchmarks --features kv-overflow \
  --bin kv_overflow_primary_cost -- \
  --iterations 200000 --keys 16384 --value-size 1024 --worker-threads 2
```

It reports plain `EmbeddedStore::set`, `KvOverflowStore::set` admission, the
enqueue overhead ratio, and the remaining `flush_remote` drain time. Use the
live two-server integration path for end-to-end SCNP latency; this benchmark is
specifically the primary CPU and synchronization budget.

Three release runs on the development machine on 2026-07-13 with the command
above measured medians of 121.9 ns/op for embedded SET and 250.2 ns/op for
KV-overflow queue admission. That is 128.3 ns of primary-path overhead, 2.05x
the plain SET cost, and approximately 4.00 million admitted 1 KiB writes/second
on one producer thread. Treat this as a local implementation regression
baseline, not a cross-machine capacity claim.
