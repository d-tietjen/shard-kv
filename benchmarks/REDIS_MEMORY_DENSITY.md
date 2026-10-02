# Redis Memory Density Benchmark

This suite compares ShardCache and Redis on resident memory for the same Redis
string keyspace, then measures GET/SET performance against that loaded data. It
reports process PSS and RSS, cgroup memory, and Redis allocator statistics when
the target exposes them. Incremental process PSS is the primary density metric;
cgroup anonymous memory is the cross-server cross-check, and allocator-specific
numbers are supporting data. Process maps are read under the server's in-container
user so Linux permits access to its `smaps_rollup` data.

## Run

Run on Adam or another Linux host with Docker Engine and cgroup v2:

```bash
python3 benchmarks/scripts/run-memory-density-benchmark.py \
  --targets redis,shardcache-resp \
  --keys 100000 \
  --value-sizes 16,64,256,1024,4096 \
  --repeats 3 \
  --vcpus 1 \
  --clients 16 \
  --pipeline 1 \
  --duration 10
```

The default starts each target in a fresh container for every data point. It
uses Redis 7.4 and builds ShardCache inside the repository's Rust 1.93
Bookworm builder stage with a bounded Cargo job count, then uses the same Debian
Bookworm runtime stage as the production Dockerfile. Both server containers
receive the same Docker CPU quota and memory/swap cap. CPU affinity is not
pinned; the client and server share the host scheduler. Redis persistence and
eviction are disabled.
The ShardCache container uses one shard to match Redis's single-threaded server
shape. The runner uses unique container names and removes only containers it
created.

By default, ShardCache is built with `redis-server`. To A/B the existing
experimental adaptive no-TTL point map for this no-TTL point-key workload, add
`--shardcache-features` with the value
`redis-server,shardmap/unsafe,shardmap/experimental-no-ttl-point-hot-path`.
The manifest and per-row CSV record the enabled feature set. The fast map
returns eligible string points from its compact table and promotes entries to
the general map when an operation needs unsupported metadata.

For a faster diagnostic pass, use `--repeats 1 --duration 5`. A run without an
authoritative resource reservation is classified as a diagnostic; performance
may be affected by concurrent host workloads. The runner writes
`memory-density.csv`, one raw saturation CSV per point, container logs and
inspection records, the ShardCache runtime image context, a manifest, and a
Markdown summary under `benchmarks/results/`.

## Measurements

The standard point workload creates fixed 18-byte keys and the selected value
size. Logical payload is `key_count * (18 + value_size)`. The runner defaults to both compressible (`x` repeated) and deterministic
high-entropy (SplitMix64) value patterns. `--value-patterns repeating` reproduces
the original deterministic 0..255 pattern. Every key has the same payload within
a profile; the payload SHA-256 is recorded and exact GETs of three sample keys
are checked after load. This is a per-value representation profile and does not
qualify inter-key deduplication. The suite records
idle and loaded memory, then computes incremental bytes per key and memory
amplification (`incremental PSS / logical key+value bytes`). It also retains
ops/sec, server CPU, and p50/p99/p99.9 latency from the existing saturation
driver against the selected key distribution. `--distributions
'uniform;hot:1000:90'` adds a hot/cold access profile; it leaves the resident
keyspace and payload unchanged. Reports group by pattern, distribution, and
value size instead of mixing distinct shapes.

The loaded sample is taken after the saturation phase and a settling interval;
all keys remain resident because SET operations overwrite keys from the same
keyspace. `DBSIZE` must match the requested key count and the performance run
must report zero errors. Each repeat uses a fresh server to avoid allocator
fragmentation from earlier points.

The density ratio in the report is ShardCache's incremental PSS bytes per key
divided by Redis's for the matching shape. A ratio below `1.0x` means
ShardCache used less incremental resident memory for that workload. The result
must be read together with throughput and tail latency: a density win that
causes a material performance regression is not an acceptable optimization.

## Initial Adam diagnostic results

Two unreserved diagnostic runs completed on Adam on 2026-10-01. Each used
100,000 keys, 18-byte keys, one server vCPU, 16 clients, an 80/20 GET/SET mix,
and three fresh-server repeats at each value size. Neither run pinned CPU
affinity, so throughput and latency are indicative rather than publishable.

The second run enabled the existing
`redis-server,shardmap/unsafe,shardmap/experimental-no-ttl-point-hot-path`
features. It changed ShardCache's median incremental PSS by less than 0.4% at
every size. It therefore does not close the small-value density gap in this
profile.

| Value bytes | Redis PSS B/key | ShardCache default B/key | Default / Redis | ShardCache fast-map B/key | Fast-map / Redis | Fast-map PSS change |
|---:|---:|---:|---:|---:|---:|---:|
| 16 | 101.01 | 278.98 | 2.762x | 277.98 | 2.743x | -0.4% |
| 64 | 158.21 | 326.92 | 2.066x | 326.29 | 2.064x | -0.2% |
| 256 | 400.06 | 519.37 | 1.298x | 517.43 | 1.294x | -0.4% |
| 1024 | 1368.08 | 1286.78 | 0.941x | 1286.14 | 0.940x | -0.05% |
| 4096 | 5241.10 | 4360.21 | 0.832x | 4359.33 | 0.832x | -0.02% |

ShardCache's memory crossover is between 256-byte and 1-KiB values in both
runs. The fast-map build showed similar throughput and tail latency to the
default build, but these unreserved runs do not qualify a performance claim.
The current measurements point to per-entry representation and allocation
overhead as the next area to profile; a useful follow-up should first test a
packed small-key/value layout, then add high-entropy values and hot/cold access
before making any online compaction or compression claim.

Full reports and raw evidence are in the ignored local results bundles:

- [Default build diagnostic](results/adam-memory-density-4ca506c/memory-density-20261001-4ca506c/report.md)
- [Adaptive fast-map diagnostic](results/adam-memory-density-19b47cb-fastmap/report.md)

## Limits and next profiles

This first profile isolates string key/value density. It does not claim
coverage of Redis hashes, lists, sets, sorted sets, or streams, whose compact
encodings and per-type metadata differ. It also does not enable object/KV
overflow, because moving values to another tier changes the storage path and
latency.

The repeating value pattern is a baseline for representation overhead, not a
compression claim. An online compaction/compression experiment should add both
high-entropy and compressible values, a changing hot/cold access distribution,
and samples while optimization runs. Its acceptance gate should compare p99 and
throughput at the same offered load, with background work bounded so storage
maintenance cannot consume the request path's full CPU budget.

## EDEN-2266 compact candidate (unqualified)

Build with `redis-server,experimental-compact-point-storage`. Selection is
automatic for plain string SETs with keys up to 64 bytes and values up to 256
bytes, including ordinary RESP/preload writes, while memory policy and object
overflow are disabled. Other keys and metadata use the general layout in the
same shard. The opt-in feature stores raw, uncompressed key/value bytes in
fixed 4 KiB boxed chunks, rounds record lengths to two-byte size classes, and uses
an ordinary descriptor hash table. Chunks never relocate their existing
payloads. Deleted records are reused; empty chunks release their payload
allocation and recycle the chunk descriptor. The allocator retains descriptor
capacity bounded by peak chunk count and caps compact payload allocations at
64 MiB per shard; exhausted compact allocation falls back to general storage.

The next allocator candidate keeps the 32-byte per-key descriptor and, on
64-bit targets, is expected to reduce each chunk descriptor from 56 to 24
bytes by replacing the growable byte vector with `Box<[u8; 4096]>` and vector indexes with
per-chunk availability links and a bounded `u16` class index. Regression checks
enforce the descriptor size limits. Fixed list heads use 640 inline bytes per
shard instead of the earlier 960 bytes of vector headers, saving 320 bytes and
avoiding their separate backing allocations. Availability insertion, removal,
and lookup remain constant-time. The finer classes retain the
two-byte minimum needed for a deleted record's free-list link. They increase
the number of classes from 40 to 160.
For 18-byte keys, 16-byte values now use 34-byte records (120 per chunk) instead
of 40-byte records (102 per chunk); 64-byte values use 82-byte records (49 per
chunk) instead of 88-byte records (46 per chunk). At 100,000 keys in one shard,
these layouts need 147 and 133 fewer payload chunks respectively, saving
602,112 and 544,768 payload bytes before descriptor and metadata changes.
The 256-byte profile still fits 14 records per chunk, so its payload chunk
count does not improve. More distinct record lengths can leave more partly
filled chunks; these source-level byte counts are not measured PSS results.

Fresh Adam diagnostics must compare this candidate with the earlier compact
layout at the same key count, all five value sizes, and both compressible and
high-entropy patterns. Acceptance requires improved incremental PSS for the
16/64-byte profiles, no worsening at 256/1024/4096 bytes, and throughput at
least 95% with p99 at most 105% of the earlier compact candidate under matched
settings. Total loaded PSS at 16 bytes must also be compared with Redis, with
the target of using no more memory. These checks remain pending until evidence
is collected; an unreserved run cannot qualify matched offered-load performance.

Small RESP GETs encode a borrowed chunk slice under the storage borrow, including
queued responses below the 2048-byte response ownership threshold. A shared/owned
`Bytes` API materializes one independent owner on the first read of each value
version and reuses it for later reads. This preserves owned-clone semantics,
but a mixed shared-owner workload can add allocation cost and duplicate payload
memory; qualify that path separately before enabling the feature in a deployment
that uses it.

Equal-length SET reuses its record when there are no read epochs. Length changes
allocate one replacement record; deletes release or retire one record. A mutation
during an epoch retains only its old record and any materialized shared owner,
then uses a new record. The allocator reclaims at most 32 retired records per
call after readers leave (64 across a compact allocation attempt and general
fallback; maintenance reclaims 32). Each compact write copies at most 64
key bytes and 256 value bytes and initializes at most one 4 KiB chunk. Normal
hash-table descriptor resizing does not copy payloads. TTL/governance/semantic
metadata and overflow generation migrate only the touched key, preserving
one representation per key and keeping untouched keys compact. Runtime policy
configuration retains both layouts and accounts for them in eviction selection;
sampled access metadata for compact keys lives in a sparse side map. Values
of 1 KiB and 4 KiB use the existing general layout from the first SET.

The feature remains unqualified until correctness, bounded allocation/reclaim,
churn, shared/borrowed epoch lifetime, mixed storage, and protocol regressions
pass on Adam and the unchanged density/performance gates below are met.

Run both the baseline (`redis-server`) and candidate at the exact same source SHA
on Adam, with the same reserved resources. For each build:

```bash
python3 benchmarks/scripts/run-memory-density-benchmark.py \
  --targets redis,shardcache-resp --keys 100000 \
  --value-sizes 16,64,256,1024,4096 \
  --value-patterns repeating,compressible,high-entropy \
  --distributions 'uniform;hot:1000:90' \
  --repeats 3 --vcpus 1 --clients 16 --pipeline 1 \
  --duration 10 --warmup 2 \
  --shardcache-features redis-server,experimental-compact-point-storage
```

Qualification is pending. At each matched pattern/distribution/size, candidate
incremental PSS must be at most Redis's for 16, 64, and 256 bytes, with cgroup
ANON corroboration. Candidate throughput must be at least 95% of the current
ShardCache baseline and p99 at most 105%. Retain the PSS advantage over Redis at
1 KiB and 4 KiB. The runner reports closed-loop saturation; use the existing
open-loop `curve` driver for any additional matched offered-load qualification.
An unreserved diagnostic remains diagnostic even if all numeric gates pass.
