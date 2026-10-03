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

## EDEN-2266 compact candidate (experimental, unqualified)

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

Candidate `78c83ad` keeps the 32-byte per-key descriptor. Its chunk descriptor
measures 24 bytes on the tested 64-bit target; the earlier layout was 56 bytes
by source-level layout calculation. It replaces the growable byte vector with
`Box<[u8; 4096]>` and vector indexes with per-chunk availability links and a
bounded `u16` class index. Adam layout checks enforce the descriptor size limits.
Fixed list heads use 640 inline bytes per shard instead of the earlier 960 bytes of vector headers, saving 320 bytes and
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

The [2026-10-02 MD05 diagnostic](#2026-10-02-md05-three-round-diagnostic)
compares this candidate with the earlier compact layout at identical key counts,
all five value sizes, and both compressible and high-entropy patterns. The
screen requires improved incremental PSS at 16/64 bytes, no increase at
256/1024/4096 bytes, throughput at least 95%, and p99 at most 105% of that
baseline. Total loaded PSS at 16 bytes must be no greater than Redis's.
The small-value and performance screens passed; three larger-shape PSS misses
remain. The shared-host diagnostic does not establish formal performance
qualification.

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
then uses a new record. Compact-record reclamation is capped at 32 retired
records per reclaim invocation after readers leave, or 64 across a compact
allocation attempt and general fallback; compact maintenance reclaims 32. Each
compact write copies at most 64 key bytes and 256 value bytes and initializes at most one 4 KiB chunk. Normal
hash-table descriptor resizing does not copy payloads. TTL/governance/semantic
metadata and overflow generation migrate only the touched key, preserving
one representation per key and keeping untouched keys compact. Runtime policy
configuration retains both layouts and accounts for them in eviction selection;
sampled access metadata for compact keys lives in a sparse side map. Values
of 1 KiB and 4 KiB use the existing general layout from the first SET.

Affected correctness, layout, feature-forwarding/minimal checks and fresh native
builds are independently accepted on Adam at source `78c83ad`. Older default
and off-feature evidence retains its original source provenance. The original
three-alias check's numeric outer-wrapper exit remains unverified; the fresh
combined suite closes alias correctness through a separate audit of all three
names and a genuine zero wrapper exit. These results do not remove the opt-in,
experimental status or satisfy the remaining density and qualification goals.

## 2026-10-02 MD05 three-round diagnostic

MD05 compares the previous compact implementation at
`98e2cc41e9b1bc79398bf03f13f2b7621c1b1d08` (B), the smaller-chunk-descriptor
candidate at `78c83addff5f140236f9a659ff1e332bb9c35522` (C), and the actual
cached Redis **7.4.11** image (R). Both ShardCache arms enable
`redis-server,experimental-compact-point-storage`; B is the prior compact
layout. Feature-off qualification is a separate comparison below.

### Methodology and reproduction inputs

The run contains three observations per arm and shape: three rotated blocks of
ten shapes per round, **90 fresh-server rows** in total. Round orders are
B/C/R, C/R/B, and R/B/C. Each profile uses 100,000 identical 18-byte keys,
16/64/256/1024/4096-byte values, and compressible repeated `x` or deterministic
high-entropy SplitMix64 payloads. All rows retain exactly 100,000 loaded keys,
three exact sample-key GET checks, zero benchmark errors, and five idle plus
five loaded PID1 memory samples (**900 verified sample gates**). Memory is
sampled after a one-second settling interval, with 0.2 seconds between samples.

Each server receives one CPU, 4 GiB memory and no swap. The client uses
16 connections, pipeline 1, uniform 80/20 GET/SET, 20-second measurements and
3-second warmups. The controller/client scope has a 4-CPU/2-GiB cap. CPU affinity is
not pinned and resources are unreserved on the shared Linux host. The recorded
kernel is 6.8.0-139-generic, x86_64. A matching-boot **post-run** inventory at
08:31:13 UTC reports an AMD Ryzen 9 3950X, 16 cores/32 logical CPUs, and
131,806,888 KiB MemTotal; this inventory does not establish available capacity
during the benchmark.

All arms use the **same freshly built candidate78 `saturation` driver**. Its
native build command was:

```bash
cargo build --locked --release --jobs 4 \
  -p shardcache-benchmarks --bin saturation --bin curve
```

The reviewed controller builds each ShardCache server image from its bound
source revision before profiling. The per-profile entry imports that source's
benchmark module and binds `SATURATION` to the common candidate78 executable,
including for B and R. Reproduction must retain that common-driver binding and
the rotated order. The nominal runner argv below records the parameters; the
source SHA, target, tag and output directory change for each arm/profile.
`--skip-build` uses the previously built source-bound server image.

```bash
python3 benchmarks/scripts/run-memory-density-benchmark.py \
  --candidate-sha "$SOURCE_SHA" --targets "$TARGET" \
  --keys 100000 --value-sizes 16,64,256,1024,4096 \
  --value-patterns compressible,high-entropy --distribution uniform \
  --repeats 1 --vcpus 1 --memory-limit 4g --build-jobs 4 \
  --clients 16 --pipeline 1 --mix 80-20 --duration 20 --warmup 3 \
  --samples 5 --sample-delay 0.2 --settle-seconds 1 --skip-build \
  --shardcache-features redis-server,experimental-compact-point-storage \
  --candidate-tag "$PROFILE_TAG" --out-dir "$PROFILE_OUTPUT"
```

Use `shardcache-resp` for B/C and `redis` for R. The tracked
[curated summary](reference/compact-storage-md05-20261002/summary.json) retains
both source SHAs/trees, all eight metric medians, profile/round identities,
exact native hashes, and the source plan/raw-result/comparison hashes. It also
binds each profile CSV and manifest. The fresh driver SHA-256 is
`b3105a655b24a8af0688fe8fea1cd652a1b8c491c09bfbf26d2c6af6fe50d1b7`.
These receipts describe source78 execution; a later documentation commit does
not constitute a new native build or benchmark run.

### Measured comparison

Incremental PSS is **7.77–8.74% lower at 16 bytes** and **4.54–5.14% lower at
64 bytes** than B. At 16 bytes, total loaded PSS is **2.35–2.93% lower than
Redis**; C's total loaded PSS is below Redis at all ten measured shapes.
All twenty throughput/p99 screens pass: throughput is **99.11–100.26%** of B
and p99 is **98.83–101.10%**. The screening thresholds are throughput at least
95% and p99 at most 105% of B.

Every table entry is a **ratio of three-run medians**. A memory ratio below
1 means less memory; a throughput ratio below 1 means fewer operations per
second; a p99 ratio below 1 means lower latency. The p99 summary is a median
of per-run p99 values, not a pooled request percentile.

| Pattern | Value bytes | Incremental PSS C/B | Incremental PSS C/R | Total loaded PSS C/R | Throughput C/B | p99 C/B |
|---|---:|---:|---:|---:|---:|---:|
| compressible | 16 | 0.922305 | 0.803129 | 0.970725 | 0.999197 | 0.988287 |
| compressible | 64 | 0.954634 | 0.831211 | 0.949101 | 0.997427 | 1.000000 |
| compressible | 256 | 0.993467 | 0.857275 | 0.907919 | 0.991095 | 1.010965 |
| compressible | 1024 | 1.000143 | 0.940425 | 0.954564 | 0.999390 | 1.001416 |
| compressible | 4096 | 0.999850 | 0.831892 | 0.836385 | 0.997451 | 1.007458 |
| high-entropy | 16 | 0.912621 | 0.801950 | 0.976544 | 1.002446 | 0.991965 |
| high-entropy | 64 | 0.948619 | 0.829826 | 0.950715 | 0.999634 | 0.995633 |
| high-entropy | 256 | 0.988669 | 0.854046 | 0.909359 | 1.002627 | 0.989091 |
| high-entropy | 1024 | 1.000303 | 0.940458 | 0.955278 | 0.991596 | 1.006429 |
| high-entropy | 4096 | 1.000221 | 0.832126 | 0.836648 | 0.997163 | 1.006868 |

The strict requirement of no incremental-PSS increase at 256/1024/4096 bytes
is **not fully satisfied**. All three misses are retained:

| Pattern | Value bytes | Baseline median bytes | Candidate median bytes | Increase bytes | Increase percent |
|---|---:|---:|---:|---:|---:|
| compressible | 1024 | 128,602,112 | 128,620,544 | 18,432 (18 KiB) | 0.014333% |
| high-entropy | 1024 | 128,575,488 | 128,614,400 | 38,912 (38 KiB) | 0.030264% |
| high-entropy | 4096 | 435,976,192 | 436,072,448 | 96,256 (94 KiB) | 0.022078% |

The cause and statistical significance of these differences are unknown.
Cgroup anonymous-memory medians are equal at both 1024-byte shapes, while C's
high-entropy 4096-byte median is 8,192 bytes lower; these observations do not
explain the PSS increases.

Raw rows, individual observations, ranges and the comparison JSON/CSV/TXT are
retained in the ignored local archive
`benchmarks/results/EDEN-2266-delivery/optimization-78c83ad/md05-three-round-comparison-20261002T0807Z/`.
Independent raw-row/sample, numerical and final lifecycle review is accepted,
including genuine zero controller-wrapper status and exact owned cleanup. The
510-entry final archive manifest SHA-256 is
`8ebb5d8a23e4754c0a619ae1104fa157594ead355bce4e50b2e0b5874ecfd544`.

Three observations do not establish a confidence interval. Rotation retains
host/time effects; unreserved closed-loop saturation does not qualify matched
offered-load performance. Fixed-length profiles and settled samples do not
qualify mixed-length fragmentation, shared-owner API costs, startup/allocation
peaks, or mutation-heavy online maintenance. Those paths and formal performance
qualification require separate evidence.

## 2026-10-02 follow-up candidate, correctness and measurement status

The follow-up product at `23bf7cc0f955d1307634d500738b834f7076460b`
passed 28 focused storage/layout regressions on Adam. Its measured per-key
descriptor is 24 bytes, versus 32 bytes in MD05 source `78c83ad`: **25% fewer
descriptor bytes only**. No process-memory or performance improvement has been
measured for this follow-up. It remains opt-in and experimental.

At source `23bf7cc`, the targeted borrowed RESP SET check passed one test and
the default suite passed 182 tests. Nine ignored documentation examples remain
excluded from coverage. Minimal embedded/no-default and feature-forwarding
compile checks also passed. The Redis feature suites subsequently passed
306 tests with compact storage disabled, 338 with compact storage enabled,
and 442 with the combined features, including the required comparisons against
qualified Redis 7.4.11. Ignored tests remain excluded from coverage. Independent
review accepted the complete execution and owned cleanup evidence.

The 306/338/442 suites ran at `23bf7cc`, with a 16 GiB memory limit and CPU and
swap unlimited. A limited comparison of the inputs for the selected commands
permits their applicability to validation source
`444513e1d4bb342eab6a6ab6341e0c0609cb0bc1`. Their receipts retain the actual
`23bf7cc` identity; no fresh current-head ordinary build or test is claimed.

Fresh API validation at `444513e` passed all **46 cases**: two collector
regressions, eight binding checks and 36 source cases. Baseline `93b81ce` and
candidate `444513e` each passed eight shared-read API unit tests and built
separate source-bound measurement executables. All four native jobs have
independently accepted source, build, resource and cleanup evidence.

The first shared-read campaign failed before any cohort; its failure evidence
and owned cleanup were accepted. Subsequent caller attempts remain preserved.
Revision six failed before completing the seven planned methods. Its complete
218-file failed archive and exact cleanup were independently accepted, with all
179 raw files and temporary inputs retained.

The revision-seven caller regression suite passed all seven methods with zero
failures, errors or skips in 43.462 seconds. Independent review accepted all
thirteen compositions and three resource cases, the actual source and limits,
genuine child exit of 0 and observed wrapper exit of 0. Its complete 502-file
evidence archive and exact owned cleanup were accepted, preserving all 428 raw
files. The fixture's direct kernel capture missed the exited process and
receives no credit.

The subsequent revision-seven campaign failed after 278.238 seconds, retaining
four completed baseline cohorts (`r1-b-1` through `r1-b-4`). No candidate
comparison, campaign summary or screen is accepted. The caller rejected a
legitimate read-only status query issued by the verified controller's child.
The original error was retained before cleanup; genuine child and independently
observed kernel outer exits were both 1. All 266 copied raw files were verified.
Exact owned-session cleanup and the complete 421-entry failed-archive manifest
were independently accepted.
No follow-up process-memory or performance result is accepted.

The revision-eight caller fixture retest ran seven methods and failed after
37.532 seconds, with one failure and zero errors or skips. Genuine child and
independently observed kernel outer exits were both 1. Review confirmed two
fixture defects: comparing process fields that change while running, and
repeating cleanup after a record had been removed. The allowed metadata query
was observed and unauthorized auxiliary actions were refused; no production
caller defect was confirmed. All 409 copied raw files were verified. Eleven
composition directories were retained, but later planned probes were not
reached. Exact owned-session cleanup and the complete 553-file failed archive
(552 manifest entries) were independently accepted, preserving raw files and
temporary inputs. The revision-nine fixture repair passed independent static
review; a fresh seven-method retest remains pending.

The planned [early shared/owned-read gate](COMPACT_SHARED_READ_GATE.md) has
30 process cohorts covering cold/concurrent initialization, warm reads, sparse
memory, drop and reclamation costs. The planned
[expanded Redis comparison](COMPACT_RESP_SCENARIOS.md) retains the ten MD05
shapes and adds mixed lengths, overwrite/delete-reinsert, scale/eligibility and
read/write hot/cold profiles: **378 final rows and 7,605 memory sample gates**.
A shared-read campaign retry and the expanded Redis benchmark execution remain
pending. These validations and plans establish no new measured result or formal
qualification; the existing MD05 findings and diagnostic limits still apply.

## Remaining qualification

Run both the baseline (`redis-server`) and candidate at the exact same source SHA
on Adam, with the same reserved resources. The example enables the compact
candidate; use `--shardcache-features redis-server` for the feature-off baseline.
This comparison has different baseline semantics from MD05:

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
