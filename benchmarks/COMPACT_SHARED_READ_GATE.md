# Compact shared-read diagnostic gate

This early gate compares the public `FlatMap` owned-read API in two source-bound
native executables. It is separate from the Redis RESP density comparison. It
has no results yet and does not qualify capacity or reserve the host.

## Inputs and build

Use the same frozen benchmark source in both executables, overlaid separately
on the accepted baseline product and the reviewed candidate product. Keep both
build SHA/tree identities, product-input equivalence witnesses, compiler/PATH
receipts, genuine build-exit evidence, ELF hashes, and independent acceptance.
Never use a candidate executable to represent the baseline implementation.
The initial baseline product is `78c83addff5f140236f9a659ff1e332bb9c35522`.
A documentation or harness overlay creates a new build revision; earlier
product results retain their original revision and are not fresh API evidence.

Build and test each frozen source on Adam:

```bash
cargo build --locked --release --jobs 4 -p shardcache-benchmarks \
  --features compact-point-storage --bin compact_shared_read_cost
cargo test --locked --release --jobs 4 -p shardcache-benchmarks \
  --features compact-point-storage --bin compact_shared_read_cost
/usr/bin/python3 benchmarks/scripts/test-compact-shared-read-gate.py
```

The benchmark feature enables experimental compact storage. No dependency or
public storage API is added. Native driver tests and the controller's offline
fixtures require fresh Adam execution; old MD05 regressions do not cover this
new protocol. Inert fixture ELF/receipt bytes are never build evidence.

## Finite campaign

Three rounds rotate source order baseline/candidate, candidate/baseline,
baseline/candidate. Each source runs the five cohorts below in this fixed order:

| Cohort | Maps live at once | Records per map | Key/value bytes | Initial owned selection | Readers | CPU cap |
| --- | ---: | ---: | ---: | --- | ---: | ---: |
| empty-value-sparse | 64 | 1 | 2/0 | all | 1 | 1 |
| small-value-sparse | 64 | 1 | 18/16 | all | 1 | 1 |
| small-value-dense | 64 | 120 | 18/16 | first record | 1 | 1 |
| point-100k-sparse-owned | 1 | 100,000 | 18/16 | every 120th record: 834 | 1 | 1 |
| eight-reader-first-owned | 64 | 1 | 18/256 | all | 8 | 4 |

This is 24 single-reader processes plus six contended processes: **30 total**.
Every process has four fresh sequential batches. At most the listed maps are
live in a batch; allocator state can carry across batches. The 2-byte keys are
unique within each map, with map identity included in the dataset digest.
The 18-byte keys are `k:` followed by sixteen hexadecimal digits. Values use
fixed bytes, not a claim about entropy or compression. No cohort asserts a
private chunk size, directory count, or that adjacent keys share a chunk.

Each batch emits these 13 acknowledged checkpoints:

1. Empty maps.
2. Loaded maps, with exhaustive borrowed reads and logical byte checks.
3. First owned reads; no owned warmup precedes this phase.
4. First owned read of the next occupied key, where applicable.
5. Five seconds of single-thread reads of the same stable owner.
6. Owners materialized for every live record.
7. All records deleted; retain all-delete and separate last-live-delete timings.
8. Same keys reinserted with 64-byte values; borrowed correctness checked.
9. Begin read epochs, retain selected old clones, and replace values with 17 bytes.
10. End epochs and perform a bounded maintenance schedule; old clones remain valid.
11. Release retained old clones.
12. Materialize owners for all current records.
13. Drop the nonempty maps and record individual destruction times.

The maintenance schedule is `ceil(records_per_map/32)+2` calls per map. The
checkpoint name describes this schedule, not a measured private reclaim count.
Every occupied state is checked through all keys, `len`, and `stored_bytes`.
`stored_bytes` includes key and value bytes. SHA-256 binds map index, key length
and bytes, and value length and bytes; empty states have the fixed header digest.

The eight-reader phase constructs threads before a Barrier and times each
getter after it. It checks one winning owner identity and cloned content.
Clone checking is outside getter timing; clones are released before the memory
snapshot. Its warm phase remains single-threaded. A Barrier does not prove
concurrent initializer calls. Both sources use `OnceLock::get_or_init`; this
diagnostic checks public owner identity and content without asserting initializer
or discarded-initializer counts. The driver uses safe public APIs and has no
access to private allocation counters.

## Timing, memory, and receipts

Getter timings include hash lookup and content validation. Histograms retain
three significant digits, p50/p99/p99.9, and a bounded prefix of 256 raw samples.
An absent next key has zero operations and null percentiles, never a zero tail.
Four batch medians form each process observation. Comparisons use the ratio of
medians of three process-round observations; these are not pooled percentiles.

At empty, borrowed-loaded, cold-first, deleted, and maps-dropped checkpoints,
the observer settles for one second and collects five samples 200 ms apart.
Other checkpoints collect one sample. This yields 33 samples per batch,
132 per process and **3,960 planned process/cgroup gates**. Each sample binds
host PID/start ticks, executable device/inode/hash, UID, boot identity, exact
cgroup membership, and all ancestor ceilings. These are host native processes,
not container PID1 observations. Retain native PSS/RSS/private bytes separately
from cgroup anonymous/current memory: the latter includes the Python observer.

The observer and native child share a 4-GiB, zero-swap scope with the cohort's
1-CPU or 4-CPU quota. The separate controller has 4 CPUs and 2 GiB, zero swap.
There is no CPU affinity or reserved capacity. Actual child terminal status is
obtained by `os.waitpid` with the default SIGCHLD disposition; the raw status,
PID, reaping receipt and exact executable identity are retained. The outer
systemd wrapper result is distinct from the native child status.
Termination signals are blocked across each actual wait and its recorded status
transition. Cleanup first establishes an unreaped child and matches its recorded
fork PID, parent PID and start ticks before signalling its process or group.
An already observed terminal child receives no cleanup signal. ECHILD records
lost wait ownership with unknown raw status and authorizes no signal; it never
becomes an exit-zero receipt. Failure receipts retain the primary error, any
genuine terminal status, and separate cleanup errors and signals.

## Prespecified diagnostic screens

Candidate/baseline ratios must satisfy:

- Cold-first total PSS and settled deleted/maps-dropped PSS: at most 1.00.
- Applicable cold-first, cold-next, last-live-delete and map-drop p99: at most 1.05.
- Warm owned-read throughput: at least 0.95; warm p99: at most 1.05.

All cohort observations, batch summaries, and epoch/reuse states remain visible.
A screen miss returns to the design loop; no cold/drop waiver is inferred or
added after seeing data. Cause and statistical significance remain unknown.
Functional completion is recorded separately from screen success and external
lifecycle acceptance. This small shared-host experiment is not a Redis claim,
a matched-offered-load experiment, or formal release qualification.

## Operating bounds

The reviewed operating package pins this script, contract and two accepted
source/build chains before effects. Pending contracts cannot execute. Per-process
limits are 180 seconds, 60 seconds per phase, 128 MiB per file; the campaign
workload stops at 80 minutes and the complete lifecycle at 90 minutes, reserving
five minutes for cleanup. Fresh native builds are separate, each bounded by the
ordinary 45-minute builder contract (16 GiB, four CPUs, four Cargo jobs).
Expected campaign time is approximately 35–65 minutes after builds, not a
measurement. Do not extend deadlines to fit an overrun.

Output is limited to 1 GiB; sampled owned source-target/output usage stops at
30 GiB, with 100 GB free disk and 8 GiB available RAM floors. These disk checks
are sampled stop thresholds, not filesystem quotas. No Cargo or target-tree
writer may run concurrently; target trees are counted once per observer and
hardlinks count conservatively twice. STOP and termination signals stop later
cohorts. The task lock is acquired before effects or cleanup. Only scopes whose
absence and launch intent were retained may be terminated. Process, cgroup,
owned lock, task socket/session, temporary-directory, and operator cleanup
receipts must close before independent lifecycle acceptance; a printed success
or controller result alone does not provide that acceptance.
