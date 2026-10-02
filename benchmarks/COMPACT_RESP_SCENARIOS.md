# Finite compact-storage RESP diagnostics

This benchmark is a diagnostic on a shared, unreserved host. It establishes no
reserved capacity, matched offered-load performance qualification, or extra data
actually inserted at a fixed RAM limit. Results are written only after execution.
The original `run-memory-density-benchmark.py` remains unchanged.

## Dataset and timing contracts

`compact_resp_scenarios` uses only TCP/RESP2 and the benchmark crate's existing
default dependencies. Build one fresh common executable and one fresh common
`saturation` executable from the final composed source; use those exact ELF bytes
for baseline compact-on78, candidate compact-on, and actual Redis7.4.11. Each
server image retains its original source/tree, features, complete raw metadata
and export log, exact Dockerfile, typed exported manifest/config/Engine identity,
and both resolved FROM materials. Reuse requires independent source-context
acceptance. A new driver SHA never relabels the server image's original source.
Materials completeness remains whatever the retained metadata actually reports.

The24 stateful cases are finite, partitioned in catalog order into three packages
of8. Each package has72 rows: three rounds, three arms, eight cases. Arm order is
B/C/R, C/R/B, R/B/C. The eight access profiles add72 rows. The existing ten core
shapes add90 rows, totaling378 final-state rows and at least3780 five-idle plus
five-final verified PID1 sample gates. Intermediate phase gates are additional;
peak observations are not settled PID1 gates. Every row uses a fresh server.

| Cases | Exact shape or trace |
|---|---|
|8 boundaries|100000 unique keys: lengths63/64/65 at value16; key18 at values0/15/17/255/257|
|2 cardinalities|1000 and1000000 keys, key18/value16, high entropy|
|2 mixed|100000 keys, key lengths8/18/63/64 modulo4; value proportions30%16,30%64,20%256,10%1024,10%4096; both patterns|
|2 all-class|All160 two-byte classes; first9 classes have one unique short key each (including empty and one-byte), remaining99991 keys cycle151 feasible classes; both patterns|
|2 overwrite|Load value64, then10 equal-length overwrite generations; both patterns|
|2 resize|Load16 then64/255/257/16, three cycles; both patterns|
|2 half|Delete even-indexed50%, then reinsert, three cycles; both patterns|
|2 groups|Delete indices modulo4 other than3, retain25% sentinels, reload64/255/17 across three cycles; both patterns|
|1 metadata|Apply7200000ms TTL to10%, PERSIST, delete, reinsert with a changed value|
|1 types|After deleting10%, replace half of that cohort with one-field hashes and half with one-element lists; require WRONGTYPE for GET, exact type/contents/no TTL, delete, reinsert strings|

Fixed keys are binary, with a collision-free eight-byte big-endian index suffix.
Class fixtures declare their exact length/count/logical-byte distribution in the
ready event. They do not claim100000 unique one-byte keys. Group deletion is a
network trace; it cannot prove which internal allocator chunks were released.
Exact payload cap, chunk reuse, owner lifetime, alias and epoch invariants remain
separate product regressions.

Values use the original SplitMix64 seed or repeated compressible bytes. Churn
changes the generation deterministically. Each arm executes the same fixed
logical transitions, command mix/count, expected state and canonical trace hash.
Sixteen clients own contiguous index ranges, pipeline1. Actual concurrent wire
interleaving may differ; the digest combines the16 ordered per-worker command
traces and does not claim to hash a globally deterministic wire ordering.

Mutation timing covers native request construction, complete responses and
required reply checks. It ends before exhaustive validation and sampling.
Report **completed logical record transitions/s** and **timed wire commands/s**
separately. Native p50/p99/p99.9 measure one completed record transition, which
may contain two commands for a string-to-collection replacement. Full state
verification has a separately labeled interval and percentile unit; its GET,
PTTL, TYPE and collection queries are not mutation performance operations.
DBSIZE and Python memory sampling are outside the measured mutation interval.
No Python elapsed interval is a server throughput result.

After every phase, validate every command reply, exhaustively GET every expected
key or absence, check PTTL, typed WRONGTYPE/TYPE/contents, and DBSIZE. The rolling
verified state digest covers the key, kind, TTL state and exact payload. The
controller independently generates expected state, command and operation-count
hashes in Python. Hashing is streaming; per-operation logs are not retained.
TTL is two hours and the whole server row is bounded to15 minutes; an expiring
key must remain within the declared positive TTL range. Accidental expiry fails.

Access profiles use values16/high-entropy and64/compressible, each with GET-only,
SET-only,80/20 hot1000:90 and80/20 uniform pipeline16. Existing saturation uses
16 clients,20 seconds measured and3 seconds warmup. Timed successful operation
counts can differ across arms because this is a time-bounded throughput test;
the streams/seed and final logical dataset remain identical. Its original CSV
operation and latency units are retained. A separate verify-only native selector
exhaustively validates this resulting keyspace. Ten finite `core-vSIZE-PATTERN`
verify-only selectors also support the existing core shapes without editing the
original core script; the operating wrapper must bind any appended verification.

## Controller and admission

`run-compact-storage-scenarios.py --plan PATH --plan-sha256 SHA` admits only
`stateful-a`, `stateful-b`, `stateful-c`, or `access`, each with exactly72 members.
An independently frozen operating plan must supply final source/tree/file hashes,
common native build/compiler/argv/artifact and genuine numeric exit evidence,
accepted early API30-process gate, accepted focused native/Python regressions,
accepted operating resource/ownership/watchdog/disk closure, and pinned image
provenance. Source reuse requires an explicit independent equivalence judgment;
old receipts keep their original source identities.

The controller imports the unchanged density build-provenance and memory helpers.
Docker runs through an exact hash-pinned reviewed MD05 adapter. Every memory sample
must append exactly one fresh verified fullID/PID1/start/cgroup/owner receipt;
missing or failed adapter proof fails the row. Five snapshots occur at idle and
at every verified phase. Their medians are retained. A0.2s observer records
current/anon sampled maxima during mutation and verification, with exact PID
start/cgroup/leaf-cap checks; it does not claim an exact allocation peak.

Child lifecycle uses the separately reviewed H1 `NativeChildWait`,
`block_termination`, `defer_cleanup_termination` and `stop_child` implementation.
The old3eef cleanup is inadmissible. One waiter owns each native or helper child;
terminal status is genuine and cached, and lost wait ownership authorizes no
signal. No global wait, whole-host lock, global Docker cleanup, or arbitrary
workload argv is used. Failures keep the primary error and separate cleanup
receipts. A startup without a returned full container ID remains a failure and
requires recovery/absence proof for its exact pre-recorded owned name. External
package supervision must prove final process/container/scope absence before
accepting a complete lifecycle.

Each package keeps140 minutes complete lifecycle,45 minutes build,15 minutes per
server runtime and5 minutes cleanup. The controller is limited to90 minutes,
leaving build and cleanup within140; it never extends a prior watchdog. A5s
watchdog, builder16GiB/4CPU/zero swap/jobs4, controller/client2GiB/4CPU/zero swap,
server4GiB/one CPU/zero swap,128MiB per file,1GiB output,30GiB sampled owned-disk
stop threshold and fresh host/ownership gates remain mandatory. The operating
adapter's disk closure must include image/build storage; tracked controller
accounting alone covers output/native target, not all Docker storage. Timeouts
fail the package; no case or row is silently removed.

## Prespecified screens and regression selection

Correctness, source/material/artifact identity, genuine zero exit, resource caps
and complete cleanup are mandatory. For new matched final states, incremental
PSS candidate/baseline must be<=1. Every phase's total PSS is separately screened
and all sampled peaks are reported. Native matched transition throughput must
be>=95% and p99<=105% of baseline; access uses the same diagnostic95/105 screens.
The original core retains eligible16/64/256 improvement, strict1024/4096
no-increase and total16<=Redis criteria. Every miss remains visible, with cause
and significance unknown. Three prior MD05 larger-value misses remain historical
and unexplained; no posthoc tolerance or noise explanation is introduced.

Report actual logical records/bytes/state, total PSS bytes/live key and incremental
PSS bytes/live key, medians and ranges, and explicit candidate/Redis memory ratios
(<1 means less memory at the same dataset). A reciprocal memory ratio is only an
estimated same-RAM density under stated assumptions, never extra data inserted.
Use ratios of three-run medians; median per-run p99 is not a pooled percentile.
Three observations supply no confidence interval or release qualification.

Required focused selections, only on Adam after independent exact-source review:

```
cargo test --locked --jobs 4 -p shardcache-benchmarks --bin compact_resp_scenarios
PYTHONDONTWRITEBYTECODE=1 python3 benchmarks/scripts/test-compact-storage-scenarios.py -v
cargo build --locked --release --jobs 4 -p shardcache-benchmarks --bin compact_resp_scenarios --bin saturation
```

The prospective source counts are21 Rust methods and37 Python methods with
subcases. These are unexecuted expectations, not pass evidence. Product/default/
feature-off validation, H1 ownership regressions, native build receipts and the
early shared-API acceptance gate remain distinct prerequisites.
