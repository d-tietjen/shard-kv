# Shardmap Grouped Telemetry Counter Benchmark

Measured on June 30, 2026.

This benchmark compares the first `fast-telemetry` grouped-counter integration
against the follow-up change that made grouped counters authoritative on the
hot path.

| Variant | Commit | Description |
| --- | --- | --- |
| Mirrored grouped counters | `014af9b` | Wrote aggregate updates into a `CounterSet`, but still mirrored those updates into the old individual `Counter` fields. |
| Authoritative grouped counters | `a9a906c` | Stores aggregate counters only in one `CounterSet`; snapshots and exporters read from that grouped set. |

## Why This Was Measured

The goal was to verify that the API/implementation change actually captured the
performance benefit of grouped counters. The important risk was that using
`CounterSet` while still updating the legacy `Counter` fields would preserve
correctness but keep most of the hot-path cost.

The benchmark therefore focuses on direct telemetry update calls, because those
are the paths changed by `a9a906c`.

## Methodology

A temporary release-mode microbenchmark crate was created under `/private/tmp`.
It depended on the current branch and a temporary detached worktree at the
mirrored grouped-counter commit. When both worktrees used the same package
version, the old worktree version metadata was given a `+mirrored` suffix so
Cargo could lock both path dependencies in one binary.

The benchmark alternated old/current runs for each repetition, used
`CacheTelemetry::new_with_latency_sample_rate(shards, u64::MAX)`, and passed
`None` for latency values so the measurement isolated counter update cost rather
than latency histogram recording.

Cases measured:

| Case | Operation |
| --- | --- |
| `record_set` | `record_set(shard_id, 64, None)` |
| `record_get_hit` | `record_get(shard_id, true, 64, None)` |
| `record_get_miss` | `record_get(shard_id, false, 0, None)` |
| `record_delete` | `record_delete(shard_id)` |
| `record_batch_get` | `record_batch_get(1)` |
| `record_wal_append` | `record_wal_append(64)` |
| `record_expiration` | `record_expiration(1)` |

Commands used for the final direct-cost runs:

```bash
cargo run --release --offline \
  --manifest-path /private/tmp/shardmap-telemetry-bench/Cargo.toml \
  -- --threads 1 --iters 50000000 --reps 5 --cases all

cargo run --release --offline \
  --manifest-path /private/tmp/shardmap-telemetry-bench/Cargo.toml \
  -- --threads 8 --iters 5000000 --reps 5 --cases all
```

## Direct Counter Cost

Median nanoseconds per operation, 1 thread:

| Path | Mirrored ns/op | Current ns/op | Delta |
| --- | ---: | ---: | ---: |
| `record_set` | 7.75 | 2.91 | -62.4% |
| `record_get_hit` | 7.03 | 3.29 | -53.2% |
| `record_get_miss` | 4.99 | 2.79 | -44.1% |
| `record_delete` | 3.87 | 2.69 | -30.5% |
| `record_batch_get` | 6.56 | 5.61 | -14.4% |
| `record_wal_append` | 4.51 | 2.11 | -53.1% |
| `record_expiration` | 2.16 | 2.03 | -5.9% |

Median aggregate wall-clock nanoseconds per operation, 8 threads:

| Path | Mirrored ns/op | Current ns/op | Delta |
| --- | ---: | ---: | ---: |
| `record_set` | 0.62 | 0.38 | -39.1% |
| `record_get_hit` | 0.88 | 0.44 | -49.5% |
| `record_get_miss` | 0.65 | 0.41 | -36.7% |
| `record_delete` | 0.44 | 0.34 | -24.1% |
| `record_batch_get` | 0.94 | 0.64 | -32.2% |
| `record_wal_append` | 0.52 | 0.29 | -44.4% |
| `record_expiration` | 0.31 | 0.31 | -2.1% |

## End-To-End Sanity Check

The existing `saturation` harness was also run against `fc-embed-telemetry` with
a SET-only workload. This is a broader workload sanity check, not the primary
proof point, because the full embedded write path is dominated by store write
behavior, value movement, key routing, and scheduler noise.

Command shape:

```bash
target/release/saturation \
  --backends fc-embed-telemetry \
  --vcpu-budget 1 \
  --clients 1 \
  --duration 5 \
  --warmup 1 \
  --value-size 64 \
  --mix set \
  --key-count 100000 \
  --latency-sample-rate 0
```

Observed rows:

| Variant | Run | Ops/sec | Logical GB/s | vCPU |
| --- | ---: | ---: | ---: | ---: |
| Current `a9a906c` | 1 | 3,134,777 | 0.201 | 1.169 |
| Current `a9a906c` | 2 | 4,063,766 | 0.260 | 1.265 |
| Current `a9a906c` | 3 | 3,887,036 | 0.249 | 1.184 |
| Mirrored `014af9b` | 1 | 3,887,875 | 0.249 | 1.254 |
| Mirrored `014af9b` | 2 | 4,314,608 | 0.276 | 1.283 |

The end-to-end result was too noisy to claim a throughput improvement. It does
not contradict the direct counter-cost result; it means this particular
embedded SET workload is not sensitive enough to isolate a few nanoseconds of
telemetry update cost.

## Interpretation

The direct telemetry benchmark confirms that `a9a906c` captures the intended
grouped-counter optimization. The largest wins are on operations that formerly
updated multiple aggregate counters:

| Operation shape | Result |
| --- | --- |
| SET, GET hit/miss, WAL append | Large improvement, roughly 36-62% lower direct counter-update cost depending on thread count and case. |
| Delete and batch-get | Moderate improvement. |
| Expiration | Essentially flat to slightly better, because it only removed one mirrored aggregate counter update. |

The important conclusion is that the authoritative grouped-counter change does
remove duplicate hot-path counter work. The remaining `fc-embed-telemetry`
end-to-end workload should not be used as the main signal for this change unless
we add a more controlled benchmark mode that isolates telemetry update overhead
inside the standard benchmark crate.

## Follow-Up

If we want this to be continuously repeatable, add a committed benchmark binary
such as `benchmarks/src/bin/telemetry_counter_cost.rs`. It should keep the same
case set and report old/current only when pointed at two worktrees, or at least
track current absolute cost over time.
