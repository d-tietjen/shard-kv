# fast-cache Embedded Release Benchmarks

Standalone Linux benchmark report for the embedded fast-cache release
candidate.

## Artifacts

| Artifact | Path |
| --- | --- |
| Full embedded report CSV | `benchmarks/results/embedded_release_20260518_015047/embedded_report.csv` |
| Full embedded report log | `benchmarks/results/embedded_release_runs/embedded_release_report_20260518_015047.log` |
| Full report monitor log | `benchmarks/results/embedded_release_runs/embedded_release_report_monitor_20260518_015047.log` |
| Targeted LRU rerun CSV | `benchmarks/results/lru_buffer_reuse_20260518_0521/lru_first.csv` |
| Targeted LRU rerun log | `benchmarks/results/embedded_release_runs/lru_buffer_reuse_20260518_0521.log` |

## Metadata

| Field | Value |
| --- | --- |
| Host | Linux |
| OS/kernel | Ubuntu 24.04.4 LTS, `Linux server 6.8.0-111-generic` |
| CPU | AMD Ryzen 9 3950X, 16 cores / 32 threads |
| Rust | `rustc 1.95.0 (59807616e 2026-04-14)` |
| Cargo | `cargo 1.95.0 (f2d3ce0bd 2026-03-21)` |
| Source | Local working tree snapshot, base Git SHA `0e746a512995f2d047940d4a8a3fc1bb64f5d85f` |
| Full report rows | `1656` result rows |
| Targeted LRU rows | `264` result rows |

The benchmark script pins rows with `taskset` by vCPU budget:
`1 => 0`, `16 => 0-15`. Published values are medians across three repeats.
Latency sampling was disabled for these throughput rows.

## How To Reproduce

Full embedded report:

```bash
PHASE=report \
REPEATS=3 \
DURATION=6 \
WARMUP=1 \
LATENCY_SAMPLE_RATE=0 \
./benchmarks/scripts/run-embedded-release-matrix.sh
```

Targeted LRU write rerun:

```bash
PHASE=lru \
REPEATS=3 \
DURATION=10 \
WARMUP=2 \
LATENCY_SAMPLE_RATE=0 \
VALUE_SIZES="64 512 4096 65536" \
MIXES="0-100" \
LRU_CPU_CLIENT_PAIRS="1:1 16:16" \
LRU_RESIDENT_PCTS="25" \
OUT_DIR=benchmarks/results/lru_buffer_reuse_20260518_0521 \
./benchmarks/scripts/run-embedded-release-matrix.sh
```

## Interpretation Notes

- `100-0` is GET/read-only, `0-100` is SET/write-only, and `80-20` is
  80% GET / 20% SET.
- `fc-embed` is the owner-local direct embedded path. This is the deployment
  model where each worker owns its local shard slab.
- `fc-shared` is the shared-handle comparison path. It uses the recommended
  shared stripe count of `4 * max(vcpu_budget, clients)` so shared-handle rows
  are not under-striped. Use `fc-shared-worker-stripes` for the older
  one-stripe-per-worker comparison shape.
- `fc-shared-prepared` / `fc-shared-prepared-ref` are prepared-key A/B rows for
  repeated point-key access. `fc-shared-copy-unlocked` keeps copy-after-unlock
  as an explicit experiment; it is not the default shared copy path.
- Build the benchmark crate with `--features no-ttl` for TTL-free shared-store
  point-key runs that remove shared hot-path TTL checks.
- Large-value fast-cache GET GB/s uses `--read-mode ref` unless noted. That is
  a logical payload rate, computed as successful operations multiplied by value
  size. It is not physical data throughput because `get_ref` does not copy the
  value bytes. Use `--read-mode copy` for materialized read-copy bandwidth;
  large-value SET GB/s remains the better indicator of physical memory write
  pressure.
- Active TTL is `60000ms`.
- LRU capacity rows use 25% resident capacity.

## Executive Summary

- The owner-local embedded path is the clear fast-cache strength. It reaches
  `422.82M ops/s` on 64B GET at 16 workers and `114.85M ops/s` on 64B SET in
  the safe build.
- The unsafe build mainly helps small SET and mixed rows. For example, 64B SET
  at 16 workers moves from `114.85M` to `158.04M ops/s`.
- TTL overhead is visible on small writes, but largely disappears for larger
  value writes where memory bandwidth dominates.
- The targeted LRU rerun confirms that the 64KiB write path is stable after the
  reusable large-buffer change. The 1/1 no-TTL LRU SET row holds around
  `187k ops/s`, and active-TTL holds around `180k ops/s`.
- The 64KiB LRU write-only row is the worst case for fast-cache because it
  turns the benchmark into large-value materialization plus eviction
  bookkeeping. It is useful as a write-pressure checkpoint, but not as an
  overall LRU score.
- On the same 64KiB LRU shape at 16 workers, read-only reaches `756.93M ops/s`
  for fast-cache embedded direct versus `3.31M ops/s` for Moka and `579.8k
  ops/s` for the `lru` crate. The 80/20 row is close to Moka: `1.45M ops/s`
  for fast-cache versus `1.42M ops/s` for Moka.
- On the smaller 4KiB LRU shape at 16 workers, fast-cache embedded direct
  reaches `676.37M ops/s` read-only, `26.70M ops/s` on 80/20, and `5.41M
  ops/s` write-only.

## fc-embed Direct Headlines

Medians for `fc-embed`, safe build, no TTL, no eviction.

### 1 vCPU / 1 Client

| Value | GET | SET | 80/20 |
| --- | ---: | ---: | ---: |
| 64B | 7.81M ops/s, 0.50 GB/s | 4.30M ops/s, 0.28 GB/s | 6.18M ops/s, 0.40 GB/s |
| 4KiB | 7.31M ops/s, 29.93 GB/s | 1.55M ops/s, 6.35 GB/s | 3.57M ops/s, 14.64 GB/s |
| 64KiB | 7.21M ops/s, 472.59 GB/s | 204.1k ops/s, 13.38 GB/s | 846.4k ops/s, 55.47 GB/s |
| 1MiB | 33.60M ops/s, 35234.57 GB/s | 13.1k ops/s, 13.69 GB/s | 64.5k ops/s, 67.63 GB/s |

### 16 vCPU / 16 Clients

| Value | GET | SET | 80/20 |
| --- | ---: | ---: | ---: |
| 64B | 422.82M ops/s, 27.06 GB/s | 114.85M ops/s, 7.35 GB/s | 253.88M ops/s, 16.25 GB/s |
| 4KiB | 370.67M ops/s, 1518.25 GB/s | 4.45M ops/s, 18.22 GB/s | 21.03M ops/s, 86.14 GB/s |
| 64KiB | 502.32M ops/s, 32919.77 GB/s | 292.5k ops/s, 19.17 GB/s | 1.44M ops/s, 94.60 GB/s |
| 1MiB | 759.61M ops/s, 796509.96 GB/s | 18.6k ops/s, 19.49 GB/s | 93.3k ops/s, 97.82 GB/s |

## Head-To-Head: No TTL, No Eviction

Medians from the full report. Values are ops/sec.

### 1 vCPU / 1 Client

| Value | Mix | fc-embed safe | fc-embed unsafe | fc-shared | DashMap | Moka |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 64B | GET | 7.81M | 7.82M | 5.20M | 5.23M | 1.65M |
| 64B | SET | 4.30M | 5.23M | 4.03M | 4.05M | 807.6k |
| 64B | 80/20 | 6.18M | 6.37M | 4.64M | 4.52M | 1.08M |
| 4KiB | GET | 7.31M | 7.29M | 1.64M | 1.62M | 1.55M |
| 4KiB | SET | 1.55M | 1.75M | 1.41M | 1.10M | 496.8k |
| 4KiB | 80/20 | 3.57M | 3.76M | 1.57M | 1.38M | 692.1k |
| 64KiB | GET | 7.21M | 7.22M | 291.9k | 291.2k | 641.6k |
| 64KiB | SET | 204.1k | 206.7k | 201.5k | 158.3k | 128.0k |
| 64KiB | 80/20 | 846.4k | 856.0k | 264.6k | 208.0k | 181.9k |
| 1MiB | GET | 33.60M | 33.66M | 20.2k | 20.3k | 40.0k |
| 1MiB | SET | 13.1k | 13.0k | 13.3k | 10.9k | 11.3k |
| 1MiB | 80/20 | 64.5k | 64.5k | 18.2k | 15.2k | 15.4k |

### 16 vCPU / 16 Clients

| Value | Mix | fc-embed safe | fc-embed unsafe | fc-shared | DashMap | Moka |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 64B | GET | 422.82M | 422.55M | 53.34M | 54.84M | 2.65M |
| 64B | SET | 114.85M | 158.04M | 21.61M | 35.40M | 1.51M |
| 64B | 80/20 | 253.88M | 290.41M | 38.82M | 45.23M | 4.52M |
| 4KiB | GET | 370.67M | 371.53M | 9.62M | 9.55M | 2.85M |
| 4KiB | SET | 4.45M | 4.66M | 4.10M | 4.04M | 1.43M |
| 4KiB | 80/20 | 21.03M | 21.66M | 7.12M | 6.90M | 3.77M |
| 64KiB | GET | 502.32M | 501.90M | 691.8k | 692.1k | 3.04M |
| 64KiB | SET | 292.5k | 293.1k | 292.6k | 288.9k | 270.6k |
| 64KiB | 80/20 | 1.44M | 1.45M | 518.2k | 514.6k | 491.7k |
| 1MiB | GET | 759.61M | 758.41M | 44.1k | 44.1k | 541.0k |
| 1MiB | SET | 18.6k | 18.6k | 18.8k | 17.5k | 18.3k |
| 1MiB | 80/20 | 93.3k | 93.3k | 31.5k | 30.6k | 32.0k |

## TTL SET Cost

Medians for `fc-embed`, safe build, no eviction. Values are SET-only.

| Value | CPU/clients | No TTL SET | TTL SET | TTL/no-TTL |
| --- | --- | ---: | ---: | ---: |
| 64B | 1/1 | 4.30M | 3.79M | 0.88x |
| 64B | 16/16 | 114.85M | 97.09M | 0.85x |
| 4KiB | 1/1 | 1.55M | 1.47M | 0.95x |
| 4KiB | 16/16 | 4.45M | 4.44M | 1.00x |
| 64KiB | 1/1 | 204.1k | 202.4k | 0.99x |
| 64KiB | 16/16 | 292.5k | 293.2k | 1.00x |
| 1MiB | 1/1 | 13.1k | 13.0k | 1.00x |
| 1MiB | 16/16 | 18.6k | 18.6k | 1.00x |

## LRU Read And Mixed Reference

LRU caches are normally evaluated with reads in the mix. The targeted rerun
below isolates write-only behavior because that is the worst fast-cache case
for large values, but the full embedded report also includes read-only and
80/20 LRU rows. Medians below are no-TTL, 25% resident-capacity rows from
`embedded_report.csv`.

Large-value fast-cache GET rows use the default `--read-mode ref` behavior:
they measure lookup plus borrowed value access through `get_ref`, not copying
the full value into a new buffer on every hit. The reported GB/s is therefore a
logical payload rate for fast-cache reference-read rows, not real data
throughput. Use `--read-mode copy` for materialized read comparisons where
physical copy bandwidth is the intended measurement.

### 4KiB, 1 vCPU / 1 Client

| Mix | fast-cache embedded direct | fast-cache shared handle | Moka | `lru` crate |
| --- | ---: | ---: | ---: | ---: |
| GET | 26.42M ops/s | 5.45M ops/s | 4.09M ops/s | 4.15M ops/s |
| 80/20 | 4.01M ops/s | 2.74M ops/s | 1.19M ops/s | 2.37M ops/s |
| SET | 1.18M ops/s | 1.10M ops/s | 657.3k ops/s | 1.02M ops/s |

### 4KiB, 16 vCPU / 16 Clients

| Mix | fast-cache embedded direct | fast-cache shared handle | Moka | `lru` crate |
| --- | ---: | ---: | ---: | ---: |
| GET | 676.37M ops/s | 37.12M ops/s | 2.98M ops/s | 1.03M ops/s |
| 80/20 | 26.70M ops/s | 7.98M ops/s | 3.28M ops/s | 847.8k ops/s |
| SET | 5.41M ops/s | 2.56M ops/s | 896.0k ops/s | 458.8k ops/s |

### 64KiB, 1 vCPU / 1 Client

| Mix | fast-cache embedded direct | fast-cache shared handle | Moka | `lru` crate |
| --- | ---: | ---: | ---: | ---: |
| GET | 34.54M ops/s | 1.12M ops/s | 2.07M ops/s | 1.02M ops/s |
| 80/20 | 859.0k ops/s | 540.1k ops/s | 456.5k ops/s | 504.1k ops/s |
| SET | 188.0k ops/s | 184.1k ops/s | 238.9k ops/s | 177.6k ops/s |

### 64KiB, 16 vCPU / 16 Clients

| Mix | fast-cache embedded direct | fast-cache shared handle | Moka | `lru` crate |
| --- | ---: | ---: | ---: | ---: |
| GET | 756.93M ops/s | 2.87M ops/s | 3.31M ops/s | 579.8k ops/s |
| 80/20 | 1.45M ops/s | 991.5k ops/s | 1.42M ops/s | 329.2k ops/s |
| SET | 291.5k ops/s | 288.4k ops/s | 510.1k ops/s | 139.0k ops/s |

## Targeted LRU Write Checkpoint

This rerun targets the large-value LRU write path after adding reusable storage
for evicted large value buffers on the TTL/LRU write path. Treat it as a
write-pressure checkpoint, not a representative LRU workload. It also includes
64B, 512B, and 4KiB guard rows to verify that the large-buffer path does not
spill into small writes.

### 64KiB Write-Only LRU

| TTL | CPU/clients | Backend | Build | Ops/sec | GB/s | vCPU |
| --- | --- | --- | --- | ---: | ---: | ---: |
| none | 1/1 | fast-cache embedded direct | safe | 186,626 | 12.231 | 1.000 |
| none | 1/1 | fast-cache embedded direct | unsafe | 187,176 | 12.267 | 1.000 |
| none | 1/1 | fast-cache shared handle | safe | 183,801 | 12.046 | 1.000 |
| none | 1/1 | Moka | competitor | 239,218 | 15.677 | 1.000 |
| none | 1/1 | `lru` crate | competitor | 176,198 | 11.547 | 1.000 |
| active | 1/1 | fast-cache embedded direct | safe | 180,478 | 11.828 | 1.000 |
| active | 1/1 | fast-cache embedded direct | unsafe | 180,248 | 11.813 | 1.000 |
| active | 1/1 | fast-cache shared handle | safe | 179,329 | 11.752 | 1.000 |
| active | 1/1 | Moka | competitor | 238,049 | 15.601 | 1.000 |
| none | 16/16 | fast-cache embedded direct | safe | 291,721 | 19.118 | 15.995 |
| none | 16/16 | fast-cache embedded direct | unsafe | 291,661 | 19.114 | 15.985 |
| none | 16/16 | fast-cache shared handle | safe | 288,029 | 18.876 | 9.696 |
| none | 16/16 | Moka | competitor | 511,055 | 33.492 | 9.239 |
| none | 16/16 | `lru` crate | competitor | 140,631 | 9.216 | 1.590 |
| active | 16/16 | fast-cache embedded direct | safe | 288,418 | 18.902 | 15.991 |
| active | 16/16 | fast-cache embedded direct | unsafe | 291,181 | 19.083 | 15.993 |
| active | 16/16 | fast-cache shared handle | safe | 285,668 | 18.722 | 9.566 |
| active | 16/16 | Moka | competitor | 507,542 | 33.262 | 9.230 |

Compared with the stale pre-optimization row in
`embedded_release_20260517_195655/lru_first.csv`, the 64KiB 1/1 no-TTL
`fc-embed` median remains improved from roughly 140k ops/sec to 187k ops/sec.
The active-TTL 64KiB path now holds the same band, at roughly 180k ops/sec.

### Small-Value Guard Rows

Medians for fast-cache embedded direct, write-only LRU.

| Value | TTL | CPU/clients | Safe ops/sec | Unsafe ops/sec |
| --- | --- | --- | ---: | ---: |
| 64B | none | 1/1 | 3,528,371 | 3,649,898 |
| 64B | active | 1/1 | 2,602,997 | 2,598,674 |
| 64B | none | 16/16 | 137,180,419 | 146,506,611 |
| 64B | active | 16/16 | 88,465,995 | 87,752,749 |
| 512B | none | 1/1 | 2,442,071 | 2,484,384 |
| 512B | active | 1/1 | 1,995,145 | 1,990,750 |
| 512B | none | 16/16 | 109,094,849 | 113,628,895 |
| 512B | active | 16/16 | 76,083,730 | 75,940,907 |
| 4KiB | none | 1/1 | 1,187,228 | 1,206,231 |
| 4KiB | active | 1/1 | 1,059,443 | 1,062,735 |
| 4KiB | none | 16/16 | 5,408,949 | 5,422,226 |
| 4KiB | active | 16/16 | 5,559,686 | 5,554,717 |

The guard rows did not show a small-value regression from the large-buffer reuse
change.

## What To Optimize Next

- The largest remaining LRU gap is the pathological 64KiB write-only row at 16
  workers: Moka is around `511k ops/sec`, while `fc-embed` is around
  `292k ops/sec`. Read-only and 80/20 LRU rows are already competitive or ahead,
  so optimize this without adding overhead to LRU read touch/accounting.
- Small-value direct embedded performance is healthy; avoid optimizations that
  add branches or allocation behavior to the 64B/512B path.
- TTL overhead is primarily a small-value write issue. For 4KiB and larger SET
  rows, TTL cost is mostly lost under memory-copy/write cost.
