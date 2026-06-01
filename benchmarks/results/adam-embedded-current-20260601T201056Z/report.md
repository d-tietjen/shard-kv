# Adam Embedded Server Current-Code Rerun

Run directory: `benchmarks/results/adam-embedded-current-20260601T201056Z`

Host: `adam` / Ubuntu 24.04.4 LTS

Date: 2026-06-01 20:17 UTC

Source:

- Branch at sync time: `embedded-shardmap-server`
- Local base commit at sync time: `236ea760baf2112c5ba96e312d67a508b9faabb1`
- Working tree was dirty and synced as a source snapshot to
  `/home/dtietjen/shard-kv-current-20260601T201056Z`.
- Build command:

```bash
cargo build --release -p shardcache-benchmarks \
  --features embedded-server \
  --bin embedded_mixed_harness --bin resp_blast
```

## Workload

- One single-shard embedded database: `--shard-count 1`
- Same hot key for embedded and external access:
  `--key-count 1 --key-distribution hot:1:100`
- 1 KiB values on both the embedded workload and the external RESP-populated key
- Embedded side: one local GET worker,
  `--internal-workers 1 --internal-mix get`
- External side: RESP `GET k:0000000000000000`, 16 clients, pipeline 64
- External client ran on Adam while embedded measurement was active
- Ports: `6411` shared-arc, `6412` shard-arc, `6413` owner-local

## Results

| Mode | External ops/sec | External p50 | External p99 | External p999 | Internal ops/sec | Internal p50 | Internal p99 | Internal p999 | Process vCPU |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `shared-arc` | 1.78M | 9.1us | 9.8us | 10.0us | 20.15M | 50ns | 180ns | 430ns | 3.506 |
| `shard-arc` | 7.63M | 2.1us | 2.9us | 3.3us | 5.37M | 260ns | 820ns | 1.47us | 8.930 |
| `owner-local` | 1.34M | 11.6us | 12.8us | 17.8us | 1.45M | 40ns | 250ns | 370ns | 1.000 |

All rows completed with `errors=0`.

## Interpretation

- `shard-arc` is still the fastest external server path in this single-hot-key
  run, at 4.3x the `shared-arc` external throughput and 5.7x the `owner-local`
  external throughput.
- `shared-arc` still protects the embedded read loop best for this read-only
  hot-key workload: 20.15M internal ops/sec with 180ns p99 while serving third
  party RESP clients.
- `owner-local` keeps all work on the one owner thread and avoids cross-core
  locking, but a real 1 KiB external GET payload now time-slices enough protocol
  and socket work onto that thread that embedded throughput falls to 1.45M
  ops/sec. Its p99 remains sub-microsecond.
- The older owner-local report should not be treated as the current 1 KiB
  external GET result. This rerun is the current-code artifact for the
  embedded-as-server head-to-head.

## Artifacts

- `summary.csv`
- `shared-arc/external.txt`
- `shared-arc/internal.csv`
- `shared-arc/harness.log`
- `shard-arc/external.txt`
- `shard-arc/internal.csv`
- `shard-arc/harness.log`
- `owner-local/external.txt`
- `owner-local/internal.csv`
- `owner-local/harness.log`
