# Adam Active-Sync Read-Heavy Release Runs

Measured on 2026-07-16 from commit
`5d06a565e54bcfd6b181d6a8896995dec948219d` on Adam. Each row is an isolated
release process pinned to CPUs `0-7`, with eight shards, eight clients, 100,000
keys, 1 KiB values, two seconds of warmup, ten seconds of measurement, a 100 ms
sync interval, and one latency sample per 10,000 operations.

Synchronized modes propagated background errors, drained bounded final rounds
until no transfer remained, and compared every key on both maps. All 12
synchronized runs converged without error.

## Three-Run Results

| Workload | Mode | Run 1 | Run 2 | Run 3 | Median | Retained | p50 | p99 | p999 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 99/1 | baseline | 17.771M | 17.468M | 18.266M | 17.771M | 100.0% | 360ns | 2.1us | 5.0us |
| 99/1 | causal-local | 15.248M | 15.241M | 15.270M | 15.248M | 85.8% | 390ns | 2.2us | 9.3us |
| 99/1 | consensus-local | 14.784M | 15.104M | 15.075M | 15.075M | 84.8% | 400ns | 2.3us | 8.2us |
| 99/1 | causal-sync | 13.451M | 13.776M | 13.861M | 13.776M | 77.5% | 390ns | 2.3us | 20.8us |
| 99/1 | consensus-sync | 13.339M | 13.259M | 13.372M | 13.339M | 75.1% | 400ns | 2.3us | 19.5us |
| 95/5 | baseline | 17.704M | 17.572M | 17.535M | 17.572M | 100.0% | 350ns | 2.2us | 5.7us |
| 95/5 | causal-local | 14.673M | 14.559M | 14.237M | 14.559M | 82.9% | 400ns | 2.3us | 7.4us |
| 95/5 | consensus-local | 14.089M | 14.375M | 13.672M | 14.089M | 80.2% | 410ns | 2.4us | 9.0us |
| 95/5 | causal-sync | 11.879M | 11.981M | 10.962M | 11.879M | 67.6% | 420ns | 2.7us | 16.6us |
| 95/5 | consensus-sync | 11.717M | 11.611M | 11.374M | 11.611M | 66.1% | 410ns | 2.5us | 25.2us |

Latency columns are the medians of the three reported run percentiles, not a
merged histogram. `Retained` divides median mode throughput by median baseline
throughput for the same workload.

## Command

The binary was built with:

```bash
cargo build --release -p shardcache-benchmarks \
  --features active-sync-consensus-ordered-eventual --bin active_sync_cost
```

Each mode and workload used a separate process:

```bash
taskset -c 0-7 target/release/active_sync_cost \
  --modes MODE --shards 8 --clients 8 --key-count 100000 \
  --value-size 1024 --read-percent MIX --warmup 2 --duration 10 \
  --sync-interval-ms 100 --latency-sample-rate 10000
```

`MODE` is one of `baseline`, `causal-local`, `consensus-local`, `causal-sync`,
or `consensus-sync`; `MIX` is `99` or `95`.
