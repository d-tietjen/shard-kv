# fast-cache vs Moka Embedded

This report records a local embedded head-to-head between fast-cache's
owner-local embedded path and `moka::sync::Cache`.

## Run Metadata

| Field | Value |
| --- | --- |
| Date | May 15, 2026 |
| Host | `Laptop-Devon.local` |
| OS | macOS, Darwin 25.2.0, arm64 |
| Online CPUs | 10 |
| Driver | `fast-cache-benchmarks` `saturation` |
| Backends | `fc-embed,moka` |
| Key distribution | uniform |
| Pipeline depth | 1 |
| Warmup | 1s |
| Measurement | 5s per row |
| Latency sampling | `--latency-sample-rate 1024` |
| Errors | 0 across all rows |
| CSV artifact | `benchmarks/results/fc_embed_vs_moka_embedded_20260515_110402.csv` |
| Log artifact | `benchmarks/results/fc_embed_vs_moka_embedded_20260515_110402.log` |

The matrix swept:

- Value sizes: `64B`, `512B`, `4KiB`, `64KiB`, `1MiB`
- Mixes: `100-0` GET, `0-100` SET, `80-20` mixed
- Clients and vCPU budgets: `1`, `4`, `10`

Large values used a 512 MiB hot-set cap to keep local memory pressure bounded:
`100k` keys through `4KiB`, `8192` keys at `64KiB`, and `512` keys at `1MiB`.

## Interpretation Notes

`fc-embed` measures the intended fast-cache embedded architecture: each worker
owns a `LocalEmbeddedStore`, and key streams are routed to the worker that owns
the corresponding shard. This is not a shared `Arc<Cache>` model.

`moka` measures `moka::sync::Cache<Vec<u8>, Arc<[u8]>>` with cloned cache
handles. On GET, this benchmark copies the returned Moka value into the scratch
buffer. The fast-cache `fc-embed` GET path checks a local point reference and
does not copy the value into scratch. Because the driver counts one logical
value-size unit for each successful operation, large-value GET GB/s is logical
workload throughput, not physical memory bandwidth.

For copy-heavy comparison, the large-value SET rows are the most useful: at
`1MiB`, `10` clients, fast-cache and Moka are close on pure SET throughput
(`119.6K` ops/s vs `116.1K` ops/s).

## Summary

fast-cache won all `45/45` paired rows in this run.

| Scope | Result |
| --- | --- |
| Small values, 10 clients | fast-cache was `25.9x` to `93.5x` faster depending on size and mix |
| 64KiB, 10 clients | fast-cache was `4.1x` faster on SET, `8.8x` on 80/20, and `418.7x` on logical GET |
| 1MiB, 10 clients | fast-cache was `1.03x` faster on SET, `6.45x` on 80/20, and `6002.9x` on logical GET |
| Tail latency | fast-cache p99 was lower in every 10-client row |

## 10-Client Headline

| Value | Mix | fast-cache ops/s | Moka ops/s | Speedup | fast-cache GB/s | Moka GB/s | fast-cache vCPU | Moka vCPU |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64B | 100-0 | 315.36M | 3.42M | 92.31x | 20.183 | 0.219 | 9.28 | 8.05 |
| 64B | 0-100 | 119.66M | 1.58M | 75.75x | 7.658 | 0.101 | 8.72 | 8.40 |
| 64B | 80-20 | 231.07M | 3.23M | 71.46x | 14.789 | 0.207 | 9.36 | 6.67 |
| 512B | 100-0 | 326.68M | 3.50M | 93.45x | 167.258 | 1.790 | 9.58 | 9.03 |
| 512B | 0-100 | 40.81M | 1.57M | 25.92x | 20.896 | 0.806 | 9.37 | 8.61 |
| 512B | 80-20 | 122.88M | 4.10M | 29.94x | 62.916 | 2.101 | 9.04 | 8.95 |
| 4KiB | 100-0 | 319.92M | 3.76M | 85.17x | 1310.409 | 15.385 | 9.37 | 9.36 |
| 4KiB | 0-100 | 9.54M | 1.52M | 6.29x | 39.069 | 6.215 | 9.19 | 8.85 |
| 4KiB | 80-20 | 37.24M | 3.61M | 10.31x | 152.538 | 14.789 | 9.45 | 9.17 |
| 64KiB | 100-0 | 500.47M | 1.20M | 418.66x | 32798.900 | 78.343 | 9.19 | 9.35 |
| 64KiB | 0-100 | 1.91M | 0.47M | 4.06x | 125.057 | 30.783 | 9.14 | 9.16 |
| 64KiB | 80-20 | 9.33M | 1.06M | 8.80x | 611.411 | 69.494 | 8.37 | 8.13 |
| 1MiB | 100-0 | 598.73M | 0.10M | 6002.92x | 627809.206 | 104.584 | 9.32 | 9.00 |
| 1MiB | 0-100 | 0.12M | 0.12M | 1.03x | 125.388 | 121.764 | 9.43 | 9.35 |
| 1MiB | 80-20 | 0.60M | 0.09M | 6.45x | 627.477 | 97.254 | 9.41 | 9.19 |

## Single-Client Baseline

| Value | Mix | fast-cache ops/s | Moka ops/s | Speedup | fast-cache GB/s | Moka GB/s | fast-cache vCPU | Moka vCPU |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64B | 100-0 | 14.36M | 2.17M | 6.61x | 0.919 | 0.139 | 0.99 | 1.00 |
| 64B | 0-100 | 7.68M | 0.86M | 8.91x | 0.492 | 0.055 | 0.99 | 1.00 |
| 64B | 80-20 | 12.83M | 1.35M | 9.49x | 0.821 | 0.087 | 1.00 | 1.00 |
| 512B | 100-0 | 15.81M | 1.81M | 8.72x | 8.094 | 0.928 | 1.00 | 1.00 |
| 512B | 0-100 | 3.32M | 0.68M | 4.85x | 1.700 | 0.350 | 1.00 | 0.97 |
| 512B | 80-20 | 9.70M | 1.14M | 8.48x | 4.967 | 0.586 | 1.00 | 1.00 |
| 4KiB | 100-0 | 14.31M | 1.42M | 10.11x | 58.628 | 5.798 | 1.00 | 1.00 |
| 4KiB | 0-100 | 1.43M | 0.55M | 2.61x | 5.857 | 2.248 | 1.00 | 1.00 |
| 4KiB | 80-20 | 4.73M | 0.79M | 6.00x | 19.380 | 3.233 | 1.00 | 1.00 |
| 64KiB | 100-0 | 51.18M | 0.77M | 66.72x | 3354.192 | 50.269 | 1.00 | 1.00 |
| 64KiB | 0-100 | 0.93M | 0.43M | 2.18x | 60.828 | 27.887 | 1.00 | 1.00 |
| 64KiB | 80-20 | 4.30M | 0.41M | 10.55x | 281.994 | 26.740 | 1.00 | 1.00 |
| 1MiB | 100-0 | 59.50M | 0.05M | 1221.42x | 62385.402 | 51.076 | 1.00 | 1.00 |
| 1MiB | 0-100 | 0.05M | 0.05M | 1.07x | 57.261 | 53.590 | 0.99 | 1.00 |
| 1MiB | 80-20 | 0.26M | 0.04M | 5.91x | 268.578 | 45.474 | 0.99 | 1.00 |

## 4-Client Scaling Point

| Value | Mix | fast-cache ops/s | Moka ops/s | Speedup | fast-cache GB/s | Moka GB/s | fast-cache vCPU | Moka vCPU |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64B | 100-0 | 84.54M | 3.48M | 24.32x | 5.410 | 0.222 | 3.96 | 3.92 |
| 64B | 0-100 | 43.86M | 1.20M | 36.63x | 2.807 | 0.077 | 3.99 | 3.99 |
| 64B | 80-20 | 70.48M | 2.98M | 23.65x | 4.511 | 0.191 | 3.98 | 3.99 |
| 512B | 100-0 | 89.38M | 4.08M | 21.90x | 45.762 | 2.089 | 3.99 | 3.99 |
| 512B | 0-100 | 14.59M | 1.16M | 12.60x | 7.470 | 0.593 | 3.99 | 3.96 |
| 512B | 80-20 | 44.13M | 2.69M | 16.42x | 22.594 | 1.376 | 3.99 | 3.98 |
| 4KiB | 100-0 | 74.68M | 3.51M | 21.28x | 305.906 | 14.374 | 3.95 | 3.99 |
| 4KiB | 0-100 | 4.95M | 1.02M | 4.87x | 20.258 | 4.163 | 3.99 | 3.93 |
| 4KiB | 80-20 | 17.88M | 2.13M | 8.39x | 73.219 | 8.729 | 3.99 | 3.98 |
| 64KiB | 100-0 | 221.34M | 0.84M | 262.34x | 14505.467 | 55.292 | 3.96 | 3.98 |
| 64KiB | 0-100 | 1.77M | 0.43M | 4.08x | 116.119 | 28.433 | 3.96 | 3.96 |
| 64KiB | 80-20 | 8.99M | 0.77M | 11.70x | 589.138 | 50.352 | 4.00 | 3.77 |
| 1MiB | 100-0 | 308.76M | 0.07M | 4367.06x | 323753.539 | 74.135 | 3.99 | 4.00 |
| 1MiB | 0-100 | 0.11M | 0.09M | 1.18x | 115.561 | 97.785 | 4.00 | 3.99 |
| 1MiB | 80-20 | 0.52M | 0.08M | 6.53x | 546.098 | 83.616 | 3.98 | 3.99 |

## 10-Client Tail Latency

| Value | Mix | fast-cache p99 | Moka p99 | fast-cache p999 | Moka p999 |
| ---: | --- | ---: | ---: | ---: | ---: |
| 64B | 100-0 | 166ns | 21.3us | 291ns | 96.6us |
| 64B | 0-100 | 291ns | 30.5us | 416ns | 160.3us |
| 64B | 80-20 | 208ns | 11.8us | 333ns | 117.8us |
| 512B | 100-0 | 166ns | 21.2us | 291ns | 110.4us |
| 512B | 0-100 | 625ns | 29.1us | 791ns | 494.6us |
| 512B | 80-20 | 375ns | 12.4us | 625ns | 84.1us |
| 4KiB | 100-0 | 166ns | 18.3us | 291ns | 92.9us |
| 4KiB | 0-100 | 1.8us | 31.1us | 2.4us | 371.5us |
| 4KiB | 80-20 | 1.2us | 10.3us | 1.7us | 86.3us |
| 64KiB | 100-0 | 41ns | 35.4us | 83ns | 93.5us |
| 64KiB | 0-100 | 6.8us | 139.8us | 55.6us | 619.5us |
| 64KiB | 80-20 | 6.0us | 51.4us | 7.3us | 923.6us |
| 1MiB | 100-0 | 41ns | 151.7us | 83ns | 1.2ms |
| 1MiB | 0-100 | 127.9us | 265.5us | 627.7us | 2.8ms |
| 1MiB | 80-20 | 90.6us | 216.8us | 163.6us | 453.1us |

## Reproduction Command

The run was produced with the release `saturation` binary:

```bash
cargo build --release -p fast-cache-benchmarks --bin saturation

mkdir -p benchmarks/results
OUT="benchmarks/results/fc_embed_vs_moka_embedded_$(date +%Y%m%d_%H%M%S).csv"

for value_size in 64 512 4096 65536 1048576; do
  key_count=$(python3 -c "print(min(100000, max(64, 536870912 // ${value_size})))")
  for mix in 100-0 0-100 80-20; do
    for clients in 1 4 10; do
      ./target/release/saturation \
        --backends fc-embed,moka \
        --value-size "$value_size" \
        --mix "$mix" \
        --vcpu-budget "$clients" \
        --clients "$clients" \
        --key-count "$key_count" \
        --warmup 1 \
        --duration 5 \
        --latency-sample-rate 1024 \
        --csv "$OUT"
    done
  done
done
```
