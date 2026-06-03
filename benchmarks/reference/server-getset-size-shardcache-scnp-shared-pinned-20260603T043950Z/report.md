# standardized server benchmark server-suite-20260603T043950Z

Primary CSV: `/home/dtietjen/shard-kv-bench-shared-scnp.kzgP5a/benchmarks/results/server-getset-size-shardcache-scnp-shared-pinned-20260603T043950Z/server-suite-20260603T043950Z/shardcache-scnp.csv`
Reference CSVs: none

## Target Summary

| Target | Cases | Clients | Duration | Sum Ops/sec | Mean Avg us | Mean P99 us | Errors | vs `shardcache-scnp` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| shardcache-scnp | 60 | 256 | 10 | 92254783.4 | 216545.5 | 343592.0 | 0 | 1.00x |

## Common Cases vs Baseline

_No comparison targets shared command cases with `shardcache-scnp`._

## Slowest Cases: shardcache-scnp

| Family | Command | Case | Profile | Ops/sec | Avg us | P99 us | Errors |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| string | `GET` | GET large 256KiB value | large | 6215.5 | 1346621.6 | 1000341.5 | 0 |
| string | `SET` | SET large 256KiB value | large | 6215.5 | 1346781.1 | 1000341.5 | 0 |
| string | `GET` | GET large 256KiB value | large | 10853.1 | 392123.6 | 1000341.5 | 0 |
| string | `SET` | SET large 256KiB value | large | 10853.1 | 405649.6 | 1000341.5 | 0 |
| string | `GET` | GET large 64KiB value | large | 22758.4 | 1407478.9 | 1000341.5 | 0 |
| string | `SET` | SET large 64KiB value | large | 22758.4 | 1408257.6 | 1000341.5 | 0 |
| string | `GET` | GET large 64KiB value | large | 39052.5 | 591618.5 | 1000341.5 | 0 |
| string | `SET` | SET large 64KiB value | large | 39052.5 | 595444.0 | 1000341.5 | 0 |
| string | `SET` | SET large 64KiB value | large | 61531.5 | 343537.0 | 808452.1 | 0 |
| string | `GET` | GET large 64KiB value | large | 61531.5 | 340713.8 | 807927.8 | 0 |
