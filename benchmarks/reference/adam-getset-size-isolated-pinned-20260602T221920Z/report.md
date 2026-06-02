# standardized server benchmark adam-getset-size-isolated-pinned-20260602T221920Z

Primary CSV: `/home/dtietjen/shard-kv-bench-redis-cluster.TnRIzc/benchmarks/results/adam-getset-size-isolated-pinned-20260602T221920Z/redis-cluster.csv`
Reference CSVs:
- `/home/dtietjen/shard-kv-bench-redis-cluster.TnRIzc/benchmarks/results/adam-getset-size-isolated-pinned-20260602T221920Z/shardcache-scnp-direct.csv`

## Target Summary

| Target | Cases | Clients | Duration | Sum Ops/sec | Mean Avg us | Mean P99 us | Errors | vs `redis-cluster` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| redis-cluster | 12 | 256 | 10 | 28130358.6 | 143324.2 | 346727.1 | 0 | 1.00x |
| shardcache-scnp-direct | 12 | 256 | 10 | 47046992.0 | 95954.2 | 246725.5 | 0 | 1.67x |

## Common Cases vs Baseline

| Target | Common Cases | Target Sum Ops/sec | `redis-cluster` Sum Ops/sec | Ratio | Target Mean Avg us | `redis-cluster` Mean Avg us | P99 Cases | Target Mean P99 us | `redis-cluster` Mean P99 us | Target Errors | `redis-cluster` Errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| shardcache-scnp-direct | 12 | 47046992.0 | 28130358.6 | 1.67x | 95954.2 | 143324.2 | 12 | 246725.5 | 346727.1 | 0 | 0 |

## Slowest Cases: redis-cluster

| Family | Command | Case | Profile | Ops/sec | Avg us | P99 us | Errors |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| string | `GET` | GET large 64KiB value | large | 45580.3 | 371829.4 | 881328.1 | 0 |
| string | `SET` | SET large 64KiB value | large | 45580.3 | 371831.0 | 881328.1 | 0 |
| string | `GET` | GET large 256KiB value | large | 11318.1 | 377184.5 | 861405.2 | 0 |
| string | `SET` | SET large 256KiB value | large | 11318.1 | 377203.4 | 861405.2 | 0 |
| string | `SET` | SET large 16KiB value | large | 201067.2 | 84882.2 | 257687.6 | 0 |
| string | `GET` | GET large 16KiB value | large | 201067.2 | 83694.0 | 255721.5 | 0 |
| string | `SET` | SET large 4KiB value | large | 1010362.7 | 17489.1 | 58785.8 | 0 |
| string | `GET` | GET large 4KiB value | large | 1010362.7 | 17263.0 | 58261.5 | 0 |
| string | `SET` | SET large 1KiB value | large | 3283024.9 | 5865.9 | 15990.8 | 0 |
| string | `GET` | GET large 1KiB value | large | 3283024.9 | 5808.7 | 15876.1 | 0 |

## Slowest Cases: shardcache-scnp-direct

| Family | Command | Case | Profile | Ops/sec | Avg us | P99 us | Errors |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| string | `SET` | SET large 256KiB value | large | 18476.0 | 227877.9 | 676331.5 | 0 |
| string | `GET` | GET large 256KiB value | large | 18476.0 | 219789.2 | 665845.8 | 0 |
| string | `SET` | SET large 64KiB value | large | 90017.6 | 236253.4 | 592969.7 | 0 |
| string | `GET` | GET large 64KiB value | large | 90017.6 | 234386.8 | 592445.4 | 0 |
| string | `GET` | GET large 16KiB value | large | 363960.8 | 86579.7 | 156237.8 | 0 |
| string | `SET` | SET large 16KiB value | large | 363960.8 | 86596.0 | 156237.8 | 0 |
| string | `GET` | GET large 4KiB value | large | 1508316.6 | 20506.4 | 39944.2 | 0 |
| string | `SET` | SET large 4KiB value | large | 1508316.6 | 20511.1 | 39944.2 | 0 |
| string | `GET` | GET large 1KiB value | large | 4185756.6 | 7596.9 | 16719.9 | 0 |
| string | `SET` | SET large 1KiB value | large | 4185756.6 | 7598.9 | 16719.9 | 0 |
