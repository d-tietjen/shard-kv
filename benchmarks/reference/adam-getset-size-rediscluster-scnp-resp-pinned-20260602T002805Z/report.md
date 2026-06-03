# standardized server benchmark adam-getset-size-rediscluster-scnp-resp-pinned-20260602T002805Z

Primary CSV: `/home/dtietjen/shard-kv-bench-redis-cluster.TnRIzc/benchmarks/results/adam-getset-size-rediscluster-scnp-resp-pinned-20260602T002805Z/redis-cluster.csv`
Reference CSVs:
- `/home/dtietjen/shard-kv-bench-redis-cluster.TnRIzc/benchmarks/results/adam-getset-size-rediscluster-scnp-resp-pinned-20260602T002805Z/shardcache-scnp-direct.csv`
- `/home/dtietjen/shard-kv-bench-redis-cluster.TnRIzc/benchmarks/results/adam-getset-size-rediscluster-scnp-resp-pinned-20260602T002805Z/shardcache-resp.csv`

## Target Summary

| Target | Cases | Clients | Duration | Sum Ops/sec | Mean Avg us | Mean P99 us | Errors | vs `redis-cluster` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| redis-cluster | 12 | 256 | 10 | 28436644.0 | 141872.4 | 325987.0 | 0 | 1.00x |
| shardcache-resp | 12 | 256 | 10 | 44427406.0 | 100701.3 | 288459.4 | 0 | 1.56x |
| shardcache-scnp-direct | 12 | 256 | 10 | 47893486.2 | 92247.4 | 238838.1 | 0 | 1.68x |

## Common Cases vs Baseline

| Target | Common Cases | Target Sum Ops/sec | `redis-cluster` Sum Ops/sec | Ratio | Target Mean Avg us | `redis-cluster` Mean Avg us | P99 Cases | Target Mean P99 us | `redis-cluster` Mean P99 us | Target Errors | `redis-cluster` Errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| shardcache-resp | 12 | 44427406.0 | 28436644.0 | 1.56x | 100701.3 | 141872.4 | 12 | 288459.4 | 325987.0 | 0 | 0 |
| shardcache-scnp-direct | 12 | 47893486.2 | 28436644.0 | 1.68x | 92247.4 | 141872.4 | 12 | 238838.1 | 325987.0 | 0 | 0 |

## Slowest Cases: redis-cluster

| Family | Command | Case | Profile | Ops/sec | Avg us | P99 us | Errors |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| string | `GET` | GET large 64KiB value | large | 46050.2 | 369643.6 | 842530.8 | 0 |
| string | `SET` | SET large 64KiB value | large | 46050.2 | 369645.2 | 842530.8 | 0 |
| string | `GET` | GET large 256KiB value | large | 11460.7 | 373459.6 | 812122.1 | 0 |
| string | `SET` | SET large 256KiB value | large | 11460.7 | 373482.6 | 812122.1 | 0 |
| string | `SET` | SET large 16KiB value | large | 208926.4 | 81616.8 | 228065.3 | 0 |
| string | `GET` | GET large 16KiB value | large | 208926.4 | 80476.3 | 226099.2 | 0 |
| string | `SET` | SET large 4KiB value | large | 971646.5 | 18147.3 | 53149.7 | 0 |
| string | `GET` | GET large 4KiB value | large | 971646.5 | 17911.3 | 52723.7 | 0 |
| string | `SET` | SET large 1KiB value | large | 3383814.5 | 5680.7 | 15130.6 | 0 |
| string | `GET` | GET large 1KiB value | large | 3383814.5 | 5624.9 | 15015.9 | 0 |

## Slowest Cases: shardcache-resp

| Family | Command | Case | Profile | Ops/sec | Avg us | P99 us | Errors |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| string | `SET` | SET large 256KiB value | large | 17074.8 | 235667.2 | 752353.3 | 0 |
| string | `GET` | GET large 256KiB value | large | 17074.8 | 226453.8 | 741867.5 | 0 |
| string | `SET` | SET large 64KiB value | large | 80645.0 | 262414.2 | 688390.1 | 0 |
| string | `GET` | GET large 64KiB value | large | 80645.0 | 260380.9 | 687341.6 | 0 |
| string | `GET` | GET large 16KiB value | large | 365301.6 | 82561.2 | 208535.6 | 0 |
| string | `SET` | SET large 16KiB value | large | 365301.6 | 82622.4 | 208535.6 | 0 |
| string | `SET` | SET large 4KiB value | large | 1533043.6 | 19711.0 | 58654.7 | 0 |
| string | `GET` | GET large 4KiB value | large | 1533043.6 | 19698.2 | 58622.0 | 0 |
| string | `GET` | GET large 1KiB value | large | 4238185.9 | 7421.1 | 23117.8 | 0 |
| string | `SET` | SET large 1KiB value | large | 4238185.9 | 7423.3 | 23117.8 | 0 |

## Slowest Cases: shardcache-scnp-direct

| Family | Command | Case | Profile | Ops/sec | Avg us | P99 us | Errors |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| string | `SET` | SET large 256KiB value | large | 19681.9 | 210171.0 | 649068.5 | 0 |
| string | `GET` | GET large 256KiB value | large | 19681.9 | 202452.6 | 640679.9 | 0 |
| string | `SET` | SET large 64KiB value | large | 89446.1 | 238383.1 | 595591.2 | 0 |
| string | `GET` | GET large 64KiB value | large | 89446.1 | 236526.5 | 595066.9 | 0 |
| string | `SET` | SET large 16KiB value | large | 388713.2 | 80901.9 | 140509.2 | 0 |
| string | `GET` | GET large 16KiB value | large | 388713.2 | 80881.9 | 140247.0 | 0 |
| string | `GET` | GET large 4KiB value | large | 1572488.9 | 19684.1 | 34504.7 | 0 |
| string | `SET` | SET large 4KiB value | large | 1572488.9 | 19688.5 | 34504.7 | 0 |
| string | `SET` | SET large 1KiB value | large | 4371801.1 | 7279.0 | 14409.7 | 0 |
| string | `GET` | GET large 1KiB value | large | 4371801.1 | 7277.1 | 14401.5 | 0 |
