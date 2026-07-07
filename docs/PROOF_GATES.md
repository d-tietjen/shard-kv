# Proof Gates

Use these gates to keep the repository honest as the crate layout, Redis
compatibility surface, and benchmark claims evolve.

## Local Gates

```bash
./scripts/proof-gate.sh quick
./scripts/proof-gate.sh redis
./scripts/proof-gate.sh release
```

| Gate | Purpose |
| --- | --- |
| `quick` | Formatting, benchmark harness unit tests, compatibility manifest freshness, feature-flag compile matrix, crates.io package artifact checks, and whitespace diff checks. |
| `redis` | Everything in `quick`, plus Redis compatibility tests, raw RESP server tests, and the live differential test against `SHARDCACHE_COMPAT_SERVER_BIN` or `redis-server`. |
| `release` | Everything in `redis`, plus the workspace test suite, formal support tests, and rustdoc for all publishable crates. |

Use `quick` while iterating on source layout, feature flags, command registry,
or docs. Use `redis` before merging compatibility changes. Use `release`
before tagging or publishing.

## Focused Gates

```bash
./scripts/check-feature-matrix.sh
./scripts/check-redis-compatibility-doc.sh
./scripts/check-publish-artifacts.sh
```

`check-feature-matrix.sh` compiles the current public feature contract for
`shardcache` and `shardmap`, including the Redis-compatible server and module
feature flags.

`check-redis-compatibility-doc.sh` regenerates
`docs/REDIS_COMPATIBILITY.md` from `benchmarks/src/redis_command_cases.rs` and
diffs it against the tracked file. When command cases change, regenerate the
doc intentionally:

```bash
cargo run -p shardcache-benchmarks --bin redis_command_manifest -- \
  --output docs/REDIS_COMPATIBILITY.md
```

`check-publish-artifacts.sh` packages every publishable crate, unpacks the
generated `.crate` archives, and compiles temporary consumer crates with
`[patch.crates-io]` pointing at those unpacked archives. This catches issues
that only appear after Cargo rewrites workspace `path` dependencies and
verifies optional Redis/server features from the same artifact layout users get
from crates.io.

## Benchmark Artifacts

The command matrix bundle script packages machine-readable and human-readable
proofs in one ignored result directory:

```bash
CASES=extended-no-keyspace \
CLIENTS=16 \
KEY_SHARDS=4 \
FIXTURE_SCOPE=shared-keyspace \
WARMUP=2 \
DURATION=10 \
./benchmarks/scripts/run-redis-command-benchmark-bundle.sh
```

The bundle contains:

| File | Purpose |
| --- | --- |
| `metadata.txt` | Git SHA, dirty status, host/runtime versions, benchmark knobs, pinning knobs. |
| `redis-command-matrix.csv` | Raw per-target, per-command throughput plus p50/p95/p99/p999 latency from the live RESP harness. |
| `report.md` | Target summary, throughput ratios, mean p99, and slowest-case tables. |
| `summary.json` | Machine-readable target totals, p99 summaries, and ratios. |
| `redis-compatibility.json` | JSON command compatibility manifest captured with the run. |

When Redis, Valkey, or Dragonfly references are already captured for the same
host and benchmark knobs, rerun only shardcache and pass the saved CSV with
`REFERENCE_CSVS=/path/to/redis-command-matrix.csv`. The report merges the saved
rows and adds a common-case comparison table so shardcache changes do not force
the external services to be rerun every iteration. `REFERENCE_CSVS` is
comma-separated when the references are split across multiple CSVs.

Raw bundles live under ignored `benchmarks/results/`. Promote only curated
writeups or selected summaries into tracked docs.
