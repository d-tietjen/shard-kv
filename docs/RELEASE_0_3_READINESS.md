# 0.3.0 Release Readiness

This note tracks what must be true before tagging `v0.3.0`.

## Release Shape

- `shardmap` is a crates.io crate for 0.3.0. It owns the embedded cache
  engine, storage, protocol, persistence, replication, and opt-in server
  internals.
- `shardcache-client-rs` is a crates.io crate for Rust clients of the native
  SCNP protocol.
- `shardcache` is a crates.io crate for the RESP/SCNP server binary and
  server-facing feature flags, including Redis/Valkey compatibility behind the
  `redis`, `redis-server`, and Redis module features.
- `shardcache-formal`, `shardcache-py`, `shardcache-runtime`, benchmarks, and
  integrations are workspace support packages and are not part of the crates.io
  publish set for this release.

## Known Limits

- `shardmap` packages the Redis compatibility source tree under
  `src/redis_compat` so optional Redis features work from the crates.io tarball;
  `shardcache` exposes that surface through its feature flags rather than a
  separate Redis compatibility package.
- Redis tier-1 compatibility now has explicit coverage for every command in the
  0.3.0 surface, including Redis 6, 7, and 8 additions tracked in the generated
  manifest.
- `shardmap` exposes the native typed `ShardMap<K, V>` API by default and the
  shared-engine `CodecShardMap<K, V>` facade behind the `codec` feature.
- `WATCH` and `UNWATCH` have snapshot-based runtime behavior; version-accurate
  invalidation for values changed away and back remains a compatibility gap.
- Benchmark writeups are curated summaries; raw outputs belong under ignored
  `benchmarks/results/`.
- `docs/REDIS_COMPATIBILITY.md` is generated from the command benchmark
  registry and guarded by `./scripts/check-redis-compatibility-doc.sh`.

## Required Proofs

Run these before tagging:

```bash
./scripts/proof-gate.sh release
```

Run the gate from a clean tree. `cargo package` intentionally rejects dirty
publishable crate files unless `--allow-dirty` is passed, and the release gate
does not pass that override. Use `--allow-dirty` only as a local diagnostic to
check package contents before the final commit.

The release gate includes `./scripts/check-publish-artifacts.sh`, which
packages every publishable crate and compiles temporary consumers against the
unpacked `.crate` archives. During a new `shardmap` version PR, the script uses
a temporary local crates.io patch while packaging dependent crates so the
pre-merge check does not wait for crates.io indexing. This is the guard for
missing packaged files, optional-feature source layout mistakes, and path
dependencies that only fail after Cargo rewrites them for crates.io.

The pure `--no-default-features` build is intentionally unsupported for 0.3.0
and should fail with a single compile error telling users to enable `embedded`
or `sharded`.

`shardmap` packages the explicit release tests and the small fuzz support
module they use. The large fuzz corpus and fuzz build artifacts stay out of the
published package.

## Benchmark Smoke

For a quick local signal after command or storage changes:

```bash
DOCKER=0 \
TARGETS=shardcache=127.0.0.1:6383 \
CASES=key,string,hash,set,zset \
CLIENTS=1 \
WARMUP=1 \
DURATION=2 \
CSV=benchmarks/results/redis-command-matrix-smoke.csv \
./benchmarks/scripts/run-redis-command-matrix.sh
```

For a reproducible artifact bundle with metadata, CSV, Markdown, JSON, and a
compatibility manifest:

```bash
CASES=extended-no-keyspace \
CLIENTS=16 \
KEY_SHARDS=16 \
FIXTURE_SCOPE=shared-keyspace \
WARMUP=2 \
DURATION=10 \
./benchmarks/scripts/run-redis-command-benchmark-bundle.sh
```

The latest benchmark-server command proof artifacts from 2026-05-24 are:

- depth 1: `benchmarks/results/redis-command-opcode-optimized-pass2-depth1-20260524T1555Z`
- ordered depth 16: `benchmarks/results/redis-command-opcode-optimized-pass2-depth16-20260524T1600Z`

For publishable claims, rerun the full Linux benchmark matrices from
`benchmarks/README.md` on a pinned host and update only curated writeups.
The curated command and transport summary for 0.3.0 is
`benchmarks/REDIS_HEAD_TO_HEAD_BENCHMARKS.md`. The typed/codec embedded
summary is `benchmarks/EMBEDDED_TYPED_CODEC_BENCHMARKS.md`. Raw result bundles
stay ignored under `benchmarks/results/`.

For full local command-path proofing, include all Redis command families and
fail on harness errors:

```bash
DOCKER=0 \
TARGETS=shardcache=127.0.0.1:6383 \
CASES=all \
CLIENTS=1 \
WARMUP=1 \
DURATION=1 \
FAIL_ON_ERROR=1 \
CSV=/private/tmp/shardcache-0.3-redis-command-matrix-all-proof.csv \
./benchmarks/scripts/run-redis-command-matrix.sh
```

When a local Redis reference is available, run the same matrix against both
targets:

```bash
DOCKER=0 \
TARGETS=shardcache=127.0.0.1:6383,redis=127.0.0.1:6384 \
CASES=all \
CLIENTS=1 \
WARMUP=1 \
DURATION=1 \
FAIL_ON_ERROR=1 \
CSV=/private/tmp/shardcache-0.3-redis-command-matrix-shardcache-vs-redis.csv \
./benchmarks/scripts/run-redis-command-matrix.sh
```

Valkey and Dragonfly should still be run before merge on a machine with Docker
or local server binaries available.

## Docker Deployment

The Dockerfile and Compose file are ready for local/private deployment testing.
Compose builds `shardcache:local` and does not push to Docker Hub or any remote
registry. The default Docker build uses the Redis/Valkey-compatible
`redis-server` feature set and starts `--disable-persistence --server-mode
direct`, so the container path is currently in-memory compatibility testing,
not durable Redis-compatible storage.

## Publish Set

The publishable crates are `shardmap`, `shardcache-client-rs`, and
`shardcache`. Because `shardcache` depends on `shardmap = 0.3.0`, its
crates.io dry-run cannot resolve until `shardmap 0.3.0` is visible in the
crates.io index.

```bash
./scripts/check-publish-artifacts.sh

cargo publish -p shardmap --dry-run
cargo publish -p shardcache-client-rs --dry-run
cargo publish -p shardmap
cargo publish -p shardcache-client-rs

# After crates.io indexes shardmap 0.3.0:
cargo publish -p shardcache --dry-run
cargo publish -p shardcache
```

Publish `shardmap` before `shardcache`, because `shardcache` depends on the new
`shardmap` version. After `shardmap 0.3.0` is indexed,
`shardcache` can be published.
`shardcache-client-rs` can be published independently. All other workspace
packages have `publish = false` for 0.3.0.
