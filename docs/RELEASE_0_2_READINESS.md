# 0.2.0 Release Readiness

This note tracks what must be true before tagging `v0.2.0`.

## Release Shape

- `fast-cache` is the public facade crate and owns the installable
  `fast-cache-server` binary.
- `fast-cache-core` owns the embedded cache engine, storage, protocol,
  persistence, replication, and server runtime implementation.
- `fast-cache-redis` owns Redis/Valkey compatibility source. It is
  `publish = false` for 0.2.0 and depends on `fast-cache-core`.
- `fast-cache-formal`, `fast-cache-py`, `fast-cache-runtime`, benchmarks, and
  integrations are workspace support crates and are not part of the crates.io
  publish set for this release.

## Known Limits

- Redis source has moved out of core, but core still path-includes it behind
  `redis-compat`. A later release should replace that bridge with a normal
  extension dependency boundary.
- Redis tier-1 compatibility intentionally excludes `DUMP` and `RESTORE`.
- `WATCH` and `UNWATCH` have snapshot-based runtime behavior and are recorded
  as partial in the compatibility ledger.
- Benchmark writeups are curated summaries; raw outputs belong under ignored
  `benchmarks/results/`.

## Required Proofs

Run these before tagging:

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo test -p fast-cache-core --features unsafe
cargo test -p fast-cache-formal
FAST_CACHE_COMPAT_SERVER_BIN=redis-server \
  cargo test -p fast-cache-core --features redis-server \
  --test redis_compat_differential_test -- --nocapture
cargo check -p fast-cache --features redis-server
cargo check -p fast-cache-redis --all-features
cargo doc -p fast-cache-core --no-deps --all-features
cargo doc -p fast-cache --no-deps --all-features
cargo doc -p fast-cache-redis --no-deps --all-features
cargo package -p fcnp-client-rs --locked
cargo package -p fast-cache-core --locked
git diff --check
```

## Benchmark Smoke

For a quick local signal after command or storage changes:

```bash
DOCKER=0 \
TARGETS=fast-cache=127.0.0.1:6383 \
CASES=key,string,hash,set,zset \
CLIENTS=1 \
WARMUP=1 \
DURATION=2 \
CSV=benchmarks/results/redis-command-matrix-smoke.csv \
./benchmarks/scripts/run-redis-command-matrix.sh
```

For publishable claims, rerun the full Linux benchmark matrices from
`benchmarks/README.md` on a pinned host and update only curated writeups.

## Publish Order

The publishable crates are `fcnp-client-rs`, `fast-cache-core`, and
`fast-cache`. Dry-run and publish `fcnp-client-rs` and `fast-cache-core`
first; then dry-run and publish `fast-cache` after `fast-cache-core` is
available from the registry.
