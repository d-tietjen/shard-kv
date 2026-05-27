# Contributing

Thanks for helping improve `shard-kv`.

## Development Setup

Use a current stable Rust toolchain that satisfies the workspace
`rust-version`, then run:

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo test -p shardmap --features unsafe
cargo test -p shardmap --features redis
cargo check -p shardcache --features redis
cargo check -p shardcache --features redis-server
cargo check -p shardcache-redis --all-features
cargo doc -p shardmap --no-deps --all-features
cargo doc -p shardcache-redis --no-deps --all-features
cargo package -p shardmap --locked
```

Before making larger changes, skim [docs/PROJECT_STRUCTURE.md](docs/PROJECT_STRUCTURE.md)
for the repository map, common change locations, and generated artifact policy.
For release work, also follow
[docs/RELEASE_0_2_READINESS.md](docs/RELEASE_0_2_READINESS.md).

## Pull Requests

- Keep changes focused and explain behavior changes in the PR body.
- Add or update tests for protocol, storage, persistence, or compatibility
  changes.
- Update rustdoc or the relevant crate README when public APIs change.
- Do not commit raw benchmark output, host snapshots, generated PDFs, local
  `.claude` config, formal-verification caches, rustc ICE reports, runtime
  data, or generated Python/Rust build artifacts.
- When publishing benchmark claims, include the command, date, machine shape,
  correctness gate, and raw result path or archive.

## Unsafe Code

The default build is safe by default. Any new unsafe hot path must:

- be gated behind the `unsafe` feature unless there is no safe alternative;
- include a local `SAFETY:` comment that states the invariant;
- have a safe fallback where practical;
- be covered by tests in both default and `--features unsafe` builds;
- update `crates/shardmap/SAFETY.md`.
