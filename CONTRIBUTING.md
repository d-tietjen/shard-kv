# Contributing

Thanks for helping improve `fast-cache`.

## Development Setup

Use a current stable Rust toolchain that satisfies the workspace
`rust-version`, then run:

```bash
cargo fmt --all -- --check
cargo test -p fast-cache
cargo test -p fast-cache --features unsafe
cargo doc -p fast-cache --no-deps --all-features
cargo package -p fast-cache --locked
```

## Pull Requests

- Keep changes focused and explain behavior changes in the PR body.
- Add or update tests for protocol, storage, persistence, or compatibility
  changes.
- Update rustdoc or `crates/fast-cache/README.md` when public APIs change.
- Do not commit raw benchmark output, host snapshots, generated PDFs, local
  `.claude` config, or generated Python/Rust build artifacts.
- When publishing benchmark claims, include the command, date, machine shape,
  correctness gate, and raw result path or archive.

## Unsafe Code

The default build is safe by default. Any new unsafe hot path must:

- be gated behind the `unsafe` feature unless there is no safe alternative;
- include a local `SAFETY:` comment that states the invariant;
- have a safe fallback where practical;
- be covered by tests in both default and `--features unsafe` builds;
- update `crates/fast-cache/SAFETY.md`.
