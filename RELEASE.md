# Release Checklist

Use this checklist before publishing the repository or the crates.io crates.

## Repository Hygiene

- `git status -sb` shows only intended changes.
- `git ls-files` contains no raw `results/` directories, `.claude` files,
  `.DS_Store`, build products, generated LaTeX sidecars, generated PDFs, or
  local logs.
- Public performance claims point to curated summaries or archived artifacts,
  not unreviewed raw host captures.
- Public documentation lives in rustdoc, `README.md`, and crate READMEs;
  private reports and integration experiments stay outside the public tree.
- `README.md`, `CONTRIBUTING.md`, `SECURITY.md`, and `LICENSE` are present.

## Validation

```bash
./scripts/proof-gate.sh release
```

For full release confidence, also run any Redis compatibility or performance
validation suites that support the release announcement. Keep raw artifacts
outside the public repository unless they have been intentionally curated.
For the 0.6.0 overflow architecture, operating contract, known limits, security
requirements, and benchmark commands, see `docs/KV_OVERFLOW.md`.

The 0.6.0 release gate also includes:

```bash
cargo fmt --check
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
./scripts/check-feature-matrix.sh
./scripts/check-publish-artifacts.sh
```

Run the filesystem-backed KV/object-overflow integration tests for every
release. The RustFS/S3 smoke test remains ignored and environment-gated; run it
against the release endpoint before advertising that backend for a production
deployment. Confirm the all-feature dependency tree contains no `openssl-sys`,
`openssl`, or `native-tls` packages.

Use `./benchmarks/scripts/run-redis-command-benchmark-bundle.sh` for command
matrix performance proofs so each run carries metadata, raw CSV, Markdown,
JSON, and compatibility-manifest artifacts together.

Docker Compose is currently a local/private deployment path only. Do not add a
Docker Hub or remote registry publish step until the compatibility surface and
release policy explicitly call for it.

## Publishing

```bash
cargo publish -p shardmap --dry-run
cargo publish -p shardcache-client-rs --dry-run
cargo publish -p shardcache --dry-run
cargo publish -p shardmap
cargo publish -p shardcache-client-rs
cargo publish -p shardcache
```

`shardmap`, `shardcache-client-rs`, and `shardcache` are the crates.io crates
for this release. `shardcache-runtime`, `shardcache-py`, `shardcache-c`, and
`shardcache-formal` are workspace support packages with `publish = false`.
Only publish after the dry runs succeed and the final changelog or performance
claims have been checked against source artifacts.
