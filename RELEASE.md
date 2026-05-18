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
cargo fmt --all -- --check
cargo test -p fast-cache
cargo test -p fast-cache --features unsafe
cargo test -p fcnp-client-rs
cargo doc -p fast-cache --no-deps --all-features
cargo doc -p fcnp-client-rs --no-deps
cargo package -p fcnp-client-rs --locked
cargo package -p fast-cache --locked
```

For full release confidence, also run any Redis compatibility or performance
validation suites that support the release announcement. Keep raw artifacts
outside the public repository unless they have been intentionally curated.

## Publishing

```bash
cargo publish -p fcnp-client-rs --dry-run
cargo publish -p fast-cache --dry-run
cargo publish -p fcnp-client-rs
cargo publish -p fast-cache
```

Only publish after the dry run succeeds and the final changelog or performance
claims have been checked against source artifacts.
