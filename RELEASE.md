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
For the current 0.2.0 release shape, known limits, and smoke benchmark command,
see `docs/RELEASE_0_2_READINESS.md`.

Use `./benchmarks/scripts/run-redis-command-benchmark-bundle.sh` for command
matrix performance proofs so each run carries metadata, raw CSV, Markdown,
JSON, and compatibility-manifest artifacts together.

## Publishing

```bash
cargo publish -p fcnp-client-rs --dry-run
cargo publish -p fast-cache-core --dry-run
cargo publish -p fcnp-client-rs
cargo publish -p fast-cache-core
cargo publish -p fast-cache --dry-run
cargo publish -p fast-cache
```

Publish `fast-cache-core` before dry-running or publishing the public
`fast-cache` facade so the facade's registry dependency can resolve. Only
publish after the dry run succeeds and the final changelog or performance
claims have been checked against source artifacts.
