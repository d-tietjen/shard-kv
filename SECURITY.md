# Security policy

## Supported versions

`shard-kv` is pre-1.0. Fixes target the latest `main` branch and the
latest published crate version.

## Reporting a vulnerability

Report suspected vulnerabilities privately. Open a private security
advisory on GitHub, or email the maintainer listed in `Cargo.toml`.

Include:

- affected version or commit
- reproduction steps
- expected impact
- which build the issue affects (default, `unsafe`, server, persistence, or protocol parsing)

Do not open a public issue for an unpatched vulnerability.

## Unsafe code

The default build uses safe code. Reviewed performance paths that use
unsafe are opt-in through `--features unsafe` and documented in
[`crates/shardmap/SAFETY.md`](crates/shardmap/SAFETY.md).
