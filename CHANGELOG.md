# Changelog

## 0.9.0 - 2026-07-27

### Changed

- Narrowed the public workspace to local storage, persistence, overflow,
  protocol, compatibility, and benchmark surfaces.
- Added runtime-neutral embedded-store extension contracts for routed
  operations, mutation admission and observation, ordered callbacks, and
  bounded snapshots.
- Aligned the optional parent telemetry runtime with `fast-telemetry 0.9.0`;
  the 0.9 public crates now require Rust 1.93.

### Removed

- Removed non-OSS distributed deployment implementations, feature flags,
  configuration, transports, examples, tests, formal models, documentation,
  and benchmark drivers from the public crates.

## 0.8.1 - 2026-07-23

### Security

- Re-keyed process-local maps and raw tables with per-process randomized AHash
  before bucket selection. Stable XXH3 key routing and exact-key comparisons
  remain unchanged, while authenticated clients that can choose keys can no
  longer precompute colliding local buckets from the public routing hash.
  Redis-compatible XXH3 `DIGEST` and `IFDEQ`/`IFDNE` tokens remain unchanged;
  they are fast comparison hints, not authorization or cryptographic integrity
  primitives.

## 0.8.0 - 2026-07-20

### Added

- Added typed vector operations, bounded request/response decoding, vector
  governance metadata, fail-closed governance guards, and reproducible Object
  RAG benchmarks.
- Added canonical vector snapshot and restore handling, TTL preservation,
  bounded HNSW search, and malformed-state rejection.

### Fixed

- Hardened object-overflow startup, cleanup, materialization, snapshots,
  filesystem access, S3 private-CA handling, and benchmark acknowledgement.
- Corrected vector routing, TTL preservation, governance filtering, response
  encoding, UID allocation, and non-finite input validation.
