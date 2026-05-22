# Changelog

## 0.2.0 - Unreleased

### Changed

- Split the public facade crate from `fast-cache-core` so downstream users can
  depend on the stable `fast-cache` entry point while core implementation code
  stays isolated.
- Moved Redis/Valkey compatibility source into `crates/fast-cache-redis/src`.
  The crate now depends on `fast-cache-core`; core still path-includes those
  files behind `redis-compat` until the extension API is complete.
- Simplified public feature defaults around the embedded sharded cache,
  `redis-compat`, and `redis-server`.
- Added transaction runtime coverage for `MULTI`, `EXEC`, `DISCARD`,
  `WATCH`, and `UNWATCH` behavior.
- Expanded Redis command coverage with `BITFIELD` and refreshed the tier-1
  compatibility ledger.

### Validation

- Workspace tests, Redis differential tests, rustdoc, core packaging, and
  formatting are part of the 0.2.0 release checklist.
- Redis tier-1 coverage is explicit: `DUMP` and `RESTORE` remain unsupported;
  `WATCH` and `UNWATCH` are intentionally partial.
