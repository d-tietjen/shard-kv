# Project Structure

This repository is organized as a Rust workspace with optional Python
integration packages and local benchmark tooling. The goal is to keep the
two published crate surfaces easy to find while leaving generated artifacts, raw
benchmark outputs, and local verification caches outside version control.

## Top-Level Map

| Path | Purpose |
| --- | --- |
| `crates/shardmap` | Published embedded cache crate. Core embedded cache, storage, protocol, persistence, replication, and opt-in server internals. Start here for base-cache behavior. |
| `crates/shardcache` | Source-only server package and optional `shardcache` binary. |
| `crates/shardcache-client-rs` | Published blocking Rust client for the native SCNP protocol and direct shard routing. |
| `crates/shardcache-runtime` | Source-only Rust-native CPU/GPU transfer runtime used by model-serving integrations. |
| `crates/shardcache-py` | Source-only PyO3 bindings used by benchmarks and integration adapters. |
| `crates/shardcache-redis` | Redis/Valkey compatibility crate and source root. Core includes these files by path only while the extension API is being finished. |
| `crates/shardcache-formal` | Formal-model support crate used with the verification workspace. |
| `benchmarks` | Local benchmark harnesses, reproduction scripts, and curated benchmark writeups. Raw run outputs live under ignored `benchmarks/results/`. |
| `integrations/lmcache_storage_backend` | Python LMCache storage backend package. |
| `integrations/vllm_direct_connector` | Python vLLM connector shim for the shardcache runtime path. |
| `docs` | Contributor-facing repository maps and design notes. |
| `scripts` | Release/proof gates and source-of-truth consistency checks. |
| `.github/workflows` | CI checks for formatting, tests, rustdoc, packaging, and repository hygiene. |
| `.cargo/config.toml` | Local cargo aliases for native CPU benchmark/server builds. |
| `why3find.json` | Tracked prover settings for Creusot/Why3 verification; generated solver caches stay ignored. |

## Core Crate Layout

Most production code lives under `crates/shardmap/src`:

| Path | Purpose |
| --- | --- |
| `cache.rs` and `embedded.rs` | Public embedded cache handles and convenience API surface. |
| `storage/` | Sharded storage engines, object stores, record layout, stats, and embedded store implementations. |
| `commands/` | Base cache command implementations for GET/SET/DEL/TTL-style behavior. Redis-only command families live in `crates/shardcache-redis/src/commands`. |
| `protocol/` | RESP and native fast protocol codecs. |
| `server/` | TCP listeners, direct shard routing, connection lifecycle, and request execution. |
| `persistence/` | WAL, snapshots, recovery, and TCP WAL export. |
| `replication/` | Native replication protocol, backlog, transport, and batching. |
| `config/` | TOML configuration, geometry, and validation. |
| `crates/shardcache/src/main.rs` | Server entry point for the source-only `shardcache` package. |
| `tests/` | Integration tests for storage, protocol, persistence, server, and compatibility behavior. |
| `fuzz/` | LibFuzzer harnesses for command-sequence validation. |

## Where To Make Changes

| Change | Primary Location | Also Check |
| --- | --- | --- |
| Embedded key/value API | `crates/shardmap/src/cache.rs`, `embedded.rs`, `storage/` | Core README, rustdoc, storage tests |
| Redis/Valkey command | `crates/shardcache-redis/src/commands/<family>/` | `crates/shardmap/src/commands/README.md`, compatibility tests, server dispatch |
| RESP or SCNP behavior | `crates/shardmap/src/protocol/`, `server/`, `crates/shardcache-client-rs` | Protocol tests and client README |
| Server configuration | `crates/shardmap/src/config/`, `shardcache.toml.example` | Root README Docker/config sections |
| Persistence or replication | `crates/shardmap/src/persistence/`, `replication/` | Recovery tests, benchmark caveats, config docs |
| Benchmark harness | `benchmarks/src`, `benchmarks/scripts` | `benchmarks/README.md` and curated benchmark writeups |
| Python integration | `integrations/<package>` and `crates/shardcache-py` | Package README and integration tests |

## Repository Hygiene

Keep source-controlled files limited to code, docs, test fixtures, package
metadata, and curated benchmark writeups. The following are intentionally
ignored and rejected by CI if tracked:

- build outputs such as `target/`, Python `build/`, `dist/`, and extension
  modules;
- local benchmark outputs under `benchmarks/results/`;
- runtime data under `var/` or `tmp/`;
- formal-verification caches such as `.why3find/` and local `verif/` output;
- host artifacts such as `.DS_Store`, rustc ICE reports, perf data, traces,
  generated PDFs, spreadsheets, and LaTeX intermediates.

When adding a new generated artifact class, update `.gitignore`,
`.dockerignore`, and the `generated artifacts stay out of git` CI check
together so the local workflow, Docker build context, and pull-request checks
stay aligned.

## Open Source Entry Points

New contributors should be able to answer the first set of questions from:

- `README.md`: project overview and links to the four public docs.
- `crates/shardmap/README.md`: published embedded crate guide.
- `crates/shardcache-client-rs/README.md`: published native Rust client guide.
- `docs/SHARDCACHE_DOCKER.md`: source-built server and Docker runbook.
- `integrations/lmcache_storage_backend/README.md`: LMCache storage backend guide.
- `crates/shardcache/README.md`: source-only server package notes.
- `crates/shardmap/SAFETY.md`: reviewed unsafe inventory and invariants.
- `CONTRIBUTING.md`: setup, pull-request expectations, and verification
  commands.
- `SECURITY.md`: vulnerability reporting.
- `RELEASE.md`: release checklist.
- `docs/RELEASE_0_1_READINESS.md`: current 0.1.0 proof, benchmark, and known
  limitation checklist.
- `docs/REDIS_COMPATIBILITY.md`: generated Redis command compatibility
  manifest based on the live command matrix registry, including supported
  commands, expected-error standalone behavior, and missing-command tracking.
- `docs/PROOF_GATES.md`: local and CI proof gates.
- `docs/OPERATIONS.md`: server build, startup, configuration, and benchmark
  artifact guidance.
