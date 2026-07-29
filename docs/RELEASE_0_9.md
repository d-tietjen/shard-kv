# shard-kv 0.9

Version 0.9 narrows the public project to its local storage, protocol,
persistence, overflow, and compatibility surfaces.

## Breaking changes

- The distributed-availability feature flags, configuration fields, runtime
  types, transports, examples, and protocol modules are no longer part of the
  public crates.
- `ShardCacheConfig` no longer accepts a distributed replication section.
- Applications upgrading from 0.8 must remove those feature selections and
  configuration fields before compiling against 0.9.
- The minimum supported Rust version is now 1.93. The optional parent
  telemetry runtime uses `fast-telemetry 0.9.0`, allowing external extensions
  to share one exact runtime type without an adapter.

The default and opt-in public storage paths retain their existing local
semantics. Persistence remains the local crash-recovery mechanism. KV overflow
continues to provide capacity-tiering behavior and is not a database failover
mode.

## Public extension boundary

`EmbeddedStore` exposes a small, runtime-neutral extension boundary for
precomputed routing, mutation admission/observation, ordered in-lock callbacks,
and bounded snapshots. These contracts contain no membership, consensus,
failover, transport, or availability policy.

## Upgrade validation

Run the same checks used by the repository:

```bash
cargo check --workspace --all-targets
cargo test --workspace
./scripts/check-feature-matrix.sh
./scripts/check-publish-artifacts.sh
```
