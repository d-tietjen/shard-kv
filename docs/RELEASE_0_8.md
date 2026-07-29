# Shardcache 0.8 Feature Guide

Shardcache 0.8 added typed vector clients, fail-closed vector governance, and
hardened object-overflow behavior. This historical guide covers only the
capabilities that remain in the public 0.9 codebase.

## Feature Catalog

| Capability | Description | Enable With |
| --- | --- | --- |
| Typed vector client | Sends bounded native `PING`, `VADD`, `VSIM`, and `VREM` requests over fanout or direct shard-0 SCNP connections. | `shardcache-client-rs` `vector` feature |
| Vector governance | Stores opaque per-embedding governance metadata and fails closed when a caller cannot present the required label. | `shardmap` `redis` feature |
| Canonical vector persistence | Preserves vector state, TTLs, governance, and pinned routing through local snapshots and restore. | `shardmap` `redis` plus persistence |
| Hardened object overflow | Materializes cold values transparently, reports backend failures through fallible APIs and server errors, and provides bounded filesystem/S3 cleanup and health accounting. | `object-overflow` or `object-overflow-s3` |

## Object Overflow Correctness

Object overflow is a capacity tier rather than authoritative recovery state.
Local WAL and snapshots remain authoritative. A cold point GET fetches and
verifies the remote payload on the first request, releases the shard lock
during I/O, and promotes only when the remote reference still names the current
key generation. `try_get_value_bytes`, governed fallible GET, snapshots, and
entry visitors return errors when the payload cannot be read; RESP and SCNP
return an explicit backend error instead of `NULL`.

Filesystem storage uses descriptor-relative no-follow traversal on Unix and
bounded reads. Generation cleanup validates marker identity, performs one
bounded traversal per prefix, and refreshes heartbeats independently of cleanup
scans. S3/RustFS supports verified HTTPS with an optional private-CA PEM bundle
and emits configured SSE-S3 headers. Runtime replacement or disablement is
rejected while remote, pending, or faulting entries still depend on that
runtime. Entropy or required background-thread creation failures reject
startup.

## Vector Correctness And Governance

`VADD ... GOVERNANCE <bytes>` stores an opaque policy label on one embedding.
Governed embeddings fail closed: ordinary point reads appear missing, list and
similarity commands omit them, and `VADD`, `VREM`, and `VSETATTR` cannot mutate
them. Supply the exact label with `GOVERNANCE` for reads, search, or removal, or
`IFGOVERNANCE` for guarded `VADD`. A guarded `VADD` can rotate the label with
`GOVERNANCE <new>` or remove it with `CLEARGOVERNANCE`.

Typed SCNP clients use `VSimOptions::allow_governance`,
`VAddOptions::if_governance`, and `vrem_governed`.
`WITHGOVERNANCE` controls whether an already-authorized result includes its
label.

Metadata is limited to 64 KiB, survives canonical vector serialization, local
WAL and snapshots, and remains separate from JSON attributes. Exact label
matching is a storage-layer guard, not user authentication or a policy
language. Authenticate the connection and derive allowed labels in the service
authorization layer.

`DUMP` is rejected for governed vector sets so a client cannot bypass element
reads by exporting canonical state. Whole-key `DEL`, expiry, and eviction
remain available because governed vectors are still cache entries. Restrict
lifecycle and overwrite commands to trusted service identities; governance
labels do not replace connection ACLs or command authorization.

Governed HNSW search applies authorization only to graph candidates reached by
the configured search effort; it never falls back to an unbounded full scan.
Increase `EF` when approximate authorized recall is too low, or use `TRUTH` for
an exact bounded collection scan. A result is never substituted merely because
it is authorized.

## Public Release Boundaries

- Vector governance protects value release and mutation; it does not replace
  connection authentication, command authorization, or a policy engine.
- Object overflow remains a capacity tier. Use local WAL and snapshots for
  crash recovery.
- The 0.9 public crate surface excludes distributed deployment
  implementations. Applications provide those policies through the
  runtime-neutral extension contracts described in
  [`RELEASE_0_9.md`](RELEASE_0_9.md).

## Validation

The public release gate covers workspace formatting, Clippy, unit and
integration tests, Redis differential compatibility, feature-matrix builds,
packaged-crate consumer builds, rustdoc, and the OSS/private boundary audit.
