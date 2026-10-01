# Safety

`shardcache` keeps a small number of reviewed `unsafe` hot paths for the server
and embedded storage APIs. The default build uses conservative locked, owned, or
slice-based alternatives. The `unsafe` feature opts into the lower-overhead hot
paths.

```bash
cargo test -p shardmap
cargo test -p shardmap --features unsafe
cargo clippy -p shardmap --features unsafe,experimental-no-ttl-point-hot-path,telemetry,server --all-targets --no-deps
```

## Unsafe Inventory

| Area | Why it uses `unsafe` | Required invariant | Safe alternative |
| --- | --- | --- | --- |
| `server::FastWriteBatchIoVec` | Implements monoio vectored writes with raw `iovec` pointers when `unsafe` is enabled. | The offset is advanced only inside the stored vector bounds, and the `Bytes` owners outlive the write. | Default build uses ordinary async writes. |
| Server fast protocol codecs | Uses unaligned integer reads and direct writes into reserved `BytesMut` spare capacity when `unsafe` is enabled. | Callers check input lengths before reads and reserve the exact output capacity before setting length. | Default build uses checked slice copies and normal buffer extension. |
| `EmbeddedStore::*single_threaded*` | Bypasses `RwLock` with `data_ptr()` in the direct single-worker server path when `unsafe` is enabled. | Exactly one worker may access the store while this path is active. | Default build disables the direct lock-bypass route and falls back to locked/owned APIs. |
| `FlatValueMap` hot lookup/update | Uses unchecked vector indexing, unaligned short-key comparison, and in-place value replacement when `unsafe` is enabled. | Control slots contain valid entry indexes; compared slices are valid for their length; in-place replacement only happens with equal lengths and no active readers. | Default build uses checked indexing, normal slice equality, and allocates replacement values instead of mutating `Bytes` in place. |
| `WorkerLocalReadSlice` | Reconstructs a slice from a pointer for worker-local reads when `unsafe` is enabled. | The pointer comes from a slice tied to the caller's exclusive `&mut WorkerLocalEmbeddedStore` borrow or from owned `Bytes`; the `Rc` phantom keeps the type thread-local. | Default build copies local read slices into owned `Bytes`, so `as_slice()` does not call `from_raw_parts`. |

## Experimental compact point storage

`experimental-compact-point-storage` adds a safe, checked implementation for
small strings without TTL or other entry metadata. It does not enable the
`unsafe` feature. Payloads occupy fixed 4 KiB chunks with reusable records in
eight-byte size classes. Growing the chunk descriptor or hash tables never
moves a chunk's payload. Empty chunks release their allocation and reuse their
descriptor; compact payload capacity is capped at 64 MiB per shard. A record
write copies at most 64 key bytes and 256 value bytes and can initialize one
4 KiB chunk. Normal descriptor hash-table resizing remains proportional to
its descriptor count and does not copy stored key/value payloads.

Ordinary reads hold the map/shard borrow or lock. Equal-length overwrite reuses
its record only when no read epoch is active. During an epoch, overwrite or
delete retires the affected record together with any lazily materialized shared
owner; neither its arena slice nor a borrowed `Bytes` buffer is overwritten,
freed, or reused until all readers leave. The allocator reclaims at most 32 retired compact records per call after
quiescence. A point mutation can attempt compact allocation and then fall back
to the general path, for a combined maximum of 64; maintenance reclaims 32. Owned `Bytes` clones
retain their independent old versions through normal reference counting.

Compact and general entries coexist with one representation per key. Changed
lengths allocate only a replacement record. Unsupported sizes, metadata, or
allocation-cap fallback migrate at most one compact key/value to general
storage. Counts, scans, snapshot/recovery, and runtime eviction/overflow policies
include both layouts. Runtime policy access samples use sparse per-record
metadata which is removed or transferred when the record changes. Small RESP
GET responses borrow slices and materialize an owner only when response lifetime
requires it. Supplied `Bytes` buffers without unique ownership remain in general
storage so caller-owned aliases retain the existing mutation contract. Raw
`value_mut_no_ttl` access rejects compact values with outstanding owned read
aliases before changing records or owners. Once those aliases drop, an active
read epoch still retains the old record and materialized owner during mutation.
Redis updates retain their bounded copy-on-write behavior. The feature remains
opt-in pending independent review and Adam memory and latency qualification.

## Anneal

[Anneal](https://github.com/google/zerocopy/tree/main/anneal) can provide a
machine-checked layer for selected unsafe contracts, but it is currently
pre-alpha and requires explicit `lean, anneal` doc blocks. Running Anneal
without those annotations does not prove the crate safe.

Install the toolchain first:

```bash
cargo install cargo-anneal@0.1.0-alpha.22 --locked
cargo anneal setup
```

Then add Anneal specs next to unsafe wrappers and run `cargo anneal` from a
checkout that contains the proof harness:

```bash
cargo anneal --allow-sorry
cargo anneal
```

Use `--allow-sorry` only while drafting proofs. A release-quality proof run
must pass without it.

### Current Anneal Coverage

The SCNP unaligned integer readers in `server::read_le_u32_at` and
`server::read_le_u64_at` are annotated with Anneal contracts:

- `read_le_u32_at`: requires `offset + 4 <= buf.len()`
- `read_le_u64_at`: requires `offset + 8 <= buf.len()`

The helpers are explicit `unsafe fn` boundaries, and each call site documents
the preceding protocol length check that satisfies the contract.

The current annotations are useful machine-readable contracts, but they are not
yet a complete machine proof of memory safety. The Rust safety argument is
still the explicit invariant: every production call site checks the frame or
field length before entering the unsafe reader, and the default build keeps a
checked slice-copy implementation available for differential testing.
