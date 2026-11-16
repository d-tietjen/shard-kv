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
two-byte size classes. Two bytes are sufficient for the free-list link stored
in a released record; payload access uses checked byte slices and has no larger
alignment requirement. Each buffer is a zero-initialized `Box<[u8; 4096]>`,
which fixes its allocation length and owns the payload until the chunk is
released. Growing the chunk descriptor or hash tables never
moves a chunk's payload. Empty chunks release their allocation and reuse their
descriptor; compact payload capacity is capped at 64 MiB per shard. A record
write copies at most 64 key bytes and 256 value bytes and can initialize one
4 KiB chunk. Normal descriptor hash-table resizing remains proportional to
its descriptor count and does not copy stored key/value payloads.

On 64-bit targets the per-key descriptor remains 32 bytes. The fixed buffer
and per-chunk availability links, with a bounded `u16` class index, are expected
to reduce the chunk descriptor from 56 to 24 bytes; regression checks enforce
those size limits. The 160 size-class list heads occupy 640 inline bytes per
shard, 320 fewer than the earlier 40 vector headers, and need no separate
allocations.
Availability membership, insertion, and removal use a head comparison and at
most two neighboring chunk links; they never scan or relocate payloads.
Future allocation preference is internal and carries no API order guarantee.
Availability updates preserve the payload addresses of untouched records.
Finer rounding reduces padding within records but can create more partially
filled chunks when a workload uses many distinct lengths. The allocation cap,
record retirement, and reclaim budget still bound those costs; memory and
latency require fresh workload measurements.

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

## Opt-in Linux succession backing

The experimental `succession` feature uses mmap/munmap, memfd size seals and Unix
peer/descriptor calls independently of the default `unsafe` performance toggle.
It is limited to 64-bit Linux, exact qualified engine builds, a fixed available
virtual range and one active process. Ordinary heap-backed APIs are unchanged.

The integer-only control prefix is checked for format, geometry, build identity,
profile, backing identity and recipient/capability before native table access.
The remaining mapping contains Rust/hashbrown tables, a single-allocation region
allocator and bounded payload slots. Native metadata is not an untrusted-byte
format. `Staged::receive` requires trusted exact producer/consumer builds and no
competing raw writes or aliases. The build fingerprint is conservative refusal,
not an ABI standard or proof that hostile native structures are valid.
`MAP_FIXED_NOREPLACE` never overwrites existing mappings; there is no relocation.

Each region honors hashbrown's requested allocation size/alignment. Capacity is
reserved before exposure, with live quota at most half the table's insertion
capacity, allowing tombstone cleanup in place during deletion churn. Payload/free
list offsets stay within disjoint geometry checked against the 1 GiB bound. Safe
writes enforce key/value limits. One local reader-biased shard lock protects each
native table; a Value guard excludes mutation of its borrowed bytes. No shared
Rust lock, arbitrary object graph, vtable or global heap allocator is imported.

Cache preparation consumes the last active handle. The private Arc cannot become
unique until every other Cache and its borrows/mutations has dropped. The caller
first closes admission and returns workers; timeout returns the active handle.
Only then can prepare publish readiness. There is no stale Cache left to fence
on each point operation. Candidates expose no entry API before the generation
CAS. Abort racing activation has one winner; no Drop infers a global decision.
Native table storage is ManuallyDrop: final Mapping Drop only unmaps/closes its
local resources and never runs a destructor that could free another owner's data.

Protected process custody is explicit before creation/import and maintained
throughout backing lifetime. SO_PEERCRED, token binding and mode 0600 supplement
PR_SET_DUMPABLE=0; none sandboxes privileged host administrators or admitted native
code with writable access. Credential withdrawal cannot revoke a mapping. PIDs
come from supervisor child custody, and both services share the relevant host/PID/
time namespaces. Never fork active handles or read backing outside its lifecycle.

Format 4 requires MADV_DONTDUMP and MADV_DONTFORK on every local mapping before
exposure; the owned mapping is constructed before these calls so failures unmap
and close it. Close-on-exec protects created and received descriptors. DONTFORK
does not close inherited descriptors: trusted forked helpers remain in the
service's custody boundary until exec/close. Reject wrong kernel peers before
closing admission, retaining authentication again at transfer/import.

The full readiness receipt is HMAC-SHA256 authenticated, including the optional
MemoryPolicy::Locked contract. Verification uses constant-time verify_slice.
Credential storage is allocated before secret reads and zeroized by RAII on every
success/error path; this does not promise erasure of cryptographic library state
or unrelated application copies. Explicit Locked calls mlock for the full backing
at creation and after authenticated import, before readiness; failure releases
only that local mapping and permits pre-commit abort. Pageable remains default.
The protections and residency policy add no checks to ordinary point operations.

The caller must coordinate common admission and the single TLS/socket/cache/SQL
decision. After B can advance state, lost ACKs never authorize stale A to resume.
The API fails closed on ambiguous ownership; it does not recover torn in-place
writes, implement durable custody, or prove full TLS/encrypted-SQL handoff.
Proof-only arena hashing runs while quiescent and is not an import or snapshot
algorithm. See the README for exact limits and reproducible component/performance
checks. Different-base attachment remains unqualified.
