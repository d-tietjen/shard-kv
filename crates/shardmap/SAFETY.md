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
