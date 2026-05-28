# shardcache-runtime

Rust-native runtime that moves cached KV data from
[`shardcache`](../shardcache) into CPU or GPU destinations for model
serving.

## What it does

1. Streams session chunks out of `LocalEmbeddedStore`.
2. Picks direct host DMA or staged-copy transfer per chunk.
3. Submits copies into host RAM or GPU memory through a transfer engine.

The CPU owns lookup and scheduling. The GPU owns bulk data movement.

## Public API

Transfer engines and policy:

- `CpuTransferEngine`, `KvTransferEngine`
- `HostTransferPolicy`, `HostStagingPool`
- `CudaTransferEngine` (Linux + NVIDIA, `cuda` feature)

Serving connectors:

- `ServingKvConnector`, `SessionRestoreSpec`, `TransferAllocator`
- `RuntimeSessionTransfer`, `RuntimeTransferTarget`
- `CpuTransferTarget`, `GpuTransferTarget`, `PagedGpuTransferTarget`, `PagedGpuTransferPage`

vLLM-shaped layer:

- `VllmKvConnector`, `VllmRestoreRequest`, `VllmRestorePlan`, `VllmRestoreReport`
- `VllmTransferAllocator`, `VllmGpuAllocation`, `VllmConnectorLoadSpec`
- `VllmBlockAllocation`, `VllmRequestedPage`

Entry points:

- `stream_session_to_cpu(...)`
- `stream_session_to_engine(...)`

Experimental queue path behind the `gpu-direct-api` feature:

- `GpuDirectProxy` and queue/descriptor scaffolding

## CPU Restore Example

Use the CPU path when the destination is host memory or when building a serving
connector before wiring GPU allocations.

```rust,ignore
use shardcache_runtime::{
    stream_session_to_cpu, CpuTransferTarget, SessionRestoreSpec,
};

let spec = SessionRestoreSpec {
    session_id: b"request-7".to_vec(),
    chunks: vec![b"layer-0/page-0".to_vec()],
};
let mut target = CpuTransferTarget::with_capacity(1 << 20);

let report = stream_session_to_cpu(&store, &spec, &mut target)?;
assert_eq!(report.missing_chunks, 0);
```

## vLLM-Shaped Restore Example

The vLLM layer builds an explicit restore plan before moving bytes, so
connectors can inspect which blocks will be loaded.

```rust,ignore
use shardcache_runtime::{VllmKvConnector, VllmRestoreRequest};

let connector = VllmKvConnector::new(store);
let request = VllmRestoreRequest::for_session("request-7");
let plan = connector.plan_restore(&request)?;

if !plan.blocks.is_empty() {
    let report = connector.restore(plan, allocator)?;
    assert_eq!(report.failed_blocks, 0);
}
```

## CUDA path

With `--features cuda` on Linux, the runtime uses `cust` for:

- primary context management
- non-blocking CUDA streams
- pinned host staging buffers

Plus a narrow raw-driver fallback for direct host DMA
(`cuMemHostRegister_v2`) and async host-to-device copies
(`cuMemcpyHtoDAsync_v2`).

The direct-DMA restore path runs on a single CUDA stream by default.
Staging remains multi-stream configurable.

## Features

- `embedded`, `sharded` (default): forwarded to `shardmap`.
- `cuda`: real CUDA engine on Linux + NVIDIA via `cust`.
- `gpu-direct-api`: experimental queue-based GPU-originated request track.

## License

Apache-2.0.
