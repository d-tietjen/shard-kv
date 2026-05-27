# fast-cache-runtime

Rust-native runtime that moves cached KV data from
[`fast-cache`](../fast-cache) into CPU or GPU destinations for model
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

- `embedded`, `sharded` (default): forwarded to `fast-cache-core`.
- `cuda`: real CUDA engine on Linux + NVIDIA via `cust`.
- `gpu-direct-api`: experimental queue-based GPU-originated request track.

## License

Apache-2.0.
