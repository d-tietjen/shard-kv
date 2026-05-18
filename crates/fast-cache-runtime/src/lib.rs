#![doc = include_str!("../README.md")]

mod cuda_ffi;

pub mod connector;
pub mod cpu_engine;
#[cfg(feature = "cuda")]
pub mod cuda_engine;
#[cfg(feature = "gpu-direct-api")]
pub mod gpu_direct_api;
#[cfg(feature = "gpu-direct-api")]
pub mod gpu_direct_proxy;
#[cfg(feature = "gpu-direct-api")]
pub mod gpu_hot_tier;
#[cfg(feature = "gpu-direct-api")]
pub mod gpu_queue;
pub mod host;
pub mod runtime;
pub mod serving;
#[cfg(test)]
pub(crate) mod test_support;
pub mod vllm;
pub mod vllm_connector;

pub use connector::{
    ConnectorTransferHandle, ConnectorTransferReport, DirectKvConnector, TransferBackend,
    TransferDestination,
};
pub use cpu_engine::CpuTransferEngine;
#[cfg(feature = "cuda")]
pub use cuda_engine::CudaTransferEngine;
#[cfg(all(feature = "cuda", target_os = "linux"))]
pub use cuda_engine::PendingCudaTransfer;
#[cfg(feature = "gpu-direct-api")]
pub use gpu_direct_api::{
    GPU_DIRECT_API_V0_PATH, GpuDirectApiVersion, GpuDirectPathSelection, HOST_DIRECT_V1_PATH,
};
#[cfg(feature = "gpu-direct-api")]
pub use gpu_direct_proxy::{GpuDirectProxy, GpuDirectProxyConfig};
#[cfg(feature = "gpu-direct-api")]
pub use gpu_hot_tier::GpuHotTierLookupResult;
#[cfg(feature = "gpu-direct-api")]
pub use gpu_queue::{
    GpuDirectCompletionStatus, GpuDirectLookupCompletion, GpuDirectLookupRequest,
    GpuDirectQueueConfig, GpuDirectRequestFlags,
};
#[cfg(all(feature = "cuda", target_os = "linux"))]
pub use host::CudaPinnedStagingPool;
pub use host::{
    HeapStagingPool, HostStagingPool, HostTransferPath, HostTransferPolicy, RejectingStagingPool,
    StagedHostBuffer,
};
pub use runtime::{
    BorrowedKvChunk, CpuTransferTarget, GpuTransferTarget, KvTransferEngine, OwnedStagedKvChunk,
    PagedGpuTransferPage, PagedGpuTransferTarget, ResolvedGpuTransferSegment, RuntimeError,
    RuntimeResult, RuntimeSessionTransfer, RuntimeSessionTransferSummary, RuntimeTransferChunk,
    RuntimeTransferTarget, stream_session_to_cpu, stream_session_to_engine,
    stream_session_to_engine_with_policy, submit_session_to_engine,
    submit_session_to_engine_with_policy,
};
pub use serving::{
    FixedBufferAllocator, PlannedRestore, PreferredTransferBackend, RestoreChunkSpec,
    ServingKvConnector, ServingRestoreHandle, SessionRestoreSpec, TransferAllocator,
};
pub use vllm::{
    VllmGpuAllocation, VllmKvConnector, VllmPagedChunkSpec, VllmPlannedPage, VllmRestoreHandle,
    VllmRestorePlan, VllmRestoreReport, VllmRestoreRequest, VllmTransferAllocator,
};
pub use vllm_connector::{
    VllmBlockAllocation, VllmConnectorLoadSpec, VllmRequestedPage, VllmTranslatedRestore,
};
