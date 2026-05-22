use fast_cache_core::cuda::{
    CudaChunkTransferDescriptor, CudaSessionChunkEvent, CudaSessionTransferRequest,
    CudaSessionTransferStats,
};
use fast_cache_core::storage::{LocalEmbeddedStore, LocalRouteError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::cpu_engine::CpuTransferEngine;
use crate::host::{
    HostStagingPool, HostTransferPath, HostTransferPolicy, RejectingStagingPool, StagedHostBuffer,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CpuTransferTarget {
    pub allocation_id: u64,
    pub dst_host_ptr: u64,
    pub dst_base_offset_bytes: u64,
}

impl CpuTransferTarget {
    #[inline(always)]
    pub fn absolute_offset_for(&self, descriptor: &CudaChunkTransferDescriptor) -> u64 {
        self.dst_base_offset_bytes
            .saturating_add(descriptor.dst_offset_bytes())
    }

    #[inline(always)]
    pub fn absolute_host_ptr_for(&self, descriptor: &CudaChunkTransferDescriptor) -> Option<usize> {
        let offset = self.absolute_offset_for(descriptor);
        (self.dst_host_ptr as usize).checked_add(offset as usize)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuTransferTarget {
    pub device_ordinal: usize,
    pub stream_ordinal: usize,
    pub allocation_id: u64,
    pub dst_device_ptr: u64,
    pub dst_base_offset_bytes: u64,
}

impl GpuTransferTarget {
    #[inline(always)]
    pub fn absolute_offset_for(&self, descriptor: &CudaChunkTransferDescriptor) -> u64 {
        self.dst_base_offset_bytes
            .saturating_add(descriptor.dst_offset_bytes())
    }

    #[inline(always)]
    pub fn absolute_device_ptr_for(
        &self,
        descriptor: &CudaChunkTransferDescriptor,
    ) -> Option<usize> {
        let offset = self.absolute_offset_for(descriptor);
        (self.dst_device_ptr as usize).checked_add(offset as usize)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PagedGpuTransferPage {
    pub page_index: usize,
    pub dst_device_ptr: u64,
    pub page_size_bytes: usize,
}

impl PagedGpuTransferPage {
    #[inline(always)]
    pub fn end_device_ptr(&self) -> Option<usize> {
        (self.dst_device_ptr as usize).checked_add(self.page_size_bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedGpuTransferSegment {
    pub page_index: usize,
    pub src_offset_bytes: usize,
    pub dst_device_ptr: usize,
    pub len_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PagedGpuTransferTarget {
    pub device_ordinal: usize,
    pub stream_ordinal: usize,
    pub allocation_id: u64,
    pub dst_base_offset_bytes: u64,
    pub pages: Vec<PagedGpuTransferPage>,
}

impl PagedGpuTransferTarget {
    #[inline(always)]
    pub fn absolute_offset_for(&self, descriptor: &CudaChunkTransferDescriptor) -> u64 {
        self.dst_base_offset_bytes
            .saturating_add(descriptor.dst_offset_bytes())
    }

    #[inline(always)]
    pub fn total_capacity_bytes(&self) -> usize {
        self.pages
            .iter()
            .map(|page| page.page_size_bytes)
            .sum::<usize>()
    }

    pub fn resolve_segments_for_descriptor(
        &self,
        descriptor: &CudaChunkTransferDescriptor,
        len_bytes: usize,
    ) -> RuntimeResult<Vec<ResolvedGpuTransferSegment>> {
        self.resolve_segments_for_range(self.absolute_offset_for(descriptor), len_bytes)
    }

    pub fn resolve_segments_for_range(
        &self,
        logical_offset_bytes: u64,
        len_bytes: usize,
    ) -> RuntimeResult<Vec<ResolvedGpuTransferSegment>> {
        let start = logical_offset_bytes as usize;
        let end = start.checked_add(len_bytes).ok_or_else(|| {
            RuntimeError::Engine("paged gpu target logical range overflow".into())
        })?;
        if end > self.total_capacity_bytes() {
            return Err(RuntimeError::Engine(format!(
                "paged gpu target range [{start}, {end}) exceeds capacity {}",
                self.total_capacity_bytes()
            )));
        }

        let mut segments = Vec::new();
        let mut remaining = len_bytes;
        let mut src_offset_bytes = 0usize;
        let mut logical_cursor = start;
        let mut page_base = 0usize;

        for page in &self.pages {
            let page_end = page_base.checked_add(page.page_size_bytes).ok_or_else(|| {
                RuntimeError::Engine("paged gpu target page range overflow".into())
            })?;
            if logical_cursor >= page_end {
                page_base = page_end;
                continue;
            }
            if logical_cursor < page_base {
                page_base = page_end;
                continue;
            }

            let in_page_offset = logical_cursor - page_base;
            let available = page.page_size_bytes - in_page_offset;
            let segment_len = available.min(remaining);
            let dst_device_ptr = (page.dst_device_ptr as usize)
                .checked_add(in_page_offset)
                .ok_or_else(|| {
                    RuntimeError::Engine("paged gpu target device pointer overflow".into())
                })?;
            segments.push(ResolvedGpuTransferSegment {
                page_index: page.page_index,
                src_offset_bytes,
                dst_device_ptr,
                len_bytes: segment_len,
            });
            remaining -= segment_len;
            src_offset_bytes += segment_len;
            logical_cursor += segment_len;
            if remaining == 0 {
                break;
            }
            page_base = page_end;
        }

        if remaining != 0 {
            return Err(RuntimeError::Engine(
                "paged gpu target did not fully resolve the requested range".into(),
            ));
        }
        Ok(segments)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RuntimeTransferTarget {
    Cpu(CpuTransferTarget),
    Gpu(GpuTransferTarget),
    PagedGpu(PagedGpuTransferTarget),
}

impl RuntimeTransferTarget {
    #[inline(always)]
    pub fn allocation_id(&self) -> u64 {
        match self {
            Self::Cpu(target) => target.allocation_id,
            Self::Gpu(target) => target.allocation_id,
            Self::PagedGpu(target) => target.allocation_id,
        }
    }

    #[inline(always)]
    pub fn absolute_offset_for(&self, descriptor: &CudaChunkTransferDescriptor) -> u64 {
        match self {
            Self::Cpu(target) => target.absolute_offset_for(descriptor),
            Self::Gpu(target) => target.absolute_offset_for(descriptor),
            Self::PagedGpu(target) => target.absolute_offset_for(descriptor),
        }
    }

    #[inline(always)]
    pub fn absolute_host_ptr_for(&self, descriptor: &CudaChunkTransferDescriptor) -> Option<usize> {
        match self {
            Self::Cpu(target) => target.absolute_host_ptr_for(descriptor),
            Self::Gpu(_) | Self::PagedGpu(_) => None,
        }
    }

    #[inline(always)]
    pub fn absolute_device_ptr_for(
        &self,
        descriptor: &CudaChunkTransferDescriptor,
    ) -> Option<usize> {
        match self {
            Self::Cpu(_) | Self::PagedGpu(_) => None,
            Self::Gpu(target) => target.absolute_device_ptr_for(descriptor),
        }
    }

    #[inline(always)]
    pub fn as_cpu(&self) -> Option<&CpuTransferTarget> {
        match self {
            Self::Cpu(target) => Some(target),
            Self::Gpu(_) | Self::PagedGpu(_) => None,
        }
    }

    #[inline(always)]
    pub fn as_gpu(&self) -> Option<&GpuTransferTarget> {
        match self {
            Self::Cpu(_) | Self::PagedGpu(_) => None,
            Self::Gpu(target) => Some(target),
        }
    }

    #[inline(always)]
    pub fn as_paged_gpu(&self) -> Option<&PagedGpuTransferTarget> {
        match self {
            Self::Cpu(_) | Self::Gpu(_) => None,
            Self::PagedGpu(target) => Some(target),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSessionTransfer {
    request: CudaSessionTransferRequest,
    target: RuntimeTransferTarget,
}

impl RuntimeSessionTransfer {
    pub fn new(request: CudaSessionTransferRequest, target: RuntimeTransferTarget) -> Self {
        Self { request, target }
    }

    #[inline(always)]
    pub fn new_cpu(request: CudaSessionTransferRequest, target: CpuTransferTarget) -> Self {
        Self::new(request, RuntimeTransferTarget::Cpu(target))
    }

    #[inline(always)]
    pub fn new_gpu(request: CudaSessionTransferRequest, target: GpuTransferTarget) -> Self {
        Self::new(request, RuntimeTransferTarget::Gpu(target))
    }

    #[inline(always)]
    pub fn new_paged_gpu(
        request: CudaSessionTransferRequest,
        target: PagedGpuTransferTarget,
    ) -> Self {
        Self::new(request, RuntimeTransferTarget::PagedGpu(target))
    }

    #[inline(always)]
    pub fn request(&self) -> &CudaSessionTransferRequest {
        &self.request
    }

    #[inline(always)]
    pub fn target(&self) -> &RuntimeTransferTarget {
        &self.target
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BorrowedKvChunk<'a> {
    descriptor: &'a CudaChunkTransferDescriptor,
    target: &'a RuntimeTransferTarget,
    bytes: &'a [u8],
    transfer_path: HostTransferPath,
}

impl<'a> BorrowedKvChunk<'a> {
    #[inline(always)]
    pub fn descriptor(&self) -> &'a CudaChunkTransferDescriptor {
        self.descriptor
    }

    #[inline(always)]
    pub fn target(&self) -> &'a RuntimeTransferTarget {
        self.target
    }

    #[inline(always)]
    pub fn bytes(&self) -> &'a [u8] {
        self.bytes
    }

    #[inline(always)]
    pub fn transfer_path(&self) -> HostTransferPath {
        self.transfer_path
    }

    #[inline(always)]
    pub fn absolute_offset_bytes(&self) -> u64 {
        self.target.absolute_offset_for(self.descriptor)
    }
}

#[derive(Debug)]
pub struct OwnedStagedKvChunk<'a> {
    descriptor: &'a CudaChunkTransferDescriptor,
    target: &'a RuntimeTransferTarget,
    buffer: StagedHostBuffer,
}

impl<'a> OwnedStagedKvChunk<'a> {
    #[inline(always)]
    pub fn descriptor(&self) -> &'a CudaChunkTransferDescriptor {
        self.descriptor
    }

    #[inline(always)]
    pub fn target(&self) -> &'a RuntimeTransferTarget {
        self.target
    }

    #[inline(always)]
    pub fn buffer(&self) -> &StagedHostBuffer {
        &self.buffer
    }

    #[inline(always)]
    pub fn into_buffer(self) -> StagedHostBuffer {
        self.buffer
    }

    #[inline(always)]
    pub fn bytes(&self) -> &[u8] {
        self.buffer.as_slice()
    }

    #[inline(always)]
    pub fn absolute_offset_bytes(&self) -> u64 {
        self.target.absolute_offset_for(self.descriptor)
    }
}

#[derive(Debug)]
pub enum RuntimeTransferChunk<'a> {
    Direct(BorrowedKvChunk<'a>),
    Staged(OwnedStagedKvChunk<'a>),
}

impl<'a> RuntimeTransferChunk<'a> {
    #[inline(always)]
    pub fn descriptor(&self) -> &'a CudaChunkTransferDescriptor {
        match self {
            Self::Direct(chunk) => chunk.descriptor(),
            Self::Staged(chunk) => chunk.descriptor(),
        }
    }

    #[inline(always)]
    pub fn target(&self) -> &'a RuntimeTransferTarget {
        match self {
            Self::Direct(chunk) => chunk.target(),
            Self::Staged(chunk) => chunk.target(),
        }
    }

    #[inline(always)]
    pub fn bytes(&self) -> &[u8] {
        match self {
            Self::Direct(chunk) => chunk.bytes(),
            Self::Staged(chunk) => chunk.bytes(),
        }
    }

    #[inline(always)]
    pub fn transfer_path(&self) -> HostTransferPath {
        match self {
            Self::Direct(chunk) => chunk.transfer_path(),
            Self::Staged(_) => HostTransferPath::PinnedStaging,
        }
    }

    #[inline(always)]
    pub fn absolute_offset_bytes(&self) -> u64 {
        match self {
            Self::Direct(chunk) => chunk.absolute_offset_bytes(),
            Self::Staged(chunk) => chunk.absolute_offset_bytes(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeSessionTransferSummary {
    pub requested_chunks: usize,
    pub hit_chunks: usize,
    pub missed_chunks: usize,
    pub transferred_bytes: usize,
}

impl From<CudaSessionTransferStats> for RuntimeSessionTransferSummary {
    fn from(value: CudaSessionTransferStats) -> Self {
        Self {
            requested_chunks: value.requested_chunks,
            hit_chunks: value.hit_chunks,
            missed_chunks: value.missed_chunks,
            transferred_bytes: value.transferred_bytes,
        }
    }
}

pub type RuntimeResult<T> = Result<T, RuntimeError>;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("local embedded route error: {0}")]
    LocalRoute(#[from] LocalRouteError),

    #[error("host staging error: {0}")]
    Staging(String),

    #[error("cuda runtime error: {0}")]
    Cuda(String),

    #[error("transfer engine error: {0}")]
    Engine(String),
}

pub trait KvTransferEngine {
    fn begin_transfer(&mut self, transfer: &RuntimeSessionTransfer) -> RuntimeResult<()>;

    fn submit_chunk(&mut self, chunk: RuntimeTransferChunk<'_>) -> RuntimeResult<()>;

    fn finish_transfer(
        &mut self,
        transfer: &RuntimeSessionTransfer,
        summary: RuntimeSessionTransferSummary,
    ) -> RuntimeResult<()>;
}

pub fn stream_session_to_engine<E>(
    store: &mut LocalEmbeddedStore,
    transfer: &RuntimeSessionTransfer,
    engine: &mut E,
) -> RuntimeResult<RuntimeSessionTransferSummary>
where
    E: KvTransferEngine,
{
    let summary = submit_session_to_engine(store, transfer, engine)?;
    engine.finish_transfer(transfer, summary)?;
    Ok(summary)
}

pub fn submit_session_to_engine<E>(
    store: &mut LocalEmbeddedStore,
    transfer: &RuntimeSessionTransfer,
    engine: &mut E,
) -> RuntimeResult<RuntimeSessionTransferSummary>
where
    E: KvTransferEngine,
{
    let policy = HostTransferPolicy::default();
    let mut no_staging = RejectingStagingPool;
    submit_session_to_engine_with_policy(store, transfer, engine, &policy, &mut no_staging)
}

pub fn stream_session_to_cpu(
    store: &mut LocalEmbeddedStore,
    request: CudaSessionTransferRequest,
    target: CpuTransferTarget,
) -> RuntimeResult<RuntimeSessionTransferSummary> {
    let transfer = RuntimeSessionTransfer::new_cpu(request, target);
    let mut engine = CpuTransferEngine::new();
    stream_session_to_engine(store, &transfer, &mut engine)
}

pub fn stream_session_to_engine_with_policy<E, P>(
    store: &mut LocalEmbeddedStore,
    transfer: &RuntimeSessionTransfer,
    engine: &mut E,
    policy: &HostTransferPolicy,
    staging_pool: &mut P,
) -> RuntimeResult<RuntimeSessionTransferSummary>
where
    E: KvTransferEngine,
    P: HostStagingPool,
{
    let summary =
        submit_session_to_engine_with_policy(store, transfer, engine, policy, staging_pool)?;
    engine.finish_transfer(transfer, summary)?;
    Ok(summary)
}

pub fn submit_session_to_engine_with_policy<E, P>(
    store: &mut LocalEmbeddedStore,
    transfer: &RuntimeSessionTransfer,
    engine: &mut E,
    policy: &HostTransferPolicy,
    staging_pool: &mut P,
) -> RuntimeResult<RuntimeSessionTransferSummary>
where
    E: KvTransferEngine,
    P: HostStagingPool,
{
    engine.begin_transfer(transfer)?;
    let request = transfer.request();
    let target = transfer.target();
    let stats = match store.stream_session_transfer_if_local(request, |event| match event {
        CudaSessionChunkEvent::Hit(hit) => {
            let bytes = hit.as_slice();
            let path = policy.path_for_len(bytes.len());
            let result = match path {
                HostTransferPath::DirectHostDma => {
                    engine.submit_chunk(RuntimeTransferChunk::Direct(BorrowedKvChunk {
                        descriptor: hit.descriptor(),
                        target,
                        bytes,
                        transfer_path: HostTransferPath::DirectHostDma,
                    }))
                }
                HostTransferPath::PinnedStaging => {
                    let staged = match staging_pool.stage(bytes) {
                        Ok(staged) => staged,
                        Err(error) => return std::ops::ControlFlow::Break(error),
                    };
                    engine.submit_chunk(RuntimeTransferChunk::Staged(OwnedStagedKvChunk {
                        descriptor: hit.descriptor(),
                        target,
                        buffer: staged,
                    }))
                }
            };
            result.map_or_else(std::ops::ControlFlow::Break, |_| {
                std::ops::ControlFlow::Continue(())
            })
        }
        CudaSessionChunkEvent::Miss(_) => std::ops::ControlFlow::Continue(()),
    })? {
        std::ops::ControlFlow::Continue(stats) => stats,
        std::ops::ControlFlow::Break(error) => return Err(error),
    };
    Ok(RuntimeSessionTransferSummary::from(stats))
}

#[cfg(test)]
mod tests {
    use fast_cache_core::cuda::{CudaChunkTransferDescriptor, CudaSessionTransferRequest};
    use fast_cache_core::storage::{EmbeddedRouteMode, EmbeddedStore, LocalEmbeddedStoreBootstrap};

    use crate::host::{HeapStagingPool, HostTransferPath, HostTransferPolicy};

    use super::{
        CpuTransferTarget, GpuTransferTarget, KvTransferEngine, PagedGpuTransferPage,
        PagedGpuTransferTarget, RuntimeError, RuntimeResult, RuntimeSessionTransfer,
        RuntimeSessionTransferSummary, RuntimeTransferChunk, RuntimeTransferTarget,
        stream_session_to_cpu, stream_session_to_engine, stream_session_to_engine_with_policy,
    };

    #[derive(Default)]
    struct RecordingEngine {
        chunks: Vec<(u32, Vec<u8>, u64, HostTransferPath)>,
        began: bool,
        finished: bool,
    }

    impl KvTransferEngine for RecordingEngine {
        fn begin_transfer(&mut self, _transfer: &RuntimeSessionTransfer) -> RuntimeResult<()> {
            self.began = true;
            Ok(())
        }

        fn submit_chunk(&mut self, chunk: RuntimeTransferChunk<'_>) -> RuntimeResult<()> {
            self.chunks.push((
                chunk.descriptor().layer_index(),
                chunk.bytes().to_vec(),
                chunk.absolute_offset_bytes(),
                chunk.transfer_path(),
            ));
            Ok(())
        }

        fn finish_transfer(
            &mut self,
            _transfer: &RuntimeSessionTransfer,
            _summary: RuntimeSessionTransferSummary,
        ) -> RuntimeResult<()> {
            self.finished = true;
            Ok(())
        }
    }

    #[test]
    fn stream_session_to_engine_preserves_order_and_offsets() {
        let store = EmbeddedStore::with_route_mode(2, EmbeddedRouteMode::SessionPrefix);
        let bootstrap = LocalEmbeddedStoreBootstrap::from_embedded(store, 2);
        let mut stores = bootstrap.into_stores();
        let mut local = stores.pop().expect("expected local store");

        let mut session = None;
        for index in 0..10_000usize {
            let candidate = format!("s:{index}").into_bytes();
            if local.session_is_local(&candidate) {
                session = Some(candidate);
                break;
            }
        }
        let session = session.expect("expected local session");

        local
            .batch_set_session_owned_no_ttl_if_local(
                session.clone(),
                vec![
                    (b"s:gpu:l:0".to_vec(), b"layer-0".to_vec()),
                    (b"s:gpu:l:1".to_vec(), b"layer-1".to_vec()),
                ],
            )
            .expect("local session write should work");

        let transfer = RuntimeSessionTransfer::new_gpu(
            CudaSessionTransferRequest::new(
                session,
                vec![
                    CudaChunkTransferDescriptor::new(b"s:gpu:l:0".to_vec(), 0, 0),
                    CudaChunkTransferDescriptor::new(b"s:gpu:l:1".to_vec(), 1, 256),
                    CudaChunkTransferDescriptor::new(b"s:gpu:l:2".to_vec(), 2, 512),
                ],
            ),
            GpuTransferTarget {
                device_ordinal: 0,
                stream_ordinal: 1,
                allocation_id: 7,
                dst_device_ptr: 0,
                dst_base_offset_bytes: 4096,
            },
        );

        let mut engine = RecordingEngine::default();
        let summary = stream_session_to_engine(&mut local, &transfer, &mut engine)
            .expect("streaming transfer should succeed");

        assert!(engine.began);
        assert!(engine.finished);
        assert_eq!(summary.requested_chunks, 3);
        assert_eq!(summary.hit_chunks, 2);
        assert_eq!(summary.missed_chunks, 1);
        assert_eq!(
            summary.transferred_bytes,
            b"layer-0".len() + b"layer-1".len()
        );
        assert_eq!(
            engine.chunks,
            vec![
                (
                    0,
                    b"layer-0".to_vec(),
                    4096,
                    HostTransferPath::DirectHostDma
                ),
                (
                    1,
                    b"layer-1".to_vec(),
                    4352,
                    HostTransferPath::DirectHostDma
                ),
            ]
        );
    }

    #[test]
    fn engine_errors_break_the_stream() {
        struct FailingEngine;

        impl KvTransferEngine for FailingEngine {
            fn begin_transfer(&mut self, _transfer: &RuntimeSessionTransfer) -> RuntimeResult<()> {
                Ok(())
            }

            fn submit_chunk(&mut self, _chunk: RuntimeTransferChunk<'_>) -> RuntimeResult<()> {
                Err(RuntimeError::Engine("boom".into()))
            }

            fn finish_transfer(
                &mut self,
                _transfer: &RuntimeSessionTransfer,
                _summary: RuntimeSessionTransferSummary,
            ) -> RuntimeResult<()> {
                Ok(())
            }
        }

        let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
        let bootstrap = LocalEmbeddedStoreBootstrap::from_embedded(store, 1);
        let mut stores = bootstrap.into_stores();
        let mut local = stores.pop().expect("expected local store");
        let session = b"s:1".to_vec();
        local
            .batch_set_session_owned_no_ttl_if_local(
                session.clone(),
                vec![(b"s:gpu:l:0".to_vec(), b"layer-0".to_vec())],
            )
            .expect("session write should work");

        let transfer = RuntimeSessionTransfer::new_gpu(
            CudaSessionTransferRequest::new(
                session,
                vec![CudaChunkTransferDescriptor::new(
                    b"s:gpu:l:0".to_vec(),
                    0,
                    0,
                )],
            ),
            GpuTransferTarget {
                device_ordinal: 0,
                stream_ordinal: 0,
                allocation_id: 1,
                dst_device_ptr: 0,
                dst_base_offset_bytes: 0,
            },
        );

        let mut engine = FailingEngine;
        let err = stream_session_to_engine(&mut local, &transfer, &mut engine)
            .expect_err("engine error should escape");
        assert!(matches!(err, RuntimeError::Engine(message) if message == "boom"));
    }

    #[test]
    fn policy_can_stage_large_chunks_before_submit() {
        let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
        let bootstrap = LocalEmbeddedStoreBootstrap::from_embedded(store, 1);
        let mut stores = bootstrap.into_stores();
        let mut local = stores.pop().expect("expected local store");
        let session = b"s:1".to_vec();
        let large = vec![7u8; 8192];
        local
            .batch_set_session_owned_no_ttl_if_local(
                session.clone(),
                vec![(b"s:gpu:l:0".to_vec(), large.clone())],
            )
            .expect("session write should work");

        let transfer = RuntimeSessionTransfer::new_gpu(
            CudaSessionTransferRequest::new(
                session,
                vec![CudaChunkTransferDescriptor::new(
                    b"s:gpu:l:0".to_vec(),
                    0,
                    0,
                )],
            ),
            GpuTransferTarget {
                device_ordinal: 0,
                stream_ordinal: 0,
                allocation_id: 1,
                dst_device_ptr: 0,
                dst_base_offset_bytes: 0,
            },
        );

        let policy = HostTransferPolicy {
            prefer_direct_host_dma: true,
            pinned_staging_threshold_bytes: 4096,
        };
        let mut engine = RecordingEngine::default();
        let mut staging = HeapStagingPool::default();
        let summary = stream_session_to_engine_with_policy(
            &mut local,
            &transfer,
            &mut engine,
            &policy,
            &mut staging,
        )
        .expect("stream should succeed");

        assert_eq!(summary.hit_chunks, 1);
        assert_eq!(staging.staged_buffers(), 1);
        assert_eq!(staging.staged_bytes(), 8192);
        assert_eq!(engine.chunks.len(), 1);
        assert_eq!(engine.chunks[0].1, large);
        assert_eq!(engine.chunks[0].3, HostTransferPath::PinnedStaging);
    }

    #[test]
    fn stream_session_to_cpu_copies_into_host_buffer() {
        let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
        let bootstrap = LocalEmbeddedStoreBootstrap::from_embedded(store, 1);
        let mut stores = bootstrap.into_stores();
        let mut local = stores.pop().expect("expected local store");
        let session = b"s:1".to_vec();
        local
            .batch_set_session_owned_no_ttl_if_local(
                session.clone(),
                vec![
                    (b"s:gpu:l:0".to_vec(), b"abcd".to_vec()),
                    (b"s:gpu:l:1".to_vec(), b"wxyz".to_vec()),
                ],
            )
            .expect("session write should work");

        let request = CudaSessionTransferRequest::new(
            session,
            vec![
                CudaChunkTransferDescriptor::new(b"s:gpu:l:0".to_vec(), 0, 0).with_expected_len(4),
                CudaChunkTransferDescriptor::new(b"s:gpu:l:1".to_vec(), 1, 8).with_expected_len(4),
            ],
        );

        let mut dst = vec![0u8; 16];
        let summary = stream_session_to_cpu(
            &mut local,
            request,
            CpuTransferTarget {
                allocation_id: 99,
                dst_host_ptr: dst.as_mut_ptr() as u64,
                dst_base_offset_bytes: 0,
            },
        )
        .expect("cpu transfer should succeed");

        assert_eq!(summary.hit_chunks, 2);
        assert_eq!(&dst[0..4], b"abcd");
        assert_eq!(&dst[8..12], b"wxyz");
    }

    #[test]
    fn transfer_target_helpers_select_cpu_and_gpu_views() {
        let cpu = RuntimeTransferTarget::Cpu(CpuTransferTarget {
            allocation_id: 1,
            dst_host_ptr: 0x1000,
            dst_base_offset_bytes: 32,
        });
        let gpu = RuntimeTransferTarget::Gpu(GpuTransferTarget {
            device_ordinal: 2,
            stream_ordinal: 3,
            allocation_id: 4,
            dst_device_ptr: 0x2000,
            dst_base_offset_bytes: 64,
        });
        let descriptor = CudaChunkTransferDescriptor::new(b"k".to_vec(), 0, 8);

        assert_eq!(cpu.allocation_id(), 1);
        assert_eq!(cpu.absolute_host_ptr_for(&descriptor), Some(0x1000 + 40));
        assert_eq!(cpu.absolute_device_ptr_for(&descriptor), None);
        assert!(cpu.as_cpu().is_some());
        assert!(cpu.as_gpu().is_none());

        assert_eq!(gpu.allocation_id(), 4);
        assert_eq!(gpu.absolute_device_ptr_for(&descriptor), Some(0x2000 + 72));
        assert_eq!(gpu.absolute_host_ptr_for(&descriptor), None);
        assert!(gpu.as_cpu().is_none());
        assert!(gpu.as_gpu().is_some());
    }

    #[test]
    fn paged_gpu_target_resolves_scatter_segments() {
        let target = PagedGpuTransferTarget {
            device_ordinal: 0,
            stream_ordinal: 0,
            allocation_id: 7,
            dst_base_offset_bytes: 2,
            pages: vec![
                PagedGpuTransferPage {
                    page_index: 0,
                    dst_device_ptr: 0x1000,
                    page_size_bytes: 4,
                },
                PagedGpuTransferPage {
                    page_index: 1,
                    dst_device_ptr: 0x2000,
                    page_size_bytes: 4,
                },
            ],
        };
        let descriptor = CudaChunkTransferDescriptor::new(b"k".to_vec(), 0, 1);

        let segments = target
            .resolve_segments_for_descriptor(&descriptor, 5)
            .expect("segments should resolve");

        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].page_index, 0);
        assert_eq!(segments[0].src_offset_bytes, 0);
        assert_eq!(segments[0].dst_device_ptr, 0x1000 + 3);
        assert_eq!(segments[0].len_bytes, 1);
        assert_eq!(segments[1].page_index, 1);
        assert_eq!(segments[1].src_offset_bytes, 1);
        assert_eq!(segments[1].dst_device_ptr, 0x2000);
        assert_eq!(segments[1].len_bytes, 4);
    }
}
