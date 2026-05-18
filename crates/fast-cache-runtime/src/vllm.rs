use fast_cache::cuda::CudaConfig;
use fast_cache::storage::{Bytes, LocalEmbeddedStore};

use crate::connector::{ConnectorTransferReport, TransferBackend};
use crate::runtime::{RuntimeResult, RuntimeSessionTransferSummary};
use crate::serving::{
    PlannedRestore, PreferredTransferBackend, RestoreChunkSpec, ServingKvConnector,
    SessionRestoreSpec, TransferAllocator,
};
use crate::{CpuTransferTarget, GpuTransferTarget, PagedGpuTransferTarget, TransferDestination};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VllmPagedChunkSpec {
    pub key: Bytes,
    pub layer_index: u32,
    pub page_index: u32,
    pub dst_offset_bytes: u64,
    pub expected_len: Option<usize>,
}

impl VllmPagedChunkSpec {
    pub fn new<K>(key: K, layer_index: u32, page_index: u32, dst_offset_bytes: u64) -> Self
    where
        K: Into<Bytes>,
    {
        Self {
            key: key.into(),
            layer_index,
            page_index,
            dst_offset_bytes,
            expected_len: None,
        }
    }

    #[inline(always)]
    pub fn with_expected_len(mut self, expected_len: usize) -> Self {
        self.expected_len = Some(expected_len);
        self
    }

    #[inline(always)]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    #[inline(always)]
    pub fn layer_index(&self) -> u32 {
        self.layer_index
    }

    #[inline(always)]
    pub fn page_index(&self) -> u32 {
        self.page_index
    }

    #[inline(always)]
    pub fn dst_offset_bytes(&self) -> u64 {
        self.dst_offset_bytes
    }

    #[inline(always)]
    pub fn expected_len(&self) -> Option<usize> {
        self.expected_len
    }

    #[inline(always)]
    fn to_planned_page(&self) -> VllmPlannedPage {
        VllmPlannedPage {
            key: self.key.clone(),
            layer_index: self.layer_index,
            page_index: self.page_index,
            dst_offset_bytes: self.dst_offset_bytes,
            expected_len: self.expected_len,
        }
    }

    #[inline(always)]
    fn to_restore_chunk(&self) -> RestoreChunkSpec {
        let chunk =
            RestoreChunkSpec::new(self.key.clone(), self.layer_index, self.dst_offset_bytes);
        match self.expected_len {
            Some(expected_len) => chunk.with_expected_len(expected_len),
            None => chunk,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VllmRestoreRequest {
    pub session_prefix: Bytes,
    pub chunks: Vec<VllmPagedChunkSpec>,
    pub preferred_backend: PreferredTransferBackend,
    pub device_ordinal: usize,
    pub stream_ordinal: usize,
    pub allow_cpu_fallback: bool,
}

impl VllmRestoreRequest {
    pub fn new<S>(
        session_prefix: S,
        chunks: Vec<VllmPagedChunkSpec>,
        preferred_backend: PreferredTransferBackend,
    ) -> Self
    where
        S: Into<Bytes>,
    {
        Self {
            session_prefix: session_prefix.into(),
            chunks,
            preferred_backend,
            device_ordinal: 0,
            stream_ordinal: 0,
            allow_cpu_fallback: true,
        }
    }

    #[inline(always)]
    pub fn with_device_ordinal(mut self, device_ordinal: usize) -> Self {
        self.device_ordinal = device_ordinal;
        self
    }

    #[inline(always)]
    pub fn with_stream_ordinal(mut self, stream_ordinal: usize) -> Self {
        self.stream_ordinal = stream_ordinal;
        self
    }

    #[inline(always)]
    pub fn with_allow_cpu_fallback(mut self, allow_cpu_fallback: bool) -> Self {
        self.allow_cpu_fallback = allow_cpu_fallback;
        self
    }

    #[inline(always)]
    pub fn with_gpu_target(mut self, device_ordinal: usize, stream_ordinal: usize) -> Self {
        self.device_ordinal = device_ordinal;
        self.stream_ordinal = stream_ordinal;
        self.preferred_backend = PreferredTransferBackend::Gpu;
        self
    }

    #[inline(always)]
    pub fn with_preferred_backend(mut self, preferred_backend: PreferredTransferBackend) -> Self {
        self.preferred_backend = preferred_backend;
        self
    }

    #[inline(always)]
    pub fn push_page(&mut self, chunk: VllmPagedChunkSpec) {
        self.chunks.push(chunk);
    }

    #[inline(always)]
    pub fn session_prefix(&self) -> &[u8] {
        &self.session_prefix
    }

    #[inline(always)]
    pub fn preferred_backend(&self) -> PreferredTransferBackend {
        self.preferred_backend
    }

    #[inline(always)]
    pub fn pages(&self) -> &[VllmPagedChunkSpec] {
        &self.chunks
    }

    #[inline(always)]
    pub fn page_count(&self) -> usize {
        self.chunks.len()
    }

    #[inline(always)]
    pub fn total_expected_bytes(&self) -> Option<usize> {
        self.chunks
            .iter()
            .map(|chunk| chunk.expected_len)
            .try_fold(0usize, |sum, len| len.map(|len| sum.saturating_add(len)))
    }

    #[inline(always)]
    fn to_session_restore_spec(&self) -> SessionRestoreSpec {
        SessionRestoreSpec {
            session_prefix: self.session_prefix.clone(),
            chunks: self
                .chunks
                .iter()
                .map(VllmPagedChunkSpec::to_restore_chunk)
                .collect(),
            preferred_backend: self.preferred_backend,
            device_ordinal: self.device_ordinal,
            stream_ordinal: self.stream_ordinal,
            allow_cpu_fallback: self.allow_cpu_fallback,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VllmPlannedPage {
    key: Bytes,
    layer_index: u32,
    page_index: u32,
    dst_offset_bytes: u64,
    expected_len: Option<usize>,
}

impl VllmPlannedPage {
    #[inline(always)]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    #[inline(always)]
    pub fn layer_index(&self) -> u32 {
        self.layer_index
    }

    #[inline(always)]
    pub fn page_index(&self) -> u32 {
        self.page_index
    }

    #[inline(always)]
    pub fn dst_offset_bytes(&self) -> u64 {
        self.dst_offset_bytes
    }

    #[inline(always)]
    pub fn expected_len(&self) -> Option<usize> {
        self.expected_len
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VllmGpuAllocation {
    Contiguous(GpuTransferTarget),
    Paged(PagedGpuTransferTarget),
}

pub trait VllmTransferAllocator {
    fn allocate_cpu(
        &mut self,
        total_expected_bytes: Option<usize>,
    ) -> RuntimeResult<CpuTransferTarget>;

    fn allocate_gpu(
        &mut self,
        device_ordinal: usize,
        stream_ordinal: usize,
        pages: &[VllmPagedChunkSpec],
        total_expected_bytes: Option<usize>,
    ) -> RuntimeResult<VllmGpuAllocation>;
}

impl<T> VllmTransferAllocator for T
where
    T: TransferAllocator,
{
    fn allocate_cpu(
        &mut self,
        total_expected_bytes: Option<usize>,
    ) -> RuntimeResult<CpuTransferTarget> {
        TransferAllocator::allocate_cpu(self, total_expected_bytes)
    }

    fn allocate_gpu(
        &mut self,
        device_ordinal: usize,
        stream_ordinal: usize,
        _pages: &[VllmPagedChunkSpec],
        total_expected_bytes: Option<usize>,
    ) -> RuntimeResult<VllmGpuAllocation> {
        TransferAllocator::allocate_gpu(self, device_ordinal, stream_ordinal, total_expected_bytes)
            .map(VllmGpuAllocation::Contiguous)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VllmRestorePlan {
    inner: PlannedRestore,
    pages: Vec<VllmPlannedPage>,
    page_count: usize,
    total_expected_bytes: Option<usize>,
}

impl VllmRestorePlan {
    #[inline(always)]
    pub fn pages(&self) -> &[VllmPlannedPage] {
        &self.pages
    }

    #[inline(always)]
    pub fn page_count(&self) -> usize {
        self.page_count
    }

    #[inline(always)]
    pub fn total_expected_bytes(&self) -> Option<usize> {
        self.total_expected_bytes
    }

    #[inline(always)]
    pub fn preferred_backend(&self) -> PreferredTransferBackend {
        self.inner.preferred_backend
    }

    #[inline(always)]
    pub fn session_prefix(&self) -> &[u8] {
        self.inner.request.session_prefix()
    }

    #[inline(always)]
    pub fn gpu_target(&self) -> Option<&GpuTransferTarget> {
        match &self.inner.destination {
            TransferDestination::Gpu { target, .. } => Some(target),
            TransferDestination::Cpu(_) | TransferDestination::PagedGpu { .. } => None,
        }
    }

    #[inline(always)]
    pub fn cpu_target(&self) -> Option<&CpuTransferTarget> {
        match &self.inner.destination {
            TransferDestination::Cpu(target) => Some(target),
            TransferDestination::Gpu { .. } | TransferDestination::PagedGpu { .. } => None,
        }
    }

    #[inline(always)]
    pub fn paged_gpu_target(&self) -> Option<&PagedGpuTransferTarget> {
        match &self.inner.destination {
            TransferDestination::PagedGpu { target, .. } => Some(target),
            TransferDestination::Cpu(_) | TransferDestination::Gpu { .. } => None,
        }
    }

    #[inline(always)]
    pub fn cpu_fallback_target(&self) -> Option<&CpuTransferTarget> {
        match &self.inner.destination {
            TransferDestination::Gpu { cpu_fallback, .. } => cpu_fallback.as_ref(),
            TransferDestination::PagedGpu { cpu_fallback, .. } => cpu_fallback.as_ref(),
            TransferDestination::Cpu(_) => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VllmRestoreReport {
    backend: TransferBackend,
    summary: RuntimeSessionTransferSummary,
    page_count: usize,
    total_expected_bytes: Option<usize>,
}

impl VllmRestoreReport {
    #[inline(always)]
    pub fn backend(&self) -> TransferBackend {
        self.backend
    }

    #[inline(always)]
    pub fn summary(&self) -> RuntimeSessionTransferSummary {
        self.summary
    }

    #[inline(always)]
    pub fn page_count(&self) -> usize {
        self.page_count
    }

    #[inline(always)]
    pub fn total_expected_bytes(&self) -> Option<usize> {
        self.total_expected_bytes
    }

    #[inline(always)]
    pub fn hit_pages(&self) -> usize {
        self.summary.hit_chunks
    }

    #[inline(always)]
    pub fn missed_pages(&self) -> usize {
        self.summary.missed_chunks
    }

    #[inline(always)]
    pub fn transferred_bytes(&self) -> usize {
        self.summary.transferred_bytes
    }

    #[inline(always)]
    pub fn all_hit(&self) -> bool {
        self.summary.requested_chunks == self.summary.hit_chunks
    }
}

#[derive(Debug)]
pub struct VllmRestoreHandle {
    inner: crate::serving::ServingRestoreHandle,
    page_count: usize,
    total_expected_bytes: Option<usize>,
}

impl VllmRestoreHandle {
    #[inline(always)]
    pub fn is_pending(&self) -> bool {
        self.inner.is_pending()
    }

    pub fn is_ready(&mut self) -> RuntimeResult<bool> {
        self.inner.is_ready()
    }

    pub fn peek_report(&self) -> VllmRestoreReport {
        VllmKvConnector::attach_plan_metadata(
            self.inner.peek_report(),
            self.page_count,
            self.total_expected_bytes,
        )
    }

    pub fn wait_on_stream(&mut self, stream_ptr: u64) -> RuntimeResult<bool> {
        self.inner.wait_on_stream(stream_ptr)
    }

    pub fn wait(self) -> RuntimeResult<VllmRestoreReport> {
        let report = self.inner.wait()?;
        Ok(VllmKvConnector::attach_plan_metadata(
            report,
            self.page_count,
            self.total_expected_bytes,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct VllmKvConnector {
    serving: ServingKvConnector,
}

impl VllmKvConnector {
    pub fn new(cuda: CudaConfig) -> Self {
        Self {
            serving: ServingKvConnector::new(cuda),
        }
    }

    #[inline(always)]
    pub fn serving(&self) -> &ServingKvConnector {
        &self.serving
    }

    pub fn plan_restore<A>(
        &self,
        request: VllmRestoreRequest,
        allocator: &mut A,
    ) -> RuntimeResult<VllmRestorePlan>
    where
        A: VllmTransferAllocator,
    {
        let page_count = request.page_count();
        let total_expected_bytes = request.total_expected_bytes();
        let pages = request
            .chunks
            .iter()
            .map(VllmPagedChunkSpec::to_planned_page)
            .collect();
        let spec = request.to_session_restore_spec();
        let inner = self.plan_with_allocator(&request, spec, allocator)?;
        Ok(VllmRestorePlan {
            inner,
            pages,
            page_count,
            total_expected_bytes,
        })
    }

    pub fn plan_restore_to_gpu_allocation(
        &self,
        request: VllmRestoreRequest,
        gpu_allocation: VllmGpuAllocation,
        cpu_fallback: Option<CpuTransferTarget>,
    ) -> RuntimeResult<VllmRestorePlan> {
        self.plan_restore_to_destination(
            request,
            match gpu_allocation {
                VllmGpuAllocation::Contiguous(target) => TransferDestination::Gpu {
                    target,
                    cpu_fallback,
                },
                VllmGpuAllocation::Paged(target) => TransferDestination::PagedGpu {
                    target,
                    cpu_fallback,
                },
            },
        )
    }

    pub fn plan_restore_to_cpu_target(
        &self,
        request: VllmRestoreRequest,
        cpu_target: CpuTransferTarget,
    ) -> RuntimeResult<VllmRestorePlan> {
        self.plan_restore_to_destination(request, TransferDestination::Cpu(cpu_target))
    }

    pub fn plan_restore_to_destination(
        &self,
        request: VllmRestoreRequest,
        destination: TransferDestination,
    ) -> RuntimeResult<VllmRestorePlan> {
        let page_count = request.page_count();
        let total_expected_bytes = request.total_expected_bytes();
        let pages = request
            .chunks
            .iter()
            .map(VllmPagedChunkSpec::to_planned_page)
            .collect();
        let inner = PlannedRestore {
            request: request.to_session_restore_spec().to_request(),
            destination,
            preferred_backend: request.preferred_backend,
            total_expected_bytes,
        };
        Ok(VllmRestorePlan {
            inner,
            pages,
            page_count,
            total_expected_bytes,
        })
    }

    pub fn execute_restore(
        &self,
        store: &mut LocalEmbeddedStore,
        plan: VllmRestorePlan,
    ) -> RuntimeResult<VllmRestoreReport> {
        self.submit_restore(store, plan)?.wait()
    }

    pub fn submit_restore(
        &self,
        store: &mut LocalEmbeddedStore,
        plan: VllmRestorePlan,
    ) -> RuntimeResult<VllmRestoreHandle> {
        Ok(VllmRestoreHandle {
            inner: self.serving.submit_restore(store, plan.inner)?,
            page_count: plan.page_count,
            total_expected_bytes: plan.total_expected_bytes,
        })
    }

    pub fn wait_restore(&self, handle: VllmRestoreHandle) -> RuntimeResult<VllmRestoreReport> {
        handle.wait()
    }

    #[cfg(test)]
    pub(crate) fn execute_restore_with_gpu_engine<E>(
        &self,
        store: &mut LocalEmbeddedStore,
        plan: VllmRestorePlan,
        engine: &mut E,
    ) -> RuntimeResult<VllmRestoreReport>
    where
        E: crate::runtime::KvTransferEngine,
    {
        let page_count = plan.page_count;
        let total_expected_bytes = plan.total_expected_bytes;
        let report = self
            .serving
            .execute_restore_with_gpu_engine(store, plan.inner, engine)?;
        Ok(Self::attach_plan_metadata(
            report,
            page_count,
            total_expected_bytes,
        ))
    }

    pub fn restore<A>(
        &self,
        store: &mut LocalEmbeddedStore,
        request: VllmRestoreRequest,
        allocator: &mut A,
    ) -> RuntimeResult<VllmRestoreReport>
    where
        A: VllmTransferAllocator,
    {
        let plan = self.plan_restore(request, allocator)?;
        self.execute_restore(store, plan)
    }

    fn plan_with_allocator<A>(
        &self,
        request: &VllmRestoreRequest,
        spec: SessionRestoreSpec,
        allocator: &mut A,
    ) -> RuntimeResult<PlannedRestore>
    where
        A: VllmTransferAllocator,
    {
        let total_expected_bytes = spec.total_expected_bytes();
        let destination = match spec.preferred_backend {
            PreferredTransferBackend::Cpu => {
                TransferDestination::Cpu(allocator.allocate_cpu(total_expected_bytes)?)
            }
            PreferredTransferBackend::Gpu => {
                let gpu = allocator.allocate_gpu(
                    spec.device_ordinal,
                    spec.stream_ordinal,
                    request.pages(),
                    total_expected_bytes,
                )?;
                let cpu_fallback = if spec.allow_cpu_fallback {
                    Some(allocator.allocate_cpu(total_expected_bytes)?)
                } else {
                    None
                };
                match gpu {
                    VllmGpuAllocation::Contiguous(target) => TransferDestination::Gpu {
                        target,
                        cpu_fallback,
                    },
                    VllmGpuAllocation::Paged(target) => TransferDestination::PagedGpu {
                        target,
                        cpu_fallback,
                    },
                }
            }
        };

        Ok(PlannedRestore {
            request: spec.to_request(),
            destination,
            preferred_backend: spec.preferred_backend,
            total_expected_bytes,
        })
    }

    #[inline(always)]
    fn attach_plan_metadata(
        report: ConnectorTransferReport,
        page_count: usize,
        total_expected_bytes: Option<usize>,
    ) -> VllmRestoreReport {
        VllmRestoreReport {
            backend: report.backend,
            summary: report.summary,
            page_count,
            total_expected_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use fast_cache::cuda::CudaConfig;
    use fast_cache::storage::{EmbeddedRouteMode, EmbeddedStore, LocalEmbeddedStoreBootstrap};

    use super::{
        PreferredTransferBackend, VllmGpuAllocation, VllmKvConnector, VllmPagedChunkSpec,
        VllmRestoreRequest, VllmTransferAllocator,
    };
    use crate::connector::TransferBackend;
    use crate::runtime::{
        CpuTransferTarget, GpuTransferTarget, PagedGpuTransferPage, PagedGpuTransferTarget,
        RuntimeResult,
    };
    use crate::serving::FixedBufferAllocator;
    use crate::test_support::SimulatedGpuEngine;

    #[test]
    fn vllm_plan_preserves_gpu_stream_and_page_count() {
        let connector = VllmKvConnector::new(CudaConfig::default());
        let mut allocator = FixedBufferAllocator::new(
            Some(CpuTransferTarget {
                allocation_id: 1,
                dst_host_ptr: 0x1000,
                dst_base_offset_bytes: 0,
            }),
            Some(GpuTransferTarget {
                device_ordinal: 3,
                stream_ordinal: 5,
                allocation_id: 2,
                dst_device_ptr: 0x2000,
                dst_base_offset_bytes: 0,
            }),
        );

        let request = VllmRestoreRequest::new(
            b"s:1".to_vec(),
            vec![
                VllmPagedChunkSpec::new(b"k0".to_vec(), 0, 0, 0).with_expected_len(4),
                VllmPagedChunkSpec::new(b"k1".to_vec(), 1, 1, 8).with_expected_len(8),
            ],
            PreferredTransferBackend::Gpu,
        )
        .with_device_ordinal(3)
        .with_stream_ordinal(5);

        let plan = connector
            .plan_restore(request, &mut allocator)
            .expect("plan should succeed");

        assert_eq!(plan.page_count(), 2);
        assert_eq!(plan.total_expected_bytes(), Some(12));
        assert_eq!(plan.pages().len(), 2);
        assert_eq!(plan.pages()[1].page_index(), 1);
        assert_eq!(plan.preferred_backend(), PreferredTransferBackend::Gpu);
        assert_eq!(plan.session_prefix(), b"s:1");
        let gpu = plan.gpu_target().expect("expected gpu target");
        assert_eq!(gpu.device_ordinal, 3);
        assert_eq!(gpu.stream_ordinal, 5);
        assert!(plan.cpu_fallback_target().is_some());
    }

    #[test]
    fn vllm_restore_executes_through_serving_connector() {
        let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
        let bootstrap = LocalEmbeddedStoreBootstrap::from_embedded(store, 1);
        let mut stores = bootstrap.into_stores();
        let mut local = stores.pop().expect("expected local store");
        let session = b"s:1".to_vec();
        local
            .batch_set_session_owned_no_ttl_if_local(
                session.clone(),
                vec![(b"s:gpu:l:0:p:0".to_vec(), b"abcd".to_vec())],
            )
            .expect("session write should work");

        let mut dst = vec![0u8; 8];
        let connector = VllmKvConnector::new(CudaConfig::default());
        let mut allocator = FixedBufferAllocator::new(
            Some(CpuTransferTarget {
                allocation_id: 8,
                dst_host_ptr: dst.as_mut_ptr() as u64,
                dst_base_offset_bytes: 0,
            }),
            None,
        );

        let report = connector
            .restore(
                &mut local,
                VllmRestoreRequest::new(
                    session,
                    vec![
                        VllmPagedChunkSpec::new(b"s:gpu:l:0:p:0".to_vec(), 0, 0, 2)
                            .with_expected_len(4),
                    ],
                    PreferredTransferBackend::Cpu,
                ),
                &mut allocator,
            )
            .expect("restore should succeed");

        assert_eq!(report.backend(), TransferBackend::Cpu);
        assert_eq!(report.page_count(), 1);
        assert_eq!(report.total_expected_bytes(), Some(4));
        assert_eq!(report.hit_pages(), 1);
        assert_eq!(report.missed_pages(), 0);
        assert!(report.all_hit());
        assert_eq!(&dst[2..6], b"abcd");
    }

    #[test]
    fn vllm_restore_can_execute_against_simulated_gpu_engine() {
        let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
        let bootstrap = LocalEmbeddedStoreBootstrap::from_embedded(store, 1);
        let mut stores = bootstrap.into_stores();
        let mut local = stores.pop().expect("expected local store");
        let session = b"s:1".to_vec();
        local
            .batch_set_session_owned_no_ttl_if_local(
                session.clone(),
                vec![(b"s:gpu:l:0:p:0".to_vec(), b"uvwx".to_vec())],
            )
            .expect("session write should work");

        let mut simulated_device = vec![0u8; 8];
        let connector = VllmKvConnector::new(CudaConfig::default());
        let mut allocator = FixedBufferAllocator::new(
            None,
            Some(GpuTransferTarget {
                device_ordinal: 1,
                stream_ordinal: 6,
                allocation_id: 77,
                dst_device_ptr: simulated_device.as_mut_ptr() as u64,
                dst_base_offset_bytes: 0,
            }),
        );

        let plan = connector
            .plan_restore(
                VllmRestoreRequest::new(
                    session,
                    vec![
                        VllmPagedChunkSpec::new(b"s:gpu:l:0:p:0".to_vec(), 0, 4, 3)
                            .with_expected_len(4),
                    ],
                    PreferredTransferBackend::Gpu,
                )
                .with_device_ordinal(1)
                .with_stream_ordinal(6)
                .with_allow_cpu_fallback(false),
                &mut allocator,
            )
            .expect("plan should succeed");

        let mut engine = SimulatedGpuEngine::default();
        let report = connector
            .execute_restore_with_gpu_engine(&mut local, plan, &mut engine)
            .expect("simulated gpu restore should succeed");

        assert_eq!(report.backend(), TransferBackend::Gpu);
        assert_eq!(report.page_count(), 1);
        assert_eq!(report.total_expected_bytes(), Some(4));
        assert_eq!(report.hit_pages(), 1);
        assert_eq!(report.transferred_bytes(), 4);
        assert_eq!(engine.device_ordinal, Some(1));
        assert_eq!(engine.stream_ordinal, Some(6));
        assert_eq!(&simulated_device[3..7], b"uvwx");
    }

    #[test]
    fn request_builders_and_accessors_work() {
        let mut request =
            VllmRestoreRequest::new(b"s:42".to_vec(), Vec::new(), PreferredTransferBackend::Cpu)
                .with_gpu_target(8, 13)
                .with_allow_cpu_fallback(false);
        request.push_page(VllmPagedChunkSpec::new(b"k0".to_vec(), 2, 9, 64).with_expected_len(16));

        assert_eq!(request.session_prefix(), b"s:42");
        assert_eq!(request.preferred_backend(), PreferredTransferBackend::Gpu);
        assert_eq!(request.page_count(), 1);
        assert_eq!(request.total_expected_bytes(), Some(16));
        assert_eq!(request.pages()[0].layer_index(), 2);
        assert_eq!(request.pages()[0].page_index(), 9);
        assert_eq!(request.pages()[0].dst_offset_bytes(), 64);
        assert_eq!(request.pages()[0].expected_len(), Some(16));
    }

    #[test]
    fn vllm_allocator_can_return_paged_gpu_destination() {
        #[derive(Clone)]
        struct PagedAllocator {
            cpu: CpuTransferTarget,
            paged: PagedGpuTransferTarget,
        }

        impl VllmTransferAllocator for PagedAllocator {
            fn allocate_cpu(
                &mut self,
                _total_expected_bytes: Option<usize>,
            ) -> RuntimeResult<CpuTransferTarget> {
                Ok(self.cpu.clone())
            }

            fn allocate_gpu(
                &mut self,
                _device_ordinal: usize,
                _stream_ordinal: usize,
                _pages: &[VllmPagedChunkSpec],
                _total_expected_bytes: Option<usize>,
            ) -> RuntimeResult<VllmGpuAllocation> {
                Ok(VllmGpuAllocation::Paged(self.paged.clone()))
            }
        }

        let connector = VllmKvConnector::new(CudaConfig::default());
        let mut page0 = vec![0u8; 4];
        let mut page1 = vec![0u8; 4];
        let mut allocator = PagedAllocator {
            cpu: CpuTransferTarget {
                allocation_id: 1,
                dst_host_ptr: 0x1000,
                dst_base_offset_bytes: 0,
            },
            paged: PagedGpuTransferTarget {
                device_ordinal: 7,
                stream_ordinal: 12,
                allocation_id: 99,
                dst_base_offset_bytes: 0,
                pages: vec![
                    PagedGpuTransferPage {
                        page_index: 0,
                        dst_device_ptr: page0.as_mut_ptr() as u64,
                        page_size_bytes: 4,
                    },
                    PagedGpuTransferPage {
                        page_index: 1,
                        dst_device_ptr: page1.as_mut_ptr() as u64,
                        page_size_bytes: 4,
                    },
                ],
            },
        };

        let plan = connector
            .plan_restore(
                VllmRestoreRequest::new(
                    b"s:1".to_vec(),
                    vec![VllmPagedChunkSpec::new(b"k0".to_vec(), 0, 0, 0).with_expected_len(8)],
                    PreferredTransferBackend::Gpu,
                )
                .with_gpu_target(7, 12),
                &mut allocator,
            )
            .expect("plan should succeed");

        let gpu = plan
            .paged_gpu_target()
            .expect("expected paged gpu destination");
        assert_eq!(gpu.device_ordinal, 7);
        assert_eq!(gpu.stream_ordinal, 12);
        assert_eq!(
            plan.cpu_fallback_target().map(|cpu| cpu.allocation_id),
            Some(1)
        );
    }

    #[test]
    fn direct_fixed_gpu_allocation_plan_works() {
        let connector = VllmKvConnector::new(CudaConfig::default());
        let plan = connector
            .plan_restore_to_gpu_allocation(
                VllmRestoreRequest::new(
                    b"s:fixed".to_vec(),
                    vec![VllmPagedChunkSpec::new(b"k0".to_vec(), 0, 0, 32).with_expected_len(8)],
                    PreferredTransferBackend::Gpu,
                )
                .with_gpu_target(1, 2),
                VllmGpuAllocation::Contiguous(GpuTransferTarget {
                    device_ordinal: 1,
                    stream_ordinal: 2,
                    allocation_id: 7,
                    dst_device_ptr: 0x2000,
                    dst_base_offset_bytes: 0,
                }),
                None,
            )
            .expect("fixed gpu plan should succeed");

        assert_eq!(plan.page_count(), 1);
        assert_eq!(plan.total_expected_bytes(), Some(8));
        assert_eq!(
            plan.gpu_target()
                .map(|target| (target.device_ordinal, target.stream_ordinal)),
            Some((1, 2))
        );
    }
}
