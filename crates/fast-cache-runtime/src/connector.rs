use fast_cache_core::cuda::{CudaConfig, CudaSessionTransferRequest};
use fast_cache_core::storage::LocalEmbeddedStore;
#[cfg(all(feature = "cuda", target_os = "linux"))]
use fast_cache_core::storage::{LocalEmbeddedSessionPackedView, PackedBatch};
#[cfg(all(feature = "cuda", target_os = "linux"))]
use std::sync::{Arc, Mutex};

use crate::cpu_engine::CpuTransferEngine;
#[cfg(all(feature = "cuda", target_os = "linux"))]
use crate::cuda_engine::{CudaTransferEngine, PendingCudaTransfer};
#[cfg(all(feature = "cuda", target_os = "linux"))]
use crate::host::{CudaPinnedStagingPool, HostTransferPath, HostTransferPolicy};
#[cfg(all(feature = "cuda", target_os = "linux"))]
use crate::runtime::submit_session_to_engine_with_policy;
use crate::runtime::{
    CpuTransferTarget, GpuTransferTarget, KvTransferEngine, PagedGpuTransferTarget, RuntimeError,
    RuntimeResult, RuntimeSessionTransfer, RuntimeSessionTransferSummary, RuntimeTransferTarget,
    stream_session_to_engine,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferDestination {
    Cpu(CpuTransferTarget),
    Gpu {
        target: GpuTransferTarget,
        cpu_fallback: Option<CpuTransferTarget>,
    },
    PagedGpu {
        target: PagedGpuTransferTarget,
        cpu_fallback: Option<CpuTransferTarget>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferBackend {
    Cpu,
    Gpu,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectorTransferReport {
    pub backend: TransferBackend,
    pub summary: RuntimeSessionTransferSummary,
}

#[derive(Debug)]
pub enum ConnectorTransferHandle {
    Completed(ConnectorTransferReport),
    #[cfg(all(feature = "cuda", target_os = "linux"))]
    PendingGpu(PendingConnectorGpuTransfer),
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
#[derive(Debug)]
pub struct PendingConnectorGpuTransfer {
    backend: TransferBackend,
    pending: Option<PendingCudaTransfer>,
    engine_pool: Arc<Mutex<ConnectorCudaEnginePool>>,
    pool_key: ConnectorCudaEnginePoolKey,
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectorCudaEnginePoolKey {
    DirectDma,
    Configured,
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
#[derive(Debug, Default)]
struct ConnectorCudaEnginePool {
    direct_dma: Vec<CudaTransferEngine>,
    configured: Vec<CudaTransferEngine>,
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
impl ConnectorCudaEnginePool {
    fn take(&mut self, key: ConnectorCudaEnginePoolKey) -> Option<CudaTransferEngine> {
        match key {
            ConnectorCudaEnginePoolKey::DirectDma => self.direct_dma.pop(),
            ConnectorCudaEnginePoolKey::Configured => self.configured.pop(),
        }
    }

    fn give_back(&mut self, key: ConnectorCudaEnginePoolKey, engine: CudaTransferEngine) {
        let pool = match key {
            ConnectorCudaEnginePoolKey::DirectDma => &mut self.direct_dma,
            ConnectorCudaEnginePoolKey::Configured => &mut self.configured,
        };
        if pool.len() < 2 {
            pool.push(engine);
        }
    }
}

impl ConnectorTransferHandle {
    #[inline(always)]
    pub fn backend(&self) -> TransferBackend {
        match self {
            Self::Completed(report) => report.backend,
            #[cfg(all(feature = "cuda", target_os = "linux"))]
            Self::PendingGpu(handle) => handle.backend,
        }
    }

    #[inline(always)]
    pub fn is_pending(&self) -> bool {
        match self {
            Self::Completed(_) => false,
            #[cfg(all(feature = "cuda", target_os = "linux"))]
            Self::PendingGpu(_) => true,
        }
    }

    pub fn is_ready(&mut self) -> RuntimeResult<bool> {
        match self {
            Self::Completed(_) => Ok(true),
            #[cfg(all(feature = "cuda", target_os = "linux"))]
            Self::PendingGpu(handle) => handle
                .pending
                .as_mut()
                .expect("pending gpu transfer missing runtime handle")
                .is_ready(),
        }
    }

    pub fn peek_report(&self) -> ConnectorTransferReport {
        match self {
            Self::Completed(report) => *report,
            #[cfg(all(feature = "cuda", target_os = "linux"))]
            Self::PendingGpu(handle) => ConnectorTransferReport {
                backend: handle.backend,
                summary: handle
                    .pending
                    .as_ref()
                    .expect("pending gpu transfer missing runtime handle")
                    .summary(),
            },
        }
    }

    pub fn wait_on_stream(
        &mut self,
        #[allow(unused_variables)] stream_ptr: u64,
    ) -> RuntimeResult<bool> {
        match self {
            Self::Completed(_) => Ok(false),
            #[cfg(all(feature = "cuda", target_os = "linux"))]
            Self::PendingGpu(handle) => {
                handle
                    .pending
                    .as_mut()
                    .expect("pending gpu transfer missing runtime handle")
                    .wait_on_stream(stream_ptr)?;
                Ok(true)
            }
        }
    }

    pub fn wait(self) -> RuntimeResult<ConnectorTransferReport> {
        match self {
            Self::Completed(report) => Ok(report),
            #[cfg(all(feature = "cuda", target_os = "linux"))]
            Self::PendingGpu(mut handle) => {
                let pending = handle
                    .pending
                    .take()
                    .expect("pending gpu transfer missing runtime handle");
                let (engine, summary) = pending.wait_with_engine()?;
                handle
                    .engine_pool
                    .lock()
                    .expect("cuda engine pool mutex poisoned")
                    .give_back(handle.pool_key, engine);
                Ok(ConnectorTransferReport {
                    backend: handle.backend,
                    summary,
                })
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct DirectKvConnector {
    cuda: CudaConfig,
    #[cfg(all(feature = "cuda", target_os = "linux"))]
    engine_pool: Arc<Mutex<ConnectorCudaEnginePool>>,
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
enum PackedGpuRestoreSource {
    ZeroCopy(LocalEmbeddedSessionPackedView),
    Packed(PackedBatch),
}

impl DirectKvConnector {
    pub fn new(cuda: CudaConfig) -> Self {
        Self {
            cuda,
            #[cfg(all(feature = "cuda", target_os = "linux"))]
            engine_pool: Arc::new(Mutex::new(ConnectorCudaEnginePool::default())),
        }
    }

    #[inline(always)]
    pub fn cuda_config(&self) -> &CudaConfig {
        &self.cuda
    }

    #[cfg(all(feature = "cuda", target_os = "linux"))]
    fn take_cuda_engine(
        &self,
        key: ConnectorCudaEnginePoolKey,
    ) -> RuntimeResult<CudaTransferEngine> {
        if let Some(engine) = self
            .engine_pool
            .lock()
            .expect("cuda engine pool mutex poisoned")
            .take(key)
        {
            return Ok(engine);
        }

        match key {
            ConnectorCudaEnginePoolKey::DirectDma => {
                CudaTransferEngine::from_config_for_direct_dma(&self.cuda)
            }
            ConnectorCudaEnginePoolKey::Configured => CudaTransferEngine::from_config(&self.cuda),
        }
    }

    pub fn restore_session(
        &self,
        store: &mut LocalEmbeddedStore,
        request: CudaSessionTransferRequest,
        destination: TransferDestination,
    ) -> RuntimeResult<ConnectorTransferReport> {
        self.submit_restore_session(store, request, destination)?
            .wait()
    }

    pub fn submit_restore_session(
        &self,
        store: &mut LocalEmbeddedStore,
        request: CudaSessionTransferRequest,
        destination: TransferDestination,
    ) -> RuntimeResult<ConnectorTransferHandle> {
        match destination {
            TransferDestination::Cpu(target) => {
                let transfer = RuntimeSessionTransfer::new_cpu(request, target);
                let mut engine = CpuTransferEngine::new();
                self.restore_with_engine(store, &transfer, TransferBackend::Cpu, &mut engine)
                    .map(ConnectorTransferHandle::Completed)
            }
            TransferDestination::Gpu {
                target,
                cpu_fallback,
            } => self.restore_gpu(
                store,
                request,
                RuntimeTransferTarget::Gpu(target),
                cpu_fallback,
            ),
            TransferDestination::PagedGpu {
                target,
                cpu_fallback,
            } => self.restore_gpu(
                store,
                request,
                RuntimeTransferTarget::PagedGpu(target),
                cpu_fallback,
            ),
        }
    }

    fn restore_gpu(
        &self,
        store: &mut LocalEmbeddedStore,
        request: CudaSessionTransferRequest,
        #[allow(unused_variables)] target: RuntimeTransferTarget,
        cpu_fallback: Option<CpuTransferTarget>,
    ) -> RuntimeResult<ConnectorTransferHandle> {
        if self.cuda.enabled {
            #[cfg(all(feature = "cuda", target_os = "linux"))]
            {
                let transfer = RuntimeSessionTransfer::new(request, target);
                let policy = HostTransferPolicy::from(&self.cuda);
                let direct_source =
                    self.prepare_packed_gpu_restore_if_possible(store, &transfer, &policy)?;
                let pool_key = if direct_source.is_some() {
                    ConnectorCudaEnginePoolKey::DirectDma
                } else {
                    ConnectorCudaEnginePoolKey::Configured
                };
                let mut engine = self.take_cuda_engine(pool_key)?;
                let summary = match direct_source {
                    Some(PackedGpuRestoreSource::ZeroCopy(view)) => {
                        engine.submit_zero_copy_session_view_transfer(&transfer, view)?
                    }
                    Some(PackedGpuRestoreSource::Packed(packed)) => {
                        engine.submit_packed_host_buffer_transfer(&transfer, packed)?
                    }
                    None => {
                        let mut staging_pool = CudaPinnedStagingPool::default();
                        submit_session_to_engine_with_policy(
                            store,
                            &transfer,
                            &mut engine,
                            &policy,
                            &mut staging_pool,
                        )?
                    }
                };
                return Ok(ConnectorTransferHandle::PendingGpu(
                    PendingConnectorGpuTransfer {
                        backend: TransferBackend::Gpu,
                        pending: Some(engine.into_pending(transfer, summary)?),
                        engine_pool: Arc::clone(&self.engine_pool),
                        pool_key,
                    },
                ));
            }
            #[cfg(not(all(feature = "cuda", target_os = "linux")))]
            {
                if !self.cuda.allow_cpu_fallback {
                    return Err(RuntimeError::Cuda(
                        "cuda is enabled in config but the runtime does not have a real Linux CUDA engine available"
                            .into(),
                    ));
                }
            }
        }

        let fallback = cpu_fallback.ok_or_else(|| {
            if self.cuda.enabled {
                RuntimeError::Cuda(
                    "gpu transfer is unavailable and no cpu fallback target was provided".into(),
                )
            } else {
                RuntimeError::Engine("gpu destination requested while cuda is disabled".into())
            }
        })?;

        if !self.cuda.allow_cpu_fallback {
            return Err(RuntimeError::Cuda(
                "gpu transfer is unavailable and cpu fallback is disabled".into(),
            ));
        }

        let transfer = RuntimeSessionTransfer::new_cpu(request, fallback);
        let mut engine = CpuTransferEngine::new();
        self.restore_with_engine(store, &transfer, TransferBackend::Cpu, &mut engine)
            .map(ConnectorTransferHandle::Completed)
    }

    fn restore_with_engine<E>(
        &self,
        store: &mut LocalEmbeddedStore,
        transfer: &RuntimeSessionTransfer,
        backend: TransferBackend,
        engine: &mut E,
    ) -> RuntimeResult<ConnectorTransferReport>
    where
        E: KvTransferEngine,
    {
        let summary = stream_session_to_engine(store, transfer, engine)?;
        Ok(ConnectorTransferReport { backend, summary })
    }

    #[cfg(all(feature = "cuda", target_os = "linux"))]
    fn prepare_packed_gpu_restore_if_possible(
        &self,
        store: &mut LocalEmbeddedStore,
        transfer: &RuntimeSessionTransfer,
        policy: &HostTransferPolicy,
    ) -> RuntimeResult<Option<PackedGpuRestoreSource>> {
        let request = transfer.request();
        let keys = request
            .chunks()
            .iter()
            .map(|descriptor| descriptor.key().to_vec())
            .collect::<Vec<_>>();
        if let Some(view) =
            store.batch_get_session_packed_view_if_local(request.session_prefix(), &keys)?
        {
            if view
                .offsets()
                .iter()
                .zip(view.lengths())
                .any(|(offset, len)| {
                    *offset != usize::MAX
                        && policy.path_for_len(*len) != HostTransferPath::DirectHostDma
                })
            {
                return Ok(None);
            }
            return Ok(Some(PackedGpuRestoreSource::ZeroCopy(view)));
        }
        let packed = store.batch_get_session_packed_if_local(request.session_prefix(), &keys)?;
        if packed
            .offsets
            .iter()
            .zip(&packed.lengths)
            .any(|(offset, len)| {
                *offset != usize::MAX
                    && policy.path_for_len(*len) != HostTransferPath::DirectHostDma
            })
        {
            return Ok(None);
        }
        Ok(Some(PackedGpuRestoreSource::Packed(packed)))
    }

    #[cfg(test)]
    pub(crate) fn restore_session_with_gpu_engine<E>(
        &self,
        store: &mut LocalEmbeddedStore,
        request: CudaSessionTransferRequest,
        target: RuntimeTransferTarget,
        engine: &mut E,
    ) -> RuntimeResult<ConnectorTransferReport>
    where
        E: KvTransferEngine,
    {
        let transfer = RuntimeSessionTransfer::new(request, target);
        self.restore_with_engine(store, &transfer, TransferBackend::Gpu, engine)
    }
}

#[cfg(test)]
mod tests {
    use fast_cache_core::cuda::{CudaChunkTransferDescriptor, CudaSessionTransferRequest};
    use fast_cache_core::storage::{EmbeddedRouteMode, EmbeddedStore, LocalEmbeddedStoreBootstrap};

    use super::{DirectKvConnector, TransferBackend, TransferDestination};
    #[cfg(all(feature = "cuda", target_os = "linux"))]
    use crate::CudaTransferEngine;
    use crate::runtime::{
        CpuTransferTarget, GpuTransferTarget, PagedGpuTransferPage, PagedGpuTransferTarget,
        RuntimeTransferTarget,
    };
    use crate::test_support::SimulatedGpuEngine;
    #[cfg(all(feature = "cuda", target_os = "linux"))]
    use crate::{
        CudaPinnedStagingPool, HostTransferPolicy, RuntimeSessionTransfer,
        stream_session_to_engine_with_policy,
    };
    #[cfg(all(feature = "cuda", target_os = "linux"))]
    use cust::memory::{CopyDestination, DeviceBuffer};

    #[test]
    fn connector_restores_to_cpu_target() {
        let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
        let bootstrap = LocalEmbeddedStoreBootstrap::from_embedded(store, 1);
        let mut stores = bootstrap.into_stores();
        let mut local = stores.pop().expect("expected local store");
        let session = b"s:1".to_vec();
        local
            .batch_set_session_owned_no_ttl_if_local(
                session.clone(),
                vec![(b"s:gpu:l:0".to_vec(), b"abcd".to_vec())],
            )
            .expect("session write should work");

        let mut dst = vec![0u8; 8];
        let connector = DirectKvConnector::new(fast_cache_core::cuda::CudaConfig::default());
        let report = connector
            .restore_session(
                &mut local,
                CudaSessionTransferRequest::new(
                    session,
                    vec![CudaChunkTransferDescriptor::new(
                        b"s:gpu:l:0".to_vec(),
                        0,
                        2,
                    )],
                ),
                TransferDestination::Cpu(CpuTransferTarget {
                    allocation_id: 1,
                    dst_host_ptr: dst.as_mut_ptr() as u64,
                    dst_base_offset_bytes: 0,
                }),
            )
            .expect("cpu restore should succeed");

        assert_eq!(report.backend, TransferBackend::Cpu);
        assert_eq!(&dst[2..6], b"abcd");
    }

    #[test]
    fn connector_can_fallback_from_gpu_to_cpu() {
        let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
        let bootstrap = LocalEmbeddedStoreBootstrap::from_embedded(store, 1);
        let mut stores = bootstrap.into_stores();
        let mut local = stores.pop().expect("expected local store");
        let session = b"s:1".to_vec();
        local
            .batch_set_session_owned_no_ttl_if_local(
                session.clone(),
                vec![(b"s:gpu:l:0".to_vec(), b"wxyz".to_vec())],
            )
            .expect("session write should work");

        let mut dst = vec![0u8; 8];
        let config = fast_cache_core::cuda::CudaConfig {
            enabled: false,
            allow_cpu_fallback: true,
            ..Default::default()
        };
        let connector = DirectKvConnector::new(config);
        let report = connector
            .restore_session(
                &mut local,
                CudaSessionTransferRequest::new(
                    session,
                    vec![CudaChunkTransferDescriptor::new(
                        b"s:gpu:l:0".to_vec(),
                        0,
                        1,
                    )],
                ),
                TransferDestination::Gpu {
                    target: GpuTransferTarget {
                        device_ordinal: 0,
                        stream_ordinal: 0,
                        allocation_id: 9,
                        dst_device_ptr: 0x1000,
                        dst_base_offset_bytes: 0,
                    },
                    cpu_fallback: Some(CpuTransferTarget {
                        allocation_id: 10,
                        dst_host_ptr: dst.as_mut_ptr() as u64,
                        dst_base_offset_bytes: 0,
                    }),
                },
            )
            .expect("cpu fallback should succeed");

        assert_eq!(report.backend, TransferBackend::Cpu);
        assert_eq!(&dst[1..5], b"wxyz");
    }

    #[test]
    fn connector_can_execute_simulated_gpu_restore_without_cuda_runtime() {
        let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
        let bootstrap = LocalEmbeddedStoreBootstrap::from_embedded(store, 1);
        let mut stores = bootstrap.into_stores();
        let mut local = stores.pop().expect("expected local store");
        let session = b"s:1".to_vec();
        local
            .batch_set_session_owned_no_ttl_if_local(
                session.clone(),
                vec![(b"s:gpu:l:0".to_vec(), b"qrst".to_vec())],
            )
            .expect("session write should work");

        let mut simulated_device = vec![0u8; 8];
        let connector = DirectKvConnector::new(fast_cache_core::cuda::CudaConfig::default());
        let mut engine = SimulatedGpuEngine::default();
        let report = connector
            .restore_session_with_gpu_engine(
                &mut local,
                CudaSessionTransferRequest::new(
                    session,
                    vec![CudaChunkTransferDescriptor::new(
                        b"s:gpu:l:0".to_vec(),
                        0,
                        2,
                    )],
                ),
                RuntimeTransferTarget::Gpu(GpuTransferTarget {
                    device_ordinal: 4,
                    stream_ordinal: 7,
                    allocation_id: 55,
                    dst_device_ptr: simulated_device.as_mut_ptr() as u64,
                    dst_base_offset_bytes: 0,
                }),
                &mut engine,
            )
            .expect("simulated gpu restore should succeed");

        assert_eq!(report.backend, TransferBackend::Gpu);
        assert_eq!(report.summary.hit_chunks, 1);
        assert!(engine.began);
        assert!(engine.finished);
        assert_eq!(engine.device_ordinal, Some(4));
        assert_eq!(engine.stream_ordinal, Some(7));
        assert_eq!(engine.allocation_id, Some(55));
        assert_eq!(&simulated_device[2..6], b"qrst");
    }

    #[test]
    fn connector_can_execute_simulated_paged_gpu_restore_without_cuda_runtime() {
        let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
        let bootstrap = LocalEmbeddedStoreBootstrap::from_embedded(store, 1);
        let mut stores = bootstrap.into_stores();
        let mut local = stores.pop().expect("expected local store");
        let session = b"s:1".to_vec();
        local
            .batch_set_session_owned_no_ttl_if_local(
                session.clone(),
                vec![(b"s:gpu:l:0".to_vec(), b"abcdefgh".to_vec())],
            )
            .expect("session write should work");

        let mut page0 = vec![0u8; 4];
        let mut page1 = vec![0u8; 4];
        let connector = DirectKvConnector::new(fast_cache_core::cuda::CudaConfig::default());
        let mut engine = SimulatedGpuEngine::default();
        let report = connector
            .restore_session_with_gpu_engine(
                &mut local,
                CudaSessionTransferRequest::new(
                    session,
                    vec![
                        CudaChunkTransferDescriptor::new(b"s:gpu:l:0".to_vec(), 0, 0)
                            .with_expected_len(8),
                    ],
                ),
                RuntimeTransferTarget::PagedGpu(PagedGpuTransferTarget {
                    device_ordinal: 6,
                    stream_ordinal: 11,
                    allocation_id: 88,
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
                }),
                &mut engine,
            )
            .expect("simulated paged gpu restore should succeed");

        assert_eq!(report.backend, TransferBackend::Gpu);
        assert_eq!(report.summary.hit_chunks, 1);
        assert_eq!(engine.device_ordinal, Some(6));
        assert_eq!(engine.stream_ordinal, Some(11));
        assert_eq!(&page0, b"abcd");
        assert_eq!(&page1, b"efgh");
    }

    #[cfg(all(feature = "cuda", target_os = "linux"))]
    #[test]
    fn connector_can_execute_real_cuda_restore_when_enabled() {
        if std::env::var_os("FAST_CACHE_RUN_REAL_CUDA_TESTS").is_none() {
            eprintln!(
                "skipping real CUDA connector smoke test; set FAST_CACHE_RUN_REAL_CUDA_TESTS=1 to enable"
            );
            return;
        }

        let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
        let bootstrap = LocalEmbeddedStoreBootstrap::from_embedded(store, 1);
        let mut stores = bootstrap.into_stores();
        let mut local = stores.pop().expect("expected local store");
        let session = b"s:real-gpu".to_vec();
        local
            .batch_set_session_owned_no_ttl_if_local(
                session.clone(),
                vec![(b"s:real-gpu:l:0".to_vec(), b"cuda".to_vec())],
            )
            .expect("session write should work");

        let connector = DirectKvConnector::new(fast_cache_core::cuda::CudaConfig::default());
        let mut engine = CudaTransferEngine::new(0).expect("real CUDA engine should initialize");
        let device_buffer =
            DeviceBuffer::from_slice(&[0u8; 8]).expect("device buffer allocation should succeed");

        let report = connector
            .restore_session_with_gpu_engine(
                &mut local,
                CudaSessionTransferRequest::new(
                    session,
                    vec![CudaChunkTransferDescriptor::new(
                        b"s:real-gpu:l:0".to_vec(),
                        0,
                        2,
                    )],
                ),
                RuntimeTransferTarget::Gpu(GpuTransferTarget {
                    device_ordinal: 0,
                    stream_ordinal: 0,
                    allocation_id: 9001,
                    dst_device_ptr: device_buffer.as_device_ptr().as_raw() as u64,
                    dst_base_offset_bytes: 0,
                }),
                &mut engine,
            )
            .expect("real cuda restore should succeed");

        assert_eq!(report.backend, TransferBackend::Gpu);
        assert_eq!(report.summary.hit_chunks, 1);

        let mut host = [0u8; 8];
        device_buffer
            .copy_to(&mut host)
            .expect("device buffer copy back should succeed");
        assert_eq!(&host[2..6], b"cuda");
    }

    #[cfg(all(feature = "cuda", target_os = "linux"))]
    #[test]
    fn connector_can_execute_real_cuda_restore_with_pinned_staging_when_enabled() {
        if std::env::var_os("FAST_CACHE_RUN_REAL_CUDA_STAGING_TESTS").is_none() {
            eprintln!(
                "skipping real CUDA pinned-staging smoke test; set FAST_CACHE_RUN_REAL_CUDA_STAGING_TESTS=1 to enable"
            );
            return;
        }

        let chunk_size = 2 * 1024 * 1024;
        let chunk_byte = 0x5au8;
        let value = vec![chunk_byte; chunk_size];

        let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
        let bootstrap = LocalEmbeddedStoreBootstrap::from_embedded(store, 1);
        let mut stores = bootstrap.into_stores();
        let mut local = stores.pop().expect("expected local store");
        let session = b"s:real-gpu:staged".to_vec();
        local
            .batch_set_session_owned_no_ttl_if_local(
                session.clone(),
                vec![(b"s:real-gpu:staged:l:0".to_vec(), value)],
            )
            .expect("session write should work");

        let mut engine = CudaTransferEngine::new(0).expect("real CUDA engine should initialize");
        let device_buffer = DeviceBuffer::from_slice(&vec![0u8; chunk_size])
            .expect("device buffer allocation should succeed");
        let transfer = RuntimeSessionTransfer::new_gpu(
            CudaSessionTransferRequest::new(
                session,
                vec![
                    CudaChunkTransferDescriptor::new(b"s:real-gpu:staged:l:0".to_vec(), 0, 0)
                        .with_expected_len(chunk_size),
                ],
            ),
            GpuTransferTarget {
                device_ordinal: 0,
                stream_ordinal: 0,
                allocation_id: 9002,
                dst_device_ptr: device_buffer.as_device_ptr().as_raw() as u64,
                dst_base_offset_bytes: 0,
            },
        );
        let policy = HostTransferPolicy::from(&fast_cache_core::cuda::CudaConfig {
            prefer_direct_host_dma: false,
            pinned_staging_threshold_bytes: 1,
            ..fast_cache_core::cuda::CudaConfig::default()
        });
        let mut staging_pool = CudaPinnedStagingPool::default();

        let summary = stream_session_to_engine_with_policy(
            &mut local,
            &transfer,
            &mut engine,
            &policy,
            &mut staging_pool,
        )
        .expect("real cuda restore with pinned staging should succeed");

        assert_eq!(summary.hit_chunks, 1);
        assert_eq!(summary.transferred_bytes, chunk_size);

        let mut host = vec![0u8; chunk_size];
        device_buffer
            .copy_to(&mut host)
            .expect("device buffer copy back should succeed");
        assert_eq!(host[0], chunk_byte);
        assert_eq!(host[chunk_size - 1], chunk_byte);
    }

    #[cfg(all(feature = "cuda", target_os = "linux"))]
    #[test]
    fn connector_can_reuse_real_cuda_direct_dma_engine_when_enabled() {
        if std::env::var_os("FAST_CACHE_RUN_REAL_CUDA_TESTS").is_none() {
            eprintln!(
                "skipping real CUDA direct-DMA reuse smoke test; set FAST_CACHE_RUN_REAL_CUDA_TESTS=1 to enable"
            );
            return;
        }

        let chunk_size = 2 * 1024 * 1024;
        let chunk_byte = 0x3cu8;
        let value = vec![chunk_byte; chunk_size];

        let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
        let bootstrap = LocalEmbeddedStoreBootstrap::from_embedded(store, 1);
        let mut stores = bootstrap.into_stores();
        let mut local = stores.pop().expect("expected local store");
        let session = b"s:real-gpu:direct-reuse".to_vec();
        local
            .batch_set_session_owned_no_ttl_if_local(
                session.clone(),
                vec![(b"s:real-gpu:direct-reuse:l:0".to_vec(), value)],
            )
            .expect("session write should work");

        let mut engine = CudaTransferEngine::new(0).expect("real CUDA engine should initialize");
        let device_buffer = DeviceBuffer::from_slice(&vec![0u8; chunk_size])
            .expect("device buffer allocation should succeed");
        let transfer = RuntimeSessionTransfer::new_gpu(
            CudaSessionTransferRequest::new(
                session,
                vec![
                    CudaChunkTransferDescriptor::new(b"s:real-gpu:direct-reuse:l:0".to_vec(), 0, 0)
                        .with_expected_len(chunk_size),
                ],
            ),
            GpuTransferTarget {
                device_ordinal: 0,
                stream_ordinal: 0,
                allocation_id: 9003,
                dst_device_ptr: device_buffer.as_device_ptr().as_raw() as u64,
                dst_base_offset_bytes: 0,
            },
        );
        let policy = HostTransferPolicy::from(&fast_cache_core::cuda::CudaConfig {
            prefer_direct_host_dma: true,
            pinned_staging_threshold_bytes: usize::MAX,
            ..fast_cache_core::cuda::CudaConfig::default()
        });
        let mut staging_pool = CudaPinnedStagingPool::default();

        for _ in 0..4 {
            let summary = stream_session_to_engine_with_policy(
                &mut local,
                &transfer,
                &mut engine,
                &policy,
                &mut staging_pool,
            )
            .expect("real cuda direct-dma restore should succeed");

            assert_eq!(summary.hit_chunks, 1);
            assert_eq!(summary.transferred_bytes, chunk_size);
        }

        let mut host = vec![0u8; chunk_size];
        device_buffer
            .copy_to(&mut host)
            .expect("device buffer copy back should succeed");
        assert_eq!(host[0], chunk_byte);
        assert_eq!(host[chunk_size - 1], chunk_byte);
    }

    #[cfg(all(feature = "cuda", target_os = "linux"))]
    #[derive(Debug, Clone, Copy)]
    enum BenchTransferLayout {
        Contiguous,
        Fragmented { gap_multiplier: usize },
    }

    #[cfg(all(feature = "cuda", target_os = "linux"))]
    impl BenchTransferLayout {
        fn gap_bytes(self, chunk_size: usize) -> usize {
            match self {
                Self::Contiguous => 0,
                Self::Fragmented { gap_multiplier } => chunk_size.saturating_mul(gap_multiplier),
            }
        }

        fn destination_span_bytes(self, chunk_size: usize, chunk_count: usize) -> usize {
            if chunk_count == 0 {
                return 0;
            }
            let stride = chunk_size.saturating_add(self.gap_bytes(chunk_size));
            stride
                .saturating_mul(chunk_count.saturating_sub(1))
                .saturating_add(chunk_size)
        }
    }

    #[cfg(all(feature = "cuda", target_os = "linux"))]
    fn real_cuda_benchmark_enabled() -> bool {
        std::env::var_os("FAST_CACHE_RUN_REAL_CUDA_BENCH_TESTS").is_some()
    }

    #[cfg(all(feature = "cuda", target_os = "linux"))]
    fn bench_median(values: &[f64]) -> f64 {
        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mid = sorted.len() / 2;
        if sorted.len() % 2 == 0 {
            (sorted[mid - 1] + sorted[mid]) / 2.0
        } else {
            sorted[mid]
        }
    }

    #[cfg(all(feature = "cuda", target_os = "linux"))]
    fn bench_bytes_to_gbps(bytes: usize, elapsed: std::time::Duration) -> f64 {
        if elapsed.is_zero() {
            0.0
        } else {
            (bytes as f64 / 1_000_000_000.0) / elapsed.as_secs_f64()
        }
    }

    #[cfg(all(feature = "cuda", target_os = "linux"))]
    fn run_real_cuda_benchmark_case(
        case_name: &str,
        cuda: fast_cache_core::cuda::CudaConfig,
        layout: BenchTransferLayout,
        use_configured_direct_dma_streams: bool,
        chunk_size: usize,
        chunk_count: usize,
        warmup_rounds: usize,
        measure_rounds: usize,
    ) -> (f64, f64) {
        let request_bytes = chunk_size * chunk_count;
        let destination_span_bytes = layout.destination_span_bytes(chunk_size, chunk_count);
        let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
        let bootstrap = LocalEmbeddedStoreBootstrap::from_embedded(store, 1);
        let mut stores = bootstrap.into_stores();
        let mut local = stores.pop().expect("expected local store");
        let session = format!("s:real-gpu:bench:{case_name}").into_bytes();
        let fragment_gap_bytes = layout.gap_bytes(chunk_size);
        let destination_stride_bytes = chunk_size.saturating_add(fragment_gap_bytes);

        let mut items = Vec::with_capacity(chunk_count);
        let mut descriptors = Vec::with_capacity(chunk_count);
        for chunk_index in 0..chunk_count {
            let key = format!("s:real-gpu:bench:{case_name}:l:{chunk_index}").into_bytes();
            items.push((key.clone(), vec![(chunk_index % 251) as u8; chunk_size]));
            descriptors.push(
                CudaChunkTransferDescriptor::new(
                    key,
                    chunk_index as u32,
                    (chunk_index * destination_stride_bytes) as u64,
                )
                .with_expected_len(chunk_size),
            );
        }
        local
            .batch_set_session_owned_no_ttl_if_local(session.clone(), items)
            .expect("session write should work");

        let mut engine = if cuda.prefer_direct_host_dma && !use_configured_direct_dma_streams {
            CudaTransferEngine::from_config_for_direct_dma(&cuda)
                .expect("real CUDA engine should initialize")
        } else {
            CudaTransferEngine::from_config(&cuda).expect("real CUDA engine should initialize")
        };
        let device_buffer = DeviceBuffer::from_slice(&vec![0u8; destination_span_bytes])
            .expect("device buffer allocation should succeed");
        let transfer = RuntimeSessionTransfer::new_gpu(
            CudaSessionTransferRequest::new(session, descriptors),
            GpuTransferTarget {
                device_ordinal: 0,
                stream_ordinal: 0,
                allocation_id: 9100,
                dst_device_ptr: device_buffer.as_device_ptr().as_raw() as u64,
                dst_base_offset_bytes: 0,
            },
        );
        let policy = HostTransferPolicy::from(&cuda);
        let mut staging_pool = CudaPinnedStagingPool::default();
        let keys = transfer
            .request()
            .chunks()
            .iter()
            .map(|descriptor| descriptor.key().to_vec())
            .collect::<Vec<_>>();

        for _ in 0..warmup_rounds {
            let summary = if cuda.prefer_direct_host_dma {
                let packed = local
                    .batch_get_session_packed_view_if_local(
                        transfer.request().session_prefix(),
                        &keys,
                    )
                    .expect("packed warmup view lookup should succeed")
                    .expect("session should be packed for zero-copy warmup");
                let summary = engine
                    .submit_zero_copy_session_view_transfer(&transfer, packed)
                    .expect("packed warmup restore should succeed");
                crate::runtime::KvTransferEngine::finish_transfer(&mut engine, &transfer, summary)
                    .expect("packed warmup finish should succeed");
                summary
            } else {
                stream_session_to_engine_with_policy(
                    &mut local,
                    &transfer,
                    &mut engine,
                    &policy,
                    &mut staging_pool,
                )
                .expect("warmup restore should succeed")
            };
            assert_eq!(summary.hit_chunks, chunk_count);
            assert_eq!(summary.transferred_bytes, request_bytes);
        }

        let mut gbps = Vec::with_capacity(measure_rounds);
        let mut round_ms = Vec::with_capacity(measure_rounds);
        for _ in 0..measure_rounds {
            let started = std::time::Instant::now();
            let summary = if cuda.prefer_direct_host_dma {
                let packed = local
                    .batch_get_session_packed_view_if_local(
                        transfer.request().session_prefix(),
                        &keys,
                    )
                    .expect("packed measured view lookup should succeed")
                    .expect("session should be packed for zero-copy measurement");
                let summary = engine
                    .submit_zero_copy_session_view_transfer(&transfer, packed)
                    .expect("packed measured restore should succeed");
                crate::runtime::KvTransferEngine::finish_transfer(&mut engine, &transfer, summary)
                    .expect("packed measured finish should succeed");
                summary
            } else {
                stream_session_to_engine_with_policy(
                    &mut local,
                    &transfer,
                    &mut engine,
                    &policy,
                    &mut staging_pool,
                )
                .expect("measured restore should succeed")
            };
            let elapsed = started.elapsed();
            assert_eq!(summary.hit_chunks, chunk_count);
            assert_eq!(summary.transferred_bytes, request_bytes);
            gbps.push(bench_bytes_to_gbps(request_bytes, elapsed));
            round_ms.push(elapsed.as_secs_f64() * 1000.0);
        }

        (bench_median(&gbps), bench_median(&round_ms))
    }

    #[cfg(all(feature = "cuda", target_os = "linux"))]
    fn report_real_cuda_benchmark_case(
        case_name: &str,
        cuda: fast_cache_core::cuda::CudaConfig,
        layout: BenchTransferLayout,
        use_configured_direct_dma_streams: bool,
    ) {
        if !real_cuda_benchmark_enabled() {
            eprintln!(
                "skipping real CUDA benchmark smoke test; set FAST_CACHE_RUN_REAL_CUDA_BENCH_TESTS=1 to enable"
            );
            return;
        }

        fn env_usize(name: &str, default: usize) -> usize {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(default)
        }

        let chunk_size = env_usize("FAST_CACHE_CUDA_BENCH_CHUNK_SIZE_BYTES", 2 * 1024 * 1024);
        let chunk_count = env_usize("FAST_CACHE_CUDA_BENCH_CHUNK_COUNT", 64);
        let warmup_rounds = env_usize("FAST_CACHE_CUDA_BENCH_WARMUP_ROUNDS", 2);
        let measure_rounds = env_usize("FAST_CACHE_CUDA_BENCH_MEASURE_ROUNDS", 6);
        let fragment_gap_bytes = layout.gap_bytes(chunk_size);
        let (median_gbps, median_ms) = run_real_cuda_benchmark_case(
            case_name,
            cuda,
            layout,
            use_configured_direct_dma_streams,
            chunk_size,
            chunk_count,
            warmup_rounds,
            measure_rounds,
        );

        eprintln!(
            "cuda-bench case={} request_bytes={} chunk_size={} chunk_count={} fragment_gap_bytes={} configured_direct_dma_streams={} median_gbps={:.2} median_ms={:.3}",
            case_name,
            chunk_size * chunk_count,
            chunk_size,
            chunk_count,
            fragment_gap_bytes,
            use_configured_direct_dma_streams,
            median_gbps,
            median_ms,
        );
    }

    #[cfg(all(feature = "cuda", target_os = "linux"))]
    #[test]
    fn connector_can_report_real_cuda_direct_dma_single_stream_benchmark_when_enabled() {
        report_real_cuda_benchmark_case(
            "engine_direct_dma_single_stream",
            fast_cache_core::cuda::CudaConfig {
                enabled: true,
                transfer_stream_count: 1,
                layer_streaming: false,
                prefer_direct_host_dma: true,
                pinned_staging_threshold_bytes: usize::MAX,
                ..fast_cache_core::cuda::CudaConfig::default()
            },
            BenchTransferLayout::Contiguous,
            false,
        );
    }

    #[cfg(all(feature = "cuda", target_os = "linux"))]
    #[test]
    fn connector_can_report_real_cuda_direct_dma_layered_4stream_benchmark_when_enabled() {
        report_real_cuda_benchmark_case(
            "engine_direct_dma_layered_4stream",
            fast_cache_core::cuda::CudaConfig {
                enabled: true,
                transfer_stream_count: 4,
                layer_streaming: true,
                prefer_direct_host_dma: true,
                pinned_staging_threshold_bytes: usize::MAX,
                ..fast_cache_core::cuda::CudaConfig::default()
            },
            BenchTransferLayout::Contiguous,
            false,
        );
    }

    #[cfg(all(feature = "cuda", target_os = "linux"))]
    #[test]
    fn connector_can_report_real_cuda_direct_dma_layered_8stream_benchmark_when_enabled() {
        report_real_cuda_benchmark_case(
            "engine_direct_dma_layered_8stream",
            fast_cache_core::cuda::CudaConfig {
                enabled: true,
                transfer_stream_count: 8,
                layer_streaming: true,
                prefer_direct_host_dma: true,
                pinned_staging_threshold_bytes: usize::MAX,
                ..fast_cache_core::cuda::CudaConfig::default()
            },
            BenchTransferLayout::Contiguous,
            false,
        );
    }

    #[cfg(all(feature = "cuda", target_os = "linux"))]
    #[test]
    fn connector_can_report_real_cuda_fragmented_direct_dma_single_stream_benchmark_when_enabled() {
        report_real_cuda_benchmark_case(
            "engine_direct_dma_fragmented_single_stream",
            fast_cache_core::cuda::CudaConfig {
                enabled: true,
                transfer_stream_count: 1,
                layer_streaming: false,
                prefer_direct_host_dma: true,
                pinned_staging_threshold_bytes: usize::MAX,
                ..fast_cache_core::cuda::CudaConfig::default()
            },
            BenchTransferLayout::Fragmented { gap_multiplier: 1 },
            true,
        );
    }

    #[cfg(all(feature = "cuda", target_os = "linux"))]
    #[test]
    fn connector_can_report_real_cuda_fragmented_direct_dma_layered_4stream_benchmark_when_enabled()
    {
        report_real_cuda_benchmark_case(
            "engine_direct_dma_fragmented_layered_4stream",
            fast_cache_core::cuda::CudaConfig {
                enabled: true,
                transfer_stream_count: 4,
                layer_streaming: true,
                prefer_direct_host_dma: true,
                pinned_staging_threshold_bytes: usize::MAX,
                ..fast_cache_core::cuda::CudaConfig::default()
            },
            BenchTransferLayout::Fragmented { gap_multiplier: 1 },
            true,
        );
    }

    #[cfg(all(feature = "cuda", target_os = "linux"))]
    #[test]
    fn connector_can_report_real_cuda_fragmented_direct_dma_layered_8stream_benchmark_when_enabled()
    {
        report_real_cuda_benchmark_case(
            "engine_direct_dma_fragmented_layered_8stream",
            fast_cache_core::cuda::CudaConfig {
                enabled: true,
                transfer_stream_count: 8,
                layer_streaming: true,
                prefer_direct_host_dma: true,
                pinned_staging_threshold_bytes: usize::MAX,
                ..fast_cache_core::cuda::CudaConfig::default()
            },
            BenchTransferLayout::Fragmented { gap_multiplier: 1 },
            true,
        );
    }

    #[cfg(all(feature = "cuda", target_os = "linux"))]
    #[test]
    fn connector_can_report_real_cuda_pinned_staging_layered_4stream_benchmark_when_enabled() {
        report_real_cuda_benchmark_case(
            "engine_pinned_staging_layered_4stream",
            fast_cache_core::cuda::CudaConfig {
                enabled: true,
                transfer_stream_count: 4,
                layer_streaming: true,
                prefer_direct_host_dma: false,
                pinned_staging_threshold_bytes: 1,
                ..fast_cache_core::cuda::CudaConfig::default()
            },
            BenchTransferLayout::Contiguous,
            false,
        );
    }

    #[cfg(all(feature = "cuda", target_os = "linux"))]
    #[test]
    fn connector_can_report_real_cuda_fragmented_pinned_staging_layered_4stream_benchmark_when_enabled()
     {
        report_real_cuda_benchmark_case(
            "engine_pinned_staging_fragmented_layered_4stream",
            fast_cache_core::cuda::CudaConfig {
                enabled: true,
                transfer_stream_count: 4,
                layer_streaming: true,
                prefer_direct_host_dma: false,
                pinned_staging_threshold_bytes: 1,
                ..fast_cache_core::cuda::CudaConfig::default()
            },
            BenchTransferLayout::Fragmented { gap_multiplier: 1 },
            false,
        );
    }
}
