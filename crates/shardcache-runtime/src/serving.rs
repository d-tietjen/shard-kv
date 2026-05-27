use shardmap::cuda::{CudaChunkTransferDescriptor, CudaConfig, CudaSessionTransferRequest};
use shardmap::storage::{Bytes, LocalEmbeddedStore};

use crate::connector::{
    ConnectorTransferHandle, ConnectorTransferReport, DirectKvConnector, TransferDestination,
};
use crate::runtime::{CpuTransferTarget, GpuTransferTarget, RuntimeError, RuntimeResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreferredTransferBackend {
    Cpu,
    Gpu,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreChunkSpec {
    pub key: Bytes,
    pub layer_index: u32,
    pub dst_offset_bytes: u64,
    pub expected_len: Option<usize>,
}

impl RestoreChunkSpec {
    pub fn new<K>(key: K, layer_index: u32, dst_offset_bytes: u64) -> Self
    where
        K: Into<Bytes>,
    {
        Self {
            key: key.into(),
            layer_index,
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
    fn to_descriptor(&self) -> CudaChunkTransferDescriptor {
        let descriptor = CudaChunkTransferDescriptor::new(
            self.key.clone(),
            self.layer_index,
            self.dst_offset_bytes,
        );
        match self.expected_len {
            Some(expected_len) => descriptor.with_expected_len(expected_len),
            None => descriptor,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRestoreSpec {
    pub session_prefix: Bytes,
    pub chunks: Vec<RestoreChunkSpec>,
    pub preferred_backend: PreferredTransferBackend,
    pub device_ordinal: usize,
    pub stream_ordinal: usize,
    pub allow_cpu_fallback: bool,
}

impl SessionRestoreSpec {
    pub fn new<S>(
        session_prefix: S,
        chunks: Vec<RestoreChunkSpec>,
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
    pub fn total_expected_bytes(&self) -> Option<usize> {
        self.chunks
            .iter()
            .map(|chunk| chunk.expected_len)
            .try_fold(0usize, |sum, len| len.map(|len| sum.saturating_add(len)))
    }

    #[inline(always)]
    pub(crate) fn to_request(&self) -> CudaSessionTransferRequest {
        CudaSessionTransferRequest::new(
            self.session_prefix.clone(),
            self.chunks
                .iter()
                .map(RestoreChunkSpec::to_descriptor)
                .collect(),
        )
    }
}

pub trait TransferAllocator {
    fn allocate_cpu(
        &mut self,
        total_expected_bytes: Option<usize>,
    ) -> RuntimeResult<CpuTransferTarget>;

    fn allocate_gpu(
        &mut self,
        device_ordinal: usize,
        stream_ordinal: usize,
        total_expected_bytes: Option<usize>,
    ) -> RuntimeResult<GpuTransferTarget>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedRestore {
    pub request: CudaSessionTransferRequest,
    pub destination: TransferDestination,
    pub preferred_backend: PreferredTransferBackend,
    pub total_expected_bytes: Option<usize>,
}

#[derive(Debug)]
pub struct ServingRestoreHandle {
    inner: ConnectorTransferHandle,
}

impl ServingRestoreHandle {
    #[inline(always)]
    pub fn is_pending(&self) -> bool {
        self.inner.is_pending()
    }

    pub fn is_ready(&mut self) -> RuntimeResult<bool> {
        self.inner.is_ready()
    }

    pub fn peek_report(&self) -> ConnectorTransferReport {
        self.inner.peek_report()
    }

    pub fn wait_on_stream(&mut self, stream_ptr: u64) -> RuntimeResult<bool> {
        self.inner.wait_on_stream(stream_ptr)
    }

    pub fn wait(self) -> RuntimeResult<ConnectorTransferReport> {
        self.inner.wait()
    }
}

#[derive(Debug, Clone)]
pub struct ServingKvConnector {
    direct: DirectKvConnector,
}

impl ServingKvConnector {
    pub fn new(cuda: CudaConfig) -> Self {
        Self {
            direct: DirectKvConnector::new(cuda),
        }
    }

    #[inline(always)]
    pub fn direct(&self) -> &DirectKvConnector {
        &self.direct
    }

    pub fn plan_restore<A>(
        &self,
        spec: SessionRestoreSpec,
        allocator: &mut A,
    ) -> RuntimeResult<PlannedRestore>
    where
        A: TransferAllocator,
    {
        let total_expected_bytes = spec.total_expected_bytes();
        let request = spec.to_request();
        let destination = match spec.preferred_backend {
            PreferredTransferBackend::Cpu => {
                TransferDestination::Cpu(allocator.allocate_cpu(total_expected_bytes)?)
            }
            PreferredTransferBackend::Gpu => {
                let gpu = allocator.allocate_gpu(
                    spec.device_ordinal,
                    spec.stream_ordinal,
                    total_expected_bytes,
                )?;
                let cpu_fallback = if spec.allow_cpu_fallback {
                    Some(allocator.allocate_cpu(total_expected_bytes)?)
                } else {
                    None
                };
                TransferDestination::Gpu {
                    target: gpu,
                    cpu_fallback,
                }
            }
        };

        Ok(PlannedRestore {
            request,
            destination,
            preferred_backend: spec.preferred_backend,
            total_expected_bytes,
        })
    }

    pub fn execute_restore(
        &self,
        store: &mut LocalEmbeddedStore,
        plan: PlannedRestore,
    ) -> RuntimeResult<ConnectorTransferReport> {
        self.submit_restore(store, plan)?.wait()
    }

    pub fn submit_restore(
        &self,
        store: &mut LocalEmbeddedStore,
        plan: PlannedRestore,
    ) -> RuntimeResult<ServingRestoreHandle> {
        Ok(ServingRestoreHandle {
            inner: self
                .direct
                .submit_restore_session(store, plan.request, plan.destination)?,
        })
    }

    #[cfg(test)]
    pub(crate) fn execute_restore_with_gpu_engine<E>(
        &self,
        store: &mut LocalEmbeddedStore,
        plan: PlannedRestore,
        engine: &mut E,
    ) -> RuntimeResult<ConnectorTransferReport>
    where
        E: crate::runtime::KvTransferEngine,
    {
        match plan.destination {
            TransferDestination::Gpu { target, .. } => self.direct.restore_session_with_gpu_engine(
                store,
                plan.request,
                crate::runtime::RuntimeTransferTarget::Gpu(target),
                engine,
            ),
            TransferDestination::PagedGpu { target, .. } => {
                self.direct.restore_session_with_gpu_engine(
                    store,
                    plan.request,
                    crate::runtime::RuntimeTransferTarget::PagedGpu(target),
                    engine,
                )
            }
            TransferDestination::Cpu(_) => Err(RuntimeError::Engine(
                "simulated gpu execution requires a gpu planned destination".into(),
            )),
        }
    }

    pub fn restore_with_allocator<A>(
        &self,
        store: &mut LocalEmbeddedStore,
        spec: SessionRestoreSpec,
        allocator: &mut A,
    ) -> RuntimeResult<ConnectorTransferReport>
    where
        A: TransferAllocator,
    {
        let plan = self.plan_restore(spec, allocator)?;
        self.execute_restore(store, plan)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedBufferAllocator {
    cpu: Option<CpuTransferTarget>,
    gpu: Option<GpuTransferTarget>,
}

impl FixedBufferAllocator {
    pub fn new(cpu: Option<CpuTransferTarget>, gpu: Option<GpuTransferTarget>) -> Self {
        Self { cpu, gpu }
    }
}

impl TransferAllocator for FixedBufferAllocator {
    fn allocate_cpu(
        &mut self,
        _total_expected_bytes: Option<usize>,
    ) -> RuntimeResult<CpuTransferTarget> {
        self.cpu
            .clone()
            .ok_or_else(|| RuntimeError::Engine("no cpu transfer target available".into()))
    }

    fn allocate_gpu(
        &mut self,
        _device_ordinal: usize,
        _stream_ordinal: usize,
        _total_expected_bytes: Option<usize>,
    ) -> RuntimeResult<GpuTransferTarget> {
        self.gpu
            .clone()
            .ok_or_else(|| RuntimeError::Engine("no gpu transfer target available".into()))
    }
}

#[cfg(test)]
mod tests {
    use shardmap::cuda::CudaConfig;
    use shardmap::storage::{EmbeddedRouteMode, EmbeddedStore, LocalEmbeddedStoreBootstrap};

    use super::{
        FixedBufferAllocator, PreferredTransferBackend, RestoreChunkSpec, ServingKvConnector,
        SessionRestoreSpec,
    };
    use crate::connector::{TransferBackend, TransferDestination};
    use crate::runtime::{CpuTransferTarget, GpuTransferTarget};
    use crate::test_support::SimulatedGpuEngine;

    #[test]
    fn plan_restore_uses_allocator_and_preserves_total_expected_bytes() {
        let connector = ServingKvConnector::new(CudaConfig::default());
        let mut allocator = FixedBufferAllocator::new(
            Some(CpuTransferTarget {
                allocation_id: 1,
                dst_host_ptr: 0x1000,
                dst_base_offset_bytes: 0,
            }),
            Some(GpuTransferTarget {
                device_ordinal: 2,
                stream_ordinal: 3,
                allocation_id: 4,
                dst_device_ptr: 0x2000,
                dst_base_offset_bytes: 0,
            }),
        );

        let plan = connector
            .plan_restore(
                SessionRestoreSpec {
                    session_prefix: b"s:1".to_vec(),
                    chunks: vec![
                        RestoreChunkSpec::new(b"k0".to_vec(), 0, 0).with_expected_len(4),
                        RestoreChunkSpec::new(b"k1".to_vec(), 1, 8).with_expected_len(8),
                    ],
                    preferred_backend: PreferredTransferBackend::Gpu,
                    device_ordinal: 2,
                    stream_ordinal: 3,
                    allow_cpu_fallback: true,
                },
                &mut allocator,
            )
            .expect("plan should succeed");

        assert_eq!(plan.total_expected_bytes, Some(12));
        match plan.destination {
            TransferDestination::Gpu {
                target,
                cpu_fallback,
            } => {
                assert_eq!(target.device_ordinal, 2);
                assert_eq!(target.stream_ordinal, 3);
                assert!(cpu_fallback.is_some());
            }
            other => panic!("expected gpu destination, got {other:?}"),
        }
    }

    #[test]
    fn restore_with_allocator_executes_cpu_plan() {
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
        let connector = ServingKvConnector::new(CudaConfig::default());
        let mut allocator = FixedBufferAllocator::new(
            Some(CpuTransferTarget {
                allocation_id: 8,
                dst_host_ptr: dst.as_mut_ptr() as u64,
                dst_base_offset_bytes: 0,
            }),
            None,
        );

        let report = connector
            .restore_with_allocator(
                &mut local,
                SessionRestoreSpec::new(
                    session,
                    vec![RestoreChunkSpec::new(b"s:gpu:l:0".to_vec(), 0, 2).with_expected_len(4)],
                    PreferredTransferBackend::Cpu,
                ),
                &mut allocator,
            )
            .expect("restore should succeed");

        assert_eq!(report.backend, TransferBackend::Cpu);
        assert_eq!(&dst[2..6], b"abcd");
    }

    #[test]
    fn simulated_gpu_plan_executes_through_serving_connector() {
        let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
        let bootstrap = LocalEmbeddedStoreBootstrap::from_embedded(store, 1);
        let mut stores = bootstrap.into_stores();
        let mut local = stores.pop().expect("expected local store");
        let session = b"s:1".to_vec();
        local
            .batch_set_session_owned_no_ttl_if_local(
                session.clone(),
                vec![(b"s:gpu:l:0".to_vec(), b"lmno".to_vec())],
            )
            .expect("session write should work");

        let mut simulated_device = vec![0u8; 8];
        let connector = ServingKvConnector::new(CudaConfig::default());
        let mut allocator = FixedBufferAllocator::new(
            None,
            Some(GpuTransferTarget {
                device_ordinal: 2,
                stream_ordinal: 9,
                allocation_id: 41,
                dst_device_ptr: simulated_device.as_mut_ptr() as u64,
                dst_base_offset_bytes: 0,
            }),
        );

        let plan = connector
            .plan_restore(
                SessionRestoreSpec {
                    session_prefix: session,
                    chunks: vec![
                        RestoreChunkSpec::new(b"s:gpu:l:0".to_vec(), 0, 1).with_expected_len(4),
                    ],
                    preferred_backend: PreferredTransferBackend::Gpu,
                    device_ordinal: 2,
                    stream_ordinal: 9,
                    allow_cpu_fallback: false,
                },
                &mut allocator,
            )
            .expect("plan should succeed");

        let mut engine = SimulatedGpuEngine::default();
        let report = connector
            .execute_restore_with_gpu_engine(&mut local, plan, &mut engine)
            .expect("simulated gpu execute should succeed");

        assert_eq!(report.backend, TransferBackend::Gpu);
        assert_eq!(report.summary.hit_chunks, 1);
        assert_eq!(engine.device_ordinal, Some(2));
        assert_eq!(engine.stream_ordinal, Some(9));
        assert_eq!(&simulated_device[1..5], b"lmno");
    }
}
