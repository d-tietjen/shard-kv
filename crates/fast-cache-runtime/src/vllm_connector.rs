use fast_cache_core::storage::{Bytes, FastHashMap};

use crate::{
    CpuTransferTarget, PagedGpuTransferPage, PagedGpuTransferTarget, RuntimeError, RuntimeResult,
    VllmGpuAllocation, VllmKvConnector, VllmPagedChunkSpec, VllmRestorePlan, VllmRestoreRequest,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VllmRequestedPage {
    pub key: Bytes,
    pub layer_index: u32,
    pub page_index: u32,
    pub len_bytes: usize,
}

impl VllmRequestedPage {
    pub fn new<K>(key: K, layer_index: u32, page_index: u32, len_bytes: usize) -> Self
    where
        K: Into<Bytes>,
    {
        Self {
            key: key.into(),
            layer_index,
            page_index,
            len_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VllmBlockAllocation {
    pub block_index: usize,
    pub dst_device_ptr: u64,
    pub block_size_bytes: usize,
}

impl VllmBlockAllocation {
    pub fn new(block_index: usize, dst_device_ptr: u64, block_size_bytes: usize) -> Self {
        Self {
            block_index,
            dst_device_ptr,
            block_size_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VllmTranslatedRestore {
    pub request: VllmRestoreRequest,
    pub gpu_allocation: VllmGpuAllocation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VllmConnectorLoadSpec {
    pub session_prefix: Bytes,
    pub requested_pages: Vec<VllmRequestedPage>,
    pub block_allocations: Vec<VllmBlockAllocation>,
    pub allocation_id: u64,
    pub device_ordinal: usize,
    pub stream_ordinal: usize,
    pub allow_cpu_fallback: bool,
}

impl VllmConnectorLoadSpec {
    fn block_base_offsets(&self) -> FastHashMap<usize, u64> {
        let mut next_offset = 0u64;
        let mut offsets = FastHashMap::default();
        for block in &self.block_allocations {
            offsets.insert(block.block_index, next_offset);
            next_offset = next_offset.saturating_add(block.block_size_bytes as u64);
        }
        offsets
    }

    pub fn new<S>(
        session_prefix: S,
        requested_pages: Vec<VllmRequestedPage>,
        block_allocations: Vec<VllmBlockAllocation>,
    ) -> Self
    where
        S: Into<Bytes>,
    {
        Self {
            session_prefix: session_prefix.into(),
            requested_pages,
            block_allocations,
            allocation_id: 0,
            device_ordinal: 0,
            stream_ordinal: 0,
            allow_cpu_fallback: true,
        }
    }

    #[inline(always)]
    pub fn with_allocation_id(mut self, allocation_id: u64) -> Self {
        self.allocation_id = allocation_id;
        self
    }

    #[inline(always)]
    pub fn with_gpu_target(mut self, device_ordinal: usize, stream_ordinal: usize) -> Self {
        self.device_ordinal = device_ordinal;
        self.stream_ordinal = stream_ordinal;
        self
    }

    #[inline(always)]
    pub fn with_allow_cpu_fallback(mut self, allow_cpu_fallback: bool) -> Self {
        self.allow_cpu_fallback = allow_cpu_fallback;
        self
    }

    #[inline(always)]
    pub fn page_count(&self) -> usize {
        self.requested_pages.len()
    }

    #[inline(always)]
    pub fn block_count(&self) -> usize {
        self.block_allocations.len()
    }

    #[inline(always)]
    pub fn total_requested_bytes(&self) -> usize {
        self.requested_pages
            .iter()
            .map(|page| page.len_bytes)
            .sum::<usize>()
    }

    #[inline(always)]
    pub fn total_allocated_bytes(&self) -> usize {
        self.block_allocations
            .iter()
            .map(|block| block.block_size_bytes)
            .sum::<usize>()
    }

    fn to_packed_cpu_restore_request(&self) -> VllmRestoreRequest {
        let mut next_offset = 0u64;
        let mut pages = Vec::with_capacity(self.requested_pages.len());
        for page in &self.requested_pages {
            pages.push(
                VllmPagedChunkSpec::new(
                    page.key.clone(),
                    page.layer_index,
                    page.page_index,
                    next_offset,
                )
                .with_expected_len(page.len_bytes),
            );
            next_offset = next_offset.saturating_add(page.len_bytes as u64);
        }

        VllmRestoreRequest::new(
            self.session_prefix.clone(),
            pages,
            crate::serving::PreferredTransferBackend::Cpu,
        )
        .with_allow_cpu_fallback(self.allow_cpu_fallback)
    }

    pub fn to_restore_request(&self) -> RuntimeResult<VllmRestoreRequest> {
        let block_offsets = self.block_base_offsets();
        let mut pages = Vec::with_capacity(self.requested_pages.len());
        for page in &self.requested_pages {
            let dst_offset = block_offsets
                .get(&(page.page_index as usize))
                .copied()
                .ok_or_else(|| {
                    RuntimeError::Engine(format!(
                        "missing vllm block allocation for requested page {}",
                        page.page_index
                    ))
                })?;
            pages.push(
                VllmPagedChunkSpec::new(
                    page.key.clone(),
                    page.layer_index,
                    page.page_index,
                    dst_offset,
                )
                .with_expected_len(page.len_bytes),
            );
        }

        Ok(VllmRestoreRequest::new(
            self.session_prefix.clone(),
            pages,
            crate::serving::PreferredTransferBackend::Gpu,
        )
        .with_gpu_target(self.device_ordinal, self.stream_ordinal)
        .with_allow_cpu_fallback(self.allow_cpu_fallback))
    }

    pub fn to_paged_gpu_target(&self) -> RuntimeResult<PagedGpuTransferTarget> {
        let requested = self.total_requested_bytes();
        let allocated = self.total_allocated_bytes();
        if requested > allocated {
            return Err(RuntimeError::Engine(format!(
                "vllm block allocations provide {allocated} bytes but restore needs {requested}"
            )));
        }
        if self.block_allocations.is_empty() && requested != 0 {
            return Err(RuntimeError::Engine(
                "vllm restore has requested pages but no block allocations".into(),
            ));
        }

        Ok(PagedGpuTransferTarget {
            device_ordinal: self.device_ordinal,
            stream_ordinal: self.stream_ordinal,
            allocation_id: self.allocation_id,
            dst_base_offset_bytes: 0,
            pages: self
                .block_allocations
                .iter()
                .map(|block| PagedGpuTransferPage {
                    page_index: block.block_index,
                    dst_device_ptr: block.dst_device_ptr,
                    page_size_bytes: block.block_size_bytes,
                })
                .collect(),
        })
    }

    pub fn translate(&self) -> RuntimeResult<VllmTranslatedRestore> {
        Ok(VllmTranslatedRestore {
            request: self.to_restore_request()?,
            gpu_allocation: VllmGpuAllocation::Paged(self.to_paged_gpu_target()?),
        })
    }

    pub fn plan(
        &self,
        connector: &VllmKvConnector,
        cpu_fallback: Option<CpuTransferTarget>,
    ) -> RuntimeResult<VllmRestorePlan> {
        if self.block_allocations.is_empty() && !self.requested_pages.is_empty() {
            if self.allow_cpu_fallback
                && let Some(cpu_target) = cpu_fallback
            {
                return connector
                    .plan_restore_to_cpu_target(self.to_packed_cpu_restore_request(), cpu_target);
            }
            return Err(RuntimeError::Engine(
                "vllm restore has requested pages but no block allocations".into(),
            ));
        }
        let translated = self.translate()?;
        connector.plan_restore_to_gpu_allocation(
            translated.request,
            translated.gpu_allocation,
            cpu_fallback,
        )
    }
}

#[cfg(test)]
mod tests {
    use fast_cache_core::cuda::CudaConfig;
    use fast_cache_core::storage::{EmbeddedRouteMode, EmbeddedStore, LocalEmbeddedStoreBootstrap};

    use super::{VllmBlockAllocation, VllmConnectorLoadSpec, VllmRequestedPage};
    use crate::test_support::SimulatedGpuEngine;
    use crate::{CpuTransferTarget, TransferBackend, VllmKvConnector};

    #[test]
    fn translation_builds_cumulative_restore_offsets() {
        let spec = VllmConnectorLoadSpec::new(
            b"s:1".to_vec(),
            vec![
                VllmRequestedPage::new(b"k0".to_vec(), 0, 0, 4),
                VllmRequestedPage::new(b"k1".to_vec(), 1, 1, 8),
            ],
            vec![
                VllmBlockAllocation::new(0, 0x1000, 8),
                VllmBlockAllocation::new(1, 0x2000, 8),
            ],
        )
        .with_gpu_target(3, 9);

        let translated = spec.translate().expect("translation should succeed");
        assert_eq!(translated.request.page_count(), 2);
        assert_eq!(translated.request.pages()[0].dst_offset_bytes(), 0);
        assert_eq!(translated.request.pages()[1].dst_offset_bytes(), 8);

        let gpu = match translated.gpu_allocation {
            crate::VllmGpuAllocation::Paged(gpu) => gpu,
            crate::VllmGpuAllocation::Contiguous(_) => panic!("expected paged allocation"),
        };
        assert_eq!(gpu.device_ordinal, 3);
        assert_eq!(gpu.stream_ordinal, 9);
        assert_eq!(gpu.allocation_id, 0);
        assert_eq!(gpu.pages.len(), 2);
        assert_eq!(gpu.pages[1].page_index, 1);
    }

    #[test]
    fn translation_rejects_missing_block_for_requested_page() {
        let spec = VllmConnectorLoadSpec::new(
            b"s:1".to_vec(),
            vec![VllmRequestedPage::new(b"k0".to_vec(), 0, 3, 4)],
            vec![VllmBlockAllocation::new(0, 0x1000, 8)],
        );

        let err = spec
            .to_restore_request()
            .expect_err("missing block allocation should fail");
        assert!(
            matches!(err, crate::RuntimeError::Engine(message) if message.contains("missing vllm block allocation"))
        );
    }

    #[test]
    fn translation_rejects_underallocated_blocks() {
        let spec = VllmConnectorLoadSpec::new(
            b"s:1".to_vec(),
            vec![VllmRequestedPage::new(b"k0".to_vec(), 0, 0, 12)],
            vec![VllmBlockAllocation::new(0, 0x1000, 8)],
        );

        let err = spec
            .to_paged_gpu_target()
            .expect_err("underallocated blocks should fail");
        assert!(
            matches!(err, crate::RuntimeError::Engine(message) if message.contains("provide 8 bytes but restore needs 12"))
        );
    }

    #[test]
    fn translated_plan_executes_against_simulated_paged_gpu_engine() {
        let store = EmbeddedStore::with_route_mode(1, EmbeddedRouteMode::SessionPrefix);
        let bootstrap = LocalEmbeddedStoreBootstrap::from_embedded(store, 1);
        let mut stores = bootstrap.into_stores();
        let mut local = stores.pop().expect("expected local store");
        let session = b"s:1".to_vec();
        local
            .batch_set_session_owned_no_ttl_if_local(
                session.clone(),
                vec![
                    (b"s:gpu:l:0:p:0".to_vec(), b"abcd".to_vec()),
                    (b"s:gpu:l:1:p:1".to_vec(), b"efgh".to_vec()),
                ],
            )
            .expect("session write should work");

        let connector = VllmKvConnector::new(CudaConfig::default());
        let mut block0 = vec![0u8; 4];
        let mut block1 = vec![0u8; 4];
        let plan = VllmConnectorLoadSpec::new(
            session,
            vec![
                VllmRequestedPage::new(b"s:gpu:l:0:p:0".to_vec(), 0, 0, 4),
                VllmRequestedPage::new(b"s:gpu:l:1:p:1".to_vec(), 1, 1, 4),
            ],
            vec![
                VllmBlockAllocation::new(0, block0.as_mut_ptr() as u64, 4),
                VllmBlockAllocation::new(1, block1.as_mut_ptr() as u64, 4),
            ],
        )
        .with_gpu_target(2, 7)
        .with_allocation_id(1234)
        .with_allow_cpu_fallback(false)
        .plan(
            &connector,
            Some(CpuTransferTarget {
                allocation_id: 44,
                dst_host_ptr: 0x1000,
                dst_base_offset_bytes: 0,
            }),
        )
        .expect("plan should succeed");

        let mut engine = SimulatedGpuEngine::default();
        let report = connector
            .execute_restore_with_gpu_engine(&mut local, plan, &mut engine)
            .expect("paged gpu restore should succeed");

        assert_eq!(report.backend(), TransferBackend::Gpu);
        assert_eq!(report.hit_pages(), 2);
        assert_eq!(engine.allocation_id, Some(1234));
        assert_eq!(&block0, b"abcd");
        assert_eq!(&block1, b"efgh");
    }

    #[test]
    fn empty_gpu_block_allocations_can_plan_cpu_fallback() {
        let connector = VllmKvConnector::new(CudaConfig::default());
        let plan = VllmConnectorLoadSpec::new(
            b"s:cpu".to_vec(),
            vec![
                VllmRequestedPage::new(b"k0".to_vec(), 0, 0, 4),
                VllmRequestedPage::new(b"k1".to_vec(), 1, 1, 8),
            ],
            Vec::new(),
        )
        .with_allow_cpu_fallback(true)
        .plan(
            &connector,
            Some(CpuTransferTarget {
                allocation_id: 77,
                dst_host_ptr: 0x1000,
                dst_base_offset_bytes: 32,
            }),
        )
        .expect("cpu fallback plan should succeed");

        assert_eq!(
            plan.preferred_backend(),
            crate::serving::PreferredTransferBackend::Cpu
        );
        assert!(plan.gpu_target().is_none());
        let cpu = plan.cpu_target().expect("expected cpu target");
        assert_eq!(cpu.allocation_id, 77);
        assert_eq!(
            plan.pages()
                .iter()
                .map(|page| page.dst_offset_bytes())
                .collect::<Vec<_>>(),
            vec![0, 4]
        );
    }
}
