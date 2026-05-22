use fast_cache_core::cuda::CudaConfig;
use fast_cache_core::storage::LocalEmbeddedStore;
use serde::{Deserialize, Serialize};

use crate::gpu_direct_api::GpuDirectApiVersion;
use crate::gpu_queue::GpuDirectQueueConfig;
use crate::{
    CpuTransferTarget, RuntimeResult, VllmConnectorLoadSpec, VllmKvConnector, VllmRestoreHandle,
    VllmRestoreReport,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuDirectProxyConfig {
    pub api_version: GpuDirectApiVersion,
    pub queue: GpuDirectQueueConfig,
}

impl Default for GpuDirectProxyConfig {
    fn default() -> Self {
        Self {
            api_version: GpuDirectApiVersion::V0,
            queue: GpuDirectQueueConfig::default(),
        }
    }
}

#[derive(Debug)]
pub struct GpuDirectProxy {
    connector: VllmKvConnector,
    config: GpuDirectProxyConfig,
}

impl GpuDirectProxy {
    pub fn new(cuda: CudaConfig) -> Self {
        Self::with_config(cuda, GpuDirectProxyConfig::default())
    }

    pub fn with_config(cuda: CudaConfig, config: GpuDirectProxyConfig) -> Self {
        Self {
            connector: VllmKvConnector::new(cuda),
            config,
        }
    }

    #[inline(always)]
    pub fn config(&self) -> &GpuDirectProxyConfig {
        &self.config
    }

    pub fn submit_vllm_restore(
        &self,
        store: &mut LocalEmbeddedStore,
        spec: VllmConnectorLoadSpec,
        cpu_fallback: Option<CpuTransferTarget>,
    ) -> RuntimeResult<VllmRestoreHandle> {
        let plan = spec.plan(&self.connector, cpu_fallback)?;
        self.connector.submit_restore(store, plan)
    }

    pub fn restore_vllm(
        &self,
        store: &mut LocalEmbeddedStore,
        spec: VllmConnectorLoadSpec,
        cpu_fallback: Option<CpuTransferTarget>,
    ) -> RuntimeResult<VllmRestoreReport> {
        self.submit_vllm_restore(store, spec, cpu_fallback)?.wait()
    }
}
