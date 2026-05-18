use fast_cache::cuda::CudaConfig;
use serde::{Deserialize, Serialize};

use crate::runtime::{RuntimeError, RuntimeResult};

/// How the runtime should hand host-resident KV bytes to the GPU transfer
/// engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HostTransferPath {
    /// The transfer engine should DMA directly from the store-owned host slice.
    #[default]
    DirectHostDma,
    /// The runtime should first copy into a staging buffer owned by the GPU
    /// transfer subsystem. The eventual CUDA implementation will back this with
    /// page-locked host memory.
    PinnedStaging,
}

/// Policy for choosing between direct host DMA and staging-copy paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct HostTransferPolicy {
    pub prefer_direct_host_dma: bool,
    pub pinned_staging_threshold_bytes: usize,
}

impl Default for HostTransferPolicy {
    fn default() -> Self {
        Self {
            prefer_direct_host_dma: true,
            pinned_staging_threshold_bytes: 2 * 1024 * 1024,
        }
    }
}

impl HostTransferPolicy {
    #[inline(always)]
    pub fn path_for_len(&self, len: usize) -> HostTransferPath {
        if !self.prefer_direct_host_dma {
            return HostTransferPath::PinnedStaging;
        }
        if self.pinned_staging_threshold_bytes > 0 && len >= self.pinned_staging_threshold_bytes {
            HostTransferPath::PinnedStaging
        } else {
            HostTransferPath::DirectHostDma
        }
    }
}

impl From<&CudaConfig> for HostTransferPolicy {
    fn from(value: &CudaConfig) -> Self {
        Self {
            prefer_direct_host_dma: value.prefer_direct_host_dma,
            pinned_staging_threshold_bytes: value.pinned_staging_threshold_bytes,
        }
    }
}

/// Owned staging buffer for the host-to-device path.
///
/// The default implementation uses heap memory. Linux CUDA builds can also back
/// this with page-locked host allocations.
#[derive(Debug, PartialEq, Eq)]
pub struct StagedHostBuffer {
    storage: StagedHostBufferStorage,
}

#[derive(Debug, PartialEq, Eq)]
enum StagedHostBufferStorage {
    Heap(Box<[u8]>),
    #[cfg(all(feature = "cuda", target_os = "linux"))]
    Pinned(CudaPinnedHostBuffer),
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
#[derive(Debug)]
struct CudaPinnedHostBuffer {
    buffer: cust::memory::LockedBuffer<u8>,
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
impl CudaPinnedHostBuffer {
    fn alloc_and_copy(bytes: &[u8]) -> RuntimeResult<Self> {
        let buffer = cust::memory::LockedBuffer::from_slice(bytes)
            .map_err(|err| RuntimeError::Staging(format!("LockedBuffer::from_slice: {err}")))?;
        Ok(Self { buffer })
    }

    #[inline(always)]
    fn as_slice(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
impl PartialEq for CudaPinnedHostBuffer {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
impl Eq for CudaPinnedHostBuffer {}

impl Default for StagedHostBuffer {
    fn default() -> Self {
        Self {
            storage: StagedHostBufferStorage::Heap(Box::default()),
        }
    }
}

impl StagedHostBuffer {
    #[inline(always)]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            storage: StagedHostBufferStorage::Heap(bytes.to_vec().into_boxed_slice()),
        }
    }

    #[inline(always)]
    pub fn from_owned_bytes(bytes: Vec<u8>) -> Self {
        Self {
            storage: StagedHostBufferStorage::Heap(bytes.into_boxed_slice()),
        }
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        match &self.storage {
            StagedHostBufferStorage::Heap(bytes) => bytes,
            #[cfg(all(feature = "cuda", target_os = "linux"))]
            StagedHostBufferStorage::Pinned(buffer) => buffer.as_slice(),
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn as_ptr(&self) -> *const u8 {
        self.as_slice().as_ptr()
    }

    #[inline(always)]
    pub fn is_pinned(&self) -> bool {
        #[cfg(all(feature = "cuda", target_os = "linux"))]
        {
            matches!(self.storage, StagedHostBufferStorage::Pinned(_))
        }
        #[cfg(not(all(feature = "cuda", target_os = "linux")))]
        {
            false
        }
    }
}

pub trait HostStagingPool {
    fn stage(&mut self, bytes: &[u8]) -> RuntimeResult<StagedHostBuffer>;
}

/// Minimal heap-backed staging pool used for tests and CPU-only integration
/// bring-up. This preserves the runtime shape without requiring CUDA today.
#[derive(Debug, Default)]
pub struct HeapStagingPool {
    staged_buffers: usize,
    staged_bytes: usize,
}

impl HeapStagingPool {
    #[inline(always)]
    pub fn staged_buffers(&self) -> usize {
        self.staged_buffers
    }

    #[inline(always)]
    pub fn staged_bytes(&self) -> usize {
        self.staged_bytes
    }
}

impl HostStagingPool for HeapStagingPool {
    fn stage(&mut self, bytes: &[u8]) -> RuntimeResult<StagedHostBuffer> {
        self.staged_buffers = self.staged_buffers.saturating_add(1);
        self.staged_bytes = self.staged_bytes.saturating_add(bytes.len());
        Ok(StagedHostBuffer::from_bytes(bytes))
    }
}

/// Staging pool that rejects staging requests. Useful when the caller wants to
/// enforce a direct-DMA-only hot path.
#[derive(Debug, Default)]
pub struct RejectingStagingPool;

impl HostStagingPool for RejectingStagingPool {
    fn stage(&mut self, _bytes: &[u8]) -> RuntimeResult<StagedHostBuffer> {
        Err(RuntimeError::Staging(
            "staging was requested but no staging pool is available".into(),
        ))
    }
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
#[derive(Debug, Default)]
pub struct CudaPinnedStagingPool {
    staged_buffers: usize,
    staged_bytes: usize,
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
impl CudaPinnedStagingPool {
    #[inline(always)]
    pub fn staged_buffers(&self) -> usize {
        self.staged_buffers
    }

    #[inline(always)]
    pub fn staged_bytes(&self) -> usize {
        self.staged_bytes
    }
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
impl HostStagingPool for CudaPinnedStagingPool {
    fn stage(&mut self, bytes: &[u8]) -> RuntimeResult<StagedHostBuffer> {
        self.staged_buffers = self.staged_buffers.saturating_add(1);
        self.staged_bytes = self.staged_bytes.saturating_add(bytes.len());
        Ok(StagedHostBuffer {
            storage: StagedHostBufferStorage::Pinned(CudaPinnedHostBuffer::alloc_and_copy(bytes)?),
        })
    }
}

#[cfg(test)]
mod tests {
    use fast_cache::cuda::CudaConfig;

    use super::{HeapStagingPool, HostStagingPool, HostTransferPath, HostTransferPolicy};

    #[test]
    fn transfer_policy_prefers_direct_for_small_chunks() {
        let policy = HostTransferPolicy {
            prefer_direct_host_dma: true,
            pinned_staging_threshold_bytes: 4096,
        };
        assert_eq!(policy.path_for_len(1024), HostTransferPath::DirectHostDma);
        assert_eq!(policy.path_for_len(4096), HostTransferPath::PinnedStaging);
    }

    #[test]
    fn transfer_policy_can_be_derived_from_cuda_config() {
        let config = CudaConfig {
            prefer_direct_host_dma: false,
            pinned_staging_threshold_bytes: 8192,
            ..CudaConfig::default()
        };
        let policy = HostTransferPolicy::from(&config);
        assert_eq!(policy.path_for_len(1), HostTransferPath::PinnedStaging);
    }

    #[test]
    fn heap_staging_pool_copies_bytes() {
        let mut pool = HeapStagingPool::default();
        let staged = pool.stage(b"hello").expect("stage");
        assert_eq!(staged.as_slice(), b"hello");
        assert_eq!(pool.staged_buffers(), 1);
        assert_eq!(pool.staged_bytes(), 5);
    }
}
