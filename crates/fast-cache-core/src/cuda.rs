//! GPU-facing configuration and transfer descriptors.
//!
//! The crate keeps these types in the public API so storage callers can
//! describe KV-cache chunk transfers without depending on a CUDA runtime in the
//! core crate. The actual GPU execution layer is intentionally outside this
//! package.

use serde::{Deserialize, Serialize};

#[cfg(feature = "sharded")]
use crate::storage::LocalEmbeddedReadSlice;
use crate::storage::{Bytes, hash_key};

/// Runtime configuration for the optional CUDA/GPU tier.
///
/// The fields are kept in the core config surface so operators can describe a
/// GPU-tier budget and staging policy in one place even when running on a CPU-
/// only build. The direct CUDA runtime remains feature-gated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CudaConfig {
    pub enabled: bool,
    pub device_ordinal: usize,
    pub hot_tier_bytes: u64,
    pub pinned_host_bytes: u64,
    pub transfer_stream_count: usize,
    pub layer_streaming: bool,
    pub prefer_direct_host_dma: bool,
    pub pinned_staging_threshold_bytes: usize,
    pub allow_cpu_fallback: bool,
}

impl Default for CudaConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            device_ordinal: 0,
            hot_tier_bytes: 10 * 1024 * 1024 * 1024,
            pinned_host_bytes: 512 * 1024 * 1024,
            transfer_stream_count: 4,
            layer_streaming: true,
            prefer_direct_host_dma: true,
            pinned_staging_threshold_bytes: 2 * 1024 * 1024,
            allow_cpu_fallback: true,
        }
    }
}

/// Precomputed routing metadata for a chunk that should be transferred to a GPU
/// destination in layer order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CudaChunkTransferDescriptor {
    key: Bytes,
    key_hash: u64,
    layer_index: u32,
    dst_offset_bytes: u64,
    expected_len: Option<usize>,
}

impl CudaChunkTransferDescriptor {
    pub fn new<K>(key: K, layer_index: u32, dst_offset_bytes: u64) -> Self
    where
        K: Into<Bytes>,
    {
        let key = key.into();
        let key_hash = hash_key(&key);
        Self {
            key,
            key_hash,
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
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    #[inline(always)]
    pub fn key_hash(&self) -> u64 {
        self.key_hash
    }

    #[inline(always)]
    pub fn layer_index(&self) -> u32 {
        self.layer_index
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

/// A session-scoped transfer request for streaming KV chunks in layer order to
/// a GPU-facing consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CudaSessionTransferRequest {
    session_prefix: Bytes,
    chunks: Vec<CudaChunkTransferDescriptor>,
}

impl CudaSessionTransferRequest {
    pub fn new<S>(session_prefix: S, chunks: Vec<CudaChunkTransferDescriptor>) -> Self
    where
        S: Into<Bytes>,
    {
        Self {
            session_prefix: session_prefix.into(),
            chunks,
        }
    }

    #[inline(always)]
    pub fn session_prefix(&self) -> &[u8] {
        &self.session_prefix
    }

    #[inline(always)]
    pub fn chunks(&self) -> &[CudaChunkTransferDescriptor] {
        &self.chunks
    }

    #[inline(always)]
    pub fn item_count(&self) -> usize {
        self.chunks.len()
    }

    #[inline(always)]
    pub fn total_expected_bytes(&self) -> Option<usize> {
        self.chunks
            .iter()
            .map(CudaChunkTransferDescriptor::expected_len)
            .try_fold(0usize, |sum, len| len.map(|len| sum.saturating_add(len)))
    }
}

/// Aggregate outcome for a session-scoped streaming transfer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CudaSessionTransferStats {
    pub requested_chunks: usize,
    pub hit_chunks: usize,
    pub missed_chunks: usize,
    pub transferred_bytes: usize,
}

impl CudaSessionTransferStats {
    #[inline(always)]
    pub fn all_hit(&self) -> bool {
        self.requested_chunks == self.hit_chunks
    }
}

#[cfg(feature = "sharded")]
#[derive(Debug, Clone)]
pub struct CudaChunkTransferHit<'a> {
    descriptor: &'a CudaChunkTransferDescriptor,
    value: LocalEmbeddedReadSlice<'a>,
}

#[cfg(feature = "sharded")]
impl<'a> CudaChunkTransferHit<'a> {
    pub(crate) fn new(
        descriptor: &'a CudaChunkTransferDescriptor,
        value: LocalEmbeddedReadSlice<'a>,
    ) -> Self {
        Self { descriptor, value }
    }

    #[inline(always)]
    pub fn descriptor(&self) -> &'a CudaChunkTransferDescriptor {
        self.descriptor
    }

    #[inline(always)]
    pub fn value(&self) -> LocalEmbeddedReadSlice<'a> {
        self.value.clone()
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        self.value.as_slice()
    }
}

#[cfg(feature = "sharded")]
#[derive(Debug, Clone)]
pub enum CudaSessionChunkEvent<'a> {
    Hit(CudaChunkTransferHit<'a>),
    Miss(&'a CudaChunkTransferDescriptor),
}

#[cfg(feature = "sharded")]
impl<'a> CudaSessionChunkEvent<'a> {
    #[inline(always)]
    pub fn descriptor(&self) -> &'a CudaChunkTransferDescriptor {
        match self {
            Self::Hit(hit) => hit.descriptor(),
            Self::Miss(descriptor) => descriptor,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CudaChunkTransferDescriptor, CudaSessionTransferRequest};

    #[test]
    fn transfer_request_precomputes_hashes_and_expected_bytes() {
        let request = CudaSessionTransferRequest::new(
            b"s:42".to_vec(),
            vec![
                CudaChunkTransferDescriptor::new(b"s:42:l:0".to_vec(), 0, 0).with_expected_len(128),
                CudaChunkTransferDescriptor::new(b"s:42:l:1".to_vec(), 1, 128)
                    .with_expected_len(256),
            ],
        );

        assert_eq!(request.item_count(), 2);
        assert_eq!(request.total_expected_bytes(), Some(384));
        assert_ne!(request.chunks()[0].key_hash(), 0);
        assert_eq!(request.chunks()[1].layer_index(), 1);
        assert_eq!(request.chunks()[1].dst_offset_bytes(), 128);
    }
}
