use crossbeam_utils::CachePadded;
#[cfg(not(feature = "embedded-read-biased-lock"))]
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
#[cfg(feature = "embedded-read-biased-lock")]
use rblock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
#[cfg(any(feature = "telemetry", feature = "scnp-tls"))]
use std::sync::Arc;
#[cfg(feature = "scnp-tls")]
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
#[cfg(feature = "redis")]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "telemetry")]
use std::time::Instant;

use crate::config::EvictionPolicy;
use crate::storage::{
    Bytes, ObjectOverflowRuntime, PackedBatch, PreparedPointKey, SemanticCacheError,
    SemanticEmbedding, SemanticMatch, StoredEntry, hash_key, hash_key_tag_from_hash, now_millis,
    validate_similarity_threshold,
};
#[cfg(feature = "telemetry")]
use crate::storage::{CacheTelemetry, CacheTelemetryHandle, TelemetryRuntime};
#[cfg(feature = "redis")]
use crate::storage::{
    RedisObjectArrayItem, RedisObjectBucket, RedisObjectError, RedisObjectReadOutcome,
    RedisObjectResult, RedisObjectStore, RedisObjectValue, RedisObjectWriteAttempt,
    RedisObjectZSetRangeItem, RedisStringLookup,
};
#[cfg(feature = "embedded")]
use crate::storage::{ShardStatsSnapshot, TierStatsSnapshot};

mod batch;
mod batch_results;
mod core;
#[cfg(feature = "redis")]
#[path = "../redis_compat/storage/embedded_store/key_scan.rs"]
mod key_scan;
mod lifecycle;

#[cfg(feature = "scnp-tls")]
type TlsReloadFn = dyn Fn() -> crate::Result<Arc<rustls::ServerConfig>> + Send + Sync;

/// Atomically reloadable TLS identity and client authorization for overflow listeners.
#[cfg(feature = "scnp-tls")]
pub(crate) struct OverflowReplicaTlsRuntime {
    current: parking_lot::RwLock<Arc<rustls::ServerConfig>>,
    reload: Arc<TlsReloadFn>,
    next_reload_ms: AtomicU64,
    reload_interval_ms: u64,
    handshake_timeout: std::time::Duration,
    client_cert_sha256: Arc<[[u8; 32]]>,
    handshake_limiter: Arc<tokio::sync::Semaphore>,
}

#[cfg(feature = "scnp-tls")]
impl std::fmt::Debug for OverflowReplicaTlsRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OverflowReplicaTlsRuntime")
            .field("reload_interval_ms", &self.reload_interval_ms)
            .field("handshake_timeout", &self.handshake_timeout)
            .field("authorized_clients", &self.client_cert_sha256.len())
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "scnp-tls")]
impl OverflowReplicaTlsRuntime {
    pub(crate) fn new(
        current: Arc<rustls::ServerConfig>,
        reload: Arc<TlsReloadFn>,
        reload_interval_ms: u64,
        handshake_timeout: std::time::Duration,
        client_cert_sha256: Arc<[[u8; 32]]>,
        max_concurrent_handshakes: usize,
    ) -> Self {
        Self {
            current: parking_lot::RwLock::new(current),
            reload,
            next_reload_ms: AtomicU64::new(now_millis().saturating_add(reload_interval_ms)),
            reload_interval_ms,
            handshake_timeout,
            client_cert_sha256,
            handshake_limiter: Arc::new(tokio::sync::Semaphore::new(max_concurrent_handshakes)),
        }
    }

    pub(crate) fn server_config(&self) -> Arc<rustls::ServerConfig> {
        let now = now_millis();
        let next = self.next_reload_ms.load(AtomicOrdering::Acquire);
        if now >= next
            && self
                .next_reload_ms
                .compare_exchange(
                    next,
                    now.saturating_add(self.reload_interval_ms),
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
        {
            match (self.reload)() {
                Ok(config) => *self.current.write() = config,
                Err(error) => {
                    tracing::warn!("SCNP TLS reload failed; retaining prior config: {error}")
                }
            }
        }
        Arc::clone(&self.current.read())
    }

    pub(crate) fn handshake_timeout(&self) -> std::time::Duration {
        self.handshake_timeout
    }

    pub(crate) fn try_handshake_permit(&self) -> crate::Result<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&self.handshake_limiter)
            .try_acquire_owned()
            .map_err(|_| {
                crate::ShardCacheError::Protocol("SCNP TLS handshake capacity exhausted".into())
            })
    }

    pub(crate) fn authorize_client(
        &self,
        certificates: Option<&[rustls::pki_types::CertificateDer<'_>]>,
    ) -> crate::Result<()> {
        use sha2::{Digest, Sha256};

        if self.client_cert_sha256.is_empty() {
            return Ok(());
        }
        let certificate = certificates
            .and_then(|certs| certs.first())
            .ok_or_else(|| {
                crate::ShardCacheError::Protocol("SCNP mTLS client certificate is required".into())
            })?;
        let actual: [u8; 32] = Sha256::digest(certificate.as_ref()).into();
        let authorized = self.client_cert_sha256.iter().any(|expected| {
            expected
                .iter()
                .zip(actual.iter())
                .fold(0u8, |difference, (left, right)| difference | (left ^ right))
                == 0
        });
        if !authorized {
            return Err(crate::ShardCacheError::Protocol(
                "SCNP mTLS client identity is not authorized".into(),
            ));
        }
        Ok(())
    }
}
#[cfg(feature = "redis-modules")]
#[path = "../redis_compat/storage/embedded_store/modules.rs"]
mod modules;
#[cfg(feature = "redis")]
#[path = "../redis_compat/storage/embedded_store/objects.rs"]
mod objects;
mod owned;
mod point;
mod routing;
mod semantic;
mod session_slots;
mod shard;
mod shard_arc;
mod views;
mod write;

#[cfg(feature = "redis")]
pub(crate) use key_scan::{DEFAULT_SCAN_COUNT, RedisKeyScanType};
#[cfg(feature = "redis-module-timeseries")]
pub(crate) use modules::TimeSeriesMultiRangeWriter;
#[cfg(feature = "redis-module-topk")]
pub(crate) use modules::TopKError;
#[cfg(feature = "redis-modules")]
pub use modules::{RedisModuleApi, RedisModuleApiResult, RedisModuleFamily};
#[cfg(feature = "redis")]
pub(crate) use objects::{
    RedisHashStore, RedisKeyStore, RedisListStore, RedisObjectStoreAccess, RedisSetStore,
    RedisStringStore, RedisZSetStore,
};
pub use owned::{
    EmbeddedShardHandle, OwnedEmbeddedShard, OwnedEmbeddedWorkerReadSession,
    OwnedEmbeddedWorkerShards,
};
#[cfg(feature = "unsafe")]
use routing::can_skip_session_lookup;
pub use routing::{
    EmbeddedKeyRoute, EmbeddedRouteMode, EmbeddedSessionRoute, shift_for, stripe_index,
};
pub(crate) use routing::{
    assert_valid_shard_count, compute_key_route, compute_session_shard, overflow_slot_shard,
    route_hash_for_shard,
};
use routing::{
    batch_derived_session_storage_prefix, can_route_with_key_hash, can_use_route_hash_as_key_hash,
    derived_session_storage_prefix, point_write_session_storage_prefix, session_route_prefix,
    uses_flat_key_storage,
};
pub use session_slots::PackedSessionWrite;
pub(crate) use session_slots::SessionSlotMap;
pub(crate) use shard::EmbeddedShard;
#[doc(hidden)]
pub use shard_arc::ShardArcEmbeddedStore;
pub use views::{
    EmbeddedBatchReadView, EmbeddedReadSlice, EmbeddedReadView, EmbeddedRef, EmbeddedRefMut,
    EmbeddedSessionBatchView, OwnedEmbeddedBatchReadView, OwnedEmbeddedReadView,
    OwnedEmbeddedRefMut, OwnedEmbeddedSessionBatchView, OwnedEmbeddedSessionPackedView,
};

/// Shared embedded in-memory database.
///
/// `EmbeddedStore` is internally sharded and can be shared across threads. It
/// offers byte-string key/value methods, TTL management, batch reads and
/// writes, and session-oriented packed transfer APIs. Redis/Valkey object
/// helpers are available with the `redis` feature.
#[derive(Debug)]
pub struct EmbeddedStore {
    shards: Box<[CachePadded<RwLock<EmbeddedShard>>]>,
    #[cfg(feature = "redis")]
    string_key_counts: Box<[CachePadded<AtomicUsize>]>,
    shift: u32,
    #[cfg(feature = "redis")]
    objects: RedisObjectStore,
    #[cfg(feature = "redis-modules")]
    module_state: modules::RedisModuleState,
    #[cfg(feature = "redis-module-topk")]
    topk: modules::TopKStore,
    route_mode: EmbeddedRouteMode,
    overflow_replica_topology: RwLock<Option<(String, u16)>>,
    overflow_replica_auth: RwLock<Option<Box<[u8]>>>,
    #[cfg(feature = "scnp-tls")]
    overflow_replica_tls: RwLock<Option<Arc<OverflowReplicaTlsRuntime>>>,
    #[cfg(feature = "telemetry")]
    metrics: Option<Arc<CacheTelemetry>>,
}

#[inline(always)]
fn reserve_batch_capacity(buffer: &mut Vec<u8>, next_len: usize, item_count: usize) {
    if buffer.capacity() == 0 && next_len > 0 {
        // Reserve for the whole batch on the first hit so large chunk reads avoid
        // repeated reallocations while packing the response payload.
        buffer.reserve(next_len.saturating_mul(item_count));
    }
}

#[inline(always)]
#[cfg(feature = "embedded")]
fn accumulate_tier_stats(target: &mut TierStatsSnapshot, snapshot: &TierStatsSnapshot) {
    target.len = target.len.saturating_add(snapshot.len);
    target.capacity = target.capacity.saturating_add(snapshot.capacity);
    target.hits = target.hits.saturating_add(snapshot.hits);
    target.misses = target.misses.saturating_add(snapshot.misses);
    target.promotions = target.promotions.saturating_add(snapshot.promotions);
    target.demotions = target.demotions.saturating_add(snapshot.demotions);
    target.evictions = target.evictions.saturating_add(snapshot.evictions);
    target.expirations = target.expirations.saturating_add(snapshot.expirations);
}

#[inline(always)]
fn write_now_ms(ttl_ms: Option<u64>, memory_limit_bytes: Option<usize>) -> u64 {
    if ttl_ms.is_some() || memory_limit_bytes.is_some() {
        now_millis()
    } else {
        0
    }
}

#[inline(always)]
fn write_resp_blob_string_into(out: &mut bytes::BytesMut, value: &[u8]) {
    #[cfg(not(feature = "unsafe"))]
    {
        let mut buf = itoa::Buffer::new();
        let len_str = buf.format(value.len()).as_bytes();
        out.extend_from_slice(b"$");
        out.extend_from_slice(len_str);
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(value);
        out.extend_from_slice(b"\r\n");
    }
    #[cfg(feature = "unsafe")]
    {
        if value.len() == 64 {
            const HEADER: &[u8] = b"$64\r\n";
            let total = HEADER.len() + 64 + 2;
            out.reserve(total);
            // SAFETY: reserve(total) ensures `total` bytes of spare capacity.
            unsafe {
                let start = out.len();
                let dst = out.as_mut_ptr().add(start);
                std::ptr::copy_nonoverlapping(HEADER.as_ptr(), dst, HEADER.len());
                std::ptr::copy_nonoverlapping(value.as_ptr(), dst.add(HEADER.len()), 64);
                *dst.add(HEADER.len() + 64) = b'\r';
                *dst.add(HEADER.len() + 65) = b'\n';
                out.set_len(start + total);
            }
            return;
        }

        let mut buf = itoa::Buffer::new();
        let len_str = buf.format(value.len()).as_bytes();
        let total = 1 + len_str.len() + 2 + value.len() + 2;
        out.reserve(total);
        // SAFETY: reserve(total) ensures `total` bytes of spare capacity.
        unsafe {
            let start = out.len();
            let dst = out.as_mut_ptr().add(start);
            *dst = b'$';
            let mut pos = 1usize;
            std::ptr::copy_nonoverlapping(len_str.as_ptr(), dst.add(pos), len_str.len());
            pos += len_str.len();
            *dst.add(pos) = b'\r';
            *dst.add(pos + 1) = b'\n';
            pos += 2;
            std::ptr::copy_nonoverlapping(value.as_ptr(), dst.add(pos), value.len());
            pos += value.len();
            *dst.add(pos) = b'\r';
            *dst.add(pos + 1) = b'\n';
            out.set_len(start + total);
        }
    }
}

#[cfg(test)]
mod tests;
