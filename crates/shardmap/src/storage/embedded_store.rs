use crossbeam_utils::CachePadded;
#[cfg(not(feature = "embedded-read-biased-lock"))]
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
#[cfg(feature = "embedded-read-biased-lock")]
use rblock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::Arc;
#[cfg(feature = "scnp-tls")]
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
#[cfg(feature = "redis")]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "telemetry")]
use std::time::Instant;

use crate::config::EvictionPolicy;
use crate::storage::{
    Bytes, GovernedRead, ObjectOverflowRuntime, PackedBatch, PreparedPointKey, SemanticCacheError,
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

type AuthReloadFn = dyn Fn() -> crate::Result<Arc<[u8]>> + Send + Sync;

struct OverflowReplicaAuthTokens {
    current: Arc<[u8]>,
    previous: Option<(Arc<[u8]>, std::time::Instant)>,
}

/// Reloadable token set with one overlap window for rolling credential changes.
pub(crate) struct OverflowReplicaAuthRuntime {
    tokens: Arc<parking_lot::RwLock<OverflowReplicaAuthTokens>>,
    shutdown: Option<crossbeam_channel::Sender<()>>,
    reload_thread: parking_lot::Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl std::fmt::Debug for OverflowReplicaAuthRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OverflowReplicaAuthRuntime")
            .field("reloadable", &self.shutdown.is_some())
            .finish_non_exhaustive()
    }
}

impl OverflowReplicaAuthRuntime {
    pub(crate) fn new_static(token: Arc<[u8]>) -> Self {
        Self {
            tokens: Arc::new(parking_lot::RwLock::new(OverflowReplicaAuthTokens {
                current: token,
                previous: None,
            })),
            shutdown: None,
            reload_thread: parking_lot::Mutex::new(None),
        }
    }

    pub(crate) fn new_reloadable(
        token: Arc<[u8]>,
        interval: std::time::Duration,
        reload: Arc<AuthReloadFn>,
    ) -> crate::Result<Self> {
        let tokens = Arc::new(parking_lot::RwLock::new(OverflowReplicaAuthTokens {
            current: token,
            previous: None,
        }));
        let (shutdown, shutdown_rx) = crossbeam_channel::bounded(1);
        let reload_tokens = Arc::clone(&tokens);
        let interval = interval.max(std::time::Duration::from_millis(100));
        let overlap = interval.saturating_mul(2);
        let reload_thread = std::thread::Builder::new()
            .name("shardmap-scnp-auth-reload".into())
            .spawn(move || {
                while shutdown_rx.recv_timeout(interval).is_err() {
                    match reload() {
                        Ok(next) => {
                            let mut tokens = reload_tokens.write();
                            if tokens.current.as_ref() != next.as_ref() {
                                let previous = std::mem::replace(&mut tokens.current, next);
                                tokens.previous = Some((previous, std::time::Instant::now() + overlap));
                            }
                        }
                        Err(error) => tracing::warn!(
                            "SCNP authentication token reload failed; retaining prior token: {error}"
                        ),
                    }
                }
            })
            .map_err(|error| {
                crate::ShardCacheError::Config(format!(
                    "failed to start SCNP authentication reload thread: {error}"
                ))
            })?;
        Ok(Self {
            tokens,
            shutdown: Some(shutdown),
            reload_thread: parking_lot::Mutex::new(Some(reload_thread)),
        })
    }

    pub(crate) fn authorize(&self, supplied: &[u8]) -> bool {
        let tokens = self.tokens.read();
        constant_time_equal(supplied, &tokens.current)
            || tokens
                .previous
                .as_ref()
                .is_some_and(|(previous, deadline)| {
                    *deadline > std::time::Instant::now() && constant_time_equal(supplied, previous)
                })
    }
}

impl Drop for OverflowReplicaAuthRuntime {
    fn drop(&mut self) {
        if let Some(shutdown) = &self.shutdown {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.reload_thread.lock().take() {
            let _ = thread.join();
        }
    }
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

#[cfg(feature = "scnp-tls")]
type TlsReloadFn = dyn Fn() -> crate::Result<Arc<rustls::ServerConfig>> + Send + Sync;

/// Atomically reloadable TLS identity and client authorization for overflow listeners.
#[cfg(feature = "scnp-tls")]
pub(crate) struct OverflowReplicaTlsRuntime {
    state: Arc<OverflowReplicaTlsReloadState>,
    shutdown: crossbeam_channel::Sender<()>,
    reload_thread: parking_lot::Mutex<Option<std::thread::JoinHandle<()>>>,
    reload_interval_ms: u64,
    handshake_timeout: std::time::Duration,
    client_cert_sha256: Arc<[[u8; 32]]>,
    handshake_limiter: Arc<tokio::sync::Semaphore>,
}

#[cfg(feature = "scnp-tls")]
struct OverflowReplicaTlsReloadState {
    current: parking_lot::RwLock<Arc<rustls::ServerConfig>>,
    reload: Arc<TlsReloadFn>,
    successes: AtomicU64,
    failures: AtomicU64,
}

#[cfg(feature = "scnp-tls")]
impl std::fmt::Debug for OverflowReplicaTlsRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OverflowReplicaTlsRuntime")
            .field("reload_interval_ms", &self.reload_interval_ms)
            .field("reload_health", &self.reload_health())
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
        let state = Arc::new(OverflowReplicaTlsReloadState {
            current: parking_lot::RwLock::new(current),
            reload,
            successes: AtomicU64::new(0),
            failures: AtomicU64::new(0),
        });
        let (shutdown, shutdown_rx) = crossbeam_channel::bounded(1);
        let reload_state = Arc::clone(&state);
        let interval = std::time::Duration::from_millis(reload_interval_ms.max(1));
        let reload_thread = std::thread::Builder::new()
            .name("shardmap-scnp-tls-reload".into())
            .spawn(move || {
                while shutdown_rx.recv_timeout(interval).is_err() {
                    match (reload_state.reload)() {
                        Ok(config) => {
                            *reload_state.current.write() = config;
                            reload_state.successes.fetch_add(1, AtomicOrdering::Relaxed);
                        }
                        Err(error) => {
                            reload_state.failures.fetch_add(1, AtomicOrdering::Relaxed);
                            tracing::warn!(
                                "SCNP TLS reload failed; retaining prior config: {error}"
                            );
                        }
                    }
                }
            })
            .expect("SCNP TLS reload thread must start");
        Self {
            state,
            shutdown,
            reload_thread: parking_lot::Mutex::new(Some(reload_thread)),
            reload_interval_ms,
            handshake_timeout,
            client_cert_sha256,
            handshake_limiter: Arc::new(tokio::sync::Semaphore::new(max_concurrent_handshakes)),
        }
    }

    pub(crate) fn server_config(&self) -> Arc<rustls::ServerConfig> {
        Arc::clone(&self.state.current.read())
    }

    pub(crate) fn reload_health(&self) -> (u64, u64) {
        (
            self.state.successes.load(AtomicOrdering::Relaxed),
            self.state.failures.load(AtomicOrdering::Relaxed),
        )
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

#[cfg(feature = "scnp-tls")]
impl Drop for OverflowReplicaTlsRuntime {
    fn drop(&mut self) {
        let _ = self.shutdown.send(());
        if let Some(thread) = self.reload_thread.lock().take()
            && thread.join().is_err()
        {
            tracing::warn!("SCNP TLS reload thread panicked during shutdown");
        }
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
    overflow_replica_auth: RwLock<Option<Arc<OverflowReplicaAuthRuntime>>>,
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
