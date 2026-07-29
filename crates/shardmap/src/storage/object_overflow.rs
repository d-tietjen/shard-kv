#[cfg(feature = "object-overflow")]
use std::collections::BinaryHeap;
#[cfg(all(feature = "object-overflow", unix))]
use std::ffi::CString;
#[cfg(all(feature = "object-overflow", not(unix)))]
use std::fs::OpenOptions;
#[cfg(feature = "object-overflow")]
use std::fs::{self, File};
#[cfg(feature = "object-overflow")]
use std::io::Read;
#[cfg(feature = "object-overflow")]
use std::io::Write;
#[cfg(all(feature = "object-overflow", unix))]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(feature = "object-overflow")]
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes as SharedBytes;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TryRecvError, TrySendError, bounded};
use lz4_flex::{block::decompress_into, compress_prepend_size};
#[cfg(feature = "object-overflow")]
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::{
    MAX_OBJECT_OVERFLOW_COOLDOWN_MS, MAX_OBJECT_OVERFLOW_DEGRADED_THRESHOLD,
    MAX_OBJECT_OVERFLOW_OPERATION_TIMEOUT_MS, MAX_OBJECT_OVERFLOW_QUEUE_CAPACITY,
    MAX_OBJECT_OVERFLOW_RETRIES, MAX_OBJECT_OVERFLOW_RETRY_BACKOFF_MS, MAX_OBJECT_OVERFLOW_WORKERS,
    ObjectOverflowBackend, ObjectOverflowCompression, ObjectOverflowConfig,
    ObjectOverflowFailurePolicy,
};
use crate::{Result, ShardCacheError};

#[cfg(feature = "object-overflow-s3")]
use futures_util::StreamExt;
#[cfg(feature = "object-overflow-s3")]
use object_store::RetryConfig;
#[cfg(feature = "object-overflow-s3")]
use object_store::aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey};
#[cfg(feature = "object-overflow-s3")]
use object_store::client::{Certificate, ClientOptions};
#[cfg(feature = "object-overflow-s3")]
use object_store::path::Path as ObjectStorePath;
#[cfg(feature = "object-overflow-s3")]
use object_store::{ObjectStore as ObjectStoreClient, ObjectStoreExt, PutPayload};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectValueRef {
    pub object_key: String,
    pub len: usize,
    pub stored_len: usize,
    pub compression: ObjectOverflowCompression,
    pub checksum: u32,
}

pub trait ObjectOverflowStore: Send + Sync + 'static {
    fn put_value(&self, object_key: &str, value: &[u8]) -> Result<()>;
    fn get_value(&self, object_key: &str) -> Result<SharedBytes>;
    fn delete_value(&self, object_key: &str) -> Result<()>;
    fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        let _ = prefix;
        Ok(Vec::new())
    }

    fn put_value_with_timeout(
        &self,
        object_key: &str,
        value: &[u8],
        _timeout: Duration,
    ) -> Result<()> {
        self.put_value(object_key, value)
    }

    fn get_value_with_timeout(&self, object_key: &str, _timeout: Duration) -> Result<SharedBytes> {
        self.get_value(object_key)
    }

    fn get_value_bounded_with_timeout(
        &self,
        object_key: &str,
        max_bytes: usize,
        timeout: Duration,
    ) -> Result<SharedBytes> {
        let value = self.get_value_with_timeout(object_key, timeout)?;
        if value.len() > max_bytes {
            return Err(ShardCacheError::ObjectIntegrity(format!(
                "object overflow payload exceeds {max_bytes} bytes"
            )));
        }
        Ok(value)
    }

    fn delete_value_with_timeout(&self, object_key: &str, _timeout: Duration) -> Result<()> {
        self.delete_value(object_key)
    }

    fn list_keys_with_timeout(&self, prefix: &str, _timeout: Duration) -> Result<Vec<String>> {
        self.list_keys(prefix)
    }

    fn list_keys_bounded_with_timeout(
        &self,
        prefix: &str,
        max_keys: usize,
        max_retained_bytes: usize,
        timeout: Duration,
    ) -> Result<Vec<String>> {
        let keys = self.list_keys_with_timeout(prefix, timeout)?;
        validate_key_listing(&keys, max_keys, max_retained_bytes)?;
        Ok(keys)
    }

    fn list_keys_page_bounded_with_timeout(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: usize,
        max_retained_bytes: usize,
        timeout: Duration,
    ) -> Result<ObjectKeyPage> {
        if max_keys == 0 || max_retained_bytes == 0 {
            return Err(ShardCacheError::Config(
                "object overflow key page bounds must be nonzero".into(),
            ));
        }
        let mut keys = self.list_keys_bounded_with_timeout(
            prefix,
            max_keys.saturating_add(1),
            max_retained_bytes,
            timeout,
        )?;
        keys.sort_unstable();
        if let Some(start_after) = start_after {
            keys.retain(|key| key.as_str() > start_after);
        }
        let next_after = (keys.len() > max_keys).then(|| keys[max_keys - 1].clone());
        keys.truncate(max_keys);
        Ok(ObjectKeyPage { keys, next_after })
    }
}

/// A bounded lexicographic page returned by object-store cleanup listings.
#[derive(Debug)]
pub struct ObjectKeyPage {
    /// Keys in this page.
    pub keys: Vec<String>,
    /// Last key to pass as `start_after` when another page may exist.
    pub next_after: Option<String>,
}

#[cfg(feature = "object-overflow")]
const MAX_CLEANUP_KEYS_PER_SCAN: usize = 100_000;
#[cfg(feature = "object-overflow")]
const MAX_CLEANUP_KEY_BYTES_PER_SCAN: usize = 16 * 1024 * 1024;
#[cfg(feature = "object-overflow")]
const MAX_GENERATION_MARKER_BYTES: usize = 64 * 1024;
#[cfg(feature = "object-overflow-s3")]
const MAX_TLS_CA_BUNDLE_BYTES: usize = 1024 * 1024;
#[cfg(feature = "object-overflow")]
const MAX_OBJECT_KEY_BYTES: usize = 4096;
#[cfg(feature = "object-overflow")]
const MAX_UNBOUNDED_ADAPTER_READ_BYTES: usize = 1024 * 1024 * 1024;
#[cfg(all(feature = "object-overflow", unix))]
static FILE_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub struct ObjectOverflowRuntimeOptions {
    pub backend: ObjectOverflowBackend,
    pub min_value_bytes: usize,
    pub offload_min_idle_ticks: u64,
    pub offload_max_frequency: u32,
    pub compression: ObjectOverflowCompression,
    pub zstd_level: i32,
    pub failure_policy: ObjectOverflowFailurePolicy,
    pub max_retries: usize,
    pub retry_backoff: Duration,
    pub operation_timeout: Duration,
    pub worker_threads: usize,
    pub queue_capacity: usize,
    pub degraded_failure_threshold: usize,
    pub degraded_cooldown: Duration,
    pub fetch_on_get: bool,
    pub delete_on_overwrite: bool,
    pub prefix: String,
    pub node_id: String,
    pub generation_id: String,
    pub cleanup_on_start: bool,
    pub cleanup_grace: Duration,
}

#[derive(Clone)]
pub struct ObjectOverflowRuntime {
    workers: Arc<ObjectOverflowWorkerPool>,
    backend: ObjectOverflowBackend,
    min_value_bytes: usize,
    offload_min_idle_ticks: u64,
    offload_max_frequency: u32,
    compression: ObjectOverflowCompression,
    zstd_level: i32,
    failure_policy: ObjectOverflowFailurePolicy,
    operation_timeout: Duration,
    degraded_failure_threshold: usize,
    degraded_cooldown: Duration,
    fetch_on_get: bool,
    delete_on_overwrite: bool,
    prefix: String,
    node_id: String,
    generation_id: String,
    #[cfg(feature = "object-overflow")]
    generation_created_ms: u64,
    consecutive_failures: Arc<AtomicUsize>,
    degraded_until_ms: Arc<AtomicU64>,
    #[cfg(feature = "object-overflow")]
    cleanup_task: Arc<ObjectOverflowCleanupTask>,
}

#[derive(Debug)]
pub(crate) struct ObjectOverflowTicket<T> {
    receiver: Receiver<Result<T>>,
    deadline: Instant,
}

impl<T> ObjectOverflowTicket<T> {
    fn try_result(&self, workers: &ObjectOverflowWorkerPool) -> Option<Result<T>> {
        match self.receiver.try_recv() {
            Ok(result) => {
                if let Err(error) = &result {
                    workers.stats.record_failure(error);
                }
                Some(result)
            }
            Err(TryRecvError::Disconnected) => {
                let error = ShardCacheError::ChannelClosed("object overflow worker response");
                workers.stats.record_failure(&error);
                Some(Err(error))
            }
            Err(TryRecvError::Empty) if Instant::now() >= self.deadline => {
                let error = object_worker_deadline();
                workers.stats.record_failure(&error);
                workers.try_spawn_replacement_worker();
                Some(Err(error))
            }
            Err(TryRecvError::Empty) => None,
        }
    }
}

impl std::fmt::Debug for ObjectOverflowRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectOverflowRuntime")
            .field("backend", &self.backend)
            .field("min_value_bytes", &self.min_value_bytes)
            .field("offload_min_idle_ticks", &self.offload_min_idle_ticks)
            .field("offload_max_frequency", &self.offload_max_frequency)
            .field("compression", &self.compression)
            .field("failure_policy", &self.failure_policy)
            .field("operation_timeout", &self.operation_timeout)
            .field("fetch_on_get", &self.fetch_on_get)
            .field("delete_on_overwrite", &self.delete_on_overwrite)
            .field("prefix", &self.prefix)
            .field("node_id", &self.node_id)
            .field("generation_id", &self.generation_id)
            .finish_non_exhaustive()
    }
}

impl ObjectOverflowRuntime {
    pub fn new(
        store: Arc<dyn ObjectOverflowStore>,
        options: ObjectOverflowRuntimeOptions,
    ) -> Result<Self> {
        Self::validate_options(&options)?;
        #[cfg(feature = "object-overflow")]
        let cleanup_on_start = options.cleanup_on_start;
        #[cfg(feature = "object-overflow")]
        let cleanup_grace = options.cleanup_grace;
        let workers = Arc::new(ObjectOverflowWorkerPool::new(
            store,
            options.worker_threads,
            options.queue_capacity,
            options.max_retries,
            options.retry_backoff,
            options.operation_timeout,
        )?);
        let runtime = Self {
            workers,
            backend: options.backend,
            min_value_bytes: options.min_value_bytes.max(1),
            offload_min_idle_ticks: options.offload_min_idle_ticks,
            offload_max_frequency: options.offload_max_frequency.max(1),
            compression: options.compression,
            zstd_level: options.zstd_level,
            failure_policy: options.failure_policy,
            operation_timeout: options.operation_timeout,
            degraded_failure_threshold: options.degraded_failure_threshold.max(1),
            degraded_cooldown: options.degraded_cooldown,
            fetch_on_get: options.fetch_on_get,
            delete_on_overwrite: options.delete_on_overwrite,
            prefix: options.prefix,
            node_id: options.node_id,
            generation_id: options.generation_id,
            #[cfg(feature = "object-overflow")]
            generation_created_ms: now_ms(),
            consecutive_failures: Arc::new(AtomicUsize::new(0)),
            degraded_until_ms: Arc::new(AtomicU64::new(0)),
            #[cfg(feature = "object-overflow")]
            cleanup_task: Arc::new(ObjectOverflowCleanupTask::default()),
        };
        #[cfg(feature = "object-overflow")]
        if cleanup_on_start {
            runtime.write_generation_marker()?;
            runtime.cleanup_stale_generations(cleanup_grace.as_secs())?;
            runtime.start_cleanup_task(None, cleanup_grace.as_secs())?;
        }
        Ok(runtime)
    }

    fn validate_options(options: &ObjectOverflowRuntimeOptions) -> Result<()> {
        let require = |condition: bool, message: &'static str| {
            condition
                .then_some(())
                .ok_or_else(|| ShardCacheError::Config(message.into()))
        };
        require(
            (1..=MAX_OBJECT_OVERFLOW_WORKERS).contains(&options.worker_threads),
            "object overflow worker_threads is outside the production bound",
        )?;
        require(
            (1..=MAX_OBJECT_OVERFLOW_QUEUE_CAPACITY).contains(&options.queue_capacity),
            "object overflow queue_capacity is outside the production bound",
        )?;
        require(
            options.max_retries <= MAX_OBJECT_OVERFLOW_RETRIES,
            "object overflow max_retries exceeds the production bound",
        )?;
        require(
            !options.retry_backoff.is_zero()
                && options.retry_backoff
                    <= Duration::from_millis(MAX_OBJECT_OVERFLOW_RETRY_BACKOFF_MS),
            "object overflow retry_backoff is outside the production bound",
        )?;
        require(
            !options.operation_timeout.is_zero()
                && options.operation_timeout
                    <= Duration::from_millis(MAX_OBJECT_OVERFLOW_OPERATION_TIMEOUT_MS),
            "object overflow operation_timeout is outside the production bound",
        )?;
        require(
            (1..=MAX_OBJECT_OVERFLOW_DEGRADED_THRESHOLD)
                .contains(&options.degraded_failure_threshold),
            "object overflow degraded_failure_threshold is outside the production bound",
        )?;
        require(
            !options.degraded_cooldown.is_zero()
                && options.degraded_cooldown
                    <= Duration::from_millis(MAX_OBJECT_OVERFLOW_COOLDOWN_MS),
            "object overflow degraded_cooldown is outside the production bound",
        )?;
        require(
            (-7..=22).contains(&options.zstd_level),
            "object overflow zstd_level must be between -7 and 22",
        )?;
        require(
            !options.prefix.trim_matches('/').is_empty(),
            "object overflow prefix must not be empty",
        )?;
        require(
            !options.node_id.trim().is_empty(),
            "object overflow node_id must not be empty",
        )?;
        require(
            !options.generation_id.trim().is_empty(),
            "object overflow generation_id must not be empty",
        )?;
        if options.cleanup_on_start {
            require(
                !options.cleanup_grace.is_zero(),
                "object overflow cleanup_grace must be nonzero",
            )?;
        }
        Ok(())
    }

    pub(crate) fn same_instance(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.workers, &other.workers)
    }

    #[cfg(feature = "object-overflow")]
    pub fn from_config(config: &ObjectOverflowConfig) -> Result<Option<Self>> {
        if !config.enabled {
            return Ok(None);
        }
        let store: Arc<dyn ObjectOverflowStore> = match config.backend {
            ObjectOverflowBackend::File => Arc::new(FileObjectOverflowStore::from_config(config)?),
            ObjectOverflowBackend::S3 => Arc::new(S3ObjectOverflowStore::from_config(config)?),
        };
        let mut options = ObjectOverflowRuntimeOptions::try_from(config)?;
        options.cleanup_on_start = false;
        let runtime = Self::new(store, options)?;
        runtime.write_generation_marker()?;
        if config.cleanup_on_start {
            runtime.cleanup_stale_generations(config.cleanup_grace_seconds)?;
        }
        if config.cleanup_on_start || config.cleanup_interval_seconds > 0 {
            runtime.start_cleanup_task(
                (config.cleanup_interval_seconds > 0)
                    .then(|| Duration::from_secs(config.cleanup_interval_seconds)),
                config.cleanup_grace_seconds,
            )?;
        }
        Ok(Some(runtime))
    }

    #[cfg(not(feature = "object-overflow"))]
    pub fn from_config(config: &ObjectOverflowConfig) -> Result<Option<Self>> {
        match config.enabled {
            false => Ok(None),
            true => Err(ShardCacheError::Config(
                "object_overflow.enabled requires the object-overflow feature".into(),
            )),
        }
    }

    #[inline(always)]
    pub(crate) fn should_offload(&self, value_len: usize) -> bool {
        value_len >= self.min_value_bytes
    }

    #[inline(always)]
    pub(crate) fn should_offload_cold_entry(
        &self,
        value_len: usize,
        last_touch: u64,
        frequency: u32,
        current_tick: u64,
    ) -> bool {
        if !self.should_offload(value_len) {
            return false;
        }
        let idle_ticks = current_tick.saturating_sub(last_touch);
        idle_ticks >= self.offload_min_idle_ticks && frequency <= self.offload_max_frequency
    }

    #[inline(always)]
    pub(crate) fn fetch_on_get(&self) -> bool {
        self.fetch_on_get
    }

    #[inline(always)]
    pub(crate) fn delete_on_overwrite(&self) -> bool {
        self.delete_on_overwrite
    }

    #[inline(always)]
    pub(crate) fn failure_policy(&self) -> ObjectOverflowFailurePolicy {
        self.failure_policy
    }

    pub(crate) fn object_key(&self, shard_id: usize, hash: u64, sequence: u64) -> String {
        format!(
            "{}/{}/{}/shard-{shard_id}/{hash:016x}-{sequence:016x}.bin",
            self.prefix.trim_matches('/'),
            self.node_id,
            self.generation_id
        )
    }

    pub(crate) fn enqueue_put_value(
        &self,
        object_key: &str,
        value: SharedBytes,
    ) -> Result<ObjectOverflowTicket<ObjectValueRef>> {
        if self.is_degraded() {
            self.workers.stats.record_unavailable();
            return Err(ShardCacheError::Persistence(
                "object overflow is degraded; new offloads are paused".into(),
            ));
        }
        self.workers.enqueue_put_value(
            object_key,
            value,
            self.compression,
            self.zstd_level,
            self.operation_timeout,
        )
    }

    pub(crate) fn poll_put_value(
        &self,
        ticket: &ObjectOverflowTicket<ObjectValueRef>,
    ) -> Option<Result<ObjectValueRef>> {
        let result = ticket.try_result(&self.workers)?;
        self.record_operation_result(&result);
        Some(result)
    }

    pub(crate) fn get_value(&self, object: &ObjectValueRef) -> Result<SharedBytes> {
        self.get_value_with_timeout(object, self.operation_timeout)
    }

    pub(crate) fn get_value_with_timeout(
        &self,
        object: &ObjectValueRef,
        timeout: Duration,
    ) -> Result<SharedBytes> {
        let result = self
            .workers
            .get_value(object.clone(), timeout.min(self.operation_timeout));
        self.record_operation_result(&result);
        result
    }

    pub(crate) fn enqueue_delete_value(&self, object_key: &str) -> Result<()> {
        self.workers
            .enqueue_detached_delete(object_key, self.operation_timeout)
    }

    pub(crate) fn health_snapshot(&self) -> ObjectOverflowHealthSnapshot {
        ObjectOverflowHealthSnapshot {
            enabled: true,
            backend: self.backend_name().to_string(),
            degraded: self.is_degraded(),
            queue_capacity: self.workers.queue_capacity,
            queue_depth: self.workers.queue_depth.load(Ordering::Relaxed),
            pending_jobs: self.workers.pending_jobs(),
            active_workers: self.workers.active_workers.load(Ordering::Relaxed),
            live_workers: self.workers.live_workers.load(Ordering::Relaxed),
            worker_threads: self.workers.worker_threads,
            max_worker_threads: self.workers.max_worker_threads,
            worker_capacity_exhausted: self
                .workers
                .worker_capacity_exhausted
                .load(Ordering::Relaxed),
            stats: self.workers.stats.snapshot(),
        }
    }

    fn backend_name(&self) -> &'static str {
        match self.backend {
            ObjectOverflowBackend::File => "file",
            ObjectOverflowBackend::S3 => "s3",
        }
    }

    fn is_degraded(&self) -> bool {
        now_ms() < self.degraded_until_ms.load(Ordering::Acquire)
    }

    fn record_operation_result<T>(&self, result: &Result<T>) {
        match result {
            Ok(_) => {
                self.consecutive_failures.store(0, Ordering::Release);
            }
            Err(_) => {
                let failures = self
                    .consecutive_failures
                    .fetch_add(1, Ordering::AcqRel)
                    .saturating_add(1);
                if failures >= self.degraded_failure_threshold {
                    let cooldown_ms = self.degraded_cooldown.as_millis() as u64;
                    self.degraded_until_ms
                        .store(now_ms().saturating_add(cooldown_ms), Ordering::Release);
                    self.consecutive_failures.store(0, Ordering::Release);
                }
            }
        }
    }

    #[cfg(feature = "object-overflow")]
    fn write_generation_marker(&self) -> Result<()> {
        let marker = GenerationMarker {
            node_id: self.node_id.clone(),
            generation_id: self.generation_id.clone(),
            created_ms: self.generation_created_ms,
            heartbeat_ms: now_ms(),
        };
        let body = serde_json::to_vec(&marker)
            .map_err(|error| ShardCacheError::Persistence(format!("generation marker: {error}")))?;
        let key = self.generation_marker_key(&self.generation_id);
        self.workers
            .put_raw(&key, SharedBytes::from(body), self.operation_timeout)
    }

    #[cfg(feature = "object-overflow")]
    fn cleanup_stale_generations(&self, cleanup_grace_seconds: u64) -> Result<()> {
        cleanup_stale_generations_with(
            &self.workers,
            &self.prefix,
            &self.node_id,
            &self.generation_id,
            self.operation_timeout,
            cleanup_grace_seconds,
            None,
        )
    }

    #[cfg(feature = "object-overflow")]
    fn start_cleanup_task(
        &self,
        cleanup_interval: Option<Duration>,
        cleanup_grace_seconds: u64,
    ) -> Result<()> {
        let mut handle = self.cleanup_task.handle.lock().map_err(|_| {
            ShardCacheError::Persistence("object overflow cleanup handle lock poisoned".into())
        })?;
        if handle.is_some() {
            return Ok(());
        }
        let shutdown = Arc::clone(&self.cleanup_task.shutdown);
        let workers = Arc::clone(&self.workers);
        let prefix = self.prefix.clone();
        let node_id = self.node_id.clone();
        let generation_id = self.generation_id.clone();
        let generation_created_ms = self.generation_created_ms;
        let operation_timeout = self.operation_timeout;
        let heartbeat_interval = cleanup_heartbeat_interval(cleanup_grace_seconds);
        let tick_interval = cleanup_interval.map_or(heartbeat_interval, |cleanup| {
            cleanup.min(heartbeat_interval)
        });
        *handle = Some(
            thread::Builder::new()
                .name("object-overflow-cleanup".into())
                .spawn(move || {
                    let mut last_cleanup = Instant::now();
                    while !shutdown.load(Ordering::Acquire) {
                        let mut slept = Duration::ZERO;
                        while slept < tick_interval && !shutdown.load(Ordering::Acquire) {
                            let step = tick_interval
                                .saturating_sub(slept)
                                .min(Duration::from_secs(1));
                            thread::sleep(step);
                            slept += step;
                        }
                        if shutdown.load(Ordering::Acquire) {
                            break;
                        }
                        let marker = GenerationMarker {
                            node_id: node_id.clone(),
                            generation_id: generation_id.clone(),
                            created_ms: generation_created_ms,
                            heartbeat_ms: now_ms(),
                        };
                        let heartbeat_ok = if let Ok(body) = serde_json::to_vec(&marker) {
                            let marker_key = format!(
                                "{}/{}/{}/_generation.json",
                                prefix.trim_matches('/'),
                                node_id,
                                generation_id
                            );
                            workers
                                .put_raw(&marker_key, SharedBytes::from(body), operation_timeout)
                                .is_ok()
                        } else {
                            false
                        };
                        if !heartbeat_ok {
                            continue;
                        }
                        if cleanup_interval
                            .is_some_and(|interval| last_cleanup.elapsed() >= interval)
                        {
                            let _ = cleanup_stale_generations_with(
                                &workers,
                                &prefix,
                                &node_id,
                                &generation_id,
                                operation_timeout,
                                cleanup_grace_seconds,
                                Some(&shutdown),
                            );
                            last_cleanup = Instant::now();
                        }
                    }
                })
                .map_err(|error| {
                    ShardCacheError::Persistence(format!("object overflow cleanup thread: {error}"))
                })?,
        );
        Ok(())
    }

    #[cfg(feature = "object-overflow")]
    fn generation_marker_key(&self, generation_id: &str) -> String {
        format!(
            "{}/{}/{generation_id}/_generation.json",
            self.prefix.trim_matches('/'),
            self.node_id
        )
    }
}

#[cfg(feature = "object-overflow")]
fn cleanup_heartbeat_interval(cleanup_grace_seconds: u64) -> Duration {
    Duration::from_secs(cleanup_grace_seconds.div_ceil(3).clamp(1, 60))
}

#[cfg(feature = "object-overflow")]
fn cleanup_stale_generations_with(
    workers: &ObjectOverflowWorkerPool,
    prefix: &str,
    node_id: &str,
    generation_id: &str,
    operation_timeout: Duration,
    cleanup_grace_seconds: u64,
    shutdown: Option<&AtomicBool>,
) -> Result<()> {
    let generation_prefix = format!("{}/{}/", prefix.trim_matches('/'), node_id);
    workers.stats.cleanup_scans.fetch_add(1, Ordering::Relaxed);
    let now = now_ms();
    let keys = workers.list_keys(&generation_prefix, operation_timeout)?;
    for key in keys.iter().filter(|key| key.ends_with("/_generation.json")) {
        if shutdown.is_some_and(|shutdown| shutdown.load(Ordering::Acquire)) {
            return Ok(());
        }
        let Some(stale_generation_id) = generation_id_from_marker_key(key) else {
            continue;
        };
        if stale_generation_id == generation_id {
            continue;
        }
        let marker = workers
            .get_raw(key, operation_timeout)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<GenerationMarker>(&bytes).ok());
        let Some(marker) = marker else {
            continue;
        };
        if marker.node_id != node_id || marker.generation_id != stale_generation_id {
            continue;
        }
        let grace_ms = cleanup_grace_seconds.saturating_mul(1000);
        if marker.heartbeat_ms.saturating_add(grace_ms) > now {
            continue;
        }
        let stale_prefix = format!(
            "{}/{stale_generation_id}/",
            generation_prefix.trim_end_matches('/')
        );
        for stale_key in workers.list_keys(&stale_prefix, operation_timeout)? {
            if shutdown.is_some_and(|shutdown| shutdown.load(Ordering::Acquire)) {
                return Ok(());
            }
            workers
                .delete_value(&stale_key, operation_timeout)
                .map(|()| {
                    workers
                        .stats
                        .cleanup_deletes
                        .fetch_add(1, Ordering::Relaxed);
                })?;
        }
    }
    Ok(())
}

#[cfg(feature = "object-overflow")]
fn generation_id_from_marker_key(key: &str) -> Option<&str> {
    let marker_suffix = "/_generation.json";
    let key = key.strip_suffix(marker_suffix)?;
    key.rsplit('/').next()
}

impl TryFrom<&ObjectOverflowConfig> for ObjectOverflowRuntimeOptions {
    type Error = ShardCacheError;

    fn try_from(config: &ObjectOverflowConfig) -> Result<Self> {
        Ok(Self {
            backend: config.backend,
            min_value_bytes: config.min_value_bytes,
            offload_min_idle_ticks: config.offload_min_idle_ticks,
            offload_max_frequency: config.offload_max_frequency,
            compression: config.compression,
            zstd_level: config.zstd_level,
            failure_policy: config.failure_policy,
            max_retries: config.max_retries,
            retry_backoff: Duration::from_millis(config.retry_backoff_ms),
            operation_timeout: Duration::from_millis(config.operation_timeout_ms),
            worker_threads: config.worker_threads,
            queue_capacity: config.queue_capacity,
            degraded_failure_threshold: config.degraded_failure_threshold,
            degraded_cooldown: Duration::from_millis(config.degraded_cooldown_ms),
            fetch_on_get: config.fetch_on_get,
            delete_on_overwrite: config.delete_on_overwrite,
            prefix: config.prefix.clone(),
            node_id: config
                .node_id
                .clone()
                .unwrap_or_else(|| "default-node".to_string()),
            generation_id: new_generation_id()?,
            cleanup_on_start: config.cleanup_on_start,
            cleanup_grace: Duration::from_secs(config.cleanup_grace_seconds),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct ObjectOverflowHealthSnapshot {
    pub enabled: bool,
    pub backend: String,
    pub degraded: bool,
    pub queue_capacity: usize,
    pub queue_depth: usize,
    pub pending_jobs: usize,
    pub active_workers: usize,
    pub live_workers: usize,
    pub worker_threads: usize,
    pub max_worker_threads: usize,
    pub worker_capacity_exhausted: bool,
    pub stats: ObjectOverflowWorkerStatsSnapshot,
}

#[derive(Debug, Clone, Default)]
pub struct ObjectOverflowWorkerStatsSnapshot {
    pub encode_ops: u64,
    pub compress_ops: u64,
    pub upload_ops: u64,
    pub download_ops: u64,
    pub decode_ops: u64,
    pub delete_ops: u64,
    pub retries: u64,
    pub timeouts: u64,
    pub auth_config_failures: u64,
    pub unavailable_failures: u64,
    pub integrity_failures: u64,
    pub not_found_failures: u64,
    pub delete_failures: u64,
    pub cleanup_scans: u64,
    pub cleanup_deletes: u64,
    pub object_bytes_raw: u64,
    pub object_bytes_stored: u64,
    pub encode_latency_us: u64,
    pub upload_latency_us: u64,
    pub download_latency_us: u64,
    pub decode_latency_us: u64,
    pub delete_latency_us: u64,
}

struct ObjectOverflowWorkerPool {
    sender: Sender<ObjectOverflowWorkerJob>,
    receiver: Receiver<ObjectOverflowWorkerJob>,
    store: Arc<dyn ObjectOverflowStore>,
    worker_threads: usize,
    max_worker_threads: usize,
    queue_capacity: usize,
    queue_depth: Arc<AtomicUsize>,
    active_workers: Arc<AtomicUsize>,
    live_workers: Arc<AtomicUsize>,
    next_worker_index: AtomicUsize,
    worker_capacity_exhausted: Arc<AtomicBool>,
    stats: Arc<ObjectOverflowWorkerStats>,
    max_retries: usize,
    retry_backoff: Duration,
    operation_timeout: Duration,
    authentication_key: [u8; 32],
    workers: Mutex<Vec<thread::JoinHandle<()>>>,
}

#[cfg(feature = "object-overflow")]
#[derive(Default)]
struct ObjectOverflowCleanupTask {
    shutdown: Arc<AtomicBool>,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
}

#[cfg(feature = "object-overflow")]
impl Drop for ObjectOverflowCleanupTask {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        let handle = self
            .handle
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }
}

enum ObjectOverflowWorkerJob {
    Put {
        object_key: String,
        raw: SharedBytes,
        compression: ObjectOverflowCompression,
        zstd_level: i32,
        deadline: Instant,
        reply: Sender<Result<ObjectValueRef>>,
    },
    #[cfg(feature = "object-overflow")]
    PutRaw {
        object_key: String,
        value: SharedBytes,
        deadline: Instant,
        reply: Sender<Result<()>>,
    },
    Get {
        object: ObjectValueRef,
        deadline: Instant,
        reply: Sender<Result<SharedBytes>>,
    },
    #[cfg(feature = "object-overflow")]
    GetRaw {
        object_key: String,
        deadline: Instant,
        reply: Sender<Result<SharedBytes>>,
    },
    #[cfg(any(feature = "object-overflow", test))]
    Delete {
        object_key: String,
        deadline: Instant,
        reply: Sender<Result<()>>,
    },
    DeleteDetached {
        object_key: String,
        deadline: Instant,
    },
    #[cfg(feature = "object-overflow")]
    List {
        prefix: String,
        max_keys: usize,
        max_retained_bytes: usize,
        deadline: Instant,
        reply: Sender<Result<Vec<String>>>,
    },
    Stop,
}

#[derive(Default)]
struct ObjectOverflowWorkerStats {
    encode_ops: AtomicU64,
    compress_ops: AtomicU64,
    upload_ops: AtomicU64,
    download_ops: AtomicU64,
    decode_ops: AtomicU64,
    delete_ops: AtomicU64,
    retries: AtomicU64,
    timeouts: AtomicU64,
    auth_config_failures: AtomicU64,
    unavailable_failures: AtomicU64,
    integrity_failures: AtomicU64,
    not_found_failures: AtomicU64,
    delete_failures: AtomicU64,
    cleanup_scans: AtomicU64,
    cleanup_deletes: AtomicU64,
    object_bytes_raw: AtomicU64,
    object_bytes_stored: AtomicU64,
    encode_latency_us: AtomicU64,
    upload_latency_us: AtomicU64,
    download_latency_us: AtomicU64,
    decode_latency_us: AtomicU64,
    delete_latency_us: AtomicU64,
}

impl ObjectOverflowWorkerPool {
    fn new(
        store: Arc<dyn ObjectOverflowStore>,
        worker_threads: usize,
        queue_capacity: usize,
        max_retries: usize,
        retry_backoff: Duration,
        operation_timeout: Duration,
    ) -> Result<Self> {
        let (sender, receiver) = bounded(queue_capacity);
        let queue_depth = Arc::new(AtomicUsize::new(0));
        let active_workers = Arc::new(AtomicUsize::new(0));
        let stats = Arc::new(ObjectOverflowWorkerStats::default());
        let pool = Self {
            sender,
            receiver,
            store,
            worker_threads,
            max_worker_threads: worker_threads.saturating_mul(2),
            queue_capacity,
            queue_depth,
            active_workers,
            live_workers: Arc::new(AtomicUsize::new(0)),
            next_worker_index: AtomicUsize::new(0),
            worker_capacity_exhausted: Arc::new(AtomicBool::new(false)),
            stats,
            max_retries,
            retry_backoff,
            operation_timeout,
            authentication_key: secure_random_32()?,
            workers: Mutex::new(Vec::with_capacity(worker_threads.saturating_mul(2))),
        };
        for _ in 0..worker_threads {
            if !pool.try_spawn_worker()? {
                return Err(ShardCacheError::Persistence(
                    "object overflow could not reserve an initial worker".into(),
                ));
            }
        }
        Ok(pool)
    }

    #[cfg(test)]
    fn put_value(
        &self,
        object_key: &str,
        raw: SharedBytes,
        compression: ObjectOverflowCompression,
        zstd_level: i32,
        timeout: Duration,
    ) -> Result<ObjectValueRef> {
        let (reply, receiver) = bounded(1);
        let deadline = self.operation_deadline(timeout);
        self.enqueue(ObjectOverflowWorkerJob::Put {
            object_key: object_key.to_string(),
            raw,
            compression,
            zstd_level,
            deadline,
            reply,
        })?;
        self.recv(
            receiver,
            deadline.saturating_duration_since(Instant::now()),
            "put",
        )
    }

    fn enqueue_put_value(
        &self,
        object_key: &str,
        raw: SharedBytes,
        compression: ObjectOverflowCompression,
        zstd_level: i32,
        timeout: Duration,
    ) -> Result<ObjectOverflowTicket<ObjectValueRef>> {
        let (reply, receiver) = bounded(1);
        let deadline = self.operation_deadline(timeout);
        self.enqueue(ObjectOverflowWorkerJob::Put {
            object_key: object_key.to_string(),
            raw,
            compression,
            zstd_level,
            deadline,
            reply,
        })?;
        Ok(ObjectOverflowTicket { receiver, deadline })
    }

    #[cfg(feature = "object-overflow")]
    fn put_raw(&self, object_key: &str, value: SharedBytes, timeout: Duration) -> Result<()> {
        let (reply, receiver) = bounded(1);
        let deadline = self.operation_deadline(timeout);
        self.enqueue(ObjectOverflowWorkerJob::PutRaw {
            object_key: object_key.to_string(),
            value,
            deadline,
            reply,
        })?;
        self.recv(
            receiver,
            deadline.saturating_duration_since(Instant::now()),
            "put raw",
        )
    }

    fn get_value(&self, object: ObjectValueRef, timeout: Duration) -> Result<SharedBytes> {
        let (reply, receiver) = bounded(1);
        let deadline = self.operation_deadline(timeout);
        self.enqueue(ObjectOverflowWorkerJob::Get {
            object,
            deadline,
            reply,
        })?;
        self.recv(
            receiver,
            deadline.saturating_duration_since(Instant::now()),
            "get",
        )
    }

    #[cfg(feature = "object-overflow")]
    fn get_raw(&self, object_key: &str, timeout: Duration) -> Result<SharedBytes> {
        let (reply, receiver) = bounded(1);
        let deadline = self.operation_deadline(timeout);
        self.enqueue(ObjectOverflowWorkerJob::GetRaw {
            object_key: object_key.to_string(),
            deadline,
            reply,
        })?;
        self.recv(
            receiver,
            deadline.saturating_duration_since(Instant::now()),
            "get raw",
        )
    }

    #[cfg(any(feature = "object-overflow", test))]
    fn delete_value(&self, object_key: &str, timeout: Duration) -> Result<()> {
        let (reply, receiver) = bounded(1);
        let deadline = self.operation_deadline(timeout);
        self.enqueue(ObjectOverflowWorkerJob::Delete {
            object_key: object_key.to_string(),
            deadline,
            reply,
        })?;
        self.recv(
            receiver,
            deadline.saturating_duration_since(Instant::now()),
            "delete",
        )
    }

    fn enqueue_detached_delete(&self, object_key: &str, timeout: Duration) -> Result<()> {
        let deadline = self.operation_deadline(timeout);
        self.enqueue(ObjectOverflowWorkerJob::DeleteDetached {
            object_key: object_key.to_string(),
            deadline,
        })
    }

    #[cfg(feature = "object-overflow")]
    fn list_keys(&self, prefix: &str, timeout: Duration) -> Result<Vec<String>> {
        let (reply, receiver) = bounded(1);
        let deadline = self.operation_deadline(timeout);
        self.enqueue(ObjectOverflowWorkerJob::List {
            prefix: prefix.to_string(),
            max_keys: MAX_CLEANUP_KEYS_PER_SCAN,
            max_retained_bytes: MAX_CLEANUP_KEY_BYTES_PER_SCAN,
            deadline,
            reply,
        })?;
        self.recv(
            receiver,
            deadline.saturating_duration_since(Instant::now()),
            "list",
        )
    }

    fn operation_deadline(&self, timeout: Duration) -> Instant {
        Instant::now() + timeout.min(self.operation_timeout)
    }

    fn enqueue(&self, job: ObjectOverflowWorkerJob) -> Result<()> {
        if self.worker_capacity_exhausted.load(Ordering::Acquire) {
            self.stats.record_unavailable();
            return Err(ShardCacheError::Persistence(
                "object overflow worker capacity exhausted by timed-out operations".into(),
            ));
        }
        self.queue_depth.fetch_add(1, Ordering::AcqRel);
        match self.sender.try_send(job) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.queue_depth.fetch_sub(1, Ordering::AcqRel);
                self.stats.record_unavailable();
                Err(ShardCacheError::Persistence(
                    "object overflow worker queue full".into(),
                ))
            }
            Err(TrySendError::Disconnected(_)) => {
                self.queue_depth.fetch_sub(1, Ordering::AcqRel);
                Err(ShardCacheError::ChannelClosed("object overflow worker"))
            }
        }
    }

    fn recv<T>(
        &self,
        receiver: Receiver<Result<T>>,
        timeout: Duration,
        operation: &'static str,
    ) -> Result<T> {
        match receiver.recv_timeout(timeout) {
            Ok(result) => {
                if let Err(error) = &result {
                    self.stats.record_failure(error);
                }
                result
            }
            Err(RecvTimeoutError::Timeout) => {
                let error =
                    ShardCacheError::Persistence(format!("object overflow {operation} timed out"));
                self.stats.record_failure(&error);
                self.try_spawn_replacement_worker();
                Err(error)
            }
            Err(RecvTimeoutError::Disconnected) => {
                let error = ShardCacheError::ChannelClosed("object overflow worker response");
                self.stats.record_failure(&error);
                Err(error)
            }
        }
    }

    fn pending_jobs(&self) -> usize {
        self.queue_depth
            .load(Ordering::Relaxed)
            .saturating_add(self.active_workers.load(Ordering::Relaxed))
    }

    fn try_spawn_replacement_worker(&self) {
        if !matches!(self.try_spawn_worker(), Ok(true)) {
            self.worker_capacity_exhausted
                .store(true, Ordering::Release);
        }
    }

    fn try_spawn_worker(&self) -> Result<bool> {
        let mut workers = self.workers.lock().map_err(|_| {
            ShardCacheError::Persistence("object overflow worker handle lock poisoned".into())
        })?;
        let mut index = 0;
        while index < workers.len() {
            if workers[index].is_finished() {
                let finished = workers.swap_remove(index);
                let _ = finished.join();
            } else {
                index += 1;
            }
        }
        let reserved =
            self.live_workers
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |live| {
                    (live < self.max_worker_threads).then_some(live + 1)
                });
        if reserved.is_err() {
            return Ok(false);
        }
        let worker = ObjectOverflowWorker {
            index: self.next_worker_index.fetch_add(1, Ordering::Relaxed),
            store: Arc::clone(&self.store),
            receiver: self.receiver.clone(),
            active_workers: Arc::clone(&self.active_workers),
            live_workers: Arc::clone(&self.live_workers),
            worker_capacity_exhausted: Arc::clone(&self.worker_capacity_exhausted),
            queue_depth: Arc::clone(&self.queue_depth),
            stats: Arc::clone(&self.stats),
            max_retries: self.max_retries,
            retry_backoff: self.retry_backoff,
            authentication_key: self.authentication_key,
        };
        let handle = thread::Builder::new()
            .name(format!("object-overflow-worker-{}", worker.index))
            .spawn(move || worker.run())
            .map_err(|error| {
                self.live_workers.fetch_sub(1, Ordering::AcqRel);
                ShardCacheError::Persistence(format!("object overflow worker thread: {error}"))
            })?;
        workers.push(handle);
        Ok(true)
    }
}

impl Drop for ObjectOverflowWorkerPool {
    fn drop(&mut self) {
        for _ in 0..self.live_workers.load(Ordering::Acquire) {
            let _ = self.sender.try_send(ObjectOverflowWorkerJob::Stop);
        }
        let workers = self
            .workers
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for worker in workers.drain(..) {
            if worker.is_finished() {
                let _ = worker.join();
            }
        }
    }
}

struct ObjectOverflowWorker {
    index: usize,
    store: Arc<dyn ObjectOverflowStore>,
    receiver: Receiver<ObjectOverflowWorkerJob>,
    active_workers: Arc<AtomicUsize>,
    live_workers: Arc<AtomicUsize>,
    worker_capacity_exhausted: Arc<AtomicBool>,
    queue_depth: Arc<AtomicUsize>,
    stats: Arc<ObjectOverflowWorkerStats>,
    max_retries: usize,
    retry_backoff: Duration,
    authentication_key: [u8; 32],
}

impl ObjectOverflowWorker {
    fn run(self) {
        while let Ok(job) = self.receiver.recv() {
            match job {
                ObjectOverflowWorkerJob::Stop => break,
                job => {
                    self.queue_depth.fetch_sub(1, Ordering::AcqRel);
                    if !self.handle_job(job) {
                        break;
                    }
                }
            }
        }
        self.live_workers.fetch_sub(1, Ordering::AcqRel);
        self.worker_capacity_exhausted
            .store(false, Ordering::Release);
        let _ = self.index;
    }

    fn handle_job(&self, job: ObjectOverflowWorkerJob) -> bool {
        self.active_workers.fetch_add(1, Ordering::AcqRel);
        match job {
            ObjectOverflowWorkerJob::Put {
                object_key,
                raw,
                compression,
                zstd_level,
                deadline,
                reply,
            } => {
                let result = self.put_encoded(&object_key, &raw, compression, zstd_level, deadline);
                let _ = reply.send(result);
            }
            #[cfg(feature = "object-overflow")]
            ObjectOverflowWorkerJob::PutRaw {
                object_key,
                value,
                deadline,
                reply,
            } => {
                let result = self.with_retries(deadline, |timeout| {
                    self.store
                        .put_value_with_timeout(&object_key, &value, timeout)
                });
                let _ = reply.send(result);
            }
            ObjectOverflowWorkerJob::Get {
                object,
                deadline,
                reply,
            } => {
                let result = self.get_decoded(&object, deadline);
                let _ = reply.send(result);
            }
            #[cfg(feature = "object-overflow")]
            ObjectOverflowWorkerJob::GetRaw {
                object_key,
                deadline,
                reply,
            } => {
                let result = self.with_retries(deadline, |timeout| {
                    self.store.get_value_bounded_with_timeout(
                        &object_key,
                        MAX_GENERATION_MARKER_BYTES,
                        timeout,
                    )
                });
                let _ = reply.send(result);
            }
            #[cfg(any(feature = "object-overflow", test))]
            ObjectOverflowWorkerJob::Delete {
                object_key,
                deadline,
                reply,
            } => {
                let start = Instant::now();
                let result = self.with_retries(deadline, |timeout| {
                    self.store.delete_value_with_timeout(&object_key, timeout)
                });
                self.stats.delete_ops.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .delete_latency_us
                    .fetch_add(elapsed_us(start), Ordering::Relaxed);
                if result.is_err() {
                    self.stats.delete_failures.fetch_add(1, Ordering::Relaxed);
                }
                let _ = reply.send(result);
            }
            ObjectOverflowWorkerJob::DeleteDetached {
                object_key,
                deadline,
            } => {
                let start = Instant::now();
                let result = self.with_retries(deadline, |timeout| {
                    self.store.delete_value_with_timeout(&object_key, timeout)
                });
                self.stats.delete_ops.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .delete_latency_us
                    .fetch_add(elapsed_us(start), Ordering::Relaxed);
                if let Err(error) = result {
                    self.stats.delete_failures.fetch_add(1, Ordering::Relaxed);
                    self.stats.record_failure(&error);
                }
            }
            #[cfg(feature = "object-overflow")]
            ObjectOverflowWorkerJob::List {
                prefix,
                max_keys,
                max_retained_bytes,
                deadline,
                reply,
            } => {
                let result = self.with_retries(deadline, |timeout| {
                    self.store.list_keys_bounded_with_timeout(
                        &prefix,
                        max_keys,
                        max_retained_bytes,
                        timeout,
                    )
                });
                let _ = reply.send(result);
            }
            ObjectOverflowWorkerJob::Stop => {}
        }
        self.active_workers.fetch_sub(1, Ordering::AcqRel);
        self.worker_capacity_exhausted
            .store(false, Ordering::Release);
        true
    }

    fn put_encoded(
        &self,
        object_key: &str,
        raw: &[u8],
        compression: ObjectOverflowCompression,
        zstd_level: i32,
        deadline: Instant,
    ) -> Result<ObjectValueRef> {
        if Instant::now() >= deadline {
            return Err(object_worker_deadline());
        }
        let encode_start = Instant::now();
        let encoded = ObjectPayloadCodec::encode(
            raw,
            compression,
            zstd_level,
            object_key,
            &self.authentication_key,
        )?;
        self.stats.encode_ops.fetch_add(1, Ordering::Relaxed);
        if compression != ObjectOverflowCompression::None {
            self.stats.compress_ops.fetch_add(1, Ordering::Relaxed);
        }
        self.stats
            .encode_latency_us
            .fetch_add(elapsed_us(encode_start), Ordering::Relaxed);
        let object_ref = ObjectValueRef {
            object_key: object_key.to_string(),
            len: raw.len(),
            stored_len: encoded.len(),
            compression,
            checksum: crc32fast::hash(raw),
        };
        let upload_start = Instant::now();
        self.with_retries(deadline, |timeout| {
            self.store
                .put_value_with_timeout(object_key, &encoded, timeout)
        })?;
        self.stats.upload_ops.fetch_add(1, Ordering::Relaxed);
        self.stats
            .upload_latency_us
            .fetch_add(elapsed_us(upload_start), Ordering::Relaxed);
        self.stats
            .object_bytes_raw
            .fetch_add(raw.len() as u64, Ordering::Relaxed);
        self.stats
            .object_bytes_stored
            .fetch_add(encoded.len() as u64, Ordering::Relaxed);
        Ok(object_ref)
    }

    fn get_decoded(&self, object: &ObjectValueRef, deadline: Instant) -> Result<SharedBytes> {
        let download_start = Instant::now();
        let encoded = self.with_retries(deadline, |timeout| {
            self.store.get_value_bounded_with_timeout(
                &object.object_key,
                object.stored_len,
                timeout,
            )
        })?;
        self.stats.download_ops.fetch_add(1, Ordering::Relaxed);
        self.stats
            .download_latency_us
            .fetch_add(elapsed_us(download_start), Ordering::Relaxed);
        let decode_start = Instant::now();
        if encoded.len() != object.stored_len {
            return Err(ShardCacheError::ObjectIntegrity(
                "object overflow stored length mismatch".into(),
            ));
        }
        let decoded = ObjectPayloadCodec::decode(
            &encoded,
            object,
            &object.object_key,
            &self.authentication_key,
        )?;
        self.stats.decode_ops.fetch_add(1, Ordering::Relaxed);
        self.stats
            .decode_latency_us
            .fetch_add(elapsed_us(decode_start), Ordering::Relaxed);
        Ok(decoded)
    }

    fn with_retries<T>(
        &self,
        deadline: Instant,
        mut op: impl FnMut(Duration) -> Result<T>,
    ) -> Result<T> {
        let mut attempts = 0usize;
        loop {
            if Instant::now() >= deadline {
                return Err(object_worker_deadline());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            match op(remaining) {
                Ok(value) => return Ok(value),
                Err(error)
                    if attempts < self.max_retries
                        && is_retryable_object_overflow_error(&error) =>
                {
                    attempts = attempts.saturating_add(1);
                    self.stats.retries.fetch_add(1, Ordering::Relaxed);
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(object_worker_deadline());
                    }
                    thread::sleep(self.retry_backoff.min(remaining));
                }
                Err(error) => return Err(error),
            }
        }
    }
}

fn is_retryable_object_overflow_error(error: &ShardCacheError) -> bool {
    match error {
        ShardCacheError::Persistence(_) => true,
        ShardCacheError::Io(error) => !matches!(
            error.kind(),
            std::io::ErrorKind::NotFound
                | std::io::ErrorKind::PermissionDenied
                | std::io::ErrorKind::InvalidInput
                | std::io::ErrorKind::InvalidData
                | std::io::ErrorKind::AlreadyExists
        ),
        _ => false,
    }
}

fn object_worker_deadline() -> ShardCacheError {
    ShardCacheError::Persistence("object overflow operation timed out".into())
}

impl ObjectOverflowWorkerStats {
    fn snapshot(&self) -> ObjectOverflowWorkerStatsSnapshot {
        ObjectOverflowWorkerStatsSnapshot {
            encode_ops: self.encode_ops.load(Ordering::Relaxed),
            compress_ops: self.compress_ops.load(Ordering::Relaxed),
            upload_ops: self.upload_ops.load(Ordering::Relaxed),
            download_ops: self.download_ops.load(Ordering::Relaxed),
            decode_ops: self.decode_ops.load(Ordering::Relaxed),
            delete_ops: self.delete_ops.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            auth_config_failures: self.auth_config_failures.load(Ordering::Relaxed),
            unavailable_failures: self.unavailable_failures.load(Ordering::Relaxed),
            integrity_failures: self.integrity_failures.load(Ordering::Relaxed),
            not_found_failures: self.not_found_failures.load(Ordering::Relaxed),
            delete_failures: self.delete_failures.load(Ordering::Relaxed),
            cleanup_scans: self.cleanup_scans.load(Ordering::Relaxed),
            cleanup_deletes: self.cleanup_deletes.load(Ordering::Relaxed),
            object_bytes_raw: self.object_bytes_raw.load(Ordering::Relaxed),
            object_bytes_stored: self.object_bytes_stored.load(Ordering::Relaxed),
            encode_latency_us: self.encode_latency_us.load(Ordering::Relaxed),
            upload_latency_us: self.upload_latency_us.load(Ordering::Relaxed),
            download_latency_us: self.download_latency_us.load(Ordering::Relaxed),
            decode_latency_us: self.decode_latency_us.load(Ordering::Relaxed),
            delete_latency_us: self.delete_latency_us.load(Ordering::Relaxed),
        }
    }

    fn record_failure(&self, error: &ShardCacheError) {
        match error {
            ShardCacheError::ObjectIntegrity(_) => {
                self.integrity_failures.fetch_add(1, Ordering::Relaxed);
            }
            ShardCacheError::Config(_) => {
                self.auth_config_failures.fetch_add(1, Ordering::Relaxed);
            }
            ShardCacheError::Io(io_error) if io_error.kind() == std::io::ErrorKind::NotFound => {
                self.not_found_failures.fetch_add(1, Ordering::Relaxed);
            }
            ShardCacheError::Persistence(message) if message.contains("timed out") => {
                self.timeouts.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.unavailable_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn record_unavailable(&self) {
        self.unavailable_failures.fetch_add(1, Ordering::Relaxed);
    }
}

fn elapsed_us(start: Instant) -> u64 {
    start.elapsed().as_micros() as u64
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn new_generation_id() -> Result<String> {
    let random = secure_random_32()?;
    Ok(random[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn secure_random_32() -> Result<[u8; 32]> {
    let mut random = [0u8; 32];
    getrandom::fill(&mut random).map_err(|error| {
        ShardCacheError::Persistence(format!(
            "operating system randomness is unavailable: {error}"
        ))
    })?;
    Ok(random)
}

#[cfg(feature = "object-overflow")]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GenerationMarker {
    node_id: String,
    generation_id: String,
    created_ms: u64,
    heartbeat_ms: u64,
}

struct ObjectPayloadCodec;

const OBJECT_PAYLOAD_MAGIC: &[u8; 8] = b"SCOVF2\0\0";
const OBJECT_PAYLOAD_HEADER_LEN: usize = 8 + 1 + 8 + 8 + 4;
const OBJECT_PAYLOAD_TAG_LEN: usize = 32;

impl ObjectPayloadCodec {
    fn encode(
        raw: &[u8],
        compression: ObjectOverflowCompression,
        zstd_level: i32,
        object_key: &str,
        authentication_key: &[u8; 32],
    ) -> Result<Vec<u8>> {
        let body = match compression {
            ObjectOverflowCompression::None => raw.to_vec(),
            ObjectOverflowCompression::Lz4 => compress_prepend_size(raw),
            ObjectOverflowCompression::Zstd => zstd::bulk::compress(raw, zstd_level)
                .map_err(|error| ShardCacheError::Persistence(format!("zstd encode: {error}")))?,
        };
        let mut encoded = Vec::with_capacity(
            OBJECT_PAYLOAD_HEADER_LEN
                .saturating_add(body.len())
                .saturating_add(OBJECT_PAYLOAD_TAG_LEN),
        );
        encoded.extend_from_slice(OBJECT_PAYLOAD_MAGIC);
        encoded.push(Self::compression_byte(compression));
        encoded.extend_from_slice(&(raw.len() as u64).to_le_bytes());
        encoded.extend_from_slice(&(body.len() as u64).to_le_bytes());
        encoded.extend_from_slice(&crc32fast::hash(raw).to_le_bytes());
        encoded.extend_from_slice(&body);
        let tag = hmac_sha256(authentication_key, object_key.as_bytes(), &encoded);
        encoded.extend_from_slice(&tag);
        Ok(encoded)
    }

    fn decode(
        encoded: &[u8],
        object: &ObjectValueRef,
        object_key: &str,
        authentication_key: &[u8; 32],
    ) -> Result<SharedBytes> {
        if encoded.len() < OBJECT_PAYLOAD_HEADER_LEN.saturating_add(OBJECT_PAYLOAD_TAG_LEN) {
            return Err(ShardCacheError::ObjectIntegrity(
                "object overflow payload header is truncated".into(),
            ));
        }
        let payload_len = encoded.len() - OBJECT_PAYLOAD_TAG_LEN;
        let (authenticated_payload, supplied_tag) = encoded.split_at(payload_len);
        let expected_tag = hmac_sha256(
            authentication_key,
            object_key.as_bytes(),
            authenticated_payload,
        );
        if !constant_time_equal(supplied_tag, &expected_tag) {
            return Err(ShardCacheError::ObjectIntegrity(
                "object overflow payload authentication failed".into(),
            ));
        }
        if &encoded[..OBJECT_PAYLOAD_MAGIC.len()] != OBJECT_PAYLOAD_MAGIC {
            return Err(ShardCacheError::ObjectIntegrity(
                "object overflow payload magic mismatch".into(),
            ));
        }
        let compression = Self::compression_from_byte(encoded[OBJECT_PAYLOAD_MAGIC.len()])?;
        let mut cursor = OBJECT_PAYLOAD_MAGIC.len() + 1;
        let raw_len = usize::try_from(Self::read_u64(encoded, &mut cursor, "raw length")?)
            .map_err(|_| {
                ShardCacheError::ObjectIntegrity("object overflow raw length is too large".into())
            })?;
        let stored_body_len = usize::try_from(Self::read_u64(
            encoded,
            &mut cursor,
            "stored length",
        )?)
        .map_err(|_| {
            ShardCacheError::ObjectIntegrity("object overflow stored length is too large".into())
        })?;
        let checksum = Self::read_u32(encoded, &mut cursor, "checksum")?;
        let body = &authenticated_payload[cursor..];
        if body.len() != stored_body_len {
            return Err(ShardCacheError::ObjectIntegrity(
                "object overflow payload length mismatch".into(),
            ));
        }
        if raw_len != object.len || checksum != object.checksum || compression != object.compression
        {
            return Err(ShardCacheError::ObjectIntegrity(
                "object overflow reference metadata mismatch".into(),
            ));
        }
        let decoded = match compression {
            ObjectOverflowCompression::None => body.to_vec(),
            ObjectOverflowCompression::Lz4 => {
                let size_prefix = body.get(..4).ok_or_else(|| {
                    ShardCacheError::ObjectIntegrity(
                        "object overflow lz4 size prefix is truncated".into(),
                    )
                })?;
                let declared = u32::from_le_bytes(
                    size_prefix
                        .try_into()
                        .expect("lz4 prefix length was checked"),
                ) as usize;
                if declared != raw_len {
                    return Err(ShardCacheError::ObjectIntegrity(
                        "object overflow lz4 length mismatch".into(),
                    ));
                }
                let mut output = Vec::new();
                output.try_reserve_exact(raw_len).map_err(|_| {
                    ShardCacheError::ObjectIntegrity(
                        "object overflow lz4 allocation is too large".into(),
                    )
                })?;
                output.resize(raw_len, 0);
                let written = decompress_into(&body[4..], &mut output).map_err(|error| {
                    ShardCacheError::ObjectIntegrity(format!("lz4 decode: {error}"))
                })?;
                if written != raw_len {
                    return Err(ShardCacheError::ObjectIntegrity(
                        "object overflow lz4 decoded length mismatch".into(),
                    ));
                }
                output
            }
            ObjectOverflowCompression::Zstd => {
                zstd::bulk::decompress(body, raw_len).map_err(|error| {
                    ShardCacheError::ObjectIntegrity(format!("zstd decode: {error}"))
                })?
            }
        };
        if decoded.len() != raw_len || crc32fast::hash(&decoded) != checksum {
            return Err(ShardCacheError::ObjectIntegrity(
                "object overflow checksum mismatch".into(),
            ));
        }
        Ok(SharedBytes::from(decoded))
    }

    fn compression_byte(compression: ObjectOverflowCompression) -> u8 {
        match compression {
            ObjectOverflowCompression::None => 0,
            ObjectOverflowCompression::Lz4 => 1,
            ObjectOverflowCompression::Zstd => 2,
        }
    }

    fn compression_from_byte(value: u8) -> Result<ObjectOverflowCompression> {
        match value {
            0 => Ok(ObjectOverflowCompression::None),
            1 => Ok(ObjectOverflowCompression::Lz4),
            2 => Ok(ObjectOverflowCompression::Zstd),
            other => Err(ShardCacheError::ObjectIntegrity(format!(
                "unsupported object overflow compression: {other}"
            ))),
        }
    }

    fn read_u64(raw: &[u8], cursor: &mut usize, field: &str) -> Result<u64> {
        let bytes = Self::read_exact(raw, cursor, 8, field)?;
        let mut value = [0; 8];
        value.copy_from_slice(bytes);
        Ok(u64::from_le_bytes(value))
    }

    fn read_u32(raw: &[u8], cursor: &mut usize, field: &str) -> Result<u32> {
        let bytes = Self::read_exact(raw, cursor, 4, field)?;
        let mut value = [0; 4];
        value.copy_from_slice(bytes);
        Ok(u32::from_le_bytes(value))
    }

    fn read_exact<'a>(
        raw: &'a [u8],
        cursor: &mut usize,
        len: usize,
        field: &str,
    ) -> Result<&'a [u8]> {
        if raw.len().saturating_sub(*cursor) < len {
            return Err(ShardCacheError::Persistence(format!(
                "object overflow {field} is truncated"
            )));
        }
        let bytes = &raw[*cursor..*cursor + len];
        *cursor += len;
        Ok(bytes)
    }
}

fn hmac_sha256(key: &[u8; 32], context: &[u8], payload: &[u8]) -> [u8; 32] {
    const BLOCK_BYTES: usize = 64;
    let mut inner_pad = [0x36u8; BLOCK_BYTES];
    let mut outer_pad = [0x5cu8; BLOCK_BYTES];
    for (index, byte) in key.iter().enumerate() {
        inner_pad[index] ^= byte;
        outer_pad[index] ^= byte;
    }
    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update((context.len() as u64).to_le_bytes());
    inner.update(context);
    inner.update(payload);
    let inner_hash = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_hash);
    outer.finalize().into()
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

fn validate_key_listing(keys: &[String], max_keys: usize, max_retained_bytes: usize) -> Result<()> {
    let retained_bytes = keys
        .iter()
        .try_fold(0usize, |total, key| total.checked_add(key.len()))
        .ok_or_else(|| ShardCacheError::Persistence("object key listing size overflow".into()))?;
    if keys.len() > max_keys || retained_bytes > max_retained_bytes {
        return Err(ShardCacheError::Persistence(format!(
            "object key listing exceeds cleanup bounds ({max_keys} keys, {max_retained_bytes} bytes)"
        )));
    }
    Ok(())
}

#[cfg(feature = "object-overflow")]
#[derive(Debug, Clone)]
pub struct FileObjectOverflowStore {
    root: PathBuf,
    #[cfg(unix)]
    root_dir: Arc<File>,
}

#[cfg(feature = "object-overflow")]
impl FileObjectOverflowStore {
    pub fn from_config(config: &ObjectOverflowConfig) -> Result<Self> {
        let endpoint = config
            .endpoint
            .strip_prefix("file://")
            .unwrap_or(&config.endpoint);
        let root = PathBuf::from(endpoint).join(&config.bucket);
        fs::create_dir_all(&root)?;
        let root = fs::canonicalize(root)?;
        #[cfg(unix)]
        let root_dir = Arc::new(File::open(&root)?);
        Ok(Self {
            root,
            #[cfg(unix)]
            root_dir,
        })
    }

    fn path_for_key(&self, object_key: &str) -> Result<PathBuf> {
        let mut path = self.root.clone();
        for component in object_key.split('/') {
            Self::validate_component(component)?;
            path.push(component);
            if let Ok(metadata) = fs::symlink_metadata(&path)
                && metadata.file_type().is_symlink()
            {
                return Err(ShardCacheError::Persistence(
                    "object overflow filesystem path contains a symbolic link".into(),
                ));
            }
        }
        Ok(path)
    }

    fn path_for_prefix(&self, prefix: &str) -> Result<PathBuf> {
        let prefix = prefix.trim_end_matches('/');
        if prefix.is_empty() {
            return Ok(self.root.clone());
        }
        self.path_for_key(prefix)
    }

    #[cfg(not(unix))]
    fn ensure_parent(path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(())
    }

    fn collect_keys(
        root: &Path,
        start: &Path,
        output: &mut Vec<String>,
        max_keys: usize,
        max_retained_bytes: usize,
    ) -> Result<()> {
        let mut directories = vec![start.to_path_buf()];
        let mut retained_bytes = 0usize;
        while let Some(current) = directories.pop() {
            for entry in fs::read_dir(current)? {
                let entry = entry?;
                let path = entry.path();
                let metadata = fs::symlink_metadata(&path)?;
                if metadata.file_type().is_symlink() {
                    return Err(ShardCacheError::Persistence(
                        "object overflow cleanup encountered a symbolic link".into(),
                    ));
                }
                if metadata.is_dir() {
                    directories.push(path);
                } else if metadata.is_file() {
                    let relative = path.strip_prefix(root).map_err(|error| {
                        ShardCacheError::Persistence(format!("object key strip prefix: {error}"))
                    })?;
                    let key = relative
                        .components()
                        .map(|component| component.as_os_str().to_string_lossy())
                        .collect::<Vec<_>>()
                        .join("/");
                    retained_bytes = retained_bytes.checked_add(key.len()).ok_or_else(|| {
                        ShardCacheError::Persistence("object key listing size overflow".into())
                    })?;
                    if output.len() >= max_keys || retained_bytes > max_retained_bytes {
                        return Err(ShardCacheError::Persistence(
                            "object key listing exceeds configured cleanup bounds".into(),
                        ));
                    }
                    output.push(key);
                }
            }
        }
        Ok(())
    }

    fn collect_key_page(
        root: &Path,
        start: &Path,
        start_after: Option<&str>,
        max_keys: usize,
        max_retained_bytes: usize,
    ) -> Result<ObjectKeyPage> {
        if max_keys == 0 || max_retained_bytes == 0 {
            return Err(ShardCacheError::Config(
                "object overflow key page bounds must be nonzero".into(),
            ));
        }
        let page_limit = max_keys.min((max_retained_bytes / MAX_OBJECT_KEY_BYTES).max(1));
        let candidate_limit = page_limit.saturating_add(1);
        let mut candidates = BinaryHeap::with_capacity(candidate_limit);
        let mut directories = vec![start.to_path_buf()];
        while let Some(current) = directories.pop() {
            for entry in fs::read_dir(current)? {
                let entry = entry?;
                let path = entry.path();
                let metadata = fs::symlink_metadata(&path)?;
                if metadata.file_type().is_symlink() {
                    return Err(ShardCacheError::Persistence(
                        "object overflow cleanup encountered a symbolic link".into(),
                    ));
                }
                if metadata.is_dir() {
                    directories.push(path);
                    continue;
                }
                if !metadata.is_file() {
                    continue;
                }
                let relative = path.strip_prefix(root).map_err(|error| {
                    ShardCacheError::Persistence(format!("object key strip prefix: {error}"))
                })?;
                let key = relative
                    .components()
                    .map(|component| component.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                if key.len() > MAX_OBJECT_KEY_BYTES {
                    return Err(ShardCacheError::Persistence(
                        "object overflow key exceeds the cleanup key-length bound".into(),
                    ));
                }
                if start_after.is_some_and(|start_after| key.as_str() <= start_after) {
                    continue;
                }
                if candidates.len() < candidate_limit {
                    candidates.push(key);
                } else if candidates
                    .peek()
                    .is_some_and(|largest| key.as_str() < largest.as_str())
                {
                    let _ = candidates.pop();
                    candidates.push(key);
                }
            }
        }
        let mut keys = candidates.into_vec();
        keys.sort_unstable();
        let next_after = (keys.len() > page_limit).then(|| keys[page_limit - 1].clone());
        keys.truncate(page_limit);
        validate_key_listing(&keys, max_keys, max_retained_bytes)?;
        Ok(ObjectKeyPage { keys, next_after })
    }

    #[cfg(unix)]
    fn open_parent_dir(&self, object_key: &str, create: bool) -> Result<(File, CString)> {
        let components = object_key.split('/').collect::<Vec<_>>();
        if components.is_empty() {
            return Err(ShardCacheError::Config(
                "object overflow key must not be empty".into(),
            ));
        }
        let mut directory = self.root_dir.try_clone()?;
        for component in &components[..components.len() - 1] {
            Self::validate_component(component)?;
            let component = CString::new(*component).map_err(|_| {
                ShardCacheError::Config("object overflow key contains a NUL byte".into())
            })?;
            loop {
                // SAFETY: the directory descriptor and NUL-terminated component are valid.
                let fd = unsafe {
                    libc::openat(
                        directory.as_raw_fd(),
                        component.as_ptr(),
                        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                    )
                };
                if fd >= 0 {
                    // SAFETY: `openat` returned a newly owned file descriptor.
                    directory = unsafe { File::from_raw_fd(fd) };
                    break;
                }
                let error = std::io::Error::last_os_error();
                if !create || error.kind() != std::io::ErrorKind::NotFound {
                    return Err(error.into());
                }
                // SAFETY: arguments are valid and creation remains relative to the open parent.
                let created =
                    unsafe { libc::mkdirat(directory.as_raw_fd(), component.as_ptr(), 0o700) };
                if created != 0 {
                    let mkdir_error = std::io::Error::last_os_error();
                    if mkdir_error.kind() != std::io::ErrorKind::AlreadyExists {
                        return Err(mkdir_error.into());
                    }
                }
            }
        }
        let final_component = components
            .last()
            .expect("validated object key has a final component");
        Self::validate_component(final_component)?;
        let final_component = CString::new(*final_component).map_err(|_| {
            ShardCacheError::Config("object overflow key contains a NUL byte".into())
        })?;
        Ok((directory, final_component))
    }

    fn validate_component(component: &str) -> Result<()> {
        if component.is_empty() || component == "." || component == ".." || component.contains('\\')
        {
            return Err(ShardCacheError::Config(format!(
                "invalid object overflow key component: {component:?}"
            )));
        }
        Ok(())
    }

    #[cfg(unix)]
    fn open_read_only(&self, object_key: &str) -> Result<File> {
        let (directory, component) = self.open_parent_dir(object_key, false)?;
        // SAFETY: the directory descriptor and NUL-terminated component are valid.
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                component.as_ptr(),
                libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: `openat` returned a newly owned file descriptor.
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    #[cfg(not(unix))]
    fn open_read_only(&self, object_key: &str) -> Result<File> {
        Ok(OpenOptions::new()
            .read(true)
            .open(self.path_for_key(object_key)?)?)
    }

    fn read_bounded(&self, object_key: &str, max_bytes: usize) -> Result<SharedBytes> {
        let file = self.open_read_only(object_key)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() > max_bytes as u64 {
            return Err(ShardCacheError::ObjectIntegrity(
                "object overflow filesystem payload exceeds its trusted bound".into(),
            ));
        }
        let initial_capacity = usize::try_from(metadata.len())
            .unwrap_or(max_bytes)
            .min(max_bytes);
        let mut body = Vec::new();
        body.try_reserve_exact(initial_capacity).map_err(|_| {
            ShardCacheError::Persistence(
                "object overflow filesystem payload allocation failed".into(),
            )
        })?;
        file.take(max_bytes.saturating_add(1) as u64)
            .read_to_end(&mut body)?;
        if body.len() > max_bytes {
            return Err(ShardCacheError::ObjectIntegrity(
                "object overflow filesystem payload exceeds its trusted bound".into(),
            ));
        }
        Ok(SharedBytes::from(body))
    }

    #[cfg(unix)]
    fn write_secure(&self, object_key: &str, value: &[u8]) -> Result<()> {
        let (directory, component) = self.open_parent_dir(object_key, true)?;
        if !object_key.ends_with("/_generation.json") {
            // Payload keys are immutable and remain recoverable from the local
            // WAL/snapshot, so keep their write path free of per-object fsync.
            // SAFETY: the directory descriptor and NUL-terminated component are valid.
            let fd = unsafe {
                libc::openat(
                    directory.as_raw_fd(),
                    component.as_ptr(),
                    libc::O_WRONLY
                        | libc::O_CREAT
                        | libc::O_TRUNC
                        | libc::O_NOFOLLOW
                        | libc::O_CLOEXEC,
                    0o600,
                )
            };
            if fd < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            // SAFETY: `openat` returned a newly owned file descriptor.
            let mut file = unsafe { File::from_raw_fd(fd) };
            file.write_all(value)?;
            return Ok(());
        }
        for _ in 0..128 {
            let sequence = FILE_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let temporary = CString::new(format!(
                ".shardcache-overflow-{}-{sequence}.tmp",
                std::process::id()
            ))
            .map_err(|_| {
                ShardCacheError::Config("object overflow temporary key contains a NUL byte".into())
            })?;
            // SAFETY: the directory descriptor and NUL-terminated component are valid.
            let fd = unsafe {
                libc::openat(
                    directory.as_raw_fd(),
                    temporary.as_ptr(),
                    libc::O_WRONLY
                        | libc::O_CREAT
                        | libc::O_EXCL
                        | libc::O_NOFOLLOW
                        | libc::O_CLOEXEC,
                    0o600,
                )
            };
            if fd < 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    continue;
                }
                return Err(error.into());
            }
            // SAFETY: `openat` returned a newly owned file descriptor.
            let mut file = unsafe { File::from_raw_fd(fd) };
            let write_result = file.write_all(value).and_then(|()| file.sync_all());
            drop(file);
            if let Err(error) = write_result {
                // SAFETY: the temporary name belongs to this open directory.
                let _ = unsafe { libc::unlinkat(directory.as_raw_fd(), temporary.as_ptr(), 0) };
                return Err(error.into());
            }
            // SAFETY: both names are relative to the same open directory.
            let renamed = unsafe {
                libc::renameat(
                    directory.as_raw_fd(),
                    temporary.as_ptr(),
                    directory.as_raw_fd(),
                    component.as_ptr(),
                )
            };
            if renamed != 0 {
                let error = std::io::Error::last_os_error();
                // SAFETY: the temporary name belongs to this open directory.
                let _ = unsafe { libc::unlinkat(directory.as_raw_fd(), temporary.as_ptr(), 0) };
                return Err(error.into());
            }
            directory.sync_all()?;
            return Ok(());
        }
        Err(ShardCacheError::Persistence(
            "object overflow could not allocate a temporary file name".into(),
        ))
    }

    #[cfg(not(unix))]
    fn write_secure(&self, object_key: &str, value: &[u8]) -> Result<()> {
        let path = self.path_for_key(object_key)?;
        Self::ensure_parent(&path)?;
        fs::write(path, value)?;
        Ok(())
    }

    #[cfg(unix)]
    fn delete_secure(&self, object_key: &str) -> Result<()> {
        let (directory, component) = match self.open_parent_dir(object_key, false) {
            Ok(parts) => parts,
            Err(ShardCacheError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        // SAFETY: the directory descriptor and NUL-terminated component are valid.
        let result = unsafe { libc::unlinkat(directory.as_raw_fd(), component.as_ptr(), 0) };
        if result == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::NotFound {
            Ok(())
        } else {
            Err(error.into())
        }
    }

    #[cfg(not(unix))]
    fn delete_secure(&self, object_key: &str) -> Result<()> {
        let path = self.path_for_key(object_key)?;
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(feature = "object-overflow")]
impl ObjectOverflowStore for FileObjectOverflowStore {
    fn put_value(&self, object_key: &str, value: &[u8]) -> Result<()> {
        self.write_secure(object_key, value)
    }

    fn get_value(&self, object_key: &str) -> Result<SharedBytes> {
        self.read_bounded(object_key, MAX_UNBOUNDED_ADAPTER_READ_BYTES)
    }

    fn get_value_bounded_with_timeout(
        &self,
        object_key: &str,
        max_bytes: usize,
        _timeout: Duration,
    ) -> Result<SharedBytes> {
        self.read_bounded(object_key, max_bytes)
    }

    fn delete_value(&self, object_key: &str) -> Result<()> {
        self.delete_secure(object_key)
    }

    fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        if !self.root.exists() {
            return Ok(keys);
        }
        let start = self.path_for_prefix(prefix)?;
        if !start.exists() {
            return Ok(keys);
        }
        Self::collect_keys(
            &self.root,
            &start,
            &mut keys,
            MAX_CLEANUP_KEYS_PER_SCAN,
            MAX_CLEANUP_KEY_BYTES_PER_SCAN,
        )?;
        keys.retain(|key| key.starts_with(prefix));
        Ok(keys)
    }

    fn list_keys_bounded_with_timeout(
        &self,
        prefix: &str,
        max_keys: usize,
        max_retained_bytes: usize,
        _timeout: Duration,
    ) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        if !self.root.exists() {
            return Ok(keys);
        }
        let start = self.path_for_prefix(prefix)?;
        if !start.exists() {
            return Ok(keys);
        }
        Self::collect_keys(&self.root, &start, &mut keys, max_keys, max_retained_bytes)?;
        keys.retain(|key| key.starts_with(prefix));
        validate_key_listing(&keys, max_keys, max_retained_bytes)?;
        Ok(keys)
    }

    fn list_keys_page_bounded_with_timeout(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: usize,
        max_retained_bytes: usize,
        _timeout: Duration,
    ) -> Result<ObjectKeyPage> {
        let start = self.path_for_prefix(prefix)?;
        if !start.exists() {
            return Ok(ObjectKeyPage {
                keys: Vec::new(),
                next_after: None,
            });
        }
        Self::collect_key_page(
            &self.root,
            &start,
            start_after,
            max_keys,
            max_retained_bytes,
        )
    }
}

#[cfg(feature = "object-overflow")]
#[derive(Debug, Clone)]
struct S3ObjectOverflowStore {
    #[cfg(feature = "object-overflow-s3")]
    client: Arc<AmazonS3>,
    #[cfg(feature = "object-overflow-s3")]
    runtime: Arc<tokio::runtime::Runtime>,
}

#[cfg(feature = "object-overflow")]
impl S3ObjectOverflowStore {
    fn from_config(config: &ObjectOverflowConfig) -> Result<Self> {
        let credentials = ObjectOverflowCredentials::from_config(config)?;
        #[cfg(feature = "object-overflow-s3")]
        {
            let mut builder = AmazonS3Builder::new()
                .with_bucket_name(config.bucket.clone())
                .with_region(config.region.clone())
                .with_endpoint(config.endpoint.clone())
                .with_allow_http(config.allow_http)
                // Retries are owned by ObjectOverflowWorkerPool so they remain
                // bounded by one deadline and are counted exactly once.
                .with_retry(RetryConfig {
                    max_retries: 0,
                    ..RetryConfig::default()
                })
                .with_virtual_hosted_style_request(!config.force_path_style)
                .with_access_key_id(credentials.access_key)
                .with_secret_access_key(credentials.secret_key);
            if !config.tls_verify || config.tls_ca_path.is_some() {
                let mut options = ClientOptions::new().with_allow_http(config.allow_http);
                if !config.tls_verify {
                    options = options.with_allow_invalid_certificates(true);
                }
                if let Some(path) = &config.tls_ca_path {
                    let pem = read_bounded_config_file(path, MAX_TLS_CA_BUNDLE_BYTES, "TLS CA")?;
                    let certificates = Certificate::from_pem_bundle(&pem).map_err(|error| {
                        ShardCacheError::Config(format!(
                            "S3 object overflow TLS CA bundle: {error}"
                        ))
                    })?;
                    if certificates.is_empty() {
                        return Err(ShardCacheError::Config(
                            "S3 object overflow TLS CA bundle contains no certificates".into(),
                        ));
                    }
                    for certificate in certificates {
                        options = options.with_root_certificate(certificate);
                    }
                }
                builder = builder.with_client_options(options);
            }
            if let Some(encryption) = &config.server_side_encryption {
                let key = "server_side_encryption"
                    .parse::<AmazonS3ConfigKey>()
                    .map_err(|error| {
                        ShardCacheError::Config(format!(
                            "S3 object overflow encryption config: {error}"
                        ))
                    })?;
                builder = builder.with_config(key, encryption.clone());
            }
            let client = builder
                .build()
                .map_err(|error| ShardCacheError::Config(format!("S3 object overflow: {error}")))?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| {
                    ShardCacheError::Persistence(format!("S3 object overflow runtime: {error}"))
                })?;
            Ok(Self {
                client: Arc::new(client),
                runtime: Arc::new(runtime),
            })
        }
        #[cfg(not(feature = "object-overflow-s3"))]
        {
            let _ = credentials;
            Err(ShardCacheError::Config(
                "object_overflow.backend = \"s3\" requires the object-overflow-s3 feature".into(),
            ))
        }
    }
}

#[cfg(feature = "object-overflow")]
impl ObjectOverflowStore for S3ObjectOverflowStore {
    fn put_value(&self, object_key: &str, value: &[u8]) -> Result<()> {
        #[cfg(feature = "object-overflow-s3")]
        {
            let path = ObjectStorePath::from(object_key);
            let payload = PutPayload::from(SharedBytes::copy_from_slice(value));
            self.runtime
                .block_on(async { self.client.put(&path, payload).await })
                .map(|_| ())
                .map_err(map_object_store_error)
        }
        #[cfg(not(feature = "object-overflow-s3"))]
        {
            let _ = (object_key, value);
            Err(ShardCacheError::Config(
                "S3 object overflow adapter is unavailable in this build".into(),
            ))
        }
    }

    fn get_value(&self, object_key: &str) -> Result<SharedBytes> {
        #[cfg(feature = "object-overflow-s3")]
        {
            let path = ObjectStorePath::from(object_key);
            self.runtime
                .block_on(async {
                    let result = self.client.get(&path).await?;
                    collect_s3_result_bounded(result, MAX_UNBOUNDED_ADAPTER_READ_BYTES).await
                })
                .map_err(map_object_store_error)
        }
        #[cfg(not(feature = "object-overflow-s3"))]
        {
            let _ = object_key;
            Err(ShardCacheError::Config(
                "S3 object overflow adapter is unavailable in this build".into(),
            ))
        }
    }

    fn delete_value(&self, object_key: &str) -> Result<()> {
        #[cfg(feature = "object-overflow-s3")]
        {
            let path = ObjectStorePath::from(object_key);
            self.runtime
                .block_on(async { self.client.delete(&path).await })
                .map_err(map_object_store_error)
        }
        #[cfg(not(feature = "object-overflow-s3"))]
        {
            let _ = object_key;
            Err(ShardCacheError::Config(
                "S3 object overflow adapter is unavailable in this build".into(),
            ))
        }
    }

    fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        #[cfg(feature = "object-overflow-s3")]
        {
            let prefix = ObjectStorePath::from(prefix);
            self.runtime.block_on(async {
                let mut stream = self.client.list(Some(&prefix));
                let mut keys = Vec::new();
                while let Some(meta) = stream.next().await {
                    let meta = meta.map_err(map_object_store_error)?;
                    keys.push(meta.location.to_string());
                }
                Ok(keys)
            })
        }
        #[cfg(not(feature = "object-overflow-s3"))]
        {
            let _ = prefix;
            Err(ShardCacheError::Config(
                "S3 object overflow adapter is unavailable in this build".into(),
            ))
        }
    }

    fn put_value_with_timeout(
        &self,
        object_key: &str,
        value: &[u8],
        timeout: Duration,
    ) -> Result<()> {
        #[cfg(feature = "object-overflow-s3")]
        {
            let path = ObjectStorePath::from(object_key);
            let payload = PutPayload::from(SharedBytes::copy_from_slice(value));
            self.runtime
                .block_on(async {
                    tokio::time::timeout(timeout, self.client.put(&path, payload)).await
                })
                .map_err(|_| object_store_deadline("put"))?
                .map(|_| ())
                .map_err(map_object_store_error)
        }
        #[cfg(not(feature = "object-overflow-s3"))]
        {
            let _ = (object_key, value, timeout);
            Err(ShardCacheError::Config(
                "S3 object overflow adapter is unavailable in this build".into(),
            ))
        }
    }

    fn get_value_with_timeout(&self, object_key: &str, timeout: Duration) -> Result<SharedBytes> {
        #[cfg(feature = "object-overflow-s3")]
        {
            let path = ObjectStorePath::from(object_key);
            self.runtime
                .block_on(async {
                    tokio::time::timeout(timeout, async {
                        let result = self.client.get(&path).await?;
                        collect_s3_result_bounded(result, MAX_UNBOUNDED_ADAPTER_READ_BYTES).await
                    })
                    .await
                })
                .map_err(|_| object_store_deadline("get"))?
                .map_err(map_object_store_error)
        }
        #[cfg(not(feature = "object-overflow-s3"))]
        {
            let _ = (object_key, timeout);
            Err(ShardCacheError::Config(
                "S3 object overflow adapter is unavailable in this build".into(),
            ))
        }
    }

    fn get_value_bounded_with_timeout(
        &self,
        object_key: &str,
        max_bytes: usize,
        timeout: Duration,
    ) -> Result<SharedBytes> {
        #[cfg(feature = "object-overflow-s3")]
        {
            let path = ObjectStorePath::from(object_key);
            self.runtime
                .block_on(async {
                    tokio::time::timeout(timeout, async {
                        let result = self.client.get(&path).await?;
                        collect_s3_result_bounded(result, max_bytes).await
                    })
                    .await
                })
                .map_err(|_| object_store_deadline("get"))?
                .map_err(map_object_store_error)
        }
        #[cfg(not(feature = "object-overflow-s3"))]
        {
            let _ = (object_key, max_bytes, timeout);
            Err(ShardCacheError::Config(
                "S3 object overflow adapter is unavailable in this build".into(),
            ))
        }
    }

    fn delete_value_with_timeout(&self, object_key: &str, timeout: Duration) -> Result<()> {
        #[cfg(feature = "object-overflow-s3")]
        {
            let path = ObjectStorePath::from(object_key);
            self.runtime
                .block_on(async { tokio::time::timeout(timeout, self.client.delete(&path)).await })
                .map_err(|_| object_store_deadline("delete"))?
                .map_err(map_object_store_error)
        }
        #[cfg(not(feature = "object-overflow-s3"))]
        {
            let _ = (object_key, timeout);
            Err(ShardCacheError::Config(
                "S3 object overflow adapter is unavailable in this build".into(),
            ))
        }
    }

    fn list_keys_with_timeout(&self, prefix: &str, timeout: Duration) -> Result<Vec<String>> {
        #[cfg(feature = "object-overflow-s3")]
        {
            let prefix = ObjectStorePath::from(prefix);
            self.runtime
                .block_on(async {
                    tokio::time::timeout(timeout, async {
                        let mut stream = self.client.list(Some(&prefix));
                        let mut keys = Vec::new();
                        while let Some(meta) = stream.next().await {
                            let meta = meta.map_err(map_object_store_error)?;
                            keys.push(meta.location.to_string());
                        }
                        Ok(keys)
                    })
                    .await
                })
                .map_err(|_| object_store_deadline("list"))?
        }
        #[cfg(not(feature = "object-overflow-s3"))]
        {
            let _ = (prefix, timeout);
            Err(ShardCacheError::Config(
                "S3 object overflow adapter is unavailable in this build".into(),
            ))
        }
    }

    fn list_keys_bounded_with_timeout(
        &self,
        prefix: &str,
        max_keys: usize,
        max_retained_bytes: usize,
        timeout: Duration,
    ) -> Result<Vec<String>> {
        #[cfg(feature = "object-overflow-s3")]
        {
            let prefix = ObjectStorePath::from(prefix);
            self.runtime
                .block_on(async {
                    tokio::time::timeout(timeout, async {
                        let mut stream = self.client.list(Some(&prefix));
                        let mut keys = Vec::new();
                        let mut retained_bytes = 0usize;
                        while let Some(meta) = stream.next().await {
                            let meta = meta.map_err(map_object_store_error)?;
                            let key = meta.location.to_string();
                            retained_bytes =
                                retained_bytes.checked_add(key.len()).ok_or_else(|| {
                                    ShardCacheError::Persistence(
                                        "object key listing size overflow".into(),
                                    )
                                })?;
                            if keys.len() >= max_keys || retained_bytes > max_retained_bytes {
                                return Err(ShardCacheError::Persistence(
                                    "object key listing exceeds configured cleanup bounds".into(),
                                ));
                            }
                            keys.push(key);
                        }
                        Ok(keys)
                    })
                    .await
                })
                .map_err(|_| object_store_deadline("list"))?
        }
        #[cfg(not(feature = "object-overflow-s3"))]
        {
            let _ = (prefix, max_keys, max_retained_bytes, timeout);
            Err(ShardCacheError::Config(
                "S3 object overflow adapter is unavailable in this build".into(),
            ))
        }
    }

    fn list_keys_page_bounded_with_timeout(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: usize,
        max_retained_bytes: usize,
        timeout: Duration,
    ) -> Result<ObjectKeyPage> {
        #[cfg(feature = "object-overflow-s3")]
        {
            if max_keys == 0 || max_retained_bytes == 0 {
                return Err(ShardCacheError::Config(
                    "object overflow key page bounds must be nonzero".into(),
                ));
            }
            let prefix = ObjectStorePath::from(prefix);
            let offset = start_after.map(ObjectStorePath::from);
            self.runtime
                .block_on(async {
                    tokio::time::timeout(timeout, async {
                        let mut stream = match offset.as_ref() {
                            Some(offset) => self.client.list_with_offset(Some(&prefix), offset),
                            None => self.client.list(Some(&prefix)),
                        };
                        let mut keys = Vec::new();
                        let mut retained_bytes = 0usize;
                        let mut next_after = None;
                        while let Some(meta) = stream.next().await {
                            let meta = meta.map_err(map_object_store_error)?;
                            let key = meta.location.to_string();
                            debug_assert!(
                                start_after.is_none_or(|start_after| key.as_str() > start_after)
                            );
                            let next_bytes =
                                retained_bytes.checked_add(key.len()).ok_or_else(|| {
                                    ShardCacheError::Persistence(
                                        "object key listing size overflow".into(),
                                    )
                                })?;
                            if keys.len() >= max_keys || next_bytes > max_retained_bytes {
                                next_after = keys.last().cloned();
                                if next_after.is_none() {
                                    return Err(ShardCacheError::Persistence(
                                        "one object key exceeds the cleanup page bound".into(),
                                    ));
                                }
                                break;
                            }
                            retained_bytes = next_bytes;
                            keys.push(key);
                        }
                        Ok(ObjectKeyPage { keys, next_after })
                    })
                    .await
                })
                .map_err(|_| object_store_deadline("list"))?
        }
        #[cfg(not(feature = "object-overflow-s3"))]
        {
            let _ = (prefix, start_after, max_keys, max_retained_bytes, timeout);
            Err(ShardCacheError::Config(
                "S3 object overflow adapter is unavailable in this build".into(),
            ))
        }
    }
}

#[cfg(feature = "object-overflow-s3")]
async fn collect_s3_result_bounded(
    result: object_store::GetResult,
    max_bytes: usize,
) -> std::result::Result<SharedBytes, object_store::Error> {
    if result.meta.size > max_bytes as u64 {
        return Err(object_store_size_error());
    }
    let initial_capacity = usize::try_from(result.meta.size).unwrap_or(max_bytes);
    let mut body = Vec::new();
    body.try_reserve_exact(initial_capacity.min(max_bytes))
        .map_err(|_| object_store_size_error())?;
    let mut stream = result.into_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if body
            .len()
            .checked_add(chunk.len())
            .is_none_or(|length| length > max_bytes)
        {
            return Err(object_store_size_error());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(SharedBytes::from(body))
}

#[cfg(feature = "object-overflow-s3")]
fn object_store_size_error() -> object_store::Error {
    object_store::Error::Generic {
        store: "S3",
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "object payload exceeds trusted bound",
        )),
    }
}

#[cfg(feature = "object-overflow-s3")]
fn object_store_deadline(operation: &str) -> ShardCacheError {
    ShardCacheError::Persistence(format!("S3 object overflow {operation} timed out"))
}

#[cfg(feature = "object-overflow-s3")]
fn map_object_store_error(error: object_store::Error) -> ShardCacheError {
    match error {
        object_store::Error::Generic { source, .. }
            if source
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::InvalidData) =>
        {
            ShardCacheError::ObjectIntegrity(format!("S3 object overflow: {source}"))
        }
        object_store::Error::NotFound { .. } => {
            ShardCacheError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, error))
        }
        object_store::Error::InvalidPath { .. }
        | object_store::Error::PermissionDenied { .. }
        | object_store::Error::Unauthenticated { .. }
        | object_store::Error::UnknownConfigurationKey { .. } => {
            ShardCacheError::Config(format!("S3 object overflow: {error}"))
        }
        _ => ShardCacheError::Persistence(format!("S3 object overflow: {error}")),
    }
}

#[cfg(feature = "object-overflow-s3")]
fn read_bounded_config_file(path: &Path, max_bytes: usize, label: &str) -> Result<Vec<u8>> {
    let file = File::open(path).map_err(|error| {
        ShardCacheError::Config(format!(
            "S3 object overflow {label} file {}: {error}",
            path.display()
        ))
    })?;
    let metadata = file.metadata().map_err(|error| {
        ShardCacheError::Config(format!(
            "S3 object overflow {label} file {}: {error}",
            path.display()
        ))
    })?;
    if !metadata.is_file() || metadata.len() > max_bytes as u64 {
        return Err(ShardCacheError::Config(format!(
            "S3 object overflow {label} file must be a regular file of at most {max_bytes} bytes"
        )));
    }
    let mut value = Vec::with_capacity(
        usize::try_from(metadata.len())
            .unwrap_or(max_bytes)
            .min(max_bytes),
    );
    file.take(max_bytes.saturating_add(1) as u64)
        .read_to_end(&mut value)
        .map_err(|error| {
            ShardCacheError::Config(format!(
                "S3 object overflow {label} file {}: {error}",
                path.display()
            ))
        })?;
    if value.len() > max_bytes {
        return Err(ShardCacheError::Config(format!(
            "S3 object overflow {label} file exceeds {max_bytes} bytes"
        )));
    }
    Ok(value)
}

#[cfg(feature = "object-overflow")]
struct ObjectOverflowCredentials {
    access_key: String,
    secret_key: String,
}

#[cfg(feature = "object-overflow")]
impl std::fmt::Debug for ObjectOverflowCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectOverflowCredentials")
            .field("access_key", &"<redacted>")
            .field("secret_key", &"<redacted>")
            .finish()
    }
}

#[cfg(feature = "object-overflow")]
impl ObjectOverflowCredentials {
    fn from_config(config: &ObjectOverflowConfig) -> Result<Self> {
        let (Some(access_key_env), Some(secret_key_env)) = (
            config.access_key_env.as_deref(),
            config.secret_key_env.as_deref(),
        ) else {
            return Err(ShardCacheError::Config(
                "S3 object overflow requires access_key_env and secret_key_env".into(),
            ));
        };
        Ok(Self {
            access_key: read_credential_env(access_key_env)?,
            secret_key: read_credential_env(secret_key_env)?,
        })
    }
}

#[cfg(feature = "object-overflow")]
fn read_credential_env(name: &str) -> Result<String> {
    let value = std::env::var(name).map_err(|_| {
        ShardCacheError::Config(format!(
            "object overflow credential environment variable is not set: {name}"
        ))
    })?;
    if value.is_empty() {
        return Err(ShardCacheError::Config(format!(
            "object overflow credential environment variable is empty: {name}"
        )));
    }
    Ok(value)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::*;

    #[derive(Debug, Default)]
    pub(crate) struct InMemoryObjectOverflowStore {
        values: Mutex<HashMap<String, SharedBytes>>,
    }

    impl InMemoryObjectOverflowStore {
        pub(crate) fn overwrite(&self, object_key: &str, value: &[u8]) {
            self.values
                .lock()
                .expect("object store lock")
                .insert(object_key.to_string(), SharedBytes::copy_from_slice(value));
        }

        pub(crate) fn remove(&self, object_key: &str) {
            self.values
                .lock()
                .expect("object store lock")
                .remove(object_key);
        }
    }

    impl ObjectOverflowStore for InMemoryObjectOverflowStore {
        fn put_value(&self, object_key: &str, value: &[u8]) -> Result<()> {
            self.values
                .lock()
                .expect("object store lock")
                .insert(object_key.to_string(), SharedBytes::copy_from_slice(value));
            Ok(())
        }

        fn get_value(&self, object_key: &str) -> Result<SharedBytes> {
            self.values
                .lock()
                .expect("object store lock")
                .get(object_key)
                .cloned()
                .ok_or_else(|| ShardCacheError::Persistence("object value missing".into()))
        }

        fn delete_value(&self, object_key: &str) -> Result<()> {
            self.values
                .lock()
                .expect("object store lock")
                .remove(object_key);
            Ok(())
        }
    }

    #[cfg(feature = "object-overflow")]
    #[derive(Debug, Default)]
    struct CleanupTraversalStore {
        values: Mutex<HashMap<String, SharedBytes>>,
        bounded_list_calls: AtomicUsize,
        paged_list_calls: AtomicUsize,
    }

    #[cfg(feature = "object-overflow")]
    impl ObjectOverflowStore for CleanupTraversalStore {
        fn put_value(&self, object_key: &str, value: &[u8]) -> Result<()> {
            self.values
                .lock()
                .expect("cleanup store lock")
                .insert(object_key.to_string(), SharedBytes::copy_from_slice(value));
            Ok(())
        }

        fn get_value(&self, object_key: &str) -> Result<SharedBytes> {
            self.values
                .lock()
                .expect("cleanup store lock")
                .get(object_key)
                .cloned()
                .ok_or_else(|| ShardCacheError::Persistence("object value missing".into()))
        }

        fn delete_value(&self, object_key: &str) -> Result<()> {
            self.values
                .lock()
                .expect("cleanup store lock")
                .remove(object_key);
            Ok(())
        }

        fn list_keys_bounded_with_timeout(
            &self,
            prefix: &str,
            max_keys: usize,
            max_retained_bytes: usize,
            _timeout: Duration,
        ) -> Result<Vec<String>> {
            self.bounded_list_calls.fetch_add(1, Ordering::Relaxed);
            let mut keys = self
                .values
                .lock()
                .expect("cleanup store lock")
                .keys()
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect::<Vec<_>>();
            keys.sort_unstable();
            validate_key_listing(&keys, max_keys, max_retained_bytes)?;
            Ok(keys)
        }

        fn list_keys_page_bounded_with_timeout(
            &self,
            _prefix: &str,
            _start_after: Option<&str>,
            _max_keys: usize,
            _max_retained_bytes: usize,
            _timeout: Duration,
        ) -> Result<ObjectKeyPage> {
            self.paged_list_calls.fetch_add(1, Ordering::Relaxed);
            Err(ShardCacheError::Persistence(
                "cleanup must not use rescanning pagination".into(),
            ))
        }
    }

    #[derive(Debug)]
    struct BlockingDeleteStore {
        values: Mutex<HashMap<String, SharedBytes>>,
        delete_started: Sender<()>,
        release_delete: Receiver<()>,
    }

    #[derive(Debug, Default)]
    struct PermanentFailureStore {
        put_calls: AtomicUsize,
    }

    impl ObjectOverflowStore for PermanentFailureStore {
        fn put_value(&self, _object_key: &str, _value: &[u8]) -> Result<()> {
            self.put_calls.fetch_add(1, Ordering::Relaxed);
            Err(ShardCacheError::Config(
                "object store authentication failed".into(),
            ))
        }

        fn get_value(&self, _object_key: &str) -> Result<SharedBytes> {
            Err(ShardCacheError::Config(
                "object store authentication failed".into(),
            ))
        }

        fn delete_value(&self, _object_key: &str) -> Result<()> {
            Err(ShardCacheError::Config(
                "object store authentication failed".into(),
            ))
        }
    }

    impl ObjectOverflowStore for BlockingDeleteStore {
        fn put_value(&self, object_key: &str, value: &[u8]) -> Result<()> {
            self.values
                .lock()
                .expect("object store lock")
                .insert(object_key.to_string(), SharedBytes::copy_from_slice(value));
            Ok(())
        }

        fn get_value(&self, object_key: &str) -> Result<SharedBytes> {
            self.values
                .lock()
                .expect("object store lock")
                .get(object_key)
                .cloned()
                .ok_or_else(|| ShardCacheError::Persistence("object value missing".into()))
        }

        fn delete_value(&self, object_key: &str) -> Result<()> {
            if object_key == "blocked" {
                self.delete_started
                    .send(())
                    .expect("signal blocked object delete");
                self.release_delete
                    .recv()
                    .expect("release blocked object delete");
            }
            self.values
                .lock()
                .expect("object store lock")
                .remove(object_key);
            Ok(())
        }
    }

    #[test]
    fn timed_out_backend_gets_a_bounded_replacement_and_cannot_block_drop() {
        let (delete_started_tx, delete_started_rx) = bounded(1);
        let (release_delete_tx, release_delete_rx) = bounded(1);
        let store = Arc::new(BlockingDeleteStore {
            values: Mutex::new(HashMap::new()),
            delete_started: delete_started_tx,
            release_delete: release_delete_rx,
        });
        let pool = ObjectOverflowWorkerPool::new(
            store,
            1,
            8,
            0,
            Duration::from_millis(1),
            Duration::from_millis(25),
        )
        .expect("worker pool");

        let error = pool
            .delete_value("blocked", Duration::from_millis(25))
            .expect_err("blocked backend operation must time out");
        assert!(error.to_string().contains("timed out"));
        assert_eq!(pool.stats.snapshot().timeouts, 1);
        delete_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("backend delete started");

        pool.put_value(
            "healthy",
            SharedBytes::from_static(b"value"),
            ObjectOverflowCompression::None,
            0,
            Duration::from_secs(1),
        )
        .expect("replacement worker must serve a healthy target");
        assert!(pool.live_workers.load(Ordering::Acquire) <= 2);

        let (drop_done_tx, drop_done_rx) = bounded(1);
        let drop_thread = thread::spawn(move || {
            drop(pool);
            drop_done_tx.send(()).expect("signal pool drop");
        });
        drop_done_rx
            .recv_timeout(Duration::from_millis(250))
            .expect("pool drop must not join a stuck backend operation");
        release_delete_tx.send(()).expect("release backend delete");
        drop_thread.join().expect("pool drop thread");
    }

    #[test]
    fn permanent_failures_are_classified_once_and_are_not_retried() {
        let store = Arc::new(PermanentFailureStore::default());
        let pool = ObjectOverflowWorkerPool::new(
            Arc::clone(&store) as Arc<dyn ObjectOverflowStore>,
            1,
            8,
            3,
            Duration::from_millis(1),
            Duration::from_secs(1),
        )
        .expect("worker pool");

        pool.put_value(
            "denied",
            SharedBytes::from_static(b"value"),
            ObjectOverflowCompression::None,
            0,
            Duration::from_secs(1),
        )
        .expect_err("authentication failure must be returned");

        let stats = pool.stats.snapshot();
        assert_eq!(store.put_calls.load(Ordering::Relaxed), 1);
        assert_eq!(stats.retries, 0);
        assert_eq!(stats.auth_config_failures, 1);
        assert_eq!(stats.unavailable_failures, 0);
    }

    #[cfg(feature = "object-overflow")]
    #[test]
    fn cleanup_uses_one_bounded_traversal_per_prefix() {
        let store = Arc::new(CleanupTraversalStore::default());
        let stale_marker = GenerationMarker {
            node_id: "node-a".into(),
            generation_id: "stale".into(),
            created_ms: 0,
            heartbeat_ms: 0,
        };
        store
            .put_value(
                "overflow/node-a/stale/_generation.json",
                &serde_json::to_vec(&stale_marker).expect("marker"),
            )
            .expect("put marker");
        for index in 0..5_000 {
            store
                .put_value(
                    &format!("overflow/node-a/stale/shard-0/{index}.bin"),
                    b"value",
                )
                .expect("put stale value");
        }
        let pool = ObjectOverflowWorkerPool::new(
            Arc::clone(&store) as Arc<dyn ObjectOverflowStore>,
            1,
            8,
            0,
            Duration::from_millis(1),
            Duration::from_secs(5),
        )
        .expect("worker pool");

        cleanup_stale_generations_with(
            &pool,
            "overflow",
            "node-a",
            "current",
            Duration::from_secs(5),
            1,
            None,
        )
        .expect("cleanup");

        assert_eq!(store.bounded_list_calls.load(Ordering::Relaxed), 2);
        assert_eq!(store.paged_list_calls.load(Ordering::Relaxed), 0);
        assert!(store.values.lock().expect("cleanup store lock").is_empty());
    }

    #[cfg(feature = "object-overflow")]
    #[test]
    fn s3_credentials_require_both_configured_environment_names() {
        let mut config = ObjectOverflowConfig {
            backend: ObjectOverflowBackend::S3,
            ..ObjectOverflowConfig::default()
        };
        assert!(ObjectOverflowCredentials::from_config(&config).is_err());
        config.access_key_env = Some("OBJECT_ACCESS_KEY".into());
        assert!(ObjectOverflowCredentials::from_config(&config).is_err());
    }

    #[cfg(feature = "object-overflow-s3")]
    #[test]
    fn typed_s3_authentication_errors_are_preserved_as_config_failures() {
        for error in [
            object_store::Error::PermissionDenied {
                path: "bucket/key".into(),
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "denied",
                )),
            },
            object_store::Error::Unauthenticated {
                path: "bucket/key".into(),
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "unauthenticated",
                )),
            },
        ] {
            assert!(matches!(
                map_object_store_error(error),
                ShardCacheError::Config(_)
            ));
        }
    }

    #[cfg(feature = "object-overflow-s3")]
    #[test]
    fn s3_adapter_emits_the_configured_server_side_encryption_header() {
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("mock S3 listener");
        let address = listener.local_addr().expect("mock S3 address");
        let server = thread::Builder::new()
            .name("mock-s3-sse".into())
            .spawn(move || {
                let (mut stream, _) = listener.accept().expect("mock S3 request");
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("mock S3 timeout");
                let mut request = Vec::new();
                let mut chunk = [0u8; 4096];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let read = stream.read(&mut chunk).expect("read mock S3 request");
                    assert_ne!(read, 0, "mock S3 request ended before its headers");
                    request.extend_from_slice(&chunk[..read]);
                    assert!(request.len() <= 64 * 1024, "mock S3 headers are bounded");
                }
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\nETag: \"test\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .expect("write mock S3 response");
                String::from_utf8(request).expect("ASCII HTTP request")
            })
            .expect("mock S3 thread");

        let access_key_env = format!("SHARDKV_TEST_S3_ACCESS_KEY_{}", address.port());
        let secret_key_env = format!("SHARDKV_TEST_S3_SECRET_KEY_{}", address.port());
        // SAFETY: these unique names are created and consumed only by this test.
        unsafe {
            std::env::set_var(&access_key_env, "test-access-key");
            std::env::set_var(&secret_key_env, "test-secret-key");
        }
        let config = ObjectOverflowConfig {
            backend: ObjectOverflowBackend::S3,
            endpoint: format!("http://{address}"),
            bucket: "test-bucket".into(),
            region: "us-east-1".into(),
            force_path_style: true,
            allow_http: true,
            server_side_encryption: Some("AES256".into()),
            access_key_env: Some(access_key_env.clone()),
            secret_key_env: Some(secret_key_env.clone()),
            ..ObjectOverflowConfig::default()
        };
        let store = S3ObjectOverflowStore::from_config(&config).expect("S3 adapter");
        store
            .put_value("prefix/value.bin", b"encrypted payload")
            .expect("S3 PUT");
        let request = server.join().expect("mock S3 server").to_ascii_lowercase();
        // SAFETY: no code reads these unique test-only names after this point.
        unsafe {
            std::env::remove_var(access_key_env);
            std::env::remove_var(secret_key_env);
        }
        assert!(
            request.contains("x-amz-server-side-encryption: aes256\r\n"),
            "S3 PUT omitted its SSE header: {request}"
        );
    }

    #[test]
    fn expired_queued_job_never_reaches_the_backend() {
        let store = Arc::new(InMemoryObjectOverflowStore::default());
        let pool = ObjectOverflowWorkerPool::new(
            Arc::clone(&store) as Arc<dyn ObjectOverflowStore>,
            1,
            8,
            0,
            Duration::from_millis(1),
            Duration::from_secs(1),
        )
        .expect("worker pool");
        let (reply, receiver) = bounded(1);
        pool.enqueue(ObjectOverflowWorkerJob::Put {
            object_key: "expired".into(),
            raw: SharedBytes::from_static(b"value"),
            compression: ObjectOverflowCompression::None,
            zstd_level: 0,
            deadline: Instant::now()
                .checked_sub(Duration::from_millis(1))
                .expect("past deadline"),
            reply,
        })
        .expect("enqueue expired job");

        let error = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("expired job response")
            .expect_err("expired job must fail");
        assert!(error.to_string().contains("timed out"));
        assert!(
            !store
                .values
                .lock()
                .expect("object store lock")
                .contains_key("expired")
        );
    }

    #[test]
    fn authenticated_payload_rejects_tampering_and_cross_key_substitution() {
        let key = [7u8; 32];
        let raw = b"authenticated overflow value";
        let encoded = ObjectPayloadCodec::encode(
            raw,
            ObjectOverflowCompression::Lz4,
            0,
            "node/generation/a.bin",
            &key,
        )
        .expect("encode");
        let object = ObjectValueRef {
            object_key: "node/generation/a.bin".into(),
            len: raw.len(),
            stored_len: encoded.len(),
            compression: ObjectOverflowCompression::Lz4,
            checksum: crc32fast::hash(raw),
        };
        assert_eq!(
            ObjectPayloadCodec::decode(&encoded, &object, &object.object_key, &key,)
                .expect("decode")
                .as_ref(),
            raw
        );

        let mut tampered = encoded.clone();
        tampered[OBJECT_PAYLOAD_HEADER_LEN] ^= 1;
        assert!(matches!(
            ObjectPayloadCodec::decode(&tampered, &object, &object.object_key, &key),
            Err(ShardCacheError::ObjectIntegrity(_))
        ));
        assert!(matches!(
            ObjectPayloadCodec::decode(&encoded, &object, "node/generation/b.bin", &key),
            Err(ShardCacheError::ObjectIntegrity(_))
        ));
    }

    #[test]
    fn authenticated_lz4_payload_rejects_signed_allocation_amplification() {
        let key = [11u8; 32];
        let object_key = "node/generation/value.bin";
        let raw = b"small";
        let mut encoded =
            ObjectPayloadCodec::encode(raw, ObjectOverflowCompression::Lz4, 0, object_key, &key)
                .expect("encode");
        encoded[OBJECT_PAYLOAD_HEADER_LEN..OBJECT_PAYLOAD_HEADER_LEN + 4]
            .copy_from_slice(&u32::MAX.to_le_bytes());
        let payload_len = encoded.len() - OBJECT_PAYLOAD_TAG_LEN;
        let tag = hmac_sha256(&key, object_key.as_bytes(), &encoded[..payload_len]);
        encoded[payload_len..].copy_from_slice(&tag);
        let object = ObjectValueRef {
            object_key: object_key.into(),
            len: raw.len(),
            stored_len: encoded.len(),
            compression: ObjectOverflowCompression::Lz4,
            checksum: crc32fast::hash(raw),
        };

        let error = ObjectPayloadCodec::decode(&encoded, &object, object_key, &key)
            .expect_err("declared LZ4 length must be checked before allocation");
        assert!(error.to_string().contains("lz4 length mismatch"));
    }

    #[cfg(feature = "object-overflow")]
    #[test]
    fn cleanup_heartbeat_is_independent_of_the_cleanup_scan_interval() {
        assert_eq!(cleanup_heartbeat_interval(1), Duration::from_secs(1));
        assert_eq!(cleanup_heartbeat_interval(30), Duration::from_secs(10));
        assert_eq!(cleanup_heartbeat_interval(86_400), Duration::from_secs(60));
    }
}
