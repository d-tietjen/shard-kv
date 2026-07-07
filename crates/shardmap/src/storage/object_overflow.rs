#[cfg(feature = "object-overflow")]
use std::fs;
#[cfg(feature = "object-overflow")]
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(feature = "object-overflow")]
use std::sync::Mutex;
#[cfg(feature = "object-overflow")]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes as SharedBytes;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TrySendError, bounded};
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
#[cfg(feature = "object-overflow")]
use serde::{Deserialize, Serialize};

use crate::config::{
    ObjectOverflowBackend, ObjectOverflowCompression, ObjectOverflowConfig,
    ObjectOverflowFailurePolicy,
};
use crate::{Result, ShardCacheError};

#[cfg(feature = "object-overflow-s3")]
use futures_util::StreamExt;
#[cfg(feature = "object-overflow-s3")]
use object_store::aws::{AmazonS3, AmazonS3Builder, AmazonS3ConfigKey};
#[cfg(feature = "object-overflow-s3")]
use object_store::client::ClientOptions;
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
}

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
    consecutive_failures: Arc<AtomicUsize>,
    degraded_until_ms: Arc<AtomicU64>,
    #[cfg(feature = "object-overflow")]
    cleanup_task: Arc<ObjectOverflowCleanupTask>,
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
    pub fn new(store: Arc<dyn ObjectOverflowStore>, options: ObjectOverflowRuntimeOptions) -> Self {
        let workers = Arc::new(ObjectOverflowWorkerPool::new(
            store,
            options.worker_threads.max(1),
            options.queue_capacity.max(1),
            options.max_retries,
            options.retry_backoff,
            options.operation_timeout,
        ));
        Self {
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
            consecutive_failures: Arc::new(AtomicUsize::new(0)),
            degraded_until_ms: Arc::new(AtomicU64::new(0)),
            #[cfg(feature = "object-overflow")]
            cleanup_task: Arc::new(ObjectOverflowCleanupTask::default()),
        }
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
        let runtime = Self::new(store, ObjectOverflowRuntimeOptions::from(config));
        runtime.write_generation_marker()?;
        if config.cleanup_on_start {
            runtime.cleanup_stale_generations(config.cleanup_grace_seconds)?;
        }
        if config.cleanup_interval_seconds > 0 {
            runtime.start_cleanup_task(
                Duration::from_secs(config.cleanup_interval_seconds),
                config.cleanup_grace_seconds,
            );
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

    pub(crate) fn put_value(&self, object_key: &str, value: &[u8]) -> Result<ObjectValueRef> {
        if self.is_degraded() {
            self.workers.stats.record_unavailable();
            return Err(ShardCacheError::Persistence(
                "object overflow is degraded; new offloads are paused".into(),
            ));
        }
        let result = self.workers.put_value(
            object_key,
            SharedBytes::copy_from_slice(value),
            self.compression,
            self.zstd_level,
            self.operation_timeout,
        );
        self.record_operation_result(&result);
        result
    }

    pub(crate) fn get_value(&self, object: &ObjectValueRef) -> Result<SharedBytes> {
        let result = self
            .workers
            .get_value(object.clone(), self.operation_timeout);
        self.record_operation_result(&result);
        result
    }

    pub(crate) fn delete_value(&self, object_key: &str) -> Result<()> {
        let result = self
            .workers
            .delete_value(object_key, self.operation_timeout);
        self.record_operation_result(&result);
        result
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
            worker_threads: self.workers.worker_threads,
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
            Err(error) => {
                self.workers.stats.record_failure(error);
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
            created_ms: now_ms(),
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
        )
    }

    #[cfg(feature = "object-overflow")]
    fn start_cleanup_task(&self, interval: Duration, cleanup_grace_seconds: u64) {
        let mut handle = self
            .cleanup_task
            .handle
            .lock()
            .expect("object overflow cleanup handle lock");
        if handle.is_some() {
            return;
        }
        let shutdown = Arc::clone(&self.cleanup_task.shutdown);
        let workers = Arc::clone(&self.workers);
        let prefix = self.prefix.clone();
        let node_id = self.node_id.clone();
        let generation_id = self.generation_id.clone();
        let operation_timeout = self.operation_timeout;
        *handle = Some(thread::spawn(move || {
            while !shutdown.load(Ordering::Acquire) {
                let mut slept = Duration::ZERO;
                while slept < interval && !shutdown.load(Ordering::Acquire) {
                    let step = interval.saturating_sub(slept).min(Duration::from_secs(1));
                    thread::sleep(step);
                    slept += step;
                }
                if shutdown.load(Ordering::Acquire) {
                    break;
                }
                let _ = cleanup_stale_generations_with(
                    &workers,
                    &prefix,
                    &node_id,
                    &generation_id,
                    operation_timeout,
                    cleanup_grace_seconds,
                );
            }
        }));
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
fn cleanup_stale_generations_with(
    workers: &ObjectOverflowWorkerPool,
    prefix: &str,
    node_id: &str,
    generation_id: &str,
    operation_timeout: Duration,
    cleanup_grace_seconds: u64,
) -> Result<()> {
    let generation_prefix = format!("{}/{}/", prefix.trim_matches('/'), node_id);
    workers.stats.cleanup_scans.fetch_add(1, Ordering::Relaxed);
    let keys = workers.list_keys(&generation_prefix, operation_timeout)?;
    let now = now_ms();
    for key in keys.iter().filter(|key| key.ends_with("/_generation.json")) {
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
        let grace_ms = cleanup_grace_seconds.saturating_mul(1000);
        if marker.created_ms.saturating_add(grace_ms) > now {
            continue;
        }
        let stale_prefix = format!(
            "{}/{stale_generation_id}/",
            generation_prefix.trim_end_matches('/')
        );
        for stale_key in workers.list_keys(&stale_prefix, operation_timeout)? {
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

impl From<&ObjectOverflowConfig> for ObjectOverflowRuntimeOptions {
    fn from(config: &ObjectOverflowConfig) -> Self {
        Self {
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
            generation_id: new_generation_id(),
            cleanup_on_start: config.cleanup_on_start,
            cleanup_grace: Duration::from_secs(config.cleanup_grace_seconds),
        }
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
    pub worker_threads: usize,
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
    worker_threads: usize,
    queue_capacity: usize,
    queue_depth: Arc<AtomicUsize>,
    active_workers: Arc<AtomicUsize>,
    stats: Arc<ObjectOverflowWorkerStats>,
    workers: Vec<thread::JoinHandle<()>>,
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
        if let Some(handle) = self
            .handle
            .lock()
            .expect("object overflow cleanup handle lock")
            .take()
        {
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
        reply: Sender<Result<ObjectValueRef>>,
    },
    #[cfg(feature = "object-overflow")]
    PutRaw {
        object_key: String,
        value: SharedBytes,
        reply: Sender<Result<()>>,
    },
    Get {
        object: ObjectValueRef,
        reply: Sender<Result<SharedBytes>>,
    },
    #[cfg(feature = "object-overflow")]
    GetRaw {
        object_key: String,
        reply: Sender<Result<SharedBytes>>,
    },
    Delete {
        object_key: String,
        reply: Sender<Result<()>>,
    },
    #[cfg(feature = "object-overflow")]
    List {
        prefix: String,
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
    ) -> Self {
        let (sender, receiver) = bounded(queue_capacity);
        let queue_depth = Arc::new(AtomicUsize::new(0));
        let active_workers = Arc::new(AtomicUsize::new(0));
        let stats = Arc::new(ObjectOverflowWorkerStats::default());
        let mut workers = Vec::with_capacity(worker_threads);
        for index in 0..worker_threads {
            let worker = ObjectOverflowWorker {
                index,
                store: Arc::clone(&store),
                receiver: receiver.clone(),
                active_workers: Arc::clone(&active_workers),
                queue_depth: Arc::clone(&queue_depth),
                stats: Arc::clone(&stats),
                max_retries,
                retry_backoff,
                operation_timeout,
            };
            workers.push(thread::spawn(move || worker.run()));
        }
        Self {
            sender,
            worker_threads,
            queue_capacity,
            queue_depth,
            active_workers,
            stats,
            workers,
        }
    }

    fn put_value(
        &self,
        object_key: &str,
        raw: SharedBytes,
        compression: ObjectOverflowCompression,
        zstd_level: i32,
        timeout: Duration,
    ) -> Result<ObjectValueRef> {
        let (reply, receiver) = bounded(1);
        self.enqueue(
            ObjectOverflowWorkerJob::Put {
                object_key: object_key.to_string(),
                raw,
                compression,
                zstd_level,
                reply,
            },
            timeout,
        )?;
        self.recv(receiver, timeout, "put")
    }

    #[cfg(feature = "object-overflow")]
    fn put_raw(&self, object_key: &str, value: SharedBytes, timeout: Duration) -> Result<()> {
        let (reply, receiver) = bounded(1);
        self.enqueue(
            ObjectOverflowWorkerJob::PutRaw {
                object_key: object_key.to_string(),
                value,
                reply,
            },
            timeout,
        )?;
        self.recv(receiver, timeout, "put raw")
    }

    fn get_value(&self, object: ObjectValueRef, timeout: Duration) -> Result<SharedBytes> {
        let (reply, receiver) = bounded(1);
        self.enqueue(ObjectOverflowWorkerJob::Get { object, reply }, timeout)?;
        self.recv(receiver, timeout, "get")
    }

    #[cfg(feature = "object-overflow")]
    fn get_raw(&self, object_key: &str, timeout: Duration) -> Result<SharedBytes> {
        let (reply, receiver) = bounded(1);
        self.enqueue(
            ObjectOverflowWorkerJob::GetRaw {
                object_key: object_key.to_string(),
                reply,
            },
            timeout,
        )?;
        self.recv(receiver, timeout, "get raw")
    }

    fn delete_value(&self, object_key: &str, timeout: Duration) -> Result<()> {
        let (reply, receiver) = bounded(1);
        self.enqueue(
            ObjectOverflowWorkerJob::Delete {
                object_key: object_key.to_string(),
                reply,
            },
            timeout,
        )?;
        self.recv(receiver, timeout, "delete")
    }

    #[cfg(feature = "object-overflow")]
    fn list_keys(&self, prefix: &str, timeout: Duration) -> Result<Vec<String>> {
        let (reply, receiver) = bounded(1);
        self.enqueue(
            ObjectOverflowWorkerJob::List {
                prefix: prefix.to_string(),
                reply,
            },
            timeout,
        )?;
        self.recv(receiver, timeout, "list")
    }

    fn enqueue(&self, job: ObjectOverflowWorkerJob, _timeout: Duration) -> Result<()> {
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
            Ok(result) => result,
            Err(RecvTimeoutError::Timeout) => {
                self.stats.timeouts.fetch_add(1, Ordering::Relaxed);
                Err(ShardCacheError::Persistence(format!(
                    "object overflow {operation} timed out"
                )))
            }
            Err(RecvTimeoutError::Disconnected) => Err(ShardCacheError::ChannelClosed(
                "object overflow worker response",
            )),
        }
    }

    fn pending_jobs(&self) -> usize {
        self.queue_depth
            .load(Ordering::Relaxed)
            .saturating_add(self.active_workers.load(Ordering::Relaxed))
    }
}

impl Drop for ObjectOverflowWorkerPool {
    fn drop(&mut self) {
        for _ in 0..self.worker_threads {
            let _ = self.sender.try_send(ObjectOverflowWorkerJob::Stop);
        }
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

struct ObjectOverflowWorker {
    index: usize,
    store: Arc<dyn ObjectOverflowStore>,
    receiver: Receiver<ObjectOverflowWorkerJob>,
    active_workers: Arc<AtomicUsize>,
    queue_depth: Arc<AtomicUsize>,
    stats: Arc<ObjectOverflowWorkerStats>,
    max_retries: usize,
    retry_backoff: Duration,
    operation_timeout: Duration,
}

impl ObjectOverflowWorker {
    fn run(self) {
        while let Ok(job) = self.receiver.recv() {
            match job {
                ObjectOverflowWorkerJob::Stop => break,
                job => {
                    self.queue_depth.fetch_sub(1, Ordering::AcqRel);
                    self.handle_job(job);
                }
            }
        }
        let _ = self.index;
    }

    fn handle_job(&self, job: ObjectOverflowWorkerJob) {
        self.active_workers.fetch_add(1, Ordering::AcqRel);
        match job {
            ObjectOverflowWorkerJob::Put {
                object_key,
                raw,
                compression,
                zstd_level,
                reply,
            } => {
                let result = self.put_encoded(&object_key, &raw, compression, zstd_level);
                let _ = reply.send(result);
            }
            #[cfg(feature = "object-overflow")]
            ObjectOverflowWorkerJob::PutRaw {
                object_key,
                value,
                reply,
            } => {
                let result = self.with_retries(|| self.store.put_value(&object_key, &value));
                let _ = reply.send(result);
            }
            ObjectOverflowWorkerJob::Get { object, reply } => {
                let result = self.get_decoded(&object);
                let _ = reply.send(result);
            }
            #[cfg(feature = "object-overflow")]
            ObjectOverflowWorkerJob::GetRaw { object_key, reply } => {
                let result = self.with_retries(|| self.store.get_value(&object_key));
                let _ = reply.send(result);
            }
            ObjectOverflowWorkerJob::Delete { object_key, reply } => {
                let start = Instant::now();
                let result = self.with_retries(|| self.store.delete_value(&object_key));
                self.stats.delete_ops.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .delete_latency_us
                    .fetch_add(elapsed_us(start), Ordering::Relaxed);
                if result.is_err() {
                    self.stats.delete_failures.fetch_add(1, Ordering::Relaxed);
                }
                let _ = reply.send(result);
            }
            #[cfg(feature = "object-overflow")]
            ObjectOverflowWorkerJob::List { prefix, reply } => {
                let result = self.with_retries(|| self.store.list_keys(&prefix));
                let _ = reply.send(result);
            }
            ObjectOverflowWorkerJob::Stop => {}
        }
        self.active_workers.fetch_sub(1, Ordering::AcqRel);
    }

    fn put_encoded(
        &self,
        object_key: &str,
        raw: &[u8],
        compression: ObjectOverflowCompression,
        zstd_level: i32,
    ) -> Result<ObjectValueRef> {
        let encode_start = Instant::now();
        let encoded = ObjectPayloadCodec::encode(raw, compression, zstd_level)?;
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
        self.with_retries(|| self.store.put_value(object_key, &encoded))?;
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

    fn get_decoded(&self, object: &ObjectValueRef) -> Result<SharedBytes> {
        let download_start = Instant::now();
        let encoded = self.with_retries(|| self.store.get_value(&object.object_key))?;
        self.stats.download_ops.fetch_add(1, Ordering::Relaxed);
        self.stats
            .download_latency_us
            .fetch_add(elapsed_us(download_start), Ordering::Relaxed);
        let decode_start = Instant::now();
        let decoded = ObjectPayloadCodec::decode(&encoded, object)?;
        self.stats.decode_ops.fetch_add(1, Ordering::Relaxed);
        self.stats
            .decode_latency_us
            .fetch_add(elapsed_us(decode_start), Ordering::Relaxed);
        Ok(decoded)
    }

    fn with_retries<T>(&self, mut op: impl FnMut() -> Result<T>) -> Result<T> {
        let deadline = Instant::now() + self.operation_timeout;
        let mut attempts = 0usize;
        loop {
            if Instant::now() >= deadline {
                self.stats.timeouts.fetch_add(1, Ordering::Relaxed);
                return Err(ShardCacheError::Persistence(
                    "object overflow operation deadline exceeded".into(),
                ));
            }
            match op() {
                Ok(value) => return Ok(value),
                Err(error) if attempts < self.max_retries => {
                    attempts = attempts.saturating_add(1);
                    self.stats.retries.fetch_add(1, Ordering::Relaxed);
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        self.stats.timeouts.fetch_add(1, Ordering::Relaxed);
                        return Err(error);
                    }
                    thread::sleep(self.retry_backoff.min(remaining));
                }
                Err(error) => return Err(error),
            }
        }
    }
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

fn new_generation_id() -> String {
    static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
    format!(
        "{}-{}-{}",
        now_ms(),
        std::process::id(),
        NEXT_GENERATION.fetch_add(1, Ordering::Relaxed)
    )
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

const OBJECT_PAYLOAD_MAGIC: &[u8; 8] = b"SCOVF1\0\0";
const OBJECT_PAYLOAD_HEADER_LEN: usize = 8 + 1 + 8 + 8 + 4;

impl ObjectPayloadCodec {
    fn encode(
        raw: &[u8],
        compression: ObjectOverflowCompression,
        zstd_level: i32,
    ) -> Result<Vec<u8>> {
        let body = match compression {
            ObjectOverflowCompression::None => raw.to_vec(),
            ObjectOverflowCompression::Lz4 => compress_prepend_size(raw),
            ObjectOverflowCompression::Zstd => zstd::bulk::compress(raw, zstd_level)
                .map_err(|error| ShardCacheError::Persistence(format!("zstd encode: {error}")))?,
        };
        let mut encoded = Vec::with_capacity(OBJECT_PAYLOAD_HEADER_LEN + body.len());
        encoded.extend_from_slice(OBJECT_PAYLOAD_MAGIC);
        encoded.push(Self::compression_byte(compression));
        encoded.extend_from_slice(&(raw.len() as u64).to_le_bytes());
        encoded.extend_from_slice(&(body.len() as u64).to_le_bytes());
        encoded.extend_from_slice(&crc32fast::hash(raw).to_le_bytes());
        encoded.extend_from_slice(&body);
        Ok(encoded)
    }

    fn decode(encoded: &[u8], object: &ObjectValueRef) -> Result<SharedBytes> {
        if encoded.len() < OBJECT_PAYLOAD_HEADER_LEN {
            return Err(ShardCacheError::ObjectIntegrity(
                "object overflow payload header is truncated".into(),
            ));
        }
        if &encoded[..OBJECT_PAYLOAD_MAGIC.len()] != OBJECT_PAYLOAD_MAGIC {
            return Err(ShardCacheError::ObjectIntegrity(
                "object overflow payload magic mismatch".into(),
            ));
        }
        let compression = Self::compression_from_byte(encoded[OBJECT_PAYLOAD_MAGIC.len()])?;
        let mut cursor = OBJECT_PAYLOAD_MAGIC.len() + 1;
        let raw_len = Self::read_u64(encoded, &mut cursor, "raw length")? as usize;
        let stored_body_len = Self::read_u64(encoded, &mut cursor, "stored length")? as usize;
        let checksum = Self::read_u32(encoded, &mut cursor, "checksum")?;
        let body = &encoded[cursor..];
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
            ObjectOverflowCompression::Lz4 => decompress_size_prepended(body)
                .map_err(|error| ShardCacheError::Persistence(format!("lz4 decode: {error}")))?,
            ObjectOverflowCompression::Zstd => zstd::bulk::decompress(body, raw_len)
                .map_err(|error| ShardCacheError::Persistence(format!("zstd decode: {error}")))?,
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
            other => Err(ShardCacheError::Persistence(format!(
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

#[cfg(feature = "object-overflow")]
#[derive(Debug, Clone)]
pub struct FileObjectOverflowStore {
    root: PathBuf,
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
        Ok(Self { root })
    }

    fn path_for_key(&self, object_key: &str) -> Result<PathBuf> {
        let mut path = self.root.clone();
        for component in object_key.split('/') {
            if component.is_empty() || component == "." || component == ".." {
                return Err(ShardCacheError::Config(format!(
                    "invalid object overflow key component: {component:?}"
                )));
            }
            path.push(component);
        }
        Ok(path)
    }

    fn ensure_parent(path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(())
    }

    fn collect_keys(root: &Path, current: &Path, output: &mut Vec<String>) -> Result<()> {
        for entry in fs::read_dir(current)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                Self::collect_keys(root, &path, output)?;
            } else if path.is_file() {
                let relative = path.strip_prefix(root).map_err(|error| {
                    ShardCacheError::Persistence(format!("object key strip prefix: {error}"))
                })?;
                let key = relative
                    .components()
                    .map(|component| component.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                output.push(key);
            }
        }
        Ok(())
    }
}

#[cfg(feature = "object-overflow")]
impl ObjectOverflowStore for FileObjectOverflowStore {
    fn put_value(&self, object_key: &str, value: &[u8]) -> Result<()> {
        let path = self.path_for_key(object_key)?;
        Self::ensure_parent(&path)?;
        fs::write(path, value)?;
        Ok(())
    }

    fn get_value(&self, object_key: &str) -> Result<SharedBytes> {
        let path = self.path_for_key(object_key)?;
        Ok(SharedBytes::from(fs::read(path)?))
    }

    fn delete_value(&self, object_key: &str) -> Result<()> {
        let path = self.path_for_key(object_key)?;
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        if !self.root.exists() {
            return Ok(keys);
        }
        Self::collect_keys(&self.root, &self.root, &mut keys)?;
        keys.retain(|key| key.starts_with(prefix));
        Ok(keys)
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
                .with_virtual_hosted_style_request(!config.force_path_style);
            if let Some(access_key) = credentials.access_key {
                builder = builder.with_access_key_id(access_key);
            }
            if let Some(secret_key) = credentials.secret_key {
                builder = builder.with_secret_access_key(secret_key);
            }
            if !config.tls_verify {
                let options = ClientOptions::new()
                    .with_allow_http(config.allow_http)
                    .with_allow_invalid_certificates(true);
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
                .block_on(self.client.put(&path, payload))
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
                    result.bytes().await
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
                .block_on(self.client.delete(&path))
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
}

#[cfg(feature = "object-overflow-s3")]
fn map_object_store_error(error: object_store::Error) -> ShardCacheError {
    match error {
        object_store::Error::NotFound { .. } => {
            ShardCacheError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, error))
        }
        object_store::Error::InvalidPath { .. } => ShardCacheError::Config(error.to_string()),
        _ => ShardCacheError::Persistence(format!("S3 object overflow: {error}")),
    }
}

#[cfg(feature = "object-overflow")]
struct ObjectOverflowCredentials {
    access_key: Option<String>,
    secret_key: Option<String>,
}

#[cfg(feature = "object-overflow")]
impl std::fmt::Debug for ObjectOverflowCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectOverflowCredentials")
            .field(
                "access_key",
                &self.access_key.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "secret_key",
                &self.secret_key.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[cfg(feature = "object-overflow")]
impl ObjectOverflowCredentials {
    fn from_config(config: &ObjectOverflowConfig) -> Result<Self> {
        Ok(Self {
            access_key: read_credential_env(config.access_key_env.as_deref())?,
            secret_key: read_credential_env(config.secret_key_env.as_deref())?,
        })
    }
}

#[cfg(feature = "object-overflow")]
fn read_credential_env(name: Option<&str>) -> Result<Option<String>> {
    let Some(name) = name else {
        return Ok(None);
    };
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
    Ok(Some(value))
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
}
