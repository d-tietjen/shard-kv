use std::hint::black_box;
use std::sync::{Arc, Mutex};

use fast_cache::storage::{
    PreparedPointKey, SharedEmbeddedConfig, SharedEmbeddedLockPolicy, SharedEmbeddedStore,
};

use crate::backend::{Backend, BackendClass, BoxError, Worker};

use super::BenchmarkCacheConfig;

pub fn new(
    id: &'static str,
    shard_count: usize,
    capacity_hint: usize,
    copy_reads: bool,
    cache_config: BenchmarkCacheConfig,
) -> Arc<dyn Backend> {
    new_scoped(
        id,
        shard_count,
        capacity_hint,
        SharedBackendOptions::new(
            shared_read_mode(copy_reads),
            SharedKeyScope::All,
            cache_config,
        ),
    )
}

pub fn new_copy_locked(
    id: &'static str,
    shard_count: usize,
    capacity_hint: usize,
    cache_config: BenchmarkCacheConfig,
) -> Arc<dyn Backend> {
    new_scoped(
        id,
        shard_count,
        capacity_hint,
        SharedBackendOptions::new(
            SharedReadMode::CopyLocked,
            SharedKeyScope::All,
            cache_config,
        ),
    )
}

pub fn new_copy_unlocked(
    id: &'static str,
    shard_count: usize,
    capacity_hint: usize,
    cache_config: BenchmarkCacheConfig,
) -> Arc<dyn Backend> {
    new_scoped(
        id,
        shard_count,
        capacity_hint,
        SharedBackendOptions::new(
            SharedReadMode::CopyUnlocked,
            SharedKeyScope::All,
            cache_config,
        ),
    )
}

pub fn new_prepared(
    id: &'static str,
    shard_count: usize,
    capacity_hint: usize,
    copy_reads: bool,
    cache_config: BenchmarkCacheConfig,
) -> Arc<dyn Backend> {
    new_scoped(
        id,
        shard_count,
        capacity_hint,
        SharedBackendOptions::new(
            shared_read_mode(copy_reads),
            SharedKeyScope::All,
            cache_config,
        )
        .with_prepared_keys(),
    )
}

pub fn new_with_policy(
    id: &'static str,
    shard_count: usize,
    capacity_hint: usize,
    copy_reads: bool,
    lock_policy: SharedEmbeddedLockPolicy,
    cache_config: BenchmarkCacheConfig,
) -> Arc<dyn Backend> {
    new_scoped(
        id,
        shard_count,
        capacity_hint,
        SharedBackendOptions::new(
            shared_read_mode(copy_reads),
            SharedKeyScope::All,
            cache_config,
        )
        .with_lock_policy(lock_policy),
    )
}

pub fn new_hot_shard(
    id: &'static str,
    shard_count: usize,
    capacity_hint: usize,
    copy_reads: bool,
    cache_config: BenchmarkCacheConfig,
) -> Arc<dyn Backend> {
    new_scoped(
        id,
        shard_count,
        capacity_hint,
        SharedBackendOptions::new(
            shared_read_mode(copy_reads),
            SharedKeyScope::Shard(0),
            cache_config,
        ),
    )
}

pub fn new_hot_shard_with_policy(
    id: &'static str,
    shard_count: usize,
    capacity_hint: usize,
    copy_reads: bool,
    lock_policy: SharedEmbeddedLockPolicy,
    cache_config: BenchmarkCacheConfig,
) -> Arc<dyn Backend> {
    new_scoped(
        id,
        shard_count,
        capacity_hint,
        SharedBackendOptions::new(
            shared_read_mode(copy_reads),
            SharedKeyScope::Shard(0),
            cache_config,
        )
        .with_lock_policy(lock_policy),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SharedReadMode {
    Ref,
    CopyLocked,
    CopyUnlocked,
}

fn shared_read_mode(copy_reads: bool) -> SharedReadMode {
    if copy_reads {
        SharedReadMode::CopyLocked
    } else {
        SharedReadMode::Ref
    }
}

#[derive(Debug, Clone, Copy)]
struct SharedBackendOptions {
    read_mode: SharedReadMode,
    prepare_keys: bool,
    key_scope: SharedKeyScope,
    lock_policy: SharedEmbeddedLockPolicy,
    cache_config: BenchmarkCacheConfig,
}

impl SharedBackendOptions {
    fn new(
        read_mode: SharedReadMode,
        key_scope: SharedKeyScope,
        cache_config: BenchmarkCacheConfig,
    ) -> Self {
        Self {
            read_mode,
            prepare_keys: false,
            key_scope,
            lock_policy: SharedEmbeddedConfig::default().lock_policy,
            cache_config,
        }
    }

    fn with_prepared_keys(mut self) -> Self {
        self.prepare_keys = true;
        self
    }

    fn with_lock_policy(mut self, lock_policy: SharedEmbeddedLockPolicy) -> Self {
        self.lock_policy = lock_policy;
        self
    }
}

fn new_scoped(
    id: &'static str,
    shard_count: usize,
    capacity_hint: usize,
    options: SharedBackendOptions,
) -> Arc<dyn Backend> {
    match shard_count.next_power_of_two().max(1) {
        1 => Arc::new(FcShared::<1>::new(id, capacity_hint, options)),
        2 => Arc::new(FcShared::<2>::new(id, capacity_hint, options)),
        4 => Arc::new(FcShared::<4>::new(id, capacity_hint, options)),
        8 => Arc::new(FcShared::<8>::new(id, capacity_hint, options)),
        16 => Arc::new(FcShared::<16>::new(id, capacity_hint, options)),
        32 => Arc::new(FcShared::<32>::new(id, capacity_hint, options)),
        64 => Arc::new(FcShared::<64>::new(id, capacity_hint, options)),
        128 => Arc::new(FcShared::<128>::new(id, capacity_hint, options)),
        256 => Arc::new(FcShared::<256>::new(id, capacity_hint, options)),
        shards => panic!("fc-shared benchmark supports up to 256 shards, got {shards}"),
    }
}

pub struct FcShared<const SHARDS: usize> {
    id: &'static str,
    store: SharedEmbeddedStore<SHARDS>,
    state: Arc<Mutex<FcSharedState>>,
    read_mode: SharedReadMode,
    prepare_keys: bool,
    key_scope: SharedKeyScope,
}

#[derive(Default)]
struct FcSharedState {
    prepared_worker_count: Option<usize>,
    all_prepared: Option<Arc<Vec<PreparedPointKey>>>,
    prepared_keys: Vec<Option<Arc<Vec<PreparedPointKey>>>>,
}

impl<const SHARDS: usize> FcShared<SHARDS> {
    fn new(id: &'static str, capacity_hint: usize, options: SharedBackendOptions) -> Self {
        Self {
            id,
            store: SharedEmbeddedStore::new(SharedEmbeddedConfig {
                total_memory_bytes: options.cache_config.total_memory_bytes(),
                eviction_policy: options.cache_config.eviction_policy,
                flat_map_capacity_hint: Some(options.cache_config.entry_capacity(capacity_hint)),
                lock_policy: options.lock_policy,
                ..SharedEmbeddedConfig::default()
            }),
            state: Arc::new(Mutex::new(FcSharedState::default())),
            read_mode: options.read_mode,
            prepare_keys: options.prepare_keys,
            key_scope: options.key_scope,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum SharedKeyScope {
    All,
    Shard(usize),
}

impl<const SHARDS: usize> Backend for FcShared<SHARDS> {
    fn id(&self) -> &str {
        self.id
    }

    fn class(&self) -> BackendClass {
        BackendClass::Embedded
    }

    fn warmup(&self, keys: &[Vec<u8>], value: &[u8]) -> Result<(), BoxError> {
        for key in keys {
            self.store.insert_slice(key, value);
        }
        Ok(())
    }

    fn warmup_ttl(&self, keys: &[Vec<u8>], value: &[u8], ttl_ms: u64) -> Result<(), BoxError> {
        #[cfg(feature = "no-ttl")]
        {
            let _ = (keys, value, ttl_ms);
            Err("fc-shared no-ttl build does not support TTL workloads".into())
        }
        #[cfg(not(feature = "no-ttl"))]
        {
            for key in keys {
                self.store.insert_slice_with_ttl(key, value, Some(ttl_ms));
            }
            Ok(())
        }
    }

    fn new_worker(&self) -> Result<Box<dyn Worker>, BoxError> {
        self.new_worker_for(0, 1)
    }

    fn new_worker_for(
        &self,
        worker_index: usize,
        _worker_count: usize,
    ) -> Result<Box<dyn Worker>, BoxError> {
        let prepared_keys = if self.prepare_keys {
            lock_state(&self.state)?
                .prepared_keys
                .get(worker_index)
                .and_then(Clone::clone)
                .unwrap_or_else(empty_prepared_keys)
        } else {
            empty_prepared_keys()
        };
        Ok(Box::new(FcSharedWorker {
            store: self.store.clone(),
            read_mode: self.read_mode,
            prepared_keys,
        }))
    }

    fn worker_key_indices(
        &self,
        keys: &[Vec<u8>],
        worker_index: usize,
        worker_count: usize,
    ) -> Result<Option<Vec<usize>>, BoxError> {
        if !self.prepare_keys {
            return match self.key_scope {
                SharedKeyScope::All => Ok(None),
                SharedKeyScope::Shard(shard_id) => {
                    let indices = keys
                        .iter()
                        .enumerate()
                        .filter_map(|(index, key)| {
                            (self.store.route_key(key).shard_id == shard_id).then_some(index)
                        })
                        .collect::<Vec<_>>();
                    if indices.is_empty() {
                        return Err(
                            format!("no benchmark keys route to shared shard {shard_id}").into(),
                        );
                    }
                    Ok(Some(indices))
                }
            };
        }
        if worker_count == 0 {
            return Err("fc-shared worker_count must be greater than zero".into());
        }

        let mut state = lock_state(&self.state)?;
        ensure_prepared_slots(&mut state, worker_count);

        match self.key_scope {
            SharedKeyScope::All => {
                let prepared = match state.all_prepared.as_ref() {
                    Some(prepared) if prepared.len() == keys.len() => Arc::clone(prepared),
                    _ => {
                        let prepared = Arc::new(
                            keys.iter()
                                .map(|key| self.store.prepare_point_key(key))
                                .collect::<Vec<_>>(),
                        );
                        state.all_prepared = Some(Arc::clone(&prepared));
                        prepared
                    }
                };
                for slot in &mut state.prepared_keys {
                    *slot = Some(Arc::clone(&prepared));
                }
                Ok(Some((0..keys.len()).collect()))
            }
            SharedKeyScope::Shard(shard_id) => {
                let mut indices = Vec::new();
                let mut prepared = Vec::new();
                for (index, key) in keys.iter().enumerate() {
                    let point_key = self.store.prepare_point_key(key);
                    if point_key.route().shard_id == shard_id {
                        indices.push(index);
                        prepared.push(point_key);
                    }
                }
                if indices.is_empty() {
                    return Err(
                        format!("no benchmark keys route to shared shard {shard_id}").into(),
                    );
                }
                state.prepared_keys[worker_index] = Some(Arc::new(prepared));
                Ok(Some(indices))
            }
        }
    }
}

struct FcSharedWorker<const SHARDS: usize> {
    store: SharedEmbeddedStore<SHARDS>,
    read_mode: SharedReadMode,
    prepared_keys: Arc<Vec<PreparedPointKey>>,
}

impl<const SHARDS: usize> Worker for FcSharedWorker<SHARDS> {
    fn get(&mut self, key: &[u8], scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        Ok(match self.read_mode {
            SharedReadMode::Ref => match self.store.get_ref(key) {
                Some(value) => {
                    black_box(value.value().len());
                    true
                }
                None => false,
            },
            SharedReadMode::CopyLocked => match self.store.get_ref(key) {
                Some(value) => {
                    scratch.clear();
                    scratch.extend_from_slice(value.value());
                    true
                }
                None => false,
            },
            SharedReadMode::CopyUnlocked => match self.store.get_value_bytes(key) {
                Some(value) => {
                    scratch.clear();
                    scratch.extend_from_slice(value.as_ref());
                    true
                }
                None => false,
            },
        })
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), BoxError> {
        self.store.insert_slice(key, value);
        Ok(())
    }

    fn set_ttl(&mut self, key: &[u8], value: &[u8], ttl_ms: u64) -> Result<(), BoxError> {
        #[cfg(feature = "no-ttl")]
        {
            let _ = (key, value, ttl_ms);
            Err("fc-shared no-ttl build does not support TTL writes".into())
        }
        #[cfg(not(feature = "no-ttl"))]
        {
            self.store.insert_slice_with_ttl(key, value, Some(ttl_ms));
            Ok(())
        }
    }

    fn supports_indexed_keys(&self) -> bool {
        !self.prepared_keys.is_empty()
    }

    fn get_index(&mut self, local_index: usize, scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        let prepared = self
            .prepared_keys
            .get(local_index)
            .ok_or("fc-shared prepared key index out of range")?;
        Ok(match self.read_mode {
            SharedReadMode::Ref => match self.store.get_prepared_ref(prepared) {
                Some(value) => {
                    black_box(value.value().len());
                    true
                }
                None => false,
            },
            SharedReadMode::CopyLocked => match self.store.get_prepared_ref(prepared) {
                Some(value) => {
                    scratch.clear();
                    scratch.extend_from_slice(value.value());
                    true
                }
                None => false,
            },
            SharedReadMode::CopyUnlocked => match self.store.get_prepared_value_bytes(prepared) {
                Some(value) => {
                    scratch.clear();
                    scratch.extend_from_slice(value.as_ref());
                    true
                }
                None => false,
            },
        })
    }

    fn set_index(&mut self, local_index: usize, value: &[u8]) -> Result<(), BoxError> {
        let prepared = self
            .prepared_keys
            .get(local_index)
            .ok_or("fc-shared prepared key index out of range")?;
        self.store.insert_prepared_slice(prepared, value);
        Ok(())
    }

    fn set_index_ttl(
        &mut self,
        local_index: usize,
        value: &[u8],
        ttl_ms: u64,
    ) -> Result<(), BoxError> {
        #[cfg(feature = "no-ttl")]
        {
            let _ = (local_index, value, ttl_ms);
            Err("fc-shared no-ttl build does not support indexed TTL writes".into())
        }
        #[cfg(not(feature = "no-ttl"))]
        {
            let prepared = self
                .prepared_keys
                .get(local_index)
                .ok_or("fc-shared prepared key index out of range")?;
            self.store
                .insert_prepared_slice_with_ttl(prepared, value, Some(ttl_ms));
            Ok(())
        }
    }
}

fn empty_prepared_keys() -> Arc<Vec<PreparedPointKey>> {
    Arc::new(Vec::new())
}

fn lock_state(
    state: &Mutex<FcSharedState>,
) -> Result<std::sync::MutexGuard<'_, FcSharedState>, BoxError> {
    state
        .lock()
        .map_err(|_| "fc-shared state mutex poisoned".into())
}

fn ensure_prepared_slots(state: &mut FcSharedState, worker_count: usize) {
    if state.prepared_worker_count == Some(worker_count) {
        return;
    }
    state.prepared_worker_count = Some(worker_count);
    state.all_prepared = None;
    state.prepared_keys = (0..worker_count).map(|_| None).collect();
}
