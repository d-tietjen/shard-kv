use std::hint::black_box;
use std::sync::Arc;

use fast_cache::storage::{SharedEmbeddedConfig, SharedEmbeddedLockPolicy, SharedEmbeddedStore};

use crate::backend::{Backend, BackendClass, BoxError, Worker};

use super::BenchmarkCacheConfig;

pub fn new(
    id: &'static str,
    shard_count: usize,
    capacity_hint: usize,
    copy_reads: bool,
    cache_config: BenchmarkCacheConfig,
) -> Arc<dyn Backend> {
    new_with_policy(
        id,
        shard_count,
        capacity_hint,
        copy_reads,
        SharedEmbeddedConfig::default().lock_policy,
        cache_config,
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
        copy_reads,
        SharedKeyScope::All,
        lock_policy,
        cache_config,
    )
}

pub fn new_hot_shard(
    id: &'static str,
    shard_count: usize,
    capacity_hint: usize,
    copy_reads: bool,
    cache_config: BenchmarkCacheConfig,
) -> Arc<dyn Backend> {
    new_hot_shard_with_policy(
        id,
        shard_count,
        capacity_hint,
        copy_reads,
        SharedEmbeddedConfig::default().lock_policy,
        cache_config,
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
        copy_reads,
        SharedKeyScope::Shard(0),
        lock_policy,
        cache_config,
    )
}

fn new_scoped(
    id: &'static str,
    shard_count: usize,
    capacity_hint: usize,
    copy_reads: bool,
    key_scope: SharedKeyScope,
    lock_policy: SharedEmbeddedLockPolicy,
    cache_config: BenchmarkCacheConfig,
) -> Arc<dyn Backend> {
    match shard_count.next_power_of_two().max(1) {
        1 => Arc::new(FcShared::<1>::new(
            id,
            capacity_hint,
            copy_reads,
            key_scope,
            lock_policy,
            cache_config,
        )),
        2 => Arc::new(FcShared::<2>::new(
            id,
            capacity_hint,
            copy_reads,
            key_scope,
            lock_policy,
            cache_config,
        )),
        4 => Arc::new(FcShared::<4>::new(
            id,
            capacity_hint,
            copy_reads,
            key_scope,
            lock_policy,
            cache_config,
        )),
        8 => Arc::new(FcShared::<8>::new(
            id,
            capacity_hint,
            copy_reads,
            key_scope,
            lock_policy,
            cache_config,
        )),
        16 => Arc::new(FcShared::<16>::new(
            id,
            capacity_hint,
            copy_reads,
            key_scope,
            lock_policy,
            cache_config,
        )),
        32 => Arc::new(FcShared::<32>::new(
            id,
            capacity_hint,
            copy_reads,
            key_scope,
            lock_policy,
            cache_config,
        )),
        64 => Arc::new(FcShared::<64>::new(
            id,
            capacity_hint,
            copy_reads,
            key_scope,
            lock_policy,
            cache_config,
        )),
        128 => Arc::new(FcShared::<128>::new(
            id,
            capacity_hint,
            copy_reads,
            key_scope,
            lock_policy,
            cache_config,
        )),
        256 => Arc::new(FcShared::<256>::new(
            id,
            capacity_hint,
            copy_reads,
            key_scope,
            lock_policy,
            cache_config,
        )),
        shards => panic!("fc-shared benchmark supports up to 256 shards, got {shards}"),
    }
}

pub struct FcShared<const SHARDS: usize> {
    id: &'static str,
    store: SharedEmbeddedStore<SHARDS>,
    copy_reads: bool,
    key_scope: SharedKeyScope,
}

impl<const SHARDS: usize> FcShared<SHARDS> {
    fn new(
        id: &'static str,
        capacity_hint: usize,
        copy_reads: bool,
        key_scope: SharedKeyScope,
        lock_policy: SharedEmbeddedLockPolicy,
        cache_config: BenchmarkCacheConfig,
    ) -> Self {
        Self {
            id,
            store: SharedEmbeddedStore::new(SharedEmbeddedConfig {
                total_memory_bytes: cache_config.total_memory_bytes(),
                eviction_policy: cache_config.eviction_policy,
                flat_map_capacity_hint: Some(cache_config.entry_capacity(capacity_hint)),
                lock_policy,
                ..SharedEmbeddedConfig::default()
            }),
            copy_reads,
            key_scope,
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
        for key in keys {
            self.store.insert_slice_with_ttl(key, value, Some(ttl_ms));
        }
        Ok(())
    }

    fn new_worker(&self) -> Result<Box<dyn Worker>, BoxError> {
        Ok(Box::new(FcSharedWorker {
            store: self.store.clone(),
            copy_reads: self.copy_reads,
        }))
    }

    fn worker_key_indices(
        &self,
        keys: &[Vec<u8>],
        _worker_index: usize,
        _worker_count: usize,
    ) -> Result<Option<Vec<usize>>, BoxError> {
        let SharedKeyScope::Shard(shard_id) = self.key_scope else {
            return Ok(None);
        };
        let indices = keys
            .iter()
            .enumerate()
            .filter_map(|(index, key)| {
                (self.store.route_key(key).shard_id == shard_id).then_some(index)
            })
            .collect::<Vec<_>>();
        if indices.is_empty() {
            return Err(format!("no benchmark keys route to shared shard {shard_id}").into());
        }
        Ok(Some(indices))
    }
}

struct FcSharedWorker<const SHARDS: usize> {
    store: SharedEmbeddedStore<SHARDS>,
    copy_reads: bool,
}

impl<const SHARDS: usize> Worker for FcSharedWorker<SHARDS> {
    fn get(&mut self, key: &[u8], scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        match self.store.get_ref(key) {
            Some(value) => {
                if self.copy_reads {
                    scratch.clear();
                    scratch.extend_from_slice(value.value());
                } else {
                    black_box(value.value().len());
                }
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), BoxError> {
        self.store.insert_slice(key, value);
        Ok(())
    }

    fn set_ttl(&mut self, key: &[u8], value: &[u8], ttl_ms: u64) -> Result<(), BoxError> {
        self.store.insert_slice_with_ttl(key, value, Some(ttl_ms));
        Ok(())
    }
}
