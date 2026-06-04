use std::hint::black_box;
use std::sync::Arc;

use shardmap::ShardMap;

use crate::backend::{Backend, BackendClass, BoxError, Worker};

use super::BenchmarkCacheConfig;

const MAX_FC_TYPED_SHARDS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypedReadMode {
    Ref,
    Owned,
}

pub fn new(
    id: &'static str,
    shard_count: usize,
    capacity_hint: usize,
    read_mode: TypedReadMode,
    cache_config: BenchmarkCacheConfig,
) -> Result<Arc<dyn Backend>, BoxError> {
    let resolved_shards = shard_count.next_power_of_two().max(1);
    let backend: Arc<dyn Backend> = match resolved_shards {
        1 => Arc::new(FcTyped::<1>::new(
            id,
            capacity_hint,
            read_mode,
            cache_config,
        )),
        2 => Arc::new(FcTyped::<2>::new(
            id,
            capacity_hint,
            read_mode,
            cache_config,
        )),
        4 => Arc::new(FcTyped::<4>::new(
            id,
            capacity_hint,
            read_mode,
            cache_config,
        )),
        8 => Arc::new(FcTyped::<8>::new(
            id,
            capacity_hint,
            read_mode,
            cache_config,
        )),
        16 => Arc::new(FcTyped::<16>::new(
            id,
            capacity_hint,
            read_mode,
            cache_config,
        )),
        32 => Arc::new(FcTyped::<32>::new(
            id,
            capacity_hint,
            read_mode,
            cache_config,
        )),
        64 => Arc::new(FcTyped::<64>::new(
            id,
            capacity_hint,
            read_mode,
            cache_config,
        )),
        128 => Arc::new(FcTyped::<128>::new(
            id,
            capacity_hint,
            read_mode,
            cache_config,
        )),
        256 => Arc::new(FcTyped::<256>::new(
            id,
            capacity_hint,
            read_mode,
            cache_config,
        )),
        shards => {
            return Err(format!(
                "{id} benchmark supports up to {MAX_FC_TYPED_SHARDS} shards; requested {shard_count}, resolved to {shards}"
            )
            .into());
        }
    };
    Ok(backend)
}

pub struct FcTyped<const SHARDS: usize> {
    id: &'static str,
    map: ShardMap<Vec<u8>, Vec<u8>, SHARDS>,
    read_mode: TypedReadMode,
}

impl<const SHARDS: usize> FcTyped<SHARDS> {
    fn new(
        id: &'static str,
        capacity_hint: usize,
        read_mode: TypedReadMode,
        _cache_config: BenchmarkCacheConfig,
    ) -> Self {
        const {
            assert!(
                SHARDS <= MAX_FC_TYPED_SHARDS,
                "fc-typed benchmark shard table must not exceed MAX_FC_TYPED_SHARDS"
            );
        }
        Self {
            id,
            map: ShardMap::with_capacity(capacity_hint),
            read_mode,
        }
    }
}

impl<const SHARDS: usize> Backend for FcTyped<SHARDS> {
    fn id(&self) -> &str {
        self.id
    }

    fn class(&self) -> BackendClass {
        BackendClass::Embedded
    }

    fn warmup(&self, keys: &[Vec<u8>], value: &[u8]) -> Result<(), BoxError> {
        for key in keys {
            self.map.insert(key.clone(), value.to_vec());
        }
        Ok(())
    }

    fn warmup_ttl(&self, _keys: &[Vec<u8>], _value: &[u8], _ttl_ms: u64) -> Result<(), BoxError> {
        Err("fc-typed native map does not support TTL workloads".into())
    }

    fn new_worker(&self) -> Result<Box<dyn Worker>, BoxError> {
        Ok(Box::new(FcTypedWorker {
            map: self.map.clone(),
            read_mode: self.read_mode,
        }))
    }
}

struct FcTypedWorker<const SHARDS: usize> {
    map: ShardMap<Vec<u8>, Vec<u8>, SHARDS>,
    read_mode: TypedReadMode,
}

impl<const SHARDS: usize> Worker for FcTypedWorker<SHARDS> {
    fn get(&mut self, key: &[u8], _scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        match self.read_mode {
            TypedReadMode::Ref => match self.map.get_ref(key) {
                Some(value) => {
                    black_box(value.value().len());
                    Ok(true)
                }
                None => Ok(false),
            },
            TypedReadMode::Owned => match self.map.get(key) {
                Some(value) => {
                    black_box(value.len());
                    Ok(true)
                }
                None => Ok(false),
            },
        }
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), BoxError> {
        self.map.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn set_ttl(&mut self, _key: &[u8], _value: &[u8], _ttl_ms: u64) -> Result<(), BoxError> {
        Err("fc-typed native map does not support TTL writes".into())
    }
}
