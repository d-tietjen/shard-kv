use std::hint::black_box;
use std::sync::Arc;

use bytes::Bytes;
use shardmap::{CodecShardMap, ShardCacheWithShards};

use crate::backend::{Backend, BackendClass, BoxError, Worker};

use super::BenchmarkCacheConfig;

const MAX_FC_CODEC_SHARDS: usize = 256;
const MULTI_NAMESPACE_COUNT: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecReadMode {
    Ref,
    Owned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecNamespaceMode {
    None,
    Single,
    Multi,
}

pub fn new(
    id: &'static str,
    shard_count: usize,
    capacity_hint: usize,
    read_mode: CodecReadMode,
    namespace_mode: CodecNamespaceMode,
    cache_config: BenchmarkCacheConfig,
) -> Result<Arc<dyn Backend>, BoxError> {
    let resolved_shards = shard_count.next_power_of_two().max(1);
    let backend: Arc<dyn Backend> = match resolved_shards {
        1 => Arc::new(FcCodec::<1>::new(
            id,
            capacity_hint,
            read_mode,
            namespace_mode,
            cache_config,
        )?),
        2 => Arc::new(FcCodec::<2>::new(
            id,
            capacity_hint,
            read_mode,
            namespace_mode,
            cache_config,
        )?),
        4 => Arc::new(FcCodec::<4>::new(
            id,
            capacity_hint,
            read_mode,
            namespace_mode,
            cache_config,
        )?),
        8 => Arc::new(FcCodec::<8>::new(
            id,
            capacity_hint,
            read_mode,
            namespace_mode,
            cache_config,
        )?),
        16 => Arc::new(FcCodec::<16>::new(
            id,
            capacity_hint,
            read_mode,
            namespace_mode,
            cache_config,
        )?),
        32 => Arc::new(FcCodec::<32>::new(
            id,
            capacity_hint,
            read_mode,
            namespace_mode,
            cache_config,
        )?),
        64 => Arc::new(FcCodec::<64>::new(
            id,
            capacity_hint,
            read_mode,
            namespace_mode,
            cache_config,
        )?),
        128 => Arc::new(FcCodec::<128>::new(
            id,
            capacity_hint,
            read_mode,
            namespace_mode,
            cache_config,
        )?),
        256 => Arc::new(FcCodec::<256>::new(
            id,
            capacity_hint,
            read_mode,
            namespace_mode,
            cache_config,
        )?),
        shards => {
            return Err(format!(
                "{id} benchmark supports up to {MAX_FC_CODEC_SHARDS} shards; requested {shard_count}, resolved to {shards}"
            )
            .into());
        }
    };
    Ok(backend)
}

type ByteCodecMap<const SHARDS: usize> = CodecShardMap<Vec<u8>, Vec<u8>, SHARDS>;

pub struct FcCodec<const SHARDS: usize> {
    id: &'static str,
    maps: Arc<Vec<ByteCodecMap<SHARDS>>>,
    read_mode: CodecReadMode,
}

impl<const SHARDS: usize> FcCodec<SHARDS> {
    fn new(
        id: &'static str,
        capacity_hint: usize,
        read_mode: CodecReadMode,
        namespace_mode: CodecNamespaceMode,
        _cache_config: BenchmarkCacheConfig,
    ) -> Result<Self, BoxError> {
        const {
            assert!(
                SHARDS <= MAX_FC_CODEC_SHARDS,
                "fc-codec benchmark shard table must not exceed MAX_FC_CODEC_SHARDS"
            );
        }

        let maps = match namespace_mode {
            CodecNamespaceMode::None => vec![CodecShardMap::with_capacity(capacity_hint)],
            CodecNamespaceMode::Single => {
                let raw = ShardCacheWithShards::<SHARDS>::with_capacity(capacity_hint);
                vec![CodecShardMap::from_shared_engine(
                    Bytes::from_static(b"bench"),
                    raw,
                )?]
            }
            CodecNamespaceMode::Multi => {
                let raw = ShardCacheWithShards::<SHARDS>::with_capacity(capacity_hint);
                (0..MULTI_NAMESPACE_COUNT)
                    .map(|index| {
                        CodecShardMap::from_shared_engine(
                            Bytes::copy_from_slice(format!("bench:{index}").as_bytes()),
                            raw.clone(),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?
            }
        };

        Ok(Self {
            id,
            maps: Arc::new(maps),
            read_mode,
        })
    }

    #[inline(always)]
    fn map_for_key(&self, key: &[u8]) -> &ByteCodecMap<SHARDS> {
        map_for_key(&self.maps, key)
    }
}

impl<const SHARDS: usize> Backend for FcCodec<SHARDS> {
    fn id(&self) -> &str {
        self.id
    }

    fn class(&self) -> BackendClass {
        BackendClass::Embedded
    }

    fn warmup(&self, keys: &[Vec<u8>], value: &[u8]) -> Result<(), BoxError> {
        for key in keys {
            self.map_for_key(key).insert_ref(key.as_slice(), value);
        }
        Ok(())
    }

    fn warmup_ttl(&self, keys: &[Vec<u8>], value: &[u8], ttl_ms: u64) -> Result<(), BoxError> {
        for key in keys {
            self.map_for_key(key)
                .insert_ref_with_ttl(key.as_slice(), value, Some(ttl_ms));
        }
        Ok(())
    }

    fn new_worker(&self) -> Result<Box<dyn Worker>, BoxError> {
        Ok(Box::new(FcCodecWorker {
            maps: Arc::clone(&self.maps),
            read_mode: self.read_mode,
        }))
    }
}

struct FcCodecWorker<const SHARDS: usize> {
    maps: Arc<Vec<ByteCodecMap<SHARDS>>>,
    read_mode: CodecReadMode,
}

impl<const SHARDS: usize> FcCodecWorker<SHARDS> {
    #[inline(always)]
    fn map_for_key(&self, key: &[u8]) -> &ByteCodecMap<SHARDS> {
        map_for_key(&self.maps, key)
    }
}

impl<const SHARDS: usize> Worker for FcCodecWorker<SHARDS> {
    fn get(&mut self, key: &[u8], _scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        let map = self.map_for_key(key);
        match self.read_mode {
            CodecReadMode::Ref => match map.get_ref(key) {
                Some(value) => {
                    black_box(value.value()?.len());
                    Ok(true)
                }
                None => Ok(false),
            },
            CodecReadMode::Owned => match map.get(key)? {
                Some(value) => {
                    black_box(value.len());
                    Ok(true)
                }
                None => Ok(false),
            },
        }
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), BoxError> {
        self.map_for_key(key).insert_ref(key, value);
        Ok(())
    }

    fn set_ttl(&mut self, key: &[u8], value: &[u8], ttl_ms: u64) -> Result<(), BoxError> {
        self.map_for_key(key)
            .insert_ref_with_ttl(key, value, Some(ttl_ms));
        Ok(())
    }
}

#[inline(always)]
fn map_for_key<'a, const SHARDS: usize>(
    maps: &'a [ByteCodecMap<SHARDS>],
    key: &[u8],
) -> &'a ByteCodecMap<SHARDS> {
    let index = if maps.len() == 1 {
        0
    } else {
        key.last().copied().unwrap_or_default() as usize & (maps.len() - 1)
    };
    &maps[index]
}
