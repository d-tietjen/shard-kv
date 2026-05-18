use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use dashmap::DashMap;

use crate::backend::{Backend, BackendClass, BoxError, Worker};
use crate::clock::cached_epoch_millis;

type Hasher = xxhash_rust::xxh3::Xxh3DefaultBuilder;
type PlainMap = DashMap<Vec<u8>, Vec<u8>, Hasher>;
type TtlMap = DashMap<Vec<u8>, TtlEntry, Hasher>;

#[derive(Debug, Clone)]
struct TtlEntry {
    value: Vec<u8>,
    expire_at_ms: u64,
}

pub struct DashMapBk {
    id: &'static str,
    plain: Arc<PlainMap>,
    ttl: Arc<TtlMap>,
    ttl_enabled: AtomicBool,
}

impl DashMapBk {
    pub fn new(capacity: usize) -> Self {
        Self {
            id: "dashmap",
            plain: Arc::new(PlainMap::with_capacity_and_hasher(
                capacity,
                Hasher::default(),
            )),
            ttl: Arc::new(TtlMap::with_capacity_and_hasher(
                capacity,
                Hasher::default(),
            )),
            ttl_enabled: AtomicBool::new(false),
        }
    }

    pub fn with_shard_amount(capacity: usize, shard_amount: usize) -> Self {
        let shard_amount = shard_amount.max(2).next_power_of_two();
        Self {
            id: "dashmap-worker-shards",
            plain: Arc::new(PlainMap::with_capacity_and_hasher_and_shard_amount(
                capacity,
                Hasher::default(),
                shard_amount,
            )),
            ttl: Arc::new(TtlMap::with_capacity_and_hasher_and_shard_amount(
                capacity,
                Hasher::default(),
                shard_amount,
            )),
            ttl_enabled: AtomicBool::new(false),
        }
    }
}

impl Backend for DashMapBk {
    fn id(&self) -> &str {
        self.id
    }
    fn class(&self) -> BackendClass {
        BackendClass::Embedded
    }
    fn warmup(&self, keys: &[Vec<u8>], value: &[u8]) -> Result<(), BoxError> {
        self.ttl_enabled.store(false, Ordering::Relaxed);
        for k in keys {
            self.plain.insert(k.clone(), value.to_vec());
        }
        Ok(())
    }
    fn warmup_ttl(&self, keys: &[Vec<u8>], value: &[u8], ttl_ms: u64) -> Result<(), BoxError> {
        self.ttl_enabled.store(true, Ordering::Relaxed);
        let expire_at_ms = cached_epoch_millis().saturating_add(ttl_ms);
        for key in keys {
            self.ttl.insert(
                key.clone(),
                TtlEntry {
                    value: value.to_vec(),
                    expire_at_ms,
                },
            );
        }
        Ok(())
    }
    fn new_worker(&self) -> Result<Box<dyn Worker>, BoxError> {
        if self.ttl_enabled.load(Ordering::Relaxed) {
            Ok(Box::new(DashMapWorker::Ttl {
                map: Arc::clone(&self.ttl),
            }))
        } else {
            Ok(Box::new(DashMapWorker::Plain {
                map: Arc::clone(&self.plain),
            }))
        }
    }
}

enum DashMapWorker {
    Plain { map: Arc<PlainMap> },
    Ttl { map: Arc<TtlMap> },
}

impl Worker for DashMapWorker {
    fn get(&mut self, key: &[u8], scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        match self {
            Self::Plain { map } => match map.get(key) {
                Some(value) => {
                    scratch.clear();
                    scratch.extend_from_slice(value.value());
                    Ok(true)
                }
                None => Ok(false),
            },
            Self::Ttl { map } => match map.get(key) {
                Some(entry) if entry.expire_at_ms > cached_epoch_millis() => {
                    scratch.clear();
                    scratch.extend_from_slice(&entry.value);
                    Ok(true)
                }
                Some(_) | None => Ok(false),
            },
        }
    }
    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), BoxError> {
        match self {
            Self::Plain { map } => {
                map.insert(key.to_vec(), value.to_vec());
                Ok(())
            }
            Self::Ttl { .. } => Err("plain SET called on DashMap TTL worker".into()),
        }
    }
    fn set_ttl(&mut self, key: &[u8], value: &[u8], ttl_ms: u64) -> Result<(), BoxError> {
        match self {
            Self::Plain { .. } => Err("TTL SET called on plain DashMap worker".into()),
            Self::Ttl { map } => {
                map.insert(
                    key.to_vec(),
                    TtlEntry {
                        value: value.to_vec(),
                        expire_at_ms: cached_epoch_millis().saturating_add(ttl_ms),
                    },
                );
                Ok(())
            }
        }
    }
}
