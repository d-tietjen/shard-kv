use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use moka::sync::Cache;

use crate::backend::{Backend, BackendClass, BoxError, Worker};

type Map = Cache<Vec<u8>, Arc<[u8]>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MokaCapacityMode {
    Entries,
    WeightedBytes,
}

pub struct MokaBk {
    id: &'static str,
    capacity: usize,
    mode: MokaCapacityMode,
    map: Map,
    ttl_map: Arc<Mutex<Option<Map>>>,
    ttl_enabled: AtomicBool,
}

impl MokaBk {
    pub fn new(capacity: usize) -> Self {
        Self::with_mode("moka", capacity.max(1), MokaCapacityMode::Entries)
    }

    pub fn weighted_bytes(capacity_bytes: usize) -> Self {
        Self::with_mode(
            "moka-weighted",
            capacity_bytes.max(1),
            MokaCapacityMode::WeightedBytes,
        )
    }

    fn with_mode(id: &'static str, capacity: usize, mode: MokaCapacityMode) -> Self {
        Self {
            id,
            capacity,
            mode,
            map: build_cache(capacity, mode, None),
            ttl_map: Arc::new(Mutex::new(None)),
            ttl_enabled: AtomicBool::new(false),
        }
    }
}

impl Backend for MokaBk {
    fn id(&self) -> &str {
        self.id
    }
    fn class(&self) -> BackendClass {
        BackendClass::Embedded
    }
    fn warmup(&self, keys: &[Vec<u8>], value: &[u8]) -> Result<(), BoxError> {
        self.ttl_enabled.store(false, Ordering::Relaxed);
        let v: Arc<[u8]> = Arc::from(value);
        for k in keys {
            self.map.insert(k.clone(), Arc::clone(&v));
        }
        Ok(())
    }

    fn warmup_ttl(&self, keys: &[Vec<u8>], value: &[u8], ttl_ms: u64) -> Result<(), BoxError> {
        let map = build_cache(
            self.capacity,
            self.mode,
            Some(Duration::from_millis(ttl_ms)),
        );
        let v: Arc<[u8]> = Arc::from(value);
        for key in keys {
            map.insert(key.clone(), Arc::clone(&v));
        }
        *self
            .ttl_map
            .lock()
            .map_err(|_| "moka ttl map mutex poisoned")? = Some(map);
        self.ttl_enabled.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn new_worker(&self) -> Result<Box<dyn Worker>, BoxError> {
        if self.ttl_enabled.load(Ordering::Relaxed) {
            let map = self
                .ttl_map
                .lock()
                .map_err(|_| "moka ttl map mutex poisoned")?
                .as_ref()
                .ok_or("moka ttl map was not initialized")?
                .clone();
            return Ok(Box::new(MokaWorker { map }));
        }
        Ok(Box::new(MokaWorker {
            map: self.map.clone(),
        }))
    }
}

fn build_cache(capacity: usize, mode: MokaCapacityMode, ttl: Option<Duration>) -> Map {
    let builder = Cache::builder().max_capacity(capacity.max(1) as u64);
    let builder = match mode {
        MokaCapacityMode::Entries => builder,
        MokaCapacityMode::WeightedBytes => {
            builder.weigher(|key: &Vec<u8>, value: &Arc<[u8]>| entry_weight_bytes(key, value))
        }
    };
    match ttl {
        Some(ttl) => builder.time_to_live(ttl).build(),
        None => builder.build(),
    }
}

fn entry_weight_bytes(key: &[u8], value: &[u8]) -> u32 {
    key.len()
        .saturating_add(value.len())
        .saturating_add(64)
        .min(u32::MAX as usize) as u32
}

struct MokaWorker {
    map: Map,
}

impl Worker for MokaWorker {
    fn get(&mut self, key: &[u8], scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        match self.map.get(key) {
            Some(v) => {
                scratch.clear();
                scratch.extend_from_slice(&v);
                Ok(true)
            }
            None => Ok(false),
        }
    }
    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), BoxError> {
        self.map.insert(key.to_vec(), Arc::from(value));
        Ok(())
    }

    fn set_ttl(&mut self, key: &[u8], value: &[u8], _ttl_ms: u64) -> Result<(), BoxError> {
        self.map.insert(key.to_vec(), Arc::from(value));
        Ok(())
    }
}
