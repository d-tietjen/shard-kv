use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::backend::{Backend, BackendClass, BoxError, Worker};

type Hasher = xxhash_rust::xxh3::Xxh3DefaultBuilder;
type Map = HashMap<Vec<u8>, Vec<u8>, Hasher>;

pub struct RwLockHashMapBk {
    map: Arc<RwLock<Map>>,
}

impl RwLockHashMapBk {
    pub fn new(capacity: usize) -> Self {
        Self {
            map: Arc::new(RwLock::new(HashMap::with_capacity_and_hasher(
                capacity,
                Hasher::default(),
            ))),
        }
    }
}

impl Backend for RwLockHashMapBk {
    fn id(&self) -> &str {
        "rwlock-hashmap"
    }
    fn class(&self) -> BackendClass {
        BackendClass::Embedded
    }
    fn warmup(&self, keys: &[Vec<u8>], value: &[u8]) -> Result<(), BoxError> {
        let mut m = self.map.write();
        for k in keys {
            m.insert(k.clone(), value.to_vec());
        }
        Ok(())
    }
    fn new_worker(&self) -> Result<Box<dyn Worker>, BoxError> {
        Ok(Box::new(RwLockHashMapWorker {
            map: Arc::clone(&self.map),
        }))
    }
}

struct RwLockHashMapWorker {
    map: Arc<RwLock<Map>>,
}

impl Worker for RwLockHashMapWorker {
    fn get(&mut self, key: &[u8], scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        match self.map.read().get(key) {
            Some(v) => {
                scratch.clear();
                scratch.extend_from_slice(v);
                Ok(true)
            }
            None => Ok(false),
        }
    }
    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), BoxError> {
        self.map.write().insert(key.to_vec(), value.to_vec());
        Ok(())
    }
}
