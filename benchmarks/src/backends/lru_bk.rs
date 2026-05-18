use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;

use crate::backend::{Backend, BackendClass, BoxError, Worker};

type Hasher = xxhash_rust::xxh3::Xxh3DefaultBuilder;
type Map = LruCache<Vec<u8>, Vec<u8>, Hasher>;

pub struct LruBk {
    map: Arc<Mutex<Map>>,
}

impl LruBk {
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1)).unwrap();
        Self {
            map: Arc::new(Mutex::new(LruCache::with_hasher(cap, Hasher::default()))),
        }
    }
}

impl Backend for LruBk {
    fn id(&self) -> &str {
        "lru"
    }
    fn class(&self) -> BackendClass {
        BackendClass::Embedded
    }
    fn warmup(&self, keys: &[Vec<u8>], value: &[u8]) -> Result<(), BoxError> {
        let mut m = self.map.lock();
        for k in keys {
            m.put(k.clone(), value.to_vec());
        }
        Ok(())
    }
    fn new_worker(&self) -> Result<Box<dyn Worker>, BoxError> {
        Ok(Box::new(LruWorker {
            map: Arc::clone(&self.map),
        }))
    }
}

struct LruWorker {
    map: Arc<Mutex<Map>>,
}

impl Worker for LruWorker {
    fn get(&mut self, key: &[u8], scratch: &mut Vec<u8>) -> Result<bool, BoxError> {
        let mut m = self.map.lock();
        match m.get(key) {
            Some(v) => {
                scratch.clear();
                scratch.extend_from_slice(v);
                Ok(true)
            }
            None => Ok(false),
        }
    }
    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), BoxError> {
        self.map.lock().put(key.to_vec(), value.to_vec());
        Ok(())
    }
}
