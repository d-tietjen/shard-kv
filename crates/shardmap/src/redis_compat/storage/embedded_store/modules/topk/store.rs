#![allow(dead_code, unused_imports)]

use super::super::*;

use super::sketch::*;
use super::types::*;

#[cfg(feature = "redis-module-topk")]
#[derive(Debug)]
pub(crate) struct TopKStore {
    shards: Vec<RwLock<FastHashMap<Bytes, TopKSketch>>>,
}

#[cfg(feature = "redis-module-topk")]
impl TopKStore {
    pub(crate) fn new(shard_count: usize) -> Self {
        Self {
            shards: (0..shard_count.max(1))
                .map(|_| RwLock::new(FastHashMap::default()))
                .collect(),
        }
    }

    fn exists(&self, route: EmbeddedKeyRoute, key: &[u8]) -> bool {
        self.shards[route.shard_id].read().contains_key(key)
    }

    fn reserve(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        k: usize,
        width: usize,
        depth: usize,
        decay: f64,
    ) -> Result<(), TopKError> {
        let mut shard = self.shards[route.shard_id].write();
        if shard.contains_key(key) {
            return Err(TopKError::AlreadyExists);
        }
        shard.insert(key.to_vec(), TopKSketch::new(k, width, depth, decay));
        Ok(())
    }

    fn update(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        updates: &[(Bytes, i64)],
    ) -> Result<Vec<Option<Bytes>>, TopKError> {
        let mut shard = self.shards[route.shard_id].write();
        let sketch = shard.get_mut(key).ok_or(TopKError::MissingKey)?;
        Ok(updates
            .iter()
            .map(|(item, increment)| sketch.increment(item, *increment))
            .collect())
    }

    fn query(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        items: &[&[u8]],
    ) -> Result<Vec<bool>, TopKError> {
        let shard = self.shards[route.shard_id].read();
        let sketch = shard.get(key).ok_or(TopKError::MissingKey)?;
        Ok(items.iter().map(|item| sketch.contains_top(item)).collect())
    }

    fn counts(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        items: &[&[u8]],
    ) -> Result<Vec<i64>, TopKError> {
        let shard = self.shards[route.shard_id].read();
        let sketch = shard.get(key).ok_or(TopKError::MissingKey)?;
        Ok(items.iter().map(|item| sketch.count(item)).collect())
    }

    fn list(&self, route: EmbeddedKeyRoute, key: &[u8]) -> Result<Vec<(Bytes, i64)>, TopKError> {
        let shard = self.shards[route.shard_id].read();
        let sketch = shard.get(key).ok_or(TopKError::MissingKey)?;
        Ok(sketch.top_entries())
    }

    fn info(&self, route: EmbeddedKeyRoute, key: &[u8]) -> Result<TopKInfo, TopKError> {
        let shard = self.shards[route.shard_id].read();
        let sketch = shard.get(key).ok_or(TopKError::MissingKey)?;
        Ok(sketch.info())
    }
}

#[cfg(feature = "redis-module-topk")]
impl EmbeddedStore {
    pub(crate) fn topk_reserve(
        &self,
        key: &[u8],
        k: usize,
        width: usize,
        depth: usize,
        decay: f64,
    ) -> Result<(), TopKError> {
        if k == 0 || width == 0 || depth == 0 || !(0.0..1.0).contains(&decay) {
            return Err(TopKError::InvalidArgument);
        }
        let route = self.route_key(key);
        if self.topk.exists(route, key) {
            return Err(TopKError::AlreadyExists);
        }
        if self.topk_key_conflicts(route, key) {
            return Err(TopKError::WrongType);
        }
        self.topk.reserve(route, key, k, width, depth, decay)
    }

    pub(crate) fn topk_add(
        &self,
        key: &[u8],
        items: &[&[u8]],
    ) -> Result<Vec<Option<Bytes>>, TopKError> {
        let updates = items
            .iter()
            .map(|item| ((*item).to_vec(), 1))
            .collect::<Vec<_>>();
        self.topk_update(key, &updates)
    }

    pub(crate) fn topk_incrby(
        &self,
        key: &[u8],
        updates: &[(Bytes, i64)],
    ) -> Result<Vec<Option<Bytes>>, TopKError> {
        if updates
            .iter()
            .any(|(_, increment)| !(1..=100_000).contains(increment))
        {
            return Err(TopKError::InvalidArgument);
        }
        self.topk_update(key, updates)
    }

    pub(crate) fn topk_query(&self, key: &[u8], items: &[&[u8]]) -> Result<Vec<bool>, TopKError> {
        let route = self.route_key(key);
        self.topk
            .query(route, key, items)
            .or_else(|err| self.topk_normalize_missing(route, key, err))
    }

    pub(crate) fn topk_counts(&self, key: &[u8], items: &[&[u8]]) -> Result<Vec<i64>, TopKError> {
        let route = self.route_key(key);
        self.topk
            .counts(route, key, items)
            .or_else(|err| self.topk_normalize_missing(route, key, err))
    }

    pub(crate) fn topk_list(&self, key: &[u8]) -> Result<Vec<(Bytes, i64)>, TopKError> {
        let route = self.route_key(key);
        self.topk
            .list(route, key)
            .or_else(|err| self.topk_normalize_missing(route, key, err))
    }

    pub(crate) fn topk_info(&self, key: &[u8]) -> Result<TopKInfo, TopKError> {
        let route = self.route_key(key);
        self.topk
            .info(route, key)
            .or_else(|err| self.topk_normalize_missing(route, key, err))
    }

    fn topk_update(
        &self,
        key: &[u8],
        updates: &[(Bytes, i64)],
    ) -> Result<Vec<Option<Bytes>>, TopKError> {
        let route = self.route_key(key);
        self.topk
            .update(route, key, updates)
            .or_else(|err| self.topk_normalize_missing(route, key, err))
    }

    fn topk_normalize_missing<T>(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        err: TopKError,
    ) -> Result<T, TopKError> {
        match err {
            TopKError::MissingKey if self.topk_key_conflicts(route, key) => {
                Err(TopKError::WrongType)
            }
            err => Err(err),
        }
    }

    fn topk_key_conflicts(&self, route: EmbeddedKeyRoute, key: &[u8]) -> bool {
        if self.get_ref(key).is_some() {
            return true;
        }
        #[cfg(feature = "redis-modules")]
        if self.module_state.read(route).contains_any(key) {
            return true;
        }
        if !self.objects.shard_has_objects(route.shard_id) {
            return false;
        }
        let bucket = self.objects.read_bucket(route.shard_id, route.key_hash);
        bucket.contains_live_object(key, now_millis())
    }
}
