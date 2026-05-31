use std::sync::atomic::Ordering;

use parking_lot::{RwLockReadGuard, RwLockWriteGuard};

use super::helpers::bucket_index;
use super::*;

impl RedisObjectStore {
    pub(crate) fn new(shard_count: usize) -> Self {
        let shard_count = shard_count.max(1);
        let shards = (0..shard_count)
            .map(|_| RedisObjectShard {
                buckets: (0..OBJECT_BUCKETS)
                    .map(|_| RwLock::new(RedisObjectBucket::default()))
                    .collect(),
                object_count: AtomicIsize::new(0),
            })
            .collect();
        Self {
            shards,
            has_objects_hint: AtomicBool::new(false),
        }
    }

    #[inline(always)]
    pub(crate) fn has_objects(&self) -> bool {
        self.has_objects_hint.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub(crate) fn shard_has_objects(&self, shard_id: usize) -> bool {
        self.shard_object_count_hint(shard_id) != 0
    }

    #[inline(always)]
    pub(crate) fn object_count(&self) -> usize {
        let count = self
            .shards
            .iter()
            .map(|shard| shard.object_count.load(Ordering::Acquire))
            .sum::<isize>();
        let count = count.max(0) as usize;
        self.has_objects_hint.store(count != 0, Ordering::Release);
        count
    }

    pub(crate) fn live_object_count(&self, now_ms: u64) -> usize {
        if !self.has_objects() {
            return 0;
        }

        self.shards
            .iter()
            .map(|shard| {
                if shard.object_count.load(Ordering::Acquire) <= 0 {
                    return 0;
                }
                shard
                    .buckets
                    .iter()
                    .map(|bucket| bucket.read().live_object_count(now_ms))
                    .sum::<usize>()
            })
            .sum()
    }

    #[inline(always)]
    pub(crate) fn read_bucket(
        &self,
        shard_id: usize,
        key_hash: u64,
    ) -> RwLockReadGuard<'_, RedisObjectBucket> {
        self.shards[shard_id].buckets[bucket_index(key_hash)].read()
    }

    #[inline(always)]
    pub(crate) fn write_bucket(
        &self,
        shard_id: usize,
        key_hash: u64,
    ) -> RwLockWriteGuard<'_, RedisObjectBucket> {
        self.shards[shard_id].buckets[bucket_index(key_hash)].write()
    }

    #[inline(always)]
    pub(crate) fn note_created(&self, shard_id: usize) {
        let previous = self.shards[shard_id]
            .object_count
            .fetch_add(1, Ordering::Relaxed);
        if previous <= 0 {
            self.has_objects_hint.store(true, Ordering::Release);
        }
    }

    #[inline(always)]
    pub(crate) fn note_deleted(&self, shard_id: usize) {
        self.shards[shard_id]
            .object_count
            .fetch_sub(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub(crate) fn shard_object_count_hint(&self, shard_id: usize) -> usize {
        self.shards[shard_id]
            .object_count
            .load(Ordering::Acquire)
            .max(0) as usize
    }

    pub(crate) fn keys_with_type_in_shard(
        &self,
        shard_id: usize,
        now_ms: u64,
    ) -> Vec<(Bytes, &'static str)> {
        if self.shard_object_count_hint(shard_id) == 0 {
            return Vec::new();
        }
        let mut keys = Vec::new();
        for bucket in &self.shards[shard_id].buckets {
            bucket.read().keys_with_type(&mut keys, now_ms);
        }
        keys
    }

    pub(crate) fn keys_in_shard(&self, shard_id: usize, now_ms: u64) -> Vec<Bytes> {
        if self.shard_object_count_hint(shard_id) == 0 {
            return Vec::new();
        }
        let mut keys = Vec::new();
        for bucket in &self.shards[shard_id].buckets {
            bucket.read().keys(&mut keys, now_ms);
        }
        keys
    }

    pub(crate) fn keys_with_type(&self, now_ms: u64) -> Vec<(Bytes, &'static str)> {
        let mut keys = Vec::with_capacity(self.object_count());
        for shard_id in 0..self.shards.len() {
            keys.extend(self.keys_with_type_in_shard(shard_id, now_ms));
        }
        keys
    }

    pub(crate) fn keys(&self, now_ms: u64) -> Vec<Bytes> {
        let mut keys = Vec::with_capacity(self.object_count());
        for shard_id in 0..self.shards.len() {
            keys.extend(self.keys_in_shard(shard_id, now_ms));
        }
        keys
    }

    pub(crate) fn visit_keys(&self, now_ms: u64, visitor: &mut impl FnMut(&[u8]) -> bool) -> bool {
        if !self.has_objects() {
            return true;
        }

        for shard_id in 0..self.shards.len() {
            if self.shard_object_count_hint(shard_id) == 0 {
                continue;
            }
            for bucket in &self.shards[shard_id].buckets {
                if !bucket.read().visit_keys(now_ms, visitor) {
                    return false;
                }
            }
        }
        true
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn scan_keys_in_shard_visit(
        &self,
        shard_id: usize,
        now_ms: u64,
        type_filter: Option<&[u8]>,
        offset: usize,
        limit: usize,
        visited: &mut usize,
        emitted: &mut usize,
        visitor: &mut impl FnMut(&[u8]) -> bool,
    ) -> Option<usize> {
        if self.shard_object_count_hint(shard_id) == 0 {
            return None;
        }

        let mut position = 0usize;
        for bucket in &self.shards[shard_id].buckets {
            if let Some(offset) = bucket.read().scan_keys_visit(
                now_ms,
                type_filter,
                offset,
                &mut position,
                visited,
                emitted,
                limit,
                visitor,
            ) {
                return Some(offset);
            }
        }
        None
    }
}
