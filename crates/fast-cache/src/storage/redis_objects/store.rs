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

    pub(crate) fn keys_with_type(&self, now_ms: u64) -> Vec<(Bytes, &'static str)> {
        let mut keys = Vec::with_capacity(self.object_count());
        for shard in &self.shards {
            for bucket in &shard.buckets {
                bucket.read().keys_with_type(&mut keys, now_ms);
            }
        }
        keys
    }

    pub(crate) fn keys(&self, now_ms: u64) -> Vec<Bytes> {
        let mut keys = Vec::with_capacity(self.object_count());
        for shard in &self.shards {
            for bucket in &shard.buckets {
                bucket.read().keys(&mut keys, now_ms);
            }
        }
        keys
    }
}
