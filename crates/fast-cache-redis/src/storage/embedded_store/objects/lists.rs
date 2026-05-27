use super::super::*;
use super::access::RedisObjectStoreAccess;

#[allow(dead_code)]
pub(crate) trait RedisListStore {
    fn lpush(&self, key: &[u8], values: &[&[u8]]) -> RedisObjectResult;
    fn rpush(&self, key: &[u8], values: &[&[u8]]) -> RedisObjectResult;
    fn lpushx(&self, key: &[u8], values: &[&[u8]]) -> RedisObjectResult;
    fn rpushx(&self, key: &[u8], values: &[&[u8]]) -> RedisObjectResult;
    fn lpop(&self, key: &[u8]) -> RedisObjectResult;
    fn rpop(&self, key: &[u8]) -> RedisObjectResult;
    fn lpop_count(&self, key: &[u8], count: usize) -> RedisObjectResult;
    fn rpop_count(&self, key: &[u8], count: usize) -> RedisObjectResult;
    fn llen(&self, key: &[u8]) -> RedisObjectResult;
    fn llen_visit(&self, key: &[u8], write: impl FnOnce(i64)) -> RedisObjectReadOutcome;
    fn lindex(&self, key: &[u8], index: i64) -> RedisObjectResult;
    fn lindex_visit(
        &self,
        key: &[u8],
        index: i64,
        write: impl FnOnce(Option<&[u8]>),
    ) -> RedisObjectReadOutcome;
    fn lrange(&self, key: &[u8], start: i64, stop: i64) -> RedisObjectResult;
    fn lrange_visit(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
        emit: impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome;
    fn lset(&self, key: &[u8], index: i64, value: &[u8]) -> RedisObjectResult;
    fn lrem(&self, key: &[u8], count: i64, value: &[u8]) -> RedisObjectResult;
    fn ltrim(&self, key: &[u8], start: i64, stop: i64) -> RedisObjectResult;
    fn linsert(&self, key: &[u8], before: bool, pivot: &[u8], value: &[u8]) -> RedisObjectResult;
    fn push_list_hashed(
        &self,
        key_hash: u64,
        key: &[u8],
        values: &[&[u8]],
        front: bool,
    ) -> RedisObjectResult;
    fn push_list_existing_hashed(
        &self,
        key_hash: u64,
        key: &[u8],
        values: &[&[u8]],
        front: bool,
    ) -> RedisObjectResult;
}

impl RedisListStore for EmbeddedStore {
    fn lpush(&self, key: &[u8], values: &[&[u8]]) -> RedisObjectResult {
        self.push_list_hashed(hash_key(key), key, values, true)
    }

    fn rpush(&self, key: &[u8], values: &[&[u8]]) -> RedisObjectResult {
        self.push_list_hashed(hash_key(key), key, values, false)
    }

    fn lpushx(&self, key: &[u8], values: &[&[u8]]) -> RedisObjectResult {
        self.push_list_existing_hashed(hash_key(key), key, values, true)
    }

    fn rpushx(&self, key: &[u8], values: &[&[u8]]) -> RedisObjectResult {
        self.push_list_existing_hashed(hash_key(key), key, values, false)
    }

    fn lpop(&self, key: &[u8]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.pop_list(key, true))
    }

    fn rpop(&self, key: &[u8]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.pop_list(key, false))
    }

    fn lpop_count(&self, key: &[u8], count: usize) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.pop_list_count(key, count, true))
    }

    fn rpop_count(&self, key: &[u8], count: usize) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.pop_list_count(key, count, false))
    }

    fn llen(&self, key: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.llen(key))
    }

    fn llen_visit(&self, key: &[u8], write: impl FnOnce(i64)) -> RedisObjectReadOutcome {
        self.object_read_hashed_visit(hash_key(key), key, |bucket| bucket.llen_visit(key, write))
    }

    fn lindex(&self, key: &[u8], index: i64) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.lindex(key, index))
    }

    fn lindex_visit(
        &self,
        key: &[u8],
        index: i64,
        write: impl FnOnce(Option<&[u8]>),
    ) -> RedisObjectReadOutcome {
        self.object_read_hashed_visit(hash_key(key), key, |bucket| {
            bucket.lindex_visit(key, index, write)
        })
    }

    fn lrange(&self, key: &[u8], start: i64, stop: i64) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.lrange(key, start, stop))
    }

    fn lrange_visit(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
        mut emit: impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome {
        self.object_read_hashed_visit(hash_key(key), key, |bucket| {
            bucket.lrange_visit(key, start, stop, &mut emit)
        })
    }

    fn lset(&self, key: &[u8], index: i64, value: &[u8]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.lset(key, index, value))
    }

    fn lrem(&self, key: &[u8], count: i64, value: &[u8]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.lrem(key, count, value))
    }

    fn ltrim(&self, key: &[u8], start: i64, stop: i64) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.ltrim(key, start, stop))
    }

    fn linsert(&self, key: &[u8], before: bool, pivot: &[u8], value: &[u8]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.linsert(key, before, pivot, value))
    }

    fn push_list_hashed(
        &self,
        key_hash: u64,
        key: &[u8],
        values: &[&[u8]],
        front: bool,
    ) -> RedisObjectResult {
        self.object_create_hashed(
            key_hash,
            key,
            |bucket, key_hash| {
                bucket.push_list_existing_or_wrongtype_hashed(key_hash, key, values, front)
            },
            |bucket, key_hash| bucket.push_list_new_unchecked_hashed(key_hash, key, values, front),
        )
    }

    fn push_list_existing_hashed(
        &self,
        key_hash: u64,
        key: &[u8],
        values: &[&[u8]],
        front: bool,
    ) -> RedisObjectResult {
        if values.is_empty() {
            return RedisObjectResult::Integer(0);
        }
        let route = self.route_key_prehashed(key_hash, key);
        if !self.objects.shard_has_objects(route.shard_id) {
            return if self.string_exists_routed(route, key) {
                RedisObjectResult::WrongType
            } else {
                RedisObjectResult::Integer(0)
            };
        }
        let bucket = self.objects.read_bucket(route.shard_id, route.key_hash);
        if bucket.has_expirations() {
            let now_ms = now_millis();
            if bucket.object_is_expired(key, now_ms) {
                drop(bucket);
                let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
                if bucket.delete_expired(key, now_ms) {
                    self.objects.note_deleted(route.shard_id);
                }
                drop(bucket);
                return if self.string_exists_routed(route, key) {
                    RedisObjectResult::WrongType
                } else {
                    RedisObjectResult::Integer(0)
                };
            }
        }
        match bucket.list_presence_hashed(route.key_hash, key) {
            RedisObjectReadOutcome::Missing => {
                drop(bucket);
                if self.string_exists_routed(route, key) {
                    RedisObjectResult::WrongType
                } else {
                    RedisObjectResult::Integer(0)
                }
            }
            RedisObjectReadOutcome::WrongType => RedisObjectResult::WrongType,
            RedisObjectReadOutcome::Written => {
                drop(bucket);
                self.object_write_hashed(key_hash, key, |bucket| {
                    bucket.push_list_existing_hashed(key_hash, key, values, front)
                })
            }
        }
    }
}
