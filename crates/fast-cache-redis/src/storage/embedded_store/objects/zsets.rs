use super::super::*;
use super::access::RedisObjectStoreAccess;

#[allow(dead_code)]
pub(crate) trait RedisZSetStore {
    fn zadd(&self, key: &[u8], score: f64, member: &[u8]) -> RedisObjectResult;
    #[allow(clippy::too_many_arguments)]
    fn zadd_cond(
        &self,
        key: &[u8],
        score: f64,
        member: &[u8],
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
        ch: bool,
        incr: bool,
    ) -> RedisObjectResult;
    fn zrem(&self, key: &[u8], member: &[u8]) -> RedisObjectResult;
    fn zrem_many(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult;
    fn zscore(&self, key: &[u8], member: &[u8]) -> RedisObjectResult;
    fn zscore_value(&self, key: &[u8], member: &[u8]) -> Result<Option<f64>, RedisObjectError>;
    fn zmscore(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult;
    fn zincrby(&self, key: &[u8], delta: f64, member: &[u8]) -> RedisObjectResult;
    fn zcard(&self, key: &[u8]) -> RedisObjectResult;
    fn zrange(&self, key: &[u8], start: i64, stop: i64) -> RedisObjectResult;
    fn zrange_entries_visit(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
        rev: bool,
        emit: impl FnMut(RedisObjectZSetRangeItem<'_>),
    ) -> RedisObjectReadOutcome;
    fn zentries(&self, key: &[u8]) -> Result<Vec<(Bytes, f64)>, RedisObjectError>;
    fn zrank(&self, key: &[u8], member: &[u8], rev: bool) -> RedisObjectResult;
    fn zrank_value(
        &self,
        key: &[u8],
        member: &[u8],
        rev: bool,
    ) -> Result<Option<usize>, RedisObjectError>;
    fn zcount(&self, key: &[u8], min: f64, max: f64) -> RedisObjectResult;
    fn zcount_range(
        &self,
        key: &[u8],
        min: f64,
        min_inclusive: bool,
        max: f64,
        max_inclusive: bool,
    ) -> Result<i64, RedisObjectError>;
    fn zpop(&self, key: &[u8], count: usize, max: bool) -> RedisObjectResult;
    fn zadd_hashed(
        &self,
        key_hash: u64,
        key: &[u8],
        score: f64,
        member: &[u8],
    ) -> RedisObjectResult;
}

impl RedisZSetStore for EmbeddedStore {
    fn zadd(&self, key: &[u8], score: f64, member: &[u8]) -> RedisObjectResult {
        self.zadd_hashed(hash_key(key), key, score, member)
    }

    #[allow(clippy::too_many_arguments)]
    fn zadd_cond(
        &self,
        key: &[u8],
        score: f64,
        member: &[u8],
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
        ch: bool,
        incr: bool,
    ) -> RedisObjectResult {
        self.object_write(key, |bucket| {
            bucket.zadd_cond(key, score, member, nx, xx, gt, lt, ch, incr)
        })
    }

    fn zrem(&self, key: &[u8], member: &[u8]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.zrem(key, member))
    }

    fn zrem_many(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.zrem_many(key, members))
    }

    fn zscore(&self, key: &[u8], member: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.zscore(key, member))
    }

    fn zscore_value(&self, key: &[u8], member: &[u8]) -> Result<Option<f64>, RedisObjectError> {
        let mut score = None;
        match self.object_read_hashed_visit(hash_key(key), key, |bucket| {
            bucket.zscore_visit(key, member, |value| score = value)
        }) {
            RedisObjectReadOutcome::Written => Ok(score),
            RedisObjectReadOutcome::Missing => Ok(None),
            RedisObjectReadOutcome::WrongType => Err(RedisObjectError::WrongType),
        }
    }

    fn zmscore(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.zmscore(key, members))
    }

    fn zincrby(&self, key: &[u8], delta: f64, member: &[u8]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.zincrby(key, delta, member))
    }

    fn zcard(&self, key: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.zcard(key))
    }

    fn zrange(&self, key: &[u8], start: i64, stop: i64) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.zrange(key, start, stop))
    }

    fn zrange_entries_visit(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
        rev: bool,
        emit: impl FnMut(RedisObjectZSetRangeItem<'_>),
    ) -> RedisObjectReadOutcome {
        self.object_read_hashed_visit(hash_key(key), key, |bucket| {
            bucket.zrange_entries_visit(key, start, stop, rev, emit)
        })
    }

    fn zentries(&self, key: &[u8]) -> Result<Vec<(Bytes, f64)>, RedisObjectError> {
        let route = self.route_key(key);
        let bucket = self.objects.read_bucket(route.shard_id, route.key_hash);
        let result = bucket
            .zentries(key)
            .map_err(|()| RedisObjectError::WrongType);
        if result.is_err() || bucket.contains_object(key) {
            return result;
        }
        drop(bucket);
        if self.string_exists_routed(route, key) {
            Err(RedisObjectError::WrongType)
        } else {
            result
        }
    }

    fn zrank(&self, key: &[u8], member: &[u8], rev: bool) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.zrank(key, member, rev))
    }

    fn zrank_value(
        &self,
        key: &[u8],
        member: &[u8],
        rev: bool,
    ) -> Result<Option<usize>, RedisObjectError> {
        let mut rank = None;
        match self.object_read_hashed_visit(hash_key(key), key, |bucket| {
            bucket.zrank_visit(key, member, rev, |value| rank = value)
        }) {
            RedisObjectReadOutcome::Written => Ok(rank),
            RedisObjectReadOutcome::Missing => Ok(None),
            RedisObjectReadOutcome::WrongType => Err(RedisObjectError::WrongType),
        }
    }

    fn zcount(&self, key: &[u8], min: f64, max: f64) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.zcount(key, min, max))
    }

    fn zcount_range(
        &self,
        key: &[u8],
        min: f64,
        min_inclusive: bool,
        max: f64,
        max_inclusive: bool,
    ) -> Result<i64, RedisObjectError> {
        let mut count = 0;
        match self.object_read_hashed_visit(hash_key(key), key, |bucket| {
            bucket.zcount_range_visit(key, min, min_inclusive, max, max_inclusive, |value| {
                count = value;
            })
        }) {
            RedisObjectReadOutcome::Written => Ok(count),
            RedisObjectReadOutcome::Missing => Ok(0),
            RedisObjectReadOutcome::WrongType => Err(RedisObjectError::WrongType),
        }
    }

    fn zpop(&self, key: &[u8], count: usize, max: bool) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.zpop(key, count, max))
    }

    fn zadd_hashed(
        &self,
        key_hash: u64,
        key: &[u8],
        score: f64,
        member: &[u8],
    ) -> RedisObjectResult {
        self.object_create_hashed(
            key_hash,
            key,
            |bucket, key_hash| {
                bucket.zadd_existing_or_wrongtype_hashed(key_hash, key, score, member)
            },
            |bucket, key_hash| bucket.zadd_new_unchecked_hashed(key_hash, key, score, member),
        )
    }
}
