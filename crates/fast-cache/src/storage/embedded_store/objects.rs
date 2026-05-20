use super::*;

impl EmbeddedStore {
    /// Returns true when Redis object containers are present.
    #[inline(always)]
    pub fn has_redis_objects(&self) -> bool {
        self.objects.has_objects()
    }

    pub fn get_string_value_into<F>(&self, key: &[u8], mut write: F) -> RedisStringLookup
    where
        F: FnMut(&bytes::Bytes),
    {
        let route = self.route_key(key);
        if self.objects.has_objects() {
            let bucket = self.objects.read_bucket(route.shard_id, route.key_hash);
            if bucket.has_expirations() && bucket.object_is_expired(key, now_millis()) {
                drop(bucket);
                let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
                if bucket.delete_expired(key, now_millis()) {
                    self.objects.note_deleted(route.shard_id);
                }
                drop(bucket);
                return if self.with_shared_value_bytes_routed(route, key, &mut write) {
                    RedisStringLookup::Hit
                } else {
                    RedisStringLookup::Miss
                };
            }
            if bucket.contains_object(key) {
                return RedisStringLookup::WrongType;
            }
            drop(bucket);
            if self.with_shared_value_bytes_routed(route, key, &mut write) {
                RedisStringLookup::Hit
            } else {
                RedisStringLookup::Miss
            }
        } else if self.with_shared_value_bytes_routed(route, key, &mut write) {
            RedisStringLookup::Hit
        } else {
            RedisStringLookup::Miss
        }
    }

    pub fn hset(&self, key: &[u8], field: &[u8], value: &[u8]) -> RedisObjectResult {
        self.hset_hashed(hash_key(key), key, field, value)
    }

    pub fn hset_many(&self, key: &[u8], fields: &[(&[u8], &[u8])]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.hset_many(key, fields))
    }

    pub fn hget(&self, key: &[u8], field: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.hget(key, field))
    }

    pub fn hexists(&self, key: &[u8], field: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.hexists(key, field))
    }

    pub fn hdel(&self, key: &[u8], field: &[u8]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.hdel(key, field))
    }

    pub fn hdel_many(&self, key: &[u8], fields: &[&[u8]]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.hdel_many(key, fields))
    }

    pub fn hlen(&self, key: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.hlen(key))
    }

    pub fn hmget(&self, key: &[u8], fields: &[&[u8]]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.hmget(key, fields))
    }

    pub fn hkeys(&self, key: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.hkeys(key))
    }

    pub fn hvals(&self, key: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.hvals(key))
    }

    pub fn hgetall(&self, key: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.hgetall(key))
    }

    pub fn hsetnx(&self, key: &[u8], field: &[u8], value: &[u8]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.hsetnx(key, field, value))
    }

    pub fn hincrby(&self, key: &[u8], field: &[u8], delta: i64) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.hincrby(key, field, delta))
    }

    pub fn hincrbyfloat(&self, key: &[u8], field: &[u8], delta: f64) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.hincrbyfloat(key, field, delta))
    }

    pub fn hrandfield(
        &self,
        key: &[u8],
        count: Option<i64>,
        with_values: bool,
    ) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.hrandfield(key, count, with_values))
    }

    pub fn lpush(&self, key: &[u8], values: &[&[u8]]) -> RedisObjectResult {
        self.push_list_hashed(hash_key(key), key, values, true)
    }

    pub fn rpush(&self, key: &[u8], values: &[&[u8]]) -> RedisObjectResult {
        self.push_list_hashed(hash_key(key), key, values, false)
    }

    pub fn lpushx(&self, key: &[u8], values: &[&[u8]]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.push_list_existing(key, values, true))
    }

    pub fn rpushx(&self, key: &[u8], values: &[&[u8]]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.push_list_existing(key, values, false))
    }

    pub fn lpop(&self, key: &[u8]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.pop_list(key, true))
    }

    pub fn rpop(&self, key: &[u8]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.pop_list(key, false))
    }

    pub fn lpop_count(&self, key: &[u8], count: usize) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.pop_list_count(key, count, true))
    }

    pub fn rpop_count(&self, key: &[u8], count: usize) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.pop_list_count(key, count, false))
    }

    pub fn llen(&self, key: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.llen(key))
    }

    pub fn lindex(&self, key: &[u8], index: i64) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.lindex(key, index))
    }

    pub fn lrange(&self, key: &[u8], start: i64, stop: i64) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.lrange(key, start, stop))
    }

    pub fn lset(&self, key: &[u8], index: i64, value: &[u8]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.lset(key, index, value))
    }

    pub fn lrem(&self, key: &[u8], count: i64, value: &[u8]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.lrem(key, count, value))
    }

    pub fn ltrim(&self, key: &[u8], start: i64, stop: i64) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.ltrim(key, start, stop))
    }

    pub fn linsert(
        &self,
        key: &[u8],
        before: bool,
        pivot: &[u8],
        value: &[u8],
    ) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.linsert(key, before, pivot, value))
    }

    pub fn sadd(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult {
        self.sadd_hashed(hash_key(key), key, members)
    }

    pub fn srem(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.srem(key, members))
    }

    pub fn sismember(&self, key: &[u8], member: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.sismember(key, member))
    }

    pub fn smismember(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.smismember(key, members))
    }

    pub fn scard(&self, key: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.scard(key))
    }

    pub fn smembers(&self, key: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.smembers(key))
    }

    pub fn set_members(&self, key: &[u8]) -> Result<Vec<Bytes>, RedisObjectError> {
        let route = self.route_key(key);
        let bucket = self.objects.read_bucket(route.shard_id, route.key_hash);
        let result = bucket
            .set_members(key)
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

    pub fn spop(&self, key: &[u8], count: Option<usize>) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.spop(key, count))
    }

    pub fn srandmember(&self, key: &[u8], count: Option<i64>) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.srandmember(key, count))
    }

    pub fn zadd(&self, key: &[u8], score: f64, member: &[u8]) -> RedisObjectResult {
        self.zadd_hashed(hash_key(key), key, score, member)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn zadd_cond(
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

    pub fn zrem(&self, key: &[u8], member: &[u8]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.zrem(key, member))
    }

    pub fn zrem_many(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.zrem_many(key, members))
    }

    pub fn zscore(&self, key: &[u8], member: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.zscore(key, member))
    }

    pub fn zmscore(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.zmscore(key, members))
    }

    pub fn zincrby(&self, key: &[u8], delta: f64, member: &[u8]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.zincrby(key, delta, member))
    }

    pub fn zcard(&self, key: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.zcard(key))
    }

    pub fn zrange(&self, key: &[u8], start: i64, stop: i64) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.zrange(key, start, stop))
    }

    pub(crate) fn zrange_entries_visit(
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

    pub fn zentries(&self, key: &[u8]) -> Result<Vec<(Bytes, f64)>, RedisObjectError> {
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

    pub fn zrank(&self, key: &[u8], member: &[u8], rev: bool) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.zrank(key, member, rev))
    }

    pub(crate) fn zrank_value(
        &self,
        key: &[u8],
        member: &[u8],
        rev: bool,
    ) -> Result<Option<usize>, RedisObjectError> {
        let route = self.route_key(key);
        let bucket = self.objects.read_bucket(route.shard_id, route.key_hash);
        let result = bucket
            .zrank_value(key, member, rev)
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

    pub fn zcount(&self, key: &[u8], min: f64, max: f64) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.zcount(key, min, max))
    }

    pub fn zpop(&self, key: &[u8], count: usize, max: bool) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.zpop(key, count, max))
    }

    pub(crate) fn hset_hashed(
        &self,
        key_hash: u64,
        key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> RedisObjectResult {
        self.object_create_hashed(
            key_hash,
            key,
            |bucket, key_hash| {
                bucket.hset_existing_or_wrongtype_hashed(key_hash, key, field, value)
            },
            |bucket, key_hash| bucket.hset_new_unchecked_hashed(key_hash, key, field, value),
        )
    }

    pub(crate) fn push_list_hashed(
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

    pub(crate) fn sadd_hashed(
        &self,
        key_hash: u64,
        key: &[u8],
        members: &[&[u8]],
    ) -> RedisObjectResult {
        self.object_create_hashed(
            key_hash,
            key,
            |bucket, key_hash| bucket.sadd_existing_or_wrongtype_hashed(key_hash, key, members),
            |bucket, key_hash| bucket.sadd_new_unchecked_hashed(key_hash, key, members),
        )
    }

    pub(crate) fn zadd_hashed(
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

    fn object_read(
        &self,
        key: &[u8],
        op: impl FnOnce(&RedisObjectBucket) -> RedisObjectResult,
    ) -> RedisObjectResult {
        self.object_read_routed(self.route_key(key), key, op)
    }

    #[allow(dead_code)]
    pub(crate) fn object_read_hashed(
        &self,
        key_hash: u64,
        key: &[u8],
        op: impl FnOnce(&RedisObjectBucket) -> RedisObjectResult,
    ) -> RedisObjectResult {
        self.object_read_routed(self.route_key_prehashed(key_hash, key), key, op)
    }

    #[allow(dead_code)]
    pub(crate) fn object_read_hashed_visit(
        &self,
        key_hash: u64,
        key: &[u8],
        op: impl FnOnce(&RedisObjectBucket) -> RedisObjectReadOutcome,
    ) -> RedisObjectReadOutcome {
        let route = self.route_key_prehashed(key_hash, key);
        let bucket = self.objects.read_bucket(route.shard_id, route.key_hash);
        if bucket.has_expirations() && bucket.object_is_expired(key, now_millis()) {
            drop(bucket);
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            if bucket.delete_expired(key, now_millis()) {
                self.objects.note_deleted(route.shard_id);
            }
            drop(bucket);
            return if self.string_exists_routed(route, key) {
                RedisObjectReadOutcome::WrongType
            } else {
                RedisObjectReadOutcome::Missing
            };
        }
        let outcome = op(&bucket);
        if !matches!(outcome, RedisObjectReadOutcome::Missing) {
            return outcome;
        }
        drop(bucket);
        if self.string_exists_routed(route, key) {
            RedisObjectReadOutcome::WrongType
        } else {
            RedisObjectReadOutcome::Missing
        }
    }

    fn object_read_routed(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        op: impl FnOnce(&RedisObjectBucket) -> RedisObjectResult,
    ) -> RedisObjectResult {
        let bucket = self.objects.read_bucket(route.shard_id, route.key_hash);
        if bucket.has_expirations() && bucket.object_is_expired(key, now_millis()) {
            drop(bucket);
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            if bucket.delete_expired(key, now_millis()) {
                self.objects.note_deleted(route.shard_id);
            }
            drop(bucket);
            return if self.string_exists_routed(route, key) {
                RedisObjectResult::WrongType
            } else {
                op(&self.objects.read_bucket(route.shard_id, route.key_hash))
            };
        }
        let result = op(&bucket);
        if matches!(result, RedisObjectResult::WrongType) || bucket.contains_object(key) {
            return result;
        }
        drop(bucket);
        if self.string_exists_routed(route, key) {
            RedisObjectResult::WrongType
        } else {
            result
        }
    }

    fn object_write(
        &self,
        key: &[u8],
        op: impl FnOnce(&mut RedisObjectBucket) -> (RedisObjectResult, bool),
    ) -> RedisObjectResult {
        self.object_write_routed(self.route_key(key), key, op)
    }

    #[allow(dead_code)]
    pub(crate) fn object_write_hashed(
        &self,
        key_hash: u64,
        key: &[u8],
        op: impl FnOnce(&mut RedisObjectBucket) -> (RedisObjectResult, bool),
    ) -> RedisObjectResult {
        self.object_write_routed(self.route_key_prehashed(key_hash, key), key, op)
    }

    fn object_create_hashed(
        &self,
        key_hash: u64,
        key: &[u8],
        existing: impl FnOnce(&mut RedisObjectBucket, u64) -> RedisObjectWriteAttempt,
        create: impl FnOnce(&mut RedisObjectBucket, u64) -> RedisObjectResult,
    ) -> RedisObjectResult {
        let route = self.route_key_prehashed(key_hash, key);
        let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
        if bucket.has_expirations() && bucket.delete_expired(key, now_millis()) {
            self.objects.note_deleted(route.shard_id);
        }
        match existing(&mut bucket, route.key_hash) {
            RedisObjectWriteAttempt::Complete(result) => result,
            RedisObjectWriteAttempt::Missing => {
                if self.string_exists_routed(route, key) {
                    RedisObjectResult::WrongType
                } else {
                    let result = create(&mut bucket, route.key_hash);
                    self.objects.note_created(route.shard_id);
                    result
                }
            }
        }
    }

    fn object_write_routed(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        op: impl FnOnce(&mut RedisObjectBucket) -> (RedisObjectResult, bool),
    ) -> RedisObjectResult {
        let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
        if bucket.has_expirations() && bucket.delete_expired(key, now_millis()) {
            self.objects.note_deleted(route.shard_id);
        }
        let had_object = bucket.contains_object(key);
        if !had_object && self.string_exists_routed(route, key) {
            return RedisObjectResult::WrongType;
        }
        let (result, object_changed) = op(&mut bucket);
        if !had_object && object_changed {
            self.objects.note_created(route.shard_id);
        } else if had_object && object_changed {
            self.objects.note_deleted(route.shard_id);
        }
        result
    }

    pub(crate) fn clone_object_value(&self, key: &[u8]) -> Option<RedisObjectValue> {
        let route = self.route_key(key);
        let bucket = self.objects.read_bucket(route.shard_id, route.key_hash);
        if bucket.has_expirations() && bucket.object_is_expired(key, now_millis()) {
            drop(bucket);
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            if bucket.delete_expired(key, now_millis()) {
                self.objects.note_deleted(route.shard_id);
            }
            return None;
        }
        bucket.clone_value(key)
    }

    pub(crate) fn set_object_value(
        &self,
        key: &[u8],
        value: RedisObjectValue,
        ttl_ms: Option<u64>,
    ) {
        let route = self.route_key(key);
        let now_ms = now_millis();
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
        let mut shard = self.shards[route.shard_id].write();
        let had_object = bucket.contains_object(key);
        if let Some(session_prefix) = point_write_session_storage_prefix(key) {
            shard
                .session_slots
                .delete_hashed(&session_prefix, route.key_hash, key);
        }
        shard.map.delete_hashed(route.key_hash, key, now_ms);
        let created = bucket.insert_value(key.to_vec(), value);
        if created && !had_object {
            self.objects.note_created(route.shard_id);
        }
        if let Some(expire_at_ms) = expire_at_ms {
            bucket.expire(key, expire_at_ms, now_ms);
        }
    }

    pub fn rename_key(
        &self,
        source: &[u8],
        dest: &[u8],
        nx: bool,
    ) -> std::result::Result<bool, RedisObjectError> {
        if source == dest {
            if !self.exists(source) {
                return Err(RedisObjectError::MissingKey);
            }
            return Ok(!nx);
        }
        if nx && self.exists(dest) {
            return Ok(false);
        }
        let ttl_ms = match self.pttl_millis(source) {
            ttl if ttl >= 0 => Some(ttl as u64),
            _ => None,
        };
        if let Some(value) = self.get_value_bytes(source) {
            self.set_value_bytes(dest, value, ttl_ms);
            self.delete(source);
            return Ok(true);
        }
        if let Some(value) = self.clone_object_value(source) {
            self.delete(dest);
            self.set_object_value(dest, value, ttl_ms);
            self.delete(source);
            return Ok(true);
        }
        Err(RedisObjectError::MissingKey)
    }

    fn string_exists_routed(&self, route: EmbeddedKeyRoute, key: &[u8]) -> bool {
        if uses_flat_key_storage(self.route_mode, key) {
            let shard = self.shards[route.shard_id].read();
            if shard.map.is_empty() && shard.session_slots.is_empty() {
                return false;
            }
            if shard.map.has_no_ttl_entries() {
                return shard.map.with_shared_value_bytes_hashed_no_ttl(
                    route.key_hash,
                    key,
                    &mut |_| {},
                );
            }
            return shard.map.with_shared_value_bytes_hashed(
                route.key_hash,
                key,
                now_millis(),
                &mut |_| {},
            );
        }
        self.with_shared_value_bytes_routed(route, key, &mut |_| {})
    }
}
