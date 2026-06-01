use super::super::*;

impl EmbeddedStore {
    pub(crate) fn redis_object_shard_wait_generation(&self, shard_id: usize) -> u64 {
        self.objects.shard_wait_generation(shard_id)
    }

    pub(crate) fn wait_for_redis_object_shard_change(
        &self,
        shard_id: usize,
        observed_generation: u64,
        timeout: Option<std::time::Duration>,
    ) -> bool {
        self.objects
            .wait_for_shard_change(shard_id, observed_generation, timeout)
    }

    pub(crate) fn notify_redis_object_shard(&self, shard_id: usize) {
        self.objects.notify_shard_waiters(shard_id);
    }

    pub(crate) fn notify_redis_object_key(&self, key: &[u8]) {
        self.notify_redis_object_shard(self.route_key(key).shard_id);
    }
}

#[allow(dead_code)]
pub(crate) trait RedisObjectStoreAccess {
    /// Returns true when Redis object containers are present.
    fn has_redis_objects(&self) -> bool;

    fn object_read(
        &self,
        key: &[u8],
        op: impl FnOnce(&RedisObjectBucket) -> RedisObjectResult,
    ) -> RedisObjectResult;

    #[allow(dead_code)]
    fn object_read_hashed(
        &self,
        key_hash: u64,
        key: &[u8],
        op: impl FnOnce(&RedisObjectBucket) -> RedisObjectResult,
    ) -> RedisObjectResult;

    #[allow(dead_code)]
    fn object_read_hashed_visit(
        &self,
        key_hash: u64,
        key: &[u8],
        op: impl FnOnce(&RedisObjectBucket) -> RedisObjectReadOutcome,
    ) -> RedisObjectReadOutcome;

    fn object_read_routed(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        op: impl FnOnce(&RedisObjectBucket) -> RedisObjectResult,
    ) -> RedisObjectResult;

    fn object_write(
        &self,
        key: &[u8],
        op: impl FnOnce(&mut RedisObjectBucket) -> (RedisObjectResult, bool),
    ) -> RedisObjectResult;

    #[allow(dead_code)]
    fn object_write_hashed(
        &self,
        key_hash: u64,
        key: &[u8],
        op: impl FnOnce(&mut RedisObjectBucket) -> (RedisObjectResult, bool),
    ) -> RedisObjectResult;

    fn object_create_hashed(
        &self,
        key_hash: u64,
        key: &[u8],
        existing: impl FnOnce(&mut RedisObjectBucket, u64) -> RedisObjectWriteAttempt,
        create: impl FnOnce(&mut RedisObjectBucket, u64) -> RedisObjectResult,
    ) -> RedisObjectResult;

    fn object_write_routed(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        op: impl FnOnce(&mut RedisObjectBucket) -> (RedisObjectResult, bool),
    ) -> RedisObjectResult;

    fn string_exists_routed(&self, route: EmbeddedKeyRoute, key: &[u8]) -> bool;
}

impl RedisObjectStoreAccess for EmbeddedStore {
    /// Returns true when Redis object containers are present.
    #[inline(always)]
    fn has_redis_objects(&self) -> bool {
        self.objects.has_objects()
    }

    fn object_read(
        &self,
        key: &[u8],
        op: impl FnOnce(&RedisObjectBucket) -> RedisObjectResult,
    ) -> RedisObjectResult {
        self.object_read_routed(self.route_key(key), key, op)
    }

    #[allow(dead_code)]
    fn object_read_hashed(
        &self,
        key_hash: u64,
        key: &[u8],
        op: impl FnOnce(&RedisObjectBucket) -> RedisObjectResult,
    ) -> RedisObjectResult {
        self.object_read_routed(self.route_key_prehashed(key_hash, key), key, op)
    }

    #[allow(dead_code)]
    fn object_read_hashed_visit(
        &self,
        key_hash: u64,
        key: &[u8],
        op: impl FnOnce(&RedisObjectBucket) -> RedisObjectReadOutcome,
    ) -> RedisObjectReadOutcome {
        let route = self.route_key_prehashed(key_hash, key);
        if !self.objects.shard_has_objects(route.shard_id) {
            return if self.string_exists_routed(route, key) {
                RedisObjectReadOutcome::WrongType
            } else {
                RedisObjectReadOutcome::Missing
            };
        }
        let bucket = self.objects.read_bucket(route.shard_id, route.key_hash);
        let now_ms = now_millis();
        if bucket.has_expirations() && bucket.object_is_expired(key, now_ms) {
            drop(bucket);
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            if bucket.delete_expired(key, now_ms) {
                self.objects.note_deleted(route.shard_id);
            }
            drop(bucket);
            return if self.string_exists_routed(route, key) {
                RedisObjectReadOutcome::WrongType
            } else {
                RedisObjectReadOutcome::Missing
            };
        }
        if bucket.hash_needs_empty_expiry_cleanup(key, now_ms) {
            drop(bucket);
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            if bucket.remove_expired_hash_if_empty(key, now_ms) {
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
        let now_ms = now_millis();
        if bucket.has_expirations() && bucket.object_is_expired(key, now_ms) {
            drop(bucket);
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            if bucket.delete_expired(key, now_ms) {
                self.objects.note_deleted(route.shard_id);
            }
            drop(bucket);
            return if self.string_exists_routed(route, key) {
                RedisObjectResult::WrongType
            } else {
                op(&self.objects.read_bucket(route.shard_id, route.key_hash))
            };
        }
        if bucket.hash_needs_empty_expiry_cleanup(key, now_ms) {
            drop(bucket);
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            if bucket.remove_expired_hash_if_empty(key, now_ms) {
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
    fn object_write_hashed(
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
        let now_ms = now_millis();
        if (bucket.has_expirations() && bucket.delete_expired(key, now_ms))
            || bucket.remove_expired_hash_if_empty(key, now_ms)
        {
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
        let now_ms = now_millis();
        if (bucket.has_expirations() && bucket.delete_expired(key, now_ms))
            || bucket.remove_expired_hash_if_empty(key, now_ms)
        {
            self.objects.note_deleted(route.shard_id);
        }
        let had_object = bucket.contains_object(key);
        if !had_object && self.string_exists_routed(route, key) {
            return RedisObjectResult::WrongType;
        }
        let (result, object_changed) = op(&mut bucket);
        match (had_object, object_changed) {
            (false, true) => self.objects.note_created(route.shard_id),
            (true, true) => self.objects.note_deleted(route.shard_id),
            (_, false) => {}
        }
        result
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
