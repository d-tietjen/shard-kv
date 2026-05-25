use super::super::*;

#[allow(dead_code)]
pub(crate) trait RedisStringStore {
    fn get_string_value_into<F>(&self, key: &[u8], write: F) -> RedisStringLookup
    where
        F: FnMut(&bytes::Bytes);

    fn mutate_string_value_no_ttl_in_place<F>(&self, key: &[u8], mutate: F) -> RedisStringLookup
    where
        F: FnMut(&mut [u8]);

    fn transform_string_value_no_ttl<R, E>(
        &self,
        key: &[u8],
        transform: impl FnOnce(Option<&[u8]>) -> std::result::Result<(R, Bytes), E>,
        wrong_type: impl FnOnce() -> E,
    ) -> std::result::Result<R, E>;
}

impl RedisStringStore for EmbeddedStore {
    #[inline(always)]
    fn get_string_value_into<F>(&self, key: &[u8], mut write: F) -> RedisStringLookup
    where
        F: FnMut(&bytes::Bytes),
    {
        let route = self.route_key(key);

        if self.with_shared_value_bytes_routed(route, key, &mut write) {
            return RedisStringLookup::Hit;
        }

        if !self.objects.shard_has_objects(route.shard_id) {
            return RedisStringLookup::Miss;
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
                return RedisStringLookup::Miss;
            }
        }
        if bucket.contains_object(key) {
            RedisStringLookup::WrongType
        } else {
            RedisStringLookup::Miss
        }
    }

    fn mutate_string_value_no_ttl_in_place<F>(&self, key: &[u8], mut mutate: F) -> RedisStringLookup
    where
        F: FnMut(&mut [u8]),
    {
        let route = self.route_key(key);
        if self.objects.shard_has_objects(route.shard_id) {
            let bucket = self.objects.read_bucket(route.shard_id, route.key_hash);
            match bucket.has_expirations() {
                true => {
                    let now_ms = now_millis();
                    match bucket.object_is_expired(key, now_ms) {
                        true => {
                            drop(bucket);
                            let mut bucket =
                                self.objects.write_bucket(route.shard_id, route.key_hash);
                            if bucket.delete_expired(key, now_ms) {
                                self.objects.note_deleted(route.shard_id);
                            }
                        }
                        false if bucket.contains_object(key) => {
                            return RedisStringLookup::WrongType;
                        }
                        false => {}
                    }
                }
                false if bucket.contains_object(key) => return RedisStringLookup::WrongType,
                false => {}
            }
        }

        let mut shard = self.shards[route.shard_id].write();
        match shard.update_value_hashed_no_ttl(route.key_hash, key, |value| mutate(value)) {
            Some(()) => RedisStringLookup::Hit,
            None => RedisStringLookup::Miss,
        }
    }

    fn transform_string_value_no_ttl<R, E>(
        &self,
        key: &[u8],
        transform: impl FnOnce(Option<&[u8]>) -> std::result::Result<(R, Bytes), E>,
        wrong_type: impl FnOnce() -> E,
    ) -> std::result::Result<R, E> {
        let route = self.route_key(key);
        if self.objects.shard_has_objects(route.shard_id) {
            let bucket = self.objects.read_bucket(route.shard_id, route.key_hash);
            match bucket.has_expirations() {
                true => {
                    let now_ms = now_millis();
                    match bucket.object_is_expired(key, now_ms) {
                        true => {
                            drop(bucket);
                            let mut bucket =
                                self.objects.write_bucket(route.shard_id, route.key_hash);
                            if bucket.delete_expired(key, now_ms) {
                                self.objects.note_deleted(route.shard_id);
                            }
                        }
                        false if bucket.contains_object(key) => return Err(wrong_type()),
                        false => {}
                    }
                }
                false if bucket.contains_object(key) => return Err(wrong_type()),
                false => {}
            }
        }

        let now_ms = now_millis();
        let mut shard = self.shards[route.shard_id].write();
        let result = shard.transform_value_hashed_no_ttl(route.key_hash, key, now_ms, transform)?;
        self.refresh_string_key_count(route.shard_id, &shard);
        Ok(result)
    }
}
