use super::super::*;

#[allow(dead_code)]
pub(crate) trait RedisKeyStore {
    fn clone_object_value(&self, key: &[u8]) -> Option<RedisObjectValue>;
    fn set_object_value(&self, key: &[u8], value: RedisObjectValue, ttl_ms: Option<u64>);
    fn rename_key(
        &self,
        source: &[u8],
        dest: &[u8],
        nx: bool,
    ) -> std::result::Result<bool, RedisObjectError>;
}

impl RedisKeyStore for EmbeddedStore {
    fn clone_object_value(&self, key: &[u8]) -> Option<RedisObjectValue> {
        let route = self.route_key(key);
        let bucket = self.objects.read_bucket(route.shard_id, route.key_hash);
        if bucket.has_expirations() {
            let now_ms = now_millis();
            if bucket.object_is_expired(key, now_ms) {
                drop(bucket);
                let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
                if bucket.delete_expired(key, now_ms) {
                    self.objects.note_deleted(route.shard_id);
                }
                return None;
            }
        }
        bucket.clone_value(key)
    }

    fn set_object_value(&self, key: &[u8], value: RedisObjectValue, ttl_ms: Option<u64>) {
        let Some(ttl_ms) = ttl_ms else {
            set_object_value_expire_at(self, key, value, None, 0);
            return;
        };
        let now_ms = now_millis();
        set_object_value_expire_at(
            self,
            key,
            value,
            Some(now_ms.saturating_add(ttl_ms)),
            now_ms,
        );
    }

    fn rename_key(
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
}

fn set_object_value_expire_at(
    store: &EmbeddedStore,
    key: &[u8],
    value: RedisObjectValue,
    expire_at_ms: Option<u64>,
    now_ms: u64,
) {
    let route = store.route_key(key);
    let mut bucket = store.objects.write_bucket(route.shard_id, route.key_hash);
    let mut shard = store.shards[route.shard_id].write();
    let had_object = bucket.contains_object(key);
    if let Some(session_prefix) = point_write_session_storage_prefix(key) {
        shard
            .session_slots
            .delete_hashed(&session_prefix, route.key_hash, key);
    }
    shard.map.delete_hashed(route.key_hash, key, now_ms);
    let created = bucket.insert_value(key.to_vec(), value);
    if created && !had_object {
        store.objects.note_created(route.shard_id);
    }
    if let Some(expire_at_ms) = expire_at_ms {
        bucket.expire(key, expire_at_ms, now_ms);
    }
}
