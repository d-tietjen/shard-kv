use super::super::*;
use crate::storage::RedisKeyError;

#[allow(dead_code)]
pub(crate) trait RedisKeyStore {
    fn clone_object_value(&self, key: &[u8]) -> Option<RedisObjectValue>;
    fn clone_vector_key_value(&self, key: &[u8]) -> Option<bytes::Bytes>;
    fn set_object_value(&self, key: &[u8], value: RedisObjectValue, ttl_ms: Option<u64>);
    fn set_pinned_vector_value(
        &self,
        key: &[u8],
        value: bytes::Bytes,
        ttl_ms: Option<u64>,
    ) -> std::result::Result<(), RedisKeyError>;
    fn rename_key(
        &self,
        source: &[u8],
        dest: &[u8],
        nx: bool,
    ) -> std::result::Result<bool, RedisKeyError>;
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
        bucket.clone_value(key, now_millis())
    }

    fn clone_vector_key_value(&self, key: &[u8]) -> Option<bytes::Bytes> {
        self.clone_vector_value(key)
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

    fn set_pinned_vector_value(
        &self,
        key: &[u8],
        value: bytes::Bytes,
        ttl_ms: Option<u64>,
    ) -> std::result::Result<(), RedisKeyError> {
        if !self.vector_mutation_is_accepted(key, &value) {
            return Err(RedisKeyError::ReplicationLimit);
        }
        let now_ms = now_millis();
        let primary_route = self.route_key(key);
        let vector_route = self.route_vector_key(key);
        if primary_route.shard_id != vector_route.shard_id {
            self.delete_routed_then(primary_route, key, now_ms, || {});
        }
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        self.set_value_bytes_routed_expire_at(
            vector_route,
            key,
            value.clone(),
            expire_at_ms,
            now_ms,
        );
        self.notify_vector_mutation(
            crate::storage::VectorMutationKind::Set,
            key,
            Some(value),
            expire_at_ms,
        );
        Ok(())
    }

    fn rename_key(
        &self,
        source: &[u8],
        dest: &[u8],
        nx: bool,
    ) -> std::result::Result<bool, RedisKeyError> {
        if source == dest {
            if !self.exists(source) {
                return Err(RedisKeyError::MissingKey);
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
        if let Some(value) = self.clone_vector_key_value(source) {
            if !self.vector_mutation_is_accepted(dest, &value) {
                return Err(RedisKeyError::ReplicationLimit);
            }
            self.delete(dest);
            self.set_pinned_vector_value(dest, value, ttl_ms)?;
            self.delete(source);
            return Ok(true);
        }
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
        Err(RedisKeyError::MissingKey)
    }
}

fn set_object_value_expire_at(
    store: &EmbeddedStore,
    key: &[u8],
    value: RedisObjectValue,
    expire_at_ms: Option<u64>,
    now_ms: u64,
) {
    let should_notify = match &value {
        RedisObjectValue::List(values) => !values.is_empty(),
        RedisObjectValue::ZSet(values) => !values.is_empty(),
        _ => false,
    };
    let route = store.route_key(key);
    store.delete_pinned_vector_value_if_distinct(route, key, now_ms);
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
    drop(shard);
    drop(bucket);
    if should_notify {
        store.notify_redis_object_shard(route.shard_id);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::storage::VECTOR_SET_PREFIX;

    use super::*;

    #[test]
    fn rejected_vector_rename_preserves_source_and_destination() {
        let store = EmbeddedStore::new(4);
        let mut vector = VECTOR_SET_PREFIX.to_vec();
        vector.extend_from_slice(b"state");
        store
            .set_pinned_vector_value(b"source", vector.into(), None)
            .unwrap();
        store.set(b"destination".to_vec(), b"original".to_vec(), None);
        store.configure_vector_mutation_validator(Some(Arc::new(|key, _| key != b"destination")));

        assert_eq!(
            store.rename_key(b"source", b"destination", false),
            Err(RedisKeyError::ReplicationLimit)
        );
        assert!(store.clone_vector_key_value(b"source").is_some());
        assert_eq!(
            store.get_value_bytes(b"destination").as_deref(),
            Some(b"original".as_slice())
        );
    }
}
