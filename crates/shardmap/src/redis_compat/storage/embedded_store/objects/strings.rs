use super::super::*;
use crate::protocol::Frame;
use crate::storage::VECTOR_SET_PREFIX;

#[allow(dead_code)]
pub(crate) trait RedisStringStore {
    fn get_raw_string_value_into<F>(&self, key: &[u8], write: F) -> RedisStringLookup
    where
        F: FnMut(&bytes::Bytes);

    fn get_raw_vector_value_into<F>(&self, key: &[u8], write: F) -> RedisStringLookup
    where
        F: FnMut(&bytes::Bytes);

    fn get_string_value_into<F>(&self, key: &[u8], write: F) -> RedisStringLookup
    where
        F: FnMut(&bytes::Bytes);

    fn mutate_string_value_no_ttl_in_place<F>(&self, key: &[u8], mutate: F) -> RedisStringLookup
    where
        F: FnMut(&mut [u8]);

    fn transform_string_value_no_ttl<R>(
        &self,
        key: &[u8],
        transform: impl FnOnce(Option<&[u8]>) -> std::result::Result<(R, Bytes), Frame>,
        wrong_type: fn() -> Frame,
    ) -> std::result::Result<R, Frame>;

    fn transform_raw_string_value_no_ttl<R>(
        &self,
        key: &[u8],
        transform: impl FnOnce(Option<&[u8]>) -> std::result::Result<(R, Bytes), Frame>,
        wrong_type: fn() -> Frame,
    ) -> std::result::Result<R, Frame>;

    fn transform_raw_vector_value_preserve_ttl<R>(
        &self,
        key: &[u8],
        transform: impl FnOnce(Option<&[u8]>) -> std::result::Result<(R, Bytes), Frame>,
        wrong_type: fn() -> Frame,
    ) -> std::result::Result<R, Frame>;
}

impl RedisStringStore for EmbeddedStore {
    #[inline(always)]
    fn get_raw_string_value_into<F>(&self, key: &[u8], mut write: F) -> RedisStringLookup
    where
        F: FnMut(&bytes::Bytes),
    {
        let route = self.route_key(key);
        get_raw_string_value_routed_into(self, route, key, &mut write)
    }

    #[inline(always)]
    fn get_raw_vector_value_into<F>(&self, key: &[u8], mut write: F) -> RedisStringLookup
    where
        F: FnMut(&bytes::Bytes),
    {
        let route = self.route_vector_key(key);
        if vector_key_conflicts_with_primary_route(self, route, key) {
            return RedisStringLookup::WrongType;
        }
        get_raw_string_value_routed_into(self, route, key, &mut write)
    }

    #[inline(always)]
    fn get_string_value_into<F>(&self, key: &[u8], mut write: F) -> RedisStringLookup
    where
        F: FnMut(&bytes::Bytes),
    {
        let mut vector_set = false;
        let lookup = self.get_raw_string_value_into(key, |bytes| {
            if bytes.starts_with(VECTOR_SET_PREFIX) {
                vector_set = true;
            } else {
                write(bytes);
            }
        });
        match (lookup, vector_set) {
            (RedisStringLookup::Hit, true) => RedisStringLookup::WrongType,
            (RedisStringLookup::Miss, _) if pinned_vector_value_exists(self, key) => {
                RedisStringLookup::WrongType
            }
            (lookup, _) => lookup,
        }
    }

    fn mutate_string_value_no_ttl_in_place<F>(&self, key: &[u8], mut mutate: F) -> RedisStringLookup
    where
        F: FnMut(&mut [u8]),
    {
        let route = self.route_key(key);
        if self.objects.shard_has_objects(route.shard_id) {
            let now_ms = now_millis();
            let bucket = self.objects.read_bucket(route.shard_id, route.key_hash);
            match () {
                _ if bucket.has_expirations() && bucket.object_is_expired(key, now_ms) => {
                    drop(bucket);
                    let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
                    if bucket.delete_expired(key, now_ms) {
                        self.objects.note_deleted(route.shard_id);
                    }
                }
                _ if bucket.hash_needs_empty_expiry_cleanup(key, now_ms) => {
                    drop(bucket);
                    let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
                    if bucket.remove_expired_hash_if_empty(key, now_ms) {
                        self.objects.note_deleted(route.shard_id);
                    }
                }
                _ if bucket.contains_object(key) => return RedisStringLookup::WrongType,
                _ => {}
            }
        }

        let mut shard = self.shards[route.shard_id].write();
        let mut vector_set = false;
        match shard.update_value_hashed_no_ttl(route.key_hash, key, |value| {
            if value.starts_with(VECTOR_SET_PREFIX) {
                vector_set = true;
            } else {
                mutate(value);
            }
        }) {
            Some(()) if vector_set => RedisStringLookup::WrongType,
            Some(()) => {
                if let Some(value) = shard.get_shared_value_bytes_hashed_no_ttl(route.key_hash, key)
                {
                    self.notify_point_mutation(
                        PointMutationKind::Set,
                        key,
                        Some(value.clone()),
                        None,
                        None,
                    );
                }
                RedisStringLookup::Hit
            }
            None if pinned_vector_value_exists(self, key) => RedisStringLookup::WrongType,
            None => RedisStringLookup::Miss,
        }
    }

    fn transform_string_value_no_ttl<R>(
        &self,
        key: &[u8],
        transform: impl FnOnce(Option<&[u8]>) -> std::result::Result<(R, Bytes), Frame>,
        wrong_type: fn() -> Frame,
    ) -> std::result::Result<R, Frame> {
        self.transform_raw_string_value_no_ttl(
            key,
            |existing| {
                if existing.is_some_and(|value| value.starts_with(VECTOR_SET_PREFIX)) {
                    Err(wrong_type())
                } else {
                    transform(existing)
                }
            },
            wrong_type,
        )
    }

    fn transform_raw_string_value_no_ttl<R>(
        &self,
        key: &[u8],
        transform: impl FnOnce(Option<&[u8]>) -> std::result::Result<(R, Bytes), Frame>,
        wrong_type: fn() -> Frame,
    ) -> std::result::Result<R, Frame> {
        if pinned_vector_value_exists(self, key) {
            return Err(wrong_type());
        }
        let route = self.route_key(key);
        if self.objects.shard_has_objects(route.shard_id) {
            let now_ms = now_millis();
            let bucket = self.objects.read_bucket(route.shard_id, route.key_hash);
            match () {
                _ if bucket.has_expirations() && bucket.object_is_expired(key, now_ms) => {
                    drop(bucket);
                    let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
                    if bucket.delete_expired(key, now_ms) {
                        self.objects.note_deleted(route.shard_id);
                    }
                }
                _ if bucket.hash_needs_empty_expiry_cleanup(key, now_ms) => {
                    drop(bucket);
                    let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
                    if bucket.remove_expired_hash_if_empty(key, now_ms) {
                        self.objects.note_deleted(route.shard_id);
                    }
                }
                _ if bucket.contains_object(key) => return Err(wrong_type()),
                _ => {}
            }
        }

        let now_ms = now_millis();
        let mut shard = self.shards[route.shard_id].write();
        let mut observed = None;
        let result =
            shard.transform_value_hashed_no_ttl(route.key_hash, key, now_ms, |existing| {
                let (result, value) = transform(existing)?;
                if !self.point_mutation_is_accepted(key, value.len(), None) {
                    return Err(crate::commands::redis::error(
                        "ERR mutation rejected by an installed storage extension",
                    ));
                }
                observed = Some(bytes::Bytes::copy_from_slice(&value));
                Ok((result, value))
            })?;
        self.refresh_string_key_count(route.shard_id, &shard);
        if let Some(value) = observed {
            self.notify_point_mutation(PointMutationKind::Set, key, Some(value), None, None);
        }
        Ok(result)
    }

    fn transform_raw_vector_value_preserve_ttl<R>(
        &self,
        key: &[u8],
        transform: impl FnOnce(Option<&[u8]>) -> std::result::Result<(R, Bytes), Frame>,
        wrong_type: fn() -> Frame,
    ) -> std::result::Result<R, Frame> {
        let route = self.route_vector_key(key);
        if vector_key_conflicts_with_primary_route(self, route, key) {
            return Err(wrong_type());
        }
        transform_raw_vector_value_routed(self, route, key, transform, wrong_type)
    }
}

fn get_raw_string_value_routed_into<F>(
    store: &EmbeddedStore,
    route: EmbeddedKeyRoute,
    key: &[u8],
    write: &mut F,
) -> RedisStringLookup
where
    F: FnMut(&bytes::Bytes),
{
    match store.try_with_shared_value_bytes_routed(route, key, write) {
        Ok(true) => return RedisStringLookup::Hit,
        Ok(false) => {}
        Err(_) => return RedisStringLookup::BackendError,
    }

    object_lookup_for_string_route(store, route, key).unwrap_or(RedisStringLookup::Miss)
}

fn transform_raw_vector_value_routed<R>(
    store: &EmbeddedStore,
    route: EmbeddedKeyRoute,
    key: &[u8],
    transform: impl FnOnce(Option<&[u8]>) -> std::result::Result<(R, Bytes), Frame>,
    wrong_type: fn() -> Frame,
) -> std::result::Result<R, Frame> {
    if matches!(
        object_lookup_for_string_route(store, route, key),
        Some(RedisStringLookup::WrongType)
    ) {
        return Err(wrong_type());
    }

    let now_ms = now_millis();
    let mut shard = store.shards[route.shard_id].write();
    let result =
        shard.transform_value_hashed_preserve_ttl(route.key_hash, key, now_ms, transform)?;
    store.refresh_string_key_count(route.shard_id, &shard);
    Ok(result)
}

fn object_lookup_for_string_route(
    store: &EmbeddedStore,
    route: EmbeddedKeyRoute,
    key: &[u8],
) -> Option<RedisStringLookup> {
    if !store.objects.shard_has_objects(route.shard_id) {
        return None;
    }

    let bucket = store.objects.read_bucket(route.shard_id, route.key_hash);
    let now_ms = now_millis();
    if bucket.has_expirations() && bucket.object_is_expired(key, now_ms) {
        drop(bucket);
        let mut bucket = store.objects.write_bucket(route.shard_id, route.key_hash);
        if bucket.delete_expired(key, now_ms) {
            store.objects.note_deleted(route.shard_id);
        }
        return Some(RedisStringLookup::Miss);
    }
    if bucket.hash_needs_empty_expiry_cleanup(key, now_ms) {
        drop(bucket);
        let mut bucket = store.objects.write_bucket(route.shard_id, route.key_hash);
        if bucket.remove_expired_hash_if_empty(key, now_ms) {
            store.objects.note_deleted(route.shard_id);
        }
        return Some(RedisStringLookup::Miss);
    }
    bucket
        .contains_live_object(key, now_ms)
        .then_some(RedisStringLookup::WrongType)
}

fn vector_key_conflicts_with_primary_route(
    store: &EmbeddedStore,
    vector_route: EmbeddedKeyRoute,
    key: &[u8],
) -> bool {
    let primary_route = store.route_key(key);
    if primary_route.shard_id == vector_route.shard_id {
        return false;
    }
    if matches!(
        object_lookup_for_string_route(store, primary_route, key),
        Some(RedisStringLookup::WrongType)
    ) {
        return true;
    }
    let mut conflicts = false;
    store.with_shared_value_bytes_routed(primary_route, key, &mut |bytes| {
        conflicts = !bytes.starts_with(VECTOR_SET_PREFIX);
    });
    conflicts
}

pub(super) fn pinned_vector_value_exists(store: &EmbeddedStore, key: &[u8]) -> bool {
    let vector_route = store.route_vector_key(key);
    if vector_route.shard_id == store.route_key(key).shard_id {
        return false;
    }
    let mut is_vector = false;
    store.with_shared_value_bytes_routed(vector_route, key, &mut |bytes| {
        is_vector = bytes.starts_with(VECTOR_SET_PREFIX);
    });
    is_vector
}
