use super::*;

impl EmbeddedStore {
    /// Inserts or replaces a byte-string value.
    ///
    /// `ttl_ms` is a relative TTL in milliseconds. Passing `None` creates a
    /// persistent value.
    pub fn set<K, V>(&self, key: K, value: V, ttl_ms: Option<u64>)
    where
        K: Into<Bytes>,
        V: Into<Bytes>,
    {
        let now_ms = now_millis();
        let key = key.into();
        let route = self.route_key(&key);
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        #[cfg(feature = "redis-compat")]
        if self.objects.has_objects() {
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            let mut shard = self.shards[route.shard_id].write();
            if bucket.delete_any(&key) {
                self.objects.note_deleted(route.shard_id);
            }
            if let Some(session_prefix) = point_write_session_storage_prefix(&key) {
                shard
                    .session_slots
                    .delete_hashed(&session_prefix, route.key_hash, &key);
            }
            shard
                .map
                .set_hashed(route.key_hash, key, value, expire_at_ms, now_ms);
            shard.enforce_memory_limit(now_ms);
            return;
        }
        let mut shard = self.shards[route.shard_id].write();
        if let Some(session_prefix) = point_write_session_storage_prefix(&key) {
            shard
                .session_slots
                .delete_hashed(&session_prefix, route.key_hash, &key);
        }
        shard
            .map
            .set_hashed(route.key_hash, key, value, expire_at_ms, now_ms);
        shard.enforce_memory_limit(now_ms);
    }

    /// Zero-copy `SET` for the multi-direct hot path. Takes `key` as a slice
    /// (copied into the entry's small `Box<[u8]>`) and `value` as an
    /// already-owned `bytes::Bytes` (typically a slice of the connection read
    /// buffer obtained via `HandoffBuffer::split_prefix`). Skips the
    /// `value.to_vec()` allocation that the generic `set` performs.
    pub fn set_value_bytes(&self, key: &[u8], value: bytes::Bytes, ttl_ms: Option<u64>) {
        let now_ms = now_millis();
        let route = self.route_key(key);
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        self.set_value_bytes_routed_expire_at(route, key, value, expire_at_ms, now_ms);
    }

    /// Stores an already-owned value using precomputed routing and an absolute
    /// expiry timestamp. This is used by native replication apply paths, where
    /// the wire frame already carries both the shard sequence and absolute TTL.
    pub(crate) fn set_value_bytes_routed_expire_at(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        value: bytes::Bytes,
        expire_at_ms: Option<u64>,
        now_ms: u64,
    ) {
        self.set_value_bytes_routed_expire_at_then(route, key, value, expire_at_ms, now_ms, || {});
    }

    /// Stores an already-owned value without reading the wall clock and runs
    /// `after_write` before releasing the shard write lock.
    pub(crate) fn set_value_bytes_routed_no_ttl_then(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        value: bytes::Bytes,
        after_write: impl FnOnce(),
    ) {
        let route = match route.shard_id < self.shards.len() {
            true => route,
            false => self.route_key(key),
        };
        #[cfg(feature = "redis-compat")]
        if self.objects.has_objects() {
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            let mut shard = self.shards[route.shard_id].write();
            if bucket.delete_any(key) {
                self.objects.note_deleted(route.shard_id);
            }
            if let Some(session_prefix) = point_write_session_storage_prefix(key) {
                shard
                    .session_slots
                    .delete_hashed(&session_prefix, route.key_hash, key);
            }
            shard
                .map
                .set_bytes_hashed(route.key_hash, key, value, None, 0);
            shard.enforce_memory_limit(0);
            after_write();
            return;
        }
        let mut shard = self.shards[route.shard_id].write();
        if let Some(session_prefix) = point_write_session_storage_prefix(key) {
            shard
                .session_slots
                .delete_hashed(&session_prefix, route.key_hash, key);
        }
        shard
            .map
            .set_bytes_hashed(route.key_hash, key, value, None, 0);
        shard.enforce_memory_limit(0);
        after_write();
    }

    /// Stores an already-owned value and runs `after_write` before releasing
    /// the shard write lock. Replication uses this to preserve same-shard
    /// mutation order without adding a second ordering mutex around storage.
    pub(crate) fn set_value_bytes_routed_expire_at_then(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        value: bytes::Bytes,
        expire_at_ms: Option<u64>,
        now_ms: u64,
        after_write: impl FnOnce(),
    ) {
        let route = match route.shard_id < self.shards.len() {
            true => route,
            false => self.route_key(key),
        };
        #[cfg(feature = "redis-compat")]
        if self.objects.has_objects() {
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            let mut shard = self.shards[route.shard_id].write();
            if bucket.delete_any(key) {
                self.objects.note_deleted(route.shard_id);
            }
            if let Some(session_prefix) = point_write_session_storage_prefix(key) {
                shard
                    .session_slots
                    .delete_hashed(&session_prefix, route.key_hash, key);
            }
            shard
                .map
                .set_bytes_hashed(route.key_hash, key, value, expire_at_ms, now_ms);
            shard.enforce_memory_limit(now_ms);
            after_write();
            return;
        }
        let mut shard = self.shards[route.shard_id].write();
        if let Some(session_prefix) = point_write_session_storage_prefix(key) {
            shard
                .session_slots
                .delete_hashed(&session_prefix, route.key_hash, key);
        }
        shard
            .map
            .set_bytes_hashed(route.key_hash, key, value, expire_at_ms, now_ms);
        shard.enforce_memory_limit(now_ms);
        after_write();
    }

    pub fn set_routed_no_ttl<K, V>(&self, route: EmbeddedKeyRoute, key: K, value: V)
    where
        K: Into<Bytes>,
        V: Into<Bytes>,
    {
        let key = key.into();
        let mut shard = self.shards[route.shard_id].write();
        if let Some(session_prefix) = point_write_session_storage_prefix(&key) {
            shard
                .session_slots
                .delete_hashed(&session_prefix, route.key_hash, &key);
        }
        shard.map.set_hashed(route.key_hash, key, value, None, 0);
        shard.enforce_memory_limit(0);
    }

    pub fn set_slice_routed_no_ttl(&self, route: EmbeddedKeyRoute, key: &[u8], value: &[u8]) {
        let mut shard = self.shards[route.shard_id].write();
        if let Some(session_prefix) = point_write_session_storage_prefix(key) {
            shard
                .session_slots
                .delete_hashed(&session_prefix, route.key_hash, key);
        }
        shard
            .map
            .set_slice_hashed(route.key_hash, key, value, None, 0);
        shard.enforce_memory_limit(0);
    }

    pub fn batch_set_session_slices_routed_no_ttl<I, K, V>(
        &self,
        route: EmbeddedSessionRoute,
        items: I,
    ) where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut shard = self.shards[route.shard_id].write();
        for (key, value) in items {
            let key = key.as_ref();
            let key_hash = hash_key(key);
            shard
                .map
                .set_slice_hashed(key_hash, key, value.as_ref(), None, 0);
        }
        shard.enforce_memory_limit(0);
    }

    pub fn batch_set_session_slices_no_ttl<I, K, V>(&self, session_prefix: &[u8], items: I)
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let route = self.route_session(session_prefix);
        let mut shard = self.shards[route.shard_id].write();
        for (key, value) in items {
            let key = key.as_ref();
            let key_hash = hash_key(key);
            shard.map.delete_hashed(key_hash, key, 0);
            shard
                .session_slots
                .set_slice_hashed(session_prefix, key_hash, key, value.as_ref());
        }
        shard.enforce_memory_limit(0);
    }

    pub fn batch_set_session_owned_no_ttl(
        &self,
        session_prefix: Bytes,
        items: Vec<(Bytes, Bytes)>,
    ) {
        if items.is_empty() {
            return;
        }
        self.batch_set_session_packed_no_ttl(PackedSessionWrite::from_owned_items(
            session_prefix,
            items,
        ));
    }

    pub fn batch_set_session_packed_no_ttl(&self, packed: PackedSessionWrite) {
        if packed.item_count() == 0 {
            return;
        }
        let route = self.route_session(&packed.session_prefix);
        let mut shard = self.shards[route.shard_id].write();
        for entry in packed.slab.entries.iter() {
            shard.map.delete_hashed(entry.hash, &entry.key, 0);
        }
        shard.session_slots.replace_session_slab(packed);
        shard.enforce_memory_limit(0);
    }

    pub fn set_routed<K, V>(&self, route: EmbeddedKeyRoute, key: K, value: V, ttl_ms: Option<u64>)
    where
        K: Into<Bytes>,
        V: Into<Bytes>,
    {
        let now_ms = now_millis();
        let key = key.into();
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        let mut shard = self.shards[route.shard_id].write();
        if let Some(session_prefix) = point_write_session_storage_prefix(&key) {
            shard
                .session_slots
                .delete_hashed(&session_prefix, route.key_hash, &key);
        }
        shard
            .map
            .set_hashed(route.key_hash, key, value, expire_at_ms, now_ms);
        shard.enforce_memory_limit(now_ms);
    }

    /// Inserts or replaces multiple byte-string values.
    ///
    /// `ttl_ms` applies the same relative TTL to every item in the batch.
    pub fn batch_set(&self, items: Vec<(Bytes, Bytes)>, ttl_ms: Option<u64>) {
        if items.is_empty() {
            return;
        }

        let now_ms = now_millis();
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        #[cfg(feature = "redis-compat")]
        if self.objects.has_objects() {
            for (key, value) in items {
                let route = self.route_key(&key);
                let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
                let mut shard = self.shards[route.shard_id].write();
                if bucket.delete_any(&key) {
                    self.objects.note_deleted(route.shard_id);
                }
                if let Some(session_prefix) = point_write_session_storage_prefix(&key) {
                    shard
                        .session_slots
                        .delete_hashed(&session_prefix, route.key_hash, &key);
                }
                shard
                    .map
                    .set_hashed(route.key_hash, key, value, expire_at_ms, now_ms);
                shard.enforce_memory_limit(now_ms);
            }
            return;
        }
        let mut groups = vec![Vec::<(Bytes, Bytes, u64)>::new(); self.shards.len()];

        for (key, value) in items {
            let (route_hash, key_hash) = self.hashes_for_key(&key);
            groups[self.route_hash(route_hash)].push((key, value, key_hash));
        }

        for (shard_id, batch) in groups.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            let mut shard = self.shards[shard_id].write();
            for (key, value, key_hash) in batch {
                if let Some(session_prefix) = point_write_session_storage_prefix(&key) {
                    shard
                        .session_slots
                        .delete_hashed(&session_prefix, key_hash, &key);
                }
                shard
                    .map
                    .set_hashed(key_hash, key, value, expire_at_ms, now_ms);
            }
            shard.enforce_memory_limit(now_ms);
        }
    }
}
