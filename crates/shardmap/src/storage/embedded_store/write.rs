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
        let value = bytes::Bytes::from(value.into());
        let route = self.route_key(&key);
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        self.set_value_bytes_routed_expire_at_then(
            route,
            &key,
            value.clone(),
            expire_at_ms,
            now_ms,
            || {
                self.notify_point_mutation(
                    PointMutationKind::Set,
                    &key,
                    Some(value),
                    expire_at_ms,
                    None,
                );
            },
        );
    }

    /// Zero-copy `SET` for the multi-direct hot path. Takes `key` as a slice
    /// (copied into the entry's small `Box<[u8]>`) and `value` as an
    /// already-owned `bytes::Bytes` (typically a slice of the connection read
    /// buffer obtained via `HandoffBuffer::split_prefix`). Skips the
    /// `value.to_vec()` allocation that the generic `set` performs.
    pub fn set_value_bytes(&self, key: &[u8], value: bytes::Bytes, ttl_ms: Option<u64>) {
        let route = self.route_key(key);
        if ttl_ms.is_none() {
            self.set_value_bytes_routed_no_ttl_then(route, key, value.clone(), || {
                self.notify_point_mutation(PointMutationKind::Set, key, Some(value), None, None);
            });
            return;
        }
        let now_ms = now_millis();
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        self.set_value_bytes_routed_expire_at_then(
            route,
            key,
            value.clone(),
            expire_at_ms,
            now_ms,
            || {
                self.notify_point_mutation(
                    PointMutationKind::Set,
                    key,
                    Some(value),
                    expire_at_ms,
                    None,
                );
            },
        );
    }

    /// Atomically inserts or replaces a protected point value, TTL, and opaque
    /// governance metadata. Ordinary point reads treat the entry as a miss.
    pub fn set_value_bytes_with_governance(
        &self,
        key: &[u8],
        value: bytes::Bytes,
        ttl_ms: Option<u64>,
        governance: bytes::Bytes,
    ) {
        let now_ms = ttl_ms.map_or(0, |_| now_millis());
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        let route = self.route_key(key);
        let observed_value = value.clone();
        let observed_governance = governance.clone();
        self.set_value_bytes_routed_with_governance_then(
            route,
            key,
            value,
            Some(governance),
            expire_at_ms,
            now_ms,
            || {
                self.notify_point_mutation(
                    PointMutationKind::Set,
                    key,
                    Some(observed_value),
                    expire_at_ms,
                    Some(observed_governance),
                );
            },
        );
    }

    /// Stores an already-owned value using precomputed routing and an absolute
    /// expiry timestamp. Advanced extensions can use this when they already
    /// carry an absolute TTL.
    #[cfg(feature = "redis")]
    #[doc(hidden)]
    pub fn set_value_bytes_routed_expire_at(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        value: bytes::Bytes,
        expire_at_ms: Option<u64>,
        now_ms: u64,
    ) {
        self.set_value_bytes_routed_expire_at_then(route, key, value, expire_at_ms, now_ms, || {});
    }

    #[doc(hidden)]
    pub fn set_value_bytes_routed_expire_at_with_governance(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        value: bytes::Bytes,
        governance: Option<bytes::Bytes>,
        expire_at_ms: Option<u64>,
        now_ms: u64,
    ) {
        self.set_value_bytes_routed_with_governance_then(
            route,
            key,
            value,
            governance,
            expire_at_ms,
            now_ms,
            || {},
        );
    }

    #[cfg(feature = "kv-overflow")]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_set_value_bytes_routed_overflow(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        value: bytes::Bytes,
        governance: Option<bytes::Bytes>,
        expire_at_ms: Option<u64>,
        generation: u64,
        hard_limit: usize,
    ) -> bool {
        let now_ms = expire_at_ms.map_or(0, |_| now_millis());
        let route = match route.shard_id < self.shards.len() {
            true => route,
            false => self.route_key(key),
        };
        #[cfg(feature = "redis")]
        self.delete_pinned_vector_value_if_distinct(route, key, now_ms);
        #[cfg(feature = "redis")]
        if self.objects.shard_has_objects(route.shard_id) {
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            let mut shard = self.shards[route.shard_id].write();
            if !shard.map.can_set_bytes_hashed_with_limit(
                route.key_hash,
                key,
                value.len(),
                governance.as_ref().map_or(0, bytes::Bytes::len),
                hard_limit,
            ) {
                return false;
            }
            if bucket.delete_any(key) {
                self.objects.note_deleted(route.shard_id);
            }
            if let Some(session_prefix) = point_write_session_storage_prefix(key) {
                shard
                    .session_slots
                    .delete_hashed(&session_prefix, route.key_hash, key);
            }
            shard.map.set_bytes_hashed_overflow(
                route.key_hash,
                key,
                value,
                governance,
                expire_at_ms,
                now_ms,
                generation,
            );
            shard.enforce_memory_limit(now_ms);
            self.refresh_string_key_count(route.shard_id, &shard);
            return true;
        }
        let mut shard = self.shards[route.shard_id].write();
        if !shard.map.can_set_bytes_hashed_with_limit(
            route.key_hash,
            key,
            value.len(),
            governance.as_ref().map_or(0, bytes::Bytes::len),
            hard_limit,
        ) {
            return false;
        }
        if let Some(session_prefix) = point_write_session_storage_prefix(key) {
            shard
                .session_slots
                .delete_hashed(&session_prefix, route.key_hash, key);
        }
        shard.map.set_bytes_hashed_overflow(
            route.key_hash,
            key,
            value,
            governance,
            expire_at_ms,
            now_ms,
            generation,
        );
        shard.enforce_memory_limit(now_ms);
        #[cfg(feature = "redis")]
        self.refresh_string_key_count(route.shard_id, &shard);
        true
    }

    #[cfg(feature = "kv-overflow")]
    pub(crate) fn overflow_generation_matches(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        generation: u64,
    ) -> bool {
        self.shards.get(route.shard_id).is_some_and(|shard| {
            shard
                .read()
                .map
                .overflow_generation_matches(route.key_hash, key, generation)
        })
    }

    /// Stores an already-owned value without reading the wall clock and runs
    /// `after_write` before releasing the shard write lock.
    #[doc(hidden)]
    pub fn set_value_bytes_routed_no_ttl_then(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        value: bytes::Bytes,
        after_write: impl FnOnce(),
    ) {
        self.set_value_bytes_routed_with_governance_then(
            route,
            key,
            value,
            None,
            None,
            0,
            after_write,
        );
    }

    #[allow(clippy::too_many_arguments)]
    #[doc(hidden)]
    pub fn set_value_bytes_routed_with_governance_then(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        value: bytes::Bytes,
        governance: Option<bytes::Bytes>,
        expire_at_ms: Option<u64>,
        now_ms: u64,
        after_write: impl FnOnce(),
    ) {
        if !self.point_mutation_is_accepted(
            key,
            value.len(),
            governance.as_ref().map(bytes::Bytes::len),
        ) {
            tracing::warn!(
                key_len = key.len(),
                value_len = value.len(),
                "point mutation rejected by an installed storage extension"
            );
            return;
        }
        let route = match route.shard_id < self.shards.len() {
            true => route,
            false => self.route_key(key),
        };
        #[cfg(feature = "redis")]
        self.delete_pinned_vector_value_if_distinct(route, key, 0);
        #[cfg(feature = "redis")]
        if self.objects.shard_has_objects(route.shard_id) {
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
            shard.map.set_bytes_hashed_with_governance_option(
                route.key_hash,
                key,
                value,
                governance,
                expire_at_ms,
                now_ms,
            );
            shard.enforce_memory_limit(now_ms);
            self.refresh_string_key_count(route.shard_id, &shard);
            after_write();
            return;
        }
        let mut shard = self.shards[route.shard_id].write();
        if let Some(session_prefix) = point_write_session_storage_prefix(key) {
            shard
                .session_slots
                .delete_hashed(&session_prefix, route.key_hash, key);
        }
        shard.map.set_bytes_hashed_with_governance_option(
            route.key_hash,
            key,
            value,
            governance,
            expire_at_ms,
            now_ms,
        );
        shard.enforce_memory_limit(now_ms);
        #[cfg(feature = "redis")]
        self.refresh_string_key_count(route.shard_id, &shard);
        after_write();
    }

    /// Stores an already-owned value and runs `after_write` before releasing
    /// the shard write lock. Advanced extensions can use this to preserve
    /// same-shard mutation order without a second ordering mutex.
    #[doc(hidden)]
    pub fn set_value_bytes_routed_expire_at_then(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        value: bytes::Bytes,
        expire_at_ms: Option<u64>,
        now_ms: u64,
        after_write: impl FnOnce(),
    ) {
        self.set_value_bytes_routed_with_governance_then(
            route,
            key,
            value,
            None,
            expire_at_ms,
            now_ms,
            after_write,
        );
    }

    pub fn set_routed_no_ttl<K, V>(&self, route: EmbeddedKeyRoute, key: K, value: V)
    where
        K: Into<Bytes>,
        V: Into<Bytes>,
    {
        let key = key.into();
        let value = bytes::Bytes::from(value.into());
        self.set_value_bytes_routed_no_ttl_then(route, &key, value.clone(), || {
            self.notify_point_mutation(PointMutationKind::Set, &key, Some(value), None, None);
        });
    }

    pub fn set_slice_routed_no_ttl(&self, route: EmbeddedKeyRoute, key: &[u8], value: &[u8]) {
        let value = bytes::Bytes::copy_from_slice(value);
        self.set_value_bytes_routed_no_ttl_then(route, key, value.clone(), || {
            self.notify_point_mutation(PointMutationKind::Set, key, Some(value), None, None);
        });
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
        #[cfg(feature = "redis")]
        self.refresh_string_key_count(route.shard_id, &shard);
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
        #[cfg(feature = "redis")]
        self.refresh_string_key_count(route.shard_id, &shard);
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
        #[cfg(feature = "redis")]
        self.refresh_string_key_count(route.shard_id, &shard);
    }

    pub fn set_routed<K, V>(&self, route: EmbeddedKeyRoute, key: K, value: V, ttl_ms: Option<u64>)
    where
        K: Into<Bytes>,
        V: Into<Bytes>,
    {
        let now_ms = now_millis();
        let key = key.into();
        let value = bytes::Bytes::from(value.into());
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        self.set_value_bytes_routed_expire_at_then(
            route,
            &key,
            value.clone(),
            expire_at_ms,
            now_ms,
            || {
                self.notify_point_mutation(
                    PointMutationKind::Set,
                    &key,
                    Some(value),
                    expire_at_ms,
                    None,
                );
            },
        );
    }

    /// Inserts or replaces multiple byte-string values.
    ///
    /// `ttl_ms` applies the same relative TTL to every item in the batch.
    pub fn batch_set(&self, items: Vec<(Bytes, Bytes)>, ttl_ms: Option<u64>) -> bool {
        self.try_batch_set(items, ttl_ms)
    }

    /// Atomically validates a byte-string batch before applying any item.
    /// Returns `false` without mutating the store when any item is not valid
    /// for the configured point-mutation extension limits.
    pub fn try_batch_set(&self, items: Vec<(Bytes, Bytes)>, ttl_ms: Option<u64>) -> bool {
        if items.is_empty() {
            return true;
        }
        if !items
            .iter()
            .all(|(key, value)| self.point_mutation_is_accepted(key, value.len(), None))
        {
            return false;
        }

        let now_ms = now_millis();
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        let mut groups = vec![Vec::<(Bytes, bytes::Bytes, u64)>::new(); self.shards.len()];

        for (key, value) in items {
            let (route_hash, key_hash) = self.hashes_for_key(&key);
            groups[self.route_hash(route_hash)].push((key, value.into(), key_hash));
        }

        for (shard_id, batch) in groups.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            #[cfg(feature = "redis")]
            if self.objects.shard_has_objects(shard_id) {
                for (key, value, key_hash) in batch {
                    let observed_value = value.clone();
                    let mut bucket = self.objects.write_bucket(shard_id, key_hash);
                    let mut shard = self.shards[shard_id].write();
                    if bucket.delete_any(&key) {
                        self.objects.note_deleted(shard_id);
                    }
                    if let Some(session_prefix) = point_write_session_storage_prefix(&key) {
                        shard
                            .session_slots
                            .delete_hashed(&session_prefix, key_hash, &key);
                    }
                    shard
                        .map
                        .set_bytes_hashed(key_hash, &key, value, expire_at_ms, now_ms);
                    shard.enforce_memory_limit(now_ms);
                    self.refresh_string_key_count(shard_id, &shard);
                    self.notify_point_mutation(
                        PointMutationKind::Set,
                        &key,
                        Some(observed_value),
                        expire_at_ms,
                        None,
                    );
                }
                continue;
            }
            let mut shard = self.shards[shard_id].write();
            for (key, value, key_hash) in batch {
                let observed_value = value.clone();
                if let Some(session_prefix) = point_write_session_storage_prefix(&key) {
                    shard
                        .session_slots
                        .delete_hashed(&session_prefix, key_hash, &key);
                }
                shard
                    .map
                    .set_bytes_hashed(key_hash, &key, value, expire_at_ms, now_ms);
                self.notify_point_mutation(
                    PointMutationKind::Set,
                    &key,
                    Some(observed_value),
                    expire_at_ms,
                    None,
                );
            }
            shard.enforce_memory_limit(now_ms);
            #[cfg(feature = "redis")]
            self.refresh_string_key_count(shard_id, &shard);
        }
        true
    }
}
