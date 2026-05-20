use super::*;

impl<const SHARDS: usize> SharedEmbeddedStore<SHARDS> {
    /// Returns a borrowed value guard for `key`.
    #[inline(always)]
    pub fn get_ref(&self, key: &[u8]) -> Option<Ref<'_>> {
        let route = self.route_key(key);
        let guard = self.stripe(route.shard_id).read();
        let value = guard.point_ref_hashed(route.key_hash, key)? as *const [u8];
        Some(Ref {
            guard,
            value,
            _not_send: PhantomData,
        })
    }

    /// Returns a borrowed value guard for `key`.
    ///
    /// This is kept as the shared-handle convenience name. It is equivalent to
    /// [`Self::get_ref`]; unlike [`EmbeddedStore::get`](crate::storage::EmbeddedStore::get),
    /// it does not materialize a `Vec<u8>`.
    #[inline(always)]
    pub fn get(&self, key: &[u8]) -> Option<Ref<'_>> {
        self.get_ref(key)
    }

    /// Precomputes route and exact-match metadata for repeated shared point lookups.
    #[inline(always)]
    pub fn prepare_point_key(&self, key: &[u8]) -> PreparedPointKey {
        let route = self.route_key(key);
        PreparedPointKey {
            route,
            key_len: key.len(),
            key_tag: hash_key_tag_from_hash(route.key_hash),
            key: key.to_vec(),
        }
    }

    /// Returns a borrowed value guard for a prepared point key.
    ///
    /// Prepared keys must be created by a store with the same shard count and
    /// route mode.
    #[inline(always)]
    pub fn get_prepared_ref(&self, prepared: &PreparedPointKey) -> Option<Ref<'_>> {
        let guard = self.stripe(prepared.route().shard_id).read();
        let value = guard.point_ref_prepared(prepared)? as *const [u8];
        Some(Ref {
            guard,
            value,
            _not_send: PhantomData,
        })
    }

    /// Returns a refcount-only clone of the stored bytes for `key`.
    ///
    /// The shard read lock is released before the returned bytes are copied or
    /// inspected by the caller.
    #[inline(always)]
    pub fn get_value_bytes(&self, key: &[u8]) -> Option<SharedBytes> {
        let route = self.route_key(key);
        self.stripe(route.shard_id)
            .read()
            .point_value_bytes(route.key_hash, key)
    }

    /// Returns a refcount-only clone of the stored bytes for a prepared key.
    ///
    /// Prepared keys must be created by a store with the same shard count and
    /// route mode.
    #[inline(always)]
    pub fn get_prepared_value_bytes(&self, prepared: &PreparedPointKey) -> Option<SharedBytes> {
        self.stripe(prepared.route().shard_id)
            .read()
            .point_value_bytes_prepared(prepared)
    }

    /// Returns a mutable guard for `key`.
    #[inline(always)]
    pub fn get_mut(&self, key: &[u8]) -> Option<RefMut<'_>> {
        let route = self.route_key(key);
        let mut guard = self.stripe(route.shard_id).write();
        #[cfg(feature = "no-ttl")]
        let expire_at_ms = guard.entry_expire_at_hashed_no_ttl(route.key_hash, key)?;
        #[cfg(not(feature = "no-ttl"))]
        let expire_at_ms = guard.entry_expire_at_hashed(route.key_hash, key, ttl_now_millis())?;
        Some(RefMut {
            guard,
            route_mode: self.inner.route_mode,
            key: SharedBytes::copy_from_slice(key),
            key_hash: route.key_hash,
            expire_at_ms,
            _not_send: PhantomData,
        })
    }

    /// Returns true when `key` is present in point-key storage.
    #[inline(always)]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        let route = self.route_key(key);
        let guard = self.stripe(route.shard_id).read();
        guard.contains_point_hashed(route.key_hash, key)
    }

    /// Inserts or replaces a point-key value without a TTL.
    #[inline(always)]
    pub fn insert(&self, key: SharedBytes, value: SharedBytes) {
        let route = self.route_key(key.as_ref());
        self.stripe(route.shard_id)
            .write()
            .set_value_bytes_hashed_no_ttl(
                self.inner.route_mode,
                route.key_hash,
                key.as_ref(),
                value,
            );
    }

    /// Inserts or replaces a point-key value with an optional relative TTL.
    ///
    /// `ttl_ms` is measured from the current Unix time in milliseconds. Passing
    /// `None` keeps the no-TTL hot path.
    #[inline(always)]
    pub fn insert_with_ttl(&self, key: SharedBytes, value: SharedBytes, ttl_ms: Option<u64>) {
        #[cfg(feature = "no-ttl")]
        {
            assert!(
                ttl_ms.is_none(),
                "fast-cache/no-ttl builds do not support shared-store TTL writes"
            );
            self.insert(key, value);
        }
        #[cfg(not(feature = "no-ttl"))]
        {
            let Some(ttl_ms) = ttl_ms else {
                self.insert(key, value);
                return;
            };
            let now_ms = ttl_now_millis();
            let expire_at_ms = Some(now_ms.saturating_add(ttl_ms));
            let route = self.route_key(key.as_ref());
            self.stripe(route.shard_id).write().set_value_bytes_hashed(
                self.inner.route_mode,
                route.key_hash,
                key.as_ref(),
                value,
                expire_at_ms,
                now_ms,
            );
        }
    }

    /// Inserts or replaces a point-key value from borrowed byte slices.
    #[inline(always)]
    pub fn insert_slice(&self, key: &[u8], value: &[u8]) {
        let route = self.route_key(key);
        self.stripe(route.shard_id).write().set_slice_hashed_no_ttl(
            self.inner.route_mode,
            route.key_hash,
            key,
            value,
        );
    }

    /// Inserts or replaces a prepared point-key value from borrowed byte slices
    /// without a TTL.
    ///
    /// Prepared keys must be created by a store with the same shard count and
    /// route mode.
    #[inline(always)]
    pub fn insert_prepared_slice(&self, prepared: &PreparedPointKey, value: &[u8]) {
        self.stripe(prepared.route().shard_id)
            .write()
            .set_slice_hashed_no_ttl(
                self.inner.route_mode,
                prepared.route().key_hash,
                prepared.key(),
                value,
            );
    }

    /// Inserts or replaces a point-key value from borrowed bytes with an
    /// optional relative TTL.
    ///
    /// `ttl_ms` is measured from the current Unix time in milliseconds. Passing
    /// `None` keeps the no-TTL hot path.
    #[inline(always)]
    pub fn insert_slice_with_ttl(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) {
        #[cfg(feature = "no-ttl")]
        {
            assert!(
                ttl_ms.is_none(),
                "fast-cache/no-ttl builds do not support shared-store TTL writes"
            );
            self.insert_slice(key, value);
        }
        #[cfg(not(feature = "no-ttl"))]
        {
            let Some(ttl_ms) = ttl_ms else {
                self.insert_slice(key, value);
                return;
            };
            let now_ms = ttl_now_millis();
            let expire_at_ms = Some(now_ms.saturating_add(ttl_ms));
            let route = self.route_key(key);
            self.stripe(route.shard_id).write().set_slice_hashed(
                self.inner.route_mode,
                route.key_hash,
                key,
                value,
                expire_at_ms,
                now_ms,
            );
        }
    }

    /// Inserts or replaces a prepared point-key value from borrowed bytes with
    /// an optional relative TTL.
    ///
    /// Prepared keys must be created by a store with the same shard count and
    /// route mode.
    #[inline(always)]
    pub fn insert_prepared_slice_with_ttl(
        &self,
        prepared: &PreparedPointKey,
        value: &[u8],
        ttl_ms: Option<u64>,
    ) {
        #[cfg(feature = "no-ttl")]
        {
            assert!(
                ttl_ms.is_none(),
                "fast-cache/no-ttl builds do not support shared-store TTL writes"
            );
            self.insert_prepared_slice(prepared, value);
        }
        #[cfg(not(feature = "no-ttl"))]
        {
            let Some(ttl_ms) = ttl_ms else {
                self.insert_prepared_slice(prepared, value);
                return;
            };
            let now_ms = ttl_now_millis();
            let expire_at_ms = Some(now_ms.saturating_add(ttl_ms));
            self.stripe(prepared.route().shard_id)
                .write()
                .set_slice_hashed(
                    self.inner.route_mode,
                    prepared.route().key_hash,
                    prepared.key(),
                    value,
                    expire_at_ms,
                    now_ms,
                );
        }
    }

    /// Removes a point-key value and returns the stored bytes when present.
    #[inline(always)]
    pub fn remove(&self, key: &[u8]) -> Option<SharedBytes> {
        let route = self.route_key(key);
        #[cfg(feature = "no-ttl")]
        {
            self.stripe(route.shard_id)
                .write()
                .remove_value_hashed(route.key_hash, key, 0)
        }
        #[cfg(not(feature = "no-ttl"))]
        {
            self.stripe(route.shard_id).write().remove_value_hashed(
                route.key_hash,
                key,
                ttl_now_millis(),
            )
        }
    }

    /// Locks the routed stripe and returns an occupied or vacant entry.
    #[inline(always)]
    pub fn entry(&self, key: SharedBytes) -> Entry<'_> {
        let route = self.route_key(key.as_ref());
        let mut guard = self.stripe(route.shard_id).write();
        #[cfg(feature = "no-ttl")]
        let expire_at_ms = guard.entry_expire_at_hashed_no_ttl(route.key_hash, key.as_ref());
        #[cfg(not(feature = "no-ttl"))]
        let expire_at_ms =
            guard.entry_expire_at_hashed(route.key_hash, key.as_ref(), ttl_now_millis());
        if let Some(expire_at_ms) = expire_at_ms {
            Entry::Occupied(RefMut {
                guard,
                route_mode: self.inner.route_mode,
                key,
                key_hash: route.key_hash,
                expire_at_ms,
                _not_send: PhantomData,
            })
        } else {
            Entry::Vacant(VacantEntry {
                guard,
                route_mode: self.inner.route_mode,
                key,
                key_hash: route.key_hash,
                _not_send: PhantomData,
            })
        }
    }
}
