use super::*;

impl<const SHARDS: usize> SharedEmbeddedStore<SHARDS> {
    /// Returns a borrowed value guard for `key`.
    #[inline(always)]
    pub fn get_ref(&self, key: &[u8]) -> Option<Ref<'_>> {
        let route = self.route_key(key);
        let guard = self.stripe(route.shard_id).read();
        let value = if guard.has_no_ttl_entries() {
            guard.get_ref_hashed_shared_no_ttl(route.key_hash, key)?
        } else {
            guard.get_ref_hashed_shared(route.key_hash, key, ttl_now_millis())?
        } as *const [u8];
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

    /// Returns a mutable guard for `key`.
    #[inline(always)]
    pub fn get_mut(&self, key: &[u8]) -> Option<RefMut<'_>> {
        let route = self.route_key(key);
        let mut guard = self.stripe(route.shard_id).write();
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
        if guard.has_no_ttl_entries() {
            guard.contains_key_hashed_no_ttl(route.key_hash, key)
        } else {
            guard.contains_key_hashed(route.key_hash, key, ttl_now_millis())
        }
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

    /// Inserts or replaces a point-key value from borrowed bytes with an
    /// optional relative TTL.
    ///
    /// `ttl_ms` is measured from the current Unix time in milliseconds. Passing
    /// `None` keeps the no-TTL hot path.
    #[inline(always)]
    pub fn insert_slice_with_ttl(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) {
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

    /// Removes a point-key value and returns the stored bytes when present.
    #[inline(always)]
    pub fn remove(&self, key: &[u8]) -> Option<SharedBytes> {
        let route = self.route_key(key);
        self.stripe(route.shard_id).write().remove_value_hashed(
            route.key_hash,
            key,
            ttl_now_millis(),
        )
    }

    /// Locks the routed stripe and returns an occupied or vacant entry.
    #[inline(always)]
    pub fn entry(&self, key: SharedBytes) -> Entry<'_> {
        let route = self.route_key(key.as_ref());
        let mut guard = self.stripe(route.shard_id).write();
        if let Some(expire_at_ms) =
            guard.entry_expire_at_hashed(route.key_hash, key.as_ref(), ttl_now_millis())
        {
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
