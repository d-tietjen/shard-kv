use super::*;
use crate::Result;

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

    /// Returns an exact point value only when `authorize` accepts its borrowed
    /// opaque governance metadata. Rejection is indistinguishable from a miss
    /// and occurs before the value handle is cloned.
    pub fn get_value_bytes_with_governance_filter<F>(
        &self,
        key: &[u8],
        authorize: F,
    ) -> Option<SharedBytes>
    where
        F: FnOnce(Option<&[u8]>) -> bool,
    {
        let route = self.route_key(key);
        let now_ms = if cfg!(feature = "no-ttl") {
            0
        } else {
            ttl_now_millis()
        };
        match self
            .stripe(route.shard_id)
            .write()
            .get_value_bytes_hashed_with_governance_filter(route.key_hash, key, now_ms, authorize)
        {
            GovernedRead::Authorized(value) => Some(value),
            GovernedRead::Missing | GovernedRead::Denied => None,
        }
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
        if guard.is_value_protected_hashed(route.key_hash, key, ttl_now_millis()) {
            return None;
        }
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
            semantic_generation: &self.inner.semantic_generation,
            semantic_shadow: self.semantic_shadow(route),
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
        {
            self.stripe(route.shard_id)
                .write()
                .set_value_bytes_hashed_no_ttl(
                    self.inner.route_mode,
                    route.key_hash,
                    key.as_ref(),
                    value,
                );
        }
        self.invalidate_semantic_shadow(route, key.as_ref(), 0);
        self.bump_semantic_generation_if_active();
    }

    pub(super) fn insert_point_shadow(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        value: &[u8],
        expire_at_ms: Option<u64>,
        now_ms: u64,
    ) {
        if route.shard_id == self.semantic_shard_id() {
            return;
        }
        self.stripe(route.shard_id).write().set_slice_hashed(
            self.inner.route_mode,
            route.key_hash,
            key,
            value,
            expire_at_ms,
            now_ms,
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
                "shardcache/no-ttl builds do not support shared-store TTL writes"
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
            self.disable_semantic_query_cache();
            {
                self.stripe(route.shard_id).write().set_value_bytes_hashed(
                    self.inner.route_mode,
                    route.key_hash,
                    key.as_ref(),
                    value,
                    expire_at_ms,
                    now_ms,
                );
            }
            self.invalidate_semantic_shadow(route, key.as_ref(), now_ms);
            self.bump_semantic_generation_if_active();
        }
    }

    /// Atomically inserts or replaces a protected point value, TTL, and opaque
    /// governance metadata.
    pub fn insert_with_ttl_and_governance(
        &self,
        key: SharedBytes,
        value: SharedBytes,
        ttl_ms: Option<u64>,
        governance: SharedBytes,
    ) {
        #[cfg(feature = "no-ttl")]
        assert!(
            ttl_ms.is_none(),
            "shardcache/no-ttl builds do not support shared-store TTL writes"
        );
        let now_ms = ttl_ms.map_or(0, |_| ttl_now_millis());
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        let route = self.route_key(key.as_ref());
        {
            self.stripe(route.shard_id)
                .write()
                .set_value_bytes_hashed_with_governance(
                    self.inner.route_mode,
                    route.key_hash,
                    key.as_ref(),
                    value,
                    governance,
                    expire_at_ms,
                    now_ms,
                );
        }
        self.invalidate_semantic_shadow(route, key.as_ref(), now_ms);
        self.bump_semantic_generation_if_active();
    }

    /// Inserts a point-key value only when the key is absent or expired.
    #[inline(always)]
    pub fn insert_if_absent(&self, key: SharedBytes, value: SharedBytes) -> bool {
        let route = self.route_key(key.as_ref());
        let mut guard = self.stripe(route.shard_id).write();
        #[cfg(feature = "no-ttl")]
        let exists = guard
            .entry_expire_at_hashed_no_ttl(route.key_hash, key.as_ref())
            .is_some();
        #[cfg(not(feature = "no-ttl"))]
        let exists = guard
            .entry_expire_at_hashed(route.key_hash, key.as_ref(), ttl_now_millis())
            .is_some();
        if exists {
            return false;
        }
        guard.set_value_bytes_hashed_no_ttl(
            self.inner.route_mode,
            route.key_hash,
            key.as_ref(),
            value,
        );
        drop(guard);
        self.invalidate_semantic_shadow(route, key.as_ref(), 0);
        self.bump_semantic_generation_if_active();
        true
    }

    /// Inserts or replaces a point-key value from borrowed byte slices.
    #[inline(always)]
    pub fn insert_slice(&self, key: &[u8], value: &[u8]) {
        let route = self.route_key(key);
        {
            self.stripe(route.shard_id).write().set_slice_hashed_no_ttl(
                self.inner.route_mode,
                route.key_hash,
                key,
                value,
            );
        }
        self.invalidate_semantic_shadow(route, key, 0);
        self.bump_semantic_generation_if_active();
    }

    /// Inserts a borrowed point-key value only when the key is absent or expired.
    #[inline(always)]
    pub fn insert_slice_if_absent(&self, key: &[u8], value: &[u8]) -> bool {
        let route = self.route_key(key);
        let mut guard = self.stripe(route.shard_id).write();
        #[cfg(feature = "no-ttl")]
        let exists = guard
            .entry_expire_at_hashed_no_ttl(route.key_hash, key)
            .is_some();
        #[cfg(not(feature = "no-ttl"))]
        let exists = guard
            .entry_expire_at_hashed(route.key_hash, key, ttl_now_millis())
            .is_some();
        if exists {
            return false;
        }
        guard.set_slice_hashed_no_ttl(self.inner.route_mode, route.key_hash, key, value);
        drop(guard);
        self.invalidate_semantic_shadow(route, key, 0);
        self.bump_semantic_generation_if_active();
        true
    }

    /// Inserts or replaces a prepared point-key value from borrowed byte slices
    /// without a TTL.
    ///
    /// Prepared keys must be created by a store with the same shard count and
    /// route mode.
    #[inline(always)]
    pub fn insert_prepared_slice(&self, prepared: &PreparedPointKey, value: &[u8]) {
        {
            self.stripe(prepared.route().shard_id)
                .write()
                .set_slice_hashed_no_ttl(
                    self.inner.route_mode,
                    prepared.route().key_hash,
                    prepared.key(),
                    value,
                );
        }
        self.invalidate_semantic_shadow(prepared.route(), prepared.key(), 0);
        self.bump_semantic_generation_if_active();
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
                "shardcache/no-ttl builds do not support shared-store TTL writes"
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
            self.disable_semantic_query_cache();
            {
                self.stripe(route.shard_id).write().set_slice_hashed(
                    self.inner.route_mode,
                    route.key_hash,
                    key,
                    value,
                    expire_at_ms,
                    now_ms,
                );
            }
            self.invalidate_semantic_shadow(route, key, now_ms);
            self.bump_semantic_generation_if_active();
        }
    }

    /// Inserts a borrowed point-key value with an optional TTL only when the
    /// key is absent or expired.
    #[inline(always)]
    pub fn insert_slice_if_absent_with_ttl(
        &self,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
    ) -> bool {
        #[cfg(feature = "no-ttl")]
        {
            assert!(
                ttl_ms.is_none(),
                "shardcache/no-ttl builds do not support shared-store TTL writes"
            );
            self.insert_slice_if_absent(key, value)
        }
        #[cfg(not(feature = "no-ttl"))]
        {
            let Some(ttl_ms) = ttl_ms else {
                return self.insert_slice_if_absent(key, value);
            };
            let now_ms = ttl_now_millis();
            let route = self.route_key(key);
            let mut guard = self.stripe(route.shard_id).write();
            if guard
                .entry_expire_at_hashed(route.key_hash, key, now_ms)
                .is_some()
            {
                return false;
            }
            self.disable_semantic_query_cache();
            guard.set_slice_hashed(
                self.inner.route_mode,
                route.key_hash,
                key,
                value,
                Some(now_ms.saturating_add(ttl_ms)),
                now_ms,
            );
            drop(guard);
            self.invalidate_semantic_shadow(route, key, now_ms);
            self.bump_semantic_generation_if_active();
            true
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
                "shardcache/no-ttl builds do not support shared-store TTL writes"
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
            self.disable_semantic_query_cache();
            {
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
            self.invalidate_semantic_shadow(prepared.route(), prepared.key(), now_ms);
            self.bump_semantic_generation_if_active();
        }
    }

    /// Removes a point-key value and returns the stored bytes when present.
    #[inline(always)]
    pub fn remove(&self, key: &[u8]) -> Option<SharedBytes> {
        let route = self.route_key(key);
        #[cfg(feature = "no-ttl")]
        {
            let mut guard = self.stripe(route.shard_id).write();
            let protected = guard.is_value_protected_hashed(route.key_hash, key, 0);
            let removed = guard.remove_value_hashed(route.key_hash, key, 0);
            drop(guard);
            let semantic_removed = self.invalidate_semantic_shadow(route, key, 0);
            if removed.is_some() || semantic_removed.is_some() {
                self.bump_semantic_generation_if_active();
            }
            (!protected).then(|| removed.or(semantic_removed)).flatten()
        }
        #[cfg(not(feature = "no-ttl"))]
        {
            let now_ms = ttl_now_millis();
            let mut guard = self.stripe(route.shard_id).write();
            let protected = guard.is_value_protected_hashed(route.key_hash, key, now_ms);
            let removed = guard.remove_value_hashed(route.key_hash, key, now_ms);
            drop(guard);
            let semantic_removed = self.invalidate_semantic_shadow(route, key, now_ms);
            if removed.is_some() || semantic_removed.is_some() {
                self.bump_semantic_generation_if_active();
            }
            (!protected).then(|| removed.or(semantic_removed)).flatten()
        }
    }

    /// Removes a point-key value only when the stored bytes match `expected`.
    #[inline(always)]
    pub fn remove_if_value_eq(&self, key: &[u8], expected: &[u8]) -> bool {
        let route = self.route_key(key);
        let mut guard = self.stripe(route.shard_id).write();
        #[cfg(feature = "no-ttl")]
        let (matches, now_ms) = (
            guard
                .get_ref_hashed_shared_no_ttl(route.key_hash, key)
                .is_some_and(|value| value == expected),
            0,
        );
        #[cfg(not(feature = "no-ttl"))]
        let (matches, now_ms) = {
            let now_ms = ttl_now_millis();
            let matches = guard
                .entry_expire_at_hashed(route.key_hash, key, now_ms)
                .is_some()
                && guard
                    .get_ref_hashed_shared(route.key_hash, key, now_ms)
                    .is_some_and(|value| value == expected);
            (matches, now_ms)
        };
        if !matches {
            return false;
        }
        guard.remove_value_hashed(route.key_hash, key, now_ms);
        drop(guard);
        self.invalidate_semantic_shadow(route, key, now_ms);
        self.bump_semantic_generation_if_active();
        true
    }

    /// Updates a point-key TTL only when the stored bytes match `expected`.
    #[inline(always)]
    pub fn update_ttl_if_value_eq(&self, key: &[u8], expected: &[u8], ttl_ms: u64) -> Result<bool> {
        #[cfg(feature = "no-ttl")]
        {
            let _ = (key, expected, ttl_ms);
            Err(crate::ShardCacheError::Config(
                "shardcache/no-ttl builds do not support shared-store TTL writes".into(),
            ))
        }
        #[cfg(not(feature = "no-ttl"))]
        {
            let now_ms = ttl_now_millis();
            let route = self.route_key(key);
            let mut guard = self.stripe(route.shard_id).write();
            let matches = guard
                .entry_expire_at_hashed(route.key_hash, key, now_ms)
                .is_some()
                && guard
                    .get_ref_hashed_shared(route.key_hash, key, now_ms)
                    .is_some_and(|value| value == expected);
            if !matches {
                return Ok(false);
            }
            self.disable_semantic_query_cache();
            guard.set_slice_hashed(
                self.inner.route_mode,
                route.key_hash,
                key,
                expected,
                Some(now_ms.saturating_add(ttl_ms)),
                now_ms,
            );
            drop(guard);
            self.invalidate_semantic_shadow(route, key, now_ms);
            self.bump_semantic_generation_if_active();
            Ok(true)
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
                semantic_generation: &self.inner.semantic_generation,
                semantic_shadow: self.semantic_shadow(route),
                _not_send: PhantomData,
            })
        } else {
            Entry::Vacant(VacantEntry {
                guard,
                route_mode: self.inner.route_mode,
                key,
                key_hash: route.key_hash,
                semantic_generation: &self.inner.semantic_generation,
                semantic_shadow: self.semantic_shadow(route),
                _not_send: PhantomData,
            })
        }
    }
}
