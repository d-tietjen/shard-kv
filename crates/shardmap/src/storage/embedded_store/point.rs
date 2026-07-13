use super::*;

impl EmbeddedStore {
    /// Returns an owned, materialized copy of the value for `key`.
    ///
    /// This is the convenience read path for callers that need to keep the
    /// bytes after the cache borrow ends. Use [`Self::get_ref`] for a
    /// zero-copy borrowed read.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let now_ms = now_millis();
        let route = self.route_key(key);
        self.get_with_route(route, key, now_ms)
    }

    /// Returns a borrowed value guard for `key` without copying value bytes.
    ///
    /// The returned guard holds the routed shard lock for as long as the value
    /// slice is used. This is the preferred embedded read path when the caller
    /// only needs to inspect the value in place. Use [`Self::get`] when an
    /// owned `Vec<u8>` is required.
    pub fn get_ref(&self, key: &[u8]) -> Option<EmbeddedRef<'_>> {
        let route = self.route_key(key);
        self.get_ref_routed(route, key)
    }

    /// Returns a mutable point-key guard for `key`.
    ///
    /// The guard holds the routed shard write lock, so callers can inspect,
    /// replace, or remove the value without a second lookup. Replacements keep
    /// the entry's existing TTL.
    pub fn get_mut(&self, key: &[u8]) -> Option<EmbeddedRefMut<'_>> {
        let route = self.route_key(key);
        self.get_mut_routed(route, key, now_millis())
    }

    /// Single-threaded fused GET + RESP encode. Bypasses the per-shard
    /// `RwLock` entirely via `data_ptr()` — saves ~5-10ns per GET in atomic
    /// ops.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that no other thread is concurrently accessing
    /// any shard of this `EmbeddedStore`. This is true in the `sc=1`
    /// multi-direct path where there is exactly one worker thread, but is
    /// unsafe in any other configuration.
    pub unsafe fn get_blob_string_into_single_threaded(
        &self,
        key: &[u8],
        out: &mut bytes::BytesMut,
    ) -> bool {
        #[cfg(not(feature = "unsafe"))]
        {
            self.get_blob_string_into(key, out)
        }
        #[cfg(feature = "unsafe")]
        {
            let key_hash = hash_key(key);
            // SAFETY: forwarded from this function's single-threaded contract.
            unsafe { self.get_blob_string_hashed_into_single_threaded(key_hash, key, out) }
        }
    }

    /// Prehashed variant of `get_blob_string_into_single_threaded` for the
    /// RESP fast path, where the key bytes have already been validated.
    ///
    /// # Safety
    ///
    /// Same contract as `get_blob_string_into_single_threaded`.
    pub unsafe fn get_blob_string_hashed_into_single_threaded(
        &self,
        key_hash: u64,
        key: &[u8],
        out: &mut bytes::BytesMut,
    ) -> bool {
        #[cfg(not(feature = "unsafe"))]
        {
            self.get_blob_string_hashed_into(key_hash, key, out)
        }
        #[cfg(feature = "unsafe")]
        {
            if self.shards.len() == 1 {
                // SAFETY (per fn contract): no other thread is accessing this shard.
                let shard = unsafe { &*self.shards[0].data_ptr() };
                if can_skip_session_lookup(key, &shard.session_slots) {
                    let value = if shard.map.has_no_ttl_entries() {
                        shard.map.get_ref_hashed_shared_no_ttl(key_hash, key)
                    } else {
                        shard.map.get_ref_hashed_shared(key_hash, key, now_millis())
                    };
                    if let Some(value) = value {
                        write_resp_blob_string_into(out, value);
                        return true;
                    }
                }
            }

            let route = self.route_key(key);
            // SAFETY (per fn contract): no other thread is accessing this shard.
            let shard = unsafe { &*self.shards[route.shard_id].data_ptr() };
            // Skip `now_millis()` syscall when no TTL entries exist — the
            // expiration filter is a no-op anyway.
            let now_ms = if shard.map.has_no_ttl_entries() {
                0
            } else {
                now_millis()
            };
            if uses_flat_key_storage(self.route_mode, key)
                && let Some(value) = shard.map.get_ref_hashed_shared(route.key_hash, key, now_ms)
            {
                write_resp_blob_string_into(out, value);
                return true;
            }
            // Remote object references and session-prefixed keys need the
            // write-capable path so a cold value can fault back into memory.
            self.get_blob_string_into(key, out)
        }
    }

    /// Single-threaded SET that bypasses the per-shard `RwLock`.
    ///
    /// # Safety
    ///
    /// Same contract as `get_blob_string_into_single_threaded`.
    pub unsafe fn set_single_threaded(&self, key: &[u8], value: &[u8], ttl_ms: Option<u64>) {
        #[cfg(not(feature = "unsafe"))]
        {
            self.set(key.to_vec(), value.to_vec(), ttl_ms);
        }
        #[cfg(feature = "unsafe")]
        {
            let key_hash = hash_key(key);
            // SAFETY: forwarded from this function's single-threaded contract.
            unsafe { self.set_single_threaded_hashed(key_hash, key, value, ttl_ms) };
        }
    }

    /// Prehashed variant of `set_single_threaded` for canonical RESP SET.
    ///
    /// # Safety
    ///
    /// Same contract as `set_single_threaded`.
    pub unsafe fn set_single_threaded_hashed(
        &self,
        key_hash: u64,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
    ) {
        #[cfg(not(feature = "unsafe"))]
        {
            self.set_slice_prehashed(key_hash, key, value, ttl_ms);
        }
        #[cfg(feature = "unsafe")]
        {
            if self.shards.len() == 1 {
                // SAFETY (per fn contract): no other thread is accessing this shard.
                let shard = unsafe { &mut *self.shards[0].data_ptr() };
                if ttl_ms.is_none()
                    && self.route_mode == EmbeddedRouteMode::FullKey
                    && shard.memory_limit_bytes.is_none()
                    && shard.eviction_policy == EvictionPolicy::None
                {
                    // SAFETY: forwarded from this function's single-threaded
                    // contract. The direct 1-shard hot path uses borrowed response
                    // encoding, so stored value buffers are not cloned while SET
                    // mutates them.
                    unsafe { shard.map.set_slice_hashed_no_ttl_hot(key_hash, key, value) };
                    #[cfg(feature = "redis")]
                    self.refresh_string_key_count(0, shard);
                    return;
                }
                let now_ms = write_now_ms(ttl_ms, shard.memory_limit_bytes);
                let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
                if let Some(session_prefix) = point_write_session_storage_prefix(key) {
                    shard
                        .session_slots
                        .delete_hashed(&session_prefix, key_hash, key);
                }
                shard
                    .map
                    .set_slice_hashed(key_hash, key, value, expire_at_ms, now_ms);
                #[cfg(feature = "redis")]
                self.refresh_string_key_count(0, shard);
                return;
            }

            let route = self.route_key(key);
            // SAFETY (per fn contract): no other thread is accessing this shard.
            let shard = unsafe { &mut *self.shards[route.shard_id].data_ptr() };
            let now_ms = write_now_ms(ttl_ms, shard.memory_limit_bytes);
            let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
            if let Some(session_prefix) = point_write_session_storage_prefix(key) {
                shard
                    .session_slots
                    .delete_hashed(&session_prefix, route.key_hash, key);
            }
            shard
                .map
                .set_slice_hashed(route.key_hash, key, value, expire_at_ms, now_ms);
            #[cfg(feature = "redis")]
            self.refresh_string_key_count(route.shard_id, shard);
        }
    }

    /// SCNP single-shard SET path with both hashes supplied by the client.
    ///
    /// # Safety
    ///
    /// Same contract as `set_single_threaded_hashed`; additionally, `key_tag`
    /// must equal `hash_key_tag_from_hash(key_hash)`.
    #[inline(always)]
    pub unsafe fn set_single_threaded_hashed_tagged_no_ttl_hot(
        &self,
        key_hash: u64,
        key_tag: u64,
        key: &[u8],
        value: &[u8],
    ) {
        #[cfg(not(feature = "unsafe"))]
        {
            let _ = key_tag;
            self.set_slice_prehashed(key_hash, key, value, None);
        }
        #[cfg(feature = "unsafe")]
        {
            debug_assert_eq!(self.shards.len(), 1);
            debug_assert_eq!(self.route_mode, EmbeddedRouteMode::FullKey);
            // SAFETY (per fn contract): no other thread is accessing this shard.
            let shard = unsafe { &mut *self.shards[0].data_ptr() };
            debug_assert_eq!(shard.memory_limit_bytes, None);
            debug_assert_eq!(shard.eviction_policy, EvictionPolicy::None);
            // SAFETY: forwarded from this function's single-threaded contract,
            // with a caller-provided key tag matching the key bytes.
            unsafe {
                shard
                    .map
                    .set_slice_hashed_tagged_no_ttl_hot(key_hash, key_tag, key, value)
            };
            #[cfg(feature = "redis")]
            self.refresh_string_key_count(0, shard);
        }
    }

    /// Fused GET + RESP-blob-string-encode for the multi-direct hot path.
    /// Writes `$<len>\r\n<value>\r\n` directly into `out` under the shard read
    /// lock, eliminating the `Vec<u8>` intermediate that the generic `get`
    /// path's `.to_vec()` allocates. Returns `true` if the key existed.
    pub fn get_blob_string_into(&self, key: &[u8], out: &mut bytes::BytesMut) -> bool {
        let route = self.route_key(key);
        self.get_blob_string_routed_into(route, key, out)
    }

    /// Prehashed fused GET + RESP encode. Used by the RESP fast path after it
    /// validates the key bytes and computes the hash once.
    pub fn get_blob_string_hashed_into(
        &self,
        key_hash: u64,
        key: &[u8],
        out: &mut bytes::BytesMut,
    ) -> bool {
        if can_route_with_key_hash(self.route_mode, self.shards.len(), key) {
            let route = EmbeddedKeyRoute {
                shard_id: self.route_hash(key_hash),
                key_hash,
            };
            return self.get_blob_string_routed_into(route, key, out);
        }

        self.get_blob_string_into(key, out)
    }

    fn get_blob_string_routed_into(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        out: &mut bytes::BytesMut,
    ) -> bool {
        use bytes::BufMut;
        // Fast path: non-session keys answer from the read lock.
        if uses_flat_key_storage(self.route_mode, key) {
            let shard = self.shards[route.shard_id].read();
            // Skip now_millis when no TTL entries — saves the vDSO call.
            let now_ms = if shard.map.has_no_ttl_entries() {
                0
            } else {
                now_millis()
            };
            if let Some(value) = shard.map.get_ref_hashed_shared(route.key_hash, key, now_ms) {
                // Inline RESP blob string encode directly into out.
                write_resp_blob_string_into(out, value);
                return true;
            }
            drop(shard);
            if let Some(value) = self.get_value_bytes_routed(route, key, now_millis()) {
                write_resp_blob_string_into(out, &value);
                return true;
            }
            return false;
        }
        // Session-prefixed keys go through the slow path with a write lock.
        let mut shard = self.shards[route.shard_id].write();
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && let Some(value) =
                shard
                    .session_slots
                    .get_ref_hashed(&session_prefix, route.key_hash, key)
        {
            out.put_u8(b'$');
            let mut buf = itoa::Buffer::new();
            out.extend_from_slice(buf.format(value.len()).as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(value);
            out.extend_from_slice(b"\r\n");
            return true;
        }
        let now_ms_session = now_millis();
        if let Some(value) = shard
            .map
            .get_ref_hashed(route.key_hash, key, now_ms_session)
        {
            out.put_u8(b'$');
            let mut buf = itoa::Buffer::new();
            out.extend_from_slice(buf.format(value.len()).as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(value);
            out.extend_from_slice(b"\r\n");
            return true;
        }
        false
    }

    /// Prehashed slice SET for the RESP fast path. Avoids rehashing in
    /// storage after parsing has already identified the key.
    pub fn set_slice_prehashed(
        &self,
        key_hash: u64,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
    ) {
        if !can_route_with_key_hash(self.route_mode, self.shards.len(), key) {
            self.set(key.to_vec(), value.to_vec(), ttl_ms);
            return;
        }

        let route = EmbeddedKeyRoute {
            shard_id: self.route_hash(key_hash),
            key_hash,
        };
        #[cfg(feature = "redis")]
        if self.objects.shard_has_objects(route.shard_id) {
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            let mut shard = self.shards[route.shard_id].write();
            if bucket.delete_any(key) {
                self.objects.note_deleted(route.shard_id);
            }
            let now_ms = write_now_ms(ttl_ms, shard.memory_limit_bytes);
            let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
            if let Some(session_prefix) = point_write_session_storage_prefix(key) {
                shard
                    .session_slots
                    .delete_hashed(&session_prefix, route.key_hash, key);
            }
            shard
                .map
                .set_slice_hashed(route.key_hash, key, value, expire_at_ms, now_ms);
            shard.enforce_memory_limit(now_ms);
            self.refresh_string_key_count(route.shard_id, &shard);
            return;
        }
        let mut shard = self.shards[route.shard_id].write();
        let now_ms = write_now_ms(ttl_ms, shard.memory_limit_bytes);
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        if let Some(session_prefix) = point_write_session_storage_prefix(key) {
            shard
                .session_slots
                .delete_hashed(&session_prefix, route.key_hash, key);
        }
        shard
            .map
            .set_slice_hashed(route.key_hash, key, value, expire_at_ms, now_ms);
        shard.enforce_memory_limit(now_ms);
        #[cfg(feature = "redis")]
        self.refresh_string_key_count(route.shard_id, &shard);
    }

    /// Zero-copy `GET` for the multi-direct hot path. Returns the stored
    /// `bytes::Bytes` directly (refcount-only clone of `FlatEntry.value`),
    /// avoiding the `Vec<u8>` allocation that `get` performs to materialize
    /// `Bytes = Vec<u8>` for the public API.
    pub fn get_value_bytes(&self, key: &[u8]) -> Option<bytes::Bytes> {
        let now_ms = now_millis();
        let route = self.route_key(key);
        self.get_value_bytes_routed(route, key, now_ms)
    }

    /// FastCodec GET path. The wire header carries a route hash so the server
    /// can usually skip hashing the key again. For session-prefixed keys in
    /// session route mode, the route hash is intentionally the session prefix
    /// hash, not the full key hash, so we fall back to the generic lookup.
    pub fn get_value_bytes_route_hashed(
        &self,
        route_hash: u64,
        key: &[u8],
    ) -> Option<bytes::Bytes> {
        if can_use_route_hash_as_key_hash(self.route_mode, key) {
            let route = EmbeddedKeyRoute {
                shard_id: self.route_hash(route_hash),
                key_hash: route_hash,
            };
            return self.get_value_bytes_routed(route, key, now_millis());
        }

        self.get_value_bytes(key)
    }

    /// FastCodec GET path that calls `write` while the value reference is
    /// still protected by the shard read lock. This avoids the refcount bump
    /// and drop from returning `bytes::Bytes` on the native GET hot path.
    pub fn with_value_bytes_route_hashed<F>(
        &self,
        route_hash: u64,
        key: &[u8],
        mut write: F,
    ) -> bool
    where
        F: FnMut(&[u8]),
    {
        if can_use_route_hash_as_key_hash(self.route_mode, key) {
            let route = EmbeddedKeyRoute {
                shard_id: self.route_hash(route_hash),
                key_hash: route_hash,
            };
            return self.with_value_bytes_routed(route, key, &mut write);
        }

        if let Some(value) = self.get_value_bytes(key) {
            write(value.as_ref());
            true
        } else {
            false
        }
    }

    /// Route-hashed GET path that exposes the stored `Bytes` object while the
    /// shard lock is held. Callers can clone the `Bytes` handle for large
    /// zero-copy writes, while small responses can still copy from the borrowed
    /// value without an extra refcount bump.
    pub fn with_shared_value_bytes_route_hashed<F>(
        &self,
        route_hash: u64,
        key: &[u8],
        mut write: F,
    ) -> bool
    where
        F: FnMut(&bytes::Bytes),
    {
        if can_use_route_hash_as_key_hash(self.route_mode, key) {
            let route = EmbeddedKeyRoute {
                shard_id: self.route_hash(route_hash),
                key_hash: route_hash,
            };
            return self.with_shared_value_bytes_routed(route, key, &mut write);
        }

        if let Some(value) = self.get_value_bytes(key) {
            write(&value);
            true
        } else {
            false
        }
    }

    /// Full-key SCNP GET path for clients that provide the key hash, key tag,
    /// and key length but omit the key bytes. This is safe for concurrent
    /// multi-shard access because the value is exposed only while the shard
    /// read lock is held.
    pub fn with_shared_value_bytes_full_key_tagged_no_ttl<F>(
        &self,
        route_hash: u64,
        key_tag: u64,
        key_len: usize,
        mut write: F,
    ) -> bool
    where
        F: FnMut(&bytes::Bytes),
    {
        if self.route_mode != EmbeddedRouteMode::FullKey {
            return false;
        }
        let route = EmbeddedKeyRoute {
            shard_id: self.route_hash(route_hash),
            key_hash: route_hash,
        };
        let shard = self.shards[route.shard_id].read();
        if !shard.map.has_no_ttl_entries() {
            return false;
        }
        if let Some(value) =
            shard
                .map
                .get_shared_value_bytes_hashed_tagged_no_ttl(route.key_hash, key_tag, key_len)
        {
            write(value);
            true
        } else {
            false
        }
    }

    /// Native GETEX path for v2. Returns the value and applies the TTL while
    /// holding the target shard write lock.
    pub fn getex_value_bytes_route_hashed(
        &self,
        route_hash: Option<u64>,
        key: &[u8],
        ttl_ms: u64,
    ) -> Option<bytes::Bytes> {
        if let Some(key_hash) = route_hash
            && can_use_route_hash_as_key_hash(self.route_mode, key)
        {
            let route = EmbeddedKeyRoute {
                shard_id: self.route_hash(key_hash),
                key_hash,
            };
            return self.getex_value_bytes_routed(route, key, ttl_ms);
        }

        let route = self.route_key(key);
        self.getex_value_bytes_routed(route, key, ttl_ms)
    }

    /// Native GETEX path that writes the value while it is borrowed from the
    /// shard. This avoids the `Bytes` clone/drop pair in the returning GETEX
    /// helper.
    pub fn with_getex_value_bytes_route_hashed<F>(
        &self,
        route_hash: Option<u64>,
        key: &[u8],
        ttl_ms: u64,
        mut write: F,
    ) -> bool
    where
        F: FnMut(&[u8]),
    {
        if let Some(key_hash) = route_hash
            && can_use_route_hash_as_key_hash(self.route_mode, key)
        {
            let route = EmbeddedKeyRoute {
                shard_id: self.route_hash(key_hash),
                key_hash,
            };
            return self.with_getex_value_bytes_routed(route, key, ttl_ms, &mut write);
        }

        let route = self.route_key(key);
        self.with_getex_value_bytes_routed(route, key, ttl_ms, &mut write)
    }

    /// Single-threaded FastCodec GET path. Avoids shard `RwLock` atomics in
    /// the same direct sc=1 setup as the RESP fused single-threaded path.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that no other thread is concurrently accessing
    /// any shard of this `EmbeddedStore`.
    pub unsafe fn get_value_bytes_route_hashed_single_threaded(
        &self,
        route_hash: u64,
        key: &[u8],
    ) -> Option<bytes::Bytes> {
        #[cfg(not(feature = "unsafe"))]
        {
            self.get_value_bytes_route_hashed(route_hash, key)
        }
        #[cfg(feature = "unsafe")]
        {
            if can_use_route_hash_as_key_hash(self.route_mode, key) {
                if self.shards.len() == 1 {
                    // SAFETY (per fn contract): no other thread is accessing this shard.
                    let shard = unsafe { &*self.shards[0].data_ptr() };
                    if can_skip_session_lookup(key, &shard.session_slots) {
                        let now_ms = if shard.map.has_no_ttl_entries() {
                            0
                        } else {
                            now_millis()
                        };
                        return shard.map.get_value_bytes_hashed(route_hash, key, now_ms);
                    }
                }

                let route = EmbeddedKeyRoute {
                    shard_id: self.route_hash(route_hash),
                    key_hash: route_hash,
                };
                // SAFETY (per fn contract): no other thread is accessing this shard.
                let shard = unsafe { &*self.shards[route.shard_id].data_ptr() };
                if uses_flat_key_storage(self.route_mode, key) {
                    let now_ms = if shard.map.has_no_ttl_entries() {
                        0
                    } else {
                        now_millis()
                    };
                    return shard
                        .map
                        .get_value_bytes_hashed(route.key_hash, key, now_ms);
                }
            }

            self.get_value_bytes(key)
        }
    }

    /// Single-threaded variant of `with_value_bytes_route_hashed`.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that no other thread is concurrently accessing
    /// any shard of this `EmbeddedStore`.
    pub unsafe fn with_value_bytes_route_hashed_single_threaded<F>(
        &self,
        route_hash: u64,
        key: &[u8],
        write: F,
    ) -> bool
    where
        F: FnMut(&[u8]),
    {
        #[cfg(not(feature = "unsafe"))]
        {
            self.with_value_bytes_route_hashed(route_hash, key, write)
        }
        #[cfg(feature = "unsafe")]
        {
            let mut write = write;
            if can_use_route_hash_as_key_hash(self.route_mode, key) {
                if self.shards.len() == 1 {
                    // SAFETY (per fn contract): no other thread is accessing this shard.
                    let shard = unsafe { &*self.shards[0].data_ptr() };
                    if can_skip_session_lookup(key, &shard.session_slots) {
                        let value = if shard.map.has_no_ttl_entries() {
                            shard.map.get_ref_hashed_shared_no_ttl(route_hash, key)
                        } else {
                            shard
                                .map
                                .get_ref_hashed_shared(route_hash, key, now_millis())
                        };
                        if let Some(value) = value {
                            write(value);
                            return true;
                        }
                    }
                }

                let route = EmbeddedKeyRoute {
                    shard_id: self.route_hash(route_hash),
                    key_hash: route_hash,
                };
                // SAFETY (per fn contract): no other thread is accessing this shard.
                let shard = unsafe { &*self.shards[route.shard_id].data_ptr() };
                if uses_flat_key_storage(self.route_mode, key) {
                    let value = if shard.map.has_no_ttl_entries() {
                        shard.map.get_ref_hashed_shared_no_ttl(route.key_hash, key)
                    } else {
                        shard
                            .map
                            .get_ref_hashed_shared(route.key_hash, key, now_millis())
                    };
                    if let Some(value) = value {
                        write(value);
                        return true;
                    }
                }
            }

            self.with_value_bytes_route_hashed(route_hash, key, write)
        }
    }

    /// Single-threaded SCNP GET helper that exposes the stored `Bytes` object
    /// to the caller. This lets the network path clone only the refcount for
    /// large zero-copy writes, while keeping small values on the borrowed
    /// inline-copy path.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that no other thread is concurrently accessing
    /// any shard of this `EmbeddedStore`.
    pub unsafe fn with_shared_value_bytes_route_hashed_single_threaded<F>(
        &self,
        route_hash: u64,
        key: &[u8],
        write: F,
    ) -> bool
    where
        F: FnMut(&bytes::Bytes),
    {
        #[cfg(not(feature = "unsafe"))]
        {
            self.with_shared_value_bytes_route_hashed(route_hash, key, write)
        }
        #[cfg(feature = "unsafe")]
        {
            let mut write = write;
            if can_use_route_hash_as_key_hash(self.route_mode, key) {
                if self.shards.len() == 1 {
                    // SAFETY (per fn contract): no other thread is accessing this shard.
                    let shard = unsafe { &*self.shards[0].data_ptr() };
                    if can_skip_session_lookup(key, &shard.session_slots) {
                        if shard.map.has_no_ttl_entries() {
                            if shard
                                .map
                                .with_shared_value_bytes_hashed_no_ttl(route_hash, key, &mut write)
                            {
                                return true;
                            }
                        } else if shard.map.with_shared_value_bytes_hashed(
                            route_hash,
                            key,
                            now_millis(),
                            &mut write,
                        ) {
                            return true;
                        }
                    }
                }

                let route = EmbeddedKeyRoute {
                    shard_id: self.route_hash(route_hash),
                    key_hash: route_hash,
                };
                // SAFETY (per fn contract): no other thread is accessing this shard.
                let shard = unsafe { &*self.shards[route.shard_id].data_ptr() };
                if uses_flat_key_storage(self.route_mode, key) {
                    if shard.map.has_no_ttl_entries() {
                        if shard.map.with_shared_value_bytes_hashed_no_ttl(
                            route.key_hash,
                            key,
                            &mut write,
                        ) {
                            return true;
                        }
                    } else if shard.map.with_shared_value_bytes_hashed(
                        route.key_hash,
                        key,
                        now_millis(),
                        &mut write,
                    ) {
                        return true;
                    }
                }
            }

            if let Some(value) = self.get_value_bytes(key) {
                write(&value);
                true
            } else {
                false
            }
        }
    }

    /// 1-shard full-key SCNP GET helper for the hottest native path. This skips
    /// route-mode, shard-count, and session-slot checks that the general routed
    /// helper must keep.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that this store has exactly one shard, uses
    /// `EmbeddedRouteMode::FullKey`, and no other thread is concurrently
    /// accessing the shard.
    #[inline(always)]
    pub unsafe fn with_shared_value_bytes_full_key_single_threaded<F>(
        &self,
        key_hash: u64,
        key: &[u8],
        write: F,
    ) -> bool
    where
        F: FnMut(&bytes::Bytes),
    {
        #[cfg(not(feature = "unsafe"))]
        {
            self.with_shared_value_bytes_route_hashed(key_hash, key, write)
        }
        #[cfg(feature = "unsafe")]
        {
            let mut write = write;
            debug_assert_eq!(self.shards.len(), 1);
            debug_assert_eq!(self.route_mode, EmbeddedRouteMode::FullKey);

            // SAFETY (per fn contract): no other thread is accessing this shard.
            let shard = unsafe { &*self.shards[0].data_ptr() };
            let found = if shard.map.has_no_ttl_entries() {
                shard
                    .map
                    .with_shared_value_bytes_hashed_no_ttl(key_hash, key, &mut write)
            } else {
                shard
                    .map
                    .with_shared_value_bytes_hashed(key_hash, key, now_millis(), &mut write)
            };
            if found {
                return true;
            }
            if let Some(value) = self.get_value_bytes_route_hashed(key_hash, key) {
                write(&value);
                true
            } else {
                false
            }
        }
    }

    /// 1-shard full-key SCNP GET helper that returns the stored `Bytes`
    /// reference directly. This keeps the hottest native GET path out of the
    /// closure-based wrapper used by the general routed path.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that this store has exactly one shard, uses
    /// `EmbeddedRouteMode::FullKey`, and no other thread is concurrently
    /// accessing the shard.
    #[inline(always)]
    pub unsafe fn get_shared_value_bytes_full_key_single_threaded(
        &self,
        key_hash: u64,
        key: &[u8],
    ) -> Option<&bytes::Bytes> {
        #[cfg(not(feature = "unsafe"))]
        {
            let _ = (key_hash, key);
            None
        }
        #[cfg(feature = "unsafe")]
        {
            debug_assert_eq!(self.shards.len(), 1);
            debug_assert_eq!(self.route_mode, EmbeddedRouteMode::FullKey);

            // SAFETY (per fn contract): no other thread is accessing this shard.
            let shard = unsafe { &*self.shards[0].data_ptr() };
            if shard.map.has_no_ttl_entries() {
                return shard
                    .map
                    .get_shared_value_bytes_hashed_no_ttl(key_hash, key);
            }
            shard
                .map
                .get_shared_value_bytes_hashed(key_hash, key, now_millis())
        }
    }

    /// 1-shard full-key SCNP GET helper for clients that supply both the key
    /// hash and key tag. The native protocol treats that tuple plus key length
    /// as the lookup identity, avoiding a key-byte compare in the hottest path.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that this store has exactly one shard, uses
    /// `EmbeddedRouteMode::FullKey`, and no other thread is concurrently
    /// accessing the shard. `key_tag` must be the tag for the key bytes that
    /// produced `key_hash`.
    #[inline(always)]
    pub unsafe fn get_shared_value_bytes_full_key_tagged_single_threaded(
        &self,
        key_hash: u64,
        key_tag: u64,
        key_len: usize,
    ) -> Option<&bytes::Bytes> {
        #[cfg(not(feature = "unsafe"))]
        {
            let _ = (key_hash, key_tag, key_len);
            None
        }
        #[cfg(feature = "unsafe")]
        {
            debug_assert_eq!(self.shards.len(), 1);
            debug_assert_eq!(self.route_mode, EmbeddedRouteMode::FullKey);

            // SAFETY (per fn contract): no other thread is accessing this shard.
            let shard = unsafe { &*self.shards[0].data_ptr() };
            if shard.map.has_no_ttl_entries() {
                return shard
                    .map
                    .get_shared_value_bytes_hashed_tagged_no_ttl(key_hash, key_tag, key_len);
            }
            None
        }
    }

    /// Safe shard-owned full-key GET helper for direct-shard server workers.
    ///
    /// This keeps safe builds on the normal shard `RwLock`, but avoids the
    /// generic route/session fallback after the SCNP frame has already proven
    /// that the request belongs to `shard_id`.
    #[inline(always)]
    pub fn with_shared_value_bytes_full_key_owned_shard_no_ttl<F>(
        &self,
        shard_id: usize,
        key_hash: u64,
        key: &[u8],
        write: F,
    ) -> Option<bool>
    where
        F: FnMut(&bytes::Bytes),
    {
        if self.route_mode != EmbeddedRouteMode::FullKey
            || shard_id >= self.shards.len()
            || self.route_hash(key_hash) != shard_id
        {
            return None;
        }

        let shard = self.shards[shard_id].read();
        if !shard.map.has_no_ttl_entries() {
            return None;
        }

        let mut write = write;
        Some(
            shard
                .map
                .with_shared_value_bytes_hashed_no_ttl(key_hash, key, &mut write),
        )
    }

    /// Shard-owned full-key SCNP GET helper for direct-shard server workers.
    ///
    /// This is the multi-shard counterpart to
    /// `get_shared_value_bytes_full_key_tagged_single_threaded`: one server
    /// worker owns `shard_id`, the client supplies the full-key hash and tag,
    /// and the lookup can skip both the shard lock and the key-byte compare.
    ///
    /// Returns `None` when the hot path is not valid, such as TTL-bearing
    /// shards, invalid shard ids, non-full-key routing, or a route mismatch.
    /// Otherwise returns `Some(true)` for a hit and `Some(false)` for a miss.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that no other thread is concurrently accessing
    /// `shard_id`, that `key_hash` routes to `shard_id`, and that `key_tag`
    /// matches the key bytes that produced `key_hash`.
    #[inline(always)]
    pub unsafe fn with_shared_value_bytes_full_key_tagged_owned_shard_no_ttl<F>(
        &self,
        shard_id: usize,
        key_hash: u64,
        key_tag: u64,
        key_len: usize,
        write: F,
    ) -> Option<bool>
    where
        F: FnMut(&bytes::Bytes),
    {
        #[cfg(not(feature = "unsafe"))]
        {
            let _ = (shard_id, key_hash, key_tag, key_len, write);
            None
        }
        #[cfg(feature = "unsafe")]
        {
            if self.route_mode != EmbeddedRouteMode::FullKey
                || shard_id >= self.shards.len()
                || self.route_hash(key_hash) != shard_id
            {
                return None;
            }

            let mut write = write;
            // SAFETY (per fn contract): this direct worker owns `shard_id`.
            let shard = unsafe { &*self.shards[shard_id].data_ptr() };
            if !shard.map.has_no_ttl_entries() {
                return None;
            }
            if let Some(value) = shard
                .map
                .get_shared_value_bytes_hashed_tagged_no_ttl(key_hash, key_tag, key_len)
            {
                write(value);
                Some(true)
            } else {
                Some(false)
            }
        }
    }

    /// Shard-owned session-prefix GET helper for direct-shard server workers.
    ///
    /// Session slabs store borrowed value slices instead of `Bytes`, so this
    /// helper writes through a slice callback and avoids the generic fallback's
    /// `Bytes::copy_from_slice` materialization. The caller has already
    /// validated that `session_prefix` routes to `shard_id`; this helper only
    /// checks storage preconditions and performs the lookup.
    ///
    /// # Safety
    ///
    /// When built with the `unsafe` feature, the caller must guarantee no other
    /// thread is concurrently accessing `shard_id`. Safe builds keep the shard
    /// read lock and therefore only require the route metadata to be valid.
    #[inline(always)]
    pub unsafe fn with_session_value_slice_owned_shard_no_ttl_prevalidated<F>(
        &self,
        shard_id: usize,
        session_hash: u64,
        key_hash: u64,
        session_prefix: &[u8],
        key: &[u8],
        write: F,
    ) -> Option<bool>
    where
        F: FnMut(&[u8]),
    {
        if self.route_mode != EmbeddedRouteMode::SessionPrefix || shard_id >= self.shards.len() {
            return None;
        }
        let mut write = write;

        #[cfg(not(feature = "unsafe"))]
        {
            let shard = self.shards[shard_id].read();
            match shard.get_session_ref_hashed_shared_no_ttl_prehashed(
                session_hash,
                session_prefix,
                key_hash,
                key,
            ) {
                Some(value) => {
                    write(value);
                    Some(true)
                }
                None => Some(false),
            }
        }
        #[cfg(feature = "unsafe")]
        {
            // SAFETY (per fn contract): this direct worker owns `shard_id`.
            let shard = unsafe { &*self.shards[shard_id].data_ptr() };
            match shard.get_session_ref_hashed_shared_no_ttl_prehashed(
                session_hash,
                session_prefix,
                key_hash,
                key,
            ) {
                Some(value) => {
                    write(value);
                    Some(true)
                }
                None => Some(false),
            }
        }
    }

    /// Safe shard-owned no-TTL session SET helper for direct-shard workers.
    ///
    /// The caller has already validated that `session_prefix` routes to
    /// `shard_id`; this helper only enforces storage preconditions and performs
    /// the session-slab write.
    #[inline(always)]
    pub fn set_session_slice_hashed_owned_shard_no_ttl_prevalidated(
        &self,
        shard_id: usize,
        session_hash: u64,
        key_hash: u64,
        session_prefix: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> bool {
        if shard_id >= self.shards.len() {
            return false;
        }
        #[cfg(feature = "redis")]
        if self.objects.shard_has_objects(shard_id) {
            return false;
        }

        let mut shard = self.shards[shard_id].write();
        if shard.memory_limit_bytes.is_some() || shard.eviction_policy != EvictionPolicy::None {
            return false;
        }
        shard.set_session_slice_hashed_no_ttl_prehashed(
            session_hash,
            session_prefix,
            key_hash,
            key,
            value,
        );
        true
    }

    /// Shard-owned no-TTL session SET helper for direct-shard workers.
    ///
    /// # Safety
    ///
    /// The caller must guarantee exclusive access to `shard_id` and that the
    /// session prefix routes to that shard.
    #[inline(always)]
    pub unsafe fn set_session_slice_hashed_owned_shard_no_ttl_hot_prevalidated(
        &self,
        shard_id: usize,
        session_hash: u64,
        key_hash: u64,
        session_prefix: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> bool {
        #[cfg(not(feature = "unsafe"))]
        {
            let _ = (shard_id, session_hash, key_hash, session_prefix, key, value);
            false
        }
        #[cfg(feature = "unsafe")]
        {
            if shard_id >= self.shards.len() {
                return false;
            }
            #[cfg(feature = "redis")]
            if self.objects.shard_has_objects(shard_id) {
                return false;
            }

            // SAFETY (per fn contract): this direct worker owns `shard_id`.
            let shard = unsafe { &mut *self.shards[shard_id].data_ptr() };
            if shard.memory_limit_bytes.is_some() || shard.eviction_policy != EvictionPolicy::None {
                return false;
            }
            shard.set_session_slice_hashed_no_ttl_prehashed(
                session_hash,
                session_prefix,
                key_hash,
                key,
                value,
            );
            true
        }
    }

    /// Safe shard-owned no-TTL SET helper for direct-shard server workers.
    ///
    /// The helper still takes the shard write lock. It only specializes the
    /// routing and no-TTL storage path when the worker-owned shard checks have
    /// already succeeded; unsupported cases return `false` so callers can
    /// preserve the generic fallback behavior.
    #[inline(always)]
    pub fn set_slice_hashed_tagged_owned_shard_no_ttl(
        &self,
        shard_id: usize,
        key_hash: u64,
        key_tag: u64,
        key: &[u8],
        value: &[u8],
    ) -> bool {
        if shard_id >= self.shards.len() {
            return false;
        }
        #[cfg(feature = "redis")]
        if self.objects.shard_has_objects(shard_id) {
            return false;
        }

        let mut shard = self.shards[shard_id].write();
        if shard.memory_limit_bytes.is_some() || shard.eviction_policy != EvictionPolicy::None {
            return false;
        }
        match self.route_mode {
            EmbeddedRouteMode::FullKey => {
                if self.route_hash(key_hash) != shard_id
                    || point_write_session_storage_prefix(key).is_some()
                {
                    return false;
                }
                shard
                    .map
                    .set_slice_hashed_tagged_no_ttl_local(key_hash, key_tag, key, value);
            }
            EmbeddedRouteMode::SessionPrefix => {
                let Some(session_prefix) = point_write_session_storage_prefix(key) else {
                    return false;
                };
                let route = compute_key_route(self.route_mode, self.shift, key);
                if route.shard_id != shard_id || route.key_hash != key_hash {
                    return false;
                }
                shard.set_session_slice_hashed_no_ttl(&session_prefix, key_hash, key, value);
            }
        }
        #[cfg(feature = "redis")]
        self.refresh_string_key_count(shard_id, &shard);
        true
    }

    /// Shard-owned full-key SCNP SET helper for direct-shard server workers.
    ///
    /// # Safety
    ///
    /// The caller must guarantee exclusive access to `shard_id`, that
    /// `key_hash` routes to `shard_id`, and that `key_tag` matches `key`.
    #[inline(always)]
    pub unsafe fn set_slice_hashed_tagged_owned_shard_no_ttl_hot(
        &self,
        shard_id: usize,
        key_hash: u64,
        key_tag: u64,
        key: &[u8],
        value: &[u8],
    ) -> bool {
        #[cfg(not(feature = "unsafe"))]
        {
            let _ = (shard_id, key_hash, key_tag, key, value);
            false
        }
        #[cfg(feature = "unsafe")]
        {
            if shard_id >= self.shards.len() {
                return false;
            }
            #[cfg(feature = "redis")]
            if self.objects.shard_has_objects(shard_id) {
                return false;
            }

            // SAFETY (per fn contract): this direct worker owns `shard_id`.
            let shard = unsafe { &mut *self.shards[shard_id].data_ptr() };
            if shard.memory_limit_bytes.is_some() || shard.eviction_policy != EvictionPolicy::None {
                return false;
            }
            match self.route_mode {
                EmbeddedRouteMode::FullKey => {
                    if self.route_hash(key_hash) != shard_id
                        || point_write_session_storage_prefix(key).is_some()
                    {
                        return false;
                    }
                    // SAFETY: forwarded from this function's worker-local ownership contract,
                    // with a caller-provided key tag matching the key bytes.
                    unsafe {
                        shard
                            .map
                            .set_slice_hashed_tagged_no_ttl_hot(key_hash, key_tag, key, value)
                    };
                }
                EmbeddedRouteMode::SessionPrefix => {
                    let Some(session_prefix) = point_write_session_storage_prefix(key) else {
                        return false;
                    };
                    let route = compute_key_route(self.route_mode, self.shift, key);
                    if route.shard_id != shard_id || route.key_hash != key_hash {
                        return false;
                    }
                    shard.set_session_slice_hashed_no_ttl(&session_prefix, key_hash, key, value);
                }
            }
            #[cfg(feature = "redis")]
            self.refresh_string_key_count(shard_id, shard);
            true
        }
    }

    /// Single-threaded native GETEX path.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that no other thread is concurrently accessing
    /// any shard of this `EmbeddedStore`.
    pub unsafe fn getex_value_bytes_route_hashed_single_threaded(
        &self,
        route_hash: Option<u64>,
        key: &[u8],
        ttl_ms: u64,
    ) -> Option<bytes::Bytes> {
        #[cfg(not(feature = "unsafe"))]
        {
            self.getex_value_bytes_route_hashed(route_hash, key, ttl_ms)
        }
        #[cfg(feature = "unsafe")]
        {
            let now_ms = now_millis();
            let expire_at_ms = now_ms.saturating_add(ttl_ms);
            if let Some(key_hash) = route_hash
                && can_use_route_hash_as_key_hash(self.route_mode, key)
            {
                let shard_id = self.route_hash(key_hash);
                // SAFETY (per fn contract): no other thread is accessing this shard.
                let shard = unsafe { &mut *self.shards[shard_id].data_ptr() };
                if uses_flat_key_storage(self.route_mode, key) {
                    return shard.map.get_value_bytes_hashed_and_expire(
                        key_hash,
                        key,
                        expire_at_ms,
                        now_ms,
                    );
                }
            }

            self.getex_value_bytes_route_hashed(route_hash, key, ttl_ms)
        }
    }

    /// Single-threaded borrowed native GETEX path.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that no other thread is concurrently accessing
    /// any shard of this `EmbeddedStore`.
    pub unsafe fn with_getex_value_bytes_route_hashed_single_threaded<F>(
        &self,
        route_hash: Option<u64>,
        key: &[u8],
        ttl_ms: u64,
        write: F,
    ) -> bool
    where
        F: FnMut(&[u8]),
    {
        #[cfg(not(feature = "unsafe"))]
        {
            self.with_getex_value_bytes_route_hashed(route_hash, key, ttl_ms, write)
        }
        #[cfg(feature = "unsafe")]
        {
            let mut write = write;
            let now_ms = now_millis();
            let expire_at_ms = now_ms.saturating_add(ttl_ms);
            if let Some(key_hash) = route_hash
                && can_use_route_hash_as_key_hash(self.route_mode, key)
            {
                let shard_id = self.route_hash(key_hash);
                // SAFETY (per fn contract): no other thread is accessing this shard.
                let shard = unsafe { &mut *self.shards[shard_id].data_ptr() };
                if uses_flat_key_storage(self.route_mode, key) {
                    return shard.map.with_value_bytes_hashed_and_expire(
                        key_hash,
                        key,
                        expire_at_ms,
                        now_ms,
                        &mut write,
                    );
                }
            }

            self.with_getex_value_bytes_route_hashed(route_hash, key, ttl_ms, write)
        }
    }

    fn get_value_bytes_routed(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        now_ms: u64,
    ) -> Option<bytes::Bytes> {
        if uses_flat_key_storage(self.route_mode, key) {
            let shard = self.shards[route.shard_id].read();
            if let Some(value) = shard
                .map
                .get_value_bytes_hashed(route.key_hash, key, now_ms)
            {
                return Some(value);
            }
            drop(shard);
            let mut shard = self.shards[route.shard_id].write();
            if let Some(value) = shard
                .map
                .get_value_bytes_hashed(route.key_hash, key, now_ms)
            {
                return Some(value);
            }
            return shard.map.fault_remote_hashed(route.key_hash, key, now_ms);
        }
        // Session-prefixed keys still go through the legacy path (write lock,
        // value copied). Bench workload doesn't use session keys.
        let mut shard = self.shards[route.shard_id].write();
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && let Some(value) =
                shard
                    .session_slots
                    .get_ref_hashed(&session_prefix, route.key_hash, key)
        {
            return Some(bytes::Bytes::copy_from_slice(value));
        }
        shard
            .map
            .get_value_bytes_hashed(route.key_hash, key, now_ms)
    }

    fn with_value_bytes_routed<F>(&self, route: EmbeddedKeyRoute, key: &[u8], write: &mut F) -> bool
    where
        F: FnMut(&[u8]),
    {
        if uses_flat_key_storage(self.route_mode, key) {
            let shard = self.shards[route.shard_id].read();
            let value = if shard.map.has_no_ttl_entries() {
                shard.map.get_ref_hashed_shared_no_ttl(route.key_hash, key)
            } else {
                shard
                    .map
                    .get_ref_hashed_shared(route.key_hash, key, now_millis())
            };
            if let Some(value) = value {
                write(value);
                return true;
            }
            drop(shard);
        }

        if let Some(value) = self.get_value_bytes_routed(route, key, now_millis()) {
            write(value.as_ref());
            true
        } else {
            false
        }
    }

    pub(super) fn with_shared_value_bytes_routed<F>(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        write: &mut F,
    ) -> bool
    where
        F: FnMut(&bytes::Bytes),
    {
        if uses_flat_key_storage(self.route_mode, key) {
            let shard = self.shards[route.shard_id].read();
            let found = if shard.map.has_no_ttl_entries() {
                shard
                    .map
                    .with_shared_value_bytes_hashed_no_ttl(route.key_hash, key, write)
            } else {
                shard
                    .map
                    .with_shared_value_bytes_hashed(route.key_hash, key, now_millis(), write)
            };
            if found {
                return true;
            }
            drop(shard);
        }

        if let Some(value) = self.get_value_bytes_routed(route, key, now_millis()) {
            write(&value);
            true
        } else {
            false
        }
    }

    fn getex_value_bytes_routed(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        ttl_ms: u64,
    ) -> Option<bytes::Bytes> {
        let now_ms = now_millis();
        let expire_at_ms = now_ms.saturating_add(ttl_ms);
        if uses_flat_key_storage(self.route_mode, key) {
            let mut shard = self.shards[route.shard_id].write();
            return shard.map.get_value_bytes_hashed_and_expire(
                route.key_hash,
                key,
                expire_at_ms,
                now_ms,
            );
        }

        let value = self.get_value_bytes(key);
        if value.is_some() {
            self.expire(key, expire_at_ms);
        }
        value
    }

    fn with_getex_value_bytes_routed<F>(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        ttl_ms: u64,
        write: &mut F,
    ) -> bool
    where
        F: FnMut(&[u8]),
    {
        let now_ms = now_millis();
        let expire_at_ms = now_ms.saturating_add(ttl_ms);
        if uses_flat_key_storage(self.route_mode, key) {
            let mut shard = self.shards[route.shard_id].write();
            return shard.map.with_value_bytes_hashed_and_expire(
                route.key_hash,
                key,
                expire_at_ms,
                now_ms,
                write,
            );
        }

        if let Some(value) = self.get_value_bytes(key) {
            write(value.as_ref());
            self.expire(key, expire_at_ms);
            true
        } else {
            false
        }
    }

    fn get_with_route(&self, route: EmbeddedKeyRoute, key: &[u8], now_ms: u64) -> Option<Bytes> {
        // Owned reads use the write path so object-overflow entries can fault
        // back into resident memory when needed.
        if uses_flat_key_storage(self.route_mode, key) {
            let mut shard = self.shards[route.shard_id].write();
            return shard.map.get(key, now_ms);
        }

        // Session-prefixed keys still need the write lock because
        // `SessionSlotMap::get_ref_hashed` requires `&mut self`.
        let mut shard = self.shards[route.shard_id].write();
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && let Some(value) =
                shard
                    .session_slots
                    .get_ref_hashed(&session_prefix, route.key_hash, key)
        {
            return Some(value.to_vec());
        }
        shard
            .map
            .get_ref_hashed(route.key_hash, key, now_ms)
            .map(<[u8]>::to_vec)
    }

    pub(super) fn get_ref_routed(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
    ) -> Option<EmbeddedRef<'_>> {
        if uses_flat_key_storage(self.route_mode, key) {
            let guard = self.shards[route.shard_id].read();
            let value = if guard.map.has_no_ttl_entries() {
                guard
                    .map
                    .get_ref_hashed_shared_no_ttl(route.key_hash, key)?
            } else {
                guard
                    .map
                    .get_ref_hashed_shared(route.key_hash, key, now_millis())?
            } as *const [u8];
            return Some(EmbeddedRef {
                guard: views::EmbeddedRefGuard::Read(guard),
                value,
                _not_send: std::marker::PhantomData,
            });
        }

        let mut guard = self.shards[route.shard_id].write();
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && let Some(value) =
                guard
                    .session_slots
                    .get_ref_hashed(&session_prefix, route.key_hash, key)
        {
            let value = value as *const [u8];
            return Some(EmbeddedRef {
                guard: views::EmbeddedRefGuard::Write(guard),
                value,
                _not_send: std::marker::PhantomData,
            });
        }
        let value = match guard.map.has_no_ttl_entries().then_some(()) {
            Some(()) => guard.map.get_ref_hashed_no_ttl(route.key_hash, key)?,
            None => guard
                .map
                .get_ref_hashed(route.key_hash, key, now_millis())?,
        } as *const [u8];
        Some(EmbeddedRef {
            guard: views::EmbeddedRefGuard::Write(guard),
            value,
            _not_send: std::marker::PhantomData,
        })
    }

    fn get_mut_routed(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        now_ms: u64,
    ) -> Option<EmbeddedRefMut<'_>> {
        let mut guard = self.shards[route.shard_id].write();

        if !uses_flat_key_storage(self.route_mode, key)
            && let Some(session_prefix) = derived_session_storage_prefix(key)
            && guard.session_slots.has_session(&session_prefix)
        {
            return None;
        }

        let expire_at_ms = guard.entry_expire_at_hashed(route.key_hash, key, now_ms)?;
        Some(EmbeddedRefMut {
            guard,
            route_mode: self.route_mode,
            key: bytes::Bytes::copy_from_slice(key),
            key_hash: route.key_hash,
            expire_at_ms,
            _not_send: std::marker::PhantomData,
        })
    }

    /// Returns an owned read view for one key.
    ///
    /// The returned view owns the bytes it exposes, so callers can safely pass
    /// the metadata through FFI or buffer-style APIs without tying it to the
    /// store's lifetime.
    pub fn get_view(&self, key: &[u8]) -> EmbeddedReadView {
        let route = self.route_key(key);
        self.get_view_routed(route, key, now_millis())
    }

    #[inline(always)]
    pub fn get_prepared_view_no_ttl(&self, prepared: &PreparedPointKey) -> EmbeddedReadView {
        let mut shard = self.shards[prepared.route().shard_id].write();
        let item = shard
            .map
            .get_ref_hashed_prepared_no_ttl(
                prepared.route().key_hash,
                prepared.key(),
                prepared.key_tag(),
            )
            .map(EmbeddedReadSlice::from_slice);
        drop(shard);
        EmbeddedReadView { item }
    }

    pub fn get_view_routed_no_ttl(&self, route: EmbeddedKeyRoute, key: &[u8]) -> EmbeddedReadView {
        let mut shard = self.shards[route.shard_id].write();
        let item = if let Some(session_prefix) = derived_session_storage_prefix(key) {
            if shard.session_slots.has_session(&session_prefix) {
                shard
                    .session_slots
                    .get_ref_hashed(&session_prefix, route.key_hash, key)
                    .map(EmbeddedReadSlice::from_slice)
            } else {
                shard
                    .map
                    .get_ref_hashed_no_ttl(route.key_hash, key)
                    .map(EmbeddedReadSlice::from_slice)
            }
        } else {
            shard
                .map
                .get_ref_hashed_no_ttl(route.key_hash, key)
                .map(EmbeddedReadSlice::from_slice)
        };
        drop(shard);
        EmbeddedReadView { item }
    }

    pub fn get_view_routed(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        now_ms: u64,
    ) -> EmbeddedReadView {
        let mut shard = self.shards[route.shard_id].write();
        let item = if let Some(session_prefix) = derived_session_storage_prefix(key) {
            if shard.session_slots.has_session(&session_prefix) {
                shard
                    .session_slots
                    .get_ref_hashed(&session_prefix, route.key_hash, key)
                    .map(EmbeddedReadSlice::from_slice)
            } else {
                shard
                    .map
                    .get_ref_hashed(route.key_hash, key, now_ms)
                    .map(EmbeddedReadSlice::from_slice)
            }
        } else {
            shard
                .map
                .get_ref_hashed(route.key_hash, key, now_ms)
                .map(EmbeddedReadSlice::from_slice)
        };
        drop(shard);
        EmbeddedReadView { item }
    }
}
