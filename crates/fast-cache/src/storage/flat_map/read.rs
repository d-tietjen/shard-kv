use super::*;

impl FlatMap {
    #[inline(always)]
    pub fn get_ref_hashed_no_ttl(&mut self, hash: u64, key: &[u8]) -> Option<&[u8]> {
        self.lookup_ref_hashed_lazy(hash, key)
    }

    /// `&self` read path. Skips entry access tracking (LFU/LRU touch). Safe for
    /// any caller that does not depend on read-touch tracking — including the
    /// shared-store hot path under `RwLock::read`.
    #[inline(always)]
    pub fn get_ref_hashed_shared(&self, hash: u64, key: &[u8], now_ms: u64) -> Option<&[u8]> {
        // Skip the expiration filter entirely when no TTL entries exist — the
        // common case for caches that don't use SET ... EX. Saves a branch
        // and removes the `now_ms` dependency from the hot path.
        if self.ttl_entries == 0 {
            #[cfg(feature = "fast-point-map")]
            if let Some(value) = self.fast_points.get(hash, key) {
                return Some(value.as_ref());
            }
            return self
                .entries
                .find(hash, |entry| entry.matches_hashed_key(hash, key))
                .map(|entry| entry.value.as_ref());
        }
        self.entries
            .find(hash, |entry| entry.matches(hash, key))
            .filter(|entry| !entry.is_expired(now_ms))
            .map(|entry| entry.value.as_ref())
    }

    /// Returns true when there are no TTL'd entries — caller can skip a
    /// `now_millis()` call since no entries can expire.
    #[inline(always)]
    pub fn has_no_ttl_entries(&self) -> bool {
        self.ttl_entries == 0
    }

    #[inline(always)]
    pub fn get_ref_hashed_shared_no_ttl(&self, hash: u64, key: &[u8]) -> Option<&[u8]> {
        #[cfg(feature = "fast-point-map")]
        if let Some(value) = self.fast_points.get(hash, key) {
            return Some(value.as_ref());
        }
        self.entries
            .find(hash, |entry| entry.matches_hashed_key(hash, key))
            .map(|entry| entry.value.as_ref())
    }

    #[inline(always)]
    pub fn with_shared_value_bytes_hashed_no_ttl<F>(
        &self,
        hash: u64,
        key: &[u8],
        write: &mut F,
    ) -> bool
    where
        F: FnMut(&SharedBytes),
    {
        #[cfg(feature = "fast-point-map")]
        if let Some(value) = self.fast_points.get(hash, key) {
            write(value);
            return true;
        }
        if let Some(entry) = self
            .entries
            .find(hash, |entry| entry.matches_hashed_key(hash, key))
        {
            write(&entry.value);
            true
        } else {
            false
        }
    }

    #[inline(always)]
    pub fn get_shared_value_bytes_hashed_no_ttl(
        &self,
        hash: u64,
        key: &[u8],
    ) -> Option<&SharedBytes> {
        #[cfg(feature = "fast-point-map")]
        if let Some(value) = self.fast_points.get(hash, key) {
            return Some(value);
        }
        self.entries
            .find(hash, |entry| entry.matches_hashed_key(hash, key))
            .map(|entry| &entry.value)
    }

    #[inline(always)]
    pub fn get_shared_value_bytes_hashed_tagged_no_ttl(
        &self,
        hash: u64,
        key_tag: u64,
        key_len: usize,
    ) -> Option<&SharedBytes> {
        #[cfg(feature = "fast-point-map")]
        if let Some(value) = self.fast_points.get_tagged(hash, key_tag, key_len) {
            return Some(value);
        }
        self.entries
            .find(hash, |entry| entry.matches_tagged(hash, key_tag, key_len))
            .map(|entry| &entry.value)
    }

    #[inline(always)]
    pub fn with_shared_value_bytes_hashed<F>(
        &self,
        hash: u64,
        key: &[u8],
        now_ms: u64,
        write: &mut F,
    ) -> bool
    where
        F: FnMut(&SharedBytes),
    {
        if let Some(entry) = self
            .entries
            .find(hash, |entry| entry.matches(hash, key))
            .filter(|entry| !entry.is_expired(now_ms))
        {
            write(&entry.value);
            true
        } else {
            false
        }
    }

    #[inline(always)]
    pub fn get_shared_value_bytes_hashed(
        &self,
        hash: u64,
        key: &[u8],
        now_ms: u64,
    ) -> Option<&SharedBytes> {
        self.entries
            .find(hash, |entry| entry.matches(hash, key))
            .filter(|entry| !entry.is_expired(now_ms))
            .map(|entry| &entry.value)
    }

    /// Returns a refcount-only clone of the stored `bytes::Bytes`. Avoids the
    /// `Vec<u8>` allocation that `get_ref_hashed_shared` callers do via
    /// `to_vec`. Hot path for multi-direct GET.
    #[inline(always)]
    pub fn get_value_bytes_hashed(
        &self,
        hash: u64,
        key: &[u8],
        now_ms: u64,
    ) -> Option<SharedBytes> {
        #[cfg(feature = "fast-point-map")]
        if let Some(value) = self.fast_points.get(hash, key) {
            return Some(value.clone());
        }
        self.entries
            .find(hash, |entry| entry.matches(hash, key))
            .filter(|entry| !entry.is_expired(now_ms))
            .map(|entry| entry.value.clone())
    }

    #[inline(always)]
    pub fn get_value_bytes_hashed_prepared(
        &self,
        hash: u64,
        key: &[u8],
        key_tag: u64,
        now_ms: u64,
    ) -> Option<SharedBytes> {
        #[cfg(feature = "fast-point-map")]
        if let Some(value) = self.fast_points.get(hash, key) {
            return Some(value.clone());
        }
        self.entries
            .find(hash, |entry| entry.matches_prepared(hash, key, key_tag))
            .filter(|entry| !entry.is_expired(now_ms))
            .map(|entry| entry.value.clone())
    }

    /// Returns the current value and updates its expiration while holding the
    /// shard write lock. This is the native GETEX primitive.
    #[inline(always)]
    pub fn get_value_bytes_hashed_and_expire(
        &mut self,
        hash: u64,
        key: &[u8],
        expire_at_ms: u64,
        now_ms: u64,
    ) -> Option<SharedBytes> {
        self.disable_fast_point_map();

        let mut entry = self
            .entries
            .find_entry(hash, |entry| entry.matches_hashed_key(hash, key))
            .ok()?;
        if entry.get().is_expired(now_ms) {
            let _ = entry;
            self.delete_hashed(hash, key, now_ms);
            return None;
        }
        let had_ttl = entry.get().expire_at_ms.is_some();
        let value = entry.get().value.clone();
        entry.get_mut().expire_at_ms = Some(expire_at_ms);
        self.adjust_ttl_count(had_ttl, true);
        Some(value)
    }

    /// Calls `write` with the current value and updates its expiration while
    /// holding the shard write lock. This is the borrowed native GETEX path:
    /// it avoids the `Bytes` refcount clone/drop pair needed by
    /// `get_value_bytes_hashed_and_expire`.
    #[inline(always)]
    pub fn with_value_bytes_hashed_and_expire<F>(
        &mut self,
        hash: u64,
        key: &[u8],
        expire_at_ms: u64,
        now_ms: u64,
        write: &mut F,
    ) -> bool
    where
        F: FnMut(&[u8]),
    {
        self.disable_fast_point_map();

        let Some(mut entry) = self
            .entries
            .find_entry(hash, |entry| entry.matches_hashed_key(hash, key))
            .ok()
        else {
            return false;
        };
        if entry.get().is_expired(now_ms) {
            let _ = entry;
            self.delete_hashed(hash, key, now_ms);
            return false;
        }

        let had_ttl = entry.get().expire_at_ms.is_some();
        write(entry.get().value.as_ref());
        entry.get_mut().expire_at_ms = Some(expire_at_ms);
        self.adjust_ttl_count(had_ttl, true);
        true
    }

    #[inline(always)]
    pub fn get_ref_hashed_prepared_no_ttl(
        &mut self,
        hash: u64,
        key: &[u8],
        key_tag: u64,
    ) -> Option<&[u8]> {
        self.lookup_ref_hashed_prepared_lazy(hash, key, key_tag)
    }

    #[inline(always)]
    pub fn get_ref(&mut self, key: &[u8], now_ms: u64) -> Option<&[u8]> {
        self.get_ref_hashed(hash_key(key), key, now_ms)
    }

    #[inline(always)]
    pub fn get_ref_hashed(&mut self, hash: u64, key: &[u8], now_ms: u64) -> Option<&[u8]> {
        #[cfg(feature = "telemetry")]
        let start = self.telemetry.as_ref().map(|_| Instant::now());
        #[cfg(feature = "telemetry")]
        let telemetry = self.telemetry.clone();

        let value = if self.ttl_entries == 0 {
            self.lookup_ref_hashed_lazy(hash, key)
        } else if self.entry_is_expired_hashed(hash, key, now_ms) {
            let _ = self.delete_hashed_internal(hash, key, now_ms, DeleteReason::Expired);
            None
        } else {
            self.lookup_ref_hashed_lazy(hash, key)
        };

        #[cfg(feature = "telemetry")]
        if let (Some(telemetry), Some(start)) = (telemetry, start) {
            telemetry.metrics.record_get(
                telemetry.shard_id,
                value.is_some(),
                value.map_or(0, |bytes| bytes.len()),
                start.elapsed().as_nanos() as u64,
            );
        }

        value
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub fn get_ref_hashed_local(&mut self, hash: u64, key: &[u8], now_ms: u64) -> Option<&[u8]> {
        #[cfg(feature = "telemetry")]
        let start = self.telemetry.as_ref().map(|_| Instant::now());
        #[cfg(feature = "telemetry")]
        let telemetry = self.telemetry.clone();

        let value = if self.ttl_entries == 0 {
            self.lookup_ref_hashed_lazy(hash, key)
        } else if self.entry_is_expired_hashed(hash, key, now_ms) {
            let _ = self.delete_hashed_local_internal(hash, key, now_ms, DeleteReason::Expired);
            None
        } else {
            self.lookup_ref_hashed_lazy(hash, key)
        };

        #[cfg(feature = "telemetry")]
        if let (Some(telemetry), Some(start)) = (telemetry, start) {
            telemetry.metrics.record_get(
                telemetry.shard_id,
                value.is_some(),
                value.map_or(0, |bytes| bytes.len()),
                start.elapsed().as_nanos() as u64,
            );
        }

        value
    }

    pub fn get(&mut self, key: &[u8], now_ms: u64) -> Option<Bytes> {
        self.get_ref(key, now_ms).map(<[u8]>::to_vec)
    }

    pub fn exists(&mut self, key: &[u8], now_ms: u64) -> bool {
        self.disable_fast_point_map();
        let hash = hash_key(key);
        if self.ttl_entries != 0 && self.entry_is_expired_hashed(hash, key, now_ms) {
            let _ = self.delete_hashed_internal(hash, key, now_ms, DeleteReason::Expired);
            return false;
        }
        self.entries
            .find(hash, |entry| entry.matches(hash, key))
            .is_some()
    }

    /// Starts a shard-local read epoch.
    ///
    /// While at least one read epoch is active, value replacements and deletes
    /// retire old buffers instead of freeing them immediately. That keeps any
    /// zero-copy readers pointing at stable memory without introducing shared
    /// reference counting.
    #[inline(always)]
    pub fn begin_read_epoch(&self) {
        self.active_readers.fetch_add(1, Ordering::AcqRel);
    }

    /// Ends a shard-local read epoch and reclaims retired values once the last
    /// reader for this shard has exited. Reclamation itself stays on the owner
    /// thread and runs lazily from the write path.
    #[inline(always)]
    pub fn end_read_epoch(&self) {
        self.active_readers.fetch_sub(1, Ordering::AcqRel);
    }
}
