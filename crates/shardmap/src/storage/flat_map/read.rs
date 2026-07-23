use super::*;

impl FlatMap {
    #[inline(always)]
    pub fn get_ref_hashed_no_ttl(&mut self, hash: u64, key: &[u8]) -> Option<&[u8]> {
        self.lookup_ref_hashed_lazy(hash, key)
    }

    #[cfg(feature = "mutable-value-slices")]
    #[inline(always)]
    pub(crate) fn value_mut_hashed_no_ttl(&mut self, hash: u64, key: &[u8]) -> Option<&mut [u8]> {
        self.disable_fast_point_map();
        let should_touch_access = self.eviction_policy != EvictionPolicy::None;
        let access_tick = if should_touch_access {
            self.next_access_tick()
        } else {
            0
        };

        let can_mutate = self
            .entries
            .find(local_table_hash(hash), |entry| {
                entry.matches_hashed_key(hash, key)
            })
            .is_some_and(|entry| {
                !entry.is_protected() && entry.expire_at_ms.is_none() && entry.value.is_unique()
            });
        if !can_mutate {
            return None;
        }
        if should_touch_access {
            self.record_lru_touch(hash, access_tick);
        }
        let entry = self.entries.find_mut(local_table_hash(hash), |entry| {
            entry.matches_hashed_key(hash, key)
        })?;
        let previous_entry_bytes = entry.stored_bytes();
        if should_touch_access {
            entry.access.record_access(access_tick);
        }
        entry.clear_semantic_embedding();
        let new_entry_bytes = entry.stored_bytes();
        if previous_entry_bytes != new_entry_bytes {
            self.stored_bytes = self
                .stored_bytes
                .saturating_sub(previous_entry_bytes)
                .saturating_add(new_entry_bytes);
        }
        shared_bytes_as_unique_slice_mut(&mut entry.value)
    }

    #[cfg(feature = "redis")]
    #[inline(always)]
    pub(crate) fn update_value_hashed_no_ttl<R>(
        &mut self,
        hash: u64,
        key: &[u8],
        update: impl FnOnce(&mut [u8]) -> R,
    ) -> Option<R> {
        self.disable_fast_point_map();
        let should_touch_access = self.eviction_policy != EvictionPolicy::None;
        let access_tick = if should_touch_access {
            self.next_access_tick()
        } else {
            0
        };

        let can_mutate = self
            .entries
            .find(local_table_hash(hash), |entry| {
                entry.matches_hashed_key(hash, key)
            })
            .is_some_and(|entry| {
                !entry.is_protected() && entry.expire_at_ms.is_none() && entry.value.is_unique()
            });
        if !can_mutate {
            return None;
        }
        if should_touch_access {
            self.record_lru_touch(hash, access_tick);
        }
        let entry = self.entries.find_mut(local_table_hash(hash), |entry| {
            entry.matches_hashed_key(hash, key)
        })?;
        let previous_entry_bytes = entry.stored_bytes();
        if should_touch_access {
            entry.access.record_access(access_tick);
        }
        entry.clear_semantic_embedding();
        let new_entry_bytes = entry.stored_bytes();
        if previous_entry_bytes != new_entry_bytes {
            self.stored_bytes = self
                .stored_bytes
                .saturating_sub(previous_entry_bytes)
                .saturating_add(new_entry_bytes);
        }

        let value = mem::take(&mut entry.value);
        match value.try_into_mut() {
            Ok(mut writable) => {
                let result = update(&mut writable[..]);
                entry.value = writable.freeze();
                Some(result)
            }
            Err(value) => {
                entry.value = value;
                None
            }
        }
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
            #[cfg(feature = "experimental-no-ttl-point-hot-path")]
            if let Some(value) = self.fast_points.get(hash, key) {
                return Some(value.as_ref());
            }
            return self
                .entries
                .find(local_table_hash(hash), |entry| {
                    entry.matches_readable(hash, key)
                })
                .map(|entry| entry.value.as_ref());
        }
        self.entries
            .find(local_table_hash(hash), |entry| {
                entry.matches_readable(hash, key)
            })
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
    pub(crate) fn entry_expire_at_hashed(
        &mut self,
        hash: u64,
        key: &[u8],
        now_ms: u64,
    ) -> Option<Option<u64>> {
        if self.ttl_entries == 0 {
            #[cfg(feature = "experimental-no-ttl-point-hot-path")]
            if self.fast_points.get(hash, key).is_some() {
                return Some(None);
            }
            return self
                .entries
                .find(local_table_hash(hash), |entry| {
                    entry.matches_hashed_key(hash, key)
                })
                .map(|entry| entry.expire_at_ms);
        }
        if self.entry_is_expired_hashed(hash, key, now_ms) {
            let _ = self.delete_hashed_internal(hash, key, now_ms, DeleteReason::Expired);
            return None;
        }
        self.entries
            .find(local_table_hash(hash), |entry| entry.matches(hash, key))
            .map(|entry| entry.expire_at_ms)
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub(crate) fn entry_expire_at_hashed_no_ttl(
        &mut self,
        hash: u64,
        key: &[u8],
    ) -> Option<Option<u64>> {
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if self.fast_points.get(hash, key).is_some() {
            return Some(None);
        }
        self.entries
            .find(local_table_hash(hash), |entry| {
                entry.matches_hashed_key(hash, key)
            })
            .map(|_| None)
    }

    #[inline(always)]
    pub fn get_ref_hashed_shared_no_ttl(&self, hash: u64, key: &[u8]) -> Option<&[u8]> {
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if let Some(value) = self.fast_points.get(hash, key) {
            return Some(value.as_ref());
        }
        self.entries
            .find(local_table_hash(hash), |entry| {
                entry.matches_readable(hash, key)
            })
            .map(|entry| entry.value.as_ref())
    }

    #[inline(always)]
    pub fn get_ref_hashed_shared_prepared_no_ttl(
        &self,
        hash: u64,
        key: &[u8],
        key_tag: u64,
    ) -> Option<&[u8]> {
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if let Some(value) = self.fast_points.get(hash, key) {
            return Some(value.as_ref());
        }
        self.entries
            .find(local_table_hash(hash), |entry| {
                entry.matches_readable_prepared(hash, key, key_tag)
            })
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
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if let Some(value) = self.fast_points.get(hash, key) {
            write(value);
            return true;
        }
        if let Some(entry) = self.entries.find(local_table_hash(hash), |entry| {
            entry.matches_readable(hash, key)
        }) {
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
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if let Some(value) = self.fast_points.get(hash, key) {
            return Some(value);
        }
        self.entries
            .find(local_table_hash(hash), |entry| {
                entry.matches_readable(hash, key)
            })
            .map(|entry| &entry.value)
    }

    #[inline(always)]
    pub fn get_shared_value_bytes_hashed_prepared_no_ttl(
        &self,
        hash: u64,
        key: &[u8],
        key_tag: u64,
    ) -> Option<&SharedBytes> {
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if let Some(value) = self.fast_points.get(hash, key) {
            return Some(value);
        }
        self.entries
            .find(local_table_hash(hash), |entry| {
                entry.matches_readable_prepared(hash, key, key_tag)
            })
            .map(|entry| &entry.value)
    }

    #[inline(always)]
    pub fn get_shared_value_bytes_hashed_tagged_no_ttl(
        &self,
        hash: u64,
        key_tag: u64,
        key_len: usize,
    ) -> Option<&SharedBytes> {
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if let Some(value) = self.fast_points.get_tagged(hash, key_tag, key_len) {
            return Some(value);
        }
        self.entries
            .find(local_table_hash(hash), |entry| {
                entry.matches_readable_tagged(hash, key_tag, key_len)
            })
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
            .find(local_table_hash(hash), |entry| {
                entry.matches_readable(hash, key)
            })
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
            .find(local_table_hash(hash), |entry| {
                entry.matches_readable(hash, key)
            })
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
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if let Some(value) = self.fast_points.get(hash, key) {
            return Some(value.clone());
        }
        self.entries
            .find(local_table_hash(hash), |entry| {
                entry.matches_readable(hash, key)
            })
            .filter(|entry| !entry.is_expired(now_ms))
            .map(|entry| entry.value.clone())
    }

    /// Applies an authorization decision to borrowed policy metadata before
    /// cloning the stored value. The outcome distinguishes a denied existing
    /// entry from a miss so higher tiers never fall through after rejection.
    #[inline(always)]
    pub(crate) fn get_value_bytes_hashed_with_governance_filter<F>(
        &mut self,
        hash: u64,
        key: &[u8],
        now_ms: u64,
        authorize: F,
    ) -> GovernedRead<SharedBytes>
    where
        F: FnOnce(Option<&[u8]>) -> bool,
    {
        match self.prepare_governed_object_fault_hashed(hash, key, now_ms, authorize) {
            GovernedObjectFault::Missing => GovernedRead::Missing,
            GovernedObjectFault::Denied => GovernedRead::Denied,
            GovernedObjectFault::Resident(value) => GovernedRead::Authorized(value),
            GovernedObjectFault::Remote(plan) => {
                let result = plan.fetch();
                match self.finish_object_fault(*plan, result, now_ms) {
                    Ok(Some(value)) => GovernedRead::Authorized(value),
                    Ok(None) | Err(_) => GovernedRead::Missing,
                }
            }
        }
    }

    pub(crate) fn prepare_governed_object_fault_hashed<F>(
        &mut self,
        hash: u64,
        key: &[u8],
        now_ms: u64,
        authorize: F,
    ) -> GovernedObjectFault
    where
        F: FnOnce(Option<&[u8]>) -> bool,
    {
        self.process_object_overflow_completions(now_ms);
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if let Some(value) = self.fast_points.get(hash, key) {
            return if authorize(None) {
                GovernedObjectFault::Resident(value.clone())
            } else {
                GovernedObjectFault::Denied
            };
        }
        if let Some(entry) = self
            .entries
            .find(local_table_hash(hash), |entry| {
                entry.matches_hashed_key(hash, key)
            })
            .filter(|entry| !entry.is_expired(now_ms))
        {
            if !authorize(entry.governance.as_deref()) {
                return GovernedObjectFault::Denied;
            }
            return GovernedObjectFault::Resident(entry.value.clone());
        }

        let Some(remote) = self.remote_entries.get(key) else {
            return GovernedObjectFault::Missing;
        };
        if !remote.matches(hash, key) {
            return GovernedObjectFault::Missing;
        }
        if remote.is_expired(now_ms) {
            let _ = self.delete_remote_hashed(hash, key, DeleteReason::Expired);
            return GovernedObjectFault::Missing;
        }
        if !authorize(remote.governance.as_deref()) {
            return GovernedObjectFault::Denied;
        }
        self.prepare_object_fault_hashed(hash, key, now_ms, true)
            .map_or(GovernedObjectFault::Missing, |plan| {
                GovernedObjectFault::Remote(Box::new(plan))
            })
    }

    #[inline(always)]
    pub(crate) fn is_value_protected_hashed(&self, hash: u64, key: &[u8], now_ms: u64) -> bool {
        self.entries
            .find(local_table_hash(hash), |entry| {
                entry.matches_hashed_key(hash, key)
            })
            .filter(|entry| !entry.is_expired(now_ms))
            .is_some_and(FlatEntry::is_protected)
            || self.remote_entries.get(key).is_some_and(|entry| {
                entry.matches(hash, key) && !entry.is_expired(now_ms) && entry.is_protected()
            })
    }

    #[inline(always)]
    pub fn get_value_bytes_hashed_prepared(
        &self,
        hash: u64,
        key: &[u8],
        key_tag: u64,
        now_ms: u64,
    ) -> Option<SharedBytes> {
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if let Some(value) = self.fast_points.get(hash, key) {
            return Some(value.clone());
        }
        self.entries
            .find(local_table_hash(hash), |entry| {
                entry.matches_readable_prepared(hash, key, key_tag)
            })
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
            .find_entry(local_table_hash(hash), |entry| {
                entry.matches_readable(hash, key)
            })
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
            .find_entry(local_table_hash(hash), |entry| {
                entry.matches_readable(hash, key)
            })
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
        let start = self.start_telemetry_latency_sample();
        #[cfg(feature = "telemetry")]
        let telemetry = self
            .telemetry
            .as_ref()
            .map(|telemetry| (telemetry.metrics.clone(), telemetry.shard_id));

        let value = match self.ttl_entries {
            0 => self.lookup_ref_hashed_lazy(hash, key),
            _ if self.entry_is_expired_hashed(hash, key, now_ms) => {
                let _ = self.delete_hashed_internal(hash, key, now_ms, DeleteReason::Expired);
                None
            }
            _ => self.lookup_ref_hashed_lazy(hash, key),
        };

        #[cfg(feature = "telemetry")]
        if let Some((metrics, shard_id)) = telemetry {
            metrics.record_get(
                shard_id,
                value.is_some(),
                value.map_or(0, |bytes| bytes.len()),
                start.map(|start| metrics.latency_elapsed_ns_since(start)),
            );
        }

        value
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub fn get_ref_hashed_local(&mut self, hash: u64, key: &[u8], now_ms: u64) -> Option<&[u8]> {
        #[cfg(feature = "telemetry")]
        let start = self.start_telemetry_latency_sample();
        #[cfg(feature = "telemetry")]
        let telemetry = self
            .telemetry
            .as_ref()
            .map(|telemetry| (telemetry.metrics.clone(), telemetry.shard_id));

        let value = match self.ttl_entries {
            0 => self.lookup_ref_hashed_lazy(hash, key),
            _ if self.entry_is_expired_hashed(hash, key, now_ms) => {
                let _ = self.delete_hashed_local_internal(hash, key, now_ms, DeleteReason::Expired);
                None
            }
            _ => self.lookup_ref_hashed_lazy(hash, key),
        };

        #[cfg(feature = "telemetry")]
        if let Some((metrics, shard_id)) = telemetry {
            metrics.record_get(
                shard_id,
                value.is_some(),
                value.map_or(0, |bytes| bytes.len()),
                start.map(|start| metrics.latency_elapsed_ns_since(start)),
            );
        }

        value
    }

    pub fn get(&mut self, key: &[u8], now_ms: u64) -> Option<Bytes> {
        let hash = hash_key(key);
        if let Some(value) = self.get_ref_hashed(hash, key, now_ms) {
            return Some(value.to_vec());
        }
        let plan = self.prepare_object_fault_hashed(hash, key, now_ms, false)?;
        let result = plan.fetch();
        self.finish_object_fault(plan, result, now_ms)
            .ok()
            .flatten()
            .map(|value| value.as_ref().to_vec())
    }

    pub fn exists(&mut self, key: &[u8], now_ms: u64) -> bool {
        self.disable_fast_point_map();
        let hash = hash_key(key);
        if self.ttl_entries != 0 && self.entry_is_expired_hashed(hash, key, now_ms) {
            let _ = self.delete_hashed_internal(hash, key, now_ms, DeleteReason::Expired);
            return false;
        }
        self.entries
            .find(local_table_hash(hash), |entry| {
                entry.matches_readable(hash, key)
            })
            .is_some()
            || self
                .remote_entries
                .get(key)
                .is_some_and(|entry| entry.matches(hash, key) && !entry.is_protected())
    }

    pub(crate) fn prepare_object_fault_hashed(
        &mut self,
        hash: u64,
        key: &[u8],
        now_ms: u64,
        allow_protected: bool,
    ) -> Option<ObjectFaultPlan> {
        self.process_object_overflow_completions(now_ms);
        let object_overflow = self.object_overflow.clone()?;
        if !object_overflow.fetch_on_get() {
            return None;
        }
        let remote = self.remote_entries.get(key)?;
        if !remote.matches(hash, key) {
            return None;
        }
        if remote.is_protected() && !allow_protected {
            return None;
        }
        if remote.is_expired(now_ms) {
            let _ = self.delete_remote_hashed(hash, key, DeleteReason::Expired);
            return None;
        }
        if !self.can_start_object_fault(remote.object.len) {
            return None;
        }

        self.object_overflow_stats.fault_attempts =
            self.object_overflow_stats.fault_attempts.saturating_add(1);
        let object = remote.object.clone();
        let remote_key = remote.key.clone();
        self.pending_object_fault_bytes =
            self.pending_object_fault_bytes.saturating_add(object.len);
        self.pending_object_faults = self.pending_object_faults.saturating_add(1);
        Some(ObjectFaultPlan {
            hash,
            key: remote_key,
            reserved_bytes: object.len,
            object,
            runtime: object_overflow,
        })
    }

    pub(super) fn can_start_object_fault(&self, value_len: usize) -> bool {
        if self.pending_object_fault_bytes == 0 {
            return true;
        }
        let limit = self
            .memory_limit_bytes
            .unwrap_or(MAX_UNBOUNDED_PENDING_FAULT_BYTES);
        self.pending_object_fault_bytes
            .checked_add(value_len)
            .is_some_and(|total| total <= limit)
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
