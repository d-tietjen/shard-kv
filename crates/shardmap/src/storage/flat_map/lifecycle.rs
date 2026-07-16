use super::*;

impl FlatMap {
    pub fn delete(&mut self, key: &[u8], now_ms: u64) -> bool {
        self.delete_hashed_internal(hash_key(key), key, now_ms, DeleteReason::Explicit)
    }

    pub fn delete_hashed(&mut self, hash: u64, key: &[u8], _now_ms: u64) -> bool {
        self.delete_hashed_internal(hash, key, _now_ms, DeleteReason::Explicit)
    }

    pub(crate) fn remove_value_hashed(
        &mut self,
        hash: u64,
        key: &[u8],
        now_ms: u64,
    ) -> Option<SharedBytes> {
        self.disable_fast_point_map();
        self.reclaim_retired_if_quiescent();
        let Some(entry) = self
            .entries
            .find_entry(hash, |entry| entry.matches(hash, key))
            .ok()
        else {
            return self.remove_remote_value_hashed(hash, key, now_ms);
        };
        if entry.get().is_expired(now_ms) {
            let _ = entry;
            let _ = self.delete_hashed_internal(hash, key, now_ms, DeleteReason::Expired);
            return None;
        }

        let protected = entry.get().is_protected();
        let removed_bytes = entry.get().stored_bytes();
        if entry.get().expire_at_ms.is_some() {
            self.ttl_entries = self.ttl_entries.saturating_sub(1);
        }
        let (removed, _) = entry.remove();
        self.stored_bytes = self.stored_bytes.saturating_sub(removed_bytes);
        #[cfg(feature = "telemetry")]
        self.record_delete_metrics(DeleteReason::Explicit, -1, -(removed_bytes as isize));
        if protected {
            self.retire_value(removed.value);
            None
        } else {
            Some(removed.value)
        }
    }

    #[cfg(feature = "embedded")]
    pub fn delete_hashed_local(&mut self, hash: u64, key: &[u8], now_ms: u64) -> bool {
        self.delete_hashed_local_internal(hash, key, now_ms, DeleteReason::Explicit)
    }

    pub(super) fn delete_hashed_internal(
        &mut self,
        hash: u64,
        key: &[u8],
        _now_ms: u64,
        #[cfg_attr(not(feature = "telemetry"), allow(unused_variables))] reason: DeleteReason,
    ) -> bool {
        self.disable_fast_point_map();
        self.reclaim_retired_if_quiescent();
        let Some(entry) = self
            .entries
            .find_entry(hash, |entry| entry.matches(hash, key))
            .ok()
        else {
            return self.delete_remote_hashed(hash, key, reason);
        };

        let removed_bytes = entry.get().stored_bytes();
        if entry.get().expire_at_ms.is_some() {
            self.ttl_entries = self.ttl_entries.saturating_sub(1);
        }
        let (removed, _) = entry.remove();
        self.stored_bytes = self.stored_bytes.saturating_sub(removed_bytes);
        self.retire_value(removed.value);
        if reason == DeleteReason::Evicted {
            self.evictions = self.evictions.saturating_add(1);
        }
        #[cfg(feature = "telemetry")]
        self.record_delete_metrics(reason, -1, -(removed_bytes as isize));
        true
    }

    #[cfg(feature = "embedded")]
    pub(super) fn delete_hashed_local_internal(
        &mut self,
        hash: u64,
        key: &[u8],
        _now_ms: u64,
        #[cfg_attr(not(feature = "telemetry"), allow(unused_variables))] reason: DeleteReason,
    ) -> bool {
        self.disable_fast_point_map();
        let Some(entry) = self
            .entries
            .find_entry(hash, |entry| entry.matches(hash, key))
            .ok()
        else {
            return self.delete_remote_hashed(hash, key, reason);
        };

        let removed_bytes = entry.get().stored_bytes();
        if entry.get().expire_at_ms.is_some() {
            self.ttl_entries = self.ttl_entries.saturating_sub(1);
        }
        let (removed, _) = entry.remove();
        drop(removed);
        self.stored_bytes = self.stored_bytes.saturating_sub(removed_bytes);
        if reason == DeleteReason::Evicted {
            self.evictions = self.evictions.saturating_add(1);
        }
        #[cfg(feature = "telemetry")]
        self.record_delete_metrics(reason, -1, -(removed_bytes as isize));
        true
    }

    pub(super) fn evict_or_offload_hashed(&mut self, hash: u64, key: &[u8], now_ms: u64) -> bool {
        match self.try_offload_hashed(hash, key, now_ms) {
            ObjectOffloadAttempt::Offloaded => true,
            ObjectOffloadAttempt::NotEligible | ObjectOffloadAttempt::FailedEvictResident => {
                self.delete_hashed_internal(hash, key, now_ms, DeleteReason::Evicted)
            }
            ObjectOffloadAttempt::HotRetainResident
            | ObjectOffloadAttempt::FailedRetainResident => false,
        }
    }

    fn try_offload_hashed(&mut self, hash: u64, key: &[u8], now_ms: u64) -> ObjectOffloadAttempt {
        let Some(object_overflow) = self.object_overflow.clone() else {
            return ObjectOffloadAttempt::NotEligible;
        };
        let Some(entry_ref) = self
            .entries
            .find(hash, |entry| entry.matches(hash, key))
            .filter(|entry| !entry.is_expired(now_ms))
        else {
            return ObjectOffloadAttempt::NotEligible;
        };
        let value_len = entry_ref.value.len();
        if !object_overflow.should_offload(value_len) {
            return ObjectOffloadAttempt::NotEligible;
        }
        if !object_overflow.should_offload_cold_entry(
            value_len,
            entry_ref.access.last_touch,
            entry_ref.access.frequency,
            self.access_clock,
        ) {
            self.object_overflow_stats.offload_hot_skips = self
                .object_overflow_stats
                .offload_hot_skips
                .saturating_add(1);
            return ObjectOffloadAttempt::HotRetainResident;
        }

        self.object_overflow_stats.offload_attempts = self
            .object_overflow_stats
            .offload_attempts
            .saturating_add(1);
        let object_key = object_overflow.object_key(
            self.object_overflow_shard_id,
            hash,
            self.object_overflow_sequence.wrapping_add(1),
        );
        let object = match object_overflow.put_value(&object_key, entry_ref.value.as_ref()) {
            Ok(object) => object,
            Err(_) => {
                self.object_overflow_stats.offload_failures = self
                    .object_overflow_stats
                    .offload_failures
                    .saturating_add(1);
                return match object_overflow.failure_policy() {
                    ObjectOverflowFailurePolicy::RetainResident => {
                        ObjectOffloadAttempt::FailedRetainResident
                    }
                    ObjectOverflowFailurePolicy::EvictResident => {
                        ObjectOffloadAttempt::FailedEvictResident
                    }
                };
            }
        };
        self.object_overflow_sequence = self.object_overflow_sequence.wrapping_add(1);

        let Some(entry) = self
            .entries
            .find_entry(hash, |entry| entry.matches(hash, key))
            .ok()
        else {
            return ObjectOffloadAttempt::NotEligible;
        };
        let removed_bytes = entry.get().stored_bytes();
        let (removed, _) = entry.remove();
        let remote = RemoteEntry {
            hash: removed.hash,
            key_len: removed.key_len,
            key: removed.key.clone(),
            object,
            expire_at_ms: removed.expire_at_ms,
            governance: removed.governance,
        };
        let remote_bytes = remote.stored_bytes();
        self.stored_bytes = self
            .stored_bytes
            .saturating_sub(removed_bytes)
            .saturating_add(remote_bytes);
        self.remote_value_bytes = self.remote_value_bytes.saturating_add(removed.value.len());
        self.retire_value(removed.value);
        self.remote_entries
            .insert(remote.key.as_ref().to_vec(), remote);
        self.evictions = self.evictions.saturating_add(1);
        self.object_overflow_stats.offload_successes = self
            .object_overflow_stats
            .offload_successes
            .saturating_add(1);
        ObjectOffloadAttempt::Offloaded
    }

    #[inline]
    pub(super) fn delete_remote_hashed(
        &mut self,
        hash: u64,
        key: &[u8],
        reason: DeleteReason,
    ) -> bool {
        if self.remote_entries.is_empty() {
            return false;
        }
        let Some(remote) = self.remote_entries.remove(key) else {
            return false;
        };
        if !remote.matches(hash, key) {
            self.remote_entries
                .insert(remote.key.as_ref().to_vec(), remote);
            return false;
        }

        self.stored_bytes = self.stored_bytes.saturating_sub(remote.stored_bytes());
        self.remote_value_bytes = self.remote_value_bytes.saturating_sub(remote.object.len);
        if remote.expire_at_ms.is_some() {
            self.ttl_entries = self.ttl_entries.saturating_sub(1);
        }
        if reason == DeleteReason::Evicted {
            self.evictions = self.evictions.saturating_add(1);
        }
        self.delete_remote_object(&remote.object);
        #[cfg(feature = "telemetry")]
        self.record_delete_metrics(reason, -1, -(remote.stored_bytes() as isize));
        true
    }

    fn remove_remote_value_hashed(
        &mut self,
        hash: u64,
        key: &[u8],
        now_ms: u64,
    ) -> Option<SharedBytes> {
        let remote = self.remote_entries.get(key)?;
        if !remote.matches(hash, key) {
            return None;
        }
        if remote.is_expired(now_ms) {
            let _ = self.delete_remote_hashed(hash, key, DeleteReason::Expired);
            return None;
        }
        if remote.is_protected() {
            let _ = self.delete_remote_hashed(hash, key, DeleteReason::Explicit);
            return None;
        }
        let bytes = self
            .object_overflow
            .as_ref()?
            .get_value(&remote.object)
            .ok()?;
        let _ = self.delete_remote_hashed(hash, key, DeleteReason::Explicit);
        Some(bytes)
    }

    pub(super) fn delete_remote_object(&mut self, object: &ObjectValueRef) {
        let Some(object_overflow) = self.object_overflow.clone() else {
            return;
        };
        if !object_overflow.delete_on_overwrite() {
            return;
        }
        self.object_overflow_stats.remote_delete_attempts = self
            .object_overflow_stats
            .remote_delete_attempts
            .saturating_add(1);
        if object_overflow.delete_value(&object.object_key).is_err() {
            self.object_overflow_stats.remote_delete_failures = self
                .object_overflow_stats
                .remote_delete_failures
                .saturating_add(1);
        }
    }

    pub fn ttl_seconds(&mut self, key: &[u8], now_ms: u64) -> i64 {
        self.disable_fast_point_map();
        let hash = hash_key(key);
        let Some(entry) = self.entries.find(hash, |entry| entry.matches(hash, key)) else {
            return self.remote_ttl(hash, key, now_ms, true);
        };
        let Some(expire_at_ms) = entry.expire_at_ms else {
            return -1;
        };
        if expire_at_ms <= now_ms {
            self.delete_hashed_internal(hash, key, now_ms, DeleteReason::Expired);
            return -2;
        }
        expire_at_ms.saturating_sub(now_ms).div_ceil(1_000) as i64
    }

    pub fn ttl_millis(&mut self, key: &[u8], now_ms: u64) -> i64 {
        self.disable_fast_point_map();
        let hash = hash_key(key);
        let Some(entry) = self.entries.find(hash, |entry| entry.matches(hash, key)) else {
            return self.remote_ttl(hash, key, now_ms, false);
        };
        let Some(expire_at_ms) = entry.expire_at_ms else {
            return -1;
        };
        if expire_at_ms <= now_ms {
            self.delete_hashed_internal(hash, key, now_ms, DeleteReason::Expired);
            return -2;
        }
        expire_at_ms.saturating_sub(now_ms) as i64
    }

    fn remote_ttl(&mut self, hash: u64, key: &[u8], now_ms: u64, seconds: bool) -> i64 {
        let Some(remote) = self.remote_entries.get(key) else {
            return -2;
        };
        if !remote.matches(hash, key) {
            return -2;
        }
        match remote.expire_at_ms {
            Some(expire_at_ms) if expire_at_ms <= now_ms => {
                let _ = self.delete_remote_hashed(hash, key, DeleteReason::Expired);
                -2
            }
            Some(expire_at_ms) if seconds => {
                expire_at_ms.saturating_sub(now_ms).div_ceil(1_000) as i64
            }
            Some(expire_at_ms) => expire_at_ms.saturating_sub(now_ms) as i64,
            None => -1,
        }
    }

    pub fn persist(&mut self, key: &[u8], now_ms: u64) -> bool {
        self.disable_fast_point_map();
        let hash = hash_key(key);
        if self.entry_is_expired_hashed(hash, key, now_ms) {
            self.delete_hashed(hash, key, now_ms);
            return false;
        }

        let Some(mut entry) = self
            .entries
            .find_entry(hash, |entry| entry.matches(hash, key))
            .ok()
        else {
            let Some(remote) = self.remote_entries.get_mut(key) else {
                return false;
            };
            if !remote.matches(hash, key) {
                return false;
            }
            if remote.expire_at_ms.is_none() {
                return false;
            }
            remote.expire_at_ms = None;
            self.adjust_ttl_count(true, false);
            return true;
        };
        if entry.get().expire_at_ms.is_none() {
            return false;
        }
        entry.get_mut().expire_at_ms = None;
        self.adjust_ttl_count(true, false);
        true
    }

    pub fn expire(&mut self, key: &[u8], expire_at_ms: u64, now_ms: u64) -> bool {
        self.disable_fast_point_map();
        let hash = hash_key(key);
        if self.entry_is_expired_hashed(hash, key, now_ms) {
            self.delete_hashed(hash, key, now_ms);
            return false;
        }

        let Some(mut entry) = self
            .entries
            .find_entry(hash, |entry| entry.matches(hash, key))
            .ok()
        else {
            let Some(remote) = self.remote_entries.get_mut(key) else {
                return false;
            };
            if !remote.matches(hash, key) {
                return false;
            }
            let had_ttl = remote.expire_at_ms.is_some();
            remote.expire_at_ms = Some(expire_at_ms);
            self.adjust_ttl_count(had_ttl, true);
            return true;
        };
        let had_ttl = entry.get().expire_at_ms.is_some();
        entry.get_mut().expire_at_ms = Some(expire_at_ms);
        self.adjust_ttl_count(had_ttl, true);
        true
    }

    pub fn snapshot_entries(&self, now_ms: u64) -> Vec<StoredEntry> {
        self.try_snapshot_entries(now_ms).unwrap_or_default()
    }

    pub fn try_snapshot_entries(&self, now_ms: u64) -> crate::Result<Vec<StoredEntry>> {
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if self.fast_points.is_active() {
            return Ok(self.fast_points.snapshot_entries());
        }
        let mut entries = self
            .entries
            .iter()
            .filter(|entry| !entry.is_expired(now_ms))
            .map(|entry| StoredEntry {
                key: entry.key.as_ref().to_vec(),
                value: entry.value.as_ref().to_vec(),
                expire_at_ms: entry.expire_at_ms,
                governance: entry.governance.as_deref().map(<[u8]>::to_vec),
            })
            .collect::<Vec<_>>();
        for entry in self
            .remote_entries
            .values()
            .filter(|entry| !entry.is_expired(now_ms))
        {
            let object_overflow = self.object_overflow.as_ref().ok_or_else(|| {
                ShardCacheError::Persistence(
                    "object overflow remote entry exists without runtime".into(),
                )
            })?;
            let value = object_overflow.get_value(&entry.object)?;
            entries.push(StoredEntry {
                key: entry.key.as_ref().to_vec(),
                value: value.as_ref().to_vec(),
                expire_at_ms: entry.expire_at_ms,
                governance: entry.governance.as_deref().map(<[u8]>::to_vec),
            });
        }
        Ok(entries)
    }

    pub fn snapshot_keys(&self, now_ms: u64) -> Vec<Bytes> {
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if self.fast_points.is_active() {
            return self.fast_points.snapshot_keys();
        }
        self.entries
            .iter()
            .filter(|entry| !entry.is_expired(now_ms) && !entry.is_protected())
            .map(|entry| entry.key.as_ref().to_vec())
            .chain(
                self.remote_entries
                    .values()
                    .filter(|entry| !entry.is_expired(now_ms) && !entry.is_protected())
                    .map(|entry| entry.key.as_ref().to_vec()),
            )
            .collect()
    }

    #[cfg(feature = "redis")]
    pub(crate) fn scan_keys_visit(
        &self,
        offset: usize,
        limit: usize,
        now_ms: u64,
        visited: &mut usize,
        emitted: &mut usize,
        visit: &mut impl FnMut(&[u8]) -> bool,
    ) -> Option<usize> {
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if self.fast_points.is_active() {
            return self
                .fast_points
                .scan_keys_visit(offset, limit, visited, emitted, visit);
        }

        for (index, entry) in self.entries.iter().enumerate().skip(offset) {
            let next_offset = index + 1;
            if entry.is_expired(now_ms) || entry.is_protected() {
                continue;
            }
            *visited = visited.saturating_add(1);
            if visit(entry.key.as_ref()) {
                *emitted = emitted.saturating_add(1);
            }
            if *visited >= limit {
                return Some(next_offset);
            }
        }
        None
    }

    pub(crate) fn visit_keys(&self, now_ms: u64, visit: &mut impl FnMut(&[u8]) -> bool) -> bool {
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if self.fast_points.is_active() {
            return self.fast_points.visit_keys(visit);
        }

        for entry in self
            .entries
            .iter()
            .filter(|entry| !entry.is_expired(now_ms) && !entry.is_protected())
        {
            if !visit(entry.key.as_ref()) {
                return false;
            }
        }
        for entry in self
            .remote_entries
            .values()
            .filter(|entry| !entry.is_expired(now_ms) && !entry.is_protected())
        {
            if !visit(entry.key.as_ref()) {
                return false;
            }
        }
        true
    }

    pub(crate) fn visit_entries(
        &self,
        now_ms: u64,
        visit: &mut impl FnMut(&[u8], &[u8], Option<u64>) -> bool,
    ) -> bool {
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if self.fast_points.is_active() {
            return self.fast_points.visit_entries(visit);
        }

        for entry in self
            .entries
            .iter()
            .filter(|entry| !entry.is_expired(now_ms) && !entry.is_protected())
        {
            if !visit(entry.key.as_ref(), entry.value.as_ref(), entry.expire_at_ms) {
                return false;
            }
        }
        for entry in self
            .remote_entries
            .values()
            .filter(|entry| !entry.is_expired(now_ms) && !entry.is_protected())
        {
            let Some(value) = self
                .object_overflow
                .as_ref()
                .and_then(|overflow| overflow.get_value(&entry.object).ok())
            else {
                continue;
            };
            if !visit(entry.key.as_ref(), value.as_ref(), entry.expire_at_ms) {
                return false;
            }
        }
        true
    }

    pub fn process_maintenance(&mut self, now_ms: u64) -> usize {
        self.reclaim_retired_if_quiescent();
        if self.ttl_entries == 0 {
            return 0;
        }

        let expired = self
            .entries
            .iter()
            .filter(|entry| entry.is_expired(now_ms))
            .map(|entry| (entry.hash, entry.key.as_ref().to_vec()))
            .collect::<Vec<_>>();

        let removed = expired.len();
        for (hash, key) in expired {
            let _ = self.delete_hashed_internal(hash, &key, now_ms, DeleteReason::Expired);
        }
        let remote_expired = self
            .remote_entries
            .values()
            .filter(|entry| entry.is_expired(now_ms))
            .map(|entry| (entry.hash, entry.key.as_ref().to_vec()))
            .collect::<Vec<_>>();
        let remote_removed = remote_expired.len();
        for (hash, key) in remote_expired {
            let _ = self.delete_remote_hashed(hash, &key, DeleteReason::Expired);
        }
        removed.saturating_add(remote_removed)
    }

    pub fn stats_snapshot(&self) -> (TierStatsSnapshot, TierStatsSnapshot, TierStatsSnapshot) {
        (
            TierStatsSnapshot {
                name: "hot",
                len: 0,
                capacity: 0,
                ..TierStatsSnapshot::default()
            },
            TierStatsSnapshot {
                name: "warm",
                len: 0,
                capacity: 0,
                ..TierStatsSnapshot::default()
            },
            TierStatsSnapshot {
                name: "cold",
                len: self.len(),
                capacity: self.len(),
                evictions: self.evictions,
                ..TierStatsSnapshot::default()
            },
        )
    }
}
