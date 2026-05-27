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
        let entry = self
            .entries
            .find_entry(hash, |entry| entry.matches(hash, key))
            .ok()?;
        if entry.get().is_expired(now_ms) {
            let _ = entry;
            let _ = self.delete_hashed_internal(hash, key, now_ms, DeleteReason::Expired);
            return None;
        }

        let removed_key_len = entry.get().key.len();
        let removed_value_len = entry.get().value.len();
        if entry.get().expire_at_ms.is_some() {
            self.ttl_entries = self.ttl_entries.saturating_sub(1);
        }
        let (removed, _) = entry.remove();
        self.stored_bytes = self
            .stored_bytes
            .saturating_sub(removed_key_len.saturating_add(removed_value_len));
        #[cfg(feature = "telemetry")]
        self.record_delete_metrics(
            DeleteReason::Explicit,
            -1,
            -((removed_key_len + removed_value_len) as isize),
        );
        Some(removed.value)
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
            return false;
        };

        let removed_key_len = entry.get().key.len();
        let removed_value_len = entry.get().value.len();
        if entry.get().expire_at_ms.is_some() {
            self.ttl_entries = self.ttl_entries.saturating_sub(1);
        }
        let (removed, _) = entry.remove();
        self.stored_bytes = self
            .stored_bytes
            .saturating_sub(removed_key_len.saturating_add(removed_value_len));
        self.retire_value(removed.value);
        if reason == DeleteReason::Evicted {
            self.evictions = self.evictions.saturating_add(1);
        }
        #[cfg(feature = "telemetry")]
        self.record_delete_metrics(
            reason,
            -1,
            -((removed_key_len + removed_value_len) as isize),
        );
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
            return false;
        };

        let removed_key_len = entry.get().key.len();
        let removed_value_len = entry.get().value.len();
        if entry.get().expire_at_ms.is_some() {
            self.ttl_entries = self.ttl_entries.saturating_sub(1);
        }
        let (removed, _) = entry.remove();
        drop(removed);
        self.stored_bytes = self
            .stored_bytes
            .saturating_sub(removed_key_len.saturating_add(removed_value_len));
        if reason == DeleteReason::Evicted {
            self.evictions = self.evictions.saturating_add(1);
        }
        #[cfg(feature = "telemetry")]
        self.record_delete_metrics(
            reason,
            -1,
            -((removed_key_len + removed_value_len) as isize),
        );
        true
    }

    pub fn ttl_seconds(&mut self, key: &[u8], now_ms: u64) -> i64 {
        self.disable_fast_point_map();
        let hash = hash_key(key);
        let Some(entry) = self.entries.find(hash, |entry| entry.matches(hash, key)) else {
            return -2;
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
            return -2;
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
            return false;
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
            return false;
        };
        let had_ttl = entry.get().expire_at_ms.is_some();
        entry.get_mut().expire_at_ms = Some(expire_at_ms);
        self.adjust_ttl_count(had_ttl, true);
        true
    }

    pub fn snapshot_entries(&self, now_ms: u64) -> Vec<StoredEntry> {
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if self.fast_points.is_active() {
            return self.fast_points.snapshot_entries();
        }
        self.entries
            .iter()
            .filter(|entry| !entry.is_expired(now_ms))
            .map(|entry| StoredEntry {
                key: entry.key.as_ref().to_vec(),
                value: entry.value.as_ref().to_vec(),
                expire_at_ms: entry.expire_at_ms,
            })
            .collect()
    }

    pub fn snapshot_keys(&self, now_ms: u64) -> Vec<Bytes> {
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if self.fast_points.is_active() {
            return self.fast_points.snapshot_keys();
        }
        self.entries
            .iter()
            .filter(|entry| !entry.is_expired(now_ms))
            .map(|entry| entry.key.as_ref().to_vec())
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
            if entry.is_expired(now_ms) {
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
            .filter(|entry| !entry.is_expired(now_ms))
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
            .filter(|entry| !entry.is_expired(now_ms))
        {
            if !visit(entry.key.as_ref(), entry.value.as_ref(), entry.expire_at_ms) {
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
        removed
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
