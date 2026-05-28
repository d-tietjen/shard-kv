use super::*;

impl FlatMap {
    pub fn new() -> Self {
        Self {
            entries: HashTable::new(),
            semantic_index: SemanticIndex::default(),
            #[cfg(feature = "experimental-no-ttl-point-hot-path")]
            fast_points: FastPointMap::default(),
            ttl_entries: 0,
            active_readers: AtomicUsize::new(0),
            retired_values: Vec::new(),
            reusable_values: Vec::new(),
            reusable_value_bytes: 0,
            stored_bytes: 0,
            memory_limit_bytes: None,
            eviction_policy: EvictionPolicy::None,
            access_clock: 0,
            read_sample_counter: 0,
            lru_touch_log: VecDeque::new(),
            evictions: 0,
            #[cfg(feature = "telemetry")]
            telemetry: None,
        }
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        let mut map = Self::new();
        if capacity > 0 {
            map.entries = HashTable::with_capacity(capacity);
        }
        map
    }

    pub fn from_entries(entries: impl IntoIterator<Item = StoredEntry>, now_ms: u64) -> Self {
        let mut map = Self::new();
        for entry in entries {
            if entry
                .expire_at_ms
                .is_some_and(|deadline| deadline <= now_ms)
            {
                continue;
            }
            map.set(entry.key, entry.value, entry.expire_at_ms, now_ms);
        }
        map
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if self.fast_points.is_active() {
            return self.fast_points.len();
        }
        self.entries.len()
    }

    #[inline(always)]
    pub fn stored_bytes(&self) -> usize {
        self.stored_bytes
    }

    #[inline(always)]
    pub fn memory_limit_bytes(&self) -> Option<usize> {
        self.memory_limit_bytes
    }

    #[inline(always)]
    pub fn eviction_policy(&self) -> EvictionPolicy {
        self.eviction_policy
    }

    #[inline(always)]
    pub fn evictions(&self) -> u64 {
        self.evictions
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if self.fast_points.is_active() {
            return self.fast_points.is_empty();
        }
        self.entries.is_empty()
    }

    #[cfg(feature = "telemetry")]
    pub fn attach_metrics(&mut self, metrics: CacheTelemetryHandle, shard_id: usize) {
        self.telemetry = Some(FlatMapTelemetry { metrics, shard_id });
        self.sync_metrics_state();
    }

    pub fn configure_memory_policy(
        &mut self,
        memory_limit_bytes: Option<usize>,
        eviction_policy: EvictionPolicy,
        now_ms: u64,
    ) {
        self.memory_limit_bytes = memory_limit_bytes.filter(|limit| *limit > 0);
        self.eviction_policy = eviction_policy;
        if self.memory_limit_bytes.is_some() || self.eviction_policy != EvictionPolicy::None {
            self.disable_fast_point_map();
        } else {
            self.reusable_values.clear();
            self.reusable_value_bytes = 0;
        }
        self.enforce_memory_limit(now_ms);
    }

    #[inline(always)]
    pub(super) fn entry_is_expired_hashed(&self, hash: u64, key: &[u8], now_ms: u64) -> bool {
        self.entries
            .find(hash, |entry| entry.matches(hash, key))
            .is_some_and(|entry| entry.is_expired(now_ms))
    }

    #[inline(always)]
    pub(super) fn lookup_ref_hashed_lazy(&mut self, hash: u64, key: &[u8]) -> Option<&[u8]> {
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if self.fast_points.is_active() {
            return self.fast_points.get(hash, key).map(|value| value.as_ref());
        }
        if self.should_sample_read() {
            let tick = self.next_access_tick();
            self.entries
                .find_mut(hash, |entry| entry.matches(hash, key))
                .map(|entry| {
                    entry.access.record_access(tick);
                    entry.value.as_ref()
                })
        } else {
            self.entries
                .find(hash, |entry| entry.matches(hash, key))
                .map(|entry| entry.value.as_ref())
        }
    }

    #[inline(always)]
    pub(super) fn lookup_ref_hashed_prepared_lazy(
        &mut self,
        hash: u64,
        key: &[u8],
        key_tag: u64,
    ) -> Option<&[u8]> {
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if self.fast_points.is_active() {
            return self.fast_points.get(hash, key).map(|value| value.as_ref());
        }
        if self.should_sample_read() {
            let tick = self.next_access_tick();
            self.entries
                .find_mut(hash, |entry| entry.matches_prepared(hash, key, key_tag))
                .map(|entry| {
                    entry.access.record_access(tick);
                    entry.value.as_ref()
                })
        } else {
            self.entries
                .find(hash, |entry| entry.matches_prepared(hash, key, key_tag))
                .map(|entry| entry.value.as_ref())
        }
    }

    #[inline(always)]
    pub(super) fn adjust_ttl_count(&mut self, had_ttl: bool, has_ttl: bool) {
        match (had_ttl, has_ttl) {
            (false, true) => {
                self.disable_fast_point_map();
                self.ttl_entries = self.ttl_entries.saturating_add(1);
            }
            (true, false) => {
                self.ttl_entries = self.ttl_entries.saturating_sub(1);
            }
            _ => {}
        }
    }

    #[inline(always)]
    pub(super) fn disable_fast_point_map(&mut self) {
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if self.fast_points.is_active() {
            debug_assert!(self.entries.is_empty());
            for fast_entry in self.fast_points.take_entries_and_disable() {
                let entry = fast_entry.into_flat_entry();
                self.entries
                    .insert_unique(entry.hash, entry, |entry| entry.hash);
            }
        }
    }

    #[inline(always)]
    pub(super) fn retire_value(&mut self, value: SharedBytes) {
        if self.has_active_readers() {
            self.retired_values.push(value);
        } else {
            self.recycle_value(value);
        }
    }

    #[inline(always)]
    pub(super) fn recycle_value(&mut self, value: SharedBytes) {
        if self.eviction_policy == EvictionPolicy::None || self.memory_limit_bytes.is_none() {
            return;
        }
        recycle_value_into_pool(
            value,
            &mut self.reusable_values,
            &mut self.reusable_value_bytes,
        );
    }

    #[inline(always)]
    pub(super) fn has_active_readers(&self) -> bool {
        self.active_readers.load(Ordering::Acquire) > 0
    }

    #[inline(always)]
    pub(super) fn reclaim_retired_if_quiescent(&mut self) {
        if !self.retired_values.is_empty() && !self.has_active_readers() {
            let retired_values = mem::take(&mut self.retired_values);
            for value in retired_values {
                self.recycle_value(value);
            }
        }
    }

    #[inline(always)]
    pub(super) fn next_access_tick(&mut self) -> u64 {
        self.access_clock = self.access_clock.wrapping_add(1);
        self.access_clock
    }

    #[inline(always)]
    pub(super) fn record_lru_touch(&mut self, hash: u64, tick: u64) {
        if tick == 0 || self.eviction_policy != EvictionPolicy::Lru {
            return;
        }
        self.lru_touch_log.push_back(LruTouch { tick, hash });
        self.compact_lru_touch_log_if_needed();
    }

    #[inline(always)]
    fn compact_lru_touch_log_if_needed(&mut self) {
        let max_log_len = self.entries.len().saturating_mul(4).max(1024);
        if self.lru_touch_log.len() <= max_log_len {
            return;
        }
        self.rebuild_lru_touch_log();
    }

    fn rebuild_lru_touch_log(&mut self) {
        let mut touches = self
            .entries
            .iter()
            .filter_map(|entry| match entry.access.last_touch {
                0 => None,
                tick => Some(LruTouch {
                    tick,
                    hash: entry.hash,
                }),
            })
            .collect::<Vec<_>>();
        touches.sort_unstable_by_key(|touch| touch.tick);
        self.lru_touch_log = touches.into();
    }

    #[inline(always)]
    fn should_sample_read(&mut self) -> bool {
        const READ_TOUCH_SAMPLE_MASK: u64 = 1023;
        if self.eviction_policy == EvictionPolicy::None {
            return false;
        }
        if self.memory_limit_bytes.is_none() {
            return false;
        }
        let limit = self.memory_limit_bytes.unwrap();
        let watermark = limit.saturating_mul(3) / 4;
        if self.stored_bytes < watermark.max(1) {
            return false;
        }
        self.read_sample_counter = self.read_sample_counter.wrapping_add(1);
        (self.read_sample_counter & READ_TOUCH_SAMPLE_MASK) == 0
    }

    pub(super) fn enforce_memory_limit(&mut self, now_ms: u64) {
        let Some(limit) = self.memory_limit_bytes else {
            return;
        };
        if self.stored_bytes <= limit {
            return;
        }
        if self.ttl_entries > 0 {
            self.process_maintenance(now_ms);
        }
        self.evict_to_memory_target(self.eviction_policy, now_ms, eviction_target_bytes(limit));
    }

    pub(crate) fn eviction_candidate(
        &self,
        policy: EvictionPolicy,
    ) -> Option<(EvictionRank, u64, Bytes)> {
        if policy == EvictionPolicy::None || self.entries.is_empty() {
            return None;
        }
        #[cfg(feature = "prefix-eviction")]
        if policy == EvictionPolicy::Prefix {
            return self.prefix_eviction_candidate();
        }

        let mut selected: Option<(EvictionRank, u64, &[u8])> = None;
        for entry in self.entries.iter() {
            let candidate = (entry.access.rank(policy), entry.hash, entry.key.as_ref());
            selected = match selected {
                Some(current) if current.0 <= candidate.0 => Some(current),
                _ => Some(candidate),
            };
        }
        selected.map(|(rank, hash, key)| (rank, hash, key.to_vec()))
    }

    pub(crate) fn evict_with_policy(&mut self, policy: EvictionPolicy, now_ms: u64) -> bool {
        let Some((_rank, hash, key)) = self.eviction_candidate(policy) else {
            return false;
        };
        self.delete_hashed_internal(hash, &key, now_ms, DeleteReason::Evicted)
    }

    pub(crate) fn evict_to_memory_target(
        &mut self,
        policy: EvictionPolicy,
        now_ms: u64,
        target_bytes: usize,
    ) -> bool {
        if policy == EvictionPolicy::None || self.entries.is_empty() {
            return false;
        }

        #[cfg(feature = "prefix-eviction")]
        if policy == EvictionPolicy::Prefix {
            let mut evicted = false;
            while self.stored_bytes > target_bytes {
                if !self.evict_with_policy(policy, now_ms) {
                    break;
                }
                evicted = true;
            }
            return evicted;
        }

        if policy == EvictionPolicy::Lru {
            let evicted = self.evict_lru_from_touch_log(now_ms, target_bytes);
            if self.stored_bytes <= target_bytes || self.entries.is_empty() {
                return evicted;
            }
        }

        let mut evicted = false;
        while self.stored_bytes > target_bytes {
            let mut candidates = self.eviction_candidates_for_target(policy, target_bytes);
            if candidates.is_empty() {
                break;
            }
            candidates.sort_unstable_by_key(|candidate| candidate.rank);
            let mut evicted_batch = false;
            for candidate in candidates {
                if self.stored_bytes <= target_bytes {
                    break;
                }
                evicted_batch |= self.delete_hashed_internal(
                    candidate.hash,
                    &candidate.key,
                    now_ms,
                    DeleteReason::Evicted,
                );
            }
            if !evicted_batch {
                break;
            }
            evicted = true;
        }
        evicted
    }

    fn evict_lru_from_touch_log(&mut self, _now_ms: u64, target_bytes: usize) -> bool {
        let mut evicted = false;
        while self.stored_bytes > target_bytes {
            let Some(touch) = self.lru_touch_log.pop_front() else {
                break;
            };
            let Some(entry) = self
                .entries
                .find_entry(touch.hash, |entry| entry.access.last_touch == touch.tick)
                .ok()
            else {
                continue;
            };
            let removed_bytes = entry.get().stored_bytes();
            let had_ttl = entry.get().expire_at_ms.is_some();
            let (removed, _) = entry.remove();
            if had_ttl {
                self.ttl_entries = self.ttl_entries.saturating_sub(1);
            }
            self.stored_bytes = self.stored_bytes.saturating_sub(removed_bytes);
            self.retire_value(removed.value);
            self.evictions = self.evictions.saturating_add(1);
            #[cfg(feature = "telemetry")]
            self.record_delete_metrics(DeleteReason::Evicted, -1, -(removed_bytes as isize));
            evicted = true;
        }
        evicted
    }

    fn eviction_candidates_for_target(
        &self,
        policy: EvictionPolicy,
        target_bytes: usize,
    ) -> Vec<EvictionCandidate> {
        let target_count = self.eviction_candidate_count(target_bytes);
        if target_count == 0 {
            return Vec::new();
        }

        let mut candidates = BinaryHeap::with_capacity(target_count);
        for entry in self.entries.iter() {
            let rank = entry.access.rank(policy);
            if candidates.len() < target_count {
                candidates.push(EvictionCandidate {
                    rank,
                    hash: entry.hash,
                    key: entry.key.as_ref().to_vec(),
                });
                continue;
            }

            let Some(mut warmest_candidate) = candidates.peek_mut() else {
                continue;
            };
            if rank < warmest_candidate.rank {
                *warmest_candidate = EvictionCandidate {
                    rank,
                    hash: entry.hash,
                    key: entry.key.as_ref().to_vec(),
                };
            }
        }
        candidates.into_vec()
    }

    #[cfg(feature = "prefix-eviction")]
    fn prefix_eviction_candidate(&self) -> Option<(EvictionRank, u64, Bytes)> {
        let mut group_ranks: HashMap<Bytes, EvictionRank> = HashMap::new();
        for entry in self.entries.iter() {
            let prefix = prefix_eviction_key_prefix(entry.key.as_ref()).to_vec();
            let rank = prefix_eviction_group_rank(entry.access);
            group_ranks
                .entry(prefix)
                .and_modify(|group_rank| {
                    if *group_rank < rank {
                        *group_rank = rank;
                    }
                })
                .or_insert(rank);
        }

        let (group_rank, prefix) = group_ranks
            .into_iter()
            .min_by_key(|(_prefix, rank)| *rank)
            .map(|(prefix, rank)| (rank, prefix))?;

        let mut selected: Option<(EvictionRank, u64, &[u8])> = None;
        for entry in self.entries.iter() {
            if prefix_eviction_key_prefix(entry.key.as_ref()) != prefix.as_slice() {
                continue;
            }
            let rank = prefix_eviction_member_rank(prefix.len(), entry.key_len, entry.access);
            let candidate = (rank, entry.hash, entry.key.as_ref());
            selected = match selected {
                Some(current) if current.0 <= candidate.0 => Some(current),
                _ => Some(candidate),
            };
        }

        selected.map(|(_member_rank, hash, key)| (group_rank, hash, key.to_vec()))
    }

    fn eviction_candidate_count(&self, target_bytes: usize) -> usize {
        let bytes_to_free = self.stored_bytes.saturating_sub(target_bytes);
        if bytes_to_free == 0 || self.entries.is_empty() {
            return 0;
        }

        let average_entry_bytes = (self.stored_bytes / self.entries.len()).max(1);
        let estimated_count = bytes_to_free.div_ceil(average_entry_bytes);
        let safety_margin = (estimated_count / 8).saturating_add(8);
        estimated_count
            .saturating_add(safety_margin)
            .min(self.entries.len())
    }

    pub(crate) fn eviction_target_bytes(limit: usize) -> usize {
        eviction_target_bytes(limit)
    }

    #[cfg(feature = "telemetry")]
    #[inline(always)]
    pub(super) fn record_set_metrics(
        &self,
        written_len: usize,
        key_delta: isize,
        memory_delta: isize,
        start: Option<Instant>,
    ) {
        if let (Some(telemetry), Some(start)) = (&self.telemetry, start) {
            telemetry.metrics.record_set(
                telemetry.shard_id,
                written_len,
                start.elapsed().as_nanos() as u64,
            );
            telemetry
                .metrics
                .adjust_keys_total(telemetry.shard_id, key_delta);
            telemetry
                .metrics
                .adjust_memory_bytes(telemetry.shard_id, memory_delta);
            telemetry
                .metrics
                .set_shard_keys(telemetry.shard_id, self.len());
        }
    }

    #[cfg(feature = "telemetry")]
    #[inline(always)]
    pub(super) fn record_delete_metrics(
        &self,
        reason: DeleteReason,
        key_delta: isize,
        memory_delta: isize,
    ) {
        if let Some(telemetry) = &self.telemetry {
            match reason {
                DeleteReason::Explicit => telemetry.metrics.record_delete(telemetry.shard_id),
                DeleteReason::Expired => telemetry.metrics.record_expiration(1),
                DeleteReason::Evicted => {}
            }
            telemetry
                .metrics
                .adjust_keys_total(telemetry.shard_id, key_delta);
            telemetry
                .metrics
                .adjust_memory_bytes(telemetry.shard_id, memory_delta);
            telemetry
                .metrics
                .set_shard_keys(telemetry.shard_id, self.len());
        }
    }

    #[cfg(feature = "telemetry")]
    #[inline(always)]
    fn sync_metrics_state(&self) {
        if let Some(telemetry) = &self.telemetry {
            telemetry
                .metrics
                .set_shard_keys(telemetry.shard_id, self.len());
            telemetry
                .metrics
                .adjust_keys_total(telemetry.shard_id, self.len() as isize);
            telemetry
                .metrics
                .adjust_memory_bytes(telemetry.shard_id, self.stored_bytes as isize);
        }
    }
}

fn eviction_target_bytes(limit: usize) -> usize {
    const EXACT_EVICTION_LIMIT_BYTES: usize = 4096;
    if limit <= EXACT_EVICTION_LIMIT_BYTES {
        return limit;
    }
    limit.saturating_sub((limit / 20).max(1))
}
