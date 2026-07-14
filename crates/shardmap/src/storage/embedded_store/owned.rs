#[cfg(not(feature = "embedded-read-biased-lock"))]
use parking_lot::RwLockWriteGuard;
#[cfg(feature = "embedded-read-biased-lock")]
use rblock::RwLockWriteGuard;

use crate::storage::FlatMap;

use super::batch_results::{
    BatchReadViewBuilder, OrderedBatchReadViewBuilder, OrderedPackedBatchBuilder,
    PackedBatchBuilder,
};
use super::*;

/// Exclusive shard-local handle for slot-owned embedded workers.
///
/// This is the no-contention path for workloads that can guarantee one thread
/// owns a shard's slot range. The caller locks the shard once, then performs
/// many point operations without re-entering the shared store routing path.
#[derive(Debug)]
pub struct EmbeddedShardHandle<'a> {
    shard_id: usize,
    shard: RwLockWriteGuard<'a, EmbeddedShard>,
}

impl<'a> EmbeddedShardHandle<'a> {
    #[inline(always)]
    pub fn shard_id(&self) -> usize {
        self.shard_id
    }

    #[inline(always)]
    pub fn get_ref_no_ttl_hashed(&mut self, key_hash: u64, key: &[u8]) -> Option<&[u8]> {
        self.shard.map.get_ref_hashed_no_ttl(key_hash, key)
    }

    #[cfg(feature = "mutable-value-slices")]
    #[inline(always)]
    pub fn value_mut_hashed_no_ttl(&mut self, key_hash: u64, key: &[u8]) -> Option<&mut [u8]> {
        self.shard.map.value_mut_hashed_no_ttl(key_hash, key)
    }

    #[inline(always)]
    pub fn set_hashed_no_ttl<K, V>(&mut self, key_hash: u64, key: K, value: V)
    where
        K: Into<Bytes>,
        V: Into<Bytes>,
    {
        self.shard.map.set_hashed(key_hash, key, value, None, 0);
        self.shard.enforce_memory_limit(0);
    }

    #[inline(always)]
    pub fn set_slice_hashed_no_ttl(&mut self, key_hash: u64, key: &[u8], value: &[u8]) {
        self.shard.map.set_slice_hashed_no_ttl(key_hash, key, value);
        self.shard.enforce_memory_limit(0);
    }
}

/// Owned shard for slot-range workers.
///
/// This is the no-mutex execution path: one worker thread owns this shard and
/// accesses its `FlatMap` directly.
#[derive(Debug)]
pub struct OwnedEmbeddedShard {
    shard_id: usize,
    map: FlatMap,
    session_slots: SessionSlotMap,
    memory_limit_bytes: Option<usize>,
    eviction_policy: EvictionPolicy,
}

impl OwnedEmbeddedShard {
    #[inline(always)]
    fn update_lazy_read_sampling(&mut self) {
        let enabled = match (self.eviction_policy, self.memory_limit_bytes) {
            (EvictionPolicy::None, _) | (_, None) => false,
            (_, Some(limit)) => {
                let watermark = limit.saturating_mul(3) / 4;
                self.stored_bytes() >= watermark.max(1)
            }
        };
        self.session_slots.configure_read_sampling(enabled);
    }

    #[inline(always)]
    pub fn shard_id(&self) -> usize {
        self.shard_id
    }

    #[inline(always)]
    pub fn get_ref_no_ttl_hashed(&mut self, key_hash: u64, key: &[u8]) -> Option<&[u8]> {
        self.map.get_ref_hashed_no_ttl(key_hash, key)
    }

    #[cfg(feature = "mutable-value-slices")]
    #[inline(always)]
    pub fn value_mut_hashed_no_ttl(&mut self, key_hash: u64, key: &[u8]) -> Option<&mut [u8]> {
        self.map.value_mut_hashed_no_ttl(key_hash, key)
    }

    #[inline(always)]
    pub fn set_hashed_no_ttl<K, V>(&mut self, key_hash: u64, key: K, value: V)
    where
        K: Into<Bytes>,
        V: Into<Bytes>,
    {
        self.map.set_hashed(key_hash, key, value, None, 0);
        self.enforce_memory_limit(0);
    }

    #[inline(always)]
    pub fn set_slice_hashed_no_ttl(&mut self, key_hash: u64, key: &[u8], value: &[u8]) {
        self.map.set_slice_hashed_no_ttl(key_hash, key, value);
        self.enforce_memory_limit(0);
    }

    #[inline(always)]
    pub fn get_session_ref_hashed_no_ttl(
        &mut self,
        session_prefix: &[u8],
        key_hash: u64,
        key: &[u8],
    ) -> Option<&[u8]> {
        self.session_slots
            .get_ref_hashed(session_prefix, key_hash, key)
    }

    #[inline(always)]
    pub fn get_ref_hashed_session_or_flat(
        &mut self,
        key_hash: u64,
        key: &[u8],
        now_ms: u64,
    ) -> Option<&[u8]> {
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && let Some(value) = self
                .session_slots
                .get_ref_hashed(&session_prefix, key_hash, key)
        {
            return Some(value);
        }

        self.map.get_ref_hashed(key_hash, key, now_ms)
    }

    #[inline(always)]
    pub fn get_ref_hashed_published_session_or_flat(
        &mut self,
        key_hash: u64,
        key: &[u8],
        now_ms: u64,
    ) -> Option<&[u8]> {
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && self.session_slots.has_session(&session_prefix)
        {
            return self
                .session_slots
                .get_ref_hashed(&session_prefix, key_hash, key);
        }

        self.map.get_ref_hashed(key_hash, key, now_ms)
    }

    #[inline(always)]
    pub fn get_ref_hashed_active_session_or_flat(
        &mut self,
        active_session_prefix: Option<&[u8]>,
        key_hash: u64,
        key: &[u8],
        now_ms: u64,
    ) -> Option<&[u8]> {
        match active_session_prefix {
            Some(session_prefix) => {
                self.session_slots
                    .get_ref_hashed(session_prefix, key_hash, key)
            }
            None => self.map.get_ref_hashed(key_hash, key, now_ms),
        }
    }

    #[inline(always)]
    pub fn get_ref_hashed_active_session_or_no_ttl_flat(
        &mut self,
        active_session_prefix: Option<&[u8]>,
        key_hash: u64,
        key: &[u8],
    ) -> Option<&[u8]> {
        match active_session_prefix {
            Some(session_prefix) => {
                self.get_session_ref_hashed_no_ttl(session_prefix, key_hash, key)
            }
            None => self.get_ref_no_ttl_hashed(key_hash, key),
        }
    }

    #[inline(always)]
    pub fn set_session_slice_hashed_no_ttl(
        &mut self,
        session_prefix: &[u8],
        key_hash: u64,
        key: &[u8],
        value: &[u8],
    ) {
        self.map.delete_hashed(key_hash, key, 0);
        self.session_slots
            .set_slice_hashed(session_prefix, key_hash, key, value);
        self.enforce_memory_limit(0);
    }

    fn stored_bytes(&self) -> usize {
        self.map
            .stored_bytes()
            .saturating_add(self.session_slots.stored_bytes())
    }

    #[inline(always)]
    fn exceeds_memory_limit(&self) -> bool {
        self.memory_limit_bytes
            .is_some_and(|limit| self.stored_bytes() > limit)
    }

    #[inline(always)]
    fn enforce_memory_limit(&mut self, now_ms: u64) {
        self.update_lazy_read_sampling();
        let Some(limit) = self.memory_limit_bytes else {
            return;
        };
        if self.stored_bytes() <= limit {
            return;
        }

        self.map.process_maintenance(now_ms);
        if self.session_slots.is_empty() {
            self.map.evict_to_memory_target(
                self.eviction_policy,
                now_ms,
                FlatMap::eviction_target_bytes(limit),
            );
            self.update_lazy_read_sampling();
            return;
        }
        while self.stored_bytes() > limit {
            let map_candidate = self.map.eviction_candidate(self.eviction_policy);
            let session_candidate = self.session_slots.eviction_candidate(self.eviction_policy);
            let evicted = match (map_candidate, session_candidate) {
                (Some((map_rank, _, _)), Some((session_rank, _, _, _))) => {
                    if session_rank < map_rank {
                        self.session_slots.evict_with_policy(self.eviction_policy)
                    } else {
                        self.map.evict_with_policy(self.eviction_policy, now_ms)
                    }
                }
                (Some((_rank, _, _)), None) => {
                    self.map.evict_with_policy(self.eviction_policy, now_ms)
                }
                (None, Some((_rank, _, _, _))) => {
                    self.session_slots.evict_with_policy(self.eviction_policy)
                }
                (None, None) => false,
            };
            if !evicted {
                break;
            }
        }
        self.update_lazy_read_sampling();
    }
}

/// One worker-local shard set for the slot-owned embedded path.
///
/// The worker owns every shard in this set exclusively, so routed operations
/// execute without any mutex on the hot path.
#[derive(Debug)]
pub struct OwnedEmbeddedWorkerShards {
    route_mode: EmbeddedRouteMode,
    shard_count: usize,
    shift: u32,
    shard_lookup: Vec<usize>,
    shards: Vec<OwnedEmbeddedShard>,
    #[cfg(feature = "telemetry")]
    metrics: Option<Arc<CacheTelemetry>>,
}

impl OwnedEmbeddedWorkerShards {
    #[cfg(feature = "embedded")]
    pub(crate) fn local_tier_stats_snapshot(
        &self,
    ) -> (TierStatsSnapshot, TierStatsSnapshot, TierStatsSnapshot) {
        let mut hot = TierStatsSnapshot {
            name: "hot",
            ..TierStatsSnapshot::default()
        };
        let mut warm = TierStatsSnapshot {
            name: "warm",
            ..TierStatsSnapshot::default()
        };
        let mut cold = TierStatsSnapshot {
            name: "cold",
            ..TierStatsSnapshot::default()
        };

        for shard in &self.shards {
            let (shard_hot, shard_warm, shard_cold) = shard.map.stats_snapshot();
            accumulate_tier_stats(&mut hot, &shard_hot);
            accumulate_tier_stats(&mut warm, &shard_warm);
            accumulate_tier_stats(&mut cold, &shard_cold);
        }

        (hot, warm, cold)
    }

    fn new(
        route_mode: EmbeddedRouteMode,
        shard_count: usize,
        shards: Vec<OwnedEmbeddedShard>,
        #[cfg(feature = "telemetry")] metrics: Option<Arc<CacheTelemetry>>,
    ) -> Self {
        assert_valid_shard_count(shard_count);
        let mut shard_lookup = vec![usize::MAX; shard_count];
        for (index, shard) in shards.iter().enumerate() {
            shard_lookup[shard.shard_id()] = index;
        }
        Self {
            route_mode,
            shard_count,
            shift: shift_for(shard_count),
            shard_lookup,
            shards,
            #[cfg(feature = "telemetry")]
            metrics,
        }
    }

    #[inline(always)]
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }

    #[inline(always)]
    pub fn route_mode(&self) -> EmbeddedRouteMode {
        self.route_mode
    }

    #[inline(always)]
    pub fn worker_shard_count(&self) -> usize {
        self.shards.len()
    }

    #[inline(always)]
    pub fn owns_shard(&self, shard_id: usize) -> bool {
        self.shard_lookup
            .get(shard_id)
            .copied()
            .is_some_and(|index| index != usize::MAX)
    }

    #[inline(always)]
    pub fn route_key(&self, key: &[u8]) -> EmbeddedKeyRoute {
        compute_key_route(self.route_mode, self.shift, key)
    }

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

    #[inline(always)]
    pub fn route_session(&self, session_prefix: &[u8]) -> EmbeddedSessionRoute {
        EmbeddedSessionRoute {
            shard_id: compute_session_shard(self.shift, session_prefix),
        }
    }

    #[inline(always)]
    pub fn begin_read_session(&mut self) -> OwnedEmbeddedWorkerReadSession<'_> {
        OwnedEmbeddedWorkerReadSession {
            opened: vec![false; self.shards.len()],
            opened_indices: Vec::with_capacity(self.shards.len()),
            worker: self,
        }
    }

    #[inline(always)]
    pub fn get_ref_no_ttl_routed(&mut self, route: EmbeddedKeyRoute, key: &[u8]) -> Option<&[u8]> {
        self.shard_for_route_mut(route.shard_id)
            .get_ref_no_ttl_hashed(route.key_hash, key)
    }

    #[cfg(feature = "mutable-value-slices")]
    #[inline(always)]
    pub(crate) fn local_value_mut_routed_no_ttl(
        &mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
    ) -> Option<&mut [u8]> {
        self.shard_for_route_mut(route.shard_id)
            .value_mut_hashed_no_ttl(route.key_hash, key)
    }

    #[inline(always)]
    pub fn get_prepared_ref_no_ttl(&mut self, prepared: &PreparedPointKey) -> Option<&[u8]> {
        self.shard_for_route_mut(prepared.route().shard_id)
            .map
            .get_ref_hashed_prepared_no_ttl(
                prepared.route().key_hash,
                prepared.key(),
                prepared.key_tag(),
            )
    }

    #[cfg(feature = "embedded")]
    pub(crate) fn local_get_slice(&mut self, key: &[u8]) -> Option<EmbeddedReadSlice> {
        let route = self.route_key(key);
        self.local_get_slice_routed(route, key)
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(crate) fn local_get_slice_routed_no_ttl(
        &mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
    ) -> Option<EmbeddedReadSlice> {
        let shard = self.shard_for_route_mut(route.shard_id);
        let value = if let Some(session_prefix) = derived_session_storage_prefix(key) {
            if shard.session_slots.has_session(&session_prefix) {
                shard
                    .session_slots
                    .get_ref_hashed_local(&session_prefix, route.key_hash, key)
            } else {
                shard.map.get_ref_hashed_no_ttl(route.key_hash, key)
            }
        } else {
            shard.map.get_ref_hashed_no_ttl(route.key_hash, key)
        };
        value.map(EmbeddedReadSlice::from_slice)
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(crate) fn local_get_slice_routed(
        &mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
    ) -> Option<EmbeddedReadSlice> {
        let now_ms = now_millis();
        let shard = self.shard_for_route_mut(route.shard_id);
        let value = if let Some(session_prefix) = derived_session_storage_prefix(key) {
            if shard.session_slots.has_session(&session_prefix) {
                shard
                    .session_slots
                    .get_ref_hashed_local(&session_prefix, route.key_hash, key)
            } else {
                shard.map.get_ref_hashed_local(route.key_hash, key, now_ms)
            }
        } else {
            shard.map.get_ref_hashed_local(route.key_hash, key, now_ms)
        };
        value.map(EmbeddedReadSlice::from_slice)
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(crate) fn local_get_point_ref_routed_no_ttl(
        &mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
    ) -> Option<&[u8]> {
        self.shard_for_route_mut(route.shard_id)
            .map
            .get_ref_hashed_no_ttl(route.key_hash, key)
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(crate) fn local_get_point_ref_prepared_routed_no_ttl(
        &mut self,
        prepared: &PreparedPointKey,
    ) -> Option<&[u8]> {
        self.shard_for_route_mut(prepared.route().shard_id)
            .map
            .get_ref_hashed_prepared_no_ttl(
                prepared.route().key_hash,
                prepared.key(),
                prepared.key_tag(),
            )
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(crate) fn local_get_ref_routed_no_ttl(
        &mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
    ) -> Option<&[u8]> {
        let shard = self.shard_for_route_mut(route.shard_id);
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && shard.session_slots.has_session(&session_prefix)
        {
            return shard
                .session_slots
                .get_ref_hashed_local(&session_prefix, route.key_hash, key);
        }
        shard.map.get_ref_hashed_no_ttl(route.key_hash, key)
    }

    #[cfg(feature = "embedded")]
    pub(crate) fn local_batch_get_slices(
        &mut self,
        keys: &[Bytes],
    ) -> Vec<Option<EmbeddedReadSlice>> {
        keys.iter()
            .map(|key| self.local_get_slice(key))
            .collect::<Vec<_>>()
    }

    #[cfg(feature = "embedded")]
    pub(crate) fn local_batch_get_session_slices_prehashed(
        &mut self,
        session_prefix: &[u8],
        keys: &[Bytes],
        key_hashes: &[u64],
    ) -> Vec<Option<EmbeddedReadSlice>> {
        assert_eq!(
            keys.len(),
            key_hashes.len(),
            "keys and key_hashes must have matching lengths",
        );
        let route = self.route_session(session_prefix);
        let shard = self.shard_for_route_mut(route.shard_id);
        let now_ms = now_millis();
        let active_session_prefix = shard
            .session_slots
            .has_session(session_prefix)
            .then_some(session_prefix);
        keys.iter()
            .zip(key_hashes.iter().copied())
            .map(|(key, key_hash)| {
                let value = match active_session_prefix {
                    Some(session_prefix) => {
                        shard
                            .session_slots
                            .get_ref_hashed_local(session_prefix, key_hash, key)
                    }
                    None => shard.map.get_ref_hashed_local(key_hash, key, now_ms),
                };
                value.map(EmbeddedReadSlice::from_slice)
            })
            .collect::<Vec<_>>()
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(crate) fn local_get_session_ref_hashed_no_ttl<'a>(
        &'a mut self,
        session_prefix: &[u8],
        key_hash: u64,
        key: &[u8],
    ) -> Option<&'a [u8]> {
        let route = self.route_session(session_prefix);
        let shard = self.shard_for_route_mut(route.shard_id);
        if shard.session_slots.has_session(session_prefix) {
            return shard
                .session_slots
                .get_ref_hashed_local(session_prefix, key_hash, key);
        }
        shard.map.get_ref_hashed_local(key_hash, key, now_millis())
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(crate) fn local_get_ref_routed(
        &mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
    ) -> Option<&[u8]> {
        let now_ms = now_millis();
        let shard = self.shard_for_route_mut(route.shard_id);
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && shard.session_slots.has_session(&session_prefix)
        {
            return shard
                .session_slots
                .get_ref_hashed_local(&session_prefix, route.key_hash, key);
        }
        shard.map.get_ref_hashed_local(route.key_hash, key, now_ms)
    }

    #[cfg(feature = "embedded")]
    pub(crate) fn local_get(&mut self, key: &[u8]) -> Option<Bytes> {
        let route = self.route_key(key);
        let now_ms = now_millis();
        let shard = self.shard_for_route_mut(route.shard_id);
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && let Some(value) =
                shard
                    .session_slots
                    .get_ref_hashed_local(&session_prefix, route.key_hash, key)
        {
            return Some(value.to_vec());
        }
        shard
            .map
            .get_ref_hashed_local(route.key_hash, key, now_ms)
            .map(<[u8]>::to_vec)
    }

    pub(crate) fn local_get_with_governance_filter<F>(
        &mut self,
        key: &[u8],
        authorize: F,
    ) -> Option<Bytes>
    where
        F: FnOnce(Option<&[u8]>) -> bool,
    {
        let route = self.route_key(key);
        let now_ms = now_millis();
        let shard = self.shard_for_route_mut(route.shard_id);
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && let Some(value) =
                shard
                    .session_slots
                    .get_ref_hashed(&session_prefix, route.key_hash, key)
        {
            return authorize(None).then(|| value.to_vec());
        }
        match shard.map.get_value_bytes_hashed_with_governance_filter(
            route.key_hash,
            key,
            now_ms,
            authorize,
        ) {
            GovernedRead::Authorized(value) => Some(value.to_vec()),
            GovernedRead::Missing | GovernedRead::Denied => None,
        }
    }

    #[cfg(feature = "embedded")]
    pub(crate) fn local_set(&mut self, key: Bytes, value: Bytes, ttl_ms: Option<u64>) {
        let now_ms = now_millis();
        let route = self.route_key(&key);
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        let shard = self.shard_for_route_mut(route.shard_id);
        if let Some(session_prefix) = point_write_session_storage_prefix(&key) {
            shard
                .session_slots
                .delete_hashed_local(&session_prefix, route.key_hash, &key);
        }
        shard
            .map
            .set_hashed_local(route.key_hash, key, value, expire_at_ms, now_ms);
        shard.enforce_memory_limit(now_ms);
    }

    pub(crate) fn local_set_with_governance(
        &mut self,
        key: Bytes,
        value: Bytes,
        ttl_ms: Option<u64>,
        governance: Bytes,
    ) {
        let now_ms = ttl_ms.map_or(0, |_| now_millis());
        let route = self.route_key(&key);
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        let shard = self.shard_for_route_mut(route.shard_id);
        if let Some(session_prefix) = point_write_session_storage_prefix(&key) {
            shard
                .session_slots
                .delete_hashed_local(&session_prefix, route.key_hash, &key);
        }
        shard.map.set_bytes_hashed_with_governance(
            route.key_hash,
            &key,
            bytes::Bytes::from(value),
            bytes::Bytes::from(governance),
            expire_at_ms,
            now_ms,
        );
        shard.enforce_memory_limit(now_ms);
    }

    #[cfg(feature = "embedded")]
    pub(crate) fn local_set_slice_no_ttl(
        &mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        value: &[u8],
    ) {
        self.local_set_slice_tagged_no_ttl(
            route,
            hash_key_tag_from_hash(route.key_hash),
            key,
            value,
        );
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(crate) fn local_set_slice(
        &mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
    ) {
        match ttl_ms {
            None => self.local_set_slice_no_ttl(route, key, value),
            Some(ttl_ms) => {
                let now_ms = now_millis();
                let expire_at_ms = Some(now_ms.saturating_add(ttl_ms));
                let shard = self.shard_for_route_mut(route.shard_id);
                if let Some(session_prefix) = point_write_session_storage_prefix(key) {
                    shard
                        .session_slots
                        .delete_hashed_local(&session_prefix, route.key_hash, key);
                }
                shard
                    .map
                    .set_slice_hashed(route.key_hash, key, value, expire_at_ms, now_ms);
                shard.enforce_memory_limit(now_ms);
            }
        }
    }

    #[cfg(feature = "embedded")]
    pub(crate) fn local_set_prepared_slice_no_ttl(
        &mut self,
        prepared: &PreparedPointKey,
        value: &[u8],
    ) {
        if self.route_mode == EmbeddedRouteMode::FullKey {
            let route = prepared.route();
            let shard = self.shard_for_route_mut(route.shard_id);
            shard.map.set_slice_hashed_tagged_no_ttl_local(
                route.key_hash,
                prepared.key_tag(),
                prepared.key(),
                value,
            );
            if shard.exceeds_memory_limit() {
                shard.enforce_memory_limit(0);
            }
            return;
        }

        self.local_set_slice_tagged_no_ttl(
            prepared.route(),
            prepared.key_tag(),
            prepared.key(),
            value,
        );
    }

    #[cfg(feature = "embedded")]
    pub(crate) fn local_set_slice_tagged_no_ttl(
        &mut self,
        route: EmbeddedKeyRoute,
        key_tag: u64,
        key: &[u8],
        value: &[u8],
    ) {
        let shard = self.shard_for_route_mut(route.shard_id);
        if let Some(session_prefix) = point_write_session_storage_prefix(key) {
            shard
                .session_slots
                .delete_hashed_local(&session_prefix, route.key_hash, key);
        }
        shard
            .map
            .set_slice_hashed_tagged_no_ttl_local(route.key_hash, key_tag, key, value);
        if shard.exceeds_memory_limit() {
            shard.enforce_memory_limit(0);
        }
    }

    pub(crate) fn local_set_value_bytes_routed_expire_at(
        &mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        value: bytes::Bytes,
        expire_at_ms: Option<u64>,
        now_ms: u64,
    ) {
        let shard = self.shard_for_route_mut(route.shard_id);
        if let Some(session_prefix) = point_write_session_storage_prefix(key) {
            shard
                .session_slots
                .delete_hashed(&session_prefix, route.key_hash, key);
        }
        shard
            .map
            .set_bytes_hashed(route.key_hash, key, value, expire_at_ms, now_ms);
        shard.enforce_memory_limit(now_ms);
    }

    pub(crate) fn local_remove_value_routed(
        &mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        now_ms: u64,
    ) -> Option<bytes::Bytes> {
        let shard = self.shard_for_route_mut(route.shard_id);
        let removed = shard.map.remove_value_hashed(route.key_hash, key, now_ms);
        if removed.is_some() {
            shard.enforce_memory_limit(now_ms);
        }
        removed
    }

    #[cfg(feature = "embedded")]
    pub(crate) fn local_batch_set(&mut self, items: Vec<(Bytes, Bytes)>, ttl_ms: Option<u64>) {
        let now_ms = now_millis();
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        for (key, value) in items {
            let route = self.route_key(&key);
            let shard = self.shard_for_route_mut(route.shard_id);
            if let Some(session_prefix) = point_write_session_storage_prefix(&key) {
                shard
                    .session_slots
                    .delete_hashed_local(&session_prefix, route.key_hash, &key);
            }
            shard
                .map
                .set_hashed_local(route.key_hash, key, value, expire_at_ms, now_ms);
            shard.enforce_memory_limit(now_ms);
        }
    }

    #[cfg(feature = "embedded")]
    pub(crate) fn local_batch_set_session_owned_no_ttl(
        &mut self,
        session_prefix: Bytes,
        items: Vec<(Bytes, Bytes)>,
    ) {
        self.local_batch_set_session_packed_no_ttl(PackedSessionWrite::from_owned_items(
            session_prefix,
            items,
        ));
    }

    #[cfg(feature = "embedded")]
    pub(crate) fn local_batch_set_session_packed_no_ttl(&mut self, packed: PackedSessionWrite) {
        if packed.item_count() == 0 {
            return;
        }
        let route = self.route_session(packed.session_prefix());
        self.local_batch_set_session_packed_routed_no_ttl(route, packed);
    }

    #[cfg(feature = "embedded")]
    pub(crate) fn local_batch_set_session_packed_routed_no_ttl(
        &mut self,
        route: EmbeddedSessionRoute,
        packed: PackedSessionWrite,
    ) {
        if packed.item_count() == 0 {
            return;
        }
        let shard = self.shard_for_route_mut(route.shard_id);
        for entry in packed.slab.entries.iter() {
            shard.map.delete_hashed_local(entry.hash, &entry.key, 0);
        }
        shard.session_slots.replace_session_slab_local(packed);
        shard.enforce_memory_limit(0);
    }

    #[cfg(feature = "embedded")]
    pub(crate) fn local_delete(&mut self, key: &[u8]) -> bool {
        let route = self.route_key(key);
        self.local_delete_routed(route, key)
    }

    #[cfg(feature = "embedded")]
    pub(crate) fn local_delete_routed(&mut self, route: EmbeddedKeyRoute, key: &[u8]) -> bool {
        let shard = self.shard_for_route_mut(route.shard_id);
        let deleted_session = derived_session_storage_prefix(key).is_some_and(|session_prefix| {
            shard
                .session_slots
                .delete_hashed_local(&session_prefix, route.key_hash, key)
        });
        let deleted_map = shard.map.delete_hashed_local(route.key_hash, key, 0);
        deleted_session || deleted_map
    }

    #[cfg(feature = "embedded")]
    pub(crate) fn local_exists(&mut self, key: &[u8]) -> bool {
        self.local_get(key).is_some()
    }

    #[cfg(feature = "embedded")]
    pub(crate) fn local_ttl_seconds(&mut self, key: &[u8]) -> i64 {
        let route = self.route_key(key);
        let now_ms = now_millis();
        let shard = self.shard_for_route_mut(route.shard_id);
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && shard
                .session_slots
                .get_ref_hashed_local(&session_prefix, route.key_hash, key)
                .is_some()
        {
            return -1;
        }
        shard.map.ttl_seconds(key, now_ms)
    }

    #[cfg(feature = "embedded")]
    pub(crate) fn local_pttl_millis(&mut self, key: &[u8]) -> i64 {
        let route = self.route_key(key);
        let now_ms = now_millis();
        let shard = self.shard_for_route_mut(route.shard_id);
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && shard
                .session_slots
                .get_ref_hashed_local(&session_prefix, route.key_hash, key)
                .is_some()
        {
            return -1;
        }
        shard.map.ttl_millis(key, now_ms)
    }

    #[cfg(feature = "embedded")]
    pub(crate) fn local_expire(&mut self, key: &[u8], expire_at_ms: u64) -> bool {
        let route = self.route_key(key);
        let now_ms = now_millis();
        let shard = self.shard_for_route_mut(route.shard_id);
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && shard
                .session_slots
                .get_ref_hashed_local(&session_prefix, route.key_hash, key)
                .is_some()
        {
            return false;
        }
        shard.map.expire(key, expire_at_ms, now_ms)
    }

    #[cfg(feature = "embedded")]
    pub(crate) fn local_persist(&mut self, key: &[u8]) -> bool {
        let route = self.route_key(key);
        let now_ms = now_millis();
        let shard = self.shard_for_route_mut(route.shard_id);
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && shard
                .session_slots
                .get_ref_hashed_local(&session_prefix, route.key_hash, key)
                .is_some()
        {
            return false;
        }
        shard.map.persist(key, now_ms)
    }

    #[inline(always)]
    pub fn get_view_routed_no_ttl(
        &mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
    ) -> OwnedEmbeddedReadView {
        let shard = self.shard_for_route_mut(route.shard_id);
        let item = shard
            .get_ref_no_ttl_hashed(route.key_hash, key)
            .map(EmbeddedReadSlice::from_slice);
        OwnedEmbeddedReadView { item }
    }

    #[inline(always)]
    pub fn set_slice_routed_no_ttl(&mut self, route: EmbeddedKeyRoute, key: &[u8], value: &[u8]) {
        self.shard_for_route_mut(route.shard_id)
            .set_slice_hashed_no_ttl(route.key_hash, key, value);
    }

    pub fn batch_set_session_slices_no_ttl<I, K, V>(&mut self, session_prefix: &[u8], items: I)
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let route = self.route_session(session_prefix);
        let shard = self.shard_for_route_mut(route.shard_id);
        for (key, value) in items {
            let key = key.as_ref();
            shard.set_session_slice_hashed_no_ttl(
                session_prefix,
                hash_key(key),
                key,
                value.as_ref(),
            );
        }
    }

    pub fn batch_set_session_packed_no_ttl(&mut self, packed: PackedSessionWrite) {
        if packed.item_count() == 0 {
            return;
        }
        let route = self.route_session(&packed.session_prefix);
        let shard = self.shard_for_route_mut(route.shard_id);
        for entry in packed.slab.entries.iter() {
            shard.map.delete_hashed(entry.hash, &entry.key, 0);
        }
        shard.session_slots.replace_session_slab(packed);
        shard.enforce_memory_limit(0);
    }

    pub fn batch_get_session_view_no_ttl(
        &mut self,
        session_prefix: &[u8],
        keys: &[Bytes],
    ) -> OwnedEmbeddedSessionBatchView {
        let key_hashes = keys.iter().map(|key| hash_key(key)).collect::<Vec<_>>();
        self.batch_get_session_view_prehashed_no_ttl(session_prefix, keys, &key_hashes)
    }

    pub fn batch_get_session_view_prehashed_no_ttl(
        &mut self,
        session_prefix: &[u8],
        keys: &[Bytes],
        key_hashes: &[u64],
    ) -> OwnedEmbeddedSessionBatchView {
        assert_eq!(
            keys.len(),
            key_hashes.len(),
            "keys and key_hashes must have matching lengths",
        );
        if keys.is_empty() {
            return OwnedEmbeddedBatchReadView {
                items: Vec::new(),
                hit_count: 0,
                total_bytes: 0,
            };
        }

        let route = self.route_session(session_prefix);
        let shard = self.shard_for_route_mut(route.shard_id);
        let mut view = BatchReadViewBuilder::new(keys.len());
        for (key, key_hash) in keys.iter().zip(key_hashes.iter().copied()) {
            view.push(shard.get_session_ref_hashed_no_ttl(session_prefix, key_hash, key));
        }

        view.finish_owned()
    }

    pub fn batch_get_session_view_routed_no_ttl(
        &mut self,
        route: EmbeddedSessionRoute,
        keys: &[Bytes],
    ) -> OwnedEmbeddedSessionBatchView {
        let key_hashes = keys.iter().map(|key| hash_key(key)).collect::<Vec<_>>();
        self.batch_get_session_view_prehashed_routed_no_ttl(route, keys, &key_hashes)
    }

    pub fn batch_get_session_view_prehashed_routed_no_ttl(
        &mut self,
        route: EmbeddedSessionRoute,
        keys: &[Bytes],
        key_hashes: &[u64],
    ) -> OwnedEmbeddedSessionBatchView {
        assert_eq!(
            keys.len(),
            key_hashes.len(),
            "keys and key_hashes must have matching lengths",
        );
        if keys.is_empty() {
            return OwnedEmbeddedBatchReadView {
                items: Vec::new(),
                hit_count: 0,
                total_bytes: 0,
            };
        }

        let shard = self.shard_for_route_mut(route.shard_id);
        let session_prefix = batch_derived_session_storage_prefix(keys);
        let active_session_prefix = session_prefix
            .as_ref()
            .filter(|prefix| shard.session_slots.has_session(prefix))
            .map(Vec::as_slice);

        let mut view = BatchReadViewBuilder::new(keys.len());
        for (key, key_hash) in keys.iter().zip(key_hashes.iter().copied()) {
            view.push(shard.get_ref_hashed_active_session_or_no_ttl_flat(
                active_session_prefix,
                key_hash,
                key,
            ));
        }

        view.finish_owned()
    }

    pub fn batch_get_session_packed_no_ttl(
        &mut self,
        session_prefix: &[u8],
        keys: &[Bytes],
    ) -> PackedBatch {
        if keys.is_empty() {
            return PackedBatch::default();
        }

        let route = self.route_session(session_prefix);
        let shard = self.shard_for_route_mut(route.shard_id);
        let mut packed = PackedBatchBuilder::new(keys.len());
        for key in keys {
            let key_hash = hash_key(key);
            packed.push(shard.get_session_ref_hashed_no_ttl(session_prefix, key_hash, key));
        }
        packed.finish()
    }

    pub fn batch_get_session_packed_routed_no_ttl(
        &mut self,
        route: EmbeddedSessionRoute,
        keys: &[Bytes],
    ) -> PackedBatch {
        if keys.is_empty() {
            return PackedBatch::default();
        }

        let shard = self.shard_for_route_mut(route.shard_id);
        let session_prefix = batch_derived_session_storage_prefix(keys);
        let active_session_prefix = session_prefix
            .as_ref()
            .filter(|prefix| shard.session_slots.has_session(prefix))
            .map(Vec::as_slice);
        let mut packed = PackedBatchBuilder::new(keys.len());
        for key in keys {
            let key_hash = hash_key(key);
            packed.push(shard.get_ref_hashed_active_session_or_no_ttl_flat(
                active_session_prefix,
                key_hash,
                key,
            ));
        }
        packed.finish()
    }

    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.map.len().saturating_add(shard.session_slots.len()))
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn process_maintenance(&mut self) -> usize {
        let now_ms = now_millis();
        self.shards
            .iter_mut()
            .map(|shard| shard.map.process_maintenance(now_ms))
            .sum()
    }

    pub fn restore_entries<I>(&mut self, entries: I)
    where
        I: IntoIterator<Item = StoredEntry>,
    {
        let now_ms = now_millis();
        for entry in entries {
            if entry
                .expire_at_ms
                .is_some_and(|expire_at_ms| expire_at_ms <= now_ms)
            {
                continue;
            }
            let route = self.route_key(&entry.key);
            let shard = self.shard_for_route_mut(route.shard_id);
            if let Some(session_prefix) = point_write_session_storage_prefix(&entry.key) {
                shard
                    .session_slots
                    .delete_hashed(&session_prefix, route.key_hash, &entry.key);
            }
            shard.map.set_bytes_hashed_with_governance_option(
                route.key_hash,
                &entry.key,
                bytes::Bytes::from(entry.value),
                entry.governance.map(bytes::Bytes::from),
                entry.expire_at_ms,
                now_ms,
            );
            shard.enforce_memory_limit(now_ms);
        }
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Bytes> {
        let now_ms = now_millis();
        let route = self.route_key(key);
        let shard = self.shard_for_route_mut(route.shard_id);
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

    /// Returns a borrowed value slice for `key` without copying bytes.
    ///
    /// The returned slice is tied to this worker's exclusive `&mut self` borrow.
    /// Use [`Self::get`] when an owned `Vec<u8>` is required.
    pub fn get_ref(&mut self, key: &[u8]) -> Option<&[u8]> {
        let route = self.route_key(key);
        let now_ms = now_millis();
        let shard = self.shard_for_route_mut(route.shard_id);
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && let Some(value) =
                shard
                    .session_slots
                    .get_ref_hashed(&session_prefix, route.key_hash, key)
        {
            return Some(value);
        }
        shard.map.get_ref_hashed(route.key_hash, key, now_ms)
    }

    /// Returns a mutable point-key guard for `key`.
    ///
    /// The returned guard is tied to this worker's exclusive `&mut self` borrow.
    /// Replacements preserve the entry's existing TTL.
    pub fn get_mut(&mut self, key: &[u8]) -> Option<OwnedEmbeddedRefMut<'_>> {
        let route = self.route_key(key);
        self.local_get_mut_routed(route, key)
    }

    pub(crate) fn local_get_mut_routed(
        &mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
    ) -> Option<OwnedEmbeddedRefMut<'_>> {
        let now_ms = now_millis();
        let expire_at_ms = {
            let shard = self.shard_for_route_mut(route.shard_id);
            if let Some(session_prefix) = derived_session_storage_prefix(key)
                && shard.session_slots.has_session(&session_prefix)
            {
                return None;
            }
            if shard
                .map
                .is_value_protected_hashed(route.key_hash, key, now_ms)
            {
                return None;
            }
            shard
                .map
                .entry_expire_at_hashed(route.key_hash, key, now_ms)?
        };

        Some(OwnedEmbeddedRefMut {
            worker: self,
            route,
            key: bytes::Bytes::copy_from_slice(key),
            expire_at_ms,
            _not_send: std::marker::PhantomData,
        })
    }

    pub fn get_view(&mut self, key: &[u8]) -> OwnedEmbeddedReadView {
        let route = self.route_key(key);
        let now_ms = now_millis();
        let shard = self.shard_for_route_mut(route.shard_id);
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
        OwnedEmbeddedReadView { item }
    }

    pub fn batch_get(&mut self, keys: Vec<Bytes>) -> Vec<Option<Bytes>> {
        let total = keys.len();
        if total == 0 {
            return Vec::new();
        }

        #[cfg(feature = "telemetry")]
        let start = self.metrics.as_ref().map(|_| Instant::now());
        let now_ms = now_millis();
        let mut values = vec![None; total];
        let mut groups = vec![Vec::<(usize, Bytes, u64)>::new(); self.shards.len()];
        let mut touched = Vec::new();

        for (index, key) in keys.into_iter().enumerate() {
            let route = self.route_key(&key);
            let local_index = self
                .shard_lookup
                .get(route.shard_id)
                .copied()
                .filter(|idx| *idx != usize::MAX)
                .expect("routed key does not belong to this owned worker");
            if groups[local_index].is_empty() {
                touched.push(route.shard_id);
            }
            groups[local_index].push((index, key, route.key_hash));
        }

        for (local_index, batch) in groups.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            let shard = &mut self.shards[local_index];
            for (index, key, key_hash) in batch {
                values[index] = shard
                    .get_ref_hashed_session_or_flat(key_hash, &key, now_ms)
                    .map(<[u8]>::to_vec);
            }
        }

        #[cfg(feature = "telemetry")]
        self.record_batch_metrics(start, &touched);
        values
    }

    pub fn batch_get_view(&mut self, keys: &[Bytes]) -> OwnedEmbeddedBatchReadView {
        let total = keys.len();
        if total == 0 {
            return OwnedEmbeddedBatchReadView {
                items: Vec::new(),
                hit_count: 0,
                total_bytes: 0,
            };
        }

        #[cfg(feature = "telemetry")]
        let start = self.metrics.as_ref().map(|_| Instant::now());
        let now_ms = now_millis();

        let mut groups = vec![Vec::<(usize, &Bytes, u64, usize)>::new(); self.shards.len()];
        let mut touched = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            let route = self.route_key(key);
            let local_index = self
                .shard_lookup
                .get(route.shard_id)
                .copied()
                .filter(|idx| *idx != usize::MAX)
                .expect("routed key does not belong to this owned worker");
            if groups[local_index].is_empty() {
                touched.push(route.shard_id);
            }
            groups[local_index].push((index, key, route.key_hash, route.shard_id));
        }

        let mut view = OrderedBatchReadViewBuilder::new(total);

        for (local_index, batch) in groups.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            let shard = &mut self.shards[local_index];
            for (index, key, key_hash, _shard_id) in batch {
                if let Some(value) =
                    shard.get_ref_hashed_published_session_or_flat(key_hash, key, now_ms)
                {
                    view.record_hit(index, value);
                }
            }
        }

        #[cfg(feature = "telemetry")]
        self.record_batch_metrics(start, &touched);
        view.finish_owned()
    }

    pub fn batch_get_session_view(
        &mut self,
        session_prefix: &[u8],
        keys: &[Bytes],
    ) -> OwnedEmbeddedSessionBatchView {
        let key_hashes = keys.iter().map(|key| hash_key(key)).collect::<Vec<_>>();
        self.batch_get_session_view_prehashed(session_prefix, keys, &key_hashes)
    }

    pub fn batch_get_session_view_prehashed(
        &mut self,
        session_prefix: &[u8],
        keys: &[Bytes],
        key_hashes: &[u64],
    ) -> OwnedEmbeddedSessionBatchView {
        assert_eq!(
            keys.len(),
            key_hashes.len(),
            "keys and key_hashes must have matching lengths",
        );
        if keys.is_empty() {
            return OwnedEmbeddedBatchReadView {
                items: Vec::new(),
                hit_count: 0,
                total_bytes: 0,
            };
        }

        #[cfg(feature = "telemetry")]
        let start = self.metrics.as_ref().map(|_| Instant::now());
        let now_ms = now_millis();
        let route = self.route_session(session_prefix);
        let shard = self.shard_for_route_mut(route.shard_id);
        let active_session_prefix = shard
            .session_slots
            .has_session(session_prefix)
            .then_some(session_prefix);

        let mut view = BatchReadViewBuilder::new(keys.len());
        for (key, key_hash) in keys.iter().zip(key_hashes.iter().copied()) {
            view.push(shard.get_ref_hashed_active_session_or_flat(
                active_session_prefix,
                key_hash,
                key,
                now_ms,
            ));
        }

        #[cfg(feature = "telemetry")]
        self.record_batch_metrics(start, &[route.shard_id]);
        view.finish_owned()
    }

    pub fn batch_get_session_packed_view_prehashed(
        &mut self,
        session_prefix: &[u8],
        keys: &[Bytes],
        key_hashes: &[u64],
    ) -> Option<OwnedEmbeddedSessionPackedView> {
        assert_eq!(
            keys.len(),
            key_hashes.len(),
            "keys and key_hashes must have matching lengths",
        );
        if keys.is_empty() {
            return Some(OwnedEmbeddedSessionPackedView {
                buffer: EmbeddedReadSlice::from_slice(&[]),
                offsets: Vec::new(),
                lengths: Vec::new(),
                hit_count: 0,
                total_bytes: 0,
            });
        }

        let route = self.route_session(session_prefix);
        let shard = self.shard_for_route_mut(route.shard_id);
        if !shard.session_slots.has_session(session_prefix) {
            return None;
        }

        let meta =
            shard
                .session_slots
                .get_packed_view_hashed_local(session_prefix, keys, key_hashes)?;

        Some(OwnedEmbeddedSessionPackedView {
            buffer: EmbeddedReadSlice { bytes: meta.buffer },
            offsets: meta.offsets,
            lengths: meta.lengths,
            hit_count: meta.hit_count,
            total_bytes: meta.total_bytes,
        })
    }

    pub fn batch_get_session_packed_view(
        &mut self,
        session_prefix: &[u8],
        keys: &[Bytes],
    ) -> Option<OwnedEmbeddedSessionPackedView> {
        let key_hashes = keys.iter().map(|key| hash_key(key)).collect::<Vec<_>>();
        self.batch_get_session_packed_view_prehashed(session_prefix, keys, &key_hashes)
    }

    pub fn batch_get_session_packed(
        &mut self,
        session_prefix: &[u8],
        keys: &[Bytes],
    ) -> PackedBatch {
        if keys.is_empty() {
            return PackedBatch::default();
        }

        #[cfg(feature = "telemetry")]
        let start = self.metrics.as_ref().map(|_| Instant::now());
        let route = self.route_session(session_prefix);
        let now_ms = now_millis();
        let shard = self.shard_for_route_mut(route.shard_id);
        let active_session_prefix = shard
            .session_slots
            .has_session(session_prefix)
            .then_some(session_prefix);
        let mut packed = PackedBatchBuilder::new(keys.len());
        for key in keys {
            let key_hash = hash_key(key);
            packed.push(shard.get_ref_hashed_active_session_or_flat(
                active_session_prefix,
                key_hash,
                key,
                now_ms,
            ));
        }
        #[cfg(feature = "telemetry")]
        self.record_batch_metrics(start, &[route.shard_id]);
        packed.finish()
    }

    pub fn batch_get_packed(&mut self, keys: &[Bytes]) -> PackedBatch {
        let total = keys.len();
        if total == 0 {
            return PackedBatch::default();
        }

        #[cfg(feature = "telemetry")]
        let start = self.metrics.as_ref().map(|_| Instant::now());
        let now_ms = now_millis();
        let mut groups = vec![Vec::<(usize, &Bytes, u64, usize)>::new(); self.shards.len()];
        let mut touched = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            let route = self.route_key(key);
            let local_index = self
                .shard_lookup
                .get(route.shard_id)
                .copied()
                .filter(|idx| *idx != usize::MAX)
                .expect("routed key does not belong to this owned worker");
            if groups[local_index].is_empty() {
                touched.push(route.shard_id);
            }
            groups[local_index].push((index, key, route.key_hash, route.shard_id));
        }

        let mut packed = OrderedPackedBatchBuilder::new(total);

        for (local_index, batch) in groups.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            let shard = &mut self.shards[local_index];
            for (index, key, key_hash, _shard_id) in batch {
                if let Some(value) = shard.get_ref_hashed_session_or_flat(key_hash, key, now_ms) {
                    packed.record_hit(index, value);
                }
            }
        }

        #[cfg(feature = "telemetry")]
        self.record_batch_metrics(start, &touched);
        packed.finish()
    }

    pub fn set(&mut self, key: Bytes, value: Bytes, ttl_ms: Option<u64>) {
        let now_ms = now_millis();
        let route = self.route_key(&key);
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        let shard = self.shard_for_route_mut(route.shard_id);
        if let Some(session_prefix) = point_write_session_storage_prefix(&key) {
            shard
                .session_slots
                .delete_hashed(&session_prefix, route.key_hash, &key);
        }
        shard
            .map
            .set_hashed(route.key_hash, key, value, expire_at_ms, now_ms);
        shard.enforce_memory_limit(now_ms);
    }

    pub fn batch_set(&mut self, items: Vec<(Bytes, Bytes)>, ttl_ms: Option<u64>) {
        if items.is_empty() {
            return;
        }

        let now_ms = now_millis();
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        let mut groups = vec![Vec::<(Bytes, Bytes, EmbeddedKeyRoute)>::new(); self.shards.len()];
        for (key, value) in items {
            let route = self.route_key(&key);
            let local_index = self
                .shard_lookup
                .get(route.shard_id)
                .copied()
                .filter(|idx| *idx != usize::MAX)
                .expect("routed key does not belong to this owned worker");
            groups[local_index].push((key, value, route));
        }

        for (local_index, batch) in groups.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            let shard = &mut self.shards[local_index];
            for (key, value, route) in batch {
                if let Some(session_prefix) = point_write_session_storage_prefix(&key) {
                    shard
                        .session_slots
                        .delete_hashed(&session_prefix, route.key_hash, &key);
                }
                shard
                    .map
                    .set_hashed(route.key_hash, key, value, expire_at_ms, now_ms);
            }
            shard.enforce_memory_limit(now_ms);
        }
    }

    pub fn batch_set_session_owned_no_ttl(
        &mut self,
        session_prefix: Bytes,
        items: Vec<(Bytes, Bytes)>,
    ) {
        if items.is_empty() {
            return;
        }
        self.local_batch_set_session_packed_no_ttl(PackedSessionWrite::from_owned_items(
            session_prefix,
            items,
        ));
    }

    pub fn delete(&mut self, key: &[u8]) -> bool {
        let now_ms = now_millis();
        let route = self.route_key(key);
        let shard = self.shard_for_route_mut(route.shard_id);
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && shard
                .session_slots
                .delete_hashed(&session_prefix, route.key_hash, key)
        {
            return true;
        }
        shard.map.delete_hashed(route.key_hash, key, now_ms)
    }

    pub fn exists(&mut self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    #[cfg(feature = "telemetry")]
    #[inline(always)]
    fn record_batch_metrics(&self, start: Option<Instant>, touched_shards: &[usize]) {
        if let (Some(metrics), Some(start)) = (&self.metrics, start) {
            metrics.record_batch_get(start.elapsed().as_nanos() as u64);
            for &shard_id in touched_shards {
                metrics.record_batch_get_shard(shard_id);
            }
        }
    }

    fn shard_for_route_mut(&mut self, shard_id: usize) -> &mut OwnedEmbeddedShard {
        let index = self
            .shard_lookup
            .get(shard_id)
            .copied()
            .filter(|index| *index != usize::MAX)
            .expect("routed key does not belong to this owned worker");
        &mut self.shards[index]
    }
}

/// Long-lived worker-local read session for the absolute max embedded read path.
///
/// This keeps shard-local read epochs open across many routed lookups so the
/// caller can avoid begin/end-epoch overhead on every individual read. It is
/// intended for worker-owned read-heavy loops, not mixed read/write traffic.
#[derive(Debug)]
pub struct OwnedEmbeddedWorkerReadSession<'a> {
    pub(super) worker: &'a mut OwnedEmbeddedWorkerShards,
    pub(super) opened: Vec<bool>,
    pub(super) opened_indices: Vec<usize>,
}

impl<'a> OwnedEmbeddedWorkerReadSession<'a> {
    #[inline(always)]
    fn shard_for_route_mut(&mut self, shard_id: usize) -> &mut OwnedEmbeddedShard {
        let index = self
            .worker
            .shard_lookup
            .get(shard_id)
            .copied()
            .filter(|index| *index != usize::MAX)
            .expect("routed key does not belong to this owned worker");
        if !self.opened[index] {
            self.worker.shards[index].map.begin_read_epoch();
            self.opened[index] = true;
            self.opened_indices.push(index);
        }
        &mut self.worker.shards[index]
    }

    #[inline(always)]
    pub fn get_ref_no_ttl_routed(&mut self, route: EmbeddedKeyRoute, key: &[u8]) -> Option<&[u8]> {
        self.shard_for_route_mut(route.shard_id)
            .get_ref_no_ttl_hashed(route.key_hash, key)
    }
}

impl Drop for OwnedEmbeddedWorkerReadSession<'_> {
    fn drop(&mut self) {
        for index in self.opened_indices.drain(..) {
            self.worker.shards[index].map.end_read_epoch();
        }
    }
}

impl EmbeddedStore {
    pub fn bind_shard(&self, shard_id: usize) -> EmbeddedShardHandle<'_> {
        assert!(shard_id < self.shards.len(), "invalid shard id");
        EmbeddedShardHandle {
            shard_id,
            shard: self.shards[shard_id].write(),
        }
    }

    pub fn into_owned_shards(self) -> Vec<OwnedEmbeddedShard> {
        self.shards
            .into_iter()
            .enumerate()
            .map(|(shard_id, shard)| {
                let shard = shard.into_inner().into_inner();
                OwnedEmbeddedShard {
                    shard_id,
                    map: shard.map,
                    session_slots: shard.session_slots,
                    memory_limit_bytes: shard.memory_limit_bytes,
                    eviction_policy: shard.eviction_policy,
                }
            })
            .collect()
    }

    pub fn into_owned_workers(self, worker_count: usize) -> Vec<OwnedEmbeddedWorkerShards> {
        let worker_count = worker_count.max(1);
        let route_mode = self.route_mode;
        let shard_count = self.shard_count();
        #[cfg(feature = "telemetry")]
        let metrics = self.metrics.clone();
        let mut buckets = (0..worker_count)
            .map(|_| Vec::<OwnedEmbeddedShard>::new())
            .collect::<Vec<_>>();

        for shard in self.into_owned_shards() {
            buckets[shard.shard_id() % worker_count].push(shard);
        }

        buckets
            .into_iter()
            .map(|shards| {
                OwnedEmbeddedWorkerShards::new(
                    route_mode,
                    shard_count,
                    shards,
                    #[cfg(feature = "telemetry")]
                    metrics.clone(),
                )
            })
            .collect()
    }
}
