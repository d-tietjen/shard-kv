use crate::config::EvictionPolicy;
#[cfg(feature = "redis")]
use crate::storage::Bytes;
use crate::storage::{FlatMap, SemanticCacheError, SemanticEmbedding, SemanticMatch};

use super::{EmbeddedRouteMode, SessionSlotMap, derived_session_storage_prefix};

#[derive(Debug, Default)]
pub(crate) struct EmbeddedShard {
    pub(super) map: FlatMap,
    pub(super) session_slots: SessionSlotMap,
    pub(super) memory_limit_bytes: Option<usize>,
    pub(super) semantic_memory_limit_bytes: Option<usize>,
    pub(super) eviction_policy: EvictionPolicy,
}

impl EmbeddedShard {
    pub(crate) fn with_limits(
        _shard_id: usize,
        memory_limit_bytes: Option<usize>,
        eviction_policy: EvictionPolicy,
        capacity_hint: Option<usize>,
    ) -> Self {
        let map = capacity_hint.map_or_else(FlatMap::new, FlatMap::with_capacity);
        let mut shard = Self {
            map,
            session_slots: SessionSlotMap::default(),
            memory_limit_bytes: None,
            semantic_memory_limit_bytes: None,
            eviction_policy: EvictionPolicy::None,
        };
        shard.configure_memory_policy(memory_limit_bytes, eviction_policy, 0);
        shard
    }

    #[inline(always)]
    pub(crate) fn stored_bytes(&self) -> usize {
        self.map
            .stored_bytes()
            .saturating_add(self.session_slots.stored_bytes())
    }

    #[inline(always)]
    pub(crate) fn visit_string_keys(
        &self,
        now_ms: u64,
        visitor: &mut impl FnMut(&[u8]) -> bool,
    ) -> bool {
        self.map.visit_keys(now_ms, visitor)
    }

    #[inline(always)]
    pub(crate) fn visit_string_entries(
        &self,
        now_ms: u64,
        visitor: &mut impl FnMut(&[u8], &[u8], Option<u64>) -> bool,
    ) -> bool {
        self.map.visit_entries(now_ms, visitor)
    }

    #[inline(always)]
    pub(crate) fn get_ref_hashed_shared_no_ttl(&self, hash: u64, key: &[u8]) -> Option<&[u8]> {
        self.map.get_ref_hashed_shared_no_ttl(hash, key)
    }

    #[inline(always)]
    pub(crate) fn get_ref_hashed_shared_prepared_no_ttl(
        &self,
        hash: u64,
        key: &[u8],
        key_tag: u64,
    ) -> Option<&[u8]> {
        self.map
            .get_ref_hashed_shared_prepared_no_ttl(hash, key, key_tag)
    }

    #[inline(always)]
    pub(crate) fn get_shared_value_bytes_hashed_no_ttl(
        &self,
        hash: u64,
        key: &[u8],
    ) -> Option<&bytes::Bytes> {
        self.map.get_shared_value_bytes_hashed_no_ttl(hash, key)
    }

    #[inline(always)]
    pub(crate) fn get_shared_value_bytes_hashed_prepared_no_ttl(
        &self,
        hash: u64,
        key: &[u8],
        key_tag: u64,
    ) -> Option<&bytes::Bytes> {
        self.map
            .get_shared_value_bytes_hashed_prepared_no_ttl(hash, key, key_tag)
    }

    #[cfg(feature = "mutable-value-slices")]
    #[inline(always)]
    pub(crate) fn value_mut_hashed_no_ttl(&mut self, hash: u64, key: &[u8]) -> Option<&mut [u8]> {
        self.map.value_mut_hashed_no_ttl(hash, key)
    }

    #[cfg(feature = "redis")]
    #[inline(always)]
    pub(crate) fn update_value_hashed_no_ttl<R>(
        &mut self,
        hash: u64,
        key: &[u8],
        update: impl FnOnce(&mut [u8]) -> R,
    ) -> Option<R> {
        self.map.update_value_hashed_no_ttl(hash, key, update)
    }

    #[cfg(feature = "redis")]
    #[inline(always)]
    pub(crate) fn transform_value_hashed_no_ttl<R, E>(
        &mut self,
        hash: u64,
        key: &[u8],
        now_ms: u64,
        transform: impl FnOnce(Option<&[u8]>) -> std::result::Result<(R, Bytes), E>,
    ) -> std::result::Result<R, E> {
        self.map
            .transform_value_hashed_no_ttl(hash, key, now_ms, transform)
    }

    #[inline(always)]
    pub(crate) fn get_ref_hashed_shared(
        &self,
        hash: u64,
        key: &[u8],
        now_ms: u64,
    ) -> Option<&[u8]> {
        self.map.get_ref_hashed_shared(hash, key, now_ms)
    }

    #[inline(always)]
    #[cfg_attr(feature = "no-ttl", allow(dead_code))]
    pub(crate) fn get_shared_value_bytes_hashed(
        &self,
        hash: u64,
        key: &[u8],
        now_ms: u64,
    ) -> Option<&bytes::Bytes> {
        self.map.get_shared_value_bytes_hashed(hash, key, now_ms)
    }

    #[inline(always)]
    #[cfg_attr(feature = "no-ttl", allow(dead_code))]
    pub(crate) fn has_no_ttl_entries(&self) -> bool {
        self.map.has_no_ttl_entries()
    }

    #[inline(always)]
    pub(crate) fn entry_expire_at_hashed(
        &mut self,
        hash: u64,
        key: &[u8],
        now_ms: u64,
    ) -> Option<Option<u64>> {
        self.map.entry_expire_at_hashed(hash, key, now_ms)
    }

    #[inline(always)]
    #[allow(dead_code)]
    pub(crate) fn entry_expire_at_hashed_no_ttl(
        &mut self,
        hash: u64,
        key: &[u8],
    ) -> Option<Option<u64>> {
        self.map.entry_expire_at_hashed_no_ttl(hash, key)
    }

    #[inline(always)]
    pub(crate) fn get_ref_hashed_session_or_flat(
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
    pub(crate) fn get_ref_hashed_published_session_or_flat(
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
    pub(crate) fn get_ref_hashed_active_session_or_flat(
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
    pub(crate) fn set_value_bytes_hashed_no_ttl(
        &mut self,
        route_mode: EmbeddedRouteMode,
        key_hash: u64,
        key: &[u8],
        value: bytes::Bytes,
    ) {
        if route_mode == EmbeddedRouteMode::SessionPrefix
            && let Some(session_prefix) = derived_session_storage_prefix(key)
        {
            self.session_slots
                .delete_hashed(&session_prefix, key_hash, key);
        }
        self.map.set_bytes_hashed(key_hash, key, value, None, 0);
        self.enforce_memory_limit(0);
    }

    #[inline(always)]
    pub(crate) fn set_value_bytes_hashed(
        &mut self,
        route_mode: EmbeddedRouteMode,
        key_hash: u64,
        key: &[u8],
        value: bytes::Bytes,
        expire_at_ms: Option<u64>,
        now_ms: u64,
    ) {
        if route_mode == EmbeddedRouteMode::SessionPrefix
            && let Some(session_prefix) = derived_session_storage_prefix(key)
        {
            self.session_slots
                .delete_hashed(&session_prefix, key_hash, key);
        }
        self.map
            .set_bytes_hashed(key_hash, key, value, expire_at_ms, now_ms);
        self.enforce_memory_limit(now_ms);
    }

    #[inline(always)]
    pub(crate) fn set_slice_hashed_no_ttl(
        &mut self,
        route_mode: EmbeddedRouteMode,
        key_hash: u64,
        key: &[u8],
        value: &[u8],
    ) {
        if route_mode == EmbeddedRouteMode::SessionPrefix
            && let Some(session_prefix) = derived_session_storage_prefix(key)
        {
            self.session_slots
                .delete_hashed(&session_prefix, key_hash, key);
        }
        self.map.set_slice_hashed_no_ttl(key_hash, key, value);
        self.enforce_memory_limit(0);
    }

    #[inline(always)]
    #[cfg_attr(feature = "no-ttl", allow(dead_code))]
    pub(crate) fn set_slice_hashed(
        &mut self,
        route_mode: EmbeddedRouteMode,
        key_hash: u64,
        key: &[u8],
        value: &[u8],
        expire_at_ms: Option<u64>,
        now_ms: u64,
    ) {
        if route_mode == EmbeddedRouteMode::SessionPrefix
            && let Some(session_prefix) = derived_session_storage_prefix(key)
        {
            self.session_slots
                .delete_hashed(&session_prefix, key_hash, key);
        }
        self.map
            .set_slice_hashed(key_hash, key, value, expire_at_ms, now_ms);
        self.enforce_memory_limit(now_ms);
    }

    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    pub(crate) fn set_semantic_slice_hashed(
        &mut self,
        route_mode: EmbeddedRouteMode,
        key_hash: u64,
        key: &[u8],
        value: &[u8],
        embedding: &[f32],
        expire_at_ms: Option<u64>,
        now_ms: u64,
    ) -> Result<(), SemanticCacheError> {
        if route_mode == EmbeddedRouteMode::SessionPrefix
            && let Some(session_prefix) = derived_session_storage_prefix(key)
        {
            self.session_slots
                .delete_hashed(&session_prefix, key_hash, key);
        }
        self.map.set_semantic_slice_hashed(
            key_hash,
            key,
            value,
            embedding,
            expire_at_ms,
            now_ms,
        )?;
        self.enforce_semantic_memory_limit(now_ms);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    pub(crate) fn set_semantic_slice_hashed_with_governance(
        &mut self,
        route_mode: EmbeddedRouteMode,
        key_hash: u64,
        key: &[u8],
        value: &[u8],
        embedding: &[f32],
        governance_metadata: &[u8],
        expire_at_ms: Option<u64>,
        now_ms: u64,
    ) -> Result<(), SemanticCacheError> {
        if route_mode == EmbeddedRouteMode::SessionPrefix
            && let Some(session_prefix) = derived_session_storage_prefix(key)
        {
            self.session_slots
                .delete_hashed(&session_prefix, key_hash, key);
        }
        self.map.set_semantic_slice_hashed_with_governance(
            key_hash,
            key,
            value,
            embedding,
            governance_metadata,
            expire_at_ms,
            now_ms,
        )?;
        self.enforce_semantic_memory_limit(now_ms);
        Ok(())
    }

    #[inline(always)]
    pub(crate) fn semantic_search(
        &self,
        query: &SemanticEmbedding,
        min_score: f32,
        now_ms: u64,
    ) -> Option<SemanticMatch> {
        self.map.semantic_search(query, min_score, now_ms)
    }

    #[inline(always)]
    pub(crate) fn semantic_search_with_governance_filter(
        &self,
        query: &SemanticEmbedding,
        min_score: f32,
        now_ms: u64,
        governance_filter: impl FnMut(Option<&[u8]>) -> bool,
    ) -> Option<SemanticMatch> {
        self.map
            .semantic_search_with_governance_filter(query, min_score, now_ms, governance_filter)
    }

    #[inline(always)]
    pub(crate) fn semantic_search_exact(
        &self,
        query: &SemanticEmbedding,
        min_score: f32,
        now_ms: u64,
    ) -> Option<SemanticMatch> {
        self.map.semantic_search_exact(query, min_score, now_ms)
    }

    #[inline(always)]
    pub(crate) fn semantic_search_exact_with_governance_filter(
        &self,
        query: &SemanticEmbedding,
        min_score: f32,
        now_ms: u64,
        governance_filter: impl FnMut(Option<&[u8]>) -> bool,
    ) -> Option<SemanticMatch> {
        self.map.semantic_search_exact_with_governance_filter(
            query,
            min_score,
            now_ms,
            governance_filter,
        )
    }

    #[inline(always)]
    pub(crate) fn remove_value_hashed(
        &mut self,
        key_hash: u64,
        key: &[u8],
        now_ms: u64,
    ) -> Option<bytes::Bytes> {
        let removed = self.map.remove_value_hashed(key_hash, key, now_ms);
        if removed.is_some() {
            self.enforce_memory_limit(now_ms);
        }
        removed
    }

    #[inline(always)]
    pub(crate) fn get_session_ref_hashed_shared_no_ttl(
        &self,
        session_prefix: &[u8],
        key_hash: u64,
        key: &[u8],
    ) -> Option<&[u8]> {
        self.session_slots
            .get_ref_hashed_shared(session_prefix, key_hash, key)
    }

    #[inline(always)]
    pub(crate) fn get_session_ref_hashed_shared_no_ttl_prehashed(
        &self,
        session_hash: u64,
        session_prefix: &[u8],
        key_hash: u64,
        key: &[u8],
    ) -> Option<&[u8]> {
        self.session_slots.get_ref_hashed_shared_prehashed(
            session_hash,
            session_prefix,
            key_hash,
            key,
        )
    }

    #[inline(always)]
    pub(crate) fn set_session_slice_hashed_no_ttl(
        &mut self,
        session_prefix: &[u8],
        key_hash: u64,
        key: &[u8],
        value: &[u8],
    ) {
        if !self.map.is_empty() {
            self.map.delete_hashed(key_hash, key, 0);
        }
        self.session_slots
            .set_slice_hashed(session_prefix, key_hash, key, value);
        self.enforce_memory_limit(0);
    }

    #[inline(always)]
    pub(crate) fn set_session_slice_hashed_no_ttl_prehashed(
        &mut self,
        session_hash: u64,
        session_prefix: &[u8],
        key_hash: u64,
        key: &[u8],
        value: &[u8],
    ) {
        if !self.map.is_empty() {
            self.map.delete_hashed(key_hash, key, 0);
        }
        self.session_slots.set_slice_hashed_prehashed(
            session_hash,
            session_prefix,
            key_hash,
            key,
            value,
        );
        self.enforce_memory_limit(0);
    }

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

    pub(super) fn configure_memory_policy(
        &mut self,
        memory_limit_bytes: Option<usize>,
        eviction_policy: EvictionPolicy,
        now_ms: u64,
    ) {
        self.memory_limit_bytes = memory_limit_bytes.filter(|limit| *limit > 0);
        self.eviction_policy = eviction_policy;
        self.session_slots.configure_access_tracking(
            self.memory_limit_bytes.is_some() && self.eviction_policy != EvictionPolicy::None,
        );
        self.update_lazy_read_sampling();
        self.map
            .configure_memory_policy(None, eviction_policy, now_ms);
        self.enforce_memory_limit(now_ms);
    }

    pub(crate) fn configure_semantic_memory_policy(
        &mut self,
        memory_limit_bytes: Option<usize>,
        now_ms: u64,
    ) {
        self.semantic_memory_limit_bytes = memory_limit_bytes.filter(|limit| *limit > 0);
        self.enforce_semantic_memory_limit(now_ms);
    }

    #[inline(always)]
    pub(super) fn enforce_memory_limit(&mut self, now_ms: u64) {
        let Some(limit) = self.memory_limit_bytes else {
            return;
        };
        self.enforce_memory_limit_to(limit, now_ms);
    }

    #[inline(always)]
    pub(super) fn enforce_semantic_memory_limit(&mut self, now_ms: u64) {
        let Some(limit) = self.semantic_memory_limit_bytes.or(self.memory_limit_bytes) else {
            return;
        };
        self.enforce_memory_limit_to(limit, now_ms);
    }

    #[inline(always)]
    fn enforce_memory_limit_to(&mut self, limit: usize, now_ms: u64) {
        self.update_lazy_read_sampling();
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
                (
                    Some((map_rank, _map_hash, _map_key)),
                    Some((session_rank, _session_prefix, _session_hash, _session_key)),
                ) => {
                    if session_rank < map_rank {
                        self.session_slots.evict_with_policy(self.eviction_policy)
                    } else {
                        self.map.evict_with_policy(self.eviction_policy, now_ms)
                    }
                }
                (Some((_rank, _map_hash, _map_key)), None) => {
                    self.map.evict_with_policy(self.eviction_policy, now_ms)
                }
                (None, Some((_rank, _session_prefix, _session_hash, _session_key))) => {
                    self.session_slots.evict_with_policy(self.eviction_policy)
                }
                (None, None) => false,
            };
            if !evicted {
                break;
            }
            if self.stored_bytes() <= limit {
                break;
            }
        }
        self.update_lazy_read_sampling();
    }
}
