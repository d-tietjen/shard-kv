use std::sync::Arc;

use crossbeam_utils::CachePadded;
#[cfg(not(feature = "embedded-read-biased-lock"))]
use parking_lot::RwLock;
#[cfg(feature = "embedded-read-biased-lock")]
use rblock::RwLock;

use super::*;

/// Shared embedded store whose sharing boundary is each storage shard.
///
/// This is a benchmark-oriented middle ground between `Arc<EmbeddedStore>` and
/// owner-local `LocalEmbeddedStore`: callers still coordinate with a per-shard
/// lock, but the reference-counted handles live at the shard level instead of
/// around the whole store.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct ShardArcEmbeddedStore {
    shards: Box<[Arc<CachePadded<RwLock<EmbeddedShard>>>]>,
    shift: u32,
    route_mode: EmbeddedRouteMode,
}

impl ShardArcEmbeddedStore {
    pub fn new(shard_count: usize) -> Self {
        Self::with_route_mode(shard_count, EmbeddedRouteMode::FullKey)
    }

    pub fn with_route_mode(shard_count: usize, route_mode: EmbeddedRouteMode) -> Self {
        assert_valid_shard_count(shard_count);
        let shift = shift_for(shard_count);
        let shards = (0..shard_count)
            .map(|shard_id| {
                Arc::new(CachePadded::new(RwLock::new(EmbeddedShard::with_limits(
                    shard_id,
                    None,
                    EvictionPolicy::None,
                    None,
                ))))
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            shards,
            shift,
            route_mode,
        }
    }

    #[inline(always)]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    #[inline(always)]
    pub fn route_mode(&self) -> EmbeddedRouteMode {
        self.route_mode
    }

    #[inline(always)]
    pub fn route_key(&self, key: &[u8]) -> EmbeddedKeyRoute {
        compute_key_route(self.route_mode, self.shift, key)
    }

    #[inline(always)]
    fn route_hash(&self, hash: u64) -> usize {
        stripe_index(hash, self.shift)
    }

    pub fn configure_memory_policy(
        &self,
        per_shard_memory_limit_bytes: Option<usize>,
        eviction_policy: EvictionPolicy,
    ) {
        let now_ms = now_millis();
        for shard in &self.shards {
            shard.write().configure_memory_policy(
                per_shard_memory_limit_bytes,
                eviction_policy,
                now_ms,
            );
        }
    }

    pub fn set_slice_routed_no_ttl(&self, route: EmbeddedKeyRoute, key: &[u8], value: &[u8]) {
        debug_assert!(route.shard_id < self.shards.len());
        self.set_slice_routed(route, key, value, None);
    }

    pub fn set_slice_prehashed(
        &self,
        key_hash: u64,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
    ) {
        if !can_route_with_key_hash(self.route_mode, self.shards.len(), key) {
            let route = self.route_key(key);
            self.set_slice_routed(route, key, value, ttl_ms);
            return;
        }
        let route = EmbeddedKeyRoute {
            shard_id: self.route_hash(key_hash),
            key_hash,
        };
        self.set_slice_routed(route, key, value, ttl_ms);
    }

    pub fn set_slice_routed(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
    ) {
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
    }

    pub fn contains_routed_no_ttl(&self, route: EmbeddedKeyRoute, key: &[u8]) -> bool {
        debug_assert!(route.shard_id < self.shards.len());
        if uses_flat_key_storage(self.route_mode, key) {
            let shard = self.shards[route.shard_id].read();
            return shard
                .map
                .get_ref_hashed_shared_no_ttl(route.key_hash, key)
                .is_some();
        }
        let mut shard = self.shards[route.shard_id].write();
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && shard
                .session_slots
                .get_ref_hashed(&session_prefix, route.key_hash, key)
                .is_some()
        {
            return true;
        }
        shard.map.get_ref_hashed(route.key_hash, key, 0).is_some()
    }

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
        let route = self.route_key(key);
        self.get_blob_string_routed_into(route, key, out)
    }

    pub fn get_blob_string_routed_into(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        out: &mut bytes::BytesMut,
    ) -> bool {
        if uses_flat_key_storage(self.route_mode, key) {
            let shard = self.shards[route.shard_id].read();
            let now_ms = if shard.map.has_no_ttl_entries() {
                0
            } else {
                now_millis()
            };
            if let Some(value) = shard.map.get_ref_hashed_shared(route.key_hash, key, now_ms) {
                write_resp_blob_string_into(out, value);
                return true;
            }
            return false;
        }

        let mut shard = self.shards[route.shard_id].write();
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && let Some(value) =
                shard
                    .session_slots
                    .get_ref_hashed(&session_prefix, route.key_hash, key)
        {
            write_resp_blob_string_into(out, value);
            return true;
        }
        if let Some(value) = shard.map.get_ref_hashed(route.key_hash, key, now_millis()) {
            write_resp_blob_string_into(out, value);
            return true;
        }
        false
    }
}
