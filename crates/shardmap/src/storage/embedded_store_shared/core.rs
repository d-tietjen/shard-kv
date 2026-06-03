use super::*;

impl<const SHARDS: usize> Clone for SharedEmbeddedStore<SHARDS> {
    #[inline(always)]
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<const SHARDS: usize> SharedEmbeddedStore<SHARDS> {
    /// Creates a cloneable shared embedded store with `SHARDS` lock stripes.
    pub fn new(config: SharedEmbeddedConfig) -> Self {
        const {
            assert!(
                SHARDS > 0 && SHARDS.is_power_of_two(),
                "SHARDS must be a non-zero power of two"
            );
        }
        assert_valid_shard_count(SHARDS);

        let per_shard_limit = config
            .total_memory_bytes
            .map(|total| total.div_ceil(SHARDS).max(1));
        let per_shard_capacity = config
            .flat_map_capacity_hint
            .map(|capacity| capacity.div_ceil(SHARDS));
        let eviction_policy = config.eviction_policy;
        let route_mode = config.route_mode;
        let lock_policy = config.lock_policy;

        Self {
            inner: Arc::new(SharedInner {
                shards: array::from_fn(|shard_id| {
                    let mut shard = EmbeddedShard::with_limits(
                        shard_id,
                        per_shard_limit,
                        eviction_policy,
                        per_shard_capacity,
                    );
                    if shard_id == 0 {
                        shard.configure_semantic_memory_policy(config.total_memory_bytes, 0);
                    }
                    CachePadded::new(SharedShardLock::new(shard, lock_policy))
                }),
                shift: shift_for(SHARDS),
                route_mode,
                semantic_generation: AtomicU64::new(0),
                semantic_query_cache_enabled: AtomicBool::new(true),
                semantic_query_cache: FairRwLock::new(SemanticQueryCache::default()),
            }),
        }
    }

    /// Returns the number of cache stripes.
    #[inline(always)]
    pub const fn shard_count(&self) -> usize {
        SHARDS
    }

    /// Returns the configured route mode.
    #[inline(always)]
    pub fn route_mode(&self) -> EmbeddedRouteMode {
        self.inner.route_mode
    }

    /// Computes the route for a key.
    #[inline(always)]
    pub fn route_key(&self, key: &[u8]) -> EmbeddedKeyRoute {
        compute_key_route(self.inner.route_mode, self.inner.shift, key)
    }

    /// Computes the route for a session prefix.
    #[inline(always)]
    pub fn route_session(&self, session_prefix: &[u8]) -> EmbeddedSessionRoute {
        EmbeddedSessionRoute {
            shard_id: compute_session_shard(self.inner.shift, session_prefix),
        }
    }

    #[inline(always)]
    pub(super) fn stripe(&self, shard_id: usize) -> &SharedShardLock<EmbeddedShard> {
        &self.inner.shards[shard_id]
    }

    #[inline(always)]
    pub(super) const fn semantic_shard_id(&self) -> usize {
        0
    }

    #[inline(always)]
    pub(super) fn semantic_shadow(
        &self,
        route: EmbeddedKeyRoute,
    ) -> Option<&SharedShardLock<EmbeddedShard>> {
        (route.shard_id != self.semantic_shard_id()).then(|| self.stripe(self.semantic_shard_id()))
    }

    #[inline(always)]
    pub(super) fn invalidate_semantic_shadow(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        now_ms: u64,
    ) -> Option<SharedBytes> {
        let semantic_shard = self.semantic_shadow(route)?;
        semantic_shard
            .write()
            .remove_value_hashed(route.key_hash, key, now_ms)
    }

    #[inline(always)]
    pub(super) fn semantic_generation(&self) -> u64 {
        self.inner.semantic_generation.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub(super) fn bump_semantic_generation(&self) {
        self.inner
            .semantic_generation
            .fetch_add(1, Ordering::AcqRel);
    }

    #[inline(always)]
    pub fn semantic_query_cache_enabled(&self) -> bool {
        self.inner
            .semantic_query_cache_enabled
            .load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn disable_semantic_query_cache(&self) {
        self.inner
            .semantic_query_cache_enabled
            .store(false, Ordering::Release);
    }

    /// Returns a sorted snapshot of currently live point keys.
    pub fn key_snapshot(&self) -> Vec<SharedBytes> {
        let mut keys = Vec::new();
        self.visit_string_keys(|key| {
            keys.push(SharedBytes::copy_from_slice(key));
            true
        });
        keys.sort();
        keys
    }

    /// Visits currently live point keys without allocating a key snapshot.
    ///
    /// The visitor runs while each shard read lock is held. Keep callbacks
    /// lightweight, and return `false` to stop early.
    pub fn visit_string_keys(&self, mut visitor: impl FnMut(&[u8]) -> bool) {
        let now_ms = ttl_now_millis();
        for shard in &self.inner.shards {
            let shard = shard.read();
            if !shard.visit_string_keys(now_ms, &mut visitor) {
                return;
            }
        }
    }

    /// Visits currently live point entries without cloning keys or values.
    ///
    /// The visitor receives `(key, value, expire_at_ms)` while each shard read
    /// lock is held. Keep callbacks lightweight, and return `false` to stop
    /// early.
    pub fn visit_string_entries(&self, mut visitor: impl FnMut(&[u8], &[u8], Option<u64>) -> bool) {
        let now_ms = ttl_now_millis();
        for shard in &self.inner.shards {
            let shard = shard.read();
            if !shard.visit_string_entries(now_ms, &mut visitor) {
                return;
            }
        }
    }
}
