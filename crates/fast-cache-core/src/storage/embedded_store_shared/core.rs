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
                    CachePadded::new(SharedShardLock::new(
                        EmbeddedShard::with_limits(
                            shard_id,
                            per_shard_limit,
                            eviction_policy,
                            per_shard_capacity,
                        ),
                        lock_policy,
                    ))
                }),
                shift: shift_for(SHARDS),
                route_mode,
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
}
