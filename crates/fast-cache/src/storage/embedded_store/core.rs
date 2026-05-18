use super::*;

impl EmbeddedStore {
    /// Creates an embedded store with full-key routing.
    pub fn new(shard_count: usize) -> Self {
        Self::with_route_mode(shard_count, EmbeddedRouteMode::FullKey)
    }

    /// Creates an embedded store with an explicit routing mode.
    pub fn with_route_mode(shard_count: usize, route_mode: EmbeddedRouteMode) -> Self {
        #[cfg(feature = "telemetry")]
        {
            Self::with_route_mode_and_metrics(shard_count, route_mode, None)
        }
        #[cfg(not(feature = "telemetry"))]
        {
            assert_valid_shard_count(shard_count);
            let shift = shift_for(shard_count);
            let shards = (0..shard_count)
                .map(|shard_id| {
                    CachePadded::new(RwLock::new(EmbeddedShard::with_limits(
                        shard_id,
                        None,
                        EvictionPolicy::None,
                        None,
                    )))
                })
                .collect::<Vec<_>>()
                .into_boxed_slice();
            Self {
                shards,
                shift,
                objects: RedisObjectStore::new(shard_count),
                route_mode,
            }
        }
    }

    #[cfg(feature = "telemetry")]
    pub fn with_route_mode_and_metrics(
        shard_count: usize,
        route_mode: EmbeddedRouteMode,
        metrics: Option<Arc<CacheTelemetry>>,
    ) -> Self {
        Self::with_route_mode_and_metrics_shard_offset(shard_count, route_mode, metrics, 0)
    }

    #[cfg(feature = "telemetry")]
    pub fn with_route_mode_and_metrics_shard_offset(
        shard_count: usize,
        route_mode: EmbeddedRouteMode,
        metrics: Option<Arc<CacheTelemetry>>,
        shard_id_base: usize,
    ) -> Self {
        assert_valid_shard_count(shard_count);
        let shift = shift_for(shard_count);
        let shards = (0..shard_count)
            .map(|shard_id| {
                let mut shard =
                    EmbeddedShard::with_limits(shard_id, None, EvictionPolicy::None, None);
                if let Some(metrics) = &metrics {
                    shard.map.attach_metrics(
                        CacheTelemetryHandle::from_arc(metrics),
                        shard_id_base + shard_id,
                    );
                }
                CachePadded::new(RwLock::new(shard))
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            shards,
            shift,
            objects: RedisObjectStore::new(shard_count),
            route_mode,
            metrics,
        }
    }

    /// Returns the number of storage shards.
    #[inline(always)]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Returns the number of currently live keys and session entries.
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                let shard = shard.read();
                shard.map.len().saturating_add(shard.session_slots.len())
            })
            .sum::<usize>()
            + self.objects.object_count()
    }

    /// Returns a sorted snapshot of currently live keys.
    pub fn key_snapshot(&self) -> Vec<Bytes> {
        let now_ms = now_millis();
        let mut keys = Vec::with_capacity(self.len());
        for shard in &self.shards {
            let shard = shard.read();
            keys.extend(
                shard
                    .map
                    .snapshot_entries(now_ms)
                    .into_iter()
                    .map(|entry| entry.key),
            );
        }
        keys.extend(
            self.objects
                .keys_with_type(now_ms)
                .into_iter()
                .map(|(key, _)| key),
        );
        keys.sort();
        keys
    }

    /// Returns currently live string entries for persistence or replication.
    ///
    /// Redis object values are intentionally excluded; the native replication
    /// stream v1 covers byte-string cache mutations.
    pub fn entry_snapshot(&self) -> Vec<StoredEntry> {
        let now_ms = now_millis();
        let mut entries = Vec::new();
        for shard in &self.shards {
            let shard = shard.read();
            entries.extend(shard.map.snapshot_entries(now_ms));
        }
        entries.sort_by_key(|entry| hash_key(entry.key.as_ref()));
        entries
    }

    /// Returns the approximate number of bytes stored in string values.
    pub fn stored_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.read().stored_bytes())
            .sum()
    }

    /// Applies a per-shard memory budget and eviction policy.
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

    /// Returns true when the store has no live keys.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the configured route mode.
    #[inline(always)]
    pub fn route_mode(&self) -> EmbeddedRouteMode {
        self.route_mode
    }

    /// Computes the route for a session prefix.
    #[inline(always)]
    pub fn route_session(&self, session_prefix: &[u8]) -> EmbeddedSessionRoute {
        EmbeddedSessionRoute {
            shard_id: compute_session_shard(self.shift, session_prefix),
        }
    }

    #[inline(always)]
    pub(crate) fn route_key_prehashed(&self, key_hash: u64, key: &[u8]) -> EmbeddedKeyRoute {
        if can_route_with_key_hash(self.route_mode, self.shard_count(), key) {
            EmbeddedKeyRoute {
                shard_id: self.route_hash(key_hash),
                key_hash,
            }
        } else {
            self.route_key(key)
        }
    }

    #[cfg(feature = "telemetry")]
    #[inline(always)]
    pub fn metrics(&self) -> Option<Arc<CacheTelemetry>> {
        self.metrics.clone()
    }

    #[cfg(feature = "telemetry")]
    pub fn export_metrics_prometheus(&self) -> Option<String> {
        self.metrics
            .as_ref()
            .map(|metrics| metrics.export_prometheus())
    }

    #[cfg(feature = "telemetry")]
    pub fn metrics_snapshot(&self) -> Option<crate::storage::CacheMetricsSnapshot> {
        self.metrics.as_ref().map(|metrics| metrics.snapshot())
    }

    /// Computes the route for a key.
    #[inline(always)]
    pub fn route_key(&self, key: &[u8]) -> EmbeddedKeyRoute {
        compute_key_route(self.route_mode, self.shift, key)
    }

    /// Returns the session-routing prefix for a key.
    ///
    /// For `s:<session>:c:<chunk>` keys this is `s:<session>`. For non-session
    /// keys this returns the whole key, matching `SessionPrefix` route mode.
    #[inline(always)]
    pub(crate) fn session_route_prefix_for_key<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        session_route_prefix(key)
    }

    /// Precomputes route and fingerprint metadata for repeated point lookups.
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
    pub(super) fn route_hash(&self, hash: u64) -> usize {
        stripe_index(hash, self.shift)
    }

    #[inline(always)]
    pub(super) fn hashes_for_key(&self, key: &[u8]) -> (u64, u64) {
        let key_hash = hash_key(key);
        let route_hash = match self.route_mode {
            EmbeddedRouteMode::FullKey => key_hash,
            EmbeddedRouteMode::SessionPrefix => hash_key(session_route_prefix(key)),
        };
        (route_hash, key_hash)
    }

    pub(super) fn single_shard_batch_route(&self, keys: &[Bytes]) -> Option<usize> {
        if self.route_mode != EmbeddedRouteMode::SessionPrefix || keys.is_empty() {
            return None;
        }

        // Session-routed batches only get the fast path when every key shares the
        // same `s:<session>` prefix. Otherwise we fall back to generic grouping.
        let first_prefix = session_route_prefix(&keys[0]);
        let first_shard = self.route_hash(hash_key(first_prefix));
        if keys[1..]
            .iter()
            .all(|key| session_route_prefix(key.as_slice()) == first_prefix)
        {
            Some(first_shard)
        } else {
            None
        }
    }

    #[cfg(test)]
    #[inline(always)]
    pub(super) fn shard_for_key(&self, key: &[u8]) -> usize {
        let (route_hash, _) = self.hashes_for_key(key);
        self.route_hash(route_hash)
    }

    #[cfg(feature = "telemetry")]
    #[inline(always)]
    pub(super) fn record_batch_metrics(&self, start: Option<Instant>, touched_shards: &[usize]) {
        if let (Some(metrics), Some(start)) = (&self.metrics, start) {
            metrics.record_batch_get(start.elapsed().as_nanos() as u64);
            for &shard_id in touched_shards {
                metrics.record_batch_get_shard(shard_id);
            }
        }
    }
}
