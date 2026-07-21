use super::*;

impl EmbeddedStore {
    /// Deletes a key and returns true when a value or object was removed.
    pub fn delete(&self, key: &[u8]) -> bool {
        if !self.point_mutation_is_replicable(key, 0, None) {
            return false;
        }
        let now_ms = now_millis();
        let route = self.route_key(key);
        #[cfg(feature = "redis")]
        let is_vector = self.clone_vector_value(key).is_some();
        let deleted = self.delete_routed_then(route, key, now_ms, || {
            #[cfg(feature = "redis")]
            if is_vector {
                return;
            }
            self.notify_point_mutation(PointMutationKind::Delete, key, None, None, None);
        });
        #[cfg(feature = "redis")]
        let deleted = deleted || self.delete_pinned_vector_value_if_distinct(route, key, now_ms);
        #[cfg(feature = "redis")]
        if deleted && is_vector {
            self.notify_vector_mutation(VectorMutationKind::Delete, key, None, None);
        }
        deleted
    }

    /// Evicts one point entry from `shard_id` according to `policy` and
    /// returns its key.
    ///
    /// This is intended for external stores whose payload memory is owned by
    /// another allocator. The caller can use Shardmap for recency and victim
    /// selection, then release the corresponding external allocation without
    /// relying on the cache's byte-budget hysteresis. Session and Redis object
    /// entries are intentionally excluded.
    pub fn evict_one_point_in_shard(
        &self,
        shard_id: usize,
        policy: EvictionPolicy,
    ) -> Option<Bytes> {
        let shard = self.shards.get(shard_id)?;
        let mut shard = shard.write();
        shard.map.evict_one_with_policy(policy, now_millis())
    }

    /// Evicts the coldest point entry accepted by `eligible` from one shard.
    ///
    /// External overflow tiers use this to retain values until a remote write
    /// is acknowledged. Session and Redis object entries are excluded.
    pub fn evict_one_point_in_shard_if(
        &self,
        shard_id: usize,
        policy: EvictionPolicy,
        eligible: impl FnMut(&[u8]) -> bool,
    ) -> Option<Bytes> {
        let shard = self.shards.get(shard_id)?;
        let mut shard = shard.write();
        shard
            .map
            .evict_one_with_policy_if(policy, now_millis(), eligible)
    }

    #[cfg(feature = "redis")]
    pub(super) fn delete_pinned_vector_value_if_distinct(
        &self,
        primary_route: EmbeddedKeyRoute,
        key: &[u8],
        now_ms: u64,
    ) -> bool {
        let vector_route = self.route_vector_key(key);
        if primary_route.shard_id == vector_route.shard_id
            || !self.pinned_vector_value_exists_routed(vector_route, key)
        {
            return false;
        }
        self.delete_routed_then(vector_route, key, now_ms, || {})
    }

    #[cfg(feature = "redis")]
    pub(super) fn pinned_vector_value_exists(&self, key: &[u8]) -> bool {
        let primary_route = self.route_key(key);
        let vector_route = self.route_vector_key(key);
        primary_route.shard_id != vector_route.shard_id
            && self.pinned_vector_value_exists_routed(vector_route, key)
    }

    #[cfg(feature = "redis")]
    fn pinned_vector_value_exists_routed(
        &self,
        vector_route: EmbeddedKeyRoute,
        key: &[u8],
    ) -> bool {
        let mut is_vector = false;
        self.with_shared_value_bytes_routed(vector_route, key, &mut |bytes| {
            is_vector = bytes.starts_with(crate::storage::VECTOR_SET_PREFIX);
        });
        is_vector
    }

    /// Deletes a routed key and runs `after_delete` before releasing the shard
    /// write lock when a mutation actually occurred.
    pub(crate) fn delete_routed_then(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        now_ms: u64,
        after_delete: impl FnOnce(),
    ) -> bool {
        let route = match route.shard_id < self.shards.len() {
            true => route,
            false => self.route_key(key),
        };
        #[cfg(feature = "redis")]
        if self.objects.shard_has_objects(route.shard_id) {
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            let mut shard = self.shards[route.shard_id].write();
            let deleted_object = bucket.delete_any(key);
            if deleted_object {
                self.objects.note_deleted(route.shard_id);
            }
            let deleted_session = if let Some(session_prefix) = derived_session_storage_prefix(key)
            {
                shard
                    .session_slots
                    .delete_hashed(&session_prefix, route.key_hash, key)
            } else {
                false
            };
            let deleted_map = shard.map.delete_hashed(route.key_hash, key, now_ms);
            let deleted = deleted_object || deleted_session || deleted_map;
            if deleted {
                after_delete();
            }
            return deleted;
        }
        let mut shard = self.shards[route.shard_id].write();
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && shard
                .session_slots
                .delete_hashed(&session_prefix, route.key_hash, key)
        {
            after_delete();
            return true;
        }
        let deleted = shard.map.delete_hashed(route.key_hash, key, now_ms);
        if deleted {
            after_delete();
        }
        deleted
    }

    /// Returns true when `key` currently exists.
    pub fn exists(&self, key: &[u8]) -> bool {
        let route = self.route_key(key);
        #[cfg(feature = "redis")]
        {
            if self.objects.shard_has_objects(route.shard_id) {
                let bucket = self.objects.read_bucket(route.shard_id, route.key_hash);
                if bucket.has_expirations() {
                    let now_ms = now_millis();
                    if bucket.object_is_expired(key, now_ms) {
                        drop(bucket);
                        let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
                        if bucket.delete_expired(key, now_ms) {
                            self.objects.note_deleted(route.shard_id);
                        }
                        return self.get_ref_routed(route, key).is_some();
                    }
                }
                if bucket.contains_live_object(key, now_millis()) {
                    return true;
                }
            }
        }
        if self.get_ref_routed(route, key).is_some() {
            return true;
        }
        #[cfg(feature = "redis")]
        {
            self.pinned_vector_value_exists(key)
        }
        #[cfg(not(feature = "redis"))]
        false
    }

    /// Returns Redis-style TTL in seconds: `-2` for missing, `-1` for no TTL.
    pub fn ttl_seconds(&self, key: &[u8]) -> i64 {
        let route = self.route_key(key);
        let now_ms = now_millis();
        #[cfg(feature = "redis")]
        if self.objects.shard_has_objects(route.shard_id) {
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            if bucket.delete_expired(key, now_ms) {
                self.objects.note_deleted(route.shard_id);
                return -2;
            }
            bucket.remove_expired_hash_fields(key, now_ms);
            if bucket.remove_hash_if_empty(key) {
                self.objects.note_deleted(route.shard_id);
                return -2;
            }
            let ttl = bucket.ttl_millis(key, now_ms);
            if ttl != -2 {
                return if ttl < 0 { ttl } else { (ttl + 999) / 1_000 };
            }
        }
        let mut shard = self.shards[route.shard_id].write();
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && shard
                .session_slots
                .get_ref_hashed(&session_prefix, route.key_hash, key)
                .is_some()
        {
            return -1;
        }
        let ttl = shard.map.ttl_seconds(key, now_ms);
        if ttl != -2 {
            return ttl;
        }
        #[cfg(feature = "redis")]
        {
            self.pinned_vector_ttl_seconds(route, key, now_ms)
        }
        #[cfg(not(feature = "redis"))]
        ttl
    }

    /// Returns Redis-style TTL in milliseconds: `-2` for missing, `-1` for no TTL.
    pub fn pttl_millis(&self, key: &[u8]) -> i64 {
        let route = self.route_key(key);
        let now_ms = now_millis();
        #[cfg(feature = "redis")]
        if self.objects.shard_has_objects(route.shard_id) {
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            if bucket.delete_expired(key, now_ms) {
                self.objects.note_deleted(route.shard_id);
                return -2;
            }
            bucket.remove_expired_hash_fields(key, now_ms);
            if bucket.remove_hash_if_empty(key) {
                self.objects.note_deleted(route.shard_id);
                return -2;
            }
            let ttl = bucket.ttl_millis(key, now_ms);
            if ttl != -2 {
                return ttl;
            }
        }
        let mut shard = self.shards[route.shard_id].write();
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && shard
                .session_slots
                .get_ref_hashed(&session_prefix, route.key_hash, key)
                .is_some()
        {
            return -1;
        }
        let ttl = shard.map.ttl_millis(key, now_ms);
        if ttl != -2 {
            return ttl;
        }
        #[cfg(feature = "redis")]
        {
            self.pinned_vector_ttl_millis(route, key, now_ms)
                .unwrap_or(-2)
        }
        #[cfg(not(feature = "redis"))]
        ttl
    }

    /// Removes the TTL from a key and returns true when a TTL was cleared.
    pub fn persist(&self, key: &[u8]) -> bool {
        if !self.point_mutation_is_replicable(key, 0, None) {
            return false;
        }
        let route = self.route_key(key);
        let now_ms = now_millis();
        #[cfg(feature = "redis")]
        let is_vector = self.clone_vector_value(key).is_some();
        #[cfg(feature = "redis")]
        if self.objects.shard_has_objects(route.shard_id) {
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            if bucket.delete_expired(key, now_ms) {
                self.objects.note_deleted(route.shard_id);
                return false;
            }
            bucket.remove_expired_hash_fields(key, now_ms);
            if bucket.remove_hash_if_empty(key) {
                self.objects.note_deleted(route.shard_id);
                return false;
            }
            let persisted = bucket.persist(key, now_ms);
            if persisted {
                self.notify_point_mutation(PointMutationKind::Expire, key, None, None, None);
                return true;
            }
            if bucket.contains_object(key) {
                return false;
            }
        }
        let mut shard = self.shards[route.shard_id].write();
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && shard
                .session_slots
                .get_ref_hashed(&session_prefix, route.key_hash, key)
                .is_some()
        {
            return false;
        }
        let persisted = shard.map.persist(key, now_ms);
        #[cfg(feature = "redis")]
        let persisted =
            persisted || self.persist_pinned_vector_value_if_distinct(route, key, now_ms);
        #[cfg(feature = "redis")]
        if persisted && is_vector {
            self.notify_vector_mutation(VectorMutationKind::Expire, key, None, None);
        }
        #[cfg(feature = "redis")]
        if persisted && !is_vector {
            self.notify_point_mutation(PointMutationKind::Expire, key, None, None, None);
        }
        #[cfg(not(feature = "redis"))]
        if persisted {
            self.notify_point_mutation(PointMutationKind::Expire, key, None, None, None);
        }
        persisted
    }

    /// Sets an absolute expiration timestamp in Unix milliseconds.
    pub fn expire(&self, key: &[u8], expire_at_ms: u64) -> bool {
        if !self.point_mutation_is_replicable(key, 0, None) {
            return false;
        }
        let route = self.route_key(key);
        let now_ms = now_millis();
        #[cfg(feature = "redis")]
        let is_vector = self.clone_vector_value(key).is_some();
        let changed = self.expire_routed_then(route, key, expire_at_ms, now_ms, || {
            #[cfg(feature = "redis")]
            if is_vector {
                return;
            }
            self.notify_point_mutation(
                PointMutationKind::Expire,
                key,
                None,
                Some(expire_at_ms),
                None,
            );
        });
        #[cfg(feature = "redis")]
        let changed = changed
            || self.expire_pinned_vector_value_if_distinct(route, key, expire_at_ms, now_ms);
        #[cfg(feature = "redis")]
        if changed && is_vector {
            self.notify_vector_mutation(VectorMutationKind::Expire, key, None, Some(expire_at_ms));
        }
        changed
    }

    #[cfg(feature = "redis")]
    fn pinned_vector_ttl_millis(
        &self,
        primary_route: EmbeddedKeyRoute,
        key: &[u8],
        now_ms: u64,
    ) -> Option<i64> {
        let vector_route = self.route_vector_key(key);
        if primary_route.shard_id == vector_route.shard_id
            || !self.pinned_vector_value_exists_routed(vector_route, key)
        {
            return None;
        }
        let mut shard = self.shards[vector_route.shard_id].write();
        let ttl = shard.map.ttl_millis(key, now_ms);
        (ttl != -2).then_some(ttl)
    }

    #[cfg(feature = "redis")]
    fn pinned_vector_ttl_seconds(
        &self,
        primary_route: EmbeddedKeyRoute,
        key: &[u8],
        now_ms: u64,
    ) -> i64 {
        match self.pinned_vector_ttl_millis(primary_route, key, now_ms) {
            Some(ttl) if ttl > 0 => (ttl + 999) / 1_000,
            Some(ttl) => ttl,
            None => -2,
        }
    }

    #[cfg(feature = "redis")]
    fn persist_pinned_vector_value_if_distinct(
        &self,
        primary_route: EmbeddedKeyRoute,
        key: &[u8],
        now_ms: u64,
    ) -> bool {
        let vector_route = self.route_vector_key(key);
        if primary_route.shard_id == vector_route.shard_id
            || !self.pinned_vector_value_exists_routed(vector_route, key)
        {
            return false;
        }
        let mut shard = self.shards[vector_route.shard_id].write();
        shard.map.persist(key, now_ms)
    }

    #[cfg(feature = "redis")]
    fn expire_pinned_vector_value_if_distinct(
        &self,
        primary_route: EmbeddedKeyRoute,
        key: &[u8],
        expire_at_ms: u64,
        now_ms: u64,
    ) -> bool {
        let vector_route = self.route_vector_key(key);
        if primary_route.shard_id == vector_route.shard_id
            || !self.pinned_vector_value_exists_routed(vector_route, key)
        {
            return false;
        }
        let mut shard = self.shards[vector_route.shard_id].write();
        shard.map.expire(key, expire_at_ms, now_ms)
    }

    /// Updates an absolute expiration timestamp and runs `after_expire` before
    /// releasing the shard write lock when a TTL mutation actually occurred.
    pub(crate) fn expire_routed_then(
        &self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        expire_at_ms: u64,
        now_ms: u64,
        after_expire: impl FnOnce(),
    ) -> bool {
        let route = match route.shard_id < self.shards.len() {
            true => route,
            false => self.route_key(key),
        };
        #[cfg(feature = "redis")]
        if self.objects.shard_has_objects(route.shard_id) {
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            if bucket.delete_expired(key, now_ms) {
                self.objects.note_deleted(route.shard_id);
                return false;
            }
            bucket.remove_expired_hash_fields(key, now_ms);
            if bucket.remove_hash_if_empty(key) {
                self.objects.note_deleted(route.shard_id);
                return false;
            }
            if bucket.expire(key, expire_at_ms, now_ms) {
                after_expire();
                return true;
            }
            if bucket.contains_object(key) {
                return false;
            }
        }
        let mut shard = self.shards[route.shard_id].write();
        if let Some(session_prefix) = derived_session_storage_prefix(key)
            && shard
                .session_slots
                .get_ref_hashed(&session_prefix, route.key_hash, key)
                .is_some()
        {
            return false;
        }
        let changed = shard.map.expire(key, expire_at_ms, now_ms);
        if changed {
            after_expire();
        }
        changed
    }

    #[cfg(feature = "redis")]
    fn redis_object_metadata(
        &self,
        key: &[u8],
        lookup: impl FnOnce(&RedisObjectBucket, &[u8], u64) -> Option<&'static str>,
    ) -> Option<&'static str> {
        let route = self.route_key(key);
        match self.objects.shard_has_objects(route.shard_id) {
            false => None,
            true => {
                let bucket = self.objects.read_bucket(route.shard_id, route.key_hash);
                let now_ms = now_millis();
                let expired = match bucket.has_expirations() {
                    true => bucket.object_is_expired(key, now_ms),
                    false => false,
                };
                match expired {
                    true => {
                        drop(bucket);
                        let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
                        let now_ms = now_millis();
                        match bucket.delete_expired(key, now_ms) {
                            true => {
                                self.objects.note_deleted(route.shard_id);
                                Some(())
                            }
                            false => None,
                        };
                        None
                    }
                    false => lookup(&bucket, key, now_ms),
                }
            }
        }
    }

    /// Returns the Redis type name for a key, or `"none"` when it is missing.
    pub fn redis_type(&self, key: &[u8]) -> &'static str {
        #[cfg(feature = "redis")]
        let object_type = self.redis_object_metadata(key, RedisObjectBucket::type_name);
        #[cfg(not(feature = "redis"))]
        let object_type: Option<&'static str> = None;

        match object_type {
            Some(kind) => kind,
            None => match self.get_ref(key) {
                #[cfg(feature = "redis")]
                Some(value) if value.starts_with(crate::storage::VECTOR_SET_PREFIX) => "vectorset",
                Some(_) => "string",
                #[cfg(feature = "redis")]
                None if self.pinned_vector_value_exists(key) => "vectorset",
                None => "none",
            },
        }
    }

    /// Returns the Redis object encoding name for a key when it exists.
    pub fn object_encoding(&self, key: &[u8]) -> Option<&'static str> {
        #[cfg(feature = "redis")]
        let object_encoding = self.redis_object_metadata(key, RedisObjectBucket::encoding);
        #[cfg(not(feature = "redis"))]
        let object_encoding: Option<&'static str> = None;

        match object_encoding {
            Some(encoding) => Some(encoding),
            None => match self.get_ref(key).is_some() {
                true => Some("raw"),
                false => {
                    #[cfg(feature = "redis")]
                    if self.pinned_vector_value_exists(key) {
                        return Some("raw");
                    }
                    None
                }
            },
        }
    }

    /// Returns per-shard statistics snapshots.
    #[cfg(feature = "embedded")]
    pub fn shard_stats_snapshot(&self) -> Vec<ShardStatsSnapshot> {
        self.shards
            .iter()
            .enumerate()
            .map(|(shard_id, shard)| {
                let shard = shard.read();
                let (hot, warm, cold) = shard.map.stats_snapshot();
                let reads = hot
                    .hits
                    .saturating_add(hot.misses)
                    .saturating_add(warm.hits)
                    .saturating_add(warm.misses)
                    .saturating_add(cold.hits)
                    .saturating_add(cold.misses);
                let expired = hot
                    .expirations
                    .saturating_add(warm.expirations)
                    .saturating_add(cold.expirations);
                ShardStatsSnapshot {
                    shard_id,
                    key_count: shard.map.len().saturating_add(shard.session_slots.len()),
                    reads,
                    writes: 0,
                    deletes: 0,
                    expired,
                    maintenance_runs: 0,
                    hot,
                    warm,
                    cold,
                    object_overflow: shard
                        .map
                        .object_overflow_stats_with_worker_stats(shard_id == 0)
                        .into(),
                }
            })
            .collect()
    }

    /// Returns aggregate hot, warm, and cold tier statistics.
    #[cfg(feature = "embedded")]
    pub fn stats_snapshot(&self) -> (TierStatsSnapshot, TierStatsSnapshot, TierStatsSnapshot) {
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
            let shard = shard.read();
            let (shard_hot, shard_warm, shard_cold) = shard.map.stats_snapshot();
            accumulate_tier_stats(&mut hot, &shard_hot);
            accumulate_tier_stats(&mut warm, &shard_warm);
            accumulate_tier_stats(&mut cold, &shard_cold);
        }

        (hot, warm, cold)
    }

    /// Runs maintenance on every shard and returns the number of expired entries.
    pub fn process_maintenance(&self) -> usize {
        let now_ms = now_millis();
        self.shards
            .iter()
            .map(|shard| {
                let mut shard = shard.write();
                shard.map.process_maintenance(now_ms)
            })
            .sum()
    }

    /// Restores persisted entries, skipping records that are already expired.
    pub fn restore_entries<I>(&self, entries: I)
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
            #[cfg(feature = "redis")]
            let route = if entry.value.starts_with(crate::storage::VECTOR_SET_PREFIX) {
                self.route_vector_key(&entry.key)
            } else {
                self.route_key(&entry.key)
            };
            #[cfg(not(feature = "redis"))]
            let route = self.route_key(&entry.key);
            let mut shard = self.shards[route.shard_id].write();
            if let Some(session_prefix) = derived_session_storage_prefix(&entry.key) {
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
            #[cfg(feature = "redis")]
            self.refresh_string_key_count(route.shard_id, &shard);
        }
    }
}
