use super::*;

impl EmbeddedStore {
    /// Deletes a key and returns true when a value or object was removed.
    pub fn delete(&self, key: &[u8]) -> bool {
        let now_ms = now_millis();
        let route = self.route_key(key);
        self.delete_routed_then(route, key, now_ms, || {})
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
        #[cfg(feature = "redis-compat")]
        if self.objects.has_objects() {
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
        #[cfg(feature = "redis-compat")]
        if self.objects.has_objects() {
            let route = self.route_key(key);
            let bucket = self.objects.read_bucket(route.shard_id, route.key_hash);
            if bucket.object_is_expired(key, now_millis()) {
                drop(bucket);
                let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
                if bucket.delete_expired(key, now_millis()) {
                    self.objects.note_deleted(route.shard_id);
                }
                return self.get(key).is_some();
            }
            if bucket.contains_object(key) {
                return true;
            }
        }
        self.get(key).is_some()
    }

    /// Returns Redis-style TTL in seconds: `-2` for missing, `-1` for no TTL.
    pub fn ttl_seconds(&self, key: &[u8]) -> i64 {
        let route = self.route_key(key);
        let now_ms = now_millis();
        #[cfg(feature = "redis-compat")]
        if self.objects.has_objects() {
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            if bucket.delete_expired(key, now_ms) {
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
        shard.map.ttl_seconds(key, now_ms)
    }

    /// Returns Redis-style TTL in milliseconds: `-2` for missing, `-1` for no TTL.
    pub fn pttl_millis(&self, key: &[u8]) -> i64 {
        let route = self.route_key(key);
        let now_ms = now_millis();
        #[cfg(feature = "redis-compat")]
        if self.objects.has_objects() {
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            if bucket.delete_expired(key, now_ms) {
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
        shard.map.ttl_millis(key, now_ms)
    }

    /// Removes the TTL from a key and returns true when a TTL was cleared.
    pub fn persist(&self, key: &[u8]) -> bool {
        let route = self.route_key(key);
        let now_ms = now_millis();
        #[cfg(feature = "redis-compat")]
        if self.objects.has_objects() {
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            if bucket.delete_expired(key, now_ms) {
                self.objects.note_deleted(route.shard_id);
                return false;
            }
            let persisted = bucket.persist(key, now_ms);
            if persisted {
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
        shard.map.persist(key, now_ms)
    }

    /// Sets an absolute expiration timestamp in Unix milliseconds.
    pub fn expire(&self, key: &[u8], expire_at_ms: u64) -> bool {
        let route = self.route_key(key);
        let now_ms = now_millis();
        self.expire_routed_then(route, key, expire_at_ms, now_ms, || {})
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
        #[cfg(feature = "redis-compat")]
        if self.objects.has_objects() {
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            if bucket.delete_expired(key, now_ms) {
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

    #[cfg(feature = "redis-compat")]
    fn redis_object_metadata(
        &self,
        key: &[u8],
        lookup: impl FnOnce(&RedisObjectBucket, &[u8]) -> Option<&'static str>,
    ) -> Option<&'static str> {
        match self.objects.has_objects() {
            false => None,
            true => {
                let route = self.route_key(key);
                let now_ms = now_millis();
                let bucket = self.objects.read_bucket(route.shard_id, route.key_hash);
                match bucket.object_is_expired(key, now_ms) {
                    true => {
                        drop(bucket);
                        let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
                        match bucket.delete_expired(key, now_ms) {
                            true => {
                                self.objects.note_deleted(route.shard_id);
                                Some(())
                            }
                            false => None,
                        };
                        None
                    }
                    false => lookup(&bucket, key),
                }
            }
        }
    }

    /// Returns the Redis type name for a key, or `"none"` when it is missing.
    pub fn redis_type(&self, key: &[u8]) -> &'static str {
        #[cfg(feature = "redis-compat")]
        let object_type = self.redis_object_metadata(key, RedisObjectBucket::type_name);
        #[cfg(not(feature = "redis-compat"))]
        let object_type: Option<&'static str> = None;

        match object_type {
            Some(kind) => kind,
            None => match self.get_value_bytes(key).is_some() {
                true => "string",
                false => "none",
            },
        }
    }

    /// Returns the Redis object encoding name for a key when it exists.
    pub fn object_encoding(&self, key: &[u8]) -> Option<&'static str> {
        #[cfg(feature = "redis-compat")]
        let object_encoding = self.redis_object_metadata(key, RedisObjectBucket::encoding);
        #[cfg(not(feature = "redis-compat"))]
        let object_encoding: Option<&'static str> = None;

        match object_encoding {
            Some(encoding) => Some(encoding),
            None => match self.get_value_bytes(key).is_some() {
                true => Some("raw"),
                false => None,
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
            let route = self.route_key(&entry.key);
            let mut shard = self.shards[route.shard_id].write();
            if let Some(session_prefix) = derived_session_storage_prefix(&entry.key) {
                shard
                    .session_slots
                    .delete_hashed(&session_prefix, route.key_hash, &entry.key);
            }
            shard.map.set_hashed(
                route.key_hash,
                entry.key,
                entry.value,
                entry.expire_at_ms,
                now_ms,
            );
            shard.enforce_memory_limit(now_ms);
            #[cfg(feature = "redis-compat")]
            self.refresh_string_key_count(route.shard_id, &shard);
        }
    }
}
