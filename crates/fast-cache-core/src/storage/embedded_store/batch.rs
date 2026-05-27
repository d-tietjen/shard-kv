use super::batch_results::{
    BatchReadViewBuilder, OrderedBatchReadViewBuilder, OrderedPackedBatchBuilder,
    PackedBatchBuilder,
};
use super::*;

impl EmbeddedStore {
    /// Returns owned values for `keys` in request order.
    pub fn batch_get(&self, keys: Vec<Bytes>) -> Vec<Option<Bytes>> {
        let total = keys.len();
        if total == 0 {
            return Vec::new();
        }

        #[cfg(feature = "telemetry")]
        let start = self.metrics.as_ref().map(|_| Instant::now());
        let now_ms = now_millis();
        if let Some(shard_id) = self.single_shard_batch_route(&keys) {
            let mut shard = self.shards[shard_id].write();
            let values = keys
                .into_iter()
                .map(|key| {
                    let (_, key_hash) = self.hashes_for_key(&key);
                    shard
                        .get_ref_hashed_session_or_flat(key_hash, &key, now_ms)
                        .map(<[u8]>::to_vec)
                })
                .collect();
            #[cfg(feature = "telemetry")]
            self.record_batch_metrics(start, &[shard_id]);
            return values;
        }

        let mut groups = vec![Vec::<(usize, Bytes, u64)>::new(); self.shards.len()];
        let mut touched = Vec::new();

        for (index, key) in keys.into_iter().enumerate() {
            let (route_hash, key_hash) = self.hashes_for_key(&key);
            let shard_id = self.route_hash(route_hash);
            if groups[shard_id].is_empty() {
                touched.push(shard_id);
            }
            groups[shard_id].push((index, key, key_hash));
        }

        let mut values = vec![None; total];
        for (shard_id, batch) in groups.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            let mut shard = self.shards[shard_id].write();
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

    /// Returns owned views for a generic batch.
    ///
    /// This path may touch multiple shards under `full_key` routing; each hit
    /// is materialized into an owned `bytes::Bytes` handle.
    pub fn batch_get_view(&self, keys: &[Bytes]) -> EmbeddedBatchReadView {
        let total = keys.len();
        if total == 0 {
            return EmbeddedBatchReadView {
                items: Vec::new(),
                hit_count: 0,
                total_bytes: 0,
            };
        }

        #[cfg(feature = "telemetry")]
        let start = self.metrics.as_ref().map(|_| Instant::now());
        let now_ms = now_millis();

        if let Some(shard_id) = self.single_shard_batch_route(keys) {
            let mut shard = self.shards[shard_id].write();
            let mut view = BatchReadViewBuilder::new(keys.len());
            for key in keys {
                let (_, key_hash) = self.hashes_for_key(key);
                view.push(shard.get_ref_hashed_published_session_or_flat(key_hash, key, now_ms));
            }
            drop(shard);
            #[cfg(feature = "telemetry")]
            self.record_batch_metrics(start, &[shard_id]);
            return view.finish();
        }

        let mut groups = vec![Vec::<(usize, &Bytes, u64)>::new(); self.shards.len()];
        let mut touched = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            let (route_hash, key_hash) = self.hashes_for_key(key);
            let shard_id = self.route_hash(route_hash);
            if groups[shard_id].is_empty() {
                touched.push(shard_id);
            }
            groups[shard_id].push((index, key, key_hash));
        }

        let mut view = OrderedBatchReadViewBuilder::new(total);
        for (shard_id, batch) in groups.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            let mut shard = self.shards[shard_id].write();
            for (index, key, key_hash) in batch {
                let value = shard.get_ref_hashed_published_session_or_flat(key_hash, key, now_ms);
                if let Some(value) = value {
                    view.record_hit(index, value);
                }
            }
            drop(shard);
        }

        #[cfg(feature = "telemetry")]
        self.record_batch_metrics(start, &touched);
        view.finish()
    }

    /// Retrieves a session-scoped batch from a known shard without rediscovering
    /// placement from every full chunk key.
    pub fn batch_get_session(&self, session_prefix: &[u8], keys: &[Bytes]) -> Vec<Option<Bytes>> {
        let key_hashes = keys.iter().map(|key| hash_key(key)).collect::<Vec<_>>();
        self.batch_get_session_prehashed(session_prefix, keys, &key_hashes)
    }

    pub fn batch_get_session_prehashed(
        &self,
        session_prefix: &[u8],
        keys: &[Bytes],
        key_hashes: &[u64],
    ) -> Vec<Option<Bytes>> {
        assert_eq!(
            keys.len(),
            key_hashes.len(),
            "keys and key_hashes must have matching lengths",
        );
        if keys.is_empty() {
            return Vec::new();
        }

        #[cfg(feature = "telemetry")]
        let start = self.metrics.as_ref().map(|_| Instant::now());
        let route = self.route_session(session_prefix);
        let now_ms = now_millis();
        let mut shard = self.shards[route.shard_id].write();
        let active_session_prefix = shard
            .session_slots
            .has_session(session_prefix)
            .then_some(session_prefix);
        let values = keys
            .iter()
            .zip(key_hashes.iter().copied())
            .map(|(key, key_hash)| {
                shard
                    .get_ref_hashed_active_session_or_flat(
                        active_session_prefix,
                        key_hash,
                        key,
                        now_ms,
                    )
                    .map(<[u8]>::to_vec)
            })
            .collect();
        #[cfg(feature = "telemetry")]
        self.record_batch_metrics(start, &[route.shard_id]);
        values
    }

    /// Retrieves a session-scoped batch from a precomputed shard route.
    pub fn batch_get_session_routed(
        &self,
        route: EmbeddedSessionRoute,
        keys: &[Bytes],
    ) -> Vec<Option<Bytes>> {
        let total = keys.len();
        if total == 0 {
            return Vec::new();
        }

        #[cfg(feature = "telemetry")]
        let start = self.metrics.as_ref().map(|_| Instant::now());
        let now_ms = now_millis();
        let mut shard = self.shards[route.shard_id].write();
        let values = keys
            .iter()
            .map(|key| {
                let key_hash = hash_key(key);
                shard
                    .map
                    .get_ref_hashed(key_hash, key, now_ms)
                    .map(<[u8]>::to_vec)
            })
            .collect();
        #[cfg(feature = "telemetry")]
        self.record_batch_metrics(start, &[route.shard_id]);
        values
    }

    /// Returns owned views for a session-scoped batch.
    ///
    /// Session-slot values are copied into owned handles so the returned view
    /// remains valid after later session updates.
    pub fn batch_get_session_view(
        &self,
        session_prefix: &[u8],
        keys: &[Bytes],
    ) -> EmbeddedSessionBatchView {
        let key_hashes = keys.iter().map(|key| hash_key(key)).collect::<Vec<_>>();
        self.batch_get_session_view_prehashed(session_prefix, keys, &key_hashes)
    }

    pub fn batch_get_session_view_prehashed(
        &self,
        session_prefix: &[u8],
        keys: &[Bytes],
        key_hashes: &[u64],
    ) -> EmbeddedSessionBatchView {
        assert_eq!(
            keys.len(),
            key_hashes.len(),
            "keys and key_hashes must have matching lengths",
        );
        if keys.is_empty() {
            return EmbeddedBatchReadView {
                items: Vec::new(),
                hit_count: 0,
                total_bytes: 0,
            };
        }

        #[cfg(feature = "telemetry")]
        let start = self.metrics.as_ref().map(|_| Instant::now());
        let route = self.route_session(session_prefix);
        let now_ms = now_millis();
        let mut shard = self.shards[route.shard_id].write();

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
        drop(shard);

        #[cfg(feature = "telemetry")]
        self.record_batch_metrics(start, &[route.shard_id]);

        view.finish()
    }

    /// Returns owned views for a session-scoped batch from a precomputed
    /// shard route.
    pub fn batch_get_session_view_routed(
        &self,
        route: EmbeddedSessionRoute,
        keys: &[Bytes],
    ) -> EmbeddedSessionBatchView {
        let key_hashes = keys.iter().map(|key| hash_key(key)).collect::<Vec<_>>();
        self.batch_get_session_view_prehashed_routed(route, keys, &key_hashes)
    }

    /// Returns owned views for a session-scoped batch from a precomputed
    /// shard route and precomputed full-key hashes.
    pub fn batch_get_session_view_prehashed_routed(
        &self,
        route: EmbeddedSessionRoute,
        keys: &[Bytes],
        key_hashes: &[u64],
    ) -> EmbeddedSessionBatchView {
        assert_eq!(
            keys.len(),
            key_hashes.len(),
            "keys and key_hashes must have matching lengths",
        );
        if keys.is_empty() {
            return EmbeddedBatchReadView {
                items: Vec::new(),
                hit_count: 0,
                total_bytes: 0,
            };
        }

        #[cfg(feature = "telemetry")]
        let start = self.metrics.as_ref().map(|_| Instant::now());
        let now_ms = now_millis();
        let mut shard = self.shards[route.shard_id].write();
        let session_prefix = batch_derived_session_storage_prefix(keys);
        let active_session_prefix = session_prefix
            .as_ref()
            .filter(|prefix| shard.session_slots.has_session(prefix))
            .map(Vec::as_slice);

        let mut view = BatchReadViewBuilder::new(keys.len());
        for (key, key_hash) in keys.iter().zip(key_hashes.iter().copied()) {
            view.push(shard.get_ref_hashed_active_session_or_flat(
                active_session_prefix,
                key_hash,
                key,
                now_ms,
            ));
        }
        drop(shard);

        #[cfg(feature = "telemetry")]
        self.record_batch_metrics(start, &[route.shard_id]);

        view.finish()
    }

    /// Retrieves a generic packed batch. This is still copy-out, but it packs
    /// all returned bytes into one contiguous buffer to minimize object churn.
    pub fn batch_get_packed(&self, keys: &[Bytes]) -> PackedBatch {
        let total = keys.len();
        if total == 0 {
            return PackedBatch::default();
        }

        #[cfg(feature = "telemetry")]
        let start = self.metrics.as_ref().map(|_| Instant::now());
        let now_ms = now_millis();
        if let Some(shard_id) = self.single_shard_batch_route(keys) {
            let mut shard = self.shards[shard_id].write();
            let mut packed = PackedBatchBuilder::new(total);
            for key in keys {
                let (_, key_hash) = self.hashes_for_key(key);
                packed.push(shard.get_ref_hashed_session_or_flat(key_hash, key, now_ms));
            }
            #[cfg(feature = "telemetry")]
            self.record_batch_metrics(start, &[shard_id]);
            return packed.finish();
        }

        let mut groups = vec![Vec::<(usize, &Bytes, u64)>::new(); self.shards.len()];
        let mut touched = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            let (route_hash, key_hash) = self.hashes_for_key(key);
            let shard_id = self.route_hash(route_hash);
            if groups[shard_id].is_empty() {
                touched.push(shard_id);
            }
            groups[shard_id].push((index, key, key_hash));
        }

        let mut packed = OrderedPackedBatchBuilder::new(total);

        for (shard_id, batch) in groups.into_iter().enumerate() {
            if batch.is_empty() {
                continue;
            }
            let mut shard = self.shards[shard_id].write();
            for (index, key, key_hash) in batch {
                if let Some(value) = shard.get_ref_hashed_session_or_flat(key_hash, key, now_ms) {
                    packed.record_hit(index, value);
                }
            }
        }

        let packed = packed.finish();
        #[cfg(feature = "telemetry")]
        self.record_batch_metrics(start, &touched);
        packed
    }

    /// Retrieves a session-scoped packed batch from a known shard.
    pub fn batch_get_session_packed(&self, session_prefix: &[u8], keys: &[Bytes]) -> PackedBatch {
        if keys.is_empty() {
            return PackedBatch::default();
        }

        #[cfg(feature = "telemetry")]
        let start = self.metrics.as_ref().map(|_| Instant::now());
        let route = self.route_session(session_prefix);
        let now_ms = now_millis();
        let mut shard = self.shards[route.shard_id].write();
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

    /// Retrieves a session-scoped packed batch from a precomputed shard route.
    pub fn batch_get_session_packed_routed(
        &self,
        route: EmbeddedSessionRoute,
        keys: &[Bytes],
    ) -> PackedBatch {
        if keys.is_empty() {
            return PackedBatch::default();
        }

        #[cfg(feature = "telemetry")]
        let start = self.metrics.as_ref().map(|_| Instant::now());
        let now_ms = now_millis();
        let mut shard = self.shards[route.shard_id].write();
        let session_prefix = batch_derived_session_storage_prefix(keys);
        let active_session_prefix = session_prefix
            .as_ref()
            .filter(|prefix| shard.session_slots.has_session(prefix))
            .map(Vec::as_slice);
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
}
