use super::*;

impl EmbeddedStore {
    /// Inserts or replaces a byte-string value with an embedding for semantic lookup.
    ///
    /// `ttl_ms` is a relative TTL in milliseconds. Passing `None` creates a
    /// persistent value.
    pub fn set_semantic_slice(
        &self,
        key: &[u8],
        value: &[u8],
        embedding: &[f32],
        ttl_ms: Option<u64>,
    ) -> Result<(), SemanticCacheError> {
        let now_ms = now_millis();
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        let route = self.route_key(key);
        #[cfg(feature = "redis")]
        if self.objects.shard_has_objects(route.shard_id) {
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            let mut shard = self.shards[route.shard_id].write();
            if bucket.delete_any(key) {
                self.objects.note_deleted(route.shard_id);
            }
            shard.set_semantic_slice_hashed(
                self.route_mode,
                route.key_hash,
                key,
                value,
                embedding,
                expire_at_ms,
                now_ms,
            )?;
            self.refresh_string_key_count(route.shard_id, &shard);
            return Ok(());
        }

        let mut shard = self.shards[route.shard_id].write();
        shard.set_semantic_slice_hashed(
            self.route_mode,
            route.key_hash,
            key,
            value,
            embedding,
            expire_at_ms,
            now_ms,
        )?;
        #[cfg(feature = "redis")]
        self.refresh_string_key_count(route.shard_id, &shard);
        Ok(())
    }

    /// Inserts or replaces a byte-string value with embedding and governance metadata.
    ///
    /// `ttl_ms` is a relative TTL in milliseconds. Passing `None` creates a
    /// persistent value.
    pub fn set_semantic_slice_with_governance(
        &self,
        key: &[u8],
        value: &[u8],
        embedding: &[f32],
        ttl_ms: Option<u64>,
        governance_metadata: &[u8],
    ) -> Result<(), SemanticCacheError> {
        let now_ms = now_millis();
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        let route = self.route_key(key);
        #[cfg(feature = "redis")]
        if self.objects.shard_has_objects(route.shard_id) {
            let mut bucket = self.objects.write_bucket(route.shard_id, route.key_hash);
            let mut shard = self.shards[route.shard_id].write();
            if bucket.delete_any(key) {
                self.objects.note_deleted(route.shard_id);
            }
            shard.set_semantic_slice_hashed_with_governance(
                self.route_mode,
                route.key_hash,
                key,
                value,
                embedding,
                governance_metadata,
                expire_at_ms,
                now_ms,
            )?;
            self.refresh_string_key_count(route.shard_id, &shard);
            return Ok(());
        }

        let mut shard = self.shards[route.shard_id].write();
        shard.set_semantic_slice_hashed_with_governance(
            self.route_mode,
            route.key_hash,
            key,
            value,
            embedding,
            governance_metadata,
            expire_at_ms,
            now_ms,
        )?;
        #[cfg(feature = "redis")]
        self.refresh_string_key_count(route.shard_id, &shard);
        Ok(())
    }

    /// Returns the best semantic match across all shards at or above `min_score`.
    pub fn semantic_search(
        &self,
        embedding: &[f32],
        min_score: f32,
    ) -> Result<Option<SemanticMatch>, SemanticCacheError> {
        let query = SemanticEmbedding::from_slice(embedding)?;
        let min_score = validate_similarity_threshold(min_score)?;
        let now_ms = now_millis();
        if min_score <= 1.0 {
            for shard in &self.shards {
                let shard = shard.read();
                let Some(exact) = shard.map.semantic_search_exact(&query, min_score, now_ms) else {
                    continue;
                };
                return Ok(Some(exact));
            }
        }

        let mut best: Option<SemanticMatch> = None;

        for shard in &self.shards {
            let shard = shard.read();
            let Some(candidate) = shard.map.semantic_search(&query, min_score, now_ms) else {
                continue;
            };
            if best
                .as_ref()
                .is_none_or(|current| candidate.score > current.score)
            {
                best = Some(candidate);
            }
        }

        Ok(best)
    }

    /// Returns the best semantic match accepted by `governance_filter`.
    ///
    /// The filter receives the stored governance metadata, or `None` when the
    /// entry was written through the default semantic APIs, and must approve
    /// the entry before the cached value is returned.
    pub fn semantic_search_with_governance_filter(
        &self,
        embedding: &[f32],
        min_score: f32,
        mut governance_filter: impl FnMut(Option<&[u8]>) -> bool,
    ) -> Result<Option<SemanticMatch>, SemanticCacheError> {
        let query = SemanticEmbedding::from_slice(embedding)?;
        let min_score = validate_similarity_threshold(min_score)?;
        let now_ms = now_millis();
        if min_score <= 1.0 {
            for shard in &self.shards {
                let shard = shard.read();
                let Some(exact) = shard.map.semantic_search_exact_with_governance_filter(
                    &query,
                    min_score,
                    now_ms,
                    &mut governance_filter,
                ) else {
                    continue;
                };
                return Ok(Some(exact));
            }
        }

        let mut best: Option<SemanticMatch> = None;

        for shard in &self.shards {
            let shard = shard.read();
            let Some(candidate) = shard.map.semantic_search_with_governance_filter(
                &query,
                min_score,
                now_ms,
                &mut governance_filter,
            ) else {
                continue;
            };
            if best
                .as_ref()
                .is_none_or(|current| candidate.score > current.score)
            {
                best = Some(candidate);
            }
        }

        Ok(best)
    }
}
