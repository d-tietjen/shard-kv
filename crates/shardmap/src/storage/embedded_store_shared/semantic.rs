use super::*;

impl<const SHARDS: usize> SharedEmbeddedStore<SHARDS> {
    /// Inserts or replaces a point-key value with an embedding for semantic lookup.
    #[inline(always)]
    pub fn insert_semantic_slice(
        &self,
        key: &[u8],
        value: &[u8],
        embedding: &[f32],
    ) -> Result<(), SemanticCacheError> {
        self.insert_semantic_slice_with_ttl(key, value, embedding, None)
    }

    /// Inserts or replaces a point-key value with an embedding and optional TTL.
    ///
    /// `ttl_ms` is measured from the current Unix time in milliseconds. Passing
    /// `None` creates a persistent value.
    pub fn insert_semantic_slice_with_ttl(
        &self,
        key: &[u8],
        value: &[u8],
        embedding: &[f32],
        ttl_ms: Option<u64>,
    ) -> Result<(), SemanticCacheError> {
        #[cfg(feature = "no-ttl")]
        {
            assert!(
                ttl_ms.is_none(),
                "shardcache/no-ttl builds do not support shared-store TTL writes"
            );
        }
        let _ = SemanticEmbedding::from_slice(embedding)?;
        let now_ms = ttl_now_millis();
        #[cfg(feature = "no-ttl")]
        let expire_at_ms = None;
        #[cfg(not(feature = "no-ttl"))]
        let expire_at_ms = ttl_ms.map(|ttl| now_ms.saturating_add(ttl));
        if ttl_ms.is_some() {
            self.disable_semantic_query_cache();
        }
        let route = self.route_key(key);
        self.insert_point_shadow(route, key, value, expire_at_ms, now_ms);
        self.stripe(self.semantic_shard_id())
            .write()
            .set_semantic_slice_hashed(
                self.inner.route_mode,
                route.key_hash,
                key,
                value,
                embedding,
                expire_at_ms,
                now_ms,
            )?;
        self.bump_semantic_generation();
        Ok(())
    }

    /// Returns the best semantic match across all stripes at or above `min_score`.
    pub fn semantic_search(
        &self,
        embedding: &[f32],
        min_score: f32,
    ) -> Result<Option<SemanticMatch>, SemanticCacheError> {
        let query = SemanticEmbedding::from_slice(embedding)?;
        let min_score = validate_similarity_threshold(min_score)?;
        let generation = self.semantic_generation();
        let query_cache_enabled = self.semantic_query_cache_enabled();
        if query_cache_enabled {
            if let Some(cached) = self
                .inner
                .semantic_query_cache
                .read()
                .lookup(&query, min_score, generation)
            {
                return Ok(cached);
            }
        }

        let now_ms = ttl_now_millis();
        let semantic_shard = self.stripe(self.semantic_shard_id()).read();
        let mut best = if min_score <= 1.0 {
            semantic_shard.semantic_search_exact(&query, min_score, now_ms)
        } else {
            None
        };
        if best.is_none() {
            best = semantic_shard.semantic_search(&query, min_score, now_ms);
        }
        drop(semantic_shard);

        if query_cache_enabled && self.semantic_generation() == generation {
            self.inner.semantic_query_cache.write().insert(
                &query,
                min_score,
                generation,
                best.clone(),
            );
        }

        Ok(best)
    }
}
