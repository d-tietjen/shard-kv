use super::*;

impl FlatMap {
    /// Inserts or replaces a value and attaches a normalized semantic embedding.
    pub(crate) fn set_semantic_slice_hashed(
        &mut self,
        hash: u64,
        key: &[u8],
        value: &[u8],
        embedding: &[f32],
        expire_at_ms: Option<u64>,
        now_ms: u64,
    ) -> Result<(), SemanticCacheError> {
        let embedding = SemanticEmbedding::from_slice(embedding)?;
        self.set_slice_hashed(hash, key, value, expire_at_ms, now_ms);
        let token = self.semantic_index.insert(hash, key, &embedding);

        let Some(entry) = self
            .entries
            .find_mut(hash, |entry| entry.matches_hashed_key(hash, key))
        else {
            return Ok(());
        };
        let previous_entry_bytes = entry.stored_bytes();
        entry.semantic_index_token = Some(token);
        let new_entry_bytes = entry.stored_bytes();
        #[cfg(feature = "telemetry")]
        let memory_delta = new_entry_bytes as isize - previous_entry_bytes as isize;
        self.stored_bytes = self
            .stored_bytes
            .saturating_sub(previous_entry_bytes)
            .saturating_add(new_entry_bytes);
        #[cfg(feature = "telemetry")]
        if memory_delta != 0
            && let Some(telemetry) = &self.telemetry
        {
            telemetry
                .metrics
                .adjust_memory_bytes(telemetry.shard_id, memory_delta);
        }
        self.enforce_memory_limit(now_ms);
        Ok(())
    }

    /// Finds the best live entry with a cosine score at or above `min_score`.
    pub(crate) fn semantic_search(
        &self,
        query: &SemanticEmbedding,
        min_score: f32,
        now_ms: u64,
    ) -> Option<SemanticMatch> {
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if self.fast_points.is_active() {
            return None;
        }

        self.semantic_index.search(query, min_score, |candidate| {
            let entry = self.entries.find(candidate.hash, |entry| {
                entry.matches_hashed_key(candidate.hash, candidate.key)
            })?;
            if entry.is_expired(now_ms) {
                return None;
            }
            let Some(token) = entry.semantic_index_token else {
                return None;
            };
            if token.id() != candidate.id {
                return None;
            }
            Some(SemanticMatch {
                key: entry.key.as_ref().to_vec(),
                value: entry.value.clone(),
                score: candidate.score,
            })
        })
    }

    /// Finds a live entry whose normalized embedding exactly matches `query`.
    pub(crate) fn semantic_search_exact(
        &self,
        query: &SemanticEmbedding,
        min_score: f32,
        now_ms: u64,
    ) -> Option<SemanticMatch> {
        #[cfg(feature = "experimental-no-ttl-point-hot-path")]
        if self.fast_points.is_active() {
            return None;
        }

        self.semantic_index
            .search_exact(query, min_score, |candidate| {
                let entry = self.entries.find(candidate.hash, |entry| {
                    entry.matches_hashed_key(candidate.hash, candidate.key)
                })?;
                if entry.is_expired(now_ms) {
                    return None;
                }
                let Some(token) = entry.semantic_index_token else {
                    return None;
                };
                if token.id() != candidate.id {
                    return None;
                }
                Some(SemanticMatch {
                    key: entry.key.as_ref().to_vec(),
                    value: entry.value.clone(),
                    score: candidate.score,
                })
            })
    }
}
