use super::helpers::{format_score, parse_f64_bytes, parse_i64_bytes};
use super::*;

impl RedisObjectBucket {
    pub(crate) fn hset(
        &mut self,
        key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> (RedisObjectResult, bool) {
        self.remove_expired_hash_fields_for_write(key);
        if let Some(slot) = self.hashes.get(key).copied() {
            let hash = self
                .hash_slab
                .get_mut(slot)
                .expect("hash slab slot missing");
            let inserted = hash.insert_slice(field, value);
            // HSET overwriting a field clears that field's TTL (Redis 7.4).
            if !self.hash_field_expire_at_ms.is_empty() {
                self.clear_hash_field_ttl(key, field);
            }
            return (RedisObjectResult::Integer(inserted as i64), false);
        }
        if self.has_non_hash(key) {
            return (RedisObjectResult::WrongType, false);
        }
        let hash = HashObject::single(field.to_vec(), value.to_vec());
        let slot = self.hash_slab.insert(hash);
        self.hashes.insert(key.to_vec(), slot);
        (RedisObjectResult::Integer(1), true)
    }

    #[inline(always)]
    pub(crate) fn hset_existing_or_wrongtype_hashed(
        &mut self,
        key_hash: u64,
        key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> RedisObjectWriteAttempt {
        self.remove_expired_hash_fields_for_write(key);
        if let Some(slot) = self.hashes.get_hashed(key_hash, key) {
            let hash = self
                .hash_slab
                .get_mut(slot)
                .expect("hash slab slot missing");
            let inserted = hash.insert_slice(field, value);
            if !self.hash_field_expire_at_ms.is_empty() {
                self.clear_hash_field_ttl(key, field);
            }
            return RedisObjectWriteAttempt::Complete(RedisObjectResult::Integer(inserted as i64));
        }
        if self.has_non_hash(key) {
            RedisObjectWriteAttempt::Complete(RedisObjectResult::WrongType)
        } else {
            RedisObjectWriteAttempt::Missing
        }
    }

    #[inline(always)]
    pub(crate) fn hset_new_unchecked_hashed(
        &mut self,
        key_hash: u64,
        key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> RedisObjectResult {
        let hash = HashObject::single(field.to_vec(), value.to_vec());
        let slot = self.hash_slab.insert(hash);
        self.hashes.insert_hashed(key_hash, key.to_vec(), slot);
        RedisObjectResult::Integer(1)
    }

    pub(crate) fn hset_many(
        &mut self,
        key: &[u8],
        fields: &[(&[u8], &[u8])],
    ) -> (RedisObjectResult, bool) {
        if fields.is_empty() {
            return (RedisObjectResult::Integer(0), false);
        }
        self.remove_expired_hash_fields_for_write(key);
        if let Some(slot) = self.hashes.get(key).copied() {
            let hash = self
                .hash_slab
                .get_mut(slot)
                .expect("hash slab slot missing");
            let inserted = fields
                .iter()
                .filter(|(field, value)| hash.insert_slice(field, value))
                .count();
            if !self.hash_field_expire_at_ms.is_empty() {
                for (field, _) in fields {
                    self.clear_hash_field_ttl(key, field);
                }
            }
            return (RedisObjectResult::Integer(inserted as i64), false);
        }
        if self.has_non_hash(key) {
            return (RedisObjectResult::WrongType, false);
        }
        let mut hash = HashObject::map_with_capacity(fields.len());
        let inserted = fields
            .iter()
            .filter(|(field, value)| hash.insert_slice(field, value))
            .count();
        let slot = self.hash_slab.insert(hash);
        self.hashes.insert(key.to_vec(), slot);
        (RedisObjectResult::Integer(inserted as i64), true)
    }

    pub(crate) fn hget(&self, key: &[u8], field: &[u8]) -> RedisObjectResult {
        match self.hashes.get(key).copied() {
            Some(slot) => {
                if self.hash_field_lazily_expired(key, field) {
                    return RedisObjectResult::Bulk(None);
                }
                RedisObjectResult::Bulk(
                    self.hash_slab
                        .get(slot)
                        .expect("hash slab slot missing")
                        .get(field)
                        .cloned(),
                )
            }
            None if self.has_non_hash(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Bulk(None),
        }
    }

    pub(crate) fn hexists(&self, key: &[u8], field: &[u8]) -> RedisObjectResult {
        match self.hashes.get(key).copied() {
            Some(slot) => {
                if self.hash_field_lazily_expired(key, field) {
                    return RedisObjectResult::Integer(0);
                }
                RedisObjectResult::Integer(
                    self.hash_slab
                        .get(slot)
                        .expect("hash slab slot missing")
                        .contains_key(field) as i64,
                )
            }
            None if self.has_non_hash(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Integer(0),
        }
    }

    pub(crate) fn hexists_visit(
        &self,
        key: &[u8],
        field: &[u8],
        write: impl FnOnce(i64),
    ) -> RedisObjectReadOutcome {
        match self.hashes.get(key).copied() {
            Some(slot) => {
                if self.hash_field_lazily_expired(key, field) {
                    write(0);
                    return RedisObjectReadOutcome::Written;
                }
                write(
                    self.hash_slab
                        .get(slot)
                        .expect("hash slab slot missing")
                        .contains_key(field) as i64,
                );
                RedisObjectReadOutcome::Written
            }
            None if self.has_non_hash(key) => RedisObjectReadOutcome::WrongType,
            None => RedisObjectReadOutcome::Missing,
        }
    }

    pub(crate) fn hget_visit(
        &self,
        key: &[u8],
        field: &[u8],
        write: impl FnOnce(Option<&[u8]>),
    ) -> RedisObjectReadOutcome {
        match self.hashes.get(key).copied() {
            Some(slot) => {
                if self.hash_field_lazily_expired(key, field) {
                    write(None);
                    return RedisObjectReadOutcome::Written;
                }
                let hash = self.hash_slab.get(slot).expect("hash slab slot missing");
                write(hash.get(field).map(Vec::as_slice));
                RedisObjectReadOutcome::Written
            }
            None if self.has_non_hash(key) => RedisObjectReadOutcome::WrongType,
            None => RedisObjectReadOutcome::Missing,
        }
    }

    pub(crate) fn hdel(&mut self, key: &[u8], field: &[u8]) -> (RedisObjectResult, bool) {
        self.remove_expired_hash_fields_for_write(key);
        if self.remove_hash_if_empty(key) {
            return (RedisObjectResult::Integer(0), true);
        }
        if let Some(slot) = self.hashes.get(key).copied() {
            let hash = self
                .hash_slab
                .get_mut(slot)
                .expect("hash slab slot missing");
            let removed = hash.remove(field).is_some();
            let empty = hash.is_empty();
            if empty {
                self.hashes.remove(key);
                self.hash_slab.remove(slot);
                self.expire_at_ms.remove(key);
                self.hash_field_expire_at_ms.remove(key);
            } else if removed {
                self.clear_hash_field_ttl(key, field);
            }
            return (RedisObjectResult::Integer(removed as i64), empty);
        }
        if self.has_non_hash(key) {
            (RedisObjectResult::WrongType, false)
        } else {
            (RedisObjectResult::Integer(0), false)
        }
    }

    pub(crate) fn hdel_many(&mut self, key: &[u8], fields: &[&[u8]]) -> (RedisObjectResult, bool) {
        self.remove_expired_hash_fields_for_write(key);
        if self.remove_hash_if_empty(key) {
            return (RedisObjectResult::Integer(0), true);
        }
        if let Some(slot) = self.hashes.get(key).copied() {
            let hash = self
                .hash_slab
                .get_mut(slot)
                .expect("hash slab slot missing");
            let removed_fields: Vec<&[u8]> = fields
                .iter()
                .copied()
                .filter(|field| hash.remove(field).is_some())
                .collect();
            let removed = removed_fields.len();
            let empty = hash.is_empty();
            if empty {
                self.hashes.remove(key);
                self.hash_slab.remove(slot);
                self.expire_at_ms.remove(key);
                self.hash_field_expire_at_ms.remove(key);
            } else {
                for field in removed_fields {
                    self.clear_hash_field_ttl(key, field);
                }
            }
            return (RedisObjectResult::Integer(removed as i64), empty);
        }
        if self.has_non_hash(key) {
            (RedisObjectResult::WrongType, false)
        } else {
            (RedisObjectResult::Integer(0), false)
        }
    }

    pub(crate) fn hlen(&self, key: &[u8]) -> RedisObjectResult {
        match self.hashes.get(key).copied() {
            Some(slot) => {
                let total = self
                    .hash_slab
                    .get(slot)
                    .expect("hash slab slot missing")
                    .len();
                let live = total.saturating_sub(self.hash_expired_field_count(key));
                RedisObjectResult::Integer(live as i64)
            }
            None if self.has_non_hash(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Integer(0),
        }
    }

    pub(crate) fn hlen_visit(&self, key: &[u8], write: impl FnOnce(i64)) -> RedisObjectReadOutcome {
        match self.hashes.get(key).copied() {
            Some(slot) => {
                let total = self
                    .hash_slab
                    .get(slot)
                    .expect("hash slab slot missing")
                    .len();
                let live = total.saturating_sub(self.hash_expired_field_count(key));
                write(live as i64);
                RedisObjectReadOutcome::Written
            }
            None if self.has_non_hash(key) => RedisObjectReadOutcome::WrongType,
            None => RedisObjectReadOutcome::Missing,
        }
    }

    pub(crate) fn hmget(&self, key: &[u8], fields: &[&[u8]]) -> RedisObjectResult {
        match self.hashes.get(key).copied() {
            Some(slot) => {
                let hash = self.hash_slab.get(slot).expect("hash slab slot missing");
                let now_ms =
                    (!self.hash_field_expire_at_ms.is_empty()).then(crate::storage::now_millis);
                RedisObjectResult::Array(
                    fields
                        .iter()
                        .map(|field| match now_ms {
                            Some(now_ms) if self.hash_field_is_expired(key, field, now_ms) => None,
                            _ => hash.get(field).cloned(),
                        })
                        .collect(),
                )
            }
            None if self.has_non_hash(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Array(vec![None; fields.len()]),
        }
    }

    pub(crate) fn hkeys(&self, key: &[u8]) -> RedisObjectResult {
        match self.hashes.get(key).copied() {
            Some(slot) => {
                let hash = self.hash_slab.get(slot).expect("hash slab slot missing");
                let now_ms = self.hash_field_now_ms_for(key);
                RedisObjectResult::Array(
                    hash.fields()
                        .into_iter()
                        .filter(|field| match now_ms {
                            Some(now_ms) => !self.hash_field_is_expired(key, field, now_ms),
                            None => true,
                        })
                        .map(Some)
                        .collect(),
                )
            }
            None if self.has_non_hash(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Array(Vec::new()),
        }
    }

    pub(crate) fn hvals(&self, key: &[u8]) -> RedisObjectResult {
        match self.hashes.get(key).copied() {
            Some(slot) => {
                let hash = self.hash_slab.get(slot).expect("hash slab slot missing");
                let now_ms = self.hash_field_now_ms_for(key);
                RedisObjectResult::Array(
                    hash.entries()
                        .into_iter()
                        .filter(|(field, _)| match now_ms {
                            Some(now_ms) => !self.hash_field_is_expired(key, field, now_ms),
                            None => true,
                        })
                        .map(|(_, value)| Some(value))
                        .collect(),
                )
            }
            None if self.has_non_hash(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Array(Vec::new()),
        }
    }

    pub(crate) fn hkeys_visit(
        &self,
        key: &[u8],
        emit: &mut impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome {
        match self.hashes.get(key).copied() {
            Some(slot) => {
                let hash = self.hash_slab.get(slot).expect("hash slab slot missing");
                match self.hash_field_now_ms_for(key) {
                    None => {
                        emit(RedisObjectArrayItem::Begin(hash.len()));
                        hash.visit_fields(|field| emit(RedisObjectArrayItem::Bulk(Some(field))));
                    }
                    Some(now_ms) => {
                        let live: Vec<Bytes> = hash
                            .fields()
                            .into_iter()
                            .filter(|field| !self.hash_field_is_expired(key, field, now_ms))
                            .collect();
                        emit(RedisObjectArrayItem::Begin(live.len()));
                        for field in &live {
                            emit(RedisObjectArrayItem::Bulk(Some(field)));
                        }
                    }
                }
                RedisObjectReadOutcome::Written
            }
            None if self.has_non_hash(key) => RedisObjectReadOutcome::WrongType,
            None => RedisObjectReadOutcome::Missing,
        }
    }

    pub(crate) fn hvals_visit(
        &self,
        key: &[u8],
        emit: &mut impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome {
        match self.hashes.get(key).copied() {
            Some(slot) => {
                let hash = self.hash_slab.get(slot).expect("hash slab slot missing");
                match self.hash_field_now_ms_for(key) {
                    None => {
                        emit(RedisObjectArrayItem::Begin(hash.len()));
                        hash.visit_values(|value| emit(RedisObjectArrayItem::Bulk(Some(value))));
                    }
                    Some(now_ms) => {
                        let live: Vec<Bytes> = hash
                            .entries()
                            .into_iter()
                            .filter(|(field, _)| !self.hash_field_is_expired(key, field, now_ms))
                            .map(|(_, value)| value)
                            .collect();
                        emit(RedisObjectArrayItem::Begin(live.len()));
                        for value in &live {
                            emit(RedisObjectArrayItem::Bulk(Some(value)));
                        }
                    }
                }
                RedisObjectReadOutcome::Written
            }
            None if self.has_non_hash(key) => RedisObjectReadOutcome::WrongType,
            None => RedisObjectReadOutcome::Missing,
        }
    }

    pub(crate) fn hgetall(&self, key: &[u8]) -> RedisObjectResult {
        match self.hashes.get(key).copied() {
            Some(slot) => {
                let hash = self.hash_slab.get(slot).expect("hash slab slot missing");
                let now_ms = self.hash_field_now_ms_for(key);
                RedisObjectResult::Array(
                    hash.entries()
                        .into_iter()
                        .filter(|(field, _)| match now_ms {
                            Some(now_ms) => !self.hash_field_is_expired(key, field, now_ms),
                            None => true,
                        })
                        .flat_map(|(field, value)| [Some(field), Some(value)])
                        .collect(),
                )
            }
            None if self.has_non_hash(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Array(Vec::new()),
        }
    }

    pub(crate) fn hgetall_visit(
        &self,
        key: &[u8],
        emit: &mut impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome {
        match self.hashes.get(key).copied() {
            Some(slot) => {
                let hash = self.hash_slab.get(slot).expect("hash slab slot missing");
                match self.hash_field_now_ms_for(key) {
                    None => {
                        emit(RedisObjectArrayItem::Begin(hash.len().saturating_mul(2)));
                        hash.visit_entries(|field, value| {
                            emit(RedisObjectArrayItem::Bulk(Some(field)));
                            emit(RedisObjectArrayItem::Bulk(Some(value)));
                        });
                    }
                    Some(now_ms) => {
                        let live: Vec<(Bytes, Bytes)> = hash
                            .entries()
                            .into_iter()
                            .filter(|(field, _)| !self.hash_field_is_expired(key, field, now_ms))
                            .collect();
                        emit(RedisObjectArrayItem::Begin(live.len().saturating_mul(2)));
                        for (field, value) in &live {
                            emit(RedisObjectArrayItem::Bulk(Some(field)));
                            emit(RedisObjectArrayItem::Bulk(Some(value)));
                        }
                    }
                }
                RedisObjectReadOutcome::Written
            }
            None if self.has_non_hash(key) => RedisObjectReadOutcome::WrongType,
            None => RedisObjectReadOutcome::Missing,
        }
    }

    pub(crate) fn hsetnx(
        &mut self,
        key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> (RedisObjectResult, bool) {
        self.remove_expired_hash_fields_for_write(key);
        if let Some(slot) = self.hashes.get(key).copied() {
            let hash = self
                .hash_slab
                .get_mut(slot)
                .expect("hash slab slot missing");
            if hash.contains_key(field) {
                return (RedisObjectResult::Integer(0), false);
            }
            hash.insert_slice(field, value);
            return (RedisObjectResult::Integer(1), false);
        }
        if self.has_non_hash(key) {
            return (RedisObjectResult::WrongType, false);
        }
        let hash = HashObject::single(field.to_vec(), value.to_vec());
        let slot = self.hash_slab.insert(hash);
        self.hashes.insert(key.to_vec(), slot);
        (RedisObjectResult::Integer(1), true)
    }

    pub(crate) fn hincrby(
        &mut self,
        key: &[u8],
        field: &[u8],
        delta: i64,
    ) -> (RedisObjectResult, bool) {
        self.remove_expired_hash_fields_for_write(key);
        let current = match self.hashes.get(key).copied() {
            Some(slot) => self
                .hash_slab
                .get(slot)
                .expect("hash slab slot missing")
                .get(field)
                .map(|value| parse_i64_bytes(value))
                .transpose(),
            None if self.has_non_hash(key) => return (RedisObjectResult::WrongType, false),
            None => Ok(None),
        };
        let Ok(current) = current else {
            return (
                RedisObjectResult::Simple("ERR hash value is not an integer"),
                false,
            );
        };
        let Some(value) = current.unwrap_or(0).checked_add(delta) else {
            return (
                RedisObjectResult::Simple("ERR increment or decrement would overflow"),
                false,
            );
        };
        let value_bytes = value.to_string();
        let (result, created) = self.hset(key, field, value_bytes.as_bytes());
        if matches!(result, RedisObjectResult::WrongType) {
            (result, created)
        } else {
            (RedisObjectResult::Integer(value), created)
        }
    }

    pub(crate) fn hincrbyfloat(
        &mut self,
        key: &[u8],
        field: &[u8],
        delta: f64,
    ) -> (RedisObjectResult, bool) {
        self.remove_expired_hash_fields_for_write(key);
        let current = match self.hashes.get(key).copied() {
            Some(slot) => self
                .hash_slab
                .get(slot)
                .expect("hash slab slot missing")
                .get(field)
                .map(|value| parse_f64_bytes(value))
                .transpose(),
            None if self.has_non_hash(key) => return (RedisObjectResult::WrongType, false),
            None => Ok(None),
        };
        let Ok(current) = current else {
            return (
                RedisObjectResult::Simple("ERR hash value is not a float"),
                false,
            );
        };
        let value = current.unwrap_or(0.0) + delta;
        if !value.is_finite() {
            return (
                RedisObjectResult::Simple("ERR increment would produce NaN or Infinity"),
                false,
            );
        }
        let value_bytes = format_score(value);
        let (result, created) = self.hset(key, field, &value_bytes);
        if matches!(result, RedisObjectResult::WrongType) {
            (result, created)
        } else {
            (RedisObjectResult::Bulk(Some(value_bytes)), created)
        }
    }

    pub(crate) fn hrandfield(
        &self,
        key: &[u8],
        count: Option<i64>,
        with_values: bool,
    ) -> RedisObjectResult {
        match self.hashes.get(key).copied() {
            Some(slot) => {
                let mut entries = self
                    .hash_slab
                    .get(slot)
                    .expect("hash slab slot missing")
                    .entries();
                if let Some(now_ms) = self.hash_field_now_ms_for(key) {
                    entries.retain(|(field, _)| !self.hash_field_is_expired(key, field, now_ms));
                }
                if let Some(count) = count {
                    let take = count.unsigned_abs() as usize;
                    let iter = entries.into_iter().take(take);
                    if with_values {
                        RedisObjectResult::Array(
                            iter.flat_map(|(field, value)| [Some(field), Some(value)])
                                .collect(),
                        )
                    } else {
                        RedisObjectResult::Array(iter.map(|(field, _)| Some(field)).collect())
                    }
                } else {
                    RedisObjectResult::Bulk(entries.into_iter().next().map(|(field, _)| field))
                }
            }
            None if self.has_non_hash(key) => RedisObjectResult::WrongType,
            None => {
                if count.is_some() {
                    RedisObjectResult::Array(Vec::new())
                } else {
                    RedisObjectResult::Bulk(None)
                }
            }
        }
    }

    pub(crate) fn hmget_visit(
        &self,
        key: &[u8],
        fields: &[&[u8]],
        mut emit: impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome {
        match self.hashes.get(key).copied() {
            Some(slot) => {
                let hash = self.hash_slab.get(slot).expect("hash slab slot missing");
                let now_ms = self.hash_field_now_ms_for(key);
                emit(RedisObjectArrayItem::Begin(fields.len()));
                for field in fields {
                    let value = match now_ms {
                        Some(now_ms) if self.hash_field_is_expired(key, field, now_ms) => None,
                        _ => hash.get(field).map(Vec::as_slice),
                    };
                    emit(RedisObjectArrayItem::Bulk(value));
                }
                RedisObjectReadOutcome::Written
            }
            None if self.has_non_hash(key) => RedisObjectReadOutcome::WrongType,
            None => RedisObjectReadOutcome::Missing,
        }
    }

    // --- Per-field TTL helpers (Redis 7.2+ hash-field-expire family) ---------

    /// Set a field's absolute expiry (unix ms). Caller guarantees the field
    /// exists. A lazily-allocated submap keeps TTL-free hashes free.
    pub(crate) fn set_hash_field_expiry(&mut self, key: &[u8], field: &[u8], expire_at_ms: u64) {
        self.hash_field_expire_at_ms
            .entry(key.to_vec())
            .or_default()
            .insert(field.to_vec(), expire_at_ms);
    }

    /// Remove a single field's TTL, dropping the key's submap when it empties.
    pub(crate) fn clear_hash_field_ttl(&mut self, key: &[u8], field: &[u8]) {
        if let Some(submap) = self.hash_field_expire_at_ms.get_mut(key) {
            submap.remove(field);
            if submap.is_empty() {
                self.hash_field_expire_at_ms.remove(key);
            }
        }
    }

    /// Absolute expiry (unix ms) for a field, if it has one.
    pub(crate) fn hash_field_expiry(&self, key: &[u8], field: &[u8]) -> Option<u64> {
        self.hash_field_expire_at_ms
            .get(key)
            .and_then(|submap| submap.get(field).copied())
    }

    /// Drop elapsed field TTLs from the physical hash map. The hash key is left
    /// in place even if it becomes empty so write commands can immediately reuse
    /// the slot without perturbing object-count bookkeeping.
    pub(crate) fn remove_expired_hash_fields(&mut self, key: &[u8], now_ms: u64) {
        let Some(ttls) = self.hash_field_expire_at_ms.get(key) else {
            return;
        };
        let expired = ttls
            .iter()
            .filter(|(_, expire_at_ms)| **expire_at_ms <= now_ms)
            .map(|(field, _)| field.clone())
            .collect::<Vec<_>>();
        if expired.is_empty() {
            return;
        }
        if let Some(slot) = self.hashes.get(key).copied() {
            let hash = self
                .hash_slab
                .get_mut(slot)
                .expect("hash slab slot missing");
            for field in &expired {
                hash.remove(field);
            }
        }
        for field in &expired {
            self.clear_hash_field_ttl(key, field);
        }
    }

    #[inline]
    pub(crate) fn remove_hash_if_empty(&mut self, key: &[u8]) -> bool {
        let Some(slot) = self.hashes.get(key).copied() else {
            return false;
        };
        if !self
            .hash_slab
            .get(slot)
            .expect("hash slab slot missing")
            .is_empty()
        {
            return false;
        }
        self.remove_hash_slot(key, slot);
        true
    }

    pub(crate) fn remove_expired_hash_if_empty(&mut self, key: &[u8], now_ms: u64) -> bool {
        self.remove_expired_hash_fields(key, now_ms);
        self.remove_hash_if_empty(key)
    }

    pub(crate) fn hash_needs_empty_expiry_cleanup(&self, key: &[u8], now_ms: u64) -> bool {
        self.hashes.contains_key(key) && !self.hash_has_live_fields(key, now_ms)
    }

    /// True when the field has a TTL that is at or before `now_ms`.
    pub(crate) fn hash_field_is_expired(&self, key: &[u8], field: &[u8], now_ms: u64) -> bool {
        self.hash_field_expiry(key, field)
            .is_some_and(|expire_at_ms| expire_at_ms <= now_ms)
    }

    /// True when the hash key has at least one field that is not logically
    /// expired. Used by key-level commands such as EXISTS/TYPE/SCAN.
    pub(crate) fn hash_has_live_fields(&self, key: &[u8], now_ms: u64) -> bool {
        let Some(slot) = self.hashes.get(key).copied() else {
            return false;
        };
        let hash = self.hash_slab.get(slot).expect("hash slab slot missing");
        if hash.is_empty() {
            return false;
        }
        if !self.hash_field_expire_at_ms.contains_key(key) {
            return true;
        }
        hash.fields()
            .into_iter()
            .any(|field| !self.hash_field_is_expired(key, &field, now_ms))
    }

    /// Lazy-expiry guard for reads. Returns true when `field` is gone due to a
    /// per-field TTL. Cheap on the common path: if no key has field TTLs the
    /// outer map is empty and we never read the clock.
    #[inline]
    fn hash_field_lazily_expired(&self, key: &[u8], field: &[u8]) -> bool {
        if self.hash_field_expire_at_ms.is_empty() {
            return false;
        }
        self.hash_field_is_expired(key, field, crate::storage::now_millis())
    }

    /// Returns `Some(now_ms)` only when `key` actually has per-field TTLs, so
    /// callers can take a zero-overhead fast path otherwise (and read the clock
    /// just once when they do need it).
    #[inline]
    pub(crate) fn hash_field_now_ms_for(&self, key: &[u8]) -> Option<u64> {
        if self.hash_field_expire_at_ms.is_empty() {
            return None;
        }
        self.hash_field_expire_at_ms
            .get(key)
            .filter(|submap| !submap.is_empty())
            .map(|_| crate::storage::now_millis())
    }

    /// Count of fields in `key` whose per-field TTL has elapsed (for HLEN-style
    /// adjustments). Zero on the common path.
    fn hash_expired_field_count(&self, key: &[u8]) -> usize {
        if self.hash_field_expire_at_ms.is_empty() {
            return 0;
        }
        let now_ms = crate::storage::now_millis();
        self.hash_field_expire_at_ms
            .get(key)
            .map(|submap| {
                submap
                    .values()
                    .filter(|expire_at_ms| **expire_at_ms <= now_ms)
                    .count()
            })
            .unwrap_or(0)
    }

    #[inline]
    fn remove_expired_hash_fields_for_write(&mut self, key: &[u8]) {
        if self.hash_field_expire_at_ms.contains_key(key) {
            self.remove_expired_hash_fields(key, crate::storage::now_millis());
        }
    }

    #[inline]
    fn remove_hash_slot(&mut self, key: &[u8], slot: SlotId) {
        self.hashes.remove(key);
        self.hash_slab.remove(slot);
        self.expire_at_ms.remove(key);
        self.hash_field_expire_at_ms.remove(key);
    }
}
