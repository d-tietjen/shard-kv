use super::helpers::{format_score, parse_f64_bytes, parse_i64_bytes};
use super::*;

impl RedisObjectBucket {
    pub(crate) fn hset(
        &mut self,
        key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> (RedisObjectResult, bool) {
        if let Some(slot) = self.hashes.get(key).copied() {
            let hash = self
                .hash_slab
                .get_mut(slot)
                .expect("hash slab slot missing");
            let inserted = hash.insert_slice(field, value);
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
        if let Some(slot) = self.hashes.get_hashed(key_hash, key) {
            let hash = self
                .hash_slab
                .get_mut(slot)
                .expect("hash slab slot missing");
            let inserted = hash.insert_slice(field, value);
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
        if let Some(slot) = self.hashes.get(key).copied() {
            let hash = self
                .hash_slab
                .get_mut(slot)
                .expect("hash slab slot missing");
            let inserted = fields
                .iter()
                .filter(|(field, value)| hash.insert_slice(field, value))
                .count();
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
            Some(slot) => RedisObjectResult::Bulk(
                self.hash_slab
                    .get(slot)
                    .expect("hash slab slot missing")
                    .get(field)
                    .cloned(),
            ),
            None if self.has_non_hash(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Bulk(None),
        }
    }

    pub(crate) fn hexists(&self, key: &[u8], field: &[u8]) -> RedisObjectResult {
        match self.hashes.get(key).copied() {
            Some(slot) => RedisObjectResult::Integer(
                self.hash_slab
                    .get(slot)
                    .expect("hash slab slot missing")
                    .contains_key(field) as i64,
            ),
            None if self.has_non_hash(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Integer(0),
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
                let hash = self.hash_slab.get(slot).expect("hash slab slot missing");
                write(hash.get(field).map(Vec::as_slice));
                RedisObjectReadOutcome::Written
            }
            None if self.has_non_hash(key) => RedisObjectReadOutcome::WrongType,
            None => RedisObjectReadOutcome::Missing,
        }
    }

    pub(crate) fn hdel(&mut self, key: &[u8], field: &[u8]) -> (RedisObjectResult, bool) {
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
        if let Some(slot) = self.hashes.get(key).copied() {
            let hash = self
                .hash_slab
                .get_mut(slot)
                .expect("hash slab slot missing");
            let removed = fields
                .iter()
                .filter(|field| hash.remove(field).is_some())
                .count();
            let empty = hash.is_empty();
            if empty {
                self.hashes.remove(key);
                self.hash_slab.remove(slot);
                self.expire_at_ms.remove(key);
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
            Some(slot) => RedisObjectResult::Integer(
                self.hash_slab
                    .get(slot)
                    .expect("hash slab slot missing")
                    .len() as i64,
            ),
            None if self.has_non_hash(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Integer(0),
        }
    }

    pub(crate) fn hlen_visit(&self, key: &[u8], write: impl FnOnce(i64)) -> RedisObjectReadOutcome {
        match self.hashes.get(key).copied() {
            Some(slot) => {
                write(
                    self.hash_slab
                        .get(slot)
                        .expect("hash slab slot missing")
                        .len() as i64,
                );
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
                RedisObjectResult::Array(
                    fields
                        .iter()
                        .map(|field| hash.get(field).cloned())
                        .collect(),
                )
            }
            None if self.has_non_hash(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Array(vec![None; fields.len()]),
        }
    }

    pub(crate) fn hkeys(&self, key: &[u8]) -> RedisObjectResult {
        match self.hashes.get(key).copied() {
            Some(slot) => RedisObjectResult::Array(
                self.hash_slab
                    .get(slot)
                    .expect("hash slab slot missing")
                    .fields()
                    .into_iter()
                    .map(Some)
                    .collect(),
            ),
            None if self.has_non_hash(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Array(Vec::new()),
        }
    }

    pub(crate) fn hvals(&self, key: &[u8]) -> RedisObjectResult {
        match self.hashes.get(key).copied() {
            Some(slot) => RedisObjectResult::Array(
                self.hash_slab
                    .get(slot)
                    .expect("hash slab slot missing")
                    .values()
                    .into_iter()
                    .map(Some)
                    .collect(),
            ),
            None if self.has_non_hash(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Array(Vec::new()),
        }
    }

    pub(crate) fn hgetall(&self, key: &[u8]) -> RedisObjectResult {
        match self.hashes.get(key).copied() {
            Some(slot) => RedisObjectResult::Array(
                self.hash_slab
                    .get(slot)
                    .expect("hash slab slot missing")
                    .entries()
                    .into_iter()
                    .flat_map(|(field, value)| [Some(field), Some(value)])
                    .collect(),
            ),
            None if self.has_non_hash(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Array(Vec::new()),
        }
    }

    pub(crate) fn hsetnx(
        &mut self,
        key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> (RedisObjectResult, bool) {
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
                let entries = self
                    .hash_slab
                    .get(slot)
                    .expect("hash slab slot missing")
                    .entries();
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
                emit(RedisObjectArrayItem::Begin(fields.len()));
                for field in fields {
                    emit(RedisObjectArrayItem::Bulk(
                        hash.get(field).map(Vec::as_slice),
                    ));
                }
                RedisObjectReadOutcome::Written
            }
            None if self.has_non_hash(key) => RedisObjectReadOutcome::WrongType,
            None => RedisObjectReadOutcome::Missing,
        }
    }
}
