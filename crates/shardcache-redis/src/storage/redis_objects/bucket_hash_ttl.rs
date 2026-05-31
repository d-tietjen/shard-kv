use super::*;

/// Condition gate for the HEXPIRE family (`NX`/`XX`/`GT`/`LT`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HashFieldExpireCond {
    None,
    Nx,
    Xx,
    Gt,
    Lt,
}

impl RedisObjectBucket {
    /// HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT core. `expire_at_ms` is the resolved
    /// absolute deadline. Returns one code per field plus the empty-collapse flag:
    ///   1  TTL set
    ///   2  field deleted (deadline already in the past)
    ///   0  NX/XX/GT/LT condition not met
    ///  -2  no such field
    /// Missing key returns one `-2` per requested field.
    pub(crate) fn hash_field_expire(
        &mut self,
        key: &[u8],
        fields: &[&[u8]],
        expire_at_ms: u64,
        cond: HashFieldExpireCond,
        now_ms: u64,
    ) -> (RedisObjectResult, bool) {
        let Some(slot) = self.hashes.get(key).copied() else {
            if self.has_non_hash(key) {
                return (RedisObjectResult::WrongType, false);
            }
            return (
                RedisObjectResult::IntegerArray(vec![-2; fields.len()]),
                false,
            );
        };

        self.remove_expired_hash_fields(key, now_ms);
        if self.remove_hash_if_empty(key) {
            return (
                RedisObjectResult::IntegerArray(vec![-2; fields.len()]),
                true,
            );
        }

        let mut codes = Vec::with_capacity(fields.len());
        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        for &field in fields {
            let hash = self.hash_slab.get(slot).expect("hash slab slot missing");
            // A field already past its own TTL is treated as absent.
            let present =
                hash.contains_key(field) && !self.hash_field_is_expired(key, field, now_ms);
            if !present {
                codes.push(-2);
                continue;
            }
            let current = self.hash_field_expiry(key, field);
            let allowed = match cond {
                HashFieldExpireCond::None => true,
                HashFieldExpireCond::Nx => current.is_none(),
                HashFieldExpireCond::Xx => current.is_some(),
                HashFieldExpireCond::Gt => current.is_some_and(|c| expire_at_ms > c),
                HashFieldExpireCond::Lt => current.is_none_or(|c| expire_at_ms < c),
            };
            if !allowed {
                codes.push(0);
                continue;
            }
            if expire_at_ms <= now_ms {
                // Deadline already passed: delete the field.
                to_delete.push(field.to_vec());
                codes.push(2);
            } else {
                self.set_hash_field_expiry(key, field, expire_at_ms);
                codes.push(1);
            }
        }

        let mut emptied = false;
        if !to_delete.is_empty() {
            let hash = self
                .hash_slab
                .get_mut(slot)
                .expect("hash slab slot missing");
            for field in &to_delete {
                hash.remove(field);
            }
            let now_empty = hash.is_empty();
            for field in &to_delete {
                self.clear_hash_field_ttl(key, field);
            }
            if now_empty {
                self.hashes.remove(key);
                self.hash_slab.remove(slot);
                self.expire_at_ms.remove(key);
                self.hash_field_expire_at_ms.remove(key);
                emptied = true;
            }
        }

        (RedisObjectResult::IntegerArray(codes), emptied)
    }

    /// HTTL/HPTTL (`as_millis`) and HEXPIRETIME/HPEXPIRETIME (`absolute`). Returns
    /// one value per field: the remaining/absolute time, `-1` if the field has no
    /// TTL, `-2` if there is no such field.
    pub(crate) fn hash_field_ttl_query(
        &self,
        key: &[u8],
        fields: &[&[u8]],
        as_millis: bool,
        absolute: bool,
        now_ms: u64,
    ) -> RedisObjectResult {
        let Some(slot) = self.hashes.get(key).copied() else {
            if self.has_non_hash(key) {
                return RedisObjectResult::WrongType;
            }
            return RedisObjectResult::IntegerArray(vec![-2; fields.len()]);
        };
        let hash = self.hash_slab.get(slot).expect("hash slab slot missing");
        let values = fields
            .iter()
            .map(|&field| {
                if !hash.contains_key(field) || self.hash_field_is_expired(key, field, now_ms) {
                    return -2;
                }
                match self.hash_field_expiry(key, field) {
                    None => -1,
                    Some(expire_at_ms) => {
                        if absolute {
                            if as_millis {
                                expire_at_ms as i64
                            } else {
                                (expire_at_ms / 1000) as i64
                            }
                        } else {
                            let remaining_ms = expire_at_ms.saturating_sub(now_ms);
                            if as_millis {
                                remaining_ms as i64
                            } else {
                                // Round up to whole seconds, like Redis.
                                remaining_ms.div_ceil(1000) as i64
                            }
                        }
                    }
                }
            })
            .collect();
        RedisObjectResult::IntegerArray(values)
    }

    /// HPERSIST: remove per-field TTLs. Returns `1` removed, `-1` field had no
    /// TTL, `-2` no such field.
    pub(crate) fn hash_field_persist(
        &mut self,
        key: &[u8],
        fields: &[&[u8]],
        now_ms: u64,
    ) -> (RedisObjectResult, bool) {
        let Some(slot) = self.hashes.get(key).copied() else {
            if self.has_non_hash(key) {
                return (RedisObjectResult::WrongType, false);
            }
            return (
                RedisObjectResult::IntegerArray(vec![-2; fields.len()]),
                false,
            );
        };
        self.remove_expired_hash_fields(key, now_ms);
        if self.remove_hash_if_empty(key) {
            return (
                RedisObjectResult::IntegerArray(vec![-2; fields.len()]),
                true,
            );
        }
        let mut codes = Vec::with_capacity(fields.len());
        for &field in fields {
            let hash = self.hash_slab.get(slot).expect("hash slab slot missing");
            if !hash.contains_key(field) || self.hash_field_is_expired(key, field, now_ms) {
                codes.push(-2);
                continue;
            }
            if self.hash_field_expiry(key, field).is_some() {
                self.clear_hash_field_ttl(key, field);
                codes.push(1);
            } else {
                codes.push(-1);
            }
        }
        (RedisObjectResult::IntegerArray(codes), false)
    }
}
