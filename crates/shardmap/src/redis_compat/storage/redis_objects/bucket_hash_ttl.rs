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

/// Expiration action used by Redis 8 HGETEX.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashFieldGetExpireAction {
    Keep,
    Persist,
    ExpireAt(u64),
}

/// Conditional write gate used by Redis 8 HSETEX.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashFieldSetCondition {
    Always,
    FnX,
    FxX,
}

/// TTL behavior for Redis 8 HSETEX writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashFieldSetExpireAction {
    Clear,
    KeepTtl,
    ExpireAt(u64),
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
            return match self.has_non_hash(key).then_some(()) {
                Some(()) => (RedisObjectResult::WrongType, false),
                None => (
                    RedisObjectResult::IntegerArray(vec![-2; fields.len()]),
                    false,
                ),
            };
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
            return match self.has_non_hash(key).then_some(()) {
                Some(()) => RedisObjectResult::WrongType,
                None => RedisObjectResult::IntegerArray(vec![-2; fields.len()]),
            };
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
                    Some(expire_at_ms) => match (absolute, as_millis) {
                        (true, true) => expire_at_ms as i64,
                        (true, false) => (expire_at_ms / 1000) as i64,
                        (false, true) => expire_at_ms.saturating_sub(now_ms) as i64,
                        (false, false) => {
                            // Round up to whole seconds, like Redis.
                            expire_at_ms.saturating_sub(now_ms).div_ceil(1000) as i64
                        }
                    },
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
            return match self.has_non_hash(key).then_some(()) {
                Some(()) => (RedisObjectResult::WrongType, false),
                None => (
                    RedisObjectResult::IntegerArray(vec![-2; fields.len()]),
                    false,
                ),
            };
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
            match self.hash_field_expiry(key, field) {
                Some(_) => {
                    self.clear_hash_field_ttl(key, field);
                    codes.push(1);
                }
                None => codes.push(-1),
            }
        }
        (RedisObjectResult::IntegerArray(codes), false)
    }

    /// HGETDEL: return the current field values and delete the live fields that
    /// were returned.
    pub(crate) fn hash_field_getdel(
        &mut self,
        key: &[u8],
        fields: &[&[u8]],
        now_ms: u64,
    ) -> (RedisObjectResult, bool) {
        let Some(slot) = self.hashes.get(key).copied() else {
            return match self.has_non_hash(key).then_some(()) {
                Some(()) => (RedisObjectResult::WrongType, false),
                None => (RedisObjectResult::Array(vec![None; fields.len()]), false),
            };
        };
        self.remove_expired_hash_fields(key, now_ms);
        if self.remove_hash_if_empty(key) {
            return (RedisObjectResult::Array(vec![None; fields.len()]), true);
        }

        let hash = self.hash_slab.get(slot).expect("hash slab slot missing");
        let values = fields
            .iter()
            .map(|field| hash.get(field).cloned())
            .collect::<Vec<_>>();
        let to_delete = fields
            .iter()
            .zip(values.iter())
            .filter(|(_, value)| value.is_some())
            .map(|(field, _)| (*field).to_vec())
            .collect::<Vec<_>>();

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
                self.remove_hash_slot(key, slot);
                emptied = true;
            }
        }

        (RedisObjectResult::Array(values), emptied)
    }

    /// HGETEX: return field values and optionally update their field TTLs.
    pub(crate) fn hash_field_getex(
        &mut self,
        key: &[u8],
        fields: &[&[u8]],
        action: HashFieldGetExpireAction,
        now_ms: u64,
    ) -> (RedisObjectResult, bool) {
        let Some(slot) = self.hashes.get(key).copied() else {
            return match self.has_non_hash(key).then_some(()) {
                Some(()) => (RedisObjectResult::WrongType, false),
                None => (RedisObjectResult::Array(vec![None; fields.len()]), false),
            };
        };
        self.remove_expired_hash_fields(key, now_ms);
        if self.remove_hash_if_empty(key) {
            return (RedisObjectResult::Array(vec![None; fields.len()]), true);
        }

        let hash = self.hash_slab.get(slot).expect("hash slab slot missing");
        let values = fields
            .iter()
            .map(|field| hash.get(field).cloned())
            .collect::<Vec<_>>();
        let existing_fields = fields
            .iter()
            .zip(values.iter())
            .filter(|(_, value)| value.is_some())
            .map(|(field, _)| (*field).to_vec())
            .collect::<Vec<_>>();

        let mut to_delete = Vec::new();
        match action {
            HashFieldGetExpireAction::Keep => {}
            HashFieldGetExpireAction::Persist => {
                for field in &existing_fields {
                    self.clear_hash_field_ttl(key, field);
                }
            }
            HashFieldGetExpireAction::ExpireAt(expire_at_ms) if expire_at_ms <= now_ms => {
                to_delete = existing_fields;
            }
            HashFieldGetExpireAction::ExpireAt(expire_at_ms) => {
                for field in &existing_fields {
                    self.set_hash_field_expiry(key, field, expire_at_ms);
                }
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
                self.remove_hash_slot(key, slot);
                emptied = true;
            }
        }

        (RedisObjectResult::Array(values), emptied)
    }

    /// HSETEX: set fields atomically under FNX/FXX and apply the requested
    /// field-TTL behavior.
    pub(crate) fn hash_field_setex(
        &mut self,
        key: &[u8],
        fields: &[(&[u8], &[u8])],
        condition: HashFieldSetCondition,
        action: HashFieldSetExpireAction,
        now_ms: u64,
    ) -> (RedisObjectResult, bool) {
        if fields.is_empty() {
            return (RedisObjectResult::Integer(0), false);
        }
        self.remove_expired_hash_fields(key, now_ms);
        if self.remove_hash_if_empty(key) {
            self.expire_at_ms.remove(key);
        }

        let existing_slot = self.hashes.get(key).copied();
        if existing_slot.is_none() && self.has_non_hash(key) {
            return (RedisObjectResult::WrongType, false);
        }

        let existing_count = existing_slot
            .and_then(|slot| self.hash_slab.get(slot))
            .map(|hash| {
                fields
                    .iter()
                    .filter(|(field, _)| hash.contains_key(field))
                    .count()
            })
            .unwrap_or(0);
        let allowed = match condition {
            HashFieldSetCondition::Always => true,
            HashFieldSetCondition::FnX => existing_count == 0,
            HashFieldSetCondition::FxX => existing_count == fields.len(),
        };
        if !allowed {
            return (RedisObjectResult::Integer(0), false);
        }

        let (slot, created) = match existing_slot {
            Some(slot) => (slot, false),
            None => {
                let slot = self
                    .hash_slab
                    .insert(HashObject::map_with_capacity(fields.len()));
                self.hashes.insert(key.to_vec(), slot);
                (slot, true)
            }
        };

        {
            let hash = self
                .hash_slab
                .get_mut(slot)
                .expect("hash slab slot missing");
            for (field, value) in fields {
                hash.insert_slice(field, value);
            }
        }

        match action {
            HashFieldSetExpireAction::KeepTtl => {}
            HashFieldSetExpireAction::Clear => {
                for (field, _) in fields {
                    self.clear_hash_field_ttl(key, field);
                }
            }
            HashFieldSetExpireAction::ExpireAt(expire_at_ms) if expire_at_ms <= now_ms => {
                let hash = self
                    .hash_slab
                    .get_mut(slot)
                    .expect("hash slab slot missing");
                for (field, _) in fields {
                    hash.remove(field);
                }
                let now_empty = hash.is_empty();
                for (field, _) in fields {
                    self.clear_hash_field_ttl(key, field);
                }
                if now_empty {
                    self.remove_hash_slot(key, slot);
                    return (RedisObjectResult::Integer(1), true);
                }
            }
            HashFieldSetExpireAction::ExpireAt(expire_at_ms) => {
                for (field, _) in fields {
                    self.set_hash_field_expiry(key, field, expire_at_ms);
                }
            }
        }

        (RedisObjectResult::Integer(1), created)
    }
}
