use super::*;

impl RedisObjectBucket {
    #[inline(always)]
    pub(crate) fn contains_object(&self, key: &[u8]) -> bool {
        self.hashes.contains_key(key)
            || self.lists.contains_key(key)
            || self.sets.contains_key(key)
            || self.zsets.contains_key(key)
    }

    #[inline(always)]
    pub(crate) fn contains_live_object(&self, key: &[u8], now_ms: u64) -> bool {
        if self.object_is_expired(key, now_ms) {
            return false;
        }
        self.hash_has_live_fields(key, now_ms)
            || self.lists.contains_key(key)
            || self.sets.contains_key(key)
            || self.zsets.contains_key(key)
    }

    #[inline(always)]
    pub(crate) fn has_expirations(&self) -> bool {
        !self.expire_at_ms.is_empty()
    }

    #[inline(always)]
    pub(crate) fn object_is_expired(&self, key: &[u8], now_ms: u64) -> bool {
        self.expire_at_ms
            .get(key)
            .is_some_and(|expire_at_ms| *expire_at_ms <= now_ms)
    }

    pub(crate) fn delete_expired(&mut self, key: &[u8], now_ms: u64) -> bool {
        if self.object_is_expired(key, now_ms) {
            return self.delete_any(key);
        }
        false
    }

    pub(crate) fn delete_any(&mut self, key: &[u8]) -> bool {
        if let Some(slot) = self.hashes.remove(key) {
            self.hash_slab.remove(slot);
            self.expire_at_ms.remove(key);
            self.hash_field_expire_at_ms.remove(key);
            return true;
        }
        if let Some(slot) = self.lists.remove(key) {
            self.list_slab.remove(slot);
            self.expire_at_ms.remove(key);
            return true;
        }
        if let Some(slot) = self.sets.remove(key) {
            self.set_slab.remove(slot);
            self.expire_at_ms.remove(key);
            return true;
        }
        if let Some(slot) = self.zsets.remove(key) {
            self.zset_slab.remove(slot);
            self.expire_at_ms.remove(key);
            return true;
        }
        false
    }

    pub(crate) fn expire(&mut self, key: &[u8], expire_at_ms: u64, now_ms: u64) -> bool {
        if self.delete_expired(key, now_ms) {
            return false;
        }
        if self.contains_object(key) {
            self.expire_at_ms.insert(key.to_vec(), expire_at_ms);
            true
        } else {
            false
        }
    }

    pub(crate) fn persist(&mut self, key: &[u8], now_ms: u64) -> bool {
        if self.delete_expired(key, now_ms) {
            return false;
        }
        self.expire_at_ms.remove(key).is_some()
    }

    pub(crate) fn ttl_millis(&mut self, key: &[u8], now_ms: u64) -> i64 {
        if self.delete_expired(key, now_ms) {
            return -2;
        }
        if self.remove_expired_hash_if_empty(key, now_ms) {
            return -2;
        }
        if !self.contains_object(key) {
            return -2;
        }
        self.expire_at_ms
            .get(key)
            .map(|expire_at_ms| expire_at_ms.saturating_sub(now_ms) as i64)
            .unwrap_or(-1)
    }

    pub(crate) fn clone_value(&self, key: &[u8], now_ms: u64) -> Option<RedisObjectValue> {
        if let Some(slot) = self.hashes.get(key).copied() {
            if !self.hash_has_live_fields(key, now_ms) {
                return None;
            }
            let now_ms = self.hash_field_now_ms_for(key);
            return Some(RedisObjectValue::Hash(
                self.hash_slab
                    .get(slot)
                    .expect("hash slab slot missing")
                    .entries()
                    .into_iter()
                    .filter(|(field, _)| match now_ms {
                        Some(now_ms) => !self.hash_field_is_expired(key, field, now_ms),
                        None => true,
                    })
                    .collect(),
            ));
        }
        if let Some(slot) = self.lists.get(key).copied() {
            return Some(RedisObjectValue::List(
                self.list_slab
                    .get(slot)
                    .expect("list slab slot missing")
                    .iter()
                    .cloned()
                    .collect(),
            ));
        }
        if let Some(slot) = self.sets.get(key).copied() {
            return Some(RedisObjectValue::Set(
                self.set_slab
                    .get(slot)
                    .expect("set slab slot missing")
                    .iter()
                    .cloned()
                    .collect(),
            ));
        }
        self.zsets.get(key).copied().map(|slot| {
            RedisObjectValue::ZSet(
                self.zset_slab
                    .get(slot)
                    .expect("zset slab slot missing")
                    .entries(),
            )
        })
    }

    pub(crate) fn insert_value(&mut self, key: Bytes, value: RedisObjectValue) -> bool {
        let existed = self.delete_any(&key);
        match value {
            RedisObjectValue::Hash(entries) => {
                let mut hash = HashObject::map_with_capacity(entries.len());
                for (field, value) in entries {
                    hash.insert_slice(&field, &value);
                }
                let slot = self.hash_slab.insert(hash);
                self.hashes.insert(key, slot);
            }
            RedisObjectValue::List(entries) => {
                let slot = self.list_slab.insert(ListObject::from_vec(entries));
                self.lists.insert(key, slot);
            }
            RedisObjectValue::Set(entries) => {
                let mut set = SetObject::empty();
                let refs = entries
                    .iter()
                    .map(Vec::as_slice)
                    .collect::<SmallVec<[&[u8]; 8]>>();
                set.insert_many(&refs);
                let slot = self.set_slab.insert(set);
                self.sets.insert(key, slot);
            }
            RedisObjectValue::ZSet(entries) => {
                let mut zset = ZSetObject::map_with_capacity(entries.len());
                for (member, score) in entries {
                    zset.insert_slice(&member, score);
                }
                let slot = self.zset_slab.insert(zset);
                self.zsets.insert(key, slot);
            }
        }
        !existed
    }

    pub(crate) fn type_name(&self, key: &[u8], now_ms: u64) -> Option<&'static str> {
        match (
            self.hash_has_live_fields(key, now_ms),
            self.lists.contains_key(key),
            self.sets.contains_key(key),
            self.zsets.contains_key(key),
        ) {
            (true, _, _, _) => Some("hash"),
            (false, true, _, _) => Some("list"),
            (false, false, true, _) => Some("set"),
            (false, false, false, true) => Some("zset"),
            (false, false, false, false) => None,
        }
    }

    pub(crate) fn encoding(&self, key: &[u8], now_ms: u64) -> Option<&'static str> {
        match (
            self.hash_has_live_fields(key, now_ms),
            self.lists.contains_key(key),
            self.sets.contains_key(key),
            self.zsets.contains_key(key),
        ) {
            (true, _, _, _) | (false, false, true, _) => Some("hashtable"),
            (false, true, _, _) => Some("quicklist"),
            (false, false, false, true) => Some("skiplist"),
            (false, false, false, false) => None,
        }
    }

    pub(crate) fn keys_with_type(&self, out: &mut Vec<(Bytes, &'static str)>, now_ms: u64) {
        out.extend(
            self.hashes
                .keys()
                .filter(|key| !self.object_is_expired(key, now_ms))
                .filter(|key| self.hash_has_live_fields(key, now_ms))
                .cloned()
                .map(|key| (key, "hash")),
        );
        out.extend(
            self.lists
                .keys()
                .filter(|key| !self.object_is_expired(key, now_ms))
                .cloned()
                .map(|key| (key, "list")),
        );
        out.extend(
            self.sets
                .keys()
                .filter(|key| !self.object_is_expired(key, now_ms))
                .cloned()
                .map(|key| (key, "set")),
        );
        out.extend(
            self.zsets
                .keys()
                .filter(|key| !self.object_is_expired(key, now_ms))
                .cloned()
                .map(|key| (key, "zset")),
        );
    }

    pub(crate) fn keys(&self, out: &mut Vec<Bytes>, now_ms: u64) {
        out.extend(
            self.hashes
                .keys()
                .filter(|key| !self.object_is_expired(key, now_ms))
                .filter(|key| self.hash_has_live_fields(key, now_ms))
                .cloned(),
        );
        out.extend(
            self.lists
                .keys()
                .filter(|key| !self.object_is_expired(key, now_ms))
                .cloned(),
        );
        out.extend(
            self.sets
                .keys()
                .filter(|key| !self.object_is_expired(key, now_ms))
                .cloned(),
        );
        out.extend(
            self.zsets
                .keys()
                .filter(|key| !self.object_is_expired(key, now_ms))
                .cloned(),
        );
    }

    pub(crate) fn live_object_count(&self, now_ms: u64) -> usize {
        if self.expire_at_ms.is_empty() && self.hash_field_expire_at_ms.is_empty() {
            return self.hashes.len() + self.lists.len() + self.sets.len() + self.zsets.len();
        }

        let hash_count = self
            .hashes
            .keys()
            .filter(|key| !self.object_is_expired(key, now_ms))
            .filter(|key| self.hash_has_live_fields(key, now_ms))
            .count();
        if self.expire_at_ms.is_empty() {
            return hash_count + self.lists.len() + self.sets.len() + self.zsets.len();
        }

        hash_count
            + self
                .lists
                .keys()
                .filter(|key| !self.object_is_expired(key, now_ms))
                .count()
            + self
                .sets
                .keys()
                .filter(|key| !self.object_is_expired(key, now_ms))
                .count()
            + self
                .zsets
                .keys()
                .filter(|key| !self.object_is_expired(key, now_ms))
                .count()
    }

    pub(crate) fn visit_keys(&self, now_ms: u64, visitor: &mut impl FnMut(&[u8]) -> bool) -> bool {
        if !self.has_expirations() {
            return visit_live_hash_key_iter(self.hashes.keys(), now_ms, self, visitor)
                && visit_key_iter(self.lists.keys(), visitor)
                && visit_key_iter(self.sets.keys(), visitor)
                && visit_key_iter(self.zsets.keys(), visitor);
        }

        self.visit_unexpired_live_hash_key_iter(self.hashes.keys(), now_ms, visitor)
            && self.visit_unexpired_key_iter(self.lists.keys(), now_ms, visitor)
            && self.visit_unexpired_key_iter(self.sets.keys(), now_ms, visitor)
            && self.visit_unexpired_key_iter(self.zsets.keys(), now_ms, visitor)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn scan_keys_visit(
        &self,
        now_ms: u64,
        type_filter: Option<&[u8]>,
        cursor_offset: usize,
        position: &mut usize,
        visited: &mut usize,
        emitted: &mut usize,
        limit: usize,
        visitor: &mut impl FnMut(&[u8]) -> bool,
    ) -> Option<usize> {
        if let Some(kind) = type_filter {
            return match kind {
                kind if kind.eq_ignore_ascii_case(b"hash") => self.scan_key_iter(
                    self.hashes.keys(),
                    now_ms,
                    cursor_offset,
                    position,
                    visited,
                    emitted,
                    limit,
                    true,
                    visitor,
                ),
                kind if kind.eq_ignore_ascii_case(b"list") => self.scan_key_iter(
                    self.lists.keys(),
                    now_ms,
                    cursor_offset,
                    position,
                    visited,
                    emitted,
                    limit,
                    false,
                    visitor,
                ),
                kind if kind.eq_ignore_ascii_case(b"set") => self.scan_key_iter(
                    self.sets.keys(),
                    now_ms,
                    cursor_offset,
                    position,
                    visited,
                    emitted,
                    limit,
                    false,
                    visitor,
                ),
                kind if kind.eq_ignore_ascii_case(b"zset") => self.scan_key_iter(
                    self.zsets.keys(),
                    now_ms,
                    cursor_offset,
                    position,
                    visited,
                    emitted,
                    limit,
                    false,
                    visitor,
                ),
                _ => None,
            };
        }

        if let Some(offset) = self.scan_key_iter(
            self.hashes.keys(),
            now_ms,
            cursor_offset,
            position,
            visited,
            emitted,
            limit,
            true,
            visitor,
        ) {
            return Some(offset);
        }
        if let Some(offset) = self.scan_key_iter(
            self.lists.keys(),
            now_ms,
            cursor_offset,
            position,
            visited,
            emitted,
            limit,
            false,
            visitor,
        ) {
            return Some(offset);
        }
        if let Some(offset) = self.scan_key_iter(
            self.sets.keys(),
            now_ms,
            cursor_offset,
            position,
            visited,
            emitted,
            limit,
            false,
            visitor,
        ) {
            return Some(offset);
        }
        self.scan_key_iter(
            self.zsets.keys(),
            now_ms,
            cursor_offset,
            position,
            visited,
            emitted,
            limit,
            false,
            visitor,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_key_iter<'a>(
        &self,
        keys: impl Iterator<Item = &'a Bytes>,
        now_ms: u64,
        cursor_offset: usize,
        position: &mut usize,
        visited: &mut usize,
        emitted: &mut usize,
        limit: usize,
        hash_keys: bool,
        visitor: &mut impl FnMut(&[u8]) -> bool,
    ) -> Option<usize> {
        if !self.has_expirations() {
            return scan_key_iter_no_expiration(
                keys,
                cursor_offset,
                position,
                visited,
                emitted,
                limit,
                hash_keys.then_some((self, now_ms)),
                visitor,
            );
        }

        for key in keys {
            if self.object_is_expired(key, now_ms) {
                continue;
            }
            if hash_keys && !self.hash_has_live_fields(key, now_ms) {
                continue;
            }

            if *position < cursor_offset {
                *position += 1;
                continue;
            }

            *position += 1;
            *visited = visited.saturating_add(1);
            if visitor(key) {
                *emitted = emitted.saturating_add(1);
            }
            if *visited >= limit {
                return Some(*position);
            }
        }
        None
    }

    #[inline(always)]
    fn visit_unexpired_key_iter<'a>(
        &self,
        keys: impl Iterator<Item = &'a Bytes>,
        now_ms: u64,
        visitor: &mut impl FnMut(&[u8]) -> bool,
    ) -> bool {
        for key in keys {
            if !self.object_is_expired(key, now_ms) && !visitor(key) {
                return false;
            }
        }
        true
    }

    #[inline(always)]
    fn visit_unexpired_live_hash_key_iter<'a>(
        &self,
        keys: impl Iterator<Item = &'a Bytes>,
        now_ms: u64,
        visitor: &mut impl FnMut(&[u8]) -> bool,
    ) -> bool {
        for key in keys {
            if !self.object_is_expired(key, now_ms)
                && self.hash_has_live_fields(key, now_ms)
                && !visitor(key)
            {
                return false;
            }
        }
        true
    }

    #[inline(always)]
    pub(super) fn has_non_hash(&self, key: &[u8]) -> bool {
        self.lists.contains_key(key) || self.sets.contains_key(key) || self.zsets.contains_key(key)
    }

    #[inline(always)]
    pub(super) fn has_non_list(&self, key: &[u8]) -> bool {
        self.hashes.contains_key(key) || self.sets.contains_key(key) || self.zsets.contains_key(key)
    }

    #[inline(always)]
    pub(super) fn has_non_list_hashed(&self, hash: u64, key: &[u8]) -> bool {
        self.hashes.contains_key_hashed(hash, key)
            || self.sets.contains_key_hashed(hash, key)
            || self.zsets.contains_key_hashed(hash, key)
    }

    #[inline(always)]
    pub(super) fn has_non_set(&self, key: &[u8]) -> bool {
        self.hashes.contains_key(key)
            || self.lists.contains_key(key)
            || self.zsets.contains_key(key)
    }

    #[inline(always)]
    pub(super) fn has_non_zset(&self, key: &[u8]) -> bool {
        self.hashes.contains_key(key) || self.lists.contains_key(key) || self.sets.contains_key(key)
    }
}

#[inline(always)]
fn visit_key_iter<'a>(
    keys: impl Iterator<Item = &'a Bytes>,
    visitor: &mut impl FnMut(&[u8]) -> bool,
) -> bool {
    for key in keys {
        if !visitor(key) {
            return false;
        }
    }
    true
}

#[inline(always)]
fn visit_live_hash_key_iter<'a>(
    keys: impl Iterator<Item = &'a Bytes>,
    now_ms: u64,
    bucket: &RedisObjectBucket,
    visitor: &mut impl FnMut(&[u8]) -> bool,
) -> bool {
    for key in keys {
        if bucket.hash_has_live_fields(key, now_ms) && !visitor(key) {
            return false;
        }
    }
    true
}

#[allow(clippy::too_many_arguments)]
#[inline(always)]
fn scan_key_iter_no_expiration<'a>(
    keys: impl Iterator<Item = &'a Bytes>,
    cursor_offset: usize,
    position: &mut usize,
    visited: &mut usize,
    emitted: &mut usize,
    limit: usize,
    live_hash_filter: Option<(&RedisObjectBucket, u64)>,
    visitor: &mut impl FnMut(&[u8]) -> bool,
) -> Option<usize> {
    for key in keys {
        if live_hash_filter
            .is_some_and(|(bucket, now_ms)| !bucket.hash_has_live_fields(key, now_ms))
        {
            continue;
        }
        if *position < cursor_offset {
            *position += 1;
            continue;
        }

        *position += 1;
        *visited = visited.saturating_add(1);
        if visitor(key) {
            *emitted = emitted.saturating_add(1);
        }
        if *visited >= limit {
            return Some(*position);
        }
    }
    None
}
