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
        if !self.contains_object(key) {
            return -2;
        }
        self.expire_at_ms
            .get(key)
            .map(|expire_at_ms| expire_at_ms.saturating_sub(now_ms) as i64)
            .unwrap_or(-1)
    }

    pub(crate) fn clone_value(&self, key: &[u8]) -> Option<RedisObjectValue> {
        if let Some(slot) = self.hashes.get(key).copied() {
            return Some(RedisObjectValue::Hash(
                self.hash_slab
                    .get(slot)
                    .expect("hash slab slot missing")
                    .entries(),
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

    pub(crate) fn type_name(&self, key: &[u8]) -> Option<&'static str> {
        if self.hashes.contains_key(key) {
            Some("hash")
        } else if self.lists.contains_key(key) {
            Some("list")
        } else if self.sets.contains_key(key) {
            Some("set")
        } else if self.zsets.contains_key(key) {
            Some("zset")
        } else {
            None
        }
    }

    pub(crate) fn encoding(&self, key: &[u8]) -> Option<&'static str> {
        if self.hashes.contains_key(key) {
            Some("hashtable")
        } else if self.lists.contains_key(key) {
            Some("quicklist")
        } else if self.sets.contains_key(key) {
            Some("hashtable")
        } else if self.zsets.contains_key(key) {
            Some("skiplist")
        } else {
            None
        }
    }

    pub(crate) fn keys_with_type(&self, out: &mut Vec<(Bytes, &'static str)>, now_ms: u64) {
        out.extend(
            self.hashes
                .keys()
                .filter(|key| !self.object_is_expired(key, now_ms))
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

    #[inline(always)]
    pub(super) fn has_non_hash(&self, key: &[u8]) -> bool {
        self.lists.contains_key(key) || self.sets.contains_key(key) || self.zsets.contains_key(key)
    }

    #[inline(always)]
    pub(super) fn has_non_list(&self, key: &[u8]) -> bool {
        self.hashes.contains_key(key) || self.sets.contains_key(key) || self.zsets.contains_key(key)
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
