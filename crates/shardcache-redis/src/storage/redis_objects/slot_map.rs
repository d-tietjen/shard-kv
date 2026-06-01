use super::*;

impl SlotMap {
    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        self.table.len()
    }

    #[inline(always)]
    pub(super) fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    #[inline(always)]
    pub(super) fn contains_key_hashed(&self, hash: u64, key: &[u8]) -> bool {
        self.get_hashed(hash, key).is_some()
    }

    #[inline(always)]
    pub(super) fn get(&self, key: &[u8]) -> Option<&SlotId> {
        let hash = hash_key(key);
        self.table
            .find(hash, |entry| entry.key.as_slice() == key)
            .map(|entry| &entry.slot)
    }

    #[inline(always)]
    pub(super) fn get_hashed(&self, hash: u64, key: &[u8]) -> Option<SlotId> {
        self.table
            .find(hash, |entry| entry.key.as_slice() == key)
            .map(|entry| entry.slot)
    }

    #[inline(always)]
    pub(super) fn insert(&mut self, key: Bytes, slot: SlotId) -> Option<SlotId> {
        let hash = hash_key(&key);
        self.insert_hashed(hash, key, slot)
    }

    #[inline(always)]
    pub(super) fn insert_hashed(&mut self, hash: u64, key: Bytes, slot: SlotId) -> Option<SlotId> {
        if let Some(entry) = self
            .table
            .find_mut(hash, |entry| entry.key.as_slice() == key.as_slice())
        {
            return Some(std::mem::replace(&mut entry.slot, slot));
        }
        self.table
            .insert_unique(hash, SlotEntry { hash, key, slot }, |entry| entry.hash);
        None
    }

    #[inline(always)]
    pub(super) fn remove(&mut self, key: &[u8]) -> Option<SlotId> {
        let hash = hash_key(key);
        self.remove_hashed(hash, key)
    }

    #[inline(always)]
    pub(super) fn remove_hashed(&mut self, hash: u64, key: &[u8]) -> Option<SlotId> {
        self.table
            .find_entry(hash, |entry| entry.key.as_slice() == key)
            .ok()
            .map(|entry| entry.remove().0.slot)
    }

    #[inline(always)]
    pub(super) fn keys(&self) -> impl Iterator<Item = &Bytes> {
        self.table.iter().map(|entry| &entry.key)
    }
}
