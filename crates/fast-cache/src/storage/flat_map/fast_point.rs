use super::*;

#[derive(Debug)]
pub(super) struct FastPointEntry {
    hash: u64,
    key_tag: u64,
    key_len: usize,
    key_inline: [u8; FastPointEntry::INLINE_KEY_CAP],
    key_heap: Option<Box<[u8]>>,
    value: SharedBytes,
}

impl FastPointEntry {
    const INLINE_KEY_CAP: usize = 16;

    #[inline(always)]
    fn new(hash: u64, key_tag: u64, key: &[u8], value: SharedBytes) -> Self {
        let mut key_inline = [0u8; Self::INLINE_KEY_CAP];
        let key_heap = if key.len() <= Self::INLINE_KEY_CAP {
            key_inline[..key.len()].copy_from_slice(key);
            None
        } else {
            Some(key.to_vec().into_boxed_slice())
        };
        Self {
            hash,
            key_tag,
            key_len: key.len(),
            key_inline,
            key_heap,
            value,
        }
    }

    #[inline(always)]
    fn key_matches(&self, key: &[u8]) -> bool {
        if self.key_len != key.len() {
            return false;
        }
        if self.key_len <= Self::INLINE_KEY_CAP {
            bytes_equal_hot(&self.key_inline[..self.key_len], key)
        } else {
            bytes_equal_hot(
                self.key_heap
                    .as_deref()
                    .expect("large fast point key has heap storage"),
                key,
            )
        }
    }

    pub(super) fn into_flat_entry(self) -> FlatEntry {
        let key = match self.key_heap {
            Some(key) => key,
            None => self.key_inline[..self.key_len].to_vec().into_boxed_slice(),
        };
        FlatEntry {
            hash: self.hash,
            key_tag: self.key_tag,
            key_len: self.key_len,
            key,
            value: self.value,
            expire_at_ms: None,
            access: EntryAccessMeta {
                last_touch: 0,
                frequency: 1,
            },
        }
    }

    fn to_stored_entry(&self) -> StoredEntry {
        let key = if self.key_len <= Self::INLINE_KEY_CAP {
            self.key_inline[..self.key_len].to_vec()
        } else {
            self.key_heap
                .as_deref()
                .expect("large fast point key has heap storage")
                .to_vec()
        };
        StoredEntry {
            key,
            value: self.value.as_ref().to_vec(),
            expire_at_ms: None,
        }
    }

    fn to_key(&self) -> Bytes {
        match self.key_len <= Self::INLINE_KEY_CAP {
            true => self.key_inline[..self.key_len].to_vec(),
            false => self
                .key_heap
                .as_deref()
                .expect("large fast point key has heap storage")
                .to_vec(),
        }
    }
}

#[derive(Debug)]
pub(super) enum FastPointMutation {
    Inserted {
        key_len: usize,
        value_len: usize,
    },
    Replaced {
        old_value: Option<SharedBytes>,
        old_value_len: usize,
        new_value_len: usize,
    },
}

#[derive(Debug, Clone, Copy)]
struct FastPointSlot {
    hash: u64,
    key_tag: u64,
    key_len: u32,
    entry_index: u32,
}

impl FastPointSlot {
    const EMPTY_INDEX: u32 = u32::MAX;

    #[inline(always)]
    fn empty() -> Self {
        Self {
            hash: 0,
            key_tag: 0,
            key_len: 0,
            entry_index: Self::EMPTY_INDEX,
        }
    }

    #[inline(always)]
    fn is_empty(self) -> bool {
        self.entry_index == Self::EMPTY_INDEX
    }
}

#[derive(Debug)]
pub(super) struct FastPointMap {
    active: bool,
    slots: Vec<FastPointSlot>,
    entries: Vec<FastPointEntry>,
}

impl Default for FastPointMap {
    fn default() -> Self {
        Self {
            active: true,
            slots: Vec::new(),
            entries: Vec::new(),
        }
    }
}

impl FastPointMap {
    const INITIAL_SLOTS: usize = 1024;
    const MAX_LOAD_NUMERATOR: usize = 7;
    const MAX_LOAD_DENOMINATOR: usize = 10;

    #[inline(always)]
    pub(super) fn is_active(&self) -> bool {
        self.active
    }

    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        if self.active { self.entries.len() } else { 0 }
    }

    #[inline(always)]
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub(super) fn take_entries_and_disable(&mut self) -> Vec<FastPointEntry> {
        self.active = false;
        self.slots.clear();
        mem::take(&mut self.entries)
    }

    #[inline(always)]
    pub(super) fn get(&self, hash: u64, key: &[u8]) -> Option<&SharedBytes> {
        if !self.active || self.slots.is_empty() {
            return None;
        }
        let key_len = key.len();
        if key_len > u32::MAX as usize {
            return None;
        }

        let mask = self.slots.len() - 1;
        let mut index = fast_point_bucket(hash, mask);
        loop {
            let slot = self.slots[index];
            if slot.is_empty() {
                return None;
            }
            if slot.hash == hash && slot.key_len as usize == key_len {
                #[cfg(not(feature = "unsafe"))]
                let entry = self.entries.get(slot.entry_index as usize)?;
                #[cfg(feature = "unsafe")]
                let entry = unsafe { self.entries.get_unchecked(slot.entry_index as usize) };
                if entry.hash == hash && entry.key_matches(key) {
                    return Some(&entry.value);
                }
            }
            index = (index + 1) & mask;
        }
    }

    #[inline(always)]
    pub(super) fn get_tagged(
        &self,
        hash: u64,
        key_tag: u64,
        key_len: usize,
    ) -> Option<&SharedBytes> {
        if !self.active || self.slots.is_empty() || key_len > u32::MAX as usize {
            return None;
        }

        let mask = self.slots.len() - 1;
        let mut index = fast_point_bucket(hash, mask);
        loop {
            let slot = self.slots[index];
            if slot.is_empty() {
                return None;
            }
            if slot.hash == hash && slot.key_tag == key_tag && slot.key_len as usize == key_len {
                #[cfg(not(feature = "unsafe"))]
                let entry = self.entries.get(slot.entry_index as usize)?;
                #[cfg(feature = "unsafe")]
                let entry = unsafe { self.entries.get_unchecked(slot.entry_index as usize) };
                if entry.hash == hash && entry.key_tag == key_tag && entry.key_len == key_len {
                    return Some(&entry.value);
                }
            }
            index = (index + 1) & mask;
        }
    }

    #[inline(always)]
    pub(super) unsafe fn upsert_slice(
        &mut self,
        hash: u64,
        key_tag: u64,
        key: &[u8],
        value: &[u8],
        #[cfg_attr(not(feature = "unsafe"), allow(unused_variables))] allow_in_place_replace: bool,
    ) -> Option<FastPointMutation> {
        if !self.active {
            return None;
        }
        if key.len() > u32::MAX as usize || self.entries.len() >= u32::MAX as usize {
            return None;
        }
        self.ensure_insert_capacity();
        if !self.active {
            return None;
        }

        let key_len = key.len();
        let mask = self.slots.len() - 1;
        let mut index = fast_point_bucket(hash, mask);
        loop {
            let slot = self.slots[index];
            if slot.is_empty() {
                let entry_index = self.entries.len();
                let stored_value = shared_bytes_from_slice(value);
                let value_len = stored_value.len();
                self.entries
                    .push(FastPointEntry::new(hash, key_tag, key, stored_value));
                self.slots[index] = FastPointSlot {
                    hash,
                    key_tag,
                    key_len: key_len as u32,
                    entry_index: entry_index as u32,
                };
                return Some(FastPointMutation::Inserted { key_len, value_len });
            }

            if slot.hash == hash && slot.key_len as usize == key_len {
                #[cfg(not(feature = "unsafe"))]
                let Some(entry) = self.entries.get_mut(slot.entry_index as usize) else {
                    self.active = false;
                    return None;
                };
                #[cfg(feature = "unsafe")]
                let entry = unsafe { self.entries.get_unchecked_mut(slot.entry_index as usize) };
                if entry.hash == hash && entry.key_matches(key) {
                    entry.key_tag = key_tag;
                    self.slots[index].key_tag = key_tag;
                    let old_value_len = entry.value.len();
                    #[cfg(feature = "unsafe")]
                    if old_value_len == value.len() && allow_in_place_replace {
                        unsafe {
                            copy_hot_value_bytes(
                                entry.value.as_ptr().cast_mut(),
                                value.as_ptr(),
                                value.len(),
                            );
                        }
                        return Some(FastPointMutation::Replaced {
                            old_value: None,
                            old_value_len,
                            new_value_len: old_value_len,
                        });
                    }
                    let new_value = shared_bytes_from_slice(value);
                    let new_value_len = new_value.len();
                    let old_value = mem::replace(&mut entry.value, new_value);
                    return Some(FastPointMutation::Replaced {
                        old_value: Some(old_value),
                        old_value_len,
                        new_value_len,
                    });
                }
            }

            index = (index + 1) & mask;
        }
    }

    fn ensure_insert_capacity(&mut self) {
        if self.slots.is_empty() {
            self.resize(Self::INITIAL_SLOTS);
            return;
        }

        let next_len = self.entries.len().saturating_add(1);
        if next_len.saturating_mul(Self::MAX_LOAD_DENOMINATOR)
            >= self.slots.len().saturating_mul(Self::MAX_LOAD_NUMERATOR)
        {
            self.resize(self.slots.len().saturating_mul(2));
        }
    }

    fn resize(&mut self, new_len: usize) {
        let new_len = new_len.next_power_of_two().max(Self::INITIAL_SLOTS);
        let mut new_slots = vec![FastPointSlot::empty(); new_len];
        let mask = new_len - 1;
        for (entry_index, entry) in self.entries.iter().enumerate() {
            let mut index = fast_point_bucket(entry.hash, mask);
            loop {
                if new_slots[index].is_empty() {
                    new_slots[index] = FastPointSlot {
                        hash: entry.hash,
                        key_tag: entry.key_tag,
                        key_len: entry.key_len as u32,
                        entry_index: entry_index as u32,
                    };
                    break;
                }
                index = (index + 1) & mask;
            }
        }
        self.slots = new_slots;
    }

    pub(super) fn snapshot_entries(&self) -> Vec<StoredEntry> {
        self.entries
            .iter()
            .map(FastPointEntry::to_stored_entry)
            .collect()
    }

    pub(super) fn snapshot_keys(&self) -> Vec<Bytes> {
        self.entries.iter().map(FastPointEntry::to_key).collect()
    }
}

#[inline(always)]
fn fast_point_bucket(hash: u64, mask: usize) -> usize {
    let mixed = hash ^ (hash >> 32);
    mixed as usize & mask
}
