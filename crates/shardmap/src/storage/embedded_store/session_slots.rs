use hashbrown::{HashMap as HashBrownMap, HashTable};
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::config::EvictionPolicy;
use crate::storage::flat_map::EvictionRank;
use crate::storage::{Bytes, hash_key};

#[derive(Debug)]
pub(super) struct SessionSlotEntry {
    pub(super) hash: u64,
    pub(super) key: Box<[u8]>,
    pub(super) value: SessionSlotValue,
    pub(super) access: SessionAccessMeta,
}

impl SessionSlotEntry {
    #[inline(always)]
    pub(super) fn matches(&self, hash: u64, key: &[u8]) -> bool {
        self.hash == hash && self.key.as_ref() == key
    }
}

#[derive(Debug)]
pub(super) enum SessionSlotValue {
    Owned(Box<[u8]>),
    Packed { offset: u32, len: u32 },
}

impl SessionSlotValue {
    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Owned(bytes) => bytes.len(),
            Self::Packed { len, .. } => *len as usize,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct SessionAccessMeta {
    last_touch: u64,
    frequency: u32,
}

#[cfg(feature = "embedded")]
#[derive(Debug)]
pub(super) struct SessionPackedViewMeta {
    pub(super) buffer: bytes::Bytes,
    pub(super) offsets: Vec<usize>,
    pub(super) lengths: Vec<usize>,
    pub(super) hit_count: usize,
    pub(super) total_bytes: usize,
}

impl SessionAccessMeta {
    #[inline(always)]
    pub(super) fn record_access(&mut self, tick: u64) {
        self.last_touch = tick;
        self.frequency = self.frequency.saturating_add(1).max(1);
    }

    #[inline(always)]
    pub(super) fn rank(&self, policy: EvictionPolicy) -> EvictionRank {
        match policy {
            EvictionPolicy::None => EvictionRank {
                primary: u64::MAX,
                secondary: u64::MAX,
            },
            EvictionPolicy::Lru => EvictionRank {
                primary: self.last_touch,
                secondary: 0,
            },
            EvictionPolicy::Lfu => EvictionRank {
                primary: self.frequency as u64,
                secondary: self.last_touch,
            },
            #[cfg(feature = "prefix-eviction")]
            EvictionPolicy::Prefix => EvictionRank {
                primary: self.last_touch,
                secondary: self.frequency as u64,
            },
        }
    }
}

#[cfg(feature = "prefix-eviction")]
#[inline(always)]
fn prefix_eviction_group_rank(access: SessionAccessMeta) -> EvictionRank {
    EvictionRank {
        primary: access.last_touch,
        secondary: access.frequency as u64,
    }
}

#[cfg(feature = "prefix-eviction")]
#[inline(always)]
fn prefix_eviction_member_rank(
    session_prefix_len: usize,
    key_len: usize,
    access: SessionAccessMeta,
) -> EvictionRank {
    let suffix_depth = key_len.saturating_sub(session_prefix_len) as u64;
    EvictionRank {
        primary: access.last_touch,
        secondary: u64::MAX.saturating_sub(suffix_depth),
    }
}

#[derive(Debug, Default)]
pub(super) struct SessionSlotSlab {
    pub(super) entries: HashTable<SessionSlotEntry>,
    pub(super) packed_values: Vec<u8>,
    pub(super) stored_bytes: usize,
}

#[derive(Debug, Default)]
pub(crate) struct SessionSlotMap {
    sessions: HashBrownMap<Bytes, SessionSlotSlab, xxhash_rust::xxh3::Xxh3DefaultBuilder>,
    active_readers: AtomicUsize,
    retired_values: Vec<Box<[u8]>>,
    retired_slabs: Vec<SessionSlotSlab>,
    stored_bytes: usize,
    track_access: bool,
    sample_reads: bool,
    access_clock: u64,
    read_sample_counter: u64,
    evictions: u64,
}

/// Packed write buffer for replacing one session's chunk set.
///
/// A packed write stores all values for a session in one slab, reducing
/// allocator work for KV-cache style batches.
#[derive(Debug)]
pub struct PackedSessionWrite {
    pub(super) session_prefix: Bytes,
    pub(super) slab: SessionSlotSlab,
}

impl PackedSessionWrite {
    /// Creates an empty packed session write with preallocated capacities.
    pub fn with_capacity(
        session_prefix: Bytes,
        item_capacity: usize,
        value_bytes_capacity: usize,
    ) -> Self {
        Self {
            session_prefix,
            slab: SessionSlotSlab {
                entries: HashTable::with_capacity(item_capacity),
                packed_values: Vec::with_capacity(value_bytes_capacity),
                stored_bytes: 0,
            },
        }
    }

    /// Builds a packed session write from owned key/value pairs.
    pub fn from_owned_items(session_prefix: Bytes, items: Vec<(Bytes, Bytes)>) -> Self {
        let total_value_bytes = items.iter().map(|(_, value)| value.len()).sum::<usize>();
        let mut packed = Self::with_capacity(session_prefix, items.len(), total_value_bytes);
        for (key, value) in items {
            packed.push_owned_record(key, value);
        }
        packed
    }

    /// Returns the number of records in the packed write.
    #[inline(always)]
    pub fn item_count(&self) -> usize {
        self.slab.entries.len()
    }

    /// Returns the number of value bytes stored in the packed write.
    #[inline(always)]
    pub fn stored_bytes(&self) -> usize {
        self.slab.stored_bytes
    }

    /// Returns the session prefix this write replaces.
    #[inline(always)]
    pub fn session_prefix(&self) -> &[u8] {
        &self.session_prefix
    }

    /// Returns the length of the contiguous value buffer.
    #[inline(always)]
    pub fn value_buffer_len(&self) -> usize {
        self.slab.packed_values.len()
    }

    /// Returns the mutable contiguous value buffer for custom packing.
    #[inline(always)]
    pub fn value_buffer_mut(&mut self) -> &mut Vec<u8> {
        &mut self.slab.packed_values
    }

    /// Appends an owned key/value record to the packed write.
    pub fn push_owned_record(&mut self, key: Bytes, value: Bytes) {
        let tick = self.slab.entries.len() as u64 + 1;
        self.slab
            .set_packed_hashed(hash_key(&key), key, value, tick);
    }

    /// Appends a key whose value already lives in the packed value buffer.
    pub fn push_prepacked_record(&mut self, key: Bytes, offset: usize, len: usize) {
        let tick = self.slab.entries.len() as u64 + 1;
        self.slab
            .set_prepacked_hashed(hash_key(&key), key, offset, len, tick);
    }

    /// Returns cloned key/value records for inspection or persistence.
    pub fn cloned_records(&self) -> Vec<(Bytes, Bytes)> {
        self.slab
            .entries
            .iter()
            .map(|entry| {
                (
                    entry.key.to_vec(),
                    self.slab.entry_value_slice(entry).to_vec(),
                )
            })
            .collect()
    }

    pub(super) fn into_parts(self) -> (Bytes, SessionSlotSlab) {
        (self.session_prefix, self.slab)
    }
}

impl SessionSlotSlab {
    #[inline(always)]
    pub(super) fn entry_value_slice<'a>(&'a self, entry: &'a SessionSlotEntry) -> &'a [u8] {
        match &entry.value {
            SessionSlotValue::Owned(bytes) => bytes.as_ref(),
            SessionSlotValue::Packed { offset, len } => {
                let offset = *offset as usize;
                let len = *len as usize;
                &self.packed_values[offset..offset + len]
            }
        }
    }

    pub(super) fn set_packed_hashed(&mut self, hash: u64, key: Bytes, value: Bytes, tick: u64) {
        let key_ref = key.as_slice();
        let key_len = key.len();
        let value_len = value.len();

        match self.entries.entry(
            hash,
            |entry| entry.matches(hash, key_ref),
            |entry| entry.hash,
        ) {
            hashbrown::hash_table::Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();
                let previous_value_len = entry.value.len();
                let offset = self.packed_values.len();
                self.packed_values.extend_from_slice(&value);
                entry.value = SessionSlotValue::Packed {
                    offset: offset as u32,
                    len: value_len as u32,
                };
                entry.access.record_access(tick);
                self.stored_bytes = self
                    .stored_bytes
                    .saturating_sub(previous_value_len)
                    .saturating_add(value_len);
            }
            hashbrown::hash_table::Entry::Vacant(vacant) => {
                let offset = self.packed_values.len();
                self.packed_values.extend_from_slice(&value);
                vacant.insert(SessionSlotEntry {
                    hash,
                    key: key.into_boxed_slice(),
                    value: SessionSlotValue::Packed {
                        offset: offset as u32,
                        len: value_len as u32,
                    },
                    access: SessionAccessMeta {
                        last_touch: tick,
                        frequency: 1,
                    },
                });
                self.stored_bytes = self
                    .stored_bytes
                    .saturating_add(key_len)
                    .saturating_add(value_len);
            }
        }
    }

    pub(super) fn set_prepacked_hashed(
        &mut self,
        hash: u64,
        key: Bytes,
        offset: usize,
        len: usize,
        tick: u64,
    ) {
        let key_ref = key.as_slice();
        let key_len = key.len();
        let value_ref = SessionSlotValue::Packed {
            offset: offset as u32,
            len: len as u32,
        };

        match self.entries.entry(
            hash,
            |entry| entry.matches(hash, key_ref),
            |entry| entry.hash,
        ) {
            hashbrown::hash_table::Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();
                let previous_value_len = entry.value.len();
                entry.value = value_ref;
                entry.access.record_access(tick);
                self.stored_bytes = self
                    .stored_bytes
                    .saturating_sub(previous_value_len)
                    .saturating_add(len);
            }
            hashbrown::hash_table::Entry::Vacant(vacant) => {
                vacant.insert(SessionSlotEntry {
                    hash,
                    key: key.into_boxed_slice(),
                    value: value_ref,
                    access: SessionAccessMeta {
                        last_touch: tick,
                        frequency: 1,
                    },
                });
                self.stored_bytes = self
                    .stored_bytes
                    .saturating_add(key_len)
                    .saturating_add(len);
            }
        }
    }
}

impl SessionSlotMap {
    #[inline(always)]
    pub(super) fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        self.sessions.values().map(|slab| slab.entries.len()).sum()
    }

    #[inline(always)]
    pub(super) fn retire_value(&mut self, value: Box<[u8]>) {
        if self.has_active_readers() {
            self.retired_values.push(value);
        }
    }

    #[inline(always)]
    pub(super) fn retire_slab(&mut self, slab: SessionSlotSlab) {
        if self.has_active_readers() {
            self.retired_slabs.push(slab);
        }
    }

    #[inline(always)]
    pub(super) fn has_active_readers(&self) -> bool {
        self.active_readers.load(Ordering::Acquire) > 0
    }

    #[inline(always)]
    pub(super) fn reclaim_retired_if_quiescent(&mut self) {
        if !self.has_active_readers() {
            if !self.retired_values.is_empty() {
                self.retired_values.clear();
            }
            if !self.retired_slabs.is_empty() {
                self.retired_slabs.clear();
            }
        }
    }

    #[inline(always)]
    pub(super) fn stored_bytes(&self) -> usize {
        self.stored_bytes
    }

    #[inline(always)]
    pub(super) fn configure_access_tracking(&mut self, enabled: bool) {
        self.track_access = enabled;
    }

    #[inline(always)]
    pub(super) fn configure_read_sampling(&mut self, enabled: bool) {
        self.sample_reads = enabled;
    }

    #[inline(always)]
    pub(super) fn has_session(&self, session_prefix: &[u8]) -> bool {
        self.sessions.contains_key(session_prefix)
    }

    #[inline(always)]
    pub(super) fn get_ref_hashed_shared(
        &self,
        session_prefix: &[u8],
        hash: u64,
        key: &[u8],
    ) -> Option<&[u8]> {
        self.get_ref_hashed_shared_prehashed(hash_key(session_prefix), session_prefix, hash, key)
    }

    #[inline(always)]
    pub(super) fn get_ref_hashed_shared_prehashed(
        &self,
        _session_hash: u64,
        session_prefix: &[u8],
        hash: u64,
        key: &[u8],
    ) -> Option<&[u8]> {
        self.sessions
            .raw_entry()
            .from_key(session_prefix)
            .and_then(|(_, slab)| {
                slab.entries
                    .find(hash, |entry| entry.matches(hash, key))
                    .map(|entry| slab.entry_value_slice(entry))
            })
    }

    #[inline(always)]
    pub(super) fn get_ref_hashed(
        &mut self,
        session_prefix: &[u8],
        hash: u64,
        key: &[u8],
    ) -> Option<&[u8]> {
        if self.should_sample_read() {
            let tick = self.next_access_tick();
            self.sessions.get_mut(session_prefix).and_then(|slab| {
                let SessionSlotSlab {
                    entries,
                    packed_values,
                    ..
                } = slab;
                let entry = entries.find_mut(hash, |entry| entry.matches(hash, key))?;
                entry.access.record_access(tick);
                Some(match &entry.value {
                    SessionSlotValue::Owned(bytes) => bytes.as_ref(),
                    SessionSlotValue::Packed { offset, len } => {
                        let offset = *offset as usize;
                        let len = *len as usize;
                        &packed_values[offset..offset + len]
                    }
                })
            })
        } else {
            self.sessions.get(session_prefix).and_then(|slab| {
                slab.entries
                    .find(hash, |entry| entry.matches(hash, key))
                    .map(|entry| slab.entry_value_slice(entry))
            })
        }
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(super) fn get_ref_hashed_local(
        &mut self,
        session_prefix: &[u8],
        hash: u64,
        key: &[u8],
    ) -> Option<&[u8]> {
        if self.should_sample_read() {
            let tick = self.next_access_tick();
            self.sessions.get_mut(session_prefix).and_then(|slab| {
                let SessionSlotSlab {
                    entries,
                    packed_values,
                    ..
                } = slab;
                let entry = entries.find_mut(hash, |entry| entry.matches(hash, key))?;
                entry.access.record_access(tick);
                Some(match &entry.value {
                    SessionSlotValue::Owned(bytes) => bytes.as_ref(),
                    SessionSlotValue::Packed { offset, len } => {
                        let offset = *offset as usize;
                        let len = *len as usize;
                        &packed_values[offset..offset + len]
                    }
                })
            })
        } else {
            self.sessions.get(session_prefix).and_then(|slab| {
                slab.entries
                    .find(hash, |entry| entry.matches(hash, key))
                    .map(|entry| slab.entry_value_slice(entry))
            })
        }
    }

    #[cfg(feature = "embedded")]
    pub(super) fn get_packed_view_hashed_local(
        &mut self,
        session_prefix: &[u8],
        keys: &[Bytes],
        key_hashes: &[u64],
    ) -> Option<SessionPackedViewMeta> {
        if keys.len() != key_hashes.len() {
            return None;
        }

        if self.should_sample_read() {
            let tick = self.next_access_tick();
            let slab = self.sessions.get_mut(session_prefix)?;
            let mut offsets = Vec::with_capacity(keys.len());
            let mut lengths = Vec::with_capacity(keys.len());
            let mut hit_count = 0usize;
            let mut total_bytes = 0usize;
            for (key, key_hash) in keys.iter().zip(key_hashes.iter().copied()) {
                let Some(entry) = slab
                    .entries
                    .find_mut(key_hash, |entry| entry.matches(key_hash, key))
                else {
                    offsets.push(usize::MAX);
                    lengths.push(0);
                    continue;
                };
                entry.access.record_access(tick);
                let SessionSlotValue::Packed { offset, len } = entry.value else {
                    return None;
                };
                let offset = offset as usize;
                let len = len as usize;
                offsets.push(offset);
                lengths.push(len);
                hit_count = hit_count.saturating_add(1);
                total_bytes = total_bytes.saturating_add(len);
            }
            Some(SessionPackedViewMeta {
                buffer: bytes::Bytes::copy_from_slice(&slab.packed_values),
                offsets,
                lengths,
                hit_count,
                total_bytes,
            })
        } else {
            let slab = self.sessions.get(session_prefix)?;
            let mut offsets = Vec::with_capacity(keys.len());
            let mut lengths = Vec::with_capacity(keys.len());
            let mut hit_count = 0usize;
            let mut total_bytes = 0usize;
            for (key, key_hash) in keys.iter().zip(key_hashes.iter().copied()) {
                let Some(entry) = slab
                    .entries
                    .find(key_hash, |entry| entry.matches(key_hash, key))
                else {
                    offsets.push(usize::MAX);
                    lengths.push(0);
                    continue;
                };
                let SessionSlotValue::Packed { offset, len } = entry.value else {
                    return None;
                };
                let offset = offset as usize;
                let len = len as usize;
                offsets.push(offset);
                lengths.push(len);
                hit_count = hit_count.saturating_add(1);
                total_bytes = total_bytes.saturating_add(len);
            }
            Some(SessionPackedViewMeta {
                buffer: bytes::Bytes::copy_from_slice(&slab.packed_values),
                offsets,
                lengths,
                hit_count,
                total_bytes,
            })
        }
    }

    pub(super) fn set_slice_hashed(
        &mut self,
        session_prefix: &[u8],
        hash: u64,
        key: &[u8],
        value: &[u8],
    ) {
        self.set_slice_hashed_prehashed(hash_key(session_prefix), session_prefix, hash, key, value);
    }

    pub(super) fn set_slice_hashed_prehashed(
        &mut self,
        _session_hash: u64,
        session_prefix: &[u8],
        hash: u64,
        key: &[u8],
        value: &[u8],
    ) {
        self.reclaim_retired_if_quiescent();
        let has_active_readers = self.has_active_readers();
        let tick = if self.track_access {
            self.next_access_tick()
        } else {
            0
        };
        let slab = match self.sessions.raw_entry_mut().from_key(session_prefix) {
            hashbrown::hash_map::RawEntryMut::Occupied(occupied) => occupied.into_mut(),
            hashbrown::hash_map::RawEntryMut::Vacant(vacant) => {
                vacant
                    .insert(session_prefix.to_vec(), SessionSlotSlab::default())
                    .1
            }
        };

        match slab
            .entries
            .entry(hash, |entry| entry.matches(hash, key), |entry| entry.hash)
        {
            hashbrown::hash_table::Entry::Occupied(mut occupied) => {
                let mut retired_value = None;
                let entry = occupied.get_mut();
                let previous_value_len = entry.value.len();
                if !has_active_readers
                    && matches!(&entry.value, SessionSlotValue::Owned(existing) if existing.len() == value.len())
                {
                    if let SessionSlotValue::Owned(existing) = &mut entry.value {
                        existing.copy_from_slice(value);
                    }
                } else {
                    let old = mem::replace(
                        &mut entry.value,
                        SessionSlotValue::Owned(value.to_vec().into_boxed_slice()),
                    );
                    if let SessionSlotValue::Owned(old_value) = old {
                        retired_value = Some(old_value);
                    }
                }
                entry.access.record_access(tick);
                slab.stored_bytes = slab
                    .stored_bytes
                    .saturating_sub(previous_value_len)
                    .saturating_add(entry.value.len());
                self.stored_bytes = self
                    .stored_bytes
                    .saturating_sub(previous_value_len)
                    .saturating_add(entry.value.len());
                if let Some(old_value) = retired_value {
                    self.retire_value(old_value);
                }
            }
            hashbrown::hash_table::Entry::Vacant(vacant) => {
                let key_len = key.len();
                let value_len = value.len();
                vacant.insert(SessionSlotEntry {
                    hash,
                    key: key.to_vec().into_boxed_slice(),
                    value: SessionSlotValue::Owned(value.to_vec().into_boxed_slice()),
                    access: SessionAccessMeta {
                        last_touch: tick,
                        frequency: 1,
                    },
                });
                slab.stored_bytes = slab
                    .stored_bytes
                    .saturating_add(key_len)
                    .saturating_add(value_len);
                self.stored_bytes = self
                    .stored_bytes
                    .saturating_add(key_len)
                    .saturating_add(value_len);
            }
        }
    }

    pub(super) fn delete_hashed(&mut self, session_prefix: &[u8], hash: u64, key: &[u8]) -> bool {
        self.reclaim_retired_if_quiescent();
        let mut removed_value = None;
        let deleted = match self.sessions.entry(session_prefix.to_vec()) {
            hashbrown::hash_map::Entry::Occupied(mut session) => {
                let (value, remove_session) = {
                    let slab = session.get_mut();
                    let Some(entry) = slab
                        .entries
                        .find_entry(hash, |entry| entry.matches(hash, key))
                        .ok()
                    else {
                        return false;
                    };

                    let (removed, _) = entry.remove();
                    slab.stored_bytes = slab
                        .stored_bytes
                        .saturating_sub(removed.key.len().saturating_add(removed.value.len()));
                    self.stored_bytes = self
                        .stored_bytes
                        .saturating_sub(removed.key.len().saturating_add(removed.value.len()));
                    (removed.value, slab.entries.is_empty())
                };

                removed_value = Some(value);
                if remove_session {
                    session.remove();
                }
                true
            }
            hashbrown::hash_map::Entry::Vacant(_) => false,
        };
        if let Some(SessionSlotValue::Owned(value)) = removed_value {
            self.retire_value(value);
        }
        deleted
    }

    #[cfg(feature = "embedded")]
    pub(super) fn delete_hashed_local(
        &mut self,
        session_prefix: &[u8],
        hash: u64,
        key: &[u8],
    ) -> bool {
        self.reclaim_retired_if_quiescent();
        let mut removed_value = None;
        let mut removed_slab = None;
        let deleted = match self.sessions.entry(session_prefix.to_vec()) {
            hashbrown::hash_map::Entry::Occupied(mut session) => {
                let remove_session = {
                    let slab = session.get_mut();
                    let Some(entry) = slab
                        .entries
                        .find_entry(hash, |entry| entry.matches(hash, key))
                        .ok()
                    else {
                        return false;
                    };

                    let removed_key_len = entry.get().key.len();
                    let removed_value_len = entry.get().value.len();
                    let (removed, _) = entry.remove();
                    removed_value = Some(removed.value);
                    slab.stored_bytes = slab
                        .stored_bytes
                        .saturating_sub(removed_key_len.saturating_add(removed_value_len));
                    self.stored_bytes = self
                        .stored_bytes
                        .saturating_sub(removed_key_len.saturating_add(removed_value_len));
                    slab.entries.is_empty()
                };

                if remove_session {
                    removed_slab = Some(session.remove());
                }
                true
            }
            hashbrown::hash_map::Entry::Vacant(_) => false,
        };
        if let Some(SessionSlotValue::Owned(value)) = removed_value {
            self.retire_value(value);
        }
        if let Some(slab) = removed_slab {
            self.retire_slab(slab);
        }
        deleted
    }

    pub(super) fn replace_session_slab(&mut self, packed: PackedSessionWrite) {
        self.reclaim_retired_if_quiescent();
        let (session_prefix, slab) = packed.into_parts();
        let new_bytes = slab.stored_bytes;
        let previous = self.sessions.insert(session_prefix, slab);
        if let Some(previous) = previous {
            self.stored_bytes = self
                .stored_bytes
                .saturating_sub(previous.stored_bytes)
                .saturating_add(new_bytes);
            self.retire_slab(previous);
        } else {
            self.stored_bytes = self.stored_bytes.saturating_add(new_bytes);
        }
    }

    #[cfg(feature = "embedded")]
    pub(super) fn replace_session_slab_local(&mut self, packed: PackedSessionWrite) {
        self.reclaim_retired_if_quiescent();
        let (session_prefix, slab) = packed.into_parts();
        let new_bytes = slab.stored_bytes;
        let previous = self.sessions.insert(session_prefix, slab);
        if let Some(previous) = previous {
            self.stored_bytes = self
                .stored_bytes
                .saturating_sub(previous.stored_bytes)
                .saturating_add(new_bytes);
            self.retire_slab(previous);
        } else {
            self.stored_bytes = self.stored_bytes.saturating_add(new_bytes);
        }
    }

    #[inline(always)]
    pub(super) fn next_access_tick(&mut self) -> u64 {
        self.access_clock = self.access_clock.saturating_add(1);
        self.access_clock
    }

    #[inline(always)]
    pub(super) fn should_sample_read(&mut self) -> bool {
        const READ_TOUCH_SAMPLE_MASK: u64 = 1023;
        if !self.track_access || !self.sample_reads {
            return false;
        }
        self.read_sample_counter = self.read_sample_counter.saturating_add(1);
        (self.read_sample_counter & READ_TOUCH_SAMPLE_MASK) == 0
    }

    pub(super) fn eviction_candidate(
        &self,
        policy: EvictionPolicy,
    ) -> Option<(EvictionRank, Bytes, u64, Bytes)> {
        if policy == EvictionPolicy::None {
            return None;
        }
        #[cfg(feature = "prefix-eviction")]
        if policy == EvictionPolicy::Prefix {
            return self.prefix_eviction_candidate();
        }

        self.sessions
            .iter()
            .flat_map(|(session_prefix, slab)| {
                slab.entries.iter().map(|entry| {
                    (
                        entry.access.rank(policy),
                        session_prefix.clone(),
                        entry.hash,
                        entry.key.as_ref().to_vec(),
                    )
                })
            })
            .min_by_key(|(rank, _, _, _)| *rank)
    }

    #[cfg(feature = "prefix-eviction")]
    fn prefix_eviction_candidate(&self) -> Option<(EvictionRank, Bytes, u64, Bytes)> {
        let (group_rank, session_prefix) = self
            .sessions
            .iter()
            .filter_map(|(session_prefix, slab)| {
                slab.entries
                    .iter()
                    .map(|entry| prefix_eviction_group_rank(entry.access))
                    .max()
                    .map(|rank| (rank, session_prefix.clone()))
            })
            .min_by_key(|(rank, _session_prefix)| *rank)?;

        let slab = self.sessions.get(&session_prefix)?;
        let session_prefix_len = session_prefix.len();
        let (_member_rank, hash, key) = slab
            .entries
            .iter()
            .map(|entry| {
                (
                    prefix_eviction_member_rank(session_prefix_len, entry.key.len(), entry.access),
                    entry.hash,
                    entry.key.as_ref().to_vec(),
                )
            })
            .min_by_key(|(rank, _, _)| *rank)?;

        Some((group_rank, session_prefix, hash, key))
    }

    pub(super) fn evict_with_policy(&mut self, policy: EvictionPolicy) -> bool {
        let Some((_rank, session_prefix, hash, key)) = self.eviction_candidate(policy) else {
            return false;
        };
        let deleted = self.delete_hashed(&session_prefix, hash, &key);
        if deleted {
            self.evictions = self.evictions.saturating_add(1);
        }
        deleted
    }
}
