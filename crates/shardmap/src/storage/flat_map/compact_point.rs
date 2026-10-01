//! Experimental, uncompressed storage for small no-TTL point strings.
//!
//! There are no unreachable arena records: equal-length overwrites reuse their
//! record, and deletion or a length-changing overwrite promotes the entire map
//! to the general representation. Promotion is permanent. Arena growth and
//! promotion require an exclusive map borrow; read epochs additionally retain
//! the original arena allocation until readers leave.

use super::*;
use std::sync::OnceLock;

#[derive(Debug)]
struct CompactPointEntry {
    hash: u64,
    offset: u32,
    key_len: u16,
    value_len: u16,
    // Existing APIs can request a &Bytes. Materialize only that entry, once
    // per value version, and keep owned clones independent of arena mutation.
    shared: OnceLock<Box<SharedBytes>>,
}

#[derive(Debug)]
pub(super) struct CompactPointMap {
    active: bool,
    slots: Vec<u32>,
    entries: Vec<CompactPointEntry>,
    arena: Vec<u8>,
}

impl Default for CompactPointMap {
    fn default() -> Self {
        Self {
            active: true,
            slots: Vec::new(),
            entries: Vec::new(),
            arena: Vec::new(),
        }
    }
}

impl CompactPointMap {
    const EMPTY: u32 = u32::MAX;
    const INITIAL_SLOTS: usize = 1024;
    const MAX_KEY_BYTES: usize = 64;
    const MAX_VALUE_BYTES: usize = 256;

    pub(super) fn is_active(&self) -> bool {
        self.active
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    fn key(&self, entry: &CompactPointEntry) -> &[u8] {
        let start = entry.offset as usize;
        &self.arena[start..start + entry.key_len as usize]
    }

    fn value(&self, entry: &CompactPointEntry) -> &[u8] {
        let start = entry.offset as usize + entry.key_len as usize;
        &self.arena[start..start + entry.value_len as usize]
    }

    #[inline(always)]
    fn find(&self, hash: u64, key: &[u8]) -> Option<usize> {
        if !self.active || self.slots.is_empty() {
            return None;
        }
        let mask = self.slots.len() - 1;
        let mut bucket = local_table_hash(hash) as usize & mask;
        loop {
            let index = self.slots[bucket];
            if index == Self::EMPTY {
                return None;
            }
            let entry = &self.entries[index as usize];
            if entry.hash == hash && self.key(entry) == key {
                return Some(index as usize);
            }
            bucket = (bucket + 1) & mask;
        }
    }

    #[inline(always)]
    pub(super) fn get(&self, hash: u64, key: &[u8]) -> Option<&[u8]> {
        self.find(hash, key)
            .map(|index| self.value(&self.entries[index]))
    }

    #[inline(always)]
    pub(super) fn get_shared(&self, hash: u64, key: &[u8]) -> Option<&SharedBytes> {
        let entry = &self.entries[self.find(hash, key)?];
        Some(
            entry
                .shared
                .get_or_init(|| Box::new(shared_bytes_from_slice(self.value(entry)))),
        )
    }

    pub(super) fn get_shared_tagged(
        &self,
        hash: u64,
        key_tag: u64,
        key_len: usize,
    ) -> Option<&SharedBytes> {
        if !self.active || self.slots.is_empty() || hash_key_tag_from_hash(hash) != key_tag {
            return None;
        }
        let mask = self.slots.len() - 1;
        let mut bucket = local_table_hash(hash) as usize & mask;
        loop {
            let index = self.slots[bucket];
            if index == Self::EMPTY {
                return None;
            }
            let entry = &self.entries[index as usize];
            if entry.hash == hash && entry.key_len as usize == key_len {
                return Some(
                    entry
                        .shared
                        .get_or_init(|| Box::new(shared_bytes_from_slice(self.value(entry)))),
                );
            }
            bucket = (bucket + 1) & mask;
        }
    }

    // Returns the old length for replacement, None for insertion. A rejected
    // operation leaves the map intact so its caller can promote before retrying.
    pub(super) fn upsert(
        &mut self,
        hash: u64,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<usize>, ()> {
        if !self.active || key.len() > Self::MAX_KEY_BYTES || value.len() > Self::MAX_VALUE_BYTES {
            return Err(());
        }
        if let Some(index) = self.find(hash, key) {
            let entry = &mut self.entries[index];
            let old_len = entry.value_len as usize;
            if old_len != value.len() {
                return Err(());
            }
            let start = entry.offset as usize + entry.key_len as usize;
            self.arena[start..start + old_len].copy_from_slice(value);
            entry.shared.take();
            return Ok(Some(old_len));
        }
        let required = self
            .arena
            .len()
            .checked_add(key.len())
            .and_then(|n| n.checked_add(value.len()));
        let Some(required) = required.filter(|n| *n <= u32::MAX as usize) else {
            return Err(());
        };
        if self.entries.len() >= u32::MAX as usize {
            return Err(());
        }
        if self.slots.is_empty() || (self.entries.len() + 1) * 10 >= self.slots.len() * 7 {
            self.resize(self.slots.len().saturating_mul(2).max(Self::INITIAL_SLOTS));
        }
        // Bound unused payload capacity to 25%, capped at 4 MiB (and a 64 KiB
        // minimum growth quantum); descriptors leave at most 8192 spare slots.
        if required > self.arena.capacity() {
            let growth = (self.arena.len() / 4).clamp(64 * 1024, 4 * 1024 * 1024);
            self.arena
                .reserve_exact(growth.max(required - self.arena.len()));
        }
        if self.entries.len() == self.entries.capacity() {
            self.entries
                .reserve_exact((self.entries.len() / 4).clamp(256, 8192));
        }
        let entry = CompactPointEntry {
            hash,
            offset: self.arena.len() as u32,
            key_len: key.len() as u16,
            value_len: value.len() as u16,
            shared: OnceLock::new(),
        };
        self.arena.extend_from_slice(key);
        self.arena.extend_from_slice(value);
        let mask = self.slots.len() - 1;
        let mut bucket = local_table_hash(hash) as usize & mask;
        while self.slots[bucket] != Self::EMPTY {
            bucket = (bucket + 1) & mask;
        }
        self.slots[bucket] = self.entries.len() as u32;
        self.entries.push(entry);
        Ok(None)
    }

    fn resize(&mut self, capacity: usize) {
        let mut slots = vec![Self::EMPTY; capacity];
        let mask = capacity - 1;
        for (index, entry) in self.entries.iter().enumerate() {
            let mut bucket = local_table_hash(entry.hash) as usize & mask;
            while slots[bucket] != Self::EMPTY {
                bucket = (bucket + 1) & mask;
            }
            slots[bucket] = index as u32;
        }
        self.slots = slots;
    }

    pub(super) fn promote_into(&mut self, entries: &mut HashTable<FlatEntry>) -> SharedBytes {
        self.active = false;
        let descriptors = mem::take(&mut self.entries);
        entries.reserve(descriptors.len(), |entry| local_table_hash(entry.hash));
        for descriptor in descriptors {
            let key = self.key(&descriptor).to_vec().into_boxed_slice();
            let value = descriptor
                .shared
                .get()
                .map(|value| value.as_ref().clone())
                .unwrap_or_else(|| shared_bytes_from_slice(self.value(&descriptor)));
            entries.insert_unique(
                local_table_hash(descriptor.hash),
                FlatEntry {
                    hash: descriptor.hash,
                    key_tag: hash_key_tag_from_hash(descriptor.hash),
                    key_len: key.len(),
                    key,
                    value,
                    expire_at_ms: None,
                    semantic_index_token: None,
                    governance: None,
                    #[cfg(feature = "kv-overflow")]
                    overflow_generation: 0,
                    access: EntryAccessMeta {
                        last_touch: 0,
                        frequency: 1,
                    },
                },
                |entry| local_table_hash(entry.hash),
            );
        }
        self.slots = Vec::new();
        // Bytes owns the original Vec allocation without moving its payload.
        SharedBytes::from(mem::take(&mut self.arena))
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.entries.iter().map(|entry| self.key(entry))
    }

    pub(super) fn snapshot_entries(&self) -> Vec<StoredEntry> {
        self.entries
            .iter()
            .map(|entry| StoredEntry {
                key: self.key(entry).to_vec(),
                value: self.value(entry).to_vec(),
                expire_at_ms: None,
                governance: None,
            })
            .collect()
    }

    pub(super) fn scan_keys_visit(
        &self,
        offset: usize,
        limit: usize,
        visited: &mut usize,
        emitted: &mut usize,
        visit: &mut impl FnMut(&[u8]) -> bool,
    ) -> Option<usize> {
        for (index, key) in self.keys().enumerate().skip(offset) {
            *visited = visited.saturating_add(1);
            if visit(key) {
                *emitted = emitted.saturating_add(1);
            }
            if *visited >= limit {
                return Some(index + 1);
            }
        }
        None
    }

    pub(super) fn visit_entries(
        &self,
        visit: &mut impl FnMut(&[u8], &[u8], Option<u64>) -> bool,
    ) -> bool {
        self.entries
            .iter()
            .all(|entry| visit(self.key(entry), self.value(entry), None))
    }
}

impl FlatMap {
    #[inline(always)]
    pub(super) fn try_set_compact_point(&mut self, hash: u64, key: &[u8], value: &[u8]) -> bool {
        if !self.compact_points.is_active()
            || self.has_active_readers()
            || self.ttl_entries != 0
            || self.memory_limit_bytes.is_some()
            || self.eviction_policy != EvictionPolicy::None
            || self.object_overflow.is_some()
        {
            return false;
        }
        #[cfg(feature = "telemetry")]
        let start = self.start_telemetry_latency_sample();
        let Ok(old_len) = self.compact_points.upsert(hash, key, value) else {
            return false;
        };
        let delta = if old_len.is_none() {
            key.len() + value.len()
        } else {
            0
        };
        self.stored_bytes = self.stored_bytes.saturating_add(delta);
        #[cfg(feature = "telemetry")]
        self.record_set_metrics(
            value.len(),
            if old_len.is_none() { 1 } else { 0 },
            delta as isize,
            start,
        );
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_points_resolve_collisions_and_resize_without_allocating_shared_values() {
        let mut map = CompactPointMap::default();
        for i in 0..2000u64 {
            let key = format!("k:{i:016x}");
            // Deliberately use a colliding hash; lookup must compare full keys.
            assert_eq!(map.upsert(7, key.as_bytes(), &i.to_le_bytes()), Ok(None));
        }
        for i in 0..2000u64 {
            let key = format!("k:{i:016x}");
            assert_eq!(map.get(7, key.as_bytes()), Some(i.to_le_bytes().as_slice()));
        }
        assert_eq!(map.get(7, b"missing"), None);
        assert!(map.entries.iter().all(|entry| entry.shared.get().is_none()));
        assert_eq!(map.arena.len(), 2000 * (18 + 8));
        assert!(map.arena.capacity() <= map.arena.len() + 64 * 1024);
        assert!(map.entries.capacity() <= map.entries.len() + 8192);
    }

    #[test]
    fn compact_points_select_all_plain_setters_and_reuse_equal_length_records() {
        let mut map = FlatMap::new();
        map.set(b"owned".to_vec(), vec![1; 16], None, 0);
        map.set_slice(b"slice", &[2; 64], None, 0);
        map.set_bytes_hashed(
            hash_key(b"bytes"),
            b"bytes",
            SharedBytes::from(vec![3; 256]),
            None,
            0,
        );
        assert!(map.compact_points.is_active());
        assert!(map.entries.is_empty());
        let arena_len = map.compact_points.arena.len();
        let arena_capacity = map.compact_points.arena.capacity();
        for _ in 0..1000 {
            map.set_slice_hashed_no_ttl(hash_key(b"slice"), b"slice", &[4; 64]);
        }
        assert_eq!(map.len(), 3);
        assert_eq!(map.stored_bytes(), 5 + 16 + 5 + 64 + 5 + 256);
        assert_eq!(map.compact_points.arena.len(), arena_len);
        assert_eq!(map.compact_points.arena.capacity(), arena_capacity);
        assert_eq!(map.get(b"slice", 0), Some(vec![4; 64]));
    }

    #[test]
    fn compact_points_capacity_hint_does_not_allocate_an_unused_general_table() {
        let mut map = FlatMap::with_capacity(100_000);
        assert_eq!(map.entries.capacity(), 0);
        map.set_slice(b"key", b"value", None, 0);
        assert!(map.compact_points.is_active());
        assert_eq!(map.entries.capacity(), 0);
        map.set_slice(b"key", b"longer", None, 0);
        assert!(!map.compact_points.is_active());
        assert_eq!(map.get(b"key", 0), Some(b"longer".to_vec()));
    }

    #[test]
    fn compact_points_shared_clones_keep_previous_versions() {
        let mut map = FlatMap::new();
        let hash = hash_key(b"key");
        map.set_slice(b"key", b"one", None, 0);
        let old = map.get_value_bytes_hashed(hash, b"key", 0).unwrap();
        let shared = map
            .get_shared_value_bytes_hashed_no_ttl(hash, b"key")
            .unwrap();
        assert_eq!(shared.as_ptr(), old.as_ptr());
        map.set_slice(b"key", b"two", None, 0);
        assert_eq!(old.as_ref(), b"one");
        assert_eq!(
            map.get_value_bytes_hashed(hash, b"key", 0)
                .unwrap()
                .as_ref(),
            b"two"
        );
        assert!(map.compact_points.is_active());
    }

    #[test]
    fn compact_points_length_change_and_delete_reinsert_permanently_promote() {
        let mut map = FlatMap::new();
        map.set_slice(b"a", b"one", None, 0);
        map.set_slice(b"a", b"longer", None, 0);
        assert!(!map.compact_points.is_active());
        assert_eq!(map.compact_points.arena.capacity(), 0);
        assert_eq!(map.get(b"a", 0), Some(b"longer".to_vec()));
        assert_eq!(map.stored_bytes(), 7);

        let mut map = FlatMap::new();
        map.set_slice(b"a", b"one", None, 0);
        map.set_slice(b"b", b"two", None, 0);
        assert!(map.delete(b"a", 0));
        assert!(!map.compact_points.is_active());
        for _ in 0..1000 {
            map.set_slice(b"a", b"new", None, 0);
            assert!(map.delete(b"a", 0));
        }
        assert_eq!(map.compact_points.arena.capacity(), 0);
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(b"b", 0), Some(b"two".to_vec()));
        assert_eq!(map.stored_bytes(), 4);
    }

    #[test]
    fn compact_points_promote_for_ttl_governance_large_values_and_memory_policy() {
        for mode in 0..4 {
            let mut map = FlatMap::new();
            map.set_slice(b"a", b"one", None, 0);
            match mode {
                0 => map.set_slice(b"b", b"two", Some(10), 0),
                1 => map.set_bytes_hashed_with_governance(
                    hash_key(b"b"),
                    b"b",
                    SharedBytes::from_static(b"two"),
                    SharedBytes::from_static(b"policy"),
                    None,
                    0,
                ),
                2 => map.set_slice(b"b", &[7; 1024], None, 0),
                _ => map.configure_memory_policy(Some(1024), EvictionPolicy::Lru, 0),
            }
            assert!(!map.compact_points.is_active());
            assert_eq!(map.get(b"a", 0), Some(b"one".to_vec()));
            if mode == 0 {
                assert_eq!(map.get(b"b", 10), None);
            }
            if mode == 1 {
                assert!(map.is_value_protected_hashed(hash_key(b"b"), b"b", 0));
            }
        }
    }

    #[test]
    fn compact_points_snapshot_paging_and_restore_preserve_values_and_limits() {
        let mut map = FlatMap::new();
        for i in 0..12u64 {
            map.set_slice(format!("k:{i:016x}").as_bytes(), &i.to_le_bytes(), None, 0);
        }
        assert!(map.snapshot_entry_keys(0, None, 1).is_err());
        assert!(
            map.snapshot_entry_keys(0, Some(std::time::Instant::now()), usize::MAX)
                .is_err()
        );
        let keys = map.snapshot_entry_keys(0, None, usize::MAX).unwrap();
        assert_eq!(keys.len(), 12);
        let (page, consumed) = map.snapshot_entry_source_page(&keys, 0, 100, 0).unwrap();
        assert!(!page.is_empty());
        assert!(consumed < 12);
        let snapshot = map.try_snapshot_entries(0).unwrap();
        let mut restored = FlatMap::from_entries(snapshot, 0);
        assert!(restored.compact_points.is_active());
        for i in 0..12u64 {
            assert_eq!(
                restored.get(format!("k:{i:016x}").as_bytes(), 0),
                Some(i.to_le_bytes().to_vec())
            );
        }
        assert!(map.compact_points.is_active());
    }

    #[cfg(feature = "redis")]
    #[test]
    fn compact_points_scan_resumes_and_honors_filter() {
        let mut map = FlatMap::new();
        for key in [b"a", b"b", b"c"] {
            map.set_slice(key, b"value", None, 0);
        }
        let mut seen = Vec::new();
        let mut offset = 0;
        loop {
            let (mut visited, mut emitted) = (0, 0);
            let next = map.scan_keys_visit(offset, 1, 0, &mut visited, &mut emitted, &mut |key| {
                if key == b"b" {
                    return false;
                }
                seen.push(key.to_vec());
                true
            });
            match next {
                Some(next) => offset = next,
                None => break,
            }
        }
        assert_eq!(seen, vec![b"a".to_vec(), b"c".to_vec()]);
        assert!(map.compact_points.is_active());
    }

    #[test]
    fn compact_points_read_epoch_retains_original_arena_during_promotion() {
        let mut map = FlatMap::new();
        map.set_slice(b"key", b"one", None, 0);
        map.begin_read_epoch();
        let old = map.get_ref(b"key", 0).unwrap();
        let ptr = old.as_ptr();
        map.set_slice(b"key", b"two", None, 0);
        assert!(!map.compact_points.is_active());
        // SAFETY: the open read epoch retains the original arena allocation.
        assert_eq!(unsafe { std::slice::from_raw_parts(ptr, 3) }, b"one");
        assert!(!map.retired_values.is_empty());
        map.end_read_epoch();
        map.process_maintenance(0);
        assert!(map.retired_values.is_empty());
        assert_eq!(map.get(b"key", 0), Some(b"two".to_vec()));
    }
}
