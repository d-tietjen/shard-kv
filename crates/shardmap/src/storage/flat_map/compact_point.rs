//! Experimental, uncompressed storage for small no-TTL point strings.
//!
//! Payload records live in fixed 4 KiB chunks and never move. Two-byte size
//! classes reuse deleted records; empty chunks release their allocation and
//! reuse their descriptor. An open read epoch retires only the replaced record,
//! with at most 32 records reclaimed per mutation/maintenance call afterwards.
//! General entries coexist with compact entries; metadata migrates one key.

use super::*;
use std::sync::OnceLock;

#[derive(Debug)]
struct CompactPointEntry {
    hash: u64,
    offset: u32,
    key_len: u16,
    value_len: u16,
    shared: OnceLock<Box<SharedBytes>>,
}

#[derive(Debug)]
struct PayloadChunk {
    bytes: Box<[u8; CompactPointMap::CHUNK_BYTES]>,
    class: u16,
    next: u16,
    free_head: u16,
    occupied: u16,
    available_prev: u32,
    available_next: u32,
}

#[derive(Debug)]
struct RetiredRecord {
    offset: u32,
    _shared: Option<Box<SharedBytes>>,
}

#[derive(Debug)]
pub(super) struct CompactPointMap {
    entries: HashTable<CompactPointEntry>,
    chunks: Vec<Option<PayloadChunk>>,
    free_chunks: Vec<u32>,
    available: [u32; CompactPointMap::SIZE_CLASSES],
    retired: VecDeque<RetiredRecord>,
    // Access metadata is needed only after a runtime policy starts sampling.
    access: FastHashMap<u32, EntryAccessMeta>,
}

impl Default for CompactPointMap {
    fn default() -> Self {
        Self {
            entries: HashTable::new(),
            chunks: Vec::new(),
            free_chunks: Vec::new(),
            available: [Self::NO_CHUNK; Self::SIZE_CLASSES],
            retired: VecDeque::new(),
            access: FastHashMap::default(),
        }
    }
}

impl CompactPointMap {
    const CHUNK_BYTES: usize = 4096;
    const MAX_CHUNKS: usize = 64 * 1024 * 1024 / Self::CHUNK_BYTES;
    const RECLAIM_BUDGET: usize = 32;
    const EMPTY: u16 = u16::MAX;
    const NO_CHUNK: u32 = u32::MAX;
    const MAX_KEY_BYTES: usize = 64;
    const MAX_VALUE_BYTES: usize = 256;
    // Released records store a u16 free-list link in their first two bytes.
    // Byte-slice payload access needs no larger alignment or size rounding.
    const CLASS_BYTES: usize = mem::size_of::<u16>();
    const SIZE_CLASSES: usize =
        (Self::MAX_KEY_BYTES + Self::MAX_VALUE_BYTES).div_ceil(Self::CLASS_BYTES);

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    fn record<'a>(chunks: &'a [Option<PayloadChunk>], entry: &CompactPointEntry) -> &'a [u8] {
        let offset = entry.offset as usize;
        let chunk = chunks[offset / Self::CHUNK_BYTES]
            .as_ref()
            .expect("live compact chunk");
        let start = offset % Self::CHUNK_BYTES;
        &chunk.bytes[start..start + entry.key_len as usize + entry.value_len as usize]
    }

    fn key(&self, entry: &CompactPointEntry) -> &[u8] {
        &Self::record(&self.chunks, entry)[..entry.key_len as usize]
    }

    fn value(&self, entry: &CompactPointEntry) -> &[u8] {
        &Self::record(&self.chunks, entry)[entry.key_len as usize..]
    }

    fn matches(
        chunks: &[Option<PayloadChunk>],
        entry: &CompactPointEntry,
        hash: u64,
        key: &[u8],
    ) -> bool {
        entry.hash == hash && &Self::record(chunks, entry)[..entry.key_len as usize] == key
    }

    #[inline(always)]
    pub(super) fn get(&self, hash: u64, key: &[u8]) -> Option<&[u8]> {
        self.entries
            .find(local_table_hash(hash), |entry| {
                Self::matches(&self.chunks, entry, hash, key)
            })
            .map(|entry| self.value(entry))
    }

    pub(super) fn get_with_access(&mut self, hash: u64, key: &[u8], tick: u64) -> Option<&[u8]> {
        let entry = self.entries.find(local_table_hash(hash), |entry| {
            Self::matches(&self.chunks, entry, hash, key)
        })?;
        if tick != 0 {
            self.access
                .entry(entry.offset)
                .or_insert(EntryAccessMeta {
                    last_touch: 0,
                    frequency: 1,
                })
                .record_access(tick);
        }
        Some(self.value(entry))
    }

    #[inline(always)]
    pub(super) fn get_shared(&self, hash: u64, key: &[u8]) -> Option<&SharedBytes> {
        let entry = self.entries.find(local_table_hash(hash), |entry| {
            Self::matches(&self.chunks, entry, hash, key)
        })?;
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
        if hash_key_tag_from_hash(hash) != key_tag {
            return None;
        }
        let entry = self.entries.find(local_table_hash(hash), |entry| {
            entry.hash == hash && entry.key_len as usize == key_len
        })?;
        Some(
            entry
                .shared
                .get_or_init(|| Box::new(shared_bytes_from_slice(self.value(entry)))),
        )
    }

    fn remove_available(&mut self, id: u32) {
        let chunk = self.chunks[id as usize]
            .as_mut()
            .expect("allocated compact chunk");
        let class = usize::from(chunk.class);
        let prev = chunk.available_prev;
        let next = chunk.available_next;
        // A linked non-head always has a predecessor. Both links are cleared
        // when a full chunk leaves the list; a singleton is also its head.
        if prev == Self::NO_CHUNK && self.available[class] != id {
            return;
        }
        chunk.available_prev = Self::NO_CHUNK;
        chunk.available_next = Self::NO_CHUNK;
        if prev == Self::NO_CHUNK {
            self.available[class] = next;
        } else {
            self.chunks[prev as usize]
                .as_mut()
                .expect("available compact chunk")
                .available_next = next;
        }
        if next != Self::NO_CHUNK {
            self.chunks[next as usize]
                .as_mut()
                .expect("available compact chunk")
                .available_prev = prev;
        }
    }

    fn add_available(&mut self, id: u32) {
        let chunk = self.chunks[id as usize]
            .as_mut()
            .expect("allocated compact chunk");
        let class = usize::from(chunk.class);
        let head = self.available[class];
        debug_assert_eq!(chunk.available_prev, Self::NO_CHUNK);
        debug_assert_eq!(chunk.available_next, Self::NO_CHUNK);
        debug_assert_ne!(head, id);
        chunk.available_next = head;
        self.available[class] = id;
        if head != Self::NO_CHUNK {
            self.chunks[head as usize]
                .as_mut()
                .expect("available compact chunk")
                .available_prev = id;
        }
    }

    fn allocate(&mut self, size: usize) -> Result<u32, ()> {
        let class = size.max(1).div_ceil(Self::CLASS_BYTES) - 1;
        let stored_class = u16::try_from(class).map_err(|_| ())?;
        let stride = (class + 1) * Self::CLASS_BYTES;
        let id = if self.available[class] != Self::NO_CHUNK {
            self.available[class]
        } else {
            let id = if let Some(id) = self.free_chunks.pop() {
                id
            } else {
                if self.chunks.len() == Self::MAX_CHUNKS {
                    return Err(());
                }
                self.chunks.push(None);
                (self.chunks.len() - 1) as u32
            };
            self.chunks[id as usize] = Some(PayloadChunk {
                bytes: Box::new([0; Self::CHUNK_BYTES]),
                class: stored_class,
                next: 0,
                free_head: Self::EMPTY,
                occupied: 0,
                available_prev: Self::NO_CHUNK,
                available_next: Self::NO_CHUNK,
            });
            self.add_available(id);
            id
        };
        let chunk = self.chunks[id as usize]
            .as_mut()
            .expect("available compact chunk");
        let offset = if chunk.free_head != Self::EMPTY {
            let offset = chunk.free_head;
            let start = offset as usize;
            chunk.free_head = u16::from_le_bytes([chunk.bytes[start], chunk.bytes[start + 1]]);
            offset
        } else {
            let offset = chunk.next as usize * stride;
            chunk.next += 1;
            offset as u16
        };
        chunk.occupied += 1;
        if chunk.free_head == Self::EMPTY && (chunk.next as usize + 1) * stride > Self::CHUNK_BYTES
        {
            self.remove_available(id);
        }
        Ok(id * Self::CHUNK_BYTES as u32 + offset as u32)
    }

    fn release(&mut self, offset: u32) {
        let id = offset as usize / Self::CHUNK_BYTES;
        let start = offset as usize % Self::CHUNK_BYTES;
        let chunk = self.chunks[id].as_mut().expect("retained compact chunk");
        chunk.occupied -= 1;
        if chunk.occupied == 0 {
            self.remove_available(id as u32);
            self.chunks[id] = None;
            self.free_chunks.push(id as u32);
        } else {
            chunk.bytes[start..start + 2].copy_from_slice(&chunk.free_head.to_le_bytes());
            chunk.free_head = start as u16;
            if chunk.available_prev == Self::NO_CHUNK
                && self.available[usize::from(chunk.class)] != id as u32
            {
                self.add_available(id as u32);
            }
        }
    }

    fn retire(&mut self, entry: CompactPointEntry, active_readers: bool) {
        self.access.remove(&entry.offset);
        if active_readers {
            self.retired.push_back(RetiredRecord {
                offset: entry.offset,
                _shared: entry.shared.into_inner(),
            });
        } else {
            self.release(entry.offset);
        }
    }

    pub(super) fn reclaim(&mut self, active_readers: bool) {
        if active_readers {
            return;
        }
        for _ in 0..Self::RECLAIM_BUDGET {
            let Some(record) = self.retired.pop_front() else {
                break;
            };
            self.release(record.offset);
        }
    }

    // Rejection preserves the old entry. Each successful write copies at most
    // 64 key bytes and 256 value bytes; chunk allocation initializes 4 KiB.
    pub(super) fn upsert(
        &mut self,
        hash: u64,
        key: &[u8],
        value: &[u8],
        active_readers: bool,
    ) -> Result<Option<usize>, ()> {
        if key.len() > Self::MAX_KEY_BYTES || value.len() > Self::MAX_VALUE_BYTES {
            return Err(());
        }
        self.reclaim(active_readers);
        let old = self
            .entries
            .find(local_table_hash(hash), |entry| {
                Self::matches(&self.chunks, entry, hash, key)
            })
            .map(|entry| (entry.offset, entry.value_len as usize));
        if let Some((offset, old_len)) = old
            && old_len == value.len()
            && !active_readers
        {
            let chunk = self.chunks[offset as usize / Self::CHUNK_BYTES]
                .as_mut()
                .expect("live compact chunk");
            let start = offset as usize % Self::CHUNK_BYTES + key.len();
            chunk.bytes[start..start + value.len()].copy_from_slice(value);
            self.entries
                .find_mut(local_table_hash(hash), |entry| {
                    Self::matches(&self.chunks, entry, hash, key)
                })
                .expect("compact entry found above")
                .shared
                .take();
            return Ok(Some(old_len));
        }
        let offset = self.allocate(key.len() + value.len())?;
        let chunk = self.chunks[offset as usize / Self::CHUNK_BYTES]
            .as_mut()
            .expect("new compact chunk");
        let start = offset as usize % Self::CHUNK_BYTES;
        chunk.bytes[start..start + key.len()].copy_from_slice(key);
        chunk.bytes[start + key.len()..start + key.len() + value.len()].copy_from_slice(value);
        let replacement = CompactPointEntry {
            hash,
            offset,
            key_len: key.len() as u16,
            value_len: value.len() as u16,
            shared: OnceLock::new(),
        };
        if let Some((old_offset, old_len)) = old {
            let access = self.access.get(&old_offset).copied();
            let old_entry = mem::replace(
                self.entries
                    .find_mut(local_table_hash(hash), |entry| {
                        Self::matches(&self.chunks, entry, hash, key)
                    })
                    .expect("compact entry found above"),
                replacement,
            );
            self.retire(old_entry, active_readers);
            if let Some(access) = access {
                self.access.insert(offset, access);
            }
            Ok(Some(old_len))
        } else {
            self.entries
                .insert_unique(local_table_hash(hash), replacement, |entry| {
                    local_table_hash(entry.hash)
                });
            Ok(None)
        }
    }

    pub(super) fn remove(&mut self, hash: u64, key: &[u8], active_readers: bool) -> Option<usize> {
        let entry = self
            .entries
            .find_entry(local_table_hash(hash), |entry| {
                Self::matches(&self.chunks, entry, hash, key)
            })
            .ok()?;
        let (entry, _) = entry.remove();
        let value_len = entry.value_len as usize;
        self.retire(entry, active_readers);
        Some(value_len)
    }

    #[cfg(feature = "mutable-value-slices")]
    pub(super) fn value_is_unique(&self, hash: u64, key: &[u8]) -> bool {
        self.entries
            .find(local_table_hash(hash), |entry| {
                Self::matches(&self.chunks, entry, hash, key)
            })
            .is_some_and(|entry| entry.shared.get().is_none_or(|value| value.is_unique()))
    }

    #[cfg(any(feature = "mutable-value-slices", feature = "redis"))]
    pub(super) fn value_mut(
        &mut self,
        hash: u64,
        key: &[u8],
        active_readers: bool,
        tick: u64,
    ) -> Option<&mut [u8]> {
        if active_readers {
            // Copy only this bounded value before retiring its borrowed record.
            let value = self.get(hash, key)?.to_vec();
            self.upsert(hash, key, &value, true).ok()?;
        }
        let entry = self.entries.find_mut(local_table_hash(hash), |entry| {
            Self::matches(&self.chunks, entry, hash, key)
        })?;
        if tick != 0 {
            self.access
                .entry(entry.offset)
                .or_insert(EntryAccessMeta {
                    last_touch: 0,
                    frequency: 1,
                })
                .record_access(tick);
        }
        entry.shared.take();
        let offset = entry.offset as usize;
        let start = offset % Self::CHUNK_BYTES + entry.key_len as usize;
        let chunk = self.chunks[offset / Self::CHUNK_BYTES]
            .as_mut()
            .expect("live compact chunk");
        Some(&mut chunk.bytes[start..start + entry.value_len as usize])
    }

    pub(super) fn entry_access(&self, hash: u64, key: &[u8]) -> Option<EntryAccessMeta> {
        let entry = self.entries.find(local_table_hash(hash), |entry| {
            Self::matches(&self.chunks, entry, hash, key)
        })?;
        Some(
            self.access
                .get(&entry.offset)
                .copied()
                .unwrap_or(EntryAccessMeta {
                    last_touch: 0,
                    frequency: 1,
                }),
        )
    }

    pub(super) fn take_general(
        &mut self,
        hash: u64,
        key: &[u8],
        active_readers: bool,
    ) -> Option<FlatEntry> {
        // General local setters may use an exclusive in-place value update.
        // Detach this bounded value from every old shared owner during migration.
        let value = shared_bytes_from_slice(self.get(hash, key)?);
        let offset = self
            .entries
            .find(local_table_hash(hash), |entry| {
                Self::matches(&self.chunks, entry, hash, key)
            })?
            .offset;
        let access = self
            .access
            .get(&offset)
            .copied()
            .unwrap_or(EntryAccessMeta {
                last_touch: 0,
                frequency: 1,
            });
        self.remove(hash, key, active_readers)?;
        Some(FlatEntry {
            hash,
            key_tag: hash_key_tag_from_hash(hash),
            key: key.to_vec().into_boxed_slice(),
            value,
            expire_at_ms: None,
            semantic_index_token: None,
            governance: None,
            #[cfg(feature = "kv-overflow")]
            overflow_generation: 0,
            access,
        })
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.entries.iter().map(|entry| self.key(entry))
    }
    pub(super) fn eviction_entries(&self) -> impl Iterator<Item = (u64, &[u8], EntryAccessMeta)> {
        self.entries.iter().map(|entry| {
            (
                entry.hash,
                self.key(entry),
                self.access
                    .get(&entry.offset)
                    .copied()
                    .unwrap_or(EntryAccessMeta {
                        last_touch: 0,
                        frequency: 1,
                    }),
            )
        })
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
    pub(super) fn visit_entries(
        &self,
        visit: &mut impl FnMut(&[u8], &[u8], Option<u64>) -> bool,
    ) -> bool {
        self.entries
            .iter()
            .all(|entry| visit(self.key(entry), self.value(entry), None))
    }

    #[cfg(test)]
    pub(crate) fn shared_owner_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.shared.get().is_some())
            .count()
    }
}

impl FlatMap {
    #[inline(always)]
    pub(super) fn try_set_compact_point(&mut self, hash: u64, key: &[u8], value: &[u8]) -> bool {
        if self.memory_limit_bytes.is_some()
            || self.eviction_policy != EvictionPolicy::None
            || self.object_overflow.is_some()
            || self
                .entries
                .find(local_table_hash(hash), |entry| {
                    entry.matches_hashed_key(hash, key)
                })
                .is_some()
            || self.remote_entries.contains_key(key)
        {
            return false;
        }
        self.disable_fast_point_map();
        #[cfg(feature = "telemetry")]
        let start = self.start_telemetry_latency_sample();
        let active = self.has_active_readers();
        let Ok(old_len) = self.compact_points.upsert(hash, key, value, active) else {
            return false;
        };
        let previous_bytes = old_len.map_or(0, |len| key.len() + len);
        let new_bytes = key.len() + value.len();
        self.stored_bytes = self
            .stored_bytes
            .saturating_sub(previous_bytes)
            .saturating_add(new_bytes);
        #[cfg(feature = "telemetry")]
        self.record_set_metrics(
            value.len(),
            if old_len.is_none() { 1 } else { 0 },
            new_bytes as isize - previous_bytes as isize,
            start,
        );
        true
    }

    pub(super) fn delete_compact_point(
        &mut self,
        hash: u64,
        key: &[u8],
        reason: DeleteReason,
    ) -> bool {
        let active = self.has_active_readers();
        let Some(value_len) = self.compact_points.remove(hash, key, active) else {
            return false;
        };
        self.compact_points.reclaim(active);
        let removed_bytes = key.len() + value_len;
        self.stored_bytes = self.stored_bytes.saturating_sub(removed_bytes);
        if reason == DeleteReason::Evicted {
            self.evictions = self.evictions.saturating_add(1);
        }
        #[cfg(feature = "telemetry")]
        self.record_delete_metrics(reason, -1, -(removed_bytes as isize));
        true
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn allocated_chunks(map: &CompactPointMap) -> usize {
        map.chunks.iter().filter(|chunk| chunk.is_some()).count()
    }

    fn assert_available_chain(map: &CompactPointMap, class: usize, expected: &[u32]) {
        let mut id = map.available[class];
        let mut prev = CompactPointMap::NO_CHUNK;
        for expected_id in expected {
            assert_eq!(id, *expected_id);
            let chunk = map.chunks[id as usize].as_ref().unwrap();
            assert_eq!(usize::from(chunk.class), class);
            assert_eq!(chunk.available_prev, prev);
            let stride = (class + 1) * CompactPointMap::CLASS_BYTES;
            assert!(
                chunk.free_head != CompactPointMap::EMPTY
                    || (chunk.next as usize + 1) * stride <= CompactPointMap::CHUNK_BYTES
            );
            prev = id;
            id = chunk.available_next;
        }
        assert_eq!(id, CompactPointMap::NO_CHUNK);
    }

    #[test]
    fn compact_points_pack_small_records_with_fixed_descriptor_bounds() {
        println!(
            "compact layout bytes: entry={} chunk={} optional_chunk={} map={} class_heads={}",
            mem::size_of::<CompactPointEntry>(),
            mem::size_of::<PayloadChunk>(),
            mem::size_of::<Option<PayloadChunk>>(),
            mem::size_of::<CompactPointMap>(),
            mem::size_of::<[u32; CompactPointMap::SIZE_CLASSES]>()
        );
        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(mem::size_of::<CompactPointEntry>(), 32);
            assert!(mem::size_of::<PayloadChunk>() <= 24);
            assert!(mem::size_of::<Option<PayloadChunk>>() <= 24);
            assert_eq!(mem::size_of::<[u32; CompactPointMap::SIZE_CLASSES]>(), 640);
            assert!(mem::size_of::<CompactPointMap>() <= 1024);
        }
        for (value_len, records_per_chunk) in [(16, 120), (64, 49), (256, 14)] {
            let mut map = CompactPointMap::default();
            let value = vec![value_len as u8; value_len];
            let anchor = b"k:0000000000000000";
            for i in 0..records_per_chunk {
                let key = format!("k:{i:016x}");
                map.upsert(hash_key(key.as_bytes()), key.as_bytes(), &value, false)
                    .unwrap();
            }
            assert_eq!(allocated_chunks(&map), 1);
            let anchor_ptr = map.get(hash_key(anchor), anchor).unwrap().as_ptr();
            let key = format!("k:{records_per_chunk:016x}");
            map.upsert(hash_key(key.as_bytes()), key.as_bytes(), &value, false)
                .unwrap();
            assert_eq!(allocated_chunks(&map), 2);
            assert_eq!(map.get(hash_key(anchor), anchor), Some(value.as_slice()));
            assert_eq!(
                map.get(hash_key(anchor), anchor).unwrap().as_ptr(),
                anchor_ptr
            );
        }
    }

    #[test]
    fn compact_points_all_size_classes_reuse_free_links_and_chunk_descriptors() {
        let mut map = CompactPointMap::default();
        // Empty records still need space for the two-byte free-list link.
        let empty = map.allocate(0).unwrap();
        map.release(empty);
        assert_eq!(CompactPointMap::SIZE_CLASSES, 160);
        for stride in (2..=320).step_by(2) {
            let count = CompactPointMap::CHUNK_BYTES / stride;
            let mut offsets = Vec::new();
            for i in 0..count {
                let offset = map.allocate(stride - 1).unwrap();
                let chunk = map.chunks[offset as usize / CompactPointMap::CHUNK_BYTES]
                    .as_mut()
                    .unwrap();
                let start = offset as usize % CompactPointMap::CHUNK_BYTES;
                chunk.bytes[start..start + stride].fill((i % 251) as u8);
                offsets.push(offset);
            }
            assert_eq!(allocated_chunks(&map), 1, "stride {stride}");
            let overflow = map.allocate(stride).unwrap();
            assert_eq!(allocated_chunks(&map), 2, "stride {stride}");
            let anchor = *offsets.last().unwrap();
            let anchor_ptr = map.chunks[anchor as usize / CompactPointMap::CHUNK_BYTES]
                .as_ref()
                .unwrap()
                .bytes
                .as_ptr();
            // A full chunk becomes available again. Release two records so
            // allocating the adjacent odd/even lengths follows both links.
            map.release(offsets[0]);
            map.release(offsets[1]);
            assert_eq!(map.allocate(stride - 1), Ok(offsets[1]));
            assert_eq!(map.allocate(stride), Ok(offsets[0]));
            for (i, offset) in offsets.iter().copied().enumerate().skip(2) {
                let chunk = map.chunks[offset as usize / CompactPointMap::CHUNK_BYTES]
                    .as_ref()
                    .unwrap();
                let start = offset as usize % CompactPointMap::CHUNK_BYTES;
                assert!(
                    chunk.bytes[start..start + stride]
                        .iter()
                        .all(|byte| *byte == (i % 251) as u8),
                    "stride {stride}, record {i}"
                );
            }
            assert_eq!(
                map.chunks[anchor as usize / CompactPointMap::CHUNK_BYTES]
                    .as_ref()
                    .unwrap()
                    .bytes
                    .as_ptr(),
                anchor_ptr
            );
            for offset in offsets {
                map.release(offset);
            }
            map.release(overflow);
            assert_eq!(allocated_chunks(&map), 0);
            assert!(
                map.available
                    .iter()
                    .all(|id| *id == CompactPointMap::NO_CHUNK)
            );
            assert!(map.chunks.len() <= 2);
        }
    }

    #[test]
    fn compact_points_availability_unlinks_every_position_and_reuses_empty_chunks() {
        let mut map = CompactPointMap::default();
        let stride = 320;
        let class = CompactPointMap::SIZE_CLASSES - 1;
        let count = CompactPointMap::CHUNK_BYTES / stride;
        let mut offsets = Vec::new();
        for _ in 0..4 {
            offsets.push(
                (0..count)
                    .map(|_| map.allocate(stride).unwrap())
                    .collect::<Vec<_>>(),
            );
        }
        assert_available_chain(&map, class, &[]);
        for (id, records) in offsets.iter().enumerate() {
            map.release(records[0]);
            let expected = (0..=id as u32).rev().collect::<Vec<_>>();
            assert_available_chain(&map, class, &expected);
        }
        // Refill the head's free slot: the full head must leave the list.
        assert_eq!(map.allocate(stride), Ok(offsets[3][0]));
        assert_available_chain(&map, class, &[2, 1, 0]);
        let full = map.chunks[3].as_ref().unwrap();
        assert_eq!(full.available_prev, CompactPointMap::NO_CHUNK);
        assert_eq!(full.available_next, CompactPointMap::NO_CHUNK);
        map.release(offsets[3][0]);
        assert_available_chain(&map, class, &[3, 2, 1, 0]);
        // Empty chunks exercise middle, tail, head, then singleton removal.
        for (id, expected) in [
            (1, &[3, 2, 0][..]),
            (0, &[3, 2][..]),
            (3, &[2][..]),
            (2, &[][..]),
        ] {
            for offset in offsets[id].iter().copied().skip(1) {
                map.release(offset);
            }
            assert!(map.chunks[id].is_none());
            assert_available_chain(&map, class, expected);
        }
        assert_eq!(allocated_chunks(&map), 0);
        let reused = map.allocate(stride).unwrap();
        assert_eq!(reused, offsets[2][0]);
        assert_eq!(map.chunks.len(), 4);
        assert_available_chain(&map, class, &[2]);
        assert!(
            map.chunks[2]
                .as_ref()
                .unwrap()
                .bytes
                .iter()
                .all(|byte| *byte == 0)
        );
        map.release(reused);
        assert_available_chain(&map, class, &[]);
    }

    #[test]
    fn compact_points_adjacent_lengths_retire_owners_and_reuse_after_quiescence() {
        let mut map = CompactPointMap::default();
        let mut previous = Vec::new();
        for i in 0..32 {
            let key = format!("k:{i:016x}");
            let hash = hash_key(key.as_bytes());
            let value = vec![i as u8; 16 + i % 4];
            map.upsert(hash, key.as_bytes(), &value, false).unwrap();
            let offset = map
                .entries
                .find(local_table_hash(hash), |entry| {
                    CompactPointMap::matches(&map.chunks, entry, hash, key.as_bytes())
                })
                .unwrap()
                .offset;
            let owner = map.get_shared(hash, key.as_bytes()).unwrap().clone();
            previous.push((key, offset, value, owner));
        }
        for (i, (key, _, _, _)) in previous.iter().enumerate() {
            let hash = hash_key(key.as_bytes());
            let value = vec![100 + i as u8; 16 + (i + 1) % 4];
            map.upsert(hash, key.as_bytes(), &value, true).unwrap();
            assert_eq!(map.get(hash, key.as_bytes()), Some(value.as_slice()));
        }
        // Retired odd and even lengths must remain readable in place, including
        // their materialized owners, while later adjacent classes are used.
        for (key, offset, value, owner) in &previous {
            let chunk = map.chunks[*offset as usize / CompactPointMap::CHUNK_BYTES]
                .as_ref()
                .unwrap();
            let start = *offset as usize % CompactPointMap::CHUNK_BYTES + key.len();
            assert_eq!(&chunk.bytes[start..start + value.len()], value.as_slice());
            assert_eq!(owner.as_ref(), value.as_slice());
            assert!(
                map.remove(hash_key(key.as_bytes()), key.as_bytes(), true)
                    .is_some()
            );
        }
        assert_eq!(map.retired.len(), 64);
        map.reclaim(true);
        assert_eq!(map.retired.len(), 64);
        map.reclaim(false);
        assert_eq!(map.retired.len(), 32);
        map.reclaim(false);
        assert!(map.retired.is_empty());
        assert_eq!(allocated_chunks(&map), 0);
        let peak_chunks = map.chunks.len();
        for (key, _, value, owner) in previous {
            map.upsert(hash_key(key.as_bytes()), key.as_bytes(), &value, false)
                .unwrap();
            assert_eq!(owner.as_ref(), value.as_slice());
            assert_eq!(
                map.get(hash_key(key.as_bytes()), key.as_bytes()),
                Some(value.as_slice())
            );
        }
        assert_eq!(map.len(), 32);
        assert_eq!(map.chunks.len(), peak_chunks);
        assert!(map.retired.is_empty());
    }

    #[test]
    fn compact_points_resolve_collisions_and_never_move_existing_payloads() {
        let mut map = CompactPointMap::default();
        map.upsert(7, b"anchor", b"unchanged", false).unwrap();
        let ptr = map.get(7, b"anchor").unwrap().as_ptr();
        for i in 0..2000u64 {
            let key = format!("k:{i:016x}");
            assert_eq!(
                map.upsert(7, key.as_bytes(), &i.to_le_bytes(), false),
                Ok(None)
            );
        }
        for i in 0..2000u64 {
            let key = format!("k:{i:016x}");
            assert_eq!(map.get(7, key.as_bytes()), Some(i.to_le_bytes().as_slice()));
        }
        assert_eq!(map.get(7, b"missing"), None);
        assert_eq!(map.get(7, b"anchor").unwrap().as_ptr(), ptr);
        assert_eq!(map.shared_owner_count(), 0);
        assert!(
            map.chunks
                .iter()
                .flatten()
                .all(|chunk| chunk.bytes.len() == CompactPointMap::CHUNK_BYTES)
        );
    }

    #[test]
    fn compact_points_select_plain_setters_and_reuse_equal_length_records() {
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
        assert_eq!(map.compact_points.len(), 3);
        assert!(map.entries.is_empty());
        let ptr = map
            .compact_points
            .get(hash_key(b"slice"), b"slice")
            .unwrap()
            .as_ptr();
        for _ in 0..1000 {
            map.set_slice_hashed_no_ttl(hash_key(b"slice"), b"slice", &[4; 64]);
        }
        assert_eq!(map.len(), 3);
        assert_eq!(map.stored_bytes(), 5 + 16 + 5 + 64 + 5 + 256);
        assert_eq!(
            map.compact_points
                .get(hash_key(b"slice"), b"slice")
                .unwrap()
                .as_ptr(),
            ptr
        );
    }

    #[test]
    fn compact_points_capacity_hint_is_deferred_and_retained_for_general_entries() {
        let mut map = FlatMap::with_capacity(100_000);
        map.set_slice(b"small", b"value", None, 0);
        assert_eq!(map.entries.capacity(), 0);
        map.set_slice(b"large", &[3; 1024], None, 0);
        assert!(map.entries.capacity() >= 100_000);
        assert_eq!(map.compact_points.len(), 1);
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(b"small", 0), Some(b"value".to_vec()));
    }

    #[test]
    fn compact_points_shared_clones_keep_previous_versions() {
        let mut map = FlatMap::new();
        let hash = hash_key(b"key");
        map.set_slice(b"key", b"one", None, 0);
        let old = map.get_value_bytes_hashed(hash, b"key", 0).unwrap();
        assert_eq!(
            map.get_shared_value_bytes_hashed_no_ttl(hash, b"key")
                .unwrap()
                .as_ptr(),
            old.as_ptr()
        );
        map.set_slice(b"key", b"two", None, 0);
        assert_eq!(old.as_ref(), b"one");
        let second = map.get_value_bytes_hashed(hash, b"key", 0).unwrap();
        map.set_slice(b"key", b"longer", None, 0);
        assert_eq!(second.as_ref(), b"two");
        assert_eq!(map.get(b"key", 0), Some(b"longer".to_vec()));
        assert_eq!(map.compact_points.len(), 1);
    }

    #[test]
    fn compact_points_shared_bytes_setter_preserves_supplied_aliases() {
        for replacing_compact in [false, true] {
            for active_readers in [false, true] {
                let mut map = FlatMap::new();
                let hash = hash_key(b"key");
                map.set_slice(b"anchor", b"untouched", None, 0);
                let old = if replacing_compact {
                    map.set_slice(b"key", b"seed", None, 0);
                    map.get_value_bytes_hashed(hash, b"key", 0)
                } else {
                    None
                };
                if active_readers {
                    map.begin_read_epoch();
                }

                let shared = SharedBytes::copy_from_slice(b"one");
                map.set_bytes_hashed(hash, b"key", shared.clone(), None, 0);
                #[cfg(feature = "mutable-value-slices")]
                assert!(map.value_mut_hashed_no_ttl(hash, b"key").is_none());
                assert_eq!(
                    map.get_shared_value_bytes_hashed_no_ttl(hash, b"key")
                        .unwrap()
                        .as_ptr(),
                    shared.as_ptr()
                );
                assert_eq!(shared.as_ref(), b"one");
                assert_eq!(map.get(b"key", 0), Some(b"one".to_vec()));
                if let Some(old) = old {
                    assert_eq!(old.as_ref(), b"seed");
                }
                assert_eq!(map.compact_points.len(), 1);
                assert_eq!(map.entries.len(), 1);
                assert_eq!(map.len(), 2);
                assert_eq!(map.stored_bytes(), 6 + 9 + 3 + 3);

                drop(shared);
                assert!(
                    map.get_shared_value_bytes_hashed_no_ttl(hash, b"key")
                        .unwrap()
                        .is_unique()
                );
                #[cfg(feature = "mutable-value-slices")]
                {
                    map.value_mut_hashed_no_ttl(hash, b"key")
                        .expect("supplied buffer is unique after its alias drops")
                        .copy_from_slice(b"two");
                    assert_eq!(map.get(b"key", 0), Some(b"two".to_vec()));
                }
                assert_eq!(map.get(b"anchor", 0), Some(b"untouched".to_vec()));
                if active_readers {
                    map.end_read_epoch();
                }
                map.process_maintenance(0);
                assert!(map.compact_points.retired.is_empty());
            }
        }
    }

    #[cfg(feature = "mutable-value-slices")]
    #[test]
    fn compact_points_raw_mutation_rejects_owned_aliases_without_changing_layout() {
        for active_readers in [false, true] {
            let mut map = FlatMap::new();
            let key = [b'k'; CompactPointMap::MAX_KEY_BYTES];
            let hash = hash_key(&key);
            let original = [1; CompactPointMap::MAX_VALUE_BYTES];
            map.set_slice(b"anchor", b"untouched", None, 0);
            map.set_slice(&key, &original, None, 0);
            let arena_ptr = map.get_ref(&key, 0).unwrap().as_ptr();
            let alias = map.get_value_bytes_hashed(hash, &key, 0).unwrap();
            let shared_ptr = alias.as_ptr();
            let chunks = allocated_chunks(&map.compact_points);
            let stored_bytes = map.stored_bytes();
            if active_readers {
                map.begin_read_epoch();
            }

            for _ in 0..64 {
                assert!(map.value_mut_hashed_no_ttl(hash, &key).is_none());
            }
            assert_eq!(alias.as_ref(), original);
            assert_eq!(map.get_ref(&key, 0).unwrap().as_ptr(), arena_ptr);
            assert_eq!(map.compact_points.shared_owner_count(), 1);
            assert!(map.compact_points.retired.is_empty());
            assert_eq!(allocated_chunks(&map.compact_points), chunks);
            assert_eq!(map.stored_bytes(), stored_bytes);
            assert_eq!(map.compact_points.len(), 2);
            assert!(map.entries.is_empty());

            drop(alias);
            map.value_mut_hashed_no_ttl(hash, &key)
                .expect("materialized owner is unique after its alias drops")
                .fill(2);
            assert_eq!(map.get(&key, 0), Some(vec![2; original.len()]));
            assert_eq!(map.get(b"anchor", 0), Some(b"untouched".to_vec()));
            assert_eq!(map.compact_points.shared_owner_count(), 0);
            if active_readers {
                assert_eq!(map.compact_points.retired.len(), 1);
                assert!(map.compact_points.retired[0]._shared.is_some());
                // SAFETY: the open epoch retains both the old arena record and
                // its materialized shared owner after the successful mutation.
                unsafe {
                    assert_eq!(
                        std::slice::from_raw_parts(arena_ptr, original.len()),
                        original
                    );
                    assert_eq!(
                        std::slice::from_raw_parts(shared_ptr, original.len()),
                        original
                    );
                }
                map.end_read_epoch();
            }
            map.process_maintenance(0);
            assert!(map.compact_points.retired.is_empty());
            assert_eq!(map.len(), 2);
            assert_eq!(map.stored_bytes(), stored_bytes);
        }
    }

    #[test]
    fn compact_points_churn_reuses_records_chunks_and_descriptors() {
        let mut map = FlatMap::new();
        map.set_slice(b"anchor", b"original", None, 0);
        let ptr = map
            .compact_points
            .get(hash_key(b"anchor"), b"anchor")
            .unwrap()
            .as_ptr();
        for i in 0..10_000 {
            map.set_slice(b"churn", &[i as u8; 16], None, 0);
            map.set_slice(b"churn", &[i as u8; 256], None, 0);
            assert!(map.delete(b"churn", 0));
            assert_eq!(map.len(), 1);
            assert_eq!(map.compact_points.len(), 1);
        }
        assert_eq!(allocated_chunks(&map.compact_points), 1);
        assert!(map.compact_points.chunks.len() <= 3);
        assert!(map.compact_points.entries.capacity() < 16);
        assert_eq!(
            map.compact_points
                .get(hash_key(b"anchor"), b"anchor")
                .unwrap()
                .as_ptr(),
            ptr
        );
        assert_eq!(map.stored_bytes(), 6 + 8);
        assert!(map.delete(b"anchor", 0));
        assert_eq!(allocated_chunks(&map.compact_points), 0);
        assert!(map.is_empty());
        map.set_slice(b"", b"", None, 0);
        assert_eq!(map.get(b"", 0), Some(Vec::new()));
        assert!(map.delete(b"", 0));
    }

    #[test]
    fn compact_points_capacity_falls_back_per_key_and_reuses_freed_records() {
        let mut map = FlatMap::new();
        let count = CompactPointMap::MAX_CHUNKS * (CompactPointMap::CHUNK_BYTES / 264);
        for i in 0..count {
            map.set_slice(&(i as u64).to_le_bytes(), &[1; 256], None, 0);
        }
        assert_eq!(map.compact_points.len(), count);
        assert_eq!(
            allocated_chunks(&map.compact_points),
            CompactPointMap::MAX_CHUNKS
        );
        let anchor = 1u64.to_le_bytes();
        let ptr = map
            .compact_points
            .get(hash_key(&anchor), &anchor)
            .unwrap()
            .as_ptr();
        map.set_slice(b"cap-fallback", &[2; 256], None, 0);
        assert_eq!(map.compact_points.len(), count);
        assert_eq!(map.entries.len(), 1);
        assert_eq!(map.get(b"cap-fallback", 0), Some(vec![2; 256]));
        assert!(map.delete(&0u64.to_le_bytes(), 0));
        map.set_slice(b"replace!", &[3; 256], None, 0);
        assert_eq!(map.compact_points.len(), count);
        assert_eq!(map.entries.len(), 1);
        assert_eq!(
            map.compact_points
                .get(hash_key(&anchor), &anchor)
                .unwrap()
                .as_ptr(),
            ptr
        );
    }

    #[test]
    fn compact_points_read_only_and_failed_operations_preserve_layout() {
        let mut map = FlatMap::new();
        map.set_slice(b"a", b"one", None, 0);
        let ptr = map
            .compact_points
            .get(hash_key(b"a"), b"a")
            .unwrap()
            .as_ptr();
        assert_eq!(map.ttl_seconds(b"a", 0), -1);
        assert_eq!(map.ttl_millis(b"a", 0), -1);
        assert!(map.exists(b"a", 0));
        assert!(!map.exists(b"missing", 0));
        assert_eq!(map.ttl_millis(b"missing", 0), -2);
        assert!(!map.delete(b"missing", 0));
        assert!(!map.persist(b"a", 0));
        assert!(!map.expire(b"missing", 100, 0));
        #[cfg(feature = "redis")]
        assert_eq!(
            map.transform_value_hashed_no_ttl(hash_key(b"a"), b"a", 0, |_| Err::<((), Bytes), _>(
                "rejected"
            )),
            Err("rejected")
        );
        assert_eq!(map.compact_points.len(), 1);
        assert!(map.entries.is_empty());
        assert_eq!(
            map.compact_points
                .get(hash_key(b"a"), b"a")
                .unwrap()
                .as_ptr(),
            ptr
        );
        assert_eq!(map.compact_points.shared_owner_count(), 0);
    }

    #[test]
    fn compact_points_metadata_and_large_values_migrate_only_the_touched_key() {
        let mut map = FlatMap::new();
        for key in [b"a", b"b", b"c", b"d"] {
            map.set_slice(key, b"one", None, 0);
        }
        let ptr = map
            .compact_points
            .get(hash_key(b"a"), b"a")
            .unwrap()
            .as_ptr();
        assert!(map.expire(b"b", 10, 0));
        map.set_bytes_hashed_with_governance(
            hash_key(b"c"),
            b"c",
            SharedBytes::from_static(b"policy value"),
            SharedBytes::from_static(b"policy"),
            None,
            0,
        );
        map.set_slice(b"d", &[7; 1024], None, 0);
        assert_eq!(map.compact_points.len(), 1);
        assert_eq!(map.entries.len(), 3);
        assert_eq!(map.len(), 4);
        assert!(map.is_value_protected_hashed(hash_key(b"c"), b"c", 0));
        assert_eq!(map.get(b"c", 0), None);
        assert_eq!(map.get(b"b", 10), None);
        assert_eq!(
            map.compact_points
                .get(hash_key(b"a"), b"a")
                .unwrap()
                .as_ptr(),
            ptr
        );
        assert_eq!(map.len(), 3);
    }

    #[test]
    fn compact_points_mixed_snapshot_paging_restore_and_limits() {
        let mut map = FlatMap::new();
        for i in 0..12u64 {
            map.set_slice(format!("k:{i:016x}").as_bytes(), &i.to_le_bytes(), None, 0);
        }
        map.set_slice(b"large", &[9; 4096], None, 0);
        map.set_slice(b"ttl", b"ttl", Some(100), 0);
        assert!(map.snapshot_entry_keys(0, None, 1).is_err());
        assert!(
            map.snapshot_entry_keys(0, Some(std::time::Instant::now()), usize::MAX)
                .is_err()
        );
        let keys = map.snapshot_entry_keys(0, None, usize::MAX).unwrap();
        assert_eq!(keys.len(), 14);
        let (page, consumed) = map.snapshot_entry_source_page(&keys, 0, 100, 0).unwrap();
        assert!(!page.is_empty());
        assert!(consumed < 14);
        let snapshot = map.try_snapshot_entries(0).unwrap();
        let mut restored = FlatMap::from_entries(snapshot, 0);
        assert_eq!(restored.len(), 14);
        assert_eq!(restored.compact_points.len(), 12);
        for i in 0..12u64 {
            assert_eq!(
                restored.get(format!("k:{i:016x}").as_bytes(), 0),
                Some(i.to_le_bytes().to_vec())
            );
        }
        assert_eq!(restored.get(b"large", 0), Some(vec![9; 4096]));
        assert_eq!(restored.get(b"ttl", 100), None);
        assert_eq!(map.snapshot_keys(0).len(), 14);
        let mut visited = 0;
        assert!(map.visit_keys(0, &mut |_| {
            visited += 1;
            true
        }));
        assert_eq!(visited, 14);
    }

    #[cfg(feature = "redis")]
    #[test]
    fn compact_points_mixed_scan_resumes_and_honors_filter() {
        let mut map = FlatMap::new();
        for key in [b"a", b"b", b"c"] {
            map.set_slice(key, b"value", None, 0);
        }
        map.set_slice(b"large", &[1; 1024], None, 0);
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
        seen.sort();
        assert_eq!(seen, vec![b"a".to_vec(), b"c".to_vec(), b"large".to_vec()]);
        assert_eq!(map.compact_points.len(), 3);
    }

    #[test]
    fn compact_points_read_epochs_retire_touched_records_and_reclaim_with_fixed_budget() {
        let mut map = FlatMap::new();
        map.set_slice(b"anchor", b"untouched", None, 0);
        map.set_slice(b"key", b"one", None, 0);
        let anchor = map
            .compact_points
            .get(hash_key(b"anchor"), b"anchor")
            .unwrap()
            .as_ptr();
        map.begin_read_epoch();
        let old = map.get_ref(b"key", 0).unwrap().as_ptr();
        map.set_slice(b"key", b"two", None, 0);
        assert!(map.delete(b"key", 0));
        for _ in 0..80 {
            map.set_slice(b"key", b"new", None, 0);
        }
        // SAFETY: the active epoch retains this exact retired payload record.
        assert_eq!(unsafe { std::slice::from_raw_parts(old, 3) }, b"one");
        assert_eq!(
            map.compact_points
                .get(hash_key(b"anchor"), b"anchor")
                .unwrap()
                .as_ptr(),
            anchor
        );
        assert_eq!(map.compact_points.len(), 2);
        assert!(map.entries.is_empty());
        let retired = map.compact_points.retired.len();
        map.end_read_epoch();
        map.process_maintenance(0);
        assert_eq!(
            map.compact_points.retired.len(),
            retired - CompactPointMap::RECLAIM_BUDGET
        );
        while !map.compact_points.retired.is_empty() {
            map.process_maintenance(0);
        }
        assert_eq!(map.get(b"key", 0), Some(b"new".to_vec()));
        assert!(map.delete(b"anchor", 0));
        assert!(map.delete(b"key", 0));
        assert_eq!(allocated_chunks(&map.compact_points), 0);
    }

    #[test]
    fn compact_points_runtime_memory_policy_accounts_for_both_layouts() {
        let mut map = FlatMap::new();
        for i in 0..100 {
            map.set_slice(format!("key{i}").as_bytes(), &[1; 64], None, 0);
        }
        map.set_slice(b"large", &[1; 1024], None, 0);
        map.configure_memory_policy(Some(1024), EvictionPolicy::Lru, 0);
        assert!(map.stored_bytes() <= 1024);
        assert_eq!(map.len(), map.compact_points.len() + map.entries.len());
        assert!(map.evictions() > 0);
        assert!(map.try_snapshot_entries(0).unwrap().len() == map.len());
    }

    #[cfg(feature = "redis")]
    #[test]
    fn compact_points_mutable_updates_preserve_shared_versions_and_epochs() {
        let mut map = FlatMap::new();
        map.set_slice(b"key", b"one", None, 0);
        let clone = map
            .get_value_bytes_hashed(hash_key(b"key"), b"key", 0)
            .unwrap();
        map.begin_read_epoch();
        let ptr = map.get_ref(b"key", 0).unwrap().as_ptr();
        assert_eq!(
            map.update_value_hashed_no_ttl(hash_key(b"key"), b"key", |value| value
                .copy_from_slice(b"two")),
            Some(())
        );
        assert_eq!(clone.as_ref(), b"one");
        // SAFETY: the active read epoch retains the old record.
        assert_eq!(unsafe { std::slice::from_raw_parts(ptr, 3) }, b"one");
        map.end_read_epoch();
        map.process_maintenance(0);
        assert_eq!(map.get(b"key", 0), Some(b"two".to_vec()));
    }

    #[test]
    fn compact_points_read_epochs_retain_materialized_shared_buffers_on_set_and_delete() {
        let mut map = FlatMap::new();
        map.set_slice(b"set", b"old set", None, 0);
        map.set_slice(b"del", b"old del", None, 0);
        map.begin_read_epoch();
        let set_ptr = map
            .get_shared_value_bytes_hashed_no_ttl(hash_key(b"set"), b"set")
            .unwrap()
            .as_ptr();
        let del_ptr = map
            .get_shared_value_bytes_hashed(hash_key(b"del"), b"del", 0)
            .unwrap()
            .as_ptr();
        map.set_slice(b"set", b"new set", None, 0);
        assert!(map.delete(b"del", 0));
        // SAFETY: retired records own both copied shared buffers until this epoch exits.
        assert_eq!(
            unsafe { std::slice::from_raw_parts(set_ptr, 7) },
            b"old set"
        );
        assert_eq!(
            unsafe { std::slice::from_raw_parts(del_ptr, 7) },
            b"old del"
        );
        assert_eq!(
            map.compact_points
                .retired
                .iter()
                .filter(|record| record._shared.is_some())
                .count(),
            2
        );
        map.end_read_epoch();
        map.process_maintenance(0);
        assert!(map.compact_points.retired.is_empty());
        assert_eq!(map.get(b"set", 0), Some(b"new set".to_vec()));
    }

    #[test]
    fn compact_points_runtime_policy_samples_and_ranks_hot_entries_in_both_layouts() {
        for policy in [EvictionPolicy::Lru, EvictionPolicy::Lfu] {
            let mut map = FlatMap::new();
            map.set_slice(b"compact-hot", &[1; 64], None, 0);
            map.set_slice(b"general-hot", &[2; 1024], None, 0);
            for i in 0..20 {
                map.set_slice(format!("cold-{i}").as_bytes(), &[3; 64], None, 0);
            }
            map.configure_memory_policy(Some(map.stored_bytes()), policy, 0);
            for _ in 0..2048 {
                assert!(map.get_ref(b"compact-hot", 0).is_some());
            }
            for _ in 0..2048 {
                assert!(map.get_ref(b"general-hot", 0).is_some());
            }
            let (_, _, compact_access) = map
                .compact_points
                .eviction_entries()
                .find(|(_, key, _)| *key == b"compact-hot")
                .unwrap();
            let general_access = map
                .entries
                .find(local_table_hash(hash_key(b"general-hot")), |entry| {
                    entry.matches(hash_key(b"general-hot"), b"general-hot")
                })
                .unwrap()
                .access;
            assert_eq!(compact_access.frequency, 3);
            assert_eq!(general_access.frequency, 3);
            assert!(compact_access.last_touch > 0);
            while map.len() > 2 {
                assert!(map.evict_with_policy(policy, 0));
            }
            assert!(map.get_ref(b"compact-hot", 0).is_some());
            assert!(map.get_ref(b"general-hot", 0).is_some());
            assert_eq!(map.compact_points.access.len(), 1);
            map.configure_memory_policy(None, EvictionPolicy::None, 0);
            map.set_slice(b"compact-hot", &[4; 256], None, 0);
            assert_eq!(map.compact_points.access.len(), 1);
            assert!(map.expire(b"compact-hot", 100, 0));
            assert!(map.compact_points.access.is_empty());
            assert!(map.delete(b"compact-hot", 0));
            map.set_slice(b"replacement", b"new", None, 0);
            assert!(map.compact_points.access.is_empty());
        }
    }

    #[cfg(all(feature = "embedded", feature = "unsafe"))]
    #[test]
    fn compact_points_local_metadata_migration_detaches_existing_owned_clones() {
        let mut map = FlatMap::new();
        let hash = hash_key(b"key");
        map.set_slice(b"key", b"one", None, 0);
        let clone = map.get_value_bytes_hashed(hash, b"key", 0).unwrap();
        map.set_slice_hashed_tagged_local(
            hash,
            hash_key_tag_from_hash(hash),
            b"key",
            b"two",
            Some(100),
            0,
        );
        assert_eq!(clone.as_ref(), b"one");
        assert_eq!(map.get(b"key", 0), Some(b"two".to_vec()));
        assert_eq!(map.compact_points.len(), 0);
        assert_eq!(map.entries.len(), 1);
    }

    #[cfg(feature = "kv-overflow")]
    #[test]
    fn compact_points_overflow_generation_attaches_to_one_general_key() {
        let mut map = FlatMap::new();
        map.set_slice(b"anchor", b"untouched", None, 0);
        map.set_bytes_hashed_overflow(
            hash_key(b"key"),
            b"key",
            SharedBytes::from_static(b"small"),
            None,
            None,
            0,
            7,
        );
        assert!(map.overflow_generation_matches(hash_key(b"key"), b"key", 7));
        assert_eq!(map.compact_points.len(), 1);
        assert_eq!(map.entries.len(), 1);
        assert_eq!(map.len(), 2);
    }
}
