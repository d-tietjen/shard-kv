#![allow(dead_code, unused_imports)]

use super::super::*;

use super::types::*;

#[cfg(feature = "redis-module-topk")]
#[derive(Debug, Clone)]
pub(super) struct TopKSketch {
    k: usize,
    width: usize,
    depth: usize,
    decay: f64,
    next_sequence: u64,
    counts: FastHashMap<Bytes, i64>,
    first_seen: FastHashMap<Bytes, u64>,
}

#[cfg(feature = "redis-module-topk")]
impl TopKSketch {
    pub(super) fn new(k: usize, width: usize, depth: usize, decay: f64) -> Self {
        Self {
            k,
            width,
            depth,
            decay,
            next_sequence: 0,
            counts: FastHashMap::default(),
            first_seen: FastHashMap::default(),
        }
    }

    pub(super) fn increment(&mut self, item: &[u8], increment: i64) -> Option<Bytes> {
        let before = self.top_items();
        let count = self.counts.entry(item.to_vec()).or_insert(0);
        *count = count.saturating_add(increment);
        self.first_seen.entry(item.to_vec()).or_insert_with(|| {
            let sequence = self.next_sequence;
            self.next_sequence = self.next_sequence.saturating_add(1);
            sequence
        });
        let after = self.top_items();
        let entered = !before.iter().any(|entry| entry.as_slice() == item)
            && after.iter().any(|entry| entry.as_slice() == item);
        if entered {
            before
                .into_iter()
                .find(|old| !after.iter().any(|entry| entry == old))
        } else {
            None
        }
    }

    pub(super) fn contains_top(&self, item: &[u8]) -> bool {
        self.top_items()
            .iter()
            .any(|entry| entry.as_slice() == item)
    }

    pub(super) fn count(&self, item: &[u8]) -> i64 {
        self.counts.get(item).copied().unwrap_or(0)
    }

    fn top_items(&self) -> Vec<Bytes> {
        self.top_entries()
            .into_iter()
            .map(|(item, _)| item)
            .collect()
    }

    pub(super) fn top_entries(&self) -> Vec<(Bytes, i64)> {
        let mut entries = self
            .counts
            .iter()
            .map(|(item, count)| (item.clone(), *count))
            .collect::<Vec<_>>();
        entries.sort_by(|(left_item, left_count), (right_item, right_count)| {
            match right_count.cmp(left_count) {
                Ordering::Equal => {
                    let left_seen = self.first_seen.get(left_item).copied().unwrap_or(u64::MAX);
                    let right_seen = self.first_seen.get(right_item).copied().unwrap_or(u64::MAX);
                    left_seen
                        .cmp(&right_seen)
                        .then_with(|| left_item.cmp(right_item))
                }
                order => order,
            }
        });
        entries.truncate(self.k);
        entries
    }

    pub(super) fn info(&self) -> TopKInfo {
        TopKInfo {
            k: self.k,
            width: self.width,
            depth: self.depth,
            decay: self.decay,
        }
    }
}
