use super::helpers::{normalize_index, normalize_range};
use super::*;

impl RedisObjectBucket {
    pub(crate) fn list_presence_hashed(&self, key_hash: u64, key: &[u8]) -> RedisObjectReadOutcome {
        match (
            self.lists.get_hashed(key_hash, key).is_some(),
            self.has_non_list_hashed(key_hash, key),
        ) {
            (true, _) => RedisObjectReadOutcome::Written,
            (false, true) => RedisObjectReadOutcome::WrongType,
            (false, false) => RedisObjectReadOutcome::Missing,
        }
    }

    #[inline(always)]
    pub(crate) fn push_list_existing_or_wrongtype_hashed(
        &mut self,
        key_hash: u64,
        key: &[u8],
        values: &[&[u8]],
        front: bool,
    ) -> RedisObjectWriteAttempt {
        if values.is_empty() {
            return RedisObjectWriteAttempt::Complete(RedisObjectResult::Integer(0));
        }
        if let Some(slot) = self.lists.get_hashed(key_hash, key) {
            let list = self
                .list_slab
                .get_mut(slot)
                .expect("list slab slot missing");
            list.push_values(values, front);
            return RedisObjectWriteAttempt::Complete(
                RedisObjectResult::Integer(list.len() as i64),
            );
        }
        if self.has_non_list(key) {
            RedisObjectWriteAttempt::Complete(RedisObjectResult::WrongType)
        } else {
            RedisObjectWriteAttempt::Missing
        }
    }

    #[inline(always)]
    pub(crate) fn push_list_new_unchecked_hashed(
        &mut self,
        key_hash: u64,
        key: &[u8],
        values: &[&[u8]],
        front: bool,
    ) -> RedisObjectResult {
        let list = ListObject::from_values(values, front);
        let slot = self.list_slab.insert(list);
        self.lists.insert_hashed(key_hash, key.to_vec(), slot);
        RedisObjectResult::Integer(values.len() as i64)
    }

    pub(crate) fn push_list_existing_hashed(
        &mut self,
        key_hash: u64,
        key: &[u8],
        values: &[&[u8]],
        front: bool,
    ) -> (RedisObjectResult, bool) {
        if values.is_empty() {
            return (RedisObjectResult::Integer(0), false);
        }
        if let Some(slot) = self.lists.get_hashed(key_hash, key) {
            let list = self
                .list_slab
                .get_mut(slot)
                .expect("list slab slot missing");
            list.push_values(values, front);
            return (RedisObjectResult::Integer(list.len() as i64), false);
        }
        if self.has_non_list_hashed(key_hash, key) {
            (RedisObjectResult::WrongType, false)
        } else {
            (RedisObjectResult::Integer(0), false)
        }
    }

    pub(crate) fn pop_list(&mut self, key: &[u8], front: bool) -> (RedisObjectResult, bool) {
        if let Some(slot) = self.lists.get(key).copied() {
            let list = self
                .list_slab
                .get_mut(slot)
                .expect("list slab slot missing");
            let value = if front {
                list.pop_front()
            } else {
                list.pop_back()
            };
            let empty = list.is_empty();
            if empty {
                self.lists.remove(key);
                self.list_slab.remove(slot);
                self.expire_at_ms.remove(key);
            }
            return (RedisObjectResult::Bulk(value), empty);
        }
        if self.has_non_list(key) {
            (RedisObjectResult::WrongType, false)
        } else {
            (RedisObjectResult::Bulk(None), false)
        }
    }

    pub(crate) fn pop_list_count(
        &mut self,
        key: &[u8],
        count: usize,
        front: bool,
    ) -> (RedisObjectResult, bool) {
        if count == 0 {
            return (RedisObjectResult::Array(Vec::new()), false);
        }
        if let Some(slot) = self.lists.get(key).copied() {
            let list = self
                .list_slab
                .get_mut(slot)
                .expect("list slab slot missing");
            let mut values = Vec::with_capacity(count.min(list.len()));
            for _ in 0..count {
                let value = if front {
                    list.pop_front()
                } else {
                    list.pop_back()
                };
                let Some(value) = value else { break };
                values.push(Some(value));
            }
            let empty = list.is_empty();
            if empty {
                self.lists.remove(key);
                self.list_slab.remove(slot);
                self.expire_at_ms.remove(key);
            }
            return (RedisObjectResult::Array(values), empty);
        }
        if self.has_non_list(key) {
            (RedisObjectResult::WrongType, false)
        } else {
            (RedisObjectResult::Bulk(None), false)
        }
    }

    pub(crate) fn llen(&self, key: &[u8]) -> RedisObjectResult {
        match self.lists.get(key).copied() {
            Some(slot) => RedisObjectResult::Integer(
                self.list_slab
                    .get(slot)
                    .expect("list slab slot missing")
                    .len() as i64,
            ),
            None if self.has_non_list(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Integer(0),
        }
    }

    pub(crate) fn llen_visit(&self, key: &[u8], write: impl FnOnce(i64)) -> RedisObjectReadOutcome {
        match self.lists.get(key).copied() {
            Some(slot) => {
                write(
                    self.list_slab
                        .get(slot)
                        .expect("list slab slot missing")
                        .len() as i64,
                );
                RedisObjectReadOutcome::Written
            }
            None if self.has_non_list(key) => RedisObjectReadOutcome::WrongType,
            None => RedisObjectReadOutcome::Missing,
        }
    }

    pub(crate) fn lpos_visit(
        &self,
        key: &[u8],
        element: &[u8],
        rank: i64,
        count: Option<i64>,
        maxlen: i64,
        write: impl FnOnce(Vec<i64>),
    ) -> RedisObjectReadOutcome {
        match self.lists.get(key).copied() {
            Some(slot) => {
                let list = self.list_slab.get(slot).expect("list slab slot missing");
                write(lpos_scan(list, element, rank, count, maxlen));
                RedisObjectReadOutcome::Written
            }
            None if self.has_non_list(key) => RedisObjectReadOutcome::WrongType,
            None => RedisObjectReadOutcome::Missing,
        }
    }

    pub(crate) fn lindex(&self, key: &[u8], index: i64) -> RedisObjectResult {
        match self.lists.get(key).copied() {
            Some(slot) => {
                let list = self.list_slab.get(slot).expect("list slab slot missing");
                let Some(index) = normalize_index(index, list.len()) else {
                    return RedisObjectResult::Bulk(None);
                };
                RedisObjectResult::Bulk(list.get(index).cloned())
            }
            None if self.has_non_list(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Bulk(None),
        }
    }

    pub(crate) fn lindex_visit(
        &self,
        key: &[u8],
        index: i64,
        write: impl FnOnce(Option<&[u8]>),
    ) -> RedisObjectReadOutcome {
        match self.lists.get(key).copied() {
            Some(slot) => {
                let list = self.list_slab.get(slot).expect("list slab slot missing");
                let value = normalize_index(index, list.len())
                    .and_then(|index| list.get(index))
                    .map(Vec::as_slice);
                write(value);
                RedisObjectReadOutcome::Written
            }
            None if self.has_non_list(key) => RedisObjectReadOutcome::WrongType,
            None => RedisObjectReadOutcome::Missing,
        }
    }

    pub(crate) fn lrange(&self, key: &[u8], start: i64, stop: i64) -> RedisObjectResult {
        match self.lists.get(key).copied() {
            Some(slot) => {
                let list = self.list_slab.get(slot).expect("list slab slot missing");
                let Some((start, stop)) = normalize_range(start, stop, list.len()) else {
                    return RedisObjectResult::Array(Vec::new());
                };
                RedisObjectResult::Array(
                    list.iter()
                        .skip(start)
                        .take(stop - start + 1)
                        .cloned()
                        .map(Some)
                        .collect(),
                )
            }
            None if self.has_non_list(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Array(Vec::new()),
        }
    }

    pub(crate) fn lset(
        &mut self,
        key: &[u8],
        index: i64,
        value: &[u8],
    ) -> (RedisObjectResult, bool) {
        if let Some(slot) = self.lists.get(key).copied() {
            let list = self
                .list_slab
                .get_mut(slot)
                .expect("list slab slot missing");
            let Some(index) = normalize_index(index, list.len()) else {
                return (RedisObjectResult::Simple("ERR index out of range"), false);
            };
            list.set(index, value.to_vec());
            return (RedisObjectResult::Simple("OK"), false);
        }
        if self.has_non_list(key) {
            (RedisObjectResult::WrongType, false)
        } else {
            (RedisObjectResult::Simple("ERR no such key"), false)
        }
    }

    pub(crate) fn lrem(
        &mut self,
        key: &[u8],
        count: i64,
        value: &[u8],
    ) -> (RedisObjectResult, bool) {
        if let Some(slot) = self.lists.get(key).copied() {
            let list = self
                .list_slab
                .get_mut(slot)
                .expect("list slab slot missing");
            let mut removed = 0usize;
            if count >= 0 {
                let limit = if count == 0 {
                    usize::MAX
                } else {
                    count as usize
                };
                let mut index = 0usize;
                while index < list.len() && removed < limit {
                    if list.get(index).is_some_and(|item| item.as_slice() == value) {
                        list.remove(index);
                        removed += 1;
                    } else {
                        index += 1;
                    }
                }
            } else {
                let limit = count.unsigned_abs() as usize;
                let mut index = list.len();
                while index > 0 && removed < limit {
                    index -= 1;
                    if list.get(index).is_some_and(|item| item.as_slice() == value) {
                        list.remove(index);
                        removed += 1;
                    }
                }
            }
            let empty = list.is_empty();
            if empty {
                self.lists.remove(key);
                self.list_slab.remove(slot);
                self.expire_at_ms.remove(key);
            }
            return (RedisObjectResult::Integer(removed as i64), empty);
        }
        if self.has_non_list(key) {
            (RedisObjectResult::WrongType, false)
        } else {
            (RedisObjectResult::Integer(0), false)
        }
    }

    pub(crate) fn ltrim(&mut self, key: &[u8], start: i64, stop: i64) -> (RedisObjectResult, bool) {
        if let Some(slot) = self.lists.get(key).copied() {
            let list = self
                .list_slab
                .get_mut(slot)
                .expect("list slab slot missing");
            let Some((start, stop)) = normalize_range(start, stop, list.len()) else {
                list.clear();
                self.lists.remove(key);
                self.list_slab.remove(slot);
                self.expire_at_ms.remove(key);
                return (RedisObjectResult::Simple("OK"), true);
            };
            let keep = stop - start + 1;
            for _ in 0..start {
                list.pop_front();
            }
            while list.len() > keep {
                list.pop_back();
            }
            let empty = list.is_empty();
            if empty {
                self.lists.remove(key);
                self.list_slab.remove(slot);
                self.expire_at_ms.remove(key);
            }
            return (RedisObjectResult::Simple("OK"), empty);
        }
        if self.has_non_list(key) {
            (RedisObjectResult::WrongType, false)
        } else {
            (RedisObjectResult::Simple("OK"), false)
        }
    }

    pub(crate) fn linsert(
        &mut self,
        key: &[u8],
        before: bool,
        pivot: &[u8],
        value: &[u8],
    ) -> (RedisObjectResult, bool) {
        if let Some(slot) = self.lists.get(key).copied() {
            let list = self
                .list_slab
                .get_mut(slot)
                .expect("list slab slot missing");
            let Some(index) = list.iter().position(|item| item.as_slice() == pivot) else {
                return (RedisObjectResult::Integer(-1), false);
            };
            let index = if before { index } else { index + 1 };
            list.insert(index, value.to_vec());
            return (RedisObjectResult::Integer(list.len() as i64), false);
        }
        if self.has_non_list(key) {
            (RedisObjectResult::WrongType, false)
        } else {
            (RedisObjectResult::Integer(0), false)
        }
    }

    pub(crate) fn lrange_visit(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
        mut emit: impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome {
        match self.lists.get(key).copied() {
            Some(slot) => {
                let list = self.list_slab.get(slot).expect("list slab slot missing");
                let Some((start, stop)) = normalize_range(start, stop, list.len()) else {
                    emit(RedisObjectArrayItem::Begin(0));
                    return RedisObjectReadOutcome::Written;
                };
                let count = stop - start + 1;
                emit(RedisObjectArrayItem::Begin(count));
                for value in list.iter().skip(start).take(count) {
                    emit(RedisObjectArrayItem::Bulk(Some(value.as_slice())));
                }
                RedisObjectReadOutcome::Written
            }
            None if self.has_non_list(key) => RedisObjectReadOutcome::WrongType,
            None => RedisObjectReadOutcome::Missing,
        }
    }
}

/// In-place LPOS scan over the stored list. `rank` is non-zero (validated by the
/// caller); positive scans head->tail, negative scans tail->head. `count` follows
/// Redis semantics: `None` returns the first match, `Some(0)` returns all matches,
/// `Some(n)` caps at `n`. `maxlen` of 0 means unlimited; otherwise it bounds how
/// many elements are compared from the scan's starting side.
fn lpos_scan(
    list: &ListObject,
    element: &[u8],
    rank: i64,
    count: Option<i64>,
    maxlen: i64,
) -> Vec<i64> {
    let len = list.len();
    let mut results = Vec::new();
    if len == 0 {
        return results;
    }

    let mut skip = (rank.unsigned_abs() - 1) as usize;
    let wanted = count.map(|value| value as usize);
    let compare_limit = maxlen as usize;

    let collect = |position: i64, skip: &mut usize, results: &mut Vec<i64>| -> bool {
        if *skip > 0 {
            *skip -= 1;
            return false;
        }
        results.push(position);
        match wanted {
            None => true,
            Some(0) => false,
            Some(limit) => results.len() >= limit,
        }
    };

    if rank > 0 {
        // Forward scan; the iterator is forward-only so this streams with early exit.
        for (index, item) in list.iter().enumerate() {
            if compare_limit != 0 && index >= compare_limit {
                break;
            }
            if item.as_slice() == element && collect(index as i64, &mut skip, &mut results) {
                break;
            }
        }
    } else {
        // Backward scan; the compared window is the last `compare_limit` elements.
        // ListObject only iterates forward, so collect matches in the window during a
        // single forward pass, then walk them tail-first.
        let window_start = if compare_limit == 0 {
            0
        } else {
            len.saturating_sub(compare_limit)
        };
        let mut matched = Vec::new();
        for (index, item) in list.iter().enumerate() {
            if index >= window_start && item.as_slice() == element {
                matched.push(index as i64);
            }
        }
        for &position in matched.iter().rev() {
            if collect(position, &mut skip, &mut results) {
                break;
            }
        }
    }

    results
}
