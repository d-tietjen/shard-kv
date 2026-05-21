use super::helpers::{normalize_range, sort_small_zset};
use super::*;

impl ZSetObject {
    pub(super) fn single(member: Bytes, score: f64) -> Self {
        let mut entries = SmallZSetEntries::new();
        entries.push((member, score));
        Self::Small(entries)
    }

    pub(super) fn map_with_capacity(capacity: usize) -> Self {
        let mut scores = FastHashMap::default();
        scores.reserve(capacity);
        Self::Map {
            scores,
            ordered: IndexTreeSet::new(),
        }
    }

    pub(super) fn insert_slice(&mut self, member: &[u8], score: f64) -> bool {
        match self {
            Self::Small(entries) => {
                if let [(existing, existing_score)] = entries.as_mut_slice()
                    && existing.as_slice() == member
                {
                    *existing_score = score;
                    return false;
                }
                if let Some((_, existing_score)) = entries
                    .iter_mut()
                    .find(|(existing, _)| existing.as_slice() == member)
                {
                    *existing_score = score;
                    sort_small_zset(entries);
                    return false;
                }
                if entries.len() < SMALL_ZSET_INLINE {
                    entries.push((member.to_vec(), score));
                    sort_small_zset(entries);
                    return true;
                }

                let capacity = entries.len() + 1;
                let old_entries = std::mem::take(entries);
                *self = Self::map_with_capacity(capacity);
                let Self::Map { scores, ordered } = self else {
                    unreachable!("zset promotion did not create map state");
                };
                for (old_member, old_score) in old_entries {
                    scores.insert(old_member.clone(), old_score);
                    ordered.insert(ZSetOrderKey {
                        score: old_score,
                        member: old_member,
                    });
                }
                let member = member.to_vec();
                scores.insert(member.clone(), score);
                ordered.insert(ZSetOrderKey { score, member });
                true
            }
            Self::Map { scores, ordered } => {
                if let Some((stored_member, old_score)) = scores.get_key_value(member) {
                    let old_score = *old_score;
                    if !old_score.total_cmp(&score).is_eq() {
                        let stored_member = stored_member.clone();
                        ordered.remove(&ZSetOrderKey {
                            score: old_score,
                            member: stored_member.clone(),
                        });
                        *scores
                            .get_mut(member)
                            .expect("zset score disappeared during update") = score;
                        ordered.insert(ZSetOrderKey {
                            score,
                            member: stored_member,
                        });
                    }
                    false
                } else {
                    let member = member.to_vec();
                    scores.insert(member.clone(), score);
                    ordered.insert(ZSetOrderKey { score, member });
                    true
                }
            }
        }
    }

    pub(super) fn remove(&mut self, member: &[u8]) -> bool {
        match self {
            Self::Small(entries) => {
                let Some(index) = entries
                    .iter()
                    .position(|(existing, _)| existing.as_slice() == member)
                else {
                    return false;
                };
                entries.remove(index);
                true
            }
            Self::Map { scores, ordered } => {
                let Some((member, score)) = scores.remove_entry(member) else {
                    return false;
                };
                ordered.remove(&ZSetOrderKey { score, member });
                true
            }
        }
    }

    pub(super) fn score(&self, member: &[u8]) -> Option<f64> {
        match self {
            Self::Small(entries) => entries
                .iter()
                .find_map(|(existing, score)| (existing.as_slice() == member).then_some(*score)),
            Self::Map { scores, .. } => scores.get(member).copied(),
        }
    }

    pub(super) fn len(&self) -> usize {
        match self {
            Self::Small(entries) => entries.len(),
            Self::Map { scores, .. } => scores.len(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        match self {
            Self::Small(entries) => entries.is_empty(),
            Self::Map { scores, .. } => scores.is_empty(),
        }
    }

    pub(super) fn pop_extreme(&mut self, count: usize, max: bool) -> Vec<(Bytes, f64)> {
        match self {
            Self::Small(entries) => {
                let take = count.min(entries.len());
                let mut popped = Vec::with_capacity(take);
                for _ in 0..take {
                    let index = if max { entries.len() - 1 } else { 0 };
                    popped.push(entries.remove(index));
                }
                popped
            }
            Self::Map { scores, ordered } => {
                let take = count.min(scores.len());
                let mut popped = Vec::with_capacity(take);
                for _ in 0..take {
                    let Some(item) = extreme_ordered_item(ordered, max) else {
                        break;
                    };
                    ordered.remove(&item);
                    scores.remove(&item.member);
                    popped.push((item.member, item.score));
                }
                popped
            }
        }
    }

    pub(super) fn count_by_score(
        &self,
        min: f64,
        min_inclusive: bool,
        max: f64,
        max_inclusive: bool,
    ) -> usize {
        match self {
            Self::Small(entries) => {
                if range_covers_sorted_scores(
                    entries.first().map(|(_, score)| *score),
                    entries.last().map(|(_, score)| *score),
                    entries.len(),
                    min,
                    min_inclusive,
                    max,
                    max_inclusive,
                ) {
                    return entries.len();
                }
                entries
                    .iter()
                    .filter(|(_, score)| {
                        score_matches_lower(*score, min, min_inclusive)
                            && score_matches_upper(*score, max, max_inclusive)
                    })
                    .count()
            }
            Self::Map { scores, ordered } => {
                if scores.is_empty() {
                    return 0;
                }
                if range_covers_sorted_scores(
                    ordered.get_first().map(|item| item.score),
                    ordered.get_last().map(|item| item.score),
                    scores.len(),
                    min,
                    min_inclusive,
                    max,
                    max_inclusive,
                ) {
                    return scores.len();
                }
                let start = zset_partition_point(ordered, |item| match min_inclusive {
                    true => item.score < min,
                    false => item.score <= min,
                });
                let end = zset_partition_point(ordered, |item| match max_inclusive {
                    true => item.score <= max,
                    false => item.score < max,
                });
                end.saturating_sub(start)
            }
        }
    }

    pub(super) fn rank(&self, member: &[u8], rev: bool) -> Option<usize> {
        match self {
            Self::Small(entries) => {
                if rev {
                    entries
                        .iter()
                        .rev()
                        .position(|(existing, _)| existing.as_slice() == member)
                } else {
                    entries
                        .iter()
                        .position(|(existing, _)| existing.as_slice() == member)
                }
            }
            Self::Map { scores, ordered } => {
                let target_score = *scores.get(member)?;
                let rank = ordered.get_index_from_key(&ZSetOrderKey {
                    score: target_score,
                    member: member.to_vec(),
                })?;
                match rev {
                    true => Some(ordered.len() - rank - 1),
                    false => Some(rank),
                }
            }
        }
    }

    pub(super) fn range(&self, start: i64, stop: i64) -> Vec<Option<Bytes>> {
        match self {
            Self::Small(entries) => {
                let Some((start, stop)) = normalize_range(start, stop, entries.len()) else {
                    return Vec::new();
                };
                entries
                    .iter()
                    .skip(start)
                    .take(stop - start + 1)
                    .map(|(member, _)| Some(member.clone()))
                    .collect()
            }
            Self::Map { ordered, .. } => {
                let Some((start, stop)) = normalize_range(start, stop, ordered.len()) else {
                    return Vec::new();
                };
                let count = stop - start + 1;
                if start > count {
                    return (start..=stop)
                        .filter_map(|index| ordered.get_key_from_index(index))
                        .map(|item| Some(item.member.clone()))
                        .collect();
                }
                ordered
                    .iter()
                    .skip(start)
                    .take(count)
                    .map(|item| Some(item.member.clone()))
                    .collect()
            }
        }
    }

    pub(super) fn range_visit(
        &self,
        start: i64,
        stop: i64,
        mut emit: impl FnMut(RedisObjectArrayItem<'_>),
    ) {
        match self {
            Self::Small(entries) => {
                let Some((start, stop)) = normalize_range(start, stop, entries.len()) else {
                    emit(RedisObjectArrayItem::Begin(0));
                    return;
                };
                let count = stop - start + 1;
                emit(RedisObjectArrayItem::Begin(count));
                for (member, _) in entries.iter().skip(start).take(count) {
                    emit(RedisObjectArrayItem::Bulk(Some(member)));
                }
            }
            Self::Map { ordered, .. } => {
                let Some((start, stop)) = normalize_range(start, stop, ordered.len()) else {
                    emit(RedisObjectArrayItem::Begin(0));
                    return;
                };
                let count = stop - start + 1;
                emit(RedisObjectArrayItem::Begin(count));
                if start > count {
                    for index in start..=stop {
                        if let Some(item) = ordered.get_key_from_index(index) {
                            emit(RedisObjectArrayItem::Bulk(Some(&item.member)));
                        }
                    }
                    return;
                }
                for item in ordered.iter().skip(start).take(count) {
                    emit(RedisObjectArrayItem::Bulk(Some(&item.member)));
                }
            }
        }
    }

    pub(super) fn range_entries_visit(
        &self,
        start: i64,
        stop: i64,
        rev: bool,
        mut emit: impl FnMut(RedisObjectZSetRangeItem<'_>),
    ) {
        match self {
            Self::Small(entries) => {
                let Some((start, stop)) = normalize_range(start, stop, entries.len()) else {
                    emit(RedisObjectZSetRangeItem::Begin(0));
                    return;
                };
                let count = stop - start + 1;
                emit(RedisObjectZSetRangeItem::Begin(count));
                if rev {
                    for (member, score) in entries.iter().rev().skip(start).take(count) {
                        emit(RedisObjectZSetRangeItem::Entry {
                            member: member.as_slice(),
                            score: *score,
                        });
                    }
                } else {
                    for (member, score) in entries.iter().skip(start).take(count) {
                        emit(RedisObjectZSetRangeItem::Entry {
                            member: member.as_slice(),
                            score: *score,
                        });
                    }
                }
            }
            Self::Map { ordered, .. } => {
                let Some((start, stop)) = normalize_range(start, stop, ordered.len()) else {
                    emit(RedisObjectZSetRangeItem::Begin(0));
                    return;
                };
                let count = stop - start + 1;
                emit(RedisObjectZSetRangeItem::Begin(count));
                match (rev, start > count) {
                    (true, _) => {
                        let first = ordered.len() - 1 - stop;
                        let last = ordered.len() - 1 - start;
                        for index in (first..=last).rev() {
                            if let Some(item) = ordered.get_key_from_index(index) {
                                emit(RedisObjectZSetRangeItem::Entry {
                                    member: item.member.as_slice(),
                                    score: item.score,
                                });
                            }
                        }
                    }
                    (false, true) => {
                        for index in start..=stop {
                            if let Some(item) = ordered.get_key_from_index(index) {
                                emit(RedisObjectZSetRangeItem::Entry {
                                    member: item.member.as_slice(),
                                    score: item.score,
                                });
                            }
                        }
                    }
                    (false, false) => {
                        for item in ordered.iter().skip(start).take(count) {
                            emit(RedisObjectZSetRangeItem::Entry {
                                member: item.member.as_slice(),
                                score: item.score,
                            });
                        }
                    }
                }
            }
        }
    }

    pub(super) fn entries(&self) -> Vec<(Bytes, f64)> {
        match self {
            Self::Small(entries) => entries.clone().into_vec(),
            Self::Map { ordered, .. } => ordered
                .iter()
                .map(|item| (item.member.clone(), item.score))
                .collect(),
        }
    }
}

#[inline(always)]
fn range_covers_sorted_scores(
    first: Option<f64>,
    last: Option<f64>,
    len: usize,
    min: f64,
    min_inclusive: bool,
    max: f64,
    max_inclusive: bool,
) -> bool {
    if len == 0 {
        return true;
    }
    let Some(first) = first else {
        return false;
    };
    let Some(last) = last else {
        return false;
    };
    score_matches_lower(first, min, min_inclusive) && score_matches_upper(last, max, max_inclusive)
}

#[inline(always)]
fn extreme_ordered_item(ordered: &IndexTreeSet<ZSetOrderKey>, max: bool) -> Option<ZSetOrderKey> {
    match max {
        true => ordered.get_last(),
        false => ordered.get_first(),
    }
    .cloned()
}

#[inline(always)]
fn score_matches_lower(score: f64, min: f64, inclusive: bool) -> bool {
    match inclusive {
        true => score >= min,
        false => score > min,
    }
}

#[inline(always)]
fn score_matches_upper(score: f64, max: f64, inclusive: bool) -> bool {
    match inclusive {
        true => score <= max,
        false => score < max,
    }
}

#[inline(always)]
fn zset_partition_point(
    ordered: &IndexTreeSet<ZSetOrderKey>,
    mut pred: impl FnMut(&ZSetOrderKey) -> bool,
) -> usize {
    let mut low = 0usize;
    let mut high = ordered.len();
    while low < high {
        let mid = low + (high - low) / 2;
        let item = ordered
            .get_key_from_index(mid)
            .expect("zset rank index must exist");
        if pred(item) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low
}
