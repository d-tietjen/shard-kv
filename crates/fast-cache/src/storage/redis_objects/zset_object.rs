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
            ordered: BTreeSet::new(),
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
                ordered
                    .iter()
                    .skip(start)
                    .take(stop - start + 1)
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
                for item in ordered.iter().skip(start).take(count) {
                    emit(RedisObjectArrayItem::Bulk(Some(&item.member)));
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
