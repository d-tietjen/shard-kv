use super::*;

impl SetObject {
    pub(super) fn empty() -> Self {
        Self::Small(SmallSetEntries::new())
    }

    pub(super) fn map_with_capacity(capacity: usize) -> Self {
        let mut set = FastHashSet::default();
        set.reserve(capacity);
        Self::Map(set)
    }

    pub(super) fn insert_many(&mut self, members: &[&[u8]]) -> usize {
        let mut inserted = 0usize;
        for member in members {
            if self.insert_slice(member) {
                inserted += 1;
            }
        }
        inserted
    }

    pub(super) fn insert_slice(&mut self, member: &[u8]) -> bool {
        match self {
            Self::Small(entries) => {
                if entries
                    .first()
                    .is_some_and(|existing| existing.as_slice() == member)
                {
                    return false;
                }
                if entries.iter().any(|existing| existing.as_slice() == member) {
                    return false;
                }
                if entries.len() < SMALL_SET_INLINE {
                    entries.push(member.to_vec());
                    return true;
                }

                let capacity = entries.len() + 1;
                let old_entries = std::mem::take(entries);
                *self = Self::map_with_capacity(capacity);
                let Self::Map(set) = self else {
                    unreachable!("set promotion did not create map state");
                };
                for old_member in old_entries {
                    set.insert(old_member);
                }
                set.insert(member.to_vec())
            }
            Self::Map(set) => set.insert(member.to_vec()),
        }
    }

    pub(super) fn remove(&mut self, member: &[u8]) -> bool {
        match self {
            Self::Small(entries) => {
                let Some(index) = entries
                    .iter()
                    .position(|existing| existing.as_slice() == member)
                else {
                    return false;
                };
                entries.remove(index);
                true
            }
            Self::Map(set) => set.remove(member),
        }
    }

    pub(super) fn contains(&self, member: &[u8]) -> bool {
        match self {
            Self::Small(entries) => entries.iter().any(|existing| existing.as_slice() == member),
            Self::Map(set) => set.contains(member),
        }
    }

    pub(super) fn len(&self) -> usize {
        match self {
            Self::Small(entries) => entries.len(),
            Self::Map(set) => set.len(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        match self {
            Self::Small(entries) => entries.is_empty(),
            Self::Map(set) => set.is_empty(),
        }
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &Bytes> {
        enum SetIter<'a> {
            Small(std::slice::Iter<'a, Bytes>),
            Map(std::collections::hash_set::Iter<'a, Bytes>),
        }

        impl<'a> Iterator for SetIter<'a> {
            type Item = &'a Bytes;

            fn next(&mut self) -> Option<Self::Item> {
                match self {
                    Self::Small(iter) => iter.next(),
                    Self::Map(iter) => iter.next(),
                }
            }
        }

        match self {
            Self::Small(entries) => SetIter::Small(entries.iter()),
            Self::Map(set) => SetIter::Map(set.iter()),
        }
    }
}
