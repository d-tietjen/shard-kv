use super::helpers::replace_bytes;
use super::*;

impl HashObject {
    pub(super) fn single(field: Bytes, value: Bytes) -> Self {
        let mut entries = SmallHashEntries::new();
        entries.push((field, value));
        Self::Small(entries)
    }

    pub(super) fn map_with_capacity(capacity: usize) -> Self {
        let mut map = FastHashMap::default();
        map.reserve(capacity);
        Self::Map(map)
    }

    pub(super) fn insert_slice(&mut self, field: &[u8], value: &[u8]) -> bool {
        match self {
            Self::Small(entries) => {
                if let Some((existing_field, existing_value)) = entries.first_mut()
                    && existing_field.as_slice() == field
                {
                    replace_bytes(existing_value, value);
                    return false;
                }
                if let Some((_, existing_value)) = entries
                    .iter_mut()
                    .find(|(existing, _)| existing.as_slice() == field)
                {
                    replace_bytes(existing_value, value);
                    return false;
                }
                if entries.len() < SMALL_HASH_INLINE {
                    entries.push((field.to_vec(), value.to_vec()));
                    return true;
                }

                let capacity = entries.len() + 1;
                let old_entries = std::mem::take(entries);
                *self = Self::map_with_capacity(capacity);
                let Self::Map(map) = self else {
                    unreachable!("hash promotion did not create map state");
                };
                for (old_field, old_value) in old_entries {
                    map.insert(old_field, old_value);
                }
                map.insert(field.to_vec(), value.to_vec());
                true
            }
            Self::Map(map) => map.insert(field.to_vec(), value.to_vec()).is_none(),
        }
    }

    pub(super) fn get(&self, field: &[u8]) -> Option<&Bytes> {
        match self {
            Self::Small(entries) => entries
                .iter()
                .find_map(|(existing, value)| (existing.as_slice() == field).then_some(value)),
            Self::Map(map) => map.get(field),
        }
    }

    pub(super) fn contains_key(&self, field: &[u8]) -> bool {
        self.get(field).is_some()
    }

    pub(super) fn fields(&self) -> Vec<Bytes> {
        let mut fields: Vec<Bytes> = match self {
            Self::Small(entries) => entries.iter().map(|(field, _)| field.clone()).collect(),
            Self::Map(map) => map.keys().cloned().collect(),
        };
        fields.sort();
        fields
    }

    pub(super) fn values(&self) -> Vec<Bytes> {
        match self {
            Self::Small(entries) => entries.iter().map(|(_, value)| value.clone()).collect(),
            Self::Map(map) => map.values().cloned().collect(),
        }
    }

    pub(super) fn entries(&self) -> Vec<(Bytes, Bytes)> {
        let mut entries = match self {
            Self::Small(entries) => entries.clone().into_vec(),
            Self::Map(map) => map
                .iter()
                .map(|(field, value)| (field.clone(), value.clone()))
                .collect(),
        };
        entries.sort_by(|(left, _), (right, _)| left.cmp(right));
        entries
    }

    pub(super) fn visit_entries(&self, mut visit: impl FnMut(&[u8], &[u8])) {
        match self {
            Self::Small(entries) => {
                for (field, value) in entries {
                    visit(field, value);
                }
            }
            Self::Map(map) => {
                for (field, value) in map {
                    visit(field, value);
                }
            }
        }
    }

    pub(super) fn visit_fields(&self, mut visit: impl FnMut(&[u8])) {
        match self {
            Self::Small(entries) => {
                for (field, _) in entries {
                    visit(field);
                }
            }
            Self::Map(map) => {
                for field in map.keys() {
                    visit(field);
                }
            }
        }
    }

    pub(super) fn visit_values(&self, mut visit: impl FnMut(&[u8])) {
        match self {
            Self::Small(entries) => {
                for (_, value) in entries {
                    visit(value);
                }
            }
            Self::Map(map) => {
                for value in map.values() {
                    visit(value);
                }
            }
        }
    }

    pub(super) fn remove(&mut self, field: &[u8]) -> Option<Bytes> {
        match self {
            Self::Small(entries) => {
                let index = entries
                    .iter()
                    .position(|(existing, _)| existing.as_slice() == field)?;
                Some(entries.remove(index).1)
            }
            Self::Map(map) => map.remove(field),
        }
    }

    pub(super) fn len(&self) -> usize {
        match self {
            Self::Small(entries) => entries.len(),
            Self::Map(map) => map.len(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        match self {
            Self::Small(entries) => entries.is_empty(),
            Self::Map(map) => map.is_empty(),
        }
    }
}
