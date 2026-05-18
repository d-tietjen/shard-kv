use super::list_deque::push_values_small;
use super::*;

impl ListObject {
    pub(super) fn from_vec(entries: Vec<Bytes>) -> Self {
        match entries.len() {
            0 => Self::Empty,
            1 => Self::Single(
                entries
                    .into_iter()
                    .next()
                    .expect("single-entry list missing value"),
            ),
            len if len <= SMALL_LIST_INLINE => Self::Small(SmallListDeque::from_vec(entries)),
            _ => Self::Segmented(SegmentedList::from_vec(entries)),
        }
    }

    pub(super) fn from_values(values: &[&[u8]], front: bool) -> Self {
        match values {
            [] => Self::Empty,
            [value] => Self::Single(value.to_vec()),
            _ => {
                let mut list = Self::Small(SmallListDeque::new());
                list.push_values(values, front);
                list
            }
        }
    }

    pub(super) fn push_values(&mut self, values: &[&[u8]], front: bool) {
        if values.is_empty() {
            return;
        }
        match self {
            Self::Empty => {
                *self = Self::from_values(values, front);
            }
            Self::Single(existing) => {
                let total = 1 + values.len();
                if total <= SMALL_LIST_INLINE {
                    let mut list = SmallListDeque::new();
                    list.push_back(std::mem::take(existing))
                        .expect("single-entry small list has capacity");
                    push_values_small(&mut list, values, front);
                    *self = Self::Small(list);
                } else {
                    let mut list = SegmentedList::with_capacity(total);
                    list.push_back(std::mem::take(existing));
                    list.push_values(values, front);
                    *self = Self::Segmented(list);
                }
            }
            Self::Small(list) => {
                if list.len() + values.len() <= SMALL_LIST_INLINE {
                    push_values_small(list, values, front);
                } else {
                    let mut segmented =
                        std::mem::replace(list, SmallListDeque::new()).into_segmented(values.len());
                    segmented.push_values(values, front);
                    *self = Self::Segmented(segmented);
                }
            }
            Self::Segmented(list) => list.push_values(values, front),
        }
    }

    pub(super) fn pop_front(&mut self) -> Option<Bytes> {
        match self {
            Self::Empty => None,
            Self::Single(_) => match std::mem::replace(self, Self::Empty) {
                Self::Single(value) => Some(value),
                _ => unreachable!("single list replaced with non-single value"),
            },
            Self::Small(list) => list.pop_front(),
            Self::Segmented(list) => list.pop_front(),
        }
    }

    pub(super) fn pop_back(&mut self) -> Option<Bytes> {
        match self {
            Self::Empty => None,
            Self::Single(_) => match std::mem::replace(self, Self::Empty) {
                Self::Single(value) => Some(value),
                _ => unreachable!("single list replaced with non-single value"),
            },
            Self::Small(list) => list.pop_back(),
            Self::Segmented(list) => list.pop_back(),
        }
    }

    pub(super) fn insert(&mut self, index: usize, value: Bytes) {
        match self {
            Self::Empty => {
                debug_assert_eq!(index, 0);
                *self = Self::Single(value);
            }
            Self::Single(_) => {
                let Self::Single(existing) = std::mem::replace(self, Self::Empty) else {
                    unreachable!("single list replaced with non-single value");
                };
                let mut list = SmallListDeque::new();
                list.push_back(existing)
                    .expect("single-entry small list has capacity");
                list.insert(index, value)
                    .expect("single-entry small list has capacity");
                *self = Self::Small(list);
            }
            Self::Small(list) => {
                if !list.is_full() {
                    let _ = list.insert(index, value);
                } else {
                    let mut segmented =
                        std::mem::replace(list, SmallListDeque::new()).into_segmented(1);
                    segmented.insert(index, value);
                    *self = Self::Segmented(segmented);
                }
            }
            Self::Segmented(list) => list.insert(index, value),
        }
    }

    pub(super) fn remove(&mut self, index: usize) -> Option<Bytes> {
        match self {
            Self::Empty => None,
            Self::Single(_) => {
                debug_assert_eq!(index, 0);
                match std::mem::replace(self, Self::Empty) {
                    Self::Single(value) => Some(value),
                    _ => unreachable!("single list replaced with non-single value"),
                }
            }
            Self::Small(list) => list.remove(index),
            Self::Segmented(list) => list.remove(index),
        }
    }

    pub(super) fn set(&mut self, index: usize, value: Bytes) {
        match self {
            Self::Empty => debug_assert_eq!(index, 0),
            Self::Single(existing) => {
                debug_assert_eq!(index, 0);
                *existing = value;
            }
            Self::Small(list) => list.set(index, value),
            Self::Segmented(list) => list.set(index, value),
        }
    }

    pub(super) fn get(&self, index: usize) -> Option<&Bytes> {
        match self {
            Self::Empty => None,
            Self::Single(value) => (index == 0).then_some(value),
            Self::Small(list) => list.get(index),
            Self::Segmented(list) => list.get(index),
        }
    }

    pub(super) fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Single(_) => 1,
            Self::Small(list) => list.len(),
            Self::Segmented(list) => list.len(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(super) fn clear(&mut self) {
        match self {
            Self::Segmented(list) => list.clear(),
            _ => *self = Self::Empty,
        }
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &Bytes> {
        enum ListIter<'a> {
            Empty(std::iter::Empty<&'a Bytes>),
            Single(std::iter::Once<&'a Bytes>),
            Small(SmallListIter<'a>),
            Segmented(SegmentedListIter<'a>),
        }

        impl<'a> Iterator for ListIter<'a> {
            type Item = &'a Bytes;

            fn next(&mut self) -> Option<Self::Item> {
                match self {
                    Self::Empty(iter) => iter.next(),
                    Self::Single(iter) => iter.next(),
                    Self::Small(iter) => iter.next(),
                    Self::Segmented(iter) => iter.next(),
                }
            }
        }

        match self {
            Self::Empty => ListIter::Empty(std::iter::empty()),
            Self::Single(value) => ListIter::Single(std::iter::once(value)),
            Self::Small(list) => ListIter::Small(list.iter()),
            Self::Segmented(list) => ListIter::Segmented(list.iter()),
        }
    }
}
