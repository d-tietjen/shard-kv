use super::*;

impl SmallListDeque {
    pub(super) fn new() -> Self {
        Self {
            entries: std::array::from_fn(|_| None),
            head: 0,
            len: 0,
        }
    }

    pub(super) fn from_vec(values: Vec<Bytes>) -> Self {
        debug_assert!(values.len() <= SMALL_LIST_INLINE);
        let mut list = Self::new();
        for value in values {
            list.push_back(value)
                .expect("small list capacity checked before construction");
        }
        list
    }

    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub(super) fn is_full(&self) -> bool {
        self.len == SMALL_LIST_INLINE
    }

    #[inline(always)]
    pub(super) fn physical_index(&self, logical_index: usize) -> usize {
        (self.head + logical_index) % SMALL_LIST_INLINE
    }

    #[inline(always)]
    pub(super) fn get(&self, index: usize) -> Option<&Bytes> {
        (index < self.len)
            .then(|| self.physical_index(index))
            .and_then(|index| self.entries[index].as_ref())
    }

    #[inline(always)]
    pub(super) fn set(&mut self, index: usize, value: Bytes) {
        let physical_index = self.physical_index(index);
        self.entries[physical_index] = Some(value);
    }

    #[inline(always)]
    pub(super) fn push_front(&mut self, value: Bytes) -> Result<(), Bytes> {
        if self.is_full() {
            return Err(value);
        }
        self.head = if self.len == 0 {
            0
        } else {
            (self.head + SMALL_LIST_INLINE - 1) % SMALL_LIST_INLINE
        };
        self.entries[self.head] = Some(value);
        self.len += 1;
        Ok(())
    }

    #[inline(always)]
    pub(super) fn push_back(&mut self, value: Bytes) -> Result<(), Bytes> {
        if self.is_full() {
            return Err(value);
        }
        let index = self.physical_index(self.len);
        self.entries[index] = Some(value);
        self.len += 1;
        Ok(())
    }

    #[inline(always)]
    pub(super) fn pop_front(&mut self) -> Option<Bytes> {
        if self.len == 0 {
            return None;
        }
        let value = self.entries[self.head].take();
        self.head = (self.head + 1) % SMALL_LIST_INLINE;
        self.len -= 1;
        if self.len == 0 {
            self.head = 0;
        }
        value
    }

    #[inline(always)]
    pub(super) fn pop_back(&mut self) -> Option<Bytes> {
        if self.len == 0 {
            return None;
        }
        let index = self.physical_index(self.len - 1);
        let value = self.entries[index].take();
        self.len -= 1;
        if self.len == 0 {
            self.head = 0;
        }
        value
    }

    pub(super) fn insert(&mut self, index: usize, value: Bytes) -> Result<(), Bytes> {
        if self.is_full() {
            return Err(value);
        }
        let mut values = self.take_all();
        values.insert(index, value);
        *self = Self::from_vec(values);
        Ok(())
    }

    pub(super) fn remove(&mut self, index: usize) -> Option<Bytes> {
        if index >= self.len {
            return None;
        }
        let mut values = self.take_all();
        let value = values.remove(index);
        *self = Self::from_vec(values);
        Some(value)
    }

    pub(super) fn take_all(&mut self) -> Vec<Bytes> {
        let len = self.len;
        let mut values = Vec::with_capacity(len);
        for index in 0..len {
            let physical_index = self.physical_index(index);
            values.push(
                self.entries[physical_index]
                    .take()
                    .expect("small list slot missing value"),
            );
        }
        self.head = 0;
        self.len = 0;
        values
    }

    pub(super) fn into_segmented(mut self, additional: usize) -> SegmentedList {
        let mut list = SegmentedList::with_capacity(self.len + additional);
        for value in self.take_all() {
            list.push_back(value);
        }
        list
    }

    pub(super) fn iter(&self) -> SmallListIter<'_> {
        SmallListIter {
            list: self,
            index: 0,
        }
    }
}

impl<'a> Iterator for SmallListIter<'a> {
    type Item = &'a Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.list.get(self.index)?;
        self.index += 1;
        Some(value)
    }
}

pub(super) fn push_values_small(list: &mut SmallListDeque, values: &[&[u8]], front: bool) {
    if front {
        for value in values {
            list.push_front(value.to_vec())
                .expect("small list capacity checked before push");
        }
    } else {
        for value in values {
            list.push_back(value.to_vec())
                .expect("small list capacity checked before push");
        }
    }
}
