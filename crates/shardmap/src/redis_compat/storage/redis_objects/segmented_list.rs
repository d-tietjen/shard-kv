use super::*;

impl SegmentedList {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            chunks: VecDeque::with_capacity(capacity.div_ceil(LIST_CHUNK_CAPACITY)),
            len: 0,
        }
    }

    pub(super) fn from_vec(values: Vec<Bytes>) -> Self {
        let mut list = Self::with_capacity(values.len());
        for value in values {
            list.push_back(value);
        }
        list
    }

    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub(super) fn push_front(&mut self, value: Bytes) {
        if let Some(chunk) = self.chunks.front_mut()
            && !chunk.is_full()
        {
            chunk
                .push_front(value)
                .expect("front chunk capacity checked before push");
            self.len += 1;
            return;
        }

        let mut chunk = ListChunk::new();
        chunk
            .push_front(value)
            .expect("new list chunk has capacity");
        self.chunks.push_front(chunk);
        self.len += 1;
    }

    #[inline(always)]
    pub(super) fn push_back(&mut self, value: Bytes) {
        if let Some(chunk) = self.chunks.back_mut()
            && !chunk.is_full()
        {
            chunk
                .push_back(value)
                .expect("back chunk capacity checked before push");
            self.len += 1;
            return;
        }

        let mut chunk = ListChunk::new();
        chunk.push_back(value).expect("new list chunk has capacity");
        self.chunks.push_back(chunk);
        self.len += 1;
    }

    pub(super) fn push_values(&mut self, values: &[&[u8]], front: bool) {
        if front {
            for value in values {
                self.push_front(value.to_vec());
            }
        } else {
            for value in values {
                self.push_back(value.to_vec());
            }
        }
    }

    #[inline(always)]
    pub(super) fn pop_front(&mut self) -> Option<Bytes> {
        let value = {
            let chunk = self.chunks.front_mut()?;
            chunk.pop_front()
        }?;
        self.len -= 1;
        if self.chunks.front().is_some_and(ListChunk::is_empty) {
            self.chunks.pop_front();
        }
        Some(value)
    }

    #[inline(always)]
    pub(super) fn pop_back(&mut self) -> Option<Bytes> {
        let value = {
            let chunk = self.chunks.back_mut()?;
            chunk.pop_back()
        }?;
        self.len -= 1;
        if self.chunks.back().is_some_and(ListChunk::is_empty) {
            self.chunks.pop_back();
        }
        Some(value)
    }

    pub(super) fn get(&self, index: usize) -> Option<&Bytes> {
        if index >= self.len {
            return None;
        }
        if index <= self.len / 2 {
            let mut offset = index;
            for chunk in &self.chunks {
                if offset < chunk.len() {
                    return chunk.get(offset);
                }
                offset -= chunk.len();
            }
        } else {
            let mut offset = self.len - index - 1;
            for chunk in self.chunks.iter().rev() {
                if offset < chunk.len() {
                    return chunk.get(chunk.len() - offset - 1);
                }
                offset -= chunk.len();
            }
        }
        None
    }

    pub(super) fn set(&mut self, index: usize, value: Bytes) {
        if index <= self.len / 2 {
            let mut offset = index;
            for chunk in &mut self.chunks {
                if offset < chunk.len() {
                    chunk.set(offset, value);
                    return;
                }
                offset -= chunk.len();
            }
        } else {
            let mut offset = self.len - index - 1;
            for chunk in self.chunks.iter_mut().rev() {
                if offset < chunk.len() {
                    let index = chunk.len() - offset - 1;
                    chunk.set(index, value);
                    return;
                }
                offset -= chunk.len();
            }
        }
    }

    pub(super) fn insert(&mut self, index: usize, value: Bytes) {
        if index == 0 {
            self.push_front(value);
            return;
        }
        if index >= self.len {
            self.push_back(value);
            return;
        }

        let mut values = self.take_all();
        values.insert(index, value);
        *self = Self::from_vec(values);
    }

    pub(super) fn remove(&mut self, index: usize) -> Option<Bytes> {
        if index >= self.len {
            return None;
        }

        let mut offset = index;
        for chunk_index in 0..self.chunks.len() {
            let chunk_len = self.chunks[chunk_index].len();
            if offset < chunk_len {
                let value = self.chunks[chunk_index].remove(offset);
                self.len -= 1;
                if self.chunks[chunk_index].is_empty() {
                    self.chunks.remove(chunk_index);
                }
                return value;
            }
            offset -= chunk_len;
        }
        None
    }

    pub(super) fn clear(&mut self) {
        self.chunks.clear();
        self.len = 0;
    }

    pub(super) fn take_all(&mut self) -> Vec<Bytes> {
        let mut values = Vec::with_capacity(self.len);
        for mut chunk in self.chunks.drain(..) {
            values.extend(chunk.take_all());
        }
        self.len = 0;
        values
    }

    pub(super) fn iter(&self) -> SegmentedListIter<'_> {
        SegmentedListIter {
            chunks: self.chunks.iter(),
            current: None,
        }
    }
}

impl<'a> Iterator for SegmentedListIter<'a> {
    type Item = &'a Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(current) = &mut self.current
                && let Some(value) = current.next()
            {
                return Some(value);
            }
            self.current = Some(self.chunks.next()?.iter());
        }
    }
}
