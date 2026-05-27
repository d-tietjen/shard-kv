use super::*;

pub(super) struct BatchReadViewBuilder {
    items: Vec<Option<EmbeddedReadSlice>>,
    hit_count: usize,
    total_bytes: usize,
}

impl BatchReadViewBuilder {
    pub(super) fn new(item_count: usize) -> Self {
        Self {
            items: Vec::with_capacity(item_count),
            hit_count: 0,
            total_bytes: 0,
        }
    }

    #[inline(always)]
    pub(super) fn push(&mut self, value: Option<&[u8]>) {
        match value {
            Some(value) => self.push_hit(value),
            None => self.items.push(None),
        }
    }

    #[inline(always)]
    pub(super) fn push_hit(&mut self, value: &[u8]) {
        self.items.push(Some(EmbeddedReadSlice::from_slice(value)));
        self.hit_count += 1;
        self.total_bytes += value.len();
    }

    pub(super) fn finish(self) -> EmbeddedBatchReadView {
        EmbeddedBatchReadView {
            items: self.items,
            hit_count: self.hit_count,
            total_bytes: self.total_bytes,
        }
    }

    pub(super) fn finish_owned(self) -> OwnedEmbeddedBatchReadView {
        OwnedEmbeddedBatchReadView {
            items: self.items,
            hit_count: self.hit_count,
            total_bytes: self.total_bytes,
        }
    }
}

pub(super) struct OrderedBatchReadViewBuilder {
    items: Vec<Option<EmbeddedReadSlice>>,
    hit_count: usize,
    total_bytes: usize,
}

impl OrderedBatchReadViewBuilder {
    pub(super) fn new(item_count: usize) -> Self {
        Self {
            items: vec![None; item_count],
            hit_count: 0,
            total_bytes: 0,
        }
    }

    #[inline(always)]
    pub(super) fn record_hit(&mut self, index: usize, value: &[u8]) {
        self.items[index] = Some(EmbeddedReadSlice::from_slice(value));
        self.hit_count += 1;
        self.total_bytes += value.len();
    }

    pub(super) fn finish(self) -> EmbeddedBatchReadView {
        EmbeddedBatchReadView {
            items: self.items,
            hit_count: self.hit_count,
            total_bytes: self.total_bytes,
        }
    }

    pub(super) fn finish_owned(self) -> OwnedEmbeddedBatchReadView {
        OwnedEmbeddedBatchReadView {
            items: self.items,
            hit_count: self.hit_count,
            total_bytes: self.total_bytes,
        }
    }
}

pub(super) struct PackedBatchBuilder {
    packed: PackedBatch,
    item_count: usize,
}

impl PackedBatchBuilder {
    pub(super) fn new(item_count: usize) -> Self {
        let mut packed = PackedBatch::default();
        packed.offsets.reserve(item_count);
        packed.lengths.reserve(item_count);
        Self { packed, item_count }
    }

    #[inline(always)]
    pub(super) fn push(&mut self, value: Option<&[u8]>) {
        match value {
            Some(value) => self.push_hit(value),
            None => self.push_miss(),
        }
    }

    #[inline(always)]
    pub(super) fn push_hit(&mut self, value: &[u8]) {
        reserve_batch_capacity(&mut self.packed.buffer, value.len(), self.item_count);
        let offset = self.packed.buffer.len();
        self.packed.buffer.extend_from_slice(value);
        self.packed.offsets.push(offset);
        self.packed.lengths.push(value.len());
        self.packed.hit_count += 1;
    }

    #[inline(always)]
    fn push_miss(&mut self) {
        self.packed.offsets.push(usize::MAX);
        self.packed.lengths.push(0);
    }

    pub(super) fn finish(self) -> PackedBatch {
        self.packed
    }
}

pub(super) struct OrderedPackedBatchBuilder {
    buffer: Bytes,
    offsets: Vec<usize>,
    lengths: Vec<usize>,
    hit_count: usize,
    item_count: usize,
}

impl OrderedPackedBatchBuilder {
    pub(super) fn new(item_count: usize) -> Self {
        Self {
            buffer: Vec::new(),
            offsets: vec![usize::MAX; item_count],
            lengths: vec![0; item_count],
            hit_count: 0,
            item_count,
        }
    }

    #[inline(always)]
    pub(super) fn record_hit(&mut self, index: usize, value: &[u8]) {
        reserve_batch_capacity(&mut self.buffer, value.len(), self.item_count);
        let offset = self.buffer.len();
        self.buffer.extend_from_slice(value);
        self.offsets[index] = offset;
        self.lengths[index] = value.len();
        self.hit_count += 1;
    }

    pub(super) fn finish(self) -> PackedBatch {
        PackedBatch {
            buffer: self.buffer,
            offsets: self.offsets,
            lengths: self.lengths,
            hit_count: self.hit_count,
        }
    }
}
