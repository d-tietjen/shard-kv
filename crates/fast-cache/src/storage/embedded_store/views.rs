/// One stable byte slice captured during a batch read.
///
/// The slice owns a `bytes::Bytes` handle. Older versions stored raw pointers
/// into shard memory and relied on a separate guard to keep those pointers
/// valid; that made `slice_meta()` too easy to misuse. Keeping the backing
/// bytes here preserves the same read API while making the metadata object
/// independently memory-safe.
#[derive(Debug, Clone)]
pub struct EmbeddedReadSlice {
    pub(super) bytes: bytes::Bytes,
}

impl EmbeddedReadSlice {
    #[inline(always)]
    pub(super) fn from_slice(value: &[u8]) -> Self {
        Self {
            bytes: bytes::Bytes::copy_from_slice(value),
        }
    }

    #[inline(always)]
    pub(crate) fn into_bytes(self) -> bytes::Bytes {
        self.bytes
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    #[inline(always)]
    pub fn as_ptr(&self) -> *const u8 {
        self.bytes.as_ptr()
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

/// Owned single-value read view.
///
/// The view carries its own `bytes::Bytes` handle, so `slice()` and
/// `slice_meta()` remain valid independently of later store mutations.
#[derive(Debug)]
pub struct EmbeddedReadView {
    pub(super) item: Option<EmbeddedReadSlice>,
}

impl EmbeddedReadView {
    #[inline(always)]
    pub fn is_hit(&self) -> bool {
        self.item.is_some()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.item.as_ref().map_or(0, EmbeddedReadSlice::len)
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn slice(&self) -> Option<&[u8]> {
        self.item.as_ref().map(EmbeddedReadSlice::as_slice)
    }

    #[inline(always)]
    pub fn slice_meta(&self) -> Option<EmbeddedReadSlice> {
        self.item.clone()
    }
}

/// Owned batch read view.
///
/// Each hit carries its own `bytes::Bytes` handle. This keeps the public view
/// API memory-safe even when callers hold metadata longer than the store.
#[derive(Debug)]
pub struct EmbeddedBatchReadView {
    pub(super) items: Vec<Option<EmbeddedReadSlice>>,
    pub(super) hit_count: usize,
    pub(super) total_bytes: usize,
}

impl EmbeddedBatchReadView {
    #[inline(always)]
    pub fn item_count(&self) -> usize {
        self.items.len()
    }

    #[inline(always)]
    pub fn hit_count(&self) -> usize {
        self.hit_count
    }

    #[inline(always)]
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    #[inline(always)]
    pub fn all_hit(&self) -> bool {
        self.hit_count == self.items.len()
    }

    #[inline(always)]
    pub fn slice(&self, index: usize) -> Option<&[u8]> {
        self.items
            .get(index)
            .and_then(|item| item.as_ref())
            .map(EmbeddedReadSlice::as_slice)
    }

    #[inline(always)]
    pub fn slice_meta(&self, index: usize) -> Option<EmbeddedReadSlice> {
        self.items.get(index).cloned().flatten()
    }

    #[inline(always)]
    pub fn lengths(&self) -> Vec<usize> {
        self.items
            .iter()
            .map(|item| item.as_ref().map_or(0, EmbeddedReadSlice::len))
            .collect()
    }
}

/// Session-scoped owned batch view.
///
/// This is currently just a clearer alias for the generic batch guard used by
/// the Python API and the LLM benchmark path.
pub type EmbeddedSessionBatchView = EmbeddedBatchReadView;

/// Owned single-value read view for the owned-worker embedded path.
#[derive(Debug)]
pub struct OwnedEmbeddedReadView {
    pub(super) item: Option<EmbeddedReadSlice>,
}

impl OwnedEmbeddedReadView {
    #[inline(always)]
    pub fn is_hit(&self) -> bool {
        self.item.is_some()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.item.as_ref().map_or(0, EmbeddedReadSlice::len)
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn slice(&self) -> Option<&[u8]> {
        self.item.as_ref().map(EmbeddedReadSlice::as_slice)
    }

    #[inline(always)]
    pub fn slice_meta(&self) -> Option<EmbeddedReadSlice> {
        self.item.clone()
    }
}

/// Owned batch read view for the owned-worker embedded path.
#[derive(Debug)]
pub struct OwnedEmbeddedBatchReadView {
    pub(super) items: Vec<Option<EmbeddedReadSlice>>,
    pub(super) hit_count: usize,
    pub(super) total_bytes: usize,
}

impl OwnedEmbeddedBatchReadView {
    #[inline(always)]
    pub fn item_count(&self) -> usize {
        self.items.len()
    }

    #[inline(always)]
    pub fn hit_count(&self) -> usize {
        self.hit_count
    }

    #[inline(always)]
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    #[inline(always)]
    pub fn all_hit(&self) -> bool {
        self.hit_count == self.items.len()
    }

    #[inline(always)]
    pub fn slice(&self, index: usize) -> Option<&[u8]> {
        self.items
            .get(index)
            .and_then(|item| item.as_ref())
            .map(EmbeddedReadSlice::as_slice)
    }

    #[inline(always)]
    pub fn slice_meta(&self, index: usize) -> Option<EmbeddedReadSlice> {
        self.items.get(index).cloned().flatten()
    }

    #[inline(always)]
    pub fn lengths(&self) -> Vec<usize> {
        self.items
            .iter()
            .map(|item| item.as_ref().map_or(0, EmbeddedReadSlice::len))
            .collect()
    }
}

pub type OwnedEmbeddedSessionBatchView = OwnedEmbeddedBatchReadView;

/// Owned packed-session view for the owned-worker embedded path.
///
/// The packed payload is copied into an owned `bytes::Bytes` buffer so the
/// offset table stays valid without a raw pointer back into the worker shard.
#[derive(Debug)]
pub struct OwnedEmbeddedSessionPackedView {
    pub(super) buffer: EmbeddedReadSlice,
    pub(super) offsets: Vec<usize>,
    pub(super) lengths: Vec<usize>,
    pub(super) hit_count: usize,
    pub(super) total_bytes: usize,
}

impl OwnedEmbeddedSessionPackedView {
    #[inline(always)]
    pub fn item_count(&self) -> usize {
        self.offsets.len()
    }

    #[inline(always)]
    pub fn hit_count(&self) -> usize {
        self.hit_count
    }

    #[inline(always)]
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    #[inline(always)]
    pub fn all_hit(&self) -> bool {
        self.hit_count == self.offsets.len()
    }

    #[inline(always)]
    pub fn buffer(&self) -> &[u8] {
        self.buffer.as_slice()
    }

    #[inline(always)]
    pub fn buffer_meta(&self) -> EmbeddedReadSlice {
        self.buffer.clone()
    }

    #[inline(always)]
    pub fn offsets(&self) -> &[usize] {
        &self.offsets
    }

    #[inline(always)]
    pub fn lengths(&self) -> &[usize] {
        &self.lengths
    }
}
