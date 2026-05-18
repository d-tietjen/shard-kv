use super::*;

/// Read metadata for a value returned by a [`WorkerLocalEmbeddedStore`].
///
/// In the default safe build this owns a `Bytes` copy. With the `unsafe`
/// feature it may hold a raw pointer into worker-owned storage; in that case
/// the lifetime is tied to the exclusive `&mut WorkerLocalEmbeddedStore` borrow
/// that produced it, and the `Rc` phantom keeps the value from being sent across
/// threads.
#[derive(Debug, Clone)]
pub struct WorkerLocalReadSlice<'a> {
    ptr: *const u8,
    len: usize,
    owned: Option<bytes::Bytes>,
    _local: PhantomData<(&'a mut (), Rc<()>)>,
}

impl<'a> WorkerLocalReadSlice<'a> {
    #[inline(always)]
    pub(super) fn from_embedded(inner: EmbeddedReadSlice) -> Self {
        let owned = inner.into_bytes();
        Self {
            ptr: owned.as_ptr(),
            len: owned.len(),
            owned: Some(owned),
            _local: PhantomData,
        }
    }

    #[inline(always)]
    pub(super) fn from_local_slice(inner: &'a [u8]) -> Self {
        #[cfg(not(feature = "unsafe"))]
        {
            let owned = bytes::Bytes::copy_from_slice(inner);
            Self {
                ptr: owned.as_ptr(),
                len: owned.len(),
                owned: Some(owned),
                _local: PhantomData,
            }
        }

        #[cfg(feature = "unsafe")]
        Self {
            ptr: inner.as_ptr(),
            len: inner.len(),
            owned: None,
            _local: PhantomData,
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        #[cfg(not(feature = "unsafe"))]
        {
            self.owned
                .as_deref()
                .expect("safe worker-local read slices are owned")
        }

        #[cfg(feature = "unsafe")]
        {
            if let Some(owned) = &self.owned {
                return owned.as_ref();
            }
            // SAFETY: `WorkerLocalReadSlice` is only constructed from a slice tied
            // to the caller's exclusive `&mut WorkerLocalEmbeddedStore` borrow or from an
            // `EmbeddedReadSlice` protected by the existing embedded-store view
            // invariants. The `Rc` phantom keeps the wrapper thread-local.
            unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
        }
    }
}

/// Optional single-key read result from a worker-local store.
#[derive(Debug)]
pub struct WorkerLocalReadView<'a> {
    pub(super) item: Option<WorkerLocalReadSlice<'a>>,
}

impl<'a> WorkerLocalReadView<'a> {
    #[inline(always)]
    pub fn is_hit(&self) -> bool {
        self.item.is_some()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.item.as_ref().map_or(0, WorkerLocalReadSlice::len)
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn slice(&self) -> Option<&[u8]> {
        self.item.as_ref().map(WorkerLocalReadSlice::as_slice)
    }

    #[inline(always)]
    pub fn slice_meta(&self) -> Option<WorkerLocalReadSlice<'a>> {
        self.item.clone()
    }
}

/// Batch read result whose item lifetimes are tied to one worker-local store borrow.
#[derive(Debug)]
pub struct WorkerLocalBatchReadView<'a> {
    items: Vec<Option<WorkerLocalReadSlice<'a>>>,
    hit_count: usize,
    total_bytes: usize,
}

impl<'a> WorkerLocalBatchReadView<'a> {
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
            .map(WorkerLocalReadSlice::as_slice)
    }

    #[inline(always)]
    pub fn slice_meta(&self, index: usize) -> Option<WorkerLocalReadSlice<'a>> {
        self.items.get(index).cloned().flatten()
    }

    #[inline(always)]
    pub fn lengths(&self) -> Vec<usize> {
        self.items
            .iter()
            .map(|item| item.as_ref().map_or(0, WorkerLocalReadSlice::len))
            .collect()
    }
}

/// Session-key batch view returned by worker-local session APIs.
pub type WorkerLocalSessionBatchView<'a> = WorkerLocalBatchReadView<'a>;

pub(super) fn worker_local_batch_view_from_embedded<'a>(
    items: Vec<Option<EmbeddedReadSlice>>,
) -> WorkerLocalBatchReadView<'a> {
    let mut converted = Vec::with_capacity(items.len());
    let mut hit_count = 0usize;
    let mut total_bytes = 0usize;
    for item in items {
        match item {
            Some(slice) => {
                hit_count += 1;
                total_bytes += slice.len();
                converted.push(Some(WorkerLocalReadSlice::from_embedded(slice)));
            }
            None => converted.push(None),
        }
    }
    WorkerLocalBatchReadView {
        items: converted,
        hit_count,
        total_bytes,
    }
}
