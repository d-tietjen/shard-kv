use std::marker::PhantomData;
use std::ops::Deref;

use bytes::Bytes as SharedBytes;

use super::{
    EmbeddedKeyRoute, EmbeddedRouteMode, EmbeddedShard, OwnedEmbeddedWorkerShards,
    PointMutationKind, PointMutationObserver, PointMutationValidator, RwLockReadGuard,
    RwLockWriteGuard, now_millis,
};

pub(super) enum EmbeddedRefGuard<'a> {
    Read(RwLockReadGuard<'a, EmbeddedShard>),
    Write(RwLockWriteGuard<'a, EmbeddedShard>),
}

impl EmbeddedRefGuard<'_> {
    #[inline(always)]
    fn shard_ptr(&self) -> *const EmbeddedShard {
        match self {
            Self::Read(guard) => &**guard as *const EmbeddedShard,
            Self::Write(guard) => &**guard as *const EmbeddedShard,
        }
    }
}

/// Borrowed value guard returned by [`EmbeddedStore::get_ref`](super::EmbeddedStore::get_ref).
///
/// The value bytes are not copied. The slice remains valid while this guard is
/// alive, and the guard is intentionally not `Send`.
pub struct EmbeddedRef<'a> {
    pub(super) guard: EmbeddedRefGuard<'a>,
    pub(super) value: *const [u8],
    pub(super) _not_send: PhantomData<*const ()>,
}

impl EmbeddedRef<'_> {
    /// Returns the borrowed value bytes.
    #[inline(always)]
    pub fn value(&self) -> &[u8] {
        let _guard = self.guard.shard_ptr();
        // SAFETY: `value` points into the shard protected by `guard`, and the
        // guard is stored in this `EmbeddedRef` for the full value lifetime.
        unsafe { &*self.value }
    }
}

impl Deref for EmbeddedRef<'_> {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

/// Mutable point-key guard returned by [`EmbeddedStore::get_mut`](super::EmbeddedStore::get_mut).
///
/// The guard holds the routed shard write lock. It can inspect, replace, or
/// remove the value without opening another store operation, and replacements
/// preserve the entry's existing TTL.
pub struct EmbeddedRefMut<'a> {
    pub(super) guard: RwLockWriteGuard<'a, EmbeddedShard>,
    pub(super) route_mode: EmbeddedRouteMode,
    pub(super) key: SharedBytes,
    pub(super) key_hash: u64,
    pub(super) expire_at_ms: Option<u64>,
    pub(super) point_mutation_observer: Option<PointMutationObserver>,
    pub(super) point_mutation_validator: Option<PointMutationValidator>,
    pub(super) notify_on_drop: bool,
    pub(super) _not_send: PhantomData<*const ()>,
}

impl EmbeddedRefMut<'_> {
    #[inline(always)]
    fn live_expire_at(&self) -> Option<Option<u64>> {
        match self.expire_at_ms {
            Some(expire_at_ms) if expire_at_ms <= now_millis() => None,
            expire_at_ms => Some(expire_at_ms),
        }
    }

    /// Returns the current value bytes when the key is still present and live.
    #[inline(always)]
    pub fn value(&self) -> Option<&[u8]> {
        match self.live_expire_at()? {
            None => self
                .guard
                .get_ref_hashed_shared_no_ttl(self.key_hash, self.key.as_ref()),
            Some(_) => {
                self.guard
                    .get_ref_hashed_shared(self.key_hash, self.key.as_ref(), now_millis())
            }
        }
    }

    /// Returns mutable access to the stored value bytes for no-TTL entries.
    ///
    /// This API is available only with the `mutable-value-slices` feature. It
    /// intentionally rejects TTL-backed values because in-place mutation skips
    /// the TTL-preserving replacement logic. It also returns `None` when the
    /// stored `bytes::Bytes` buffer is shared with another handle.
    #[cfg(feature = "mutable-value-slices")]
    #[inline(always)]
    pub fn value_mut_no_ttl(&mut self) -> Option<&mut [u8]> {
        let value = match self.live_expire_at()? {
            None => self
                .guard
                .value_mut_hashed_no_ttl(self.key_hash, self.key.as_ref()),
            Some(_) => None,
        }?;
        self.notify_on_drop = true;
        Some(value)
    }

    /// Replaces the value while preserving the entry's existing TTL.
    #[inline(always)]
    pub fn set(&mut self, value: SharedBytes) {
        if self
            .point_mutation_validator
            .as_ref()
            .is_some_and(|validator| !validator.allows(&self.key, value.len(), None))
        {
            tracing::warn!(
                key_len = self.key.len(),
                value_len = value.len(),
                "mutable point replacement rejected before commit because it is rejected by an installed storage extension"
            );
            return;
        }
        self.notify_on_drop = false;
        let observed = self.point_mutation_observer.as_ref().map(|_| value.clone());
        match self.live_expire_at() {
            Some(None) => self.guard.set_value_bytes_hashed_no_ttl(
                self.route_mode,
                self.key_hash,
                self.key.as_ref(),
                value,
            ),
            Some(expire_at_ms) => {
                let now_ms = now_millis();
                self.guard.set_value_bytes_hashed(
                    self.route_mode,
                    self.key_hash,
                    self.key.as_ref(),
                    value,
                    expire_at_ms,
                    now_ms,
                );
            }
            None => {
                let removed =
                    self.guard
                        .remove_value_hashed(self.key_hash, self.key.as_ref(), now_millis());
                if removed.is_some()
                    && let Some(observer) = &self.point_mutation_observer
                {
                    observer.notify(PointMutationKind::Delete, &self.key, None, None, None);
                }
                return;
            }
        }
        if let (Some(observer), Some(value)) = (&self.point_mutation_observer, observed) {
            observer.notify(
                PointMutationKind::Set,
                &self.key,
                Some(value),
                self.expire_at_ms,
                None,
            );
        }
    }

    /// Replaces the value from a borrowed slice while preserving the existing TTL.
    #[inline(always)]
    pub fn set_slice(&mut self, value: &[u8]) {
        self.set(SharedBytes::copy_from_slice(value));
    }

    /// Removes the entry and returns the stored bytes when present and live.
    #[inline(always)]
    pub fn remove(mut self) -> Option<SharedBytes> {
        self.notify_on_drop = false;
        let removed =
            self.guard
                .remove_value_hashed(self.key_hash, self.key.as_ref(), now_millis());
        if removed.is_some()
            && let Some(observer) = &self.point_mutation_observer
        {
            observer.notify(PointMutationKind::Delete, &self.key, None, None, None);
        }
        removed
    }
}

impl Drop for EmbeddedRefMut<'_> {
    fn drop(&mut self) {
        if !self.notify_on_drop {
            return;
        }
        let Some(observer) = &self.point_mutation_observer else {
            return;
        };
        let Some(value) = self
            .guard
            .get_shared_value_bytes_hashed_no_ttl(self.key_hash, self.key.as_ref())
            .cloned()
        else {
            return;
        };
        observer.notify(PointMutationKind::Set, &self.key, Some(value), None, None);
    }
}

/// Mutable point-key guard for the owned-worker embedded path.
///
/// This guard is tied to the worker's exclusive `&mut` borrow instead of a
/// shard lock. Replacements preserve the entry's existing TTL.
pub struct OwnedEmbeddedRefMut<'a> {
    pub(super) worker: &'a mut OwnedEmbeddedWorkerShards,
    pub(super) route: EmbeddedKeyRoute,
    pub(super) key: SharedBytes,
    pub(super) expire_at_ms: Option<u64>,
    pub(super) _not_send: PhantomData<*const ()>,
}

impl OwnedEmbeddedRefMut<'_> {
    #[inline(always)]
    fn live_expire_at(&self) -> Option<Option<u64>> {
        match self.expire_at_ms {
            Some(expire_at_ms) if expire_at_ms <= now_millis() => None,
            expire_at_ms => Some(expire_at_ms),
        }
    }

    /// Returns the current value bytes when the key is still present and live.
    #[inline(always)]
    pub fn value(&mut self) -> Option<&[u8]> {
        let _ = self.live_expire_at()?;
        self.worker
            .local_get_ref_routed(self.route, self.key.as_ref())
    }

    /// Returns mutable access to the stored value bytes for no-TTL entries.
    ///
    /// This API is available only with the `mutable-value-slices` feature. It
    /// intentionally rejects TTL-backed values because in-place mutation skips
    /// the TTL-preserving replacement logic. It also returns `None` when the
    /// stored `bytes::Bytes` buffer is shared with another handle.
    #[cfg(feature = "mutable-value-slices")]
    #[inline(always)]
    pub fn value_mut_no_ttl(&mut self) -> Option<&mut [u8]> {
        match self.live_expire_at()? {
            None => self
                .worker
                .local_value_mut_routed_no_ttl(self.route, self.key.as_ref()),
            Some(_) => None,
        }
    }

    /// Replaces the value while preserving the entry's existing TTL.
    #[inline(always)]
    pub fn set(&mut self, value: SharedBytes) {
        match self.live_expire_at() {
            Some(expire_at_ms) => {
                self.worker.local_set_value_bytes_routed_expire_at(
                    self.route,
                    self.key.as_ref(),
                    value,
                    expire_at_ms,
                    now_millis(),
                );
            }
            None => {
                let _ = self.worker.local_remove_value_routed(
                    self.route,
                    self.key.as_ref(),
                    now_millis(),
                );
            }
        }
    }

    /// Replaces the value from a borrowed slice while preserving the existing TTL.
    #[inline(always)]
    pub fn set_slice(&mut self, value: &[u8]) {
        self.set(SharedBytes::copy_from_slice(value));
    }

    /// Removes the entry and returns the stored bytes when present and live.
    #[inline(always)]
    pub fn remove(self) -> Option<SharedBytes> {
        self.worker
            .local_remove_value_routed(self.route, self.key.as_ref(), now_millis())
    }
}

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
    pub(crate) fn from_item(item: Option<EmbeddedReadSlice>) -> Self {
        Self { item }
    }

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
    pub(crate) fn from_items(items: Vec<Option<EmbeddedReadSlice>>) -> Self {
        let mut hit_count = 0usize;
        let mut total_bytes = 0usize;
        for item in items.iter().flatten() {
            hit_count += 1;
            total_bytes += item.len();
        }
        Self {
            items,
            hit_count,
            total_bytes,
        }
    }

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
