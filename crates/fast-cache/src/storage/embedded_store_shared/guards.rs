use super::*;

/// Borrowed value guard returned by [`SharedEmbeddedStore::get`].
///
/// The value slice is valid while this guard is alive.
///
/// ```compile_fail
/// use fast_cache::storage::{SharedEmbeddedConfig, SharedEmbeddedStore};
///
/// let store = SharedEmbeddedStore::<4>::new(SharedEmbeddedConfig::default());
/// store.insert_slice(b"k", b"v");
/// let value = store.get(b"k").unwrap();
///
/// fn require_send<T: Send>(_: T) {}
/// require_send(value);
/// ```
pub struct Ref<'a> {
    pub(super) guard: SharedReadGuard<'a, EmbeddedShard>,
    pub(super) value: *const [u8],
    pub(super) _not_send: PhantomData<*const ()>,
}

impl Ref<'_> {
    /// Returns the borrowed value bytes.
    #[inline(always)]
    pub fn value(&self) -> &[u8] {
        let _guard = &self.guard;
        // SAFETY: `value` points into `guard`'s shard, and the read guard is
        // held for this `Ref`'s full lifetime.
        unsafe { &*self.value }
    }
}

impl Deref for Ref<'_> {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

/// Mutable point-key guard returned by [`SharedEmbeddedStore::get_mut`].
pub struct RefMut<'a> {
    pub(super) guard: SharedWriteGuard<'a, EmbeddedShard>,
    pub(super) route_mode: EmbeddedRouteMode,
    pub(super) key: SharedBytes,
    pub(super) key_hash: u64,
    pub(super) _not_send: PhantomData<*const ()>,
}

impl RefMut<'_> {
    /// Returns the current value bytes when the key is still present.
    #[inline(always)]
    pub fn value(&self) -> Option<&[u8]> {
        self.guard
            .get_ref_hashed_shared_no_ttl(self.key_hash, self.key.as_ref())
    }

    /// Replaces the value without changing the key.
    #[inline(always)]
    pub fn set(&mut self, value: SharedBytes) {
        self.guard.set_value_bytes_hashed_no_ttl(
            self.route_mode,
            self.key_hash,
            self.key.as_ref(),
            value,
        );
    }

    /// Removes the entry and returns the stored bytes when present.
    #[inline(always)]
    pub fn remove(mut self) -> Option<SharedBytes> {
        self.guard
            .remove_value_hashed_no_ttl(self.key_hash, self.key.as_ref())
    }
}

/// Occupied or vacant routed entry.
pub enum Entry<'a> {
    /// Existing point-key value.
    Occupied(RefMut<'a>),
    /// Missing point-key value.
    Vacant(VacantEntry<'a>),
}

impl<'a> Entry<'a> {
    /// Inserts `value` when vacant and returns a mutable guard either way.
    #[inline(always)]
    pub fn or_insert(self, value: SharedBytes) -> RefMut<'a> {
        match self {
            Self::Occupied(entry) => entry,
            Self::Vacant(entry) => entry.insert(value),
        }
    }
}

/// Vacant routed entry.
pub struct VacantEntry<'a> {
    pub(super) guard: SharedWriteGuard<'a, EmbeddedShard>,
    pub(super) route_mode: EmbeddedRouteMode,
    pub(super) key: SharedBytes,
    pub(super) key_hash: u64,
    pub(super) _not_send: PhantomData<*const ()>,
}

impl<'a> VacantEntry<'a> {
    /// Inserts `value` and returns a mutable guard for the new entry.
    #[inline(always)]
    pub fn insert(mut self, value: SharedBytes) -> RefMut<'a> {
        self.guard.set_value_bytes_hashed_no_ttl(
            self.route_mode,
            self.key_hash,
            self.key.as_ref(),
            value,
        );
        RefMut {
            guard: self.guard,
            route_mode: self.route_mode,
            key: self.key,
            key_hash: self.key_hash,
            _not_send: PhantomData,
        }
    }
}
