use super::*;

/// Borrowed value guard returned by [`SharedEmbeddedStore::get`].
///
/// The value slice is valid while this guard is alive.
///
/// ```compile_fail
/// use shardmap::storage::{SharedEmbeddedConfig, SharedEmbeddedStore};
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
    #[cfg_attr(feature = "no-ttl", allow(dead_code))]
    pub(super) expire_at_ms: Option<u64>,
    pub(super) semantic_generation: &'a AtomicU64,
    pub(super) semantic_shadow: Option<&'a SharedShardLock<EmbeddedShard>>,
    pub(super) _not_send: PhantomData<*const ()>,
}

impl RefMut<'_> {
    #[inline(always)]
    fn bump_semantic_generation(&self) {
        self.semantic_generation.fetch_add(1, Ordering::AcqRel);
    }

    #[inline(always)]
    fn invalidate_semantic_shadow(&self, now_ms: u64) -> Option<SharedBytes> {
        let semantic_shadow = self.semantic_shadow?;
        semantic_shadow
            .write()
            .remove_value_hashed(self.key_hash, self.key.as_ref(), now_ms)
    }

    #[cfg(feature = "no-ttl")]
    #[inline(always)]
    fn live_expire_at(&self) -> Option<Option<u64>> {
        Some(None)
    }

    #[cfg(not(feature = "no-ttl"))]
    #[inline(always)]
    fn live_expire_at(&self) -> Option<Option<u64>> {
        match self.expire_at_ms {
            Some(expire_at_ms) if expire_at_ms <= ttl_now_millis() => None,
            expire_at_ms => Some(expire_at_ms),
        }
    }

    /// Returns the current value bytes when the key is still present.
    #[inline(always)]
    pub fn value(&self) -> Option<&[u8]> {
        match self.live_expire_at()? {
            None => self
                .guard
                .get_ref_hashed_shared_no_ttl(self.key_hash, self.key.as_ref()),
            Some(_) => {
                self.guard
                    .get_ref_hashed_shared(self.key_hash, self.key.as_ref(), ttl_now_millis())
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
        let semantic_generation = self.semantic_generation;
        let semantic_shadow = self.semantic_shadow;
        let key_hash = self.key_hash;
        let key = self.key.clone();
        match self.live_expire_at()? {
            None => {
                let value = self.guard.value_mut_hashed_no_ttl(key_hash, key.as_ref());
                if value.is_some() {
                    if let Some(semantic_shadow) = semantic_shadow {
                        semantic_shadow
                            .write()
                            .remove_value_hashed(key_hash, key.as_ref(), 0);
                    }
                    semantic_generation.fetch_add(1, Ordering::AcqRel);
                }
                value
            }
            Some(_) => None,
        }
    }

    /// Replaces the value without changing the key or existing TTL.
    #[inline(always)]
    pub fn set(&mut self, value: SharedBytes) {
        match self.live_expire_at() {
            Some(None) => {
                self.guard.set_value_bytes_hashed_no_ttl(
                    self.route_mode,
                    self.key_hash,
                    self.key.as_ref(),
                    value,
                );
                self.invalidate_semantic_shadow(0);
            }
            Some(expire_at_ms) => {
                let now_ms = ttl_now_millis();
                self.guard.set_value_bytes_hashed(
                    self.route_mode,
                    self.key_hash,
                    self.key.as_ref(),
                    value,
                    expire_at_ms,
                    now_ms,
                );
                self.invalidate_semantic_shadow(now_ms);
            }
            None => {
                let now_ms = ttl_now_millis();
                let removed =
                    self.guard
                        .remove_value_hashed(self.key_hash, self.key.as_ref(), now_ms);
                let semantic_removed = self.invalidate_semantic_shadow(now_ms);
                if removed.is_some() || semantic_removed.is_some() {
                    self.bump_semantic_generation();
                }
                return;
            }
        }
        self.bump_semantic_generation();
    }

    /// Replaces the value from a borrowed slice without changing the existing TTL.
    #[inline(always)]
    pub fn set_slice(&mut self, value: &[u8]) {
        self.set(SharedBytes::copy_from_slice(value));
    }

    /// Removes the entry and returns the stored bytes when present.
    #[inline(always)]
    pub fn remove(mut self) -> Option<SharedBytes> {
        let now_ms = ttl_now_millis();
        let removed = self
            .guard
            .remove_value_hashed(self.key_hash, self.key.as_ref(), now_ms);
        let semantic_removed = self.invalidate_semantic_shadow(now_ms);
        if removed.is_some() || semantic_removed.is_some() {
            self.bump_semantic_generation();
        }
        removed.or(semantic_removed)
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
    pub(super) semantic_generation: &'a AtomicU64,
    pub(super) semantic_shadow: Option<&'a SharedShardLock<EmbeddedShard>>,
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
        if let Some(semantic_shadow) = self.semantic_shadow {
            semantic_shadow
                .write()
                .remove_value_hashed(self.key_hash, self.key.as_ref(), 0);
        }
        self.semantic_generation.fetch_add(1, Ordering::AcqRel);
        RefMut {
            guard: self.guard,
            route_mode: self.route_mode,
            key: self.key,
            key_hash: self.key_hash,
            expire_at_ms: None,
            semantic_generation: self.semantic_generation,
            semantic_shadow: self.semantic_shadow,
            _not_send: PhantomData,
        }
    }
}
