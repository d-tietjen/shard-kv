use crate::storage::{EmbeddedRouteMode, EmbeddedStore};

/// SET-specific storage write selection for embedded server paths.
///
/// The server dispatcher should only route decoded envelopes. This trait keeps
/// the choice between owned, prehashed, and single-threaded SET storage paths
/// close to the SET command implementation.
pub(crate) trait EmbeddedStringWrite {
    #[inline(always)]
    fn set_decoded(
        store: &EmbeddedStore,
        key_hash: Option<u64>,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
        single_threaded: bool,
    ) {
        let routed_key_hash = key_hash.filter(|_| Self::route_hash_is_key_hash(store, key));
        EmbeddedStringWriteTarget::decoded(
            store.has_redis_objects(),
            routed_key_hash,
            single_threaded,
        )
        .write(store, key, value, ttl_ms);
    }

    #[inline(always)]
    fn set_prehashed(
        store: &EmbeddedStore,
        key_hash: u64,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
        single_threaded: bool,
    ) {
        EmbeddedStringWriteTarget::prehashed(
            store.has_redis_objects(),
            Self::route_hash_is_key_hash(store, key),
            key_hash,
            single_threaded,
        )
        .write(store, key, value, ttl_ms);
    }

    #[inline(always)]
    fn route_hash_is_key_hash(store: &EmbeddedStore, key: &[u8]) -> bool {
        store.route_mode() == EmbeddedRouteMode::FullKey || !key.starts_with(b"s:")
    }
}

enum EmbeddedStringWriteTarget {
    RedisObjectFallback,
    PrehashedShared(u64),
    PrehashedSingleThreaded(u64),
    SingleThreaded,
    OwnedFallback,
}

impl EmbeddedStringWriteTarget {
    #[inline(always)]
    fn decoded(has_redis_objects: bool, key_hash: Option<u64>, single_threaded: bool) -> Self {
        match (has_redis_objects, key_hash, single_threaded) {
            (true, _, _) => Self::RedisObjectFallback,
            (false, Some(key_hash), true) => Self::PrehashedSingleThreaded(key_hash),
            (false, Some(key_hash), false) => Self::PrehashedShared(key_hash),
            (false, None, true) => Self::SingleThreaded,
            (false, None, false) => Self::OwnedFallback,
        }
    }

    #[inline(always)]
    fn prehashed(
        has_redis_objects: bool,
        route_hash_is_key_hash: bool,
        key_hash: u64,
        single_threaded: bool,
    ) -> Self {
        match (has_redis_objects, route_hash_is_key_hash, single_threaded) {
            (true, _, _) => Self::RedisObjectFallback,
            (false, true, true) => Self::PrehashedSingleThreaded(key_hash),
            (false, true, false) => Self::PrehashedShared(key_hash),
            (false, false, true) => Self::SingleThreaded,
            (false, false, false) => Self::OwnedFallback,
        }
    }

    #[inline(always)]
    fn write(self, store: &EmbeddedStore, key: &[u8], value: &[u8], ttl_ms: Option<u64>) {
        match self {
            Self::RedisObjectFallback | Self::OwnedFallback => {
                store.set(key.to_vec(), value.to_vec(), ttl_ms);
            }
            Self::PrehashedShared(key_hash) => {
                store.set_slice_prehashed(key_hash, key, value, ttl_ms);
            }
            Self::PrehashedSingleThreaded(key_hash) => {
                // SAFETY: callers only select this target when exactly one
                // worker can access the store.
                unsafe { store.set_single_threaded_hashed(key_hash, key, value, ttl_ms) };
            }
            Self::SingleThreaded => {
                // SAFETY: callers only select this target when exactly one
                // worker can access the store.
                unsafe { store.set_single_threaded(key, value, ttl_ms) };
            }
        }
    }
}
