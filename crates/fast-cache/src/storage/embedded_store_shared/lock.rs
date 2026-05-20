use super::*;

#[derive(Debug)]
pub(super) enum SharedShardLock<T> {
    ReadBiased(ReadBiasedRwLock<T>),
    Fair(FairRwLock<T>),
}

pub(super) enum SharedReadGuard<'a, T> {
    ReadBiased(ReadBiasedRwLockReadGuard<'a, T>),
    Fair(FairRwLockReadGuard<'a, T>),
}

pub(super) enum SharedWriteGuard<'a, T> {
    ReadBiased(ReadBiasedRwLockWriteGuard<'a, T>),
    Fair(FairRwLockWriteGuard<'a, T>),
}

impl<T> SharedShardLock<T> {
    #[inline(always)]
    pub(super) fn new(value: T, policy: SharedEmbeddedLockPolicy) -> Self {
        match policy {
            SharedEmbeddedLockPolicy::ReadBiased => Self::ReadBiased(ReadBiasedRwLock::new(value)),
            SharedEmbeddedLockPolicy::Fair => Self::Fair(FairRwLock::new(value)),
        }
    }

    #[inline(always)]
    pub(super) fn read(&self) -> SharedReadGuard<'_, T> {
        match self {
            Self::ReadBiased(lock) => SharedReadGuard::ReadBiased(lock.read()),
            Self::Fair(lock) => SharedReadGuard::Fair(lock.read()),
        }
    }

    #[inline(always)]
    pub(super) fn write(&self) -> SharedWriteGuard<'_, T> {
        match self {
            Self::ReadBiased(lock) => SharedWriteGuard::ReadBiased(lock.write()),
            Self::Fair(lock) => SharedWriteGuard::Fair(lock.write()),
        }
    }
}

#[inline(always)]
fn point_ref_hashed<'a>(shard: &'a EmbeddedShard, hash: u64, key: &[u8]) -> Option<&'a [u8]> {
    #[cfg(feature = "no-ttl")]
    {
        shard.get_ref_hashed_shared_no_ttl(hash, key)
    }
    #[cfg(not(feature = "no-ttl"))]
    {
        if shard.has_no_ttl_entries() {
            shard.get_ref_hashed_shared_no_ttl(hash, key)
        } else {
            shard.get_ref_hashed_shared(hash, key, ttl_now_millis())
        }
    }
}

#[inline(always)]
fn point_ref_prepared<'a>(
    shard: &'a EmbeddedShard,
    prepared: &PreparedPointKey,
) -> Option<&'a [u8]> {
    #[cfg(feature = "no-ttl")]
    {
        shard.get_ref_hashed_shared_prepared_no_ttl(
            prepared.route().key_hash,
            prepared.key(),
            prepared.key_tag(),
        )
    }
    #[cfg(not(feature = "no-ttl"))]
    {
        if shard.has_no_ttl_entries() {
            shard.get_ref_hashed_shared_prepared_no_ttl(
                prepared.route().key_hash,
                prepared.key(),
                prepared.key_tag(),
            )
        } else {
            shard.get_ref_hashed_shared(prepared.route().key_hash, prepared.key(), ttl_now_millis())
        }
    }
}

#[inline(always)]
fn point_value_bytes(shard: &EmbeddedShard, hash: u64, key: &[u8]) -> Option<SharedBytes> {
    #[cfg(feature = "no-ttl")]
    {
        shard
            .get_shared_value_bytes_hashed_no_ttl(hash, key)
            .cloned()
    }
    #[cfg(not(feature = "no-ttl"))]
    {
        if shard.has_no_ttl_entries() {
            shard
                .get_shared_value_bytes_hashed_no_ttl(hash, key)
                .cloned()
        } else {
            shard
                .get_shared_value_bytes_hashed(hash, key, ttl_now_millis())
                .cloned()
        }
    }
}

#[inline(always)]
fn point_value_bytes_prepared(
    shard: &EmbeddedShard,
    prepared: &PreparedPointKey,
) -> Option<SharedBytes> {
    #[cfg(feature = "no-ttl")]
    {
        shard
            .get_shared_value_bytes_hashed_prepared_no_ttl(
                prepared.route().key_hash,
                prepared.key(),
                prepared.key_tag(),
            )
            .cloned()
    }
    #[cfg(not(feature = "no-ttl"))]
    {
        if shard.has_no_ttl_entries() {
            shard
                .get_shared_value_bytes_hashed_prepared_no_ttl(
                    prepared.route().key_hash,
                    prepared.key(),
                    prepared.key_tag(),
                )
                .cloned()
        } else {
            shard
                .get_shared_value_bytes_hashed(
                    prepared.route().key_hash,
                    prepared.key(),
                    ttl_now_millis(),
                )
                .cloned()
        }
    }
}

impl SharedReadGuard<'_, EmbeddedShard> {
    #[inline(always)]
    pub(super) fn point_ref_hashed(&self, hash: u64, key: &[u8]) -> Option<&[u8]> {
        match self {
            Self::ReadBiased(guard) => point_ref_hashed(guard, hash, key),
            Self::Fair(guard) => point_ref_hashed(guard, hash, key),
        }
    }

    #[inline(always)]
    pub(super) fn point_ref_prepared(&self, prepared: &PreparedPointKey) -> Option<&[u8]> {
        match self {
            Self::ReadBiased(guard) => point_ref_prepared(guard, prepared),
            Self::Fair(guard) => point_ref_prepared(guard, prepared),
        }
    }

    #[inline(always)]
    pub(super) fn point_value_bytes(&self, hash: u64, key: &[u8]) -> Option<SharedBytes> {
        match self {
            Self::ReadBiased(guard) => point_value_bytes(guard, hash, key),
            Self::Fair(guard) => point_value_bytes(guard, hash, key),
        }
    }

    #[inline(always)]
    pub(super) fn point_value_bytes_prepared(
        &self,
        prepared: &PreparedPointKey,
    ) -> Option<SharedBytes> {
        match self {
            Self::ReadBiased(guard) => point_value_bytes_prepared(guard, prepared),
            Self::Fair(guard) => point_value_bytes_prepared(guard, prepared),
        }
    }

    #[inline(always)]
    pub(super) fn contains_point_hashed(&self, hash: u64, key: &[u8]) -> bool {
        self.point_ref_hashed(hash, key).is_some()
    }
}

impl SharedWriteGuard<'_, EmbeddedShard> {
    #[inline(always)]
    pub(super) fn set_slice_hashed_no_ttl(
        &mut self,
        route_mode: EmbeddedRouteMode,
        key_hash: u64,
        key: &[u8],
        value: &[u8],
    ) {
        match self {
            Self::ReadBiased(guard) => {
                guard.set_slice_hashed_no_ttl(route_mode, key_hash, key, value)
            }
            Self::Fair(guard) => guard.set_slice_hashed_no_ttl(route_mode, key_hash, key, value),
        }
    }
}

impl<T> Deref for SharedReadGuard<'_, T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        match self {
            Self::ReadBiased(guard) => guard,
            Self::Fair(guard) => guard,
        }
    }
}

impl<T> Deref for SharedWriteGuard<'_, T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        match self {
            Self::ReadBiased(guard) => guard,
            Self::Fair(guard) => guard,
        }
    }
}

impl<T> DerefMut for SharedWriteGuard<'_, T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::ReadBiased(guard) => guard,
            Self::Fair(guard) => guard,
        }
    }
}
