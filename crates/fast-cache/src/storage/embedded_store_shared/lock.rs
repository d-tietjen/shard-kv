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
