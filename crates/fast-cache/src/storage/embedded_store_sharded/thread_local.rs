use super::*;

impl WorkerLocalEmbeddedStore {
    /// Install this store into the current thread's local worker slot.
    ///
    /// Only one worker-local store can be installed per thread. Use
    /// [`crate::storage::take_local_embedded_store`] to remove it again.
    pub fn install_local(self) -> Result<(), LocalStoreInstallError> {
        THREAD_LOCAL_EMBEDDED_STORE.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot.is_some() {
                return Err(LocalStoreInstallError::AlreadyInstalled);
            }
            *slot = Some(self);
            Ok(())
        })
    }
}

/// Run a closure against the worker-local embedded store installed on this thread.
pub fn with_thread_local_embedded_store<R>(
    f: impl FnOnce(&mut WorkerLocalEmbeddedStore) -> R,
) -> Result<R, LocalStoreAccessError> {
    THREAD_LOCAL_EMBEDDED_STORE.with(|slot| {
        let mut slot = slot.borrow_mut();
        let store = slot.as_mut().ok_or(LocalStoreAccessError::NotInstalled)?;
        Ok(f(store))
    })
}

/// Remove and return the worker-local embedded store installed on this thread.
pub fn take_thread_local_embedded_store() -> Option<WorkerLocalEmbeddedStore> {
    THREAD_LOCAL_EMBEDDED_STORE.with(|slot| slot.borrow_mut().take())
}
