use super::*;

impl WorkerLocalEmbeddedStoreBootstrap {
    pub fn from_embedded(store: EmbeddedStore, worker_count: usize) -> Self {
        store.into_local_store_bootstrap(worker_count)
    }

    pub fn len(&self) -> usize {
        self.stores.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stores.is_empty()
    }

    pub fn into_stores(self) -> Vec<WorkerLocalEmbeddedStore> {
        self.stores
    }
}

impl EmbeddedStore {
    /// Partition this store into worker-local embedded stores.
    pub fn into_local_store_bootstrap(
        self,
        worker_count: usize,
    ) -> WorkerLocalEmbeddedStoreBootstrap {
        WorkerLocalEmbeddedStoreBootstrap {
            stores: self
                .into_owned_workers(worker_count)
                .into_iter()
                .map(WorkerLocalEmbeddedStore::from_owned_worker)
                .collect(),
        }
    }

    /// Partition this store and return the worker-local stores directly.
    pub fn into_local_stores(self, worker_count: usize) -> Vec<WorkerLocalEmbeddedStore> {
        self.into_local_store_bootstrap(worker_count).into_stores()
    }
}
