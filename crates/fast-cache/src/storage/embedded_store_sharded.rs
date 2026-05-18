use std::cell::RefCell;
use std::fmt;
use std::marker::PhantomData;
use std::ops::ControlFlow;
use std::rc::Rc;

use crate::cuda::{
    CudaChunkTransferHit, CudaSessionChunkEvent, CudaSessionTransferRequest,
    CudaSessionTransferStats,
};
use crate::storage::{
    Bytes, EmbeddedKeyRoute, EmbeddedReadSlice, EmbeddedRouteMode, EmbeddedSessionRoute,
    EmbeddedStore, OwnedEmbeddedBatchReadView, OwnedEmbeddedReadView,
    OwnedEmbeddedSessionPackedView, OwnedEmbeddedWorkerShards, PackedBatch, PackedSessionWrite,
    PreparedPointKey, TierStatsSnapshot,
};

thread_local! {
    static THREAD_LOCAL_EMBEDDED_STORE: RefCell<Option<WorkerLocalEmbeddedStore>> = const { RefCell::new(None) };
}

/// Worker-owned view of the shards assigned to one local server worker.
///
/// This is the sharded counterpart to [`EmbeddedStore`]. An `EmbeddedStore` can
/// be split into one `WorkerLocalEmbeddedStore` per worker. Each handle owns
/// only its assigned shards and exposes local operations through `&mut self`,
/// letting hot paths skip shared `RwLock` traffic when the key or session route
/// belongs to this worker.
///
/// Methods ending in `_if_local` return [`LocalRouteError`] when a key/session
/// routes to another worker. Methods ending in `_local` assume the caller has
/// already checked routing and use debug assertions for that contract.
#[derive(Debug)]
pub struct WorkerLocalEmbeddedStore {
    inner: OwnedEmbeddedWorkerShards,
}

/// Temporary container produced while partitioning an [`EmbeddedStore`] across workers.
///
/// `from_embedded` consumes the shared store and produces one
/// [`WorkerLocalEmbeddedStore`] per requested worker. Call [`Self::into_stores`]
/// to hand those stores to worker threads or runtimes.
#[derive(Debug)]
pub struct WorkerLocalEmbeddedStoreBootstrap {
    stores: Vec<WorkerLocalEmbeddedStore>,
}
mod bootstrap;
mod core;
mod errors;
mod lifecycle;
mod read;
#[cfg(test)]
mod tests;
mod thread_local;
mod views;
mod write;

pub use errors::{LocalRouteError, LocalStoreAccessError, LocalStoreInstallError};
pub use thread_local::{take_thread_local_embedded_store, with_thread_local_embedded_store};
use views::worker_local_batch_view_from_embedded;
pub use views::{
    WorkerLocalBatchReadView, WorkerLocalReadSlice, WorkerLocalReadView,
    WorkerLocalSessionBatchView,
};
