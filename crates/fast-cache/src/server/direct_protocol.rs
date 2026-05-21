use super::commands::{
    FastCommandDispatcher, FcnpCommandContext, FcnpCommandDispatcher, FcnpFrame,
    RawCommandDispatcher,
};
use super::fast_write::FastWriteQueue;
use super::transactions::{TransactionCoordinator, TransactionState};
use super::wire::*;
use super::*;

mod borrowed;
mod fast;
mod fcnp;
mod read;
mod request;
mod resp;

#[cfg(feature = "embedded")]
pub(in crate::server) use request::SharedRequestBufferContext;

pub(super) struct DirectProtocol;

#[cfg(feature = "embedded")]
pub(super) type RespDirectArgs<'a> = smallvec::SmallVec<[&'a [u8]; 8]>;

#[cfg(feature = "embedded")]
pub(super) type RespDirectCommandBox<'a> = Box<dyn RespDirectCommand<'a> + 'a>;

#[cfg(feature = "embedded")]
pub(super) trait RespDirectCommand<'a> {
    fn mutates_value(&self) -> bool;

    fn execute(
        self: Box<Self>,
        store: &EmbeddedStore,
        out: &mut BytesMut,
        fast_write_queue: Option<&mut FastWriteQueue>,
        single_threaded: bool,
        started_at: Instant,
    );
}

#[derive(Debug)]
pub(crate) enum FcnpDispatch {
    Complete(usize),
    Incomplete,
    Unsupported,
}
