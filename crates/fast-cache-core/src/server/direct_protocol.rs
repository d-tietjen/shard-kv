use super::commands::{
    FastCommandDispatcher, FcnpCommandContext, FcnpCommandDispatcher, FcnpFrame,
    RawCommandDispatcher,
};
use super::fast_write::FastWriteQueue;
use super::transactions::{RespTransactionCommand, TransactionCoordinator, TransactionState};
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
pub(super) use resp::RespDirectCommand;

#[cfg(feature = "embedded")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FcnpScanCommand {
    Scan,
    ScanShard,
}

#[cfg(feature = "embedded")]
impl FcnpScanCommand {
    const NAMES: &'static [(&'static [u8], Self)] = &[
        (b"FCNP.SCAN", Self::Scan),
        (b"FCNP.SCANSHARD", Self::ScanShard),
        (b"FCNP.SCAN.SHARD", Self::ScanShard),
    ];

    pub(super) fn from_name(name: &[u8]) -> Option<Self> {
        Self::NAMES.iter().find_map(|(candidate, command)| {
            name.eq_ignore_ascii_case(candidate).then_some(*command)
        })
    }

    pub(super) fn from_parts(parts: &[&[u8]]) -> Option<Self> {
        parts.first().and_then(|name| Self::from_name(name))
    }

    #[cfg(feature = "redis")]
    pub(super) fn write_fast_response(
        self,
        store: &EmbeddedStore,
        args: &[&[u8]],
        out: &mut BytesMut,
    ) {
        match self {
            Self::Scan => crate::commands::redis::write_fcnp_scan_fast_response(store, args, out),
            Self::ScanShard => {
                crate::commands::redis::write_fcnp_scan_shard_fast_response(store, args, out);
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum FcnpDispatch {
    Complete(usize),
    Incomplete,
    Unsupported,
}
