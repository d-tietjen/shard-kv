use super::commands::{
    FastCommandDispatcher, RawCommandDispatcher, ScnpCommandContext, ScnpCommandDispatcher,
    ScnpFrame,
};
use super::fast_write::FastWriteQueue;
use super::transactions::{RespTransactionCommand, TransactionCoordinator, TransactionState};
use super::wire::*;
use super::*;

mod borrowed;
mod fast;
mod read;
mod request;
mod resp;
mod scnp;

#[cfg(feature = "embedded")]
pub(in crate::server) use request::SharedRequestBufferContext;

pub(super) struct DirectProtocol;

#[cfg(feature = "embedded")]
pub(super) type RespDirectArgs<'a> = smallvec::SmallVec<[&'a [u8]; 8]>;

#[cfg(feature = "embedded")]
pub(super) use resp::RespDirectCommand;

#[cfg(feature = "embedded")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ScnpScanCommand {
    Scan,
    ScanShard,
}

#[cfg(feature = "embedded")]
impl ScnpScanCommand {
    const NAMES: &'static [(&'static [u8], Self)] = &[
        (b"SCNP.SCAN", Self::Scan),
        (b"SCNP.SCANSHARD", Self::ScanShard),
        (b"SCNP.SCAN.SHARD", Self::ScanShard),
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
            Self::Scan => crate::commands::redis::write_scnp_scan_fast_response(store, args, out),
            Self::ScanShard => {
                crate::commands::redis::write_scnp_scan_shard_fast_response(store, args, out);
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum ScnpDispatch {
    Complete(usize),
    Incomplete,
    Unsupported,
}
