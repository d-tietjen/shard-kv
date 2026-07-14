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
    Topology,
}

#[cfg(feature = "embedded")]
impl ScnpScanCommand {
    const NAMES: &'static [(&'static [u8], Self)] = &[
        (b"SCNP.SCAN", Self::Scan),
        (b"SCNP.SCANSHARD", Self::ScanShard),
        (b"SCNP.SCAN.SHARD", Self::ScanShard),
        (b"SCNP.TOPOLOGY", Self::Topology),
    ];

    pub(super) fn from_name(name: &[u8]) -> Option<Self> {
        Self::NAMES.iter().find_map(|(candidate, command)| {
            name.eq_ignore_ascii_case(candidate).then_some(*command)
        })
    }

    pub(super) fn from_parts(parts: &[&[u8]]) -> Option<Self> {
        parts.first().and_then(|name| Self::from_name(name))
    }

    pub(super) fn write_fast_response(
        self,
        store: &EmbeddedStore,
        args: &[&[u8]],
        out: &mut BytesMut,
    ) {
        match self {
            Self::Topology => {
                if !args.is_empty() {
                    ServerWire::write_fast_error(
                        out,
                        "ERR wrong number of arguments for SCNP.TOPOLOGY",
                    );
                    return;
                }
                let (node_id, direct_base_port) =
                    store.overflow_replica_topology().unwrap_or_default();
                let payload = format!(
                    "{}\t{}\t{}\t{}\toverflow_slot_v1",
                    node_id,
                    store.shard_count(),
                    store.route_mode().as_str(),
                    direct_base_port,
                );
                let start = ServerWire::begin_fast_value(out);
                ServerWire::write_resp_blob_string(out, payload.as_bytes());
                ServerWire::finish_fast_value(out, start);
            }
            #[cfg(feature = "redis")]
            Self::Scan => crate::commands::redis::write_scnp_scan_fast_response(store, args, out),
            #[cfg(not(feature = "redis"))]
            Self::Scan => ServerWire::write_fast_error(out, "ERR SCNP.SCAN requires redis support"),
            #[cfg(feature = "redis")]
            Self::ScanShard => {
                crate::commands::redis::write_scnp_scan_shard_fast_response(store, args, out);
            }
            #[cfg(not(feature = "redis"))]
            Self::ScanShard => {
                ServerWire::write_fast_error(out, "ERR SCNP.SCANSHARD requires redis support");
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
