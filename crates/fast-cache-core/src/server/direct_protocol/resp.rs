use super::*;
use crate::server::commands::{RawCommandContext, RawDirectCommand};

impl DirectProtocol {
    #[cfg(feature = "embedded")]
    #[inline(always)]
    #[allow(dead_code)]
    pub(in crate::server) fn try_resp_direct_dispatch<'a>(
        buf: &'a [u8],
    ) -> Option<(usize, RespDirectCommand<'a>)> {
        let (pos, command, args) = DirectProtocol::try_resp_command_parts(buf)?;
        DirectProtocol::parse_resp_direct_command(command, args).map(|command| (pos, command))
    }

    #[inline(always)]
    pub(in crate::server) fn try_resp_command_parts<'a>(
        buf: &'a [u8],
    ) -> Option<(usize, &'a [u8], RespDirectArgs<'a>)> {
        let (arg_count, mut pos) = DirectProtocol::read_resp_array_header(buf)?;
        match RespDirectArgCount::from_raw(arg_count) {
            RespDirectArgCount::Supported => {
                let command = DirectProtocol::read_resp_bulk_arg(buf, &mut pos)?;
                let mut args = RespDirectArgs::new();
                for _ in 1..arg_count {
                    args.push(DirectProtocol::read_resp_bulk_arg(buf, &mut pos)?);
                }
                Some((pos, command, args))
            }
            RespDirectArgCount::Unsupported => None,
        }
    }

    #[cfg(feature = "embedded")]
    pub(in crate::server) fn parse_resp_direct_command<'a>(
        command: &'a [u8],
        args: RespDirectArgs<'a>,
    ) -> Option<RespDirectCommand<'a>> {
        RawCommandDispatcher::find(command).map(|handler| RespDirectCommand { handler, args })
    }

    #[cfg(feature = "embedded")]
    pub(in crate::server) fn parse_redis_opcode_direct_command<'a>(
        kind: crate::protocol::FastCommandKind,
        args: RespDirectArgs<'a>,
    ) -> Option<RespDirectCommand<'a>> {
        RawCommandDispatcher::find_redis_opcode(kind)
            .map(|handler| RespDirectCommand { handler, args })
    }
}

enum RespDirectArgCount {
    Supported,
    Unsupported,
}

impl RespDirectArgCount {
    fn from_raw(arg_count: usize) -> Self {
        match arg_count {
            1..=64 => Self::Supported,
            _ => Self::Unsupported,
        }
    }
}

#[cfg(feature = "embedded")]
pub(crate) struct RespDirectCommand<'a> {
    handler: &'static dyn RawDirectCommand,
    args: RespDirectArgs<'a>,
}

#[cfg(feature = "embedded")]
impl RespDirectCommand<'_> {
    pub(super) fn mutates_value(&self) -> bool {
        self.handler.mutates_value()
    }

    pub(super) fn try_execute_owned_shard(
        &self,
        store: &EmbeddedStore,
        out: &mut BytesMut,
        owned_shard_id: usize,
    ) -> bool {
        self.handler
            .execute_owned_shard(store, self.args.as_slice(), out, owned_shard_id)
    }

    fn execute(
        self,
        store: &EmbeddedStore,
        out: &mut BytesMut,
        fast_write_queue: Option<&mut FastWriteQueue>,
        single_threaded: bool,
        _started_at: Instant,
    ) {
        self.handler.execute(RawCommandContext {
            store,
            args: self.args,
            out,
            fast_write_queue,
            single_threaded,
        });
    }

    fn execute_fast(
        self,
        store: &EmbeddedStore,
        out: &mut BytesMut,
        fast_write_queue: Option<&mut FastWriteQueue>,
        single_threaded: bool,
        _started_at: Instant,
    ) {
        self.handler.execute_fast(RawCommandContext {
            store,
            args: self.args,
            out,
            fast_write_queue,
            single_threaded,
        });
    }
}

impl DirectProtocol {
    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(in crate::server) fn shared_execute_resp_direct_cmd_into(
        store: &EmbeddedStore,
        command: RespDirectCommand<'_>,
        out: &mut BytesMut,
        fast_write_queue: Option<&mut FastWriteQueue>,
        single_threaded: bool,
        started_at: Instant,
    ) {
        command.execute(store, out, fast_write_queue, single_threaded, started_at);
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(in crate::server) fn shared_execute_fast_direct_cmd_into(
        store: &EmbeddedStore,
        command: RespDirectCommand<'_>,
        out: &mut BytesMut,
        fast_write_queue: Option<&mut FastWriteQueue>,
        single_threaded: bool,
        started_at: Instant,
    ) {
        command.execute_fast(store, out, fast_write_queue, single_threaded, started_at);
    }
}
