use super::*;

impl DirectProtocol {
    #[cfg(feature = "embedded")]
    #[inline(always)]
    #[allow(dead_code)]
    pub(in crate::server) fn try_resp_direct_dispatch<'a>(
        buf: &'a [u8],
    ) -> Option<(usize, RespDirectCommandBox<'a>)> {
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
    ) -> Option<RespDirectCommandBox<'a>> {
        RawCommandDispatcher::supports(command)
            .then(|| Box::new(RawRespDirectCommand { command, args }) as RespDirectCommandBox<'a>)
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
struct RawRespDirectCommand<'a> {
    command: &'a [u8],
    args: RespDirectArgs<'a>,
}

#[cfg(feature = "embedded")]
impl<'a> RespDirectCommand<'a> for RawRespDirectCommand<'a> {
    fn mutates_value(&self) -> bool {
        RawCommandDispatcher::mutates_value(self.command)
    }

    fn execute(
        self: Box<Self>,
        store: &EmbeddedStore,
        out: &mut BytesMut,
        _fast_write_queue: Option<&mut FastWriteQueue>,
        _single_threaded: bool,
        started_at: Instant,
    ) {
        RawCommandDispatcher::execute(store, self.command, self.args, out, started_at);
    }
}

impl DirectProtocol {
    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(in crate::server) fn shared_execute_resp_direct_cmd_into(
        store: &EmbeddedStore,
        command: RespDirectCommandBox<'_>,
        out: &mut BytesMut,
        fast_write_queue: Option<&mut FastWriteQueue>,
        single_threaded: bool,
        started_at: Instant,
    ) {
        command.execute(store, out, fast_write_queue, single_threaded, started_at);
    }
}
