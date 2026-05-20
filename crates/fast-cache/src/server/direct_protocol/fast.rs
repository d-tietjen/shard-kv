use super::*;

impl DirectProtocol {
    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(in crate::server) fn fast_command_mutates_value(command: &FastCommand<'_>) -> bool {
        match command {
            FastCommand::RespCommand { parts } => parts
                .first()
                .is_some_and(|command| RawCommandDispatcher::mutates_value(command)),
            other => FastCommandDispatcher::mutates_value(other),
        }
    }
}

impl DirectProtocol {
    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub(in crate::server) fn shared_execute_fast_into(
        store: &EmbeddedStore,
        request: FastRequest<'_>,
        out: &mut BytesMut,
        single_threaded: bool,
        started_at: Instant,
    ) {
        match request.command {
            FastCommand::RespCommand { parts } => {
                DirectProtocol::execute_fast_resp_command(store, parts, out, started_at)
            }
            _ => match FastCommandDispatcher::execute(store, request, out, single_threaded) {
                true => {}
                false => ServerWire::write_fast_error(out, "ERR unsupported command"),
            },
        }
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    fn execute_fast_resp_command(
        store: &EmbeddedStore,
        parts: Vec<&[u8]>,
        out: &mut BytesMut,
        started_at: Instant,
    ) {
        match parts.split_first() {
            Some((command, args)) => {
                #[cfg(feature = "redis-compat")]
                if command.eq_ignore_ascii_case(b"FCNP.SCAN") {
                    let mut resp = BytesMut::new();
                    crate::commands::redis_compat::write_fcnp_scan_response(store, args, &mut resp);
                    ServerWire::write_fast_value(out, &resp);
                    return;
                }
                #[cfg(feature = "redis-compat")]
                if command.eq_ignore_ascii_case(b"FCNP.SCANSHARD")
                    || command.eq_ignore_ascii_case(b"FCNP.SCAN.SHARD")
                {
                    let mut resp = BytesMut::new();
                    crate::commands::redis_compat::write_fcnp_scan_shard_response(
                        store, args, &mut resp,
                    );
                    ServerWire::write_fast_value(out, &resp);
                    return;
                }
                let mut direct_args = RespDirectArgs::new();
                direct_args.extend(args.iter().copied());
                match DirectProtocol::parse_resp_direct_command(command, direct_args) {
                    Some(command) => {
                        let mut resp = BytesMut::new();
                        DirectProtocol::shared_execute_resp_direct_cmd_into(
                            store, command, &mut resp, None, false, started_at,
                        );
                        ServerWire::write_fast_value(out, &resp);
                    }
                    None => ServerWire::write_fast_error(out, "ERR unsupported command"),
                }
            }
            None => {
                ServerWire::write_fast_error(out, "ERR empty RESP command");
            }
        }
    }
}
