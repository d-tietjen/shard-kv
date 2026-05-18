use crate::commands::CommandSpec;
use crate::protocol::{FastCommand, FastRequest, FastResponse};
use crate::server::commands::{
    BorrowedCommandContext, DirectCommandContext, DirectFastCommand, FastCommandContext,
    FastDirectCommand, RawCommandContext, RawDirectCommand,
};
use crate::server::wire::ServerWire;
use crate::storage::EmbeddedStore;

use super::Del;

#[cfg(feature = "server")]
impl RawDirectCommand for Del {
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_>) {
        let RawCommandContext { store, args, out } = ctx;
        match DelRawArgs::from_args(args.as_slice()) {
            DelRawArgs::Ready { keys } => {
                ServerWire::write_resp_integer(out, Del::delete_embedded_keys(store, keys));
            }
            DelRawArgs::WrongArity => ServerWire::write_resp_error(
                out,
                &format!(
                    "ERR wrong number of arguments for '{}' command",
                    <Self as CommandSpec>::NAME
                ),
            ),
        }
    }
}

#[cfg(feature = "server")]
enum DelRawArgs<'a> {
    Ready { keys: &'a [&'a [u8]] },
    WrongArity,
}

#[cfg(feature = "server")]
impl<'a> DelRawArgs<'a> {
    fn from_args(args: &'a [&'a [u8]]) -> Self {
        match args {
            [] => Self::WrongArity,
            keys => Self::Ready { keys },
        }
    }
}

#[cfg(feature = "server")]
impl Del {
    pub(super) fn execute_borrowed(ctx: BorrowedCommandContext<'_, '_, '_>, keys: &[&[u8]]) {
        let deleted = Self::delete_embedded_keys(ctx.store, keys);
        ServerWire::write_resp_integer(ctx.out, deleted);
    }

    pub(super) fn delete_embedded_keys(store: &EmbeddedStore, keys: &[&[u8]]) -> i64 {
        keys.iter().filter(|key| store.delete(key)).count() as i64
    }
}

#[cfg(feature = "server")]
impl DirectFastCommand for Del {
    fn execute_direct_fast(
        &self,
        ctx: DirectCommandContext,
        request: FastRequest<'_>,
    ) -> FastResponse {
        match request.command {
            FastCommand::Delete { key } => FastResponse::Integer(ctx.delete(key) as i64),
            _ => FastResponse::Error(b"ERR unsupported command".to_vec()),
        }
    }
}

#[cfg(feature = "server")]
impl FastDirectCommand for Del {
    fn execute_fast(&self, ctx: FastCommandContext<'_, '_>, command: FastCommand<'_>) {
        match command {
            FastCommand::Delete { key } => {
                let deleted = ctx.store.delete(key);
                ServerWire::write_fast_integer(ctx.out, deleted as i64);
            }
            _ => ServerWire::write_fast_error(ctx.out, "ERR unsupported command"),
        }
    }
}
