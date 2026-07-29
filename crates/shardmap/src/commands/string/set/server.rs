use crate::commands::CommandSpec;
use crate::protocol::{FastCommand, FastRequest, FastResponse};
use crate::server::commands::{
    DirectCommandContext, DirectFastCommand, FastCommandContext, FastDirectCommand,
    RawCommandContext, RawDirectCommand,
};
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedStore, hash_key};

use super::Set;
use super::options::{SetCondition, SetOptions};
use super::storage::EmbeddedStringWrite;

#[cfg(feature = "server")]
impl RawDirectCommand for Set {
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_, '_>) {
        let RawCommandContext {
            store,
            args,
            out,
            resp_protocol,
            ..
        } = ctx;
        match SetRawArgs::from_args(store, args.as_slice()) {
            SetRawArgs::Ready { key, value, ttl_ms } => {
                if !store.point_mutation_is_accepted(key, value.len(), None) {
                    ServerWire::write_resp_error(
                        out,
                        "ERR mutation rejected by an installed storage extension",
                    );
                    return;
                }
                store.set_slice_prehashed(hash_key(key), key, value, ttl_ms);
                out.extend_from_slice(b"+OK\r\n");
            }
            SetRawArgs::Null => ServerWire::write_resp_null(out, resp_protocol),
            SetRawArgs::WrongArity => ServerWire::write_resp_error(
                out,
                &format!(
                    "ERR wrong number of arguments for '{}' command",
                    <Self as CommandSpec>::NAME
                ),
            ),
            SetRawArgs::Syntax => ServerWire::write_resp_error(out, "ERR syntax error"),
        }
    }
}

#[cfg(feature = "server")]
enum SetRawArgs<'a> {
    Ready {
        key: &'a [u8],
        value: &'a [u8],
        ttl_ms: Option<u64>,
    },
    Null,
    WrongArity,
    Syntax,
}

#[cfg(feature = "server")]
impl<'a> SetRawArgs<'a> {
    fn from_args(store: &EmbeddedStore, args: &'a [&'a [u8]]) -> Self {
        match args {
            [key, value] => Self::Ready {
                key,
                value,
                ttl_ms: None,
            },
            [key, value, rest @ ..] => match SetOptions::parse(rest) {
                Some(options) => Self::from_options(store, key, value, options),
                None => Self::Syntax,
            },
            _ => Self::WrongArity,
        }
    }

    fn from_options(
        store: &EmbeddedStore,
        key: &'a [u8],
        value: &'a [u8],
        options: SetOptions,
    ) -> Self {
        let allowed = match options.condition {
            SetCondition::Always => true,
            SetCondition::Nx => !store.exists(key),
            SetCondition::Xx => store.exists(key),
        };
        match allowed {
            true => Self::Ready {
                key,
                value,
                ttl_ms: options.ttl_ms(store, key),
            },
            false => Self::Null,
        }
    }
}

#[cfg(feature = "server")]
impl EmbeddedStringWrite for Set {}

#[cfg(feature = "server")]
impl DirectFastCommand for Set {
    fn execute_direct_fast(
        &self,
        ctx: DirectCommandContext,
        request: FastRequest<'_>,
    ) -> FastResponse {
        match request.command {
            FastCommand::Set { key, value } => {
                ctx.set_owned(key.to_vec(), value.to_vec(), None);
                FastResponse::Ok
            }
            _ => FastResponse::Error(b"ERR unsupported command".to_vec()),
        }
    }
}

#[cfg(feature = "server")]
impl FastDirectCommand for Set {
    fn execute_fast(&self, ctx: FastCommandContext<'_, '_>, command: FastCommand<'_>) {
        match command {
            FastCommand::Set { key, value } => {
                if !<Self as EmbeddedStringWrite>::set_decoded(
                    ctx.store,
                    ctx.key_hash,
                    key,
                    value,
                    None,
                    ctx.single_threaded,
                ) {
                    ServerWire::write_fast_error(
                        ctx.out,
                        "ERR mutation rejected by an installed storage extension",
                    );
                } else {
                    ServerWire::write_fast_ok(ctx.out);
                }
            }
            _ => ServerWire::write_fast_error(ctx.out, "ERR unsupported command"),
        }
    }
}
