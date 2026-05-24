use crate::commands::CommandSpec;
use crate::protocol::{FastCommand, FastRequest, FastResponse};
use crate::server::commands::{
    BorrowedCommandContext, DirectCommandContext, DirectFastCommand, FastCommandContext,
    FastDirectCommand, RawCommandContext, RawDirectCommand,
};
use crate::server::wire::ServerWire;
use crate::storage::hash_key;
#[cfg(feature = "redis-compat")]
use crate::storage::{RedisStringLookup, WRONGTYPE_MESSAGE};

use super::Get;

#[cfg(feature = "server")]
impl RawDirectCommand for Get {
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_, '_>) {
        let RawCommandContext {
            store, args, out, ..
        } = ctx;
        match GetRawArgs::from_args(args.as_slice()) {
            GetRawArgs::Ready { key } => match store.get(key) {
                Some(value) => ServerWire::write_resp_blob_string(out, &value),
                None => out.extend_from_slice(b"$-1\r\n"),
            },
            GetRawArgs::WrongArity => ServerWire::write_resp_error(
                out,
                &format!(
                    "ERR wrong number of arguments for '{}' command",
                    <Self as CommandSpec>::NAME
                ),
            ),
        }
    }

    fn execute_fast(&self, mut ctx: RawCommandContext<'_, '_, '_, '_>) {
        match GetRawArgs::from_args(ctx.args.as_slice()) {
            GetRawArgs::Ready { key } => {
                let key_hash = hash_key(key);
                let out = &mut *ctx.out;
                let found = if ctx.single_threaded {
                    // SAFETY: forwarded from the connection worker's single-threaded contract.
                    unsafe {
                        ctx.store
                            .with_shared_value_bytes_route_hashed_single_threaded(
                                key_hash,
                                key,
                                |value| {
                                    if let Some(queue) = ctx.fast_write_queue.as_mut() {
                                        queue.push_fast_value(out, value);
                                    } else {
                                        ServerWire::write_fast_value(out, value.as_ref());
                                    }
                                },
                            )
                    }
                } else {
                    ctx.store
                        .with_shared_value_bytes_route_hashed(key_hash, key, |value| {
                            if let Some(queue) = ctx.fast_write_queue.as_mut() {
                                queue.push_fast_value(out, value);
                            } else {
                                ServerWire::write_fast_value(out, value.as_ref());
                            }
                        })
                };
                if !found {
                    ServerWire::write_fast_null(ctx.out);
                }
            }
            GetRawArgs::WrongArity => ServerWire::write_fast_error(
                ctx.out,
                &format!(
                    "ERR wrong number of arguments for '{}' command",
                    <Self as CommandSpec>::NAME
                ),
            ),
        }
    }
}

#[cfg(feature = "server")]
enum GetRawArgs<'a> {
    Ready { key: &'a [u8] },
    WrongArity,
}

#[cfg(feature = "server")]
impl<'a> GetRawArgs<'a> {
    fn from_args(args: &'a [&'a [u8]]) -> Self {
        match args {
            [key] => Self::Ready { key },
            _ => Self::WrongArity,
        }
    }
}

#[cfg(feature = "server")]
impl Get {
    #[inline(always)]
    pub(super) fn execute_borrowed_single_threaded(
        ctx: &mut BorrowedCommandContext<'_, '_, '_>,
        key: &[u8],
    ) {
        #[cfg(feature = "redis-compat")]
        if ctx.store.has_redis_objects() {
            Self::write_resp_string_lookup(ctx, key);
            return;
        }
        let hit = if let Some(queue) = ctx.fast_write_queue.as_mut() {
            let key_hash = hash_key(key);
            let out = &mut *ctx.out;
            // SAFETY: forwarded from caller's single-worker contract.
            unsafe {
                ctx.store
                    .with_shared_value_bytes_route_hashed_single_threaded(key_hash, key, |value| {
                        queue.push_resp_value(out, value)
                    })
            }
        } else {
            // SAFETY: forwarded from caller's single-worker contract.
            unsafe { ctx.store.get_blob_string_into_single_threaded(key, ctx.out) }
        };
        match hit {
            true => {}
            false => ctx.out.extend_from_slice(b"$-1\r\n"),
        }
    }

    #[inline(always)]
    pub(super) fn execute_borrowed_shared(
        ctx: &mut BorrowedCommandContext<'_, '_, '_>,
        key: &[u8],
    ) {
        #[cfg(feature = "redis-compat")]
        if ctx.store.has_redis_objects() {
            Self::write_resp_string_lookup(ctx, key);
            return;
        }
        let hit = if let Some(queue) = ctx.fast_write_queue.as_mut() {
            let key_hash = hash_key(key);
            let out = &mut *ctx.out;
            ctx.store
                .with_shared_value_bytes_route_hashed(key_hash, key, |value| {
                    queue.push_resp_value(out, value)
                })
        } else {
            ctx.store.get_blob_string_into(key, ctx.out)
        };
        match hit {
            true => {}
            false => ctx.out.extend_from_slice(b"$-1\r\n"),
        }
    }

    #[cfg(feature = "redis-compat")]
    #[inline(always)]
    fn write_resp_string_lookup(ctx: &mut BorrowedCommandContext<'_, '_, '_>, key: &[u8]) {
        let lookup = if let Some(queue) = ctx.fast_write_queue.as_mut() {
            let out = &mut *ctx.out;
            ctx.store.get_string_value_into(key, |value| {
                queue.push_resp_value(out, value);
            })
        } else {
            ctx.store.get_string_value_into(key, |value| {
                ServerWire::write_resp_blob_string(ctx.out, value.as_ref());
            })
        };
        match lookup {
            RedisStringLookup::Hit => {}
            RedisStringLookup::Miss => ctx.out.extend_from_slice(b"$-1\r\n"),
            RedisStringLookup::WrongType => {
                ServerWire::write_resp_error(ctx.out, WRONGTYPE_MESSAGE)
            }
        }
    }
}

#[cfg(feature = "server")]
impl DirectFastCommand for Get {
    fn execute_direct_fast(
        &self,
        ctx: DirectCommandContext,
        request: FastRequest<'_>,
    ) -> FastResponse {
        match request.command {
            FastCommand::Get { key } => {
                ctx.get(key).map_or(FastResponse::Null, FastResponse::Value)
            }
            _ => FastResponse::Error(b"ERR unsupported command".to_vec()),
        }
    }
}

#[cfg(feature = "server")]
impl FastDirectCommand for Get {
    fn execute_fast(&self, mut ctx: FastCommandContext<'_, '_>, command: FastCommand<'_>) {
        match command {
            FastCommand::Get { key } => {
                #[cfg(feature = "redis-compat")]
                if ctx.store.has_redis_objects() {
                    match ctx.store.get_string_value_into(key, |value| {
                        ServerWire::write_fast_value(ctx.out, value)
                    }) {
                        RedisStringLookup::Hit => {}
                        RedisStringLookup::Miss => ServerWire::write_fast_null(ctx.out),
                        RedisStringLookup::WrongType => {
                            ServerWire::write_fast_error(ctx.out, WRONGTYPE_MESSAGE)
                        }
                    }
                    return;
                }
                match Self::fast_get_decoded_value_into(&mut ctx, key) {
                    true => {}
                    false => ServerWire::write_fast_null(ctx.out),
                }
            }
            _ => ServerWire::write_fast_error(ctx.out, "ERR unsupported command"),
        }
    }
}

#[cfg(feature = "server")]
impl Get {
    #[inline(always)]
    fn fast_get_decoded_value_into(ctx: &mut FastCommandContext<'_, '_>, key: &[u8]) -> bool {
        let store = ctx.store;
        let out = &mut *ctx.out;
        match (ctx.key_hash, ctx.single_threaded) {
            (Some(key_hash), true) => {
                // SAFETY: caller only enables this when exactly one worker can
                // access the store.
                unsafe {
                    store.with_value_bytes_route_hashed_single_threaded(key_hash, key, |value| {
                        ServerWire::write_fast_value(out, value);
                    })
                }
            }
            (Some(key_hash), false) => {
                store.with_value_bytes_route_hashed(key_hash, key, |value| {
                    ServerWire::write_fast_value(out, value);
                })
            }
            (None, _) => match store.get_value_bytes(key) {
                Some(value) => {
                    ServerWire::write_fast_value(out, value.as_ref());
                    true
                }
                None => false,
            },
        }
    }
}
