use crate::protocol::{FAST_FLAG_KEY_HASH, FAST_FLAG_KEY_TAG, FAST_FLAG_ROUTE_SHARD};
use crate::server::commands::{FcnpCommandContext, FcnpDirectCommand, FcnpDispatch};
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedRouteMode, hash_key};

use super::Get;

const FCNP_FULL_KEY_FLAGS: u8 = FAST_FLAG_KEY_HASH | FAST_FLAG_ROUTE_SHARD;
const FCNP_FULL_KEY_TAGGED_FLAGS: u8 =
    FAST_FLAG_KEY_HASH | FAST_FLAG_ROUTE_SHARD | FAST_FLAG_KEY_TAG;

#[cfg(feature = "server")]
impl FcnpDirectCommand for Get {
    #[inline(always)]
    fn opcode(&self) -> u8 {
        1
    }

    #[inline(always)]
    fn try_execute_fcnp(&self, ctx: FcnpCommandContext<'_, '_, '_, '_>) -> FcnpDispatch {
        match GetFcnpPath::select(&ctx) {
            GetFcnpPath::Unsupported => FcnpDispatch::Unsupported,
            GetFcnpPath::FullKeyTaggedOwnedShard => {
                Self::try_execute_full_key_tagged_owned_shard(ctx)
            }
            GetFcnpPath::FullKeyTagged => Self::try_execute_full_key_tagged(ctx),
            GetFcnpPath::FullKey => Self::try_execute_full_key(ctx),
            GetFcnpPath::Generic => Self::try_execute_generic(ctx),
        }
    }
}

#[cfg(feature = "server")]
enum GetFcnpPath {
    Unsupported,
    FullKeyTaggedOwnedShard,
    FullKeyTagged,
    FullKey,
    Generic,
}

#[cfg(feature = "server")]
impl GetFcnpPath {
    #[inline(always)]
    fn select(ctx: &FcnpCommandContext<'_, '_, '_, '_>) -> Self {
        let full_key_route = ctx.store.route_mode() == EmbeddedRouteMode::FullKey;
        match (
            ctx.store.has_redis_objects(),
            ctx.frame.flags,
            ctx.owned_shard_id.is_some(),
            ctx.single_threaded,
            full_key_route,
        ) {
            (true, _, _, _, _) => Self::Unsupported,
            (false, FCNP_FULL_KEY_TAGGED_FLAGS, true, _, _) => Self::FullKeyTaggedOwnedShard,
            (false, FCNP_FULL_KEY_TAGGED_FLAGS, _, true, true) => Self::FullKeyTagged,
            (false, FCNP_FULL_KEY_FLAGS, _, true, true) => Self::FullKey,
            (false, _, _, _, _) => Self::Generic,
        }
    }
}

#[cfg(feature = "server")]
impl Get {
    #[inline(always)]
    fn try_execute_full_key(mut ctx: FcnpCommandContext<'_, '_, '_, '_>) -> FcnpDispatch {
        const BODY_PREFIX_LEN: usize = 16;
        const KEY_HASH_OFFSET: usize = 8;
        const KEY_LEN_OFFSET: usize = 20;
        const KEY_OFFSET: usize = 24;

        if ctx.frame.body_len < BODY_PREFIX_LEN {
            return FcnpDispatch::Unsupported;
        }

        // SAFETY: callers pass a validated complete frame, and `body_len >= 16`
        // guarantees the fixed GET header fields through byte 24 are present.
        let (key_hash, key_len) = unsafe {
            (
                ctx.frame.read_u64_at(KEY_HASH_OFFSET),
                ctx.frame.read_u32_at(KEY_LEN_OFFSET) as usize,
            )
        };
        if key_len != ctx.frame.body_len - BODY_PREFIX_LEN {
            return FcnpDispatch::Unsupported;
        }

        let key = &ctx.frame.buf[KEY_OFFSET..KEY_OFFSET + key_len];
        // SAFETY: the dispatcher selected this path only for the single-threaded
        // full-key worker.
        let value = unsafe {
            ctx.store
                .get_shared_value_bytes_full_key_single_threaded(key_hash, key)
        };
        match value {
            Some(value) => Self::write_fast_value(&mut ctx, value),
            None => ServerWire::write_fast_null(ctx.out),
        }
        FcnpDispatch::Complete(ctx.frame.frame_len)
    }

    #[inline(always)]
    fn try_execute_full_key_tagged(mut ctx: FcnpCommandContext<'_, '_, '_, '_>) -> FcnpDispatch {
        const BODY_PREFIX_LEN: usize = 24;
        const KEY_HASH_OFFSET: usize = 8;
        const KEY_TAG_OFFSET: usize = 20;
        const KEY_LEN_OFFSET: usize = 28;
        const KEY_OFFSET: usize = 32;

        if ctx.frame.body_len < BODY_PREFIX_LEN {
            return FcnpDispatch::Unsupported;
        }

        // SAFETY: callers pass a validated complete frame, and `body_len >= 24`
        // guarantees the fixed tagged GET header fields through byte 32 are present.
        let (key_hash, key_tag, key_len) = unsafe {
            (
                ctx.frame.read_u64_at(KEY_HASH_OFFSET),
                ctx.frame.read_u64_at(KEY_TAG_OFFSET),
                ctx.frame.read_u32_at(KEY_LEN_OFFSET) as usize,
            )
        };
        let has_embedded_key = ctx.frame.body_len > BODY_PREFIX_LEN;
        if has_embedded_key && key_len != ctx.frame.body_len - BODY_PREFIX_LEN {
            return FcnpDispatch::Unsupported;
        }
        let expected_frame_len = if has_embedded_key {
            KEY_OFFSET + key_len
        } else {
            KEY_OFFSET
        };
        if ctx.frame.frame_len != expected_frame_len {
            return FcnpDispatch::Unsupported;
        }

        // SAFETY: the dispatcher selected this path only for the single-threaded
        // full-key worker, and the client supplied a matching key tag.
        let value = unsafe {
            ctx.store
                .get_shared_value_bytes_full_key_tagged_single_threaded(key_hash, key_tag, key_len)
        };
        match value {
            Some(value) => Self::write_fast_value(&mut ctx, value),
            None => ServerWire::write_fast_null(ctx.out),
        }
        FcnpDispatch::Complete(ctx.frame.frame_len)
    }

    #[inline(always)]
    fn try_execute_full_key_tagged_owned_shard(
        mut ctx: FcnpCommandContext<'_, '_, '_, '_>,
    ) -> FcnpDispatch {
        const BODY_PREFIX_LEN: usize = 24;
        const KEY_HASH_OFFSET: usize = 8;
        const ROUTE_SHARD_OFFSET: usize = 16;
        const KEY_TAG_OFFSET: usize = 20;
        const KEY_LEN_OFFSET: usize = 28;
        const KEY_OFFSET: usize = 32;

        if ctx.frame.body_len < BODY_PREFIX_LEN {
            return FcnpDispatch::Unsupported;
        }

        // SAFETY: callers pass a validated complete frame, and `body_len >= 24`
        // guarantees the fixed tagged GET header fields through byte 32 are present.
        let (key_hash, route_shard, key_tag, key_len) = unsafe {
            (
                ctx.frame.read_u64_at(KEY_HASH_OFFSET),
                ctx.frame.read_u32_at(ROUTE_SHARD_OFFSET) as usize,
                ctx.frame.read_u64_at(KEY_TAG_OFFSET),
                ctx.frame.read_u32_at(KEY_LEN_OFFSET) as usize,
            )
        };
        let has_embedded_key = ctx.frame.body_len > BODY_PREFIX_LEN;
        if has_embedded_key && key_len != ctx.frame.body_len - BODY_PREFIX_LEN {
            return FcnpDispatch::Unsupported;
        }
        let expected_frame_len = if has_embedded_key {
            KEY_OFFSET + key_len
        } else {
            KEY_OFFSET
        };
        if ctx.frame.frame_len != expected_frame_len {
            return FcnpDispatch::Unsupported;
        }
        if ctx.store.route_mode() == EmbeddedRouteMode::FullKey
            && !ctx.request_matches_owned_shard(route_shard, key_hash)
        {
            ServerWire::write_fast_error(ctx.out, "ERR FCNP route shard mismatch");
            return FcnpDispatch::Complete(ctx.frame.frame_len);
        }

        #[cfg(feature = "unsafe")]
        {
            // SAFETY: direct-shard workers bind one worker to one shard-owned
            // port. `request_matches_owned_shard` ensures this connection only
            // touches the worker-owned shard.
            let outcome = unsafe {
                let fast_write_queue = &mut ctx.fast_write_queue;
                let out = &mut *ctx.out;
                ctx.store
                    .with_shared_value_bytes_full_key_tagged_owned_shard_no_ttl(
                        route_shard,
                        key_hash,
                        key_tag,
                        key_len,
                        |value| {
                            if let Some(queue) = fast_write_queue.as_mut() {
                                queue.push_fast_value(out, value);
                            } else {
                                ServerWire::write_fast_value(out, value.as_ref());
                            }
                        },
                    )
            };
            if let Some(found) = outcome {
                if !found {
                    ServerWire::write_fast_null(ctx.out);
                }
                return FcnpDispatch::Complete(ctx.frame.frame_len);
            }
        }

        if !has_embedded_key {
            return FcnpDispatch::Unsupported;
        }
        if ctx.owned_shard_id.is_none() {
            return FcnpDispatch::Unsupported;
        }
        let key = &ctx.frame.buf[KEY_OFFSET..KEY_OFFSET + key_len];
        match ctx.store.route_mode() == EmbeddedRouteMode::SessionPrefix && key.starts_with(b"s:") {
            true => {
                let session_prefix = ctx.store.session_route_prefix_for_key(key);
                let session_hash = hash_key(session_prefix);
                if !ctx.request_matches_owned_session_hash(route_shard, session_hash) {
                    ServerWire::write_fast_error(ctx.out, "ERR FCNP route shard mismatch");
                    return FcnpDispatch::Complete(ctx.frame.frame_len);
                }
                let outcome = {
                    let out = &mut *ctx.out;
                    // SAFETY: direct-shard workers bind one worker to one shard-owned port,
                    // and the route check above proves this key belongs to that shard.
                    unsafe {
                        ctx.store
                            .with_session_value_slice_owned_shard_no_ttl_prevalidated(
                                route_shard,
                                session_hash,
                                key_hash,
                                session_prefix,
                                key,
                                |value| ServerWire::write_fast_value(out, value),
                            )
                    }
                };
                match outcome {
                    Some(false) => {
                        ServerWire::write_fast_null(ctx.out);
                        return FcnpDispatch::Complete(ctx.frame.frame_len);
                    }
                    Some(true) => return FcnpDispatch::Complete(ctx.frame.frame_len),
                    None => {}
                }
            }
            false if !ctx.request_matches_owned_shard_for_key(route_shard, key_hash, key) => {
                ServerWire::write_fast_error(ctx.out, "ERR FCNP route shard mismatch");
                return FcnpDispatch::Complete(ctx.frame.frame_len);
            }
            false => {}
        }

        #[cfg(not(feature = "unsafe"))]
        {
            let _ = key_tag;
            let outcome = {
                let fast_write_queue = &mut ctx.fast_write_queue;
                let out = &mut *ctx.out;
                ctx.store
                    .with_shared_value_bytes_full_key_owned_shard_no_ttl(
                        route_shard,
                        key_hash,
                        key,
                        |value| {
                            if let Some(queue) = fast_write_queue.as_mut() {
                                queue.push_fast_value(out, value);
                            } else {
                                ServerWire::write_fast_value(out, value.as_ref());
                            }
                        },
                    )
            };
            if let Some(found) = outcome {
                if !found {
                    ServerWire::write_fast_null(ctx.out);
                }
                return FcnpDispatch::Complete(ctx.frame.frame_len);
            }
        }
        let found = {
            let fast_write_queue = &mut ctx.fast_write_queue;
            let out = &mut *ctx.out;
            // SAFETY: direct-shard workers bind one worker to one shard-owned port.
            unsafe {
                ctx.store
                    .with_shared_value_bytes_route_hashed_single_threaded(key_hash, key, |value| {
                        if let Some(queue) = fast_write_queue.as_mut() {
                            queue.push_fast_value(out, value);
                        } else {
                            ServerWire::write_fast_value(out, value.as_ref());
                        }
                    })
            }
        };
        if !found {
            ServerWire::write_fast_null(ctx.out);
        }
        FcnpDispatch::Complete(ctx.frame.frame_len)
    }

    #[inline(always)]
    fn try_execute_generic(mut ctx: FcnpCommandContext<'_, '_, '_, '_>) -> FcnpDispatch {
        let Some(prefix) = ctx.frame.read_key_prefix() else {
            return FcnpDispatch::Unsupported;
        };
        let body_end = ctx.frame.body_end();
        if body_end.saturating_sub(prefix.cursor) < 4 {
            return FcnpDispatch::Unsupported;
        }
        // SAFETY: the key-length check above guarantees four bytes at `cursor`.
        let key_len = unsafe { ctx.frame.read_u32_at(prefix.cursor) } as usize;
        let cursor = prefix.cursor + 4;
        let remaining = body_end.saturating_sub(cursor);
        if remaining == 0
            && let Some(key_tag) = prefix.key_tag
            && ctx.store.route_mode() == EmbeddedRouteMode::FullKey
        {
            let found = {
                let fast_write_queue = &mut ctx.fast_write_queue;
                let out = &mut *ctx.out;
                ctx.store.with_shared_value_bytes_full_key_tagged_no_ttl(
                    prefix.key_hash,
                    key_tag,
                    key_len,
                    |value| {
                        if let Some(queue) = fast_write_queue.as_mut() {
                            queue.push_fast_value(out, value);
                        } else {
                            ServerWire::write_fast_value(out, value.as_ref());
                        }
                    },
                )
            };
            if !found {
                ServerWire::write_fast_null(ctx.out);
            }
            return FcnpDispatch::Complete(ctx.frame.frame_len);
        }
        if remaining != key_len {
            return FcnpDispatch::Unsupported;
        }
        let key = &ctx.frame.buf[cursor..body_end];
        if !Self::fast_get_value_into(&mut ctx, prefix.key_hash, key) {
            ServerWire::write_fast_null(ctx.out);
        }
        FcnpDispatch::Complete(ctx.frame.frame_len)
    }

    #[inline(always)]
    fn fast_get_value_into(
        ctx: &mut FcnpCommandContext<'_, '_, '_, '_>,
        key_hash: u64,
        key: &[u8],
    ) -> bool {
        let store = ctx.store;
        let out = &mut *ctx.out;
        if ctx.single_threaded
            && let Some(queue) = ctx.fast_write_queue.as_mut()
        {
            let mut found = false;
            // SAFETY: caller only enables this when exactly one worker can
            // access the store.
            unsafe {
                if store.route_mode() == EmbeddedRouteMode::FullKey {
                    store.with_shared_value_bytes_full_key_single_threaded(
                        key_hash,
                        key,
                        |value| {
                            queue.push_fast_value(out, value);
                            found = true;
                        },
                    );
                } else {
                    store.with_shared_value_bytes_route_hashed_single_threaded(
                        key_hash,
                        key,
                        |value| {
                            queue.push_fast_value(out, value);
                            found = true;
                        },
                    );
                }
            }
            return found;
        }

        if ctx.single_threaded {
            // SAFETY: caller only enables this when exactly one worker can
            // access the store.
            unsafe {
                store.with_value_bytes_route_hashed_single_threaded(key_hash, key, |value| {
                    ServerWire::write_fast_value(out, value);
                })
            }
        } else {
            store.with_value_bytes_route_hashed(key_hash, key, |value| {
                ServerWire::write_fast_value(out, value);
            })
        }
    }

    #[inline(always)]
    fn write_fast_value(ctx: &mut FcnpCommandContext<'_, '_, '_, '_>, value: &bytes::Bytes) {
        if let Some(queue) = ctx.fast_write_queue.as_mut() {
            queue.push_fast_value(ctx.out, value);
        } else {
            ServerWire::write_fast_value(ctx.out, value.as_ref());
        }
    }
}
