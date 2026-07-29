use crate::protocol::{FAST_FLAG_KEY_HASH, FAST_FLAG_KEY_TAG, FAST_FLAG_ROUTE_SHARD};
use crate::server::commands::{ScnpCommandContext, ScnpDirectCommand, ScnpDispatch};
use crate::server::wire::ServerWire;
use crate::storage::{EmbeddedRouteMode, hash_key};

use super::Set;
use super::storage::EmbeddedStringWrite;

const SCNP_FULL_KEY_FLAGS: u8 = FAST_FLAG_KEY_HASH | FAST_FLAG_ROUTE_SHARD;
const SCNP_FULL_KEY_TAGGED_FLAGS: u8 =
    FAST_FLAG_KEY_HASH | FAST_FLAG_ROUTE_SHARD | FAST_FLAG_KEY_TAG;

#[cfg(feature = "server")]
impl ScnpDirectCommand for Set {
    #[inline(always)]
    fn opcode(&self) -> u8 {
        2
    }

    #[inline(always)]
    fn try_execute_scnp(&self, ctx: ScnpCommandContext<'_, '_, '_, '_>) -> ScnpDispatch {
        match SetScnpPath::select(&ctx) {
            SetScnpPath::FullKeyTaggedOwnedShard => {
                Self::try_execute_full_key_tagged_owned_shard(ctx)
            }
            SetScnpPath::FullKeyTagged => Self::try_execute_full_key_tagged(ctx),
            SetScnpPath::FullKey => Self::try_execute_full_key(ctx),
            SetScnpPath::Generic => Self::try_execute_generic(ctx),
        }
    }
}

#[cfg(feature = "server")]
enum SetScnpPath {
    FullKeyTaggedOwnedShard,
    FullKeyTagged,
    FullKey,
    Generic,
}

#[cfg(feature = "server")]
impl SetScnpPath {
    #[inline(always)]
    fn select(ctx: &ScnpCommandContext<'_, '_, '_, '_>) -> Self {
        let full_key_route = ctx.store.route_mode() == EmbeddedRouteMode::FullKey;
        match (
            ctx.frame.flags,
            ctx.owned_shard_id.is_some(),
            ctx.single_threaded,
            full_key_route,
            ctx.store.has_redis_objects(),
        ) {
            (SCNP_FULL_KEY_TAGGED_FLAGS, true, _, _, false) => Self::FullKeyTaggedOwnedShard,
            (SCNP_FULL_KEY_TAGGED_FLAGS, _, true, true, _) => Self::FullKeyTagged,
            (SCNP_FULL_KEY_FLAGS, _, true, true, _) => Self::FullKey,
            _ => Self::Generic,
        }
    }
}

#[cfg(feature = "server")]
impl Set {
    #[inline(always)]
    fn try_execute_full_key(ctx: ScnpCommandContext<'_, '_, '_, '_>) -> ScnpDispatch {
        const BODY_PREFIX_LEN: usize = 20;
        const KEY_HASH_OFFSET: usize = 8;
        const KEY_LEN_OFFSET: usize = 20;
        const VALUE_LEN_OFFSET: usize = 24;
        const PAYLOAD_OFFSET: usize = 28;

        if ctx.frame.body_len < BODY_PREFIX_LEN {
            return ScnpDispatch::Unsupported;
        }

        // SAFETY: callers pass a validated complete frame, and `body_len >= 20`
        // guarantees the fixed SET header fields through byte 28 are present.
        let (key_hash, key_len, value_len) = unsafe {
            (
                ctx.frame.read_u64_at(KEY_HASH_OFFSET),
                ctx.frame.read_u32_at(KEY_LEN_OFFSET) as usize,
                ctx.frame.read_u32_at(VALUE_LEN_OFFSET) as usize,
            )
        };
        let fields_len = ctx.frame.body_len - BODY_PREFIX_LEN;
        if key_len > fields_len || value_len != fields_len - key_len {
            return ScnpDispatch::Unsupported;
        }

        let key_start = PAYLOAD_OFFSET;
        let key_end = key_start + key_len;
        let key = &ctx.frame.buf[key_start..key_end];
        let value = &ctx.frame.buf[key_end..key_end + value_len];
        if !ctx.store.point_mutation_is_accepted(key, value.len(), None) {
            ServerWire::write_fast_error(
                ctx.out,
                "ERR mutation rejected by an installed storage extension",
            );
            return ScnpDispatch::Complete(ctx.frame.frame_len);
        }
        if ctx.store.has_redis_objects() {
            ctx.store.set(key.to_vec(), value.to_vec(), None);
        } else {
            // SAFETY: the dispatcher selected this path only for the
            // single-threaded direct worker.
            unsafe {
                ctx.store
                    .set_single_threaded_hashed(key_hash, key, value, None)
            };
        }
        ServerWire::write_fast_ok(ctx.out);
        ScnpDispatch::Complete(ctx.frame.frame_len)
    }

    #[inline(always)]
    fn try_execute_full_key_tagged_owned_shard(
        ctx: ScnpCommandContext<'_, '_, '_, '_>,
    ) -> ScnpDispatch {
        const BODY_PREFIX_LEN: usize = 28;
        const KEY_HASH_OFFSET: usize = 8;
        const ROUTE_SHARD_OFFSET: usize = 16;
        const KEY_TAG_OFFSET: usize = 20;
        const KEY_LEN_OFFSET: usize = 28;
        const VALUE_LEN_OFFSET: usize = 32;
        const PAYLOAD_OFFSET: usize = 36;

        if ctx.frame.body_len < BODY_PREFIX_LEN {
            return ScnpDispatch::Unsupported;
        }

        // SAFETY: callers pass a validated complete frame, and `body_len >= 28`
        // guarantees the fixed tagged SET header fields through byte 36 are present.
        let (key_hash, route_shard, key_tag, key_len, value_len) = unsafe {
            (
                ctx.frame.read_u64_at(KEY_HASH_OFFSET),
                ctx.frame.read_u32_at(ROUTE_SHARD_OFFSET) as usize,
                ctx.frame.read_u64_at(KEY_TAG_OFFSET),
                ctx.frame.read_u32_at(KEY_LEN_OFFSET) as usize,
                ctx.frame.read_u32_at(VALUE_LEN_OFFSET) as usize,
            )
        };
        let fields_len = ctx.frame.body_len - BODY_PREFIX_LEN;
        if key_len > fields_len || value_len != fields_len - key_len {
            return ScnpDispatch::Unsupported;
        }

        let expected_frame_len = PAYLOAD_OFFSET + key_len + value_len;
        if ctx.frame.frame_len != expected_frame_len {
            return ScnpDispatch::Unsupported;
        }

        let key_start = PAYLOAD_OFFSET;
        let key_end = key_start + key_len;
        let key = &ctx.frame.buf[key_start..key_end];
        let value = &ctx.frame.buf[key_end..key_end + value_len];
        if !ctx.store.point_mutation_is_accepted(key, value.len(), None) {
            ServerWire::write_fast_error(
                ctx.out,
                "ERR mutation rejected by an installed storage extension",
            );
            return ScnpDispatch::Complete(ctx.frame.frame_len);
        }

        let written = match ctx.store.route_mode() {
            EmbeddedRouteMode::SessionPrefix if key.starts_with(b"s:") => {
                let session_prefix = ctx.store.session_route_prefix_for_key(key);
                let session_hash = hash_key(session_prefix);
                if !ctx.request_matches_owned_session_hash(route_shard, session_hash) {
                    ServerWire::write_fast_error(ctx.out, "ERR SCNP route shard mismatch");
                    return ScnpDispatch::Complete(ctx.frame.frame_len);
                }
                #[cfg(feature = "unsafe")]
                {
                    // SAFETY: direct-shard workers bind one worker to one shard-owned
                    // port, and the route check above proves this session belongs to it.
                    unsafe {
                        ctx.store
                            .set_session_slice_hashed_owned_shard_no_ttl_hot_prevalidated(
                                route_shard,
                                session_hash,
                                key_hash,
                                session_prefix,
                                key,
                                value,
                            )
                    }
                }
                #[cfg(not(feature = "unsafe"))]
                {
                    ctx.store
                        .set_session_slice_hashed_owned_shard_no_ttl_prevalidated(
                            route_shard,
                            session_hash,
                            key_hash,
                            session_prefix,
                            key,
                            value,
                        )
                }
            }
            _ => {
                if !ctx.request_matches_owned_shard_for_key(route_shard, key_hash, key) {
                    ServerWire::write_fast_error(ctx.out, "ERR SCNP route shard mismatch");
                    return ScnpDispatch::Complete(ctx.frame.frame_len);
                }
                #[cfg(feature = "unsafe")]
                {
                    // SAFETY: direct-shard workers bind one worker to one shard-owned
                    // port. `request_matches_owned_shard` ensures this connection only
                    // touches the worker-owned shard; unsupported settings fall back to
                    // the locked safe path below.
                    unsafe {
                        ctx.store.set_slice_hashed_tagged_owned_shard_no_ttl_hot(
                            route_shard,
                            key_hash,
                            key_tag,
                            key,
                            value,
                        )
                    }
                }
                #[cfg(not(feature = "unsafe"))]
                {
                    ctx.store.set_slice_hashed_tagged_owned_shard_no_ttl(
                        route_shard,
                        key_hash,
                        key_tag,
                        key,
                        value,
                    )
                }
            }
        };
        if written {
            ServerWire::write_fast_ok(ctx.out);
            return ScnpDispatch::Complete(ctx.frame.frame_len);
        }

        ctx.store.set_slice_prehashed(key_hash, key, value, None);
        ServerWire::write_fast_ok(ctx.out);
        ScnpDispatch::Complete(ctx.frame.frame_len)
    }

    #[inline(always)]
    fn try_execute_full_key_tagged(ctx: ScnpCommandContext<'_, '_, '_, '_>) -> ScnpDispatch {
        const BODY_PREFIX_LEN: usize = 28;
        const KEY_HASH_OFFSET: usize = 8;
        const KEY_TAG_OFFSET: usize = 20;
        const KEY_LEN_OFFSET: usize = 28;
        const VALUE_LEN_OFFSET: usize = 32;
        const PAYLOAD_OFFSET: usize = 36;

        if ctx.frame.body_len < BODY_PREFIX_LEN {
            return ScnpDispatch::Unsupported;
        }

        // SAFETY: callers pass a validated complete frame, and `body_len >= 28`
        // guarantees the fixed tagged SET header fields through byte 36 are present.
        let (key_hash, key_tag, key_len, value_len) = unsafe {
            (
                ctx.frame.read_u64_at(KEY_HASH_OFFSET),
                ctx.frame.read_u64_at(KEY_TAG_OFFSET),
                ctx.frame.read_u32_at(KEY_LEN_OFFSET) as usize,
                ctx.frame.read_u32_at(VALUE_LEN_OFFSET) as usize,
            )
        };
        let fields_len = ctx.frame.body_len - BODY_PREFIX_LEN;
        if key_len > fields_len || value_len != fields_len - key_len {
            return ScnpDispatch::Unsupported;
        }

        let key_start = PAYLOAD_OFFSET;
        let key_end = key_start + key_len;
        let key = &ctx.frame.buf[key_start..key_end];
        let value = &ctx.frame.buf[key_end..key_end + value_len];
        if !ctx.store.point_mutation_is_accepted(key, value.len(), None) {
            ServerWire::write_fast_error(
                ctx.out,
                "ERR mutation rejected by an installed storage extension",
            );
            return ScnpDispatch::Complete(ctx.frame.frame_len);
        }
        if ctx.store.has_redis_objects() {
            ctx.store.set(key.to_vec(), value.to_vec(), None);
        } else {
            // SAFETY: the dispatcher selected this path only for the
            // single-threaded direct worker; the key tag is supplied by SCNP
            // and matches the key bytes.
            unsafe {
                ctx.store
                    .set_single_threaded_hashed_tagged_no_ttl_hot(key_hash, key_tag, key, value)
            };
        }
        ServerWire::write_fast_ok(ctx.out);
        ScnpDispatch::Complete(ctx.frame.frame_len)
    }

    #[inline(always)]
    fn try_execute_generic(ctx: ScnpCommandContext<'_, '_, '_, '_>) -> ScnpDispatch {
        let Some(prefix) = ctx.frame.read_key_prefix() else {
            return ScnpDispatch::Unsupported;
        };
        let body_end = ctx.frame.body_end();
        let mut cursor = prefix.cursor;
        if body_end.saturating_sub(cursor) < 8 {
            return ScnpDispatch::Unsupported;
        }
        // SAFETY: the key/value length check above guarantees four bytes at `cursor`.
        let key_len = unsafe { ctx.frame.read_u32_at(cursor) } as usize;
        cursor += 4;
        // SAFETY: the key/value length check above guarantees four bytes at `cursor`.
        let value_len = unsafe { ctx.frame.read_u32_at(cursor) } as usize;
        cursor += 4;
        let Some(fields_len) = key_len.checked_add(value_len) else {
            return ScnpDispatch::Unsupported;
        };
        if body_end.saturating_sub(cursor) != fields_len {
            return ScnpDispatch::Unsupported;
        }
        let key_end = cursor + key_len;
        if !<Self as EmbeddedStringWrite>::set_prehashed(
            ctx.store,
            prefix.key_hash,
            &ctx.frame.buf[cursor..key_end],
            &ctx.frame.buf[key_end..body_end],
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
        ScnpDispatch::Complete(ctx.frame.frame_len)
    }
}
