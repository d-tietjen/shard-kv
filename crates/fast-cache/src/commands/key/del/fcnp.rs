use crate::protocol::{FAST_FLAG_KEY_HASH, FAST_FLAG_KEY_TAG, FAST_FLAG_ROUTE_SHARD};
use crate::server::commands::{FcnpCommandContext, FcnpDirectCommand, FcnpDispatch};
use crate::server::wire::ServerWire;

use super::Del;

#[cfg(feature = "server")]
impl FcnpDirectCommand for Del {
    #[inline(always)]
    fn opcode(&self) -> u8 {
        5
    }

    #[inline(always)]
    fn try_execute_fcnp(&self, ctx: FcnpCommandContext<'_, '_, '_, '_>) -> FcnpDispatch {
        DelFcnpFrame::decode(&ctx).map_or(FcnpDispatch::Unsupported, |frame| {
            Del::execute_fcnp(ctx, frame)
        })
    }
}

#[cfg(feature = "server")]
#[derive(Clone, Copy)]
struct DelFcnpFrame<'a> {
    key_hash: u64,
    route_shard: Option<usize>,
    key: &'a [u8],
}

#[cfg(feature = "server")]
impl<'a> DelFcnpFrame<'a> {
    #[inline(always)]
    fn decode(ctx: &FcnpCommandContext<'a, '_, '_, '_>) -> Option<DelFcnpFrame<'a>> {
        let mut cursor = FcnpCursor::new(ctx)?;
        let key_hash = cursor.read_u64()?;
        let route_shard = cursor.read_optional_route_shard()?;
        cursor.skip_optional_key_tag()?;
        let key_len = cursor.read_u32()? as usize;
        let key = cursor.read_tail(key_len)?;
        Some(Self {
            key_hash,
            route_shard,
            key,
        })
    }
}

#[cfg(feature = "server")]
struct FcnpCursor<'ctx, 'buf, 'store, 'out, 'queue> {
    ctx: &'ctx FcnpCommandContext<'buf, 'store, 'out, 'queue>,
    cursor: usize,
}

#[cfg(feature = "server")]
impl<'ctx, 'buf, 'store, 'out, 'queue> FcnpCursor<'ctx, 'buf, 'store, 'out, 'queue> {
    #[inline(always)]
    fn new(ctx: &'ctx FcnpCommandContext<'buf, 'store, 'out, 'queue>) -> Option<Self> {
        match ctx.frame.flags & FAST_FLAG_KEY_HASH != 0 {
            true => Some(Self { ctx, cursor: 8 }),
            false => None,
        }
    }

    #[inline(always)]
    fn read_u64(&mut self) -> Option<u64> {
        match self.remaining() >= 8 {
            true => {
                // SAFETY: remaining length check proves eight bytes at cursor.
                let value = unsafe { self.ctx.frame.read_u64_at(self.cursor) };
                self.cursor += 8;
                Some(value)
            }
            false => None,
        }
    }

    #[inline(always)]
    fn read_u32(&mut self) -> Option<u32> {
        match self.remaining() >= 4 {
            true => {
                // SAFETY: remaining length check proves four bytes at cursor.
                let value = unsafe { self.ctx.frame.read_u32_at(self.cursor) };
                self.cursor += 4;
                Some(value)
            }
            false => None,
        }
    }

    #[inline(always)]
    fn read_optional_route_shard(&mut self) -> Option<Option<usize>> {
        match self.ctx.frame.flags & FAST_FLAG_ROUTE_SHARD != 0 {
            true => self.read_u32().map(|value| Some(value as usize)),
            false => Some(None),
        }
    }

    #[inline(always)]
    fn skip_optional_key_tag(&mut self) -> Option<()> {
        match self.ctx.frame.flags & FAST_FLAG_KEY_TAG != 0 {
            true => self.read_u64().map(|_| ()),
            false => Some(()),
        }
    }

    #[inline(always)]
    fn read_tail(&mut self, len: usize) -> Option<&'buf [u8]> {
        match self.remaining() == len {
            true => {
                let start = self.cursor;
                self.cursor += len;
                Some(&self.ctx.frame.buf[start..start + len])
            }
            false => None,
        }
    }

    #[inline(always)]
    fn remaining(&self) -> usize {
        self.ctx.frame.body_end().saturating_sub(self.cursor)
    }
}

#[cfg(feature = "server")]
impl Del {
    #[inline(always)]
    fn execute_fcnp(
        ctx: FcnpCommandContext<'_, '_, '_, '_>,
        frame: DelFcnpFrame<'_>,
    ) -> FcnpDispatch {
        match (
            ctx.owned_shard_id.is_some(),
            ctx.fcnp_route_matches_owned_shard_for_key(
                frame.route_shard,
                frame.key_hash,
                frame.key,
            ),
        ) {
            (true, false) if frame.route_shard.is_none() => return FcnpDispatch::Unsupported,
            (true, false) => {
                ServerWire::write_fast_error(ctx.out, "ERR FCNP route shard mismatch");
                return FcnpDispatch::Complete(ctx.frame.frame_len);
            }
            _ => {}
        }

        let deleted = ctx.store.delete(frame.key);
        ServerWire::write_fast_integer(ctx.out, deleted as i64);
        FcnpDispatch::Complete(ctx.frame.frame_len)
    }
}
