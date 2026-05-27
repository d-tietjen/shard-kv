use crate::protocol::{FAST_FLAG_KEY_HASH, FAST_FLAG_KEY_TAG, FAST_FLAG_ROUTE_SHARD};
use crate::server::commands::{ScnpCommandContext, ScnpDirectCommand, ScnpDispatch};
use crate::server::wire::ServerWire;

use super::Del;

#[cfg(feature = "server")]
impl ScnpDirectCommand for Del {
    #[inline(always)]
    fn opcode(&self) -> u8 {
        5
    }

    #[inline(always)]
    fn try_execute_scnp(&self, ctx: ScnpCommandContext<'_, '_, '_, '_>) -> ScnpDispatch {
        DelScnpFrame::decode(&ctx).map_or(ScnpDispatch::Unsupported, |frame| {
            Del::execute_scnp(ctx, frame)
        })
    }
}

#[cfg(feature = "server")]
#[derive(Clone, Copy)]
struct DelScnpFrame<'a> {
    key_hash: u64,
    route_shard: Option<usize>,
    key: &'a [u8],
}

#[cfg(feature = "server")]
impl<'a> DelScnpFrame<'a> {
    #[inline(always)]
    fn decode(ctx: &ScnpCommandContext<'a, '_, '_, '_>) -> Option<DelScnpFrame<'a>> {
        let mut cursor = ScnpCursor::new(ctx)?;
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
struct ScnpCursor<'ctx, 'buf, 'store, 'out, 'queue> {
    ctx: &'ctx ScnpCommandContext<'buf, 'store, 'out, 'queue>,
    cursor: usize,
}

#[cfg(feature = "server")]
impl<'ctx, 'buf, 'store, 'out, 'queue> ScnpCursor<'ctx, 'buf, 'store, 'out, 'queue> {
    #[inline(always)]
    fn new(ctx: &'ctx ScnpCommandContext<'buf, 'store, 'out, 'queue>) -> Option<Self> {
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
    fn execute_scnp(
        ctx: ScnpCommandContext<'_, '_, '_, '_>,
        frame: DelScnpFrame<'_>,
    ) -> ScnpDispatch {
        match (
            ctx.owned_shard_id.is_some(),
            ctx.scnp_route_matches_owned_shard_for_key(
                frame.route_shard,
                frame.key_hash,
                frame.key,
            ),
        ) {
            (true, false) if frame.route_shard.is_none() => return ScnpDispatch::Unsupported,
            (true, false) => {
                ServerWire::write_fast_error(ctx.out, "ERR SCNP route shard mismatch");
                return ScnpDispatch::Complete(ctx.frame.frame_len);
            }
            _ => {}
        }

        let deleted = ctx.store.delete(frame.key);
        ServerWire::write_fast_integer(ctx.out, deleted as i64);
        ScnpDispatch::Complete(ctx.frame.frame_len)
    }
}
