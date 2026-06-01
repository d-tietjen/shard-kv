use std::marker::PhantomData;

#[cfg(feature = "server")]
use bytes::BytesMut;
use smallvec::SmallVec;

use super::dispatch::{dispatch, route_key_for_command, route_key_for_owned_command};
#[cfg(feature = "server")]
use super::frame::{with_resp_protocol, write_fast_frame, write_frame};
use crate::commands::CommandSpec;
use crate::protocol::{FastCommand, Frame};
#[cfg(feature = "server")]
use crate::server::commands::{
    BorrowedCommandContext, DirectCommandContext, FastCommandContext, FastDirectCommand,
    RawCommandContext, RawDirectCommand,
};
#[cfg(feature = "server")]
use crate::server::wire::{RespProtocolVersion, ServerWire};
use crate::storage::{Command, EmbeddedStore, EngineCommandContext, EngineFrameFuture};
use crate::{Result, ShardCacheError};

pub(crate) type BorrowedArgs<'a> = SmallVec<[&'a [u8]; 16]>;

#[derive(Clone)]
pub(crate) struct OwnedRedisCommand<C>
where
    C: RedisCommand,
{
    args: Vec<Vec<u8>>,
    _marker: PhantomData<C>,
}

impl<C> std::fmt::Debug for OwnedRedisCommand<C>
where
    C: RedisCommand,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(C::NAME).field("args", &self.args).finish()
    }
}

impl<C> OwnedRedisCommand<C>
where
    C: RedisCommand,
{
    fn new(args: Vec<Vec<u8>>) -> Self {
        Self {
            args,
            _marker: PhantomData,
        }
    }
}

#[derive(Clone)]
pub(crate) struct BorrowedRedisCommand<'a, C>
where
    C: RedisCommand,
{
    args: BorrowedArgs<'a>,
    _marker: PhantomData<C>,
}

impl<C> std::fmt::Debug for BorrowedRedisCommand<'_, C>
where
    C: RedisCommand,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(C::NAME).field("args", &self.args).finish()
    }
}

impl<'a, C> BorrowedRedisCommand<'a, C>
where
    C: RedisCommand,
{
    fn new(args: BorrowedArgs<'a>) -> Self {
        Self {
            args,
            _marker: PhantomData,
        }
    }
}

pub(crate) trait RedisCommand: CommandSpec + Send + Sync + 'static {
    #[inline(always)]
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        dispatch(Self::NAME, store, args)
    }

    #[cfg(feature = "server")]
    #[inline(always)]
    fn write_resp(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        write_frame(out, &Self::execute(store, args));
    }

    #[cfg(feature = "server")]
    #[inline(always)]
    fn write_fast(store: &EmbeddedStore, args: &[&[u8]], out: &mut BytesMut) {
        let start = ServerWire::begin_fast_value(out);
        with_resp_protocol(RespProtocolVersion::Resp2, || {
            Self::write_resp(store, args, out);
        });
        ServerWire::finish_fast_value(out, start);
    }

    #[cfg(feature = "server")]
    #[inline(always)]
    fn write_resp_owned_shard(
        _store: &EmbeddedStore,
        _args: &[&[u8]],
        _owned_shard_id: usize,
        _out: &mut BytesMut,
    ) -> bool {
        false
    }

    #[inline(always)]
    fn matches_fast(_command: &FastCommand<'_>) -> bool {
        false
    }

    #[cfg(feature = "server")]
    #[inline(always)]
    fn execute_fast(_store: &EmbeddedStore, _command: FastCommand<'_>) -> Option<Frame> {
        None
    }
}

impl<C> crate::commands::OwnedCommandData for OwnedRedisCommand<C>
where
    C: RedisCommand,
{
    type Spec = C;

    fn route_key(&self) -> Option<&[u8]> {
        route_key_for_owned_command(C::NAME, &self.args)
    }

    fn to_borrowed_command(&self) -> crate::commands::BorrowedCommandBox<'_> {
        let args = self
            .args
            .iter()
            .map(Vec::as_slice)
            .collect::<SmallVec<[_; 16]>>();
        Box::new(BorrowedRedisCommand::<C>::new(args))
    }
}

impl<'a, C> crate::commands::BorrowedCommandData<'a> for BorrowedRedisCommand<'a, C>
where
    C: RedisCommand,
{
    type Spec = C;

    fn route_key(&self) -> Option<&'a [u8]> {
        route_key_for_command(C::NAME, &self.args)
    }

    fn to_owned_command(&self) -> Command {
        Command::new(Box::new(OwnedRedisCommand::<C>::new(
            self.args.iter().map(|arg| arg.to_vec()).collect(),
        )))
    }

    fn execute_engine<'b>(&'b self, _ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b,
    {
        Box::pin(async move {
            Err(ShardCacheError::Command(format!(
                "{} requires embedded Redis compatibility storage",
                C::NAME
            )))
        })
    }

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &EmbeddedStore, _now_ms: u64) -> Frame {
        C::execute(store, &self.args)
    }

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: BorrowedCommandContext<'_, '_, '_>) {
        with_resp_protocol(ctx.resp_protocol, || {
            C::write_resp(ctx.store, &self.args, ctx.out);
        });
    }

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, _ctx: DirectCommandContext) -> Frame {
        Frame::Error(format!(
            "ERR {} requires embedded Redis compatibility storage",
            C::NAME
        ))
    }
}

impl<C> crate::commands::OwnedCommandParse for C
where
    C: RedisCommand,
{
    fn parse_owned(parts: &[Vec<u8>]) -> Result<Command> {
        Ok(Command::new(Box::new(OwnedRedisCommand::<C>::new(
            parts.iter().skip(1).cloned().collect(),
        ))))
    }
}

impl<'a, C> crate::commands::BorrowedCommandParse<'a> for C
where
    C: RedisCommand,
{
    fn parse_borrowed(parts: &[&'a [u8]]) -> Result<crate::commands::BorrowedCommandBox<'a>> {
        Ok(Box::new(BorrowedRedisCommand::<C>::new(
            parts.iter().skip(1).copied().collect(),
        )))
    }
}

impl<C> crate::commands::DecodedFastCommand for C
where
    C: RedisCommand,
{
    fn matches_decoded_fast(&self, command: &FastCommand<'_>) -> bool {
        C::matches_fast(command)
    }
}

#[cfg(feature = "server")]
impl<C> RawDirectCommand for C
where
    C: RedisCommand,
{
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_, '_>) {
        with_resp_protocol(ctx.resp_protocol, || {
            C::write_resp(ctx.store, &ctx.args, ctx.out);
        });
    }

    fn execute_fast(&self, ctx: RawCommandContext<'_, '_, '_, '_>) {
        C::write_fast(ctx.store, &ctx.args, ctx.out);
    }

    fn execute_owned_shard(
        &self,
        store: &EmbeddedStore,
        args: &[&[u8]],
        out: &mut BytesMut,
        owned_shard_id: usize,
    ) -> bool {
        C::write_resp_owned_shard(store, args, owned_shard_id, out)
    }
}

#[cfg(feature = "server")]
impl<C> FastDirectCommand for C
where
    C: RedisCommand,
{
    fn execute_fast(&self, ctx: FastCommandContext<'_, '_>, command: FastCommand<'_>) {
        match C::execute_fast(ctx.store, command) {
            Some(frame) => write_fast_frame(ctx.out, &frame),
            None => ServerWire::write_fast_error(ctx.out, "ERR unsupported command"),
        }
    }
}
