//! EXISTS command parsing and execution.

use crate::Result;
#[cfg(feature = "server")]
use crate::protocol::FastCodec;
use crate::protocol::{FastCommand, FastRequest, FastResponse, Frame};
#[cfg(feature = "server")]
use crate::server::commands::{
    BorrowedCommandContext, DirectCommandContext, DirectFastCommand, FastCommandContext,
    FastDirectCommand, FcnpCommandContext, FcnpDirectCommand, FcnpDispatch, RawCommandContext,
    RawDirectCommand,
};
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{Command, EngineCommandContext, EngineFastFuture, EngineFrameFuture};
use crate::storage::{ShardOperation, ShardReply};
use crate::{FastCacheError, commands::EngineCommandDispatch};

use super::DecodedFastCommand;
use super::parsing::CommandArity;

pub(crate) struct Exists;
pub(crate) static COMMAND: Exists = Exists;

#[derive(Debug, Clone)]
pub(crate) struct OwnedExists {
    key: Vec<u8>,
}

impl OwnedExists {
    fn new(key: Vec<u8>) -> Self {
        Self { key }
    }
}

impl super::OwnedCommandData for OwnedExists {
    type Spec = Exists;

    fn route_key(&self) -> Option<&[u8]> {
        Some(&self.key)
    }

    fn to_borrowed_command(&self) -> super::BorrowedCommandBox<'_> {
        Box::new(BorrowedExists::new(&self.key))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BorrowedExists<'a> {
    key: &'a [u8],
}

impl<'a> BorrowedExists<'a> {
    fn new(key: &'a [u8]) -> Self {
        Self { key }
    }
}

impl<'a> super::BorrowedCommandData<'a> for BorrowedExists<'a> {
    type Spec = Exists;

    fn route_key(&self) -> Option<&'a [u8]> {
        Some(self.key)
    }

    fn to_owned_command(&self) -> Command {
        Command::new(Box::new(OwnedExists::new(self.key.to_vec())))
    }

    fn execute_engine<'b>(&'b self, ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b,
    {
        let key = self.key;
        Box::pin(async move { Exists::execute_engine_frame(ctx, key).await })
    }

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &crate::storage::EmbeddedStore, _now_ms: u64) -> Frame {
        Frame::Integer(store.exists(self.key) as i64)
    }

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: BorrowedCommandContext<'_, '_, '_>) {
        ServerWire::write_resp_integer(ctx.out, ctx.store.exists(self.key) as i64);
    }

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, ctx: DirectCommandContext) -> Frame {
        Frame::Integer(ctx.exists(self.key) as i64)
    }
}

impl super::CommandSpec for Exists {
    const NAME: &'static str = "EXISTS";
    const MUTATES_VALUE: bool = false;
}

impl super::OwnedCommandParse for Exists {
    fn parse_owned(parts: &[Vec<u8>]) -> Result<Command> {
        CommandArity::<Self>::exact(parts.len(), 2)?;
        Ok(Command::new(Box::new(OwnedExists::new(parts[1].clone()))))
    }
}

impl<'a> super::BorrowedCommandParse<'a> for Exists {
    fn parse_borrowed(parts: &[&'a [u8]]) -> Result<super::BorrowedCommandBox<'a>> {
        CommandArity::<Self>::exact(parts.len(), 2)?;
        Ok(Box::new(BorrowedExists::new(parts[1])))
    }
}

impl DecodedFastCommand for Exists {
    fn matches_decoded_fast(&self, command: &FastCommand<'_>) -> bool {
        matches!(command, FastCommand::Exists { .. })
    }
}

impl EngineCommandDispatch for Exists {
    fn execute_engine_fast<'a>(
        &'static self,
        ctx: EngineCommandContext<'a>,
        request: FastRequest<'a>,
    ) -> EngineFastFuture<'a> {
        Box::pin(async move {
            match request.command {
                FastCommand::Exists { key } => Exists::execute_engine_integer(ctx, key)
                    .await
                    .map(FastResponse::Integer),
                _ => Ok(FastResponse::Error(b"ERR unsupported command".to_vec())),
            }
        })
    }
}

impl Exists {
    async fn execute_engine_frame(ctx: EngineCommandContext<'_>, key: &[u8]) -> Result<Frame> {
        Self::execute_engine_integer(ctx, key)
            .await
            .map(Frame::Integer)
    }

    async fn execute_engine_integer(ctx: EngineCommandContext<'_>, key: &[u8]) -> Result<i64> {
        let shard = ctx.route_key(key);
        match ctx
            .request(shard, ShardOperation::Exists(key.to_vec()))
            .await?
        {
            ShardReply::Integer(value) => Ok(value),
            _ => Err(FastCacheError::Command(
                "EXISTS received unexpected shard reply".into(),
            )),
        }
    }
}

#[cfg(feature = "server")]
impl RawDirectCommand for Exists {
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_>) {
        match ctx.args.as_slice() {
            [key] => ServerWire::write_resp_integer(ctx.out, ctx.store.exists(key) as i64),
            _ => ServerWire::write_resp_error(
                ctx.out,
                "ERR wrong number of arguments for 'EXISTS' command",
            ),
        }
    }
}

#[cfg(feature = "server")]
impl DirectFastCommand for Exists {
    fn execute_direct_fast(
        &self,
        ctx: DirectCommandContext,
        request: FastRequest<'_>,
    ) -> FastResponse {
        match request.command {
            FastCommand::Exists { key } => FastResponse::Integer(ctx.exists(key) as i64),
            _ => FastResponse::Error(b"ERR unsupported command".to_vec()),
        }
    }
}

#[cfg(feature = "server")]
impl FastDirectCommand for Exists {
    fn execute_fast(&self, ctx: FastCommandContext<'_, '_>, command: FastCommand<'_>) {
        match command {
            FastCommand::Exists { key } => {
                ServerWire::write_fast_integer(ctx.out, ctx.store.exists(key) as i64);
            }
            _ => ServerWire::write_fast_error(ctx.out, "ERR unsupported command"),
        }
    }
}

#[cfg(feature = "server")]
impl FcnpDirectCommand for Exists {
    fn opcode(&self) -> u8 {
        6
    }

    fn try_execute_fcnp(&self, ctx: FcnpCommandContext<'_, '_, '_, '_>) -> FcnpDispatch {
        let frame_len = ctx.frame.frame_len;
        let Ok(Some((request, consumed))) = FastCodec::decode_request(&ctx.frame.buf[..frame_len])
        else {
            return FcnpDispatch::Unsupported;
        };
        let FastCommand::Exists { key } = request.command else {
            return FcnpDispatch::Unsupported;
        };
        let Some(key_hash) = request.key_hash else {
            return FcnpDispatch::Unsupported;
        };
        if let Some(owned_shard_id) = ctx.owned_shard_id {
            match request.route_shard.map(|shard| shard as usize) {
                Some(route_shard)
                    if route_shard == owned_shard_id
                        && ctx.request_matches_owned_shard_for_key(route_shard, key_hash, key) => {}
                _ => {
                    ServerWire::write_fast_error(ctx.out, "ERR FCNP route shard mismatch");
                    return FcnpDispatch::Complete(consumed);
                }
            }
        }
        ServerWire::write_fast_integer(ctx.out, ctx.store.exists(key) as i64);
        FcnpDispatch::Complete(consumed)
    }
}
