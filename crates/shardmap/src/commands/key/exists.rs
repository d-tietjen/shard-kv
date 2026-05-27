//! EXISTS command parsing and execution.

use smallvec::SmallVec;

use crate::Result;
#[cfg(feature = "server")]
use crate::protocol::FastCodec;
use crate::protocol::{FastCommand, FastRequest, FastResponse, Frame};
#[cfg(feature = "server")]
use crate::server::commands::{
    BorrowedCommandContext, DirectCommandContext, DirectFastCommand, FastCommandContext,
    FastDirectCommand, RawCommandContext, RawDirectCommand, ScnpCommandContext, ScnpDirectCommand,
    ScnpDispatch,
};
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{Command, EngineCommandContext, EngineFastFuture, EngineFrameFuture};
use crate::storage::{ShardOperation, ShardReply};
use crate::{ShardCacheError, commands::EngineCommandDispatch};

use super::DecodedFastCommand;
use super::parsing::CommandArity;

type BorrowedKeys<'a> = SmallVec<[&'a [u8]; 8]>;

pub(crate) struct Exists;
pub(crate) static COMMAND: Exists = Exists;

#[derive(Debug, Clone)]
pub(crate) struct OwnedExists {
    keys: Vec<Vec<u8>>,
}

impl OwnedExists {
    fn new(keys: Vec<Vec<u8>>) -> Self {
        Self { keys }
    }
}

impl super::OwnedCommandData for OwnedExists {
    type Spec = Exists;

    fn route_key(&self) -> Option<&[u8]> {
        self.keys.first().map(Vec::as_slice)
    }

    fn to_borrowed_command(&self) -> super::BorrowedCommandBox<'_> {
        Box::new(BorrowedExists::from_owned(&self.keys))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BorrowedExists<'a> {
    keys: BorrowedKeys<'a>,
}

impl<'a> BorrowedExists<'a> {
    fn new(keys: impl IntoIterator<Item = &'a [u8]>) -> Self {
        Self {
            keys: keys.into_iter().collect(),
        }
    }

    fn from_owned(keys: &'a [Vec<u8>]) -> Self {
        Self::new(keys.iter().map(Vec::as_slice))
    }
}

impl<'a> super::BorrowedCommandData<'a> for BorrowedExists<'a> {
    type Spec = Exists;

    fn route_key(&self) -> Option<&'a [u8]> {
        self.keys.first().copied()
    }

    fn to_owned_command(&self) -> Command {
        Command::new(Box::new(OwnedExists::new(
            self.keys.iter().map(|key| key.to_vec()).collect(),
        )))
    }

    fn execute_engine<'b>(&'b self, ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b,
    {
        Box::pin(async move { Exists::execute_engine_frame(ctx, self.keys.as_slice()).await })
    }

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &crate::storage::EmbeddedStore, _now_ms: u64) -> Frame {
        Frame::Integer(Exists::count_embedded_keys(store, self.keys.as_slice()))
    }

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: BorrowedCommandContext<'_, '_, '_>) {
        ServerWire::write_resp_integer(
            ctx.out,
            Exists::count_embedded_keys(ctx.store, self.keys.as_slice()),
        );
    }

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, ctx: DirectCommandContext) -> Frame {
        Frame::Integer(Exists::count_direct_keys(ctx, self.keys.as_slice()))
    }
}

impl super::CommandSpec for Exists {
    const NAME: &'static str = "EXISTS";
    const MUTATES_VALUE: bool = false;
}

impl super::OwnedCommandParse for Exists {
    fn parse_owned(parts: &[Vec<u8>]) -> Result<Command> {
        CommandArity::<Self>::at_least(parts.len(), 2, "key")?;
        Ok(Command::new(Box::new(OwnedExists::new(
            parts[1..].to_vec(),
        ))))
    }
}

impl<'a> super::BorrowedCommandParse<'a> for Exists {
    fn parse_borrowed(parts: &[&'a [u8]]) -> Result<super::BorrowedCommandBox<'a>> {
        CommandArity::<Self>::at_least(parts.len(), 2, "key")?;
        Ok(Box::new(BorrowedExists::new(parts[1..].iter().copied())))
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
    async fn execute_engine_frame(ctx: EngineCommandContext<'_>, keys: &[&[u8]]) -> Result<Frame> {
        let mut total = 0i64;
        for key in keys {
            total += Self::execute_engine_integer(ctx, key).await?;
        }
        Ok(Frame::Integer(total))
    }

    async fn execute_engine_integer(ctx: EngineCommandContext<'_>, key: &[u8]) -> Result<i64> {
        let shard = ctx.route_key(key);
        match ctx
            .request(shard, ShardOperation::Exists(key.to_vec()))
            .await?
        {
            ShardReply::Integer(value) => Ok(value),
            _ => Err(ShardCacheError::Command(
                "EXISTS received unexpected shard reply".into(),
            )),
        }
    }

    #[cfg(feature = "server")]
    fn count_embedded_keys(store: &crate::storage::EmbeddedStore, keys: &[&[u8]]) -> i64 {
        keys.iter().filter(|key| store.exists(key)).count() as i64
    }

    #[cfg(feature = "server")]
    fn count_direct_keys(ctx: DirectCommandContext, keys: &[&[u8]]) -> i64 {
        keys.iter().filter(|key| ctx.exists(key)).count() as i64
    }
}

#[cfg(feature = "server")]
impl RawDirectCommand for Exists {
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_, '_>) {
        match ctx.args.as_slice() {
            [] => ServerWire::write_resp_error(
                ctx.out,
                "ERR wrong number of arguments for 'EXISTS' command",
            ),
            keys => ServerWire::write_resp_integer(
                ctx.out,
                Exists::count_embedded_keys(ctx.store, keys),
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
impl ScnpDirectCommand for Exists {
    fn opcode(&self) -> u8 {
        6
    }

    fn try_execute_scnp(&self, ctx: ScnpCommandContext<'_, '_, '_, '_>) -> ScnpDispatch {
        let frame_len = ctx.frame.frame_len;
        let Ok(Some((request, consumed))) = FastCodec::decode_request(&ctx.frame.buf[..frame_len])
        else {
            return ScnpDispatch::Unsupported;
        };
        let FastCommand::Exists { key } = request.command else {
            return ScnpDispatch::Unsupported;
        };
        let Some(key_hash) = request.key_hash else {
            return ScnpDispatch::Unsupported;
        };
        match ctx.scnp_route_matches_owned_shard_for_key(request.route_shard, key_hash, key) {
            true => {}
            false => {
                ServerWire::write_fast_error(ctx.out, "ERR SCNP route shard mismatch");
                return ScnpDispatch::Complete(consumed);
            }
        }
        ServerWire::write_fast_integer(ctx.out, ctx.store.exists(key) as i64);
        ScnpDispatch::Complete(consumed)
    }
}
