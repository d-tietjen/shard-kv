//! TTL command parsing and execution.

use crate::Result;
use crate::ShardCacheError;
use crate::commands::EngineCommandDispatch;
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
use crate::storage::{
    Command, EngineCommandContext, EngineFastFuture, EngineFrameFuture, ShardOperation, ShardReply,
};

use super::DecodedFastCommand;
use super::parsing::CommandArity;

pub(crate) struct Ttl;
pub(crate) static COMMAND: Ttl = Ttl;

#[derive(Debug, Clone)]
pub(crate) struct OwnedTtl {
    key: Vec<u8>,
}

impl OwnedTtl {
    fn new(key: Vec<u8>) -> Self {
        Self { key }
    }
}

impl super::OwnedCommandData for OwnedTtl {
    type Spec = Ttl;

    fn route_key(&self) -> Option<&[u8]> {
        Some(&self.key)
    }

    fn to_borrowed_command(&self) -> super::BorrowedCommandBox<'_> {
        Box::new(BorrowedTtl::new(&self.key))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BorrowedTtl<'a> {
    key: &'a [u8],
}

impl<'a> BorrowedTtl<'a> {
    fn new(key: &'a [u8]) -> Self {
        Self { key }
    }
}

impl<'a> super::BorrowedCommandData<'a> for BorrowedTtl<'a> {
    type Spec = Ttl;

    fn route_key(&self) -> Option<&'a [u8]> {
        Some(self.key)
    }

    fn to_owned_command(&self) -> Command {
        Command::new(Box::new(OwnedTtl::new(self.key.to_vec())))
    }

    fn execute_engine<'b>(&'b self, ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b,
    {
        Box::pin(async move { Ttl::execute_engine_frame(ctx, self.key).await })
    }

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &crate::storage::EmbeddedStore, _now_ms: u64) -> Frame {
        Frame::Integer(store.ttl_seconds(self.key))
    }

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: BorrowedCommandContext<'_, '_, '_>) {
        ServerWire::write_resp_integer(ctx.out, ctx.store.ttl_seconds(self.key));
    }

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, ctx: DirectCommandContext) -> Frame {
        Frame::Integer(ctx.ttl(self.key, false))
    }
}

impl super::CommandSpec for Ttl {
    const NAME: &'static str = "TTL";
    const MUTATES_VALUE: bool = false;
}

impl super::OwnedCommandParse for Ttl {
    fn parse_owned(parts: &[Vec<u8>]) -> Result<Command> {
        CommandArity::<Self>::exact(parts.len(), 2)?;
        Ok(Command::new(Box::new(OwnedTtl::new(parts[1].clone()))))
    }
}

impl<'a> super::BorrowedCommandParse<'a> for Ttl {
    fn parse_borrowed(parts: &[&'a [u8]]) -> Result<super::BorrowedCommandBox<'a>> {
        CommandArity::<Self>::exact(parts.len(), 2)?;
        Ok(Box::new(BorrowedTtl::new(parts[1])))
    }
}

impl DecodedFastCommand for Ttl {
    fn matches_decoded_fast(&self, command: &FastCommand<'_>) -> bool {
        matches!(command, FastCommand::Ttl { .. })
    }
}

impl EngineCommandDispatch for Ttl {
    fn execute_engine_fast<'a>(
        &'static self,
        ctx: EngineCommandContext<'a>,
        request: FastRequest<'a>,
    ) -> EngineFastFuture<'a> {
        Box::pin(async move {
            let key_hash = request.key_hash;
            match request.command {
                FastCommand::Ttl { key } => Ttl::execute_engine_integer(ctx, key_hash, key, false)
                    .await
                    .map(FastResponse::Integer),
                _ => Ok(FastResponse::Error(b"ERR unsupported command".to_vec())),
            }
        })
    }
}

impl Ttl {
    pub(crate) async fn execute_engine_integer(
        ctx: EngineCommandContext<'_>,
        key_hash: Option<u64>,
        key: &[u8],
        millis: bool,
    ) -> Result<i64> {
        let shard = key_hash
            .map(|hash| ctx.route_key_hash(hash))
            .unwrap_or_else(|| ctx.route_key(key));
        match ctx
            .request(
                shard,
                ShardOperation::Ttl {
                    key: key.to_vec(),
                    millis,
                },
            )
            .await?
        {
            ShardReply::Integer(value) => Ok(value),
            _ => Err(ShardCacheError::Command(
                "TTL received unexpected shard reply".into(),
            )),
        }
    }

    async fn execute_engine_frame(ctx: EngineCommandContext<'_>, key: &[u8]) -> Result<Frame> {
        Self::execute_engine_integer(ctx, None, key, false)
            .await
            .map(Frame::Integer)
    }
}

#[cfg(feature = "server")]
impl RawDirectCommand for Ttl {
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_, '_>) {
        match ctx.args.as_slice() {
            [key] => ServerWire::write_resp_integer(ctx.out, ctx.store.ttl_seconds(key)),
            _ => ServerWire::write_resp_error(
                ctx.out,
                "ERR wrong number of arguments for 'TTL' command",
            ),
        }
    }
}

#[cfg(feature = "server")]
impl DirectFastCommand for Ttl {
    fn execute_direct_fast(
        &self,
        ctx: DirectCommandContext,
        request: FastRequest<'_>,
    ) -> FastResponse {
        match request.command {
            FastCommand::Ttl { key } => FastResponse::Integer(ctx.ttl(key, false)),
            _ => FastResponse::Error(b"ERR unsupported command".to_vec()),
        }
    }
}

#[cfg(feature = "server")]
impl FastDirectCommand for Ttl {
    fn execute_fast(&self, ctx: FastCommandContext<'_, '_>, command: FastCommand<'_>) {
        match command {
            FastCommand::Ttl { key } => {
                ServerWire::write_fast_integer(ctx.out, ctx.store.ttl_seconds(key));
            }
            _ => ServerWire::write_fast_error(ctx.out, "ERR unsupported command"),
        }
    }
}

#[cfg(feature = "server")]
impl ScnpDirectCommand for Ttl {
    fn opcode(&self) -> u8 {
        7
    }

    fn try_execute_scnp(&self, ctx: ScnpCommandContext<'_, '_, '_, '_>) -> ScnpDispatch {
        let frame_len = ctx.frame.frame_len;
        let Ok(Some((request, consumed))) = FastCodec::decode_request(&ctx.frame.buf[..frame_len])
        else {
            return ScnpDispatch::Unsupported;
        };
        let FastCommand::Ttl { key } = request.command else {
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
        ServerWire::write_fast_integer(ctx.out, ctx.store.ttl_seconds(key));
        ScnpDispatch::Complete(consumed)
    }
}
