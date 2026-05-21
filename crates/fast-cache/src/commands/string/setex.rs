//! SETEX command parsing and execution.

use crate::commands::EngineCommandDispatch;
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
use crate::storage::{
    Command, EngineCommandContext, EngineFastFuture, EngineFrameFuture, ShardKey, ShardOperation,
    ShardReply, ShardValue, hash_key, now_millis,
};
use crate::{FastCacheError, Result};

use super::DecodedFastCommand;
use super::parsing::{CommandArity, TtlMillis};

pub(crate) struct SetEx;
pub(crate) static COMMAND: SetEx = SetEx;

#[derive(Debug, Clone)]
pub(crate) struct OwnedSetEx {
    key: Vec<u8>,
    ttl_ms: u64,
    value: Vec<u8>,
}

impl OwnedSetEx {
    fn new(key: Vec<u8>, ttl_ms: u64, value: Vec<u8>) -> Self {
        Self { key, ttl_ms, value }
    }
}

impl super::OwnedCommandData for OwnedSetEx {
    type Spec = SetEx;

    fn route_key(&self) -> Option<&[u8]> {
        Some(&self.key)
    }

    fn to_borrowed_command(&self) -> super::BorrowedCommandBox<'_> {
        Box::new(BorrowedSetEx::new(&self.key, self.ttl_ms, &self.value))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BorrowedSetEx<'a> {
    key: &'a [u8],
    ttl_ms: u64,
    value: &'a [u8],
}

impl<'a> BorrowedSetEx<'a> {
    fn new(key: &'a [u8], ttl_ms: u64, value: &'a [u8]) -> Self {
        Self { key, ttl_ms, value }
    }
}

impl<'a> super::BorrowedCommandData<'a> for BorrowedSetEx<'a> {
    type Spec = SetEx;

    fn route_key(&self) -> Option<&'a [u8]> {
        Some(self.key)
    }

    fn to_owned_command(&self) -> Command {
        Command::new(Box::new(OwnedSetEx::new(
            self.key.to_vec(),
            self.ttl_ms,
            self.value.to_vec(),
        )))
    }

    fn execute_engine<'b>(&'b self, ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b,
    {
        let key = self.key;
        let ttl_ms = self.ttl_ms;
        let value = self.value;
        Box::pin(async move { SetEx::execute_engine_frame(ctx, key, ttl_ms, value).await })
    }

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &crate::storage::EmbeddedStore, _now_ms: u64) -> Frame {
        store.set(self.key.to_vec(), self.value.to_vec(), Some(self.ttl_ms));
        Frame::SimpleString("OK".into())
    }

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: BorrowedCommandContext<'_, '_, '_>) {
        ctx.store
            .set(self.key.to_vec(), self.value.to_vec(), Some(self.ttl_ms));
        ctx.out.extend_from_slice(b"+OK\r\n");
    }

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, ctx: DirectCommandContext) -> Frame {
        ctx.set_owned(self.key.to_vec(), self.value.to_vec(), Some(self.ttl_ms));
        Frame::SimpleString("OK".into())
    }
}

impl super::CommandSpec for SetEx {
    const NAME: &'static str = "SETEX";
    const MUTATES_VALUE: bool = true;
}

impl super::OwnedCommandParse for SetEx {
    fn parse_owned(parts: &[Vec<u8>]) -> Result<Command> {
        CommandArity::<Self>::exact(parts.len(), 4)?;
        Ok(Command::new(Box::new(OwnedSetEx::new(
            parts[1].clone(),
            TtlMillis::<Self>::seconds(&parts[2])?,
            parts[3].clone(),
        ))))
    }
}

impl<'a> super::BorrowedCommandParse<'a> for SetEx {
    fn parse_borrowed(parts: &[&'a [u8]]) -> Result<super::BorrowedCommandBox<'a>> {
        CommandArity::<Self>::exact(parts.len(), 4)?;
        Ok(Box::new(BorrowedSetEx::new(
            parts[1],
            TtlMillis::<Self>::seconds(parts[2])?,
            parts[3],
        )))
    }
}

impl DecodedFastCommand for SetEx {
    fn matches_decoded_fast(&self, command: &FastCommand<'_>) -> bool {
        matches!(command, FastCommand::SetEx { .. })
    }
}

impl EngineCommandDispatch for SetEx {
    fn execute_engine_fast<'a>(
        &'static self,
        ctx: EngineCommandContext<'a>,
        request: FastRequest<'a>,
    ) -> EngineFastFuture<'a> {
        Box::pin(async move {
            match request.command {
                FastCommand::SetEx { key, value, ttl_ms } => {
                    SetEx::store_value(ctx, key, ttl_ms, value).await?;
                    Ok(FastResponse::Ok)
                }
                _ => Ok(FastResponse::Error(b"ERR unsupported command".to_vec())),
            }
        })
    }
}

impl SetEx {
    async fn execute_engine_frame(
        ctx: EngineCommandContext<'_>,
        key: &[u8],
        ttl_ms: u64,
        value: &[u8],
    ) -> Result<Frame> {
        Self::store_value(ctx, key, ttl_ms, value).await?;
        Ok(Frame::SimpleString("OK".into()))
    }

    pub(crate) async fn store_value(
        ctx: EngineCommandContext<'_>,
        key: &[u8],
        ttl_ms: u64,
        value: &[u8],
    ) -> Result<()> {
        let key_hash = hash_key(key);
        let shard = ctx.route_key_hash(key_hash);
        let expire_at_ms = Some(now_millis().saturating_add(ttl_ms));
        match ctx
            .request(
                shard,
                ShardOperation::Set {
                    key_hash,
                    key: ShardKey::inline(key),
                    value: ShardValue::inline(value),
                    expire_at_ms,
                },
            )
            .await?
        {
            ShardReply::Ok => Ok(()),
            _ => Err(FastCacheError::Command(
                "SETEX received unexpected shard reply".into(),
            )),
        }
    }
}

#[cfg(feature = "server")]
impl RawDirectCommand for SetEx {
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_>) {
        match ctx.args.as_slice() {
            [key, ttl, value] => match TtlMillis::<()>::ascii_seconds(ttl) {
                Some(ttl_ms) => {
                    ctx.store.set(key.to_vec(), value.to_vec(), Some(ttl_ms));
                    ctx.out.extend_from_slice(b"+OK\r\n");
                }
                None => ServerWire::write_resp_error(ctx.out, "ERR value is not an integer"),
            },
            _ => ServerWire::write_resp_error(
                ctx.out,
                "ERR wrong number of arguments for 'SETEX' command",
            ),
        }
    }
}

#[cfg(feature = "server")]
impl DirectFastCommand for SetEx {
    fn execute_direct_fast(
        &self,
        ctx: DirectCommandContext,
        request: FastRequest<'_>,
    ) -> FastResponse {
        match request.command {
            FastCommand::SetEx { key, value, ttl_ms } => {
                ctx.set_owned(key.to_vec(), value.to_vec(), Some(ttl_ms));
                FastResponse::Ok
            }
            _ => FastResponse::Error(b"ERR unsupported command".to_vec()),
        }
    }
}

#[cfg(feature = "server")]
impl FastDirectCommand for SetEx {
    fn execute_fast(&self, ctx: FastCommandContext<'_, '_>, command: FastCommand<'_>) {
        match command {
            FastCommand::SetEx { key, value, ttl_ms } => {
                ctx.store.set(key.to_vec(), value.to_vec(), Some(ttl_ms));
                ServerWire::write_fast_ok(ctx.out);
            }
            _ => ServerWire::write_fast_error(ctx.out, "ERR unsupported command"),
        }
    }
}

#[cfg(feature = "server")]
impl FcnpDirectCommand for SetEx {
    fn opcode(&self) -> u8 {
        3
    }

    fn try_execute_fcnp(&self, ctx: FcnpCommandContext<'_, '_, '_, '_>) -> FcnpDispatch {
        let frame_len = ctx.frame.frame_len;
        let Ok(Some((request, consumed))) = FastCodec::decode_request(&ctx.frame.buf[..frame_len])
        else {
            return FcnpDispatch::Unsupported;
        };
        let FastCommand::SetEx { key, value, ttl_ms } = request.command else {
            return FcnpDispatch::Unsupported;
        };
        let Some(key_hash) = request.key_hash else {
            return FcnpDispatch::Unsupported;
        };
        match ctx.fcnp_route_matches_owned_shard_for_key(request.route_shard, key_hash, key) {
            true => {}
            false => {
                ServerWire::write_fast_error(ctx.out, "ERR FCNP route shard mismatch");
                return FcnpDispatch::Complete(consumed);
            }
        }
        ctx.store.set(key.to_vec(), value.to_vec(), Some(ttl_ms));
        ServerWire::write_fast_ok(ctx.out);
        FcnpDispatch::Complete(consumed)
    }
}
