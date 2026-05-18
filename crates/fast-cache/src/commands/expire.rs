//! EXPIRE command parsing and execution.

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
    ShardReply, hash_key, now_millis,
};
use crate::{FastCacheError, Result};

use super::DecodedFastCommand;
use super::parsing::{CommandArity, TtlMillis};

pub(crate) struct Expire;
pub(crate) static COMMAND: Expire = Expire;

#[derive(Debug, Clone)]
pub(crate) struct OwnedExpire {
    key: Vec<u8>,
    ttl_ms: u64,
}

impl OwnedExpire {
    fn new(key: Vec<u8>, ttl_ms: u64) -> Self {
        Self { key, ttl_ms }
    }
}

impl super::OwnedCommandData for OwnedExpire {
    type Spec = Expire;

    fn route_key(&self) -> Option<&[u8]> {
        Some(&self.key)
    }

    fn to_borrowed_command(&self) -> super::BorrowedCommandBox<'_> {
        Box::new(BorrowedExpire::new(&self.key, self.ttl_ms))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BorrowedExpire<'a> {
    key: &'a [u8],
    ttl_ms: u64,
}

impl<'a> BorrowedExpire<'a> {
    fn new(key: &'a [u8], ttl_ms: u64) -> Self {
        Self { key, ttl_ms }
    }
}

impl<'a> super::BorrowedCommandData<'a> for BorrowedExpire<'a> {
    type Spec = Expire;

    fn route_key(&self) -> Option<&'a [u8]> {
        Some(self.key)
    }

    fn to_owned_command(&self) -> Command {
        Command::new(Box::new(OwnedExpire::new(self.key.to_vec(), self.ttl_ms)))
    }

    fn execute_engine<'b>(&'b self, ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b,
    {
        let key = self.key;
        let expire_at_ms = relative_expire_at_ms(self.ttl_ms);
        Box::pin(async move { Expire::execute_engine_frame(ctx, key, expire_at_ms).await })
    }

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &crate::storage::EmbeddedStore, _now_ms: u64) -> Frame {
        Frame::Integer(store.expire(self.key, relative_expire_at_ms(self.ttl_ms)) as i64)
    }

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: BorrowedCommandContext<'_, '_, '_>) {
        let changed = ctx
            .store
            .expire(self.key, relative_expire_at_ms(self.ttl_ms));
        ServerWire::write_resp_integer(ctx.out, changed as i64);
    }

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, ctx: DirectCommandContext) -> Frame {
        Frame::Integer(ctx.expire_at(self.key, ctx.now_ms.saturating_add(self.ttl_ms)) as i64)
    }
}

impl super::CommandSpec for Expire {
    const NAME: &'static str = "EXPIRE";
    const MUTATES_VALUE: bool = true;
}

impl super::OwnedCommandParse for Expire {
    fn parse_owned(parts: &[Vec<u8>]) -> Result<Command> {
        CommandArity::<Self>::exact(parts.len(), 3)?;
        Ok(Command::new(Box::new(OwnedExpire::new(
            parts[1].clone(),
            TtlMillis::<Self>::seconds(&parts[2])?,
        ))))
    }
}

impl<'a> super::BorrowedCommandParse<'a> for Expire {
    fn parse_borrowed(parts: &[&'a [u8]]) -> Result<super::BorrowedCommandBox<'a>> {
        CommandArity::<Self>::exact(parts.len(), 3)?;
        Ok(Box::new(BorrowedExpire::new(
            parts[1],
            TtlMillis::<Self>::seconds(parts[2])?,
        )))
    }
}

impl DecodedFastCommand for Expire {
    fn matches_decoded_fast(&self, command: &FastCommand<'_>) -> bool {
        matches!(command, FastCommand::Expire { .. })
    }
}

impl EngineCommandDispatch for Expire {
    fn execute_engine_fast<'a>(
        &'static self,
        ctx: EngineCommandContext<'a>,
        request: FastRequest<'a>,
    ) -> EngineFastFuture<'a> {
        Box::pin(async move {
            match request.command {
                FastCommand::Expire { key, ttl_ms } => {
                    Expire::execute_engine_integer(ctx, key, relative_expire_at_ms(ttl_ms))
                        .await
                        .map(FastResponse::Integer)
                }
                _ => Ok(FastResponse::Error(b"ERR unsupported command".to_vec())),
            }
        })
    }
}

impl Expire {
    pub(crate) async fn execute_engine_integer(
        ctx: EngineCommandContext<'_>,
        key: &[u8],
        expire_at_ms: u64,
    ) -> Result<i64> {
        let key_hash = hash_key(key);
        let shard = ctx.route_key_hash(key_hash);
        match ctx
            .request(
                shard,
                ShardOperation::Expire {
                    key_hash,
                    key: ShardKey::inline(key),
                    expire_at_ms: Some(expire_at_ms),
                },
            )
            .await?
        {
            ShardReply::Integer(value) => Ok(value),
            _ => Err(FastCacheError::Command(
                "EXPIRE received unexpected shard reply".into(),
            )),
        }
    }

    async fn execute_engine_frame(
        ctx: EngineCommandContext<'_>,
        key: &[u8],
        expire_at_ms: u64,
    ) -> Result<Frame> {
        Self::execute_engine_integer(ctx, key, expire_at_ms)
            .await
            .map(Frame::Integer)
    }
}

pub(crate) fn relative_expire_at_ms(ttl_ms: u64) -> u64 {
    now_millis().saturating_add(ttl_ms)
}

#[cfg(feature = "server")]
impl RawDirectCommand for Expire {
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_>) {
        match ctx.args.as_slice() {
            [key, ttl] => match TtlMillis::<()>::ascii_seconds(ttl) {
                Some(ttl_ms) => {
                    let changed = ctx.store.expire(key, relative_expire_at_ms(ttl_ms));
                    ServerWire::write_resp_integer(ctx.out, changed as i64);
                }
                None => ServerWire::write_resp_error(ctx.out, "ERR value is not an integer"),
            },
            _ => ServerWire::write_resp_error(
                ctx.out,
                "ERR wrong number of arguments for 'EXPIRE' command",
            ),
        }
    }
}

#[cfg(feature = "server")]
impl DirectFastCommand for Expire {
    fn execute_direct_fast(
        &self,
        ctx: DirectCommandContext,
        request: FastRequest<'_>,
    ) -> FastResponse {
        match request.command {
            FastCommand::Expire { key, ttl_ms } => {
                FastResponse::Integer(ctx.expire_at(key, ctx.now_ms.saturating_add(ttl_ms)) as i64)
            }
            _ => FastResponse::Error(b"ERR unsupported command".to_vec()),
        }
    }
}

#[cfg(feature = "server")]
impl FastDirectCommand for Expire {
    fn execute_fast(&self, ctx: FastCommandContext<'_, '_>, command: FastCommand<'_>) {
        match command {
            FastCommand::Expire { key, ttl_ms } => {
                let changed = ctx.store.expire(key, relative_expire_at_ms(ttl_ms));
                ServerWire::write_fast_integer(ctx.out, changed as i64);
            }
            _ => ServerWire::write_fast_error(ctx.out, "ERR unsupported command"),
        }
    }
}

#[cfg(feature = "server")]
impl FcnpDirectCommand for Expire {
    fn opcode(&self) -> u8 {
        8
    }

    fn try_execute_fcnp(&self, ctx: FcnpCommandContext<'_, '_, '_, '_>) -> FcnpDispatch {
        let frame_len = ctx.frame.frame_len;
        let Ok(Some((request, consumed))) = FastCodec::decode_request(&ctx.frame.buf[..frame_len])
        else {
            return FcnpDispatch::Unsupported;
        };
        let FastCommand::Expire { key, ttl_ms } = request.command else {
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
        let changed = ctx.store.expire(key, relative_expire_at_ms(ttl_ms));
        ServerWire::write_fast_integer(ctx.out, changed as i64);
        FcnpDispatch::Complete(consumed)
    }
}
