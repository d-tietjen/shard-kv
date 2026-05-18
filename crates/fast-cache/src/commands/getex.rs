//! GETEX command parsing and execution.

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
    Command, EngineCommandContext, EngineFastFuture, EngineFrameFuture, ExpirationChange, ShardKey,
    ShardOperation, ShardReply, hash_key, now_millis,
};
use crate::{FastCacheError, Result};

use super::DecodedFastCommand;
use super::parsing::{CommandArity, TtlMillis};

pub(crate) struct GetEx;
pub(crate) static COMMAND: GetEx = GetEx;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GetExExpiration {
    Keep,
    ExpireIn(u64),
    Persist,
}

impl GetExExpiration {
    fn to_engine(self) -> ExpirationChange {
        match self {
            Self::Keep => ExpirationChange::Keep,
            Self::ExpireIn(ttl_ms) => ExpirationChange::ExpireAt(relative_expire_at_ms(ttl_ms)),
            Self::Persist => ExpirationChange::Persist,
        }
    }

    #[cfg(feature = "server")]
    fn expire_at_ms(self, now_ms: u64) -> Option<Option<u64>> {
        match self {
            Self::Keep => None,
            Self::ExpireIn(ttl_ms) => Some(Some(now_ms.saturating_add(ttl_ms))),
            Self::Persist => Some(None),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OwnedGetEx {
    key: Vec<u8>,
    expiration: GetExExpiration,
}

impl OwnedGetEx {
    fn new(key: Vec<u8>, expiration: GetExExpiration) -> Self {
        Self { key, expiration }
    }
}

impl super::OwnedCommandData for OwnedGetEx {
    type Spec = GetEx;

    fn route_key(&self) -> Option<&[u8]> {
        Some(&self.key)
    }

    fn to_borrowed_command(&self) -> super::BorrowedCommandBox<'_> {
        Box::new(BorrowedGetEx::new(&self.key, self.expiration))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BorrowedGetEx<'a> {
    key: &'a [u8],
    expiration: GetExExpiration,
}

impl<'a> BorrowedGetEx<'a> {
    fn new(key: &'a [u8], expiration: GetExExpiration) -> Self {
        Self { key, expiration }
    }
}

impl<'a> super::BorrowedCommandData<'a> for BorrowedGetEx<'a> {
    type Spec = GetEx;

    fn route_key(&self) -> Option<&'a [u8]> {
        Some(self.key)
    }

    fn to_owned_command(&self) -> Command {
        Command::new(Box::new(OwnedGetEx::new(
            self.key.to_vec(),
            self.expiration,
        )))
    }

    fn execute_engine<'b>(&'b self, ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b,
    {
        let key = self.key;
        let expiration = self.expiration.to_engine();
        Box::pin(async move { GetEx::execute_engine_frame(ctx, key, expiration).await })
    }

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &crate::storage::EmbeddedStore, now_ms: u64) -> Frame {
        match Self::execute_embedded(store, self.key, self.expiration, now_ms) {
            Some(value) => Frame::BlobString(value),
            None => Frame::Null,
        }
    }

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: BorrowedCommandContext<'_, '_, '_>) {
        match Self::execute_embedded(ctx.store, self.key, self.expiration, now_millis()) {
            Some(value) => ServerWire::write_resp_blob_string(ctx.out, &value),
            None => ctx.out.extend_from_slice(b"$-1\r\n"),
        }
    }

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, ctx: DirectCommandContext) -> Frame {
        let expire_at_ms = self.expiration.expire_at_ms(ctx.now_ms);
        let value = match expire_at_ms {
            Some(expire_at_ms) => ctx.getex(self.key, expire_at_ms),
            None => ctx.get(self.key),
        };
        value.map_or(Frame::Null, Frame::BlobString)
    }
}

impl super::CommandSpec for GetEx {
    const NAME: &'static str = "GETEX";
    const MUTATES_VALUE: bool = true;
}

impl super::OwnedCommandParse for GetEx {
    fn parse_owned(parts: &[Vec<u8>]) -> Result<Command> {
        CommandArity::<Self>::at_least(parts.len(), 2, "key")?;
        Ok(Command::new(Box::new(OwnedGetEx::new(
            parts[1].clone(),
            parse_getex_expiration(&parts[2..])?,
        ))))
    }
}

impl<'a> super::BorrowedCommandParse<'a> for GetEx {
    fn parse_borrowed(parts: &[&'a [u8]]) -> Result<super::BorrowedCommandBox<'a>> {
        CommandArity::<Self>::at_least(parts.len(), 2, "key")?;
        Ok(Box::new(BorrowedGetEx::new(
            parts[1],
            parse_getex_expiration(&parts[2..])?,
        )))
    }
}

impl DecodedFastCommand for GetEx {
    fn matches_decoded_fast(&self, command: &FastCommand<'_>) -> bool {
        matches!(command, FastCommand::GetEx { .. })
    }
}

impl EngineCommandDispatch for GetEx {
    fn execute_engine_fast<'a>(
        &'static self,
        ctx: EngineCommandContext<'a>,
        request: FastRequest<'a>,
    ) -> EngineFastFuture<'a> {
        Box::pin(async move {
            match request.command {
                FastCommand::GetEx { key, ttl_ms } => {
                    GetEx::execute_engine_response(
                        ctx,
                        key,
                        ExpirationChange::ExpireAt(relative_expire_at_ms(ttl_ms)),
                    )
                    .await
                }
                _ => Ok(FastResponse::Error(b"ERR unsupported command".to_vec())),
            }
        })
    }
}

impl GetEx {
    async fn execute_engine_frame(
        ctx: EngineCommandContext<'_>,
        key: &[u8],
        expiration: ExpirationChange,
    ) -> Result<Frame> {
        match Self::load_and_update(ctx, key, expiration).await? {
            Some(value) => Ok(Frame::BlobString(value)),
            None => Ok(Frame::Null),
        }
    }

    async fn execute_engine_response(
        ctx: EngineCommandContext<'_>,
        key: &[u8],
        expiration: ExpirationChange,
    ) -> Result<FastResponse> {
        match Self::load_and_update(ctx, key, expiration).await? {
            Some(value) => Ok(FastResponse::Value(value)),
            None => Ok(FastResponse::Null),
        }
    }

    async fn load_and_update(
        ctx: EngineCommandContext<'_>,
        key: &[u8],
        expiration: ExpirationChange,
    ) -> Result<Option<Vec<u8>>> {
        let key_hash = hash_key(key);
        let shard = ctx.route_key_hash(key_hash);
        match ctx
            .request(
                shard,
                ShardOperation::GetEx {
                    key_hash,
                    key: ShardKey::inline(key),
                    expiration,
                },
            )
            .await?
        {
            ShardReply::Value(value) => Ok(value),
            _ => Err(FastCacheError::Command(
                "GETEX received unexpected shard reply".into(),
            )),
        }
    }
}

#[cfg(feature = "server")]
impl<'a> BorrowedGetEx<'a> {
    fn execute_embedded(
        store: &crate::storage::EmbeddedStore,
        key: &[u8],
        expiration: GetExExpiration,
        now_ms: u64,
    ) -> Option<Vec<u8>> {
        let value = store.get(key);
        if value.is_some() {
            match expiration.expire_at_ms(now_ms) {
                Some(Some(expire_at_ms)) => {
                    store.expire(key, expire_at_ms);
                }
                Some(None) => {
                    store.persist(key);
                }
                None => {}
            }
        }
        value
    }
}

fn parse_getex_expiration(args: &[impl AsRef<[u8]>]) -> Result<GetExExpiration> {
    match args {
        [] => Ok(GetExExpiration::Keep),
        [option] if option.as_ref().eq_ignore_ascii_case(b"PERSIST") => {
            Ok(GetExExpiration::Persist)
        }
        [option, value] if option.as_ref().eq_ignore_ascii_case(b"EX") => {
            TtlMillis::<GetEx>::seconds(value.as_ref()).map(GetExExpiration::ExpireIn)
        }
        [option, value] if option.as_ref().eq_ignore_ascii_case(b"PX") => {
            TtlMillis::<GetEx>::millis(value.as_ref()).map(GetExExpiration::ExpireIn)
        }
        _ => Err(FastCacheError::Command("GETEX syntax error".into())),
    }
}

fn relative_expire_at_ms(ttl_ms: u64) -> u64 {
    now_millis().saturating_add(ttl_ms)
}

#[cfg(feature = "server")]
impl RawDirectCommand for GetEx {
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_>) {
        match parse_getex_raw(ctx.args.as_slice()) {
            Some((key, expiration)) => {
                match BorrowedGetEx::execute_embedded(ctx.store, key, expiration, now_millis()) {
                    Some(value) => ServerWire::write_resp_blob_string(ctx.out, &value),
                    None => ctx.out.extend_from_slice(b"$-1\r\n"),
                }
            }
            None => ServerWire::write_resp_error(ctx.out, "ERR syntax error"),
        }
    }
}

#[cfg(feature = "server")]
fn parse_getex_raw<'a>(args: &'a [&'a [u8]]) -> Option<(&'a [u8], GetExExpiration)> {
    match args {
        [key] => Some((key, GetExExpiration::Keep)),
        [key, option] if option.eq_ignore_ascii_case(b"PERSIST") => {
            Some((key, GetExExpiration::Persist))
        }
        [key, option, value] if option.eq_ignore_ascii_case(b"EX") => {
            TtlMillis::<()>::ascii_seconds(value)
                .map(|ttl_ms| (*key, GetExExpiration::ExpireIn(ttl_ms)))
        }
        [key, option, value] if option.eq_ignore_ascii_case(b"PX") => {
            TtlMillis::<()>::ascii_millis(value)
                .map(|ttl_ms| (*key, GetExExpiration::ExpireIn(ttl_ms)))
        }
        _ => None,
    }
}

#[cfg(feature = "server")]
impl DirectFastCommand for GetEx {
    fn execute_direct_fast(
        &self,
        ctx: DirectCommandContext,
        request: FastRequest<'_>,
    ) -> FastResponse {
        match request.command {
            FastCommand::GetEx { key, ttl_ms } => ctx
                .getex(key, Some(ctx.now_ms.saturating_add(ttl_ms)))
                .map_or(FastResponse::Null, FastResponse::Value),
            _ => FastResponse::Error(b"ERR unsupported command".to_vec()),
        }
    }
}

#[cfg(feature = "server")]
impl FastDirectCommand for GetEx {
    fn execute_fast(&self, ctx: FastCommandContext<'_, '_>, command: FastCommand<'_>) {
        match command {
            FastCommand::GetEx { key, ttl_ms } => match ctx.store.get(key) {
                Some(value) => {
                    ctx.store.expire(key, relative_expire_at_ms(ttl_ms));
                    ServerWire::write_fast_value(ctx.out, &value);
                }
                None => ServerWire::write_fast_null(ctx.out),
            },
            _ => ServerWire::write_fast_error(ctx.out, "ERR unsupported command"),
        }
    }
}

#[cfg(feature = "server")]
impl FcnpDirectCommand for GetEx {
    fn opcode(&self) -> u8 {
        4
    }

    fn try_execute_fcnp(&self, ctx: FcnpCommandContext<'_, '_, '_, '_>) -> FcnpDispatch {
        let frame_len = ctx.frame.frame_len;
        let Ok(Some((request, consumed))) = FastCodec::decode_request(&ctx.frame.buf[..frame_len])
        else {
            return FcnpDispatch::Unsupported;
        };
        let FastCommand::GetEx { key, ttl_ms } = request.command else {
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
        match ctx.store.get(key) {
            Some(value) => {
                ctx.store.expire(key, relative_expire_at_ms(ttl_ms));
                ServerWire::write_fast_value(ctx.out, &value);
            }
            None => ServerWire::write_fast_null(ctx.out),
        }
        FcnpDispatch::Complete(consumed)
    }
}
