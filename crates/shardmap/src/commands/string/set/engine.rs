use std::ops::Range;

use bytes::Bytes as SharedBytes;

use crate::commands::parsing::CommandArity;
use crate::commands::{EngineCommandDispatch, EngineRespSpanCommandDispatch};
use crate::protocol::{CommandSpanFrame, FastCommand, FastRequest, FastResponse, Frame};
use crate::storage::{
    EngineCommandContext, EngineFastFuture, EngineRespSpanFuture, ShardKey, ShardOperation,
    ShardReply, ShardValue, hash_key, now_millis,
};
use crate::{Result, ShardCacheError};

use super::Set;
use super::options::StorageSetOptions;

impl EngineCommandDispatch for Set {
    fn execute_engine_fast<'a>(
        &'static self,
        ctx: EngineCommandContext<'a>,
        request: FastRequest<'a>,
    ) -> EngineFastFuture<'a> {
        Box::pin(async move {
            let key_hash = request.key_hash;
            match request.command {
                FastCommand::Set { key, value } => {
                    Set::execute_engine_fast_response(ctx, key_hash, key, value, None).await
                }
                _ => Ok(FastResponse::Error(b"ERR unsupported command".to_vec())),
            }
        })
    }
}

impl EngineRespSpanCommandDispatch for Set {
    fn execute_engine_resp_spanned<'a>(
        &'static self,
        ctx: EngineCommandContext<'a>,
        frame: CommandSpanFrame,
        owner: SharedBytes,
        out: &'a mut Vec<u8>,
    ) -> EngineRespSpanFuture<'a> {
        Box::pin(async move { Self::execute_resp_spanned(ctx, frame, owner, out).await })
    }
}

impl Set {
    async fn execute_resp_spanned(
        ctx: EngineCommandContext<'_>,
        frame: CommandSpanFrame,
        owner: SharedBytes,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        CommandArity::<Self>::at_least(frame.parts.len(), 3, "key and value")?;
        let options = StorageSetOptions::parse(
            frame.parts[3..]
                .iter()
                .map(|range| span_slice(&owner, range)),
        )?;
        Self::store_spanned_value(
            ctx,
            owner,
            copy_span(&frame.parts[1]),
            copy_span(&frame.parts[2]),
            options.ttl_ms,
        )
        .await?;
        out.extend_from_slice(b"+OK\r\n");
        Ok(())
    }

    pub(super) async fn execute_engine_frame(
        ctx: EngineCommandContext<'_>,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
    ) -> Result<Frame> {
        Self::store_value(ctx, None, key, value, ttl_ms).await?;
        Ok(Frame::SimpleString("OK".into()))
    }

    async fn execute_engine_fast_response(
        ctx: EngineCommandContext<'_>,
        key_hash: Option<u64>,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
    ) -> Result<FastResponse> {
        Self::store_value(ctx, key_hash, key, value, ttl_ms).await?;
        Ok(FastResponse::Ok)
    }

    async fn store_value(
        ctx: EngineCommandContext<'_>,
        key_hash: Option<u64>,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
    ) -> Result<()> {
        let key_hash = key_hash.unwrap_or_else(|| hash_key(key));
        let shard = ctx.route_key_hash(key_hash);
        let expire_at_ms = ttl_ms.map(|ttl| now_millis().saturating_add(ttl));
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
            _ => Err(ShardCacheError::Command(
                "SET received unexpected shard reply".into(),
            )),
        }
    }

    async fn store_spanned_value(
        ctx: EngineCommandContext<'_>,
        owner: SharedBytes,
        key_range: std::ops::Range<usize>,
        value_range: std::ops::Range<usize>,
        ttl_ms: Option<u64>,
    ) -> Result<()> {
        let key = &owner[key_range.start..key_range.end];
        let key_hash = hash_key(key);
        let shard = ctx.route_key_hash(key_hash);
        let expire_at_ms = ttl_ms.map(|ttl| now_millis().saturating_add(ttl));
        match ctx
            .request(
                shard,
                ShardOperation::Set {
                    key_hash,
                    key: ShardKey::from_owner(&owner, key_range),
                    value: ShardValue::from_owner(&owner, value_range),
                    expire_at_ms,
                },
            )
            .await?
        {
            ShardReply::Ok => Ok(()),
            _ => Err(ShardCacheError::Command(
                "SET received unexpected shard reply".into(),
            )),
        }
    }
}

fn span_slice<'a>(owner: &'a SharedBytes, range: &Range<usize>) -> &'a [u8] {
    &owner[range.start..range.end]
}

fn copy_span(range: &Range<usize>) -> Range<usize> {
    range.start..range.end
}
