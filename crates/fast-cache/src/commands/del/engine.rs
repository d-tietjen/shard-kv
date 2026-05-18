use crate::commands::EngineCommandDispatch;
use crate::protocol::{FastCommand, FastRequest, FastResponse, Frame};
use crate::storage::{
    EngineCommandContext, EngineFastFuture, ShardKey, ShardOperation, ShardReply, hash_key,
};
use crate::{FastCacheError, Result};

use super::Del;

impl EngineCommandDispatch for Del {
    fn execute_engine_fast<'a>(
        &'static self,
        ctx: EngineCommandContext<'a>,
        request: FastRequest<'a>,
    ) -> EngineFastFuture<'a> {
        Box::pin(async move {
            let key_hash = request.key_hash;
            match request.command {
                FastCommand::Delete { key } => {
                    Del::execute_engine_fast_response(ctx, key_hash, key).await
                }
                _ => Ok(FastResponse::Error(b"ERR unsupported command".to_vec())),
            }
        })
    }
}

impl Del {
    pub(super) async fn execute_engine_frame(
        ctx: EngineCommandContext<'_>,
        keys: &[&[u8]],
    ) -> Result<Frame> {
        Self::delete_keys(ctx, keys).await.map(Frame::Integer)
    }

    async fn execute_engine_fast_response(
        ctx: EngineCommandContext<'_>,
        key_hash: Option<u64>,
        key: &[u8],
    ) -> Result<FastResponse> {
        Self::delete_key(ctx, key_hash, key)
            .await
            .map(|deleted| FastResponse::Integer(deleted as i64))
    }

    async fn delete_keys(ctx: EngineCommandContext<'_>, keys: &[&[u8]]) -> Result<i64> {
        let mut deleted = 0i64;
        for key in keys {
            deleted += Self::delete_key(ctx, None, key).await? as i64;
        }
        Ok(deleted)
    }

    async fn delete_key(
        ctx: EngineCommandContext<'_>,
        key_hash: Option<u64>,
        key: &[u8],
    ) -> Result<bool> {
        let key_hash = key_hash.unwrap_or_else(|| hash_key(key));
        let shard = ctx.route_key_hash(key_hash);
        match ctx
            .request(
                shard,
                ShardOperation::Delete {
                    key_hash,
                    key: ShardKey::inline(key),
                },
            )
            .await?
        {
            ShardReply::Integer(deleted) => Ok(deleted != 0),
            _ => Err(FastCacheError::Command(
                "DEL received unexpected shard reply".into(),
            )),
        }
    }
}
