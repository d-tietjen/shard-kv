use crate::protocol::{FastCommand, FastRequest, FastResponse, Frame};
use crate::storage::{EngineCommandContext, EngineFastFuture, ShardOperation, ShardReply};
use crate::{Result, ShardCacheError};

use super::Get;
use crate::commands::EngineCommandDispatch;

impl EngineCommandDispatch for Get {
    fn execute_engine_fast<'a>(
        &'static self,
        ctx: EngineCommandContext<'a>,
        request: FastRequest<'a>,
    ) -> EngineFastFuture<'a> {
        Box::pin(async move {
            let key_hash = request.key_hash;
            match request.command {
                FastCommand::Get { key } => {
                    Get::execute_engine_fast_response(ctx, key_hash, key).await
                }
                _ => Ok(FastResponse::Error(b"ERR unsupported command".to_vec())),
            }
        })
    }
}

impl Get {
    pub(super) async fn execute_engine_frame(
        ctx: EngineCommandContext<'_>,
        key: &[u8],
    ) -> Result<Frame> {
        match Self::load_value(ctx, None, key).await? {
            Some(value) => Ok(Frame::BlobString(value)),
            None => Ok(Frame::Null),
        }
    }

    async fn execute_engine_fast_response(
        ctx: EngineCommandContext<'_>,
        key_hash: Option<u64>,
        key: &[u8],
    ) -> Result<FastResponse> {
        match Self::load_value(ctx, key_hash, key).await? {
            Some(value) => Ok(FastResponse::Value(value)),
            None => Ok(FastResponse::Null),
        }
    }

    async fn load_value(
        ctx: EngineCommandContext<'_>,
        key_hash: Option<u64>,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let shard = key_hash
            .map(|hash| ctx.route_key_hash(hash))
            .unwrap_or_else(|| ctx.route_key(key));
        match ctx
            .request(shard, ShardOperation::Get(key.to_vec()))
            .await?
        {
            ShardReply::Value(value) => Ok(value),
            _ => Err(ShardCacheError::Command(
                "GET received unexpected shard reply".into(),
            )),
        }
    }
}
