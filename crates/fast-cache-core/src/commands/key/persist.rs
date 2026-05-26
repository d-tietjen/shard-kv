//! PERSIST command parsing and execution.

use crate::FastCacheError;
use crate::Result;
use crate::protocol::{FastCommand, Frame};
#[cfg(feature = "server")]
use crate::server::commands::{BorrowedCommandContext, DirectCommandContext};
#[cfg(feature = "server")]
use crate::server::commands::{RawCommandContext, RawDirectCommand};
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{
    Command, EngineCommandContext, EngineFrameFuture, ShardKey, ShardOperation, ShardReply,
    hash_key,
};

use super::DecodedFastCommand;
use super::parsing::CommandArity;

pub(crate) struct Persist;
pub(crate) static COMMAND: Persist = Persist;

#[derive(Debug, Clone)]
pub(crate) struct OwnedPersist {
    key: Vec<u8>,
}

impl OwnedPersist {
    fn new(key: Vec<u8>) -> Self {
        Self { key }
    }
}

impl super::OwnedCommandData for OwnedPersist {
    type Spec = Persist;

    fn route_key(&self) -> Option<&[u8]> {
        Some(&self.key)
    }

    fn to_borrowed_command(&self) -> super::BorrowedCommandBox<'_> {
        Box::new(BorrowedPersist::new(&self.key))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BorrowedPersist<'a> {
    key: &'a [u8],
}

impl<'a> BorrowedPersist<'a> {
    fn new(key: &'a [u8]) -> Self {
        Self { key }
    }
}

impl<'a> super::BorrowedCommandData<'a> for BorrowedPersist<'a> {
    type Spec = Persist;

    fn route_key(&self) -> Option<&'a [u8]> {
        Some(self.key)
    }

    fn to_owned_command(&self) -> Command {
        Command::new(Box::new(OwnedPersist::new(self.key.to_vec())))
    }

    fn execute_engine<'b>(&'b self, ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b,
    {
        Box::pin(async move { Persist::execute_engine_frame(ctx, self.key).await })
    }

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &crate::storage::EmbeddedStore, _now_ms: u64) -> Frame {
        Frame::Integer(store.persist(self.key) as i64)
    }

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: BorrowedCommandContext<'_, '_, '_>) {
        ServerWire::write_resp_integer(ctx.out, ctx.store.persist(self.key) as i64);
    }

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, ctx: DirectCommandContext) -> Frame {
        Frame::Integer(ctx.persist(self.key) as i64)
    }
}

impl super::CommandSpec for Persist {
    const NAME: &'static str = "PERSIST";
    const MUTATES_VALUE: bool = true;
}

impl super::OwnedCommandParse for Persist {
    fn parse_owned(parts: &[Vec<u8>]) -> Result<Command> {
        CommandArity::<Self>::exact(parts.len(), 2)?;
        Ok(Command::new(Box::new(OwnedPersist::new(parts[1].clone()))))
    }
}

impl<'a> super::BorrowedCommandParse<'a> for Persist {
    fn parse_borrowed(parts: &[&'a [u8]]) -> Result<super::BorrowedCommandBox<'a>> {
        CommandArity::<Self>::exact(parts.len(), 2)?;
        Ok(Box::new(BorrowedPersist::new(parts[1])))
    }
}

impl DecodedFastCommand for Persist {
    fn matches_decoded_fast(&self, _command: &FastCommand<'_>) -> bool {
        false
    }
}

impl Persist {
    async fn execute_engine_frame(ctx: EngineCommandContext<'_>, key: &[u8]) -> Result<Frame> {
        let key_hash = hash_key(key);
        let shard = ctx.route_key_hash(key_hash);
        match ctx
            .request(
                shard,
                ShardOperation::Expire {
                    key_hash,
                    key: ShardKey::inline(key),
                    expire_at_ms: None,
                },
            )
            .await?
        {
            ShardReply::Integer(value) => Ok(Frame::Integer(value)),
            _ => Err(FastCacheError::Command(
                "PERSIST received unexpected shard reply".into(),
            )),
        }
    }
}

#[cfg(feature = "server")]
impl RawDirectCommand for Persist {
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_, '_>) {
        match ctx.args.as_slice() {
            [key] => ServerWire::write_resp_integer(ctx.out, ctx.store.persist(key) as i64),
            _ => ServerWire::write_resp_error(
                ctx.out,
                "ERR wrong number of arguments for 'PERSIST' command",
            ),
        }
    }
}
