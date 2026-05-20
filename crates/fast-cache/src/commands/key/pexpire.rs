//! PEXPIRE command parsing and execution.

use crate::Result;
use crate::protocol::{FastCommand, Frame};
#[cfg(feature = "server")]
use crate::server::commands::{BorrowedCommandContext, DirectCommandContext};
#[cfg(feature = "server")]
use crate::server::commands::{RawCommandContext, RawDirectCommand};
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{Command, EngineCommandContext, EngineFrameFuture};

use super::DecodedFastCommand;
use super::expire::{Expire, relative_expire_at_ms};
use super::parsing::{CommandArity, TtlMillis};

pub(crate) struct PExpire;
pub(crate) static COMMAND: PExpire = PExpire;

#[derive(Debug, Clone)]
pub(crate) struct OwnedPExpire {
    key: Vec<u8>,
    ttl_ms: u64,
}

impl OwnedPExpire {
    fn new(key: Vec<u8>, ttl_ms: u64) -> Self {
        Self { key, ttl_ms }
    }
}

impl super::OwnedCommandData for OwnedPExpire {
    type Spec = PExpire;

    fn route_key(&self) -> Option<&[u8]> {
        Some(&self.key)
    }

    fn to_borrowed_command(&self) -> super::BorrowedCommandBox<'_> {
        Box::new(BorrowedPExpire::new(&self.key, self.ttl_ms))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BorrowedPExpire<'a> {
    key: &'a [u8],
    ttl_ms: u64,
}

impl<'a> BorrowedPExpire<'a> {
    fn new(key: &'a [u8], ttl_ms: u64) -> Self {
        Self { key, ttl_ms }
    }
}

impl<'a> super::BorrowedCommandData<'a> for BorrowedPExpire<'a> {
    type Spec = PExpire;

    fn route_key(&self) -> Option<&'a [u8]> {
        Some(self.key)
    }

    fn to_owned_command(&self) -> Command {
        Command::new(Box::new(OwnedPExpire::new(self.key.to_vec(), self.ttl_ms)))
    }

    fn execute_engine<'b>(&'b self, ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b,
    {
        let key = self.key;
        let expire_at_ms = relative_expire_at_ms(self.ttl_ms);
        Box::pin(async move {
            Expire::execute_engine_integer(ctx, key, expire_at_ms)
                .await
                .map(Frame::Integer)
        })
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

impl super::CommandSpec for PExpire {
    const NAME: &'static str = "PEXPIRE";
    const MUTATES_VALUE: bool = true;
}

impl super::OwnedCommandParse for PExpire {
    fn parse_owned(parts: &[Vec<u8>]) -> Result<Command> {
        CommandArity::<Self>::exact(parts.len(), 3)?;
        Ok(Command::new(Box::new(OwnedPExpire::new(
            parts[1].clone(),
            TtlMillis::<Self>::millis(&parts[2])?,
        ))))
    }
}

impl<'a> super::BorrowedCommandParse<'a> for PExpire {
    fn parse_borrowed(parts: &[&'a [u8]]) -> Result<super::BorrowedCommandBox<'a>> {
        CommandArity::<Self>::exact(parts.len(), 3)?;
        Ok(Box::new(BorrowedPExpire::new(
            parts[1],
            TtlMillis::<Self>::millis(parts[2])?,
        )))
    }
}

impl DecodedFastCommand for PExpire {
    fn matches_decoded_fast(&self, _command: &FastCommand<'_>) -> bool {
        false
    }
}

#[cfg(feature = "server")]
impl RawDirectCommand for PExpire {
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_>) {
        match ctx.args.as_slice() {
            [key, ttl] => match TtlMillis::<()>::ascii_millis(ttl) {
                Some(ttl_ms) => {
                    let changed = ctx.store.expire(key, relative_expire_at_ms(ttl_ms));
                    ServerWire::write_resp_integer(ctx.out, changed as i64);
                }
                None => ServerWire::write_resp_error(ctx.out, "ERR value is not an integer"),
            },
            _ => ServerWire::write_resp_error(
                ctx.out,
                "ERR wrong number of arguments for 'PEXPIRE' command",
            ),
        }
    }
}
