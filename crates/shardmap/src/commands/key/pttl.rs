//! PTTL command parsing and execution.

use crate::Result;
use crate::protocol::{FastCommand, Frame};
#[cfg(feature = "server")]
use crate::server::commands::{BorrowedCommandContext, DirectCommandContext};
#[cfg(feature = "server")]
use crate::server::commands::{RawCommandContext, RawDirectCommand};
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{Command, EngineCommandContext, EngineFrameFuture};

use super::parsing::CommandArity;
use super::{DecodedFastCommand, ttl::Ttl};

pub(crate) struct Pttl;
pub(crate) static COMMAND: Pttl = Pttl;

#[derive(Debug, Clone)]
pub(crate) struct OwnedPttl {
    key: Vec<u8>,
}

impl OwnedPttl {
    fn new(key: Vec<u8>) -> Self {
        Self { key }
    }
}

impl super::OwnedCommandData for OwnedPttl {
    type Spec = Pttl;

    fn route_key(&self) -> Option<&[u8]> {
        Some(&self.key)
    }

    fn to_borrowed_command(&self) -> super::BorrowedCommandBox<'_> {
        Box::new(BorrowedPttl::new(&self.key))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BorrowedPttl<'a> {
    key: &'a [u8],
}

impl<'a> BorrowedPttl<'a> {
    fn new(key: &'a [u8]) -> Self {
        Self { key }
    }
}

impl<'a> super::BorrowedCommandData<'a> for BorrowedPttl<'a> {
    type Spec = Pttl;

    fn route_key(&self) -> Option<&'a [u8]> {
        Some(self.key)
    }

    fn to_owned_command(&self) -> Command {
        Command::new(Box::new(OwnedPttl::new(self.key.to_vec())))
    }

    fn execute_engine<'b>(&'b self, ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b,
    {
        Box::pin(async move {
            Ttl::execute_engine_integer(ctx, self.key, true)
                .await
                .map(Frame::Integer)
        })
    }

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &crate::storage::EmbeddedStore, _now_ms: u64) -> Frame {
        Frame::Integer(store.pttl_millis(self.key))
    }

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: BorrowedCommandContext<'_, '_, '_>) {
        ServerWire::write_resp_integer(ctx.out, ctx.store.pttl_millis(self.key));
    }

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, ctx: DirectCommandContext) -> Frame {
        Frame::Integer(ctx.ttl(self.key, true))
    }
}

impl super::CommandSpec for Pttl {
    const NAME: &'static str = "PTTL";
    const MUTATES_VALUE: bool = false;
}

impl super::OwnedCommandParse for Pttl {
    fn parse_owned(parts: &[Vec<u8>]) -> Result<Command> {
        CommandArity::<Self>::exact(parts.len(), 2)?;
        Ok(Command::new(Box::new(OwnedPttl::new(parts[1].clone()))))
    }
}

impl<'a> super::BorrowedCommandParse<'a> for Pttl {
    fn parse_borrowed(parts: &[&'a [u8]]) -> Result<super::BorrowedCommandBox<'a>> {
        CommandArity::<Self>::exact(parts.len(), 2)?;
        Ok(Box::new(BorrowedPttl::new(parts[1])))
    }
}

impl DecodedFastCommand for Pttl {
    fn matches_decoded_fast(&self, _command: &FastCommand<'_>) -> bool {
        false
    }
}

#[cfg(feature = "server")]
impl RawDirectCommand for Pttl {
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_, '_>) {
        match ctx.args.as_slice() {
            [key] => ServerWire::write_resp_integer(ctx.out, ctx.store.pttl_millis(key)),
            _ => ServerWire::write_resp_error(
                ctx.out,
                "ERR wrong number of arguments for 'PTTL' command",
            ),
        }
    }
}
