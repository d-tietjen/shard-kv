//! DEL command parsing and execution.
//!
//! DEL-specific behavior starts here. Transport-specific implementations live
//! in `del/*.rs` submodules.

mod engine;
#[cfg(feature = "server")]
mod fcnp;
#[cfg(feature = "server")]
mod server;

use smallvec::SmallVec;

use crate::Result;
use crate::protocol::{FastCommand, Frame};
#[cfg(feature = "server")]
use crate::server::commands::{BorrowedCommandContext, DirectCommandContext};
use crate::storage::{Command, EngineCommandContext, EngineFrameFuture};

use super::DecodedFastCommand;
use super::parsing::CommandArity;

type BorrowedKeys<'a> = SmallVec<[&'a [u8]; 8]>;

/// Command spec registered in the command catalogs.
pub(crate) struct Del;
pub(crate) static COMMAND: Del = Del;

/// Owned DEL payload used when a RESP frame has already been copied out.
#[derive(Debug, Clone)]
pub(crate) struct OwnedDel {
    keys: Vec<Vec<u8>>,
}

impl OwnedDel {
    fn new(keys: Vec<Vec<u8>>) -> Self {
        Self { keys }
    }
}

impl super::OwnedCommandData for OwnedDel {
    type Spec = Del;

    fn route_key(&self) -> Option<&[u8]> {
        self.keys.first().map(Vec::as_slice)
    }

    fn to_borrowed_command(&self) -> super::BorrowedCommandBox<'_> {
        Box::new(BorrowedDel::from_owned(&self.keys))
    }
}

/// Borrowed DEL payload used on zero-copy request paths.
#[derive(Debug, Clone)]
pub(crate) struct BorrowedDel<'a> {
    keys: BorrowedKeys<'a>,
}

impl<'a> BorrowedDel<'a> {
    fn new(keys: impl IntoIterator<Item = &'a [u8]>) -> Self {
        Self {
            keys: keys.into_iter().collect(),
        }
    }

    fn from_owned(keys: &'a [Vec<u8>]) -> Self {
        Self::new(keys.iter().map(Vec::as_slice))
    }
}

impl<'a> super::BorrowedCommandData<'a> for BorrowedDel<'a> {
    type Spec = Del;

    fn route_key(&self) -> Option<&'a [u8]> {
        self.keys.first().copied()
    }

    fn to_owned_command(&self) -> Command {
        Command::new(Box::new(OwnedDel::new(
            self.keys.iter().map(|key| key.to_vec()).collect(),
        )))
    }

    fn execute_engine<'b>(&'b self, ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b,
    {
        let keys = self.keys.clone();
        Box::pin(async move { Del::execute_engine_frame(ctx, keys.as_slice()).await })
    }

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &crate::storage::EmbeddedStore, _now_ms: u64) -> Frame {
        Frame::Integer(Del::delete_embedded_keys(store, self.keys.as_slice()))
    }

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: BorrowedCommandContext<'_, '_, '_>) {
        Del::execute_borrowed(ctx, self.keys.as_slice());
    }

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, ctx: DirectCommandContext) -> Frame {
        Frame::Integer(self.keys.iter().filter(|key| ctx.delete(key)).count() as i64)
    }
}

impl super::CommandSpec for Del {
    const NAME: &'static str = "DEL";
    const MUTATES_VALUE: bool = true;
}

impl super::OwnedCommandParse for Del {
    fn parse_owned(parts: &[Vec<u8>]) -> Result<Command> {
        CommandArity::<Self>::at_least(parts.len(), 2, "key")?;
        Ok(Command::new(Box::new(OwnedDel::new(parts[1..].to_vec()))))
    }
}

impl<'a> super::BorrowedCommandParse<'a> for Del {
    fn parse_borrowed(parts: &[&'a [u8]]) -> Result<super::BorrowedCommandBox<'a>> {
        CommandArity::<Self>::at_least(parts.len(), 2, "key")?;
        Ok(Box::new(BorrowedDel::new(parts[1..].iter().copied())))
    }
}

impl DecodedFastCommand for Del {
    fn matches_decoded_fast(&self, command: &FastCommand<'_>) -> bool {
        matches!(command, FastCommand::Delete { .. })
    }
}
