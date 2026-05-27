//! GET command parsing and execution.
//!
//! All GET-specific behavior starts here. Transport-specific implementations
//! live in `get/*.rs` submodules.

#[path = "get/engine.rs"]
mod engine;
#[cfg(feature = "server")]
#[path = "get/fcnp.rs"]
mod fcnp;
#[cfg(feature = "server")]
#[path = "get/server.rs"]
mod server;

use crate::Result;
use crate::protocol::{FastCommand, Frame};
#[cfg(feature = "server")]
use crate::server::commands::{BorrowedCommandContext, DirectCommandContext};
use crate::storage::{Command, EngineCommandContext, EngineFrameFuture};

use super::DecodedFastCommand;
use super::parsing::CommandArity;

/// Command spec registered in the command catalogs.
pub(crate) struct Get;
pub(crate) static COMMAND: Get = Get;

/// Owned GET payload used when a RESP frame has already been copied out.
#[derive(Debug, Clone)]
pub(crate) struct OwnedGet {
    key: Vec<u8>,
}

impl OwnedGet {
    fn new(key: Vec<u8>) -> Self {
        Self { key }
    }
}

impl super::OwnedCommandData for OwnedGet {
    type Spec = Get;

    fn route_key(&self) -> Option<&[u8]> {
        Some(&self.key)
    }

    fn to_borrowed_command(&self) -> super::BorrowedCommandBox<'_> {
        Box::new(BorrowedGet::new(&self.key))
    }
}

/// Borrowed GET payload used on zero-copy request paths.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BorrowedGet<'a> {
    key: &'a [u8],
}

impl<'a> BorrowedGet<'a> {
    fn new(key: &'a [u8]) -> Self {
        Self { key }
    }
}

impl<'a> super::BorrowedCommandData<'a> for BorrowedGet<'a> {
    type Spec = Get;

    fn route_key(&self) -> Option<&'a [u8]> {
        Some(self.key)
    }

    fn to_owned_command(&self) -> Command {
        Command::new(Box::new(OwnedGet::new(self.key.to_vec())))
    }

    fn execute_engine<'b>(&'b self, ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b,
    {
        Box::pin(async move { Get::execute_engine_frame(ctx, self.key).await })
    }

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &crate::storage::EmbeddedStore, _now_ms: u64) -> Frame {
        match store.get(self.key) {
            Some(value) => Frame::BlobString(value),
            None => Frame::Null,
        }
    }

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, mut ctx: BorrowedCommandContext<'_, '_, '_>) {
        #[cfg(feature = "unsafe")]
        if ctx.single_threaded {
            Get::execute_borrowed_single_threaded(&mut ctx, self.key);
            return;
        }
        #[cfg(not(feature = "unsafe"))]
        let _ = ctx.single_threaded;
        Get::execute_borrowed_shared(&mut ctx, self.key);
    }

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, ctx: DirectCommandContext) -> Frame {
        ctx.get(self.key).map_or(Frame::Null, Frame::BlobString)
    }
}

impl super::CommandSpec for Get {
    const NAME: &'static str = "GET";
    const MUTATES_VALUE: bool = false;
}

impl super::OwnedCommandParse for Get {
    fn parse_owned(parts: &[Vec<u8>]) -> Result<Command> {
        CommandArity::<Self>::exact(parts.len(), 2)?;
        Ok(Command::new(Box::new(OwnedGet::new(parts[1].clone()))))
    }
}

impl<'a> super::BorrowedCommandParse<'a> for Get {
    fn parse_borrowed(parts: &[&'a [u8]]) -> Result<super::BorrowedCommandBox<'a>> {
        CommandArity::<Self>::exact(parts.len(), 2)?;
        Ok(Box::new(BorrowedGet::new(parts[1])))
    }
}

impl DecodedFastCommand for Get {
    fn matches_decoded_fast(&self, command: &FastCommand<'_>) -> bool {
        matches!(command, FastCommand::Get { .. })
    }
}
