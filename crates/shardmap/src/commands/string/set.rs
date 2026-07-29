//! SET command parsing and execution.
//!
//! SET-specific behavior starts here. Transport-specific implementations,
//! option parsing, and write-path selection live in `set/*.rs` submodules.

#[path = "set/engine.rs"]
mod engine;
#[path = "set/options.rs"]
mod options;
#[cfg(feature = "server")]
#[path = "set/scnp.rs"]
mod scnp;
#[cfg(feature = "server")]
#[path = "set/server.rs"]
mod server;
#[cfg(feature = "server")]
#[path = "set/storage.rs"]
mod storage;

use crate::Result;
use crate::protocol::{FastCommand, Frame};
#[cfg(feature = "server")]
use crate::server::commands::{BorrowedCommandContext, DirectCommandContext};
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
#[cfg(feature = "server")]
use crate::storage::EmbeddedStore;
use crate::storage::{Command, EngineCommandContext, EngineFrameFuture, RESP_SPANNED_VALUE_MIN};

use super::DecodedFastCommand;
use super::parsing::CommandArity;
use options::StorageSetOptions;

/// Command spec registered in the command catalogs.
pub(crate) struct Set;
pub(crate) static COMMAND: Set = Set;

/// Owned SET payload used when a RESP frame has already been copied out.
#[derive(Debug, Clone)]
pub(crate) struct OwnedSet {
    key: Vec<u8>,
    value: Vec<u8>,
    ttl_ms: Option<u64>,
}

impl OwnedSet {
    fn new(key: Vec<u8>, value: Vec<u8>, ttl_ms: Option<u64>) -> Self {
        Self { key, value, ttl_ms }
    }
}

impl super::OwnedCommandData for OwnedSet {
    type Spec = Set;

    fn route_key(&self) -> Option<&[u8]> {
        Some(&self.key)
    }

    fn to_borrowed_command(&self) -> super::BorrowedCommandBox<'_> {
        Box::new(BorrowedSet::new(&self.key, &self.value, self.ttl_ms))
    }
}

/// Borrowed SET payload used on zero-copy request paths.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BorrowedSet<'a> {
    key: &'a [u8],
    value: &'a [u8],
    ttl_ms: Option<u64>,
}

impl<'a> BorrowedSet<'a> {
    fn new(key: &'a [u8], value: &'a [u8], ttl_ms: Option<u64>) -> Self {
        Self { key, value, ttl_ms }
    }
}

impl<'a> super::BorrowedCommandData<'a> for BorrowedSet<'a> {
    type Spec = Set;

    fn route_key(&self) -> Option<&'a [u8]> {
        Some(self.key)
    }

    fn supports_spanned_resp(&self) -> bool {
        self.value.len() >= RESP_SPANNED_VALUE_MIN
    }

    fn to_owned_command(&self) -> Command {
        Command::new(Box::new(OwnedSet::new(
            self.key.to_vec(),
            self.value.to_vec(),
            self.ttl_ms,
        )))
    }

    fn execute_engine<'b>(&'b self, ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b,
    {
        Box::pin(
            async move { Set::execute_engine_frame(ctx, self.key, self.value, self.ttl_ms).await },
        )
    }

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &EmbeddedStore, _now_ms: u64) -> Frame {
        if !store.point_mutation_is_accepted(self.key, self.value.len(), None) {
            return Frame::Error("ERR mutation rejected by an installed storage extension".into());
        }
        store.set(self.key.to_vec(), self.value.to_vec(), self.ttl_ms);
        Frame::SimpleString("OK".into())
    }

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: BorrowedCommandContext<'_, '_, '_>) {
        if !ctx
            .store
            .point_mutation_is_accepted(self.key, self.value.len(), None)
        {
            ServerWire::write_resp_error(
                ctx.out,
                "ERR mutation rejected by an installed storage extension",
            );
            return;
        }
        #[cfg(feature = "unsafe")]
        if ctx.single_threaded && !ctx.store.has_redis_objects() {
            // SAFETY: forwarded from caller's single-worker contract.
            unsafe {
                ctx.store
                    .set_single_threaded(self.key, self.value, self.ttl_ms)
            };
            ctx.out.extend_from_slice(b"+OK\r\n");
            return;
        }
        #[cfg(not(feature = "unsafe"))]
        let _ = ctx.single_threaded;
        ctx.store
            .set(self.key.to_vec(), self.value.to_vec(), self.ttl_ms);
        ctx.out.extend_from_slice(b"+OK\r\n");
    }

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, ctx: DirectCommandContext) -> Frame {
        ctx.set_owned(self.key.to_vec(), self.value.to_vec(), self.ttl_ms);
        Frame::SimpleString("OK".into())
    }
}

impl super::CommandSpec for Set {
    const NAME: &'static str = "SET";
    const MUTATES_VALUE: bool = true;
}

impl super::OwnedCommandParse for Set {
    fn parse_owned(parts: &[Vec<u8>]) -> Result<Command> {
        CommandArity::<Self>::at_least(parts.len(), 3, "key and value")?;
        let options = StorageSetOptions::parse(parts[3..].iter().map(Vec::as_slice))?;
        Ok(Command::new(Box::new(OwnedSet::new(
            parts[1].clone(),
            parts[2].clone(),
            options.ttl_ms,
        ))))
    }
}

impl<'a> super::BorrowedCommandParse<'a> for Set {
    fn parse_borrowed(parts: &[&'a [u8]]) -> Result<super::BorrowedCommandBox<'a>> {
        CommandArity::<Self>::at_least(parts.len(), 3, "key and value")?;
        let options = StorageSetOptions::parse(parts[3..].iter().copied())?;
        Ok(Box::new(BorrowedSet::new(
            parts[1],
            parts[2],
            options.ttl_ms,
        )))
    }
}

impl DecodedFastCommand for Set {
    fn matches_decoded_fast(&self, command: &FastCommand<'_>) -> bool {
        matches!(command, FastCommand::Set { .. })
    }
}
