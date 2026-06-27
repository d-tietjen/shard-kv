//! PSETEX command parsing and execution.

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
use super::parsing::{CommandArity, TtlMillis};
use super::setex::SetEx;

pub(crate) struct PSetEx;
pub(crate) static COMMAND: PSetEx = PSetEx;

#[derive(Debug, Clone)]
pub(crate) struct OwnedPSetEx {
    key: Vec<u8>,
    ttl_ms: u64,
    value: Vec<u8>,
}

impl OwnedPSetEx {
    fn new(key: Vec<u8>, ttl_ms: u64, value: Vec<u8>) -> Self {
        Self { key, ttl_ms, value }
    }
}

impl super::OwnedCommandData for OwnedPSetEx {
    type Spec = PSetEx;

    fn route_key(&self) -> Option<&[u8]> {
        Some(&self.key)
    }

    fn to_borrowed_command(&self) -> super::BorrowedCommandBox<'_> {
        Box::new(BorrowedPSetEx::new(&self.key, self.ttl_ms, &self.value))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BorrowedPSetEx<'a> {
    key: &'a [u8],
    ttl_ms: u64,
    value: &'a [u8],
}

impl<'a> BorrowedPSetEx<'a> {
    fn new(key: &'a [u8], ttl_ms: u64, value: &'a [u8]) -> Self {
        Self { key, ttl_ms, value }
    }
}

impl<'a> super::BorrowedCommandData<'a> for BorrowedPSetEx<'a> {
    type Spec = PSetEx;

    fn route_key(&self) -> Option<&'a [u8]> {
        Some(self.key)
    }

    fn to_owned_command(&self) -> Command {
        Command::new(Box::new(OwnedPSetEx::new(
            self.key.to_vec(),
            self.ttl_ms,
            self.value.to_vec(),
        )))
    }

    fn execute_engine<'b>(&'b self, ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b,
    {
        Box::pin(async move {
            SetEx::store_value(ctx, None, self.key, self.ttl_ms, self.value)
                .await
                .map(|_| Frame::SimpleString("OK".into()))
        })
    }

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &crate::storage::EmbeddedStore, _now_ms: u64) -> Frame {
        store.set(self.key.to_vec(), self.value.to_vec(), Some(self.ttl_ms));
        Frame::SimpleString("OK".into())
    }

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: BorrowedCommandContext<'_, '_, '_>) {
        ctx.store
            .set(self.key.to_vec(), self.value.to_vec(), Some(self.ttl_ms));
        ctx.out.extend_from_slice(b"+OK\r\n");
    }

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, ctx: DirectCommandContext) -> Frame {
        ctx.set_owned(self.key.to_vec(), self.value.to_vec(), Some(self.ttl_ms));
        Frame::SimpleString("OK".into())
    }
}

impl super::CommandSpec for PSetEx {
    const NAME: &'static str = "PSETEX";
    const MUTATES_VALUE: bool = true;
}

impl super::OwnedCommandParse for PSetEx {
    fn parse_owned(parts: &[Vec<u8>]) -> Result<Command> {
        CommandArity::<Self>::exact(parts.len(), 4)?;
        Ok(Command::new(Box::new(OwnedPSetEx::new(
            parts[1].clone(),
            TtlMillis::<Self>::millis(&parts[2])?,
            parts[3].clone(),
        ))))
    }
}

impl<'a> super::BorrowedCommandParse<'a> for PSetEx {
    fn parse_borrowed(parts: &[&'a [u8]]) -> Result<super::BorrowedCommandBox<'a>> {
        CommandArity::<Self>::exact(parts.len(), 4)?;
        Ok(Box::new(BorrowedPSetEx::new(
            parts[1],
            TtlMillis::<Self>::millis(parts[2])?,
            parts[3],
        )))
    }
}

impl DecodedFastCommand for PSetEx {
    fn matches_decoded_fast(&self, _command: &FastCommand<'_>) -> bool {
        false
    }
}

#[cfg(feature = "server")]
impl RawDirectCommand for PSetEx {
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_, '_>) {
        match ctx.args.as_slice() {
            [key, ttl, value] => match TtlMillis::<()>::ascii_millis(ttl) {
                Some(ttl_ms) => {
                    ctx.store.set(key.to_vec(), value.to_vec(), Some(ttl_ms));
                    ctx.out.extend_from_slice(b"+OK\r\n");
                }
                None => ServerWire::write_resp_error(ctx.out, "ERR value is not an integer"),
            },
            _ => ServerWire::write_resp_error(
                ctx.out,
                "ERR wrong number of arguments for 'PSETEX' command",
            ),
        }
    }
}
