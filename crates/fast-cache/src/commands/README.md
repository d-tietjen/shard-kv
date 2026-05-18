# Command Modules

Each Redis command owns its command-specific parsing, routing metadata, storage
execution, and direct-server execution. The root command file is
`commands/<name>.rs`; larger commands may use named submodules under
`commands/<name>/`, declared from the root file. Do not add `mod.rs` files.

Server, protocol, and storage modules should decode envelopes, route to a
command object, and write generic wire responses. They should not contain
GET/SET-style business logic, parser option handling, or command-name string
checks.

## Adding A Command

1. Add `commands/<name>.rs`.
2. Export the module from `src/commands.rs`.
3. Define a zero-sized command spec and one static `COMMAND`.
4. Define owned and borrowed payload types.
5. Implement `OwnedCommandData` and `BorrowedCommandData`.
6. Implement `CommandSpec`, `OwnedCommandParse`, and `BorrowedCommandParse`.
7. Put async engine behavior in `commands/<name>/engine.rs` when it grows past a
   tiny helper.
8. Under `#[cfg(feature = "server")]`, put direct RESP and fast-protocol
   behavior in `commands/<name>/server.rs`.
9. Put native FCNP behavior in `commands/<name>/fcnp.rs`.
10. Put command-local option parsing in `commands/<name>/options.rs`.
11. Put reusable command traits in command-owned submodules, for example
    `commands/set/storage.rs`.
12. Add `&commands::<name>::COMMAND` to only the catalogs that can execute it:
    `commands.rs::CATALOG`, `commands.rs::EngineCommandCatalog`, and the
    `server/commands.rs` catalogs.
13. Add tests for RESP and fast protocol paths. Hot commands should also get a
    benchmark or a benchmark update.

Do not add command variants to `storage/command.rs`. `Command` and
`BorrowedCommand` are trait-object wrappers; the concrete command module is the
source of truth. Error strings should reference
`<Self as CommandSpec>::NAME` or `<Command as CommandSpec>::NAME` instead of
writing command names by hand. Use shared helpers such as `CommandArity`,
`StorageInteger`, `DecodedFastCommand`, and command-local traits before adding
floating helper methods.

## Root Template

```rust
mod engine;
#[cfg(feature = "server")]
mod fcnp;
#[cfg(feature = "server")]
mod server;

use crate::protocol::{FastCommand, Frame};
#[cfg(feature = "server")]
use crate::server::commands::{BorrowedCommandContext, DirectCommandContext};
#[cfg(feature = "server")]
use crate::server::wire::ServerWire;
use crate::storage::{Command, EngineCommandContext, EngineFrameFuture};
use crate::Result;

use super::parsing::CommandArity;
use super::DecodedFastCommand;

pub(crate) struct Example;
pub(crate) static COMMAND: Example = Example;

#[derive(Debug, Clone)]
pub(crate) struct OwnedExample {
    key: Vec<u8>,
}

impl OwnedExample {
    fn new(key: Vec<u8>) -> Self {
        Self { key }
    }
}

impl super::OwnedCommandData for OwnedExample {
    type Spec = Example;

    fn route_key(&self) -> Option<&[u8]> {
        Some(&self.key)
    }

    fn to_borrowed_command(&self) -> super::BorrowedCommandBox<'_> {
        Box::new(BorrowedExample::new(&self.key))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BorrowedExample<'a> {
    key: &'a [u8],
}

impl<'a> BorrowedExample<'a> {
    fn new(key: &'a [u8]) -> Self {
        Self { key }
    }
}

impl<'a> super::BorrowedCommandData<'a> for BorrowedExample<'a> {
    type Spec = Example;

    fn route_key(&self) -> Option<&'a [u8]> {
        Some(self.key)
    }

    fn to_owned_command(&self) -> Command {
        Command::new(Box::new(OwnedExample::new(self.key.to_vec())))
    }

    fn execute_engine<'b>(&'b self, ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b,
    {
        let key = self.key;
        Box::pin(async move { Example::execute_engine_frame(ctx, key).await })
    }

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(
        &self,
        store: &crate::storage::EmbeddedStore,
        now_ms: u64,
    ) -> Frame {
        let _ = (store, now_ms);
        Frame::Error("ERR example command template is not implemented".to_string())
    }

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: BorrowedCommandContext<'_, '_, '_>) {
        ServerWire::write_resp_error(ctx.out, "ERR example command template is not implemented");
    }

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, ctx: DirectCommandContext) -> Frame {
        let _ = ctx;
        Frame::Error("ERR example command template is not implemented".to_string())
    }
}

impl super::CommandSpec for Example {
    const NAME: &'static str = "EXAMPLE";
    const MUTATES_VALUE: bool = false;
}

impl super::OwnedCommandParse for Example {
    fn parse_owned(parts: &[Vec<u8>]) -> Result<Command> {
        CommandArity::<Self>::exact(parts.len(), 2)?;
        Ok(Command::new(Box::new(OwnedExample::new(parts[1].clone()))))
    }
}

impl<'a> super::BorrowedCommandParse<'a> for Example {
    fn parse_borrowed(parts: &[&'a [u8]]) -> Result<super::BorrowedCommandBox<'a>> {
        CommandArity::<Self>::exact(parts.len(), 2)?;
        Ok(Box::new(BorrowedExample::new(parts[1])))
    }
}

impl DecodedFastCommand for Example {
    fn matches_decoded_fast(&self, command: &FastCommand<'_>) -> bool {
        matches!(command, FastCommand::Example { .. })
    }
}
```

## Engine Submodule Template

```rust
use crate::commands::EngineCommandDispatch;
use crate::protocol::{FastCommand, FastRequest, FastResponse, Frame};
use crate::storage::{EngineCommandContext, EngineFastFuture};
use crate::Result;

use super::Example;

impl EngineCommandDispatch for Example {
    fn execute_engine_fast<'a>(
        &'static self,
        ctx: EngineCommandContext<'a>,
        request: FastRequest<'a>,
    ) -> EngineFastFuture<'a> {
        Box::pin(async move {
            match request.command {
                FastCommand::Example { key } => {
                    let _ = (ctx, key);
                    Ok(FastResponse::Error(
                        b"ERR example command template is not implemented".to_vec(),
                    ))
                }
                _ => Ok(FastResponse::Error(b"ERR unsupported command".to_vec())),
            }
        })
    }
}

impl Example {
    pub(super) async fn execute_engine_frame(
        ctx: EngineCommandContext<'_>,
        key: &[u8],
    ) -> Result<Frame> {
        let _ = (ctx, key);
        Ok(Frame::Error(
            "ERR example command template is not implemented".to_string(),
        ))
    }
}
```

## Server Submodule Template

```rust
use crate::commands::CommandSpec;
use crate::protocol::{FastCommand, FastRequest, FastResponse};
use crate::server::commands::{
    DirectCommandContext, DirectFastCommand, FastCommandContext, FastDirectCommand,
    RawCommandContext, RawDirectCommand,
};
use crate::server::wire::ServerWire;

use super::Example;

#[cfg(feature = "server")]
impl RawDirectCommand for Example {
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_>) {
        let RawCommandContext { store, args, out } = ctx;
        let _ = (store, args);
        ServerWire::write_resp_error(out, "ERR example command template is not implemented");
    }
}

#[cfg(feature = "server")]
impl DirectFastCommand for Example {
    fn execute_direct_fast(
        &self,
        ctx: DirectCommandContext,
        request: FastRequest<'_>,
    ) -> FastResponse {
        let _ = (ctx, request);
        FastResponse::Error(b"ERR example command template is not implemented".to_vec())
    }
}

#[cfg(feature = "server")]
impl FastDirectCommand for Example {
    fn execute_fast(&self, ctx: FastCommandContext<'_, '_>, command: FastCommand<'_>) {
        let _ = command;
        ServerWire::write_fast_error(ctx.out, "ERR example command template is not implemented");
    }
}
```
