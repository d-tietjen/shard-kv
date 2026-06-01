# Command Modules

Each Redis command owns its command-specific parsing, routing metadata, storage
execution, and direct-server execution. Command files live under family folders,
for example `commands/string/get.rs`, `commands/key/ttl.rs`, and
`commands/hash/hset.rs`. Larger commands may use named submodules under their
command file stem, for example `commands/string/get/server.rs`. Do not add
`mod.rs` files.

Server, protocol, and storage modules should decode envelopes, route to a
command object, and write generic wire responses. They should not contain
GET/SET-style business logic, parser option handling, or command-name string
checks.

## Adding A Command

1. Add `commands/<family>/<name>.rs`.
2. Export the module from `src/commands.rs` with an explicit `#[path]`.
3. Define a zero-sized command spec and one static `COMMAND`.
4. Define owned and borrowed payload types.
5. Implement `OwnedCommandData` and `BorrowedCommandData`.
6. Implement `CommandSpec`, `OwnedCommandParse`, and `BorrowedCommandParse`.
7. Put async engine behavior in `commands/<family>/<name>/engine.rs` when it
   grows past a tiny helper.
8. Under `#[cfg(feature = "server")]`, put direct RESP and fast-protocol
    behavior in `commands/<family>/<name>/server.rs`.
9. Put native SCNP behavior in `commands/<family>/<name>/scnp.rs`.
10. Put command-local option parsing in `commands/<family>/<name>/options.rs`.
11. Put reusable command traits in command-owned submodules, for example
    `commands/string/set/storage.rs`.
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

## Compatibility Staging

Redis/Valkey compatibility is documented by upstream server version and by
claim strength. For 0.2.0, `docs/REDIS_COMPATIBILITY.md` is the source of
truth: it is generated from the live command benchmark registry and currently
tracks every Redis 5.0.14 command as supported, with standalone-only
expected-error behavior called out explicitly.

The command surface also includes selected post-Redis-5 cache commands and
forms such as `GETDEL`, `GETEX`, `BLMOVE`, `BLMPOP`, `BZMPOP`, `COPY`,
`HRANDFIELD`, `SMISMEMBER`, `ZRANGESTORE`, `ZDIFF`, `ZDIFFSTORE`, `ZINTER`,
`ZINTERCARD`, `ZMPOP`, `ZMSCORE`, `ZRANDMEMBER`, and `ZUNION`.

Do not describe this as byte-for-byte Redis server parity. The compatibility
claim is a Redis/Valkey-compatible cache-command profile with live RESP command
coverage. Important 0.2.0 caveats:

- Expected-error standalone commands, including disabled cluster, replication,
  monitor, module, migration, cross-DB, shutdown, and security-warning paths,
  intentionally return Redis-style errors rather than implementing those
  background subsystems.
- `WATCH` and `UNWATCH` use snapshot-based conflict detection. Version-accurate
  invalidation for values changed away and back remains a known gap.
- Scripting uses a constrained evaluator for return values, `KEYS`/`ARGV`,
  `tonumber`, and `redis.call`/`redis.pcall` over supported commands; it is not
  a general Lua VM.
- Stream support covers basic append, read, range, trim, ID, and lightweight
  group/readgroup behavior. Full pending-entry-list and consumer-group parity
  remains intentionally lightweight.
- Pub/Sub support covers publish-without-subscribers, subscription and
  unsubscribe acknowledgements, and empty introspection. Persistent subscriber
  fanout is not part of the 0.2.0 semantics.
- HyperLogLog commands return compatible cardinalities for covered operations,
  but use an exact internal representation rather than Redis' binary HLL
  encoding.
- Blocking list and sorted-set commands wait on the owning shard for shard-local
  key sets, with ready, timeout, and cross-client wakeup paths covered by tests.
  Empty multi-key waits that span shards return `CROSSSLOT` instead of using a
  global waiter.
- RESP2/RESP3 support is covered by protocol tests and command smoke tests, but
  exact error wording and obscure inline-frame edge cases should still be
  checked against the target upstream version before expanding public claims.

## Formal Semantics

Pure command laws live under `commands/formal/`. Keep these modules free of
storage locks, protocol I/O, heap-heavy object implementations, and async
runtime types so they can be checked both by normal Rust tests and by Creusot.
When command behavior needs range, bound, rank, or set-algebra logic, prefer
moving the pure rule into the relevant formal module and calling it from the
command path.

The proof-shaped functions are normal boolean checks under `cargo test`, and
carry `cfg(creusot)` contracts for `cargo creusot` runs. To verify them with
Creusot, use the small verification crate rather than pointing Creusot at the
full server/storage crate:

```sh
cargo creusot -p shardcache-formal prove
```

The intent is not to verify sockets, shard locks, or Redis itself here. This
layer proves small deterministic command laws; differential tests remain the
compatibility oracle for Redis/Valkey wire behavior.

## Root Template

```rust
#[path = "example/engine.rs"]
mod engine;
#[cfg(feature = "server")]
#[path = "example/scnp.rs"]
mod scnp;
#[cfg(feature = "server")]
#[path = "example/server.rs"]
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
        Box::pin(async move { Example::execute_engine_frame(ctx, self.key).await })
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
