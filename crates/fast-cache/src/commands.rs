pub mod del;
pub mod exists;
pub mod expire;
pub mod get;
pub mod getex;
pub(crate) mod parsing;
pub mod persist;
pub mod pexpire;
pub mod psetex;
pub mod pttl;
pub mod set;
pub mod setex;
pub mod ttl;

use bytes::Bytes as SharedBytes;

use crate::protocol::{CommandSpanFrame, FastCommand, FastRequest, FastResponse, Frame};
use crate::storage::{
    EngineCommandContext, EngineFastFuture, EngineFrameFuture, EngineRespSpanFuture,
};
use crate::{FastCacheError, Result};

pub(crate) trait CommandSpec {
    const NAME: &'static str;
    const MUTATES_VALUE: bool;

    #[inline(always)]
    fn matches(name: &[u8]) -> bool {
        name.eq_ignore_ascii_case(Self::NAME.as_bytes())
    }
}

pub(crate) trait OwnedCommandParse: CommandSpec {
    fn parse_owned(parts: &[Vec<u8>]) -> Result<crate::storage::Command>;
}

pub(crate) type OwnedCommandBox = Box<dyn OwnedCommandObject>;

/// Command-owned data that was parsed from an owned RESP frame.
///
/// Implement this for the concrete owned command payload. The blanket
/// `OwnedCommandObject` impl supplies the command name and mutation metadata
/// from the command spec so each command file does not repeat that boilerplate.
pub(crate) trait OwnedCommandData: std::fmt::Debug + Send + Sync {
    type Spec: CommandSpec;

    fn route_key(&self) -> Option<&[u8]>;
    fn to_borrowed_command(&self) -> BorrowedCommandBox<'_>;
}

/// Parsed owned command data owned by a concrete command module.
pub(crate) trait OwnedCommandObject: std::fmt::Debug + Send + Sync {
    fn name(&self) -> &'static str;
    fn mutates_value(&self) -> bool;
    fn route_key(&self) -> Option<&[u8]>;
    fn to_borrowed_command(&self) -> BorrowedCommandBox<'_>;
}

impl<T> OwnedCommandObject for T
where
    T: OwnedCommandData,
{
    fn name(&self) -> &'static str {
        <T::Spec as CommandSpec>::NAME
    }

    fn mutates_value(&self) -> bool {
        <T::Spec as CommandSpec>::MUTATES_VALUE
    }

    fn route_key(&self) -> Option<&[u8]> {
        <T as OwnedCommandData>::route_key(self)
    }

    fn to_borrowed_command(&self) -> BorrowedCommandBox<'_> {
        <T as OwnedCommandData>::to_borrowed_command(self)
    }
}

pub(crate) type BorrowedCommandBox<'a> = Box<dyn BorrowedCommandObject<'a> + 'a>;

/// Command-owned data that borrows directly from a decoded request buffer.
///
/// The blanket `BorrowedCommandObject` impl keeps name/mutation metadata
/// centralized while allowing each command file to own execution details.
pub(crate) trait BorrowedCommandData<'a>: std::fmt::Debug + Send + Sync {
    type Spec: CommandSpec;

    fn route_key(&self) -> Option<&'a [u8]>;
    fn supports_spanned_resp(&self) -> bool {
        false
    }
    fn to_owned_command(&self) -> crate::storage::Command;
    fn execute_engine<'b>(&'b self, ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b;

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &crate::storage::EmbeddedStore, now_ms: u64) -> Frame;

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: crate::server::commands::BorrowedCommandContext<'_, '_, '_>);

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, ctx: crate::server::commands::DirectCommandContext) -> Frame;
}

/// Parsed borrowed command data owned by a concrete command module.
pub(crate) trait BorrowedCommandObject<'a>: std::fmt::Debug + Send + Sync {
    fn name(&self) -> &'static str;
    fn mutates_value(&self) -> bool;
    fn route_key(&self) -> Option<&'a [u8]>;
    fn supports_spanned_resp(&self) -> bool;
    fn to_owned_command(&self) -> crate::storage::Command;
    fn execute_engine<'b>(&'b self, ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b;

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &crate::storage::EmbeddedStore, now_ms: u64) -> Frame;

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: crate::server::commands::BorrowedCommandContext<'_, '_, '_>);

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, ctx: crate::server::commands::DirectCommandContext) -> Frame;
}

impl<'a, T> BorrowedCommandObject<'a> for T
where
    T: BorrowedCommandData<'a>,
{
    fn name(&self) -> &'static str {
        <T::Spec as CommandSpec>::NAME
    }

    fn mutates_value(&self) -> bool {
        <T::Spec as CommandSpec>::MUTATES_VALUE
    }

    fn route_key(&self) -> Option<&'a [u8]> {
        <T as BorrowedCommandData<'a>>::route_key(self)
    }

    fn supports_spanned_resp(&self) -> bool {
        <T as BorrowedCommandData<'a>>::supports_spanned_resp(self)
    }

    fn to_owned_command(&self) -> crate::storage::Command {
        <T as BorrowedCommandData<'a>>::to_owned_command(self)
    }

    fn execute_engine<'b>(&'b self, ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b,
    {
        <T as BorrowedCommandData<'a>>::execute_engine(self, ctx)
    }

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &crate::storage::EmbeddedStore, now_ms: u64) -> Frame {
        <T as BorrowedCommandData<'a>>::execute_borrowed_frame(self, store, now_ms)
    }

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: crate::server::commands::BorrowedCommandContext<'_, '_, '_>) {
        <T as BorrowedCommandData<'a>>::execute_borrowed(self, ctx);
    }

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, ctx: crate::server::commands::DirectCommandContext) -> Frame {
        <T as BorrowedCommandData<'a>>::execute_direct_borrowed(self, ctx)
    }
}

pub(crate) trait BorrowedCommandParse<'a>: CommandSpec {
    fn parse_borrowed(parts: &[&'a [u8]]) -> Result<BorrowedCommandBox<'a>>;
}

/// Object-safe metadata shared by command implementations.
pub(crate) trait CommandMetadata: Sync {
    fn mutates_value(&self) -> bool;
    fn matches(&self, name: &[u8]) -> bool;
}

impl<T> CommandMetadata for T
where
    T: CommandSpec + Sync,
{
    fn mutates_value(&self) -> bool {
        T::MUTATES_VALUE
    }

    #[inline(always)]
    fn matches(&self, name: &[u8]) -> bool {
        <T as CommandSpec>::matches(name)
    }
}

/// Object-safe parser entry owned by a command object.
pub(crate) trait CommandDefinition: CommandMetadata {
    fn parse_owned(&self, parts: &[Vec<u8>]) -> Result<crate::storage::Command>;
    fn parse_borrowed<'a>(&self, parts: &[&'a [u8]]) -> Result<BorrowedCommandBox<'a>>;
}

impl<T> CommandDefinition for T
where
    T: CommandSpec + OwnedCommandParse + Sync,
    for<'a> T: BorrowedCommandParse<'a>,
{
    fn parse_owned(&self, parts: &[Vec<u8>]) -> Result<crate::storage::Command> {
        <T as OwnedCommandParse>::parse_owned(parts)
    }

    fn parse_borrowed<'a>(&self, parts: &[&'a [u8]]) -> Result<BorrowedCommandBox<'a>> {
        <T as BorrowedCommandParse<'a>>::parse_borrowed(parts)
    }
}

pub(crate) trait DecodedFastCommand: CommandMetadata {
    fn matches_decoded_fast(&self, command: &FastCommand<'_>) -> bool;
}

pub(crate) trait EngineCommandDispatch: DecodedFastCommand {
    fn execute_engine_fast<'a>(
        &'static self,
        ctx: EngineCommandContext<'a>,
        request: FastRequest<'a>,
    ) -> EngineFastFuture<'a>;
}

pub(crate) trait EngineRespSpanCommandDispatch: CommandMetadata {
    fn execute_engine_resp_spanned<'a>(
        &'static self,
        ctx: EngineCommandContext<'a>,
        frame: CommandSpanFrame,
        owner: SharedBytes,
        out: &'a mut Vec<u8>,
    ) -> EngineRespSpanFuture<'a>;
}

pub(crate) static CATALOG: &[&dyn CommandDefinition] = &[
    &get::COMMAND,
    &set::COMMAND,
    &del::COMMAND,
    &exists::COMMAND,
    &ttl::COMMAND,
    &pttl::COMMAND,
    &expire::COMMAND,
    &pexpire::COMMAND,
    &persist::COMMAND,
    &getex::COMMAND,
    &setex::COMMAND,
    &psetex::COMMAND,
];

pub(crate) struct CommandCatalog;

impl CommandCatalog {
    pub(crate) fn find(name: &[u8]) -> Option<&'static dyn CommandDefinition> {
        CATALOG
            .iter()
            .copied()
            .find(|command| command.matches(name))
    }

    pub(crate) fn parse_owned(parts: &[Vec<u8>]) -> Result<crate::storage::Command> {
        let command = Self::find_required(parts.first().map(Vec::as_slice))?;
        command.parse_owned(parts)
    }

    pub(crate) fn parse_borrowed<'a>(parts: &[&'a [u8]]) -> Result<BorrowedCommandBox<'a>> {
        let command = Self::find_required(parts.first().copied())?;
        command.parse_borrowed(parts)
    }

    fn find_required(name: Option<&[u8]>) -> Result<&'static dyn CommandDefinition> {
        match name {
            Some(name) => Self::find(name).ok_or_else(|| {
                FastCacheError::Command(format!(
                    "unsupported command: {}",
                    String::from_utf8_lossy(name)
                ))
            }),
            None => Err(FastCacheError::Command("empty command".into())),
        }
    }
}

pub(crate) struct EngineCommandCatalog;

impl EngineCommandCatalog {
    fn find_fast(command: &FastCommand<'_>) -> Option<&'static dyn EngineCommandDispatch> {
        [
            &get::COMMAND as &dyn EngineCommandDispatch,
            &set::COMMAND,
            &del::COMMAND,
            &exists::COMMAND,
            &ttl::COMMAND,
            &expire::COMMAND,
            &getex::COMMAND,
            &setex::COMMAND,
        ]
        .into_iter()
        .find(|candidate| candidate.matches_decoded_fast(command))
    }

    fn find_resp_span(name: &[u8]) -> Option<&'static dyn EngineRespSpanCommandDispatch> {
        [&set::COMMAND as &dyn EngineRespSpanCommandDispatch]
            .into_iter()
            .find(|candidate| candidate.matches(name))
    }

    pub(crate) async fn execute_fast<'a>(
        ctx: EngineCommandContext<'a>,
        request: FastRequest<'a>,
    ) -> Option<Result<FastResponse>> {
        let handler = Self::find_fast(&request.command)?;
        Some(handler.execute_engine_fast(ctx, request).await)
    }

    pub(crate) async fn execute_resp_spanned<'a>(
        ctx: EngineCommandContext<'a>,
        frame: CommandSpanFrame,
        owner: SharedBytes,
        out: &'a mut Vec<u8>,
    ) -> Option<Result<()>> {
        let name = &owner[frame.parts.first()?.clone()];
        let handler = Self::find_resp_span(name)?;
        Some(
            handler
                .execute_engine_resp_spanned(ctx, frame, owner, out)
                .await,
        )
    }
}
