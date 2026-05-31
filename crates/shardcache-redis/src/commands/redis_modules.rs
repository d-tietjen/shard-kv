use crate::commands::redis::{bulk, error, int, simple};
#[cfg(feature = "server")]
use crate::commands::redis::{with_resp_protocol, write_frame};
use crate::protocol::Frame;
use crate::storage::{
    Command, EmbeddedStore, EngineCommandContext, EngineFrameFuture, RedisModuleApiResult,
    RedisModuleFamily,
};
use crate::{Result, ShardCacheError};
#[cfg(feature = "server")]
use crate::{
    server::commands::{BorrowedCommandContext, DirectCommandContext, RawCommandContext},
    server::wire::ServerWire,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RedisModuleCommandInfo {
    pub(crate) name: &'static str,
    pub(crate) mutates: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct EnabledModuleInfo {
    pub(crate) name: &'static str,
    pub(crate) version: i64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RedisModuleCommand {
    name: &'static str,
    mutates: bool,
    family: &'static str,
    implemented: bool,
}

#[allow(unused_macros)]
macro_rules! redis_module_commands {
    ($family:literal; $($name:literal => $mutates:expr),+ $(,)?) => {
        &[
            $(
                RedisModuleCommand {
                    name: $name,
                    mutates: $mutates,
                    family: $family,
                    implemented: true,
                },
            )+
        ]
    };
}

#[cfg(feature = "redis-module-ai")]
#[path = "redis_modules/ai.rs"]
mod ai;
#[cfg(feature = "redis-module-bloom")]
#[path = "redis_modules/bloom.rs"]
mod bloom;
#[cfg(feature = "redis-module-cell")]
#[path = "redis_modules/cell.rs"]
mod cell;
#[cfg(feature = "redis-module-cms")]
#[path = "redis_modules/count_min_sketch.rs"]
mod count_min_sketch;
#[cfg(feature = "redis-module-cthulhu")]
#[path = "redis_modules/cthulhu.rs"]
mod cthulhu;
#[cfg(feature = "redis-module-gears")]
#[path = "redis_modules/gears.rs"]
mod gears;
#[cfg(feature = "redis-module-graph")]
#[path = "redis_modules/graph.rs"]
mod graph;
#[cfg(feature = "redis-module-json")]
#[path = "redis_modules/json.rs"]
mod json;
#[cfg(feature = "redis-module-neural")]
#[path = "redis_modules/neural.rs"]
mod neural;
#[cfg(feature = "redis-module-rede")]
#[path = "redis_modules/rede.rs"]
mod rede;
#[cfg(feature = "redis-module-roaring")]
#[path = "redis_modules/roaring.rs"]
mod roaring;
#[cfg(feature = "redis-module-search")]
#[path = "redis_modules/search.rs"]
mod search;
#[cfg(feature = "redis-module-session-gate")]
#[path = "redis_modules/session_gate.rs"]
mod session_gate;
#[cfg(feature = "redis-module-snowflake")]
#[path = "redis_modules/snowflake.rs"]
mod snowflake;
#[cfg(feature = "redis-module-tdigest")]
#[path = "redis_modules/tdigest.rs"]
mod tdigest;
#[cfg(feature = "redis-module-timeseries")]
#[path = "redis_modules/timeseries.rs"]
mod timeseries;
#[cfg(feature = "redis-module-topk")]
#[path = "redis_modules/topk.rs"]
mod topk;

impl RedisModuleCommand {
    #[inline(always)]
    fn info(self) -> RedisModuleCommandInfo {
        RedisModuleCommandInfo {
            name: self.name,
            mutates: self.mutates,
        }
    }

    fn execute_frame(self, store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        #[cfg(feature = "redis-module-topk")]
        if self.family == "topk" {
            return topk::execute(self.name, store, args);
        }
        if let Some(family) = self.family_enum() {
            return module_api_result_frame(store.redis_module_execute(family, self.name, args));
        }
        let _ = (store, args);
        module_command_error(self.family)
    }

    fn family_enum(self) -> Option<RedisModuleFamily> {
        match self.family {
            "RediSearch" => Some(RedisModuleFamily::RediSearch),
            "RedisBloom" => Some(RedisModuleFamily::RedisBloom),
            "RedisTimeSeries" => Some(RedisModuleFamily::RedisTimeSeries),
            "RedisGraph" => Some(RedisModuleFamily::RedisGraph),
            "RedisJSON" => Some(RedisModuleFamily::RedisJson),
            "RedisAI" => Some(RedisModuleFamily::RedisAi),
            "RedisGears" => Some(RedisModuleFamily::RedisGears),
            "redis-cell" => Some(RedisModuleFamily::RedisCell),
            "neural-redis" => Some(RedisModuleFamily::NeuralRedis),
            "redis-tdigest" => Some(RedisModuleFamily::RedisTDigest),
            "Cthulhu" => Some(RedisModuleFamily::Cthulhu),
            "Redis Snowflake" => Some(RedisModuleFamily::RedisSnowflake),
            "Redis-roaring" => Some(RedisModuleFamily::RedisRoaring),
            "Session Gate" => Some(RedisModuleFamily::SessionGate),
            "ReDe" => Some(RedisModuleFamily::ReDe),
            "topk" => Some(RedisModuleFamily::TopK),
            "countminsketch" => Some(RedisModuleFamily::CountMinSketch),
            _ => None,
        }
    }
}

impl crate::commands::CommandMetadata for RedisModuleCommand {
    fn name(&self) -> &'static str {
        self.name
    }

    fn mutates_value(&self) -> bool {
        self.mutates
    }

    #[inline(always)]
    fn matches(&self, name: &[u8]) -> bool {
        name.eq_ignore_ascii_case(self.name.as_bytes())
    }
}

impl crate::commands::CommandDefinition for RedisModuleCommand {
    fn parse_owned(&self, parts: &[Vec<u8>]) -> Result<Command> {
        let command = parts
            .first()
            .and_then(|name| find_redis_module_command(name))
            .ok_or_else(|| ShardCacheError::Command("unknown Redis module command".to_string()))?;
        Ok(Command::new(Box::new(OwnedRedisModuleCommand {
            command,
            args: parts.iter().skip(1).cloned().collect(),
        })))
    }

    fn parse_borrowed<'a>(
        &self,
        parts: &[&'a [u8]],
    ) -> Result<crate::commands::BorrowedCommandBox<'a>> {
        let command = parts
            .first()
            .and_then(|name| find_redis_module_command(name))
            .ok_or_else(|| ShardCacheError::Command("unknown Redis module command".to_string()))?;
        Ok(Box::new(BorrowedRedisModuleCommand {
            command,
            args: parts.iter().skip(1).copied().collect(),
        }))
    }
}

#[derive(Debug, Clone)]
struct OwnedRedisModuleCommand {
    command: &'static RedisModuleCommand,
    args: Vec<Vec<u8>>,
}

impl crate::commands::OwnedCommandObject for OwnedRedisModuleCommand {
    fn name(&self) -> &'static str {
        self.command.name
    }

    fn mutates_value(&self) -> bool {
        self.command.mutates
    }

    fn route_key(&self) -> Option<&[u8]> {
        self.args.first().map(Vec::as_slice)
    }

    fn to_borrowed_command(&self) -> crate::commands::BorrowedCommandBox<'_> {
        Box::new(BorrowedRedisModuleCommand {
            command: self.command,
            args: self.args.iter().map(Vec::as_slice).collect(),
        })
    }
}

#[derive(Debug, Clone)]
struct BorrowedRedisModuleCommand<'a> {
    command: &'static RedisModuleCommand,
    args: Vec<&'a [u8]>,
}

impl<'a> crate::commands::BorrowedCommandObject<'a> for BorrowedRedisModuleCommand<'a> {
    fn name(&self) -> &'static str {
        self.command.name
    }

    fn mutates_value(&self) -> bool {
        self.command.mutates
    }

    fn route_key(&self) -> Option<&'a [u8]> {
        self.args.first().copied()
    }

    fn supports_spanned_resp(&self) -> bool {
        false
    }

    fn to_owned_command(&self) -> Command {
        Command::new(Box::new(OwnedRedisModuleCommand {
            command: self.command,
            args: self.args.iter().map(|arg| arg.to_vec()).collect(),
        }))
    }

    fn execute_engine<'b>(&'b self, _ctx: EngineCommandContext<'b>) -> EngineFrameFuture<'b>
    where
        'a: 'b,
    {
        Box::pin(async move {
            Err(ShardCacheError::Command(format!(
                "{} requires embedded Redis module storage",
                self.command.name
            )))
        })
    }

    #[cfg(feature = "server")]
    fn execute_borrowed_frame(&self, store: &EmbeddedStore, _now_ms: u64) -> Frame {
        self.command.execute_frame(store, &self.args)
    }

    #[cfg(feature = "server")]
    fn execute_borrowed(&self, ctx: BorrowedCommandContext<'_, '_, '_>) {
        with_resp_protocol(ctx.resp_protocol, || {
            write_frame(ctx.out, &self.command.execute_frame(ctx.store, &self.args));
        });
    }

    #[cfg(feature = "server")]
    fn execute_direct_borrowed(&self, _ctx: DirectCommandContext) -> Frame {
        module_command_error(self.command.family)
    }
}

#[cfg(feature = "server")]
impl crate::server::commands::RawDirectCommand for RedisModuleCommand {
    fn execute(&self, ctx: RawCommandContext<'_, '_, '_, '_>) {
        with_resp_protocol(ctx.resp_protocol, || {
            write_frame(ctx.out, &self.execute_frame(ctx.store, &ctx.args));
        });
    }

    fn execute_fast(&self, ctx: RawCommandContext<'_, '_, '_, '_>) {
        let start = ServerWire::begin_fast_value(ctx.out);
        with_resp_protocol(crate::server::wire::RespProtocolVersion::Resp2, || {
            write_frame(ctx.out, &self.execute_frame(ctx.store, &ctx.args));
        });
        ServerWire::finish_fast_value(ctx.out, start);
    }
}

fn module_command_error(family: &str) -> Frame {
    Frame::Error(format!(
        "ERR {family} module storage engine is not implemented yet"
    ))
}

fn module_api_result_frame(result: RedisModuleApiResult) -> Frame {
    match result {
        RedisModuleApiResult::Simple(value) => simple(value),
        RedisModuleApiResult::Integer(value) => int(value),
        RedisModuleApiResult::Bulk(Some(value)) => bulk(value),
        RedisModuleApiResult::Bulk(None) => Frame::Null,
        RedisModuleApiResult::Array(items) => {
            Frame::Array(items.into_iter().map(module_api_result_frame).collect())
        }
        RedisModuleApiResult::Error(message) => error(&message),
        RedisModuleApiResult::TopKInfo {
            k,
            width,
            depth,
            decay,
        } => Frame::Array(vec![
            simple("k"),
            int(k as i64),
            simple("width"),
            int(width as i64),
            simple("depth"),
            int(depth as i64),
            simple("decay"),
            simple(&format!("{decay:.17}")),
        ]),
        RedisModuleApiResult::Unsupported {
            family, command, ..
        } => error(&format!(
            "ERR {family:?} module command is not supported: {command}"
        )),
    }
}

pub(crate) fn dispatch(name: &str, store: &EmbeddedStore, args: &[&[u8]]) -> Option<Frame> {
    find_redis_module_command(name.as_bytes()).map(|command| command.execute_frame(store, args))
}

#[allow(dead_code)]
pub(crate) fn module_family_for_command(name: &[u8]) -> Option<&'static str> {
    find_redis_module_command(name).map(|command| command.family)
}

pub(crate) fn find_command_definition(
    name: &[u8],
) -> Option<&'static dyn crate::commands::CommandDefinition> {
    find_redis_module_command(name)
        .map(|command| command as &'static dyn crate::commands::CommandDefinition)
}

#[cfg(feature = "server")]
pub(crate) fn find_raw_direct_command(
    name: &[u8],
) -> Option<&'static dyn crate::server::commands::RawDirectCommand> {
    find_redis_module_command(name)
        .map(|command| command as &'static dyn crate::server::commands::RawDirectCommand)
}

fn enabled_command_sets() -> &'static [&'static [RedisModuleCommand]] {
    &[
        #[cfg(feature = "redis-module-ai")]
        ai::COMMANDS,
        #[cfg(feature = "redis-module-bloom")]
        bloom::BLOOM_COMMANDS,
        #[cfg(feature = "redis-module-bloom")]
        bloom::CUCKOO_COMMANDS,
        #[cfg(feature = "redis-module-cell")]
        cell::COMMANDS,
        #[cfg(feature = "redis-module-cms")]
        count_min_sketch::COMMANDS,
        #[cfg(feature = "redis-module-cthulhu")]
        cthulhu::COMMANDS,
        #[cfg(feature = "redis-module-gears")]
        gears::COMMANDS,
        #[cfg(feature = "redis-module-graph")]
        graph::COMMANDS,
        #[cfg(feature = "redis-module-json")]
        json::COMMANDS,
        #[cfg(feature = "redis-module-neural")]
        neural::COMMANDS,
        #[cfg(feature = "redis-module-rede")]
        rede::COMMANDS,
        #[cfg(feature = "redis-module-roaring")]
        roaring::COMMANDS,
        #[cfg(feature = "redis-module-search")]
        search::COMMANDS,
        #[cfg(feature = "redis-module-session-gate")]
        session_gate::COMMANDS,
        #[cfg(feature = "redis-module-snowflake")]
        snowflake::COMMANDS,
        #[cfg(feature = "redis-module-tdigest")]
        tdigest::COMMANDS,
        #[cfg(feature = "redis-module-timeseries")]
        timeseries::COMMANDS,
        #[cfg(feature = "redis-module-topk")]
        topk::COMMANDS,
    ]
}

fn enabled_module_slices() -> &'static [&'static [EnabledModuleInfo]] {
    &[
        #[cfg(feature = "redis-module-ai")]
        ai::MODULES,
        #[cfg(feature = "redis-module-bloom")]
        bloom::MODULES,
        #[cfg(feature = "redis-module-cell")]
        cell::MODULES,
        #[cfg(feature = "redis-module-cms")]
        count_min_sketch::MODULES,
        #[cfg(feature = "redis-module-cthulhu")]
        cthulhu::MODULES,
        #[cfg(feature = "redis-module-gears")]
        gears::MODULES,
        #[cfg(feature = "redis-module-graph")]
        graph::MODULES,
        #[cfg(feature = "redis-module-json")]
        json::MODULES,
        #[cfg(feature = "redis-module-neural")]
        neural::MODULES,
        #[cfg(feature = "redis-module-rede")]
        rede::MODULES,
        #[cfg(feature = "redis-module-roaring")]
        roaring::MODULES,
        #[cfg(feature = "redis-module-search")]
        search::MODULES,
        #[cfg(feature = "redis-module-session-gate")]
        session_gate::MODULES,
        #[cfg(feature = "redis-module-snowflake")]
        snowflake::MODULES,
        #[cfg(feature = "redis-module-tdigest")]
        tdigest::MODULES,
        #[cfg(feature = "redis-module-timeseries")]
        timeseries::MODULES,
        #[cfg(feature = "redis-module-topk")]
        topk::MODULES,
    ]
}

pub(crate) fn find_redis_module_command(name: &[u8]) -> Option<&'static RedisModuleCommand> {
    enabled_command_sets()
        .iter()
        .copied()
        .find_map(|commands| find_in_commands(commands, name))
}

fn find_in_commands(
    commands: &'static [RedisModuleCommand],
    name: &[u8],
) -> Option<&'static RedisModuleCommand> {
    commands
        .iter()
        .find(|command| command.implemented && name.eq_ignore_ascii_case(command.name.as_bytes()))
}

pub(crate) fn command_info_metadata() -> Vec<RedisModuleCommandInfo> {
    enabled_command_sets()
        .iter()
        .flat_map(|commands| commands.iter().copied())
        .filter(|command| command.implemented)
        .map(RedisModuleCommand::info)
        .collect()
}

pub(crate) fn enabled_modules() -> Vec<EnabledModuleInfo> {
    enabled_module_slices()
        .iter()
        .flat_map(|modules| modules.iter().copied())
        .collect()
}

pub(crate) fn module_list_frame() -> Frame {
    Frame::Array(
        enabled_modules()
            .into_iter()
            .map(|module| {
                Frame::Array(vec![
                    bulk(b"name".to_vec()),
                    bulk(module.name.as_bytes().to_vec()),
                    bulk(b"ver".to_vec()),
                    Frame::Integer(module.version),
                    bulk(b"path".to_vec()),
                    bulk(Vec::new()),
                    bulk(b"args".to_vec()),
                    Frame::Array(Vec::new()),
                ])
            })
            .collect(),
    )
}
