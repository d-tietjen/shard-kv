use super::{EnabledModuleInfo, RedisModuleCommand};

pub(super) const MODULES: &[EnabledModuleInfo] = &[EnabledModuleInfo {
    name: "redis-cell",
    version: 1,
}];

pub(super) const COMMANDS: &[RedisModuleCommand] =
    redis_module_commands!["redis-cell"; "CL.THROTTLE" => true];
