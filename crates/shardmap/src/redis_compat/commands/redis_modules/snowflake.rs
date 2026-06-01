use super::{EnabledModuleInfo, RedisModuleCommand};

pub(super) const MODULES: &[EnabledModuleInfo] = &[EnabledModuleInfo {
    name: "snowflake",
    version: 1,
}];

pub(super) const COMMANDS: &[RedisModuleCommand] = redis_module_commands![
    "Redis Snowflake";
    "SNOWFLAKE.NEXT" => true,
    "SNOWFLAKE.INFO" => false
];
