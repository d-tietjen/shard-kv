use super::{EnabledModuleInfo, RedisModuleCommand};

pub(super) const MODULES: &[EnabledModuleInfo] = &[EnabledModuleInfo {
    name: "cthulhu",
    version: 1,
}];

pub(super) const COMMANDS: &[RedisModuleCommand] =
    redis_module_commands!["Cthulhu"; "JS.EVAL" => true, "JS.GET" => false, "JS.DEL" => true];
