use super::{EnabledModuleInfo, RedisModuleCommand};

pub(super) const MODULES: &[EnabledModuleInfo] = &[EnabledModuleInfo {
    name: "session-gate",
    version: 1,
}];

pub(super) const COMMANDS: &[RedisModuleCommand] = redis_module_commands![
    "Session Gate";
    "SG.CREATE" => true,
    "SG.VALIDATE" => false,
    "SG.DELETE" => true
];
