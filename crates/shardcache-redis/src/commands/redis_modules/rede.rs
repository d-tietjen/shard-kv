use super::{EnabledModuleInfo, RedisModuleCommand};

pub(super) const MODULES: &[EnabledModuleInfo] = &[EnabledModuleInfo {
    name: "rede",
    version: 1,
}];

pub(super) const COMMANDS: &[RedisModuleCommand] = redis_module_commands![
    "ReDe";
    "REDE.CREATE" => true,
    "REDE.GET" => false,
    "REDE.DELETE" => true
];
