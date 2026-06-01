use super::{EnabledModuleInfo, RedisModuleCommand};

pub(super) const MODULES: &[EnabledModuleInfo] = &[EnabledModuleInfo {
    name: "neural-redis",
    version: 1,
}];

pub(super) const COMMANDS: &[RedisModuleCommand] = redis_module_commands![
    "neural-redis";
    "NR.CREATE" => true, "NR.RUN" => true, "NR.OBSERVE" => true,
    "NR.TRAIN" => true, "NR.INFO" => false, "NR.DELETE" => true,
];
