use super::{EnabledModuleInfo, RedisModuleCommand};

pub(super) const MODULES: &[EnabledModuleInfo] = &[EnabledModuleInfo {
    name: "timeseries",
    version: 1,
}];

pub(super) const COMMANDS: &[RedisModuleCommand] = redis_module_commands![
    "RedisTimeSeries";
    "TS.ADD" => true,
    "TS.ALTER" => true,
    "TS.CREATE" => true,
    "TS.CREATERULE" => true,
    "TS.DECRBY" => true,
    "TS.DEL" => true,
    "TS.DELETERULE" => true,
    "TS.GET" => false,
    "TS.INCRBY" => true,
    "TS.INFO" => false,
    "TS.MADD" => true,
    "TS.MGET" => false,
    "TS.MRANGE" => false,
    "TS.MREVRANGE" => false,
    "TS.QUERYINDEX" => false,
    "TS.RANGE" => false,
    "TS.REVRANGE" => false,
];
