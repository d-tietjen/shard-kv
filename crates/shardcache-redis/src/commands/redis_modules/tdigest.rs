use super::{EnabledModuleInfo, RedisModuleCommand};

pub(super) const MODULES: &[EnabledModuleInfo] = &[EnabledModuleInfo {
    name: "tdigest",
    version: 1,
}];

pub(super) const COMMANDS: &[RedisModuleCommand] = redis_module_commands![
    "redis-tdigest";
    "TDIGEST.ADD" => true,
    "TDIGEST.BYRANK" => false,
    "TDIGEST.BYREVRANK" => false,
    "TDIGEST.CDF" => false,
    "TDIGEST.CREATE" => true,
    "TDIGEST.INFO" => false,
    "TDIGEST.MAX" => false,
    "TDIGEST.MERGE" => true,
    "TDIGEST.MIN" => false,
    "TDIGEST.QUANTILE" => false,
    "TDIGEST.RANK" => false,
    "TDIGEST.RESET" => true,
    "TDIGEST.REVRANK" => false,
    "TDIGEST.TRIMMED_MEAN" => false,
];
