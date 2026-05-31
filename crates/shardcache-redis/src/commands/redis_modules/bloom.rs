use super::{EnabledModuleInfo, RedisModuleCommand};

pub(super) const MODULES: &[EnabledModuleInfo] = &[EnabledModuleInfo {
    name: "bf",
    version: 1,
}];

pub(super) const BLOOM_COMMANDS: &[RedisModuleCommand] = redis_module_commands![
    "RedisBloom";
    "BF.ADD" => true,
    "BF.CARD" => false,
    "BF.EXISTS" => false,
    "BF.INFO" => false,
    "BF.INSERT" => true,
    "BF.LOADCHUNK" => true,
    "BF.MADD" => true,
    "BF.MEXISTS" => false,
    "BF.RESERVE" => true,
    "BF.SCANDUMP" => false,
];

pub(super) const CUCKOO_COMMANDS: &[RedisModuleCommand] = redis_module_commands![
    "RedisBloom";
    "CF.ADD" => true,
    "CF.ADDNX" => true,
    "CF.COUNT" => false,
    "CF.DEL" => true,
    "CF.EXISTS" => false,
    "CF.INFO" => false,
    "CF.INSERT" => true,
    "CF.INSERTNX" => true,
    "CF.LOADCHUNK" => true,
    "CF.MEXISTS" => false,
    "CF.RESERVE" => true,
    "CF.SCANDUMP" => false,
];
