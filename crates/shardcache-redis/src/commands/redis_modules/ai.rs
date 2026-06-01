use super::{EnabledModuleInfo, RedisModuleCommand};

pub(super) const MODULES: &[EnabledModuleInfo] = &[EnabledModuleInfo {
    name: "ai",
    version: 1,
}];

pub(super) const COMMANDS: &[RedisModuleCommand] = redis_module_commands![
    "RedisAI";
    "AI._MODELSCAN" => false,
    "AI._SCRIPTSCAN" => false,
    "AI.CONFIG" => true,
    "AI.DAGEXECUTE" => true,
    "AI.DAGRUN" => true,
    "AI.DAGRUN_RO" => false,
    "AI.INFO" => false,
    "AI.MODELDEL" => true,
    "AI.MODELEXECUTE" => true,
    "AI.MODELGET" => false,
    "AI.MODELRUN" => true,
    "AI.MODELSET" => true,
    "AI.MODELSTORE" => true,
    "AI.SCRIPTDEL" => true,
    "AI.SCRIPTEXECUTE" => true,
    "AI.SCRIPTGET" => false,
    "AI.SCRIPTRUN" => true,
    "AI.SCRIPTSET" => true,
    "AI.SCRIPTSTORE" => true,
    "AI.TENSORDEL" => true,
    "AI.TENSORGET" => false,
    "AI.TENSORSET" => true,
];
