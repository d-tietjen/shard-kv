use super::{EnabledModuleInfo, RedisModuleCommand};

pub(super) const MODULES: &[EnabledModuleInfo] = &[EnabledModuleInfo {
    name: "rg",
    version: 1,
}];

pub(super) const COMMANDS: &[RedisModuleCommand] = redis_module_commands![
    "RedisGears";
    "RG.ABORTEXECUTION" => true,
    "RG.CONFIGGET" => false,
    "RG.CONFIGSET" => true,
    "RG.DUMPEXECUTIONS" => false,
    "RG.DUMPREGISTRATIONS" => false,
    "RG.GETRESULTS" => false,
    "RG.GETRESULTSBLOCKING" => false,
    "RG.JDUMPSESSIONS" => false,
    "RG.JEXECUTE" => true,
    "RG.PYDUMPEXECUTIONS" => false,
    "RG.PYDUMPREQS" => false,
    "RG.PYEXECUTE" => true,
    "RG.PYSTATS" => false,
    "RG.TRIGGER" => true,
    "RG.UNREGISTER" => true,
];
