use super::{EnabledModuleInfo, RedisModuleCommand};

pub(super) const MODULES: &[EnabledModuleInfo] = &[EnabledModuleInfo {
    name: "cms",
    version: 1,
}];

pub(super) const COMMANDS: &[RedisModuleCommand] = redis_module_commands![
    "countminsketch";
    "CMS.INITBYDIM" => true, "CMS.INITBYPROB" => true, "CMS.INCRBY" => true,
    "CMS.QUERY" => false, "CMS.MERGE" => true, "CMS.INFO" => false,
];
