use super::{EnabledModuleInfo, RedisModuleCommand};

pub(super) const MODULES: &[EnabledModuleInfo] = &[EnabledModuleInfo {
    name: "ReJSON",
    version: 1,
}];

pub(super) const COMMANDS: &[RedisModuleCommand] = redis_module_commands![
    "RedisJSON";
    "JSON.ARRAPPEND" => true,
    "JSON.ARRINDEX" => false,
    "JSON.ARRINSERT" => true,
    "JSON.ARRLEN" => false,
    "JSON.ARRPOP" => true,
    "JSON.ARRTRIM" => true,
    "JSON.CLEAR" => true,
    "JSON.DEBUG" => false,
    "JSON.DEL" => true,
    "JSON.FORGET" => true,
    "JSON.GET" => false,
    "JSON.MERGE" => true,
    "JSON.MGET" => false,
    "JSON.MSET" => true,
    "JSON.NUMINCRBY" => true,
    "JSON.NUMMULTBY" => true,
    "JSON.OBJKEYS" => false,
    "JSON.OBJLEN" => false,
    "JSON.RESP" => false,
    "JSON.SET" => true,
    "JSON.STRAPPEND" => true,
    "JSON.STRLEN" => false,
    "JSON.TOGGLE" => true,
    "JSON.TYPE" => false,
];
