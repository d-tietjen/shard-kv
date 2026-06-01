use super::{EnabledModuleInfo, RedisModuleCommand};

pub(super) const MODULES: &[EnabledModuleInfo] = &[EnabledModuleInfo {
    name: "search",
    version: 1,
}];

pub(super) const COMMANDS: &[RedisModuleCommand] = redis_module_commands![
    "RediSearch";
    "FT._LIST" => false,
    "FT.AGGREGATE" => false,
    "FT.ALIASADD" => true,
    "FT.ALIASDEL" => true,
    "FT.ALIASUPDATE" => true,
    "FT.ALTER" => true,
    "FT.CONFIG" => true,
    "FT.CREATE" => true,
    "FT.CURSOR" => true,
    "FT.DICTADD" => true,
    "FT.DICTDEL" => true,
    "FT.DICTDUMP" => false,
    "FT.DROPINDEX" => true,
    "FT.EXPLAIN" => false,
    "FT.EXPLAINCLI" => false,
    "FT.HYBRID" => false,
    "FT.INFO" => false,
    "FT.PROFILE" => false,
    "FT.SEARCH" => false,
    "FT.SPELLCHECK" => false,
    "FT.SUGADD" => true,
    "FT.SUGDEL" => true,
    "FT.SUGGET" => false,
    "FT.SUGLEN" => false,
    "FT.SYNDUMP" => false,
    "FT.SYNUPDATE" => true,
    "FT.TAGVALS" => false,
];
