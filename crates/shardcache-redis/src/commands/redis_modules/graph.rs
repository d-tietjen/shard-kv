use super::{EnabledModuleInfo, RedisModuleCommand};

pub(super) const MODULES: &[EnabledModuleInfo] = &[EnabledModuleInfo {
    name: "graph",
    version: 1,
}];

pub(super) const COMMANDS: &[RedisModuleCommand] = redis_module_commands![
    "RedisGraph";
    "GRAPH.CONFIG" => true,
    "GRAPH.DELETE" => true,
    "GRAPH.EXPLAIN" => false,
    "GRAPH.LIST" => false,
    "GRAPH.PROFILE" => false,
    "GRAPH.QUERY" => true,
    "GRAPH.RO_QUERY" => false,
    "GRAPH.SLOWLOG" => false,
];
