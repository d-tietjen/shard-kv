#![allow(dead_code, unused_imports)]

use super::super::*;

#[cfg(feature = "redis-module-graph")]
impl EmbeddedStore {
    pub(crate) fn graph_api_execute(&self, command: &str, args: &[&[u8]]) -> RedisModuleApiResult {
        match command.to_ascii_uppercase().as_str() {
            "GRAPH.LIST" => RedisModuleApiResult::Array(
                self.module_record_keys()
                    .into_iter()
                    .map(result_bulk_bytes)
                    .collect(),
            ),
            "GRAPH.QUERY" | "GRAPH.RO_QUERY" if args.len() >= 2 => {
                let route = self.route_key(args[0]);
                self.module_state
                    .write(route)
                    .records
                    .insert(args[0].to_vec(), ModuleRecord::new(args));
                RedisModuleApiResult::Array(vec![
                    RedisModuleApiResult::Array(Vec::new()),
                    RedisModuleApiResult::Array(Vec::new()),
                    RedisModuleApiResult::Array(vec![result_bulk_string(
                        "Query internal execution time: 0 ms",
                    )]),
                ])
            }
            "GRAPH.EXPLAIN" | "GRAPH.PROFILE" if args.len() >= 2 => {
                RedisModuleApiResult::Array(vec![result_bulk_bytes(args[1].to_vec())])
            }
            "GRAPH.DELETE" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let removed = self
                    .module_state
                    .write(route)
                    .records
                    .remove(args[0])
                    .is_some();
                RedisModuleApiResult::Integer(if removed { 1 } else { 0 })
            }
            "GRAPH.SLOWLOG" => RedisModuleApiResult::Array(Vec::new()),
            "GRAPH.CONFIG" => {
                if args.first().is_some_and(|arg| bytes_eq(arg, b"GET")) {
                    RedisModuleApiResult::Array(Vec::new())
                } else {
                    RedisModuleApiResult::Simple("OK")
                }
            }
            _ => self.module_record_command(RedisModuleFamily::RedisGraph, command, args),
        }
    }
}
