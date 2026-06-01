#![allow(dead_code, unused_imports)]

use super::super::*;

#[cfg(feature = "redis-module-search")]
impl EmbeddedStore {
    pub(crate) fn search_api_execute(&self, command: &str, args: &[&[u8]]) -> RedisModuleApiResult {
        match normalize_module_command(command).as_ref() {
            "FT._LIST" => RedisModuleApiResult::Array(
                self.module_search_index_keys()
                    .into_iter()
                    .map(result_bulk_bytes)
                    .collect(),
            ),
            "FT.CREATE" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                shard.search_indexes.insert(args[0].to_vec());
                shard
                    .records
                    .insert(args[0].to_vec(), ModuleRecord::new(args));
                RedisModuleApiResult::Simple("OK")
            }
            "FT.ALIASADD" | "FT.ALIASUPDATE" | "FT.ALTER" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                self.module_state
                    .write(route)
                    .records
                    .insert(args[0].to_vec(), ModuleRecord::new(args));
                RedisModuleApiResult::Simple("OK")
            }
            "FT.ALIASDEL" | "FT.DROPINDEX" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                shard.search_indexes.remove(args[0]);
                let removed = shard.records.remove(args[0]).is_some();
                RedisModuleApiResult::Integer(if removed { 1 } else { 0 })
            }
            "FT.INFO" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let shard = self.module_state.read(route);
                let fields = shard
                    .records
                    .get(args[0])
                    .map_or(0, |record| record.args.len());
                RedisModuleApiResult::Array(vec![
                    result_bulk_string("index_name"),
                    result_bulk_bytes(args[0].to_vec()),
                    result_bulk_string("num_docs"),
                    RedisModuleApiResult::Integer(0),
                    result_bulk_string("num_fields"),
                    RedisModuleApiResult::Integer(fields as i64),
                ])
            }
            "FT.SEARCH" | "FT.AGGREGATE" | "FT.HYBRID" => {
                RedisModuleApiResult::Array(vec![RedisModuleApiResult::Integer(0)])
            }
            "FT.EXPLAIN" | "FT.EXPLAINCLI" | "FT.PROFILE" => result_bulk_string("EMPTY PLAN"),
            "FT.CURSOR" => RedisModuleApiResult::Array(Vec::new()),
            "FT.DICTADD" | "FT.SUGADD" | "FT.SYNUPDATE" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                let set = shard.sets.entry(args[0].to_vec()).or_default();
                let before = set.len();
                for arg in &args[1..] {
                    set.insert((*arg).to_vec());
                }
                RedisModuleApiResult::Integer(set.len().saturating_sub(before) as i64)
            }
            "FT.DICTDEL" | "FT.SUGDEL" if args.len() >= 2 => {
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                let removed = shard
                    .sets
                    .get_mut(args[0])
                    .is_some_and(|set| set.remove(args[1]));
                RedisModuleApiResult::Integer(if removed { 1 } else { 0 })
            }
            "FT.DICTDUMP" | "FT.SUGGET" | "FT.SPELLCHECK" | "FT.SYNDUMP" | "FT.TAGVALS"
                if !args.is_empty() =>
            {
                let route = self.route_key(args[0]);
                let shard = self.module_state.read(route);
                RedisModuleApiResult::Array(
                    shard
                        .sets
                        .get(args[0])
                        .map(|set| set.iter().cloned().map(result_bulk_bytes).collect())
                        .unwrap_or_default(),
                )
            }
            "FT.SUGLEN" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let len = self
                    .module_state
                    .read(route)
                    .sets
                    .get(args[0])
                    .map_or(0, FastHashSet::len);
                RedisModuleApiResult::Integer(len as i64)
            }
            "FT.CONFIG" => {
                if args.first().is_some_and(|arg| bytes_eq(arg, b"GET")) {
                    RedisModuleApiResult::Array(Vec::new())
                } else {
                    RedisModuleApiResult::Simple("OK")
                }
            }
            _ => self.module_record_command(RedisModuleFamily::RediSearch, command, args),
        }
    }
}
