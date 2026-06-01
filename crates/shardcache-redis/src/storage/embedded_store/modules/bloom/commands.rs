#![allow(dead_code, unused_imports)]

use super::super::*;

#[cfg(feature = "redis-module-bloom")]
impl EmbeddedStore {
    pub(crate) fn bloom_api_execute(&self, command: &str, args: &[&[u8]]) -> RedisModuleApiResult {
        match command.to_ascii_uppercase().as_str() {
            "BF.RESERVE" if args.len() >= 3 => {
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                shard.sets.entry(args[0].to_vec()).or_default();
                shard
                    .records
                    .insert(args[0].to_vec(), ModuleRecord::new(args));
                RedisModuleApiResult::Simple("OK")
            }
            "BF.ADD" if args.len() >= 2 => {
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                let inserted = shard
                    .sets
                    .entry(args[0].to_vec())
                    .or_default()
                    .insert(args[1].to_vec());
                RedisModuleApiResult::Integer(if inserted { 1 } else { 0 })
            }
            "BF.MADD" if args.len() >= 2 => {
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                let set = shard.sets.entry(args[0].to_vec()).or_default();
                RedisModuleApiResult::Array(
                    args[1..]
                        .iter()
                        .map(|item| {
                            RedisModuleApiResult::Integer(if set.insert((*item).to_vec()) {
                                1
                            } else {
                                0
                            })
                        })
                        .collect(),
                )
            }
            "BF.EXISTS" if args.len() >= 2 => {
                let route = self.route_key(args[0]);
                let exists = self
                    .module_state
                    .read(route)
                    .sets
                    .get(args[0])
                    .is_some_and(|set| set.contains(args[1]));
                RedisModuleApiResult::Integer(if exists { 1 } else { 0 })
            }
            "BF.MEXISTS" if args.len() >= 2 => {
                let route = self.route_key(args[0]);
                let shard = self.module_state.read(route);
                let set = shard.sets.get(args[0]);
                RedisModuleApiResult::Array(
                    args[1..]
                        .iter()
                        .map(|item| {
                            RedisModuleApiResult::Integer(
                                if set.is_some_and(|set| set.contains(*item)) {
                                    1
                                } else {
                                    0
                                },
                            )
                        })
                        .collect(),
                )
            }
            "BF.CARD" | "BF.INFO" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let len = self
                    .module_state
                    .read(route)
                    .sets
                    .get(args[0])
                    .map_or(0, FastHashSet::len);
                if command.eq_ignore_ascii_case("BF.CARD") {
                    RedisModuleApiResult::Integer(len as i64)
                } else {
                    RedisModuleApiResult::Array(vec![
                        result_bulk_string("Capacity"),
                        RedisModuleApiResult::Integer(len as i64),
                        result_bulk_string("Size"),
                        RedisModuleApiResult::Integer(len as i64),
                        result_bulk_string("Number of filters"),
                        RedisModuleApiResult::Integer(1),
                        result_bulk_string("Number of items inserted"),
                        RedisModuleApiResult::Integer(len as i64),
                    ])
                }
            }
            "BF.INSERT" if !args.is_empty() => {
                let Some(items_at) = args.iter().position(|arg| bytes_eq(arg, b"ITEMS")) else {
                    return invalid_arg("BF.INSERT requires ITEMS");
                };
                if items_at + 1 >= args.len() {
                    return invalid_arg("BF.INSERT requires at least one item");
                }
                self.bloom_api_execute("BF.MADD", &[&[args[0]], &args[items_at + 1..]].concat())
            }
            "BF.SCANDUMP" if !args.is_empty() => {
                RedisModuleApiResult::Array(vec![RedisModuleApiResult::Integer(0), result_null()])
            }
            "BF.LOADCHUNK" if args.len() >= 3 => {
                let route = self.route_key(args[0]);
                self.module_state
                    .write(route)
                    .records
                    .insert(args[0].to_vec(), ModuleRecord::new(args));
                RedisModuleApiResult::Simple("OK")
            }
            "CF.RESERVE" if args.len() >= 2 => {
                let route = self.route_key(args[0]);
                self.module_state
                    .write(route)
                    .multisets
                    .entry(args[0].to_vec())
                    .or_default();
                RedisModuleApiResult::Simple("OK")
            }
            "CF.ADD" | "CF.ADDNX" if args.len() >= 2 => {
                let add_nx = command.eq_ignore_ascii_case("CF.ADDNX");
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                let counts = shard.multisets.entry(args[0].to_vec()).or_default();
                let count = counts.entry(args[1].to_vec()).or_insert(0);
                if add_nx && *count > 0 {
                    RedisModuleApiResult::Integer(0)
                } else {
                    *count = count.saturating_add(1);
                    RedisModuleApiResult::Integer(1)
                }
            }
            "CF.INSERT" | "CF.INSERTNX" if !args.is_empty() => {
                let Some(items_at) = args.iter().position(|arg| bytes_eq(arg, b"ITEMS")) else {
                    return invalid_arg("CF.INSERT requires ITEMS");
                };
                let add_nx = command.eq_ignore_ascii_case("CF.INSERTNX");
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                let counts = shard.multisets.entry(args[0].to_vec()).or_default();
                RedisModuleApiResult::Array(
                    args[items_at + 1..]
                        .iter()
                        .map(|item| {
                            let count = counts.entry((*item).to_vec()).or_insert(0);
                            if add_nx && *count > 0 {
                                RedisModuleApiResult::Integer(0)
                            } else {
                                *count = count.saturating_add(1);
                                RedisModuleApiResult::Integer(1)
                            }
                        })
                        .collect(),
                )
            }
            "CF.EXISTS" | "CF.COUNT" if args.len() >= 2 => {
                let route = self.route_key(args[0]);
                let count = self
                    .module_state
                    .read(route)
                    .multisets
                    .get(args[0])
                    .and_then(|counts| counts.get(args[1]).copied())
                    .unwrap_or(0);
                if command.eq_ignore_ascii_case("CF.EXISTS") {
                    RedisModuleApiResult::Integer(if count > 0 { 1 } else { 0 })
                } else {
                    RedisModuleApiResult::Integer(count)
                }
            }
            "CF.MEXISTS" if args.len() >= 2 => {
                let route = self.route_key(args[0]);
                let shard = self.module_state.read(route);
                let counts = shard.multisets.get(args[0]);
                RedisModuleApiResult::Array(
                    args[1..]
                        .iter()
                        .map(|item| {
                            let exists = counts
                                .and_then(|counts| counts.get(*item))
                                .is_some_and(|count| *count > 0);
                            RedisModuleApiResult::Integer(if exists { 1 } else { 0 })
                        })
                        .collect(),
                )
            }
            "CF.DEL" if args.len() >= 2 => {
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                let Some(counts) = shard.multisets.get_mut(args[0]) else {
                    return RedisModuleApiResult::Integer(0);
                };
                let Some(count) = counts.get_mut(args[1]) else {
                    return RedisModuleApiResult::Integer(0);
                };
                *count -= 1;
                if *count <= 0 {
                    counts.remove(args[1]);
                }
                RedisModuleApiResult::Integer(1)
            }
            "CF.INFO" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let len = self
                    .module_state
                    .read(route)
                    .multisets
                    .get(args[0])
                    .map_or(0, FastHashMap::len);
                RedisModuleApiResult::Array(vec![
                    result_bulk_string("Size"),
                    RedisModuleApiResult::Integer(len as i64),
                    result_bulk_string("Number of buckets"),
                    RedisModuleApiResult::Integer(len as i64),
                    result_bulk_string("Number of items inserted"),
                    RedisModuleApiResult::Integer(len as i64),
                ])
            }
            "CF.SCANDUMP" if !args.is_empty() => {
                RedisModuleApiResult::Array(vec![RedisModuleApiResult::Integer(0), result_null()])
            }
            "CF.LOADCHUNK" if args.len() >= 3 => RedisModuleApiResult::Simple("OK"),
            _ => self.module_record_command(RedisModuleFamily::RedisBloom, command, args),
        }
    }
}
