#![allow(dead_code, unused_imports)]

use super::super::*;

#[cfg(feature = "redis-module-cms")]
impl EmbeddedStore {
    pub(crate) fn cms_api_execute(&self, command: &str, args: &[&[u8]]) -> RedisModuleApiResult {
        match command.to_ascii_uppercase().as_str() {
            "CMS.INITBYDIM" | "CMS.INITBYPROB" if args.len() >= 3 => {
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                shard.multisets.entry(args[0].to_vec()).or_default();
                shard
                    .records
                    .insert(args[0].to_vec(), ModuleRecord::new(args));
                RedisModuleApiResult::Simple("OK")
            }
            "CMS.INCRBY" if args.len() >= 3 && args[1..].len().is_multiple_of(2) => {
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                let counts = shard.multisets.entry(args[0].to_vec()).or_default();
                let mut out = Vec::with_capacity(args[1..].len() / 2);
                for pair in args[1..].chunks_exact(2) {
                    let Some(increment) = parse_i64_lossy(pair[1]) else {
                        return invalid_arg("invalid CMS increment");
                    };
                    let count = counts.entry(pair[0].to_vec()).or_insert(0);
                    *count = count.saturating_add(increment);
                    out.push(RedisModuleApiResult::Integer(*count));
                }
                RedisModuleApiResult::Array(out)
            }
            "CMS.QUERY" if args.len() >= 2 => {
                let route = self.route_key(args[0]);
                let shard = self.module_state.read(route);
                let counts = shard.multisets.get(args[0]);
                RedisModuleApiResult::Array(
                    args[1..]
                        .iter()
                        .map(|item| {
                            RedisModuleApiResult::Integer(
                                counts
                                    .and_then(|counts| counts.get(*item))
                                    .copied()
                                    .unwrap_or(0),
                            )
                        })
                        .collect(),
                )
            }
            "CMS.MERGE" if args.len() >= 3 => {
                let dest = args[0];
                let Some(key_count) = parse_usize_lossy(args[1]) else {
                    return invalid_arg("invalid CMS key count");
                };
                if args.len() < 2 + key_count {
                    return invalid_arg("CMS.MERGE missing source keys");
                }
                let dest_route = self.route_key(dest);
                let mut merged = FastHashMap::<Bytes, i64>::default();
                for key in &args[2..2 + key_count] {
                    let route = self.route_key(key);
                    let shard = self.module_state.read(route);
                    if let Some(counts) = shard.multisets.get(*key) {
                        for (item, count) in counts {
                            *merged.entry(item.clone()).or_insert(0) += count;
                        }
                    }
                }
                self.module_state
                    .write(dest_route)
                    .multisets
                    .insert(dest.to_vec(), merged);
                RedisModuleApiResult::Simple("OK")
            }
            "CMS.INFO" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let len = self
                    .module_state
                    .read(route)
                    .multisets
                    .get(args[0])
                    .map_or(0, FastHashMap::len);
                RedisModuleApiResult::Array(vec![
                    result_bulk_string("width"),
                    RedisModuleApiResult::Integer(len.max(1) as i64),
                    result_bulk_string("depth"),
                    RedisModuleApiResult::Integer(1),
                    result_bulk_string("count"),
                    RedisModuleApiResult::Integer(len as i64),
                ])
            }
            _ => self.module_record_command(RedisModuleFamily::CountMinSketch, command, args),
        }
    }
}
