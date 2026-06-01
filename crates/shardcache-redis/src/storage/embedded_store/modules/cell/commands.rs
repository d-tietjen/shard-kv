#![allow(dead_code, unused_imports)]

use super::super::*;

#[cfg(feature = "redis-module-cell")]
impl EmbeddedStore {
    pub(crate) fn cell_api_execute(&self, command: &str, args: &[&[u8]]) -> RedisModuleApiResult {
        if !command.eq_ignore_ascii_case("CL.THROTTLE") || args.len() < 5 {
            return self.module_record_command(RedisModuleFamily::RedisCell, command, args);
        }
        let max_burst = parse_i64_lossy(args[1]).unwrap_or(0).max(0);
        let count = parse_i64_lossy(args[2]).unwrap_or(1).max(1);
        let period = parse_i64_lossy(args[3]).unwrap_or(1).max(1);
        let quantity = args
            .get(4)
            .and_then(|raw| parse_i64_lossy(raw))
            .unwrap_or(1)
            .max(1);
        let limit = max_burst + count;
        let route = self.route_key(args[0]);
        let mut shard = self.module_state.write(route);
        let bucket = shard
            .cell_buckets
            .entry(args[0].to_vec())
            .or_insert(CellBucket {
                remaining: limit,
                reset_after: period,
            });
        if bucket.remaining < quantity {
            RedisModuleApiResult::Array(vec![
                RedisModuleApiResult::Integer(1),
                RedisModuleApiResult::Integer(limit),
                RedisModuleApiResult::Integer(bucket.remaining),
                RedisModuleApiResult::Integer(bucket.reset_after),
                RedisModuleApiResult::Integer(bucket.reset_after),
            ])
        } else {
            bucket.remaining -= quantity;
            RedisModuleApiResult::Array(vec![
                RedisModuleApiResult::Integer(0),
                RedisModuleApiResult::Integer(limit),
                RedisModuleApiResult::Integer(bucket.remaining),
                RedisModuleApiResult::Integer(-1),
                RedisModuleApiResult::Integer(bucket.reset_after),
            ])
        }
    }
}
