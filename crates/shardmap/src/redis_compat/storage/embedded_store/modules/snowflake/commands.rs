#![allow(dead_code, unused_imports)]

use super::super::*;

#[cfg(feature = "redis-module-snowflake")]
impl EmbeddedStore {
    pub(crate) fn snowflake_api_execute(
        &self,
        command: &str,
        args: &[&[u8]],
    ) -> RedisModuleApiResult {
        match command.to_ascii_uppercase().as_str() {
            "SNOWFLAKE.NEXT" => {
                let key = args.first().copied().unwrap_or(b"__snowflake__");
                let route = self.route_key(key);
                let mut shard = self.module_state.write(route);
                let next = shard.counters.entry(key.to_vec()).or_insert(0);
                *next = next.saturating_add(1);
                RedisModuleApiResult::Integer(*next as i64)
            }
            "SNOWFLAKE.INFO" => {
                let total = self
                    .module_state
                    .shards
                    .iter()
                    .map(|shard| shard.read().counters.values().sum::<u64>())
                    .sum::<u64>();
                RedisModuleApiResult::Array(vec![
                    result_bulk_string("generated"),
                    RedisModuleApiResult::Integer(total as i64),
                ])
            }
            _ => self.module_record_command(RedisModuleFamily::RedisSnowflake, command, args),
        }
    }
}
