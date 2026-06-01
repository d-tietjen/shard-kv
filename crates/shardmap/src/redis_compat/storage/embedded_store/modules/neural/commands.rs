#![allow(dead_code, unused_imports)]

use super::super::*;

#[cfg(feature = "redis-module-neural")]
impl EmbeddedStore {
    pub(crate) fn neural_api_execute(&self, command: &str, args: &[&[u8]]) -> RedisModuleApiResult {
        match command.to_ascii_uppercase().as_str() {
            "NR.CREATE" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                self.module_state
                    .write(route)
                    .records
                    .insert(args[0].to_vec(), ModuleRecord::new(args));
                RedisModuleApiResult::Simple("OK")
            }
            "NR.OBSERVE" | "NR.TRAIN" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                shard
                    .records
                    .entry(args[0].to_vec())
                    .or_insert_with(|| ModuleRecord::new(args))
                    .hits += 1;
                RedisModuleApiResult::Simple("OK")
            }
            "NR.RUN" => RedisModuleApiResult::Array(Vec::new()),
            "NR.INFO" if !args.is_empty() => {
                self.module_record_command(RedisModuleFamily::NeuralRedis, command, args)
            }
            "NR.DELETE" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let removed = self
                    .module_state
                    .write(route)
                    .records
                    .remove(args[0])
                    .is_some();
                RedisModuleApiResult::Integer(if removed { 1 } else { 0 })
            }
            _ => self.module_record_command(RedisModuleFamily::NeuralRedis, command, args),
        }
    }
}
