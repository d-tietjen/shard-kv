#![allow(dead_code, unused_imports)]

use super::super::*;

#[cfg(feature = "redis-module-ai")]
impl EmbeddedStore {
    pub(crate) fn ai_api_execute(&self, command: &str, args: &[&[u8]]) -> RedisModuleApiResult {
        match command.to_ascii_uppercase().as_str() {
            "AI.TENSORSET" | "AI.MODELSET" | "AI.MODELSTORE" | "AI.SCRIPTSET"
            | "AI.SCRIPTSTORE"
                if !args.is_empty() =>
            {
                let route = self.route_key(args[0]);
                self.module_state
                    .write(route)
                    .records
                    .insert(args[0].to_vec(), ModuleRecord::new(args));
                RedisModuleApiResult::Simple("OK")
            }
            "AI.TENSORGET" | "AI.MODELGET" | "AI.SCRIPTGET" | "AI.INFO" if !args.is_empty() => {
                self.module_record_command(RedisModuleFamily::RedisAi, command, args)
            }
            "AI.TENSORDEL" | "AI.MODELDEL" | "AI.SCRIPTDEL" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let removed = self
                    .module_state
                    .write(route)
                    .records
                    .remove(args[0])
                    .is_some();
                RedisModuleApiResult::Integer(if removed { 1 } else { 0 })
            }
            "AI.CONFIG" => {
                if args.first().is_some_and(|arg| bytes_eq(arg, b"GET")) {
                    RedisModuleApiResult::Array(Vec::new())
                } else {
                    RedisModuleApiResult::Simple("OK")
                }
            }
            "AI.MODELRUN" | "AI.MODELEXECUTE" | "AI.SCRIPTRUN" | "AI.SCRIPTEXECUTE"
            | "AI.DAGRUN" | "AI.DAGRUN_RO" | "AI.DAGEXECUTE" => {
                RedisModuleApiResult::Array(Vec::new())
            }
            "AI._MODELSCAN" | "AI._SCRIPTSCAN" => RedisModuleApiResult::Array(
                self.module_record_keys()
                    .into_iter()
                    .map(result_bulk_bytes)
                    .collect(),
            ),
            _ => self.module_record_command(RedisModuleFamily::RedisAi, command, args),
        }
    }
}
