#![allow(dead_code, unused_imports)]

use super::super::*;

#[cfg(feature = "redis-module-gears")]
impl EmbeddedStore {
    pub(crate) fn gears_api_execute(&self, command: &str, args: &[&[u8]]) -> RedisModuleApiResult {
        match command.to_ascii_uppercase().as_str() {
            "RG.CONFIGGET" => RedisModuleApiResult::Array(Vec::new()),
            "RG.CONFIGSET" => RedisModuleApiResult::Simple("OK"),
            "RG.PYEXECUTE" | "RG.JEXECUTE" | "RG.TRIGGER" => {
                let id = format!("exec-{}", now_millis()).into_bytes();
                let route = self.route_key(&id);
                self.module_state
                    .write(route)
                    .records
                    .insert(id.clone(), ModuleRecord::new(args));
                result_bulk_bytes(id)
            }
            "RG.DUMPEXECUTIONS"
            | "RG.DUMPREGISTRATIONS"
            | "RG.PYDUMPEXECUTIONS"
            | "RG.PYDUMPREQS"
            | "RG.JDUMPSESSIONS" => RedisModuleApiResult::Array(
                self.module_record_keys()
                    .into_iter()
                    .map(result_bulk_bytes)
                    .collect(),
            ),
            "RG.GETRESULTS" | "RG.GETRESULTSBLOCKING" | "RG.PYSTATS" => {
                RedisModuleApiResult::Array(Vec::new())
            }
            "RG.UNREGISTER" | "RG.ABORTEXECUTION" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let removed = self
                    .module_state
                    .write(route)
                    .records
                    .remove(args[0])
                    .is_some();
                RedisModuleApiResult::Integer(if removed { 1 } else { 0 })
            }
            _ => self.module_record_command(RedisModuleFamily::RedisGears, command, args),
        }
    }
}
