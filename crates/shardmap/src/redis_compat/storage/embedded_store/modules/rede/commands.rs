#![allow(dead_code, unused_imports)]

use super::super::*;

#[cfg(feature = "redis-module-rede")]
impl EmbeddedStore {
    pub(crate) fn rede_api_execute(&self, command: &str, args: &[&[u8]]) -> RedisModuleApiResult {
        match command.to_ascii_uppercase().as_str() {
            "REDE.CREATE" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                self.module_state
                    .write(route)
                    .records
                    .insert(args[0].to_vec(), ModuleRecord::new(args));
                RedisModuleApiResult::Simple("OK")
            }
            "REDE.GET" if !args.is_empty() => {
                self.module_record_command(RedisModuleFamily::ReDe, command, args)
            }
            "REDE.DELETE" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let removed = self
                    .module_state
                    .write(route)
                    .records
                    .remove(args[0])
                    .is_some();
                RedisModuleApiResult::Integer(if removed { 1 } else { 0 })
            }
            _ => self.module_record_command(RedisModuleFamily::ReDe, command, args),
        }
    }
}
