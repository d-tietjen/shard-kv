#![allow(dead_code, unused_imports)]

use super::super::*;

#[cfg(feature = "redis-module-session-gate")]
impl EmbeddedStore {
    pub(crate) fn session_gate_api_execute(
        &self,
        command: &str,
        args: &[&[u8]],
    ) -> RedisModuleApiResult {
        match command.to_ascii_uppercase().as_str() {
            "SG.CREATE" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                self.module_state
                    .write(route)
                    .sets
                    .entry(args[0].to_vec())
                    .or_default();
                RedisModuleApiResult::Simple("OK")
            }
            "SG.VALIDATE" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let exists = self.module_state.read(route).sets.contains_key(args[0]);
                RedisModuleApiResult::Integer(if exists { 1 } else { 0 })
            }
            "SG.DELETE" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let removed = self
                    .module_state
                    .write(route)
                    .sets
                    .remove(args[0])
                    .is_some();
                RedisModuleApiResult::Integer(if removed { 1 } else { 0 })
            }
            _ => self.module_record_command(RedisModuleFamily::SessionGate, command, args),
        }
    }
}
