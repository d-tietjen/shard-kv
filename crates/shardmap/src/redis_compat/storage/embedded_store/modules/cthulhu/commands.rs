#![allow(dead_code, unused_imports)]

use super::super::*;

#[cfg(feature = "redis-module-cthulhu")]
impl EmbeddedStore {
    pub(crate) fn cthulhu_api_execute(
        &self,
        command: &str,
        args: &[&[u8]],
    ) -> RedisModuleApiResult {
        match command.to_ascii_uppercase().as_str() {
            "JS.EVAL" => result_bulk_string("null"),
            "JS.GET" if !args.is_empty() => {
                self.module_record_command(RedisModuleFamily::Cthulhu, command, args)
            }
            "JS.DEL" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let removed = self
                    .module_state
                    .write(route)
                    .records
                    .remove(args[0])
                    .is_some();
                RedisModuleApiResult::Integer(if removed { 1 } else { 0 })
            }
            _ => self.module_record_command(RedisModuleFamily::Cthulhu, command, args),
        }
    }
}
