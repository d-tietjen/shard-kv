#![allow(dead_code, unused_imports)]

use super::super::*;
use super::helpers::*;

#[cfg(feature = "redis-module-json")]
impl EmbeddedStore {
    pub(crate) fn json_value(&self, key: &[u8], path: &[u8]) -> Option<serde_json::Value> {
        let route = self.route_key(key);
        let shard = self.module_state.read(route);
        let doc = shard.json.get(key)?;
        json_path(doc, path).cloned()
    }

    pub(crate) fn json_array_mutation(
        &self,
        key: &[u8],
        path: &[u8],
        op: impl FnOnce(&mut Vec<serde_json::Value>) -> i64,
    ) -> RedisModuleApiResult {
        let route = self.route_key(key);
        let mut shard = self.module_state.write(route);
        let doc = shard
            .json
            .entry(key.to_vec())
            .or_insert_with(|| serde_json::Value::Array(Vec::new()));
        let target = json_path_or_root_mut(doc, path);
        match target {
            serde_json::Value::Array(array) => RedisModuleApiResult::Integer(op(array)),
            _ => invalid_arg("JSON path is not an array"),
        }
    }

    pub(crate) fn json_array_pop(&self, key: &[u8], path: &[u8]) -> RedisModuleApiResult {
        let route = self.route_key(key);
        let mut shard = self.module_state.write(route);
        let Some(doc) = shard.json.get_mut(key) else {
            return result_null();
        };
        let Some(target) = json_path_mut(doc, path) else {
            return result_null();
        };
        match target {
            serde_json::Value::Array(array) => {
                array.pop().map_or_else(result_null, json_bulk_result)
            }
            _ => invalid_arg("JSON path is not an array"),
        }
    }

    pub(crate) fn json_number_mutation(
        &self,
        key: &[u8],
        path: &[u8],
        op: impl FnOnce(f64) -> f64,
    ) -> RedisModuleApiResult {
        let route = self.route_key(key);
        let mut shard = self.module_state.write(route);
        let Some(doc) = shard.json.get_mut(key) else {
            return result_null();
        };
        let Some(target) = json_path_mut(doc, path) else {
            return result_null();
        };
        let Some(current) = target.as_f64() else {
            return invalid_arg("JSON path is not a number");
        };
        let next = op(current);
        *target = serde_json::Number::from_f64(next)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null);
        json_bulk_result(target.clone())
    }
}
