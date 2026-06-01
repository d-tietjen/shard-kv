#![allow(dead_code, unused_imports)]

use super::super::*;
use super::helpers::*;

#[cfg(feature = "redis-module-json")]
impl EmbeddedStore {
    pub(crate) fn json_api_execute(&self, command: &str, args: &[&[u8]]) -> RedisModuleApiResult {
        match command.to_ascii_uppercase().as_str() {
            "JSON.SET" if args.len() >= 3 => {
                let key = args[0];
                let path = args[1];
                let Ok(value) = serde_json::from_slice::<serde_json::Value>(args[2]) else {
                    return invalid_arg("invalid JSON");
                };
                let nx = args[3..].iter().any(|arg| bytes_eq(arg, b"NX"));
                let xx = args[3..].iter().any(|arg| bytes_eq(arg, b"XX"));
                let route = self.route_key(key);
                let exists = self.module_state.read(route).json.contains_key(key);
                if (nx && exists) || (xx && !exists) {
                    return result_null();
                }
                let mut shard = self.module_state.write(route);
                if json_path_is_root(path) {
                    shard.json.insert(key.to_vec(), value);
                } else {
                    let doc = shard
                        .json
                        .entry(key.to_vec())
                        .or_insert_with(|| serde_json::Value::Object(Default::default()));
                    json_set_path(doc, path, value);
                }
                RedisModuleApiResult::Simple("OK")
            }
            "JSON.GET" if !args.is_empty() => {
                let path = args.get(1).copied().unwrap_or(b"$");
                self.json_value(args[0], path)
                    .map_or_else(result_null, json_bulk_result)
            }
            "JSON.MGET" if args.len() >= 2 => {
                let path = args[args.len() - 1];
                RedisModuleApiResult::Array(
                    args[..args.len() - 1]
                        .iter()
                        .map(|key| {
                            self.json_value(key, path)
                                .map_or_else(result_null, json_bulk_result)
                        })
                        .collect(),
                )
            }
            "JSON.MSET" if args.len() >= 3 && args.len().is_multiple_of(3) => {
                for triple in args.chunks_exact(3) {
                    let result = self.json_api_execute("JSON.SET", triple);
                    if matches!(result, RedisModuleApiResult::Error(_)) {
                        return result;
                    }
                }
                RedisModuleApiResult::Simple("OK")
            }
            "JSON.DEL" | "JSON.FORGET" if !args.is_empty() => {
                let path = args.get(1).copied().unwrap_or(b"$");
                let route = self.route_key(args[0]);
                let removed = if json_path_is_root(path) {
                    self.module_state
                        .write(route)
                        .json
                        .remove(args[0])
                        .is_some()
                } else {
                    self.module_state
                        .write(route)
                        .json
                        .get_mut(args[0])
                        .is_some_and(|doc| json_remove_path(doc, path))
                };
                RedisModuleApiResult::Integer(if removed { 1 } else { 0 })
            }
            "JSON.TYPE" if !args.is_empty() => {
                let path = args.get(1).copied().unwrap_or(b"$");
                self.json_value(args[0], path)
                    .map(|value| result_bulk_string(json_type_name(&value)))
                    .unwrap_or_else(result_null)
            }
            "JSON.OBJKEYS" if !args.is_empty() => {
                let path = args.get(1).copied().unwrap_or(b"$");
                match self.json_value(args[0], path) {
                    Some(serde_json::Value::Object(map)) => RedisModuleApiResult::Array(
                        map.keys()
                            .map(|key| result_bulk_string(key.clone()))
                            .collect(),
                    ),
                    Some(_) => RedisModuleApiResult::Array(Vec::new()),
                    None => result_null(),
                }
            }
            "JSON.OBJLEN" if !args.is_empty() => {
                let path = args.get(1).copied().unwrap_or(b"$");
                match self.json_value(args[0], path) {
                    Some(serde_json::Value::Object(map)) => {
                        RedisModuleApiResult::Integer(map.len() as i64)
                    }
                    Some(_) => RedisModuleApiResult::Integer(0),
                    None => result_null(),
                }
            }
            "JSON.ARRLEN" if !args.is_empty() => {
                let path = args.get(1).copied().unwrap_or(b"$");
                match self.json_value(args[0], path) {
                    Some(serde_json::Value::Array(items)) => {
                        RedisModuleApiResult::Integer(items.len() as i64)
                    }
                    Some(_) => RedisModuleApiResult::Integer(0),
                    None => result_null(),
                }
            }
            "JSON.ARRAPPEND" if args.len() >= 3 => {
                self.json_array_mutation(args[0], args[1], |array| {
                    for raw in &args[2..] {
                        let value = serde_json::from_slice::<serde_json::Value>(raw)
                            .unwrap_or_else(|_| {
                                serde_json::Value::String(String::from_utf8_lossy(raw).into())
                            });
                        array.push(value);
                    }
                    array.len() as i64
                })
            }
            "JSON.ARRINSERT" if args.len() >= 4 => {
                let index = parse_usize_lossy(args[2]).unwrap_or(0);
                self.json_array_mutation(args[0], args[1], |array| {
                    let mut offset = index.min(array.len());
                    for raw in &args[3..] {
                        let value = serde_json::from_slice::<serde_json::Value>(raw)
                            .unwrap_or_else(|_| {
                                serde_json::Value::String(String::from_utf8_lossy(raw).into())
                            });
                        array.insert(offset, value);
                        offset += 1;
                    }
                    array.len() as i64
                })
            }
            "JSON.ARRPOP" if !args.is_empty() => {
                let path = args.get(1).copied().unwrap_or(b"$");
                self.json_array_pop(args[0], path)
            }
            "JSON.ARRTRIM" if args.len() >= 4 => {
                let start = parse_usize_lossy(args[2]).unwrap_or(0);
                let stop = parse_usize_lossy(args[3]).unwrap_or(start);
                self.json_array_mutation(args[0], args[1], |array| {
                    let end = stop.min(array.len().saturating_sub(1));
                    let keep = if start <= end && start < array.len() {
                        array[start..=end].to_vec()
                    } else {
                        Vec::new()
                    };
                    *array = keep;
                    array.len() as i64
                })
            }
            "JSON.ARRINDEX" if args.len() >= 3 => {
                let needle =
                    serde_json::from_slice::<serde_json::Value>(args[2]).unwrap_or_else(|_| {
                        serde_json::Value::String(String::from_utf8_lossy(args[2]).into())
                    });
                match self.json_value(args[0], args[1]) {
                    Some(serde_json::Value::Array(items)) => RedisModuleApiResult::Integer(
                        items
                            .iter()
                            .position(|item| item == &needle)
                            .map_or(-1, |idx| idx as i64),
                    ),
                    _ => RedisModuleApiResult::Integer(-1),
                }
            }
            "JSON.NUMINCRBY" | "JSON.NUMMULTBY" if args.len() >= 3 => {
                let Some(delta) = parse_f64_lossy(args[2]) else {
                    return invalid_arg("invalid JSON number");
                };
                let multiply = command.eq_ignore_ascii_case("JSON.NUMMULTBY");
                self.json_number_mutation(args[0], args[1], |value| {
                    if multiply {
                        value * delta
                    } else {
                        value + delta
                    }
                })
            }
            "JSON.STRAPPEND" if args.len() >= 3 => {
                let append = String::from_utf8_lossy(args[2])
                    .trim_matches('"')
                    .to_string();
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                let doc = shard
                    .json
                    .entry(args[0].to_vec())
                    .or_insert_with(|| serde_json::Value::String(String::new()));
                let target = json_path_or_root_mut(doc, args[1]);
                match target {
                    serde_json::Value::String(value) => {
                        value.push_str(&append);
                        RedisModuleApiResult::Integer(value.len() as i64)
                    }
                    _ => invalid_arg("JSON path is not a string"),
                }
            }
            "JSON.STRLEN" if !args.is_empty() => {
                let path = args.get(1).copied().unwrap_or(b"$");
                match self.json_value(args[0], path) {
                    Some(serde_json::Value::String(value)) => {
                        RedisModuleApiResult::Integer(value.len() as i64)
                    }
                    Some(_) => RedisModuleApiResult::Integer(0),
                    None => result_null(),
                }
            }
            "JSON.TOGGLE" if args.len() >= 2 => {
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                let Some(doc) = shard.json.get_mut(args[0]) else {
                    return result_null();
                };
                let Some(value) = json_path_mut(doc, args[1]) else {
                    return result_null();
                };
                match value {
                    serde_json::Value::Bool(flag) => {
                        *flag = !*flag;
                        RedisModuleApiResult::Integer(if *flag { 1 } else { 0 })
                    }
                    _ => invalid_arg("JSON path is not a boolean"),
                }
            }
            "JSON.CLEAR" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                let Some(value) = shard.json.get_mut(args[0]) else {
                    return RedisModuleApiResult::Integer(0);
                };
                json_clear_value(value);
                RedisModuleApiResult::Integer(1)
            }
            "JSON.MERGE" if args.len() >= 3 => {
                let Ok(value) = serde_json::from_slice::<serde_json::Value>(args[2]) else {
                    return invalid_arg("invalid JSON");
                };
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                let doc = shard
                    .json
                    .entry(args[0].to_vec())
                    .or_insert(serde_json::Value::Null);
                if json_path_is_root(args[1]) {
                    json_merge_value(doc, value);
                } else {
                    let target = json_path_mut(doc, args[1]);
                    if let Some(target) = target {
                        json_merge_value(target, value);
                    }
                }
                RedisModuleApiResult::Simple("OK")
            }
            "JSON.RESP" if !args.is_empty() => self
                .json_value(args[0], args.get(1).copied().unwrap_or(b"$"))
                .map_or_else(result_null, json_bulk_result),
            "JSON.DEBUG" => match args.first().copied() {
                Some(subcommand) if bytes_eq(subcommand, b"HELP") => RedisModuleApiResult::Array(
                    ["JSON.DEBUG HELP", "JSON.DEBUG MEMORY <key> [path]"]
                        .into_iter()
                        .map(result_bulk_string)
                        .collect(),
                ),
                Some(subcommand) if bytes_eq(subcommand, b"MEMORY") && args.len() >= 2 => {
                    let path = args.get(2).copied().unwrap_or(b"$");
                    let bytes = self
                        .json_value(args[1], path)
                        .and_then(|value| serde_json::to_vec(&value).ok())
                        .map_or(0, |value| value.len());
                    RedisModuleApiResult::Integer(bytes as i64)
                }
                _ => result_bulk_string("OK"),
            },
            _ => self.module_record_command(RedisModuleFamily::RedisJson, command, args),
        }
    }
}
