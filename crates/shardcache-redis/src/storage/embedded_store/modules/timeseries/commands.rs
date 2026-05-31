#![allow(dead_code, unused_imports)]

use super::super::*;

#[cfg(feature = "redis-module-timeseries")]
impl EmbeddedStore {
    pub(crate) fn timeseries_api_execute(
        &self,
        command: &str,
        args: &[&[u8]],
    ) -> RedisModuleApiResult {
        match command.to_ascii_uppercase().as_str() {
            "TS.CREATE" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                shard.series.entry(args[0].to_vec()).or_default();
                shard
                    .records
                    .insert(args[0].to_vec(), ModuleRecord::new(args));
                RedisModuleApiResult::Simple("OK")
            }
            "TS.ADD" if args.len() >= 3 => self.ts_add(args[0], args[1], args[2]),
            "TS.MADD" if args.len() >= 3 && args.len().is_multiple_of(3) => {
                let mut out = Vec::with_capacity(args.len() / 3);
                for triple in args.chunks_exact(3) {
                    out.push(match self.ts_add(triple[0], triple[1], triple[2]) {
                        RedisModuleApiResult::Integer(timestamp) => {
                            RedisModuleApiResult::Integer(timestamp)
                        }
                        err => err,
                    });
                }
                RedisModuleApiResult::Array(out)
            }
            "TS.GET" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let shard = self.module_state.read(route);
                match shard
                    .series
                    .get(args[0])
                    .and_then(|series| series.last_key_value())
                {
                    Some((timestamp, value)) => RedisModuleApiResult::Array(vec![
                        RedisModuleApiResult::Integer(*timestamp),
                        result_bulk_string(value.to_string()),
                    ]),
                    None => result_null(),
                }
            }
            "TS.RANGE" | "TS.REVRANGE" if args.len() >= 3 => {
                let start = parse_ts_bound(args[1], i64::MIN);
                let end = parse_ts_bound(args[2], i64::MAX);
                let route = self.route_key(args[0]);
                let shard = self.module_state.read(route);
                let mut rows = shard
                    .series
                    .get(args[0])
                    .map(|series| {
                        series
                            .range(start..=end)
                            .map(|(timestamp, value)| {
                                RedisModuleApiResult::Array(vec![
                                    RedisModuleApiResult::Integer(*timestamp),
                                    result_bulk_string(value.to_string()),
                                ])
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                if command.eq_ignore_ascii_case("TS.REVRANGE") {
                    rows.reverse();
                }
                RedisModuleApiResult::Array(rows)
            }
            "TS.DEL" if args.len() >= 3 => {
                let start = parse_ts_bound(args[1], i64::MIN);
                let end = parse_ts_bound(args[2], i64::MAX);
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                let Some(series) = shard.series.get_mut(args[0]) else {
                    return RedisModuleApiResult::Integer(0);
                };
                let keys = series
                    .range(start..=end)
                    .map(|(ts, _)| *ts)
                    .collect::<Vec<_>>();
                let removed = keys.len();
                for key in keys {
                    series.remove(&key);
                }
                RedisModuleApiResult::Integer(removed as i64)
            }
            "TS.INCRBY" | "TS.DECRBY" if args.len() >= 2 => {
                let Some(delta) = parse_f64_lossy(args[1]) else {
                    return invalid_arg("invalid TS delta");
                };
                let signed = if command.eq_ignore_ascii_case("TS.DECRBY") {
                    -delta
                } else {
                    delta
                };
                let timestamp = args
                    .windows(2)
                    .find(|pair| bytes_eq(pair[0], b"TIMESTAMP"))
                    .and_then(|pair| parse_i64_lossy(pair[1]))
                    .unwrap_or_else(|| now_millis() as i64);
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                let series = shard.series.entry(args[0].to_vec()).or_default();
                let previous = series.last_key_value().map_or(0.0, |(_, value)| *value);
                series.insert(timestamp, previous + signed);
                RedisModuleApiResult::Integer(timestamp)
            }
            "TS.INFO" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                let len = self
                    .module_state
                    .read(route)
                    .series
                    .get(args[0])
                    .map_or(0, BTreeMap::len);
                RedisModuleApiResult::Array(vec![
                    result_bulk_string("totalSamples"),
                    RedisModuleApiResult::Integer(len as i64),
                    result_bulk_string("memoryUsage"),
                    RedisModuleApiResult::Integer((len * 16) as i64),
                    result_bulk_string("labels"),
                    RedisModuleApiResult::Array(Vec::new()),
                ])
            }
            "TS.QUERYINDEX" => {
                let keys = self.module_series_keys();
                RedisModuleApiResult::Array(keys.into_iter().map(result_bulk_bytes).collect())
            }
            "TS.MGET" => RedisModuleApiResult::Array(
                self.module_series_keys()
                    .into_iter()
                    .filter_map(|key| {
                        let route = self.route_key(&key);
                        let shard = self.module_state.read(route);
                        let (timestamp, value) = shard.series.get(&key)?.last_key_value()?;
                        Some(RedisModuleApiResult::Array(vec![
                            result_bulk_bytes(key),
                            RedisModuleApiResult::Array(Vec::new()),
                            RedisModuleApiResult::Array(vec![
                                RedisModuleApiResult::Integer(*timestamp),
                                result_bulk_string(value.to_string()),
                            ]),
                        ]))
                    })
                    .collect(),
            ),
            "TS.MRANGE" | "TS.MREVRANGE" if args.len() >= 2 => {
                let start = parse_ts_bound(args[0], i64::MIN);
                let end = parse_ts_bound(args[1], i64::MAX);
                RedisModuleApiResult::Array(
                    self.module_series_keys()
                        .into_iter()
                        .map(|key| {
                            let range = self.timeseries_api_execute(
                                if command.eq_ignore_ascii_case("TS.MREVRANGE") {
                                    "TS.REVRANGE"
                                } else {
                                    "TS.RANGE"
                                },
                                &[&key, args[0], args[1]],
                            );
                            let _ = (start, end);
                            RedisModuleApiResult::Array(vec![
                                result_bulk_bytes(key),
                                RedisModuleApiResult::Array(Vec::new()),
                                range,
                            ])
                        })
                        .collect(),
                )
            }
            "TS.ALTER" | "TS.CREATERULE" | "TS.DELETERULE" => {
                if let Some(key) = args.first() {
                    let route = self.route_key(key);
                    self.module_state
                        .write(route)
                        .records
                        .insert((*key).to_vec(), ModuleRecord::new(args));
                }
                RedisModuleApiResult::Simple("OK")
            }
            _ => self.module_record_command(RedisModuleFamily::RedisTimeSeries, command, args),
        }
    }

    fn ts_add(&self, key: &[u8], raw_timestamp: &[u8], raw_value: &[u8]) -> RedisModuleApiResult {
        let timestamp = if raw_timestamp == b"*" {
            now_millis() as i64
        } else {
            parse_i64_lossy(raw_timestamp).unwrap_or(now_millis() as i64)
        };
        let Some(value) = parse_f64_lossy(raw_value) else {
            return invalid_arg("invalid TS value");
        };
        let route = self.route_key(key);
        self.module_state
            .write(route)
            .series
            .entry(key.to_vec())
            .or_default()
            .insert(timestamp, value);
        RedisModuleApiResult::Integer(timestamp)
    }

    fn module_series_keys(&self) -> Vec<Bytes> {
        let mut keys = Vec::new();
        for shard in &self.module_state.shards {
            keys.extend(shard.read().series.keys().cloned());
        }
        keys.sort();
        keys
    }
}

#[cfg(feature = "redis-module-timeseries")]
fn parse_ts_bound(raw: &[u8], fallback: i64) -> i64 {
    if raw == b"-" {
        i64::MIN
    } else if raw == b"+" {
        i64::MAX
    } else {
        parse_i64_lossy(raw).unwrap_or(fallback)
    }
}
