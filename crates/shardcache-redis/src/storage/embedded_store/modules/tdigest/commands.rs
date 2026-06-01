#![allow(dead_code, unused_imports)]

use super::super::*;

#[cfg(feature = "redis-module-tdigest")]
impl EmbeddedStore {
    pub(crate) fn tdigest_api_execute(
        &self,
        command: &str,
        args: &[&[u8]],
    ) -> RedisModuleApiResult {
        match command.to_ascii_uppercase().as_str() {
            "TDIGEST.CREATE" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                self.module_state
                    .write(route)
                    .floats
                    .entry(args[0].to_vec())
                    .or_default();
                RedisModuleApiResult::Simple("OK")
            }
            "TDIGEST.ADD" if args.len() >= 2 => {
                let route = self.route_key(args[0]);
                let mut shard = self.module_state.write(route);
                let values = shard.floats.entry(args[0].to_vec()).or_default();
                for raw in &args[1..] {
                    let Some(value) = parse_f64_lossy(raw) else {
                        return invalid_arg("invalid TDIGEST value");
                    };
                    values.push(value);
                }
                RedisModuleApiResult::Simple("OK")
            }
            "TDIGEST.RESET" if !args.is_empty() => {
                let route = self.route_key(args[0]);
                if let Some(values) = self.module_state.write(route).floats.get_mut(args[0]) {
                    values.clear();
                }
                RedisModuleApiResult::Simple("OK")
            }
            "TDIGEST.MIN" | "TDIGEST.MAX" if !args.is_empty() => {
                let values = self.sorted_floats(args[0]);
                let value = if command.eq_ignore_ascii_case("TDIGEST.MIN") {
                    values.first().copied()
                } else {
                    values.last().copied()
                };
                value.map_or_else(result_null, |value| result_bulk_string(value.to_string()))
            }
            "TDIGEST.QUANTILE" if args.len() >= 2 => {
                let values = self.sorted_floats(args[0]);
                RedisModuleApiResult::Array(
                    args[1..]
                        .iter()
                        .map(|raw| {
                            let Some(q) = parse_f64_lossy(raw) else {
                                return invalid_arg("invalid quantile");
                            };
                            if values.is_empty() {
                                result_null()
                            } else {
                                let idx = ((values.len() - 1) as f64 * q.clamp(0.0, 1.0)).round()
                                    as usize;
                                result_bulk_string(values[idx].to_string())
                            }
                        })
                        .collect(),
                )
            }
            "TDIGEST.CDF" | "TDIGEST.RANK" | "TDIGEST.REVRANK" if args.len() >= 2 => {
                let values = self.sorted_floats(args[0]);
                RedisModuleApiResult::Array(
                    args[1..]
                        .iter()
                        .map(|raw| {
                            let Some(value) = parse_f64_lossy(raw) else {
                                return invalid_arg("invalid TDIGEST value");
                            };
                            match command.to_ascii_uppercase().as_str() {
                                "TDIGEST.CDF" => {
                                    let count =
                                        values.iter().filter(|sample| **sample <= value).count();
                                    let cdf = if values.is_empty() {
                                        0.0
                                    } else {
                                        count as f64 / values.len() as f64
                                    };
                                    result_bulk_string(cdf.to_string())
                                }
                                "TDIGEST.RANK" => RedisModuleApiResult::Integer(
                                    values.iter().filter(|sample| **sample < value).count() as i64,
                                ),
                                _ => RedisModuleApiResult::Integer(
                                    values.iter().filter(|sample| **sample > value).count() as i64,
                                ),
                            }
                        })
                        .collect(),
                )
            }
            "TDIGEST.BYRANK" | "TDIGEST.BYREVRANK" if args.len() >= 2 => {
                let values = self.sorted_floats(args[0]);
                RedisModuleApiResult::Array(
                    args[1..]
                        .iter()
                        .map(|raw| {
                            let Some(rank) = parse_usize_lossy(raw) else {
                                return invalid_arg("invalid TDIGEST rank");
                            };
                            let value = if command.eq_ignore_ascii_case("TDIGEST.BYRANK") {
                                values.get(rank).copied()
                            } else {
                                values.iter().rev().nth(rank).copied()
                            };
                            value.map_or_else(result_null, |value| {
                                result_bulk_string(value.to_string())
                            })
                        })
                        .collect(),
                )
            }
            "TDIGEST.TRIMMED_MEAN" if args.len() >= 3 => {
                let values = self.sorted_floats(args[0]);
                let low = parse_f64_lossy(args[1]).unwrap_or(0.0).clamp(0.0, 1.0);
                let high = parse_f64_lossy(args[2]).unwrap_or(1.0).clamp(low, 1.0);
                if values.is_empty() {
                    return result_null();
                }
                let start = ((values.len() - 1) as f64 * low).floor() as usize;
                let end = ((values.len() - 1) as f64 * high).ceil() as usize;
                let slice = &values[start..=end];
                let mean = slice.iter().sum::<f64>() / slice.len() as f64;
                result_bulk_string(mean.to_string())
            }
            "TDIGEST.MERGE" if args.len() >= 3 => {
                let dest = args[0];
                let Some(key_count) = parse_usize_lossy(args[1]) else {
                    return invalid_arg("invalid TDIGEST key count");
                };
                let mut merged = Vec::new();
                for key in &args[2..args.len().min(2 + key_count)] {
                    merged.extend(self.sorted_floats(key));
                }
                let route = self.route_key(dest);
                self.module_state
                    .write(route)
                    .floats
                    .insert(dest.to_vec(), merged);
                RedisModuleApiResult::Simple("OK")
            }
            "TDIGEST.INFO" if !args.is_empty() => {
                let len = self.sorted_floats(args[0]).len();
                RedisModuleApiResult::Array(vec![
                    result_bulk_string("Compression"),
                    RedisModuleApiResult::Integer(100),
                    result_bulk_string("Merged nodes"),
                    RedisModuleApiResult::Integer(len as i64),
                    result_bulk_string("Unmerged nodes"),
                    RedisModuleApiResult::Integer(0),
                ])
            }
            _ => self.module_record_command(RedisModuleFamily::RedisTDigest, command, args),
        }
    }

    fn sorted_floats(&self, key: &[u8]) -> Vec<f64> {
        let route = self.route_key(key);
        let mut values = self
            .module_state
            .read(route)
            .floats
            .get(key)
            .cloned()
            .unwrap_or_default();
        values.sort_by(|left, right| left.total_cmp(right));
        values
    }
}
