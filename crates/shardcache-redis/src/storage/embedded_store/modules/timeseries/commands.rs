#![allow(dead_code, unused_imports)]

use super::super::*;

#[cfg(feature = "redis-module-timeseries")]
impl EmbeddedStore {
    pub(crate) fn timeseries_api_execute(
        &self,
        command: &str,
        args: &[&[u8]],
    ) -> RedisModuleApiResult {
        let command = normalize_module_command(command);
        match command.as_ref() {
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
                        result_bulk_bytes(value.raw.clone()),
                    ]),
                    None => result_null(),
                }
            }
            "TS.RANGE" | "TS.REVRANGE" if args.len() >= 3 => {
                let start = parse_ts_bound(args[1], i64::MIN);
                let end = parse_ts_bound(args[2], i64::MAX);
                let reverse = command.as_ref() == "TS.REVRANGE";
                let route = self.route_key(args[0]);
                let shard = self.module_state.read(route);
                let rows = shard
                    .series
                    .get(args[0])
                    .map(|series| ts_range_rows(series, start, end, reverse))
                    .unwrap_or_default();
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
                let signed = if command.as_ref() == "TS.DECRBY" {
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
                let value = series
                    .last_key_value()
                    .map_or(0.0, |(_, sample)| sample.value)
                    + signed;
                series.insert(timestamp, ts_sample(value));
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
                                result_bulk_bytes(value.raw.clone()),
                            ]),
                        ]))
                    })
                    .collect(),
            ),
            "TS.MRANGE" | "TS.MREVRANGE" if args.len() >= 2 => RedisModuleApiResult::Array(
                self.timeseries_multi_range_rows(command.as_ref(), args)
                    .into_iter()
                    .map(timeseries_multi_range_row_result)
                    .collect(),
            ),
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
            _ => self.module_record_command(
                RedisModuleFamily::RedisTimeSeries,
                command.as_ref(),
                args,
            ),
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
            .insert(
                timestamp,
                TimeSeriesSample {
                    value,
                    raw: raw_value.to_vec(),
                },
            );
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

    pub(crate) fn timeseries_multi_range_rows(
        &self,
        command: &str,
        args: &[&[u8]],
    ) -> Vec<(Bytes, Vec<(i64, Bytes)>)> {
        if args.len() < 2 {
            return Vec::new();
        }
        let start = parse_ts_bound(args[0], i64::MIN);
        let end = parse_ts_bound(args[1], i64::MAX);
        let reverse = command.eq_ignore_ascii_case("TS.MREVRANGE");
        let mut ranges: Vec<(Bytes, Vec<(i64, Bytes)>)> = Vec::new();
        for shard in &self.module_state.shards {
            let shard = shard.read();
            ranges.reserve(shard.series.len());
            for (key, series) in &shard.series {
                ranges.push((
                    key.clone(),
                    ts_range_row_values(series, start, end, reverse),
                ));
            }
        }
        ranges.sort_by(|left, right| left.0.cmp(&right.0));
        ranges
    }

    pub(crate) fn write_timeseries_multi_range<W: TimeSeriesMultiRangeWriter>(
        &self,
        command: &str,
        args: &[&[u8]],
        writer: &mut W,
    ) {
        if args.len() < 2 {
            writer.begin_rows(0);
            return;
        }
        let start = parse_ts_bound(args[0], i64::MIN);
        let end = parse_ts_bound(args[1], i64::MAX);
        let reverse = command.eq_ignore_ascii_case("TS.MREVRANGE");
        let mut keys = Vec::new();
        for shard in &self.module_state.shards {
            let shard = shard.read();
            keys.reserve(shard.series.len());
            keys.extend(shard.series.keys().cloned());
        }
        keys.sort();
        writer.begin_rows(keys.len());
        for key in keys {
            let route = self.route_key(&key);
            let shard = self.module_state.read(route);
            let Some(series) = shard.series.get(&key) else {
                writer.begin_series(&key, 0);
                continue;
            };
            let samples = ts_range_len(series, start, end);
            writer.begin_series(&key, samples);
            if reverse {
                for (timestamp, sample) in series.range(start..=end).rev() {
                    writer.sample(*timestamp, &sample.raw);
                }
            } else {
                for (timestamp, sample) in series.range(start..=end) {
                    writer.sample(*timestamp, &sample.raw);
                }
            }
        }
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

#[cfg(feature = "redis-module-timeseries")]
fn ts_range_rows(
    series: &BTreeMap<i64, TimeSeriesSample>,
    start: i64,
    end: i64,
    reverse: bool,
) -> Vec<RedisModuleApiResult> {
    if reverse {
        series.range(start..=end).rev().map(ts_sample_row).collect()
    } else {
        series.range(start..=end).map(ts_sample_row).collect()
    }
}

#[cfg(feature = "redis-module-timeseries")]
fn ts_range_row_values(
    series: &BTreeMap<i64, TimeSeriesSample>,
    start: i64,
    end: i64,
    reverse: bool,
) -> Vec<(i64, Bytes)> {
    let mut rows = Vec::with_capacity(ts_range_len(series, start, end));
    let iter = series
        .range(start..=end)
        .map(|(timestamp, sample)| (*timestamp, sample.raw.clone()));
    if reverse {
        rows.extend(iter.rev());
    } else {
        rows.extend(iter);
    }
    rows
}

#[cfg(feature = "redis-module-timeseries")]
fn ts_sample_row((timestamp, sample): (&i64, &TimeSeriesSample)) -> RedisModuleApiResult {
    RedisModuleApiResult::Array(vec![
        RedisModuleApiResult::Integer(*timestamp),
        result_bulk_bytes(sample.raw.clone()),
    ])
}

#[cfg(feature = "redis-module-timeseries")]
fn timeseries_multi_range_row_result(
    (key, samples): (Bytes, Vec<(i64, Bytes)>),
) -> RedisModuleApiResult {
    RedisModuleApiResult::Array(vec![
        result_bulk_bytes(key),
        RedisModuleApiResult::Array(Vec::new()),
        RedisModuleApiResult::Array(
            samples
                .into_iter()
                .map(|(timestamp, value)| {
                    RedisModuleApiResult::Array(vec![
                        RedisModuleApiResult::Integer(timestamp),
                        result_bulk_bytes(value),
                    ])
                })
                .collect(),
        ),
    ])
}

#[cfg(feature = "redis-module-timeseries")]
fn ts_sample(value: f64) -> TimeSeriesSample {
    TimeSeriesSample {
        value,
        raw: value.to_string().into_bytes(),
    }
}

#[cfg(feature = "redis-module-timeseries")]
fn ts_range_len(series: &BTreeMap<i64, TimeSeriesSample>, start: i64, end: i64) -> usize {
    match (series.first_key_value(), series.last_key_value()) {
        (Some((first, _)), Some((last, _))) if start <= *first && *last <= end => series.len(),
        _ => series.range(start..=end).count(),
    }
}
