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
                series.insert(timestamp, ts_sample(timestamp, value));
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
                let filters = parse_ts_label_filters(args);
                let keys = self.timeseries_matching_keys(&filters);
                RedisModuleApiResult::Array(keys.into_iter().map(result_bulk_bytes).collect())
            }
            "TS.MGET" => {
                let filters = parse_ts_filters_after_keyword(args, 0);
                RedisModuleApiResult::Array(
                    self.timeseries_matching_keys(&filters)
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
                )
            }
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
            .insert(timestamp, ts_sample_from_raw(timestamp, value, raw_value));
        RedisModuleApiResult::Integer(timestamp)
    }

    fn timeseries_matching_keys(&self, filters: &[TsLabelFilter<'_>]) -> Vec<Bytes> {
        let mut keys = Vec::new();
        for shard in &self.module_state.shards {
            let shard = shard.read();
            keys.reserve(shard.series.len());
            keys.extend(
                shard
                    .series
                    .keys()
                    .filter(|key| {
                        ts_record_matches_filters(shard.records.get(key.as_slice()), filters)
                    })
                    .cloned(),
            );
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
        let filters = parse_ts_filters_after_keyword(args, 2);
        let mut ranges: Vec<(Bytes, Vec<(i64, Bytes)>)> = Vec::new();
        for shard in &self.module_state.shards {
            let shard = shard.read();
            ranges.reserve(shard.series.len());
            for (key, series) in &shard.series {
                if ts_record_matches_filters(shard.records.get(key.as_slice()), &filters) {
                    ranges.push((
                        key.clone(),
                        ts_range_row_values(series, start, end, reverse),
                    ));
                }
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
        let filters = parse_ts_filters_after_keyword(args, 2);
        if self.module_state.shards.len() == 1 {
            let shard = self.module_state.shards[0].read();
            let mut rows = shard
                .series
                .iter()
                .filter(|(key, _)| {
                    ts_record_matches_filters(shard.records.get(key.as_slice()), &filters)
                })
                .collect::<Vec<_>>();
            rows.sort_unstable_by(|left, right| left.0.cmp(right.0));
            writer.begin_rows(rows.len());
            for (key, series) in rows {
                write_timeseries_range_series(writer, key, series, start, end, reverse);
            }
            return;
        }
        let mut keys = Vec::new();
        for shard in &self.module_state.shards {
            let shard = shard.read();
            keys.reserve(shard.series.len());
            keys.extend(
                shard
                    .series
                    .keys()
                    .filter(|key| {
                        ts_record_matches_filters(shard.records.get(key.as_slice()), &filters)
                    })
                    .cloned(),
            );
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
            write_timeseries_range_series(writer, &key, series, start, end, reverse);
        }
    }
}

#[cfg(feature = "redis-module-timeseries")]
#[derive(Clone, Copy)]
struct TsLabelFilter<'a> {
    label: &'a [u8],
    value: &'a [u8],
}

#[cfg(feature = "redis-module-timeseries")]
fn parse_ts_filters_after_keyword<'a>(args: &[&'a [u8]], start: usize) -> Vec<TsLabelFilter<'a>> {
    let Some(tail) = args.get(start..) else {
        return Vec::new();
    };
    let Some(filter_at) = tail.iter().position(|arg| bytes_eq(arg, b"FILTER")) else {
        return Vec::new();
    };
    parse_ts_label_filters(&args[start + filter_at + 1..])
}

#[cfg(feature = "redis-module-timeseries")]
fn parse_ts_label_filters<'a>(args: &[&'a [u8]]) -> Vec<TsLabelFilter<'a>> {
    args.iter()
        .filter_map(|raw| parse_ts_label_filter(raw))
        .collect()
}

#[cfg(feature = "redis-module-timeseries")]
fn parse_ts_label_filter(raw: &[u8]) -> Option<TsLabelFilter<'_>> {
    let eq_at = raw.iter().position(|byte| *byte == b'=')?;
    if eq_at == 0 {
        return None;
    }
    Some(TsLabelFilter {
        label: &raw[..eq_at],
        value: &raw[eq_at + 1..],
    })
}

#[cfg(feature = "redis-module-timeseries")]
fn ts_record_matches_filters(record: Option<&ModuleRecord>, filters: &[TsLabelFilter<'_>]) -> bool {
    if filters.is_empty() {
        return true;
    }
    let Some(record) = record else {
        return false;
    };
    filters.iter().all(|filter| {
        ts_record_label_value(record, filter.label).is_some_and(|value| value == filter.value)
    })
}

#[cfg(feature = "redis-module-timeseries")]
fn ts_record_label_value<'a>(record: &'a ModuleRecord, label: &[u8]) -> Option<&'a [u8]> {
    let labels_at = record
        .args
        .iter()
        .position(|arg| bytes_eq(arg, b"LABELS"))?;
    for pair in record.args[labels_at + 1..].chunks_exact(2) {
        if pair[0].as_slice() == label {
            return Some(pair[1].as_slice());
        }
    }
    None
}

#[cfg(feature = "redis-module-timeseries")]
fn write_timeseries_range_series<W: TimeSeriesMultiRangeWriter>(
    writer: &mut W,
    key: &[u8],
    series: &BTreeMap<i64, TimeSeriesSample>,
    start: i64,
    end: i64,
    reverse: bool,
) {
    let samples = ts_range_len(series, start, end);
    writer.begin_series(key, samples);
    if reverse {
        for (_timestamp, sample) in series.range(start..=end).rev() {
            writer.sample_encoded(&sample.encoded_resp);
        }
    } else {
        for (_timestamp, sample) in series.range(start..=end) {
            writer.sample_encoded(&sample.encoded_resp);
        }
    }
}

#[cfg(feature = "redis-module-timeseries")]
fn parse_ts_bound(raw: &[u8], fallback: i64) -> i64 {
    match raw {
        b"-" => i64::MIN,
        b"+" => i64::MAX,
        raw => parse_i64_lossy(raw).unwrap_or(fallback),
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
fn ts_sample(timestamp: i64, value: f64) -> TimeSeriesSample {
    let raw = value.to_string().into_bytes();
    ts_sample_from_raw(timestamp, value, &raw)
}

#[cfg(feature = "redis-module-timeseries")]
fn ts_sample_from_raw(timestamp: i64, value: f64, raw: &[u8]) -> TimeSeriesSample {
    TimeSeriesSample {
        value,
        raw: raw.to_vec(),
        encoded_resp: encode_ts_resp_sample(timestamp, raw),
    }
}

#[cfg(feature = "redis-module-timeseries")]
fn encode_ts_resp_sample(timestamp: i64, value: &[u8]) -> Bytes {
    let timestamp = timestamp.to_string();
    let value_len = value.len().to_string();
    let mut out = Vec::with_capacity(12 + timestamp.len() + value_len.len() + value.len());
    out.extend_from_slice(b"*2\r\n:");
    out.extend_from_slice(timestamp.as_bytes());
    out.extend_from_slice(b"\r\n$");
    out.extend_from_slice(value_len.as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(value);
    out.extend_from_slice(b"\r\n");
    out
}

#[cfg(feature = "redis-module-timeseries")]
fn ts_range_len(series: &BTreeMap<i64, TimeSeriesSample>, start: i64, end: i64) -> usize {
    match (series.first_key_value(), series.last_key_value()) {
        (Some((first, _)), Some((last, _))) if start <= *first && *last <= end => series.len(),
        _ => series.range(start..=end).count(),
    }
}
