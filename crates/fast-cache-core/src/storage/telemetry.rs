//! Feature-gated operational telemetry for fast-cache.
//!
//! The hot-path integration deliberately keeps metrics collection separate from
//! storage ownership. Stores receive an optional shared telemetry handle; when
//! the `telemetry` feature is disabled, all of this code is compiled out.

use std::sync::Arc;

use fast_telemetry::{
    Counter, DynamicCounter, DynamicCounterSeries, DynamicGaugeI64, DynamicGaugeI64Series,
    ExportMetrics, Histogram,
};
use serde::Serialize;

const LATENCY_NS_BUCKETS: &[u64] = &[
    50,
    100,
    250,
    500,
    1_000,
    2_500,
    5_000,
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    5_000_000,
    10_000_000,
    50_000_000,
    100_000_000,
];

#[derive(Debug, Clone, Serialize)]
pub struct HistogramSummary {
    pub count: u64,
    pub sum: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShardOpMetricSnapshot {
    pub shard_id: usize,
    pub op: String,
    pub value: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShardGaugeMetricSnapshot {
    pub shard_id: usize,
    pub value: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CacheMetricsSnapshot {
    pub gets: u64,
    pub sets: u64,
    pub deletes: u64,
    pub batch_gets: u64,
    pub hits: u64,
    pub misses: u64,
    pub miss_rate: f64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub get_latency_ns: HistogramSummary,
    pub set_latency_ns: HistogramSummary,
    pub batch_get_latency_ns: HistogramSummary,
    pub keys_total: i64,
    pub memory_bytes: i64,
    pub expirations: u64,
    pub wal_writes: u64,
    pub wal_bytes: u64,
    pub wal_flush_latency_ns: HistogramSummary,
    pub shard_ops: Vec<ShardOpMetricSnapshot>,
    pub shard_keys: Vec<ShardGaugeMetricSnapshot>,
}

/// Exported operational metrics for fast-cache.
///
/// The struct is intentionally flat so `fast-telemetry` can derive Prometheus
/// and DogStatsD exporters directly from it.
#[derive(ExportMetrics)]
#[metric_prefix = "fast_cache"]
pub struct CacheMetrics {
    #[help = "Total point lookups served by the flat store"]
    pub gets: Counter,

    #[help = "Total write operations applied to the flat store"]
    pub sets: Counter,

    #[help = "Total delete operations applied to the flat store"]
    pub deletes: Counter,

    #[help = "Total batch retrieval operations served by the embedded adapter"]
    pub batch_gets: Counter,

    #[help = "Total successful key lookups"]
    pub hits: Counter,

    #[help = "Total failed key lookups"]
    pub misses: Counter,

    #[help = "Total payload bytes returned to readers"]
    pub bytes_read: Counter,

    #[help = "Total payload bytes accepted on writes"]
    pub bytes_written: Counter,

    #[help = "Flat store get latency in nanoseconds"]
    pub get_latency_ns: Histogram,

    #[help = "Flat store set latency in nanoseconds"]
    pub set_latency_ns: Histogram,

    #[help = "Batch retrieval latency in nanoseconds"]
    pub batch_get_latency_ns: Histogram,

    #[help = "Current total key count across all shards"]
    pub keys_total: DynamicGaugeI64,

    #[help = "Current total resident key and value bytes across all shards"]
    pub memory_bytes: DynamicGaugeI64,

    #[help = "Total expirations processed by lazy lookup or maintenance sweeps"]
    pub expirations: Counter,

    #[help = "Total WAL entries appended"]
    pub wal_writes: Counter,

    #[help = "Total encoded WAL bytes written"]
    pub wal_bytes: Counter,

    #[help = "WAL flush latency in nanoseconds"]
    pub wal_flush_latency_ns: Histogram,

    #[help = "Per-shard operation counts"]
    pub shard_ops: DynamicCounter,

    #[help = "Per-shard key counts"]
    pub shard_keys: DynamicGaugeI64,
}

impl CacheMetrics {
    pub fn new(metric_shards: usize) -> Self {
        let metric_shards = metric_shards.max(1);
        Self {
            gets: Counter::new(metric_shards),
            sets: Counter::new(metric_shards),
            deletes: Counter::new(metric_shards),
            batch_gets: Counter::new(metric_shards),
            hits: Counter::new(metric_shards),
            misses: Counter::new(metric_shards),
            bytes_read: Counter::new(metric_shards),
            bytes_written: Counter::new(metric_shards),
            get_latency_ns: Histogram::new(LATENCY_NS_BUCKETS, metric_shards),
            set_latency_ns: Histogram::new(LATENCY_NS_BUCKETS, metric_shards),
            batch_get_latency_ns: Histogram::new(LATENCY_NS_BUCKETS, metric_shards),
            keys_total: DynamicGaugeI64::with_max_series(metric_shards, metric_shards.max(1) * 8),
            memory_bytes: DynamicGaugeI64::with_max_series(metric_shards, metric_shards.max(1) * 8),
            expirations: Counter::new(metric_shards),
            wal_writes: Counter::new(metric_shards),
            wal_bytes: Counter::new(metric_shards),
            wal_flush_latency_ns: Histogram::new(LATENCY_NS_BUCKETS, metric_shards),
            shard_ops: DynamicCounter::with_max_series(metric_shards, metric_shards.max(1) * 32),
            shard_keys: DynamicGaugeI64::with_max_series(metric_shards, metric_shards.max(1) * 8),
        }
    }
}

struct ShardOperationSeries {
    get: DynamicCounterSeries,
    set: DynamicCounterSeries,
    delete: DynamicCounterSeries,
    batch_get: DynamicCounterSeries,
}

/// Runtime helper around the exported metrics struct.
///
/// `CacheMetrics` owns only exporter-visible metric primitives. This wrapper
/// holds pre-resolved dynamic series handles and aggregate totals so the store
/// hot paths do not rebuild label sets on every update.
pub struct CacheTelemetry {
    metrics: CacheMetrics,
    shard_ops: Vec<ShardOperationSeries>,
    shard_keys_total: Vec<DynamicGaugeI64Series>,
    shard_memory_bytes: Vec<DynamicGaugeI64Series>,
    shard_keys: Vec<DynamicGaugeI64Series>,
}

impl std::fmt::Debug for CacheTelemetry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheTelemetry")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

/// Copyable hot-path handle into a shared telemetry owner.
///
/// `fast-telemetry` already keeps the actual metric state inside its own
/// sharded atomics, so storage hot paths do not need to carry `Arc` handles.
/// The runtime keeps ownership of the backing `CacheTelemetry`; worker code and
/// shard maps only keep this pointer-like handle.
#[derive(Debug, Clone)]
pub struct CacheTelemetryHandle {
    inner: Arc<CacheTelemetry>,
}

impl CacheTelemetryHandle {
    #[inline(always)]
    pub fn from_arc(metrics: &Arc<CacheTelemetry>) -> Self {
        Self {
            inner: Arc::clone(metrics),
        }
    }

    #[inline(always)]
    fn get(&self) -> &CacheTelemetry {
        self.inner.as_ref()
    }

    #[inline(always)]
    pub fn record_get(&self, shard_id: usize, hit: bool, value_len: usize, latency_ns: u64) {
        self.get().record_get(shard_id, hit, value_len, latency_ns);
    }

    #[inline(always)]
    pub fn record_set(&self, shard_id: usize, value_len: usize, latency_ns: u64) {
        self.get().record_set(shard_id, value_len, latency_ns);
    }

    #[inline(always)]
    pub fn record_delete(&self, shard_id: usize) {
        self.get().record_delete(shard_id);
    }

    #[inline(always)]
    pub fn record_batch_get(&self, latency_ns: u64) {
        self.get().record_batch_get(latency_ns);
    }

    #[inline(always)]
    pub fn record_batch_get_shard(&self, shard_id: usize) {
        self.get().record_batch_get_shard(shard_id);
    }

    #[inline(always)]
    pub fn record_expiration(&self, count: usize) {
        self.get().record_expiration(count);
    }

    #[inline(always)]
    pub fn record_wal_append(&self, bytes: usize) {
        self.get().record_wal_append(bytes);
    }

    #[inline(always)]
    pub fn record_wal_flush(&self, latency_ns: u64) {
        self.get().record_wal_flush(latency_ns);
    }

    #[inline(always)]
    pub fn adjust_keys_total(&self, shard_id: usize, delta: isize) {
        self.get().adjust_keys_total(shard_id, delta);
    }

    #[inline(always)]
    pub fn adjust_memory_bytes(&self, shard_id: usize, delta: isize) {
        self.get().adjust_memory_bytes(shard_id, delta);
    }

    #[inline(always)]
    pub fn set_shard_keys(&self, shard_id: usize, value: usize) {
        self.get().set_shard_keys(shard_id, value);
    }
}

impl CacheTelemetry {
    pub fn new(shard_count: usize) -> Arc<Self> {
        let metric_shards = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or_else(|_| shard_count.max(1));
        let metrics = CacheMetrics::new(metric_shards);
        let mut shard_ops = Vec::with_capacity(shard_count);
        let mut shard_keys_total = Vec::with_capacity(shard_count);
        let mut shard_memory_bytes = Vec::with_capacity(shard_count);
        let mut shard_keys = Vec::with_capacity(shard_count);

        for shard_id in 0..shard_count {
            let shard = shard_id.to_string();
            shard_ops.push(ShardOperationSeries {
                get: metrics
                    .shard_ops
                    .series(&[("op", "get"), ("shard", shard.as_str())]),
                set: metrics
                    .shard_ops
                    .series(&[("op", "set"), ("shard", shard.as_str())]),
                delete: metrics
                    .shard_ops
                    .series(&[("op", "delete"), ("shard", shard.as_str())]),
                batch_get: metrics
                    .shard_ops
                    .series(&[("op", "batch_get"), ("shard", shard.as_str())]),
            });
            shard_keys_total.push(metrics.keys_total.series(&[("shard", shard.as_str())]));
            shard_memory_bytes.push(metrics.memory_bytes.series(&[("shard", shard.as_str())]));
            shard_keys.push(metrics.shard_keys.series(&[("shard", shard.as_str())]));
        }

        Arc::new(Self {
            metrics,
            shard_ops,
            shard_keys_total,
            shard_memory_bytes,
            shard_keys,
        })
    }

    #[inline(always)]
    pub fn metrics(&self) -> &CacheMetrics {
        &self.metrics
    }

    #[inline(always)]
    pub fn record_get(&self, shard_id: usize, hit: bool, value_len: usize, latency_ns: u64) {
        self.metrics.gets.inc();
        if let Some(series) = self.shard_ops.get(shard_id) {
            series.get.inc();
        }
        if hit {
            self.metrics.hits.inc();
            self.metrics.bytes_read.add(value_len as isize);
        } else {
            self.metrics.misses.inc();
        }
        self.metrics.get_latency_ns.record(latency_ns);
    }

    #[inline(always)]
    pub fn record_set(&self, shard_id: usize, value_len: usize, latency_ns: u64) {
        self.metrics.sets.inc();
        if let Some(series) = self.shard_ops.get(shard_id) {
            series.set.inc();
        }
        self.metrics.bytes_written.add(value_len as isize);
        self.metrics.set_latency_ns.record(latency_ns);
    }

    #[inline(always)]
    pub fn record_delete(&self, shard_id: usize) {
        self.metrics.deletes.inc();
        if let Some(series) = self.shard_ops.get(shard_id) {
            series.delete.inc();
        }
    }

    #[inline(always)]
    pub fn record_batch_get(&self, latency_ns: u64) {
        self.metrics.batch_gets.inc();
        self.metrics.batch_get_latency_ns.record(latency_ns);
    }

    #[inline(always)]
    pub fn record_batch_get_shard(&self, shard_id: usize) {
        if let Some(series) = self.shard_ops.get(shard_id) {
            series.batch_get.inc();
        }
    }

    #[inline(always)]
    pub fn record_expiration(&self, count: usize) {
        if count > 0 {
            self.metrics.expirations.add(count as isize);
        }
    }

    #[inline(always)]
    pub fn record_wal_append(&self, bytes: usize) {
        self.metrics.wal_writes.inc();
        self.metrics.wal_bytes.add(bytes as isize);
    }

    #[inline(always)]
    pub fn record_wal_flush(&self, latency_ns: u64) {
        self.metrics.wal_flush_latency_ns.record(latency_ns);
    }

    #[inline(always)]
    pub fn adjust_keys_total(&self, shard_id: usize, delta: isize) {
        if delta == 0 {
            return;
        }
        if let Some(series) = self.shard_keys_total.get(shard_id) {
            series.add(delta as i64);
        }
    }

    #[inline(always)]
    pub fn adjust_memory_bytes(&self, shard_id: usize, delta: isize) {
        if delta == 0 {
            return;
        }
        if let Some(series) = self.shard_memory_bytes.get(shard_id) {
            series.add(delta as i64);
        }
    }

    #[inline(always)]
    pub fn set_shard_keys(&self, shard_id: usize, value: usize) {
        if let Some(series) = self.shard_keys.get(shard_id) {
            series.set(value as i64);
        }
    }

    pub fn export_prometheus(&self) -> String {
        let mut output = String::new();
        self.metrics.export_prometheus(&mut output);
        output
    }

    pub fn snapshot(&self) -> CacheMetricsSnapshot {
        let gets = self.metrics.gets.sum().max(0) as u64;
        let hits = self.metrics.hits.sum().max(0) as u64;
        let misses = self.metrics.misses.sum().max(0) as u64;
        CacheMetricsSnapshot {
            gets,
            sets: self.metrics.sets.sum().max(0) as u64,
            deletes: self.metrics.deletes.sum().max(0) as u64,
            batch_gets: self.metrics.batch_gets.sum().max(0) as u64,
            hits,
            misses,
            miss_rate: if gets == 0 {
                0.0
            } else {
                misses as f64 / gets as f64
            },
            bytes_read: self.metrics.bytes_read.sum().max(0) as u64,
            bytes_written: self.metrics.bytes_written.sum().max(0) as u64,
            get_latency_ns: HistogramSummary {
                count: self.metrics.get_latency_ns.count(),
                sum: self.metrics.get_latency_ns.sum(),
            },
            set_latency_ns: HistogramSummary {
                count: self.metrics.set_latency_ns.count(),
                sum: self.metrics.set_latency_ns.sum(),
            },
            batch_get_latency_ns: HistogramSummary {
                count: self.metrics.batch_get_latency_ns.count(),
                sum: self.metrics.batch_get_latency_ns.sum(),
            },
            keys_total: self
                .metrics
                .keys_total
                .snapshot()
                .into_iter()
                .map(|(_, value)| value)
                .sum(),
            memory_bytes: self
                .metrics
                .memory_bytes
                .snapshot()
                .into_iter()
                .map(|(_, value)| value)
                .sum(),
            expirations: self.metrics.expirations.sum().max(0) as u64,
            wal_writes: self.metrics.wal_writes.sum().max(0) as u64,
            wal_bytes: self.metrics.wal_bytes.sum().max(0) as u64,
            wal_flush_latency_ns: HistogramSummary {
                count: self.metrics.wal_flush_latency_ns.count(),
                sum: self.metrics.wal_flush_latency_ns.sum(),
            },
            shard_ops: self
                .metrics
                .shard_ops
                .snapshot()
                .into_iter()
                .filter_map(|(labels, value)| {
                    let shard_id = label_value(&labels, "shard")?.parse::<usize>().ok()?;
                    let op = label_value(&labels, "op")?.to_string();
                    Some(ShardOpMetricSnapshot {
                        shard_id,
                        op,
                        value: value.max(0) as u64,
                    })
                })
                .collect(),
            shard_keys: self
                .metrics
                .shard_keys
                .snapshot()
                .into_iter()
                .filter_map(|(labels, value)| {
                    let shard_id = label_value(&labels, "shard")?.parse::<usize>().ok()?;
                    Some(ShardGaugeMetricSnapshot { shard_id, value })
                })
                .collect(),
        }
    }
}

fn label_value<'a>(labels: &'a fast_telemetry::DynamicLabelSet, name: &str) -> Option<&'a str> {
    labels
        .pairs()
        .iter()
        .find_map(|(key, value)| (key == name).then_some(value.as_str()))
}
