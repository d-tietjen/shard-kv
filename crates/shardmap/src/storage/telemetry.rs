//! Feature-gated operational telemetry for shardcache.
//!
//! The hot-path integration deliberately keeps metrics collection separate from
//! storage ownership. Stores receive an optional shared telemetry handle; when
//! the `telemetry` feature is disabled, all of this code is compiled out.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use fast_telemetry::{
    CounterSet, DynamicCounter, DynamicCounterSeries, DynamicGaugeI64, DynamicGaugeI64Series,
    ExportMetrics, Histogram, MetricKind, MetricLabels, MetricMeta, MetricScope, MetricVisitor,
    PrometheusExport, RegisteredMetrics, Runtime, RuntimeConfig,
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

const LATENCY_NS_PER_MICROSECOND: u64 = 1_000;
const DEFAULT_SHARED_CLOCK_UPDATE_INTERVAL: Duration = Duration::from_micros(1);
const SHARDMAP_METRIC_SCOPE: &str = "shardmap";
const AGGREGATE_COUNTER_COUNT: usize = 11;
const AGG_GETS: usize = 0;
const AGG_SETS: usize = 1;
const AGG_DELETES: usize = 2;
const AGG_BATCH_GETS: usize = 3;
const AGG_HITS: usize = 4;
const AGG_MISSES: usize = 5;
const AGG_BYTES_READ: usize = 6;
const AGG_BYTES_WRITTEN: usize = 7;
const AGG_EXPIRATIONS: usize = 8;
const AGG_WAL_WRITES: usize = 9;
const AGG_WAL_BYTES: usize = 10;

struct AggregateCounterMetric {
    index: usize,
    name: &'static str,
    help: &'static str,
}

const AGGREGATE_COUNTER_METRICS: &[AggregateCounterMetric] = &[
    AggregateCounterMetric {
        index: AGG_GETS,
        name: "shardmap_gets",
        help: "Total point lookups served by the flat store",
    },
    AggregateCounterMetric {
        index: AGG_SETS,
        name: "shardmap_sets",
        help: "Total write operations applied to the flat store",
    },
    AggregateCounterMetric {
        index: AGG_DELETES,
        name: "shardmap_deletes",
        help: "Total delete operations applied to the flat store",
    },
    AggregateCounterMetric {
        index: AGG_BATCH_GETS,
        name: "shardmap_batch_gets",
        help: "Total batch retrieval operations served by the embedded adapter",
    },
    AggregateCounterMetric {
        index: AGG_HITS,
        name: "shardmap_hits",
        help: "Total successful key lookups",
    },
    AggregateCounterMetric {
        index: AGG_MISSES,
        name: "shardmap_misses",
        help: "Total failed key lookups",
    },
    AggregateCounterMetric {
        index: AGG_BYTES_READ,
        name: "shardmap_bytes_read",
        help: "Total payload bytes returned to readers",
    },
    AggregateCounterMetric {
        index: AGG_BYTES_WRITTEN,
        name: "shardmap_bytes_written",
        help: "Total payload bytes accepted on writes",
    },
    AggregateCounterMetric {
        index: AGG_EXPIRATIONS,
        name: "shardmap_expirations",
        help: "Total expirations processed by lazy lookup or maintenance sweeps",
    },
    AggregateCounterMetric {
        index: AGG_WAL_WRITES,
        name: "shardmap_wal_writes",
        help: "Total WAL entries appended",
    },
    AggregateCounterMetric {
        index: AGG_WAL_BYTES,
        name: "shardmap_wal_bytes",
        help: "Total encoded WAL bytes written",
    },
];

static SHARED_LATENCY_CLOCK: OnceLock<Arc<SharedLatencyClock>> = OnceLock::new();

pub type TelemetryRuntime = Runtime;
pub type TelemetryRuntimeConfig = RuntimeConfig;

struct SharedLatencyClock {
    started_at: Instant,
    now_us: AtomicU64,
}

impl SharedLatencyClock {
    fn start(update_interval: Duration) -> Arc<Self> {
        let update_interval = normalize_shared_clock_interval(update_interval);
        let clock = Arc::new(Self {
            started_at: Instant::now(),
            now_us: AtomicU64::new(0),
        });
        let updater = Arc::clone(&clock);
        let _ = thread::Builder::new()
            .name("shardmap-telemetry-clock".to_owned())
            .spawn(move || {
                loop {
                    updater.refresh();
                    thread::sleep(update_interval);
                }
            });
        clock
    }

    #[inline(always)]
    fn now_us(&self) -> u64 {
        self.now_us.load(Ordering::Relaxed)
    }

    #[inline]
    fn refresh(&self) {
        self.now_us
            .store(elapsed_micros(self.started_at), Ordering::Relaxed);
    }
}

fn shared_latency_clock() -> Arc<SharedLatencyClock> {
    Arc::clone(
        SHARED_LATENCY_CLOCK
            .get_or_init(|| SharedLatencyClock::start(DEFAULT_SHARED_CLOCK_UPDATE_INTERVAL)),
    )
}

fn shared_latency_clock_with_interval(update_interval: Duration) -> Arc<SharedLatencyClock> {
    let update_interval = normalize_shared_clock_interval(update_interval);
    if update_interval == DEFAULT_SHARED_CLOCK_UPDATE_INTERVAL {
        shared_latency_clock()
    } else {
        SharedLatencyClock::start(update_interval)
    }
}

fn normalize_shared_clock_interval(update_interval: Duration) -> Duration {
    update_interval.max(DEFAULT_SHARED_CLOCK_UPDATE_INTERVAL)
}

fn elapsed_micros(started_at: Instant) -> u64 {
    let micros = started_at.elapsed().as_micros();
    micros.min(u128::from(u64::MAX)) as u64
}

/// Clock source used for sampled cache latency histograms.
///
/// `Instant` takes a timestamp for each sampled operation. `SharedMicroseconds`
/// reads a process-wide microsecond clock maintained by a background thread
/// that updates every 1 microsecond. `SharedMicrosecondsWithInterval` uses the
/// same low-overhead hot-path reads with a caller-selected update interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheTelemetryClock {
    /// Use `Instant::now()` at operation start and elapsed time at record time.
    Instant,
    /// Use a shared process-wide clock updated every 1 microsecond.
    SharedMicroseconds,
    /// Use a shared clock with a custom update interval.
    ///
    /// Zero and sub-microsecond intervals are normalized to 1 microsecond.
    SharedMicrosecondsWithInterval(Duration),
}

enum LatencyClock {
    Instant,
    SharedMicroseconds(Arc<SharedLatencyClock>),
}

pub(crate) enum LatencySampleStart {
    Instant(Instant),
    SharedMicroseconds(u64),
}

impl LatencyClock {
    fn new(mode: CacheTelemetryClock) -> Self {
        match mode {
            CacheTelemetryClock::Instant => Self::Instant,
            CacheTelemetryClock::SharedMicroseconds => {
                Self::SharedMicroseconds(shared_latency_clock())
            }
            CacheTelemetryClock::SharedMicrosecondsWithInterval(update_interval) => {
                Self::SharedMicroseconds(shared_latency_clock_with_interval(update_interval))
            }
        }
    }

    #[inline(always)]
    fn start(&self) -> LatencySampleStart {
        match self {
            Self::Instant => LatencySampleStart::Instant(Instant::now()),
            Self::SharedMicroseconds(clock) => {
                LatencySampleStart::SharedMicroseconds(clock.now_us())
            }
        }
    }

    #[inline(always)]
    fn elapsed_ns_since(&self, start: LatencySampleStart) -> u64 {
        match (self, start) {
            (Self::Instant, LatencySampleStart::Instant(start)) => {
                start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
            }
            (Self::SharedMicroseconds(clock), LatencySampleStart::SharedMicroseconds(start_us)) => {
                clock
                    .now_us()
                    .saturating_sub(start_us)
                    .saturating_mul(LATENCY_NS_PER_MICROSECOND)
            }
            (Self::Instant, LatencySampleStart::SharedMicroseconds(_))
            | (Self::SharedMicroseconds(_), LatencySampleStart::Instant(_)) => 0,
        }
    }
}

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

/// Exported operational metrics for shardcache.
///
/// Aggregate counters are stored in one `CounterSet` so a cache operation can
/// update related totals against the same thread-local shard row. The remaining
/// metric primitives stay directly accessible for latency histograms and
/// runtime-labelled per-shard series.
pub struct CacheMetrics {
    aggregate_counters: CounterSet,
    pub get_latency_ns: Histogram,
    pub set_latency_ns: Histogram,
    pub batch_get_latency_ns: Histogram,
    pub keys_total: DynamicGaugeI64,
    pub memory_bytes: DynamicGaugeI64,
    pub wal_flush_latency_ns: Histogram,
    pub shard_ops: DynamicCounter,
    pub shard_keys: DynamicGaugeI64,
}

impl CacheMetrics {
    pub fn new(metric_shards: usize) -> Self {
        let metric_shards = metric_shards.max(1);
        Self {
            aggregate_counters: CounterSet::new(metric_shards, AGGREGATE_COUNTER_COUNT),
            get_latency_ns: Histogram::new(LATENCY_NS_BUCKETS, metric_shards),
            set_latency_ns: Histogram::new(LATENCY_NS_BUCKETS, metric_shards),
            batch_get_latency_ns: Histogram::new(LATENCY_NS_BUCKETS, metric_shards),
            keys_total: DynamicGaugeI64::with_max_series(metric_shards, metric_shards.max(1) * 8),
            memory_bytes: DynamicGaugeI64::with_max_series(metric_shards, metric_shards.max(1) * 8),
            wal_flush_latency_ns: Histogram::new(LATENCY_NS_BUCKETS, metric_shards),
            shard_ops: DynamicCounter::with_max_series(metric_shards, metric_shards.max(1) * 32),
            shard_keys: DynamicGaugeI64::with_max_series(metric_shards, metric_shards.max(1) * 8),
        }
    }

    #[inline(always)]
    fn aggregate_counter(&self, index: usize) -> u64 {
        self.aggregate_counters.sum(index).max(0) as u64
    }

    pub fn export_prometheus(&self, output: &mut String) {
        for metric in AGGREGATE_COUNTER_METRICS {
            write_prometheus_counter(
                output,
                metric.name,
                metric.help,
                self.aggregate_counter(metric.index),
            );
        }
        self.get_latency_ns.export_prometheus(
            output,
            "shardmap_get_latency_ns",
            "Flat store get latency in nanoseconds",
        );
        self.set_latency_ns.export_prometheus(
            output,
            "shardmap_set_latency_ns",
            "Flat store set latency in nanoseconds",
        );
        self.batch_get_latency_ns.export_prometheus(
            output,
            "shardmap_batch_get_latency_ns",
            "Batch retrieval latency in nanoseconds",
        );
        self.keys_total.export_prometheus(
            output,
            "shardmap_keys_total",
            "Current total key count across all shards",
        );
        self.memory_bytes.export_prometheus(
            output,
            "shardmap_memory_bytes",
            "Current total resident key and value bytes across all shards",
        );
        self.wal_flush_latency_ns.export_prometheus(
            output,
            "shardmap_wal_flush_latency_ns",
            "WAL flush latency in nanoseconds",
        );
        self.shard_ops.export_prometheus(
            output,
            "shardmap_shard_ops",
            "Per-shard operation counts",
        );
        self.shard_keys
            .export_prometheus(output, "shardmap_shard_keys", "Per-shard key counts");
    }
}

impl ExportMetrics for CacheMetrics {
    fn visit_metrics<V: MetricVisitor + ?Sized>(&self, visitor: &mut V) {
        for metric in AGGREGATE_COUNTER_METRICS {
            visitor.counter(
                metric_meta(metric.name, metric.help, MetricKind::Counter),
                MetricLabels::none(),
                self.aggregate_counter(metric.index) as i64,
            );
        }

        visitor.histogram(
            metric_meta(
                "shardmap_get_latency_ns",
                "Flat store get latency in nanoseconds",
                MetricKind::Histogram,
            ),
            MetricLabels::none(),
            &self.get_latency_ns,
        );
        visitor.histogram(
            metric_meta(
                "shardmap_set_latency_ns",
                "Flat store set latency in nanoseconds",
                MetricKind::Histogram,
            ),
            MetricLabels::none(),
            &self.set_latency_ns,
        );
        visitor.histogram(
            metric_meta(
                "shardmap_batch_get_latency_ns",
                "Batch retrieval latency in nanoseconds",
                MetricKind::Histogram,
            ),
            MetricLabels::none(),
            &self.batch_get_latency_ns,
        );

        visit_dynamic_gauge_i64(
            visitor,
            &self.keys_total,
            "shardmap_keys_total",
            "Current total key count across all shards",
        );
        visit_dynamic_gauge_i64(
            visitor,
            &self.memory_bytes,
            "shardmap_memory_bytes",
            "Current total resident key and value bytes across all shards",
        );

        visitor.histogram(
            metric_meta(
                "shardmap_wal_flush_latency_ns",
                "WAL flush latency in nanoseconds",
                MetricKind::Histogram,
            ),
            MetricLabels::none(),
            &self.wal_flush_latency_ns,
        );
        visit_dynamic_counter(
            visitor,
            &self.shard_ops,
            "shardmap_shard_ops",
            "Per-shard operation counts",
        );
        visit_dynamic_gauge_i64(
            visitor,
            &self.shard_keys,
            "shardmap_shard_keys",
            "Per-shard key counts",
        );
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
/// holds pre-resolved dynamic series handles so the store hot paths do not
/// rebuild label sets on every update.
pub struct CacheTelemetry {
    runtime: Arc<TelemetryRuntime>,
    metrics: RegisteredMetrics<CacheMetrics>,
    shard_ops: Vec<ShardOperationSeries>,
    shard_keys_total: Vec<DynamicGaugeI64Series>,
    shard_memory_bytes: Vec<DynamicGaugeI64Series>,
    shard_keys: Vec<DynamicGaugeI64Series>,
    latency_clock: LatencyClock,
    latency_sample_mask: u64,
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
    pub fn latency_sample_mask(&self) -> u64 {
        self.get().latency_sample_mask
    }

    #[inline(always)]
    pub(crate) fn start_latency_sample(&self) -> LatencySampleStart {
        self.get().start_latency_sample()
    }

    #[inline(always)]
    pub(crate) fn latency_elapsed_ns_since(&self, start: LatencySampleStart) -> u64 {
        self.get().latency_elapsed_ns_since(start)
    }

    #[inline(always)]
    pub fn record_get(
        &self,
        shard_id: usize,
        hit: bool,
        value_len: usize,
        latency_ns: Option<u64>,
    ) {
        self.get().record_get(shard_id, hit, value_len, latency_ns);
    }

    #[inline(always)]
    pub fn record_set(&self, shard_id: usize, value_len: usize, latency_ns: Option<u64>) {
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
    const DEFAULT_LATENCY_SAMPLE_RATE: u64 = 1024;

    pub fn new(shard_count: usize) -> Arc<Self> {
        Self::new_with_latency_sample_rate(shard_count, Self::DEFAULT_LATENCY_SAMPLE_RATE)
    }

    pub fn new_with_runtime(
        shard_count: usize,
        runtime: Option<Arc<TelemetryRuntime>>,
    ) -> Arc<Self> {
        Self::new_with_runtime_latency_sample_rate_and_clock(
            shard_count,
            runtime,
            Self::DEFAULT_LATENCY_SAMPLE_RATE,
            CacheTelemetryClock::SharedMicroseconds,
        )
    }

    /// Creates telemetry with a configurable latency histogram sample rate.
    ///
    /// Counters, byte totals, key gauges, and memory gauges are still updated on
    /// every operation. Latency histograms are sampled because taking a
    /// timestamp for every cache operation is expensive on the hot path.
    /// `latency_sample_rate = 1` records every operation.
    pub fn new_with_latency_sample_rate(shard_count: usize, latency_sample_rate: u64) -> Arc<Self> {
        Self::new_with_runtime_latency_sample_rate(shard_count, None, latency_sample_rate)
    }

    pub fn new_with_runtime_latency_sample_rate(
        shard_count: usize,
        runtime: Option<Arc<TelemetryRuntime>>,
        latency_sample_rate: u64,
    ) -> Arc<Self> {
        Self::build_with_runtime_latency_sample_rate_and_clock(
            shard_count,
            runtime,
            latency_sample_rate,
            CacheTelemetryClock::SharedMicroseconds,
        )
    }

    pub fn new_with_runtime_latency_sample_rate_and_clock(
        shard_count: usize,
        runtime: Option<Arc<TelemetryRuntime>>,
        latency_sample_rate: u64,
        clock: CacheTelemetryClock,
    ) -> Arc<Self> {
        Self::build_with_runtime_latency_sample_rate_and_clock(
            shard_count,
            runtime,
            latency_sample_rate,
            clock,
        )
    }

    /// Create telemetry with an explicit latency sample rate and clock source.
    ///
    /// Use `CacheTelemetryClock::SharedMicroseconds` for low-overhead full-rate
    /// latency sampling with the default 1 microsecond clock interval,
    /// `CacheTelemetryClock::SharedMicrosecondsWithInterval` to trade timing
    /// precision for fewer clock-thread wakeups, or `CacheTelemetryClock::Instant`
    /// when each sample should be measured directly from `Instant::now()`.
    pub fn new_with_latency_sample_rate_and_clock(
        shard_count: usize,
        latency_sample_rate: u64,
        clock: CacheTelemetryClock,
    ) -> Arc<Self> {
        Self::build_with_runtime_latency_sample_rate_and_clock(
            shard_count,
            None,
            latency_sample_rate,
            clock,
        )
    }

    fn build_with_runtime_latency_sample_rate_and_clock(
        shard_count: usize,
        runtime: Option<Arc<TelemetryRuntime>>,
        latency_sample_rate: u64,
        clock: CacheTelemetryClock,
    ) -> Arc<Self> {
        let metric_shards = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or_else(|_| shard_count.max(1));
        let runtime = runtime.unwrap_or_else(|| TelemetryRuntime::new(RuntimeConfig::default()));
        let metrics = runtime.register_metrics(
            MetricScope::from(SHARDMAP_METRIC_SCOPE),
            CacheMetrics::new(metric_shards),
        );
        let mut shard_ops = Vec::with_capacity(shard_count);
        let mut shard_keys_total = Vec::with_capacity(shard_count);
        let mut shard_memory_bytes = Vec::with_capacity(shard_count);
        let mut shard_keys = Vec::with_capacity(shard_count);
        let metric_handles = metrics.metrics();

        for shard_id in 0..shard_count {
            let shard = shard_id.to_string();
            shard_ops.push(ShardOperationSeries {
                get: metric_handles
                    .shard_ops
                    .series(&[("op", "get"), ("shard", shard.as_str())]),
                set: metric_handles
                    .shard_ops
                    .series(&[("op", "set"), ("shard", shard.as_str())]),
                delete: metric_handles
                    .shard_ops
                    .series(&[("op", "delete"), ("shard", shard.as_str())]),
                batch_get: metric_handles
                    .shard_ops
                    .series(&[("op", "batch_get"), ("shard", shard.as_str())]),
            });
            shard_keys_total.push(
                metric_handles
                    .keys_total
                    .series(&[("shard", shard.as_str())]),
            );
            shard_memory_bytes.push(
                metric_handles
                    .memory_bytes
                    .series(&[("shard", shard.as_str())]),
            );
            shard_keys.push(
                metric_handles
                    .shard_keys
                    .series(&[("shard", shard.as_str())]),
            );
        }

        let latency_sample_rate = latency_sample_rate
            .max(1)
            .checked_next_power_of_two()
            .unwrap_or(1 << 63);
        Arc::new(Self {
            runtime,
            metrics,
            shard_ops,
            shard_keys_total,
            shard_memory_bytes,
            shard_keys,
            latency_clock: LatencyClock::new(clock),
            latency_sample_mask: latency_sample_rate - 1,
        })
    }

    #[inline(always)]
    pub fn runtime(&self) -> &Arc<TelemetryRuntime> {
        &self.runtime
    }

    #[inline(always)]
    pub fn metric_scope(&self) -> &MetricScope {
        self.metrics.scope()
    }

    #[inline(always)]
    pub fn metrics(&self) -> &CacheMetrics {
        self.metrics.metrics().as_ref()
    }

    #[inline(always)]
    fn aggregate_counter(&self, index: usize) -> u64 {
        self.metrics.aggregate_counter(index)
    }

    #[inline(always)]
    fn start_latency_sample(&self) -> LatencySampleStart {
        self.latency_clock.start()
    }

    #[inline(always)]
    fn latency_elapsed_ns_since(&self, start: LatencySampleStart) -> u64 {
        self.latency_clock.elapsed_ns_since(start)
    }

    #[inline(always)]
    pub fn record_get(
        &self,
        shard_id: usize,
        hit: bool,
        value_len: usize,
        latency_ns: Option<u64>,
    ) {
        if hit {
            self.metrics.aggregate_counters.add_index_values(&[
                (AGG_GETS, 1),
                (AGG_HITS, 1),
                (AGG_BYTES_READ, value_len as isize),
            ]);
        } else {
            self.metrics
                .aggregate_counters
                .add_index_values(&[(AGG_GETS, 1), (AGG_MISSES, 1)]);
        }
        if let Some(series) = self.shard_ops.get(shard_id) {
            series.get.inc();
        }
        if let Some(latency_ns) = latency_ns {
            self.metrics.get_latency_ns.record(latency_ns);
        }
    }

    #[inline(always)]
    pub fn record_set(&self, shard_id: usize, value_len: usize, latency_ns: Option<u64>) {
        self.metrics
            .aggregate_counters
            .add_index_values(&[(AGG_SETS, 1), (AGG_BYTES_WRITTEN, value_len as isize)]);
        if let Some(series) = self.shard_ops.get(shard_id) {
            series.set.inc();
        }
        if let Some(latency_ns) = latency_ns {
            self.metrics.set_latency_ns.record(latency_ns);
        }
    }

    #[inline(always)]
    pub fn record_delete(&self, shard_id: usize) {
        self.metrics.aggregate_counters.inc(AGG_DELETES);
        if let Some(series) = self.shard_ops.get(shard_id) {
            series.delete.inc();
        }
    }

    #[inline(always)]
    pub fn record_batch_get(&self, latency_ns: u64) {
        self.metrics.aggregate_counters.inc(AGG_BATCH_GETS);
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
            self.metrics
                .aggregate_counters
                .add(AGG_EXPIRATIONS, count as isize);
        }
    }

    #[inline(always)]
    pub fn record_wal_append(&self, bytes: usize) {
        self.metrics
            .aggregate_counters
            .add_index_values(&[(AGG_WAL_WRITES, 1), (AGG_WAL_BYTES, bytes as isize)]);
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
        let gets = self.aggregate_counter(AGG_GETS);
        let hits = self.aggregate_counter(AGG_HITS);
        let misses = self.aggregate_counter(AGG_MISSES);
        CacheMetricsSnapshot {
            gets,
            sets: self.aggregate_counter(AGG_SETS),
            deletes: self.aggregate_counter(AGG_DELETES),
            batch_gets: self.aggregate_counter(AGG_BATCH_GETS),
            hits,
            misses,
            miss_rate: if gets == 0 {
                0.0
            } else {
                misses as f64 / gets as f64
            },
            bytes_read: self.aggregate_counter(AGG_BYTES_READ),
            bytes_written: self.aggregate_counter(AGG_BYTES_WRITTEN),
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
            expirations: self.aggregate_counter(AGG_EXPIRATIONS),
            wal_writes: self.aggregate_counter(AGG_WAL_WRITES),
            wal_bytes: self.aggregate_counter(AGG_WAL_BYTES),
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

fn metric_meta(name: &'static str, help: &'static str, kind: MetricKind) -> MetricMeta<'static> {
    MetricMeta {
        name,
        help,
        kind,
        unit: None,
    }
}

fn visit_dynamic_counter<V: MetricVisitor + ?Sized>(
    visitor: &mut V,
    counter: &DynamicCounter,
    name: &'static str,
    help: &'static str,
) {
    let meta = metric_meta(name, help, MetricKind::Counter);
    let overflow = counter.overflow_count();
    if overflow > 0 {
        visitor.dynamic_overflow(meta, overflow);
    }
    counter.visit_series(|labels, current| {
        visitor.counter(meta, MetricLabels::dynamic_pairs(labels), current as i64);
    });
}

fn visit_dynamic_gauge_i64<V: MetricVisitor + ?Sized>(
    visitor: &mut V,
    gauge: &DynamicGaugeI64,
    name: &'static str,
    help: &'static str,
) {
    let meta = metric_meta(name, help, MetricKind::Gauge);
    let overflow = gauge.overflow_count();
    if overflow > 0 {
        visitor.dynamic_overflow(meta, overflow);
    }
    gauge.visit_series(|labels, current| {
        visitor.gauge_i64(meta, MetricLabels::dynamic_pairs(labels), current);
    });
}

fn write_prometheus_counter(output: &mut String, name: &str, help: &str, value: u64) {
    output.push_str("# HELP ");
    output.push_str(name);
    output.push(' ');
    output.push_str(help);
    output.push_str("\n# TYPE ");
    output.push_str(name);
    output.push_str(" counter\n");
    output.push_str(name);
    output.push(' ');
    output.push_str(&value.to_string());
    output.push('\n');
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct CounterVisitor {
        counters: BTreeMap<String, i64>,
    }

    impl MetricVisitor for CounterVisitor {
        fn counter(&mut self, meta: MetricMeta<'_>, labels: MetricLabels<'_>, value: i64) {
            if labels.iter().next().is_none() {
                self.counters.insert(meta.name.to_owned(), value);
            }
        }

        fn gauge_i64(&mut self, _meta: MetricMeta<'_>, _labels: MetricLabels<'_>, _value: i64) {}

        fn gauge_f64(&mut self, _meta: MetricMeta<'_>, _labels: MetricLabels<'_>, _value: f64) {}

        fn histogram(
            &mut self,
            _meta: MetricMeta<'_>,
            _labels: MetricLabels<'_>,
            _histogram: &dyn fast_telemetry::HistogramSnapshot,
        ) {
        }
    }

    #[test]
    fn shared_latency_clock_advances_in_microseconds() {
        let clock = shared_latency_clock();
        let start = clock.now_us();
        for _ in 0..100 {
            thread::sleep(Duration::from_millis(1));
            if clock.now_us() > start {
                return;
            }
        }
        panic!("shared latency clock did not advance");
    }

    #[test]
    fn shared_latency_clock_interval_is_configurable_and_clamped() {
        assert_eq!(
            normalize_shared_clock_interval(Duration::ZERO),
            DEFAULT_SHARED_CLOCK_UPDATE_INTERVAL
        );
        assert_eq!(
            normalize_shared_clock_interval(Duration::from_nanos(1)),
            DEFAULT_SHARED_CLOCK_UPDATE_INTERVAL
        );
        assert_eq!(
            normalize_shared_clock_interval(Duration::from_micros(10)),
            Duration::from_micros(10)
        );

        let custom = shared_latency_clock_with_interval(Duration::from_micros(10));
        let clamped = shared_latency_clock_with_interval(Duration::ZERO);
        let default = shared_latency_clock();

        assert!(!Arc::ptr_eq(&custom, &default));
        assert!(Arc::ptr_eq(&clamped, &default));
    }

    #[test]
    fn runtime_visitor_reads_grouped_aggregate_counters() {
        let metrics = CacheTelemetry::new_with_latency_sample_rate(1, 1);

        metrics.record_set(0, 5, None);
        metrics.record_get(0, true, 5, None);
        metrics.record_get(0, false, 0, None);

        let mut visitor = CounterVisitor::default();
        metrics
            .runtime()
            .visit_metrics_for_scope(metrics.metric_scope(), &mut visitor);

        assert_eq!(visitor.counters.get("shardmap_gets"), Some(&2));
        assert_eq!(visitor.counters.get("shardmap_sets"), Some(&1));
        assert_eq!(visitor.counters.get("shardmap_hits"), Some(&1));
        assert_eq!(visitor.counters.get("shardmap_misses"), Some(&1));
        assert_eq!(visitor.counters.get("shardmap_bytes_read"), Some(&5));
        assert_eq!(visitor.counters.get("shardmap_bytes_written"), Some(&5));
    }
}
