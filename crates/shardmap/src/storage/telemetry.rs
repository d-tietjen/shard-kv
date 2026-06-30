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
    Counter, CounterSet, DynamicCounter, DynamicCounterSeries, DynamicGaugeI64,
    DynamicGaugeI64Series, ExportMetrics, Histogram, MetricScope, RegisteredMetrics, Runtime,
    RuntimeConfig,
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
/// The struct is intentionally flat so `fast-telemetry` can derive Prometheus
/// and DogStatsD exporters directly from it.
#[derive(ExportMetrics)]
#[metric_prefix = "shardmap"]
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
    runtime: Arc<TelemetryRuntime>,
    metrics: RegisteredMetrics<CacheMetrics>,
    aggregate_counters: CounterSet,
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
            aggregate_counters: CounterSet::new(metric_shards, AGGREGATE_COUNTER_COUNT),
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
        self.aggregate_counters.sum(index).max(0) as u64
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
            self.aggregate_counters.add_index_values(&[
                (AGG_GETS, 1),
                (AGG_HITS, 1),
                (AGG_BYTES_READ, value_len as isize),
            ]);
        } else {
            self.aggregate_counters
                .add_index_values(&[(AGG_GETS, 1), (AGG_MISSES, 1)]);
        }
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
        if let Some(latency_ns) = latency_ns {
            self.metrics.get_latency_ns.record(latency_ns);
        }
    }

    #[inline(always)]
    pub fn record_set(&self, shard_id: usize, value_len: usize, latency_ns: Option<u64>) {
        self.aggregate_counters
            .add_index_values(&[(AGG_SETS, 1), (AGG_BYTES_WRITTEN, value_len as isize)]);
        self.metrics.sets.inc();
        if let Some(series) = self.shard_ops.get(shard_id) {
            series.set.inc();
        }
        self.metrics.bytes_written.add(value_len as isize);
        if let Some(latency_ns) = latency_ns {
            self.metrics.set_latency_ns.record(latency_ns);
        }
    }

    #[inline(always)]
    pub fn record_delete(&self, shard_id: usize) {
        self.aggregate_counters.inc(AGG_DELETES);
        self.metrics.deletes.inc();
        if let Some(series) = self.shard_ops.get(shard_id) {
            series.delete.inc();
        }
    }

    #[inline(always)]
    pub fn record_batch_get(&self, latency_ns: u64) {
        self.aggregate_counters.inc(AGG_BATCH_GETS);
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
            self.aggregate_counters.add(AGG_EXPIRATIONS, count as isize);
            self.metrics.expirations.add(count as isize);
        }
    }

    #[inline(always)]
    pub fn record_wal_append(&self, bytes: usize) {
        self.aggregate_counters
            .add_index_values(&[(AGG_WAL_WRITES, 1), (AGG_WAL_BYTES, bytes as isize)]);
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
