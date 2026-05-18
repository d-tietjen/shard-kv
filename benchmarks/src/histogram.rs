use hdrhistogram::Histogram;

pub struct LatencyHistogram {
    h: Histogram<u64>,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHistogram {
    pub fn new() -> Self {
        // 1 ns .. ~1 s, three significant digits.
        Self {
            h: Histogram::<u64>::new_with_bounds(1, 1_000_000_000, 3).expect("histogram bounds"),
        }
    }

    pub fn record(&mut self, latency_ns: u64) {
        let v = latency_ns.clamp(1, 1_000_000_000);
        self.h.record(v).ok();
    }

    pub fn merge(&mut self, other: &LatencyHistogram) {
        self.h.add(&other.h).ok();
    }

    pub fn count(&self) -> u64 {
        self.h.len()
    }

    pub fn p50_ns(&self) -> u64 {
        self.h.value_at_quantile(0.5)
    }
    pub fn p99_ns(&self) -> u64 {
        self.h.value_at_quantile(0.99)
    }
    pub fn p999_ns(&self) -> u64 {
        self.h.value_at_quantile(0.999)
    }
}

pub fn format_ns(ns: u64) -> String {
    if ns < 1_000 {
        format!("{ns}ns")
    } else if ns < 1_000_000 {
        format!("{:.1}us", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.1}ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.2}s", ns as f64 / 1_000_000_000.0)
    }
}
