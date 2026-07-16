use crate::{HermeticRunReport, NodePerfReport};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterProfile {
    pub final_time_ms: u64,
    pub nodes: Vec<NodeProfile>,
}

impl ClusterProfile {
    pub fn from_run_report(report: &HermeticRunReport) -> Self {
        let final_time_ms = report
            .log
            .records
            .iter()
            .map(|record| record.delivered_at_ms)
            .max()
            .unwrap_or(0);
        Self::from_perf_report(final_time_ms, &report.perf.nodes)
    }

    pub fn from_perf_report(final_time_ms: u64, nodes: &[NodePerfReport]) -> Self {
        Self {
            final_time_ms,
            nodes: nodes
                .iter()
                .map(|node| NodeProfile::from_perf(final_time_ms, node))
                .collect(),
        }
    }

    pub fn total_handled_requests(&self) -> u64 {
        self.nodes
            .iter()
            .map(|node| node.handled_requests)
            .fold(0, u64::saturating_add)
    }

    pub fn total_simulated_cpu_ms(&self) -> u64 {
        self.nodes
            .iter()
            .map(|node| node.simulated_cpu_ms)
            .fold(0, u64::saturating_add)
    }

    pub fn total_simulated_cpu_wait_ms(&self) -> u64 {
        self.nodes
            .iter()
            .map(|node| node.simulated_cpu_wait_ms)
            .fold(0, u64::saturating_add)
    }

    pub fn total_observed_handler_nanos(&self) -> u128 {
        self.nodes
            .iter()
            .map(|node| node.observed_handler_nanos)
            .sum()
    }

    pub fn busiest_simulated_cpu_node(&self) -> Option<&NodeProfile> {
        self.nodes
            .iter()
            .max_by_key(|node| node.simulated_cpu_utilization_ppm)
    }

    pub fn busiest_observed_cpu_node(&self) -> Option<&NodeProfile> {
        self.nodes.iter().max_by_key(|node| node.observed_cpu_ppm)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeProfile {
    pub node: usize,
    pub delivered_requests: u64,
    pub handled_requests: u64,
    pub responses: u64,
    pub handler_errors: u64,
    pub dropped: u64,
    pub unavailable: u64,
    pub cpu_stalled: u64,
    pub hardware_faults: u64,
    pub simulated_cpu_ms: u64,
    pub simulated_cpu_wait_ms: u64,
    pub simulated_cpu_utilization_ppm: u64,
    pub observed_handler_nanos: u128,
    pub observed_handler_nanos_per_handled: u128,
    pub observed_cpu_ppm: u64,
}

impl NodeProfile {
    pub fn from_perf(final_time_ms: u64, perf: &NodePerfReport) -> Self {
        let observed_handler_nanos_per_handled = match perf.handled_requests {
            0 => 0,
            handled => perf.observed_handler_nanos / handled as u128,
        };
        Self {
            node: perf.node,
            delivered_requests: perf.delivered_requests,
            handled_requests: perf.handled_requests,
            responses: perf.responses,
            handler_errors: perf.handler_errors,
            dropped: perf.dropped,
            unavailable: perf.unavailable,
            cpu_stalled: perf.cpu_stalled,
            hardware_faults: perf.hardware_faults,
            simulated_cpu_ms: perf.simulated_cpu_ms,
            simulated_cpu_wait_ms: perf.simulated_cpu_wait_ms,
            simulated_cpu_utilization_ppm: utilization_ppm(
                perf.simulated_cpu_ms as u128,
                final_time_ms as u128,
            ),
            observed_handler_nanos: perf.observed_handler_nanos,
            observed_handler_nanos_per_handled,
            observed_cpu_ppm: utilization_ppm(
                perf.observed_handler_nanos,
                final_time_ms as u128 * 1_000_000,
            ),
        }
    }

    pub fn simulated_cpu_utilization_percent(&self) -> f64 {
        self.simulated_cpu_utilization_ppm as f64 / 10_000.0
    }

    pub fn observed_cpu_percent(&self) -> f64 {
        self.observed_cpu_ppm as f64 / 10_000.0
    }
}

fn utilization_ppm(work: u128, capacity: u128) -> u64 {
    if capacity == 0 {
        return 0;
    }
    let ppm = work.saturating_mul(1_000_000) / capacity;
    u64::try_from(ppm).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_computes_utilization_and_hot_nodes() {
        let profile = ClusterProfile::from_perf_report(
            100,
            &[
                NodePerfReport {
                    node: 0,
                    handled_requests: 1,
                    simulated_cpu_ms: 20,
                    observed_handler_nanos: 1_000_000,
                    ..NodePerfReport::default()
                },
                NodePerfReport {
                    node: 1,
                    handled_requests: 2,
                    simulated_cpu_ms: 40,
                    simulated_cpu_wait_ms: 5,
                    observed_handler_nanos: 2_000_000,
                    ..NodePerfReport::default()
                },
            ],
        );

        assert_eq!(profile.total_handled_requests(), 3);
        assert_eq!(profile.total_simulated_cpu_ms(), 60);
        assert_eq!(profile.total_simulated_cpu_wait_ms(), 5);
        assert_eq!(profile.nodes[0].simulated_cpu_utilization_ppm, 200_000);
        assert_eq!(
            profile.nodes[1].observed_handler_nanos_per_handled,
            1_000_000
        );
        assert_eq!(profile.busiest_simulated_cpu_node().unwrap().node, 1);
        assert_eq!(profile.busiest_observed_cpu_node().unwrap().node, 1);
    }
}
