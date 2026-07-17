//! Generic deterministic testing environment primitives.
//!
//! This crate is intentionally protocol-agnostic. Applications provide their
//! own node type by implementing [`HermeticNode`] and can attach app-specific
//! configuration to [`HermeticPlan::app_config`]. The environment owns the
//! reusable mechanics: deterministic event ordering, node up/down transitions,
//! latency and drop injection, CPU throttling/stalls, hardware-style faults,
//! replayable logs, and network chaos sampling. `blossom-sim` wraps these
//! pieces for Blossom `WireRequest` handling.

mod chaos;
mod error;
mod hermetic;
mod profile;
mod resources;
mod rng;

pub use chaos::{
    CHAOS_RATE_DENOMINATOR, ChaosSample, NetworkChaos, NetworkChaosConfig, NetworkChaosReport,
};
pub use error::{Result, SimEnvError};
pub use hermetic::{
    HermeticActionRecord, HermeticCluster, HermeticEventLog, HermeticEventRecord, HermeticNode,
    HermeticNodeFuture, HermeticOutcome, HermeticPerfReport, HermeticPlan, HermeticRunReport,
    HermeticSimConfig, NodePerfReport, PlannedAction, replay_matches_with, run_plan_with,
    run_plan_with_perf,
};
pub use profile::{ClusterProfile, NodeProfile};
pub use resources::{CpuProfile, HardwareFaultConfig, HardwareFaultKind};
pub use rng::splitmix64;
