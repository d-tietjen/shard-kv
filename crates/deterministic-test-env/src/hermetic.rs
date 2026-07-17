use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

use crate::{
    CHAOS_RATE_DENOMINATOR, CpuProfile, HardwareFaultConfig, HardwareFaultKind, Result,
    SimEnvError, splitmix64,
};

pub type HermeticNodeFuture<'a> = Pin<Box<dyn Future<Output = Result<&'static str>> + Send + 'a>>;

pub trait HermeticNode<Request>: Send {
    fn handle_request<'a>(&'a mut self, request: Request) -> HermeticNodeFuture<'a>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HermeticSimConfig {
    pub seed: u64,
    pub default_latency_ms: u64,
    pub jitter_ms: u64,
    pub drop_ppm: u32,
}

impl Default for HermeticSimConfig {
    fn default() -> Self {
        Self {
            seed: 0x7369_6d5f_626c_6f31,
            default_latency_ms: 0,
            jitter_ms: 0,
            drop_ppm: 0,
        }
    }
}

impl HermeticSimConfig {
    pub fn validate(&self) -> Result<()> {
        if self.drop_ppm > CHAOS_RATE_DENOMINATOR {
            return Err(SimEnvError::InvalidRate {
                label: "drop_ppm",
                value: self.drop_ppm,
                max: CHAOS_RATE_DENOMINATOR,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct HermeticPlan<Request, AppConfig = ()> {
    pub node_count: usize,
    pub app_config: AppConfig,
    pub config: HermeticSimConfig,
    actions: Vec<PlannedAction<Request>>,
}

impl<Request, AppConfig> HermeticPlan<Request, AppConfig> {
    pub fn new(node_count: usize, app_config: AppConfig, config: HermeticSimConfig) -> Self {
        Self {
            node_count,
            app_config,
            config,
            actions: Vec::new(),
        }
    }

    pub fn request(
        &mut self,
        at_ms: u64,
        source: usize,
        target: usize,
        request: Request,
    ) -> &mut Self {
        self.actions.push(PlannedAction::Request {
            at_ms,
            source,
            target,
            request: Box::new(request),
        });
        self
    }

    pub fn node_down(&mut self, at_ms: u64, node: usize) -> &mut Self {
        self.actions.push(PlannedAction::NodeDown { at_ms, node });
        self
    }

    pub fn node_up(&mut self, at_ms: u64, node: usize) -> &mut Self {
        self.actions.push(PlannedAction::NodeUp { at_ms, node });
        self
    }

    pub fn set_latency(
        &mut self,
        at_ms: u64,
        nodes: impl IntoIterator<Item = usize>,
        latency_ms: u64,
    ) -> &mut Self {
        self.actions.push(PlannedAction::SetLatency {
            at_ms,
            nodes: nodes.into_iter().collect(),
            latency_ms,
        });
        self
    }

    pub fn set_cpu(
        &mut self,
        at_ms: u64,
        nodes: impl IntoIterator<Item = usize>,
        profile: CpuProfile,
    ) -> &mut Self {
        self.actions.push(PlannedAction::SetCpu {
            at_ms,
            nodes: nodes.into_iter().collect(),
            profile,
        });
        self
    }

    pub fn set_hardware_faults(
        &mut self,
        at_ms: u64,
        nodes: impl IntoIterator<Item = usize>,
        faults: HardwareFaultConfig,
    ) -> &mut Self {
        self.actions.push(PlannedAction::SetHardwareFaults {
            at_ms,
            nodes: nodes.into_iter().collect(),
            faults,
        });
        self
    }

    pub fn actions(&self) -> &[PlannedAction<Request>] {
        &self.actions
    }
}

#[derive(Debug, Clone)]
pub enum PlannedAction<Request> {
    Request {
        at_ms: u64,
        source: usize,
        target: usize,
        request: Box<Request>,
    },
    NodeDown {
        at_ms: u64,
        node: usize,
    },
    NodeUp {
        at_ms: u64,
        node: usize,
    },
    SetLatency {
        at_ms: u64,
        nodes: Vec<usize>,
        latency_ms: u64,
    },
    SetCpu {
        at_ms: u64,
        nodes: Vec<usize>,
        profile: CpuProfile,
    },
    SetHardwareFaults {
        at_ms: u64,
        nodes: Vec<usize>,
        faults: HardwareFaultConfig,
    },
}

impl<Request> PlannedAction<Request> {
    pub fn at_ms(&self) -> u64 {
        match self {
            Self::Request { at_ms, .. }
            | Self::NodeDown { at_ms, .. }
            | Self::NodeUp { at_ms, .. }
            | Self::SetLatency { at_ms, .. }
            | Self::SetCpu { at_ms, .. }
            | Self::SetHardwareFaults { at_ms, .. } => *at_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HermeticEventLog {
    pub config: HermeticSimConfig,
    pub records: Vec<HermeticEventRecord>,
}

impl HermeticEventLog {
    pub fn response_count(&self) -> usize {
        self.records
            .iter()
            .filter(|record| matches!(record.outcome, HermeticOutcome::Response { .. }))
            .count()
    }

    pub fn dropped_count(&self) -> usize {
        self.records
            .iter()
            .filter(|record| matches!(record.outcome, HermeticOutcome::Dropped))
            .count()
    }

    pub fn unavailable_count(&self) -> usize {
        self.records
            .iter()
            .filter(|record| matches!(record.outcome, HermeticOutcome::NodeUnavailable))
            .count()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HermeticRunReport {
    pub log: HermeticEventLog,
    pub perf: HermeticPerfReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HermeticPerfReport {
    pub nodes: Vec<NodePerfReport>,
}

impl HermeticPerfReport {
    pub fn total_observed_handler_nanos(&self) -> u128 {
        self.nodes
            .iter()
            .map(|node| node.observed_handler_nanos)
            .sum()
    }

    pub fn total_simulated_cpu_ms(&self) -> u64 {
        self.nodes
            .iter()
            .map(|node| node.simulated_cpu_ms)
            .fold(0u64, u64::saturating_add)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NodePerfReport {
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
    pub observed_handler_nanos: u128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HermeticEventRecord {
    pub event_id: u64,
    pub planned_at_ms: u64,
    pub delivered_at_ms: u64,
    pub action: HermeticActionRecord,
    pub outcome: HermeticOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HermeticActionRecord {
    Request {
        source: usize,
        target: usize,
        request_kind: &'static str,
    },
    NodeDown {
        node: usize,
    },
    NodeUp {
        node: usize,
    },
    SetLatency {
        nodes: Vec<usize>,
        latency_ms: u64,
    },
    SetCpu {
        nodes: Vec<usize>,
        profile: CpuProfile,
    },
    SetHardwareFaults {
        nodes: Vec<usize>,
        faults: HardwareFaultConfig,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HermeticOutcome {
    Response { response_kind: &'static str },
    Error { message: String },
    Dropped,
    NodeUnavailable,
    CpuStalled,
    HardwareFault { kind: HardwareFaultKind },
    Applied,
}

pub struct HermeticCluster<Node, Request> {
    nodes: Vec<Node>,
    now_ms: u64,
    next_event_id: u64,
    queue: BTreeMap<(u64, u64), ScheduledEvent<Request>>,
    up: Vec<bool>,
    latency_ms: Vec<u64>,
    cpu_profiles: Vec<CpuProfile>,
    cpu_available_at_ms: Vec<u64>,
    hardware_faults: Vec<HardwareFaultConfig>,
    config: HermeticSimConfig,
    request_kind: fn(&Request) -> &'static str,
    log: Vec<HermeticEventRecord>,
    perf: Vec<NodePerfReport>,
}

enum ScheduledEvent<Request> {
    Request {
        planned_at_ms: u64,
        source: usize,
        target: usize,
        request: Box<Request>,
    },
    DeliverRequest {
        planned_at_ms: u64,
        source: usize,
        target: usize,
        request: Box<Request>,
    },
    NodeDown {
        planned_at_ms: u64,
        node: usize,
    },
    NodeUp {
        planned_at_ms: u64,
        node: usize,
    },
    SetLatency {
        planned_at_ms: u64,
        nodes: Vec<usize>,
        latency_ms: u64,
    },
    SetCpu {
        planned_at_ms: u64,
        nodes: Vec<usize>,
        profile: CpuProfile,
    },
    SetHardwareFaults {
        planned_at_ms: u64,
        nodes: Vec<usize>,
        faults: HardwareFaultConfig,
    },
}

impl<Node, Request> HermeticCluster<Node, Request>
where
    Node: HermeticNode<Request>,
    Request: Clone,
{
    pub fn new(
        nodes: Vec<Node>,
        config: HermeticSimConfig,
        request_kind: fn(&Request) -> &'static str,
    ) -> Result<Self> {
        if nodes.is_empty() {
            return Err(SimEnvError::InvalidNodeCount);
        }
        config.validate()?;
        let node_count = nodes.len();
        Ok(Self {
            nodes,
            now_ms: 0,
            next_event_id: 0,
            queue: BTreeMap::new(),
            up: vec![true; node_count],
            latency_ms: vec![config.default_latency_ms; node_count],
            cpu_profiles: vec![CpuProfile::default(); node_count],
            cpu_available_at_ms: vec![0; node_count],
            hardware_faults: vec![HardwareFaultConfig::default(); node_count],
            config,
            request_kind,
            log: Vec::new(),
            perf: (0..node_count)
                .map(|node| NodePerfReport {
                    node,
                    ..NodePerfReport::default()
                })
                .collect(),
        })
    }

    pub fn from_plan<AppConfig>(
        plan: &HermeticPlan<Request, AppConfig>,
        nodes: Vec<Node>,
        request_kind: fn(&Request) -> &'static str,
    ) -> Result<Self> {
        let mut cluster = Self::new(nodes, plan.config.clone(), request_kind)?;
        cluster.schedule_plan(plan)?;
        Ok(cluster)
    }

    pub fn now_ms(&self) -> u64 {
        self.now_ms
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn log(&self) -> &[HermeticEventRecord] {
        &self.log
    }

    pub fn perf_report(&self) -> HermeticPerfReport {
        HermeticPerfReport {
            nodes: self.perf.clone(),
        }
    }

    pub fn into_log(self) -> HermeticEventLog {
        HermeticEventLog {
            config: self.config,
            records: self.log,
        }
    }

    pub fn into_report(self) -> HermeticRunReport {
        HermeticRunReport {
            log: HermeticEventLog {
                config: self.config,
                records: self.log,
            },
            perf: HermeticPerfReport { nodes: self.perf },
        }
    }

    pub fn set_node_latency(&mut self, node: usize, latency_ms: u64) -> Result<()> {
        self.ensure_node(node)?;
        self.latency_ms[node] = latency_ms;
        Ok(())
    }

    pub fn set_nodes_latency(
        &mut self,
        nodes: impl IntoIterator<Item = usize>,
        latency_ms: u64,
    ) -> Result<()> {
        for node in nodes {
            self.set_node_latency(node, latency_ms)?;
        }
        Ok(())
    }

    pub fn set_node_cpu(&mut self, node: usize, profile: CpuProfile) -> Result<()> {
        self.ensure_node(node)?;
        profile.validate()?;
        self.cpu_profiles[node] = profile;
        Ok(())
    }

    pub fn set_nodes_cpu(
        &mut self,
        nodes: impl IntoIterator<Item = usize>,
        profile: CpuProfile,
    ) -> Result<()> {
        profile.validate()?;
        for node in nodes {
            self.set_node_cpu(node, profile)?;
        }
        Ok(())
    }

    pub fn set_node_hardware_faults(
        &mut self,
        node: usize,
        faults: HardwareFaultConfig,
    ) -> Result<()> {
        self.ensure_node(node)?;
        faults.validate()?;
        self.hardware_faults[node] = faults;
        Ok(())
    }

    pub fn set_nodes_hardware_faults(
        &mut self,
        nodes: impl IntoIterator<Item = usize>,
        faults: HardwareFaultConfig,
    ) -> Result<()> {
        faults.validate()?;
        for node in nodes {
            self.set_node_hardware_faults(node, faults)?;
        }
        Ok(())
    }

    pub fn set_node_up(&mut self, node: usize, up: bool) -> Result<()> {
        self.ensure_node(node)?;
        self.up[node] = up;
        Ok(())
    }

    pub fn schedule_plan<AppConfig>(
        &mut self,
        plan: &HermeticPlan<Request, AppConfig>,
    ) -> Result<()> {
        if plan.node_count != self.nodes.len() {
            return Err(SimEnvError::PlanNodeCountMismatch {
                plan_node_count: plan.node_count,
                cluster_node_count: self.nodes.len(),
            });
        }
        for action in plan.actions() {
            match action.clone() {
                PlannedAction::Request {
                    at_ms,
                    source,
                    target,
                    request,
                } => self.schedule_request(at_ms, source, target, *request)?,
                PlannedAction::NodeDown { at_ms, node } => self.schedule_node_down(at_ms, node)?,
                PlannedAction::NodeUp { at_ms, node } => self.schedule_node_up(at_ms, node)?,
                PlannedAction::SetLatency {
                    at_ms,
                    nodes,
                    latency_ms,
                } => self.schedule_latency(at_ms, nodes, latency_ms)?,
                PlannedAction::SetCpu {
                    at_ms,
                    nodes,
                    profile,
                } => self.schedule_cpu(at_ms, nodes, profile)?,
                PlannedAction::SetHardwareFaults {
                    at_ms,
                    nodes,
                    faults,
                } => self.schedule_hardware_faults(at_ms, nodes, faults)?,
            }
        }
        Ok(())
    }

    pub fn schedule_request(
        &mut self,
        at_ms: u64,
        source: usize,
        target: usize,
        request: Request,
    ) -> Result<()> {
        self.ensure_node(source)?;
        self.ensure_node(target)?;
        self.push_event(
            at_ms,
            ScheduledEvent::Request {
                planned_at_ms: at_ms,
                source,
                target,
                request: Box::new(request),
            },
        );
        Ok(())
    }

    pub fn schedule_node_down(&mut self, at_ms: u64, node: usize) -> Result<()> {
        self.ensure_node(node)?;
        self.push_event(
            at_ms,
            ScheduledEvent::NodeDown {
                planned_at_ms: at_ms,
                node,
            },
        );
        Ok(())
    }

    pub fn schedule_node_up(&mut self, at_ms: u64, node: usize) -> Result<()> {
        self.ensure_node(node)?;
        self.push_event(
            at_ms,
            ScheduledEvent::NodeUp {
                planned_at_ms: at_ms,
                node,
            },
        );
        Ok(())
    }

    pub fn schedule_latency(
        &mut self,
        at_ms: u64,
        nodes: impl IntoIterator<Item = usize>,
        latency_ms: u64,
    ) -> Result<()> {
        let nodes = nodes.into_iter().collect::<Vec<_>>();
        for node in &nodes {
            self.ensure_node(*node)?;
        }
        self.push_event(
            at_ms,
            ScheduledEvent::SetLatency {
                planned_at_ms: at_ms,
                nodes,
                latency_ms,
            },
        );
        Ok(())
    }

    pub fn schedule_cpu(
        &mut self,
        at_ms: u64,
        nodes: impl IntoIterator<Item = usize>,
        profile: CpuProfile,
    ) -> Result<()> {
        profile.validate()?;
        let nodes = nodes.into_iter().collect::<Vec<_>>();
        for node in &nodes {
            self.ensure_node(*node)?;
        }
        self.push_event(
            at_ms,
            ScheduledEvent::SetCpu {
                planned_at_ms: at_ms,
                nodes,
                profile,
            },
        );
        Ok(())
    }

    pub fn schedule_hardware_faults(
        &mut self,
        at_ms: u64,
        nodes: impl IntoIterator<Item = usize>,
        faults: HardwareFaultConfig,
    ) -> Result<()> {
        faults.validate()?;
        let nodes = nodes.into_iter().collect::<Vec<_>>();
        for node in &nodes {
            self.ensure_node(*node)?;
        }
        self.push_event(
            at_ms,
            ScheduledEvent::SetHardwareFaults {
                planned_at_ms: at_ms,
                nodes,
                faults,
            },
        );
        Ok(())
    }

    pub async fn run_until_idle(&mut self) -> Result<()> {
        while let Some(((delivered_at_ms, event_id), event)) = self.pop_next() {
            self.now_ms = delivered_at_ms;
            if let Some(record) = self.handle_event(event_id, delivered_at_ms, event).await? {
                self.log.push(record);
            }
        }
        Ok(())
    }

    async fn handle_event(
        &mut self,
        event_id: u64,
        delivered_at_ms: u64,
        event: ScheduledEvent<Request>,
    ) -> Result<Option<HermeticEventRecord>> {
        match event {
            ScheduledEvent::Request {
                planned_at_ms,
                source,
                target,
                request,
            } => {
                let request_delay_ms = self.latency_ms[target]
                    .checked_add(self.jitter_ms(event_id, source, target))
                    .ok_or(SimEnvError::TimeOverflow)?;
                let request_delivered_at_ms = delivered_at_ms
                    .checked_add(request_delay_ms)
                    .ok_or(SimEnvError::TimeOverflow)?;
                self.push_event(
                    request_delivered_at_ms,
                    ScheduledEvent::DeliverRequest {
                        planned_at_ms,
                        source,
                        target,
                        request,
                    },
                );
                Ok(None)
            }
            ScheduledEvent::DeliverRequest {
                planned_at_ms,
                source,
                target,
                request,
            } => {
                let request_kind = (self.request_kind)(&request);
                let action = HermeticActionRecord::Request {
                    source,
                    target,
                    request_kind,
                };
                self.perf[target].delivered_requests += 1;
                let mut completed_at_ms = delivered_at_ms;
                let outcome = if !self.up[target] {
                    self.perf[target].unavailable += 1;
                    HermeticOutcome::NodeUnavailable
                } else if self.should_drop(event_id, source, target) {
                    self.perf[target].dropped += 1;
                    HermeticOutcome::Dropped
                } else if self.should_cpu_stall(event_id, source, target) {
                    self.perf[target].cpu_stalled += 1;
                    HermeticOutcome::CpuStalled
                } else {
                    let cpu_delay_ms = self.cpu_delay_ms(event_id, source, target)?;
                    let cpu_started_at_ms = delivered_at_ms.max(self.cpu_available_at_ms[target]);
                    let cpu_wait_ms = cpu_started_at_ms.saturating_sub(delivered_at_ms);
                    completed_at_ms = cpu_started_at_ms
                        .checked_add(cpu_delay_ms)
                        .ok_or(SimEnvError::TimeOverflow)?;
                    self.perf[target].simulated_cpu_ms = self.perf[target]
                        .simulated_cpu_ms
                        .saturating_add(cpu_delay_ms);
                    self.perf[target].simulated_cpu_wait_ms = self.perf[target]
                        .simulated_cpu_wait_ms
                        .saturating_add(cpu_wait_ms);
                    self.cpu_available_at_ms[target] = completed_at_ms;
                    match self.hardware_fault(event_id, source, target) {
                        Some(kind) => {
                            if kind == HardwareFaultKind::Crash {
                                self.up[target] = false;
                            }
                            self.perf[target].hardware_faults += 1;
                            HermeticOutcome::HardwareFault { kind }
                        }
                        None => {
                            let started = Instant::now();
                            let result = self.nodes[target].handle_request(*request).await;
                            self.perf[target].observed_handler_nanos +=
                                started.elapsed().as_nanos();
                            self.perf[target].handled_requests += 1;
                            match result {
                                Ok(response_kind) => {
                                    self.perf[target].responses += 1;
                                    HermeticOutcome::Response { response_kind }
                                }
                                Err(err) => {
                                    self.perf[target].handler_errors += 1;
                                    HermeticOutcome::Error {
                                        message: err.to_string(),
                                    }
                                }
                            }
                        }
                    }
                };
                Ok(Some(HermeticEventRecord {
                    event_id,
                    planned_at_ms,
                    delivered_at_ms: completed_at_ms,
                    action,
                    outcome,
                }))
            }
            ScheduledEvent::NodeDown {
                planned_at_ms,
                node,
            } => {
                self.up[node] = false;
                Ok(Some(HermeticEventRecord {
                    event_id,
                    planned_at_ms,
                    delivered_at_ms,
                    action: HermeticActionRecord::NodeDown { node },
                    outcome: HermeticOutcome::Applied,
                }))
            }
            ScheduledEvent::NodeUp {
                planned_at_ms,
                node,
            } => {
                self.up[node] = true;
                Ok(Some(HermeticEventRecord {
                    event_id,
                    planned_at_ms,
                    delivered_at_ms,
                    action: HermeticActionRecord::NodeUp { node },
                    outcome: HermeticOutcome::Applied,
                }))
            }
            ScheduledEvent::SetLatency {
                planned_at_ms,
                nodes,
                latency_ms,
            } => {
                for node in &nodes {
                    self.latency_ms[*node] = latency_ms;
                }
                Ok(Some(HermeticEventRecord {
                    event_id,
                    planned_at_ms,
                    delivered_at_ms,
                    action: HermeticActionRecord::SetLatency { nodes, latency_ms },
                    outcome: HermeticOutcome::Applied,
                }))
            }
            ScheduledEvent::SetCpu {
                planned_at_ms,
                nodes,
                profile,
            } => {
                for node in &nodes {
                    self.cpu_profiles[*node] = profile;
                }
                Ok(Some(HermeticEventRecord {
                    event_id,
                    planned_at_ms,
                    delivered_at_ms,
                    action: HermeticActionRecord::SetCpu { nodes, profile },
                    outcome: HermeticOutcome::Applied,
                }))
            }
            ScheduledEvent::SetHardwareFaults {
                planned_at_ms,
                nodes,
                faults,
            } => {
                for node in &nodes {
                    self.hardware_faults[*node] = faults;
                }
                Ok(Some(HermeticEventRecord {
                    event_id,
                    planned_at_ms,
                    delivered_at_ms,
                    action: HermeticActionRecord::SetHardwareFaults { nodes, faults },
                    outcome: HermeticOutcome::Applied,
                }))
            }
        }
    }

    fn push_event(&mut self, delivered_at_ms: u64, event: ScheduledEvent<Request>) {
        let event_id = self.next_event_id;
        self.next_event_id += 1;
        self.queue.insert((delivered_at_ms, event_id), event);
    }

    fn pop_next(&mut self) -> Option<((u64, u64), ScheduledEvent<Request>)> {
        let key = *self.queue.keys().next()?;
        self.queue.remove_entry(&key)
    }

    fn jitter_ms(&self, event_id: u64, source: usize, target: usize) -> u64 {
        if self.config.jitter_ms == 0 {
            return 0;
        }
        let seed = self.config.seed
            ^ event_id.rotate_left(13)
            ^ (source as u64).rotate_left(29)
            ^ (target as u64).rotate_left(47);
        splitmix64(seed) % (self.config.jitter_ms + 1)
    }

    fn should_drop(&self, event_id: u64, source: usize, target: usize) -> bool {
        self.config.drop_ppm > 0
            && (splitmix64(
                self.config.seed
                    ^ event_id.rotate_left(17)
                    ^ (source as u64).rotate_left(31)
                    ^ (target as u64).rotate_left(43),
            ) % CHAOS_RATE_DENOMINATOR as u64)
                < self.config.drop_ppm as u64
    }

    fn cpu_delay_ms(&self, event_id: u64, source: usize, target: usize) -> Result<u64> {
        let profile = self.cpu_profiles[target];
        let jitter = if profile.jitter_ms == 0 {
            0
        } else {
            let seed = self.config.seed
                ^ event_id.rotate_left(23)
                ^ (source as u64).rotate_left(37)
                ^ (target as u64).rotate_left(53)
                ^ 0xc901_5ca1_1ed0_0001;
            splitmix64(seed) % (profile.jitter_ms + 1)
        };
        profile
            .processing_delay_ms
            .checked_add(jitter)
            .ok_or(SimEnvError::TimeOverflow)
    }

    fn should_cpu_stall(&self, event_id: u64, source: usize, target: usize) -> bool {
        let ppm = self.cpu_profiles[target].stall_ppm;
        ppm > 0
            && (splitmix64(
                self.config.seed
                    ^ event_id.rotate_left(29)
                    ^ (source as u64).rotate_left(41)
                    ^ (target as u64).rotate_left(7)
                    ^ 0xc901_57a1_1000_0001,
            ) % CHAOS_RATE_DENOMINATOR as u64)
                < ppm as u64
    }

    fn hardware_fault(
        &self,
        event_id: u64,
        source: usize,
        target: usize,
    ) -> Option<HardwareFaultKind> {
        let faults = self.hardware_faults[target];
        let seed = self.config.seed
            ^ event_id.rotate_left(31)
            ^ (source as u64).rotate_left(11)
            ^ (target as u64).rotate_left(47)
            ^ 0x4a17_d0e5_5afe_0001;
        if self.sample_rate(seed, 0x11, faults.crash_ppm) {
            return Some(HardwareFaultKind::Crash);
        }
        if self.sample_rate(seed, 0x22, faults.io_error_ppm) {
            return Some(HardwareFaultKind::IoError);
        }
        if self.sample_rate(seed, 0x33, faults.memory_error_ppm) {
            return Some(HardwareFaultKind::MemoryError);
        }
        None
    }

    fn sample_rate(&self, seed: u64, salt: u64, ppm: u32) -> bool {
        ppm > 0 && (splitmix64(seed ^ salt) % CHAOS_RATE_DENOMINATOR as u64) < ppm as u64
    }

    fn ensure_node(&self, node: usize) -> Result<()> {
        if node >= self.nodes.len() {
            return Err(SimEnvError::InvalidNode {
                node,
                node_count: self.nodes.len(),
            });
        }
        Ok(())
    }
}

pub async fn run_plan_with<Node, Request, AppConfig>(
    plan: &HermeticPlan<Request, AppConfig>,
    nodes: Vec<Node>,
    request_kind: fn(&Request) -> &'static str,
) -> Result<HermeticEventLog>
where
    Node: HermeticNode<Request>,
    Request: Clone,
{
    let mut cluster = HermeticCluster::from_plan(plan, nodes, request_kind)?;
    cluster.run_until_idle().await?;
    Ok(cluster.into_log())
}

pub async fn run_plan_with_perf<Node, Request, AppConfig>(
    plan: &HermeticPlan<Request, AppConfig>,
    nodes: Vec<Node>,
    request_kind: fn(&Request) -> &'static str,
) -> Result<HermeticRunReport>
where
    Node: HermeticNode<Request>,
    Request: Clone,
{
    let mut cluster = HermeticCluster::from_plan(plan, nodes, request_kind)?;
    cluster.run_until_idle().await?;
    Ok(cluster.into_report())
}

pub async fn replay_matches_with<Node, Request, AppConfig>(
    plan: &HermeticPlan<Request, AppConfig>,
    expected: &HermeticEventLog,
    nodes: Vec<Node>,
    request_kind: fn(&Request) -> &'static str,
) -> Result<bool>
where
    Node: HermeticNode<Request>,
    Request: Clone,
{
    let replayed = run_plan_with(plan, nodes, request_kind).await?;
    Ok(&replayed == expected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct EchoNode;

    impl HermeticNode<&'static str> for EchoNode {
        fn handle_request<'a>(&'a mut self, request: &'static str) -> HermeticNodeFuture<'a> {
            Box::pin(async move {
                match request {
                    "fail" => Err(SimEnvError::App("injected failure".to_string())),
                    "ping" => Ok("pong"),
                    _ => Ok("ok"),
                }
            })
        }
    }

    fn request_kind(request: &&'static str) -> &'static str {
        request
    }

    #[tokio::test]
    async fn generic_hermetic_plan_replays_exactly() {
        let mut plan = HermeticPlan::new(
            3,
            (),
            HermeticSimConfig {
                seed: 7,
                default_latency_ms: 1,
                jitter_ms: 0,
                drop_ppm: 0,
            },
        );
        plan.set_latency(0, [1], 50)
            .node_down(20, 1)
            .node_up(40, 1)
            .request(1, 0, 1, "ping")
            .request(1, 0, 2, "ping");

        let nodes = || vec![EchoNode, EchoNode, EchoNode];
        let first = run_plan_with(&plan, nodes(), request_kind).await.unwrap();
        let second = run_plan_with(&plan, nodes(), request_kind).await.unwrap();

        assert_eq!(first, second);
        assert_eq!(first.response_count(), 2);
        let request_delivery_times = first
            .records
            .iter()
            .filter_map(|record| {
                matches!(record.action, HermeticActionRecord::Request { .. })
                    .then_some(record.delivered_at_ms)
            })
            .collect::<Vec<_>>();
        assert_eq!(request_delivery_times, vec![2, 51]);
    }

    #[tokio::test]
    async fn perf_report_tracks_handler_time_by_node_without_changing_replay_log() {
        let mut plan = HermeticPlan::new(3, (), HermeticSimConfig::default());
        plan.request(1, 0, 1, "ping")
            .request(2, 0, 1, "fail")
            .request(3, 0, 2, "ping");

        let report = run_plan_with_perf(&plan, vec![EchoNode, EchoNode, EchoNode], request_kind)
            .await
            .unwrap();

        assert_eq!(report.log.response_count(), 2);
        assert_eq!(report.perf.nodes[1].delivered_requests, 2);
        assert_eq!(report.perf.nodes[1].handled_requests, 2);
        assert_eq!(report.perf.nodes[1].responses, 1);
        assert_eq!(report.perf.nodes[1].handler_errors, 1);
        assert_eq!(report.perf.nodes[2].responses, 1);
        assert!(report.perf.total_observed_handler_nanos() > 0);

        let replayed = run_plan_with(&plan, vec![EchoNode, EchoNode, EchoNode], request_kind)
            .await
            .unwrap();
        assert_eq!(report.log, replayed);
    }

    #[tokio::test]
    async fn cpu_profile_controls_completion_time_and_stalls() {
        let mut plan = HermeticPlan::new(
            2,
            (),
            HermeticSimConfig {
                seed: 11,
                default_latency_ms: 1,
                jitter_ms: 0,
                drop_ppm: 0,
            },
        );
        plan.set_cpu(
            0,
            [1],
            CpuProfile {
                processing_delay_ms: 10,
                jitter_ms: 0,
                stall_ppm: 0,
            },
        )
        .request(1, 0, 1, "ping");

        let log = run_plan_with(&plan, vec![EchoNode, EchoNode], request_kind)
            .await
            .unwrap();
        let request = log
            .records
            .iter()
            .find(|record| matches!(record.action, HermeticActionRecord::Request { .. }))
            .unwrap();
        assert_eq!(request.delivered_at_ms, 12);
        assert_eq!(
            request.outcome,
            HermeticOutcome::Response {
                response_kind: "pong"
            }
        );

        let mut stall_plan = HermeticPlan::new(2, (), HermeticSimConfig::default());
        stall_plan
            .set_cpu(
                0,
                [1],
                CpuProfile {
                    stall_ppm: CHAOS_RATE_DENOMINATOR,
                    ..CpuProfile::default()
                },
            )
            .request(1, 0, 1, "ping");
        let log = run_plan_with(&stall_plan, vec![EchoNode, EchoNode], request_kind)
            .await
            .unwrap();
        assert!(matches!(
            log.records.last().map(|record| &record.outcome),
            Some(HermeticOutcome::CpuStalled)
        ));
    }

    #[tokio::test]
    async fn hardware_faults_can_crash_nodes_deterministically() {
        let mut plan = HermeticPlan::new(2, (), HermeticSimConfig::default());
        plan.set_hardware_faults(
            0,
            [1],
            HardwareFaultConfig {
                crash_ppm: CHAOS_RATE_DENOMINATOR,
                ..HardwareFaultConfig::default()
            },
        )
        .request(1, 0, 1, "ping")
        .request(2, 0, 1, "ping");

        let log = run_plan_with(&plan, vec![EchoNode, EchoNode], request_kind)
            .await
            .unwrap();
        let outcomes = log
            .records
            .iter()
            .filter_map(|record| {
                matches!(record.action, HermeticActionRecord::Request { .. })
                    .then_some(&record.outcome)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            outcomes,
            vec![
                &HermeticOutcome::HardwareFault {
                    kind: HardwareFaultKind::Crash
                },
                &HermeticOutcome::NodeUnavailable
            ]
        );
    }

    #[tokio::test]
    async fn cpu_profile_queues_work_per_node() {
        let mut plan = HermeticPlan::new(
            2,
            (),
            HermeticSimConfig {
                seed: 13,
                default_latency_ms: 1,
                jitter_ms: 0,
                drop_ppm: 0,
            },
        );
        plan.set_cpu(
            0,
            [1],
            CpuProfile {
                processing_delay_ms: 10,
                jitter_ms: 0,
                stall_ppm: 0,
            },
        )
        .request(1, 0, 1, "ping")
        .request(1, 0, 1, "ping");

        let report = run_plan_with_perf(&plan, vec![EchoNode, EchoNode], request_kind)
            .await
            .unwrap();
        let completion_times = report
            .log
            .records
            .iter()
            .filter_map(|record| {
                matches!(record.action, HermeticActionRecord::Request { .. })
                    .then_some(record.delivered_at_ms)
            })
            .collect::<Vec<_>>();

        assert_eq!(completion_times, vec![12, 22]);
        assert_eq!(report.perf.nodes[1].simulated_cpu_ms, 20);
        assert_eq!(report.perf.nodes[1].simulated_cpu_wait_ms, 10);
    }
}
