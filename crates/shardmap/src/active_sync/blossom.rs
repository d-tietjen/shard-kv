use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use sha2::{Digest, Sha256};

use super::{
    ConflictClaim, ConflictDecision, ConflictOrderer, MutationDot, Result, ShardCacheError,
};

const GROUP_DOMAIN: &[u8] = b"shardmap.active-sync.blossom.group.v1";

/// Proof returned by a Blossom adapter after an exact conflict claim is part
/// of a finalized epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlossomConflictCertificate {
    pub group_id: [u8; 32],
    pub epoch_nonce: u64,
    pub epoch_hash: [u8; 32],
    pub claim_digest: [u8; 32],
    /// Earliest finalized epoch containing each candidate in canonical claim
    /// order. These ranks must remain stable across every claim in the group.
    pub candidate_epochs: [u64; 2],
}

/// Minimal service boundary between ShardMap and Blossom.
///
/// Implementations must verify that the returned epoch is finalized by the
/// configured Blossom validator set and contains `claim_payload`. The bridge
/// must index the earliest finalized epoch in which each candidate dot appears
/// and return those stable ranks in canonical claim order. Repeated claims and
/// overlapping candidate pairs must return the same rank for a candidate; this
/// creates a transitive total order across three or more concurrent writes.
/// The payload is compact conflict metadata, never a key, value, or WAL block.
pub trait BlossomConflictConsensus: Send + Sync {
    fn commit_conflict(
        &self,
        group_id: [u8; 32],
        claim_payload: &[u8],
        deadline: Duration,
    ) -> Result<BlossomConflictCertificate>;
}

/// Bounded execution policy for Blossom conflict ordering.
#[derive(Debug, Clone)]
pub struct BlossomConflictOrdererOptions {
    pub deadline: Duration,
    pub queue_capacity_per_group: usize,
    pub decision_cache_capacity_per_group: usize,
    pub candidate_cache_capacity_per_group: usize,
    pub max_groups: usize,
    pub max_retries: usize,
    pub retry_backoff: Duration,
    pub circuit_failure_threshold: usize,
    pub circuit_cooldown: Duration,
}

impl Default for BlossomConflictOrdererOptions {
    fn default() -> Self {
        Self {
            deadline: Duration::from_secs(2),
            queue_capacity_per_group: 64,
            decision_cache_capacity_per_group: 4_096,
            candidate_cache_capacity_per_group: 8_192,
            max_groups: 1_024,
            max_retries: 2,
            retry_backoff: Duration::from_millis(10),
            circuit_failure_threshold: 3,
            circuit_cooldown: Duration::from_secs(5),
        }
    }
}

impl BlossomConflictOrdererOptions {
    fn validate(&self) -> Result<()> {
        if self.deadline.is_zero()
            || self.queue_capacity_per_group == 0
            || self.decision_cache_capacity_per_group == 0
            || self.candidate_cache_capacity_per_group == 0
            || self.max_groups == 0
            || self.circuit_failure_threshold == 0
            || self.circuit_cooldown.is_zero()
        {
            return Err(ShardCacheError::Config(
                "Blossom conflict ordering limits and deadlines must be nonzero".into(),
            ));
        }
        Ok(())
    }
}

/// Lock-free aggregate counters for the bounded ordering lanes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlossomConflictOrdererHealth {
    pub groups: u64,
    pub requests: u64,
    pub cache_hits: u64,
    pub retries: u64,
    pub timeouts: u64,
    pub queue_full: u64,
    pub circuit_rejections: u64,
    pub rank_mismatches: u64,
    pub failures: u64,
    pub completed: u64,
}

#[derive(Debug, Default)]
struct OrdererMetrics {
    groups: AtomicU64,
    requests: AtomicU64,
    cache_hits: AtomicU64,
    retries: AtomicU64,
    timeouts: AtomicU64,
    queue_full: AtomicU64,
    circuit_rejections: AtomicU64,
    rank_mismatches: AtomicU64,
    failures: AtomicU64,
    completed: AtomicU64,
}

impl OrdererMetrics {
    fn snapshot(&self) -> BlossomConflictOrdererHealth {
        BlossomConflictOrdererHealth {
            groups: self.groups.load(Ordering::Relaxed),
            requests: self.requests.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            queue_full: self.queue_full.load(Ordering::Relaxed),
            circuit_rejections: self.circuit_rejections.load(Ordering::Relaxed),
            rank_mismatches: self.rank_mismatches.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
        }
    }
}

/// Consensus-backed orderer for ambiguous active-sync conflicts.
pub struct BlossomConflictOrderer {
    consensus: Arc<dyn BlossomConflictConsensus>,
    options: BlossomConflictOrdererOptions,
    lanes: Mutex<HashMap<[u8; 32], Arc<OrderingLane>>>,
    metrics: Arc<OrdererMetrics>,
}

impl BlossomConflictOrderer {
    pub fn new(consensus: Arc<dyn BlossomConflictConsensus>, deadline: Duration) -> Result<Self> {
        Self::with_options(
            consensus,
            BlossomConflictOrdererOptions {
                deadline,
                ..BlossomConflictOrdererOptions::default()
            },
        )
    }

    pub fn with_options(
        consensus: Arc<dyn BlossomConflictConsensus>,
        options: BlossomConflictOrdererOptions,
    ) -> Result<Self> {
        options.validate()?;
        Ok(Self {
            consensus,
            options,
            lanes: Mutex::new(HashMap::new()),
            metrics: Arc::new(OrdererMetrics::default()),
        })
    }

    pub fn group_id(claim: &ConflictClaim) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(GROUP_DOMAIN);
        digest.update(claim.cluster_digest());
        digest.update(claim.shard_id().to_le_bytes());
        digest.finalize().into()
    }

    pub fn health_snapshot(&self) -> BlossomConflictOrdererHealth {
        self.metrics.snapshot()
    }

    fn lane(&self, group_id: [u8; 32]) -> Result<Arc<OrderingLane>> {
        let mut lanes = self.lanes.lock();
        if let Some(lane) = lanes.get(&group_id) {
            return Ok(Arc::clone(lane));
        }
        if lanes.len() >= self.options.max_groups {
            return Err(ShardCacheError::Backpressure(
                "Blossom conflict ordering group limit reached",
            ));
        }
        let lane = OrderingLane::start(
            group_id,
            Arc::clone(&self.consensus),
            self.options.clone(),
            Arc::clone(&self.metrics),
        )?;
        lanes.insert(group_id, Arc::clone(&lane));
        self.metrics.groups.fetch_add(1, Ordering::Relaxed);
        Ok(lane)
    }
}

impl fmt::Debug for BlossomConflictOrderer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlossomConflictOrderer")
            .field("options", &self.options)
            .field("health", &self.health_snapshot())
            .finish_non_exhaustive()
    }
}

impl ConflictOrderer for BlossomConflictOrderer {
    fn decide(&self, claim: &ConflictClaim) -> Result<ConflictDecision> {
        self.metrics.requests.fetch_add(1, Ordering::Relaxed);
        let group_id = Self::group_id(claim);
        self.lane(group_id)?
            .decide(claim.clone(), self.options.deadline)
    }
}

struct OrderingJob {
    claim: ConflictClaim,
    response: SyncSender<Result<ConflictDecision>>,
}

#[derive(Debug, Default)]
struct CircuitState {
    consecutive_failures: usize,
    open_until: Option<Instant>,
    half_open_probe: bool,
}

struct OrderingLane {
    sender: SyncSender<OrderingJob>,
    cache: Arc<Mutex<LaneCache>>,
    circuit: Mutex<CircuitState>,
    failure_threshold: usize,
    cooldown: Duration,
    metrics: Arc<OrdererMetrics>,
}

impl OrderingLane {
    fn start(
        group_id: [u8; 32],
        consensus: Arc<dyn BlossomConflictConsensus>,
        options: BlossomConflictOrdererOptions,
        metrics: Arc<OrdererMetrics>,
    ) -> Result<Arc<Self>> {
        let (sender, receiver) = sync_channel(options.queue_capacity_per_group);
        let cache = Arc::new(Mutex::new(LaneCache::new(
            options.decision_cache_capacity_per_group,
            options.candidate_cache_capacity_per_group,
        )));
        let lane = Arc::new(Self {
            sender,
            cache: Arc::clone(&cache),
            circuit: Mutex::new(CircuitState::default()),
            failure_threshold: options.circuit_failure_threshold,
            cooldown: options.circuit_cooldown,
            metrics: Arc::clone(&metrics),
        });
        let thread_name = format!(
            "blossom-order-{:02x}{:02x}{:02x}{:02x}",
            group_id[0], group_id[1], group_id[2], group_id[3]
        );
        thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                run_ordering_lane(group_id, consensus, options, metrics, cache, receiver)
            })
            .map_err(|error| {
                ShardCacheError::Config(format!(
                    "failed to start Blossom conflict ordering lane: {error}"
                ))
            })?;
        Ok(lane)
    }

    fn decide(&self, claim: ConflictClaim, deadline: Duration) -> Result<ConflictDecision> {
        if let Some(decision) = self.cache.lock().decisions.get(&claim.digest()).cloned() {
            self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(decision);
        }
        if !self.admit() {
            self.metrics
                .circuit_rejections
                .fetch_add(1, Ordering::Relaxed);
            return Err(ShardCacheError::Backpressure(
                "Blossom conflict ordering circuit is open",
            ));
        }

        let (response, receiver) = sync_channel(1);
        match self.sender.try_send(OrderingJob { claim, response }) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.metrics.queue_full.fetch_add(1, Ordering::Relaxed);
                self.record_failure();
                return Err(ShardCacheError::Backpressure(
                    "Blossom conflict ordering queue is full",
                ));
            }
            Err(TrySendError::Disconnected(_)) => {
                self.record_failure();
                return Err(ShardCacheError::ChannelClosed(
                    "Blossom conflict ordering worker stopped",
                ));
            }
        }
        match receiver.recv_timeout(deadline) {
            Ok(Ok(decision)) => {
                self.record_success();
                self.metrics.completed.fetch_add(1, Ordering::Relaxed);
                Ok(decision)
            }
            Ok(Err(error)) => {
                self.record_failure();
                self.metrics.failures.fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
            Err(_) => {
                self.metrics.timeouts.fetch_add(1, Ordering::Relaxed);
                self.metrics.failures.fetch_add(1, Ordering::Relaxed);
                self.record_failure();
                Err(ShardCacheError::Protocol(
                    "Blossom conflict ordering deadline exceeded".into(),
                ))
            }
        }
    }

    fn admit(&self) -> bool {
        let now = Instant::now();
        let mut circuit = self.circuit.lock();
        match circuit.open_until {
            Some(until) if until > now => false,
            Some(_) if circuit.half_open_probe => false,
            Some(_) => {
                circuit.half_open_probe = true;
                true
            }
            None => true,
        }
    }

    fn record_success(&self) {
        *self.circuit.lock() = CircuitState::default();
    }

    fn record_failure(&self) {
        let mut circuit = self.circuit.lock();
        circuit.half_open_probe = false;
        circuit.consecutive_failures = circuit.consecutive_failures.saturating_add(1);
        if circuit.consecutive_failures >= self.failure_threshold {
            circuit.open_until = Some(Instant::now() + self.cooldown);
        }
    }
}

struct LaneCache {
    decisions: BoundedCache<[u8; 32], ConflictDecision>,
    candidate_epochs: BoundedCache<MutationDot, u64>,
}

impl LaneCache {
    fn new(decision_capacity: usize, candidate_capacity: usize) -> Self {
        Self {
            decisions: BoundedCache::new(decision_capacity),
            candidate_epochs: BoundedCache::new(candidate_capacity),
        }
    }
}

struct BoundedCache<K, V> {
    capacity: usize,
    values: HashMap<K, V>,
    order: VecDeque<K>,
}

impl<K, V> BoundedCache<K, V>
where
    K: Clone + Eq + std::hash::Hash,
{
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            values: HashMap::with_capacity(capacity.min(1_024)),
            order: VecDeque::with_capacity(capacity.min(1_024)),
        }
    }

    fn get(&self, key: &K) -> Option<&V> {
        self.values.get(key)
    }

    fn insert(&mut self, key: K, value: V) {
        if let Some(existing) = self.values.get_mut(&key) {
            *existing = value;
            return;
        }
        while self.values.len() >= self.capacity {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.values.remove(&oldest);
        }
        self.order.push_back(key.clone());
        self.values.insert(key, value);
    }
}

fn run_ordering_lane(
    group_id: [u8; 32],
    consensus: Arc<dyn BlossomConflictConsensus>,
    options: BlossomConflictOrdererOptions,
    metrics: Arc<OrdererMetrics>,
    cache: Arc<Mutex<LaneCache>>,
    receiver: Receiver<OrderingJob>,
) {
    while let Ok(job) = receiver.recv() {
        let digest = job.claim.digest();
        if let Some(decision) = cache.lock().decisions.get(&digest).cloned() {
            metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            let _ = job.response.send(Ok(decision));
            continue;
        }

        let started = Instant::now();
        let mut attempt = 0usize;
        let result = loop {
            let remaining = options.deadline.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                break Err(ShardCacheError::Protocol(
                    "Blossom conflict ordering deadline exceeded".into(),
                ));
            }
            let result =
                consensus.commit_conflict(group_id, &job.claim.canonical_bytes(), remaining);
            match result {
                Ok(certificate) => {
                    break validate_and_cache_decision(
                        &job.claim,
                        group_id,
                        certificate,
                        &cache,
                        &metrics,
                    );
                }
                Err(error) if attempt < options.max_retries => {
                    attempt += 1;
                    metrics.retries.fetch_add(1, Ordering::Relaxed);
                    let delay = options
                        .retry_backoff
                        .saturating_mul(u32::try_from(attempt).unwrap_or(u32::MAX));
                    let remaining = options.deadline.saturating_sub(started.elapsed());
                    if remaining <= delay {
                        break Err(error);
                    }
                    thread::sleep(delay);
                }
                Err(error) => break Err(error),
            }
        };
        let _ = job.response.send(result);
    }
}

fn validate_and_cache_decision(
    claim: &ConflictClaim,
    group_id: [u8; 32],
    certificate: BlossomConflictCertificate,
    cache: &Mutex<LaneCache>,
    metrics: &OrdererMetrics,
) -> Result<ConflictDecision> {
    if certificate.group_id != group_id
        || certificate.epoch_nonce == 0
        || certificate.claim_digest != claim.digest()
        || certificate
            .candidate_epochs
            .iter()
            .any(|epoch| *epoch == 0 || *epoch > certificate.epoch_nonce)
    {
        return Err(ShardCacheError::Protocol(
            "Blossom conflict certificate does not match the claim".into(),
        ));
    }

    let first = &claim.candidates()[0];
    let second = &claim.candidates()[1];
    let mut cache = cache.lock();
    for (candidate, epoch) in claim.candidates().iter().zip(certificate.candidate_epochs) {
        if cache
            .candidate_epochs
            .get(candidate.dot())
            .is_some_and(|known| *known != epoch)
        {
            metrics.rank_mismatches.fetch_add(1, Ordering::Relaxed);
            return Err(ShardCacheError::Protocol(
                "Blossom candidate epoch changed across finalized claims".into(),
            ));
        }
    }
    let winner =
        if (certificate.candidate_epochs[0], first) >= (certificate.candidate_epochs[1], second) {
            first.dot().clone()
        } else {
            second.dot().clone()
        };
    let decision = ConflictDecision::new(claim, winner)?;
    for (candidate, epoch) in claim.candidates().iter().zip(certificate.candidate_epochs) {
        cache
            .candidate_epochs
            .insert(candidate.dot().clone(), epoch);
    }
    cache.decisions.insert(claim.digest(), decision.clone());
    Ok(decision)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    use super::*;
    use crate::active_sync::{
        ConflictCandidate, ConflictMutationClass, IncarnationId, MutationDot, NodeId,
    };

    #[derive(Debug)]
    struct RecordingConsensus {
        payloads: StdMutex<Vec<Vec<u8>>>,
        epoch_hash: [u8; 32],
        candidate_epochs: [u64; 2],
    }

    impl BlossomConflictConsensus for RecordingConsensus {
        fn commit_conflict(
            &self,
            group_id: [u8; 32],
            claim_payload: &[u8],
            _deadline: Duration,
        ) -> Result<BlossomConflictCertificate> {
            self.payloads.lock().unwrap().push(claim_payload.to_vec());
            Ok(BlossomConflictCertificate {
                group_id,
                epoch_nonce: 7,
                epoch_hash: self.epoch_hash,
                claim_digest: Sha256::digest(
                    [
                        b"shardmap.active-sync.conflict-claim.v1".as_slice(),
                        claim_payload,
                    ]
                    .concat(),
                )
                .into(),
                candidate_epochs: self.candidate_epochs,
            })
        }
    }

    fn candidate(node: &str, sequence: u64) -> ConflictCandidate {
        ConflictCandidate {
            dot: MutationDot {
                node_id: NodeId::new(node).unwrap(),
                incarnation_id: IncarnationId(sequence as u128),
                shard_id: 0,
                sequence,
            },
            class: ConflictMutationClass::Set,
        }
    }

    #[test]
    fn claim_payload_contains_no_key_or_value_bytes() {
        let key = b"private-customer-key";
        let value = b"private-customer-value";
        let claim = ConflictClaim::new(b"cluster", 0, key, candidate("a", 1), candidate("b", 2));
        let payload = claim.canonical_bytes();
        assert!(!payload.windows(key.len()).any(|window| window == key));
        assert!(!payload.windows(value.len()).any(|window| window == value));
        assert!(payload.len() < 768);
    }

    #[test]
    fn finalized_epoch_orders_the_same_claim_deterministically() {
        let consensus = Arc::new(RecordingConsensus {
            payloads: StdMutex::new(Vec::new()),
            epoch_hash: [9; 32],
            candidate_epochs: [3, 7],
        });
        let orderer =
            BlossomConflictOrderer::new(consensus.clone(), Duration::from_secs(1)).unwrap();
        let claim = ConflictClaim::new(b"cluster", 0, b"key", candidate("b", 2), candidate("a", 1));

        let first = orderer.decide(&claim).unwrap();
        let second = orderer.decide(&claim).unwrap();
        assert_eq!(first, second);
        assert_eq!(consensus.payloads.lock().unwrap().len(), 1);
    }

    #[test]
    fn claim_epoch_hash_cannot_change_the_total_order() {
        let claim = ConflictClaim::new(b"cluster", 0, b"key", candidate("a", 1), candidate("b", 2));
        let decide = |epoch_hash| {
            BlossomConflictOrderer::new(
                Arc::new(RecordingConsensus {
                    payloads: StdMutex::new(Vec::new()),
                    epoch_hash,
                    candidate_epochs: [6, 2],
                }),
                Duration::from_secs(1),
            )
            .unwrap()
            .decide(&claim)
            .unwrap()
        };

        assert_eq!(decide([1; 32]), decide([2; 32]));
    }

    #[test]
    fn invalid_candidate_epoch_is_rejected() {
        let orderer = BlossomConflictOrderer::new(
            Arc::new(RecordingConsensus {
                payloads: StdMutex::new(Vec::new()),
                epoch_hash: [1; 32],
                candidate_epochs: [0, 1],
            }),
            Duration::from_secs(1),
        )
        .unwrap();
        let claim = ConflictClaim::new(b"cluster", 0, b"key", candidate("a", 1), candidate("b", 2));

        assert!(orderer.decide(&claim).is_err());
    }

    #[test]
    fn caller_deadline_and_circuit_breaker_bound_a_hung_bridge() {
        struct SleepingConsensus {
            calls: AtomicUsize,
        }
        impl BlossomConflictConsensus for SleepingConsensus {
            fn commit_conflict(
                &self,
                _group_id: [u8; 32],
                _claim_payload: &[u8],
                _deadline: Duration,
            ) -> Result<BlossomConflictCertificate> {
                self.calls.fetch_add(1, AtomicOrdering::Relaxed);
                thread::sleep(Duration::from_millis(200));
                Err(ShardCacheError::Protocol("late bridge response".into()))
            }
        }

        let consensus = Arc::new(SleepingConsensus {
            calls: AtomicUsize::new(0),
        });
        let orderer = BlossomConflictOrderer::with_options(
            consensus.clone(),
            BlossomConflictOrdererOptions {
                deadline: Duration::from_millis(20),
                max_retries: 0,
                circuit_failure_threshold: 1,
                circuit_cooldown: Duration::from_secs(1),
                ..BlossomConflictOrdererOptions::default()
            },
        )
        .unwrap();
        let claim = ConflictClaim::new(b"cluster", 0, b"key", candidate("a", 1), candidate("b", 2));

        let started = Instant::now();
        assert!(orderer.decide(&claim).is_err());
        assert!(started.elapsed() < Duration::from_millis(100));
        let started = Instant::now();
        assert!(orderer.decide(&claim).is_err());
        assert!(started.elapsed() < Duration::from_millis(10));
        assert_eq!(consensus.calls.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(orderer.health_snapshot().timeouts, 1);
        assert_eq!(orderer.health_snapshot().circuit_rejections, 1);
    }

    #[test]
    fn candidate_epoch_changes_across_claims_fail_closed() {
        struct SequencedConsensus {
            epochs: StdMutex<VecDeque<[u64; 2]>>,
        }
        impl BlossomConflictConsensus for SequencedConsensus {
            fn commit_conflict(
                &self,
                group_id: [u8; 32],
                claim_payload: &[u8],
                _deadline: Duration,
            ) -> Result<BlossomConflictCertificate> {
                Ok(BlossomConflictCertificate {
                    group_id,
                    epoch_nonce: 10,
                    epoch_hash: [7; 32],
                    claim_digest: Sha256::digest(
                        [
                            b"shardmap.active-sync.conflict-claim.v1".as_slice(),
                            claim_payload,
                        ]
                        .concat(),
                    )
                    .into(),
                    candidate_epochs: self.epochs.lock().unwrap().pop_front().unwrap(),
                })
            }
        }

        let orderer = BlossomConflictOrderer::new(
            Arc::new(SequencedConsensus {
                epochs: StdMutex::new(VecDeque::from([[1, 2], [3, 4]])),
            }),
            Duration::from_secs(1),
        )
        .unwrap();
        let first = ConflictClaim::new(b"cluster", 0, b"key", candidate("a", 1), candidate("b", 2));
        let overlapping =
            ConflictClaim::new(b"cluster", 0, b"key", candidate("a", 1), candidate("c", 3));

        assert!(orderer.decide(&first).is_ok());
        assert!(orderer.decide(&overlapping).is_err());
        assert_eq!(orderer.health_snapshot().rank_mismatches, 1);
    }

    #[test]
    fn mismatched_certificate_is_rejected() {
        struct WrongConsensus;
        impl BlossomConflictConsensus for WrongConsensus {
            fn commit_conflict(
                &self,
                group_id: [u8; 32],
                _claim_payload: &[u8],
                _deadline: Duration,
            ) -> Result<BlossomConflictCertificate> {
                Ok(BlossomConflictCertificate {
                    group_id,
                    epoch_nonce: 1,
                    epoch_hash: [1; 32],
                    claim_digest: [2; 32],
                    candidate_epochs: [1, 1],
                })
            }
        }

        let orderer =
            BlossomConflictOrderer::new(Arc::new(WrongConsensus), Duration::from_secs(1)).unwrap();
        let claim = ConflictClaim::new(b"cluster", 0, b"key", candidate("a", 1), candidate("b", 2));
        assert!(orderer.decide(&claim).is_err());
    }
}
