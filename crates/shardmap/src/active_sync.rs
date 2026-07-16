//! Eventually consistent, active-active synchronization for exact point values.
//!
//! This module is deliberately feature gated. It keeps causal metadata and
//! block retention out of the ordinary [`crate::ShardMap`] layout and hot path.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fmt;
use std::fs::{self, OpenOptions};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::io::{Read, Write};
use std::num::NonZeroU64;
use std::ops::Deref;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes as SharedBytes;
use hashbrown::HashMap as HashBrownMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use smallvec::SmallVec;
use xxhash_rust::xxh3::Xxh3;

use crate::storage::{EmbeddedKeyRoute, EmbeddedStore, hash_key, now_millis, ttl_now_millis};
use crate::{Result, ShardCacheError};

#[cfg(feature = "active-sync-consensus-ordered-eventual")]
mod blossom;
#[cfg(feature = "active-sync-tls")]
mod tls;
#[cfg(feature = "active-sync-consensus-ordered-eventual")]
pub use blossom::{
    BlossomConflictCertificate, BlossomConflictConsensus, BlossomConflictOrderer,
    BlossomConflictOrdererHealth, BlossomConflictOrdererOptions,
};
#[cfg(feature = "active-sync-tls")]
pub use tls::{
    ActiveSyncAuthorizedPeer, ActiveSyncMemberHealth, ActiveSyncMemberState,
    ActiveSyncMembershipHealthSnapshot, ActiveSyncMembershipOptions,
    ActiveSyncTlsClientCredentials, ActiveSyncTlsMembership, ActiveSyncTlsPeer,
    ActiveSyncTlsServer, ActiveSyncTlsServerCredentials, ActiveSyncTlsServerOptions,
};

const BLOCK_FORMAT_VERSION: u8 = 1;
const BLOCK_MAGIC: &[u8; 4] = b"ASB1";
const SNAPSHOT_MAGIC: &[u8; 4] = b"ASS1";
const SNAPSHOT_HEADER_BYTES: usize = 4 + 1 + 8 + 32;
static INCARNATION_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Stable active-sync node identity.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(Arc<str>);

impl NodeId {
    pub fn new(value: impl Into<Box<str>>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() || value.len() > 255 {
            return Err(ShardCacheError::Config(
                "active-sync node_id must contain 1..=255 bytes".into(),
            ));
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(ShardCacheError::Config(
                "active-sync node_id contains unsupported characters".into(),
            ));
        }
        Ok(Self(Arc::from(value)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("NodeId").field(&self.0).finish()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Process incarnation. A fresh value must be used after every restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IncarnationId(pub u128);

impl IncarnationId {
    /// Produces a process-local unique incarnation seed without adding an RNG
    /// dependency. Deployments should persist the chosen value before writes.
    pub fn generate() -> Self {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let counter = INCARNATION_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
        let high = (elapsed.as_nanos() as u64) ^ u64::from(std::process::id()).rotate_left(17);
        Self((u128::from(high) << 64) | u128::from(counter))
    }
}

/// Hybrid logical clock used only as the deterministic concurrent SET tie-breaker.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct HybridLogicalClock {
    pub physical_ms: u64,
    pub logical: u32,
}

impl HybridLogicalClock {
    fn tick(&mut self, wall_ms: u64) -> Self {
        if wall_ms > self.physical_ms {
            self.physical_ms = wall_ms;
            self.logical = 0;
        } else {
            self.logical = self.logical.saturating_add(1);
        }
        *self
    }

    fn observe(&mut self, remote: Self, wall_ms: u64) {
        let maximum = self.physical_ms.max(remote.physical_ms).max(wall_ms);
        self.logical = if maximum == self.physical_ms && maximum == remote.physical_ms {
            self.logical.max(remote.logical).saturating_add(1)
        } else if maximum == self.physical_ms {
            self.logical.saturating_add(1)
        } else if maximum == remote.physical_ms {
            remote.logical.saturating_add(1)
        } else {
            0
        };
        self.physical_ms = maximum;
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(transparent)]
struct CompactExpiration(Option<NonZeroU64>);

impl CompactExpiration {
    fn new(expire_at_ms: Option<u64>) -> Self {
        // Zero is already expired for every valid wall clock, while MAX is
        // the wire-format sentinel for no deadline. Moving either inward by
        // one millisecond preserves its practical behavior and representation.
        Self(expire_at_ms.map(|deadline| {
            NonZeroU64::new(deadline.clamp(1, u64::MAX - 1))
                .expect("canonical deadline is non-zero")
        }))
    }

    fn get(self) -> Option<u64> {
        self.0.map(NonZeroU64::get)
    }

    fn is_some(self) -> bool {
        self.0.is_some()
    }

    fn is_none(self) -> bool {
        self.0.is_none()
    }

    fn is_some_and(self, predicate: impl FnOnce(u64) -> bool) -> bool {
        self.get().is_some_and(predicate)
    }

    fn unwrap_or(self, default: u64) -> u64 {
        self.get().unwrap_or(default)
    }
}

/// Globally unique identity for one point mutation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MutationDot {
    pub node_id: NodeId,
    pub incarnation_id: IncarnationId,
    pub shard_id: u32,
    pub sequence: u64,
}

/// Causal token returned by local writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncToken {
    pub slot: u32,
    pub dot: MutationDot,
}

/// The mutation category exposed to an external conflict-ordering service.
///
/// Values, keys, governance metadata, TTLs, and WAL bytes are deliberately not
/// part of this surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ConflictMutationClass {
    Set,
    Delete,
    Expired,
    ClusterEvict,
}

/// One content-addressed candidate in an ambiguous concurrent conflict.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConflictCandidate {
    dot: MutationDot,
    class: ConflictMutationClass,
}

impl ConflictCandidate {
    pub fn dot(&self) -> &MutationDot {
        &self.dot
    }

    pub fn class(&self) -> ConflictMutationClass {
        self.class
    }
}

/// Compact conflict metadata submitted for external ordering.
///
/// The key is represented by a SHA-256 digest and candidates contain only
/// mutation identities and classes. This prevents the ordering plane from
/// becoming a second WAL or value-replication path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictClaim {
    cluster_digest: [u8; 32],
    shard_id: u32,
    key_digest: [u8; 32],
    candidates: [ConflictCandidate; 2],
}

impl ConflictClaim {
    fn new(
        cluster_id: &[u8],
        shard_id: u32,
        key: &[u8],
        first: ConflictCandidate,
        second: ConflictCandidate,
    ) -> Self {
        let mut candidates = [first, second];
        candidates.sort();
        Self {
            cluster_digest: Sha256::digest(cluster_id).into(),
            shard_id,
            key_digest: Sha256::digest(key).into(),
            candidates,
        }
    }

    pub fn cluster_digest(&self) -> [u8; 32] {
        self.cluster_digest
    }

    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }

    pub fn key_digest(&self) -> [u8; 32] {
        self.key_digest
    }

    pub fn candidates(&self) -> &[ConflictCandidate; 2] {
        &self.candidates
    }

    pub fn digest(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"shardmap.active-sync.conflict-claim.v1");
        digest.update(self.canonical_bytes());
        digest.finalize().into()
    }

    /// Stable bytes suitable for an opaque Blossom transaction.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(192);
        bytes.extend_from_slice(b"SCC1");
        bytes.extend_from_slice(&self.cluster_digest);
        bytes.extend_from_slice(&self.shard_id.to_le_bytes());
        bytes.extend_from_slice(&self.key_digest);
        for candidate in &self.candidates {
            put_conflict_dot(&mut bytes, &candidate.dot);
            bytes.push(conflict_class_code(candidate.class));
        }
        bytes
    }
}

/// A winner bound to one exact conflict claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictDecision {
    claim_digest: [u8; 32],
    winner: MutationDot,
}

impl ConflictDecision {
    pub fn new(claim: &ConflictClaim, winner: MutationDot) -> Result<Self> {
        if !claim
            .candidates
            .iter()
            .any(|candidate| candidate.dot == winner)
        {
            return Err(ShardCacheError::Protocol(
                "active-sync conflict winner is not a claim candidate".into(),
            ));
        }
        Ok(Self {
            claim_digest: claim.digest(),
            winner,
        })
    }

    pub fn claim_digest(&self) -> [u8; 32] {
        self.claim_digest
    }

    pub fn winner(&self) -> &MutationDot {
        &self.winner
    }

    fn validate_for(&self, claim: &ConflictClaim) -> Result<()> {
        if self.claim_digest != claim.digest()
            || !claim
                .candidates
                .iter()
                .any(|candidate| candidate.dot == self.winner)
        {
            return Err(ShardCacheError::Protocol(
                "active-sync conflict decision does not match the current claim".into(),
            ));
        }
        Ok(())
    }
}

/// Orders only ambiguous concurrent mutations.
///
/// Implementations may block waiting for consensus. ShardMap always invokes
/// this method without holding a storage-shard lock.
pub trait ConflictOrderer: Send + Sync {
    fn decide(&self, claim: &ConflictClaim) -> Result<ConflictDecision>;
}

/// Cross-node consistency guarantee selected for ambiguous concurrent writes.
///
/// Both modes keep reads local and require explicit or background synchronization
/// for remote visibility. Neither mode is linearizable or serializable.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ActiveConsistencyMode {
    /// Deterministic causal resolution with HLC ordering for concurrent SETs.
    #[default]
    CausalEventual,
    /// Eventual replication with externally finalized ordering for ambiguous
    /// concurrent mutations.
    ConsensusOrderedEventual,
}

impl ActiveConsistencyMode {
    /// Returns whether ambiguous conflicts require an external orderer.
    pub const fn requires_external_orderer(self) -> bool {
        matches!(self, Self::ConsensusOrderedEventual)
    }

    /// Active-sync modes never make local reads linearizable across nodes.
    pub const fn is_linearizable(self) -> bool {
        false
    }

    /// Active-sync modes never provide cross-node serializable transactions.
    pub const fn is_serializable(self) -> bool {
        false
    }
}

/// Hard resource limits and local identity for one active map.
#[derive(Debug, Clone)]
pub struct ActiveSyncConfig {
    pub cluster_id: Box<str>,
    pub node_id: NodeId,
    pub incarnation_id: IncarnationId,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub max_governance_bytes: usize,
    pub max_causal_origins_per_shard: usize,
    pub max_pending_records_per_shard: usize,
    pub max_block_records: usize,
    pub max_block_bytes: usize,
    pub max_retained_block_bytes_per_shard: usize,
    pub max_pending_block_gaps_per_shard: usize,
    pub max_snapshot_bytes: usize,
    pub max_conflict_order_retries: usize,
    pub max_clock_skew: Duration,
}

impl ActiveSyncConfig {
    pub fn new(cluster_id: impl Into<Box<str>>, node_id: NodeId) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            node_id,
            incarnation_id: IncarnationId::generate(),
            max_key_bytes: 1024 * 1024,
            max_value_bytes: 64 * 1024 * 1024,
            max_governance_bytes: 1024 * 1024,
            max_causal_origins_per_shard: 16,
            max_pending_records_per_shard: 65_536,
            max_block_records: 65_536,
            max_block_bytes: 4 * 1024 * 1024,
            max_retained_block_bytes_per_shard: 256 * 1024 * 1024,
            max_pending_block_gaps_per_shard: 1024,
            max_snapshot_bytes: 1024 * 1024 * 1024,
            max_conflict_order_retries: 4,
            max_clock_skew: Duration::from_secs(5),
        }
    }

    fn validate(&self) -> Result<()> {
        if self.cluster_id.is_empty() || self.cluster_id.len() > 255 {
            return Err(ShardCacheError::Config(
                "active-sync cluster_id must contain 1..=255 bytes".into(),
            ));
        }
        if self.max_key_bytes == 0
            || self.max_value_bytes == 0
            || self.max_causal_origins_per_shard == 0
            || self.max_pending_records_per_shard == 0
            || self.max_block_records == 0
            || self.max_block_bytes == 0
            || self.max_retained_block_bytes_per_shard == 0
            || self.max_pending_block_gaps_per_shard == 0
            || self.max_snapshot_bytes == 0
            || self.max_conflict_order_retries == 0
        {
            return Err(ShardCacheError::Config(
                "active-sync resource limits must be nonzero".into(),
            ));
        }
        Ok(())
    }
}

/// Bounds one caller-driven synchronization round.
#[derive(Debug, Clone)]
pub struct SyncOptions {
    pub max_blocks: usize,
    pub max_bytes: usize,
}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            max_blocks: 128,
            max_bytes: 16 * 1024 * 1024,
        }
    }
}

/// Result of one direct, bidirectional synchronization round.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BidirectionalSyncReport {
    pub blocks_to_local: usize,
    pub blocks_to_peer: usize,
    pub bytes_to_local: usize,
    pub bytes_to_peer: usize,
    pub applied_mutations: usize,
    pub duplicate_mutations: usize,
    pub conflicts: usize,
    pub state_snapshot_fallbacks: usize,
    pub truncated: bool,
}

/// Exact local-residency eviction result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionOutcome {
    Evicted,
    Stale,
    Missing,
    NoRecoverySource,
    AlreadyEvicted,
}

/// Bounded health information that does not expose keys or credentials.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActiveSyncHealthSnapshot {
    pub consistency_mode: ActiveConsistencyMode,
    pub shard_count: usize,
    pub live_versions: usize,
    pub tombstones: usize,
    pub evicted_versions: usize,
    pub conflicted_versions: usize,
    pub pending_records: usize,
    pub retained_blocks: usize,
    pub retained_block_bytes: usize,
    pub conflict_order_requests: u64,
    pub conflict_order_failures: u64,
    pub conflict_order_retries: u64,
    pub conflict_ordered: u64,
}

#[derive(Clone)]
pub struct ActiveShardMap {
    inner: Arc<ActiveShardMapInner>,
}

struct ActiveShardMapInner {
    store: EmbeddedStore,
    config: ActiveSyncConfig,
    shards: Box<[Mutex<ActiveShardState>]>,
    local_origins: Box<[Arc<CausalOrigin>]>,
    special_reads: Box<[AtomicBool]>,
    // Monotonic so a readable store hit can bypass metadata unless this shard
    // has ever materialized a governance conflict.
    conflict_reads: Box<[AtomicBool]>,
    conflict_orderer: Option<Arc<dyn ConflictOrderer>>,
    conflict_order_requests: AtomicU64,
    conflict_order_failures: AtomicU64,
    conflict_order_retries: AtomicU64,
    conflict_ordered: AtomicU64,
}

impl fmt::Debug for ActiveShardMap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActiveShardMap")
            .field("node_id", &self.inner.config.node_id)
            .field("shard_count", &self.inner.shards.len())
            .field(
                "external_conflict_ordering",
                &self.inner.conflict_orderer.is_some(),
            )
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct CausalOrigin {
    node_id: NodeId,
    incarnation_id: IncarnationId,
    shard_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct CompactMutationDot {
    origin: Arc<CausalOrigin>,
    sequence: u64,
}

impl CompactMutationDot {
    fn from_shared_origin(origin: &Arc<CausalOrigin>, sequence: u64) -> Self {
        Self {
            origin: Arc::clone(origin),
            sequence,
        }
    }

    fn to_public(&self) -> MutationDot {
        MutationDot {
            node_id: self.origin.node_id.clone(),
            incarnation_id: self.origin.incarnation_id,
            shard_id: self.origin.shard_id,
            sequence: self.sequence,
        }
    }

    fn matches_public(&self, other: &MutationDot) -> bool {
        self.sequence == other.sequence
            && self.origin.shard_id == other.shard_id
            && self.origin.incarnation_id == other.incarnation_id
            && self.origin.node_id == other.node_id
    }
}

impl Deref for CompactMutationDot {
    type Target = CausalOrigin;

    fn deref(&self) -> &Self::Target {
        &self.origin
    }
}

impl From<&MutationDot> for CompactMutationDot {
    fn from(dot: &MutationDot) -> Self {
        Self {
            origin: Arc::new(CausalOrigin::from(dot)),
            sequence: dot.sequence,
        }
    }
}

impl PartialEq<MutationDot> for CompactMutationDot {
    fn eq(&self, other: &MutationDot) -> bool {
        self.matches_public(other)
    }
}

impl PartialEq<CompactMutationDot> for MutationDot {
    fn eq(&self, other: &CompactMutationDot) -> bool {
        other.matches_public(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CausalFrontier {
    origin: Arc<CausalOrigin>,
    sequence: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
// Local writes normally observe one origin. Keeping four full origins inline
// made every live version and retained mutation substantially larger, which in
// turn amplified cache misses and pending-block relocation costs.
struct CausalContext(SmallVec<[CausalFrontier; 1]>);

impl CausalContext {
    fn observes(&self, dot: &CompactMutationDot) -> bool {
        self.0
            .binary_search_by(|frontier| frontier.origin.as_ref().cmp(dot.origin.as_ref()))
            .ok()
            .is_some_and(|index| self.0[index].sequence >= dot.sequence)
    }

    fn observe(&mut self, dot: &CompactMutationDot) {
        self.observe_shared_origin(&dot.origin, dot.sequence);
    }

    fn join(&mut self, other: &Self) {
        for frontier in &other.0 {
            self.observe_origin(frontier.origin.as_ref().clone(), frontier.sequence);
        }
    }

    fn observe_origin(&mut self, origin: CausalOrigin, sequence: u64) -> bool {
        match self
            .0
            .binary_search_by(|frontier| frontier.origin.as_ref().cmp(&origin))
        {
            Ok(index) => {
                self.0[index].sequence = self.0[index].sequence.max(sequence);
                false
            }
            Err(index) => {
                self.0.insert(
                    index,
                    CausalFrontier {
                        origin: Arc::new(origin),
                        sequence,
                    },
                );
                true
            }
        }
    }

    fn observe_shared_origin(&mut self, origin: &Arc<CausalOrigin>, sequence: u64) -> bool {
        if self.0.len() == 1 && Arc::ptr_eq(&self.0[0].origin, origin) {
            self.0[0].sequence = self.0[0].sequence.max(sequence);
            return false;
        }
        match self
            .0
            .binary_search_by(|frontier| frontier.origin.as_ref().cmp(origin.as_ref()))
        {
            Ok(index) => {
                self.0[index].sequence = self.0[index].sequence.max(sequence);
                false
            }
            Err(index) => {
                self.0.insert(
                    index,
                    CausalFrontier {
                        origin: Arc::clone(origin),
                        sequence,
                    },
                );
                true
            }
        }
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn iter(&self) -> impl Iterator<Item = (&CausalOrigin, u64)> {
        self.0
            .iter()
            .map(|frontier| (frontier.origin.as_ref(), frontier.sequence))
    }
}

impl From<&MutationDot> for CausalOrigin {
    fn from(dot: &MutationDot) -> Self {
        Self {
            node_id: dot.node_id.clone(),
            incarnation_id: dot.incarnation_id,
            shard_id: dot.shard_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TombstoneKind {
    Delete,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MutationKind {
    Set,
    Tombstone(TombstoneKind),
    // Cluster eviction is rare; boxing its large dot keeps every ordinary SET
    // and tombstone record compact.
    ClusterEvict { target: Box<MutationDot> },
}

#[derive(Debug, Clone)]
struct ActiveMutation {
    dot: CompactMutationDot,
    hlc: HybridLogicalClock,
    context: CausalContext,
    key_hash: u64,
    key: SharedBytes,
    value: Option<SharedBytes>,
    expire_at_ms: CompactExpiration,
    governance: Option<SharedBytes>,
    kind: MutationKind,
}

impl ActiveMutation {
    fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.key.len())
            .saturating_add(self.value.as_ref().map_or(0, SharedBytes::len))
            .saturating_add(self.governance.as_ref().map_or(0, SharedBytes::len))
            .saturating_add(self.context.len().saturating_mul(96))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
struct Residency(u64);

impl Residency {
    const RESIDENT: Self = Self(0);
    const CLUSTER_EVICTED: Self = Self(u64::MAX);

    fn evicted(generation: u64) -> Self {
        // MAX is reserved for ClusterEvicted. Reaching this saturation point
        // would require more than 2^64 eviction cycles for one exact version.
        Self(generation.saturating_add(1).min(u64::MAX - 1))
    }

    fn evicted_generation(self) -> Option<u64> {
        (self != Self::RESIDENT && self != Self::CLUSTER_EVICTED).then(|| self.0 - 1)
    }

    fn is_resident(self) -> bool {
        self == Self::RESIDENT
    }

    fn is_evicted(self) -> bool {
        self.evicted_generation().is_some()
    }
}

#[derive(Debug, Clone)]
struct VersionState {
    dot: CompactMutationDot,
    hlc: HybridLogicalClock,
    context: CausalContext,
    kind: MutationKind,
    expire_at_ms: CompactExpiration,
    governance: Option<SharedBytes>,
    residency: Residency,
    // Local versions have no recovery peer. Allocate the set only after a
    // remote copy is actually acknowledged.
    recovery_peers: Option<Box<RecoveryPeerSet>>,
    governance_conflict: bool,
}

#[derive(Debug, Clone)]
struct RecoveryPeerSet {
    peers: HashSet<NodeId>,
}

impl VersionState {
    fn from_mutation(mutation: &ActiveMutation, source_peer: Option<&NodeId>) -> Self {
        let residency = match mutation.kind {
            MutationKind::Set if mutation.value.is_some() => Residency::RESIDENT,
            MutationKind::Set => Residency::evicted(0),
            MutationKind::Tombstone(_) => Residency::RESIDENT,
            MutationKind::ClusterEvict { .. } => Residency::CLUSTER_EVICTED,
        };
        let recovery_peers = if mutation.value.is_some()
            && matches!(mutation.kind, MutationKind::Set)
            && let Some(peer) = source_peer
        {
            Some(Box::new(RecoveryPeerSet {
                peers: HashSet::from([peer.clone()]),
            }))
        } else {
            None
        };
        Self {
            dot: mutation.dot.clone(),
            hlc: mutation.hlc,
            context: mutation.context.clone(),
            kind: mutation.kind.clone(),
            expire_at_ms: mutation.expire_at_ms,
            governance: mutation.governance.clone(),
            residency,
            recovery_peers,
            governance_conflict: false,
        }
    }

    fn full_context(&self) -> CausalContext {
        let mut context = self.context.clone();
        context.observe(&self.dot);
        context
    }

    fn full_context_with_local_origin(&self, local_origin: &Arc<CausalOrigin>) -> CausalContext {
        let mut context = self.context.clone();
        if Arc::ptr_eq(local_origin, &self.dot.origin)
            || local_origin.as_ref() == self.dot.origin.as_ref()
        {
            context.observe_shared_origin(local_origin, self.dot.sequence);
        } else {
            context.observe(&self.dot);
        }
        context
    }

    fn has_recovery_peer(&self) -> bool {
        self.recovery_peers
            .as_ref()
            .is_some_and(|peers| !peers.peers.is_empty())
    }

    fn add_recovery_peer(&mut self, peer: NodeId) {
        self.recovery_peers
            .get_or_insert_with(|| {
                Box::new(RecoveryPeerSet {
                    peers: HashSet::new(),
                })
            })
            .peers
            .insert(peer);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct BlockOrigin {
    node_id: NodeId,
    incarnation_id: IncarnationId,
    shard_id: u32,
}

#[derive(Debug, Clone)]
struct SyncBlock {
    cluster_id: Box<str>,
    origin: BlockOrigin,
    sequence: u64,
    digest: OnceLock<[u8; 32]>,
    // Keep the Vec allocation so sealing is an O(1) ownership transfer. An
    // Arc<[T]> conversion may relocate every large mutation while the shard is
    // write locked.
    records: Arc<Vec<ActiveMutation>>,
    encoded_bytes: usize,
}

impl SyncBlock {
    fn digest(&self) -> [u8; 32] {
        *self.digest.get_or_init(|| {
            block_digest(&self.cluster_id, &self.origin, self.sequence, &self.records)
        })
    }

    fn digest_is_valid(&self) -> bool {
        let computed = block_digest(&self.cluster_id, &self.origin, self.sequence, &self.records);
        match self.digest.get() {
            Some(expected) => *expected == computed,
            None => {
                let _ = self.digest.set(computed);
                true
            }
        }
    }
}

#[derive(Debug, Default)]
struct ActiveShardState {
    next_mutation_sequence: u64,
    next_block_sequence: u64,
    clock: HybridLogicalClock,
    versions: ActiveVersionMap,
    pending: Vec<ActiveMutation>,
    pending_bytes: usize,
    blocks: VecDeque<Arc<SyncBlock>>,
    retained_block_bytes: usize,
    block_frontiers: BTreeMap<BlockOrigin, u64>,
    pending_block_gaps: BTreeMap<BlockOrigin, BTreeMap<u64, [u8; 32]>>,
}

#[derive(Debug, Default)]
struct ActiveVersionMap {
    entries: HashBrownMap<ActiveVersionKey, VersionState, BuildHasherDefault<Xxh3>>,
}

#[derive(Debug, Eq, PartialEq)]
struct ActiveVersionKey(SharedBytes);

impl Hash for ActiveVersionKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(&self.0);
    }
}

impl ActiveVersionMap {
    #[inline]
    fn get(&self, key: &[u8]) -> Option<&VersionState> {
        self.get_key_value_hashed(hash_key(key), key)
            .map(|(_, version)| version)
    }

    #[inline]
    fn get_mut(&mut self, key: &[u8]) -> Option<&mut VersionState> {
        let hash = hash_key(key);
        match self
            .entries
            .raw_entry_mut()
            .from_hash(hash, |stored_key| stored_key.0.as_ref() == key)
        {
            hashbrown::hash_map::RawEntryMut::Occupied(occupied) => Some(occupied.into_mut()),
            hashbrown::hash_map::RawEntryMut::Vacant(_) => None,
        }
    }

    #[inline]
    fn get_key_value_hashed(&self, hash: u64, key: &[u8]) -> Option<(&SharedBytes, &VersionState)> {
        self.entries
            .raw_entry()
            .from_hash(hash, |stored_key| stored_key.0.as_ref() == key)
            .map(|(stored_key, version)| (&stored_key.0, version))
    }

    #[inline]
    fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    #[inline]
    fn insert(&mut self, key: SharedBytes, version: VersionState) -> Option<VersionState> {
        self.replace_hashed(hash_key(&key), key, version)
    }

    #[inline]
    fn replace_hashed(
        &mut self,
        hash: u64,
        key: SharedBytes,
        replacement: VersionState,
    ) -> Option<VersionState> {
        match self
            .entries
            .raw_entry_mut()
            .from_hash(hash, |stored_key| stored_key.0.as_ref() == key.as_ref())
        {
            hashbrown::hash_map::RawEntryMut::Occupied(mut occupied) => {
                Some(std::mem::replace(occupied.get_mut(), replacement))
            }
            hashbrown::hash_map::RawEntryMut::Vacant(vacant) => {
                vacant.insert_hashed_nocheck(hash, ActiveVersionKey(key), replacement);
                None
            }
        }
    }

    fn iter(&self) -> impl Iterator<Item = (&SharedBytes, &VersionState)> {
        self.entries.iter().map(|(key, version)| (&key.0, version))
    }

    fn values(&self) -> impl Iterator<Item = &VersionState> {
        self.entries.values()
    }
}

#[derive(Debug, Default)]
struct ApplyStats {
    applied: usize,
    duplicates: usize,
    conflicts: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct ActiveSnapshotDocument {
    cluster_id: String,
    node_id: String,
    incarnation_id: u128,
    #[serde(default)]
    block_frontiers: Vec<SnapshotFrontier>,
    versions: Vec<SnapshotVersion>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotVersion {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    dot: SnapshotDot,
    hlc_physical_ms: u64,
    hlc_logical: u32,
    context: Vec<SnapshotFrontier>,
    kind: SnapshotKind,
    expire_at_ms: Option<u64>,
    governance: Option<Vec<u8>>,
    governance_conflict: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotDot {
    node_id: String,
    incarnation_id: u128,
    shard_id: u32,
    sequence: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotFrontier {
    node_id: String,
    incarnation_id: u128,
    shard_id: u32,
    sequence: u64,
}

#[derive(Debug, Serialize, Deserialize)]
enum SnapshotKind {
    Set,
    Delete,
    Expired,
    ClusterEvict { target: SnapshotDot },
}

impl ActiveShardMap {
    /// Builds a causally ordered map that converges after synchronization and
    /// eventual message delivery.
    pub fn new_causal_eventual(shard_count: usize, config: ActiveSyncConfig) -> Result<Self> {
        Self::new_inner(shard_count, config, None)
    }

    /// Backwards-compatible alias for [`Self::new_causal_eventual`].
    pub fn new(shard_count: usize, config: ActiveSyncConfig) -> Result<Self> {
        Self::new_causal_eventual(shard_count, config)
    }

    /// Builds an eventually consistent map whose ambiguous concurrent
    /// conflicts require an externally finalized total order.
    #[cfg(feature = "active-sync-consensus-ordered-eventual")]
    pub fn new_consensus_ordered_eventual(
        shard_count: usize,
        config: ActiveSyncConfig,
        conflict_orderer: Arc<dyn ConflictOrderer>,
    ) -> Result<Self> {
        Self::new_inner(shard_count, config, Some(conflict_orderer))
    }

    /// Backwards-compatible alias for
    /// [`Self::new_consensus_ordered_eventual`].
    #[cfg(feature = "active-sync-consensus-ordered-eventual")]
    pub fn new_with_conflict_orderer(
        shard_count: usize,
        config: ActiveSyncConfig,
        conflict_orderer: Arc<dyn ConflictOrderer>,
    ) -> Result<Self> {
        Self::new_consensus_ordered_eventual(shard_count, config, conflict_orderer)
    }

    fn new_inner(
        shard_count: usize,
        config: ActiveSyncConfig,
        conflict_orderer: Option<Arc<dyn ConflictOrderer>>,
    ) -> Result<Self> {
        config.validate()?;
        if shard_count == 0 || !shard_count.is_power_of_two() {
            return Err(ShardCacheError::Config(
                "active-sync shard_count must be a nonzero power of two".into(),
            ));
        }
        let shards = (0..shard_count)
            .map(|_| Mutex::new(ActiveShardState::default()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let local_origins = (0..shard_count)
            .map(|shard_id| {
                Arc::new(CausalOrigin {
                    node_id: config.node_id.clone(),
                    incarnation_id: config.incarnation_id,
                    shard_id: shard_id as u32,
                })
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let special_reads = (0..shard_count)
            .map(|_| AtomicBool::new(false))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let conflict_reads = (0..shard_count)
            .map(|_| AtomicBool::new(false))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self {
            inner: Arc::new(ActiveShardMapInner {
                store: EmbeddedStore::new(shard_count),
                config,
                shards,
                local_origins,
                special_reads,
                conflict_reads,
                conflict_orderer,
                conflict_order_requests: AtomicU64::new(0),
                conflict_order_failures: AtomicU64::new(0),
                conflict_order_retries: AtomicU64::new(0),
                conflict_ordered: AtomicU64::new(0),
            }),
        })
    }

    pub fn shard_count(&self) -> usize {
        self.inner.shards.len()
    }

    pub fn node_id(&self) -> &NodeId {
        &self.inner.config.node_id
    }

    /// Returns the configured cross-node conflict guarantee.
    pub fn consistency_mode(&self) -> ActiveConsistencyMode {
        if self.inner.conflict_orderer.is_some() {
            ActiveConsistencyMode::ConsensusOrderedEventual
        } else {
            ActiveConsistencyMode::CausalEventual
        }
    }

    pub fn set(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<SyncToken> {
        self.set_shared(
            key.as_ref(),
            SharedBytes::copy_from_slice(value.as_ref()),
            None,
            None,
        )
    }

    /// Stores an already-owned value without copying its payload.
    pub fn set_value_bytes(&self, key: impl AsRef<[u8]>, value: SharedBytes) -> Result<SyncToken> {
        self.set_shared(key.as_ref(), value, None, None)
    }

    pub fn set_with_ttl(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        ttl: Duration,
    ) -> Result<SyncToken> {
        self.set_value_bytes_with_ttl(key, SharedBytes::copy_from_slice(value.as_ref()), ttl)
    }

    /// Stores an already-owned value with a TTL without copying its payload.
    pub fn set_value_bytes_with_ttl(
        &self,
        key: impl AsRef<[u8]>,
        value: SharedBytes,
        ttl: Duration,
    ) -> Result<SyncToken> {
        let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
        self.set_shared(
            key.as_ref(),
            value,
            Some(now_millis().saturating_add(ttl_ms)),
            None,
        )
    }

    pub fn set_with_governance(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        governance: impl AsRef<[u8]>,
    ) -> Result<SyncToken> {
        self.set_shared(
            key.as_ref(),
            SharedBytes::copy_from_slice(value.as_ref()),
            None,
            Some(governance.as_ref()),
        )
    }

    fn set_shared(
        &self,
        key: &[u8],
        value: SharedBytes,
        expire_at_ms: Option<u64>,
        governance: Option<&[u8]>,
    ) -> Result<SyncToken> {
        self.validate_point(key, &value, governance)?;
        let route = self.inner.store.route_key(key);
        let wall_ms = ttl_now_millis();
        let mut shard = self.inner.shards[route.shard_id].lock();
        let mutation = self.local_mutation(
            &mut shard,
            route.shard_id,
            route.key_hash,
            key,
            MutationKind::Set,
            Some(value),
            expire_at_ms,
            governance,
            wall_ms,
        )?;
        let (retired, mutation_bytes) = self.prepare_local_mutation_queue(&mut shard, &mutation)?;
        self.store_mutation_at(&mutation, wall_ms);
        replace_version_hashed(
            &mut shard.versions,
            route.key_hash,
            mutation.key.clone(),
            VersionState::from_mutation(&mutation, None),
        );
        if expire_at_ms.is_some() || governance.is_some() {
            self.inner.special_reads[route.shard_id].store(true, AtomicOrdering::Release);
        }
        let dot = mutation.dot.to_public();
        self.queue_local_mutation(&mut shard, mutation, mutation_bytes);
        drop(shard);
        drop(retired);
        Ok(SyncToken {
            slot: route.shard_id as u32,
            dot,
        })
    }

    pub fn delete(&self, key: impl AsRef<[u8]>) -> Result<SyncToken> {
        self.tombstone(key.as_ref(), TombstoneKind::Delete)
    }

    fn tombstone(&self, key: &[u8], kind: TombstoneKind) -> Result<SyncToken> {
        self.validate_point(key, &[], None)?;
        let route = self.inner.store.route_key(key);
        let wall_ms = ttl_now_millis();
        let mut shard = self.inner.shards[route.shard_id].lock();
        let mutation = self.local_mutation(
            &mut shard,
            route.shard_id,
            route.key_hash,
            key,
            MutationKind::Tombstone(kind),
            None,
            None,
            None,
            wall_ms,
        )?;
        let (retired, mutation_bytes) = self.prepare_local_mutation_queue(&mut shard, &mutation)?;
        self.store_mutation_at(&mutation, wall_ms);
        replace_version_hashed(
            &mut shard.versions,
            route.key_hash,
            mutation.key.clone(),
            VersionState::from_mutation(&mutation, None),
        );
        let dot = mutation.dot.to_public();
        self.queue_local_mutation(&mut shard, mutation, mutation_bytes);
        drop(shard);
        drop(retired);
        Ok(SyncToken {
            slot: route.shard_id as u32,
            dot,
        })
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<Vec<u8>> {
        let key = key.as_ref();
        let route = self.inner.store.route_key(key);
        if !self.inner.special_reads[route.shard_id].load(AtomicOrdering::Acquire) {
            return self.inner.store.get_routed(route, key);
        }
        let value = self.inner.store.get_routed(route, key);
        if value.is_some()
            && !self.inner.conflict_reads[route.shard_id].load(AtomicOrdering::Acquire)
        {
            return value;
        }
        match self.is_readable_routed(route, key) {
            Ok(true) => value,
            Ok(false) | Err(_) => None,
        }
    }

    pub fn try_get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        let route = self.inner.store.route_key(key);
        if !self.is_readable_routed(route, key)? {
            return Ok(None);
        }
        Ok(self.inner.store.get_routed(route, key))
    }

    pub fn version_dot(&self, key: impl AsRef<[u8]>) -> Option<MutationDot> {
        let key = key.as_ref();
        let route = self.inner.store.route_key(key);
        self.inner.shards[route.shard_id]
            .lock()
            .versions
            .get(key)
            .map(|version| version.dot.to_public())
    }

    pub fn evict_local_exact(
        &self,
        key: impl AsRef<[u8]>,
        expected: &MutationDot,
    ) -> Result<EvictionOutcome> {
        let key = key.as_ref();
        let route = self.inner.store.route_key(key);
        let mut shard = self.inner.shards[route.shard_id].lock();
        let Some(version) = shard.versions.get_mut(key) else {
            return Ok(EvictionOutcome::Missing);
        };
        if &version.dot != expected {
            return Ok(EvictionOutcome::Stale);
        }
        if version.residency.is_evicted() {
            return Ok(EvictionOutcome::AlreadyEvicted);
        }
        if !matches!(version.kind, MutationKind::Set) {
            return Ok(EvictionOutcome::Missing);
        }
        if !version.has_recovery_peer() {
            return Ok(EvictionOutcome::NoRecoverySource);
        }
        let generation = version
            .residency
            .evicted_generation()
            .map_or(1, |generation| generation.saturating_add(1));
        if !self.inner.store.delete(key) {
            return Ok(EvictionOutcome::Missing);
        }
        version.residency = Residency::evicted(generation);
        Ok(EvictionOutcome::Evicted)
    }

    /// Commits a previously coordinated cluster eviction for one exact value.
    ///
    /// Quorum nomination is intentionally an orchestration concern. This
    /// primitive only emits the deterministic, version-targeted causal record.
    /// A stale expected dot returns `Ok(None)` and cannot remove a refresh.
    pub fn commit_cluster_eviction_exact(
        &self,
        key: impl AsRef<[u8]>,
        expected: &MutationDot,
    ) -> Result<Option<SyncToken>> {
        let key = key.as_ref();
        self.validate_point(key, &[], None)?;
        let route = self.inner.store.route_key(key);
        let wall_ms = ttl_now_millis();
        let mut shard = self.inner.shards[route.shard_id].lock();
        let Some(version) = shard.versions.get(key) else {
            return Ok(None);
        };
        if &version.dot != expected || !matches!(version.kind, MutationKind::Set) {
            return Ok(None);
        }
        let mutation = self.local_mutation(
            &mut shard,
            route.shard_id,
            route.key_hash,
            key,
            MutationKind::ClusterEvict {
                target: Box::new(expected.clone()),
            },
            None,
            None,
            None,
            wall_ms,
        )?;
        let (retired, mutation_bytes) = self.prepare_local_mutation_queue(&mut shard, &mutation)?;
        self.store_mutation_at(&mutation, wall_ms);
        replace_version_hashed(
            &mut shard.versions,
            route.key_hash,
            mutation.key.clone(),
            VersionState::from_mutation(&mutation, None),
        );
        let dot = mutation.dot.to_public();
        self.queue_local_mutation(&mut shard, mutation, mutation_bytes);
        drop(shard);
        drop(retired);
        Ok(Some(SyncToken {
            slot: route.shard_id as u32,
            dot,
        }))
    }

    pub fn fault_in_from(&self, key: impl AsRef<[u8]>, peer: &Self) -> Result<Option<Vec<u8>>> {
        self.validate_peer(peer)?;
        let key = key.as_ref();
        let route = self.inner.store.route_key(key);
        let (expected, generation) = {
            let shard = self.inner.shards[route.shard_id].lock();
            let Some(version) = shard.versions.get(key) else {
                return Ok(None);
            };
            let Some(generation) = version.residency.evicted_generation() else {
                return self.try_get(key);
            };
            (version.dot.clone(), generation)
        };
        let Some(value) = peer.read_exact(key, &expected.to_public())? else {
            return Ok(None);
        };
        let mut shard = self.inner.shards[route.shard_id].lock();
        let Some(version) = shard.versions.get_mut(key) else {
            return Ok(None);
        };
        if version.dot != expected || version.residency != Residency::evicted(generation) {
            return Ok(None);
        }
        let mutation = ActiveMutation {
            dot: version.dot.clone(),
            hlc: version.hlc,
            context: version.context.clone(),
            key_hash: route.key_hash,
            key: SharedBytes::copy_from_slice(key),
            value: Some(SharedBytes::copy_from_slice(&value)),
            expire_at_ms: version.expire_at_ms,
            governance: version.governance.clone(),
            kind: version.kind.clone(),
        };
        self.store_mutation(&mutation);
        version.residency = Residency::RESIDENT;
        version.add_recovery_peer(peer.node_id().clone());
        Ok(Some(value))
    }

    fn read_exact(&self, key: &[u8], expected: &MutationDot) -> Result<Option<Vec<u8>>> {
        let route = self.inner.store.route_key(key);
        let shard = self.inner.shards[route.shard_id].lock();
        let Some(version) = shard.versions.get(key) else {
            return Ok(None);
        };
        if &version.dot != expected
            || !matches!(version.kind, MutationKind::Set)
            || !version.residency.is_resident()
            || version.governance_conflict
        {
            return Ok(None);
        }
        Ok(self.inner.store.get(key))
    }

    pub fn sync_with(&self, peer: &Self, options: SyncOptions) -> Result<BidirectionalSyncReport> {
        self.validate_peer(peer)?;
        if Arc::ptr_eq(&self.inner, &peer.inner) {
            return Ok(BidirectionalSyncReport::default());
        }
        self.seal_pending()?;
        peer.seal_pending()?;

        let local_frontiers = self.frontiers();
        let peer_frontiers = peer.frontiers();
        let (to_peer, local_gap) = self.missing_blocks(&peer_frontiers, &options);
        let (to_local, peer_gap) = peer.missing_blocks(&local_frontiers, &options);
        let mut report = BidirectionalSyncReport::default();

        if local_gap {
            let snapshot = self.state_snapshot()?;
            report.state_snapshot_fallbacks += 1;
            Self::apply_snapshot(peer, &snapshot, self.node_id(), &mut report)?;
            for shard_id in 0..self.shard_count() {
                peer.accept_snapshot_frontiers(shard_id, &self.shard_frontiers(shard_id)?)?;
            }
        }
        if peer_gap {
            let snapshot = peer.state_snapshot()?;
            report.state_snapshot_fallbacks += 1;
            Self::apply_snapshot(self, &snapshot, peer.node_id(), &mut report)?;
            for shard_id in 0..self.shard_count() {
                self.accept_snapshot_frontiers(shard_id, &peer.shard_frontiers(shard_id)?)?;
            }
        }

        for block in to_peer {
            report.blocks_to_peer += 1;
            report.bytes_to_peer = report.bytes_to_peer.saturating_add(block.encoded_bytes);
            let stats = peer.apply_block(Arc::clone(&block), self.node_id())?;
            report.applied_mutations += stats.applied;
            report.duplicate_mutations += stats.duplicates;
            report.conflicts += stats.conflicts;
        }
        for block in to_local {
            report.blocks_to_local += 1;
            report.bytes_to_local = report.bytes_to_local.saturating_add(block.encoded_bytes);
            let stats = self.apply_block(Arc::clone(&block), peer.node_id())?;
            report.applied_mutations += stats.applied;
            report.duplicate_mutations += stats.duplicates;
            report.conflicts += stats.conflicts;
        }
        report.truncated = self.has_missing_blocks(&peer.frontiers())
            || peer.has_missing_blocks(&self.frontiers());
        self.mark_peer_recovery(peer);
        peer.mark_peer_recovery(self);
        Ok(report)
    }

    pub fn seal_pending(&self) -> Result<usize> {
        let mut sealed = 0;
        for shard_id in 0..self.shard_count() {
            let mut shard = self.inner.shards[shard_id].lock();
            if shard.pending.is_empty() {
                continue;
            }
            let retired = self.seal_shard(&mut shard, shard_id)?;
            drop(shard);
            drop(retired);
            sealed += 1;
        }
        Ok(sealed)
    }

    fn seal_pending_shard(&self, shard_id: usize) -> Result<bool> {
        let shard =
            self.inner.shards.get(shard_id).ok_or_else(|| {
                ShardCacheError::Protocol("active-sync shard is out of range".into())
            })?;
        let mut shard = shard.lock();
        if shard.pending.is_empty() {
            return Ok(false);
        }
        let retired = self.seal_shard(&mut shard, shard_id)?;
        drop(shard);
        drop(retired);
        Ok(true)
    }

    pub fn health_snapshot(&self) -> ActiveSyncHealthSnapshot {
        let mut health = ActiveSyncHealthSnapshot {
            consistency_mode: self.consistency_mode(),
            shard_count: self.shard_count(),
            conflict_order_requests: self
                .inner
                .conflict_order_requests
                .load(AtomicOrdering::Relaxed),
            conflict_order_failures: self
                .inner
                .conflict_order_failures
                .load(AtomicOrdering::Relaxed),
            conflict_order_retries: self
                .inner
                .conflict_order_retries
                .load(AtomicOrdering::Relaxed),
            conflict_ordered: self.inner.conflict_ordered.load(AtomicOrdering::Relaxed),
            ..ActiveSyncHealthSnapshot::default()
        };
        for shard in &self.inner.shards {
            let shard = shard.lock();
            health.pending_records += shard.pending.len();
            health.retained_blocks += shard.blocks.len();
            health.retained_block_bytes += shard.retained_block_bytes;
            for version in shard.versions.values() {
                match version.kind {
                    MutationKind::Set => health.live_versions += 1,
                    MutationKind::Tombstone(_) => health.tombstones += 1,
                    MutationKind::ClusterEvict { .. } => health.evicted_versions += 1,
                }
                if version.residency.is_evicted() {
                    health.evicted_versions += 1;
                }
                if version.governance_conflict {
                    health.conflicted_versions += 1;
                }
            }
        }
        health
    }

    /// Writes a checksummed recovery snapshot using an atomic rename.
    ///
    /// Every logical value must be materializable. In particular, peer-only
    /// residency stubs make this operation fail until the exact value is
    /// faulted back in.
    pub fn save_snapshot(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        self.seal_pending()?;
        let document = self.snapshot_document()?;
        let payload = serde_json::to_vec(&document).map_err(|error| {
            ShardCacheError::Persistence(format!("failed to encode active-sync snapshot: {error}"))
        })?;
        if payload.len() > self.inner.config.max_snapshot_bytes {
            return Err(ShardCacheError::Backpressure(
                "active-sync snapshot byte limit reached",
            ));
        }
        let mut encoded = Vec::with_capacity(SNAPSHOT_HEADER_BYTES.saturating_add(payload.len()));
        encoded.extend_from_slice(SNAPSHOT_MAGIC);
        encoded.push(1);
        encoded.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        encoded.extend_from_slice(&Sha256::digest(&payload));
        encoded.extend_from_slice(&payload);

        let temp_name = format!(
            ".{}.active-sync-{}-{}.tmp",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("snapshot"),
            std::process::id(),
            INCARNATION_COUNTER.fetch_add(1, AtomicOrdering::Relaxed)
        );
        let temp_path = path.with_file_name(temp_name);
        let write_result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)?;
            file.write_all(&encoded)?;
            file.sync_all()?;
            fs::rename(&temp_path, path)?;
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                let directory = fs::File::open(parent)?;
                directory.sync_all()?;
            }
            Ok(())
        })();
        if write_result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }
        write_result
    }

    /// Restores an active map from a checksummed recovery snapshot.
    ///
    /// `config.incarnation_id` must be fresh. The stable node and cluster IDs
    /// must match the snapshot, preventing accidental cross-cluster replay.
    pub fn load_snapshot(
        shard_count: usize,
        config: ActiveSyncConfig,
        path: impl AsRef<Path>,
    ) -> Result<Self> {
        config.validate()?;
        let path = path.as_ref();
        let metadata = fs::metadata(path)?;
        let file_len = usize::try_from(metadata.len()).map_err(|_| {
            ShardCacheError::Persistence("active-sync snapshot is too large".into())
        })?;
        let maximum_file_len = config
            .max_snapshot_bytes
            .checked_add(SNAPSHOT_HEADER_BYTES)
            .ok_or_else(|| {
                ShardCacheError::Config("active-sync snapshot limit overflows usize".into())
            })?;
        if file_len > maximum_file_len || file_len < SNAPSHOT_HEADER_BYTES {
            return Err(ShardCacheError::Persistence(
                "active-sync snapshot size is outside configured bounds".into(),
            ));
        }
        let mut encoded = Vec::with_capacity(file_len);
        fs::File::open(path)?.read_to_end(&mut encoded)?;
        if encoded.len() != file_len || &encoded[..4] != SNAPSHOT_MAGIC || encoded[4] != 1 {
            return Err(ShardCacheError::Persistence(
                "active-sync snapshot header is invalid".into(),
            ));
        }
        let payload_len = u64::from_le_bytes(encoded[5..13].try_into().map_err(|_| {
            ShardCacheError::Persistence("active-sync snapshot length is invalid".into())
        })?);
        let payload_len = usize::try_from(payload_len).map_err(|_| {
            ShardCacheError::Persistence("active-sync snapshot payload is too large".into())
        })?;
        if payload_len > config.max_snapshot_bytes
            || SNAPSHOT_HEADER_BYTES.saturating_add(payload_len) != encoded.len()
        {
            return Err(ShardCacheError::Persistence(
                "active-sync snapshot payload length is invalid".into(),
            ));
        }
        let payload = &encoded[SNAPSHOT_HEADER_BYTES..];
        let expected_digest = &encoded[13..45];
        if Sha256::digest(payload).as_slice() != expected_digest {
            return Err(ShardCacheError::Persistence(
                "active-sync snapshot checksum mismatch".into(),
            ));
        }
        let document: ActiveSnapshotDocument =
            serde_json::from_slice(payload).map_err(|error| {
                ShardCacheError::Persistence(format!(
                    "failed to decode active-sync snapshot: {error}"
                ))
            })?;
        if document.cluster_id.as_str() != config.cluster_id.as_ref()
            || document.node_id.as_str() != config.node_id.as_str()
        {
            return Err(ShardCacheError::Config(
                "active-sync snapshot identity does not match configuration".into(),
            ));
        }
        if document.incarnation_id == config.incarnation_id.0 {
            return Err(ShardCacheError::Config(
                "active-sync recovery requires a fresh incarnation ID".into(),
            ));
        }

        let map = Self::new(shard_count, config)?;
        map.restore_document(document)?;
        Ok(map)
    }

    fn validate_point(&self, key: &[u8], value: &[u8], governance: Option<&[u8]>) -> Result<()> {
        if key.is_empty() || key.len() > self.inner.config.max_key_bytes {
            return Err(ShardCacheError::Protocol(
                "active-sync key exceeds configured bounds".into(),
            ));
        }
        if value.len() > self.inner.config.max_value_bytes {
            return Err(ShardCacheError::Protocol(
                "active-sync value exceeds configured bounds".into(),
            ));
        }
        if governance
            .is_some_and(|metadata| metadata.len() > self.inner.config.max_governance_bytes)
        {
            return Err(ShardCacheError::Protocol(
                "active-sync governance metadata exceeds configured bounds".into(),
            ));
        }
        Ok(())
    }

    fn snapshot_document(&self) -> Result<ActiveSnapshotDocument> {
        let mut versions = Vec::new();
        let mut block_frontiers = Vec::new();
        let mut estimated_bytes = 0usize;
        for shard in &self.inner.shards {
            let shard = shard.lock();
            for (origin, sequence) in &shard.block_frontiers {
                block_frontiers.push(SnapshotFrontier::from_block_origin(origin, *sequence));
            }
            for (key, version) in shard.versions.iter() {
                let value = match version.kind {
                    MutationKind::Set => {
                        if !version.residency.is_resident() {
                            return Err(ShardCacheError::Persistence(format!(
                                "active-sync value for key hash {} is not materialized",
                                hash_key(key)
                            )));
                        }
                        Some(self.inner.store.get(key).ok_or_else(|| {
                            ShardCacheError::Persistence(format!(
                                "active-sync resident value for key hash {} is missing",
                                hash_key(key)
                            ))
                        })?)
                    }
                    MutationKind::Tombstone(_) | MutationKind::ClusterEvict { .. } => None,
                };
                let snapshot = SnapshotVersion::from_state(key, value, version);
                estimated_bytes = estimated_bytes
                    .checked_add(snapshot.estimated_bytes())
                    .ok_or(ShardCacheError::Backpressure(
                        "active-sync snapshot byte accounting overflowed",
                    ))?;
                if estimated_bytes > self.inner.config.max_snapshot_bytes {
                    return Err(ShardCacheError::Backpressure(
                        "active-sync snapshot byte limit reached",
                    ));
                }
                versions.push(snapshot);
            }
        }
        Ok(ActiveSnapshotDocument {
            cluster_id: self.inner.config.cluster_id.to_string(),
            node_id: self.node_id().to_string(),
            incarnation_id: self.inner.config.incarnation_id.0,
            block_frontiers,
            versions,
        })
    }

    fn restore_document(&self, document: ActiveSnapshotDocument) -> Result<()> {
        for frontier in document.block_frontiers {
            let (origin, sequence) = frontier.to_block_origin()?;
            let shard_id = origin.shard_id as usize;
            let shard = self.inner.shards.get(shard_id).ok_or_else(|| {
                ShardCacheError::Persistence(
                    "active-sync snapshot frontier is assigned to a missing shard".into(),
                )
            })?;
            let mut shard = shard.lock();
            if !shard.block_frontiers.contains_key(&origin)
                && shard.block_frontiers.len() >= self.inner.config.max_causal_origins_per_shard
            {
                return Err(ShardCacheError::Backpressure(
                    "active-sync snapshot origin limit reached",
                ));
            }
            if shard.block_frontiers.insert(origin, sequence).is_some() {
                return Err(ShardCacheError::Persistence(
                    "active-sync snapshot contains a duplicate block frontier".into(),
                ));
            }
        }
        for snapshot in document.versions {
            let governance_conflict = snapshot.governance_conflict;
            let mutation = snapshot.to_mutation()?;
            self.validate_incoming_mutation(&mutation)?;
            let route = self
                .inner
                .store
                .route_key_prehashed(mutation.key_hash, &mutation.key);
            if route.shard_id != mutation.dot.shard_id as usize {
                return Err(ShardCacheError::Persistence(
                    "active-sync snapshot mutation is assigned to the wrong shard".into(),
                ));
            }
            self.ensure_context_bound(&mutation.context)?;
            let mut shard = self.inner.shards[route.shard_id].lock();
            if shard.versions.contains_key(mutation.key.as_ref()) {
                return Err(ShardCacheError::Persistence(
                    "active-sync snapshot contains a duplicate key".into(),
                ));
            }
            if mutation.expire_at_ms.is_some()
                || mutation.governance.is_some()
                || governance_conflict
            {
                self.inner.special_reads[route.shard_id].store(true, AtomicOrdering::Release);
            }
            if governance_conflict {
                self.inner.conflict_reads[route.shard_id].store(true, AtomicOrdering::Release);
            }
            self.store_mutation(&mutation);
            let mut state = VersionState::from_mutation(&mutation, None);
            state.governance_conflict = governance_conflict;
            shard.versions.insert(mutation.key.clone(), state);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn local_mutation(
        &self,
        shard: &mut ActiveShardState,
        shard_id: usize,
        key_hash: u64,
        key: &[u8],
        kind: MutationKind,
        value: Option<SharedBytes>,
        expire_at_ms: Option<u64>,
        governance: Option<&[u8]>,
        wall_ms: u64,
    ) -> Result<ActiveMutation> {
        let (context, key) = shard
            .versions
            .get_key_value_hashed(key_hash, key)
            .map_or_else(
                || (CausalContext::default(), SharedBytes::copy_from_slice(key)),
                |(stored_key, version)| {
                    (
                        version.full_context_with_local_origin(&self.inner.local_origins[shard_id]),
                        stored_key.clone(),
                    )
                },
            );
        if context.len() > self.inner.config.max_causal_origins_per_shard {
            return Err(ShardCacheError::Backpressure(
                "active-sync causal context limit reached",
            ));
        }
        shard.next_mutation_sequence = shard
            .next_mutation_sequence
            .checked_add(1)
            .ok_or_else(|| ShardCacheError::Persistence("active-sync sequence exhausted".into()))?;
        let dot = CompactMutationDot::from_shared_origin(
            &self.inner.local_origins[shard_id],
            shard.next_mutation_sequence,
        );
        Ok(ActiveMutation {
            dot,
            hlc: shard.clock.tick(wall_ms),
            context,
            key_hash,
            key,
            value,
            expire_at_ms: CompactExpiration::new(expire_at_ms),
            governance: governance.map(SharedBytes::copy_from_slice),
            kind,
        })
    }

    fn prepare_local_mutation_queue(
        &self,
        shard: &mut ActiveShardState,
        mutation: &ActiveMutation,
    ) -> Result<(Vec<Arc<SyncBlock>>, usize)> {
        let bytes = mutation.estimated_bytes();
        if bytes > self.inner.config.max_block_bytes {
            return Err(ShardCacheError::Protocol(
                "active-sync mutation exceeds block byte limit".into(),
            ));
        }
        if !shard.pending.is_empty()
            && (shard.pending.len() >= self.inner.config.max_pending_records_per_shard
                || shard.pending.len() >= self.inner.config.max_block_records
                || shard.pending_bytes.saturating_add(bytes) > self.inner.config.max_block_bytes)
        {
            let shard_id = mutation.dot.shard_id as usize;
            return self
                .seal_shard(shard, shard_id)
                .map(|retired| (retired, bytes));
        }
        Ok((Vec::new(), bytes))
    }

    fn queue_local_mutation(
        &self,
        shard: &mut ActiveShardState,
        mutation: ActiveMutation,
        bytes: usize,
    ) {
        shard.pending_bytes = shard.pending_bytes.saturating_add(bytes);
        shard.pending.push(mutation);
    }

    fn seal_shard(
        &self,
        shard: &mut ActiveShardState,
        shard_id: usize,
    ) -> Result<Vec<Arc<SyncBlock>>> {
        if shard.pending.is_empty() {
            return Ok(Vec::new());
        }
        shard.next_block_sequence = shard.next_block_sequence.checked_add(1).ok_or_else(|| {
            ShardCacheError::Persistence("active-sync block sequence exhausted".into())
        })?;
        let records = std::mem::take(&mut shard.pending);
        let next_capacity = records
            .len()
            .min(self.inner.config.max_pending_records_per_shard)
            .min(self.inner.config.max_block_records);
        shard.pending = Vec::with_capacity(next_capacity);
        let records = Arc::new(records);
        let encoded_bytes = shard.pending_bytes;
        shard.pending_bytes = 0;
        let origin = BlockOrigin {
            node_id: self.inner.config.node_id.clone(),
            incarnation_id: self.inner.config.incarnation_id,
            shard_id: shard_id as u32,
        };
        let block = Arc::new(SyncBlock {
            cluster_id: self.inner.config.cluster_id.clone(),
            origin: origin.clone(),
            sequence: shard.next_block_sequence,
            digest: OnceLock::new(),
            records,
            encoded_bytes,
        });
        shard
            .block_frontiers
            .insert(origin, shard.next_block_sequence);
        Ok(self.retain_block(shard, block))
    }

    fn retain_block(
        &self,
        shard: &mut ActiveShardState,
        block: Arc<SyncBlock>,
    ) -> Vec<Arc<SyncBlock>> {
        let mut retired = Vec::new();
        shard.retained_block_bytes = shard
            .retained_block_bytes
            .saturating_add(block.encoded_bytes);
        shard.blocks.push_back(block);
        while shard.retained_block_bytes > self.inner.config.max_retained_block_bytes_per_shard {
            let Some(removed) = shard.blocks.pop_front() else {
                break;
            };
            shard.retained_block_bytes = shard
                .retained_block_bytes
                .saturating_sub(removed.encoded_bytes);
            retired.push(removed);
        }
        retired
    }

    fn store_mutation(&self, mutation: &ActiveMutation) {
        self.store_mutation_at(mutation, now_millis());
    }

    fn store_mutation_at(&self, mutation: &ActiveMutation, now_ms: u64) {
        let route = self
            .inner
            .store
            .route_key_prehashed(mutation.key_hash, &mutation.key);
        match mutation.kind {
            MutationKind::Set => {
                if let Some(value) = &mutation.value {
                    if mutation.governance.is_none() && mutation.expire_at_ms.is_none() {
                        self.inner.store.set_value_bytes_routed_no_ttl_then(
                            route,
                            &mutation.key,
                            value.clone(),
                            || {},
                        );
                    } else {
                        self.inner
                            .store
                            .set_value_bytes_routed_with_governance_then(
                                route,
                                &mutation.key,
                                value.clone(),
                                mutation.governance.clone(),
                                mutation.expire_at_ms.get(),
                                now_ms,
                                || {},
                            );
                    }
                }
            }
            MutationKind::Tombstone(_) | MutationKind::ClusterEvict { .. } => {
                self.inner
                    .store
                    .delete_routed_then(route, &mutation.key, now_ms, || {});
            }
        }
    }

    fn apply_block(&self, block: Arc<SyncBlock>, source_peer: &NodeId) -> Result<ApplyStats> {
        if block.cluster_id.as_ref() != self.inner.config.cluster_id.as_ref() {
            return Err(ShardCacheError::Protocol(
                "active-sync block belongs to another cluster".into(),
            ));
        }
        let decoded_bytes = block.records.iter().try_fold(0usize, |total, record| {
            total.checked_add(record.estimated_bytes())
        });
        if block.sequence == 0
            || block.records.is_empty()
            || block.records.len() > self.inner.config.max_block_records
            || decoded_bytes.is_none_or(|bytes| bytes > self.inner.config.max_block_bytes)
            || !block.digest_is_valid()
        {
            return Err(ShardCacheError::Protocol(
                "active-sync block failed bounds or digest validation".into(),
            ));
        }
        let shard_id = block.origin.shard_id as usize;
        let Some(shard_lock) = self.inner.shards.get(shard_id) else {
            return Err(ShardCacheError::Protocol(
                "active-sync block shard is out of range".into(),
            ));
        };
        for mutation in block.records.iter() {
            if mutation.dot.node_id != block.origin.node_id
                || mutation.dot.incarnation_id != block.origin.incarnation_id
                || mutation.dot.shard_id != block.origin.shard_id
            {
                return Err(ShardCacheError::Protocol(
                    "active-sync block record provenance does not match its origin".into(),
                ));
            }
            self.validate_incoming_mutation(mutation)?;
        }
        let mut shard = shard_lock.lock();
        if !shard.block_frontiers.contains_key(&block.origin)
            && shard.block_frontiers.len() >= self.inner.config.max_causal_origins_per_shard
        {
            return Err(ShardCacheError::Backpressure(
                "active-sync block origin limit reached",
            ));
        }
        let frontier = shard
            .block_frontiers
            .get(&block.origin)
            .copied()
            .unwrap_or(0);
        if block.sequence <= frontier {
            return Ok(ApplyStats {
                duplicates: block.records.len(),
                ..ApplyStats::default()
            });
        }
        if let Some(existing) = shard
            .pending_block_gaps
            .get(&block.origin)
            .and_then(|sequences| sequences.get(&block.sequence))
        {
            if existing != &block.digest() {
                return Err(ShardCacheError::Protocol(
                    "active-sync block sequence has conflicting digests".into(),
                ));
            }
            return Ok(ApplyStats {
                duplicates: block.records.len(),
                ..ApplyStats::default()
            });
        }
        let pending_gap_count = shard
            .pending_block_gaps
            .values()
            .map(BTreeMap::len)
            .sum::<usize>();
        if block.sequence > frontier.saturating_add(1)
            && pending_gap_count >= self.inner.config.max_pending_block_gaps_per_shard
        {
            return Err(ShardCacheError::Backpressure(
                "active-sync pending block gap limit reached",
            ));
        }
        let mut stats = ApplyStats::default();
        for mutation in block.records.iter() {
            let mut retries = 0usize;
            loop {
                let claim = self.ambiguous_conflict_claim(&shard, mutation);
                let Some((orderer, claim)) = self.inner.conflict_orderer.as_ref().zip(claim) else {
                    self.apply_remote_mutation(
                        &mut shard,
                        mutation,
                        source_peer,
                        &mut stats,
                        None,
                    )?;
                    break;
                };

                // Consensus can take an epoch. Never pin a primary shard while
                // the ordering plane is unavailable or waiting for quorum.
                drop(shard);
                let decision = self.order_conflict(orderer.as_ref(), &claim)?;
                shard = shard_lock.lock();

                let current_claim = self.ambiguous_conflict_claim(&shard, mutation);
                if current_claim.as_ref().map(ConflictClaim::digest) != Some(claim.digest()) {
                    retries = retries.saturating_add(1);
                    self.inner
                        .conflict_order_retries
                        .fetch_add(1, AtomicOrdering::Relaxed);
                    if retries >= self.inner.config.max_conflict_order_retries {
                        return Err(ShardCacheError::Backpressure(
                            "active-sync conflict changed while awaiting consensus",
                        ));
                    }
                    continue;
                }
                self.apply_remote_mutation(
                    &mut shard,
                    mutation,
                    source_peer,
                    &mut stats,
                    Some(decision.winner()),
                )?;
                self.inner
                    .conflict_ordered
                    .fetch_add(1, AtomicOrdering::Relaxed);
                break;
            }
        }
        if shard
            .block_frontiers
            .get(&block.origin)
            .is_some_and(|frontier| block.sequence <= *frontier)
        {
            return Ok(stats);
        }
        shard.clock.observe(
            block
                .records
                .iter()
                .map(|record| record.hlc)
                .max()
                .unwrap_or_default(),
            now_millis(),
        );
        observe_block_sequence(&mut shard, &block.origin, block.sequence, block.digest());
        let retired = self.retain_block(&mut shard, block);
        drop(shard);
        drop(retired);
        Ok(stats)
    }

    fn apply_remote_mutation(
        &self,
        shard: &mut ActiveShardState,
        incoming: &ActiveMutation,
        source_peer: &NodeId,
        stats: &mut ApplyStats,
        conflict_winner: Option<&MutationDot>,
    ) -> Result<()> {
        self.validate_incoming_mutation(incoming)?;
        let route = self
            .inner
            .store
            .route_key_prehashed(incoming.key_hash, &incoming.key);
        if incoming.expire_at_ms.is_some() || incoming.governance.is_some() {
            self.inner.special_reads[route.shard_id].store(true, AtomicOrdering::Release);
        }

        let Some(local) = shard.versions.get_mut(incoming.key.as_ref()) else {
            let state = VersionState::from_mutation(incoming, Some(source_peer));
            self.store_mutation(incoming);
            shard.versions.insert(incoming.key.clone(), state);
            stats.applied += 1;
            return Ok(());
        };
        if local.dot == incoming.dot {
            local.context.join(&incoming.context);
            if incoming.value.is_some() {
                local.add_recovery_peer(source_peer.clone());
            }
            stats.duplicates += 1;
            return Ok(());
        }

        let incoming_dominates = incoming.context.observes(&local.dot);
        let local_dominates = local.context.observes(&incoming.dot);
        if incoming_dominates && !local_dominates {
            let mut replacement = VersionState::from_mutation(incoming, Some(source_peer));
            replacement.context.join(&local.context);
            replacement.context.observe(&local.dot);
            self.ensure_context_bound(&replacement.context)?;
            self.store_mutation(incoming);
            *local = replacement;
            stats.applied += 1;
            return Ok(());
        }
        if local_dominates && !incoming_dominates {
            let mut joined = local.context.clone();
            joined.join(&incoming.context);
            joined.observe(&incoming.dot);
            self.ensure_context_bound(&joined)?;
            local.context = joined;
            stats.duplicates += 1;
            return Ok(());
        }

        stats.conflicts += 1;
        let governance_conflict = local.governance.as_deref() != incoming.governance.as_deref();
        if governance_conflict {
            self.inner.conflict_reads[route.shard_id].store(true, AtomicOrdering::Release);
        }
        let incoming_wins = match semantic_concurrent_incoming_wins(local, incoming) {
            Some(incoming_wins) => incoming_wins,
            None => match conflict_winner {
                Some(winner) if winner == &incoming.dot => true,
                Some(winner) if winner == &local.dot => false,
                Some(_) => {
                    return Err(ShardCacheError::Protocol(
                        "active-sync conflict decision selected an unknown mutation".into(),
                    ));
                }
                None => concurrent_clock_incoming_wins(local, incoming),
            },
        };
        let mut joined = local.full_context();
        joined.join(&incoming.context);
        joined.observe(&incoming.dot);
        self.ensure_context_bound(&joined)?;
        if incoming_wins {
            let mut replacement = VersionState::from_mutation(incoming, Some(source_peer));
            replacement.context = joined;
            replacement.governance_conflict = governance_conflict || local.governance_conflict;
            self.store_mutation(incoming);
            *local = replacement;
            stats.applied += 1;
        } else {
            local.context = joined;
            local.governance_conflict |= governance_conflict;
            stats.duplicates += 1;
        }
        Ok(())
    }

    fn ambiguous_conflict_claim(
        &self,
        shard: &ActiveShardState,
        incoming: &ActiveMutation,
    ) -> Option<ConflictClaim> {
        let local = shard.versions.get(incoming.key.as_ref())?;
        if local.dot == incoming.dot
            || incoming.context.observes(&local.dot)
            || local.context.observes(&incoming.dot)
            || semantic_concurrent_incoming_wins(local, incoming).is_some()
        {
            return None;
        }
        Some(ConflictClaim::new(
            self.inner.config.cluster_id.as_bytes(),
            incoming.dot.shard_id,
            incoming.key.as_ref(),
            ConflictCandidate {
                dot: local.dot.to_public(),
                class: conflict_mutation_class(&local.kind),
            },
            ConflictCandidate {
                dot: incoming.dot.to_public(),
                class: conflict_mutation_class(&incoming.kind),
            },
        ))
    }

    fn order_conflict(
        &self,
        orderer: &dyn ConflictOrderer,
        claim: &ConflictClaim,
    ) -> Result<ConflictDecision> {
        self.inner
            .conflict_order_requests
            .fetch_add(1, AtomicOrdering::Relaxed);
        let result = orderer
            .decide(claim)
            .and_then(|decision| decision.validate_for(claim).map(|()| decision));
        if result.is_err() {
            self.inner
                .conflict_order_failures
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
        result
    }

    fn validate_incoming_mutation(&self, incoming: &ActiveMutation) -> Result<()> {
        self.validate_point(
            &incoming.key,
            incoming.value.as_deref().unwrap_or_default(),
            incoming.governance.as_deref(),
        )?;
        let route = self
            .inner
            .store
            .route_key_prehashed(incoming.key_hash, &incoming.key);
        if incoming.dot.sequence == 0
            || route.shard_id != incoming.dot.shard_id as usize
            || incoming.key_hash != hash_key(&incoming.key)
        {
            return Err(ShardCacheError::Protocol(
                "active-sync mutation routing or sequence metadata is invalid".into(),
            ));
        }
        if incoming.context.len() > self.inner.config.max_causal_origins_per_shard
            || incoming
                .context
                .iter()
                .any(|(origin, sequence)| origin.shard_id != incoming.dot.shard_id || sequence == 0)
        {
            return Err(ShardCacheError::Protocol(
                "active-sync causal context exceeds configured bounds".into(),
            ));
        }
        let shape_is_valid = match &incoming.kind {
            MutationKind::Set => incoming.value.is_some(),
            MutationKind::Tombstone(_) => {
                incoming.value.is_none()
                    && incoming.expire_at_ms.is_none()
                    && incoming.governance.is_none()
            }
            MutationKind::ClusterEvict { target } => {
                incoming.value.is_none()
                    && incoming.expire_at_ms.is_none()
                    && incoming.governance.is_none()
                    && target.shard_id == incoming.dot.shard_id
                    && target.sequence != 0
            }
        };
        if !shape_is_valid {
            return Err(ShardCacheError::Protocol(
                "active-sync mutation payload does not match its kind".into(),
            ));
        }
        let wall_ms = now_millis();
        let maximum_remote_ms = wall_ms.saturating_add(
            u64::try_from(self.inner.config.max_clock_skew.as_millis()).unwrap_or(u64::MAX),
        );
        if incoming.hlc.physical_ms > maximum_remote_ms {
            return Err(ShardCacheError::Protocol(
                "active-sync mutation clock exceeds configured skew".into(),
            ));
        }
        Ok(())
    }

    fn ensure_context_bound(&self, context: &CausalContext) -> Result<()> {
        if context.len() > self.inner.config.max_causal_origins_per_shard {
            return Err(ShardCacheError::Backpressure(
                "active-sync causal context limit reached",
            ));
        }
        Ok(())
    }

    fn is_readable_routed(&self, route: EmbeddedKeyRoute, key: &[u8]) -> Result<bool> {
        loop {
            let should_expire = {
                let shard = self.inner.shards[route.shard_id].lock();
                let Some(version) = shard.versions.get(key) else {
                    return Ok(false);
                };
                if version.governance_conflict {
                    return Err(ShardCacheError::Command(
                        "active-sync governance conflict requires explicit resolution".into(),
                    ));
                }
                if !matches!(version.kind, MutationKind::Set) || !version.residency.is_resident() {
                    return Ok(false);
                }
                version
                    .expire_at_ms
                    .is_some_and(|deadline| deadline <= now_millis())
            };
            if !should_expire {
                return Ok(true);
            }
            if self.expire_if_needed_routed(route, key)? {
                return Ok(false);
            }
        }
    }

    fn expire_if_needed_routed(&self, route: EmbeddedKeyRoute, key: &[u8]) -> Result<bool> {
        let wall_ms = ttl_now_millis();
        let mut shard = self.inner.shards[route.shard_id].lock();
        let should_expire = shard.versions.get(key).is_some_and(|version| {
            matches!(version.kind, MutationKind::Set)
                && version
                    .expire_at_ms
                    .is_some_and(|deadline| deadline <= wall_ms)
        });
        if !should_expire {
            return Ok(false);
        }
        let mutation = self.local_mutation(
            &mut shard,
            route.shard_id,
            route.key_hash,
            key,
            MutationKind::Tombstone(TombstoneKind::Expired),
            None,
            None,
            None,
            wall_ms,
        )?;
        let (retired, mutation_bytes) = self.prepare_local_mutation_queue(&mut shard, &mutation)?;
        self.store_mutation_at(&mutation, wall_ms);
        replace_version_hashed(
            &mut shard.versions,
            route.key_hash,
            mutation.key.clone(),
            VersionState::from_mutation(&mutation, None),
        );
        self.queue_local_mutation(&mut shard, mutation, mutation_bytes);
        drop(shard);
        drop(retired);
        Ok(true)
    }

    fn validate_peer(&self, peer: &Self) -> Result<()> {
        if self.inner.config.cluster_id != peer.inner.config.cluster_id {
            return Err(ShardCacheError::Config(
                "active-sync peers use different cluster IDs".into(),
            ));
        }
        if self.shard_count() != peer.shard_count() {
            return Err(ShardCacheError::Config(
                "active-sync peers use different shard counts".into(),
            ));
        }
        if self.node_id() == peer.node_id() {
            return Err(ShardCacheError::Config(
                "active-sync peers must use unique node IDs".into(),
            ));
        }
        Ok(())
    }

    fn frontiers(&self) -> BTreeMap<BlockOrigin, u64> {
        let mut frontiers = BTreeMap::new();
        for shard in &self.inner.shards {
            for (origin, sequence) in &shard.lock().block_frontiers {
                frontiers
                    .entry(origin.clone())
                    .and_modify(|current: &mut u64| *current = (*current).max(*sequence))
                    .or_insert(*sequence);
            }
        }
        frontiers
    }

    fn shard_frontiers(&self, shard_id: usize) -> Result<BTreeMap<BlockOrigin, u64>> {
        let shard =
            self.inner.shards.get(shard_id).ok_or_else(|| {
                ShardCacheError::Protocol("active-sync shard is out of range".into())
            })?;
        Ok(shard.lock().block_frontiers.clone())
    }

    fn missing_blocks_in_shard(
        &self,
        shard_id: usize,
        origin_node: &NodeId,
        receiver: &BTreeMap<BlockOrigin, u64>,
        options: &SyncOptions,
    ) -> Result<(Vec<Arc<SyncBlock>>, bool, bool)> {
        let shard =
            self.inner.shards.get(shard_id).ok_or_else(|| {
                ShardCacheError::Protocol("active-sync shard is out of range".into())
            })?;
        let shard = shard.lock();
        let mut oldest = BTreeMap::<BlockOrigin, u64>::new();
        for block in &shard.blocks {
            if &block.origin.node_id == origin_node {
                oldest.entry(block.origin.clone()).or_insert(block.sequence);
            }
        }
        let gap = shard
            .block_frontiers
            .iter()
            .any(|(origin, sender_frontier)| {
                if &origin.node_id != origin_node {
                    return false;
                }
                let receiver_frontier = receiver.get(origin).copied().unwrap_or(0);
                receiver_frontier < *sender_frontier
                    && oldest
                        .get(origin)
                        .is_none_or(|first| receiver_frontier.saturating_add(1) < *first)
            });
        let mut blocks = Vec::new();
        let mut bytes = 0usize;
        let mut truncated = false;
        for block in &shard.blocks {
            if &block.origin.node_id != origin_node
                || block.sequence <= receiver.get(&block.origin).copied().unwrap_or(0)
            {
                continue;
            }
            if blocks.len() >= options.max_blocks
                || bytes.saturating_add(block.encoded_bytes) > options.max_bytes
            {
                truncated = true;
                break;
            }
            bytes = bytes.saturating_add(block.encoded_bytes);
            blocks.push(Arc::clone(block));
        }
        Ok((blocks, gap, truncated))
    }

    fn materialized_shard_snapshot(
        &self,
        shard_id: usize,
        origin_node: &NodeId,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<Vec<ActiveMutation>> {
        let shard =
            self.inner.shards.get(shard_id).ok_or_else(|| {
                ShardCacheError::Protocol("active-sync shard is out of range".into())
            })?;
        let shard = shard.lock();
        let mut records = Vec::new();
        let mut bytes = 0usize;
        for (key, version) in shard.versions.iter() {
            if &version.dot.node_id != origin_node {
                continue;
            }
            let value = match version.kind {
                MutationKind::Set => {
                    if !version.residency.is_resident() {
                        return Err(ShardCacheError::Persistence(
                            "active-sync state transfer requires materialized values".into(),
                        ));
                    }
                    Some(SharedBytes::from(self.inner.store.get(key).ok_or_else(
                        || {
                            ShardCacheError::Persistence(
                                "active-sync resident state transfer value is missing".into(),
                            )
                        },
                    )?))
                }
                MutationKind::Tombstone(_) | MutationKind::ClusterEvict { .. } => None,
            };
            let mutation = ActiveMutation {
                dot: version.dot.clone(),
                hlc: version.hlc,
                context: version.context.clone(),
                key_hash: hash_key(key),
                key: SharedBytes::copy_from_slice(key),
                value,
                expire_at_ms: version.expire_at_ms,
                governance: version.governance.clone(),
                kind: version.kind.clone(),
            };
            let next_bytes = bytes.checked_add(mutation.estimated_bytes()).ok_or(
                ShardCacheError::Backpressure(
                    "active-sync state transfer byte accounting overflowed",
                ),
            )?;
            if records.len() >= max_records || next_bytes > max_bytes {
                return Err(ShardCacheError::Backpressure(
                    "active-sync state transfer budget exhausted",
                ));
            }
            bytes = next_bytes;
            records.push(mutation);
        }
        Ok(records)
    }

    fn apply_state_mutation(
        &self,
        mutation: &ActiveMutation,
        source_peer: &NodeId,
    ) -> Result<ApplyStats> {
        let shard_id = mutation.dot.shard_id as usize;
        let shard = self.inner.shards.get(shard_id).ok_or_else(|| {
            ShardCacheError::Protocol("active-sync state shard is out of range".into())
        })?;
        let mut stats = ApplyStats::default();
        let mut retries = 0usize;
        loop {
            let mut state = shard.lock();
            let claim = self.ambiguous_conflict_claim(&state, mutation);
            let Some((orderer, claim)) = self.inner.conflict_orderer.as_ref().zip(claim) else {
                self.apply_remote_mutation(&mut state, mutation, source_peer, &mut stats, None)?;
                return Ok(stats);
            };
            drop(state);

            let decision = self.order_conflict(orderer.as_ref(), &claim)?;
            let mut state = shard.lock();
            let current_claim = self.ambiguous_conflict_claim(&state, mutation);
            if current_claim.as_ref().map(ConflictClaim::digest) != Some(claim.digest()) {
                retries = retries.saturating_add(1);
                self.inner
                    .conflict_order_retries
                    .fetch_add(1, AtomicOrdering::Relaxed);
                if retries >= self.inner.config.max_conflict_order_retries {
                    return Err(ShardCacheError::Backpressure(
                        "active-sync state conflict changed while awaiting consensus",
                    ));
                }
                continue;
            }
            self.apply_remote_mutation(
                &mut state,
                mutation,
                source_peer,
                &mut stats,
                Some(decision.winner()),
            )?;
            self.inner
                .conflict_ordered
                .fetch_add(1, AtomicOrdering::Relaxed);
            return Ok(stats);
        }
    }

    fn accept_snapshot_frontiers(
        &self,
        shard_id: usize,
        frontiers: &BTreeMap<BlockOrigin, u64>,
    ) -> Result<()> {
        let shard = self.inner.shards.get(shard_id).ok_or_else(|| {
            ShardCacheError::Protocol("active-sync snapshot shard is out of range".into())
        })?;
        let mut shard = shard.lock();
        if frontiers
            .iter()
            .any(|(origin, sequence)| *sequence == 0 || origin.shard_id as usize != shard_id)
        {
            return Err(ShardCacheError::Protocol(
                "active-sync snapshot frontier metadata is invalid".into(),
            ));
        }
        let new_origins = frontiers
            .keys()
            .filter(|origin| !shard.block_frontiers.contains_key(*origin))
            .count();
        if shard.block_frontiers.len().saturating_add(new_origins)
            > self.inner.config.max_causal_origins_per_shard
        {
            return Err(ShardCacheError::Backpressure(
                "active-sync snapshot origin limit reached",
            ));
        }
        for (origin, sequence) in frontiers {
            shard
                .block_frontiers
                .entry(origin.clone())
                .and_modify(|current| *current = (*current).max(*sequence))
                .or_insert(*sequence);
            if let Some(gaps) = shard.pending_block_gaps.get_mut(origin) {
                gaps.retain(|pending, _| pending > sequence);
            }
        }
        shard.pending_block_gaps.retain(|_, gaps| !gaps.is_empty());
        Ok(())
    }

    fn missing_blocks(
        &self,
        receiver: &BTreeMap<BlockOrigin, u64>,
        options: &SyncOptions,
    ) -> (Vec<Arc<SyncBlock>>, bool) {
        let mut blocks = Vec::new();
        let mut bytes = 0usize;
        let mut gap = false;
        for shard in &self.inner.shards {
            let shard = shard.lock();
            let mut oldest = BTreeMap::<BlockOrigin, u64>::new();
            for block in &shard.blocks {
                oldest.entry(block.origin.clone()).or_insert(block.sequence);
            }
            for (origin, sender_frontier) in &shard.block_frontiers {
                let receiver_frontier = receiver.get(origin).copied().unwrap_or(0);
                if receiver_frontier < *sender_frontier
                    && oldest
                        .get(origin)
                        .is_none_or(|first| receiver_frontier.saturating_add(1) < *first)
                {
                    gap = true;
                }
            }
            for block in &shard.blocks {
                if block.sequence <= receiver.get(&block.origin).copied().unwrap_or(0) {
                    continue;
                }
                if blocks.len() >= options.max_blocks
                    || bytes.saturating_add(block.encoded_bytes) > options.max_bytes
                {
                    return (blocks, gap);
                }
                bytes = bytes.saturating_add(block.encoded_bytes);
                blocks.push(Arc::clone(block));
            }
        }
        (blocks, gap)
    }

    fn has_missing_blocks(&self, receiver: &BTreeMap<BlockOrigin, u64>) -> bool {
        self.inner.shards.iter().any(|shard| {
            shard
                .lock()
                .blocks
                .iter()
                .any(|block| block.sequence > receiver.get(&block.origin).copied().unwrap_or(0))
        })
    }

    fn state_snapshot(&self) -> Result<Vec<ActiveMutation>> {
        let mut records = Vec::new();
        let mut bytes = 0usize;
        for (shard_id, shard) in self.inner.shards.iter().enumerate() {
            let shard = shard.lock();
            for (key, version) in shard.versions.iter() {
                let value = match version.kind {
                    MutationKind::Set => {
                        if !version.residency.is_resident() {
                            return Err(ShardCacheError::Persistence(
                                "active-sync state transfer requires materialized values".into(),
                            ));
                        }
                        Some(SharedBytes::from(self.inner.store.get(key).ok_or_else(
                            || {
                                ShardCacheError::Persistence(
                                    "active-sync resident state transfer value is missing".into(),
                                )
                            },
                        )?))
                    }
                    MutationKind::Tombstone(_) | MutationKind::ClusterEvict { .. } => None,
                };
                let mutation = ActiveMutation {
                    dot: version.dot.clone(),
                    hlc: version.hlc,
                    context: version.context.clone(),
                    key_hash: hash_key(key),
                    key: SharedBytes::copy_from_slice(key),
                    value,
                    expire_at_ms: version.expire_at_ms,
                    governance: version.governance.clone(),
                    kind: version.kind.clone(),
                };
                if mutation.dot.shard_id as usize != shard_id {
                    return Err(ShardCacheError::Persistence(
                        "active-sync snapshot contains misplaced version".into(),
                    ));
                }
                bytes = bytes.checked_add(mutation.estimated_bytes()).ok_or(
                    ShardCacheError::Backpressure(
                        "active-sync state transfer byte accounting overflowed",
                    ),
                )?;
                if bytes > self.inner.config.max_snapshot_bytes {
                    return Err(ShardCacheError::Backpressure(
                        "active-sync state transfer budget exhausted",
                    ));
                }
                records.push(mutation);
            }
        }
        Ok(records)
    }

    fn apply_snapshot(
        destination: &Self,
        records: &[ActiveMutation],
        source_peer: &NodeId,
        report: &mut BidirectionalSyncReport,
    ) -> Result<()> {
        for mutation in records {
            let stats = destination.apply_state_mutation(mutation, source_peer)?;
            report.applied_mutations += stats.applied;
            report.duplicate_mutations += stats.duplicates;
            report.conflicts += stats.conflicts;
        }
        Ok(())
    }

    fn mark_peer_recovery(&self, peer: &Self) {
        for shard_id in 0..self.shard_count() {
            let peer_versions = {
                let shard = peer.inner.shards[shard_id].lock();
                shard
                    .versions
                    .iter()
                    .filter(|(_, version)| {
                        matches!(version.kind, MutationKind::Set) && version.residency.is_resident()
                    })
                    .map(|(key, version)| (key.clone(), version.dot.clone()))
                    .collect::<Vec<_>>()
            };
            let mut shard = self.inner.shards[shard_id].lock();
            for (key, dot) in peer_versions {
                if let Some(version) = shard.versions.get_mut(&key)
                    && version.dot == dot
                {
                    version.add_recovery_peer(peer.node_id().clone());
                }
            }
        }
    }
}

impl SnapshotVersion {
    fn from_state(key: &[u8], value: Option<Vec<u8>>, version: &VersionState) -> Self {
        let kind = match &version.kind {
            MutationKind::Set => SnapshotKind::Set,
            MutationKind::Tombstone(TombstoneKind::Delete) => SnapshotKind::Delete,
            MutationKind::Tombstone(TombstoneKind::Expired) => SnapshotKind::Expired,
            MutationKind::ClusterEvict { target } => SnapshotKind::ClusterEvict {
                target: SnapshotDot::from(target.as_ref()),
            },
        };
        Self {
            key: key.to_vec(),
            value,
            dot: SnapshotDot::from(&version.dot),
            hlc_physical_ms: version.hlc.physical_ms,
            hlc_logical: version.hlc.logical,
            context: version
                .context
                .iter()
                .map(|(origin, sequence)| SnapshotFrontier {
                    node_id: origin.node_id.to_string(),
                    incarnation_id: origin.incarnation_id.0,
                    shard_id: origin.shard_id,
                    sequence,
                })
                .collect(),
            kind,
            expire_at_ms: version.expire_at_ms.get(),
            governance: version.governance.as_deref().map(<[u8]>::to_vec),
            governance_conflict: version.governance_conflict,
        }
    }

    fn to_mutation(&self) -> Result<ActiveMutation> {
        let dot = CompactMutationDot::from(&self.dot.to_dot()?);
        let mut context = CausalContext::default();
        for frontier in &self.context {
            let origin = CausalOrigin {
                node_id: NodeId::new(frontier.node_id.clone())?,
                incarnation_id: IncarnationId(frontier.incarnation_id),
                shard_id: frontier.shard_id,
            };
            context.observe_origin(origin, frontier.sequence);
        }
        let kind = match &self.kind {
            SnapshotKind::Set => {
                if self.value.is_none() {
                    return Err(ShardCacheError::Persistence(
                        "active-sync snapshot SET is missing its value".into(),
                    ));
                }
                MutationKind::Set
            }
            SnapshotKind::Delete => MutationKind::Tombstone(TombstoneKind::Delete),
            SnapshotKind::Expired => MutationKind::Tombstone(TombstoneKind::Expired),
            SnapshotKind::ClusterEvict { target } => MutationKind::ClusterEvict {
                target: Box::new(target.to_dot()?),
            },
        };
        Ok(ActiveMutation {
            dot,
            hlc: HybridLogicalClock {
                physical_ms: self.hlc_physical_ms,
                logical: self.hlc_logical,
            },
            context,
            key_hash: hash_key(&self.key),
            key: SharedBytes::copy_from_slice(&self.key),
            value: self.value.as_deref().map(SharedBytes::copy_from_slice),
            expire_at_ms: CompactExpiration::new(self.expire_at_ms),
            governance: self.governance.as_deref().map(SharedBytes::copy_from_slice),
            kind,
        })
    }

    fn estimated_bytes(&self) -> usize {
        let binary_bytes = self
            .key
            .len()
            .saturating_add(self.value.as_ref().map_or(0, Vec::len))
            .saturating_add(self.governance.as_ref().map_or(0, Vec::len));
        binary_bytes
            .saturating_mul(4)
            .saturating_add(
                self.context
                    .iter()
                    .map(|frontier| frontier.node_id.len().saturating_add(40))
                    .sum::<usize>(),
            )
            .saturating_add(self.dot.node_id.len())
            .saturating_add(128)
    }
}

impl SnapshotFrontier {
    fn from_block_origin(origin: &BlockOrigin, sequence: u64) -> Self {
        Self {
            node_id: origin.node_id.to_string(),
            incarnation_id: origin.incarnation_id.0,
            shard_id: origin.shard_id,
            sequence,
        }
    }

    fn to_block_origin(&self) -> Result<(BlockOrigin, u64)> {
        if self.sequence == 0 {
            return Err(ShardCacheError::Persistence(
                "active-sync snapshot block frontier sequence is invalid".into(),
            ));
        }
        Ok((
            BlockOrigin {
                node_id: NodeId::new(self.node_id.clone())?,
                incarnation_id: IncarnationId(self.incarnation_id),
                shard_id: self.shard_id,
            },
            self.sequence,
        ))
    }
}

#[inline]
fn replace_version_hashed(
    versions: &mut ActiveVersionMap,
    hash: u64,
    key: SharedBytes,
    replacement: VersionState,
) {
    let _ = versions.replace_hashed(hash, key, replacement);
}

impl SnapshotDot {
    fn to_dot(&self) -> Result<MutationDot> {
        Ok(MutationDot {
            node_id: NodeId::new(self.node_id.clone())?,
            incarnation_id: IncarnationId(self.incarnation_id),
            shard_id: self.shard_id,
            sequence: self.sequence,
        })
    }
}

impl From<&MutationDot> for SnapshotDot {
    fn from(dot: &MutationDot) -> Self {
        Self {
            node_id: dot.node_id.to_string(),
            incarnation_id: dot.incarnation_id.0,
            shard_id: dot.shard_id,
            sequence: dot.sequence,
        }
    }
}

impl From<&CompactMutationDot> for SnapshotDot {
    fn from(dot: &CompactMutationDot) -> Self {
        Self {
            node_id: dot.node_id.to_string(),
            incarnation_id: dot.incarnation_id.0,
            shard_id: dot.shard_id,
            sequence: dot.sequence,
        }
    }
}

fn semantic_concurrent_incoming_wins(
    local: &VersionState,
    incoming: &ActiveMutation,
) -> Option<bool> {
    match (&local.kind, &incoming.kind) {
        (MutationKind::Tombstone(_), MutationKind::Set) => Some(false),
        (MutationKind::Set, MutationKind::Tombstone(_)) => Some(true),
        (MutationKind::ClusterEvict { target }, MutationKind::Set) => {
            Some(&incoming.dot != target.as_ref())
        }
        (MutationKind::Set, MutationKind::ClusterEvict { target }) => {
            Some(&local.dot == target.as_ref())
        }
        (MutationKind::Tombstone(_), MutationKind::ClusterEvict { .. }) => Some(false),
        (MutationKind::ClusterEvict { .. }, MutationKind::Tombstone(_)) => Some(true),
        _ => None,
    }
}

fn concurrent_clock_incoming_wins(local: &VersionState, incoming: &ActiveMutation) -> bool {
    (incoming.hlc, &incoming.dot.node_id, &incoming.dot).cmp(&(
        local.hlc,
        &local.dot.node_id,
        &local.dot,
    )) == Ordering::Greater
}

fn conflict_mutation_class(kind: &MutationKind) -> ConflictMutationClass {
    match kind {
        MutationKind::Set => ConflictMutationClass::Set,
        MutationKind::Tombstone(TombstoneKind::Delete) => ConflictMutationClass::Delete,
        MutationKind::Tombstone(TombstoneKind::Expired) => ConflictMutationClass::Expired,
        MutationKind::ClusterEvict { .. } => ConflictMutationClass::ClusterEvict,
    }
}

fn conflict_class_code(class: ConflictMutationClass) -> u8 {
    match class {
        ConflictMutationClass::Set => 1,
        ConflictMutationClass::Delete => 2,
        ConflictMutationClass::Expired => 3,
        ConflictMutationClass::ClusterEvict => 4,
    }
}

fn put_conflict_dot(bytes: &mut Vec<u8>, dot: &MutationDot) {
    bytes.push(dot.node_id.as_str().len() as u8);
    bytes.extend_from_slice(dot.node_id.as_str().as_bytes());
    bytes.extend_from_slice(&dot.incarnation_id.0.to_le_bytes());
    bytes.extend_from_slice(&dot.shard_id.to_le_bytes());
    bytes.extend_from_slice(&dot.sequence.to_le_bytes());
}

fn observe_block_sequence(
    shard: &mut ActiveShardState,
    origin: &BlockOrigin,
    sequence: u64,
    digest: [u8; 32],
) {
    let frontier = shard.block_frontiers.entry(origin.clone()).or_insert(0);
    if sequence == frontier.saturating_add(1) {
        *frontier = sequence;
        if let Some(gaps) = shard.pending_block_gaps.get_mut(origin) {
            while gaps.remove(&frontier.saturating_add(1)).is_some() {
                *frontier = frontier.saturating_add(1);
            }
            if gaps.is_empty() {
                shard.pending_block_gaps.remove(origin);
            }
        }
    } else if sequence > frontier.saturating_add(1) {
        shard
            .pending_block_gaps
            .entry(origin.clone())
            .or_default()
            .insert(sequence, digest);
    }
}

fn block_digest(
    cluster_id: &str,
    origin: &BlockOrigin,
    sequence: u64,
    records: &[ActiveMutation],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(BLOCK_MAGIC);
    digest.update([BLOCK_FORMAT_VERSION]);
    hash_bytes(&mut digest, cluster_id.as_bytes());
    hash_bytes(&mut digest, origin.node_id.as_str().as_bytes());
    digest.update(origin.incarnation_id.0.to_le_bytes());
    digest.update(origin.shard_id.to_le_bytes());
    digest.update(sequence.to_le_bytes());
    digest.update((records.len() as u64).to_le_bytes());
    for record in records {
        hash_bytes(&mut digest, record.dot.node_id.as_str().as_bytes());
        digest.update(record.dot.incarnation_id.0.to_le_bytes());
        digest.update(record.dot.shard_id.to_le_bytes());
        digest.update(record.dot.sequence.to_le_bytes());
        digest.update(record.hlc.physical_ms.to_le_bytes());
        digest.update(record.hlc.logical.to_le_bytes());
        digest.update(record.key_hash.to_le_bytes());
        hash_bytes(&mut digest, &record.key);
        hash_optional_bytes(&mut digest, record.value.as_deref());
        hash_optional_bytes(&mut digest, record.governance.as_deref());
        digest.update(record.expire_at_ms.unwrap_or(u64::MAX).to_le_bytes());
        match &record.kind {
            MutationKind::Set => digest.update([1]),
            MutationKind::Tombstone(TombstoneKind::Delete) => digest.update([2]),
            MutationKind::Tombstone(TombstoneKind::Expired) => digest.update([3]),
            MutationKind::ClusterEvict { target } => {
                digest.update([4]);
                hash_bytes(&mut digest, target.node_id.as_str().as_bytes());
                digest.update(target.incarnation_id.0.to_le_bytes());
                digest.update(target.shard_id.to_le_bytes());
                digest.update(target.sequence.to_le_bytes());
            }
        }
        digest.update((record.context.len() as u64).to_le_bytes());
        for (context_origin, context_sequence) in record.context.iter() {
            hash_bytes(&mut digest, context_origin.node_id.as_str().as_bytes());
            digest.update(context_origin.incarnation_id.0.to_le_bytes());
            digest.update(context_origin.shard_id.to_le_bytes());
            digest.update(context_sequence.to_le_bytes());
        }
    }
    digest.finalize().into()
}

fn hash_bytes(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_le_bytes());
    digest.update(bytes);
}

fn hash_optional_bytes(digest: &mut Sha256, bytes: Option<&[u8]>) {
    match bytes {
        Some(bytes) => {
            digest.update([1]);
            hash_bytes(digest, bytes);
        }
        None => digest.update([0]),
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "active-sync-consensus-ordered-eventual")]
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    use super::*;

    #[cfg(feature = "active-sync-consensus-ordered-eventual")]
    struct NodeWinnerOrderer {
        calls: AtomicUsize,
        available: AtomicBool,
        winner: NodeId,
    }

    #[cfg(feature = "active-sync-consensus-ordered-eventual")]
    impl ConflictOrderer for NodeWinnerOrderer {
        fn decide(&self, claim: &ConflictClaim) -> Result<ConflictDecision> {
            self.calls.fetch_add(1, AtomicOrdering::Relaxed);
            if !self.available.load(AtomicOrdering::Relaxed) {
                return Err(ShardCacheError::Protocol(
                    "test conflict orderer unavailable".into(),
                ));
            }
            let winner = claim
                .candidates()
                .iter()
                .find(|candidate| candidate.dot().node_id == self.winner)
                .ok_or_else(|| {
                    ShardCacheError::Protocol("configured test winner is absent".into())
                })?
                .dot()
                .clone();
            ConflictDecision::new(claim, winner)
        }
    }

    #[test]
    fn active_metadata_layout_stays_compact() {
        assert!(std::mem::size_of::<CompactMutationDot>() <= 16);
        assert!(std::mem::size_of::<CausalContext>() <= 32);
        assert!(std::mem::size_of::<ActiveMutation>() <= 192);
        assert!(std::mem::size_of::<VersionState>() <= 144);
        assert!(std::mem::size_of::<MutationKind>() <= 16);
        assert!(std::mem::size_of::<CompactExpiration>() <= 8);
        assert!(std::mem::size_of::<Residency>() <= 8);
    }

    #[test]
    fn compact_metadata_preserves_deadline_and_residency_semantics() {
        assert_eq!(CompactExpiration::new(None).get(), None);
        assert_eq!(CompactExpiration::new(Some(0)).get(), Some(1));
        assert_eq!(
            CompactExpiration::new(Some(u64::MAX)).get(),
            Some(u64::MAX - 1)
        );

        assert_eq!(Residency::RESIDENT.evicted_generation(), None);
        assert_eq!(Residency::evicted(0).evicted_generation(), Some(0));
        assert_eq!(Residency::evicted(42).evicted_generation(), Some(42));
        assert_eq!(Residency::CLUSTER_EVICTED.evicted_generation(), None);
    }

    #[test]
    fn causal_consistency_mode_reports_its_actual_guarantee() {
        let causal = ActiveShardMap::new_causal_eventual(
            1,
            ActiveSyncConfig::new("test-cluster", NodeId::new("causal").unwrap()),
        )
        .unwrap();
        assert_eq!(
            causal.consistency_mode(),
            ActiveConsistencyMode::CausalEventual
        );
        assert!(!causal.consistency_mode().requires_external_orderer());
        assert!(!causal.consistency_mode().is_linearizable());
        assert!(!causal.consistency_mode().is_serializable());
    }

    #[cfg(feature = "active-sync-consensus-ordered-eventual")]
    #[test]
    fn consensus_consistency_mode_reports_its_actual_guarantee() {
        let consensus = ActiveShardMap::new_consensus_ordered_eventual(
            1,
            ActiveSyncConfig::new("test-cluster", NodeId::new("consensus").unwrap()),
            Arc::new(NodeWinnerOrderer {
                calls: AtomicUsize::new(0),
                available: AtomicBool::new(true),
                winner: NodeId::new("consensus").unwrap(),
            }),
        )
        .unwrap();
        let mode = consensus.consistency_mode();
        assert_eq!(mode, ActiveConsistencyMode::ConsensusOrderedEventual);
        assert_eq!(consensus.health_snapshot().consistency_mode, mode);
        assert!(mode.requires_external_orderer());
        assert!(!mode.is_linearizable());
        assert!(!mode.is_serializable());
    }

    #[test]
    fn repeated_local_writes_reuse_the_shard_origin() {
        let map = map("left");
        map.set("key", "one").unwrap();
        map.set("key", "two").unwrap();

        let shard_id = map.inner.store.route_key(b"key").shard_id;
        let shard = map.inner.shards[shard_id].lock();
        let pending_origin = &shard.pending[1].context.0[0].origin;
        let version_origin = &shard.versions.get(b"key").unwrap().context.0[0].origin;
        assert!(Arc::ptr_eq(
            pending_origin,
            &map.inner.local_origins[shard_id]
        ));
        assert!(Arc::ptr_eq(
            version_origin,
            &map.inner.local_origins[shard_id]
        ));
    }

    #[test]
    fn sealed_block_capacity_is_reused_for_the_next_interval() {
        let mut config = ActiveSyncConfig::new("test-cluster", NodeId::new("left").unwrap());
        config.incarnation_id = IncarnationId(1);
        config.max_pending_records_per_shard = 2;
        config.max_block_records = 2;
        let map = ActiveShardMap::new(1, config).unwrap();

        map.set("one", "1").unwrap();
        map.set("two", "2").unwrap();
        map.set("three", "3").unwrap();

        let shard = map.inner.shards[0].lock();
        assert_eq!(shard.blocks.len(), 1);
        assert_eq!(shard.pending.len(), 1);
        assert!(shard.pending.capacity() >= 2);
    }

    fn map(node: &str) -> ActiveShardMap {
        let mut config = ActiveSyncConfig::new("test-cluster", NodeId::new(node).unwrap());
        config.incarnation_id = IncarnationId(u128::from(node.as_bytes()[0]));
        config.max_clock_skew = Duration::from_secs(60);
        ActiveShardMap::new(4, config).unwrap()
    }

    #[cfg(feature = "active-sync-consensus-ordered-eventual")]
    fn map_with_orderer(node: &str, orderer: Arc<dyn ConflictOrderer>) -> ActiveShardMap {
        let mut config = ActiveSyncConfig::new("test-cluster", NodeId::new(node).unwrap());
        config.incarnation_id = IncarnationId(u128::from(node.as_bytes()[0]));
        config.max_clock_skew = Duration::from_secs(60);
        ActiveShardMap::new_with_conflict_orderer(4, config, orderer).unwrap()
    }

    #[cfg(feature = "active-sync-consensus-ordered-eventual")]
    #[test]
    fn external_orderer_is_not_called_without_a_conflict() {
        let orderer = Arc::new(NodeWinnerOrderer {
            calls: AtomicUsize::new(0),
            available: AtomicBool::new(true),
            winner: NodeId::new("left").unwrap(),
        });
        let left = map_with_orderer("left", orderer.clone());
        let right = map_with_orderer("right", orderer.clone());
        left.set("key", "value").unwrap();

        left.sync_with(&right, SyncOptions::default()).unwrap();

        assert_eq!(orderer.calls.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(right.get("key"), Some(b"value".to_vec()));
    }

    #[cfg(feature = "active-sync-consensus-ordered-eventual")]
    #[test]
    fn external_orderer_selects_the_concurrent_set_winner() {
        let orderer = Arc::new(NodeWinnerOrderer {
            calls: AtomicUsize::new(0),
            available: AtomicBool::new(true),
            winner: NodeId::new("left").unwrap(),
        });
        let left = map_with_orderer("left", orderer.clone());
        let right = map_with_orderer("right", orderer.clone());
        left.set("key", "left-value").unwrap();
        right.set("key", "right-value").unwrap();

        left.sync_with(&right, SyncOptions::default()).unwrap();

        assert_eq!(left.get("key"), Some(b"left-value".to_vec()));
        assert_eq!(right.get("key"), Some(b"left-value".to_vec()));
        assert_eq!(left.version_dot("key"), right.version_dot("key"));
        assert_eq!(orderer.calls.load(AtomicOrdering::Relaxed), 2);
        assert_eq!(left.health_snapshot().conflict_ordered, 1);
        assert_eq!(right.health_snapshot().conflict_ordered, 1);
    }

    #[cfg(feature = "active-sync-consensus-ordered-eventual")]
    #[test]
    fn unavailable_orderer_does_not_acknowledge_or_replace_the_local_value() {
        let orderer = Arc::new(NodeWinnerOrderer {
            calls: AtomicUsize::new(0),
            available: AtomicBool::new(false),
            winner: NodeId::new("left").unwrap(),
        });
        let left = map_with_orderer("left", orderer.clone());
        let right = map_with_orderer("right", orderer.clone());
        left.set("key", "left-value").unwrap();
        right.set("key", "right-value").unwrap();

        assert!(left.sync_with(&right, SyncOptions::default()).is_err());
        assert_eq!(right.get("key"), Some(b"right-value".to_vec()));
        assert_eq!(right.health_snapshot().conflict_order_failures, 1);

        orderer.available.store(true, AtomicOrdering::Relaxed);
        left.sync_with(&right, SyncOptions::default()).unwrap();
        assert_eq!(left.get("key"), Some(b"left-value".to_vec()));
        assert_eq!(right.get("key"), Some(b"left-value".to_vec()));
    }

    #[cfg(feature = "active-sync-consensus-ordered-eventual")]
    #[test]
    fn semantic_remove_wins_without_consensus_work() {
        let orderer = Arc::new(NodeWinnerOrderer {
            calls: AtomicUsize::new(0),
            available: AtomicBool::new(true),
            winner: NodeId::new("right").unwrap(),
        });
        let left = map_with_orderer("left", orderer.clone());
        let right = map_with_orderer("right", orderer.clone());
        left.set("key", "old").unwrap();
        left.sync_with(&right, SyncOptions::default()).unwrap();

        left.delete("key").unwrap();
        right.set("key", "concurrent").unwrap();
        left.sync_with(&right, SyncOptions::default()).unwrap();

        assert_eq!(left.get("key"), None);
        assert_eq!(right.get("key"), None);
        assert_eq!(orderer.calls.load(AtomicOrdering::Relaxed), 0);
    }

    #[cfg(feature = "active-sync-consensus-ordered-eventual")]
    struct ChangingOrderer {
        calls: AtomicUsize,
        target: Mutex<Option<ActiveShardMap>>,
    }

    #[cfg(feature = "active-sync-consensus-ordered-eventual")]
    impl ConflictOrderer for ChangingOrderer {
        fn decide(&self, claim: &ConflictClaim) -> Result<ConflictDecision> {
            let call = self.calls.fetch_add(1, AtomicOrdering::Relaxed);
            if call == 0 {
                self.target
                    .lock()
                    .as_ref()
                    .unwrap()
                    .set("key", "new-local-value")?;
            }
            let winner = claim
                .candidates()
                .iter()
                .find(|candidate| candidate.dot().node_id.as_str() == "right")
                .unwrap()
                .dot()
                .clone();
            ConflictDecision::new(claim, winner)
        }
    }

    #[cfg(feature = "active-sync-consensus-ordered-eventual")]
    #[test]
    fn stale_consensus_decision_is_retried_without_holding_the_shard_lock() {
        let orderer = Arc::new(ChangingOrderer {
            calls: AtomicUsize::new(0),
            target: Mutex::new(None),
        });
        let left = map("left");
        let right = map_with_orderer("right", orderer.clone());
        *orderer.target.lock() = Some(right.clone());
        left.set("key", "left-value").unwrap();
        right.set("key", "right-value").unwrap();
        left.seal_pending().unwrap();
        let shard_id = left.inner.store.route_key(b"key").shard_id;
        let block = left.inner.shards[shard_id]
            .lock()
            .blocks
            .back()
            .unwrap()
            .clone();

        right.apply_block(block, left.node_id()).unwrap();

        assert_eq!(right.get("key"), Some(b"new-local-value".to_vec()));
        assert_eq!(orderer.calls.load(AtomicOrdering::Relaxed), 2);
    }

    #[test]
    fn direct_sync_converges_concurrent_sets() {
        let left = map("left");
        let right = map("right");
        left.set("key", "left-value").unwrap();
        right.set("key", "right-value").unwrap();

        let report = left.sync_with(&right, SyncOptions::default()).unwrap();

        assert_eq!(left.get("key"), right.get("key"));
        assert_eq!(report.conflicts, 2);
        assert_eq!(left.version_dot("key"), right.version_dot("key"));
    }

    #[test]
    fn remove_wins_and_later_observed_set_recreates_key() {
        let left = map("left");
        let right = map("right");
        left.set("key", "old").unwrap();
        left.sync_with(&right, SyncOptions::default()).unwrap();

        left.delete("key").unwrap();
        right.set("key", "concurrent").unwrap();
        left.sync_with(&right, SyncOptions::default()).unwrap();
        assert_eq!(left.get("key"), None);
        assert_eq!(right.get("key"), None);

        right.set("key", "new").unwrap();
        left.sync_with(&right, SyncOptions::default()).unwrap();
        assert_eq!(left.get("key"), Some(b"new".to_vec()));
        assert_eq!(right.get("key"), Some(b"new".to_vec()));
    }

    #[test]
    fn duplicate_and_reordered_sync_are_idempotent() {
        let left = map("left");
        let right = map("right");
        for index in 0..100 {
            left.set(format!("key-{index}"), format!("value-{index}"))
                .unwrap();
        }
        left.sync_with(&right, SyncOptions::default()).unwrap();
        let second = left.sync_with(&right, SyncOptions::default()).unwrap();
        assert_eq!(second.blocks_to_local + second.blocks_to_peer, 0);
        assert_eq!(left.health_snapshot().live_versions, 100);
        assert_eq!(right.health_snapshot().live_versions, 100);
    }

    #[test]
    fn local_eviction_requires_a_recovery_peer_and_does_not_replicate() {
        let left = map("left");
        let right = map("right");
        let token = left.set("key", "value").unwrap();
        assert_eq!(
            left.evict_local_exact("key", &token.dot).unwrap(),
            EvictionOutcome::NoRecoverySource
        );
        left.sync_with(&right, SyncOptions::default()).unwrap();
        assert_eq!(
            left.evict_local_exact("key", &token.dot).unwrap(),
            EvictionOutcome::Evicted
        );
        assert_eq!(left.get("key"), None);
        assert_eq!(right.get("key"), Some(b"value".to_vec()));
        left.sync_with(&right, SyncOptions::default()).unwrap();
        assert_eq!(left.get("key"), None);
        assert_eq!(
            left.fault_in_from("key", &right).unwrap(),
            Some(b"value".to_vec())
        );
    }

    #[test]
    fn stale_eviction_cannot_remove_a_newer_write() {
        let left = map("left");
        let right = map("right");
        let old = left.set("key", "old").unwrap();
        left.sync_with(&right, SyncOptions::default()).unwrap();
        left.set("key", "new").unwrap();
        assert_eq!(
            left.evict_local_exact("key", &old.dot).unwrap(),
            EvictionOutcome::Stale
        );
        assert_eq!(left.get("key"), Some(b"new".to_vec()));
    }

    #[test]
    fn cluster_eviction_suppresses_only_the_targeted_version() {
        let left = map("left");
        let right = map("right");
        let old = left.set("key", "old").unwrap();
        left.sync_with(&right, SyncOptions::default()).unwrap();

        left.commit_cluster_eviction_exact("key", &old.dot)
            .unwrap()
            .unwrap();
        right.set("key", "refreshed").unwrap();
        left.sync_with(&right, SyncOptions::default()).unwrap();
        assert_eq!(left.get("key"), Some(b"refreshed".to_vec()));
        assert_eq!(right.get("key"), Some(b"refreshed".to_vec()));
    }

    #[test]
    fn eviction_commit_arriving_before_value_prevents_resurrection() {
        let mut config = ActiveSyncConfig::new("test-cluster", NodeId::new("left").unwrap());
        config.incarnation_id = IncarnationId(1);
        config.max_block_records = 1;
        let left = ActiveShardMap::new(1, config).unwrap();
        let target = left.set("key", "value").unwrap();
        left.commit_cluster_eviction_exact("key", &target.dot)
            .unwrap()
            .unwrap();
        left.seal_pending().unwrap();
        let blocks = left.inner.shards[0]
            .lock()
            .blocks
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let receiver = ActiveShardMap::new(
            1,
            ActiveSyncConfig {
                incarnation_id: IncarnationId(2),
                ..ActiveSyncConfig::new("test-cluster", NodeId::new("right").unwrap())
            },
        )
        .unwrap();
        receiver
            .apply_block(blocks[1].clone(), left.node_id())
            .unwrap();
        receiver
            .apply_block(blocks[0].clone(), left.node_id())
            .unwrap();
        assert_eq!(receiver.get("key"), None);
    }

    #[test]
    fn governance_conflicts_fail_closed() {
        let left = map("left");
        let right = map("right");
        left.set_with_governance("key", "left", "tenant-a").unwrap();
        right
            .set_with_governance("key", "right", "tenant-b")
            .unwrap();
        left.sync_with(&right, SyncOptions::default()).unwrap();
        assert!(left.try_get("key").is_err());
        assert!(right.try_get("key").is_err());
        assert_eq!(left.get("key"), None);
        assert_eq!(right.get("key"), None);
    }

    #[test]
    fn governance_conflict_with_unguarded_winner_fails_closed() {
        let left = map("left");
        let right = map("right");
        left.set("key", "unguarded").unwrap();
        right
            .set_with_governance("key", "guarded", "tenant-a")
            .unwrap();

        left.sync_with(&right, SyncOptions::default()).unwrap();

        assert!(left.try_get("key").is_err());
        assert!(right.try_get("key").is_err());
        assert_eq!(left.get("key"), None);
        assert_eq!(right.get("key"), None);
    }

    #[test]
    fn expired_values_create_replicated_tombstones() {
        let left = map("left");
        let right = map("right");
        left.set_with_ttl("key", "value", Duration::ZERO).unwrap();
        assert_eq!(left.get("key"), None);
        left.sync_with(&right, SyncOptions::default()).unwrap();
        assert_eq!(right.get("key"), None);
        assert_eq!(right.health_snapshot().tombstones, 1);
    }

    #[test]
    fn live_ttl_and_plain_values_share_the_read_path() {
        let map = map("left");
        map.set_with_ttl("ttl", "ttl-value", Duration::from_secs(60))
            .unwrap();
        map.set("plain", "plain-value").unwrap();

        assert_eq!(map.get("ttl"), Some(b"ttl-value".to_vec()));
        assert_eq!(map.get("plain"), Some(b"plain-value".to_vec()));
        assert_eq!(map.try_get("ttl").unwrap(), Some(b"ttl-value".to_vec()));
        assert_eq!(map.try_get("plain").unwrap(), Some(b"plain-value".to_vec()));
    }

    #[test]
    fn memory_and_context_limits_reject_work() {
        let mut config = ActiveSyncConfig::new("test-cluster", NodeId::new("left").unwrap());
        config.max_value_bytes = 3;
        let map = ActiveShardMap::new(1, config).unwrap();
        assert!(map.set("key", "four").is_err());
        assert_eq!(map.health_snapshot().pending_records, 0);
    }

    #[test]
    fn block_admission_failure_does_not_change_local_state() {
        let mut config = ActiveSyncConfig::new("test-cluster", NodeId::new("left").unwrap());
        config.max_block_bytes = 32;
        let map = ActiveShardMap::new(1, config).unwrap();
        assert!(map.set("key", "value").is_err());
        assert_eq!(map.get("key"), None);
        assert_eq!(map.version_dot("key"), None);
    }

    #[test]
    fn out_of_order_blocks_do_not_skip_frontier_gaps() {
        let mut config = ActiveSyncConfig::new("test-cluster", NodeId::new("left").unwrap());
        config.incarnation_id = IncarnationId(1);
        config.max_block_records = 1;
        let left = ActiveShardMap::new(1, config).unwrap();
        let right = ActiveShardMap::new(
            1,
            ActiveSyncConfig {
                incarnation_id: IncarnationId(2),
                ..ActiveSyncConfig::new("test-cluster", NodeId::new("right").unwrap())
            },
        )
        .unwrap();
        left.set("one", "1").unwrap();
        left.set("two", "2").unwrap();
        left.set("three", "3").unwrap();
        left.seal_pending().unwrap();
        let blocks = left.inner.shards[0]
            .lock()
            .blocks
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(blocks.len(), 3);

        right
            .apply_block(blocks[2].clone(), left.node_id())
            .unwrap();
        assert_eq!(right.frontiers().values().copied().next(), Some(0));
        right
            .apply_block(blocks[0].clone(), left.node_id())
            .unwrap();
        assert_eq!(right.frontiers().values().copied().next(), Some(1));
        right
            .apply_block(blocks[1].clone(), left.node_id())
            .unwrap();
        assert_eq!(right.frontiers().values().copied().next(), Some(3));
        assert_eq!(right.get("three"), Some(b"3".to_vec()));
    }

    #[test]
    fn block_provenance_is_validated_before_any_record_is_applied() {
        let left = map("left");
        let right = map("right");
        left.set("one", "1").unwrap();
        left.seal_pending().unwrap();
        let original = left
            .inner
            .shards
            .iter()
            .find_map(|shard| shard.lock().blocks.front().cloned())
            .unwrap();
        let valid = original.records[0].clone();
        let mut forged_record = valid.clone();
        forged_record.dot.origin = Arc::new(CausalOrigin {
            node_id: NodeId::new("forged").unwrap(),
            incarnation_id: forged_record.dot.incarnation_id,
            shard_id: forged_record.dot.shard_id,
        });
        let records = vec![valid, forged_record];
        let encoded_bytes = records.iter().map(ActiveMutation::estimated_bytes).sum();
        let forged = Arc::new(SyncBlock {
            cluster_id: original.cluster_id.clone(),
            origin: original.origin.clone(),
            sequence: original.sequence,
            digest: OnceLock::new(),
            records: records.into(),
            encoded_bytes,
        });

        let error = right.apply_block(forged, left.node_id()).unwrap_err();
        assert!(error.to_string().contains("provenance"));
        assert_eq!(right.get("one"), None);
    }

    #[test]
    fn fully_compacted_history_uses_state_transfer() {
        let left = map("left");
        let right = map("right");
        left.set("key", "value").unwrap();
        left.seal_pending().unwrap();
        for shard in &left.inner.shards {
            let mut shard = shard.lock();
            shard.blocks.clear();
            shard.retained_block_bytes = 0;
        }

        let report = left.sync_with(&right, SyncOptions::default()).unwrap();
        assert_eq!(report.state_snapshot_fallbacks, 1);
        assert_eq!(right.get("key"), Some(b"value".to_vec()));
        let second = left.sync_with(&right, SyncOptions::default()).unwrap();
        assert_eq!(second.state_snapshot_fallbacks, 0);
        assert_eq!(second.applied_mutations, 0);
    }

    #[test]
    fn state_transfer_rejects_unmaterialized_local_evictions() {
        let left = map("left");
        let recovery_peer = map("right");
        let fresh_peer = map("fresh");
        let token = left.set("key", "value").unwrap();
        left.sync_with(&recovery_peer, SyncOptions::default())
            .unwrap();
        assert_eq!(
            left.evict_local_exact("key", &token.dot).unwrap(),
            EvictionOutcome::Evicted
        );
        for shard in &left.inner.shards {
            let mut shard = shard.lock();
            shard.blocks.clear();
            shard.retained_block_bytes = 0;
        }

        let error = left
            .sync_with(&fresh_peer, SyncOptions::default())
            .unwrap_err();
        assert!(error.to_string().contains("materialized values"));
        assert_eq!(fresh_peer.get("key"), None);
    }

    #[test]
    fn snapshot_round_trip_preserves_tombstones_and_causal_state() {
        let source = map("left");
        source.set("live", "value").unwrap();
        source.set("deleted", "old").unwrap();
        source.delete("deleted").unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("active.snapshot");
        source.save_snapshot(&path).unwrap();

        let mut config = ActiveSyncConfig::new("test-cluster", NodeId::new("left").unwrap());
        config.incarnation_id = IncarnationId(999);
        let restored = ActiveShardMap::load_snapshot(4, config, &path).unwrap();
        assert_eq!(restored.get("live"), Some(b"value".to_vec()));
        assert_eq!(restored.get("deleted"), None);
        assert_eq!(restored.health_snapshot().tombstones, 1);
        assert_eq!(restored.health_snapshot().pending_records, 0);

        let peer = map("right");
        let report = restored.sync_with(&peer, SyncOptions::default()).unwrap();
        assert_eq!(report.state_snapshot_fallbacks, 1);
        assert_eq!(peer.get("live"), Some(b"value".to_vec()));
        assert_eq!(peer.get("deleted"), None);

        let peer = map("right");
        restored.sync_with(&peer, SyncOptions::default()).unwrap();
        assert_eq!(peer.get("live"), Some(b"value".to_vec()));
        assert_eq!(peer.get("deleted"), None);
    }

    #[test]
    fn snapshot_rejects_corruption_and_unmaterialized_values() {
        let source = map("left");
        let peer = map("right");
        let token = source.set("key", "value").unwrap();
        source.sync_with(&peer, SyncOptions::default()).unwrap();
        source.evict_local_exact("key", &token.dot).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("active.snapshot");
        assert!(source.save_snapshot(&path).is_err());

        peer.save_snapshot(&path).unwrap();
        let mut encoded = fs::read(&path).unwrap();
        *encoded.last_mut().unwrap() ^= 0x01;
        fs::write(&path, encoded).unwrap();
        let mut config = ActiveSyncConfig::new("test-cluster", NodeId::new("right").unwrap());
        config.incarnation_id = IncarnationId(999);
        assert!(ActiveShardMap::load_snapshot(4, config, &path).is_err());
    }

    #[test]
    fn incompatible_peers_fail_before_exchange() {
        let left = map("left");
        let right = ActiveShardMap::new(
            4,
            ActiveSyncConfig::new("another-cluster", NodeId::new("right").unwrap()),
        )
        .unwrap();
        assert!(left.sync_with(&right, SyncOptions::default()).is_err());
    }
}

#[cfg(all(test, feature = "active-sync-consensus-ordered-eventual"))]
mod deterministic_tests;
