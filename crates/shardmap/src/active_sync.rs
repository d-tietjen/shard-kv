//! Eventually consistent, active-active synchronization for exact point values.
//!
//! This module is deliberately feature gated. It keeps causal metadata and
//! block retention out of the ordinary [`crate::ShardMap`] layout and hot path.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes as SharedBytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use smallvec::SmallVec;

use crate::storage::{EmbeddedKeyRoute, EmbeddedStore, FastHashMap, hash_key, now_millis};
use crate::{Result, ShardCacheError};

#[cfg(feature = "active-sync-tls")]
mod tls;
#[cfg(feature = "active-sync-tls")]
pub use tls::{
    ActiveSyncAuthorizedPeer, ActiveSyncTlsClientCredentials, ActiveSyncTlsPeer,
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
    pub shard_count: usize,
    pub live_versions: usize,
    pub tombstones: usize,
    pub evicted_versions: usize,
    pub conflicted_versions: usize,
    pub pending_records: usize,
    pub retained_blocks: usize,
    pub retained_block_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct ActiveShardMap {
    inner: Arc<ActiveShardMapInner>,
}

#[derive(Debug)]
struct ActiveShardMapInner {
    store: EmbeddedStore,
    config: ActiveSyncConfig,
    shards: Box<[RwLock<ActiveShardState>]>,
    special_reads: Box<[AtomicBool]>,
    // Monotonic so a readable store hit can bypass metadata unless this shard
    // has ever materialized a governance conflict.
    conflict_reads: Box<[AtomicBool]>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CausalOrigin {
    node_id: NodeId,
    incarnation_id: IncarnationId,
    shard_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CausalFrontier {
    origin: CausalOrigin,
    sequence: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct CausalContext(SmallVec<[CausalFrontier; 4]>);

impl CausalContext {
    fn observes(&self, dot: &MutationDot) -> bool {
        let origin = CausalOrigin::from(dot);
        self.0
            .binary_search_by(|frontier| frontier.origin.cmp(&origin))
            .ok()
            .is_some_and(|index| self.0[index].sequence >= dot.sequence)
    }

    fn observe(&mut self, dot: &MutationDot) {
        self.observe_origin(CausalOrigin::from(dot), dot.sequence);
    }

    fn join(&mut self, other: &Self) {
        for frontier in &other.0 {
            self.observe_origin(frontier.origin.clone(), frontier.sequence);
        }
    }

    fn observe_origin(&mut self, origin: CausalOrigin, sequence: u64) -> bool {
        match self
            .0
            .binary_search_by(|frontier| frontier.origin.cmp(&origin))
        {
            Ok(index) => {
                self.0[index].sequence = self.0[index].sequence.max(sequence);
                false
            }
            Err(index) => {
                self.0.insert(index, CausalFrontier { origin, sequence });
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
            .map(|frontier| (&frontier.origin, frontier.sequence))
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
    ClusterEvict { target: MutationDot },
}

#[derive(Debug, Clone)]
struct ActiveMutation {
    dot: MutationDot,
    hlc: HybridLogicalClock,
    context: CausalContext,
    key_hash: u64,
    key: SharedBytes,
    value: Option<SharedBytes>,
    expire_at_ms: Option<u64>,
    governance: Option<SharedBytes>,
    kind: MutationKind,
}

impl ActiveMutation {
    fn estimated_bytes(&self) -> usize {
        160usize
            .saturating_add(self.key.len())
            .saturating_add(self.value.as_ref().map_or(0, SharedBytes::len))
            .saturating_add(self.governance.as_ref().map_or(0, SharedBytes::len))
            .saturating_add(self.context.len().saturating_mul(96))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Residency {
    Resident,
    Evicted { generation: u64 },
    ClusterEvicted,
}

#[derive(Debug, Clone)]
struct VersionState {
    dot: MutationDot,
    hlc: HybridLogicalClock,
    context: CausalContext,
    kind: MutationKind,
    expire_at_ms: Option<u64>,
    governance: Option<SharedBytes>,
    residency: Residency,
    recovery_peers: HashSet<NodeId>,
    governance_conflict: bool,
}

impl VersionState {
    fn from_mutation(mutation: &ActiveMutation, source_peer: Option<&NodeId>) -> Self {
        let residency = match mutation.kind {
            MutationKind::Set if mutation.value.is_some() => Residency::Resident,
            MutationKind::Set => Residency::Evicted { generation: 0 },
            MutationKind::Tombstone(_) => Residency::Resident,
            MutationKind::ClusterEvict { .. } => Residency::ClusterEvicted,
        };
        let mut recovery_peers = HashSet::new();
        if mutation.value.is_some()
            && matches!(mutation.kind, MutationKind::Set)
            && let Some(peer) = source_peer
        {
            recovery_peers.insert(peer.clone());
        }
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
    records: Arc<[ActiveMutation]>,
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
    versions: FastHashMap<SharedBytes, VersionState>,
    pending: Vec<ActiveMutation>,
    pending_bytes: usize,
    blocks: VecDeque<Arc<SyncBlock>>,
    retained_block_bytes: usize,
    block_frontiers: BTreeMap<BlockOrigin, u64>,
    pending_block_gaps: BTreeMap<BlockOrigin, BTreeMap<u64, [u8; 32]>>,
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
    pub fn new(shard_count: usize, config: ActiveSyncConfig) -> Result<Self> {
        config.validate()?;
        if shard_count == 0 || !shard_count.is_power_of_two() {
            return Err(ShardCacheError::Config(
                "active-sync shard_count must be a nonzero power of two".into(),
            ));
        }
        let shards = (0..shard_count)
            .map(|_| RwLock::new(ActiveShardState::default()))
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
                special_reads,
                conflict_reads,
            }),
        })
    }

    pub fn shard_count(&self) -> usize {
        self.inner.shards.len()
    }

    pub fn node_id(&self) -> &NodeId {
        &self.inner.config.node_id
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
        let mut shard = self.inner.shards[route.shard_id].write();
        let mutation = self.local_mutation(
            &mut shard,
            route.shard_id,
            route.key_hash,
            key,
            MutationKind::Set,
            Some(value),
            expire_at_ms,
            governance,
        )?;
        self.prepare_local_mutation_queue(&mut shard, &mutation)?;
        self.store_mutation(&mutation);
        replace_version(
            &mut shard.versions,
            mutation.key.clone(),
            VersionState::from_mutation(&mutation, None),
        );
        if expire_at_ms.is_some() || governance.is_some() {
            self.inner.special_reads[route.shard_id].store(true, AtomicOrdering::Release);
        }
        let dot = mutation.dot.clone();
        self.queue_local_mutation(&mut shard, mutation);
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
        let mut shard = self.inner.shards[route.shard_id].write();
        let mutation = self.local_mutation(
            &mut shard,
            route.shard_id,
            route.key_hash,
            key,
            MutationKind::Tombstone(kind),
            None,
            None,
            None,
        )?;
        self.prepare_local_mutation_queue(&mut shard, &mutation)?;
        self.store_mutation(&mutation);
        replace_version(
            &mut shard.versions,
            mutation.key.clone(),
            VersionState::from_mutation(&mutation, None),
        );
        let dot = mutation.dot.clone();
        self.queue_local_mutation(&mut shard, mutation);
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
            .read()
            .versions
            .get(key)
            .map(|version| version.dot.clone())
    }

    pub fn evict_local_exact(
        &self,
        key: impl AsRef<[u8]>,
        expected: &MutationDot,
    ) -> Result<EvictionOutcome> {
        let key = key.as_ref();
        let route = self.inner.store.route_key(key);
        let mut shard = self.inner.shards[route.shard_id].write();
        let Some(version) = shard.versions.get_mut(key) else {
            return Ok(EvictionOutcome::Missing);
        };
        if &version.dot != expected {
            return Ok(EvictionOutcome::Stale);
        }
        if matches!(version.residency, Residency::Evicted { .. }) {
            return Ok(EvictionOutcome::AlreadyEvicted);
        }
        if !matches!(version.kind, MutationKind::Set) {
            return Ok(EvictionOutcome::Missing);
        }
        if version.recovery_peers.is_empty() {
            return Ok(EvictionOutcome::NoRecoverySource);
        }
        let generation = match version.residency {
            Residency::Evicted { generation } => generation.saturating_add(1),
            _ => 1,
        };
        if !self.inner.store.delete(key) {
            return Ok(EvictionOutcome::Missing);
        }
        version.residency = Residency::Evicted { generation };
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
        let mut shard = self.inner.shards[route.shard_id].write();
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
                target: expected.clone(),
            },
            None,
            None,
            None,
        )?;
        self.prepare_local_mutation_queue(&mut shard, &mutation)?;
        self.store_mutation(&mutation);
        replace_version(
            &mut shard.versions,
            mutation.key.clone(),
            VersionState::from_mutation(&mutation, None),
        );
        let dot = mutation.dot.clone();
        self.queue_local_mutation(&mut shard, mutation);
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
            let shard = self.inner.shards[route.shard_id].read();
            let Some(version) = shard.versions.get(key) else {
                return Ok(None);
            };
            let Residency::Evicted { generation } = version.residency else {
                return self.try_get(key);
            };
            (version.dot.clone(), generation)
        };
        let Some(value) = peer.read_exact(key, &expected)? else {
            return Ok(None);
        };
        let mut shard = self.inner.shards[route.shard_id].write();
        let Some(version) = shard.versions.get_mut(key) else {
            return Ok(None);
        };
        if version.dot != expected || version.residency != (Residency::Evicted { generation }) {
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
        version.residency = Residency::Resident;
        version.recovery_peers.insert(peer.node_id().clone());
        Ok(Some(value))
    }

    fn read_exact(&self, key: &[u8], expected: &MutationDot) -> Result<Option<Vec<u8>>> {
        let route = self.inner.store.route_key(key);
        let shard = self.inner.shards[route.shard_id].read();
        let Some(version) = shard.versions.get(key) else {
            return Ok(None);
        };
        if &version.dot != expected
            || !matches!(version.kind, MutationKind::Set)
            || !matches!(version.residency, Residency::Resident)
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
            let mut shard = self.inner.shards[shard_id].write();
            if shard.pending.is_empty() {
                continue;
            }
            self.seal_shard(&mut shard, shard_id)?;
            sealed += 1;
        }
        Ok(sealed)
    }

    fn seal_pending_shard(&self, shard_id: usize) -> Result<bool> {
        let shard =
            self.inner.shards.get(shard_id).ok_or_else(|| {
                ShardCacheError::Protocol("active-sync shard is out of range".into())
            })?;
        let mut shard = shard.write();
        if shard.pending.is_empty() {
            return Ok(false);
        }
        self.seal_shard(&mut shard, shard_id)?;
        Ok(true)
    }

    pub fn health_snapshot(&self) -> ActiveSyncHealthSnapshot {
        let mut health = ActiveSyncHealthSnapshot {
            shard_count: self.shard_count(),
            ..ActiveSyncHealthSnapshot::default()
        };
        for shard in &self.inner.shards {
            let shard = shard.read();
            health.pending_records += shard.pending.len();
            health.retained_blocks += shard.blocks.len();
            health.retained_block_bytes += shard.retained_block_bytes;
            for version in shard.versions.values() {
                match version.kind {
                    MutationKind::Set => health.live_versions += 1,
                    MutationKind::Tombstone(_) => health.tombstones += 1,
                    MutationKind::ClusterEvict { .. } => health.evicted_versions += 1,
                }
                if matches!(version.residency, Residency::Evicted { .. }) {
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
            let shard = shard.read();
            for (origin, sequence) in &shard.block_frontiers {
                block_frontiers.push(SnapshotFrontier::from_block_origin(origin, *sequence));
            }
            for (key, version) in &shard.versions {
                let value = match version.kind {
                    MutationKind::Set => {
                        if !matches!(version.residency, Residency::Resident) {
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
            let mut shard = shard.write();
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
            let mut shard = self.inner.shards[route.shard_id].write();
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
    ) -> Result<ActiveMutation> {
        let (context, key) = shard.versions.get_key_value(key).map_or_else(
            || (CausalContext::default(), SharedBytes::copy_from_slice(key)),
            |(stored_key, version)| (version.full_context(), stored_key.clone()),
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
        let dot = MutationDot {
            node_id: self.inner.config.node_id.clone(),
            incarnation_id: self.inner.config.incarnation_id,
            shard_id: shard_id as u32,
            sequence: shard.next_mutation_sequence,
        };
        Ok(ActiveMutation {
            dot,
            hlc: shard.clock.tick(now_millis()),
            context,
            key_hash,
            key,
            value,
            expire_at_ms,
            governance: governance.map(SharedBytes::copy_from_slice),
            kind,
        })
    }

    fn prepare_local_mutation_queue(
        &self,
        shard: &mut ActiveShardState,
        mutation: &ActiveMutation,
    ) -> Result<()> {
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
            self.seal_shard(shard, shard_id)?;
        }
        Ok(())
    }

    fn queue_local_mutation(&self, shard: &mut ActiveShardState, mutation: ActiveMutation) {
        let bytes = mutation.estimated_bytes();
        shard.pending_bytes = shard.pending_bytes.saturating_add(bytes);
        shard.pending.push(mutation);
    }

    fn seal_shard(&self, shard: &mut ActiveShardState, shard_id: usize) -> Result<()> {
        if shard.pending.is_empty() {
            return Ok(());
        }
        shard.next_block_sequence = shard.next_block_sequence.checked_add(1).ok_or_else(|| {
            ShardCacheError::Persistence("active-sync block sequence exhausted".into())
        })?;
        let records: Arc<[ActiveMutation]> = std::mem::take(&mut shard.pending).into();
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
        self.retain_block(shard, block);
        Ok(())
    }

    fn retain_block(&self, shard: &mut ActiveShardState, block: Arc<SyncBlock>) {
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
        }
    }

    fn store_mutation(&self, mutation: &ActiveMutation) {
        let route = self
            .inner
            .store
            .route_key_prehashed(mutation.key_hash, &mutation.key);
        match mutation.kind {
            MutationKind::Set => {
                if let Some(value) = &mutation.value {
                    self.inner
                        .store
                        .set_value_bytes_routed_with_governance_then(
                            route,
                            &mutation.key,
                            value.clone(),
                            mutation.governance.clone(),
                            mutation.expire_at_ms,
                            now_millis(),
                            || {},
                        );
                }
            }
            MutationKind::Tombstone(_) | MutationKind::ClusterEvict { .. } => {
                self.inner
                    .store
                    .delete_routed_then(route, &mutation.key, now_millis(), || {});
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
        let mut shard = shard_lock.write();
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
            self.apply_remote_mutation(&mut shard, mutation, source_peer, &mut stats)?;
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
        self.retain_block(&mut shard, block);
        Ok(stats)
    }

    fn apply_remote_mutation(
        &self,
        shard: &mut ActiveShardState,
        incoming: &ActiveMutation,
        source_peer: &NodeId,
        stats: &mut ApplyStats,
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
                local.recovery_peers.insert(source_peer.clone());
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
        let incoming_wins = concurrent_incoming_wins(local, incoming);
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
                let shard = self.inner.shards[route.shard_id].read();
                let Some(version) = shard.versions.get(key) else {
                    return Ok(false);
                };
                if version.governance_conflict {
                    return Err(ShardCacheError::Command(
                        "active-sync governance conflict requires explicit resolution".into(),
                    ));
                }
                if !matches!(version.kind, MutationKind::Set)
                    || !matches!(version.residency, Residency::Resident)
                {
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
        let mut shard = self.inner.shards[route.shard_id].write();
        let should_expire = shard.versions.get(key).is_some_and(|version| {
            matches!(version.kind, MutationKind::Set)
                && version
                    .expire_at_ms
                    .is_some_and(|deadline| deadline <= now_millis())
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
        )?;
        self.prepare_local_mutation_queue(&mut shard, &mutation)?;
        self.store_mutation(&mutation);
        replace_version(
            &mut shard.versions,
            mutation.key.clone(),
            VersionState::from_mutation(&mutation, None),
        );
        self.queue_local_mutation(&mut shard, mutation);
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
            for (origin, sequence) in &shard.read().block_frontiers {
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
        Ok(shard.read().block_frontiers.clone())
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
        let shard = shard.read();
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
        let shard = shard.read();
        let mut records = Vec::new();
        let mut bytes = 0usize;
        for (key, version) in &shard.versions {
            if &version.dot.node_id != origin_node {
                continue;
            }
            let value = match version.kind {
                MutationKind::Set => {
                    if !matches!(version.residency, Residency::Resident) {
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
        let mut shard = shard.write();
        let mut stats = ApplyStats::default();
        self.apply_remote_mutation(&mut shard, mutation, source_peer, &mut stats)?;
        Ok(stats)
    }

    fn accept_snapshot_frontiers(
        &self,
        shard_id: usize,
        frontiers: &BTreeMap<BlockOrigin, u64>,
    ) -> Result<()> {
        let shard = self.inner.shards.get(shard_id).ok_or_else(|| {
            ShardCacheError::Protocol("active-sync snapshot shard is out of range".into())
        })?;
        let mut shard = shard.write();
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
            let shard = shard.read();
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
                .read()
                .blocks
                .iter()
                .any(|block| block.sequence > receiver.get(&block.origin).copied().unwrap_or(0))
        })
    }

    fn state_snapshot(&self) -> Result<Vec<ActiveMutation>> {
        let mut records = Vec::new();
        let mut bytes = 0usize;
        for (shard_id, shard) in self.inner.shards.iter().enumerate() {
            let shard = shard.read();
            for (key, version) in &shard.versions {
                let value = match version.kind {
                    MutationKind::Set => {
                        if !matches!(version.residency, Residency::Resident) {
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
            let shard_id = mutation.dot.shard_id as usize;
            let Some(shard) = destination.inner.shards.get(shard_id) else {
                return Err(ShardCacheError::Protocol(
                    "active-sync snapshot shard is out of range".into(),
                ));
            };
            let mut shard = shard.write();
            let mut stats = ApplyStats::default();
            destination.apply_remote_mutation(&mut shard, mutation, source_peer, &mut stats)?;
            report.applied_mutations += stats.applied;
            report.duplicate_mutations += stats.duplicates;
            report.conflicts += stats.conflicts;
        }
        Ok(())
    }

    fn mark_peer_recovery(&self, peer: &Self) {
        for shard_id in 0..self.shard_count() {
            let peer_versions = {
                let shard = peer.inner.shards[shard_id].read();
                shard
                    .versions
                    .iter()
                    .filter(|(_, version)| {
                        matches!(version.kind, MutationKind::Set)
                            && matches!(version.residency, Residency::Resident)
                    })
                    .map(|(key, version)| (key.clone(), version.dot.clone()))
                    .collect::<Vec<_>>()
            };
            let mut shard = self.inner.shards[shard_id].write();
            for (key, dot) in peer_versions {
                if let Some(version) = shard.versions.get_mut(&key)
                    && version.dot == dot
                {
                    version.recovery_peers.insert(peer.node_id().clone());
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
                target: SnapshotDot::from(target),
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
            expire_at_ms: version.expire_at_ms,
            governance: version.governance.as_deref().map(<[u8]>::to_vec),
            governance_conflict: version.governance_conflict,
        }
    }

    fn to_mutation(&self) -> Result<ActiveMutation> {
        let dot = self.dot.to_dot()?;
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
                target: target.to_dot()?,
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
            expire_at_ms: self.expire_at_ms,
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

fn replace_version(
    versions: &mut FastHashMap<SharedBytes, VersionState>,
    key: SharedBytes,
    replacement: VersionState,
) {
    if let Some(version) = versions.get_mut(key.as_ref()) {
        *version = replacement;
    } else {
        versions.insert(key, replacement);
    }
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

fn concurrent_incoming_wins(local: &VersionState, incoming: &ActiveMutation) -> bool {
    match (&local.kind, &incoming.kind) {
        (MutationKind::Tombstone(_), MutationKind::Set) => false,
        (MutationKind::Set, MutationKind::Tombstone(_)) => true,
        (MutationKind::ClusterEvict { target }, MutationKind::Set) => &incoming.dot != target,
        (MutationKind::Set, MutationKind::ClusterEvict { target }) => &local.dot == target,
        (MutationKind::Tombstone(_), MutationKind::ClusterEvict { .. }) => false,
        (MutationKind::ClusterEvict { .. }, MutationKind::Tombstone(_)) => true,
        _ => {
            (incoming.hlc, &incoming.dot.node_id, &incoming.dot).cmp(&(
                local.hlc,
                &local.dot.node_id,
                &local.dot,
            )) == Ordering::Greater
        }
    }
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
    use super::*;

    fn map(node: &str) -> ActiveShardMap {
        let mut config = ActiveSyncConfig::new("test-cluster", NodeId::new(node).unwrap());
        config.incarnation_id = IncarnationId(u128::from(node.as_bytes()[0]));
        config.max_clock_skew = Duration::from_secs(60);
        ActiveShardMap::new(4, config).unwrap()
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
            .read()
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
            .read()
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
            .find_map(|shard| shard.read().blocks.front().cloned())
            .unwrap();
        let valid = original.records[0].clone();
        let mut forged_record = valid.clone();
        forged_record.dot.node_id = NodeId::new("forged").unwrap();
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
            let mut shard = shard.write();
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
            let mut shard = shard.write();
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
