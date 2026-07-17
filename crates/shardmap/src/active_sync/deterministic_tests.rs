use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use deterministic_test_env::{
    HermeticEventLog, HermeticNode, HermeticNodeFuture, HermeticPlan, HermeticSimConfig,
    SimEnvError, replay_matches_with, run_plan_with, splitmix64,
};
use sha2::{Digest, Sha256};

use super::blossom::{
    BlossomConflictCertificate, BlossomConflictConsensus, BlossomConflictOrderer,
};
use super::*;

const NODE_COUNT: usize = 3;
const CONTENDED_KEY: &[u8] = b"contended-key";

struct AdversarialConsensus {
    seed: u64,
    available: AtomicBool,
    corrupt_next: AtomicBool,
}

impl BlossomConflictConsensus for AdversarialConsensus {
    fn commit_conflict(
        &self,
        group_id: [u8; 32],
        claim_payload: &[u8],
        _deadline: Duration,
    ) -> Result<BlossomConflictCertificate> {
        if !self.available.load(Ordering::Acquire) {
            return Err(ShardCacheError::Protocol(
                "deterministic consensus outage".into(),
            ));
        }

        let mut claim_digest: [u8; 32] = Sha256::digest(
            [
                b"shardmap.active-sync.conflict-claim.v1".as_slice(),
                claim_payload,
            ]
            .concat(),
        )
        .into();
        let mut epoch = Sha256::new();
        epoch.update(b"shardmap.active-sync.deterministic-test.v1");
        epoch.update(self.seed.to_le_bytes());
        epoch.update(claim_digest);
        let epoch_hash: [u8; 32] = epoch.finalize().into();
        let epoch_nonce = u64::from_le_bytes(epoch_hash[..8].try_into().unwrap()).max(3);

        if self.corrupt_next.swap(false, Ordering::AcqRel) {
            claim_digest[0] ^= 0x80;
        }
        Ok(BlossomConflictCertificate {
            group_id,
            epoch_nonce,
            epoch_hash,
            claim_digest,
            candidate_epochs: candidate_epochs(claim_payload),
        })
    }
}

fn candidate_epochs(claim_payload: &[u8]) -> [u64; 2] {
    let mut offset = 4 + 32 + 4 + 32;
    let mut epochs = [0; 2];
    for epoch in &mut epochs {
        let node_len = usize::from(claim_payload[offset]);
        offset += 1;
        let node_id = &claim_payload[offset..offset + node_len];
        *epoch = u64::from(node_id[node_id.len() - 1] - b'0') + 1;
        offset += node_len + 16 + 4 + 8 + 1;
    }
    epochs
}

#[derive(Clone)]
enum ChaosRequest {
    DeliverBlock { origin: usize },
    SetConsensusAvailable(bool),
}

struct ChaosNode {
    map: ActiveShardMap,
    source_ids: Arc<[NodeId]>,
    blocks: Arc<[Arc<SyncBlock>]>,
    consensus: Arc<AdversarialConsensus>,
}

impl HermeticNode<ChaosRequest> for ChaosNode {
    fn handle_request<'a>(&'a mut self, request: ChaosRequest) -> HermeticNodeFuture<'a> {
        Box::pin(async move {
            match request {
                ChaosRequest::DeliverBlock { origin } => self
                    .map
                    .apply_block(Arc::clone(&self.blocks[origin]), &self.source_ids[origin])
                    .map(|_| "block-applied")
                    .map_err(|error| SimEnvError::App(error.to_string())),
                ChaosRequest::SetConsensusAvailable(available) => {
                    self.consensus.available.store(available, Ordering::Release);
                    Ok("consensus-state-changed")
                }
            }
        })
    }
}

struct ChaosHarness {
    maps: Vec<ActiveShardMap>,
    nodes: Vec<ChaosNode>,
}

fn build_harness(seed: u64) -> ChaosHarness {
    let consensus = Arc::new(AdversarialConsensus {
        seed,
        available: AtomicBool::new(false),
        corrupt_next: AtomicBool::new(true),
    });
    let maps = (0..NODE_COUNT)
        .map(|node| {
            let mut config = ActiveSyncConfig::new(
                "deterministic-conflict-cluster",
                NodeId::new(format!("node-{node}")).unwrap(),
            );
            config.incarnation_id = IncarnationId((node + 1) as u128);
            config.max_clock_skew = Duration::from_secs(60);
            let orderer = Arc::new(
                BlossomConflictOrderer::new(consensus.clone(), Duration::from_secs(1)).unwrap(),
            );
            ActiveShardMap::new_with_conflict_orderer(1, config, orderer).unwrap()
        })
        .collect::<Vec<_>>();

    for (node, map) in maps.iter().enumerate() {
        map.set(CONTENDED_KEY, format!("value-from-node-{node}"))
            .unwrap();
        map.seal_pending().unwrap();
    }
    let source_ids: Arc<[NodeId]> = maps
        .iter()
        .map(|map| map.node_id().clone())
        .collect::<Vec<_>>()
        .into();
    let blocks: Arc<[Arc<SyncBlock>]> = maps
        .iter()
        .map(|map| {
            Arc::clone(
                map.inner.shards[0]
                    .lock()
                    .blocks
                    .back()
                    .expect("initial mutation must produce a block"),
            )
        })
        .collect::<Vec<_>>()
        .into();
    let nodes = maps
        .iter()
        .cloned()
        .map(|map| ChaosNode {
            map,
            source_ids: Arc::clone(&source_ids),
            blocks: Arc::clone(&blocks),
            consensus: Arc::clone(&consensus),
        })
        .collect();
    ChaosHarness { maps, nodes }
}

fn request_kind(request: &ChaosRequest) -> &'static str {
    match request {
        ChaosRequest::DeliverBlock { .. } => "deliver-block",
        ChaosRequest::SetConsensusAvailable(_) => "set-consensus-available",
    }
}

fn build_plan(seed: u64) -> HermeticPlan<ChaosRequest> {
    let mut plan = HermeticPlan::new(
        NODE_COUNT,
        (),
        HermeticSimConfig {
            seed,
            default_latency_ms: 0,
            jitter_ms: 3,
            drop_ppm: 0,
        },
    );

    // The first delivery must fail without advancing its source frontier.
    plan.request(0, 1, 0, ChaosRequest::DeliverBlock { origin: 1 });
    plan.request(5, 0, 0, ChaosRequest::SetConsensusAvailable(true));

    // Crash one target across a delivery, then heal it. The next accepted
    // delivery receives one deliberately invalid consensus certificate.
    plan.node_down(1, 2);
    plan.request(2, 0, 2, ChaosRequest::DeliverBlock { origin: 0 });
    plan.node_up(9, 2);

    for target in 0..NODE_COUNT {
        let mut origins = (0..NODE_COUNT)
            .filter(|origin| *origin != target)
            .collect::<Vec<_>>();
        if splitmix64(seed ^ target as u64) & 1 == 1 {
            origins.reverse();
        }
        plan.set_latency(
            10,
            [target],
            (splitmix64(seed ^ (target as u64) << 9) % 5) + 1,
        );
        for (offset, origin) in origins.into_iter().enumerate() {
            plan.request(
                11 + offset as u64,
                origin,
                target,
                ChaosRequest::DeliverBlock { origin },
            );
        }
    }

    // Remove latency and redeliver every block twice. At this point every
    // healthy node must have acknowledged every source frontier exactly once.
    plan.set_latency(30, 0..NODE_COUNT, 0);
    for round in 0..2 {
        for target in 0..NODE_COUNT {
            for origin in 0..NODE_COUNT {
                plan.request(
                    31 + round,
                    origin,
                    target,
                    ChaosRequest::DeliverBlock { origin },
                );
            }
        }
    }
    plan
}

fn assert_converged(seed: u64, maps: &[ActiveShardMap]) {
    let expected_value = maps[0].get(CONTENDED_KEY);
    let expected_dot = maps[0].version_dot(CONTENDED_KEY);
    for map in &maps[1..] {
        assert_eq!(map.get(CONTENDED_KEY), expected_value, "value; seed={seed}");
        assert_eq!(
            map.version_dot(CONTENDED_KEY),
            expected_dot,
            "version dot; seed={seed}"
        );
    }
}

async fn run_once(seed: u64) -> (HermeticEventLog, Vec<ActiveShardMap>) {
    let plan = build_plan(seed);
    let harness = build_harness(seed);
    let maps = harness.maps;
    let log = run_plan_with(&plan, harness.nodes, request_kind)
        .await
        .unwrap();
    (log, maps)
}

#[tokio::test(flavor = "current_thread")]
async fn reordered_duplicate_wal_delivery_converges_after_faults_and_replays() {
    for seed in 0..32 {
        let (log, maps) = run_once(seed).await;
        assert!(log.unavailable_count() >= 1, "seed={seed}");
        assert!(
            log.records
                .iter()
                .filter(|record| matches!(
                    record.outcome,
                    deterministic_test_env::HermeticOutcome::Error { .. }
                ))
                .count()
                >= 2,
            "seed={seed}"
        );
        assert_converged(seed, &maps);

        let plan = build_plan(seed);
        let replay = build_harness(seed);
        assert!(
            replay_matches_with(&plan, &log, replay.nodes, request_kind)
                .await
                .unwrap(),
            "event replay diverged; seed={seed}"
        );
        assert_converged(seed, &replay.maps);
    }
}
