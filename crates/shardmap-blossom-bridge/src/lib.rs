//! Verified Blossom adapter for ShardMap active-active conflict ordering.
//!
//! Blossom's native TCP service is intentionally restricted to loopback by
//! this adapter. Put an authenticated mTLS proxy in front of remote Blossom
//! nodes; quorum reads and signed blocks provide integrity, not transport
//! confidentiality.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use blossom::{
    ConsensusGroupId, Epoch, EpochChain, EpochTarget, HashType, Keypair, Nonce, PubKey, SecKey,
    Transaction, WireRequest, WireResponse, decode_wire_response_payload, signed_block,
    supermajority_count, write_wire_request,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use shardmap::{
    ActiveShardMap, ActiveSyncConfig, BlossomConflictCertificate, BlossomConflictConsensus,
    BlossomConflictOrderer, BlossomConflictOrdererOptions, Result, ShardCacheError,
};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

const CLAIM_MAGIC: &[u8; 4] = b"SCC1";
const CLAIM_FIXED_BYTES: usize = 4 + 32 + 4 + 32;
const MAX_CLAIM_BYTES: usize = 768;
const MAX_ENDPOINTS: usize = 64;
const MAX_GROUPS: usize = 4_096;
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_INFLIGHT_RESPONSE_BYTES: usize = 32 * 1024 * 1024;
const STATE_VERSION: u8 = 1;

/// A validator-set generation. Generations must cover every nonce from zero
/// without gaps; an overlap is represented by keeping both keys in one set.
#[derive(Debug, Clone)]
pub struct BlossomValidatorGeneration {
    pub active_from_nonce: u64,
    pub validators: BTreeSet<PubKey>,
}

/// Trusted mapping from one ShardMap conflict group to one Blossom group.
#[derive(Debug, Clone)]
pub struct BlossomGroupConfig {
    pub shardmap_group_id: [u8; 32],
    pub blossom_group_id: ConsensusGroupId,
    pub checkpoint_nonce: u64,
    pub checkpoint_hash: HashType,
    pub validator_generations: Vec<BlossomValidatorGeneration>,
}

/// Hard limits and trust roots for the TCP bridge.
#[derive(Debug, Clone)]
pub struct BlossomValidatorEndpoint {
    /// Loopback listener for this validator or its identity-bound mTLS proxy.
    pub address: SocketAddr,
    pub validator: PubKey,
}

/// Hard limits and trust roots for the TCP bridge.
#[derive(Clone)]
pub struct BlossomTcpBridgeConfig {
    pub endpoints: Vec<BlossomValidatorEndpoint>,
    pub signer_secret_path: PathBuf,
    pub state_dir: PathBuf,
    pub groups: Vec<BlossomGroupConfig>,
    pub poll_interval: Duration,
    pub max_epochs_per_fetch: u32,
    pub max_candidates_per_group: usize,
    pub max_receipts_per_group: usize,
    pub max_state_bytes_per_group: u64,
    pub max_response_bytes: usize,
}

impl std::fmt::Debug for BlossomTcpBridgeConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlossomTcpBridgeConfig")
            .field("endpoints", &self.endpoints)
            .field("signer_secret_path", &"[REDACTED]")
            .field("state_dir", &self.state_dir)
            .field("groups", &self.groups)
            .field("poll_interval", &self.poll_interval)
            .field("max_epochs_per_fetch", &self.max_epochs_per_fetch)
            .field("max_candidates_per_group", &self.max_candidates_per_group)
            .field("max_receipts_per_group", &self.max_receipts_per_group)
            .field("max_state_bytes_per_group", &self.max_state_bytes_per_group)
            .field("max_response_bytes", &self.max_response_bytes)
            .finish()
    }
}

impl BlossomTcpBridgeConfig {
    fn validate(&self) -> Result<()> {
        if self.endpoints.is_empty()
            || self.groups.is_empty()
            || self.endpoints.len() > MAX_ENDPOINTS
            || self.groups.len() > MAX_GROUPS
            || self.poll_interval.is_zero()
            || self.max_epochs_per_fetch == 0
            || self.max_epochs_per_fetch > 4_096
            || self.max_candidates_per_group == 0
            || self.max_receipts_per_group == 0
            || self.max_state_bytes_per_group == 0
            || self.max_response_bytes == 0
            || self.max_response_bytes > MAX_RESPONSE_BYTES
            || self
                .max_response_bytes
                .checked_mul(self.endpoints.len())
                .is_none_or(|bytes| bytes > MAX_INFLIGHT_RESPONSE_BYTES)
        {
            return Err(config_error(
                "invalid zero or out-of-range Blossom bridge limit",
            ));
        }
        if self
            .endpoints
            .iter()
            .any(|endpoint| !endpoint.address.ip().is_loopback())
        {
            return Err(config_error(
                "Blossom TCP endpoints must be loopback; use an authenticated mTLS proxy remotely",
            ));
        }
        let endpoint_validators = self
            .endpoints
            .iter()
            .map(|endpoint| endpoint.validator)
            .collect::<BTreeSet<_>>();
        if endpoint_validators.len() != self.endpoints.len() {
            return Err(config_error(
                "duplicate Blossom validator endpoint identity",
            ));
        }
        let mut group_ids = BTreeSet::new();
        for group in &self.groups {
            if !group_ids.insert(group.shardmap_group_id) {
                return Err(config_error("duplicate ShardMap Blossom group mapping"));
            }
            validate_generations(group)?;
            for generation in &group.validator_generations {
                let available = generation
                    .validators
                    .intersection(&endpoint_validators)
                    .count();
                if available < supermajority_count(generation.validators.len()) {
                    return Err(config_error(
                        "Blossom endpoints do not cover a validator supermajority",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Concrete, deadline-enforced and restart-durable Blossom bridge.
pub struct BlossomTcpConflictBridge {
    config: Arc<BlossomTcpBridgeConfig>,
    runtime: tokio::runtime::Runtime,
    groups: HashMap<[u8; 32], Arc<Mutex<GroupRuntime>>>,
}

impl std::fmt::Debug for BlossomTcpConflictBridge {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlossomTcpConflictBridge")
            .field("endpoints", &self.config.endpoints)
            .field("state_dir", &self.config.state_dir)
            .field("groups", &self.groups.len())
            .finish_non_exhaustive()
    }
}

impl BlossomTcpConflictBridge {
    pub fn open(config: BlossomTcpBridgeConfig) -> Result<Self> {
        config.validate()?;
        validate_secret_file(&config.signer_secret_path)?;
        fs::create_dir_all(&config.state_dir).map_err(persistence_error)?;
        let mut groups = HashMap::with_capacity(config.groups.len());
        for group in &config.groups {
            let state_path = state_path(&config.state_dir, group.shardmap_group_id);
            let state = load_group_state(&state_path, group, config.max_state_bytes_per_group)?;
            if state.candidate_epochs.len() > config.max_candidates_per_group
                || state.receipts.len() > config.max_receipts_per_group
            {
                return Err(ShardCacheError::Persistence(
                    "Blossom bridge state exceeds configured entry limits".into(),
                ));
            }
            groups.insert(
                group.shardmap_group_id,
                Arc::new(Mutex::new(GroupRuntime {
                    config: group.clone(),
                    state_path,
                    state,
                })),
            );
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("shardmap-blossom-bridge")
            .build()
            .map_err(|error| config_error(format!("failed to create bridge runtime: {error}")))?;
        Ok(Self {
            config: Arc::new(config),
            runtime,
            groups,
        })
    }

    /// Builds an active map using this verified bridge and the supplied bounded
    /// ordering policy.
    pub fn active_map(
        self: &Arc<Self>,
        shard_count: usize,
        active_sync: ActiveSyncConfig,
        ordering: BlossomConflictOrdererOptions,
    ) -> Result<ActiveShardMap> {
        let consensus: Arc<dyn BlossomConflictConsensus> = self.clone();
        let orderer = Arc::new(BlossomConflictOrderer::with_options(consensus, ordering)?);
        ActiveShardMap::new_with_conflict_orderer(shard_count, active_sync, orderer)
    }

    fn commit(
        &self,
        logical_group: [u8; 32],
        claim_payload: &[u8],
        deadline: Duration,
    ) -> Result<BlossomConflictCertificate> {
        validate_claim(claim_payload)?;
        if deadline.is_zero() {
            return Err(protocol_error("Blossom bridge deadline is zero"));
        }
        let group = self
            .groups
            .get(&logical_group)
            .ok_or_else(|| config_error("unconfigured ShardMap Blossom group"))?;
        let mut group = group.lock();
        let claim_digest = claim_digest(claim_payload);
        if let Some(receipt) = group.state.receipts.get(&digest_key(claim_digest)) {
            return certificate(logical_group, claim_digest, receipt);
        }

        let expires = Instant::now() + deadline;
        self.runtime.block_on(async {
            refresh_until_current(&self.config, &mut group, expires).await?;
            if let Some(receipt) = group.state.receipts.get(&digest_key(claim_digest)) {
                return certificate(logical_group, claim_digest, receipt);
            }

            let mut submitted_nonce =
                submit_claim(&self.config, &group.config, claim_payload, expires).await?;

            loop {
                refresh_until_current(&self.config, &mut group, expires).await?;
                if let Some(receipt) = group.state.receipts.get(&digest_key(claim_digest)) {
                    return certificate(logical_group, claim_digest, receipt);
                }
                if group.state.checkpoint_nonce >= submitted_nonce {
                    submitted_nonce =
                        submit_claim(&self.config, &group.config, claim_payload, expires).await?;
                }
                sleep_bounded(self.config.poll_interval, expires).await?;
            }
        })
    }
}

async fn submit_claim(
    config: &BlossomTcpBridgeConfig,
    group: &BlossomGroupConfig,
    claim_payload: &[u8],
    expires: Instant,
) -> Result<u64> {
    let secret = load_signer(&config.signer_secret_path)?;
    let signer = Keypair::from_secret(secret);
    let endpoint = config
        .endpoints
        .iter()
        .find(|endpoint| endpoint.validator == signer.public)
        .ok_or_else(|| config_error("Blossom signer has no configured validator endpoint"))?;
    let mut last_error = None;
    for candidate_endpoint in std::iter::once(endpoint).chain(
        config
            .endpoints
            .iter()
            .filter(|candidate| candidate.validator != endpoint.validator),
    ) {
        let target = match request_target(
            candidate_endpoint,
            group,
            expires,
            config.max_response_bytes,
        )
        .await
        {
            Ok(target) => target,
            Err(error) => {
                last_error = Some(error.to_string());
                continue;
            }
        };
        let submitted_nonce = target.nonce.value();
        let validators = consensus_validators_for(group, submitted_nonce)?;
        if !validators.contains(&signer.public) {
            return Err(config_error(
                "Blossom signing key is not trusted for the target validator generation",
            ));
        }
        let block = signed_block(target, secret, [Transaction::new(claim_payload)]);
        match submit_block(
            candidate_endpoint,
            group,
            block,
            expires,
            config.max_response_bytes,
        )
        .await
        {
            Ok(()) => return Ok(submitted_nonce),
            Err(error) => last_error = Some(error.to_string()),
        }
    }
    Err(protocol_error(format!(
        "all Blossom submission endpoints failed: {}",
        last_error.unwrap_or_else(|| "no endpoint attempted".into())
    )))
}

impl BlossomConflictConsensus for BlossomTcpConflictBridge {
    fn commit_conflict(
        &self,
        group_id: [u8; 32],
        claim_payload: &[u8],
        deadline: Duration,
    ) -> Result<BlossomConflictCertificate> {
        self.commit(group_id, claim_payload, deadline)
    }
}

#[derive(Debug)]
struct GroupRuntime {
    config: BlossomGroupConfig,
    state_path: PathBuf,
    state: PersistedGroupState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedGroupState {
    version: u8,
    shardmap_group_id: [u8; 32],
    blossom_group_id: [u8; 32],
    checkpoint_nonce: u64,
    checkpoint_hash: [u8; 32],
    candidate_epochs: BTreeMap<String, u64>,
    receipts: BTreeMap<String, PersistedReceipt>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedReceipt {
    epoch_nonce: u64,
    epoch_hash: [u8; 32],
    candidate_epochs: [u64; 2],
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedStateEnvelope {
    checksum: [u8; 32],
    state: PersistedGroupState,
}

async fn refresh_until_current(
    config: &BlossomTcpBridgeConfig,
    group: &mut GroupRuntime,
    expires: Instant,
) -> Result<()> {
    loop {
        let from_nonce = group.state.checkpoint_nonce.saturating_add(1);
        let chain = request_chain_quorum(config, &group.config, from_nonce, expires).await?;
        if chain.epochchain.is_empty() {
            return Ok(());
        }
        let fetched = chain.epochchain.len();
        apply_epochs(config, group, chain)?;
        if fetched < config.max_epochs_per_fetch as usize {
            return Ok(());
        }
    }
}

fn apply_epochs(
    config: &BlossomTcpBridgeConfig,
    group: &mut GroupRuntime,
    chain: EpochChain,
) -> Result<()> {
    for epoch in chain.epochchain {
        verify_epoch(&group.config, &group.state, &epoch)?;
        let nonce = epoch.body.nonce.value();
        let mut next_state = group.state.clone();
        for block in epoch.body.blocks.values() {
            block
                .verify_integrity()
                .map_err(|error| protocol_error(format!("invalid Blossom block: {error}")))?;
            if block.body.nonce.value() != nonce
                || block.body.last_epoch.0 != group.state.checkpoint_hash
            {
                return Err(protocol_error(
                    "Blossom block target does not match its epoch",
                ));
            }
            for transaction in &block.body.txs {
                let payload = transaction.payload();
                let Ok(parsed_candidates) = candidate_ids(payload) else {
                    continue;
                };
                for candidate_id in parsed_candidates {
                    let candidate_key = digest_key(candidate_id);
                    if !next_state.candidate_epochs.contains_key(&candidate_key)
                        && next_state.candidate_epochs.len() >= config.max_candidates_per_group
                    {
                        return Err(ShardCacheError::Backpressure(
                            "Blossom candidate rank state limit reached",
                        ));
                    }
                    next_state
                        .candidate_epochs
                        .entry(candidate_key)
                        .or_insert(nonce);
                }
                let digest = digest_key(claim_digest(payload));
                if !next_state.receipts.contains_key(&digest)
                    && next_state.receipts.len() >= config.max_receipts_per_group
                {
                    return Err(ShardCacheError::Backpressure(
                        "Blossom finalized receipt state limit reached",
                    ));
                }
                let ids = candidate_ids(payload)?;
                let ranks = [
                    *next_state
                        .candidate_epochs
                        .get(&digest_key(ids[0]))
                        .ok_or_else(|| {
                            protocol_error("missing finalized Blossom candidate rank")
                        })?,
                    *next_state
                        .candidate_epochs
                        .get(&digest_key(ids[1]))
                        .ok_or_else(|| {
                            protocol_error("missing finalized Blossom candidate rank")
                        })?,
                ];
                next_state
                    .receipts
                    .entry(digest)
                    .or_insert(PersistedReceipt {
                        epoch_nonce: nonce,
                        epoch_hash: epoch.hash.0,
                        candidate_epochs: ranks,
                    });
            }
        }
        next_state.checkpoint_nonce = nonce;
        next_state.checkpoint_hash = epoch.hash.0;
        persist_group_state(
            &group.state_path,
            &next_state,
            config.max_state_bytes_per_group,
        )?;
        group.state = next_state;
    }
    Ok(())
}

fn verify_epoch(
    config: &BlossomGroupConfig,
    state: &PersistedGroupState,
    epoch: &Epoch,
) -> Result<()> {
    let expected_nonce = state.checkpoint_nonce.saturating_add(1);
    if epoch.body.group_id != config.blossom_group_id
        || epoch.body.nonce.value() != expected_nonce
        || epoch.body.last_epoch.0 != state.checkpoint_hash
    {
        return Err(protocol_error(
            "non-contiguous or wrong-group Blossom epoch",
        ));
    }
    let mut recomputed = epoch.clone();
    recomputed.set_hash();
    if recomputed.hash != epoch.hash {
        return Err(protocol_error("Blossom epoch hash mismatch"));
    }
    let expected = validators_for(config, expected_nonce)?;
    let actual = epoch
        .body
        .verifiers
        .keys()
        .copied()
        .collect::<BTreeSet<_>>();
    if actual != *expected {
        return Err(protocol_error("Blossom epoch validator set mismatch"));
    }
    Ok(())
}

async fn request_target(
    endpoint: &BlossomValidatorEndpoint,
    group: &BlossomGroupConfig,
    expires: Instant,
    max_response_bytes: usize,
) -> Result<EpochTarget> {
    match request_endpoint(
        endpoint,
        group,
        WireRequest::NextNonce,
        expires,
        max_response_bytes,
    )
    .await?
    {
        WireResponse::NextNonce(target) if target.group_id == group.blossom_group_id => Ok(target),
        response => Err(protocol_error(format!(
            "unexpected Blossom NextNonce response: {}",
            response.kind()
        ))),
    }
}

async fn request_chain_quorum(
    config: &BlossomTcpBridgeConfig,
    group: &BlossomGroupConfig,
    from_nonce: u64,
    expires: Instant,
) -> Result<EpochChain> {
    let mut responses = Vec::with_capacity(config.endpoints.len());
    let mut pending = tokio::task::JoinSet::new();
    for endpoint in &config.endpoints {
        let endpoint = endpoint.clone();
        let group = group.clone();
        let max_epochs = config.max_epochs_per_fetch;
        let max_response_bytes = config.max_response_bytes;
        pending.spawn(async move {
            let chain_request = WireRequest::EpochChainRange {
                from_nonce: Nonce::new(from_nonce),
                max_epochs,
            };
            let response = request_endpoint(
                &endpoint,
                &group,
                chain_request,
                expires,
                max_response_bytes,
            )
            .await;
            (endpoint.validator, response)
        });
    }
    while let Some(joined) = pending.join_next().await {
        if let Ok((validator, Ok(WireResponse::EpochChain(chain)))) = joined {
            responses.push((validator, chain));
            let finalized = assemble_finalized_chain(
                group,
                &responses,
                from_nonce,
                config.max_epochs_per_fetch,
            )?;
            if !finalized.epochchain.is_empty()
                || quorum_reports_absent(group, &responses, from_nonce)?
            {
                return Ok(finalized);
            }
        }
    }
    let trusted = consensus_validators_for(group, from_nonce)?;
    let responders = responses
        .iter()
        .filter(|(validator, _)| trusted.contains(validator))
        .count();
    if responders < supermajority_count(trusted.len()) {
        return Err(protocol_error("Blossom quorum read is unavailable"));
    }
    assemble_finalized_chain(group, &responses, from_nonce, config.max_epochs_per_fetch)
}

fn assemble_finalized_chain(
    group: &BlossomGroupConfig,
    responses: &[(PubKey, EpochChain)],
    from_nonce: u64,
    max_epochs: u32,
) -> Result<EpochChain> {
    let mut finalized = Vec::new();
    for nonce in from_nonce..from_nonce.saturating_add(u64::from(max_epochs)) {
        let trusted = consensus_validators_for(group, nonce)?;
        let required = supermajority_count(trusted.len());
        let mut votes: BTreeMap<HashType, (Epoch, BTreeSet<PubKey>)> = BTreeMap::new();
        for (validator, chain) in responses {
            if !trusted.contains(validator) {
                continue;
            }
            if let Some(epoch) = chain
                .epochchain
                .iter()
                .find(|epoch| epoch.body.nonce.value() == nonce)
            {
                votes
                    .entry(epoch.hash)
                    .or_insert_with(|| (epoch.clone(), BTreeSet::new()))
                    .1
                    .insert(*validator);
            }
        }
        let mut approved = votes
            .into_values()
            .filter(|(_, validators)| validators.len() >= required);
        let Some((epoch, _)) = approved.next() else {
            break;
        };
        if approved.next().is_some() {
            return Err(protocol_error("conflicting Blossom quorum-read receipts"));
        }
        finalized.push(epoch);
    }
    Ok(EpochChain {
        epochchain: finalized,
    })
}

fn quorum_reports_absent(
    group: &BlossomGroupConfig,
    responses: &[(PubKey, EpochChain)],
    nonce: u64,
) -> Result<bool> {
    let trusted = consensus_validators_for(group, nonce)?;
    let absent = responses
        .iter()
        .filter(|(validator, chain)| {
            trusted.contains(validator)
                && chain
                    .epochchain
                    .iter()
                    .all(|epoch| epoch.body.nonce.value() != nonce)
        })
        .count();
    Ok(absent >= supermajority_count(trusted.len()))
}

async fn submit_block(
    endpoint: &BlossomValidatorEndpoint,
    group: &BlossomGroupConfig,
    block: blossom::Block,
    expires: Instant,
    max_response_bytes: usize,
) -> Result<()> {
    match request_endpoint(
        endpoint,
        group,
        WireRequest::SubmitBlock(block),
        expires,
        max_response_bytes,
    )
    .await?
    {
        WireResponse::BlockAccepted(_) | WireResponse::Ok => Ok(()),
        WireResponse::Error(error) => Err(protocol_error(format!(
            "Blossom rejected conflict block: {error}"
        ))),
        response => Err(protocol_error(format!(
            "unexpected Blossom SubmitBlock response: {}",
            response.kind()
        ))),
    }
}

async fn request_endpoint(
    endpoint: &BlossomValidatorEndpoint,
    group: &BlossomGroupConfig,
    request: WireRequest,
    expires: Instant,
    max_response_bytes: usize,
) -> Result<WireResponse> {
    let request = if group.blossom_group_id == ConsensusGroupId::root() {
        request
    } else {
        WireRequest::Group {
            group_id: group.blossom_group_id,
            request: Box::new(request),
        }
    };
    let remaining = expires.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(protocol_error("Blossom bridge deadline exceeded"));
    }
    tokio::time::timeout(
        remaining,
        send_bounded_request(endpoint.address, &request, max_response_bytes),
    )
    .await
    .map_err(|_| protocol_error("Blossom bridge request timed out"))?
}

async fn send_bounded_request(
    address: SocketAddr,
    request: &WireRequest,
    max_response_bytes: usize,
) -> Result<WireResponse> {
    let mut stream = TcpStream::connect(address)
        .await
        .map_err(|error| protocol_error(format!("Blossom endpoint connect failed: {error}")))?;
    stream
        .set_nodelay(true)
        .map_err(|error| protocol_error(format!("Blossom endpoint setup failed: {error}")))?;
    write_wire_request(&mut stream, request)
        .await
        .map_err(|error| protocol_error(format!("Blossom request write failed: {error}")))?;

    let mut prefix = [0; 4];
    stream
        .read_exact(&mut prefix)
        .await
        .map_err(|error| protocol_error(format!("Blossom response header failed: {error}")))?;
    let payload_len = u32::from_be_bytes(prefix) as usize;
    if payload_len == 0 || payload_len > max_response_bytes {
        return Err(protocol_error(
            "Blossom response exceeds configured byte limit",
        ));
    }
    let mut payload = vec![0; payload_len];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|error| protocol_error(format!("Blossom response body failed: {error}")))?;
    decode_wire_response_payload(&payload)
        .map_err(|error| protocol_error(format!("invalid Blossom response: {error}")))
}

async fn sleep_bounded(delay: Duration, expires: Instant) -> Result<()> {
    let remaining = expires.saturating_duration_since(Instant::now());
    if remaining.is_zero() || delay >= remaining {
        return Err(protocol_error("Blossom bridge finality deadline exceeded"));
    }
    tokio::time::sleep(delay).await;
    Ok(())
}

fn validate_generations(group: &BlossomGroupConfig) -> Result<()> {
    if group.validator_generations.is_empty()
        || group.validator_generations[0].active_from_nonce > group.checkpoint_nonce
    {
        return Err(config_error(
            "validator generations do not cover the checkpoint",
        ));
    }
    let mut previous = None;
    for generation in &group.validator_generations {
        if generation.validators.is_empty()
            || previous.is_some_and(|nonce| nonce >= generation.active_from_nonce)
        {
            return Err(config_error(
                "validator generations must be nonempty and strictly ordered",
            ));
        }
        previous = Some(generation.active_from_nonce);
    }
    Ok(())
}

fn validators_for(config: &BlossomGroupConfig, nonce: u64) -> Result<&BTreeSet<PubKey>> {
    config
        .validator_generations
        .iter()
        .rev()
        .find(|generation| generation.active_from_nonce <= nonce)
        .map(|generation| &generation.validators)
        .ok_or_else(|| config_error("no trusted Blossom validator generation for nonce"))
}

fn consensus_validators_for(
    config: &BlossomGroupConfig,
    target_nonce: u64,
) -> Result<&BTreeSet<PubKey>> {
    validators_for(config, target_nonce.saturating_sub(1))
}

fn validate_claim(payload: &[u8]) -> Result<()> {
    if payload.len() > MAX_CLAIM_BYTES {
        return Err(protocol_error(
            "Blossom conflict claim exceeds hard byte limit",
        ));
    }
    candidate_ids(payload).map(|_| ())
}

fn candidate_ids(payload: &[u8]) -> Result<[[u8; 32]; 2]> {
    if payload.len() < CLAIM_FIXED_BYTES || &payload[..4] != CLAIM_MAGIC {
        return Err(protocol_error("invalid Blossom conflict claim header"));
    }
    let mut offset = CLAIM_FIXED_BYTES;
    let mut ids = [[0; 32]; 2];
    for id in &mut ids {
        let start = offset;
        let name_len = usize::from(
            *payload
                .get(offset)
                .ok_or_else(|| protocol_error("truncated Blossom conflict candidate"))?,
        );
        offset = offset.saturating_add(1);
        let candidate_len = name_len.saturating_add(16 + 4 + 8 + 1);
        let end = offset.saturating_add(candidate_len);
        if name_len == 0 || end > payload.len() {
            return Err(protocol_error("malformed Blossom conflict candidate"));
        }
        let class = payload[end - 1];
        if !(1..=4).contains(&class) {
            return Err(protocol_error("unknown Blossom conflict mutation class"));
        }
        *id = Sha256::digest(&payload[start..end]).into();
        offset = end;
    }
    if offset != payload.len() {
        return Err(protocol_error("trailing bytes in Blossom conflict claim"));
    }
    Ok(ids)
}

fn claim_digest(payload: &[u8]) -> [u8; 32] {
    Sha256::digest(
        [
            b"shardmap.active-sync.conflict-claim.v1".as_slice(),
            payload,
        ]
        .concat(),
    )
    .into()
}

fn certificate(
    group_id: [u8; 32],
    claim_digest: [u8; 32],
    receipt: &PersistedReceipt,
) -> Result<BlossomConflictCertificate> {
    Ok(BlossomConflictCertificate {
        group_id,
        epoch_nonce: receipt.epoch_nonce,
        epoch_hash: receipt.epoch_hash,
        claim_digest,
        candidate_epochs: receipt.candidate_epochs,
    })
}

fn load_signer(path: &Path) -> Result<SecKey> {
    validate_secret_file(path)?;
    let value = fs::read_to_string(path)
        .map_err(|error| config_error(format!("failed to read Blossom signing key: {error}")))?;
    SecKey::try_from_hex(value.trim())
        .map_err(|error| config_error(format!("invalid Blossom signing key: {error}")))
}

fn validate_secret_file(path: &Path) -> Result<()> {
    let metadata = fs::metadata(path)
        .map_err(|error| config_error(format!("failed to inspect Blossom signing key: {error}")))?;
    if !metadata.is_file() {
        return Err(config_error("Blossom signing key path is not a file"));
    }
    #[cfg(unix)]
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(config_error(
            "Blossom signing key must not be accessible by group or other users",
        ));
    }
    Ok(())
}

fn state_path(directory: &Path, group_id: [u8; 32]) -> PathBuf {
    let mut name = String::with_capacity(64 + 5);
    for byte in group_id {
        use std::fmt::Write as _;
        let _ = write!(name, "{byte:02x}");
    }
    name.push_str(".json");
    directory.join(name)
}

fn load_group_state(
    path: &Path,
    config: &BlossomGroupConfig,
    max_state_bytes: u64,
) -> Result<PersistedGroupState> {
    if !path.exists() {
        return Ok(PersistedGroupState {
            version: STATE_VERSION,
            shardmap_group_id: config.shardmap_group_id,
            blossom_group_id: config.blossom_group_id.hash().0,
            checkpoint_nonce: config.checkpoint_nonce,
            checkpoint_hash: config.checkpoint_hash.0,
            candidate_epochs: BTreeMap::new(),
            receipts: BTreeMap::new(),
        });
    }
    let metadata = fs::metadata(path).map_err(persistence_error)?;
    if metadata.len() > max_state_bytes {
        return Err(ShardCacheError::Persistence(
            "Blossom bridge state exceeds configured byte limit".into(),
        ));
    }
    let bytes = fs::read(path).map_err(persistence_error)?;
    let envelope: PersistedStateEnvelope = serde_json::from_slice(&bytes).map_err(|error| {
        ShardCacheError::Persistence(format!("failed to decode Blossom bridge state: {error}"))
    })?;
    let state_bytes = serde_json::to_vec(&envelope.state).map_err(|error| {
        ShardCacheError::Persistence(format!("failed to verify Blossom bridge state: {error}"))
    })?;
    let actual_checksum: [u8; 32] = Sha256::digest(&state_bytes).into();
    if actual_checksum != envelope.checksum {
        return Err(ShardCacheError::Persistence(
            "Blossom bridge state checksum mismatch".into(),
        ));
    }
    let state = envelope.state;
    if state.version != STATE_VERSION
        || state.shardmap_group_id != config.shardmap_group_id
        || state.blossom_group_id != config.blossom_group_id.hash().0
        || state.checkpoint_nonce < config.checkpoint_nonce
        || (state.checkpoint_nonce == config.checkpoint_nonce
            && state.checkpoint_hash != config.checkpoint_hash.0)
    {
        return Err(ShardCacheError::Persistence(
            "Blossom bridge state does not match its trusted checkpoint".into(),
        ));
    }
    if state.candidate_epochs.iter().any(|(digest, epoch)| {
        !valid_digest_key(digest) || *epoch == 0 || *epoch > state.checkpoint_nonce
    }) || state.receipts.iter().any(|(digest, receipt)| {
        !valid_digest_key(digest)
            || receipt.epoch_nonce == 0
            || receipt.epoch_nonce > state.checkpoint_nonce
            || receipt.epoch_hash == [0; 32]
            || receipt
                .candidate_epochs
                .iter()
                .any(|epoch| *epoch == 0 || *epoch > receipt.epoch_nonce)
    }) {
        return Err(ShardCacheError::Persistence(
            "Blossom bridge state contains invalid rank or receipt bounds".into(),
        ));
    }
    Ok(state)
}

fn digest_key(digest: [u8; 32]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn valid_digest_key(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn persist_group_state(
    path: &Path,
    state: &PersistedGroupState,
    max_state_bytes: u64,
) -> Result<()> {
    let state_bytes = serde_json::to_vec(state).map_err(|error| {
        ShardCacheError::Persistence(format!("failed to encode Blossom bridge state: {error}"))
    })?;
    let envelope = PersistedStateEnvelope {
        checksum: Sha256::digest(&state_bytes).into(),
        state: state.clone(),
    };
    let bytes = serde_json::to_vec(&envelope).map_err(|error| {
        ShardCacheError::Persistence(format!("failed to encode Blossom bridge state: {error}"))
    })?;
    if bytes.len() as u64 > max_state_bytes {
        return Err(ShardCacheError::Backpressure(
            "Blossom bridge state byte limit reached",
        ));
    }
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut options = OpenOptions::new();
    options.create(true).write(true).truncate(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(&temporary).map_err(persistence_error)?;
    file.write_all(&bytes).map_err(persistence_error)?;
    file.sync_all().map_err(persistence_error)?;
    fs::rename(&temporary, path).map_err(persistence_error)?;
    if let Some(parent) = path.parent() {
        let directory = OpenOptions::new()
            .read(true)
            .open(parent)
            .map_err(persistence_error)?;
        directory.sync_all().map_err(persistence_error)?;
    }
    Ok(())
}

fn protocol_error(message: impl Into<String>) -> ShardCacheError {
    ShardCacheError::Protocol(message.into())
}

fn config_error(message: impl Into<String>) -> ShardCacheError {
    ShardCacheError::Config(message.into())
}

fn persistence_error(error: std::io::Error) -> ShardCacheError {
    ShardCacheError::Persistence(format!("Blossom bridge persistence failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_remote_plaintext_endpoints() {
        let keypair = Keypair::generate();
        let config = BlossomTcpBridgeConfig {
            endpoints: vec![BlossomValidatorEndpoint {
                address: "192.0.2.1:9000".parse().unwrap(),
                validator: keypair.public,
            }],
            signer_secret_path: PathBuf::from("unused"),
            state_dir: PathBuf::from("unused"),
            groups: vec![BlossomGroupConfig {
                shardmap_group_id: [1; 32],
                blossom_group_id: ConsensusGroupId::root(),
                checkpoint_nonce: 0,
                checkpoint_hash: HashType::default(),
                validator_generations: vec![BlossomValidatorGeneration {
                    active_from_nonce: 0,
                    validators: BTreeSet::from([keypair.public]),
                }],
            }],
            poll_interval: Duration::from_millis(10),
            max_epochs_per_fetch: 16,
            max_candidates_per_group: 100,
            max_receipts_per_group: 100,
            max_state_bytes_per_group: 1_000_000,
            max_response_bytes: 1024 * 1024,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_malformed_claims_without_allocating_from_lengths() {
        let mut payload = vec![0; CLAIM_FIXED_BYTES];
        payload[..4].copy_from_slice(CLAIM_MAGIC);
        payload.push(u8::MAX);
        assert!(candidate_ids(&payload).is_err());
    }

    #[test]
    fn membership_transition_uses_previous_epoch_for_consensus() {
        let old = (0..4)
            .map(|_| Keypair::generate().public)
            .collect::<BTreeSet<_>>();
        let new = (0..4)
            .map(|_| Keypair::generate().public)
            .collect::<BTreeSet<_>>();
        let group = BlossomGroupConfig {
            shardmap_group_id: [1; 32],
            blossom_group_id: ConsensusGroupId::root(),
            checkpoint_nonce: 0,
            checkpoint_hash: HashType::default(),
            validator_generations: vec![
                BlossomValidatorGeneration {
                    active_from_nonce: 0,
                    validators: old.clone(),
                },
                BlossomValidatorGeneration {
                    active_from_nonce: 5,
                    validators: new.clone(),
                },
            ],
        };

        assert_eq!(validators_for(&group, 5).unwrap(), &new);
        assert_eq!(consensus_validators_for(&group, 5).unwrap(), &old);
        assert_eq!(consensus_validators_for(&group, 6).unwrap(), &new);
    }

    #[test]
    fn failed_persist_does_not_publish_checkpoint_in_memory() {
        let keypair = Keypair::generate();
        let identity =
            blossom::NodeIdentity::new(keypair.public, None, "tcp", "127.0.0.1", 9000, false);
        let genesis = blossom::genesis_epoch([identity]);
        let group_config = BlossomGroupConfig {
            shardmap_group_id: [7; 32],
            blossom_group_id: ConsensusGroupId::root(),
            checkpoint_nonce: 0,
            checkpoint_hash: genesis.hash,
            validator_generations: vec![BlossomValidatorGeneration {
                active_from_nonce: 0,
                validators: BTreeSet::from([keypair.public]),
            }],
        };
        let temporary = tempfile::tempdir().unwrap();
        let state_path = temporary.path().join("state.json");
        let state = load_group_state(&state_path, &group_config, 1_024).unwrap();
        let mut runtime = GroupRuntime {
            config: group_config.clone(),
            state_path,
            state,
        };
        let mut epoch = Epoch {
            body: blossom::EpochBody {
                group_id: ConsensusGroupId::root(),
                verifiers: genesis.body.verifiers,
                last_epoch: genesis.hash,
                nonce: Nonce::new(1),
                ..blossom::EpochBody::default()
            },
            ..Epoch::default()
        };
        epoch.set_hash();
        let config = BlossomTcpBridgeConfig {
            endpoints: vec![BlossomValidatorEndpoint {
                address: "127.0.0.1:9000".parse().unwrap(),
                validator: keypair.public,
            }],
            signer_secret_path: temporary.path().join("unused"),
            state_dir: temporary.path().to_path_buf(),
            groups: vec![group_config],
            poll_interval: Duration::from_millis(1),
            max_epochs_per_fetch: 1,
            max_candidates_per_group: 1,
            max_receipts_per_group: 1,
            max_state_bytes_per_group: 1,
            max_response_bytes: 1024 * 1024,
        };

        assert!(
            apply_epochs(
                &config,
                &mut runtime,
                EpochChain {
                    epochchain: vec![epoch]
                }
            )
            .is_err()
        );
        assert_eq!(runtime.state.checkpoint_nonce, 0);
        assert_eq!(runtime.state.checkpoint_hash, genesis.hash.0);
    }

    #[tokio::test]
    async fn response_limit_is_checked_before_payload_allocation() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(&1_000_000u32.to_be_bytes()).await.unwrap();
        });

        let result = send_bounded_request(address, &WireRequest::Health, 64).await;
        assert!(result.is_err());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn quorum_read_does_not_wait_for_delayed_validator() {
        let keypairs = (0..4).map(|_| Keypair::generate()).collect::<Vec<_>>();
        let validators = keypairs
            .iter()
            .map(|keypair| keypair.public)
            .collect::<BTreeSet<_>>();
        let genesis =
            blossom::genesis_epoch(keypairs.iter().enumerate().map(|(index, keypair)| {
                blossom::NodeIdentity::new(
                    keypair.public,
                    None,
                    "tcp",
                    "127.0.0.1",
                    9100 + index as u16,
                    false,
                )
            }));
        let mut epoch = Epoch {
            body: blossom::EpochBody {
                group_id: ConsensusGroupId::root(),
                verifiers: genesis.body.verifiers.clone(),
                last_epoch: genesis.hash,
                nonce: Nonce::new(1),
                ..blossom::EpochBody::default()
            },
            ..Epoch::default()
        };
        epoch.set_hash();
        let response = WireResponse::EpochChain(EpochChain {
            epochchain: vec![epoch],
        });
        let mut endpoints = Vec::new();
        let mut servers = Vec::new();
        for (index, keypair) in keypairs.iter().enumerate() {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            endpoints.push(BlossomValidatorEndpoint {
                address: listener.local_addr().unwrap(),
                validator: keypair.public,
            });
            let response = response.clone();
            servers.push(tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                if index == 3 {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                blossom::write_wire_response(&mut stream, &response)
                    .await
                    .unwrap();
            }));
        }
        let temporary = tempfile::tempdir().unwrap();
        let group = BlossomGroupConfig {
            shardmap_group_id: [6; 32],
            blossom_group_id: ConsensusGroupId::root(),
            checkpoint_nonce: 0,
            checkpoint_hash: genesis.hash,
            validator_generations: vec![BlossomValidatorGeneration {
                active_from_nonce: 0,
                validators,
            }],
        };
        let config = BlossomTcpBridgeConfig {
            endpoints,
            signer_secret_path: temporary.path().join("unused"),
            state_dir: temporary.path().to_path_buf(),
            groups: vec![group.clone()],
            poll_interval: Duration::from_millis(1),
            max_epochs_per_fetch: 1,
            max_candidates_per_group: 1,
            max_receipts_per_group: 1,
            max_state_bytes_per_group: 1024,
            max_response_bytes: 1024 * 1024,
        };

        let started = Instant::now();
        let chain = request_chain_quorum(
            &config,
            &group,
            1,
            Instant::now() + Duration::from_millis(500),
        )
        .await
        .unwrap();
        assert_eq!(chain.epochchain.len(), 1);
        assert!(started.elapsed() < Duration::from_millis(400));
        for server in servers {
            server.abort();
        }
    }

    #[test]
    fn signer_file_rotation_is_reloaded_atomically() {
        let temporary = tempfile::tempdir().unwrap();
        let signer_path = temporary.path().join("signer.key");
        let replacement_path = temporary.path().join("signer.next");
        let first = Keypair::generate();
        let second = Keypair::generate();
        fs::write(&signer_path, first.secret.to_string()).unwrap();
        fs::write(&replacement_path, second.secret.to_string()).unwrap();
        #[cfg(unix)]
        {
            fs::set_permissions(&signer_path, fs::Permissions::from_mode(0o600)).unwrap();
            fs::set_permissions(&replacement_path, fs::Permissions::from_mode(0o600)).unwrap();
        }

        assert_eq!(load_signer(&signer_path).unwrap(), first.secret);
        fs::rename(&replacement_path, &signer_path).unwrap();
        assert_eq!(load_signer(&signer_path).unwrap(), second.secret);
    }
}
