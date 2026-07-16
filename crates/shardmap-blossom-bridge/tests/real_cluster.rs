use std::collections::BTreeSet;
use std::fs;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use blossom::{
    ConsensusDriverConfig, ConsensusGroupId, NodeIdentity, SimulatedCluster, genesis_epoch,
};
use shardmap::BlossomConflictConsensus;
use shardmap_blossom_bridge::{
    BlossomGroupConfig, BlossomTcpBridgeConfig, BlossomTcpConflictBridge, BlossomValidatorEndpoint,
    BlossomValidatorGeneration,
};

fn claim_payload(first: (&str, u64), second: (&str, u64)) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(b"SCC1");
    payload.extend_from_slice(&[3; 32]);
    payload.extend_from_slice(&0u32.to_le_bytes());
    payload.extend_from_slice(&[4; 32]);
    for (node, sequence) in [first, second] {
        payload.push(node.len() as u8);
        payload.extend_from_slice(node.as_bytes());
        payload.extend_from_slice(&u128::from(sequence).to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.extend_from_slice(&sequence.to_le_bytes());
        payload.push(1);
    }
    payload
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_finality_receipt_survives_bridge_restart() {
    let cluster = SimulatedCluster::spawn_autonomous_with_config(
        6,
        ConsensusDriverConfig {
            interval: Duration::from_millis(500),
            ..ConsensusDriverConfig::default()
        },
    )
    .await
    .unwrap();
    let genesis = genesis_epoch(cluster.nodes().iter().map(|node| {
        NodeIdentity::new(
            node.identity.public_key(),
            None,
            node.identity.protocol.clone(),
            node.identity.host.clone(),
            node.identity.port,
            node.identity.shuffle,
        )
    }));
    let validators = cluster
        .nodes()
        .iter()
        .map(|node| node.identity.public_key())
        .collect::<BTreeSet<_>>();
    let logical_group = [9; 32];
    let temporary = tempfile::tempdir().unwrap();
    let signer_path = temporary.path().join("blossom-signer.key");
    fs::write(&signer_path, cluster.node(0).keypair.secret.to_string()).unwrap();
    #[cfg(unix)]
    fs::set_permissions(&signer_path, fs::Permissions::from_mode(0o600)).unwrap();
    let config = BlossomTcpBridgeConfig {
        endpoints: cluster
            .nodes()
            .iter()
            .map(|node| BlossomValidatorEndpoint {
                address: node.addr().parse::<SocketAddr>().unwrap(),
                validator: node.identity.public_key(),
            })
            .collect(),
        signer_secret_path: signer_path,
        state_dir: temporary.path().join("state"),
        groups: vec![BlossomGroupConfig {
            shardmap_group_id: logical_group,
            blossom_group_id: ConsensusGroupId::root(),
            checkpoint_nonce: 0,
            checkpoint_hash: genesis.hash,
            validator_generations: vec![BlossomValidatorGeneration {
                active_from_nonce: 0,
                validators,
            }],
        }],
        poll_interval: Duration::from_millis(20),
        max_epochs_per_fetch: 256,
        max_candidates_per_group: 1_024,
        max_receipts_per_group: 1_024,
        max_state_bytes_per_group: 4 * 1024 * 1024,
        max_response_bytes: 4 * 1024 * 1024,
    };
    let claims = vec![
        claim_payload(("node-a", 1), ("node-b", 1)),
        claim_payload(("node-c", 2), ("node-d", 2)),
    ];

    let first_config = config.clone();
    let first_claims = claims.clone();
    let first = tokio::task::spawn_blocking(move || {
        let bridge = BlossomTcpConflictBridge::open(first_config).unwrap();
        bridge
            .commit_conflicts(logical_group, &first_claims, Duration::from_secs(10))
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(first.len(), 2);
    assert!(first[0].epoch_nonce > 0);
    assert_eq!(first[0].epoch_nonce, first[1].epoch_nonce);
    assert_eq!(first[0].candidate_epochs[0], first[0].epoch_nonce);
    assert_eq!(first[0].candidate_epochs[1], first[0].epoch_nonce);

    drop(cluster);
    let restart_config = config.clone();
    let restarted = tokio::task::spawn_blocking(move || {
        let bridge = BlossomTcpConflictBridge::open(restart_config).unwrap();
        bridge
            .commit_conflicts(logical_group, &claims, Duration::from_millis(100))
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(restarted, first);

    let state_path = fs::read_dir(&config.state_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut document: serde_json::Value =
        serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
    let checksum_byte = document["checksum"][0].as_u64().unwrap();
    document["checksum"][0] = serde_json::json!((checksum_byte + 1) % 256);
    fs::write(&state_path, serde_json::to_vec(&document).unwrap()).unwrap();
    assert!(BlossomTcpConflictBridge::open(config).is_err());
}
