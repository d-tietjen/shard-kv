use shardmap::{ActiveShardMap, ActiveSyncConfig, NodeId, SyncOptions};

fn main() -> shardmap::Result<()> {
    let left = ActiveShardMap::new(
        4,
        ActiveSyncConfig::new("example-cluster", NodeId::new("left")?),
    )?;
    let right = ActiveShardMap::new(
        4,
        ActiveSyncConfig::new("example-cluster", NodeId::new("right")?),
    )?;

    left.set("session:42", "ready")?;
    right.set("session:7", "running")?;

    let report = left.sync_with(&right, SyncOptions::default())?;
    assert_eq!(left.get("session:7"), Some(b"running".to_vec()));
    assert_eq!(right.get("session:42"), Some(b"ready".to_vec()));
    assert!(!report.truncated);

    Ok(())
}
