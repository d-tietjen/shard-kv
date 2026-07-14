use std::fs;
use std::path::{Path, PathBuf};

use crate::Result;
use crate::config::PersistenceConfig;
use crate::storage::{Bytes, FastHashMap, MutationOp, MutationRecord, StoredEntry};

use super::{LoadedSnapshot, RecoveryState, SnapshotRepository, SnapshotStore, wal};

pub(super) struct RecoveryLoader<'a> {
    config: &'a PersistenceConfig,
}

struct RecoveryAccumulator {
    entries: FastHashMap<Bytes, StoredEntry>,
    snapshot_timestamp_ms: u64,
}

enum SnapshotLoad {
    Loaded(LoadedSnapshot),
    Empty,
}

pub(super) struct PersistenceDataDir;

impl<'a> RecoveryLoader<'a> {
    pub(super) fn new(config: &'a PersistenceConfig) -> Self {
        Self { config }
    }

    pub(super) fn load(&self) -> Result<RecoveryState> {
        match self.config.enabled {
            true => self.load_enabled(),
            false => Ok(RecoveryState::empty()),
        }
    }

    fn load_enabled(&self) -> Result<RecoveryState> {
        fs::create_dir_all(&self.config.data_dir)?;
        let mut accumulator = RecoveryAccumulator::default();
        accumulator.load_snapshot(&SnapshotStore::new(&self.config.data_dir))?;
        accumulator.replay_records(wal::SegmentStore::new(&self.config.data_dir).read_all()?);
        Ok(accumulator.into_state())
    }
}

impl RecoveryAccumulator {
    fn default() -> Self {
        Self {
            entries: FastHashMap::<Bytes, StoredEntry>::default(),
            snapshot_timestamp_ms: 0,
        }
    }

    fn load_snapshot(&mut self, snapshots: &impl SnapshotRepository) -> Result<()> {
        match SnapshotLoad::from_loaded(snapshots.load_latest_snapshot()?) {
            SnapshotLoad::Loaded(snapshot) => self.apply_snapshot(snapshot),
            SnapshotLoad::Empty => {}
        }
        Ok(())
    }

    fn apply_snapshot(&mut self, snapshot: LoadedSnapshot) {
        self.snapshot_timestamp_ms = snapshot.timestamp_ms;
        for entry in snapshot.entries {
            self.entries.insert(entry.key.clone(), entry);
        }
    }

    fn replay_records(&mut self, records: Vec<MutationRecord>) {
        for record in records {
            self.apply_record(record);
        }
    }

    fn apply_record(&mut self, record: MutationRecord) {
        match record.timestamp_ms < self.snapshot_timestamp_ms {
            true => {}
            false => self.apply_current_record(record),
        }
    }

    fn apply_current_record(&mut self, record: MutationRecord) {
        match record.op {
            MutationOp::Set => self.apply_write(record),
            MutationOp::Expire => self.apply_expire(record),
            MutationOp::Del => {
                self.entries.remove(record.key.as_ref());
            }
        }
    }

    fn apply_write(&mut self, record: MutationRecord) {
        let key = record.key.to_vec();
        self.entries.insert(
            key.clone(),
            StoredEntry {
                key,
                value: record.value.to_vec(),
                expire_at_ms: record.expire_at_ms,
                governance: record.governance.map(|metadata| metadata.to_vec()),
            },
        );
    }

    fn apply_expire(&mut self, record: MutationRecord) {
        if let Some(entry) = self.entries.get_mut(record.key.as_ref()) {
            entry.expire_at_ms = record.expire_at_ms;
        }
    }

    fn into_state(self) -> RecoveryState {
        RecoveryState {
            entries: self.entries.into_values().collect(),
            snapshot_timestamp_ms: self.snapshot_timestamp_ms,
        }
    }
}

impl SnapshotLoad {
    fn from_loaded(snapshot: Option<LoadedSnapshot>) -> Self {
        match snapshot {
            Some(snapshot) => Self::Loaded(snapshot),
            None => Self::Empty,
        }
    }
}

impl RecoveryState {
    fn empty() -> Self {
        Self {
            entries: Vec::new(),
            snapshot_timestamp_ms: 0,
        }
    }
}

impl PersistenceDataDir {
    pub(super) fn normalize(path: &Path) -> PathBuf {
        path.to_path_buf()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes as SharedBytes;

    use super::*;

    #[test]
    fn wal_recovery_replaces_value_ttl_and_governance_atomically() {
        let mut recovery = RecoveryAccumulator::default();
        recovery.replay_records(vec![MutationRecord {
            shard_id: 0,
            sequence: 1,
            timestamp_ms: 10,
            op: MutationOp::Set,
            key: SharedBytes::from_static(b"private"),
            value: SharedBytes::from_static(b"model-state"),
            expire_at_ms: Some(99),
            governance: Some(SharedBytes::from_static(b"tenant-a/repo-private")),
        }]);

        let state = recovery.into_state();
        assert_eq!(state.entries.len(), 1);
        assert_eq!(state.entries[0].value, b"model-state");
        assert_eq!(state.entries[0].expire_at_ms, Some(99));
        assert_eq!(
            state.entries[0].governance.as_deref(),
            Some(b"tenant-a/repo-private".as_slice())
        );
    }
}
