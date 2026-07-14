use std::collections::VecDeque;

use smallvec::SmallVec;

use crate::{Result, ShardCacheError};

use super::ReplicationFrameBytes;
use super::protocol::{ReplicationFrame, ReplicationMutation, ShardWatermarks, decode_frame};

#[derive(Debug, Clone)]
pub enum BacklogCatchUp {
    Available(Vec<ReplicationFrameBytes>),
    NeedsSnapshot,
}

#[derive(Debug)]
pub struct ReplicationBacklog {
    max_bytes: usize,
    current_bytes: usize,
    shard_count: usize,
    entries: VecDeque<BacklogEntry>,
    /// High watermark of mutations ever pushed, even if since trimmed. Used to
    /// short-circuit catch-up when a caller is already ahead of everything we
    /// ever held.
    high_watermarks: ShardWatermarks,
    trimmed: bool,
}

#[derive(Debug, Clone)]
struct BacklogEntry {
    frame: ReplicationFrameBytes,
    spans: SmallVec<[BacklogShardSpan; 1]>,
    byte_len: usize,
}

#[derive(Debug, Clone, Copy)]
struct BacklogShardSpan {
    shard_id: usize,
    min_sequence: u64,
    max_sequence: u64,
}

impl ReplicationBacklog {
    pub fn new(max_bytes: usize, shard_count: usize) -> Self {
        let shard_count = shard_count.max(1);
        Self {
            max_bytes: max_bytes.max(1),
            current_bytes: 0,
            shard_count,
            entries: VecDeque::new(),
            high_watermarks: ShardWatermarks::new(shard_count),
            trimmed: false,
        }
    }

    pub fn push(&mut self, frame: ReplicationFrameBytes, mutations: &[ReplicationMutation]) {
        self.push_encoded(frame, mutations);
    }

    pub fn push_encoded(
        &mut self,
        frame: ReplicationFrameBytes,
        mutations: &[ReplicationMutation],
    ) {
        let mut spans = SmallVec::<[BacklogShardSpan; 1]>::new();
        for mutation in mutations {
            self.ensure_shard_capacity(mutation.shard_id);
            self.high_watermarks
                .observe(mutation.shard_id, mutation.sequence);
            match spans
                .iter_mut()
                .find(|span| span.shard_id == mutation.shard_id)
            {
                Some(span) => {
                    span.min_sequence = span.min_sequence.min(mutation.sequence);
                    span.max_sequence = span.max_sequence.max(mutation.sequence);
                }
                None => spans.push(BacklogShardSpan {
                    shard_id: mutation.shard_id,
                    min_sequence: mutation.sequence,
                    max_sequence: mutation.sequence,
                }),
            }
        }

        let byte_len = frame.len();
        self.current_bytes = self.current_bytes.saturating_add(byte_len);
        self.entries.push_back(BacklogEntry {
            frame,
            spans,
            byte_len,
        });
        self.trim();
    }

    pub(crate) fn push_encoded_span(
        &mut self,
        frame: ReplicationFrameBytes,
        shard_id: usize,
        min_sequence: u64,
        max_sequence: u64,
    ) {
        self.ensure_shard_capacity(shard_id);
        self.high_watermarks.observe(shard_id, max_sequence);

        let byte_len = frame.len();
        self.current_bytes = self.current_bytes.saturating_add(byte_len);
        self.entries.push_back(BacklogEntry {
            frame,
            spans: SmallVec::from_buf([BacklogShardSpan {
                shard_id,
                min_sequence,
                max_sequence,
            }]),
            byte_len,
        });
        self.trim();
    }

    fn ensure_shard_capacity(&mut self, shard_id: usize) {
        if shard_id >= self.shard_count {
            self.shard_count = shard_id + 1;
        }
    }

    pub fn catch_up_since(&self, watermarks: &ShardWatermarks) -> Result<BacklogCatchUp> {
        if self.caller_is_caught_up(watermarks) {
            return Ok(BacklogCatchUp::Available(Vec::new()));
        }
        if self.entries.is_empty() {
            return Ok(if self.trimmed {
                BacklogCatchUp::NeedsSnapshot
            } else {
                BacklogCatchUp::Available(Vec::new())
            });
        }
        let earliest_retained = self.earliest_retained_sequences();
        for (shard_id, high) in self.high_watermarks.as_slice().iter().enumerate() {
            if *high <= watermarks.get(shard_id) {
                continue;
            }
            let earliest = earliest_retained.get(shard_id).copied().unwrap_or(0);
            if earliest == 0 || watermarks.get(shard_id).saturating_add(1) < earliest {
                return Ok(BacklogCatchUp::NeedsSnapshot);
            }
        }

        let mut frames = Vec::new();
        for entry in &self.entries {
            let needed = entry
                .spans
                .iter()
                .any(|span| span.max_sequence > watermarks.get(span.shard_id));
            if !needed {
                continue;
            }
            // Validate the frame can be decoded; a corrupt backlog entry is a
            // hard error so the caller can fall back to a snapshot.
            decode_frame(entry.frame.as_ref()).map_err(|error| {
                ShardCacheError::Protocol(format!("backlog frame is corrupt: {error}"))
            })?;
            frames.push(entry.frame.clone());
        }
        Ok(BacklogCatchUp::Available(frames))
    }

    pub fn decode_frames(frames: &[ReplicationFrameBytes]) -> Result<Vec<ReplicationFrame>> {
        frames
            .iter()
            .map(|frame| decode_frame(frame.as_ref()))
            .collect()
    }

    pub fn latest_watermarks(&self) -> ShardWatermarks {
        self.high_watermarks.clone()
    }

    fn caller_is_caught_up(&self, watermarks: &ShardWatermarks) -> bool {
        let highs = self.high_watermarks.as_slice();
        for (shard_id, high) in highs.iter().enumerate() {
            if *high > watermarks.get(shard_id) {
                return false;
            }
        }
        true
    }

    pub fn retained_bytes(&self) -> usize {
        self.current_bytes
    }

    fn earliest_retained_sequences(&self) -> Vec<u64> {
        let mut earliest = vec![0_u64; self.shard_count];
        for entry in &self.entries {
            for span in &entry.spans {
                if earliest.get(span.shard_id).copied().unwrap_or(0) == 0 {
                    if earliest.len() <= span.shard_id {
                        earliest.resize(span.shard_id + 1, 0);
                    }
                    earliest[span.shard_id] = span.min_sequence;
                }
            }
        }
        earliest
    }

    fn trim(&mut self) {
        while self.current_bytes > self.max_bytes {
            let Some(entry) = self.entries.pop_front() else {
                break;
            };
            self.trimmed = true;
            self.current_bytes = self.current_bytes.saturating_sub(entry.byte_len);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::replication::protocol::{
        FrameKind, ReplicationCompressionMode, ReplicationMutation, ReplicationMutationOp,
        encode_frame, encode_mutation_batch,
    };
    use bytes::Bytes as SharedBytes;

    use crate::storage::{hash_key, hash_key_tag};

    use super::*;

    fn mutation(shard_id: usize, sequence: u64) -> ReplicationMutation {
        let key = format!("key-{shard_id}-{sequence}").into_bytes();
        ReplicationMutation {
            shard_id,
            sequence,
            timestamp_ms: 1,
            op: ReplicationMutationOp::Set,
            key_hash: hash_key(&key),
            key_tag: hash_key_tag(&key),
            key: SharedBytes::from(key),
            value: SharedBytes::from_static(b"v"),
            expire_at_ms: None,
            governance: None,
        }
    }

    fn frame_for(mutations: &[ReplicationMutation]) -> ReplicationFrameBytes {
        let payload = encode_mutation_batch(mutations);
        ReplicationFrameBytes::from(
            encode_frame(
                FrameKind::MutationBatch,
                ReplicationCompressionMode::None,
                0,
                &payload,
            )
            .expect("frame"),
        )
    }

    #[test]
    fn catches_up_when_watermark_is_retained() {
        let mut backlog = ReplicationBacklog::new(1024 * 1024, 1);
        for seq in 1..=3 {
            let mutations = vec![mutation(0, seq)];
            backlog.push(frame_for(&mutations), &mutations);
        }
        match backlog
            .catch_up_since(&ShardWatermarks::from_vec(vec![1]))
            .expect("catch_up_since")
        {
            BacklogCatchUp::Available(frames) => assert_eq!(frames.len(), 2),
            BacklogCatchUp::NeedsSnapshot => panic!("expected backlog catch-up"),
        }
    }

    #[test]
    fn empty_backlog_after_trimming_requests_snapshot() {
        let mut backlog = ReplicationBacklog::new(1, 1);
        let mutations = vec![mutation(0, 1)];
        backlog.push(frame_for(&mutations), &mutations);
        let mutations = vec![mutation(0, 2)];
        backlog.push(frame_for(&mutations), &mutations);
        match backlog
            .catch_up_since(&ShardWatermarks::from_vec(vec![0]))
            .expect("catch_up_since")
        {
            BacklogCatchUp::NeedsSnapshot => {}
            BacklogCatchUp::Available(_) => panic!("expected NeedsSnapshot after trimming"),
        }
    }

    #[test]
    fn multi_shard_catch_up_returns_relevant_frames() {
        let mut backlog = ReplicationBacklog::new(1024 * 1024, 2);
        let m1 = vec![mutation(0, 1)];
        backlog.push(frame_for(&m1), &m1);
        let m2 = vec![mutation(1, 1)];
        backlog.push(frame_for(&m2), &m2);
        let m3 = vec![mutation(0, 2)];
        backlog.push(frame_for(&m3), &m3);

        // Replica is caught up on shard 0 through seq 1, but knows nothing
        // about shard 1.
        match backlog
            .catch_up_since(&ShardWatermarks::from_vec(vec![1, 0]))
            .expect("catch_up_since")
        {
            BacklogCatchUp::Available(frames) => assert_eq!(frames.len(), 2),
            BacklogCatchUp::NeedsSnapshot => panic!("expected backlog catch-up"),
        }
    }

    #[test]
    fn catch_up_resends_partially_needed_batch() {
        let mut backlog = ReplicationBacklog::new(1024 * 1024, 1);
        let mutations = vec![mutation(0, 1), mutation(0, 2)];
        backlog.push(frame_for(&mutations), &mutations);

        match backlog
            .catch_up_since(&ShardWatermarks::from_vec(vec![1]))
            .expect("catch_up_since")
        {
            BacklogCatchUp::Available(frames) => assert_eq!(frames.len(), 1),
            BacklogCatchUp::NeedsSnapshot => panic!("expected backlog catch-up"),
        }
    }
}
