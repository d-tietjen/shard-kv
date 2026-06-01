use super::*;

pub(crate) const DEFAULT_SCAN_COUNT: usize = 10;

const CURSOR_OBJECT_PHASE: u64 = 1 << 63;
const CURSOR_SHARD_SHIFT: u64 = 32;
const CURSOR_OFFSET_MASK: u64 = u32::MAX as u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyScanPhase {
    String,
    Object,
}

#[derive(Debug, Clone, Copy)]
struct KeyScanCursor {
    phase: KeyScanPhase,
    shard_id: usize,
    offset: usize,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum RedisKeyScanType<'a> {
    All,
    String,
    Object(&'a [u8]),
}

#[derive(Debug)]
pub(crate) struct RedisKeyScanVisitResult {
    pub(crate) cursor: u64,
    pub(crate) keys: usize,
}

impl EmbeddedStore {
    pub(crate) fn visit_redis_keys(&self, visitor: &mut impl FnMut(&[u8]) -> bool) {
        let now_ms = now_millis();
        for shard_id in 0..self.shards.len() {
            if self.string_key_count_hint(shard_id) == 0 {
                continue;
            }
            let shard = self.shards[shard_id].read();
            if !shard.map.visit_keys(now_ms, visitor) {
                return;
            }
        }

        let _ = self.objects.visit_keys(now_ms, visitor);
    }

    pub(crate) fn scan_redis_keys_visit(
        &self,
        cursor: u64,
        count: usize,
        kind: RedisKeyScanType<'_>,
        visit: &mut impl FnMut(&[u8]) -> bool,
    ) -> RedisKeyScanVisitResult {
        let limit = count.max(1);
        let now_ms = now_millis();
        let mut visited = 0usize;
        let mut keys = 0usize;
        let mut cursor = self.initial_global_scan_cursor(cursor, kind);

        loop {
            while cursor.shard_id < self.shards.len() {
                let stopped = match cursor.phase {
                    KeyScanPhase::String => self.scan_string_shard(
                        cursor.shard_id,
                        cursor.offset,
                        limit,
                        now_ms,
                        &mut visited,
                        &mut keys,
                        visit,
                    ),
                    KeyScanPhase::Object => self.scan_object_shard(
                        cursor.shard_id,
                        cursor.offset,
                        limit,
                        now_ms,
                        kind,
                        &mut visited,
                        &mut keys,
                        visit,
                    ),
                };

                if let Some(offset) = stopped {
                    return RedisKeyScanVisitResult {
                        cursor: encode_global_cursor(cursor.phase, cursor.shard_id, offset),
                        keys,
                    };
                }

                cursor.shard_id += 1;
                cursor.offset = 0;
            }

            if cursor.phase == KeyScanPhase::String
                && matches!(kind, RedisKeyScanType::All)
                && self.objects.has_objects()
            {
                cursor = KeyScanCursor {
                    phase: KeyScanPhase::Object,
                    shard_id: 0,
                    offset: 0,
                };
                continue;
            }

            return RedisKeyScanVisitResult { cursor: 0, keys };
        }
    }

    pub(crate) fn scan_redis_keys_in_shard_visit(
        &self,
        shard_id: usize,
        cursor: u64,
        count: usize,
        kind: RedisKeyScanType<'_>,
        visit: &mut impl FnMut(&[u8]) -> bool,
    ) -> Option<RedisKeyScanVisitResult> {
        if shard_id >= self.shards.len() {
            return None;
        }

        let limit = count.max(1);
        let now_ms = now_millis();
        let mut visited = 0usize;
        let mut keys = 0usize;
        let mut phase = initial_local_scan_phase(cursor, kind);
        let mut offset = decode_local_cursor(cursor).offset;

        loop {
            let stopped = match phase {
                KeyScanPhase::String => self.scan_string_shard(
                    shard_id,
                    offset,
                    limit,
                    now_ms,
                    &mut visited,
                    &mut keys,
                    visit,
                ),
                KeyScanPhase::Object => self.scan_object_shard(
                    shard_id,
                    offset,
                    limit,
                    now_ms,
                    kind,
                    &mut visited,
                    &mut keys,
                    visit,
                ),
            };

            if let Some(offset) = stopped {
                return Some(RedisKeyScanVisitResult {
                    cursor: encode_local_cursor(phase, offset),
                    keys,
                });
            }

            if phase == KeyScanPhase::String
                && matches!(kind, RedisKeyScanType::All)
                && self.objects.shard_object_count_hint(shard_id) > 0
            {
                phase = KeyScanPhase::Object;
                offset = 0;
                continue;
            }

            return Some(RedisKeyScanVisitResult { cursor: 0, keys });
        }
    }

    fn initial_global_scan_cursor(&self, cursor: u64, kind: RedisKeyScanType<'_>) -> KeyScanCursor {
        if cursor == 0 {
            return KeyScanCursor {
                phase: initial_scan_phase(kind),
                shard_id: 0,
                offset: 0,
            };
        }

        let decoded = decode_global_cursor(cursor);
        match kind {
            RedisKeyScanType::Object(_) if decoded.phase == KeyScanPhase::String => KeyScanCursor {
                phase: KeyScanPhase::Object,
                shard_id: decoded.shard_id,
                offset: decoded.offset,
            },
            RedisKeyScanType::String if decoded.phase == KeyScanPhase::Object => KeyScanCursor {
                phase: KeyScanPhase::String,
                shard_id: self.shards.len(),
                offset: 0,
            },
            _ => decoded,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_string_shard(
        &self,
        shard_id: usize,
        offset: usize,
        limit: usize,
        now_ms: u64,
        visited: &mut usize,
        emitted: &mut usize,
        visit: &mut impl FnMut(&[u8]) -> bool,
    ) -> Option<usize> {
        if self.string_key_count_hint(shard_id) == 0 {
            return None;
        }

        let shard = self.shards[shard_id].read();
        shard
            .map
            .scan_keys_visit(offset, limit, now_ms, visited, emitted, visit)
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_object_shard(
        &self,
        shard_id: usize,
        offset: usize,
        limit: usize,
        now_ms: u64,
        kind: RedisKeyScanType<'_>,
        visited: &mut usize,
        emitted: &mut usize,
        visit: &mut impl FnMut(&[u8]) -> bool,
    ) -> Option<usize> {
        if self.objects.shard_object_count_hint(shard_id) == 0 {
            return None;
        }

        let type_filter = match kind {
            RedisKeyScanType::All => None,
            RedisKeyScanType::String => return None,
            RedisKeyScanType::Object(kind) => Some(kind),
        };
        self.objects.scan_keys_in_shard_visit(
            shard_id,
            now_ms,
            type_filter,
            offset,
            limit,
            visited,
            emitted,
            visit,
        )
    }
}

fn initial_scan_phase(kind: RedisKeyScanType<'_>) -> KeyScanPhase {
    match kind {
        RedisKeyScanType::All | RedisKeyScanType::String => KeyScanPhase::String,
        RedisKeyScanType::Object(_) => KeyScanPhase::Object,
    }
}

fn initial_local_scan_phase(cursor: u64, kind: RedisKeyScanType<'_>) -> KeyScanPhase {
    if cursor == 0 {
        return initial_scan_phase(kind);
    }
    let decoded = decode_local_cursor(cursor);
    match kind {
        RedisKeyScanType::Object(_) if decoded.phase == KeyScanPhase::String => {
            KeyScanPhase::Object
        }
        RedisKeyScanType::String if decoded.phase == KeyScanPhase::Object => KeyScanPhase::String,
        _ => decoded.phase,
    }
}

fn decode_global_cursor(cursor: u64) -> KeyScanCursor {
    KeyScanCursor {
        phase: decode_phase(cursor),
        shard_id: ((cursor & !CURSOR_OBJECT_PHASE) >> CURSOR_SHARD_SHIFT) as usize,
        offset: (cursor & CURSOR_OFFSET_MASK) as usize,
    }
}

fn decode_local_cursor(cursor: u64) -> KeyScanCursor {
    KeyScanCursor {
        phase: decode_phase(cursor),
        shard_id: 0,
        offset: (cursor & CURSOR_OFFSET_MASK) as usize,
    }
}

fn decode_phase(cursor: u64) -> KeyScanPhase {
    if cursor & CURSOR_OBJECT_PHASE == 0 {
        KeyScanPhase::String
    } else {
        KeyScanPhase::Object
    }
}

fn encode_global_cursor(phase: KeyScanPhase, shard_id: usize, offset: usize) -> u64 {
    let phase_bits = match phase {
        KeyScanPhase::String => 0,
        KeyScanPhase::Object => CURSOR_OBJECT_PHASE,
    };
    phase_bits | ((shard_id as u64) << CURSOR_SHARD_SHIFT) | (offset as u64 & CURSOR_OFFSET_MASK)
}

fn encode_local_cursor(phase: KeyScanPhase, offset: usize) -> u64 {
    let phase_bits = match phase {
        KeyScanPhase::String => 0,
        KeyScanPhase::Object => CURSOR_OBJECT_PHASE,
    };
    phase_bits | (offset as u64 & CURSOR_OFFSET_MASK)
}
