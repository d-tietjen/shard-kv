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
pub(crate) struct RedisKeyScanResult {
    pub(crate) cursor: u64,
    pub(crate) keys: Vec<Bytes>,
}

impl EmbeddedStore {
    pub(crate) fn scan_redis_keys(
        &self,
        cursor: u64,
        count: usize,
        kind: RedisKeyScanType<'_>,
    ) -> RedisKeyScanResult {
        let limit = count.max(1);
        let now_ms = now_millis();
        let mut keys = Vec::with_capacity(limit.min(1024));
        let mut cursor = self.initial_global_scan_cursor(cursor, kind);

        loop {
            while cursor.shard_id < self.shards.len() {
                let stopped = match cursor.phase {
                    KeyScanPhase::String => self.scan_string_shard(
                        cursor.shard_id,
                        cursor.offset,
                        limit,
                        now_ms,
                        &mut keys,
                    ),
                    KeyScanPhase::Object => self.scan_object_shard(
                        cursor.shard_id,
                        cursor.offset,
                        limit,
                        now_ms,
                        kind,
                        &mut keys,
                    ),
                };

                if let Some(offset) = stopped {
                    return RedisKeyScanResult {
                        cursor: encode_global_cursor(cursor.phase, cursor.shard_id, offset),
                        keys,
                    };
                }

                cursor.shard_id += 1;
                cursor.offset = 0;
                if keys.len() >= limit {
                    return RedisKeyScanResult {
                        cursor: self.next_global_cursor(kind, cursor.phase, cursor.shard_id),
                        keys,
                    };
                }
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

            return RedisKeyScanResult { cursor: 0, keys };
        }
    }

    pub(crate) fn scan_redis_keys_in_shard(
        &self,
        shard_id: usize,
        cursor: u64,
        count: usize,
        kind: RedisKeyScanType<'_>,
    ) -> Option<RedisKeyScanResult> {
        if shard_id >= self.shards.len() {
            return None;
        }

        let limit = count.max(1);
        let now_ms = now_millis();
        let mut keys = Vec::with_capacity(limit.min(1024));
        let mut phase = initial_local_scan_phase(cursor, kind);
        let mut offset = decode_local_cursor(cursor).offset;

        loop {
            let stopped = match phase {
                KeyScanPhase::String => {
                    self.scan_string_shard(shard_id, offset, limit, now_ms, &mut keys)
                }
                KeyScanPhase::Object => {
                    self.scan_object_shard(shard_id, offset, limit, now_ms, kind, &mut keys)
                }
            };

            if let Some(offset) = stopped {
                return Some(RedisKeyScanResult {
                    cursor: encode_local_cursor(phase, offset),
                    keys,
                });
            }

            if keys.len() >= limit {
                let cursor = match phase {
                    KeyScanPhase::String if matches!(kind, RedisKeyScanType::All) => {
                        encode_local_cursor(KeyScanPhase::Object, 0)
                    }
                    _ => 0,
                };
                return Some(RedisKeyScanResult { cursor, keys });
            }

            if phase == KeyScanPhase::String
                && matches!(kind, RedisKeyScanType::All)
                && self.objects.shard_object_count_hint(shard_id) > 0
            {
                phase = KeyScanPhase::Object;
                offset = 0;
                continue;
            }

            return Some(RedisKeyScanResult { cursor: 0, keys });
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

    fn next_global_cursor(
        &self,
        kind: RedisKeyScanType<'_>,
        phase: KeyScanPhase,
        shard_id: usize,
    ) -> u64 {
        if shard_id < self.shards.len() {
            return encode_global_cursor(phase, shard_id, 0);
        }

        match (phase, kind) {
            (KeyScanPhase::String, RedisKeyScanType::All) if self.objects.has_objects() => {
                encode_global_cursor(KeyScanPhase::Object, 0, 0)
            }
            _ => 0,
        }
    }

    fn scan_string_shard(
        &self,
        shard_id: usize,
        offset: usize,
        limit: usize,
        now_ms: u64,
        out: &mut Vec<Bytes>,
    ) -> Option<usize> {
        if self.string_key_count_hint(shard_id) == 0 {
            return None;
        }

        let shard = self.shards[shard_id].read();
        shard.map.scan_keys_into(offset, limit, now_ms, out)
    }

    fn scan_object_shard(
        &self,
        shard_id: usize,
        offset: usize,
        limit: usize,
        now_ms: u64,
        kind: RedisKeyScanType<'_>,
        out: &mut Vec<Bytes>,
    ) -> Option<usize> {
        if self.objects.shard_object_count_hint(shard_id) == 0 {
            return None;
        }

        let keys = match kind {
            RedisKeyScanType::All => self.objects.keys_in_shard(shard_id, now_ms),
            RedisKeyScanType::String => Vec::new(),
            RedisKeyScanType::Object(kind) => self
                .objects
                .keys_with_type_in_shard(shard_id, now_ms)
                .into_iter()
                .filter(|(_, redis_type)| kind.eq_ignore_ascii_case(redis_type.as_bytes()))
                .map(|(key, _)| key)
                .collect(),
        };
        let start = offset.min(keys.len());
        let mut next_offset = start;
        for key in keys.into_iter().skip(start) {
            if out.len() >= limit {
                return Some(next_offset);
            }
            out.push(key);
            next_offset += 1;
        }
        None
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
