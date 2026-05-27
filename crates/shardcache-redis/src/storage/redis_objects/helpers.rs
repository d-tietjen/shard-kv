use super::*;

#[inline(always)]
pub(super) fn replace_bytes(existing: &mut Bytes, value: &[u8]) {
    if existing.len() == value.len() {
        existing.as_mut_slice().copy_from_slice(value);
    } else {
        existing.clear();
        existing.extend_from_slice(value);
    }
}

#[inline(always)]
pub(super) fn sort_small_zset(entries: &mut SmallZSetEntries) {
    entries.sort_by(|(left_member, left_score), (right_member, right_score)| {
        left_score
            .total_cmp(right_score)
            .then_with(|| left_member.cmp(right_member))
    });
}

pub(super) fn normalize_index(index: i64, len: usize) -> Option<usize> {
    if len == 0 {
        return None;
    }
    let len_i = len as i64;
    let index = if index < 0 { len_i + index } else { index };
    (0..len_i).contains(&index).then_some(index as usize)
}

pub(super) fn normalize_range(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let len_i = len as i64;
    let mut start = if start < 0 { len_i + start } else { start };
    let mut stop = if stop < 0 { len_i + stop } else { stop };
    if start < 0 {
        start = 0;
    }
    if stop < 0 {
        return None;
    }
    if start >= len_i {
        return None;
    }
    if stop >= len_i {
        stop = len_i - 1;
    }
    (start <= stop).then_some((start as usize, stop as usize))
}

pub(super) fn format_score(score: f64) -> Bytes {
    if score.fract() == 0.0 && score.is_finite() {
        (score as i64).to_string().into_bytes()
    } else {
        score.to_string().into_bytes()
    }
}

pub(super) fn parse_i64_bytes(value: &[u8]) -> Result<i64, ()> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|text| text.parse::<i64>().ok())
        .ok_or(())
}

pub(super) fn parse_f64_bytes(value: &[u8]) -> Result<f64, ()> {
    let value = std::str::from_utf8(value)
        .ok()
        .and_then(|text| text.parse::<f64>().ok())
        .ok_or(())?;
    value.is_finite().then_some(value).ok_or(())
}

#[inline(always)]
pub(super) fn bucket_index(key_hash: u64) -> usize {
    (key_hash & 0xff) as usize
}
