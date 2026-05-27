#[cfg(creusot)]
use creusot_std::macros::{ensures, requires};

/// Redis-style inclusive range after applying negative indexes and clamping.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(not(creusot), derive(PartialEq, Eq))]
pub(crate) struct NormalizedRedisRange {
    pub(crate) start: usize,
    pub(crate) stop: usize,
}

impl NormalizedRedisRange {
    #[inline(always)]
    pub(crate) fn into_bounds(self) -> (usize, usize) {
        (self.start, self.stop)
    }
}

/// Normalize Redis range indexes such as `GETRANGE key -4 -1` or
/// `ZRANGE key 0 -1` into an inclusive Rust slice range.
#[inline(always)]
#[cfg_attr(creusot, requires(len@ <= i64::MAX@))]
#[cfg_attr(
    creusot,
    ensures(match result {
        Some(range) => range.start@ <= range.stop@ && range.stop@ < len@,
        None => true,
    })
)]
#[cfg_attr(creusot, ensures(len@ == 0 ==> result == None))]
pub(crate) fn normalize_redis_range(
    len: usize,
    start: i64,
    stop: i64,
) -> Option<NormalizedRedisRange> {
    if len == 0 {
        return None;
    }

    let len_i64 = len as i64;
    let start = if start < 0 { len_i64 + start } else { start }.clamp(0, len_i64);
    let stop = if stop < 0 { len_i64 + stop } else { stop }.clamp(0, len_i64 - 1);
    if start <= stop {
        Some(NormalizedRedisRange {
            start: start as usize,
            stop: stop as usize,
        })
    } else {
        None
    }
}

#[cfg(any(test, creusot))]
#[cfg_attr(creusot, requires(len@ <= i64::MAX@))]
#[cfg_attr(creusot, ensures(result))]
pub(crate) fn normalized_range_is_in_bounds(len: usize, start: i64, stop: i64) -> bool {
    match normalize_redis_range(len, start, stop) {
        Some(range) => range.start <= range.stop && range.stop < len,
        None => true,
    }
}

#[cfg(any(test, creusot))]
#[cfg_attr(creusot, ensures(result))]
pub(crate) fn empty_range_normalizes_to_none(start: i64, stop: i64) -> bool {
    normalize_redis_range(0, start, stop).is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalized_ranges_stay_in_bounds_for_representative_inputs() {
        for len in 0..32 {
            for start in -40..40 {
                for stop in -40..40 {
                    assert!(normalized_range_is_in_bounds(len, start, stop));
                    if len == 0 {
                        assert!(empty_range_normalizes_to_none(start, stop));
                    }
                }
            }
        }
    }

    #[test]
    fn range_normalization_matches_redis_edge_cases() {
        assert_eq!(normalize_redis_range(0, 0, -1), None);
        assert_eq!(
            normalize_redis_range(4, 0, -1),
            Some(NormalizedRedisRange { start: 0, stop: 3 })
        );
        assert_eq!(
            normalize_redis_range(4, -2, -1),
            Some(NormalizedRedisRange { start: 2, stop: 3 })
        );
        assert_eq!(normalize_redis_range(4, 4, 10), None);
        assert_eq!(normalize_redis_range(4, 3, 1), None);
        assert_eq!(
            normalize_redis_range(4, -20, 2),
            Some(NormalizedRedisRange { start: 0, stop: 2 })
        );
    }
}
