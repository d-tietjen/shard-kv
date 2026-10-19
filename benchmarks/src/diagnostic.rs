//! Validation and measurement boundaries shared by focused diagnostics.

use std::collections::BTreeSet;
use std::hint::black_box;
use std::mem::size_of;
use std::time::{Duration, Instant};

use crate::backend::BoxError;

/// Rejects invalid topology and overflowing corpus/container sizes before setup.
/// Zero-byte values are useful controls and remain valid.
pub fn validate_geometry(count: usize, value_size: usize, shards: usize) -> Result<(), BoxError> {
    if count == 0 {
        return Err("the corpus must contain at least one key or record".into());
    }
    if shards == 0 || !shards.is_power_of_two() {
        return Err("shards must be a non-zero power of two".into());
    }
    // The two diagnostics retain multiple value copies and encoded forms.
    // Budget four payload-sized buffers plus conservative key/container space.
    let per_record = value_size
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(512))
        .ok_or("corpus size overflow")?;
    for (items, bytes) in [(count, per_record), (shards, size_of::<usize>())] {
        if items
            .checked_mul(bytes)
            .is_none_or(|size| size > isize::MAX as usize)
        {
            return Err("corpus or shard bookkeeping exceeds addressable memory".into());
        }
    }
    Ok(())
}

/// Validates operation and repetition counts without silently replacing zero.
pub fn validate_repetitions(ops: usize, iterations: usize, warmups: usize) -> Result<(), BoxError> {
    if ops == 0 || iterations == 0 {
        return Err("operation count and measured iterations must be positive".into());
    }
    iterations
        .checked_add(warmups)
        .and_then(|runs| runs.checked_mul(ops))
        .ok_or("total operation count overflows usize")?;
    Ok(())
}

/// Rejects empty selections and duplicate row identities.
pub fn ensure_unique<T: Ord>(
    items: impl IntoIterator<Item = T>,
    name: &str,
) -> Result<(), BoxError> {
    let mut seen = BTreeSet::new();
    for item in items {
        if !seen.insert(item) {
            return Err(format!("{name} selection contains duplicates").into());
        }
    }
    if seen.is_empty() {
        return Err(format!("{name} selection is empty").into());
    }
    Ok(())
}

/// One actual measured iteration. The checksum belongs to that iteration.
#[derive(Debug, Clone)]
pub struct Measurement {
    pub duration: Duration,
    pub checksum: u64,
}

/// Times operations, then validates their results outside the timed interval.
/// Every warmup and measured iteration must succeed and produce the same witness.
pub fn measure<T>(
    iterations: usize,
    warmups: usize,
    mut operation: impl FnMut() -> Result<T, BoxError>,
    mut verify: impl FnMut(T) -> Result<u64, BoxError>,
) -> Result<Measurement, BoxError> {
    validate_repetitions(1, iterations, warmups)?;
    let mut expected = None;
    let mut check = |output| {
        let checksum = verify(output)?;
        if expected.is_some_and(|expected| expected != checksum) {
            return Err("diagnostic checksum changed between iterations".into());
        }
        expected = Some(checksum);
        Ok::<_, BoxError>(checksum)
    };
    for _ in 0..warmups {
        black_box(check(black_box(operation()?))?);
    }
    let mut best: Option<Measurement> = None;
    for _ in 0..iterations {
        let started = Instant::now();
        let output = black_box(operation());
        let duration = started.elapsed();
        let checksum = check(output?)?;
        if duration.is_zero() {
            return Err("timer resolution is too coarse; increase the workload".into());
        }
        if best.as_ref().is_none_or(|best| duration < best.duration) {
            best = Some(Measurement { duration, checksum });
        }
    }
    best.ok_or_else(|| "no measured iterations".into())
}

/// Keeps cleanup errors visible even when the measured operation already failed.
pub fn finish<T>(work: Result<T, BoxError>, cleanup: Result<(), BoxError>) -> Result<T, BoxError> {
    match (work, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(work), Ok(())) => Err(work),
        (Ok(_), Err(cleanup)) => Err(cleanup),
        (Err(work), Err(cleanup)) => Err(format!("{work}; cleanup also failed: {cleanup}").into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn rejects_invalid_and_overflowing_layouts_and_counts() {
        for (count, size, shards) in [
            (0, 1, 1),
            (1, 1, 0),
            (1, 1, 3),
            (usize::MAX, 1, 1),
            (1, usize::MAX, 1),
            (1, 1, 1usize << (usize::BITS - 1)),
        ] {
            assert!(validate_geometry(count, size, shards).is_err());
        }
        assert!(validate_geometry(1, 0, 1).is_ok());
        for (ops, runs, warmups) in [(0, 1, 0), (1, 0, 0), (1, usize::MAX, 1), (usize::MAX, 2, 0)] {
            assert!(validate_repetitions(ops, runs, warmups).is_err());
        }
        assert!(ensure_unique(Vec::<usize>::new(), "cases").is_err());
        assert!(ensure_unique([1, 1], "cases").is_err());
    }

    #[test]
    fn measures_real_operations_and_rejects_failed_or_inconsistent_results() {
        let calls = Cell::new(0);
        let result = measure(
            2,
            1,
            || {
                calls.set(calls.get() + 1);
                std::thread::yield_now();
                Ok(7)
            },
            |value| Ok(value),
        )
        .unwrap();
        assert_eq!(calls.get(), 3);
        assert_eq!(result.checksum, 7);
        assert!(!result.duration.is_zero());
        assert!(measure::<()>(1, 0, || Err("operation failed".into()), |_| Ok(0)).is_err());
        assert!(measure(1, 0, || Ok(1), |_| Err("verification failed".into())).is_err());
        let n = Cell::new(0u64);
        assert!(
            measure(
                1,
                1,
                || {
                    n.set(n.get() + 1);
                    Ok(n.get())
                },
                Ok
            )
            .is_err()
        );
        let error = finish::<()>(Err("work".into()), Err("cleanup".into())).unwrap_err();
        assert!(error.to_string().contains("work") && error.to_string().contains("cleanup"));
    }
}
