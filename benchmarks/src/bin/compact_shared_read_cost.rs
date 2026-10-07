//! Bounded, phase-separated owned-read diagnostic. It never reads private layout counters.
use std::hint::black_box;
use std::io::{self, Read, Write};
use std::time::{Duration, Instant};

use clap::Parser;
use hdrhistogram::Histogram;
use serde::Serialize;
use sha2::{Digest, Sha256};
use shardmap::storage::{FlatMap, hash_key};

type Error = Box<dyn std::error::Error + Send + Sync>;
const PHASES: &[&str] = &[
    "empty",
    "borrowed_loaded",
    "cold_first",
    "cold_next",
    "warm_same",
    "all_owned",
    "quiescent_deleted",
    "class_reused",
    "epoch_retired",
    "epoch_reclaimed_owners_held",
    "epoch_owners_released",
    "reowned",
    "maps_dropped",
];

#[derive(Parser)]
struct Args {
    #[arg(long)]
    cohort: String,
    #[arg(long, default_value_t = 4)]
    batches: usize,
    #[arg(long, default_value_t = 5)]
    warm_seconds: u64,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct Cohort {
    maps: usize,
    records_per_map: usize,
    key_bytes: usize,
    value_bytes: usize,
    owned_step: usize,
    cold_readers: usize,
}

impl Cohort {
    fn select(name: &str) -> Result<Self, Error> {
        let shape = match name {
            "empty-value-sparse" => (64, 1, 2, 0, 1, 1),
            "small-value-sparse" => (64, 1, 18, 16, 1, 1),
            "small-value-dense" => (64, 120, 18, 16, 120, 1),
            "point-100k-sparse-owned" => (1, 100_000, 18, 16, 120, 1),
            "eight-reader-first-owned" => (64, 1, 18, 256, 1, 8),
            _ => return Err("unknown finite cohort".into()),
        };
        Ok(Self {
            maps: shape.0,
            records_per_map: shape.1,
            key_bytes: shape.2,
            value_bytes: shape.3,
            owned_step: shape.4,
            cold_readers: shape.5,
        })
    }

    fn keys(self) -> Vec<Vec<u8>> {
        (0..self.records_per_map)
            .map(|i| {
                if self.key_bytes == 2 {
                    (i as u16).to_be_bytes().to_vec()
                } else {
                    format!("k:{i:016x}").into_bytes()
                }
            })
            .collect()
    }

    fn selected(self) -> Vec<usize> {
        (0..self.records_per_map).step_by(self.owned_step).collect()
    }
}

#[derive(Default)]
struct ReadLedger(Vec<bool>);
impl ReadLedger {
    fn cold(&mut self, index: usize) -> Result<(), Error> {
        if *self.0.get(index).ok_or("owned index outside cohort")? {
            return Err("cold read repeated after materialization".into());
        }
        self.0[index] = true;
        Ok(())
    }
}

#[derive(Serialize)]
struct Timing {
    operations: u64,
    elapsed_ns: u64,
    p50_ns: Option<u64>,
    p99_ns: Option<u64>,
    p999_ns: Option<u64>,
    // A bounded prefix of raw cold/drop observations complements the histogram.
    first_samples_ns: Vec<u64>,
}

struct Timer {
    histogram: Histogram<u64>,
    samples: Vec<u64>,
    elapsed_ns: u64,
}
impl Timer {
    fn new() -> Result<Self, Error> {
        Ok(Self {
            histogram: Histogram::new(3)?,
            samples: Vec::with_capacity(256),
            elapsed_ns: 0,
        })
    }
    fn record(&mut self, elapsed: Duration) -> Result<(), Error> {
        let ns = u64::try_from(elapsed.as_nanos())?.max(1);
        self.histogram.record(ns)?;
        if self.samples.len() < 256 {
            self.samples.push(ns);
        }
        self.elapsed_ns = self.elapsed_ns.checked_add(ns).ok_or("timer overflow")?;
        Ok(())
    }
    fn result(self, wall: Option<Duration>) -> Result<Timing, Error> {
        let count = self.histogram.len();
        Ok(Timing {
            operations: count,
            elapsed_ns: match wall {
                Some(d) => u64::try_from(d.as_nanos())?,
                None => self.elapsed_ns,
            },
            p50_ns: (count > 0).then(|| self.histogram.value_at_quantile(0.5)),
            p99_ns: (count > 0).then(|| self.histogram.value_at_quantile(0.99)),
            p999_ns: (count > 0).then(|| self.histogram.value_at_quantile(0.999)),
            first_samples_ns: self.samples,
        })
    }
}

fn check_maps(maps: &[FlatMap], keys: &[Vec<u8>], value: &[u8]) -> Result<(), Error> {
    for map in maps {
        if map.len() != keys.len()
            || map.stored_bytes() != keys.iter().map(|k| k.len() + value.len()).sum::<usize>()
        {
            return Err("logical cardinality/value bytes differ".into());
        }
        for key in keys {
            if map.get_ref_hashed_shared_no_ttl(hash_key(key), key) != Some(value) {
                return Err("borrowed value mismatch".into());
            }
        }
    }
    Ok(())
}

fn dataset_hash(maps: usize, keys: &[Vec<u8>], value: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"compact-shared-read-cost-v1\0");
    for map_index in 0..maps {
        for key in keys {
            digest.update((map_index as u64).to_le_bytes());
            digest.update((key.len() as u64).to_le_bytes());
            digest.update(key);
            digest.update((value.len() as u64).to_le_bytes());
            digest.update(value);
        }
    }
    format!("{:x}", digest.finalize())
}

// Keep the fixed checkpoint protocol fields explicit at every phase boundary.
#[allow(clippy::too_many_arguments)]
fn checkpoint(
    batch: usize,
    phase: &str,
    shape: Cohort,
    maps: &[FlatMap],
    keys: &[Vec<u8>],
    value: &[u8],
    timing: Option<Timing>,
    last_delete: Option<Timing>,
    owned_count: usize,
) -> Result<(), Error> {
    let rows = maps.iter().map(FlatMap::len).sum::<usize>();
    let stored = maps.iter().map(FlatMap::stored_bytes).sum::<usize>();
    let message = serde_json::json!({
        "schema_version": 1, "batch": batch, "phase": phase, "pid": std::process::id(),
        "status": "awaiting_snapshot_ack", "cohort": shape, "live_maps": maps.len(),
        "logical_records": rows, "logical_value_bytes": rows * value.len(),
        "logical_key_value_bytes": stored,
        "dataset_sha256": dataset_hash(maps.len(), if rows == 0 { &[] } else { keys }, value),
        "timing": timing, "last_live_delete_timing": last_delete,
        "recorded_initial_owned_calls": owned_count,
        "scope": "public FlatMap API; physical chunk/directory counts are not observed",
    });
    let stdout = io::stdout();
    let mut out = stdout.lock();
    serde_json::to_writer(&mut out, &message)?;
    writeln!(out)?;
    out.flush()?;
    let mut ack = [0; 9];
    io::stdin().lock().read_exact(&mut ack)?;
    if &ack != b"continue\n" {
        return Err("invalid snapshot acknowledgment".into());
    }
    Ok(())
}

fn owned(map: &FlatMap, key: &[u8], value: &[u8]) -> Result<usize, Error> {
    let owner = map
        .get_shared_value_bytes_hashed_no_ttl(hash_key(key), key)
        .ok_or("owned key missing")?;
    if owner.as_ref() != value {
        return Err("owned value mismatch".into());
    }
    Ok(owner as *const _ as usize)
}

fn contended_owned(
    map: &FlatMap,
    key: &[u8],
    value: &[u8],
    readers: usize,
) -> Result<Vec<(usize, Duration)>, Error> {
    if readers != 8 {
        return Err("requires exactly eight contended readers".into());
    }
    // Thread construction and Barrier waiting are outside every getter timer.
    let barrier = std::sync::Barrier::new(readers);
    let samples = std::thread::scope(|scope| -> Result<Vec<(usize, Duration)>, Error> {
        let mut handles = Vec::new();
        for _ in 0..readers {
            let barrier = &barrier;
            handles.push(scope.spawn(move || -> Result<(usize, Duration), Error> {
                barrier.wait();
                let at = Instant::now();
                let owner = map
                    .get_shared_value_bytes_hashed_no_ttl(hash_key(key), key)
                    .ok_or("contended key missing")?;
                if owner.as_ref() != value {
                    return Err("contended value mismatch".into());
                }
                let elapsed = at.elapsed();
                let address = owner as *const _ as usize;
                // Clone/content validation is outside the getter timer, with no retained clone at the snapshot.
                let clone = owner.clone();
                if clone.as_ref() != value {
                    return Err("contended clone content mismatch".into());
                }
                Ok((address, elapsed))
            }));
        }
        let mut samples = Vec::new();
        for handle in handles {
            samples.push(handle.join().map_err(|_| "contended reader panicked")??);
        }
        Ok(samples)
    })?;
    let winner = samples.first().ok_or("no contended readers")?.0;
    if samples.iter().any(|(address, _)| *address != winner) {
        return Err("first readers returned different owner identities".into());
    }
    Ok(samples)
}

fn batch(args: &Args, shape: Cohort, number: usize, keys: &[Vec<u8>]) -> Result<(), Error> {
    let initial = vec![0x42; shape.value_bytes];
    let reused = vec![0x63; 64];
    let replaced = vec![0x27; 17];
    let selected = shape.selected();
    let mut maps = (0..shape.maps).map(|_| FlatMap::new()).collect::<Vec<_>>();
    let mut ledger = ReadLedger(vec![false; shape.maps * shape.records_per_map]);
    let mut calls = 0;
    checkpoint(
        number, "empty", shape, &maps, keys, &initial, None, None, calls,
    )?;
    for map in &mut maps {
        for key in keys {
            map.set_slice_hashed_no_ttl(hash_key(key), key, &initial);
        }
    }
    check_maps(&maps, keys, &initial)?;
    // Borrowed verification above cannot materialize an owner. No warmup precedes cold_first.
    checkpoint(
        number,
        "borrowed_loaded",
        shape,
        &maps,
        keys,
        &initial,
        None,
        None,
        calls,
    )?;
    let mut cold = Timer::new()?;
    let mut addresses = Vec::with_capacity(shape.maps * selected.len());
    for (m, map) in maps.iter().enumerate() {
        for &i in &selected {
            ledger.cold(m * shape.records_per_map + i)?;
            let address = if shape.cold_readers == 1 {
                let at = Instant::now();
                let address = owned(map, &keys[i], &initial)?;
                cold.record(at.elapsed())?;
                address
            } else {
                let samples = contended_owned(map, &keys[i], &initial, shape.cold_readers)?;
                let winner = samples[0].0;
                for (_, elapsed) in samples {
                    cold.record(elapsed)?;
                }
                winner
            };
            addresses.push(address);
            calls += shape.cold_readers;
        }
    }
    checkpoint(
        number,
        "cold_first",
        shape,
        &maps,
        keys,
        &initial,
        Some(cold.result(None)?),
        None,
        calls,
    )?;
    let mut next = Timer::new()?;
    if shape.records_per_map > 1 {
        for (m, map) in maps.iter().enumerate() {
            for &i in &selected {
                if i + 1 < shape.records_per_map {
                    ledger.cold(m * shape.records_per_map + i + 1)?;
                    let at = Instant::now();
                    black_box(owned(map, &keys[i + 1], &initial)?);
                    next.record(at.elapsed())?;
                    calls += 1;
                }
            }
        }
    }
    // No claim that i+1 occupies the same physical chunk in every candidate layout.
    checkpoint(
        number,
        "cold_next",
        shape,
        &maps,
        keys,
        &initial,
        Some(next.result(None)?),
        None,
        calls,
    )?;
    let mut warm = Timer::new()?;
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(args.warm_seconds) {
        let mut position = 0;
        for map in &maps {
            for &i in &selected {
                let at = Instant::now();
                let address = black_box(owned(map, &keys[i], &initial)?);
                warm.record(at.elapsed())?;
                if address != addresses[position] {
                    return Err("warm owner address changed".into());
                }
                position += 1;
            }
        }
    }
    checkpoint(
        number,
        "warm_same",
        shape,
        &maps,
        keys,
        &initial,
        Some(warm.result(Some(started.elapsed()))?),
        None,
        calls,
    )?;
    for map in &maps {
        for key in keys {
            black_box(owned(map, key, &initial)?);
            calls += 1;
        }
    }
    checkpoint(
        number,
        "all_owned",
        shape,
        &maps,
        keys,
        &initial,
        None,
        None,
        calls,
    )?;
    let mut deletes = Timer::new()?;
    let mut last = Timer::new()?;
    for map in &mut maps {
        for (i, key) in keys.iter().enumerate() {
            let at = Instant::now();
            if !map.delete_hashed(hash_key(key), key, 0) {
                return Err("delete key missing".into());
            }
            let elapsed = at.elapsed();
            deletes.record(elapsed)?;
            if i + 1 == keys.len() {
                last.record(elapsed)?;
            }
        }
        if !map.is_empty() || map.stored_bytes() != 0 {
            return Err("delete left live records".into());
        }
        for key in keys {
            if map
                .get_ref_hashed_shared_no_ttl(hash_key(key), key)
                .is_some()
            {
                return Err("deleted key resurrected".into());
            }
        }
    }
    checkpoint(
        number,
        "quiescent_deleted",
        shape,
        &maps,
        keys,
        &initial,
        Some(deletes.result(None)?),
        Some(last.result(None)?),
        calls,
    )?;
    for map in &mut maps {
        for key in keys {
            map.set_slice_hashed_no_ttl(hash_key(key), key, &reused);
        }
    }
    check_maps(&maps, keys, &reused)?;
    checkpoint(
        number,
        "class_reused",
        shape,
        &maps,
        keys,
        &reused,
        None,
        None,
        calls,
    )?;
    let mut held = Vec::new();
    for (m, map) in maps.iter().enumerate() {
        map.begin_read_epoch();
        for &i in &selected {
            let owner = map
                .get_shared_value_bytes_hashed_no_ttl(hash_key(&keys[i]), &keys[i])
                .ok_or("epoch old key missing")?;
            held.push((m, i, owner.clone()));
        }
    }
    for map in &mut maps {
        for key in keys {
            map.set_slice_hashed_no_ttl(hash_key(key), key, &replaced);
        }
    }
    check_maps(&maps, keys, &replaced)?;
    for (_, _, old) in &held {
        if old.as_ref() != reused.as_slice() {
            return Err("epoch old clone changed".into());
        }
    }
    checkpoint(
        number,
        "epoch_retired",
        shape,
        &maps,
        keys,
        &replaced,
        None,
        None,
        calls,
    )?;
    for map in &mut maps {
        map.end_read_epoch();
        // A finite conservative schedule, not an assertion about private counters.
        for _ in 0..shape.records_per_map.div_ceil(32) + 2 {
            map.process_maintenance(0);
        }
    }
    for (_, _, old) in &held {
        if old.as_ref() != reused.as_slice() {
            return Err("reclaim invalidated retained clone".into());
        }
    }
    check_maps(&maps, keys, &replaced)?;
    checkpoint(
        number,
        "epoch_reclaimed_owners_held",
        shape,
        &maps,
        keys,
        &replaced,
        None,
        None,
        calls,
    )?;
    drop(held);
    checkpoint(
        number,
        "epoch_owners_released",
        shape,
        &maps,
        keys,
        &replaced,
        None,
        None,
        calls,
    )?;
    for map in &maps {
        for key in keys {
            black_box(owned(map, key, &replaced)?);
        }
    }
    checkpoint(
        number, "reowned", shape, &maps, keys, &replaced, None, None, calls,
    )?;
    let mut drops = Timer::new()?;
    while let Some(map) = maps.pop() {
        let at = Instant::now();
        drop(map);
        drops.record(at.elapsed())?;
    }
    checkpoint(
        number,
        "maps_dropped",
        shape,
        &maps,
        keys,
        &replaced,
        Some(drops.result(None)?),
        None,
        calls,
    )?;
    Ok(())
}

fn main() -> Result<(), Error> {
    let args = Args::parse();
    if args.batches != 4 || args.warm_seconds != 5 {
        return Err("requires four batches and five-second warm phase".into());
    }
    let shape = Cohort::select(&args.cohort)?;
    let keys = shape.keys();
    for number in 1..=args.batches {
        batch(&args, shape, number, &keys)?;
    }
    serde_json::to_writer(
        io::stdout().lock(),
        &serde_json::json!({"schema_version":1,"status":"complete","cohort":args.cohort,"batches":args.batches,"phases_per_batch":PHASES.len()}),
    )?;
    println!();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn finite_cohort_rejects_unknown_and_preserves_small_key_cardinality() {
        assert!(Cohort::select("unknown").is_err());
        let c = Cohort::select("empty-value-sparse").unwrap();
        assert_eq!(c.key_bytes, 2);
        assert_eq!(c.records_per_map, 1);
        assert_eq!(c.maps, 64);
    }
    #[test]
    fn contended_cohort_declares_eight_readers_and_maximum_value() {
        let c = Cohort::select("eight-reader-first-owned").unwrap();
        assert_eq!(c.cold_readers, 8);
        assert_eq!(c.value_bytes, 256);
        assert_eq!(c.maps, 64);
    }
    #[test]
    fn practical_keys_are_unique_and_have_exact_length() {
        let c = Cohort::select("point-100k-sparse-owned").unwrap();
        let keys = c.keys();
        let unique = keys.iter().collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique.len(), 100_000);
        assert!(keys.iter().all(|k| k.len() == 18));
        assert_eq!(c.selected().len(), 834);
    }
    #[test]
    fn cold_ledger_rejects_duplicate_and_out_of_bounds() {
        let mut l = ReadLedger(vec![false; 2]);
        assert!(l.cold(0).is_ok());
        assert!(l.cold(0).is_err());
        assert!(l.cold(2).is_err());
        assert!(l.cold(1).is_ok());
    }
    #[test]
    fn dataset_hash_binds_map_identity_value_and_empty_state() {
        let keys = vec![b"ab".to_vec()];
        assert_ne!(dataset_hash(1, &keys, b"x"), dataset_hash(2, &keys, b"x"));
        assert_ne!(dataset_hash(1, &keys, b"x"), dataset_hash(1, &keys, b"y"));
        assert_ne!(dataset_hash(1, &keys, b"x"), dataset_hash(1, &[], b"x"));
    }
    #[test]
    fn histogram_retains_cold_tail_and_marks_no_next_slot_as_absent() {
        let mut t = Timer::new().unwrap();
        t.record(Duration::from_nanos(3)).unwrap();
        t.record(Duration::from_nanos(9000)).unwrap();
        let r = t.result(None).unwrap();
        assert_eq!(r.operations, 2);
        assert!(r.p99_ns.unwrap() >= 9000);
        assert_eq!(Timer::new().unwrap().result(None).unwrap().p99_ns, None);
    }
    #[test]
    fn eight_first_readers_return_one_stable_owner_and_clone_content() {
        let mut map = FlatMap::new();
        let key = b"cold-key";
        let value = [0x42; 256];
        map.set_slice_hashed_no_ttl(hash_key(key), key, &value);
        let samples = contended_owned(&map, key, &value, 8).unwrap();
        assert_eq!(samples.len(), 8);
        assert!(samples.iter().all(|s| s.0 == samples[0].0));
        let clone = map
            .get_shared_value_bytes_hashed_no_ttl(hash_key(key), key)
            .unwrap()
            .clone();
        assert_eq!(clone.as_ref(), value.as_slice());
        assert_eq!(owned(&map, key, &value).unwrap(), samples[0].0);
        assert!(contended_owned(&map, key, &value, 1).is_err());
    }
    #[test]
    fn borrowed_control_then_owned_clone_survives_epoch_replace_and_reclaim() {
        let mut map = FlatMap::new();
        let key = b"key";
        let h = hash_key(key);
        map.set_slice_hashed_no_ttl(h, key, b"old");
        check_maps(&[map], &[key.to_vec()], b"old").unwrap();
        let mut map = FlatMap::new();
        map.set_slice_hashed_no_ttl(h, key, b"old");
        map.begin_read_epoch();
        let old = map
            .get_shared_value_bytes_hashed_no_ttl(h, key)
            .unwrap()
            .clone();
        map.set_slice_hashed_no_ttl(h, key, b"new-value");
        map.end_read_epoch();
        map.process_maintenance(0);
        assert_eq!(old.as_ref(), b"old");
        assert_eq!(
            map.get_ref_hashed_shared_no_ttl(h, key),
            Some(b"new-value".as_slice())
        );
        assert!(map.delete_hashed(h, key, 0));
        assert_eq!(map.len(), 0);
        assert_eq!(map.stored_bytes(), 0);
    }
}
