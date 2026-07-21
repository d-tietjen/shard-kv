use std::fs::{self, OpenOptions};
use std::hint::black_box;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use clap::Parser;
use shardmap::config::{
    EvictionPolicy, ObjectOverflowBackend, ObjectOverflowCompression, ObjectOverflowConfig,
    ObjectOverflowFailurePolicy,
};
use shardmap::storage::{EmbeddedStore, ObjectOverflowRuntime};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Parser)]
#[command(about = "Filesystem object-overflow cost benchmark")]
struct Args {
    #[arg(long, default_value_t = 65_536)]
    value_size: usize,
    #[arg(long, default_value_t = 1_024)]
    keys: usize,
    #[arg(long, default_value_t = 3)]
    repetitions: usize,
    #[arg(long, default_value = "zstd")]
    compression: String,
    #[arg(long, default_value_t = 2)]
    worker_threads: usize,
    #[arg(long, default_value_t = 1_024)]
    queue_capacity: usize,
    #[arg(long, default_value_t = 128)]
    cold_idle_ticks: u64,
    #[arg(long)]
    csv: Option<String>,
    #[arg(long)]
    keep_dir: bool,
}

#[derive(Debug, Clone)]
struct BenchRow {
    case: &'static str,
    value_size: usize,
    keys: usize,
    repetitions: usize,
    duration: Duration,
    ops: u64,
    checksum: u64,
    remote_entries: usize,
    remote_stored_bytes: usize,
}

impl BenchRow {
    fn ops_per_sec(&self) -> f64 {
        self.ops as f64 / self.duration.as_secs_f64()
    }

    fn mb_per_sec(&self) -> f64 {
        (self.ops as f64 * self.value_size as f64) / self.duration.as_secs_f64() / 1_000_000.0
    }
}

fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    let compression = parse_compression(&args.compression)?;
    let root = unique_bench_dir();
    fs::create_dir_all(&root)?;

    println!(
        "object-overflow-fs-cost: root={} value_size={} keys={} repetitions={} compression={} workers={} queue={} cold_idle_ticks={}",
        root.display(),
        args.value_size,
        args.keys,
        args.repetitions,
        args.compression,
        args.worker_threads,
        args.queue_capacity,
        args.cold_idle_ticks
    );
    println!(
        "| {:<22} | {:>10} | {:>8} | {:>12} | {:>12} | {:>12} | {:>10} |",
        "case", "value", "ops", "ops/sec", "MB/sec", "remote-bytes", "checksum"
    );
    println!(
        "| {:-<22} | {:-<10} | {:-<8} | {:-<12} | {:-<12} | {:-<12} | {:-<10} |",
        "", "", "", "", "", "", ""
    );

    let rows = [
        bench_resident_set_get(&args)?,
        bench_overflow_offload(&args, &root.join("offload"), compression)?,
        bench_overflow_fault_in(&args, &root.join("fault-in"), compression)?,
    ];
    for row in &rows {
        print_row(row);
    }
    if let Some(path) = &args.csv {
        write_csv(path, &rows)?;
    }
    if !args.keep_dir {
        remove_bench_dir(&root)?;
    }
    Ok(())
}

fn bench_resident_set_get(args: &Args) -> Result<BenchRow, BoxError> {
    let value = Bytes::from(vec![11u8; args.value_size]);
    let store = EmbeddedStore::new(1);
    store.configure_memory_policy(
        Some(
            args.value_size
                .saturating_mul(args.keys)
                .saturating_mul(4)
                .max(1),
        ),
        EvictionPolicy::Lru,
    );
    let mut checksum = 0u64;
    let start = Instant::now();
    for repetition in 0..args.repetitions {
        for index in 0..args.keys {
            let key = key_for("resident", repetition, index);
            store.set_value_bytes(&key, value.clone(), None);
            let found = store.get_value_bytes(&key).expect("resident get");
            checksum = checksum.wrapping_add(black_box(found.len() as u64));
        }
    }
    Ok(BenchRow {
        case: "resident-set-get",
        value_size: args.value_size,
        keys: args.keys,
        repetitions: args.repetitions,
        duration: start.elapsed(),
        ops: (args.keys * args.repetitions * 2) as u64,
        checksum,
        remote_entries: 0,
        remote_stored_bytes: 0,
    })
}

fn bench_overflow_offload(
    args: &Args,
    root: &Path,
    compression: ObjectOverflowCompression,
) -> Result<BenchRow, BoxError> {
    let value = Bytes::from(vec![17u8; args.value_size]);
    let mut checksum = 0u64;
    let stores = (0..args.repetitions)
        .map(|repetition| {
            overflow_store(
                args,
                root,
                "offload-node",
                repetition,
                compression,
                resident_memory_limit(args),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    for (repetition, store) in stores.iter().enumerate() {
        for index in 0..args.keys {
            let key = key_for("offload", repetition, index);
            store.set_value_bytes(&key, value.clone(), None);
            checksum = checksum.wrapping_add(black_box(key.len() as u64));
        }
        age_overflow_candidates(store, args, "offload-age", repetition);
    }
    let start = Instant::now();
    let mut last_remote_entries = 0usize;
    let mut last_remote_stored_bytes = 0usize;
    for store in &stores {
        store.configure_memory_policy(Some(1), EvictionPolicy::Lru);
        wait_for_remote_entries(store, args.keys)?;
        let stats = object_overflow_stats(store);
        last_remote_entries = stats.0;
        last_remote_stored_bytes = stats.1;
    }
    Ok(BenchRow {
        case: "overflow-offload",
        value_size: args.value_size,
        keys: args.keys,
        repetitions: args.repetitions,
        duration: start.elapsed(),
        ops: (args.keys * args.repetitions) as u64,
        checksum,
        remote_entries: last_remote_entries,
        remote_stored_bytes: last_remote_stored_bytes,
    })
}

fn bench_overflow_fault_in(
    args: &Args,
    root: &Path,
    compression: ObjectOverflowCompression,
) -> Result<BenchRow, BoxError> {
    let value = Bytes::from(vec![23u8; args.value_size]);
    let mut duration = Duration::ZERO;
    let mut checksum = 0u64;
    let mut last_remote_entries = 0usize;
    let mut last_remote_stored_bytes = 0usize;
    for repetition in 0..args.repetitions {
        let store = overflow_store(
            args,
            root,
            "fault-node",
            repetition,
            compression,
            resident_memory_limit(args),
        )?;
        let keys = (0..args.keys)
            .map(|index| key_for("fault", repetition, index))
            .collect::<Vec<_>>();
        for key in &keys {
            store.set_value_bytes(key, value.clone(), None);
        }
        age_overflow_candidates(&store, args, "fault-age", repetition);
        store.configure_memory_policy(Some(1), EvictionPolicy::Lru);
        wait_for_remote_entries(&store, args.keys)?;
        let stats = object_overflow_stats(&store);
        last_remote_entries = stats.0;
        last_remote_stored_bytes = stats.1;
        let start = Instant::now();
        for key in &keys {
            let found = store.get(key).expect("fault-in get");
            checksum = checksum.wrapping_add(black_box(found.len() as u64));
        }
        duration += start.elapsed();
    }
    Ok(BenchRow {
        case: "overflow-fault-in",
        value_size: args.value_size,
        keys: args.keys,
        repetitions: args.repetitions,
        duration,
        ops: (args.keys * args.repetitions) as u64,
        checksum,
        remote_entries: last_remote_entries,
        remote_stored_bytes: last_remote_stored_bytes,
    })
}

fn overflow_store(
    args: &Args,
    root: &Path,
    node: &str,
    repetition: usize,
    compression: ObjectOverflowCompression,
    memory_limit: usize,
) -> Result<EmbeddedStore, BoxError> {
    let config = ObjectOverflowConfig {
        enabled: true,
        backend: ObjectOverflowBackend::File,
        endpoint: root.display().to_string(),
        bucket: format!("bucket-{repetition}"),
        prefix: "overflow".to_string(),
        node_id: Some(node.to_string()),
        min_value_bytes: args.value_size,
        offload_min_idle_ticks: args.cold_idle_ticks,
        compression,
        zstd_level: 1,
        failure_policy: ObjectOverflowFailurePolicy::RetainResident,
        max_retries: 0,
        retry_backoff_ms: 1,
        operation_timeout_ms: 10_000,
        worker_threads: args.worker_threads,
        queue_capacity: args.queue_capacity,
        cleanup_on_start: false,
        cleanup_interval_seconds: 0,
        cleanup_grace_seconds: 60,
        ..ObjectOverflowConfig::default()
    };
    let runtime = ObjectOverflowRuntime::from_config(&config)?.expect("enabled runtime");
    let store = EmbeddedStore::new(1);
    store.configure_memory_policy(Some(memory_limit), EvictionPolicy::Lru);
    store
        .configure_object_overflow(Some(runtime))
        .expect("configure object overflow");
    Ok(store)
}

fn resident_memory_limit(args: &Args) -> usize {
    args.value_size
        .saturating_mul(args.keys)
        .saturating_mul(2)
        .saturating_add((args.cold_idle_ticks as usize).saturating_add(1))
        .max(1)
}

fn age_overflow_candidates(store: &EmbeddedStore, args: &Args, prefix: &str, repetition: usize) {
    for tick in 0..=args.cold_idle_ticks {
        let key = format!("{prefix}:{repetition}:{tick:016x}").into_bytes();
        store.set_value_bytes(&key, Bytes::from_static(b"x"), None);
    }
}

fn object_overflow_stats(store: &EmbeddedStore) -> (usize, usize) {
    let stats = store.shard_stats_snapshot();
    let remote_entries = stats
        .iter()
        .map(|shard| shard.object_overflow.remote_entries)
        .sum();
    let remote_stored_bytes = stats
        .iter()
        .map(|shard| shard.object_overflow.remote_stored_bytes)
        .sum();
    (remote_entries, remote_stored_bytes)
}

fn wait_for_remote_entries(store: &EmbeddedStore, expected: usize) -> Result<(), BoxError> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        store.process_maintenance();
        let (remote_entries, _) = object_overflow_stats(store);
        if remote_entries >= expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for object overflow: expected {expected}, observed {remote_entries}"
            )
            .into());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn remove_bench_dir(root: &Path) -> Result<(), BoxError> {
    for attempt in 0..50 {
        match fs::remove_dir_all(root) {
            Ok(()) => return Ok(()),
            Err(error) if attempt < 49 => {
                let _ = error;
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error.into()),
        }
    }
    unreachable!("cleanup retry loop always returns")
}

fn parse_compression(value: &str) -> Result<ObjectOverflowCompression, BoxError> {
    match value {
        "none" => Ok(ObjectOverflowCompression::None),
        "lz4" => Ok(ObjectOverflowCompression::Lz4),
        "zstd" => Ok(ObjectOverflowCompression::Zstd),
        other => Err(format!("unknown compression `{other}`").into()),
    }
}

fn key_for(prefix: &str, repetition: usize, index: usize) -> Vec<u8> {
    format!("{prefix}:{repetition}:{index:08}").into_bytes()
}

fn unique_bench_dir() -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    std::env::temp_dir().join(format!(
        "shardcache-object-overflow-fs-cost-{}-{now}",
        std::process::id()
    ))
}

fn print_row(row: &BenchRow) {
    println!(
        "| {:<22} | {:>10} | {:>8} | {:>12.0} | {:>12.1} | {:>12} | {:>10} |",
        row.case,
        row.value_size,
        row.ops,
        row.ops_per_sec(),
        row.mb_per_sec(),
        row.remote_stored_bytes,
        row.checksum
    );
}

fn write_csv(path: &str, rows: &[BenchRow]) -> Result<(), BoxError> {
    let existed = Path::new(path).exists();
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    if !existed {
        writeln!(
            file,
            "case,value_size,keys,repetitions,duration_s,ops,ops_per_sec,mb_per_sec,remote_entries,remote_stored_bytes,checksum"
        )?;
    }
    for row in rows {
        writeln!(
            file,
            "{},{},{},{},{:.6},{},{:.3},{:.3},{},{},{}",
            row.case,
            row.value_size,
            row.keys,
            row.repetitions,
            row.duration.as_secs_f64(),
            row.ops,
            row.ops_per_sec(),
            row.mb_per_sec(),
            row.remote_entries,
            row.remote_stored_bytes,
            row.checksum
        )?;
    }
    Ok(())
}
