//! Focused decode and shard-population benchmark.
//!
//! This intentionally isolates the paths where binary layout parsing might
//! matter: WAL frame replay, FCRP mutation batch decode/visit, and restoring
//! decoded entries into a fresh embedded store.

use std::hint::black_box;
use std::time::{Duration, Instant};

use bytes::Bytes as SharedBytes;
use clap::Parser;
use shardcache_client_rs::ShardCacheDirectRouter;
use shardmap::config::ShardCacheConfig;
use shardmap::persistence::{decode_wal_records, encode_wal_record_frame};
use shardmap::protocol::{FastCommand, FastRequest, FastResponse};
use shardmap::replication::{
    BorrowedReplicationMutation, ReplicationMutation, decode_mutation_batch, encode_mutation_batch,
    visit_mutation_batch_payload,
};
use shardmap::storage::{
    EmbeddedStore, EngineHandle, MutationOp, MutationRecord, StoredEntry, hash_key,
};
use tokio::runtime::Builder;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser, Debug)]
#[command(about = "Measure encoded replay decode and shard population cost")]
struct Args {
    #[arg(long, default_value_t = 50_000)]
    records: usize,

    #[arg(long, default_value_t = 512)]
    value_size: usize,

    #[arg(long, default_value_t = 16)]
    shards: usize,

    #[arg(long, default_value_t = 5)]
    iterations: usize,

    #[arg(long, default_value_t = 1)]
    warmup_iterations: usize,
}

fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    let corpus = Corpus::build(args.records, args.value_size, args.shards);
    let router = ShardCacheDirectRouter::new(("127.0.0.1", 6500), args.shards)?;
    let client_routes = corpus
        .records
        .iter()
        .map(|record| router.route_key(record.key.as_ref()))
        .collect::<Vec<_>>();
    let wal_segment = corpus.wal_segment(false);
    let mutation_payload = encode_mutation_batch(&corpus.replication);
    let runtime = Builder::new_current_thread().enable_all().build()?;
    let engine = open_request_path_engine(args.shards)?;
    populate_engine(&runtime, &engine, &corpus);

    println!(
        "records={} value_size={} shards={} wal_bytes={} fcrp_payload_bytes={}",
        args.records,
        args.value_size,
        args.shards,
        wal_segment.len(),
        mutation_payload.len()
    );
    println!("| path | ns/record | records/s | best ms | checksum |");
    println!("| --- | ---: | ---: | ---: | ---: |");

    run_case(
        "wal_encode_uncompressed",
        args.records,
        args.warmup_iterations,
        args.iterations,
        || {
            corpus.records.iter().fold(0_u64, |checksum, record| {
                let frame = black_box(encode_wal_record_frame(record, false));
                checksum
                    .wrapping_add(frame.len() as u64)
                    .wrapping_add(frame.first().copied().unwrap_or_default() as u64)
            })
        },
    );
    run_case(
        "wal_encode_compressed",
        args.records,
        args.warmup_iterations,
        args.iterations,
        || {
            corpus.records.iter().fold(0_u64, |checksum, record| {
                let frame = black_box(encode_wal_record_frame(record, true));
                checksum
                    .wrapping_add(frame.len() as u64)
                    .wrapping_add(frame.first().copied().unwrap_or_default() as u64)
            })
        },
    );
    run_case(
        "wal_decode_owned",
        args.records,
        args.warmup_iterations,
        args.iterations,
        || {
            let records = decode_wal_records(SharedBytes::from(wal_segment.clone()))
                .expect("WAL decode should succeed");
            checksum_mutation_records(&records)
        },
    );
    run_case(
        "fcrp_decode_owned",
        args.records,
        args.warmup_iterations,
        args.iterations,
        || {
            let mutations =
                decode_mutation_batch(&mutation_payload).expect("FCRP decode should succeed");
            checksum_replication_mutations(&mutations)
        },
    );
    run_case(
        "fcrp_visit_borrowed",
        args.records,
        args.warmup_iterations,
        args.iterations,
        || {
            let mut checksum = 0_u64;
            visit_mutation_batch_payload(&mutation_payload, |mutation| {
                checksum = checksum.wrapping_add(checksum_borrowed_mutation(mutation));
                Ok(())
            })
            .expect("FCRP borrowed visit should succeed");
            checksum
        },
    );
    run_case(
        "client_route_key",
        args.records,
        args.warmup_iterations,
        args.iterations,
        || {
            corpus.records.iter().fold(0_u64, |checksum, record| {
                checksum.wrapping_add(checksum_route(router.route_key(record.key.as_ref())))
            })
        },
    );
    run_case(
        "client_cached_route_scan",
        args.records,
        args.warmup_iterations,
        args.iterations,
        || {
            client_routes.iter().fold(0_u64, |checksum, route| {
                checksum.wrapping_add(checksum_route(*route))
            })
        },
    );
    run_case(
        "engine_fast_exists_no_hash",
        args.records,
        args.warmup_iterations,
        args.iterations,
        || engine_fast_exists(&runtime, &engine, &corpus, false),
    );
    run_case(
        "engine_fast_exists_prehashed",
        args.records,
        args.warmup_iterations,
        args.iterations,
        || engine_fast_exists(&runtime, &engine, &corpus, true),
    );
    run_case(
        "engine_fast_expire_no_hash",
        args.records,
        args.warmup_iterations,
        args.iterations,
        || engine_fast_expire(&runtime, &engine, &corpus, false),
    );
    run_case(
        "engine_fast_expire_prehashed",
        args.records,
        args.warmup_iterations,
        args.iterations,
        || engine_fast_expire(&runtime, &engine, &corpus, true),
    );
    run_case(
        "restore_entries",
        args.records,
        args.warmup_iterations,
        args.iterations,
        || {
            let store = EmbeddedStore::new(args.shards);
            store.restore_entries(corpus.entries.clone());
            store.entry_snapshot().len() as u64
        },
    );
    run_case(
        "wal_decode_then_restore",
        args.records,
        args.warmup_iterations,
        args.iterations,
        || {
            let records = decode_wal_records(SharedBytes::from(wal_segment.clone()))
                .expect("WAL decode should succeed");
            let store = EmbeddedStore::new(args.shards);
            store.restore_entries(records.into_iter().map(stored_entry_from_mutation));
            store.entry_snapshot().len() as u64
        },
    );

    runtime.block_on(engine.shutdown())?;
    Ok(())
}

struct Corpus {
    records: Vec<MutationRecord>,
    key_hashes: Vec<u64>,
    replication: Vec<ReplicationMutation>,
    entries: Vec<StoredEntry>,
}

impl Corpus {
    fn build(records: usize, value_size: usize, shards: usize) -> Self {
        let mut mutation_records = Vec::with_capacity(records);
        let mut key_hashes = Vec::with_capacity(records);
        let mut replication = Vec::with_capacity(records);
        let mut entries = Vec::with_capacity(records);
        for index in 0..records {
            let key = format!("population-key-{index:08}").into_bytes();
            let value = value_bytes(index, value_size);
            let key_hash = hash_key(&key);
            let shard_id = shard_id(key_hash, shards);
            let key = SharedBytes::from(key);
            let value = SharedBytes::from(value);
            let record = MutationRecord {
                shard_id,
                sequence: index as u64 + 1,
                timestamp_ms: 1_700_000_000_000 + index as u64,
                op: MutationOp::Set,
                key: key.clone(),
                value: value.clone(),
                expire_at_ms: None,
            };
            replication.push(ReplicationMutation::from_record_with_key_hash(
                &record, key_hash,
            ));
            key_hashes.push(key_hash);
            entries.push(StoredEntry {
                key: key.to_vec(),
                value: value.to_vec(),
                expire_at_ms: None,
            });
            mutation_records.push(record);
        }
        Self {
            records: mutation_records,
            key_hashes,
            replication,
            entries,
        }
    }

    fn wal_segment(&self, compress: bool) -> Vec<u8> {
        let mut out = Vec::new();
        for record in &self.records {
            out.extend_from_slice(&encode_wal_record_frame(record, compress));
        }
        out
    }
}

fn stored_entry_from_mutation(record: MutationRecord) -> StoredEntry {
    StoredEntry {
        key: record.key.to_vec(),
        value: record.value.to_vec(),
        expire_at_ms: record.expire_at_ms,
    }
}

fn run_case<F>(label: &str, records: usize, warmup_iterations: usize, iterations: usize, mut f: F)
where
    F: FnMut() -> u64,
{
    for _ in 0..warmup_iterations {
        black_box(f());
    }
    let mut best = Duration::MAX;
    let mut checksum = 0_u64;
    for _ in 0..iterations {
        let started = Instant::now();
        checksum = black_box(f());
        best = best.min(started.elapsed());
    }
    let ns_per_record = best.as_nanos() as f64 / records.max(1) as f64;
    let records_per_second = records as f64 / best.as_secs_f64();
    println!(
        "| {label} | {ns_per_record:.1} | {records_per_second:.0} | {:.3} | {checksum} |",
        best.as_secs_f64() * 1_000.0
    );
}

fn checksum_mutation_records(records: &[MutationRecord]) -> u64 {
    records.iter().fold(0_u64, |checksum, record| {
        checksum
            .wrapping_add(record.sequence)
            .wrapping_add(record.key.len() as u64)
            .wrapping_add(record.value.len() as u64)
    })
}

fn checksum_replication_mutations(records: &[ReplicationMutation]) -> u64 {
    records.iter().fold(0_u64, |checksum, record| {
        checksum
            .wrapping_add(record.sequence)
            .wrapping_add(record.key.len() as u64)
            .wrapping_add(record.value.len() as u64)
    })
}

fn checksum_borrowed_mutation(mutation: BorrowedReplicationMutation<'_>) -> u64 {
    mutation
        .sequence
        .wrapping_add(mutation.key.len() as u64)
        .wrapping_add(mutation.value.len() as u64)
}

fn checksum_route(route: shardcache_client_rs::ShardCacheRoute) -> u64 {
    route
        .key_hash
        .wrapping_add(route.key_tag)
        .wrapping_add(route.shard_id as u64)
}

fn open_request_path_engine(shards: usize) -> Result<EngineHandle, BoxError> {
    let mut config = ShardCacheConfig::default();
    config.shard_count = shards.next_power_of_two().max(1);
    config.persistence.enabled = false;
    config.replication.enabled = false;
    EngineHandle::open(config).map_err(Into::into)
}

fn populate_engine(runtime: &tokio::runtime::Runtime, engine: &EngineHandle, corpus: &Corpus) {
    runtime.block_on(async {
        for (index, record) in corpus.records.iter().enumerate() {
            let response = engine
                .execute_fast(FastRequest {
                    key_hash: Some(corpus.key_hashes[index]),
                    route_shard: None,
                    key_tag: None,
                    command: FastCommand::Set {
                        key: record.key.as_ref(),
                        value: record.value.as_ref(),
                    },
                })
                .await
                .expect("engine SET should succeed");
            debug_assert_eq!(response, FastResponse::Ok);
        }
    });
}

fn engine_fast_exists(
    runtime: &tokio::runtime::Runtime,
    engine: &EngineHandle,
    corpus: &Corpus,
    prehashed: bool,
) -> u64 {
    runtime.block_on(async {
        let mut checksum = 0_u64;
        for (index, record) in corpus.records.iter().enumerate() {
            let response = engine
                .execute_fast(FastRequest {
                    key_hash: prehashed.then_some(corpus.key_hashes[index]),
                    route_shard: None,
                    key_tag: None,
                    command: FastCommand::Exists {
                        key: record.key.as_ref(),
                    },
                })
                .await
                .expect("engine EXISTS should succeed");
            checksum = checksum.wrapping_add(checksum_fast_response(response));
        }
        checksum
    })
}

fn engine_fast_expire(
    runtime: &tokio::runtime::Runtime,
    engine: &EngineHandle,
    corpus: &Corpus,
    prehashed: bool,
) -> u64 {
    runtime.block_on(async {
        let mut checksum = 0_u64;
        for (index, record) in corpus.records.iter().enumerate() {
            let response = engine
                .execute_fast(FastRequest {
                    key_hash: prehashed.then_some(corpus.key_hashes[index]),
                    route_shard: None,
                    key_tag: None,
                    command: FastCommand::Expire {
                        key: record.key.as_ref(),
                        ttl_ms: 60_000,
                    },
                })
                .await
                .expect("engine EXPIRE should succeed");
            checksum = checksum.wrapping_add(checksum_fast_response(response));
        }
        checksum
    })
}

fn checksum_fast_response(response: FastResponse) -> u64 {
    match response {
        FastResponse::Ok | FastResponse::Null => 0,
        FastResponse::Error(value) | FastResponse::Value(value) => value.len() as u64,
        FastResponse::Integer(value) => value as u64,
        FastResponse::Boolean(value) => value as u64,
        FastResponse::Array(values) => values.len() as u64,
        FastResponse::Float(value) => value.to_bits(),
    }
}

fn shard_id(key_hash: u64, shards: usize) -> usize {
    let shift = shardmap::storage::shift_for(shards);
    shardmap::storage::stripe_index(key_hash, shift)
}

fn value_bytes(index: usize, len: usize) -> Vec<u8> {
    let mut value = vec![0_u8; len];
    for (offset, byte) in value.iter_mut().enumerate() {
        *byte = ((index.wrapping_mul(31) ^ offset.wrapping_mul(17)) & 0xff) as u8;
    }
    value
}
