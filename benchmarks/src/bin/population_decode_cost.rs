//! Focused WAL, routing, and shard-population diagnostics.
//!
//! Timings include each named operation and its output construction. Full
//! semantic checks run after the timer for every warmup and measured iteration.
//! The fixed-header rows are isolated read probes; they do not replace or
//! qualify the production WAL and snapshot decoders.

use std::hint::black_box;

use bytes::Bytes as SharedBytes;
use clap::Parser;
use shardcache_benchmarks::diagnostic::{
    Measurement, ensure_unique, finish, measure, validate_geometry, validate_repetitions,
};
use shardcache_benchmarks::diagnostic_headers::HeaderKind;
use shardcache_client_rs::{
    ShardCacheDirectRouter, ShardCacheRoute, hash_key, hash_key_tag, shard_index,
};
use shardmap::config::ShardCacheConfig;
use shardmap::persistence::{decode_wal_records, encode_wal_record_frame};
use shardmap::protocol::{FastCommand, FastRequest, FastResponse};
use shardmap::storage::{
    EmbeddedStore, EngineHandle, MutationOp, MutationRecord, StoredEntry,
    hash_key as store_hash_key,
};
use tokio::runtime::{Builder, Runtime};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Parser, Debug)]
#[command(about = "Measure public WAL decode, routing, and shard population paths")]
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

    /// Comma-separated case labels, or `all` for every public and header case.
    #[arg(long, default_value = "all")]
    cases: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Case {
    WalEncodeUncompressed,
    WalEncodeCompressed,
    WalDecodeOwned,
    ClientRouteKey,
    ClientCachedRouteScan,
    EngineFastExistsNoHash,
    EngineFastExistsPrehashed,
    EngineFastExpireNoHash,
    EngineFastExpirePrehashed,
    RestoreEntries,
    WalDecodeThenRestore,
    FrameHeaderNative,
    FrameHeaderTyped,
    LegacyHeaderNative,
    LegacyHeaderTyped,
    U32HeaderNative,
    U32HeaderTyped,
    U64HeaderNative,
    U64HeaderTyped,
}

impl Case {
    const ALL: [Self; 19] = [
        Self::WalEncodeUncompressed,
        Self::WalEncodeCompressed,
        Self::WalDecodeOwned,
        Self::ClientRouteKey,
        Self::ClientCachedRouteScan,
        Self::EngineFastExistsNoHash,
        Self::EngineFastExistsPrehashed,
        Self::EngineFastExpireNoHash,
        Self::EngineFastExpirePrehashed,
        Self::RestoreEntries,
        Self::WalDecodeThenRestore,
        Self::FrameHeaderNative,
        Self::FrameHeaderTyped,
        Self::LegacyHeaderNative,
        Self::LegacyHeaderTyped,
        Self::U32HeaderNative,
        Self::U32HeaderTyped,
        Self::U64HeaderNative,
        Self::U64HeaderTyped,
    ];

    fn parse(label: &str) -> Result<Self, BoxError> {
        match label.trim() {
            "wal_encode_uncompressed" => Ok(Self::WalEncodeUncompressed),
            "wal_encode_compressed" => Ok(Self::WalEncodeCompressed),
            "wal_decode_owned" => Ok(Self::WalDecodeOwned),
            "client_route_key" => Ok(Self::ClientRouteKey),
            "client_cached_route_scan" => Ok(Self::ClientCachedRouteScan),
            "engine_fast_exists_no_hash" => Ok(Self::EngineFastExistsNoHash),
            "engine_fast_exists_prehashed" => Ok(Self::EngineFastExistsPrehashed),
            "engine_fast_expire_no_hash" => Ok(Self::EngineFastExpireNoHash),
            "engine_fast_expire_prehashed" => Ok(Self::EngineFastExpirePrehashed),
            "restore_entries" => Ok(Self::RestoreEntries),
            "wal_decode_then_restore" => Ok(Self::WalDecodeThenRestore),
            "frame_header_native" => Ok(Self::FrameHeaderNative),
            "frame_header_typed" => Ok(Self::FrameHeaderTyped),
            "legacy_header_native" => Ok(Self::LegacyHeaderNative),
            "legacy_header_typed" => Ok(Self::LegacyHeaderTyped),
            "u32_header_native" => Ok(Self::U32HeaderNative),
            "u32_header_typed" => Ok(Self::U32HeaderTyped),
            "u64_header_native" => Ok(Self::U64HeaderNative),
            "u64_header_typed" => Ok(Self::U64HeaderTyped),
            other => Err(format!("unknown case `{other}`").into()),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::WalEncodeUncompressed => "wal_encode_uncompressed",
            Self::WalEncodeCompressed => "wal_encode_compressed",
            Self::WalDecodeOwned => "wal_decode_owned",
            Self::ClientRouteKey => "client_route_key",
            Self::ClientCachedRouteScan => "client_cached_route_scan",
            Self::EngineFastExistsNoHash => "engine_fast_exists_no_hash",
            Self::EngineFastExistsPrehashed => "engine_fast_exists_prehashed",
            Self::EngineFastExpireNoHash => "engine_fast_expire_no_hash",
            Self::EngineFastExpirePrehashed => "engine_fast_expire_prehashed",
            Self::RestoreEntries => "restore_entries",
            Self::WalDecodeThenRestore => "wal_decode_then_restore",
            Self::FrameHeaderNative => "frame_header_native",
            Self::FrameHeaderTyped => "frame_header_typed",
            Self::LegacyHeaderNative => "legacy_header_native",
            Self::LegacyHeaderTyped => "legacy_header_typed",
            Self::U32HeaderNative => "u32_header_native",
            Self::U32HeaderTyped => "u32_header_typed",
            Self::U64HeaderNative => "u64_header_native",
            Self::U64HeaderTyped => "u64_header_typed",
        }
    }

    fn header(self) -> Option<(HeaderKind, bool)> {
        match self {
            Self::FrameHeaderNative => Some((HeaderKind::Frame, false)),
            Self::FrameHeaderTyped => Some((HeaderKind::Frame, true)),
            Self::LegacyHeaderNative => Some((HeaderKind::Legacy, false)),
            Self::LegacyHeaderTyped => Some((HeaderKind::Legacy, true)),
            Self::U32HeaderNative => Some((HeaderKind::U32, false)),
            Self::U32HeaderTyped => Some((HeaderKind::U32, true)),
            Self::U64HeaderNative => Some((HeaderKind::U64, false)),
            Self::U64HeaderTyped => Some((HeaderKind::U64, true)),
            _ => None,
        }
    }
}

struct Corpus {
    records: Vec<MutationRecord>,
    key_hashes: Vec<u64>,
    entries: Vec<StoredEntry>,
    sorted_entries: Vec<StoredEntry>,
}

struct PublicContext<'a> {
    args: &'a Args,
    corpus: &'a Corpus,
    router: &'a ShardCacheDirectRouter,
    expected_routes: &'a [ShardCacheRoute],
    wal_plain: &'a [u8],
    runtime: &'a Runtime,
    engine: &'a EngineHandle,
}

impl Corpus {
    fn build(count: usize, value_size: usize, shards: usize) -> Result<Self, BoxError> {
        let mut records = Vec::new();
        let mut key_hashes = Vec::new();
        let mut entries = Vec::new();
        records.try_reserve_exact(count)?;
        key_hashes.try_reserve_exact(count)?;
        entries.try_reserve_exact(count)?;

        for index in 0..count {
            let sequence = u64::try_from(index)
                .map_err(|_| "record index exceeds the WAL sequence range")?
                .checked_add(1)
                .ok_or("WAL sequence overflow")?;
            let key = format!("population-key-{index:08}").into_bytes();
            let value = value_bytes(index, value_size)?;
            let key_hash = hash_key(&key);
            let shard_id = shard_index(key_hash, shards)?;
            let expire_at_ms = Some(
                4_000_000_000_000_u64
                    .checked_add(u64::try_from(index).map_err(|_| "record index overflow")?)
                    .ok_or("expiration timestamp overflow")?,
            );
            let governance = (index % 3 == 0)
                .then(|| SharedBytes::from(format!("governance-{index}").into_bytes()));
            let key = SharedBytes::from(key);
            let value = SharedBytes::from(value);
            records.push(MutationRecord {
                shard_id,
                sequence,
                timestamp_ms: 1_700_000_000_000_u64
                    .checked_add(sequence)
                    .ok_or("mutation timestamp overflow")?,
                op: MutationOp::Set,
                key: key.clone(),
                value: value.clone(),
                expire_at_ms,
                governance: governance.clone(),
            });
            key_hashes.push(key_hash);
            entries.push(StoredEntry {
                key: key.to_vec(),
                value: value.to_vec(),
                expire_at_ms,
                governance: governance.map(|bytes| bytes.to_vec()),
            });
        }

        let mut sorted_entries = entries.clone();
        sorted_entries.sort_by_key(|entry| store_hash_key(&entry.key));
        Ok(Self {
            records,
            key_hashes,
            entries,
            sorted_entries,
        })
    }

    fn wal_segment(&self, compress: bool) -> Result<Vec<u8>, BoxError> {
        let mut segment = Vec::new();
        for record in &self.records {
            let frame = encode_wal_record_frame(record, compress);
            segment.try_reserve(frame.len())?;
            segment.extend_from_slice(&frame);
        }
        Ok(segment)
    }
}

fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    let cases = validate_args(&args)?;
    let corpus = Corpus::build(args.records, args.value_size, args.shards)?;
    let router = ShardCacheDirectRouter::new(("127.0.0.1", 6500), args.shards)?;
    let expected_routes = corpus
        .records
        .iter()
        .map(|record| {
            let key = record.key.as_ref();
            let key_hash = hash_key(key);
            Ok(ShardCacheRoute {
                key_hash,
                key_tag: hash_key_tag(key),
                shard_id: shard_index(key_hash, args.shards)?,
            })
        })
        .collect::<Result<Vec<_>, BoxError>>()?;
    let wal_plain = corpus.wal_segment(false)?;
    let frame_header = wal_plain
        .get(..9)
        .ok_or("encoded WAL is missing its 9-byte frame header")?
        .to_vec();
    let legacy_header = wal_plain
        .get(9..34)
        .ok_or("encoded WAL is missing its 25-byte legacy record header")?
        .to_vec();
    let u32_header = 0x1234_5678_u32.to_le_bytes().to_vec();
    let u64_header = 0x1234_5678_9abc_def0_u64.to_le_bytes().to_vec();
    let runtime = Builder::new_current_thread().enable_all().build()?;
    let engine = open_engine(args.shards)?;
    let public = PublicContext {
        args: &args,
        corpus: &corpus,
        router: &router,
        expected_routes: &expected_routes,
        wal_plain: &wal_plain,
        runtime: &runtime,
        engine: &engine,
    };

    let work = (|| {
        populate_engine(&runtime, &engine, &corpus)?;
        verify_engine_contents(&runtime, &engine, &corpus)?;
        println!(
            "records={} value_size={} shards={} wal_bytes={} cases={}",
            args.records,
            args.value_size,
            args.shards,
            wal_plain.len(),
            cases
                .iter()
                .map(|case| case.label())
                .collect::<Vec<_>>()
                .join(",")
        );
        println!("| path | ns/record | records/s | best ms | checksum |");
        println!("| --- | ---: | ---: | ---: | ---: |");

        for case in cases {
            let measurement = if let Some((kind, typed)) = case.header() {
                let bytes = match kind {
                    HeaderKind::Frame => frame_header.as_slice(),
                    HeaderKind::Legacy => legacy_header.as_slice(),
                    HeaderKind::U32 => u32_header.as_slice(),
                    HeaderKind::U64 => u64_header.as_slice(),
                };
                run_header_case(
                    kind,
                    typed,
                    bytes,
                    args.records,
                    args.warmup_iterations,
                    args.iterations,
                )?
            } else {
                run_public_case(case, &public)?
            };
            report(case.label(), &measurement, args.records);
        }
        Ok::<(), BoxError>(())
    })();
    finish(
        work,
        runtime.block_on(engine.shutdown()).map_err(Into::into),
    )?;
    Ok(())
}

fn run_public_case(case: Case, context: &PublicContext<'_>) -> Result<Measurement, BoxError> {
    let PublicContext {
        args,
        corpus,
        router,
        expected_routes,
        wal_plain,
        runtime,
        engine,
    } = context;
    match case {
        Case::WalEncodeUncompressed | Case::WalEncodeCompressed => {
            let compressed = case == Case::WalEncodeCompressed;
            measure(
                args.iterations,
                args.warmup_iterations,
                || encode_records(corpus, compressed),
                |frames| verify_encoded_records(&frames, &corpus.records),
            )
        }
        Case::WalDecodeOwned => measure(
            args.iterations,
            args.warmup_iterations,
            || {
                // The Vec-to-Bytes ownership conversion copies the segment and
                // remains inside the measured decode operation.
                decode_wal_records(SharedBytes::from(wal_plain.to_vec())).map_err(Into::into)
            },
            |records| verify_mutation_records(&records, &corpus.records),
        ),
        Case::ClientRouteKey => measure(
            args.iterations,
            args.warmup_iterations,
            || {
                let checksum = corpus.records.iter().fold(0_u64, |sum, record| {
                    let route = black_box(router.route_key(record.key.as_ref()));
                    sum.wrapping_add(checksum_route(route))
                });
                Ok(checksum)
            },
            |checksum| verify_route_checksum(checksum, expected_routes),
        ),
        Case::ClientCachedRouteScan => measure(
            args.iterations,
            args.warmup_iterations,
            || {
                let checksum = expected_routes.iter().copied().fold(0_u64, |sum, route| {
                    sum.wrapping_add(checksum_route(black_box(route)))
                });
                Ok(checksum)
            },
            |checksum| verify_route_checksum(checksum, expected_routes),
        ),
        Case::EngineFastExistsNoHash | Case::EngineFastExistsPrehashed => measure(
            args.iterations,
            args.warmup_iterations,
            || {
                engine_fast_exists(
                    runtime,
                    engine,
                    corpus,
                    case == Case::EngineFastExistsPrehashed,
                )
            },
            |count| {
                let count = verify_engine_count(count, corpus.records.len())?;
                verify_engine_contents(runtime, engine, corpus)?;
                if matches!(
                    case,
                    Case::EngineFastExpireNoHash | Case::EngineFastExpirePrehashed
                ) {
                    verify_engine_expirations(runtime, engine, corpus)?;
                }
                Ok(count)
            },
        ),
        Case::EngineFastExpireNoHash | Case::EngineFastExpirePrehashed => measure(
            args.iterations,
            args.warmup_iterations,
            || {
                engine_fast_expire(
                    runtime,
                    engine,
                    corpus,
                    case == Case::EngineFastExpirePrehashed,
                )
            },
            |count| {
                let count = verify_engine_count(count, corpus.records.len())?;
                verify_engine_contents(runtime, engine, corpus)?;
                verify_engine_expirations(runtime, engine, corpus)?;
                Ok(count)
            },
        ),
        Case::RestoreEntries => measure(
            args.iterations,
            args.warmup_iterations,
            || {
                let store = EmbeddedStore::new(args.shards);
                store.restore_entries(corpus.entries.clone());
                store.try_entry_snapshot().map_err(Into::into)
            },
            |entries| verify_stored_entries(&entries, &corpus.sorted_entries),
        ),
        Case::WalDecodeThenRestore => measure(
            args.iterations,
            args.warmup_iterations,
            || {
                let records = decode_wal_records(SharedBytes::from(wal_plain.to_vec()))?;
                let store = EmbeddedStore::new(args.shards);
                store.restore_entries(records.into_iter().map(stored_entry_from_mutation));
                store.try_entry_snapshot().map_err(Into::into)
            },
            |entries| verify_stored_entries(&entries, &corpus.sorted_entries),
        ),
        header_case => Err(format!("unexpected non-header case {}", header_case.label()).into()),
    }
}

fn run_header_case(
    kind: HeaderKind,
    typed: bool,
    bytes: &[u8],
    records: usize,
    warmups: usize,
    iterations: usize,
) -> Result<Measurement, BoxError> {
    let expected = kind.native(bytes)?;
    if kind.typed(bytes)? != expected {
        return Err("native and typed header probes disagree before timing".into());
    }
    let expected_checksum = (0..records).fold(0_u64, |sum, _| sum.wrapping_add(expected));
    measure(
        iterations,
        warmups,
        || {
            let mut checksum = 0_u64;
            for _ in 0..records {
                let value = if typed {
                    kind.typed(black_box(bytes))?
                } else {
                    kind.native(black_box(bytes))?
                };
                checksum = checksum.wrapping_add(value);
            }
            Ok(checksum)
        },
        |checksum| {
            if checksum == expected_checksum {
                Ok(checksum)
            } else {
                Err("fixed-header checksum mismatch".into())
            }
        },
    )
}

fn encode_records(corpus: &Corpus, compressed: bool) -> Result<Vec<Vec<u8>>, BoxError> {
    let mut frames = Vec::new();
    frames.try_reserve_exact(corpus.records.len())?;
    for record in &corpus.records {
        frames.push(black_box(encode_wal_record_frame(record, compressed)));
    }
    Ok(frames)
}

fn verify_encoded_records(
    frames: &[Vec<u8>],
    expected: &[MutationRecord],
) -> Result<u64, BoxError> {
    if frames.len() != expected.len() {
        return Err("WAL encoder returned the wrong number of frames".into());
    }
    let mut checksum = 0_u64;
    for (frame, expected_record) in frames.iter().zip(expected) {
        let records = decode_wal_records(SharedBytes::from(frame.clone()))?;
        if records.len() != 1 {
            return Err("encoded WAL frame did not decode to exactly one record".into());
        }
        verify_mutation_records(&records, std::slice::from_ref(expected_record))?;
        checksum = checksum
            .wrapping_add(frame.len() as u64)
            .wrapping_add(frame.first().copied().unwrap_or_default() as u64);
    }
    Ok(checksum)
}

fn verify_mutation_records(
    actual: &[MutationRecord],
    expected: &[MutationRecord],
) -> Result<u64, BoxError> {
    if actual.len() != expected.len() {
        return Err(format!(
            "WAL record count mismatch: got {}, expected {}",
            actual.len(),
            expected.len()
        )
        .into());
    }
    let mut checksum = 0_u64;
    for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        let same_op = matches!(
            (&actual.op, &expected.op),
            (MutationOp::Set, MutationOp::Set)
        );
        // The public WAL wire format stores the operation timestamp and record
        // payload, but not the recovery sequence or shard. The current decoder
        // deliberately supplies zero for those contextual fields.
        if !same_op
            || actual.shard_id != 0
            || actual.sequence != 0
            || actual.timestamp_ms != expected.timestamp_ms
            || actual.key != expected.key
            || actual.value != expected.value
            || actual.expire_at_ms != expected.expire_at_ms
            || actual.governance != expected.governance
        {
            return Err(format!("WAL record {index} differs from its source record").into());
        }
        checksum = checksum
            .wrapping_add(actual.sequence)
            .wrapping_add(actual.shard_id as u64)
            .wrapping_add(actual.timestamp_ms)
            .wrapping_add(actual.key.len() as u64)
            .wrapping_add(actual.value.len() as u64)
            .wrapping_add(actual.expire_at_ms.unwrap_or_default())
            .wrapping_add(
                actual
                    .governance
                    .as_ref()
                    .map_or(0, |value| value.len() as u64),
            );
    }
    Ok(checksum)
}

fn verify_route_checksum(
    actual_checksum: u64,
    expected_routes: &[ShardCacheRoute],
) -> Result<u64, BoxError> {
    let expected_checksum = expected_routes
        .iter()
        .copied()
        .fold(0_u64, |sum, route| sum.wrapping_add(checksum_route(route)));
    if actual_checksum != expected_checksum {
        return Err("route checksum does not match the independently derived routes".into());
    }
    Ok(actual_checksum)
}

fn verify_engine_count(actual: u64, expected: usize) -> Result<u64, BoxError> {
    if actual != expected as u64 {
        return Err(
            format!("engine operation count mismatch: got {actual}, expected {expected}").into(),
        );
    }
    Ok(actual)
}

fn verify_stored_entries(
    actual: &[StoredEntry],
    expected: &[StoredEntry],
) -> Result<u64, BoxError> {
    if actual.len() != expected.len() {
        return Err(format!(
            "snapshot entry count mismatch: got {}, expected {}",
            actual.len(),
            expected.len()
        )
        .into());
    }
    let mut checksum = 0_u64;
    for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        if actual != expected {
            return Err(format!("snapshot entry {index} differs from its source entry").into());
        }
        checksum = checksum
            .wrapping_add(actual.key.len() as u64)
            .wrapping_add(actual.value.len() as u64)
            .wrapping_add(actual.expire_at_ms.unwrap_or_default())
            .wrapping_add(
                actual
                    .governance
                    .as_ref()
                    .map_or(0, |value| value.len() as u64),
            );
    }
    Ok(checksum)
}

fn report(label: &str, measurement: &Measurement, records: usize) {
    let ns_per_record = measurement.duration.as_nanos() as f64 / records as f64;
    let records_per_second = records as f64 / measurement.duration.as_secs_f64();
    println!(
        "| {label} | {ns_per_record:.1} | {records_per_second:.0} | {:.3} | {} |",
        measurement.duration.as_secs_f64() * 1_000.0,
        measurement.checksum
    );
}

fn parse_cases(value: &str) -> Result<Vec<Case>, BoxError> {
    if value.trim() == "all" {
        return Ok(Case::ALL.to_vec());
    }
    let cases = value
        .split(',')
        .map(Case::parse)
        .collect::<Result<Vec<_>, _>>()?;
    ensure_unique(cases.iter().map(|case| case.label()), "cases")?;
    Ok(cases)
}

fn validate_args(args: &Args) -> Result<Vec<Case>, BoxError> {
    let cases = parse_cases(&args.cases)?;
    validate_geometry(args.records, args.value_size, args.shards)?;
    validate_repetitions(args.records, args.iterations, args.warmup_iterations)?;
    args.records
        .checked_mul(
            args.value_size
                .checked_add(256)
                .ok_or("WAL size overflow")?,
        )
        .ok_or("estimated WAL size overflows usize")?;
    Ok(cases)
}

fn stored_entry_from_mutation(record: MutationRecord) -> StoredEntry {
    StoredEntry {
        key: record.key.to_vec(),
        value: record.value.to_vec(),
        expire_at_ms: record.expire_at_ms,
        governance: record.governance.map(|bytes| bytes.to_vec()),
    }
}

fn checksum_route(route: ShardCacheRoute) -> u64 {
    route
        .key_hash
        .wrapping_add(route.key_tag.rotate_left(11))
        .wrapping_add(route.shard_id as u64)
}

fn open_engine(shards: usize) -> Result<EngineHandle, BoxError> {
    let mut config = ShardCacheConfig {
        shard_count: shards,
        ..ShardCacheConfig::default()
    };
    config.persistence.enabled = false;
    Ok(EngineHandle::open(config)?)
}

fn populate_engine(
    runtime: &Runtime,
    engine: &EngineHandle,
    corpus: &Corpus,
) -> Result<(), BoxError> {
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
                .await?;
            if response != FastResponse::Ok {
                return Err(format!("engine population SET failed: {response:?}").into());
            }
        }
        Ok(())
    })
}

fn engine_fast_exists(
    runtime: &Runtime,
    engine: &EngineHandle,
    corpus: &Corpus,
    prehashed: bool,
) -> Result<u64, BoxError> {
    runtime.block_on(async {
        let mut count = 0_u64;
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
                .await?;
            count = count.wrapping_add(expect_integer_one(response, "EXISTS")?);
        }
        Ok(count)
    })
}

fn engine_fast_expire(
    runtime: &Runtime,
    engine: &EngineHandle,
    corpus: &Corpus,
    prehashed: bool,
) -> Result<u64, BoxError> {
    runtime.block_on(async {
        let mut count = 0_u64;
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
                .await?;
            count = count.wrapping_add(expect_integer_one(response, "EXPIRE")?);
        }
        Ok(count)
    })
}

fn expect_integer_one(response: FastResponse, command: &str) -> Result<u64, BoxError> {
    match response {
        FastResponse::Integer(1) => Ok(1),
        other => Err(format!("{command} returned an unexpected response: {other:?}").into()),
    }
}

fn verify_engine_contents(
    runtime: &Runtime,
    engine: &EngineHandle,
    corpus: &Corpus,
) -> Result<(), BoxError> {
    runtime.block_on(async {
        for (index, record) in corpus.records.iter().enumerate() {
            let response = engine
                .execute_fast(FastRequest {
                    key_hash: Some(corpus.key_hashes[index]),
                    route_shard: None,
                    key_tag: None,
                    command: FastCommand::Get {
                        key: record.key.as_ref(),
                    },
                })
                .await?;
            match response {
                FastResponse::Value(value) if value.as_slice() == record.value.as_ref() => {}
                other => {
                    return Err(format!("engine content verification failed: {other:?}").into());
                }
            }
        }
        Ok(())
    })
}

fn verify_engine_expirations(
    runtime: &Runtime,
    engine: &EngineHandle,
    corpus: &Corpus,
) -> Result<(), BoxError> {
    runtime.block_on(async {
        for record in &corpus.records {
            let response = engine
                .execute_fast(FastRequest {
                    key_hash: Some(hash_key(record.key.as_ref())),
                    route_shard: None,
                    key_tag: None,
                    command: FastCommand::Ttl {
                        key: record.key.as_ref(),
                    },
                })
                .await?;
            if !matches!(response, FastResponse::Integer(0..=60)) {
                return Err(format!("EXPIRE did not retain a live TTL: {response:?}").into());
            }
        }
        Ok(())
    })
}

fn value_bytes(index: usize, len: usize) -> Result<Vec<u8>, BoxError> {
    let mut value = Vec::new();
    value.try_reserve_exact(len)?;
    value.resize(len, 0);
    for (offset, byte) in value.iter_mut().enumerate() {
        *byte = ((index.wrapping_mul(31) ^ offset.wrapping_mul(17)) & 0xff) as u8;
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_cases_reject_empty_unknown_and_duplicate_names() {
        assert!(parse_cases("").is_err());
        assert!(parse_cases("wal_decode_owned,wal_decode_owned").is_err());
        assert!(parse_cases("wal_decode_owned,").is_err());
        assert!(parse_cases("not_a_case").is_err());
        assert_eq!(parse_cases("all").unwrap().len(), Case::ALL.len());
        let args = Args {
            records: 0,
            value_size: 1,
            shards: 1,
            iterations: 1,
            warmup_iterations: 0,
            cases: "wal_decode_owned".to_owned(),
        };
        assert!(validate_args(&args).is_err());
    }

    #[test]
    fn bounded_smoke_executes_all_public_and_fixed_header_cases() {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");
        let router =
            ShardCacheDirectRouter::new(("127.0.0.1", 6500), 2).expect("test router should build");
        let engine = open_engine(2).expect("test engine should open");
        let work = (|| {
            for value_size in [0, 17] {
                let corpus = Corpus::build(3, value_size, 2)?;
                let expected_routes = corpus
                    .records
                    .iter()
                    .map(|record| router.route_key(record.key.as_ref()))
                    .collect::<Vec<_>>();
                let wal_plain = corpus.wal_segment(false)?;
                let u32_header = 0x1234_5678_u32.to_le_bytes().to_vec();
                let u64_header = 0x1234_5678_9abc_def0_u64.to_le_bytes().to_vec();
                let args = Args {
                    records: 3,
                    value_size,
                    shards: 2,
                    iterations: 1,
                    warmup_iterations: 0,
                    cases: "all".to_owned(),
                };
                let public = PublicContext {
                    args: &args,
                    corpus: &corpus,
                    router: &router,
                    expected_routes: &expected_routes,
                    wal_plain: &wal_plain,
                    runtime: &runtime,
                    engine: &engine,
                };
                populate_engine(&runtime, &engine, &corpus)?;
                verify_engine_contents(&runtime, &engine, &corpus)?;
                for case in Case::ALL {
                    if let Some((kind, typed)) = case.header() {
                        let bytes = match kind {
                            HeaderKind::Frame => {
                                wal_plain.get(..9).ok_or("missing frame header")?
                            }
                            HeaderKind::Legacy => {
                                wal_plain.get(9..34).ok_or("missing legacy header")?
                            }
                            HeaderKind::U32 => u32_header.as_slice(),
                            HeaderKind::U64 => u64_header.as_slice(),
                        };
                        run_header_case(kind, typed, bytes, 3, 0, 1)?;
                    } else {
                        run_public_case(case, &public)?;
                    }
                }
            }
            Ok::<(), BoxError>(())
        })();
        finish(
            work,
            runtime.block_on(engine.shutdown()).map_err(Into::into),
        )
        .expect("all bounded diagnostic cases should pass");
    }

    #[test]
    fn corruption_truncation_and_full_field_mismatch_are_errors() {
        let corpus = Corpus::build(2, 9, 2).expect("test corpus should build");
        let segment = corpus.wal_segment(false).expect("test WAL should encode");
        let truncated =
            decode_wal_records(SharedBytes::from(segment[..segment.len() - 1].to_vec()))
                .expect("the public decoder may stop at an incomplete trailing frame");
        assert!(verify_mutation_records(&truncated, &corpus.records).is_err());
        let mut corrupted = segment.clone();
        let first_frame_len = encode_wal_record_frame(&corpus.records[0], false).len();
        corrupted[first_frame_len - 1] ^= 1;
        let decoded = decode_wal_records(SharedBytes::from(corrupted))
            .expect("the public decoder may skip a frame with a bad checksum");
        assert!(verify_mutation_records(&decoded, &corpus.records).is_err());

        let decoded = decode_wal_records(SharedBytes::from(segment.clone()))
            .expect("valid test WAL should decode");
        verify_mutation_records(&decoded, &corpus.records)
            .expect("encoded fields should match the public WAL semantics");
        let mut changed = decoded;
        changed[0].value = SharedBytes::copy_from_slice(b"different");
        assert!(verify_mutation_records(&changed, &corpus.records).is_err());
        let mut changed_entries = corpus.sorted_entries.clone();
        changed_entries[0].governance = Some(b"different".to_vec());
        assert!(verify_stored_entries(&changed_entries, &corpus.sorted_entries).is_err());
    }

    #[test]
    fn engine_rows_reject_null_wrong_types_and_changed_operation_counts() {
        assert_eq!(
            expect_integer_one(FastResponse::Integer(1), "EXISTS").unwrap(),
            1
        );
        assert!(expect_integer_one(FastResponse::Null, "EXISTS").is_err());
        assert!(expect_integer_one(FastResponse::Ok, "EXPIRE").is_err());
        assert!(expect_integer_one(FastResponse::Integer(0), "EXPIRE").is_err());
        assert!(verify_engine_count(2, 3).is_err());
    }
}
