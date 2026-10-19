//! Large-value request-path copy and allocation benchmark.
//!
//! This isolates the paths where zero-copy owner handoff can matter for
//! embedded and engine workloads. The allocator counters measure heap pressure;
//! The modeled payload-copy lower bound records known full-value copy work even
//! when shardmap reuses value buffers and avoids new allocations.

use std::alloc::{GlobalAlloc, Layout, System};
use std::fmt;
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes as SharedBytes;
use clap::Parser;
use shardcache_benchmarks::diagnostic::{
    ensure_unique, finish, validate_geometry, validate_repetitions,
};
use shardmap::config::ShardCacheConfig;
use shardmap::protocol::{FastCommand, FastRequest, FastResponse, RespCodec};
use shardmap::storage::{BorrowedCommand, EmbeddedStore, EngineHandle, hash_key};
use tokio::runtime::{Builder, Runtime};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

// SAFETY: pointer/layout contracts are forwarded unchanged to System. Counters
// never access allocations or alter returned pointers.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        // SAFETY: the GlobalAlloc caller supplies the unchanged System contract.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation(layout.size());
        // SAFETY: the GlobalAlloc caller supplies the unchanged System contract.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the GlobalAlloc caller supplies the unchanged System contract.
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_allocation(new_size);
        // SAFETY: the GlobalAlloc caller supplies the unchanged System contract.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

fn record_allocation(size: usize) {
    ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
    ALLOC_BYTES.fetch_add(size as u64, Ordering::Relaxed);
}

fn reset_allocations() {
    ALLOC_COUNT.store(0, Ordering::Relaxed);
    ALLOC_BYTES.store(0, Ordering::Relaxed);
}

fn allocation_snapshot() -> AllocationSnapshot {
    AllocationSnapshot {
        count: ALLOC_COUNT.load(Ordering::Relaxed),
        bytes: ALLOC_BYTES.load(Ordering::Relaxed),
    }
}

#[derive(Debug, Clone, Copy)]
struct AllocationSnapshot {
    count: u64,
    bytes: u64,
}

#[derive(Parser, Debug)]
#[command(about = "Measure large-value copies and allocations across shardmap request paths")]
struct Args {
    #[arg(long, default_value = "4096,65536,1048576")]
    value_sizes: String,

    #[arg(long, default_value_t = 512)]
    ops: usize,

    #[arg(long, default_value_t = 32)]
    keys: usize,

    #[arg(long, default_value_t = 16)]
    shards: usize,

    #[arg(long, default_value_t = 3)]
    iterations: usize,

    #[arg(long, default_value_t = 1)]
    warmup_iterations: usize,

    #[arg(
        long,
        default_value = "embedded-set-slice,embedded-set-owned-bytes,embedded-set-copy-bytes,embedded-get-copy,embedded-get-value-bytes,embedded-get-ref,embedded-with-value-bytes,engine-fast-set,engine-fast-get,engine-resp-borrowed-set,engine-resp-spanned-set,engine-resp-get"
    )]
    modes: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    EmbeddedSetSlice,
    EmbeddedSetOwnedBytes,
    EmbeddedSetCopyBytes,
    EmbeddedGetCopy,
    EmbeddedGetValueBytes,
    EmbeddedGetRef,
    EmbeddedWithValueBytes,
    EngineFastSet,
    EngineFastGet,
    EngineRespBorrowedSet,
    EngineRespSpannedSet,
    EngineRespGet,
}

impl Mode {
    fn parse(value: &str) -> Result<Self, BoxError> {
        match value.trim() {
            "embedded-set-slice" => Ok(Self::EmbeddedSetSlice),
            "embedded-set-owned-bytes" => Ok(Self::EmbeddedSetOwnedBytes),
            "embedded-set-copy-bytes" => Ok(Self::EmbeddedSetCopyBytes),
            "embedded-get-copy" => Ok(Self::EmbeddedGetCopy),
            "embedded-get-value-bytes" => Ok(Self::EmbeddedGetValueBytes),
            "embedded-get-ref" => Ok(Self::EmbeddedGetRef),
            "embedded-with-value-bytes" => Ok(Self::EmbeddedWithValueBytes),
            "engine-fast-set" => Ok(Self::EngineFastSet),
            "engine-fast-get" => Ok(Self::EngineFastGet),
            "engine-resp-borrowed-set" => Ok(Self::EngineRespBorrowedSet),
            "engine-resp-spanned-set" => Ok(Self::EngineRespSpannedSet),
            "engine-resp-get" => Ok(Self::EngineRespGet),
            other => Err(format!("unknown mode `{other}`").into()),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::EmbeddedSetSlice => "embedded-set-slice",
            Self::EmbeddedSetOwnedBytes => "embedded-set-owned-bytes",
            Self::EmbeddedSetCopyBytes => "embedded-set-copy-bytes",
            Self::EmbeddedGetCopy => "embedded-get-copy",
            Self::EmbeddedGetValueBytes => "embedded-get-value-bytes",
            Self::EmbeddedGetRef => "embedded-get-ref",
            Self::EmbeddedWithValueBytes => "embedded-with-value-bytes",
            Self::EngineFastSet => "engine-fast-set",
            Self::EngineFastGet => "engine-fast-get",
            Self::EngineRespBorrowedSet => "engine-resp-borrowed-set",
            Self::EngineRespSpannedSet => "engine-resp-spanned-set",
            Self::EngineRespGet => "engine-resp-get",
        }
    }

    fn modeled_copy_lower_bound_bytes_per_op(self, value_size: usize) -> usize {
        match self {
            Self::EmbeddedSetSlice
            | Self::EmbeddedSetCopyBytes
            | Self::EmbeddedGetCopy
            | Self::EngineFastSet
            | Self::EngineFastGet
            | Self::EngineRespBorrowedSet
            | Self::EngineRespGet => value_size,
            Self::EmbeddedSetOwnedBytes
            | Self::EmbeddedGetValueBytes
            | Self::EmbeddedGetRef
            | Self::EmbeddedWithValueBytes
            | Self::EngineRespSpannedSet => 0,
        }
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

#[derive(Debug)]
struct Corpus {
    keys: Vec<Vec<u8>>,
    key_hashes: Vec<u64>,
    values: Vec<Vec<u8>>,
    shared_values: Vec<SharedBytes>,
    resp_set_frames: Vec<SharedBytes>,
    resp_get_frames: Vec<SharedBytes>,
}

impl Corpus {
    fn build(key_count: usize, value_size: usize) -> Self {
        let mut keys = Vec::with_capacity(key_count);
        let mut key_hashes = Vec::with_capacity(key_count);
        let mut values = Vec::with_capacity(key_count);
        let mut shared_values = Vec::with_capacity(key_count);
        let mut resp_set_frames = Vec::with_capacity(key_count);
        let mut resp_get_frames = Vec::with_capacity(key_count);

        for index in 0..key_count {
            let key = format!("large-value-key-{index:08}").into_bytes();
            let value = value_bytes(index, value_size);
            key_hashes.push(hash_key(&key));
            resp_set_frames.push(SharedBytes::from(encode_resp_set(&key, &value)));
            resp_get_frames.push(SharedBytes::from(encode_resp_get(&key)));
            shared_values.push(SharedBytes::copy_from_slice(&value));
            values.push(value);
            keys.push(key);
        }

        Self {
            keys,
            key_hashes,
            values,
            shared_values,
            resp_set_frames,
            resp_get_frames,
        }
    }

    fn len(&self) -> usize {
        self.keys.len()
    }

    fn index(&self, op: usize) -> usize {
        op % self.keys.len()
    }
}

#[derive(Debug, Clone)]
struct CaseResult {
    duration: Duration,
    allocations: AllocationSnapshot,
    checksum: u64,
}

impl CaseResult {
    fn ns_per_op(&self, ops: usize) -> f64 {
        self.duration.as_secs_f64() * 1_000_000_000.0 / ops as f64
    }

    fn ops_per_sec(&self, ops: usize) -> f64 {
        ops as f64 / self.duration.as_secs_f64()
    }

    fn allocations_per_op(&self, ops: usize) -> f64 {
        self.allocations.count as f64 / ops as f64
    }

    fn allocated_bytes_per_op(&self, ops: usize) -> f64 {
        self.allocations.bytes as f64 / ops as f64
    }
}

fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    let (value_sizes, modes) = validate_args(&args)?;
    let ops = args.ops;
    let keys = args.keys;
    let shards = args.shards;
    let runtime = Builder::new_current_thread().enable_all().build()?;

    println!(
        "large-value-copy-cost: sizes={:?} modes={} ops={} keys={} shards={} iterations={} warmups={}",
        value_sizes, args.modes, ops, keys, shards, args.iterations, args.warmup_iterations
    );
    println!(
        "| {:<31} | {:>9} | {:>12} | {:>12} | {:>10} | {:>14} | {:>18} | {:>10} |",
        "mode",
        "value",
        "best ns/op",
        "ops/s",
        "allocs/op",
        "alloc bytes/op",
        "modeled copy lower-bound B/op",
        "checksum"
    );
    println!(
        "| {:-<31} | {:-<9} | {:-<12} | {:-<12} | {:-<10} | {:-<14} | {:-<18} | {:-<10} |",
        "", "", "", "", "", "", "", ""
    );

    for value_size in value_sizes {
        let corpus = Corpus::build(keys, value_size);
        for mode in modes.iter().copied() {
            let result = run_mode(
                mode,
                &corpus,
                ops,
                shards,
                args.warmup_iterations,
                args.iterations,
                &runtime,
            )?;
            println!(
                "| {:<31} | {:>9} | {:>12.1} | {:>12.0} | {:>10.2} | {:>14.1} | {:>18} | {:>10} |",
                mode,
                value_size,
                result.ns_per_op(ops),
                result.ops_per_sec(ops),
                result.allocations_per_op(ops),
                result.allocated_bytes_per_op(ops),
                mode.modeled_copy_lower_bound_bytes_per_op(value_size),
                result.checksum
            );
        }
    }

    Ok(())
}

fn run_mode(
    mode: Mode,
    corpus: &Corpus,
    ops: usize,
    shards: usize,
    warmup_iterations: usize,
    iterations: usize,
    runtime: &Runtime,
) -> Result<CaseResult, BoxError> {
    validate_repetitions(ops, iterations, warmup_iterations)?;
    if corpus.len() == 0 {
        return Err("empty corpus".into());
    }
    for _ in 0..warmup_iterations {
        let result = run_once(mode, corpus, ops, shards, runtime)?;
        verify_witness(mode, corpus, ops, &result)?;
    }
    let measured_iterations = iterations;
    let mut best: Option<CaseResult> = None;
    for _ in 0..measured_iterations {
        let result = run_once(mode, corpus, ops, shards, runtime)?;
        verify_witness(mode, corpus, ops, &result)?;
        if best
            .as_ref()
            .is_none_or(|current| result.duration < current.duration)
        {
            best = Some(result);
        }
    }

    best.ok_or_else(|| "no benchmark iterations ran".into())
}

fn run_once(
    mode: Mode,
    corpus: &Corpus,
    ops: usize,
    shards: usize,
    runtime: &Runtime,
) -> Result<CaseResult, BoxError> {
    match mode {
        Mode::EmbeddedSetSlice
        | Mode::EmbeddedSetOwnedBytes
        | Mode::EmbeddedSetCopyBytes
        | Mode::EmbeddedGetCopy
        | Mode::EmbeddedGetValueBytes
        | Mode::EmbeddedGetRef
        | Mode::EmbeddedWithValueBytes => run_embedded_once(mode, corpus, ops, shards),
        Mode::EngineFastSet
        | Mode::EngineFastGet
        | Mode::EngineRespBorrowedSet
        | Mode::EngineRespSpannedSet
        | Mode::EngineRespGet => run_engine_once(mode, corpus, ops, shards, runtime),
    }
}

fn run_embedded_once(
    mode: Mode,
    corpus: &Corpus,
    ops: usize,
    shards: usize,
) -> Result<CaseResult, BoxError> {
    let store = EmbeddedStore::new(shards);
    populate_store(&store, corpus);
    verify_embedded(mode, &store, corpus)?;
    reset_allocations();
    let started = Instant::now();
    let mut checksum = 0_u64;
    for op in 0..ops {
        let index = corpus.index(op);
        let key = corpus.keys[index].as_slice();
        let value = corpus.values[index].as_slice();
        let key_hash = corpus.key_hashes[index];
        checksum = checksum.wrapping_add(match mode {
            Mode::EmbeddedSetSlice => {
                store.set_slice_prehashed(key_hash, key, value, None);
                value.len() as u64
            }
            Mode::EmbeddedSetOwnedBytes => {
                store.set_value_bytes(key, corpus.shared_values[index].clone(), None);
                value.len() as u64
            }
            Mode::EmbeddedSetCopyBytes => {
                store.set_value_bytes(key, SharedBytes::copy_from_slice(value), None);
                value.len() as u64
            }
            Mode::EmbeddedGetCopy => checksum_bytes(store.get(key).ok_or("unexpected GET miss")?),
            Mode::EmbeddedGetValueBytes => checksum_shared(
                store
                    .get_value_bytes(key)
                    .ok_or("unexpected shared GET miss")?,
            ),
            Mode::EmbeddedGetRef => {
                checksum_slice(&store.get_ref(key).ok_or("unexpected borrowed GET miss")?)
            }
            Mode::EmbeddedWithValueBytes => {
                let mut local = 0;
                if !store.with_value_bytes_route_hashed(key_hash, key, |value| {
                    local = checksum_slice(value)
                }) {
                    return Err("unexpected callback GET miss".into());
                }
                local
            }
            _ => return Err("non-embedded mode".into()),
        });
    }
    black_box(checksum);
    let duration = started.elapsed();
    let allocations = allocation_snapshot();
    verify_embedded(mode, &store, corpus)?;
    Ok(CaseResult {
        duration,
        allocations,
        checksum,
    })
}

fn verify_embedded(mode: Mode, store: &EmbeddedStore, corpus: &Corpus) -> Result<(), BoxError> {
    for (index, key) in corpus.keys.iter().enumerate() {
        let expected = &corpus.values[index];
        let matched = match mode {
            Mode::EmbeddedGetValueBytes => store
                .get_value_bytes(key)
                .is_some_and(|v| v.as_ref() == expected.as_slice()),
            Mode::EmbeddedGetRef => store
                .get_ref(key)
                .is_some_and(|v| &*v == expected.as_slice()),
            Mode::EmbeddedWithValueBytes => {
                let mut matched = false;
                let found =
                    store.with_value_bytes_route_hashed(corpus.key_hashes[index], key, |value| {
                        matched = value == expected.as_slice()
                    });
                found && matched
            }
            _ => store.get(key).is_some_and(|v| v == *expected),
        };
        if !matched {
            return Err(format!("{mode} content verification failed for key {index}").into());
        }
    }
    Ok(())
}

fn run_engine_once(
    mode: Mode,
    corpus: &Corpus,
    ops: usize,
    shards: usize,
    runtime: &Runtime,
) -> Result<CaseResult, BoxError> {
    let engine = open_engine(shards)?;
    let work = (|| {
        populate_engine(runtime, &engine, corpus)?;
        verify_engine(mode, runtime, &engine, corpus)?;
        reset_allocations();
        let started = Instant::now();
        let checksum = runtime.block_on(async {
            let mut checksum = 0_u64;
            let mut out = Vec::with_capacity(corpus.values[0].len().saturating_add(64));
            for op in 0..ops {
                let index = corpus.index(op);
                let key = corpus.keys[index].as_slice();
                let value = corpus.values[index].as_slice();
                let key_hash = corpus.key_hashes[index];
                checksum = checksum.wrapping_add(match mode {
                    Mode::EngineFastSet | Mode::EngineFastGet => {
                        let command = if mode == Mode::EngineFastSet {
                            FastCommand::Set { key, value }
                        } else {
                            FastCommand::Get { key }
                        };
                        let response = engine
                            .execute_fast(FastRequest {
                                key_hash: Some(key_hash),
                                route_shard: None,
                                key_tag: None,
                                command,
                            })
                            .await?;
                        checksum_fast_response(response, mode == Mode::EngineFastSet)?
                    }
                    Mode::EngineRespBorrowedSet | Mode::EngineRespGet => {
                        let owner = if mode == Mode::EngineRespGet {
                            &corpus.resp_get_frames[index]
                        } else {
                            &corpus.resp_set_frames[index]
                        };
                        let (frame, consumed) =
                            RespCodec::decode_command(owner)?.ok_or("incomplete RESP command")?;
                        if consumed != owner.len() {
                            return Err("trailing RESP command data".into());
                        }
                        let response = engine
                            .execute_borrowed(BorrowedCommand::from_frame(frame)?)
                            .await?;
                        match (&mode, &response) {
                            (
                                Mode::EngineRespBorrowedSet,
                                shardmap::protocol::Frame::SimpleString(value),
                            ) if value == "OK" => {}
                            (Mode::EngineRespGet, shardmap::protocol::Frame::BlobString(_)) => {}
                            _ => {
                                return Err(
                                    format!("unexpected RESP response: {response:?}").into()
                                );
                            }
                        }
                        out.clear();
                        RespCodec::encode(&response, &mut out);
                        out.len() as u64
                    }
                    Mode::EngineRespSpannedSet => {
                        let owner = corpus.resp_set_frames[index].clone();
                        let (frame, consumed) = RespCodec::decode_command_spans(&owner)?
                            .ok_or("incomplete owned RESP SET")?;
                        if consumed != owner.len() {
                            return Err("trailing owned RESP data".into());
                        }
                        out.clear();
                        engine
                            .execute_resp_owned_into(frame, owner, &mut out)
                            .await?;
                        if out != b"+OK\r\n" {
                            return Err("unexpected owned SET response".into());
                        }
                        out.len() as u64
                    }
                    _ => return Err("non-engine mode".into()),
                });
            }
            Ok::<u64, BoxError>(checksum)
        })?;
        black_box(checksum);
        let duration = started.elapsed();
        let allocations = allocation_snapshot();
        verify_engine(mode, runtime, &engine, corpus)?;
        Ok(CaseResult {
            duration,
            allocations,
            checksum,
        })
    })();
    finish(
        work,
        runtime.block_on(engine.shutdown()).map_err(Into::into),
    )
}

fn verify_engine(
    mode: Mode,
    runtime: &Runtime,
    engine: &EngineHandle,
    corpus: &Corpus,
) -> Result<(), BoxError> {
    runtime.block_on(async {
        for (index, key) in corpus.keys.iter().enumerate() {
            let expected = &corpus.values[index];
            if mode == Mode::EngineFastGet {
                let response = engine
                    .execute_fast(FastRequest {
                        key_hash: Some(corpus.key_hashes[index]),
                        route_shard: None,
                        key_tag: None,
                        command: FastCommand::Get { key },
                    })
                    .await?;
                if response != FastResponse::Value(expected.clone()) {
                    return Err("fast GET content verification failed".into());
                }
            } else {
                let parts: &[&[u8]] = &[b"GET", key];
                let response = engine
                    .execute_borrowed(BorrowedCommand::from_parts(parts)?)
                    .await?;
                if response != shardmap::protocol::Frame::BlobString(expected.clone()) {
                    return Err("engine content verification failed".into());
                }
                if mode == Mode::EngineRespGet {
                    let mut bytes = Vec::new();
                    RespCodec::encode(&response, &mut bytes);
                    let (decoded, consumed) =
                        RespCodec::decode(&bytes)?.ok_or("incomplete encoded GET response")?;
                    if decoded != response || consumed != bytes.len() {
                        return Err("RESP GET encoding verification failed".into());
                    }
                }
            }
        }
        Ok(())
    })
}

fn populate_store(store: &EmbeddedStore, corpus: &Corpus) {
    for index in 0..corpus.len() {
        store.set_slice_prehashed(
            corpus.key_hashes[index],
            corpus.keys[index].as_slice(),
            corpus.values[index].as_slice(),
            None,
        );
    }
}

fn populate_engine(
    runtime: &Runtime,
    engine: &EngineHandle,
    corpus: &Corpus,
) -> Result<(), BoxError> {
    runtime.block_on(async {
        for index in 0..corpus.len() {
            let response = engine
                .execute_fast(FastRequest {
                    key_hash: Some(corpus.key_hashes[index]),
                    route_shard: None,
                    key_tag: None,
                    command: FastCommand::Set {
                        key: corpus.keys[index].as_slice(),
                        value: corpus.values[index].as_slice(),
                    },
                })
                .await?;
            if response != FastResponse::Ok {
                return Err(format!("unexpected populate response: {response:?}").into());
            }
        }
        Ok(())
    })
}

fn open_engine(shards: usize) -> Result<EngineHandle, BoxError> {
    let mut config = ShardCacheConfig {
        shard_count: shards,
        ..ShardCacheConfig::default()
    };
    config.persistence.enabled = false;
    Ok(EngineHandle::open(config)?)
}

fn checksum_fast_response(response: FastResponse, set: bool) -> Result<u64, BoxError> {
    match (set, response) {
        (true, FastResponse::Ok) => Ok(1),
        (false, FastResponse::Value(value)) => Ok(checksum_bytes(value)),
        (_, other) => Err(format!("unexpected fast response: {other:?}").into()),
    }
}

fn verify_witness(
    mode: Mode,
    corpus: &Corpus,
    ops: usize,
    result: &CaseResult,
) -> Result<(), BoxError> {
    let expected = (0..ops).fold(0u64, |sum, op| {
        let value = &corpus.values[corpus.index(op)];
        sum.wrapping_add(match mode {
            Mode::EmbeddedSetSlice | Mode::EmbeddedSetOwnedBytes | Mode::EmbeddedSetCopyBytes => {
                value.len() as u64
            }
            Mode::EngineFastSet => 1,
            Mode::EngineRespBorrowedSet | Mode::EngineRespSpannedSet => 5,
            Mode::EngineRespGet => (value.len() + value.len().to_string().len() + 5) as u64,
            _ => checksum_slice(value),
        })
    });
    if result.duration.is_zero() {
        return Err("timer resolution is too coarse; increase ops".into());
    }
    if result.checksum != expected {
        return Err(format!("{mode} operation witness mismatch").into());
    }
    Ok(())
}

fn validate_args(args: &Args) -> Result<(Vec<usize>, Vec<Mode>), BoxError> {
    let sizes = parse_usize_list(&args.value_sizes)?;
    let modes = parse_modes(&args.modes)?;
    ensure_unique(sizes.iter().copied(), "value sizes")?;
    ensure_unique(modes.iter().map(|mode| mode.label()), "modes")?;
    validate_repetitions(args.ops, args.iterations, args.warmup_iterations)?;
    for &size in &sizes {
        validate_geometry(args.keys, size, args.shards)?;
    }
    Ok((sizes, modes))
}

fn checksum_bytes(value: Vec<u8>) -> u64 {
    checksum_slice(&value)
}

fn checksum_shared(value: SharedBytes) -> u64 {
    checksum_slice(value.as_ref())
}

fn checksum_slice(value: &[u8]) -> u64 {
    value
        .len()
        .wrapping_add(value.first().copied().unwrap_or_default() as usize)
        .wrapping_add(value.last().copied().unwrap_or_default() as usize) as u64
}

fn value_bytes(index: usize, value_size: usize) -> Vec<u8> {
    let seed = index as u8;
    (0..value_size)
        .map(|offset| seed.wrapping_add(offset as u8).wrapping_mul(31))
        .collect()
}

fn encode_resp_set(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + value.len() + 64);
    out.extend_from_slice(b"*3\r\n$3\r\nSET\r\n");
    push_bulk(key, &mut out);
    push_bulk(value, &mut out);
    out
}

fn encode_resp_get(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + 32);
    out.extend_from_slice(b"*2\r\n$3\r\nGET\r\n");
    push_bulk(key, &mut out);
    out
}

fn push_bulk(value: &[u8], out: &mut Vec<u8>) {
    out.push(b'$');
    out.extend_from_slice(value.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(value);
    out.extend_from_slice(b"\r\n");
}

fn parse_usize_list(value: &str) -> Result<Vec<usize>, BoxError> {
    value
        .split(',')
        .map(|item| {
            item.trim()
                .parse::<usize>()
                .map_err(|error| format!("invalid usize `{}`: {error}", item.trim()).into())
        })
        .collect()
}

fn parse_modes(value: &str) -> Result<Vec<Mode>, BoxError> {
    value.split(',').map(Mode::parse).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_MODES: [Mode; 12] = [
        Mode::EmbeddedSetSlice,
        Mode::EmbeddedSetOwnedBytes,
        Mode::EmbeddedSetCopyBytes,
        Mode::EmbeddedGetCopy,
        Mode::EmbeddedGetValueBytes,
        Mode::EmbeddedGetRef,
        Mode::EmbeddedWithValueBytes,
        Mode::EngineFastSet,
        Mode::EngineFastGet,
        Mode::EngineRespBorrowedSet,
        Mode::EngineRespSpannedSet,
        Mode::EngineRespGet,
    ];

    #[test]
    fn bounded_smoke_runs_every_public_mode_with_empty_and_binary_values() {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");
        for value_size in [0, 17, 32 * 1024 + 1] {
            let corpus = Corpus::build(3, value_size);
            for mode in ALL_MODES {
                let result = run_mode(mode, &corpus, 7, 2, 0, 1, &runtime)
                    .unwrap_or_else(|error| panic!("{mode} at {value_size} bytes: {error}"));
                verify_witness(mode, &corpus, 7, &result)
                    .unwrap_or_else(|error| panic!("{mode} at {value_size} bytes: {error}"));
            }
        }
    }

    #[test]
    fn argument_validation_rejects_empty_duplicate_and_overflowing_inputs() {
        let args = |value_sizes: &str, modes: &str, ops: usize, keys: usize, shards: usize| Args {
            value_sizes: value_sizes.to_owned(),
            ops,
            keys,
            shards,
            iterations: 1,
            warmup_iterations: 0,
            modes: modes.to_owned(),
        };
        assert!(validate_args(&args("", "embedded-get-copy", 1, 1, 1)).is_err());
        assert!(validate_args(&args("1,1", "embedded-get-copy", 1, 1, 1)).is_err());
        assert!(validate_args(&args("1", "", 1, 1, 1)).is_err());
        assert!(validate_args(&args("1", "embedded-get-copy,embedded-get-copy", 1, 1, 1)).is_err());
        assert!(validate_args(&args("1", "embedded-get-copy", 0, 1, 1)).is_err());
        assert!(validate_args(&args("1", "embedded-get-copy", 1, 0, 1)).is_err());
        assert!(validate_args(&args("1", "embedded-get-copy", 1, 1, 3)).is_err());
        assert!(
            validate_args(&args(&usize::MAX.to_string(), "embedded-get-copy", 1, 1, 1)).is_err()
        );
    }

    #[test]
    fn full_value_oracle_detects_changes_that_preserve_the_thin_witness() {
        let corpus = Corpus::build(1, 17);
        let store = EmbeddedStore::new(2);
        let mut changed = corpus.values[0].clone();
        changed[8] ^= 0x80;
        store.set_slice_prehashed(corpus.key_hashes[0], &corpus.keys[0], &changed, None);
        assert_eq!(checksum_slice(&changed), checksum_slice(&corpus.values[0]));
        assert!(verify_embedded(Mode::EmbeddedGetCopy, &store, &corpus).is_err());
    }

    #[test]
    fn strict_response_and_iteration_witness_checks_reject_bad_results() {
        assert_eq!(checksum_fast_response(FastResponse::Ok, true).unwrap(), 1);
        assert_eq!(
            checksum_fast_response(FastResponse::Value(b"value".to_vec()), false).unwrap(),
            checksum_slice(b"value")
        );
        assert!(checksum_fast_response(FastResponse::Null, false).is_err());
        assert!(checksum_fast_response(FastResponse::Ok, false).is_err());
        let corpus = Corpus::build(1, 0);
        let result = CaseResult {
            duration: Duration::from_nanos(1),
            allocations: AllocationSnapshot { count: 0, bytes: 0 },
            checksum: 99,
        };
        assert!(verify_witness(Mode::EmbeddedGetCopy, &corpus, 1, &result).is_err());
    }
}
