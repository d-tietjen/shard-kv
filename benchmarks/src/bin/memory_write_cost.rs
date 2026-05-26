use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::fmt;
use std::fs::OpenOptions;
use std::hint::black_box;
use std::io::Write;
use std::ptr::NonNull;
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::Parser;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "4096,16384,65536,1048576")]
    value_sizes: String,
    #[arg(
        long,
        default_value = "slice-copy,bytes-copy,vec-bytes,bytes-reuse,aligned-copy,nt-sse2,nt-avx2"
    )]
    modes: String,
    #[arg(long, default_value_t = 1)]
    warmup_seconds: u64,
    #[arg(long, default_value_t = 5)]
    duration_seconds: u64,
    #[arg(long, default_value_t = 8)]
    pool_len: usize,
    #[arg(long, default_value_t = 1)]
    threads: usize,
    #[arg(long)]
    macos_qos: bool,
    #[arg(long)]
    csv: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    ReadScan,
    ReadScanUnrolled,
    ReadScanNeon,
    WriteFill,
    SliceCopy,
    BytesCopy,
    VecBytes,
    BytesReuse,
    AlignedCopy,
    NtSse2,
    NtAvx2,
}

impl Mode {
    fn parse(value: &str) -> Result<Self, BoxError> {
        match value.trim() {
            "read-scan" => Ok(Self::ReadScan),
            "read-scan-unrolled" => Ok(Self::ReadScanUnrolled),
            "read-scan-neon" => Ok(Self::ReadScanNeon),
            "write-fill" => Ok(Self::WriteFill),
            "slice-copy" => Ok(Self::SliceCopy),
            "bytes-copy" => Ok(Self::BytesCopy),
            "vec-bytes" => Ok(Self::VecBytes),
            "bytes-reuse" => Ok(Self::BytesReuse),
            "aligned-copy" => Ok(Self::AlignedCopy),
            "nt-sse2" => Ok(Self::NtSse2),
            "nt-avx2" => Ok(Self::NtAvx2),
            other => Err(format!("unknown mode `{other}`").into()),
        }
    }

    fn is_available(self) -> bool {
        match self {
            Self::ReadScanNeon => cfg!(target_arch = "aarch64"),
            Self::NtSse2 => cfg!(target_arch = "x86_64"),
            Self::NtAvx2 => cfg!(target_arch = "x86_64") && is_x86_feature_detected_runtime("avx2"),
            _ => true,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::ReadScan => "read-scan",
            Self::ReadScanUnrolled => "read-scan-unrolled",
            Self::ReadScanNeon => "read-scan-neon",
            Self::WriteFill => "write-fill",
            Self::SliceCopy => "slice-copy",
            Self::BytesCopy => "bytes-copy",
            Self::VecBytes => "vec-bytes",
            Self::BytesReuse => "bytes-reuse",
            Self::AlignedCopy => "aligned-copy",
            Self::NtSse2 => "nt-sse2",
            Self::NtAvx2 => "nt-avx2",
        }
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

#[derive(Debug)]
struct RunResult {
    mode: Mode,
    value_size: usize,
    threads: usize,
    duration: Duration,
    ops: u64,
    checksum: u64,
}

impl RunResult {
    fn bytes(&self) -> u128 {
        self.ops as u128 * self.value_size as u128
    }

    fn ops_per_sec(&self) -> f64 {
        self.ops as f64 / self.duration.as_secs_f64()
    }

    fn gb_per_sec(&self) -> f64 {
        self.bytes() as f64 / self.duration.as_secs_f64() / 1_000_000_000.0
    }
}

fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    let value_sizes = parse_usize_list(&args.value_sizes)?;
    let modes = parse_modes(&args.modes)?;
    let pool_len = args.pool_len.max(1);
    let threads = args.threads.max(1);
    let warmup = Duration::from_secs(args.warmup_seconds);
    let duration = Duration::from_secs(args.duration_seconds);

    let mut csv = match args.csv.as_ref() {
        Some(path) => {
            let existed = std::path::Path::new(path).exists();
            let mut file = OpenOptions::new().create(true).append(true).open(path)?;
            if !existed {
                writeln!(
                    file,
                    "mode,value_size,threads,duration_s,ops,bytes,ops_per_sec,gb_per_sec,checksum"
                )?;
            }
            Some(file)
        }
        None => None,
    };

    println!(
        "memory-write-cost: sizes={:?} modes={} warmup={}s duration={}s pool_len={} threads={}",
        value_sizes, args.modes, args.warmup_seconds, args.duration_seconds, pool_len, threads
    );
    println!(
        "| {:<14} | {:>10} | {:>7} | {:>14} | {:>10} | {:>10} | {:>10} |",
        "mode", "size", "threads", "ops/sec", "GB/s", "ops", "checksum"
    );
    println!(
        "| {:-<14} | {:-<10} | {:-<7} | {:-<14} | {:-<10} | {:-<10} | {:-<10} |",
        "", "", "", "", "", "", ""
    );

    for value_size in value_sizes {
        for mode in modes.iter().copied() {
            if !mode.is_available() {
                println!(
                    "| {:<14} | {:>10} | {:>7} | {:>14} | {:>10} | {:>10} | {:>10} |",
                    mode, value_size, threads, "unsupported", "-", "-", "-"
                );
                continue;
            }
            let _ = run_once(mode, value_size, pool_len, threads, args.macos_qos, warmup)?;
            let result = run_once(
                mode,
                value_size,
                pool_len,
                threads,
                args.macos_qos,
                duration,
            )?;
            println!(
                "| {:<14} | {:>10} | {:>7} | {:>14.0} | {:>10.3} | {:>10} | {:>10} |",
                result.mode,
                result.value_size,
                result.threads,
                result.ops_per_sec(),
                result.gb_per_sec(),
                result.ops,
                result.checksum
            );
            if let Some(file) = csv.as_mut() {
                writeln!(
                    file,
                    "{},{},{},{:.6},{},{},{:.3},{:.6},{}",
                    result.mode,
                    result.value_size,
                    result.threads,
                    result.duration.as_secs_f64(),
                    result.ops,
                    result.bytes(),
                    result.ops_per_sec(),
                    result.gb_per_sec(),
                    result.checksum
                )?;
            }
        }
    }
    Ok(())
}

fn run_once(
    mode: Mode,
    value_size: usize,
    pool_len: usize,
    threads: usize,
    macos_qos: bool,
    duration: Duration,
) -> Result<RunResult, BoxError> {
    let started = Instant::now();
    let (ops, checksum) = run_threaded(mode, value_size, pool_len, threads, macos_qos, duration)?;
    Ok(RunResult {
        mode,
        value_size,
        threads,
        duration: started.elapsed(),
        ops,
        checksum: black_box(checksum),
    })
}

fn run_threaded(
    mode: Mode,
    value_size: usize,
    pool_len: usize,
    threads: usize,
    macos_qos: bool,
    duration: Duration,
) -> Result<(u64, u64), BoxError> {
    if threads <= 1 {
        set_benchmark_thread_priority(macos_qos);
        return run_mode_once(mode, value_size, pool_len, duration);
    }

    let mut handles = Vec::with_capacity(threads);
    for _ in 0..threads {
        handles.push(thread::spawn(move || {
            set_benchmark_thread_priority(macos_qos);
            run_mode_once(mode, value_size, pool_len, duration).map_err(|error| error.to_string())
        }));
    }

    let mut total_ops = 0u64;
    let mut checksum = 0u64;
    for handle in handles {
        let (ops, worker_checksum) = handle
            .join()
            .map_err(|_| "memory benchmark worker panicked")?
            .map_err(|error| -> BoxError { error.into() })?;
        total_ops = total_ops.wrapping_add(ops);
        checksum = checksum.wrapping_add(worker_checksum);
    }
    Ok((total_ops, checksum))
}

fn set_benchmark_thread_priority(macos_qos: bool) {
    #[cfg(target_os = "macos")]
    {
        if macos_qos {
            unsafe {
                libc::pthread_set_qos_class_self_np(
                    libc::qos_class_t::QOS_CLASS_USER_INTERACTIVE,
                    0,
                );
            }
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = macos_qos;
    }
}

fn run_mode_once(
    mode: Mode,
    value_size: usize,
    pool_len: usize,
    duration: Duration,
) -> Result<(u64, u64), BoxError> {
    let (ops, checksum) = match mode {
        Mode::ReadScan => run_read_scan(value_size, pool_len, duration),
        Mode::ReadScanUnrolled => run_read_scan_unrolled(value_size, pool_len, duration),
        Mode::ReadScanNeon => run_read_scan_neon(value_size, pool_len, duration)?,
        Mode::WriteFill => run_write_fill(value_size, pool_len, duration),
        Mode::SliceCopy => run_slice_copy(value_size, pool_len, duration),
        Mode::BytesCopy => run_bytes_copy(value_size, pool_len, duration),
        Mode::VecBytes => run_vec_bytes(value_size, pool_len, duration),
        Mode::BytesReuse => run_bytes_reuse(value_size, pool_len, duration),
        Mode::AlignedCopy => run_aligned_copy(value_size, pool_len, duration)?,
        Mode::NtSse2 => run_nt_sse2(value_size, pool_len, duration)?,
        Mode::NtAvx2 => run_nt_avx2(value_size, pool_len, duration)?,
    };
    Ok((ops, checksum))
}

fn run_read_scan(value_size: usize, pool_len: usize, duration: Duration) -> (u64, u64) {
    let pool = (0..pool_len)
        .map(|index| source_value(value_size, index as u8))
        .collect::<Vec<_>>();
    let mut checksum = 0u64;
    let ops = run_loop(duration, |op| {
        let index = op as usize % pool_len;
        checksum = checksum.wrapping_add(read_sum(&pool[index]));
    });
    (ops, checksum)
}

fn run_read_scan_unrolled(value_size: usize, pool_len: usize, duration: Duration) -> (u64, u64) {
    let pool = (0..pool_len)
        .map(|index| source_value(value_size, index as u8))
        .collect::<Vec<_>>();
    let mut checksum = 0u64;
    let ops = run_loop(duration, |op| {
        let index = op as usize % pool_len;
        checksum = checksum.wrapping_add(read_sum_unrolled(&pool[index]));
    });
    (ops, checksum)
}

fn run_read_scan_neon(
    value_size: usize,
    pool_len: usize,
    duration: Duration,
) -> Result<(u64, u64), BoxError> {
    if !Mode::ReadScanNeon.is_available() {
        return Err("read-scan-neon is only available on aarch64".into());
    }
    let pool = (0..pool_len)
        .map(|index| source_value(value_size, index as u8))
        .collect::<Vec<_>>();
    let mut checksum = 0u64;
    let ops = run_loop(duration, |op| {
        let index = op as usize % pool_len;
        checksum = checksum.wrapping_add(read_sum_neon(&pool[index]));
    });
    Ok((ops, checksum))
}

fn run_write_fill(value_size: usize, pool_len: usize, duration: Duration) -> (u64, u64) {
    let mut pool = vec![vec![0u8; value_size]; pool_len];
    let ops = run_loop(duration, |op| {
        let index = op as usize % pool_len;
        pool[index].fill(op as u8);
    });
    (ops, checksum_slices(pool.iter().map(Vec::as_slice)))
}

fn run_slice_copy(value_size: usize, pool_len: usize, duration: Duration) -> (u64, u64) {
    let sources = source_values(value_size);
    let mut pool = vec![vec![0u8; value_size]; pool_len];
    let ops = run_loop(duration, |op| {
        let index = op as usize % pool_len;
        pool[index].copy_from_slice(&sources[op as usize & 1]);
    });
    (ops, checksum_slices(pool.iter().map(Vec::as_slice)))
}

fn run_bytes_copy(value_size: usize, pool_len: usize, duration: Duration) -> (u64, u64) {
    let sources = source_values(value_size);
    let mut pool = vec![Bytes::new(); pool_len];
    let ops = run_loop(duration, |op| {
        let index = op as usize % pool_len;
        pool[index] = Bytes::copy_from_slice(&sources[op as usize & 1]);
    });
    (ops, checksum_slices(pool.iter().map(Bytes::as_ref)))
}

fn run_vec_bytes(value_size: usize, pool_len: usize, duration: Duration) -> (u64, u64) {
    let sources = source_values(value_size);
    let mut pool = vec![Bytes::new(); pool_len];
    let ops = run_loop(duration, |op| {
        let index = op as usize % pool_len;
        pool[index] = Bytes::from(sources[op as usize & 1].to_vec());
    });
    (ops, checksum_slices(pool.iter().map(Bytes::as_ref)))
}

fn run_bytes_reuse(value_size: usize, pool_len: usize, duration: Duration) -> (u64, u64) {
    let sources = source_values(value_size);
    let mut pool = (0..pool_len)
        .map(|_| Bytes::from(vec![0u8; value_size]))
        .collect::<Vec<_>>();
    let ops = run_loop(duration, |op| {
        let index = op as usize % pool_len;
        let current = std::mem::take(&mut pool[index]);
        pool[index] = match current.try_into_mut() {
            Ok(mut writable) => {
                writable[..].copy_from_slice(&sources[op as usize & 1]);
                writable.freeze()
            }
            Err(_current) => Bytes::from(sources[op as usize & 1].to_vec()),
        };
    });
    (ops, checksum_slices(pool.iter().map(Bytes::as_ref)))
}

fn run_aligned_copy(
    value_size: usize,
    pool_len: usize,
    duration: Duration,
) -> Result<(u64, u64), BoxError> {
    let sources = aligned_sources(value_size)?;
    let mut pool = aligned_pool(value_size, pool_len)?;
    let ops = run_loop(duration, |op| {
        let index = op as usize % pool_len;
        pool[index]
            .as_mut_slice()
            .copy_from_slice(sources[op as usize & 1].as_slice());
    });
    Ok((
        ops,
        checksum_slices(pool.iter().map(AlignedBuffer::as_slice)),
    ))
}

fn run_nt_sse2(
    value_size: usize,
    pool_len: usize,
    duration: Duration,
) -> Result<(u64, u64), BoxError> {
    if !Mode::NtSse2.is_available() {
        return Err("nt-sse2 is only available on x86_64".into());
    }
    let sources = aligned_sources(value_size)?;
    let mut pool = aligned_pool(value_size, pool_len)?;
    let ops = run_loop(duration, |op| {
        let index = op as usize % pool_len;
        copy_non_temporal_sse2(
            pool[index].as_mut_ptr(),
            sources[op as usize & 1].as_ptr(),
            value_size,
        );
    });
    Ok((
        ops,
        checksum_slices(pool.iter().map(AlignedBuffer::as_slice)),
    ))
}

fn run_nt_avx2(
    value_size: usize,
    pool_len: usize,
    duration: Duration,
) -> Result<(u64, u64), BoxError> {
    if !Mode::NtAvx2.is_available() {
        return Err("nt-avx2 is only available on x86_64 with avx2".into());
    }
    let sources = aligned_sources(value_size)?;
    let mut pool = aligned_pool(value_size, pool_len)?;
    let ops = run_loop(duration, |op| {
        let index = op as usize % pool_len;
        copy_non_temporal_avx2(
            pool[index].as_mut_ptr(),
            sources[op as usize & 1].as_ptr(),
            value_size,
        );
    });
    Ok((
        ops,
        checksum_slices(pool.iter().map(AlignedBuffer::as_slice)),
    ))
}

fn run_loop(mut duration: Duration, mut op: impl FnMut(u64)) -> u64 {
    const CHECK_EVERY: u64 = 1024;
    if duration.is_zero() {
        duration = Duration::from_nanos(1);
    }
    let started = Instant::now();
    let mut ops = 0u64;
    loop {
        for _ in 0..CHECK_EVERY {
            op(ops);
            ops = ops.wrapping_add(1);
        }
        if started.elapsed() >= duration {
            return ops;
        }
    }
}

fn source_values(value_size: usize) -> [Vec<u8>; 2] {
    [
        source_value(value_size, 0x51),
        source_value(value_size, 0xa7),
    ]
}

fn source_value(value_size: usize, seed: u8) -> Vec<u8> {
    let mut value = vec![0u8; value_size];
    for (index, byte) in value.iter_mut().enumerate() {
        *byte = seed
            .wrapping_add(index as u8)
            .rotate_left((index & 7) as u32);
    }
    value
}

fn aligned_sources(value_size: usize) -> Result<[AlignedBuffer; 2], BoxError> {
    let mut first = AlignedBuffer::new(value_size)?;
    let mut second = AlignedBuffer::new(value_size)?;
    first
        .as_mut_slice()
        .copy_from_slice(&source_value(value_size, 0x51));
    second
        .as_mut_slice()
        .copy_from_slice(&source_value(value_size, 0xa7));
    Ok([first, second])
}

fn aligned_pool(value_size: usize, pool_len: usize) -> Result<Vec<AlignedBuffer>, BoxError> {
    (0..pool_len)
        .map(|_| AlignedBuffer::new(value_size))
        .collect::<Result<Vec<_>, _>>()
}

fn checksum_slices<'a>(slices: impl Iterator<Item = &'a [u8]>) -> u64 {
    let mut checksum = 0u64;
    for slice in slices {
        if slice.is_empty() {
            continue;
        }
        checksum = checksum.wrapping_add(slice[0] as u64);
        checksum = checksum.wrapping_add(slice[slice.len() / 2] as u64);
        checksum = checksum.wrapping_add(slice[slice.len() - 1] as u64);
    }
    checksum
}

fn read_sum(slice: &[u8]) -> u64 {
    let mut checksum = 0u64;
    let mut chunks = slice.chunks_exact(std::mem::size_of::<u64>());
    for chunk in &mut chunks {
        checksum = checksum.wrapping_add(u64::from_ne_bytes(
            chunk.try_into().expect("chunk has u64 width"),
        ));
    }
    for byte in chunks.remainder() {
        checksum = checksum.wrapping_add(*byte as u64);
    }
    black_box(checksum)
}

fn read_sum_unrolled(slice: &[u8]) -> u64 {
    const WORDS: usize = 8;
    const BLOCK: usize = WORDS * std::mem::size_of::<u64>();

    let mut acc = [0u64; WORDS];
    let mut chunks = slice.chunks_exact(BLOCK);
    for chunk in &mut chunks {
        let ptr = chunk.as_ptr();
        for (word, slot) in acc.iter_mut().enumerate() {
            // SAFETY: `chunks_exact(BLOCK)` guarantees `word * 8 + 8 <= chunk.len()`.
            let value = unsafe { std::ptr::read_unaligned(ptr.add(word * 8).cast::<u64>()) };
            *slot = slot.wrapping_add(value);
        }
    }

    let mut checksum = acc.into_iter().fold(0u64, u64::wrapping_add);
    let mut tail = chunks.remainder().chunks_exact(std::mem::size_of::<u64>());
    for chunk in &mut tail {
        checksum = checksum.wrapping_add(u64::from_ne_bytes(
            chunk.try_into().expect("chunk has u64 width"),
        ));
    }
    for byte in tail.remainder() {
        checksum = checksum.wrapping_add(*byte as u64);
    }
    black_box(checksum)
}

fn read_sum_neon(slice: &[u8]) -> u64 {
    #[cfg(target_arch = "aarch64")]
    unsafe {
        read_sum_neon_aarch64(slice)
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        let _ = slice;
        unreachable!("read-scan-neon is only available on aarch64");
    }
}

#[cfg(target_arch = "aarch64")]
unsafe fn read_sum_neon_aarch64(slice: &[u8]) -> u64 {
    use std::arch::aarch64::{vaddq_u64, vdupq_n_u64, vld1q_u64, vst1q_u64};

    const LANES: usize = 2 * std::mem::size_of::<u64>();
    const VECTORS: usize = 8;
    const BLOCK: usize = LANES * VECTORS;

    let mut acc0 = unsafe { vdupq_n_u64(0) };
    let mut acc1 = unsafe { vdupq_n_u64(0) };
    let mut acc2 = unsafe { vdupq_n_u64(0) };
    let mut acc3 = unsafe { vdupq_n_u64(0) };
    let mut acc4 = unsafe { vdupq_n_u64(0) };
    let mut acc5 = unsafe { vdupq_n_u64(0) };
    let mut acc6 = unsafe { vdupq_n_u64(0) };
    let mut acc7 = unsafe { vdupq_n_u64(0) };

    let ptr = slice.as_ptr();
    let mut offset = 0usize;
    while offset + BLOCK <= slice.len() {
        // SAFETY: the loop bounds guarantee each 16-byte vector load is inside `slice`.
        acc0 = unsafe { vaddq_u64(acc0, vld1q_u64(ptr.add(offset).cast::<u64>())) };
        acc1 = unsafe { vaddq_u64(acc1, vld1q_u64(ptr.add(offset + LANES).cast::<u64>())) };
        acc2 = unsafe { vaddq_u64(acc2, vld1q_u64(ptr.add(offset + LANES * 2).cast::<u64>())) };
        acc3 = unsafe { vaddq_u64(acc3, vld1q_u64(ptr.add(offset + LANES * 3).cast::<u64>())) };
        acc4 = unsafe { vaddq_u64(acc4, vld1q_u64(ptr.add(offset + LANES * 4).cast::<u64>())) };
        acc5 = unsafe { vaddq_u64(acc5, vld1q_u64(ptr.add(offset + LANES * 5).cast::<u64>())) };
        acc6 = unsafe { vaddq_u64(acc6, vld1q_u64(ptr.add(offset + LANES * 6).cast::<u64>())) };
        acc7 = unsafe { vaddq_u64(acc7, vld1q_u64(ptr.add(offset + LANES * 7).cast::<u64>())) };
        offset += BLOCK;
    }

    acc0 = unsafe { vaddq_u64(acc0, acc1) };
    acc2 = unsafe { vaddq_u64(acc2, acc3) };
    acc4 = unsafe { vaddq_u64(acc4, acc5) };
    acc6 = unsafe { vaddq_u64(acc6, acc7) };
    acc0 = unsafe { vaddq_u64(acc0, acc2) };
    acc4 = unsafe { vaddq_u64(acc4, acc6) };
    acc0 = unsafe { vaddq_u64(acc0, acc4) };

    let mut reduced = [0u64; 2];
    // SAFETY: `reduced` has one full NEON vector worth of initialized storage.
    unsafe { vst1q_u64(reduced.as_mut_ptr(), acc0) };
    let mut checksum = reduced.into_iter().fold(0u64, u64::wrapping_add);
    checksum = checksum.wrapping_add(read_sum_unrolled(&slice[offset..]));
    black_box(checksum)
}

fn parse_usize_list(values: &str) -> Result<Vec<usize>, BoxError> {
    values
        .split(',')
        .map(|value| {
            value
                .trim()
                .parse::<usize>()
                .map_err(|error| format!("invalid usize `{value}`: {error}").into())
        })
        .collect()
}

fn parse_modes(values: &str) -> Result<Vec<Mode>, BoxError> {
    values.split(',').map(Mode::parse).collect()
}

fn is_x86_feature_detected_runtime(feature: &str) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        match feature {
            "avx2" => std::is_x86_feature_detected!("avx2"),
            _ => false,
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let _ = feature;
        false
    }
}

struct AlignedBuffer {
    ptr: NonNull<u8>,
    len: usize,
    layout: Layout,
}

impl AlignedBuffer {
    fn new(len: usize) -> Result<Self, BoxError> {
        let layout = Layout::from_size_align(len.max(1), 64)?;
        let raw = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(raw).ok_or("aligned allocation failed")?;
        Ok(Self { ptr, len, layout })
    }

    fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

fn copy_non_temporal_sse2(dst: *mut u8, src: *const u8, len: usize) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        copy_non_temporal_sse2_x86_64(dst, src, len);
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let _ = (dst, src, len);
        unreachable!("nt-sse2 is only available on x86_64");
    }
}

fn copy_non_temporal_avx2(dst: *mut u8, src: *const u8, len: usize) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        copy_non_temporal_avx2_x86_64(dst, src, len);
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let _ = (dst, src, len);
        unreachable!("nt-avx2 is only available on x86_64");
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn copy_non_temporal_sse2_x86_64(dst: *mut u8, src: *const u8, len: usize) {
    use std::arch::x86_64::{__m128i, _mm_loadu_si128, _mm_sfence, _mm_stream_si128};

    let mut offset = 0usize;
    while offset + 16 <= len {
        let chunk = unsafe { _mm_loadu_si128(src.add(offset).cast::<__m128i>()) };
        unsafe { _mm_stream_si128(dst.add(offset).cast::<__m128i>(), chunk) };
        offset += 16;
    }
    if offset < len {
        unsafe { std::ptr::copy_nonoverlapping(src.add(offset), dst.add(offset), len - offset) };
    }
    _mm_sfence();
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn copy_non_temporal_avx2_x86_64(dst: *mut u8, src: *const u8, len: usize) {
    use std::arch::x86_64::{__m256i, _mm_sfence, _mm256_loadu_si256, _mm256_stream_si256};

    let mut offset = 0usize;
    while offset + 32 <= len {
        let chunk = unsafe { _mm256_loadu_si256(src.add(offset).cast::<__m256i>()) };
        unsafe { _mm256_stream_si256(dst.add(offset).cast::<__m256i>(), chunk) };
        offset += 32;
    }
    if offset < len {
        unsafe { std::ptr::copy_nonoverlapping(src.add(offset), dst.add(offset), len - offset) };
    }
    _mm_sfence();
}
