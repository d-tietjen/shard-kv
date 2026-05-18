#[derive(Debug, Clone, Copy)]
pub struct FastClock {
    numer: u64,
    denom: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct FastInstant(u64);

impl FastClock {
    pub fn new() -> Self {
        let (numer, denom) = platform_scale();
        Self { numer, denom }
    }

    #[inline(always)]
    pub fn now(&self) -> FastInstant {
        FastInstant(platform_ticks())
    }

    #[inline(always)]
    pub fn elapsed_ns(&self, start: FastInstant) -> u64 {
        self.ticks_to_ns(platform_ticks().saturating_sub(start.0))
    }

    #[inline(always)]
    fn ticks_to_ns(&self, ticks: u64) -> u64 {
        ((ticks as u128 * self.numer as u128) / self.denom as u128) as u64
    }
}

impl Default for FastClock {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns a cached Unix timestamp in milliseconds for benchmark TTL paths.
///
/// TTL workloads only need millisecond granularity. Keeping this as an atomic
/// load lets TTL backends compare storage behavior without each operation
/// paying for a full wall-clock read.
#[inline(always)]
pub fn cached_epoch_millis() -> u64 {
    TTL_CLOCK_START.call_once(start_ttl_clock);
    if TTL_CLOCK_RUNNING.load(std::sync::atomic::Ordering::Relaxed) {
        TTL_CLOCK_MS.load(std::sync::atomic::Ordering::Relaxed)
    } else {
        exact_epoch_millis()
    }
}

static TTL_CLOCK_START: std::sync::Once = std::sync::Once::new();
static TTL_CLOCK_RUNNING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static TTL_CLOCK_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn start_ttl_clock() {
    TTL_CLOCK_MS.store(exact_epoch_millis(), std::sync::atomic::Ordering::Relaxed);
    match std::thread::Builder::new()
        .name("fast-cache-bench-ttl-clock".to_string())
        .spawn(|| {
            loop {
                TTL_CLOCK_MS.store(exact_epoch_millis(), std::sync::atomic::Ordering::Relaxed);
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }) {
        Ok(_handle) => TTL_CLOCK_RUNNING.store(true, std::sync::atomic::Ordering::Relaxed),
        Err(_error) => {}
    }
}

#[inline(always)]
fn exact_epoch_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(target_os = "macos")]
#[repr(C)]
struct MachTimebaseInfo {
    numer: u32,
    denom: u32,
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn mach_absolute_time() -> u64;
    fn mach_timebase_info(info: *mut MachTimebaseInfo) -> libc::c_int;
}

#[cfg(target_os = "macos")]
fn platform_scale() -> (u64, u64) {
    let mut info = MachTimebaseInfo { numer: 1, denom: 1 };
    // SAFETY: `info` is a valid writable pointer for the duration of the call.
    let rc = unsafe { mach_timebase_info(&mut info) };
    if rc == 0 && info.denom != 0 {
        (info.numer as u64, info.denom as u64)
    } else {
        (1, 1)
    }
}

#[cfg(target_os = "macos")]
#[inline(always)]
fn platform_ticks() -> u64 {
    // SAFETY: `mach_absolute_time` has no preconditions and returns a
    // monotonic hardware-backed tick count.
    unsafe { mach_absolute_time() }
}

#[cfg(target_os = "linux")]
fn platform_scale() -> (u64, u64) {
    (1, 1)
}

#[cfg(target_os = "linux")]
#[inline(always)]
fn platform_ticks() -> u64 {
    clock_gettime_ns(libc::CLOCK_MONOTONIC_RAW)
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn platform_scale() -> (u64, u64) {
    (1, 1)
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
#[inline(always)]
fn platform_ticks() -> u64 {
    clock_gettime_ns(libc::CLOCK_MONOTONIC)
}

#[cfg(all(unix, not(target_os = "macos")))]
#[inline(always)]
fn clock_gettime_ns(clock_id: libc::clockid_t) -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid writable pointer for `clock_gettime`.
    let rc = unsafe { libc::clock_gettime(clock_id, &mut ts) };
    debug_assert_eq!(rc, 0);
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

#[cfg(not(unix))]
fn platform_scale() -> (u64, u64) {
    (1, 1)
}

#[cfg(not(unix))]
#[inline(always)]
fn platform_ticks() -> u64 {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    START
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_nanos() as u64
}
