use std::time::Duration;

/// Process CPU time consumed (user + system) measured via `getrusage`.
/// Returns `Duration` since the call. Suitable for in-process measurement
/// of embedded backends and the bench process itself.
pub fn process_cpu_time() -> Duration {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    if rc != 0 {
        return Duration::ZERO;
    }
    let u = Duration::new(
        ru.ru_utime.tv_sec as u64,
        (ru.ru_utime.tv_usec as u32) * 1_000,
    );
    let s = Duration::new(
        ru.ru_stime.tv_sec as u64,
        (ru.ru_stime.tv_usec as u32) * 1_000,
    );
    u + s
}

/// Linux-only: read user + system jiffies for an external pid from
/// `/proc/<pid>/stat`. Returns `None` on macOS or if the read fails.
#[cfg(target_os = "linux")]
pub fn external_cpu_time(pid: u32) -> Option<Duration> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // Skip "(comm)" which may contain spaces.
    let close = stat.rfind(')')?;
    let rest = &stat[close + 1..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // After ')' field 3 is state. utime is overall field 14, so rest index 11.
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as u64;
    if hz == 0 {
        return None;
    }
    let total_ns = (utime + stime).saturating_mul(1_000_000_000) / hz;
    Some(Duration::from_nanos(total_ns))
}

#[cfg(not(target_os = "linux"))]
pub fn external_cpu_time(_pid: u32) -> Option<Duration> {
    None
}

/// Convert a CPU duration consumed over a wall-clock window to vCPU
/// (where 1.0 == one fully-busy core for that window).
pub fn vcpu(consumed: Duration, wall: Duration) -> f64 {
    if wall.is_zero() {
        0.0
    } else {
        consumed.as_secs_f64() / wall.as_secs_f64()
    }
}
