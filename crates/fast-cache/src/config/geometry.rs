use std::fs;
use std::process::Command;

use super::CacheGeometry;

const HOT_SLOT_BYTES: usize = 64;
const HOT_L1_BUDGET_NUMERATOR: usize = 3;
const HOT_L1_BUDGET_DENOMINATOR: usize = 5;
const HOT_TARGET_LOAD_NUMERATOR: usize = 4;
const HOT_TARGET_LOAD_DENOMINATOR: usize = 5;

pub(super) struct DefaultShardCount;

pub(super) struct HotTierCapacity;

pub(super) struct CacheGeometryDetector;

pub(super) struct CacheSizeParser<'a> {
    raw: &'a str,
}

struct NearestPowerOfTwo;

enum CacheGeometryProbe {
    Linux,
    MacOs,
    Fallback,
}

enum CacheSizeSuffix {
    Bytes,
    KiB,
    MiB,
}

enum SysctlOutput {
    Ready(Vec<u8>),
    Missing,
}

impl DefaultShardCount {
    pub(super) fn current() -> usize {
        std::thread::available_parallelism()
            .map_or(1, |count| count.get())
            .next_power_of_two()
    }
}

impl HotTierCapacity {
    pub(super) fn from_l1(l1d_bytes: usize) -> usize {
        let target_slots = usize::max(
            4,
            (l1d_bytes.saturating_mul(HOT_L1_BUDGET_NUMERATOR) / HOT_L1_BUDGET_DENOMINATOR)
                / HOT_SLOT_BYTES,
        );
        let slot_count = NearestPowerOfTwo::from(target_slots).clamp(256, 2_048);
        (slot_count.saturating_mul(HOT_TARGET_LOAD_NUMERATOR) / HOT_TARGET_LOAD_DENOMINATOR)
            .clamp(128, 16_384)
    }
}

impl CacheGeometryDetector {
    pub(super) fn detect() -> CacheGeometry {
        Self::probe(CacheGeometryProbe::Linux)
    }

    fn probe(probe: CacheGeometryProbe) -> CacheGeometry {
        match probe {
            CacheGeometryProbe::Linux => match Self::detect_linux() {
                Some(geometry) => geometry,
                None => Self::probe(CacheGeometryProbe::MacOs),
            },
            CacheGeometryProbe::MacOs => match Self::detect_macos() {
                Some(geometry) => geometry,
                None => Self::probe(CacheGeometryProbe::Fallback),
            },
            CacheGeometryProbe::Fallback => Self::fallback(),
        }
    }

    fn detect_linux() -> Option<CacheGeometry> {
        let l1d_bytes = Self::read_sysfs_cache("/sys/devices/system/cpu/cpu0/cache/index0/size")
            .or_else(|| Self::read_sysfs_cache("/sys/devices/system/cpu/cpu0/cache/index1/size"))?;
        let l2_bytes = Self::read_sysfs_cache("/sys/devices/system/cpu/cpu0/cache/index2/size")?;
        let l3_bytes = Self::read_sysfs_cache("/sys/devices/system/cpu/cpu0/cache/index3/size")?;
        Some(CacheGeometry {
            l1d_bytes,
            l2_bytes,
            l3_bytes,
        })
    }

    fn read_sysfs_cache(path: &str) -> Option<usize> {
        let raw = fs::read_to_string(path).ok()?;
        CacheSizeParser::parse(raw.trim())
    }

    fn detect_macos() -> Option<CacheGeometry> {
        let l1d_bytes = Self::read_sysctl_cache("hw.l1dcachesize")?;
        let l2_bytes = Self::read_sysctl_cache("hw.l2cachesize")?;
        let l3_bytes =
            Self::read_sysctl_cache("hw.l3cachesize").unwrap_or_else(|| Self::fallback().l3_bytes);
        Some(CacheGeometry {
            l1d_bytes,
            l2_bytes,
            l3_bytes,
        })
    }

    fn read_sysctl_cache(name: &str) -> Option<usize> {
        match SysctlOutput::read(name) {
            SysctlOutput::Ready(output) => String::from_utf8(output).ok()?.trim().parse().ok(),
            SysctlOutput::Missing => None,
        }
    }

    fn fallback() -> CacheGeometry {
        CacheGeometry {
            l1d_bytes: 64 * 1024,
            l2_bytes: 4 * 1024 * 1024,
            l3_bytes: 16 * 1024 * 1024,
        }
    }
}

impl<'a> CacheSizeParser<'a> {
    pub(super) fn parse(raw: &'a str) -> Option<usize> {
        Self { raw }.parse_inner()
    }

    fn parse_inner(&self) -> Option<usize> {
        match self.raw.is_empty() {
            true => None,
            false => self.parse_non_empty(),
        }
    }

    fn parse_non_empty(&self) -> Option<usize> {
        let digits = self.digits();
        let value = digits.parse::<usize>().ok()?;
        Some(CacheSizeSuffix::from_raw(self.raw, digits.len()).apply(value))
    }

    fn digits(&self) -> String {
        self.raw
            .chars()
            .take_while(|ch| ch.is_ascii_digit())
            .collect::<String>()
    }
}

impl NearestPowerOfTwo {
    fn from(value: usize) -> usize {
        match value {
            0 | 1 => 1,
            value => Self::nearest_non_trivial(value),
        }
    }

    fn nearest_non_trivial(value: usize) -> usize {
        let lower = value.next_power_of_two() >> 1;
        let upper = value.next_power_of_two();
        match value.saturating_sub(lower) <= upper.saturating_sub(value) {
            true => lower.max(1),
            false => upper,
        }
    }
}

impl CacheSizeSuffix {
    fn from_raw(raw: &str, digit_len: usize) -> Self {
        match raw.chars().nth(digit_len).unwrap_or('B') {
            'K' | 'k' => Self::KiB,
            'M' | 'm' => Self::MiB,
            _ => Self::Bytes,
        }
    }

    fn apply(self, value: usize) -> usize {
        match self {
            Self::Bytes => value,
            Self::KiB => value * 1024,
            Self::MiB => value * 1024 * 1024,
        }
    }
}

impl SysctlOutput {
    fn read(name: &str) -> Self {
        match Command::new("sysctl").arg("-n").arg(name).output() {
            Ok(output) if output.status.success() => Self::Ready(output.stdout),
            Ok(_) | Err(_) => Self::Missing,
        }
    }
}
