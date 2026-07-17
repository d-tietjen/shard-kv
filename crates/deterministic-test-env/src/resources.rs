use crate::{CHAOS_RATE_DENOMINATOR, Result, SimEnvError};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CpuProfile {
    pub processing_delay_ms: u64,
    pub jitter_ms: u64,
    pub stall_ppm: u32,
}

impl CpuProfile {
    pub fn validate(&self) -> Result<()> {
        if self.stall_ppm > CHAOS_RATE_DENOMINATOR {
            return Err(SimEnvError::InvalidRate {
                label: "cpu.stall_ppm",
                value: self.stall_ppm,
                max: CHAOS_RATE_DENOMINATOR,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HardwareFaultConfig {
    pub crash_ppm: u32,
    pub io_error_ppm: u32,
    pub memory_error_ppm: u32,
}

impl HardwareFaultConfig {
    pub fn validate(&self) -> Result<()> {
        for (label, value) in [
            ("hardware.crash_ppm", self.crash_ppm),
            ("hardware.io_error_ppm", self.io_error_ppm),
            ("hardware.memory_error_ppm", self.memory_error_ppm),
        ] {
            if value > CHAOS_RATE_DENOMINATOR {
                return Err(SimEnvError::InvalidRate {
                    label,
                    value,
                    max: CHAOS_RATE_DENOMINATOR,
                });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HardwareFaultKind {
    Crash,
    IoError,
    MemoryError,
}

impl HardwareFaultKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Crash => "hardware_crash",
            Self::IoError => "hardware_io_error",
            Self::MemoryError => "hardware_memory_error",
        }
    }
}
