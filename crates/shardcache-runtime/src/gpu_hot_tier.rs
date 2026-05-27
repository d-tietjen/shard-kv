use serde::{Deserialize, Serialize};

use crate::gpu_queue::GpuDirectCompletionStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuHotTierLookupResult {
    pub status: GpuDirectCompletionStatus,
    pub resolved_device_ptr: Option<u64>,
    pub generation: u32,
    pub resident_len_bytes: usize,
}

impl GpuHotTierLookupResult {
    #[inline(always)]
    pub fn miss() -> Self {
        Self {
            status: GpuDirectCompletionStatus::Miss,
            resolved_device_ptr: None,
            generation: 0,
            resident_len_bytes: 0,
        }
    }

    #[inline(always)]
    pub fn hit(resolved_device_ptr: u64, resident_len_bytes: usize, generation: u32) -> Self {
        Self {
            status: GpuDirectCompletionStatus::GpuHotHit,
            resolved_device_ptr: Some(resolved_device_ptr),
            generation,
            resident_len_bytes,
        }
    }

    #[inline(always)]
    pub fn is_hit(self) -> bool {
        self.status == GpuDirectCompletionStatus::GpuHotHit
    }
}
