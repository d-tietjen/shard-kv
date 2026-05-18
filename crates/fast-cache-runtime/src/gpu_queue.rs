use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u32)]
pub enum GpuDirectCompletionStatus {
    Queued = 0,
    GpuHotHit = 1,
    HostHit = 2,
    PartialHit = 3,
    Miss = 4,
    RemotePending = 5,
    Error = 6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(transparent)]
pub struct GpuDirectRequestFlags(pub u32);

impl GpuDirectRequestFlags {
    pub const LOOKUP_ONLY: u32 = 1 << 0;
    pub const RESTORE_IF_HIT: u32 = 1 << 1;
    pub const ALLOW_HOST_FALLBACK: u32 = 1 << 2;
    pub const ALLOW_PARTIAL: u32 = 1 << 3;
    pub const PROMOTE_ON_HIT: u32 = 1 << 4;

    #[inline(always)]
    pub fn contains(self, flag: u32) -> bool {
        self.0 & flag == flag
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuDirectQueueConfig {
    pub request_capacity: u32,
    pub completion_capacity: u32,
    pub max_batch_size: u32,
}

impl Default for GpuDirectQueueConfig {
    fn default() -> Self {
        Self {
            request_capacity: 1024,
            completion_capacity: 1024,
            max_batch_size: 64,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[repr(C)]
pub struct GpuDirectLookupRequest {
    pub ticket: u64,
    pub request_id: u64,
    pub allocation_id: u64,
    pub dst_device_ptr: u64,
    pub dst_len_bytes: u64,
    pub model_id: u32,
    pub tokenizer_id: u32,
    pub layer_index: u32,
    pub page_index: u32,
    pub device_ordinal: u32,
    pub stream_ordinal: u32,
    pub flags: GpuDirectRequestFlags,
    pub expected_len_bytes: u32,
    pub session_prefix_hash: [u8; 16],
    pub key_hash: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(C)]
pub struct GpuDirectLookupCompletion {
    pub ticket: u64,
    pub status: GpuDirectCompletionStatus,
    pub matched_pages: u32,
    pub missed_pages: u32,
    pub bytes_restored: u32,
    pub generation: u32,
    pub needs_remote: u32,
    pub reserved: u32,
    pub resolved_device_ptr: u64,
    pub aux: u64,
}

impl Default for GpuDirectLookupCompletion {
    fn default() -> Self {
        Self {
            ticket: 0,
            status: GpuDirectCompletionStatus::Queued,
            matched_pages: 0,
            missed_pages: 0,
            bytes_restored: 0,
            generation: 0,
            needs_remote: 0,
            reserved: 0,
            resolved_device_ptr: 0,
            aux: 0,
        }
    }
}
