use crate::host::HostTransferPath;
use crate::runtime::{
    KvTransferEngine, RuntimeError, RuntimeResult, RuntimeSessionTransfer,
    RuntimeSessionTransferSummary, RuntimeTransferChunk,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimulatedTransferredChunk {
    pub layer_index: u32,
    pub bytes: Vec<u8>,
    pub absolute_offset_bytes: u64,
    pub transfer_path: HostTransferPath,
}

#[derive(Debug, Default)]
pub struct SimulatedGpuEngine {
    pub began: bool,
    pub finished: bool,
    pub device_ordinal: Option<usize>,
    pub stream_ordinal: Option<usize>,
    pub allocation_id: Option<u64>,
    pub chunks: Vec<SimulatedTransferredChunk>,
    pub final_summary: Option<RuntimeSessionTransferSummary>,
}

impl KvTransferEngine for SimulatedGpuEngine {
    fn begin_transfer(&mut self, transfer: &RuntimeSessionTransfer) -> RuntimeResult<()> {
        self.began = true;
        match transfer.target() {
            crate::runtime::RuntimeTransferTarget::Gpu(gpu) => {
                self.device_ordinal = Some(gpu.device_ordinal);
                self.stream_ordinal = Some(gpu.stream_ordinal);
                self.allocation_id = Some(gpu.allocation_id);
            }
            crate::runtime::RuntimeTransferTarget::PagedGpu(gpu) => {
                self.device_ordinal = Some(gpu.device_ordinal);
                self.stream_ordinal = Some(gpu.stream_ordinal);
                self.allocation_id = Some(gpu.allocation_id);
            }
            crate::runtime::RuntimeTransferTarget::Cpu(_) => {
                return Err(RuntimeError::Engine(
                    "simulated gpu engine requires a gpu target".into(),
                ));
            }
        }
        Ok(())
    }

    fn submit_chunk(&mut self, chunk: RuntimeTransferChunk<'_>) -> RuntimeResult<()> {
        let bytes = chunk.bytes();
        let len = bytes.len();
        match chunk.target() {
            crate::runtime::RuntimeTransferTarget::Gpu(_) => {
                let dst = chunk
                    .target()
                    .absolute_device_ptr_for(chunk.descriptor())
                    .ok_or_else(|| {
                        RuntimeError::Engine(
                            "simulated gpu engine missing device destination".into(),
                        )
                    })?;
                if len != 0 {
                    // Safety: tests provide a host-backed buffer pointer as the
                    // simulated device address, and the runtime computes disjoint
                    // offsets for each restored chunk in those tests.
                    unsafe {
                        std::ptr::copy_nonoverlapping(bytes.as_ptr(), dst as *mut u8, len);
                    }
                }
            }
            crate::runtime::RuntimeTransferTarget::PagedGpu(target) => {
                for segment in target.resolve_segments_for_descriptor(chunk.descriptor(), len)? {
                    if segment.len_bytes == 0 {
                        continue;
                    }
                    // Safety: tests provide host-backed per-page buffers as the
                    // simulated device pages. The resolved segments are disjoint
                    // slices inside those page buffers.
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            bytes[segment.src_offset_bytes..].as_ptr(),
                            segment.dst_device_ptr as *mut u8,
                            segment.len_bytes,
                        );
                    }
                }
            }
            crate::runtime::RuntimeTransferTarget::Cpu(_) => {
                return Err(RuntimeError::Engine(
                    "simulated gpu engine requires a gpu target".into(),
                ));
            }
        }
        self.chunks.push(SimulatedTransferredChunk {
            layer_index: chunk.descriptor().layer_index(),
            bytes: bytes.to_vec(),
            absolute_offset_bytes: chunk.absolute_offset_bytes(),
            transfer_path: chunk.transfer_path(),
        });
        Ok(())
    }

    fn finish_transfer(
        &mut self,
        _transfer: &RuntimeSessionTransfer,
        summary: RuntimeSessionTransferSummary,
    ) -> RuntimeResult<()> {
        self.finished = true;
        self.final_summary = Some(summary);
        Ok(())
    }
}
