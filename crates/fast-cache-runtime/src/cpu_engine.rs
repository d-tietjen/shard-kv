use crate::runtime::{
    KvTransferEngine, RuntimeError, RuntimeResult, RuntimeSessionTransfer,
    RuntimeSessionTransferSummary, RuntimeTransferChunk,
};

#[derive(Debug, Default)]
pub struct CpuTransferEngine;

impl CpuTransferEngine {
    #[inline(always)]
    pub fn new() -> Self {
        Self
    }
}

impl KvTransferEngine for CpuTransferEngine {
    fn begin_transfer(&mut self, transfer: &RuntimeSessionTransfer) -> RuntimeResult<()> {
        let cpu = transfer
            .target()
            .as_cpu()
            .ok_or_else(|| RuntimeError::Engine("cpu transfer requires a cpu target".into()))?;
        if cpu.dst_host_ptr == 0 {
            return Err(RuntimeError::Engine(
                "cpu transfer target is missing a destination host pointer".into(),
            ));
        }
        Ok(())
    }

    fn submit_chunk(&mut self, chunk: RuntimeTransferChunk<'_>) -> RuntimeResult<()> {
        let dst = chunk
            .target()
            .absolute_host_ptr_for(chunk.descriptor())
            .ok_or_else(|| RuntimeError::Engine("destination host pointer overflow".into()))?;
        let bytes = chunk.bytes();
        // SAFETY: the caller provided a valid host destination covering this
        // chunk's byte range, and the source slice is live for the synchronous
        // copy.
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), dst as *mut u8, bytes.len());
        }
        Ok(())
    }

    fn finish_transfer(
        &mut self,
        transfer: &RuntimeSessionTransfer,
        _summary: RuntimeSessionTransferSummary,
    ) -> RuntimeResult<()> {
        if transfer.target().as_cpu().is_none() {
            return Err(RuntimeError::Engine(
                "cpu transfer requires a cpu target".into(),
            ));
        }
        Ok(())
    }
}
