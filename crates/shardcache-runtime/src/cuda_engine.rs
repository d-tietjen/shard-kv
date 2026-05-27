use crate::runtime::{RuntimeError, RuntimeResult};

#[cfg(all(feature = "cuda", target_os = "linux"))]
use crate::runtime::{
    KvTransferEngine, RuntimeSessionTransfer, RuntimeSessionTransferSummary, RuntimeTransferChunk,
    RuntimeTransferTarget,
};

#[cfg(all(feature = "cuda", target_os = "linux"))]
use shardmap::cuda::CudaConfig;
#[cfg(all(feature = "cuda", target_os = "linux"))]
use shardmap::storage::{LocalEmbeddedSessionPackedView, PackedBatch};

#[cfg(all(feature = "cuda", target_os = "linux"))]
use cust::{
    CudaFlags,
    context::{Context, CurrentContext},
    device::Device,
    stream::{Stream, StreamFlags},
};

#[cfg(all(feature = "cuda", target_os = "linux"))]
const HOST_PAGE_BYTES: usize = 4096;
#[cfg(all(feature = "cuda", target_os = "linux"))]
const MAX_CACHED_SESSION_VIEWS: usize = 4;

#[cfg(any(test, all(feature = "cuda", target_os = "linux")))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HostRange {
    start: usize,
    end: usize,
}

#[cfg(any(test, all(feature = "cuda", target_os = "linux")))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StridedHostCopyPlan {
    row_bytes: usize,
    row_count: usize,
    src_offset: usize,
    dst_pitch: usize,
}

#[cfg(any(test, all(feature = "cuda", target_os = "linux")))]
fn aligned_host_range(ptr: *const u8, len: usize, page_bytes: usize) -> RuntimeResult<HostRange> {
    if len == 0 {
        let start = ptr as usize;
        return Ok(HostRange { start, end: start });
    }

    let start = ptr as usize;
    let end = start
        .checked_add(len)
        .ok_or_else(|| RuntimeError::Cuda("host registration range overflow".into()))?;
    let aligned_start = start & !(page_bytes - 1);
    let aligned_end = end
        .checked_add(page_bytes - 1)
        .ok_or_else(|| RuntimeError::Cuda("host registration range overflow".into()))?
        & !(page_bytes - 1);
    Ok(HostRange {
        start: aligned_start,
        end: aligned_end,
    })
}

#[cfg(any(test, all(feature = "cuda", target_os = "linux")))]
fn uncovered_host_ranges(range: HostRange, existing: &[HostRange]) -> Vec<HostRange> {
    if range.start >= range.end {
        return Vec::new();
    }

    let mut sorted = existing.to_vec();
    sorted.sort_unstable_by_key(|existing| existing.start);

    let mut cursor = range.start;
    let mut uncovered = Vec::new();
    for existing in sorted {
        if existing.end <= cursor {
            continue;
        }
        if existing.start >= range.end {
            break;
        }
        if existing.start > cursor {
            uncovered.push(HostRange {
                start: cursor,
                end: existing.start.min(range.end),
            });
        }
        cursor = cursor.max(existing.end.min(range.end));
        if cursor >= range.end {
            return uncovered;
        }
    }

    if cursor < range.end {
        uncovered.push(HostRange {
            start: cursor,
            end: range.end,
        });
    }
    uncovered
}

#[cfg(any(test, all(feature = "cuda", target_os = "linux")))]
fn detect_strided_host_copy_plan(
    offsets: &[usize],
    lengths: &[usize],
    dst_offsets: &[u64],
    start_index: usize,
) -> Option<StridedHostCopyPlan> {
    if start_index + 1 >= offsets.len()
        || start_index + 1 >= lengths.len()
        || start_index + 1 >= dst_offsets.len()
    {
        return None;
    }

    let start_offset = *offsets.get(start_index)?;
    let row_bytes = *lengths.get(start_index)?;
    if start_offset == usize::MAX || row_bytes == 0 {
        return None;
    }

    let next_offset = *offsets.get(start_index + 1)?;
    let next_len = *lengths.get(start_index + 1)?;
    if next_offset == usize::MAX || next_len != row_bytes {
        return None;
    }
    if next_offset != start_offset.saturating_add(row_bytes) {
        return None;
    }

    let start_dst_offset = *dst_offsets.get(start_index)?;
    let next_dst_offset = *dst_offsets.get(start_index + 1)?;
    if next_dst_offset <= start_dst_offset {
        return None;
    }
    let dst_pitch = (next_dst_offset - start_dst_offset) as usize;
    if dst_pitch <= row_bytes {
        return None;
    }

    let mut row_count = 2usize;
    let mut previous_src_end = next_offset.saturating_add(row_bytes);
    let mut previous_dst_offset = next_dst_offset;
    while start_index + row_count < offsets.len()
        && start_index + row_count < lengths.len()
        && start_index + row_count < dst_offsets.len()
    {
        let offset = offsets[start_index + row_count];
        let len = lengths[start_index + row_count];
        let dst_offset = dst_offsets[start_index + row_count];
        if offset == usize::MAX || len != row_bytes {
            break;
        }
        if offset != previous_src_end {
            break;
        }
        if dst_offset != previous_dst_offset.saturating_add(dst_pitch as u64) {
            break;
        }
        row_count += 1;
        previous_src_end = offset.saturating_add(row_bytes);
        previous_dst_offset = dst_offset;
    }

    Some(StridedHostCopyPlan {
        row_bytes,
        row_count,
        src_offset: start_offset,
        dst_pitch,
    })
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
#[derive(Debug)]
struct RegisteredHostRegion {
    range: HostRange,
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
impl Drop for RegisteredHostRegion {
    fn drop(&mut self) {
        let _ = crate::cuda_ffi::unregister_host_region(self.range.start as *mut u8);
    }
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
#[derive(Debug)]
struct CachedSessionView {
    buffer_ptr: usize,
    buffer_len: usize,
    range: HostRange,
    _view: LocalEmbeddedSessionPackedView,
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
#[derive(Debug)]
struct CudaCompletionEvent {
    raw: crate::cuda_ffi::RawCudaEvent,
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
impl Drop for CudaCompletionEvent {
    fn drop(&mut self) {
        let _ = crate::cuda_ffi::destroy_event(self.raw);
    }
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
#[derive(Debug)]
pub struct CudaTransferEngine {
    device_ordinal: usize,
    context: Context,
    streams: Vec<Stream>,
    active_stream_index: usize,
    layer_streaming: bool,
    persistent_registered_regions: Vec<RegisteredHostRegion>,
    temporary_registered_regions: Vec<RegisteredHostRegion>,
    staged_buffers: Vec<crate::host::StagedHostBuffer>,
    borrowed_session_views: Vec<LocalEmbeddedSessionPackedView>,
    cached_session_views: Vec<CachedSessionView>,
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
#[derive(Debug)]
pub struct PendingCudaTransfer {
    engine: CudaTransferEngine,
    transfer: RuntimeSessionTransfer,
    summary: RuntimeSessionTransferSummary,
    completion_events: Vec<CudaCompletionEvent>,
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
impl PendingCudaTransfer {
    #[inline(always)]
    pub fn summary(&self) -> RuntimeSessionTransferSummary {
        self.summary
    }

    pub fn is_ready(&mut self) -> RuntimeResult<bool> {
        self.engine.ensure_current_context()?;
        for event in &self.completion_events {
            if !crate::cuda_ffi::event_is_ready(event.raw)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn wait_on_stream(&mut self, stream_ptr: u64) -> RuntimeResult<()> {
        self.engine.ensure_current_context()?;
        for event in &self.completion_events {
            crate::cuda_ffi::stream_wait_event(stream_ptr, event.raw)?;
        }
        Ok(())
    }

    pub fn wait(mut self) -> RuntimeResult<RuntimeSessionTransferSummary> {
        self.engine.ensure_current_context()?;
        for event in &self.completion_events {
            crate::cuda_ffi::synchronize_event(event.raw)?;
        }
        self.engine.finish_transfer(&self.transfer, self.summary)?;
        Ok(self.summary)
    }

    pub fn wait_with_engine(
        mut self,
    ) -> RuntimeResult<(CudaTransferEngine, RuntimeSessionTransferSummary)> {
        self.engine.ensure_current_context()?;
        for event in &self.completion_events {
            crate::cuda_ffi::synchronize_event(event.raw)?;
        }
        let summary = self.summary;
        self.engine.finish_transfer(&self.transfer, summary)?;
        let PendingCudaTransfer { engine, .. } = self;
        Ok((engine, summary))
    }
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
impl CudaTransferEngine {
    pub fn new(device_ordinal: usize) -> RuntimeResult<Self> {
        Self::new_with_streams_and_layer_streaming(device_ordinal, 1, false)
    }

    pub fn from_config(config: &CudaConfig) -> RuntimeResult<Self> {
        Self::new_with_streams_and_layer_streaming(
            config.device_ordinal,
            config.transfer_stream_count.max(1),
            config.layer_streaming,
        )
    }

    pub fn from_config_for_direct_dma(config: &CudaConfig) -> RuntimeResult<Self> {
        Self::new_with_streams_and_layer_streaming(config.device_ordinal, 1, false)
    }

    pub fn new_with_streams(device_ordinal: usize, stream_count: usize) -> RuntimeResult<Self> {
        Self::new_with_streams_and_layer_streaming(device_ordinal, stream_count, false)
    }

    pub fn new_with_streams_and_layer_streaming(
        device_ordinal: usize,
        stream_count: usize,
        layer_streaming: bool,
    ) -> RuntimeResult<Self> {
        cust::init(CudaFlags::empty())
            .map_err(|err| RuntimeError::Cuda(format!("cust::init: {err}")))?;
        let device = Device::get_device(device_ordinal as u32)
            .map_err(|err| RuntimeError::Cuda(format!("Device::get_device: {err}")))?;
        let context = Context::new(device)
            .map_err(|err| RuntimeError::Cuda(format!("Context::new: {err}")))?;
        CurrentContext::set_current(&context)
            .map_err(|err| RuntimeError::Cuda(format!("CurrentContext::set_current: {err}")))?;
        let mut streams = Vec::with_capacity(stream_count.max(1));
        for _ in 0..stream_count.max(1) {
            streams.push(
                Stream::new(StreamFlags::NON_BLOCKING, None)
                    .map_err(|err| RuntimeError::Cuda(format!("Stream::new: {err}")))?,
            );
        }
        Ok(Self {
            device_ordinal,
            context,
            streams,
            active_stream_index: 0,
            layer_streaming,
            persistent_registered_regions: Vec::new(),
            temporary_registered_regions: Vec::new(),
            staged_buffers: Vec::new(),
            borrowed_session_views: Vec::new(),
            cached_session_views: Vec::new(),
        })
    }

    fn ensure_target_device(&self, target: &RuntimeTransferTarget) -> RuntimeResult<()> {
        let (device_ordinal, maybe_dst_ptr) = match target {
            RuntimeTransferTarget::Gpu(target) => {
                (target.device_ordinal, Some(target.dst_device_ptr))
            }
            RuntimeTransferTarget::PagedGpu(target) => (target.device_ordinal, None),
            RuntimeTransferTarget::Cpu(_) => {
                return Err(RuntimeError::Cuda(
                    "cuda transfer requires a gpu target".into(),
                ));
            }
        };
        if device_ordinal != self.device_ordinal {
            return Err(RuntimeError::Cuda(format!(
                "transfer target device {} does not match engine device {}",
                device_ordinal, self.device_ordinal
            )));
        }
        if maybe_dst_ptr == Some(0) {
            return Err(RuntimeError::Cuda(
                "transfer target is missing a destination device pointer".into(),
            ));
        }
        Ok(())
    }

    fn ensure_current_context(&self) -> RuntimeResult<()> {
        CurrentContext::set_current(&self.context)
            .map_err(|err| RuntimeError::Cuda(format!("CurrentContext::set_current: {err}")))
    }

    fn active_stream(&self) -> &Stream {
        &self.streams[self.active_stream_index]
    }

    #[inline(always)]
    fn advance_stream(&mut self) {
        if self.layer_streaming && self.streams.len() > 1 {
            self.active_stream_index = (self.active_stream_index + 1) % self.streams.len();
        }
    }

    fn submit_host_copy(
        &mut self,
        target: &RuntimeTransferTarget,
        descriptor: &shardmap::cuda::CudaChunkTransferDescriptor,
        src: *const u8,
        len: usize,
    ) -> RuntimeResult<()> {
        if let Some(dst) = target.absolute_device_ptr_for(descriptor) {
            return crate::cuda_ffi::memcpy_host_to_device_async(
                dst as u64,
                src,
                len,
                self.active_stream(),
            )
            .map_err(|error| {
                RuntimeError::Cuda(format!(
                    "layer={} dst_offset_bytes={} len_bytes={} direct_copy_failed: {error}",
                    descriptor.layer_index(),
                    descriptor.dst_offset_bytes(),
                    len,
                ))
            });
        }

        if let Some(paged) = target.as_paged_gpu() {
            for segment in paged.resolve_segments_for_descriptor(descriptor, len)? {
                let segment_src = unsafe { src.add(segment.src_offset_bytes) };
                crate::cuda_ffi::memcpy_host_to_device_async(
                    segment.dst_device_ptr as u64,
                    segment_src,
                    segment.len_bytes,
                    self.active_stream(),
                )
                .map_err(|error| {
                    RuntimeError::Cuda(format!(
                        "layer={} page_index={} dst_offset_bytes={} src_offset_bytes={} len_bytes={} paged_copy_failed: {error}",
                        descriptor.layer_index(),
                        segment.page_index,
                        descriptor.dst_offset_bytes(),
                        segment.src_offset_bytes,
                        segment.len_bytes,
                    ))
                })?;
            }
            return Ok(());
        }

        Err(RuntimeError::Cuda(
            "cuda transfer requires a gpu target".into(),
        ))
    }

    fn submit_host_copy_2d(
        &mut self,
        target: &RuntimeTransferTarget,
        descriptor: &shardmap::cuda::CudaChunkTransferDescriptor,
        src: *const u8,
        row_bytes: usize,
        row_count: usize,
        dst_pitch: usize,
    ) -> RuntimeResult<()> {
        if let RuntimeTransferTarget::Gpu(target) = target {
            let dst = target
                .absolute_device_ptr_for(descriptor)
                .ok_or_else(|| RuntimeError::Cuda("missing destination device pointer".into()))?;
            return crate::cuda_ffi::memcpy_host_to_device_2d_async(
                dst as u64,
                dst_pitch,
                src,
                row_bytes,
                row_bytes,
                row_count,
                self.active_stream(),
            )
            .map_err(|error| {
                RuntimeError::Cuda(format!(
                    "layer={} dst_offset_bytes={} row_bytes={} row_count={} dst_pitch={} direct_copy_2d_failed: {error}",
                    descriptor.layer_index(),
                    descriptor.dst_offset_bytes(),
                    row_bytes,
                    row_count,
                    dst_pitch,
                ))
            });
        }

        Err(RuntimeError::Cuda(
            "cuda 2d transfer requires a contiguous gpu target".into(),
        ))
    }

    fn registered_host_ranges(&self) -> Vec<HostRange> {
        self.persistent_registered_regions
            .iter()
            .chain(self.temporary_registered_regions.iter())
            .map(|registered| registered.range)
            .collect()
    }

    fn register_temporary_host_pages(&mut self, ptr: *const u8, len: usize) -> RuntimeResult<()> {
        let aligned = aligned_host_range(ptr, len, HOST_PAGE_BYTES)?;
        let existing = self.registered_host_ranges();
        for uncovered in uncovered_host_ranges(aligned, &existing) {
            crate::cuda_ffi::register_host_region(uncovered.start as *mut u8, uncovered.len())?;
            self.temporary_registered_regions
                .push(RegisteredHostRegion { range: uncovered });
        }
        Ok(())
    }

    fn buffer_is_persistently_cached(&self, ptr: *const u8, len: usize) -> bool {
        let buffer_ptr = ptr as usize;
        self.cached_session_views
            .iter()
            .any(|cached| cached.buffer_ptr == buffer_ptr && cached.buffer_len == len)
    }

    fn rebuild_persistent_registrations(&mut self) -> RuntimeResult<()> {
        self.persistent_registered_regions.clear();
        let mut existing = Vec::with_capacity(self.cached_session_views.len());
        for cached in &self.cached_session_views {
            for uncovered in uncovered_host_ranges(cached.range, &existing) {
                crate::cuda_ffi::register_host_region(uncovered.start as *mut u8, uncovered.len())?;
                self.persistent_registered_regions
                    .push(RegisteredHostRegion { range: uncovered });
                existing.push(uncovered);
            }
        }
        Ok(())
    }

    fn cache_session_view(&mut self, packed: LocalEmbeddedSessionPackedView) -> RuntimeResult<()> {
        let buffer = packed.buffer();
        if buffer.is_empty() {
            return Ok(());
        }

        let buffer_ptr = buffer.as_ptr() as usize;
        let buffer_len = buffer.len();
        if self
            .cached_session_views
            .iter()
            .any(|cached| cached.buffer_ptr == buffer_ptr && cached.buffer_len == buffer_len)
        {
            return Ok(());
        }

        let range = aligned_host_range(buffer.as_ptr(), buffer_len, HOST_PAGE_BYTES)?;
        self.cached_session_views.push(CachedSessionView {
            buffer_ptr,
            buffer_len,
            range,
            _view: packed,
        });
        if self.cached_session_views.len() > MAX_CACHED_SESSION_VIEWS {
            self.cached_session_views.remove(0);
        }
        self.rebuild_persistent_registrations()
    }

    pub fn submit_packed_host_buffer_transfer(
        &mut self,
        transfer: &RuntimeSessionTransfer,
        packed: PackedBatch,
    ) -> RuntimeResult<RuntimeSessionTransferSummary> {
        self.begin_transfer(transfer)?;

        let request = transfer.request();
        if packed.offsets.len() != request.chunks().len()
            || packed.lengths.len() != request.chunks().len()
        {
            return Err(RuntimeError::Engine(
                "packed batch shape does not match transfer request".into(),
            ));
        }

        let buffer = crate::host::StagedHostBuffer::from_owned_bytes(packed.buffer);
        let src_base = buffer.as_ptr();
        self.register_temporary_host_pages(src_base, buffer.len())?;
        let summary = RuntimeSessionTransferSummary {
            requested_chunks: request.chunks().len(),
            hit_chunks: packed.hit_count,
            missed_chunks: request.chunks().len().saturating_sub(packed.hit_count),
            transferred_bytes: buffer.len(),
        };

        let descriptors = request.chunks();
        let dst_offsets = descriptors
            .iter()
            .map(|descriptor| transfer.target().absolute_offset_for(descriptor))
            .collect::<Vec<_>>();
        let mut index = 0usize;
        while index < descriptors.len() {
            let start_offset = packed.offsets[index];
            if start_offset == usize::MAX {
                index += 1;
                continue;
            }

            if matches!(transfer.target(), RuntimeTransferTarget::Gpu(_)) {
                if let Some(plan) = detect_strided_host_copy_plan(
                    &packed.offsets,
                    &packed.lengths,
                    &dst_offsets,
                    index,
                ) {
                    let src = unsafe { src_base.add(plan.src_offset) };
                    self.submit_host_copy_2d(
                        transfer.target(),
                        &descriptors[index],
                        src,
                        plan.row_bytes,
                        plan.row_count,
                        plan.dst_pitch,
                    )?;
                    index += plan.row_count;
                    self.advance_stream();
                    continue;
                }
            }

            let start_index = index;
            let mut total_len = packed.lengths[index];
            let mut previous_src_end = start_offset.saturating_add(packed.lengths[index]);
            let mut previous_dst_end = dst_offsets[index] + packed.lengths[index] as u64;
            index += 1;

            while index < descriptors.len() {
                let next_offset = packed.offsets[index];
                if next_offset == usize::MAX {
                    break;
                }
                let next_dst_offset = dst_offsets[index];
                if next_offset != previous_src_end || next_dst_offset != previous_dst_end {
                    break;
                }
                total_len = total_len.saturating_add(packed.lengths[index]);
                previous_src_end = next_offset.saturating_add(packed.lengths[index]);
                previous_dst_end = next_dst_offset.saturating_add(packed.lengths[index] as u64);
                index += 1;
            }

            let src = unsafe { src_base.add(start_offset) };
            self.submit_host_copy(transfer.target(), &descriptors[start_index], src, total_len)?;
            self.advance_stream();
        }

        self.staged_buffers.push(buffer);
        Ok(summary)
    }

    pub fn submit_zero_copy_session_view_transfer(
        &mut self,
        transfer: &RuntimeSessionTransfer,
        packed: LocalEmbeddedSessionPackedView,
    ) -> RuntimeResult<RuntimeSessionTransferSummary> {
        self.begin_transfer(transfer)?;

        let request = transfer.request();
        if packed.offsets().len() != request.chunks().len()
            || packed.lengths().len() != request.chunks().len()
        {
            return Err(RuntimeError::Engine(
                "packed session view shape does not match transfer request".into(),
            ));
        }

        let src_base = packed.buffer().as_ptr();
        let src_len = packed.buffer().len();
        let was_persistently_cached = self.buffer_is_persistently_cached(src_base, src_len);
        let summary = RuntimeSessionTransferSummary {
            requested_chunks: request.chunks().len(),
            hit_chunks: packed.hit_count(),
            missed_chunks: request.chunks().len().saturating_sub(packed.hit_count()),
            transferred_bytes: packed.total_bytes(),
        };

        let descriptors = request.chunks();
        let dst_offsets = descriptors
            .iter()
            .map(|descriptor| transfer.target().absolute_offset_for(descriptor))
            .collect::<Vec<_>>();
        let offsets = packed.offsets();
        let lengths = packed.lengths();
        let mut index = 0usize;
        while index < descriptors.len() {
            let start_offset = offsets[index];
            if start_offset == usize::MAX {
                index += 1;
                continue;
            }

            if matches!(transfer.target(), RuntimeTransferTarget::Gpu(_)) {
                if let Some(plan) =
                    detect_strided_host_copy_plan(offsets, lengths, &dst_offsets, index)
                {
                    let src = unsafe { src_base.add(plan.src_offset) };
                    self.submit_host_copy_2d(
                        transfer.target(),
                        &descriptors[index],
                        src,
                        plan.row_bytes,
                        plan.row_count,
                        plan.dst_pitch,
                    )?;
                    index += plan.row_count;
                    self.advance_stream();
                    continue;
                }
            }

            let start_index = index;
            let mut total_len = lengths[index];
            let mut previous_src_end = start_offset.saturating_add(lengths[index]);
            let mut previous_dst_end = dst_offsets[index] + lengths[index] as u64;
            index += 1;

            while index < descriptors.len() {
                let next_offset = offsets[index];
                if next_offset == usize::MAX {
                    break;
                }
                let next_dst_offset = dst_offsets[index];
                if next_offset != previous_src_end || next_dst_offset != previous_dst_end {
                    break;
                }
                total_len = total_len.saturating_add(lengths[index]);
                previous_src_end = next_offset.saturating_add(lengths[index]);
                previous_dst_end = next_dst_offset.saturating_add(lengths[index] as u64);
                index += 1;
            }

            let src = unsafe { src_base.add(start_offset) };
            self.submit_host_copy(transfer.target(), &descriptors[start_index], src, total_len)?;
            self.advance_stream();
        }

        if !was_persistently_cached {
            self.cache_session_view(packed)?;
        }
        Ok(summary)
    }

    #[inline(always)]
    pub fn into_pending(
        self,
        transfer: RuntimeSessionTransfer,
        summary: RuntimeSessionTransferSummary,
    ) -> RuntimeResult<PendingCudaTransfer> {
        self.ensure_current_context()?;
        let mut completion_events = Vec::with_capacity(self.streams.len());
        for stream in &self.streams {
            let event = crate::cuda_ffi::create_event()?;
            crate::cuda_ffi::record_event(event, stream)?;
            completion_events.push(CudaCompletionEvent { raw: event });
        }
        Ok(PendingCudaTransfer {
            engine: self,
            transfer,
            summary,
            completion_events,
        })
    }
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
impl Drop for CudaTransferEngine {
    fn drop(&mut self) {
        let _ = self.ensure_current_context();
        for stream in &self.streams {
            let _ = stream.synchronize();
        }
        self.streams.clear();
        self.temporary_registered_regions.clear();
        self.persistent_registered_regions.clear();
        self.cached_session_views.clear();
        self.borrowed_session_views.clear();
        self.staged_buffers.clear();
    }
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
impl KvTransferEngine for CudaTransferEngine {
    fn begin_transfer(&mut self, transfer: &RuntimeSessionTransfer) -> RuntimeResult<()> {
        self.ensure_current_context()?;
        self.ensure_target_device(transfer.target())?;
        let stream_ordinal = match transfer.target() {
            RuntimeTransferTarget::Gpu(gpu) => gpu.stream_ordinal,
            RuntimeTransferTarget::PagedGpu(gpu) => gpu.stream_ordinal,
            RuntimeTransferTarget::Cpu(_) => {
                return Err(RuntimeError::Cuda(
                    "cuda transfer requires a gpu target".into(),
                ));
            }
        };
        self.active_stream_index = stream_ordinal % self.streams.len();
        self.borrowed_session_views.clear();
        self.staged_buffers.clear();
        self.temporary_registered_regions.clear();
        Ok(())
    }

    fn submit_chunk(&mut self, chunk: RuntimeTransferChunk<'_>) -> RuntimeResult<()> {
        self.ensure_target_device(chunk.target())?;

        match chunk {
            RuntimeTransferChunk::Direct(chunk) => {
                let owned = crate::host::StagedHostBuffer::from_bytes(chunk.bytes());
                let src = owned.as_ptr();
                let len = owned.len();
                self.register_temporary_host_pages(src, len)?;
                self.submit_host_copy(chunk.target(), chunk.descriptor(), src, len)?;
                self.staged_buffers.push(owned);
                self.advance_stream();
            }
            RuntimeTransferChunk::Staged(chunk) => {
                let src = chunk.bytes().as_ptr();
                let len = chunk.bytes().len();
                self.submit_host_copy(chunk.target(), chunk.descriptor(), src, len)?;
                self.staged_buffers.push(chunk.into_buffer());
                self.advance_stream();
            }
        }
        Ok(())
    }

    fn finish_transfer(
        &mut self,
        _transfer: &RuntimeSessionTransfer,
        _summary: RuntimeSessionTransferSummary,
    ) -> RuntimeResult<()> {
        for stream in &self.streams {
            stream
                .synchronize()
                .map_err(|err| RuntimeError::Cuda(format!("Stream::synchronize: {err}")))?;
        }
        self.borrowed_session_views.clear();
        self.staged_buffers.clear();
        self.temporary_registered_regions.clear();
        Ok(())
    }
}

#[cfg(not(all(feature = "cuda", target_os = "linux")))]
use shardmap::cuda::CudaConfig;

#[cfg(not(all(feature = "cuda", target_os = "linux")))]
#[derive(Debug)]
pub struct CudaTransferEngine;

#[cfg(not(all(feature = "cuda", target_os = "linux")))]
impl CudaTransferEngine {
    pub fn new(_device_ordinal: usize) -> RuntimeResult<Self> {
        Err(RuntimeError::Cuda(
            "real CUDA transfer engine is only available on Linux builds with the `cuda` feature"
                .into(),
        ))
    }

    pub fn from_config(_config: &CudaConfig) -> RuntimeResult<Self> {
        Err(RuntimeError::Cuda(
            "real CUDA transfer engine is only available on Linux builds with the `cuda` feature"
                .into(),
        ))
    }

    pub fn from_config_for_direct_dma(_config: &CudaConfig) -> RuntimeResult<Self> {
        Err(RuntimeError::Cuda(
            "real CUDA transfer engine is only available on Linux builds with the `cuda` feature"
                .into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        HostRange, StridedHostCopyPlan, aligned_host_range, detect_strided_host_copy_plan,
        uncovered_host_ranges,
    };

    #[test]
    fn aligned_host_range_rounds_to_page_boundaries() {
        let range = aligned_host_range(0x1234usize as *const u8, 9000, 4096)
            .expect("range alignment should succeed");
        assert_eq!(
            range,
            HostRange {
                start: 0x1000,
                end: 0x4000,
            }
        );
    }

    #[test]
    fn uncovered_host_ranges_ignores_fully_registered_range() {
        let requested = HostRange {
            start: 0x2000,
            end: 0x3000,
        };
        let existing = [HostRange {
            start: 0x1000,
            end: 0x4000,
        }];
        assert!(uncovered_host_ranges(requested, &existing).is_empty());
    }

    #[test]
    fn uncovered_host_ranges_splits_overlap_into_unregistered_segments() {
        let requested = HostRange {
            start: 0x1000,
            end: 0x5000,
        };
        let existing = [
            HostRange {
                start: 0x1000,
                end: 0x2000,
            },
            HostRange {
                start: 0x3000,
                end: 0x4000,
            },
        ];
        assert_eq!(
            uncovered_host_ranges(requested, &existing),
            vec![
                HostRange {
                    start: 0x2000,
                    end: 0x3000,
                },
                HostRange {
                    start: 0x4000,
                    end: 0x5000,
                },
            ]
        );
    }

    #[test]
    fn detect_strided_host_copy_plan_finds_fragmented_equal_rows() {
        let offsets = [0usize, 256, 512, 768];
        let lengths = [256usize, 256, 256, 256];
        let dst_offsets = [0u64, 512, 1024, 1536];
        assert_eq!(
            detect_strided_host_copy_plan(&offsets, &lengths, &dst_offsets, 0),
            Some(StridedHostCopyPlan {
                row_bytes: 256,
                row_count: 4,
                src_offset: 0,
                dst_pitch: 512,
            })
        );
    }

    #[test]
    fn detect_strided_host_copy_plan_rejects_contiguous_dst_layout() {
        let offsets = [0usize, 256, 512];
        let lengths = [256usize, 256, 256];
        let dst_offsets = [0u64, 256, 512];
        assert_eq!(
            detect_strided_host_copy_plan(&offsets, &lengths, &dst_offsets, 0),
            None
        );
    }
}
