#[cfg(all(feature = "cuda", target_os = "linux"))]
use std::os::raw::c_uint;
#[cfg(all(feature = "cuda", target_os = "linux"))]
use std::os::raw::c_void;

#[cfg(all(feature = "cuda", target_os = "linux"))]
use crate::runtime::{RuntimeError, RuntimeResult};
#[cfg(all(feature = "cuda", target_os = "linux"))]
use cust::stream::Stream;

#[cfg(all(feature = "cuda", target_os = "linux"))]
pub type RawCudaEvent = cust::sys::CUevent;

#[cfg(all(feature = "cuda", target_os = "linux"))]
const CU_MEMORYTYPE_HOST: c_uint = 0x01;
#[cfg(all(feature = "cuda", target_os = "linux"))]
const CU_MEMORYTYPE_DEVICE: c_uint = 0x02;

#[cfg(all(feature = "cuda", target_os = "linux"))]
#[repr(C)]
struct RawCudaMemcpy2D {
    src_x_in_bytes: usize,
    src_y: usize,
    src_memory_type: c_uint,
    src_host: *const c_void,
    src_device: u64,
    src_array: *mut c_void,
    src_pitch: usize,
    dst_x_in_bytes: usize,
    dst_y: usize,
    dst_memory_type: c_uint,
    dst_host: *mut c_void,
    dst_device: u64,
    dst_array: *mut c_void,
    dst_pitch: usize,
    width_in_bytes: usize,
    height: usize,
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
unsafe extern "C" {
    fn cuMemcpy2DAsync_v2(
        copy: *const RawCudaMemcpy2D,
        stream: cust::sys::CUstream,
    ) -> cust::sys::CUresult;
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
#[inline(always)]
fn cuda_call(name: &str, result: cust::sys::CUresult) -> RuntimeResult<()> {
    match result {
        cust::sys::CUresult::CUDA_SUCCESS => Ok(()),
        err => Err(RuntimeError::Cuda(format!("{name}: {err:?}"))),
    }
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
pub fn register_host_region(ptr: *mut u8, len: usize) -> RuntimeResult<()> {
    // SAFETY: ptr/len describe a live host region for the duration of the transfer.
    unsafe {
        cuda_call(
            "cuMemHostRegister_v2",
            cust::sys::cuMemHostRegister_v2(ptr.cast(), len, 0 as c_uint),
        )
    }
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
pub fn unregister_host_region(ptr: *mut u8) -> RuntimeResult<()> {
    // SAFETY: ptr was previously registered with CUDA.
    unsafe {
        cuda_call(
            "cuMemHostUnregister",
            cust::sys::cuMemHostUnregister(ptr.cast()),
        )
    }
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
pub fn memcpy_host_to_device_async(
    dst_device_ptr: u64,
    src: *const u8,
    len: usize,
    stream: &Stream,
) -> RuntimeResult<()> {
    // SAFETY: caller guarantees pointers and length are valid for an HtoD copy,
    // and `stream` belongs to the current CUDA context.
    unsafe {
        cuda_call(
            "cuMemcpyHtoDAsync_v2",
            cust::sys::cuMemcpyHtoDAsync_v2(dst_device_ptr, src.cast(), len, stream.as_inner()),
        )
    }
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
pub fn memcpy_host_to_device_2d_async(
    dst_device_ptr: u64,
    dst_pitch: usize,
    src: *const u8,
    src_pitch: usize,
    width_in_bytes: usize,
    height: usize,
    stream: &Stream,
) -> RuntimeResult<()> {
    let copy = RawCudaMemcpy2D {
        src_x_in_bytes: 0,
        src_y: 0,
        src_memory_type: CU_MEMORYTYPE_HOST,
        src_host: src.cast(),
        src_device: 0,
        src_array: std::ptr::null_mut(),
        src_pitch,
        dst_x_in_bytes: 0,
        dst_y: 0,
        dst_memory_type: CU_MEMORYTYPE_DEVICE,
        dst_host: std::ptr::null_mut(),
        dst_device: dst_device_ptr,
        dst_array: std::ptr::null_mut(),
        dst_pitch,
        width_in_bytes,
        height,
    };
    // SAFETY: caller guarantees the source and destination rows are valid for
    // the requested 2D HtoD copy and that `stream` belongs to the current CUDA
    // context.
    unsafe {
        cuda_call(
            "cuMemcpy2DAsync_v2",
            cuMemcpy2DAsync_v2(&copy, stream.as_inner()),
        )
    }
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
pub fn create_event() -> RuntimeResult<RawCudaEvent> {
    // SAFETY: CUDA initializes the event handle.
    unsafe {
        let mut event = std::ptr::null_mut();
        cuda_call(
            "cuEventCreate",
            cust::sys::cuEventCreate(&mut event, 0 as c_uint),
        )?;
        Ok(event)
    }
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
pub fn destroy_event(event: RawCudaEvent) -> RuntimeResult<()> {
    // SAFETY: event was created by CUDA and is no longer used after destruction.
    unsafe { cuda_call("cuEventDestroy_v2", cust::sys::cuEventDestroy_v2(event)) }
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
pub fn record_event(event: RawCudaEvent, stream: &Stream) -> RuntimeResult<()> {
    // SAFETY: event and stream belong to the current CUDA context.
    unsafe {
        cuda_call(
            "cuEventRecord",
            cust::sys::cuEventRecord(event, stream.as_inner()),
        )
    }
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
pub fn event_is_ready(event: RawCudaEvent) -> RuntimeResult<bool> {
    // SAFETY: event belongs to the current CUDA context.
    unsafe {
        match cust::sys::cuEventQuery(event) {
            cust::sys::CUresult::CUDA_SUCCESS => Ok(true),
            cust::sys::CUresult::CUDA_ERROR_NOT_READY => Ok(false),
            err => Err(RuntimeError::Cuda(format!("cuEventQuery: {err:?}"))),
        }
    }
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
pub fn synchronize_event(event: RawCudaEvent) -> RuntimeResult<()> {
    // SAFETY: event belongs to the current CUDA context.
    unsafe { cuda_call("cuEventSynchronize", cust::sys::cuEventSynchronize(event)) }
}

#[cfg(all(feature = "cuda", target_os = "linux"))]
pub fn stream_wait_event(stream_ptr: u64, event: RawCudaEvent) -> RuntimeResult<()> {
    // SAFETY: stream_ptr is provided by the caller as a live CUDA stream for the
    // current context, and event belongs to the same CUDA context.
    unsafe {
        cuda_call(
            "cuStreamWaitEvent",
            cust::sys::cuStreamWaitEvent(stream_ptr as cust::sys::CUstream, event, 0 as c_uint),
        )
    }
}
