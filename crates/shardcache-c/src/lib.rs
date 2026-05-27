#![allow(non_camel_case_types)]
#![doc = include_str!("../README.md")]

use bytes::Bytes as SharedBytes;
use shardmap::config::EvictionPolicy;
use shardmap::storage::{EmbeddedRouteMode, EmbeddedStore, PreparedPointKey};
use std::ffi::{c_char, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicU64, Ordering};

pub type shardcache_status_t = i32;

pub const SHARDCACHE_OK: shardcache_status_t = 0;
pub const SHARDCACHE_NOT_FOUND: shardcache_status_t = 1;
pub const SHARDCACHE_INVALID_ARGUMENT: shardcache_status_t = 2;
pub const SHARDCACHE_UNSUPPORTED: shardcache_status_t = 3;
pub const SHARDCACHE_PANIC: shardcache_status_t = 255;

pub const SHARDCACHE_OPTIONS_VERSION: u32 = 1;

pub const SHARDCACHE_EVICTION_NONE: u32 = 0;
pub const SHARDCACHE_EVICTION_LRU: u32 = 1;
pub const SHARDCACHE_EVICTION_LFU: u32 = 2;
pub const SHARDCACHE_EVICTION_PREFIX: u32 = 3;

pub const SHARDCACHE_ROUTE_FULL_KEY: u32 = 0;
pub const SHARDCACHE_ROUTE_SESSION_PREFIX: u32 = 1;

const DEFAULT_SHARD_COUNT: usize = 64;
static NEXT_DB_ID: AtomicU64 = AtomicU64::new(1);

#[repr(C)]
pub struct shardcache_db_t {
    _private: [u8; 0],
}

#[repr(C)]
pub struct shardcache_prepared_key_t {
    _private: [u8; 0],
}

struct ShardCacheDb {
    id: u64,
    store: EmbeddedStore,
}

struct ShardCachePreparedKey {
    owner_id: u64,
    prepared: PreparedPointKey,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct shardcache_options_t {
    pub version: u32,
    pub shard_count: usize,
    pub max_memory_bytes: usize,
    pub eviction_policy: u32,
    pub route_mode: u32,
    pub reserved: [u64; 4],
}

impl Default for shardcache_options_t {
    fn default() -> Self {
        Self {
            version: SHARDCACHE_OPTIONS_VERSION,
            shard_count: DEFAULT_SHARD_COUNT,
            max_memory_bytes: 0,
            eviction_policy: SHARDCACHE_EVICTION_NONE,
            route_mode: SHARDCACHE_ROUTE_FULL_KEY,
            reserved: [0; 4],
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct shardcache_slice_t {
    pub ptr: *const u8,
    pub len: usize,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct shardcache_set_item_t {
    pub key: shardcache_slice_t,
    pub value: shardcache_slice_t,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct shardcache_bytes_t {
    pub ptr: *const u8,
    pub len: usize,
    pub owner: *mut c_void,
}

impl Default for shardcache_bytes_t {
    fn default() -> Self {
        Self {
            ptr: ptr::null(),
            len: 0,
            owner: ptr::null_mut(),
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct shardcache_batch_t {
    pub values: *mut shardcache_bytes_t,
    pub len: usize,
    pub hit_count: usize,
    pub owner: *mut c_void,
}

impl Default for shardcache_batch_t {
    fn default() -> Self {
        Self {
            values: ptr::null_mut(),
            len: 0,
            hit_count: 0,
            owner: ptr::null_mut(),
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn shardcache_version() -> u32 {
    SHARDCACHE_OPTIONS_VERSION
}

#[unsafe(no_mangle)]
pub extern "C" fn shardcache_status_string(status: shardcache_status_t) -> *const c_char {
    match status {
        SHARDCACHE_OK => c"ok".as_ptr(),
        SHARDCACHE_NOT_FOUND => c"not found".as_ptr(),
        SHARDCACHE_INVALID_ARGUMENT => c"invalid argument".as_ptr(),
        SHARDCACHE_UNSUPPORTED => c"unsupported".as_ptr(),
        SHARDCACHE_PANIC => c"panic".as_ptr(),
        _ => c"unknown".as_ptr(),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shardcache_options_default(
    out_options: *mut shardcache_options_t,
) -> shardcache_status_t {
    ffi_status(|| {
        if out_options.is_null() {
            return SHARDCACHE_INVALID_ARGUMENT;
        }
        unsafe {
            *out_options = shardcache_options_t::default();
        }
        SHARDCACHE_OK
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shardcache_open(
    options: *const shardcache_options_t,
    out_db: *mut *mut shardcache_db_t,
) -> shardcache_status_t {
    ffi_status(|| {
        if out_db.is_null() {
            return SHARDCACHE_INVALID_ARGUMENT;
        }

        let options = unsafe { options.as_ref().copied() }.unwrap_or_default();
        let Some(shard_count) = normalized_shard_count(options.shard_count) else {
            unsafe {
                *out_db = ptr::null_mut();
            }
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        let Ok(route_mode) = route_mode_from_raw(options.route_mode) else {
            unsafe {
                *out_db = ptr::null_mut();
            }
            return SHARDCACHE_UNSUPPORTED;
        };
        let Ok(eviction_policy) = eviction_policy_from_raw(options.eviction_policy) else {
            unsafe {
                *out_db = ptr::null_mut();
            }
            return SHARDCACHE_UNSUPPORTED;
        };
        if options.version != 0 && options.version != SHARDCACHE_OPTIONS_VERSION {
            unsafe {
                *out_db = ptr::null_mut();
            }
            return SHARDCACHE_UNSUPPORTED;
        }

        let store = EmbeddedStore::with_route_mode(shard_count, route_mode);
        store.configure_memory_policy(
            per_shard_memory_limit(options.max_memory_bytes, shard_count),
            eviction_policy,
        );
        let db = Box::new(ShardCacheDb {
            id: NEXT_DB_ID.fetch_add(1, Ordering::Relaxed),
            store,
        });
        unsafe {
            *out_db = Box::into_raw(db).cast::<shardcache_db_t>();
        }
        SHARDCACHE_OK
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shardcache_close(db: *mut shardcache_db_t) {
    if !db.is_null() {
        unsafe {
            drop(Box::from_raw(db.cast::<ShardCacheDb>()));
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shardcache_prepare_key(
    db: *mut shardcache_db_t,
    key_ptr: *const u8,
    key_len: usize,
    out_prepared: *mut *mut shardcache_prepared_key_t,
) -> shardcache_status_t {
    ffi_status(|| {
        let Some(db) = (unsafe { db_ref(db) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        let Ok(key) = (unsafe { input_slice(key_ptr, key_len) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        if out_prepared.is_null() {
            return SHARDCACHE_INVALID_ARGUMENT;
        }
        let prepared = Box::new(ShardCachePreparedKey {
            owner_id: db.id,
            prepared: db.store.prepare_point_key(key),
        });
        unsafe {
            *out_prepared = Box::into_raw(prepared).cast::<shardcache_prepared_key_t>();
        }
        SHARDCACHE_OK
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shardcache_prepared_key_free(prepared: *mut shardcache_prepared_key_t) {
    if !prepared.is_null() {
        unsafe {
            drop(Box::from_raw(prepared.cast::<ShardCachePreparedKey>()));
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shardcache_set(
    db: *mut shardcache_db_t,
    key_ptr: *const u8,
    key_len: usize,
    value_ptr: *const u8,
    value_len: usize,
) -> shardcache_status_t {
    ffi_status(|| {
        let Some(db) = (unsafe { db_ref(db) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        let Ok(key) = (unsafe { input_slice(key_ptr, key_len) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        let Ok(value) = (unsafe { input_slice(value_ptr, value_len) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        db.store.set(key.to_vec(), value.to_vec(), None);
        SHARDCACHE_OK
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shardcache_set_ttl(
    db: *mut shardcache_db_t,
    key_ptr: *const u8,
    key_len: usize,
    value_ptr: *const u8,
    value_len: usize,
    ttl_ms: u64,
) -> shardcache_status_t {
    ffi_status(|| {
        let Some(db) = (unsafe { db_ref(db) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        let Ok(key) = (unsafe { input_slice(key_ptr, key_len) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        let Ok(value) = (unsafe { input_slice(value_ptr, value_len) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        let ttl_ms = (ttl_ms > 0).then_some(ttl_ms);
        db.store.set(key.to_vec(), value.to_vec(), ttl_ms);
        SHARDCACHE_OK
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shardcache_set_prepared(
    db: *mut shardcache_db_t,
    prepared: *const shardcache_prepared_key_t,
    value_ptr: *const u8,
    value_len: usize,
) -> shardcache_status_t {
    ffi_status(|| {
        let Some(db) = (unsafe { db_ref(db) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        let Some(prepared) = (unsafe { prepared_ref(prepared) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        if prepared.owner_id != db.id {
            return SHARDCACHE_INVALID_ARGUMENT;
        }
        let Ok(value) = (unsafe { input_slice(value_ptr, value_len) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        db.store.set_slice_prehashed(
            prepared.prepared.route().key_hash,
            prepared.prepared.key(),
            value,
            None,
        );
        SHARDCACHE_OK
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shardcache_get(
    db: *mut shardcache_db_t,
    key_ptr: *const u8,
    key_len: usize,
    out_bytes: *mut shardcache_bytes_t,
) -> shardcache_status_t {
    ffi_status(|| {
        let Some(db) = (unsafe { db_ref(db) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        let Ok(key) = (unsafe { input_slice(key_ptr, key_len) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        if out_bytes.is_null() {
            return SHARDCACHE_INVALID_ARGUMENT;
        }
        match db.store.get_value_bytes(key) {
            Some(bytes) => unsafe { write_bytes_out(out_bytes, bytes) },
            None => {
                unsafe {
                    clear_bytes_out(out_bytes);
                }
                SHARDCACHE_NOT_FOUND
            }
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shardcache_get_prepared(
    db: *mut shardcache_db_t,
    prepared: *const shardcache_prepared_key_t,
    out_bytes: *mut shardcache_bytes_t,
) -> shardcache_status_t {
    ffi_status(|| {
        let Some(db) = (unsafe { db_ref(db) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        let Some(prepared) = (unsafe { prepared_ref(prepared) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        if prepared.owner_id != db.id || out_bytes.is_null() {
            return SHARDCACHE_INVALID_ARGUMENT;
        }
        let view = db.store.get_prepared_view_no_ttl(&prepared.prepared);
        match view.slice() {
            Some(bytes) => unsafe {
                write_bytes_out(out_bytes, SharedBytes::copy_from_slice(bytes))
            },
            None => {
                unsafe {
                    clear_bytes_out(out_bytes);
                }
                SHARDCACHE_NOT_FOUND
            }
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shardcache_delete(
    db: *mut shardcache_db_t,
    key_ptr: *const u8,
    key_len: usize,
) -> shardcache_status_t {
    ffi_status(|| {
        let Some(db) = (unsafe { db_ref(db) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        let Ok(key) = (unsafe { input_slice(key_ptr, key_len) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        if db.store.delete(key) {
            SHARDCACHE_OK
        } else {
            SHARDCACHE_NOT_FOUND
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shardcache_batch_set(
    db: *mut shardcache_db_t,
    items_ptr: *const shardcache_set_item_t,
    items_len: usize,
) -> shardcache_status_t {
    ffi_status(|| {
        let Some(db) = (unsafe { db_ref(db) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        let Ok(items) = (unsafe { input_items(items_ptr, items_len) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        let mut owned = Vec::with_capacity(items.len());
        for item in items {
            let Ok(key) = (unsafe { input_slice(item.key.ptr, item.key.len) }) else {
                return SHARDCACHE_INVALID_ARGUMENT;
            };
            let Ok(value) = (unsafe { input_slice(item.value.ptr, item.value.len) }) else {
                return SHARDCACHE_INVALID_ARGUMENT;
            };
            owned.push((key.to_vec(), value.to_vec()));
        }
        db.store.batch_set(owned, None);
        SHARDCACHE_OK
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shardcache_batch_get(
    db: *mut shardcache_db_t,
    keys_ptr: *const shardcache_slice_t,
    keys_len: usize,
    out_batch: *mut shardcache_batch_t,
) -> shardcache_status_t {
    ffi_status(|| {
        let Some(db) = (unsafe { db_ref(db) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        let Ok(keys) = (unsafe { input_slices(keys_ptr, keys_len) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        if out_batch.is_null() {
            return SHARDCACHE_INVALID_ARGUMENT;
        }
        let mut owned_keys = Vec::with_capacity(keys.len());
        for key in keys {
            let Ok(key) = (unsafe { input_slice(key.ptr, key.len) }) else {
                return SHARDCACHE_INVALID_ARGUMENT;
            };
            owned_keys.push(key.to_vec());
        }

        let values = db.store.batch_get(owned_keys);
        unsafe { write_batch_out(out_batch, values) }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shardcache_session_set(
    db: *mut shardcache_db_t,
    session_ptr: *const u8,
    session_len: usize,
    key_ptr: *const u8,
    key_len: usize,
    value_ptr: *const u8,
    value_len: usize,
) -> shardcache_status_t {
    ffi_status(|| {
        let Some(db) = (unsafe { db_ref(db) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        let Ok(session) = (unsafe { input_slice(session_ptr, session_len) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        let Ok(key) = (unsafe { input_slice(key_ptr, key_len) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        let Ok(value) = (unsafe { input_slice(value_ptr, value_len) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        db.store
            .batch_set_session_slices_no_ttl(session, [(key, value)]);
        SHARDCACHE_OK
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shardcache_session_get(
    db: *mut shardcache_db_t,
    session_ptr: *const u8,
    session_len: usize,
    key_ptr: *const u8,
    key_len: usize,
    out_bytes: *mut shardcache_bytes_t,
) -> shardcache_status_t {
    ffi_status(|| {
        let Some(db) = (unsafe { db_ref(db) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        let Ok(session) = (unsafe { input_slice(session_ptr, session_len) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        let Ok(key) = (unsafe { input_slice(key_ptr, key_len) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        if out_bytes.is_null() {
            return SHARDCACHE_INVALID_ARGUMENT;
        }

        let keys = [key.to_vec()];
        match db.store.batch_get_session(session, &keys).pop().flatten() {
            Some(bytes) => unsafe { write_bytes_out(out_bytes, SharedBytes::from(bytes)) },
            None => {
                unsafe {
                    clear_bytes_out(out_bytes);
                }
                SHARDCACHE_NOT_FOUND
            }
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shardcache_contains(
    db: *mut shardcache_db_t,
    key_ptr: *const u8,
    key_len: usize,
    out_present: *mut bool,
) -> shardcache_status_t {
    ffi_status(|| {
        let Some(db) = (unsafe { db_ref(db) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        let Ok(key) = (unsafe { input_slice(key_ptr, key_len) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        if out_present.is_null() {
            return SHARDCACHE_INVALID_ARGUMENT;
        }
        unsafe {
            *out_present = db.store.get_value_bytes(key).is_some();
        }
        SHARDCACHE_OK
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shardcache_len(
    db: *mut shardcache_db_t,
    out_len: *mut usize,
) -> shardcache_status_t {
    ffi_status(|| {
        let Some(db) = (unsafe { db_ref(db) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        if out_len.is_null() {
            return SHARDCACHE_INVALID_ARGUMENT;
        }
        unsafe {
            *out_len = db.store.len();
        }
        SHARDCACHE_OK
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shardcache_stored_bytes(
    db: *mut shardcache_db_t,
    out_bytes: *mut usize,
) -> shardcache_status_t {
    ffi_status(|| {
        let Some(db) = (unsafe { db_ref(db) }) else {
            return SHARDCACHE_INVALID_ARGUMENT;
        };
        if out_bytes.is_null() {
            return SHARDCACHE_INVALID_ARGUMENT;
        }
        unsafe {
            *out_bytes = db.store.stored_bytes();
        }
        SHARDCACHE_OK
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shardcache_bytes_free(bytes: *mut shardcache_bytes_t) {
    if bytes.is_null() {
        return;
    }
    let bytes = unsafe { &mut *bytes };
    if !bytes.owner.is_null() {
        unsafe {
            drop(Box::from_raw(bytes.owner.cast::<SharedBytes>()));
        }
    }
    *bytes = shardcache_bytes_t::default();
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shardcache_batch_free(batch: *mut shardcache_batch_t) {
    if batch.is_null() {
        return;
    }
    let batch = unsafe { &mut *batch };
    if !batch.values.is_null() {
        for index in 0..batch.len {
            let item = unsafe { batch.values.add(index) };
            unsafe {
                shardcache_bytes_free(item);
            }
        }
        let raw = ptr::slice_from_raw_parts_mut(batch.values, batch.len);
        unsafe {
            drop(Box::from_raw(raw));
        }
    }
    *batch = shardcache_batch_t::default();
}

fn ffi_status(f: impl FnOnce() -> shardcache_status_t) -> shardcache_status_t {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or(SHARDCACHE_PANIC)
}

fn normalized_shard_count(shard_count: usize) -> Option<usize> {
    let shard_count = if shard_count == 0 {
        DEFAULT_SHARD_COUNT
    } else {
        shard_count
    };
    (shard_count > 0 && shard_count.is_power_of_two()).then_some(shard_count)
}

fn per_shard_memory_limit(max_memory_bytes: usize, shard_count: usize) -> Option<usize> {
    (max_memory_bytes > 0).then(|| max_memory_bytes.div_ceil(shard_count))
}

fn eviction_policy_from_raw(raw: u32) -> Result<EvictionPolicy, ()> {
    match raw {
        SHARDCACHE_EVICTION_NONE => Ok(EvictionPolicy::None),
        SHARDCACHE_EVICTION_LRU => Ok(EvictionPolicy::Lru),
        SHARDCACHE_EVICTION_LFU => Ok(EvictionPolicy::Lfu),
        #[cfg(feature = "prefix-eviction")]
        SHARDCACHE_EVICTION_PREFIX => Ok(EvictionPolicy::Prefix),
        #[cfg(not(feature = "prefix-eviction"))]
        SHARDCACHE_EVICTION_PREFIX => Err(()),
        _ => Err(()),
    }
}

fn route_mode_from_raw(raw: u32) -> Result<EmbeddedRouteMode, ()> {
    match raw {
        SHARDCACHE_ROUTE_FULL_KEY => Ok(EmbeddedRouteMode::FullKey),
        SHARDCACHE_ROUTE_SESSION_PREFIX => Ok(EmbeddedRouteMode::SessionPrefix),
        _ => Err(()),
    }
}

unsafe fn db_ref<'a>(db: *mut shardcache_db_t) -> Option<&'a ShardCacheDb> {
    unsafe { db.cast::<ShardCacheDb>().as_ref() }
}

unsafe fn prepared_ref<'a>(
    prepared: *const shardcache_prepared_key_t,
) -> Option<&'a ShardCachePreparedKey> {
    unsafe { prepared.cast::<ShardCachePreparedKey>().as_ref() }
}

unsafe fn input_slice<'a>(ptr: *const u8, len: usize) -> Result<&'a [u8], ()> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(());
    }
    Ok(unsafe { slice::from_raw_parts(ptr, len) })
}

unsafe fn input_slices<'a>(
    ptr: *const shardcache_slice_t,
    len: usize,
) -> Result<&'a [shardcache_slice_t], ()> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(());
    }
    Ok(unsafe { slice::from_raw_parts(ptr, len) })
}

unsafe fn input_items<'a>(
    ptr: *const shardcache_set_item_t,
    len: usize,
) -> Result<&'a [shardcache_set_item_t], ()> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(());
    }
    Ok(unsafe { slice::from_raw_parts(ptr, len) })
}

unsafe fn write_bytes_out(
    out_bytes: *mut shardcache_bytes_t,
    bytes: SharedBytes,
) -> shardcache_status_t {
    unsafe {
        *out_bytes = bytes_from_shared(bytes);
    }
    SHARDCACHE_OK
}

unsafe fn write_batch_out(
    out_batch: *mut shardcache_batch_t,
    values: Vec<Option<Vec<u8>>>,
) -> shardcache_status_t {
    let mut hit_count = 0usize;
    let mut items = Vec::with_capacity(values.len());
    for value in values {
        match value {
            Some(value) => {
                hit_count = hit_count.saturating_add(1);
                items.push(bytes_from_shared(SharedBytes::from(value)));
            }
            None => items.push(shardcache_bytes_t::default()),
        }
    }
    let mut boxed = items.into_boxed_slice();
    let values = boxed.as_mut_ptr();
    let len = boxed.len();
    unsafe {
        *out_batch = shardcache_batch_t {
            values,
            len,
            hit_count,
            owner: values.cast::<c_void>(),
        };
    }
    std::mem::forget(boxed);
    SHARDCACHE_OK
}

fn bytes_from_shared(bytes: SharedBytes) -> shardcache_bytes_t {
    let owner = Box::new(bytes);
    let ptr = owner.as_ptr();
    let len = owner.len();
    shardcache_bytes_t {
        ptr,
        len,
        owner: Box::into_raw(owner).cast::<c_void>(),
    }
}

unsafe fn clear_bytes_out(out_bytes: *mut shardcache_bytes_t) {
    unsafe {
        *out_bytes = shardcache_bytes_t::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn c_api_round_trips_point_keys() {
        unsafe {
            let mut options = shardcache_options_t::default();
            assert_eq!(shardcache_options_default(&mut options), SHARDCACHE_OK);
            options.shard_count = 4;

            let mut db = ptr::null_mut();
            assert_eq!(shardcache_open(&options, &mut db), SHARDCACHE_OK);
            assert!(!db.is_null());

            assert_eq!(
                shardcache_set(db, b"alpha".as_ptr(), 5, b"one".as_ptr(), 3),
                SHARDCACHE_OK
            );

            let mut out = shardcache_bytes_t::default();
            assert_eq!(
                shardcache_get(db, b"alpha".as_ptr(), 5, &mut out),
                SHARDCACHE_OK
            );
            assert_eq!(slice::from_raw_parts(out.ptr, out.len), b"one");
            shardcache_bytes_free(&mut out);
            assert!(out.ptr.is_null());

            assert_eq!(shardcache_delete(db, b"alpha".as_ptr(), 5), SHARDCACHE_OK);
            assert_eq!(
                shardcache_get(db, b"alpha".as_ptr(), 5, &mut out),
                SHARDCACHE_NOT_FOUND
            );

            shardcache_close(db);
        }
    }

    #[test]
    fn c_api_round_trips_prepared_keys() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(shardcache_open(ptr::null(), &mut db), SHARDCACHE_OK);

            let mut prepared = ptr::null_mut();
            assert_eq!(
                shardcache_prepare_key(db, b"hot-prefix".as_ptr(), 10, &mut prepared),
                SHARDCACHE_OK
            );
            assert!(!prepared.is_null());

            assert_eq!(
                shardcache_set_prepared(db, prepared, b"value".as_ptr(), 5),
                SHARDCACHE_OK
            );

            let mut out = shardcache_bytes_t::default();
            assert_eq!(
                shardcache_get_prepared(db, prepared, &mut out),
                SHARDCACHE_OK
            );
            assert_eq!(slice::from_raw_parts(out.ptr, out.len), b"value");
            shardcache_bytes_free(&mut out);

            shardcache_prepared_key_free(prepared);
            shardcache_close(db);
        }
    }

    #[test]
    fn c_api_batch_set_get_preserves_request_order() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(shardcache_open(ptr::null(), &mut db), SHARDCACHE_OK);

            let items = [
                shardcache_set_item_t {
                    key: shardcache_slice_t {
                        ptr: b"a".as_ptr(),
                        len: 1,
                    },
                    value: shardcache_slice_t {
                        ptr: b"one".as_ptr(),
                        len: 3,
                    },
                },
                shardcache_set_item_t {
                    key: shardcache_slice_t {
                        ptr: b"b".as_ptr(),
                        len: 1,
                    },
                    value: shardcache_slice_t {
                        ptr: b"two".as_ptr(),
                        len: 3,
                    },
                },
            ];
            assert_eq!(
                shardcache_batch_set(db, items.as_ptr(), items.len()),
                SHARDCACHE_OK
            );

            let keys = [
                shardcache_slice_t {
                    ptr: b"b".as_ptr(),
                    len: 1,
                },
                shardcache_slice_t {
                    ptr: b"missing".as_ptr(),
                    len: 7,
                },
                shardcache_slice_t {
                    ptr: b"a".as_ptr(),
                    len: 1,
                },
            ];
            let mut batch = shardcache_batch_t::default();
            assert_eq!(
                shardcache_batch_get(db, keys.as_ptr(), keys.len(), &mut batch),
                SHARDCACHE_OK
            );
            assert_eq!(batch.len, 3);
            assert_eq!(batch.hit_count, 2);
            let values = slice::from_raw_parts(batch.values, batch.len);
            assert_eq!(slice::from_raw_parts(values[0].ptr, values[0].len), b"two");
            assert!(values[1].ptr.is_null());
            assert_eq!(slice::from_raw_parts(values[2].ptr, values[2].len), b"one");
            shardcache_batch_free(&mut batch);
            assert!(batch.values.is_null());

            shardcache_close(db);
        }
    }

    #[test]
    fn c_api_rejects_invalid_arguments() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(
                shardcache_open(ptr::null(), ptr::null_mut()),
                SHARDCACHE_INVALID_ARGUMENT
            );

            let options = shardcache_options_t {
                shard_count: 3,
                ..shardcache_options_t::default()
            };
            assert_eq!(
                shardcache_open(&options, &mut db),
                SHARDCACHE_INVALID_ARGUMENT
            );
            assert!(db.is_null());
        }
    }

    #[cfg(feature = "prefix-eviction")]
    #[test]
    fn c_api_prefix_eviction_works_for_session_groups() {
        unsafe {
            let options = shardcache_options_t {
                shard_count: 1,
                max_memory_bytes: 12,
                eviction_policy: SHARDCACHE_EVICTION_PREFIX,
                route_mode: SHARDCACHE_ROUTE_SESSION_PREFIX,
                ..shardcache_options_t::default()
            };
            let mut db = ptr::null_mut();
            assert_eq!(shardcache_open(&options, &mut db), SHARDCACHE_OK);

            assert_eq!(
                shardcache_session_set(
                    db,
                    b"s:cold".as_ptr(),
                    6,
                    b"s:cold:c:0".as_ptr(),
                    10,
                    b"x".as_ptr(),
                    1,
                ),
                SHARDCACHE_OK
            );
            assert_eq!(
                shardcache_session_set(
                    db,
                    b"s:hot".as_ptr(),
                    5,
                    b"s:hot:c:0".as_ptr(),
                    9,
                    b"y".as_ptr(),
                    1,
                ),
                SHARDCACHE_OK
            );

            let mut out = shardcache_bytes_t::default();
            assert_eq!(
                shardcache_session_get(
                    db,
                    b"s:cold".as_ptr(),
                    6,
                    b"s:cold:c:0".as_ptr(),
                    10,
                    &mut out,
                ),
                SHARDCACHE_NOT_FOUND
            );
            assert_eq!(
                shardcache_session_get(
                    db,
                    b"s:hot".as_ptr(),
                    5,
                    b"s:hot:c:0".as_ptr(),
                    9,
                    &mut out,
                ),
                SHARDCACHE_OK
            );
            assert_eq!(slice::from_raw_parts(out.ptr, out.len), b"y");
            shardcache_bytes_free(&mut out);
            shardcache_close(db);
        }
    }
}
