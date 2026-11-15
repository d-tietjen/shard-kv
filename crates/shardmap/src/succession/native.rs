//! Fixed-address, native Swiss-table backing and ordinary sharded point operations.
//! Only the lifecycle module inspects ownership state. Tables and payloads stay
//! in the mapping; cache-padded reader-biased locks are recreated per owner.
use super::*;
use allocator_api2::alloc::{AllocError, Allocator};
use crossbeam_utils::CachePadded;
use hashbrown::HashTable;
use rblock::{RwLock, RwLockReadGuard};
use std::alloc::Layout;
use std::hash::BuildHasher;
use std::mem::{ManuallyDrop, align_of, size_of};
use std::ptr::NonNull;
use std::sync::atomic::AtomicBool;

include!(concat!(env!("OUT_DIR"), "/succession_abi.rs"));

pub(super) const MAX_SHARDS: usize = 64;
const DATA: usize = 16 * 1024;
const NONE: u32 = u32::MAX;

/// Geometry derived from checked public limits, never read from native pointers.
#[derive(Clone, Copy)]
pub(super) struct Geometry {
    pub shards: usize,
    pub quota: usize,
    pub stride: usize,
    pub index_bytes: usize,
    pub block_bytes: usize,
    pub len: usize,
}
impl Options {
    pub(super) fn geometry(self) -> Result<Geometry, Error> {
        if self.capacity == 0 || self.max_key_bytes == 0 || self.max_value_bytes == 0 {
            return Err(Error::Profile);
        }
        let shards = if self.capacity >= 256 { MAX_SHARDS } else { 1 };
        let quota = (self.capacity as usize).div_ceil(shards);
        let stride = SLOT_HEADER
            .checked_add(self.max_key_bytes as usize)
            .and_then(|n| n.checked_add(self.max_value_bytes as usize))
            .and_then(|n| n.checked_add(7))
            .map(|n| n & !7)
            .ok_or(Error::Limit)?;
        // Keep occupancy <= half the table's insertion capacity. hashbrown can
        // then clean tombstones in place during churn without allocator growth.
        let reserve = quota.checked_mul(2).ok_or(Error::Limit)?;
        let buckets = reserve
            .checked_mul(8)
            .and_then(|n| n.checked_add(6))
            .map(|n| (n / 7).max(4))
            .and_then(usize::checked_next_power_of_two)
            .ok_or(Error::Limit)?;
        let index_bytes = buckets
            .checked_mul(size_of::<Row>() + 1)
            .and_then(|n| n.checked_add(127))
            .map(|n| n & !63)
            .ok_or(Error::Limit)?;
        let block_bytes = stride
            .checked_mul(quota)
            .and_then(|n| n.checked_add(index_bytes))
            .and_then(|n| n.checked_add(63))
            .map(|n| n & !63)
            .ok_or(Error::Limit)?;
        let len = block_bytes
            .checked_mul(shards)
            .and_then(|n| n.checked_add(DATA))
            .filter(|n| *n <= MAX_BACKING)
            .ok_or(Error::Limit)?;
        Ok(Geometry {
            shards,
            quota,
            stride,
            index_bytes,
            block_bytes,
            len,
        })
    }
}

/// Immutable format/configuration plus atomic control-plane state. This prefix
/// contains no native table pointers and is checked before accessing native data.
#[repr(C)]
pub(super) struct Header {
    pub magic: u64,
    pub layout: u64,
    pub store: [u8; 16],
    pub options: OptionsRaw,
    pub base: u64,
    pub engine_abi: [u8; 32],
    pub table_size: u64,
    pub stripe_size: u64,
    pub row_size: u64,
    pub row_align: u64,
    pub seeds: [u64; 4],
    pub state: AtomicU64,
    pub authorization: [AtomicU64; 4],
}
/// Integer-only representation; no Rust option or enum is imported in the prefix.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct OptionsRaw {
    capacity: u32,
    key_bytes: u32,
    value_bytes: u32,
    reserved: u32,
    ttl: u64,
    has_ttl: u64,
}
impl From<Options> for OptionsRaw {
    fn from(o: Options) -> Self {
        Self {
            capacity: o.capacity,
            key_bytes: o.max_key_bytes,
            value_bytes: o.max_value_bytes,
            reserved: 0,
            ttl: o.default_ttl_ms.unwrap_or(0),
            has_ttl: u64::from(o.default_ttl_ms.is_some()),
        }
    }
}

/// One mapped allocation for the native bucket array. It can allocate once;
/// tables reserve twice their slot quota, so ordinary churn never grows them.
#[repr(C)]
struct Region {
    ptr: NonNull<u8>,
    len: usize,
    used: AtomicBool,
}
// SAFETY: allocation is private at creation, then only accessed under a shard
// lock by the unique active process. No general heap allocations are persisted.
unsafe impl Send for Region {}
// SAFETY: used is atomic; table mutation remains exclusively stripe-locked.
unsafe impl Sync for Region {}
// SAFETY: allocate honors size/alignment, yields disjoint per-shard storage once,
// and deallocation retains storage until the last mapping handle is dropped.
unsafe impl Allocator for Region {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if layout.size() > self.len
            || !(self.ptr.as_ptr() as usize).is_multiple_of(layout.align())
            || self.used.swap(true, Ordering::Relaxed)
        {
            return Err(AllocError);
        }
        Ok(NonNull::slice_from_raw_parts(self.ptr, layout.size()))
    }
    unsafe fn deallocate(&self, _ptr: NonNull<u8>, _layout: Layout) {}
}

/// Native key/value pointers and table metadata require the exact mapped base.
/// Offset diagnostics are calculated explicitly, outside ordinary reads.
#[repr(C)]
struct Row {
    hash: u64,
    key: NonNull<u8>,
    value: NonNull<u8>,
    key_len: usize,
    value_len: usize,
    deadline: Option<u64>,
    slot: u32,
}
type Table = HashTable<Row, Region>;
/// Header of one retained native table and intrusive free-slot list.
#[repr(C)]
struct Stripe {
    table: ManuallyDrop<Table>,
    free: u32,
    quota: u32,
}
const _: () = assert!(size_of::<Header>() <= ARENA);
const _: () = assert!(ARENA + MAX_SHARDS * size_of::<Stripe>() <= DATA);

/// Owned memfd mapping. Native table Drop must never free another process's data.
pub(super) struct Mapping {
    pub ptr: NonNull<u8>,
    pub len: usize,
    pub file: File,
}
// SAFETY: native pointers are valid only at the fixed base. Active access needs
// the process-local shard lock; consumed Cache ownership gates succession.
unsafe impl Send for Mapping {}
// SAFETY: the same unique-owner and stripe-lock invariant protects shared use.
unsafe impl Sync for Mapping {}
impl Mapping {
    pub fn map(file: File, len: usize, address: usize) -> Result<Self, Error> {
        let flags = libc::MAP_SHARED
            | if address == 0 {
                0
            } else {
                libc::MAP_FIXED_NOREPLACE
            };
        // SAFETY: bounded, size-sealed file; NOREPLACE never overwrites a mapping.
        let raw = unsafe {
            libc::mmap(
                address as *mut _,
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                flags,
                file.as_raw_fd(),
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(io::Error::last_os_error().into());
        }
        if raw.is_null() || (address != 0 && raw as usize != address) {
            // SAFETY: release only the mapping returned by this mmap call.
            unsafe {
                libc::munmap(raw, len);
            }
            return Err(Error::Profile);
        }
        Ok(Self {
            ptr: NonNull::new(raw.cast()).ok_or(Error::Profile)?,
            len,
            file,
        })
    }
    pub fn header(&self) -> &Header {
        // SAFETY: validated length/alignment. Prefix fields are integer values;
        // immutable after creation, with only atomic control words changing.
        unsafe { &*self.ptr.as_ptr().cast::<Header>() }
    }
    pub fn identity(&self, stride: usize) -> Result<Identity, Error> {
        use std::os::unix::fs::MetadataExt;
        let m = self.file.metadata()?;
        Ok(Identity {
            store: self.header().store,
            device: m.dev(),
            inode: m.ino(),
            bytes: self.len as u64,
            arena_offset: ARENA as u64,
            slot_stride: stride as u64,
        })
    }
    fn stripe(&self, index: usize) -> *mut Stripe {
        // SAFETY: only called for a checked active shard number, within metadata.
        unsafe { self.ptr.as_ptr().add(ARENA).cast::<Stripe>().add(index) }
    }
    fn bytes(&self, ptr: NonNull<u8>, len: usize) -> &[u8] {
        // SAFETY: pointers/lengths originate in bounded API writes, imported only
        // from a trusted exact engine. Caller retains active custody and a lock.
        unsafe { std::slice::from_raw_parts(ptr.as_ptr(), len) }
    }
}
impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: all local users and borrowed guards have dropped. Never run
        // table destructors or clear data that another process may still own.
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), self.len);
        }
    }
}

struct StripePointer(*mut Stripe);
// SAFETY: dereferences are under the associated local stripe lock and active custody.
unsafe impl Send for StripePointer {}
// SAFETY: read guards share immutable data; mutation requires a write guard.
unsafe impl Sync for StripePointer {}
pub(super) struct Inner {
    pub mapping: Arc<Mapping>,
    pub options: Options,
    pub geometry: Geometry,
    pub generation: u64,
    locks: [CachePadded<RwLock<StripePointer>>; MAX_SHARDS],
    hash: ahash::RandomState,
}
impl Inner {
    pub fn new(mapping: Arc<Mapping>, options: Options, generation: u64) -> Result<Self, Error> {
        let seeds = mapping.header().seeds;
        let geometry = options.geometry()?;
        Ok(Self {
            locks: std::array::from_fn(|i| {
                CachePadded::new(RwLock::new(StripePointer(mapping.stripe(i))))
            }),
            mapping,
            options,
            geometry,
            generation,
            hash: ahash::RandomState::with_seeds(seeds[0], seeds[1], seeds[2], seeds[3]),
        })
    }
    #[inline(always)]
    fn route(&self, key: &[u8]) -> (u64, usize) {
        let hash = crate::storage::hash_key(key);
        (
            hash,
            if self.geometry.shards == 1 {
                0
            } else {
                crate::storage::stripe_index(hash, crate::storage::shift_for(self.geometry.shards))
            },
        )
    }
    fn slot_offset(&self, shard: usize, slot: u32) -> usize {
        DATA + shard * self.geometry.block_bytes
            + self.geometry.index_bytes
            + slot as usize * self.geometry.stride
    }
    fn release(&self, shard: usize, state: &mut Stripe, slot: u32) {
        let offset = self.slot_offset(shard, slot);
        // SAFETY: exclusively locked, valid slot removed from the table. The
        // first eight bytes of each slot are reserved for this free-list link.
        unsafe {
            self.mapping
                .ptr
                .as_ptr()
                .add(offset)
                .cast::<u32>()
                .write(state.free);
        }
        state.free = slot;
    }
}

impl Cache {
    /// Create a bounded native-table cache before warming it. Protected process
    /// custody is required. Attachment supports only the exact engine build and
    /// the same available virtual range; private heap caches cannot be imported.
    pub fn create(options: Options) -> Result<Self, Error> {
        require_protected_process()?;
        let geometry = options.geometry()?;
        // SAFETY: static C name and supported flags, descriptor checked below.
        let fd = unsafe {
            libc::memfd_create(
                c"shardmap-succession".as_ptr(),
                libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error().into());
        }
        // SAFETY: fresh owned descriptor returned by memfd_create.
        let file = unsafe { File::from_raw_fd(fd) };
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        file.set_len(geometry.len as u64)?;
        // SAFETY: integer-only seals on the valid owned descriptor.
        if unsafe {
            libc::fcntl(
                fd,
                libc::F_ADD_SEALS,
                libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL,
            )
        } < 0
        {
            return Err(io::Error::last_os_error().into());
        }
        let mapping = Arc::new(Mapping::map(file, geometry.len, 0)?);
        let mut random = [0; 48];
        getrandom::fill(&mut random).map_err(|e| io::Error::other(e.to_string()))?;
        let header = Header {
            magic: MAGIC,
            layout: LAYOUT,
            store: random[..16].try_into().map_err(|_| Error::Profile)?,
            options: options.into(),
            base: mapping.ptr.as_ptr() as u64,
            engine_abi: ENGINE_ABI,
            table_size: size_of::<Table>() as u64,
            stripe_size: size_of::<Stripe>() as u64,
            row_size: size_of::<Row>() as u64,
            row_align: align_of::<Row>() as u64,
            seeds: std::array::from_fn(|i| {
                let mut word = [0; 8];
                word.copy_from_slice(&random[16 + i * 8..24 + i * 8]);
                u64::from_le_bytes(word)
            }),
            state: AtomicU64::new(4),
            authorization: std::array::from_fn(|_| AtomicU64::new(0)),
        };
        // SAFETY: unexposed newly zeroed mapping, aligned prefix fits in ARENA.
        unsafe {
            mapping.ptr.as_ptr().cast::<Header>().write(header);
        }
        for shard in 0..geometry.shards {
            let start = DATA + shard * geometry.block_bytes;
            // SAFETY: checked disjoint geometry; no references yet exist.
            let ptr = unsafe { NonNull::new_unchecked(mapping.ptr.as_ptr().add(start)) };
            let region = Region {
                ptr,
                len: geometry.index_bytes,
                used: AtomicBool::new(false),
            };
            let mut table = Table::new_in(region);
            let quota = options.capacity as usize / geometry.shards
                + usize::from(shard < options.capacity as usize % geometry.shards);
            table
                .try_reserve(quota * 2, |row| row.hash)
                .map_err(|_| Error::Limit)?;
            for slot in 0..quota {
                let offset = start + geometry.index_bytes + slot * geometry.stride;
                let next = if slot + 1 == quota {
                    NONE
                } else {
                    slot as u32 + 1
                };
                // SAFETY: fixed disjoint aligned slot header inside mapping.
                unsafe {
                    mapping.ptr.as_ptr().add(offset).cast::<u32>().write(next);
                }
            }
            // SAFETY: initialized native table in its permanent mapped location.
            unsafe {
                mapping.stripe(shard).write(Stripe {
                    table: ManuallyDrop::new(table),
                    free: 0,
                    quota: quota as u32,
                });
            }
        }
        Ok(Self {
            inner: Arc::new(Inner::new(mapping, options, 4)?),
        })
    }
    /// Stable retained backing identity; no entry operation or ownership check.
    pub fn identity(&self) -> Result<Identity, Error> {
        self.inner.mapping.identity(self.inner.geometry.stride)
    }
    /// Required local virtual base. Native tables cannot relocate on attachment.
    pub fn mapping_address(&self) -> usize {
        self.inner.mapping.ptr.as_ptr() as usize
    }
    /// Bounded profile, including the total slot budget split across shards.
    pub fn options(&self) -> Options {
        self.inner.options
    }
    /// Number of routed shards (one for small profiles, otherwise 64).
    pub fn shard_count(&self) -> usize {
        self.inner.geometry.shards
    }
    /// Quota for a shard, or None for an invalid shard number. No dynamic spill.
    pub fn shard_capacity(&self, shard: usize) -> Option<usize> {
        (shard < self.shard_count()).then(|| {
            self.options().capacity as usize / self.shard_count()
                + usize::from(shard < self.options().capacity as usize % self.shard_count())
        })
    }
    /// Explicitly inspect (slot offset, payload offset) for a borrowed value.
    /// Refuses a value from another cache. This diagnostic does not run in GET.
    pub fn value_offsets(&self, value: &Value<'_>) -> Result<(u64, u64), Error> {
        let offset = (value.as_ptr() as usize)
            .checked_sub(self.mapping_address())
            .ok_or(Error::Profile)?;
        let slot = offset
            .checked_sub(SLOT_HEADER + self.inner.options.max_key_bytes as usize)
            .ok_or(Error::Profile)?;
        let relative = slot.checked_sub(DATA).ok_or(Error::Profile)?;
        let shard = relative / self.inner.geometry.block_bytes;
        let in_block = relative % self.inner.geometry.block_bytes;
        let in_slots = in_block
            .checked_sub(self.inner.geometry.index_bytes)
            .ok_or(Error::Profile)?;
        if !in_slots.is_multiple_of(self.inner.geometry.stride)
            || self
                .shard_capacity(shard)
                .is_none_or(|quota| in_slots / self.inner.geometry.stride >= quota)
            || offset
                .checked_add(value.len())
                .is_none_or(|end| end > self.inner.mapping.len)
        {
            return Err(Error::Profile);
        }
        Ok((slot as u64, offset as u64))
    }
    /// Borrow directly under one reader-biased shard lock. No admission, epoch,
    /// capability, global lock or IPC work is added to this ordinary operation.
    #[inline]
    pub fn get(&self, key: &[u8]) -> Result<Option<Value<'_>>, Error> {
        if key.len() > self.inner.options.max_key_bytes as usize {
            return Err(Error::Limit);
        }
        let (hash, shard) = self.inner.route(key);
        let guard = self.inner.locks[shard].read();
        // SAFETY: locked native metadata, active Cache only exists in one process.
        let state = unsafe { &*guard.0 };
        let row = state.table.find(self.inner.hash.hash_one(hash), |row| {
            row.hash == hash && self.inner.mapping.bytes(row.key, row.key_len) == key
        });
        let Some(row) = row else {
            return Ok(None);
        };
        if row
            .deadline
            .is_some_and(|deadline| deadline <= crate::storage::ttl_now_millis())
        {
            return Ok(None);
        }
        let bytes = self.inner.mapping.bytes(row.value, row.value_len);
        Ok(Some(Value {
            _guard: guard,
            bytes,
        }))
    }
    /// Insert/update within one shard's fixed quota. Full shards may reclaim
    /// expired entries; live entries are not evicted or moved between shards.
    /// TTL follows the ordinary engine's cached Unix-ms clock; None uses default.
    pub fn insert(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<(), Error> {
        if key.len() > self.inner.options.max_key_bytes as usize
            || value.len() > self.inner.options.max_value_bytes as usize
        {
            return Err(Error::Limit);
        }
        let ttl = ttl
            .map(|ttl| u64::try_from(ttl.as_millis()).map_err(|_| Error::Limit))
            .transpose()?
            .or(self.inner.options.default_ttl_ms);
        let deadline = ttl
            .map(|ttl| {
                crate::storage::ttl_now_millis()
                    .checked_add(ttl)
                    .ok_or(Error::Limit)
            })
            .transpose()?;
        let (hash, shard) = self.inner.route(key);
        let mut guard = self.inner.locks[shard].write();
        // SAFETY: unique local writer of this shard under active process custody.
        let state = unsafe { &mut *guard.0 };
        let table_hash = self.inner.hash.hash_one(hash);
        if let Some(row) = state.table.find_mut(table_hash, |row| {
            row.hash == hash && self.inner.mapping.bytes(row.key, row.key_len) == key
        }) {
            // SAFETY: bounded payload reservation; no active read guard. An input
            // borrowed from this shard cannot coexist with its write guard.
            unsafe {
                std::ptr::copy_nonoverlapping(value.as_ptr(), row.value.as_ptr(), value.len());
            }
            row.value_len = value.len();
            row.deadline = deadline;
            return Ok(());
        }
        if state.free == NONE {
            let now = crate::storage::ttl_now_millis();
            let mut free = state.free;
            state.table.retain(|row| {
                if row.deadline.is_some_and(|deadline| deadline <= now) {
                    let offset = self.inner.slot_offset(shard, row.slot);
                    // SAFETY: removing a valid expired row while shard-exclusive.
                    unsafe {
                        self.inner
                            .mapping
                            .ptr
                            .as_ptr()
                            .add(offset)
                            .cast::<u32>()
                            .write(free);
                    }
                    free = row.slot;
                    false
                } else {
                    true
                }
            });
            state.free = free;
        }
        if state.free == NONE {
            return Err(Error::Capacity);
        }
        let slot = state.free;
        let offset = self.inner.slot_offset(shard, slot);
        // SAFETY: valid exclusively held free-list head and bounded input sizes.
        unsafe {
            let base = self.inner.mapping.ptr.as_ptr();
            state.free = base.add(offset).cast::<u32>().read();
            std::ptr::copy_nonoverlapping(key.as_ptr(), base.add(offset + SLOT_HEADER), key.len());
            std::ptr::copy_nonoverlapping(
                value.as_ptr(),
                base.add(offset + SLOT_HEADER + self.inner.options.max_key_bytes as usize),
                value.len(),
            );
        }
        state.table.insert_unique(
            table_hash,
            Row {
                hash,
                // SAFETY: fixed, disjoint slot within the validated mapping.
                key: unsafe {
                    NonNull::new_unchecked(
                        self.inner.mapping.ptr.as_ptr().add(offset + SLOT_HEADER),
                    )
                },
                // SAFETY: bounded value reservation in the same live slot.
                value: unsafe {
                    NonNull::new_unchecked(
                        self.inner
                            .mapping
                            .ptr
                            .as_ptr()
                            .add(offset + SLOT_HEADER + self.inner.options.max_key_bytes as usize),
                    )
                },
                key_len: key.len(),
                value_len: value.len(),
                deadline,
                slot,
            },
            |row| self.inner.hash.hash_one(row.hash),
        );
        Ok(())
    }
    /// Remove a row and release its fixed allocation. Expired entries are removed
    /// too but return false, matching the API's live-entry result.
    pub fn remove(&self, key: &[u8]) -> Result<bool, Error> {
        if key.len() > self.inner.options.max_key_bytes as usize {
            return Err(Error::Limit);
        }
        let (hash, shard) = self.inner.route(key);
        let mut guard = self.inner.locks[shard].write();
        // SAFETY: unique shard writer in the active process.
        let state = unsafe { &mut *guard.0 };
        let Ok(entry) = state
            .table
            .find_entry(self.inner.hash.hash_one(hash), |row| {
                row.hash == hash && self.inner.mapping.bytes(row.key, row.key_len) == key
            })
        else {
            return Ok(false);
        };
        let (row, _) = entry.remove();
        self.inner.release(shard, state, row.slot);
        Ok(row
            .deadline
            .is_none_or(|deadline| deadline > crate::storage::ttl_now_millis()))
    }
}

/// Borrowed payload at stable backing offsets. The guard excludes same-shard
/// writes; the borrowed Cache handle prevents preparation until it is returned.
pub struct Value<'a> {
    _guard: RwLockReadGuard<'a, StripePointer>,
    bytes: &'a [u8],
}
impl Deref for Value<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.bytes
    }
}

/// Prefix validation happens before native table references are formed.
pub(super) fn compatible(mapping: &Mapping, receipt: &Readiness) -> bool {
    let h = mapping.header();
    h.magic == MAGIC
        && h.layout == LAYOUT
        && h.options == receipt.options.into()
        && h.base == receipt.mapping_address
        && h.engine_abi == ENGINE_ABI
        && receipt.engine_abi == ENGINE_ABI
        && h.table_size == size_of::<Table>() as u64
        && h.stripe_size == size_of::<Stripe>() as u64
        && h.row_size == size_of::<Row>() as u64
        && h.row_align == align_of::<Row>() as u64
}
pub(super) fn engine_abi() -> [u8; 32] {
    ENGINE_ABI
}
