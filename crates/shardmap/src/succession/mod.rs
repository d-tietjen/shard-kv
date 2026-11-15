//! Linux same-host cache succession, selected at cache creation.
//!
//! This is a bounded point-cache profile, not an importer for [`crate::SharedCache`].
//! A successor maps the existing index and payload arena; only descriptors and
//! control metadata travel over a private authenticated Unix socket. See
//! `examples/cache_succession.rs` and the crate README for the coordinator contract.

mod transport;

use std::ffi::CString;
use std::fs::File;
use std::io;
use std::ops::Deref;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::{RwLock, RwLockReadGuard};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use xxhash_rust::xxh3::xxh3_64_with_seed;

const MAGIC: u64 = 0x534d_4150_5355_4331;
const LAYOUT: u64 = 1;
const ARENA: usize = 4096;
const SLOT_HEADER: usize = 32;
const MAX_BACKING: usize = 1 << 30;
const PREPARED: u64 = 1;

/// Failure to admit an operation or transfer. No error implies rollback after commit.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// System call or private control-channel failure.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// The bounded point-cache profile or backing format is invalid.
    #[error("unsupported or malformed succession profile")]
    Profile,
    /// This handle is parked, staged, stale, or has lost ownership.
    #[error("cache admission is closed or ownership has changed")]
    Inactive,
    /// A borrowed view or mutation did not drain before the bound.
    #[error("cache operations did not drain before the deadline")]
    DrainTimeout,
    /// A recipient, credential, operation, or generation does not match.
    #[error("succession authorization failed")]
    Unauthorized,
    /// No free or expired slot remains; live entries are never implicitly evicted.
    #[error("succession cache is at its configured capacity")]
    Capacity,
    /// A key, value, deadline, or generation exceeds the supported bounds.
    #[error("succession cache limit exceeded")]
    Limit,
}

/// Explicit fixed-capacity profile. No Redis, vector, overflow, persistence,
/// eviction, or generic Rust object state is accepted by this API.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Options {
    /// Maximum number of live entries. Insertions refuse at this boundary.
    pub capacity: u32,
    /// Maximum key length, including zero-length keys.
    pub max_key_bytes: u32,
    /// Maximum value length.
    pub max_value_bytes: u32,
    /// Default TTL in milliseconds; `None` means no expiration.
    pub default_ttl_ms: Option<u64>,
}

impl Options {
    fn geometry(self) -> Result<(usize, usize), Error> {
        if self.capacity == 0 || self.max_key_bytes == 0 || self.max_value_bytes == 0 {
            return Err(Error::Profile);
        }
        let stride = SLOT_HEADER
            .checked_add(self.max_key_bytes as usize)
            .and_then(|n| n.checked_add(self.max_value_bytes as usize))
            .and_then(|n| n.checked_add(7))
            .map(|n| n & !7)
            .ok_or(Error::Limit)?;
        let len = stride
            .checked_mul(self.capacity as usize)
            .and_then(|n| n.checked_add(ARENA))
            .ok_or(Error::Limit)?;
        if len > MAX_BACKING {
            return Err(Error::Limit);
        }
        Ok((stride, len))
    }
}

/// Caller-assigned service-wide upgrade operation identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Operation(pub [u8; 16]);

/// Kernel-authenticated endpoint identity. The supervisor must obtain the PID
/// from its own child custody, not from an untrusted registration request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peer {
    /// Process ID on this host (PID namespaces must agree).
    pub pid: u32,
    /// Effective user ID.
    pub uid: u32,
}

impl Peer {
    /// Identity of the calling process.
    pub fn current() -> Self {
        // SAFETY: geteuid has no pointer arguments or preconditions.
        Self {
            pid: std::process::id(),
            uid: unsafe { libc::geteuid() },
        }
    }
}

/// Stable backing/slot identity for diagnostics; addresses are process-local.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    /// Random store identity, retained through every succession.
    pub store: [u8; 16],
    /// Kernel backing device.
    pub device: u64,
    /// Kernel backing inode.
    pub inode: u64,
    /// Fixed mapped byte length (also the reserved cache budget).
    pub bytes: u64,
    /// Index and payload arena starts here in the same backing object.
    pub arena_offset: u64,
    /// Fixed allocation per index slot, including key/value reservation.
    pub slot_stride: u64,
}

/// Readiness bound to the exact operation, recipient, profile and generation.
/// It is evidence for the external coordinator, not a global commit decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Readiness {
    /// Service-wide upgrade operation.
    pub operation: Operation,
    /// Intended new owner.
    pub recipient: Peer,
    /// New cache generation if committed.
    pub generation: u64,
    /// Existing backing identity.
    pub backing: Identity,
    /// Exact supported cache profile.
    pub options: Options,
}

// Only the control words are shared atomics. All slot access requires exclusive
// active ownership plus the process-local gate. No Rust lock or heap pointer is
// persisted in the transferable layout.
#[repr(C)]
struct Header {
    magic: u64,
    layout: u64,
    store: [u8; 16],
    capacity: u32,
    key_bytes: u32,
    value_bytes: u32,
    reserved: u32,
    default_ttl: u64,
    has_ttl: u64,
    seed: u64,
    state: AtomicU64,
    authorization: [AtomicU64; 4],
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct Slot {
    occupied: u32,
    key_len: u32,
    value_len: u32,
    reserved: u32,
    deadline_ms: u64,
    hash: u64,
}

struct Mapping {
    ptr: NonNull<u8>,
    len: usize,
    file: File,
}

// SAFETY: immutable header fields never change. Control words are atomic. Every
// access to entries is under Inner::gate and a unique active-generation check;
// prepare drains guards before publishing a new generation. Imported mappings
// require the trusted-process contract on Staged::receive.
unsafe impl Send for Mapping {}
// SAFETY: the same gate/ownership invariant covers all shared references.
unsafe impl Sync for Mapping {}

impl Mapping {
    fn map(file: File, len: usize) -> Result<Self, Error> {
        // SAFETY: a valid owned file, positive validated length, shared RW mapping.
        // The caller seals size changes before mapping. Lifetime ends in Drop.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error().into());
        }
        let Some(ptr) = NonNull::new(ptr.cast()) else {
            // SAFETY: even a successful zero-address mapping must be released;
            // the Rust representation intentionally requires a non-null base.
            unsafe {
                libc::munmap(ptr, len);
            }
            return Err(Error::Profile);
        };
        Ok(Self { ptr, len, file })
    }
    fn header(&self) -> &Header {
        // SAFETY: mmap is page-aligned, Header fits in ARENA, and initialized
        // exactly once before descriptor publication. Only atomic fields mutate.
        unsafe { &*self.ptr.as_ptr().cast::<Header>() }
    }
    fn identity(&self, stride: usize) -> Result<Identity, Error> {
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
}
impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: last mapping owner has dropped, so no local guards remain.
        // munmap does not destroy entries or alter another process's mapping.
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), self.len);
        }
    }
}

// Restore local admission on a failure before readiness was published. After
// publication (or an ownership change), errors and Drop must leave A parked.
struct AdmissionPause<'a> {
    inner: &'a Inner,
    active: u64,
}
impl Drop for AdmissionPause<'_> {
    fn drop(&mut self) {
        if self.inner.mapping.header().state.load(Ordering::Acquire) == self.active {
            self.inner.accepting.store(true, Ordering::Release);
        }
    }
}

struct Inner {
    mapping: Mapping,
    options: Options,
    stride: usize,
    gate: RwLock<()>,
    accepting: AtomicBool,
    active: AtomicU64,
}

impl Inner {
    fn admit(&self) -> Result<(), Error> {
        if !self.accepting.load(Ordering::Acquire)
            || self.mapping.header().state.load(Ordering::Acquire)
                != self.active.load(Ordering::Acquire)
        {
            return Err(Error::Inactive);
        }
        Ok(())
    }
    fn offset(&self, slot: usize) -> usize {
        ARENA + slot * self.stride
    }
    fn slot(&self, slot: usize) -> Result<Slot, Error> {
        // SAFETY: slot index is bounded by Options::capacity, the mapping length
        // is checked against geometry, and caller holds the gate after admit().
        let s = unsafe {
            self.mapping
                .ptr
                .as_ptr()
                .add(self.offset(slot))
                .cast::<Slot>()
                .read()
        };
        if s.occupied > 2
            || s.key_len > self.options.max_key_bytes
            || s.value_len > self.options.max_value_bytes
            || s.reserved != 0
        {
            return Err(Error::Profile);
        }
        Ok(s)
    }
    fn bytes(&self, offset: usize, len: usize) -> &[u8] {
        // SAFETY: all offsets/lengths derive from validated geometry and Slot;
        // the caller retains the gate and active ownership for the borrow.
        unsafe { std::slice::from_raw_parts(self.mapping.ptr.as_ptr().add(offset), len) }
    }
    fn find(&self, key: &[u8], now: u64) -> Result<(Option<usize>, Option<usize>), Error> {
        if key.len() > self.options.max_key_bytes as usize {
            return Err(Error::Limit);
        }
        let hash = xxh3_64_with_seed(key, self.mapping.header().seed);
        let n = self.options.capacity as usize;
        let mut free = None;
        for i in 0..n {
            let index = ((hash % n as u64) as usize + i) % n;
            let s = self.slot(index)?;
            let expired = s.deadline_ms != 0 && s.deadline_ms <= now;
            if s.occupied != 1 || expired {
                free = free.or(Some(index));
            }
            if s.occupied == 0 {
                break;
            }
            if s.occupied == 1
                && s.hash == hash
                && self.bytes(self.offset(index) + SLOT_HEADER, s.key_len as usize) == key
            {
                return Ok(((!expired).then_some(index), Some(index)));
            }
        }
        Ok((None, free))
    }
}

/// Cloneable handle to one active handoff-capable cache. Cloning a handle does
/// not clone the cache. All handles in A stop admitting operations on prepare.
#[derive(Clone)]
pub struct Cache {
    inner: Arc<Inner>,
}

impl Cache {
    /// Create sealed, anonymous retained backing. No path or named shared-memory
    /// object is published. This must be used before warming the old service.
    pub fn create(options: Options) -> Result<Self, Error> {
        let (stride, len) = options.geometry()?;
        let name = CString::new("shardmap-succession").map_err(|_| Error::Profile)?;
        // SAFETY: valid NUL-terminated name and supported flags; fd checked below.
        let fd = unsafe {
            libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING)
        };
        if fd < 0 {
            return Err(io::Error::last_os_error().into());
        }
        // SAFETY: memfd_create returned a fresh owned descriptor.
        let file = unsafe { File::from_raw_fd(fd) };
        file.set_len(len as u64)?;
        // SAFETY: fd is valid, integer seals protect size for every mapping.
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
        let mapping = Mapping::map(file, len)?;
        let mut random = [0u8; 24];
        getrandom::fill(&mut random).map_err(|e| io::Error::other(e.to_string()))?;
        let header = Header {
            magic: MAGIC,
            layout: LAYOUT,
            store: random[..16].try_into().map_err(|_| Error::Profile)?,
            capacity: options.capacity,
            key_bytes: options.max_key_bytes,
            value_bytes: options.max_value_bytes,
            reserved: 0,
            default_ttl: options.default_ttl_ms.unwrap_or(0),
            has_ttl: u64::from(options.default_ttl_ms.is_some()),
            seed: u64::from_le_bytes(random[16..].try_into().map_err(|_| Error::Profile)?),
            state: AtomicU64::new(4),
            authorization: std::array::from_fn(|_| AtomicU64::new(0)),
        };
        // SAFETY: private newly zero-filled mapping is sufficiently large/aligned;
        // no references to Header or entries exist before initialization.
        unsafe {
            mapping.ptr.as_ptr().cast::<Header>().write(header);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                mapping,
                options,
                stride,
                gate: RwLock::new(()),
                accepting: AtomicBool::new(true),
                active: AtomicU64::new(4),
            }),
        })
    }

    /// Existing backing identity (no entry access).
    pub fn identity(&self) -> Result<Identity, Error> {
        self.inner.mapping.identity(self.inner.stride)
    }
    /// Process-local mapping base, for offset-identity diagnostics only.
    pub fn mapping_address(&self) -> usize {
        self.inner.mapping.ptr.as_ptr() as usize
    }
    /// Profile selected at creation.
    pub fn options(&self) -> Options {
        self.inner.options
    }

    /// Borrow the retained value directly. Holding this guard prevents prepare
    /// from completing. No owned-result fallback or per-operation IPC is used.
    pub fn get(&self, key: &[u8]) -> Result<Option<Value<'_>>, Error> {
        let guard = self.inner.gate.read();
        self.inner.admit()?;
        let (found, _) = self.inner.find(key, now_ms()?)?;
        let Some(slot) = found else {
            return Ok(None);
        };
        let s = self.inner.slot(slot)?;
        let offset =
            self.inner.offset(slot) + SLOT_HEADER + self.inner.options.max_key_bytes as usize;
        Ok(Some(Value {
            _guard: guard,
            bytes: self.inner.bytes(offset, s.value_len as usize),
            slot_offset: self.inner.offset(slot) as u64,
            value_offset: offset as u64,
        }))
    }

    /// Insert/update a point key. Ordinary writes copy caller bytes into their
    /// fixed slot; transfer/attachment never copies or relocates existing entries.
    /// `None` selects the configured default TTL. Zero TTL expires immediately.
    pub fn insert(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<(), Error> {
        let _guard = self.inner.gate.write();
        self.inner.admit()?;
        if value.len() > self.inner.options.max_value_bytes as usize {
            return Err(Error::Limit);
        }
        let now = now_ms()?;
        let ttl = ttl
            .map(|n| u64::try_from(n.as_millis()).map_err(|_| Error::Limit))
            .transpose()?
            .or(self.inner.options.default_ttl_ms);
        let deadline = ttl
            .map(|n| now.checked_add(n).ok_or(Error::Limit))
            .transpose()?
            .map(|n| n.max(1))
            .unwrap_or(0);
        let (found, free) = self.inner.find(key, now)?;
        let index = found.or(free).ok_or(Error::Capacity)?;
        let offset = self.inner.offset(index);
        let hash = xxh3_64_with_seed(key, self.inner.mapping.header().seed);
        // SAFETY: exclusive gate and active ownership, key/value sizes checked,
        // fixed disjoint slot ranges. Input cannot alias mutable arena bytes:
        // any Value borrow would hold a read guard and prevent this write gate.
        unsafe {
            let base = self.inner.mapping.ptr.as_ptr();
            std::ptr::copy_nonoverlapping(key.as_ptr(), base.add(offset + SLOT_HEADER), key.len());
            std::ptr::copy_nonoverlapping(
                value.as_ptr(),
                base.add(offset + SLOT_HEADER + self.inner.options.max_key_bytes as usize),
                value.len(),
            );
            base.add(offset).cast::<Slot>().write(Slot {
                occupied: 1,
                key_len: key.len() as u32,
                value_len: value.len() as u32,
                reserved: 0,
                deadline_ms: deadline,
                hash,
            });
        }
        Ok(())
    }

    /// Delete a live key; returns whether a live entry was removed.
    pub fn remove(&self, key: &[u8]) -> Result<bool, Error> {
        let _guard = self.inner.gate.write();
        self.inner.admit()?;
        let (found, _) = self.inner.find(key, now_ms()?)?;
        let Some(index) = found else {
            return Ok(false);
        };
        // SAFETY: exclusive gate and active ownership, bounded aligned slot.
        unsafe {
            self.inner
                .mapping
                .ptr
                .as_ptr()
                .add(self.inner.offset(index))
                .cast::<Slot>()
                .write(Slot {
                    occupied: 2,
                    ..Slot::default()
                });
        }
        Ok(true)
    }

    /// Close local admission, drain all guards/mutations within `timeout`, and
    /// park A. The returned participant cannot commit. Dropping it leaves A
    /// parked: an uncertain global decision must never implicitly roll back.
    pub fn prepare(
        &self,
        operation: Operation,
        recipient: Peer,
        timeout: Duration,
    ) -> Result<Prepared, Error> {
        if recipient.pid == 0 || recipient.pid == std::process::id() {
            return Err(Error::Unauthorized);
        }
        self.inner
            .accepting
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::Inactive)?;
        let Some(_guard) = self.inner.gate.try_write_for(timeout) else {
            self.inner.accepting.store(true, Ordering::Release);
            return Err(Error::DrainTimeout);
        };
        let active = self.inner.active.load(Ordering::Acquire);
        let _pause = AdmissionPause {
            inner: &self.inner,
            active,
        };
        let prepared = active
            .checked_add(5)
            .filter(|n| n.checked_add(3).is_some())
            .ok_or(Error::Limit)?;
        let mut credential = [0; 32];
        getrandom::fill(&mut credential).map_err(|e| io::Error::other(e.to_string()))?;
        let receipt = Readiness {
            operation,
            recipient,
            generation: prepared >> 2,
            backing: self.identity()?,
            options: self.inner.options,
        };
        let hash = authorization(&credential, &receipt)?;
        let h = self.inner.mapping.header();
        if h.state.load(Ordering::Acquire) != active {
            return Err(Error::Inactive);
        }
        for (word, bytes) in h.authorization.iter().zip(hash.as_chunks::<8>().0) {
            word.store(u64::from_le_bytes(*bytes), Ordering::Relaxed);
        }
        h.state
            .compare_exchange(active, prepared, Ordering::Release, Ordering::Acquire)
            .map_err(|_| Error::Inactive)?;
        Ok(Prepared {
            inner: self.inner.clone(),
            receipt,
            credential,
            sent: false,
        })
    }
}

/// Borrowed value inside the retained allocation. Its process-local address may
/// change after exec; backing identity and offsets remain stable through attach.
pub struct Value<'a> {
    _guard: RwLockReadGuard<'a, ()>,
    bytes: &'a [u8],
    /// Index-slot offset within the backing.
    pub slot_offset: u64,
    /// Payload offset within the backing.
    pub value_offset: u64,
}
impl Deref for Value<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.bytes
    }
}

/// Parked cache participant owned by A. Keep A alive and the common service
/// admission gate closed until the external coordinator decides the whole bundle.
pub struct Prepared {
    inner: Arc<Inner>,
    receipt: Readiness,
    credential: [u8; 32],
    sent: bool,
}
impl Prepared {
    /// Candidate readiness, to compare with the recipient's independently checked receipt.
    pub fn readiness(&self) -> Readiness {
        self.receipt
    }
    /// Send one descriptor and a one-use credential to exactly the kernel peer
    /// named during prepare. A private supervisor-controlled channel is required.
    pub fn send(&mut self, stream: &mut UnixStream) -> Result<(), Error> {
        if self.sent {
            return Err(Error::Unauthorized);
        }
        if self.inner.mapping.header().state.load(Ordering::Acquire)
            != (self.receipt.generation << 2) | PREPARED
        {
            return Err(Error::Inactive);
        }
        transport::authenticate(stream, self.receipt.recipient)?;
        // Consume the send right before I/O: a partial transfer is ambiguous and
        // must not be retried as a second admission. Explicit abort remains possible.
        self.sent = true;
        transport::send(
            stream,
            self.inner.mapping.file.as_raw_fd(),
            &self.receipt,
            &self.credential,
        )
    }
    /// Resume A only after the coordinator has decided to abort before commit.
    /// A concurrent or completed B activation makes this fail closed.
    pub fn abort(self) -> Result<(), Error> {
        let _guard = self.inner.gate.write();
        let expected = (self.receipt.generation << 2) | PREPARED;
        let resumed = expected.checked_add(3).ok_or(Error::Limit)?;
        self.inner
            .mapping
            .header()
            .state
            .compare_exchange(expected, resumed, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::Inactive)?;
        self.inner.active.store(resumed, Ordering::Release);
        self.inner.accepting.store(true, Ordering::Release);
        Ok(())
    }
}
impl Drop for Prepared {
    fn drop(&mut self) {
        self.credential.fill(0);
    }
}

/// An authenticated mapped candidate, with no entry APIs until activation.
pub struct Staged {
    inner: Arc<Inner>,
    receipt: Readiness,
}
impl Staged {
    /// Receive a candidate from the expected old process on a private channel.
    /// Size seals, layout, profile, capability and recipient are verified before
    /// readiness. No index scan, payload copy, or cache operation occurs here.
    ///
    /// # Safety
    /// Both processes and the supervisor must be trusted to use this protocol
    /// exclusively, with no raw writes, other mmap aliases, forked active handles,
    /// or hostile descriptor holders. Peer credentials authenticate identity, not
    /// trustworthy code. An admitted writable mapping is not a sandbox and cannot
    /// be revoked by withdrawing a credential. A crash during mutation is not
    /// recoverable by this API. The old process must obey prepare/drain/retirement.
    pub unsafe fn receive(stream: &mut UnixStream, owner: Peer) -> Result<Self, Error> {
        transport::authenticate(stream, owner)?;
        let (fd, receipt, mut credential) = transport::receive(stream)?;
        let result = Self::attach(fd, receipt, &credential);
        credential.fill(0);
        result
    }
    fn attach(fd: OwnedFd, receipt: Readiness, credential: &[u8; 32]) -> Result<Self, Error> {
        if receipt.recipient != Peer::current() {
            return Err(Error::Unauthorized);
        }
        let (stride, len) = receipt.options.geometry()?;
        let file = File::from(fd);
        if file.metadata()?.len() != len as u64 {
            return Err(Error::Profile);
        }
        // SAFETY: a valid owned descriptor; check mandatory size seals before mmap.
        let seals = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GET_SEALS) };
        let required = libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL;
        if seals < 0 || seals & required != required {
            return Err(Error::Profile);
        }
        let mapping = Mapping::map(file, len)?;
        let h = mapping.header();
        if h.magic != MAGIC
            || h.layout != LAYOUT
            || h.reserved != 0
            || h.capacity != receipt.options.capacity
            || h.key_bytes != receipt.options.max_key_bytes
            || h.value_bytes != receipt.options.max_value_bytes
            || h.has_ttl != u64::from(receipt.options.default_ttl_ms.is_some())
            || h.default_ttl != receipt.options.default_ttl_ms.unwrap_or(0)
            || mapping.identity(stride)? != receipt.backing
        {
            return Err(Error::Profile);
        }
        let expected = (receipt.generation.checked_shl(2).ok_or(Error::Limit)?) | PREPARED;
        if receipt.generation > u64::MAX >> 2
            || receipt.generation == 0
            || h.state.load(Ordering::Acquire) != expected
        {
            return Err(Error::Inactive);
        }
        let hash = authorization(credential, &receipt)?;
        let mut mismatch = 0;
        for (word, bytes) in h.authorization.iter().zip(hash.as_chunks::<8>().0) {
            mismatch |= word.load(Ordering::Relaxed) ^ u64::from_le_bytes(*bytes);
        }
        if mismatch != 0 {
            return Err(Error::Unauthorized);
        }
        if h.state.load(Ordering::Acquire) != expected {
            return Err(Error::Inactive);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                mapping,
                options: receipt.options,
                stride,
                gate: RwLock::new(()),
                accepting: AtomicBool::new(false),
                active: AtomicU64::new(receipt.generation << 2),
            }),
            receipt,
        })
    }
    /// Validated participant receipt; this does not admit cache operations.
    pub fn readiness(&self) -> Readiness {
        self.receipt
    }
    /// Mapping base for offset-identity diagnostics; not a transferable pointer.
    pub fn mapping_address(&self) -> usize {
        self.inner.mapping.ptr.as_ptr() as usize
    }
    /// Activate only after the caller records the service-wide COMMIT decision
    /// for this operation, with every TLS/socket/embedded-SQL participant ready,
    /// A parked and common request admission still closed. Keep admission closed
    /// until all participants activate. This CAS only commits the cache participant.
    /// A must then retire before the supervisor reports the upgrade effective.
    ///
    /// On an ambiguous outcome keep admission closed and reconcile the coordinator
    /// decision; never infer abort from timeout. Once this succeeds, A cannot abort.
    /// Forward recovery or a fresh reverse handoff must use B's latest state.
    pub fn activate(self, committed_operation: Operation) -> Result<Cache, Error> {
        if committed_operation != self.receipt.operation {
            return Err(Error::Unauthorized);
        }
        let active = self.receipt.generation << 2;
        self.inner
            .mapping
            .header()
            .state
            .compare_exchange(
                active | PREPARED,
                active,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| Error::Inactive)?;
        self.inner.accepting.store(true, Ordering::Release);
        Ok(Cache { inner: self.inner })
    }
}

fn authorization(credential: &[u8; 32], receipt: &Readiness) -> Result<[u8; 32], Error> {
    let mut hash = Sha256::new();
    hash.update(b"shardmap-succession-v1");
    hash.update(credential);
    hash.update(serde_json::to_vec(receipt).map_err(io::Error::other)?);
    Ok(hash.finalize().into())
}
// Participants must share the host time namespace as well as PID namespace.
fn now_ms() -> Result<u64, Error> {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: valid writable timespec; BOOTTIME is host-wide and includes suspend,
    // so deadlines do not restart across process exec or interface replacement.
    if unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut time) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    Ok((time.tv_sec as u64).saturating_mul(1000) + time.tv_nsec as u64 / 1_000_000)
}

#[cfg(test)]
mod tests;
