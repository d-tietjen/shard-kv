//! Linux same-host cache succession, selected at cache creation.
//!
//! This is a bounded point-cache profile, not an importer for [`crate::SharedCache`].
//! A successor maps the existing index and payload arena; only descriptors and
//! control metadata travel over a private authenticated Unix socket. See
//! `examples/cache_succession.rs` and the crate README for the coordinator contract.

mod lifecycle;
mod native;
mod transport;

pub use lifecycle::{Prepared, Staged};
pub use native::Value;
use native::*;

use std::fs::File;
use std::io;
use std::ops::Deref;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use zeroize::Zeroizing;

const MAGIC: u64 = 0x534d_4150_5355_4331;
const LAYOUT: u64 = 4;
const ARENA: usize = 4096;
const SLOT_HEADER: usize = 8;
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
    /// The process permits unprivileged descriptor inspection/core dumps.
    #[error(
        "succession requires a non-dumpable process; call protect_current_process before creating or receiving backing"
    )]
    ProcessProtection,
    /// Requested cache residency could not be established; never downgraded.
    #[error("unable to lock succession backing in RAM: {0}")]
    MemoryLock(#[source] io::Error),
    /// A lifecycle operation refers to a parked, stale or changed ownership state.
    #[error("cache admission is closed or ownership has changed")]
    Inactive,
    /// Other active worker handles and their borrows did not drain before the bound.
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

/// Explicitly disable core dumps and unprivileged process/descriptor inspection
/// for this entire process, before creating or receiving cache backing.
///
/// This process-wide Linux policy also restricts debuggers and crash handlers.
/// Call it after final credential changes in each independently executed service;
/// never restore dumpability while cache mappings or descriptors are alive.
/// It does not sandbox privileged host administrators or trusted native code.
/// Cache creation/import refuse without this protection; ordinary cache operations
/// do not make a protection system call.
pub fn protect_current_process() -> Result<(), Error> {
    // SAFETY: integer-only process policy call; no pointers or borrowed buffers.
    if unsafe {
        libc::prctl(
            libc::PR_SET_DUMPABLE,
            0 as libc::c_ulong,
            0 as libc::c_ulong,
            0 as libc::c_ulong,
            0 as libc::c_ulong,
        )
    } != 0
    {
        return Err(io::Error::last_os_error().into());
    }
    require_protected_process()
}

fn require_protected_process() -> Result<(), Error> {
    // SAFETY: query-only prctl call with no pointer arguments.
    let state = unsafe {
        libc::prctl(
            libc::PR_GET_DUMPABLE,
            0 as libc::c_ulong,
            0 as libc::c_ulong,
            0 as libc::c_ulong,
            0 as libc::c_ulong,
        )
    };
    if state < 0 {
        return Err(io::Error::last_os_error().into());
    }
    if state != 0 {
        return Err(Error::ProcessProtection);
    }
    Ok(())
}

/// Residency policy authenticated as part of the cache's custody contract.
/// Locked applies only to this mapping, not application buffers or the process.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u64)]
pub enum MemoryPolicy {
    /// Normal pageable memory. Dump/fork mapping protections still apply.
    #[default]
    Pageable = 0,
    /// Every owner must successfully mlock the complete backing before readiness.
    /// Requires sufficient RAM and RLIMIT_MEMLOCK; failure never falls back.
    Locked = 1,
}

/// Verify kernel PID/UID on a connected Unix channel before pausing service
/// admission. Obtain the expected peer from supervisor-owned child custody.
/// This does not send backing, issue a credential or replace send/import checks.
/// Reject/close unregistered connections while A remains active.
pub fn authenticate_peer(stream: &UnixStream, expected: Peer) -> Result<(), Error> {
    transport::authenticate(stream, expected)
}

/// Fixed-length, heap-owned capability. Allocated before receiving secret bytes,
/// never resized, cloned or formatted; every success/error path erases its buffer.
struct Credential(Zeroizing<Vec<u8>>);
impl Credential {
    fn new() -> Self {
        Self(Zeroizing::new(vec![0; 32]))
    }
    fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}
impl Deref for Credential {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

/// Explicit fixed-capacity profile. No Redis, vector, overflow, persistence,
/// eviction, or generic Rust object state is accepted by this API.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Options {
    /// Total slot budget, divided into fixed shard quotas. A full routed shard
    /// refuses insertion even if another shard still has unused slots.
    pub capacity: u32,
    /// Maximum key length, including zero-length keys.
    pub max_key_bytes: u32,
    /// Maximum value length.
    pub max_value_bytes: u32,
    /// Default TTL in milliseconds; `None` means no expiration.
    pub default_ttl_ms: Option<u64>,
}

/// Caller-assigned service-wide upgrade operation identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Operation(pub [u8; 16]);

/// Exact engine-build fingerprint emitted by the succession build script.
/// Build identities pin a deployment's prior qualification to named artifacts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EngineBuildDigest(pub [u8; 32]);

impl EngineBuildDigest {
    /// Build fingerprint of the loaded ShardMap engine.
    pub fn current() -> Self {
        Self(engine_abi())
    }
}

/// Directional Format 4 qualification provisioned by a trusted supervisor.
/// Only the named source may transfer its original backing to the named target.
/// The supervisor must pin both executables and their qualification evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QualifiedBuildPair {
    source: EngineBuildDigest,
    target: EngineBuildDigest,
}

impl QualifiedBuildPair {
    /// Select two distinct exact digests from a matching Format 4 qualification.
    /// Compatibility must already be established for both builds and the profile
    /// in use. Importing with this pair requires [`Staged::receive_qualified`].
    pub fn new(source: EngineBuildDigest, target: EngineBuildDigest) -> Result<Self, Error> {
        if source == target {
            return Err(Error::Profile);
        }
        Ok(Self { source, target })
    }
}

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
    /// Required virtual address; attachment refuses rather than replacing a mapping.
    pub mapping_address: u64,
    /// Exact source build fingerprint, matching the backing creator.
    /// Target qualification comes from the supervisor's local configuration.
    pub engine_abi: [u8; 32],
    /// Both owners must establish this exact residency policy before readiness.
    pub memory_policy: MemoryPolicy,
}

/// Cloneable active cache handle. Cloning shares the same cache. Preparation
/// consumes the last handle after the caller stops admission and returns workers.
/// No handoff state is consulted by ordinary reads or writes.
#[derive(Clone)]
pub struct Cache {
    inner: Arc<Inner>,
}
impl std::fmt::Debug for Cache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache")
            .field("options", &self.inner.options)
            .field("mapping_address", &self.mapping_address())
            .finish_non_exhaustive()
    }
}

/// Failed preparation retains the original active handle. Nothing was published;
/// close application admission, return worker handles and retry, or resume work.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct PrepareError {
    /// Cause of refusal; the cache remains active in this process.
    pub error: Error,
    /// Original cache, including all acknowledged operations.
    pub cache: Cache,
}

fn authorization_mac(credential: &[u8], receipt: &Readiness) -> Result<Hmac<Sha256>, Error> {
    if credential.len() != 32 {
        return Err(Error::Unauthorized);
    }
    let mut mac = Hmac::<Sha256>::new_from_slice(credential).map_err(|_| Error::Unauthorized)?;
    mac.update(b"shardmap-succession-v4");
    mac.update(&serde_json::to_vec(receipt).map_err(io::Error::other)?);
    Ok(mac)
}

fn authorization(credential: &[u8], receipt: &Readiness) -> Result<[u8; 32], Error> {
    Ok(authorization_mac(credential, receipt)?
        .finalize()
        .into_bytes()
        .into())
}

#[cfg(test)]
mod tests;
