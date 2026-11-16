//! Consuming ownership transitions and authenticated descriptor custody. No
//! ordinary point operation calls this module or reads these control words.
use super::*;
use std::time::Instant;

impl Cache {
    /// Consume the last active handle and park this participant for `operation`.
    /// First close service admission and return/drop all worker clones; borrowed
    /// guards must also finish. Waits at most `timeout` for existing handles.
    /// Refusal returns the original active Cache inside [`PrepareError`].
    ///
    /// ```compile_fail
    /// # use shardmap::succession::{Cache, Operation, Peer};
    /// # fn example(cache: Cache, peer: Peer) {
    /// let prepared = cache.prepare(Operation([0; 16]), peer, std::time::Duration::ZERO);
    /// let stale = cache.get(b"key"); // consumed; no per-read generation fence needed
    /// # }
    /// ```
    pub fn prepare(
        self,
        operation: Operation,
        recipient: Peer,
        timeout: Duration,
    ) -> Result<Prepared, PrepareError> {
        let prepare = || -> Result<(Credential, Readiness, u64), Error> {
            require_protected_process()?;
            if recipient.pid == 0 || recipient.pid == std::process::id() {
                return Err(Error::Unauthorized);
            }
            let started = Instant::now();
            while Arc::strong_count(&self.inner) != 1 {
                if started.elapsed() >= timeout {
                    return Err(Error::DrainTimeout);
                }
                std::thread::sleep(
                    Duration::from_micros(50).min(timeout.saturating_sub(started.elapsed())),
                );
            }
            let active = self.inner.generation;
            let prepared = active
                .checked_add(5)
                .filter(|n| n.checked_add(3).is_some())
                .ok_or(Error::Limit)?;
            let mut credential = Credential::new();
            getrandom::fill(credential.bytes_mut()).map_err(|e| io::Error::other(e.to_string()))?;
            let receipt = Readiness {
                operation,
                recipient,
                generation: prepared >> 2,
                backing: self.identity()?,
                options: self.inner.options,
                mapping_address: self.mapping_address() as u64,
                engine_abi: engine_abi(),
                memory_policy: self.memory_policy(),
            };
            let hash = authorization(&credential, &receipt)?;
            let header = self.inner.mapping.header();
            if header.state.load(Ordering::Acquire) != active {
                return Err(Error::Inactive);
            }
            // strong_count==1 is stable: no other safe handle/borrow remains to
            // clone this private Arc, and the API never exposes Weak handles.
            for (word, bytes) in header.authorization.iter().zip(hash.as_chunks::<8>().0) {
                word.store(u64::from_le_bytes(*bytes), Ordering::Relaxed);
            }
            header
                .state
                .compare_exchange(active, prepared, Ordering::Release, Ordering::Acquire)
                .map_err(|_| Error::Inactive)?;
            Ok((credential, receipt, prepared))
        };
        match prepare() {
            Ok((credential, receipt, _)) => Ok(Prepared {
                mapping: self.inner.mapping.clone(),
                receipt,
                credential,
                sent: false,
            }),
            Err(error) => Err(PrepareError { error, cache: self }),
        }
    }
}

/// Parked participant with no entry APIs. Retain it while the caller decides the
/// complete TLS/socket/cache/embedded-SQL bundle. Drop never guesses an abort.
pub struct Prepared {
    pub(super) mapping: Arc<Mapping>,
    pub(super) receipt: Readiness,
    pub(super) credential: Credential,
    sent: bool,
}
impl std::fmt::Debug for Prepared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Prepared")
            .field("receipt", &self.receipt)
            .finish_non_exhaustive()
    }
}
impl Prepared {
    /// Operation/recipient/generation-bound readiness, not a service commit.
    pub fn readiness(&self) -> Readiness {
        self.receipt
    }
    /// Required virtual base, for custody diagnostics while A has no active Cache.
    pub fn mapping_address(&self) -> usize {
        self.mapping.ptr.as_ptr() as usize
    }
    /// Send the descriptor and credential once, including partial-send failures.
    /// The peer PID/UID must be the supervisor-owned successor selected at prepare.
    pub fn send(&mut self, stream: &mut UnixStream) -> Result<(), Error> {
        require_protected_process()?;
        if self.sent {
            return Err(Error::Unauthorized);
        }
        if self.mapping.header().state.load(Ordering::Acquire)
            != (self.receipt.generation << 2) | PREPARED
        {
            return Err(Error::Inactive);
        }
        transport::authenticate(stream, self.receipt.recipient)?;
        self.sent = true;
        transport::send(
            stream,
            self.mapping.file.as_raw_fd(),
            &self.receipt,
            &self.credential,
        )
    }
    /// Resume A after the recorded global decision is ABORT before commit.
    /// Activation racing/completing in B makes this fail closed. The returned
    /// Cache is the sole new active handle; previous handles were consumed.
    pub fn abort(self) -> Result<Cache, Error> {
        require_protected_process()?;
        let expected = (self.receipt.generation << 2) | PREPARED;
        let resumed = expected.checked_add(3).ok_or(Error::Limit)?;
        let inner = Inner::new(self.mapping.clone(), self.receipt.options, resumed)?;
        self.mapping
            .header()
            .state
            .compare_exchange(expected, resumed, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::Inactive)?;
        Ok(Cache {
            inner: Arc::new(inner),
        })
    }
}

/// Authenticated mapping with no cache operations until an explicit activation.
pub struct Staged {
    mapping: Arc<Mapping>,
    receipt: Readiness,
}
impl Staged {
    /// Authenticate and stage the old owner's backing. No table traversal,
    /// index reconstruction or payload copying occurs during attachment.
    ///
    /// # Safety
    /// Both exact engine builds and the supervisor must obey exclusive custody:
    /// no raw/competing writes, untracked aliases, or forked active handles.
    /// Native hashbrown/allocator metadata must originate in the same qualified
    /// engine build. The fingerprint is a conservative compatibility check, not
    /// an ABI standard or validation of arbitrary native bytes. Admitted writable
    /// descriptor holders and privileged host administrators remain trusted.
    /// A crash during mutation is not recoverable by this API. Keep the service
    /// admission gate closed until all TLS/cache/SQL participants activate.
    pub unsafe fn receive(stream: &mut UnixStream, owner: Peer) -> Result<Self, Error> {
        require_protected_process()?;
        transport::authenticate(stream, owner)?;
        let (fd, receipt, credential) = transport::receive(stream)?;
        Self::attach(fd, receipt, &credential)
    }
    pub(super) fn attach(
        fd: OwnedFd,
        receipt: Readiness,
        credential: &[u8],
    ) -> Result<Self, Error> {
        require_protected_process()?;
        if receipt.recipient != Peer::current() {
            return Err(Error::Unauthorized);
        }
        // Refuse incompatible builds before mapping any native structures.
        if receipt.engine_abi != engine_abi() {
            return Err(Error::Profile);
        }
        let geometry = receipt.options.geometry()?;
        let address = usize::try_from(receipt.mapping_address).map_err(|_| Error::Profile)?;
        if address == 0 || address & 4095 != 0 || address.checked_add(geometry.len).is_none() {
            return Err(Error::Profile);
        }
        let file = File::from(fd);
        use std::os::unix::fs::PermissionsExt;
        let metadata = file.metadata()?;
        if metadata.permissions().mode() & 0o777 != 0o600 || metadata.len() != geometry.len as u64 {
            return Err(Error::Profile);
        }
        // SAFETY: integer-only query on an owned received descriptor.
        let seals = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GET_SEALS) };
        let required = libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL;
        if seals < 0 || seals & required != required {
            return Err(Error::Profile);
        }
        Self::validate(
            Arc::new(Mapping::map(
                file,
                geometry.len,
                address,
                receipt.memory_policy,
            )?),
            receipt,
            credential,
        )
    }
    pub(super) fn validate(
        mapping: Arc<Mapping>,
        receipt: Readiness,
        credential: &[u8],
    ) -> Result<Self, Error> {
        if !compatible(&mapping, &receipt)
            || mapping.identity(receipt.options.geometry()?.stride)? != receipt.backing
        {
            return Err(Error::Profile);
        }
        let expected = receipt
            .generation
            .checked_mul(4)
            .filter(|n| *n != 0)
            .ok_or(Error::Limit)?
            | PREPARED;
        let header = mapping.header();
        if header.state.load(Ordering::Acquire) != expected {
            return Err(Error::Inactive);
        }
        let mut expected_mac = [0; 32];
        for (word, bytes) in header
            .authorization
            .iter()
            .zip(expected_mac.as_chunks_mut::<8>().0)
        {
            *bytes = word.load(Ordering::Relaxed).to_le_bytes();
        }
        authorization_mac(credential, &receipt)?
            .verify_slice(&expected_mac)
            .map_err(|_| Error::Unauthorized)?;
        // Pin only authenticated backing, and before a recipient can announce
        // readiness. On failure the staged mapping drops; A can still abort.
        mapping.lock_if_required()?;
        if header.state.load(Ordering::Acquire) != expected {
            return Err(Error::Inactive);
        }
        Ok(Self { mapping, receipt })
    }
    /// Independently checked readiness, still without entry APIs.
    pub fn readiness(&self) -> Readiness {
        self.receipt
    }
    /// Required local mapping base; ordinary payload and native index pointers match A.
    pub fn mapping_address(&self) -> usize {
        self.mapping.ptr.as_ptr() as usize
    }
    /// Populate this authenticated mapping's read page tables before readiness.
    /// Optional control-plane preparation; never copies or rebuilds cached data.
    /// Locked backing is already resident. Unsupported kernels or resource
    /// failures return an I/O error; the caller may abort before global commit.
    pub fn prefault(&self) -> Result<(), Error> {
        if self.receipt.memory_policy == MemoryPolicy::Locked {
            return Ok(());
        }
        // SAFETY: complete authenticated live mapping; read population changes
        // only this process's page tables and never the shared payload bytes.
        if unsafe {
            libc::madvise(
                self.mapping.ptr.as_ptr().cast(),
                self.mapping.len,
                libc::MADV_POPULATE_READ,
            )
        } != 0
        {
            return Err(io::Error::last_os_error().into());
        }
        Ok(())
    }
    /// Activate only after recording global COMMIT for this exact operation and
    /// all TLS/socket/cache/SQL participants. Common admission stays closed until
    /// the entire bundle activates and A retires. This CAS commits only the cache.
    /// Lost acknowledgements never grant permission to restore stale A; reconcile
    /// the decision or use a qualified reverse handoff of B's latest state.
    pub fn activate(self, committed_operation: Operation) -> Result<Cache, Error> {
        require_protected_process()?;
        if self.receipt.recipient != Peer::current()
            || self.receipt.operation != committed_operation
        {
            return Err(Error::Unauthorized);
        }
        let active = self
            .receipt
            .generation
            .checked_mul(4)
            .filter(|n| *n != 0)
            .ok_or(Error::Limit)?;
        let inner = Inner::new(self.mapping.clone(), self.receipt.options, active)?;
        self.mapping
            .header()
            .state
            .compare_exchange(
                active | PREPARED,
                active,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| Error::Inactive)?;
        Ok(Cache {
            inner: Arc::new(inner),
        })
    }
}
