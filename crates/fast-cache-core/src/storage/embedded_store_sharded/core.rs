use super::*;

impl WorkerLocalEmbeddedStore {
    /// Wrap the shard bundle assigned to one worker.
    pub fn from_owned_worker(inner: OwnedEmbeddedWorkerShards) -> Self {
        Self { inner }
    }

    #[inline(always)]
    pub fn worker_shard_count(&self) -> usize {
        self.inner.worker_shard_count()
    }

    #[inline(always)]
    pub fn shard_count(&self) -> usize {
        self.inner.shard_count()
    }

    #[inline(always)]
    pub fn route_mode(&self) -> EmbeddedRouteMode {
        self.inner.route_mode()
    }

    #[inline(always)]
    pub fn owns_shard(&self, shard_id: usize) -> bool {
        self.inner.owns_shard(shard_id)
    }

    #[inline(always)]
    pub fn route_key(&self, key: &[u8]) -> EmbeddedKeyRoute {
        self.inner.route_key(key)
    }

    #[inline(always)]
    pub fn prepare_point_key(&self, key: &[u8]) -> PreparedPointKey {
        self.inner.prepare_point_key(key)
    }

    #[inline(always)]
    pub fn prepare_point_key_if_local(
        &self,
        key: &[u8],
    ) -> Result<PreparedPointKey, LocalRouteError> {
        let prepared = self.prepare_point_key(key);
        self.ensure_local_route(prepared.route())?;
        Ok(prepared)
    }

    #[inline(always)]
    pub fn route_session(&self, session_prefix: &[u8]) -> EmbeddedSessionRoute {
        self.inner.route_session(session_prefix)
    }

    #[inline(always)]
    pub fn key_is_local(&self, key: &[u8]) -> bool {
        self.owns_shard(self.route_key(key).shard_id)
    }

    #[inline(always)]
    pub fn session_is_local(&self, session_prefix: &[u8]) -> bool {
        self.owns_shard(self.route_session(session_prefix).shard_id)
    }

    pub fn stream_session_transfer<B, F>(
        &mut self,
        request: &CudaSessionTransferRequest,
        on_chunk: F,
    ) -> ControlFlow<B, CudaSessionTransferStats>
    where
        F: for<'a> FnMut(CudaSessionChunkEvent<'a>) -> ControlFlow<B>,
    {
        self.stream_session_transfer_if_local(request, on_chunk)
            .expect("worker-local embedded store session does not belong to this thread")
    }

    pub fn stream_session_transfer_if_local<B, F>(
        &mut self,
        request: &CudaSessionTransferRequest,
        mut on_chunk: F,
    ) -> Result<ControlFlow<B, CudaSessionTransferStats>, LocalRouteError>
    where
        F: for<'a> FnMut(CudaSessionChunkEvent<'a>) -> ControlFlow<B>,
    {
        self.local_session_route(request.session_prefix())?;
        let mut stats = CudaSessionTransferStats::default();
        for descriptor in request.chunks() {
            stats.requested_chunks += 1;
            let event = match self.inner.local_get_session_ref_hashed_no_ttl(
                request.session_prefix(),
                descriptor.key_hash(),
                descriptor.key(),
            ) {
                Some(value) => {
                    stats.hit_chunks += 1;
                    stats.transferred_bytes += value.len();
                    CudaSessionChunkEvent::Hit(CudaChunkTransferHit::new(
                        descriptor,
                        WorkerLocalReadSlice::from_local_slice(value),
                    ))
                }
                None => {
                    stats.missed_chunks += 1;
                    CudaSessionChunkEvent::Miss(descriptor)
                }
            };

            if let ControlFlow::Break(result) = on_chunk(event) {
                return Ok(ControlFlow::Break(result));
            }
        }
        Ok(ControlFlow::Continue(stats))
    }

    pub(super) fn local_key_route(&self, key: &[u8]) -> Result<EmbeddedKeyRoute, LocalRouteError> {
        let route = self.route_key(key);
        self.ensure_local_route(route)
    }

    #[inline(always)]
    pub(super) fn ensure_local_route(
        &self,
        route: EmbeddedKeyRoute,
    ) -> Result<EmbeddedKeyRoute, LocalRouteError> {
        if self.owns_shard(route.shard_id) {
            Ok(route)
        } else {
            Err(LocalRouteError::KeyNotLocal {
                shard_id: route.shard_id,
            })
        }
    }

    pub(super) fn local_session_route(
        &self,
        session_prefix: &[u8],
    ) -> Result<EmbeddedSessionRoute, LocalRouteError> {
        let route = self.route_session(session_prefix);
        if self.owns_shard(route.shard_id) {
            Ok(route)
        } else {
            Err(LocalRouteError::SessionNotLocal {
                shard_id: route.shard_id,
            })
        }
    }

    pub(super) fn ensure_local_keys(&self, keys: &[Bytes]) -> Result<(), LocalRouteError> {
        for key in keys {
            self.local_key_route(key)?;
        }
        Ok(())
    }
}
