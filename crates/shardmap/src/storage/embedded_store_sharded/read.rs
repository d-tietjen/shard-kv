use super::*;

impl WorkerLocalEmbeddedStore {
    /// Returns a borrowed value slice for `key` without copying bytes.
    ///
    /// The slice is tied to this worker-local store's exclusive `&mut self`
    /// borrow. Use [`Self::get`] when an owned `Vec<u8>` is required.
    pub fn get_ref<'a>(&'a mut self, key: &[u8]) -> Option<&'a [u8]> {
        self.get_ref_if_local(key)
            .expect("worker-local embedded store key does not belong to this thread")
    }

    pub fn get_ref_if_local<'a>(
        &'a mut self,
        key: &[u8],
    ) -> Result<Option<&'a [u8]>, LocalRouteError> {
        let route = self.local_key_route(key)?;
        Ok(self.inner.local_get_ref_routed(route, key))
    }

    /// Returns a mutable point-key guard for `key`.
    ///
    /// The guard is tied to this worker's exclusive `&mut self` borrow, and
    /// replacements preserve the entry's existing TTL.
    pub fn get_mut<'a>(&'a mut self, key: &[u8]) -> Option<WorkerLocalEmbeddedRefMut<'a>> {
        self.get_mut_if_local(key)
            .expect("worker-local embedded store key does not belong to this thread")
    }

    pub fn get_mut_if_local<'a>(
        &'a mut self,
        key: &[u8],
    ) -> Result<Option<WorkerLocalEmbeddedRefMut<'a>>, LocalRouteError> {
        let route = self.local_key_route(key)?;
        Ok(self.inner.local_get_mut_routed(route, key))
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Bytes> {
        self.get_if_local(key)
            .expect("worker-local embedded store key does not belong to this thread")
    }

    pub fn get_if_local(&mut self, key: &[u8]) -> Result<Option<Bytes>, LocalRouteError> {
        self.local_key_route(key)?;
        Ok(self.inner.local_get(key))
    }

    pub fn get_with_governance_filter<F>(&mut self, key: &[u8], authorize: F) -> Option<Bytes>
    where
        F: FnOnce(Option<&[u8]>) -> bool,
    {
        self.get_with_governance_filter_if_local(key, authorize)
            .expect("worker-local embedded store key does not belong to this thread")
    }

    pub fn get_with_governance_filter_if_local<F>(
        &mut self,
        key: &[u8],
        authorize: F,
    ) -> Result<Option<Bytes>, LocalRouteError>
    where
        F: FnOnce(Option<&[u8]>) -> bool,
    {
        self.local_key_route(key)?;
        Ok(self.inner.local_get_with_governance_filter(key, authorize))
    }

    pub fn get_view<'a>(&'a mut self, key: &[u8]) -> WorkerLocalReadView<'a> {
        self.get_view_if_local(key)
            .expect("worker-local embedded store key does not belong to this thread")
    }

    #[inline(always)]
    pub fn get_owned_view_local(&mut self, key: &[u8]) -> OwnedEmbeddedReadView {
        self.inner.get_view(key)
    }

    #[inline(always)]
    pub fn get_owned_view_routed_local(
        &mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
    ) -> OwnedEmbeddedReadView {
        debug_assert!(self.owns_shard(route.shard_id));
        OwnedEmbeddedReadView::from_item(self.inner.local_get_slice_routed(route, key))
    }

    pub fn get_view_if_local<'a>(
        &'a mut self,
        key: &[u8],
    ) -> Result<WorkerLocalReadView<'a>, LocalRouteError> {
        let route = self.local_key_route(key)?;
        Ok(WorkerLocalReadView {
            item: self
                .inner
                .local_get_slice_routed(route, key)
                .map(WorkerLocalReadSlice::from_embedded),
        })
    }

    #[inline(always)]
    pub fn get_view_routed_local<'a>(
        &'a mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
    ) -> WorkerLocalReadView<'a> {
        debug_assert!(self.owns_shard(route.shard_id));
        WorkerLocalReadView {
            item: self
                .inner
                .local_get_slice_routed(route, key)
                .map(WorkerLocalReadSlice::from_embedded),
        }
    }

    #[inline(always)]
    pub fn get_ref_routed_local<'a>(
        &'a mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
    ) -> Option<&'a [u8]> {
        debug_assert!(self.owns_shard(route.shard_id));
        self.inner.local_get_ref_routed(route, key)
    }

    #[inline(always)]
    pub fn get_view_routed_no_ttl<'a>(
        &'a mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
    ) -> WorkerLocalReadView<'a> {
        self.get_view_routed_no_ttl_local(route, key)
    }

    #[inline(always)]
    pub fn get_view_routed_no_ttl_if_local<'a>(
        &'a mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
    ) -> Result<WorkerLocalReadView<'a>, LocalRouteError> {
        self.ensure_local_route(route)?;
        Ok(WorkerLocalReadView {
            item: self
                .inner
                .local_get_slice_routed_no_ttl(route, key)
                .map(WorkerLocalReadSlice::from_embedded),
        })
    }

    #[inline(always)]
    pub fn get_point_view_routed_no_ttl_local<'a>(
        &'a mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
    ) -> WorkerLocalReadView<'a> {
        debug_assert!(self.owns_shard(route.shard_id));
        WorkerLocalReadView {
            item: self
                .inner
                .local_get_point_ref_routed_no_ttl(route, key)
                .map(WorkerLocalReadSlice::from_local_slice),
        }
    }

    #[inline(always)]
    pub fn get_point_ref_routed_no_ttl_local<'a>(
        &'a mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
    ) -> Option<&'a [u8]> {
        debug_assert!(self.owns_shard(route.shard_id));
        self.inner.local_get_point_ref_routed_no_ttl(route, key)
    }

    #[inline(always)]
    pub fn get_prepared_point_view_no_ttl_local<'a>(
        &'a mut self,
        prepared: &PreparedPointKey,
    ) -> WorkerLocalReadView<'a> {
        debug_assert!(self.owns_shard(prepared.route().shard_id));
        WorkerLocalReadView {
            item: self
                .inner
                .local_get_point_ref_prepared_routed_no_ttl(prepared)
                .map(WorkerLocalReadSlice::from_local_slice),
        }
    }

    #[inline(always)]
    pub fn get_prepared_point_ref_no_ttl_local<'a>(
        &'a mut self,
        prepared: &PreparedPointKey,
    ) -> Option<&'a [u8]> {
        debug_assert!(self.owns_shard(prepared.route().shard_id));
        self.inner
            .local_get_point_ref_prepared_routed_no_ttl(prepared)
    }

    #[inline(always)]
    pub fn get_view_routed_no_ttl_local<'a>(
        &'a mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
    ) -> WorkerLocalReadView<'a> {
        debug_assert!(self.owns_shard(route.shard_id));
        WorkerLocalReadView {
            item: self
                .inner
                .local_get_ref_routed_no_ttl(route, key)
                .map(WorkerLocalReadSlice::from_local_slice),
        }
    }

    #[inline(always)]
    pub fn get_ref_routed_no_ttl_if_local<'a>(
        &'a mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
    ) -> Result<Option<&'a [u8]>, LocalRouteError> {
        self.ensure_local_route(route)?;
        Ok(self.inner.local_get_ref_routed_no_ttl(route, key))
    }

    #[inline(always)]
    pub fn get_ref_routed_no_ttl_local<'a>(
        &'a mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
    ) -> Option<&'a [u8]> {
        debug_assert!(self.owns_shard(route.shard_id));
        self.inner.local_get_ref_routed_no_ttl(route, key)
    }

    pub fn batch_get(&mut self, keys: Vec<Bytes>) -> Vec<Option<Bytes>> {
        self.batch_get_if_local(keys)
            .expect("worker-local embedded store batch includes keys owned by another thread")
    }

    pub fn batch_get_if_local(
        &mut self,
        keys: Vec<Bytes>,
    ) -> Result<Vec<Option<Bytes>>, LocalRouteError> {
        self.ensure_local_keys(&keys)?;
        Ok(keys
            .into_iter()
            .map(|key| self.inner.local_get(&key))
            .collect::<Vec<_>>())
    }

    pub fn batch_get_view<'a>(&'a mut self, keys: &[Bytes]) -> WorkerLocalBatchReadView<'a> {
        self.batch_get_view_if_local(keys)
            .expect("worker-local embedded store batch includes keys owned by another thread")
    }

    #[inline(always)]
    pub fn batch_get_owned_view_local(&mut self, keys: &[Bytes]) -> OwnedEmbeddedBatchReadView {
        self.inner.batch_get_view(keys)
    }

    #[inline(always)]
    pub fn batch_get_owned_view_routed_local(
        &mut self,
        keys: &[(EmbeddedKeyRoute, Bytes)],
    ) -> OwnedEmbeddedBatchReadView {
        let items = keys
            .iter()
            .map(|(route, key)| {
                debug_assert!(self.owns_shard(route.shard_id));
                self.inner.local_get_slice_routed(*route, key)
            })
            .collect::<Vec<_>>();
        OwnedEmbeddedBatchReadView::from_items(items)
    }

    pub fn batch_get_view_if_local<'a>(
        &'a mut self,
        keys: &[Bytes],
    ) -> Result<WorkerLocalBatchReadView<'a>, LocalRouteError> {
        self.ensure_local_keys(keys)?;
        Ok(worker_local_batch_view_from_embedded(
            self.inner.local_batch_get_slices(keys),
        ))
    }

    pub fn batch_get_session_view<'a>(
        &'a mut self,
        session_prefix: &[u8],
        keys: &[Bytes],
    ) -> WorkerLocalSessionBatchView<'a> {
        self.batch_get_session_view_if_local(session_prefix, keys)
            .expect("worker-local embedded store session does not belong to this thread")
    }

    pub fn batch_get_session_view_if_local<'a>(
        &'a mut self,
        session_prefix: &[u8],
        keys: &[Bytes],
    ) -> Result<WorkerLocalSessionBatchView<'a>, LocalRouteError> {
        let key_hashes = keys
            .iter()
            .map(|key| crate::storage::hash_key(key))
            .collect::<Vec<_>>();
        self.batch_get_session_view_prehashed_if_local(session_prefix, keys, &key_hashes)
    }

    #[inline(always)]
    pub fn batch_get_session_owned_view_local(
        &mut self,
        session_prefix: &[u8],
        keys: &[Bytes],
    ) -> OwnedEmbeddedBatchReadView {
        self.inner.batch_get_session_view(session_prefix, keys)
    }

    #[inline(always)]
    pub fn batch_get_session_owned_view_routed_local(
        &mut self,
        route: EmbeddedSessionRoute,
        keys: &[Bytes],
    ) -> OwnedEmbeddedBatchReadView {
        debug_assert!(self.owns_shard(route.shard_id));
        self.inner.batch_get_session_view_routed_no_ttl(route, keys)
    }

    pub fn batch_get_session_view_prehashed_if_local<'a>(
        &'a mut self,
        session_prefix: &[u8],
        keys: &[Bytes],
        key_hashes: &[u64],
    ) -> Result<WorkerLocalSessionBatchView<'a>, LocalRouteError> {
        self.local_session_route(session_prefix)?;
        Ok(worker_local_batch_view_from_embedded(
            self.inner
                .local_batch_get_session_slices_prehashed(session_prefix, keys, key_hashes),
        ))
    }

    #[inline(always)]
    pub fn batch_get_session_owned_view_prehashed_local(
        &mut self,
        session_prefix: &[u8],
        keys: &[Bytes],
        key_hashes: &[u64],
    ) -> OwnedEmbeddedBatchReadView {
        self.inner
            .batch_get_session_view_prehashed(session_prefix, keys, key_hashes)
    }

    #[inline(always)]
    pub fn batch_get_session_owned_view_prehashed_routed_local(
        &mut self,
        route: EmbeddedSessionRoute,
        keys: &[Bytes],
        key_hashes: &[u64],
    ) -> OwnedEmbeddedBatchReadView {
        debug_assert!(self.owns_shard(route.shard_id));
        self.inner
            .batch_get_session_view_prehashed_routed_no_ttl(route, keys, key_hashes)
    }

    pub fn batch_get_session_packed(
        &mut self,
        session_prefix: &[u8],
        keys: &[Bytes],
    ) -> PackedBatch {
        self.batch_get_session_packed_if_local(session_prefix, keys)
            .expect("worker-local embedded store session does not belong to this thread")
    }

    pub fn batch_get_session_packed_if_local(
        &mut self,
        session_prefix: &[u8],
        keys: &[Bytes],
    ) -> Result<PackedBatch, LocalRouteError> {
        self.local_session_route(session_prefix)?;
        Ok(self.inner.batch_get_session_packed(session_prefix, keys))
    }

    #[inline(always)]
    pub fn batch_get_session_packed_routed_local(
        &mut self,
        route: EmbeddedSessionRoute,
        keys: &[Bytes],
    ) -> PackedBatch {
        debug_assert!(self.owns_shard(route.shard_id));
        self.inner
            .batch_get_session_packed_routed_no_ttl(route, keys)
    }

    pub fn batch_get_session_packed_view(
        &mut self,
        session_prefix: &[u8],
        keys: &[Bytes],
    ) -> Option<OwnedEmbeddedSessionPackedView> {
        self.batch_get_session_packed_view_if_local(session_prefix, keys)
            .expect("worker-local embedded store session does not belong to this thread")
    }

    pub fn batch_get_session_packed_view_if_local(
        &mut self,
        session_prefix: &[u8],
        keys: &[Bytes],
    ) -> Result<Option<OwnedEmbeddedSessionPackedView>, LocalRouteError> {
        let key_hashes = keys
            .iter()
            .map(|key| crate::storage::hash_key(key))
            .collect::<Vec<_>>();
        self.batch_get_session_packed_view_prehashed_if_local(session_prefix, keys, &key_hashes)
    }

    pub fn batch_get_session_packed_view_prehashed_if_local(
        &mut self,
        session_prefix: &[u8],
        keys: &[Bytes],
        key_hashes: &[u64],
    ) -> Result<Option<OwnedEmbeddedSessionPackedView>, LocalRouteError> {
        self.local_session_route(session_prefix)?;
        Ok(self
            .inner
            .batch_get_session_packed_view_prehashed(session_prefix, keys, key_hashes))
    }

    pub fn batch_get_packed(&mut self, keys: &[Bytes]) -> PackedBatch {
        self.batch_get_packed_if_local(keys)
            .expect("worker-local embedded store batch includes keys owned by another thread")
    }

    pub fn batch_get_packed_if_local(
        &mut self,
        keys: &[Bytes],
    ) -> Result<PackedBatch, LocalRouteError> {
        self.ensure_local_keys(keys)?;
        Ok(self.inner.batch_get_packed(keys))
    }
}
