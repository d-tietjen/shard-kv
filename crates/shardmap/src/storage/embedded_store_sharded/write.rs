use super::*;

impl WorkerLocalEmbeddedStore {
    pub fn set(&mut self, key: Bytes, value: Bytes, ttl_ms: Option<u64>) {
        self.set_if_local(key, value, ttl_ms)
            .expect("worker-local embedded store key does not belong to this thread");
    }

    pub fn set_with_governance(
        &mut self,
        key: Bytes,
        value: Bytes,
        ttl_ms: Option<u64>,
        governance: Bytes,
    ) {
        self.set_with_governance_if_local(key, value, ttl_ms, governance)
            .expect("worker-local embedded store key does not belong to this thread");
    }

    pub fn set_slice_no_ttl(&mut self, key: &[u8], value: &[u8]) {
        self.set_slice_no_ttl_if_local(key, value)
            .expect("worker-local embedded store key does not belong to this thread");
    }

    #[inline(always)]
    pub fn set_slice_routed_no_ttl_local(
        &mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        value: &[u8],
    ) {
        debug_assert!(self.owns_shard(route.shard_id));
        self.inner.local_set_slice_no_ttl(route, key, value);
    }

    #[inline(always)]
    pub fn set_slice_routed_local(
        &mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
    ) {
        debug_assert!(self.owns_shard(route.shard_id));
        self.inner.local_set_slice(route, key, value, ttl_ms);
    }

    #[inline(always)]
    pub fn set_prepared_point_slice_no_ttl_local(
        &mut self,
        prepared: &PreparedPointKey,
        value: &[u8],
    ) {
        debug_assert!(self.owns_shard(prepared.route().shard_id));
        self.inner.local_set_prepared_slice_no_ttl(prepared, value);
    }

    pub fn set_slice_no_ttl_if_local(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), LocalRouteError> {
        let route = self.local_key_route(key)?;
        self.inner.local_set_slice_no_ttl(route, key, value);
        Ok(())
    }

    #[inline(always)]
    pub fn set_slice_routed_no_ttl_if_local(
        &mut self,
        route: EmbeddedKeyRoute,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), LocalRouteError> {
        self.ensure_local_route(route)?;
        self.inner.local_set_slice_no_ttl(route, key, value);
        Ok(())
    }

    pub fn set_if_local(
        &mut self,
        key: Bytes,
        value: Bytes,
        ttl_ms: Option<u64>,
    ) -> Result<(), LocalRouteError> {
        self.local_key_route(&key)?;
        self.inner.local_set(key, value, ttl_ms);
        Ok(())
    }

    pub fn set_with_governance_if_local(
        &mut self,
        key: Bytes,
        value: Bytes,
        ttl_ms: Option<u64>,
        governance: Bytes,
    ) -> Result<(), LocalRouteError> {
        self.local_key_route(&key)?;
        self.inner
            .local_set_with_governance(key, value, ttl_ms, governance);
        Ok(())
    }

    pub fn batch_set(&mut self, items: Vec<(Bytes, Bytes)>, ttl_ms: Option<u64>) {
        self.batch_set_if_local(items, ttl_ms)
            .expect("worker-local embedded store batch includes keys owned by another thread");
    }

    pub fn batch_set_if_local(
        &mut self,
        items: Vec<(Bytes, Bytes)>,
        ttl_ms: Option<u64>,
    ) -> Result<(), LocalRouteError> {
        for (key, _) in &items {
            self.local_key_route(key)?;
        }
        self.inner.local_batch_set(items, ttl_ms);
        Ok(())
    }

    pub fn batch_set_session_owned_no_ttl(
        &mut self,
        session_prefix: Bytes,
        items: Vec<(Bytes, Bytes)>,
    ) {
        self.batch_set_session_owned_no_ttl_if_local(session_prefix, items)
            .expect("worker-local embedded store session does not belong to this thread");
    }

    pub fn batch_set_session_owned_no_ttl_routed_local(
        &mut self,
        route: EmbeddedSessionRoute,
        session_prefix: Bytes,
        items: Vec<(Bytes, Bytes)>,
    ) {
        debug_assert!(self.owns_shard(route.shard_id));
        self.inner.local_batch_set_session_packed_routed_no_ttl(
            route,
            PackedSessionWrite::from_owned_items(session_prefix, items),
        );
    }

    pub fn batch_set_session_owned_no_ttl_if_local(
        &mut self,
        session_prefix: Bytes,
        items: Vec<(Bytes, Bytes)>,
    ) -> Result<(), LocalRouteError> {
        self.local_session_route(&session_prefix)?;
        self.inner
            .local_batch_set_session_owned_no_ttl(session_prefix, items);
        Ok(())
    }

    pub fn batch_set_session_packed_no_ttl(&mut self, packed: PackedSessionWrite) {
        self.batch_set_session_packed_no_ttl_if_local(packed)
            .expect("worker-local embedded store session does not belong to this thread");
    }

    pub fn batch_set_session_packed_no_ttl_routed_local(
        &mut self,
        route: EmbeddedSessionRoute,
        packed: PackedSessionWrite,
    ) {
        debug_assert!(self.owns_shard(route.shard_id));
        self.inner
            .local_batch_set_session_packed_routed_no_ttl(route, packed);
    }

    pub fn batch_set_session_packed_no_ttl_if_local(
        &mut self,
        packed: PackedSessionWrite,
    ) -> Result<(), LocalRouteError> {
        self.local_session_route(packed.session_prefix())?;
        self.inner.local_batch_set_session_packed_no_ttl(packed);
        Ok(())
    }
}
