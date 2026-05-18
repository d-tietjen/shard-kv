use super::*;

impl WorkerLocalEmbeddedStore {
    pub fn delete(&mut self, key: &[u8]) -> bool {
        self.delete_if_local(key)
            .expect("worker-local embedded store key does not belong to this thread")
    }

    pub fn delete_if_local(&mut self, key: &[u8]) -> Result<bool, LocalRouteError> {
        self.local_key_route(key)?;
        Ok(self.inner.local_delete(key))
    }

    pub fn exists(&mut self, key: &[u8]) -> bool {
        self.exists_if_local(key)
            .expect("worker-local embedded store key does not belong to this thread")
    }

    pub fn exists_if_local(&mut self, key: &[u8]) -> Result<bool, LocalRouteError> {
        self.local_key_route(key)?;
        Ok(self.inner.local_exists(key))
    }

    pub fn ttl_seconds(&mut self, key: &[u8]) -> i64 {
        self.ttl_seconds_if_local(key)
            .expect("worker-local embedded store key does not belong to this thread")
    }

    pub fn ttl_seconds_if_local(&mut self, key: &[u8]) -> Result<i64, LocalRouteError> {
        self.local_key_route(key)?;
        Ok(self.inner.local_ttl_seconds(key))
    }

    pub fn pttl_millis(&mut self, key: &[u8]) -> i64 {
        self.pttl_millis_if_local(key)
            .expect("worker-local embedded store key does not belong to this thread")
    }

    pub fn pttl_millis_if_local(&mut self, key: &[u8]) -> Result<i64, LocalRouteError> {
        self.local_key_route(key)?;
        Ok(self.inner.local_pttl_millis(key))
    }

    pub fn expire(&mut self, key: &[u8], expire_at_ms: u64) -> bool {
        self.expire_if_local(key, expire_at_ms)
            .expect("worker-local embedded store key does not belong to this thread")
    }

    pub fn expire_if_local(
        &mut self,
        key: &[u8],
        expire_at_ms: u64,
    ) -> Result<bool, LocalRouteError> {
        self.local_key_route(key)?;
        Ok(self.inner.local_expire(key, expire_at_ms))
    }

    pub fn persist(&mut self, key: &[u8]) -> bool {
        self.persist_if_local(key)
            .expect("worker-local embedded store key does not belong to this thread")
    }

    pub fn persist_if_local(&mut self, key: &[u8]) -> Result<bool, LocalRouteError> {
        self.local_key_route(key)?;
        Ok(self.inner.local_persist(key))
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn process_maintenance(&mut self) -> usize {
        self.inner.process_maintenance()
    }

    pub fn stats_snapshot(&self) -> (TierStatsSnapshot, TierStatsSnapshot, TierStatsSnapshot) {
        self.inner.local_tier_stats_snapshot()
    }
}
