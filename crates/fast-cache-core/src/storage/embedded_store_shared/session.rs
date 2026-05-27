use super::*;

impl<const SHARDS: usize> SharedEmbeddedStore<SHARDS> {
    /// Returns a borrowed session value guard.
    #[inline(always)]
    pub fn get_session(&self, session_prefix: &[u8], key: &[u8]) -> Option<Ref<'_>> {
        let route = self.route_session(session_prefix);
        let key_hash = hash_key(key);
        let guard = self.stripe(route.shard_id).read();
        let value = guard.get_session_ref_hashed_shared_no_ttl(session_prefix, key_hash, key)?
            as *const [u8];
        Some(Ref {
            guard,
            value,
            _not_send: PhantomData,
        })
    }

    /// Inserts or replaces a session-scoped value without a TTL.
    #[inline(always)]
    pub fn set_session(&self, session_prefix: &[u8], key: &[u8], value: &[u8]) {
        let route = self.route_session(session_prefix);
        let key_hash = hash_key(key);
        self.stripe(route.shard_id)
            .write()
            .set_session_slice_hashed_no_ttl(session_prefix, key_hash, key, value);
    }
}
