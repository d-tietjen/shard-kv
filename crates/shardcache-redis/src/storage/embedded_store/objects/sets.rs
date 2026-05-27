use super::super::*;
use super::access::RedisObjectStoreAccess;

#[allow(dead_code)]
pub(crate) trait RedisSetStore {
    fn sadd(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult;
    fn srem(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult;
    fn sismember(&self, key: &[u8], member: &[u8]) -> RedisObjectResult;
    fn smismember(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult;
    fn scard(&self, key: &[u8]) -> RedisObjectResult;
    fn smembers(&self, key: &[u8]) -> RedisObjectResult;
    fn smembers_visit(
        &self,
        key: &[u8],
        emit: impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome;
    fn set_members(&self, key: &[u8]) -> Result<Vec<Bytes>, RedisObjectError>;
    fn spop(&self, key: &[u8], count: Option<usize>) -> RedisObjectResult;
    fn srandmember(&self, key: &[u8], count: Option<i64>) -> RedisObjectResult;
    fn srandmember_positive_visit(
        &self,
        key: &[u8],
        requested: usize,
        emit: impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome;
    fn sadd_hashed(&self, key_hash: u64, key: &[u8], members: &[&[u8]]) -> RedisObjectResult;
}

impl RedisSetStore for EmbeddedStore {
    fn sadd(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult {
        self.sadd_hashed(hash_key(key), key, members)
    }

    fn srem(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.srem(key, members))
    }

    fn sismember(&self, key: &[u8], member: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.sismember(key, member))
    }

    fn smismember(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.smismember(key, members))
    }

    fn scard(&self, key: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.scard(key))
    }

    fn smembers(&self, key: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.smembers(key))
    }

    fn smembers_visit(
        &self,
        key: &[u8],
        mut emit: impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome {
        self.object_read_hashed_visit(hash_key(key), key, |bucket| {
            bucket.smembers_visit(key, &mut emit)
        })
    }

    fn set_members(&self, key: &[u8]) -> Result<Vec<Bytes>, RedisObjectError> {
        let route = self.route_key(key);
        let bucket = self.objects.read_bucket(route.shard_id, route.key_hash);
        let result = bucket
            .set_members(key)
            .map_err(|()| RedisObjectError::WrongType);
        if result.is_err() || bucket.contains_object(key) {
            return result;
        }
        drop(bucket);
        if self.string_exists_routed(route, key) {
            Err(RedisObjectError::WrongType)
        } else {
            result
        }
    }

    fn spop(&self, key: &[u8], count: Option<usize>) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.spop(key, count))
    }

    fn srandmember(&self, key: &[u8], count: Option<i64>) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.srandmember(key, count))
    }

    fn srandmember_positive_visit(
        &self,
        key: &[u8],
        requested: usize,
        mut emit: impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome {
        self.object_read_hashed_visit(hash_key(key), key, |bucket| {
            bucket.srandmember_positive_visit(key, requested, &mut emit)
        })
    }

    fn sadd_hashed(&self, key_hash: u64, key: &[u8], members: &[&[u8]]) -> RedisObjectResult {
        self.object_create_hashed(
            key_hash,
            key,
            |bucket, key_hash| bucket.sadd_existing_or_wrongtype_hashed(key_hash, key, members),
            |bucket, key_hash| bucket.sadd_new_unchecked_hashed(key_hash, key, members),
        )
    }
}
