use super::super::*;
use super::{
    RedisHashStore, RedisKeyStore, RedisListStore, RedisObjectStoreAccess, RedisSetStore,
    RedisStringStore, RedisZSetStore,
};

impl EmbeddedStore {
    /// Returns true when Redis object containers are present.
    #[inline(always)]
    pub fn has_redis_objects(&self) -> bool {
        <Self as RedisObjectStoreAccess>::has_redis_objects(self)
    }

    pub fn get_string_value_into<F>(&self, key: &[u8], write: F) -> RedisStringLookup
    where
        F: FnMut(&bytes::Bytes),
    {
        <Self as RedisStringStore>::get_string_value_into(self, key, write)
    }

    pub fn hset(&self, key: &[u8], field: &[u8], value: &[u8]) -> RedisObjectResult {
        <Self as RedisHashStore>::hset(self, key, field, value)
    }

    pub fn hset_many(&self, key: &[u8], fields: &[(&[u8], &[u8])]) -> RedisObjectResult {
        <Self as RedisHashStore>::hset_many(self, key, fields)
    }

    pub fn hget(&self, key: &[u8], field: &[u8]) -> RedisObjectResult {
        <Self as RedisHashStore>::hget(self, key, field)
    }

    pub fn hexists(&self, key: &[u8], field: &[u8]) -> RedisObjectResult {
        <Self as RedisHashStore>::hexists(self, key, field)
    }

    pub fn hdel(&self, key: &[u8], field: &[u8]) -> RedisObjectResult {
        <Self as RedisHashStore>::hdel(self, key, field)
    }

    pub fn hdel_many(&self, key: &[u8], fields: &[&[u8]]) -> RedisObjectResult {
        <Self as RedisHashStore>::hdel_many(self, key, fields)
    }

    pub fn hlen(&self, key: &[u8]) -> RedisObjectResult {
        <Self as RedisHashStore>::hlen(self, key)
    }

    pub fn hmget(&self, key: &[u8], fields: &[&[u8]]) -> RedisObjectResult {
        <Self as RedisHashStore>::hmget(self, key, fields)
    }

    pub(crate) fn hmget_visit(
        &self,
        key: &[u8],
        fields: &[&[u8]],
        emit: impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome {
        <Self as RedisHashStore>::hmget_visit(self, key, fields, emit)
    }

    pub fn hkeys(&self, key: &[u8]) -> RedisObjectResult {
        <Self as RedisHashStore>::hkeys(self, key)
    }

    pub fn hvals(&self, key: &[u8]) -> RedisObjectResult {
        <Self as RedisHashStore>::hvals(self, key)
    }

    pub fn hgetall(&self, key: &[u8]) -> RedisObjectResult {
        <Self as RedisHashStore>::hgetall(self, key)
    }

    pub fn hsetnx(&self, key: &[u8], field: &[u8], value: &[u8]) -> RedisObjectResult {
        <Self as RedisHashStore>::hsetnx(self, key, field, value)
    }

    pub fn hincrby(&self, key: &[u8], field: &[u8], delta: i64) -> RedisObjectResult {
        <Self as RedisHashStore>::hincrby(self, key, field, delta)
    }

    pub fn hincrbyfloat(&self, key: &[u8], field: &[u8], delta: f64) -> RedisObjectResult {
        <Self as RedisHashStore>::hincrbyfloat(self, key, field, delta)
    }

    pub fn hrandfield(
        &self,
        key: &[u8],
        count: Option<i64>,
        with_values: bool,
    ) -> RedisObjectResult {
        <Self as RedisHashStore>::hrandfield(self, key, count, with_values)
    }

    pub fn lpush(&self, key: &[u8], values: &[&[u8]]) -> RedisObjectResult {
        <Self as RedisListStore>::lpush(self, key, values)
    }

    pub fn rpush(&self, key: &[u8], values: &[&[u8]]) -> RedisObjectResult {
        <Self as RedisListStore>::rpush(self, key, values)
    }

    pub fn lpushx(&self, key: &[u8], values: &[&[u8]]) -> RedisObjectResult {
        <Self as RedisListStore>::lpushx(self, key, values)
    }

    pub fn rpushx(&self, key: &[u8], values: &[&[u8]]) -> RedisObjectResult {
        <Self as RedisListStore>::rpushx(self, key, values)
    }

    pub fn lpop(&self, key: &[u8]) -> RedisObjectResult {
        <Self as RedisListStore>::lpop(self, key)
    }

    pub fn rpop(&self, key: &[u8]) -> RedisObjectResult {
        <Self as RedisListStore>::rpop(self, key)
    }

    pub fn lpop_count(&self, key: &[u8], count: usize) -> RedisObjectResult {
        <Self as RedisListStore>::lpop_count(self, key, count)
    }

    pub fn rpop_count(&self, key: &[u8], count: usize) -> RedisObjectResult {
        <Self as RedisListStore>::rpop_count(self, key, count)
    }

    pub fn llen(&self, key: &[u8]) -> RedisObjectResult {
        <Self as RedisListStore>::llen(self, key)
    }

    pub fn lindex(&self, key: &[u8], index: i64) -> RedisObjectResult {
        <Self as RedisListStore>::lindex(self, key, index)
    }

    pub fn lrange(&self, key: &[u8], start: i64, stop: i64) -> RedisObjectResult {
        <Self as RedisListStore>::lrange(self, key, start, stop)
    }

    pub fn lset(&self, key: &[u8], index: i64, value: &[u8]) -> RedisObjectResult {
        <Self as RedisListStore>::lset(self, key, index, value)
    }

    pub fn lrem(&self, key: &[u8], count: i64, value: &[u8]) -> RedisObjectResult {
        <Self as RedisListStore>::lrem(self, key, count, value)
    }

    pub fn ltrim(&self, key: &[u8], start: i64, stop: i64) -> RedisObjectResult {
        <Self as RedisListStore>::ltrim(self, key, start, stop)
    }

    pub fn linsert(
        &self,
        key: &[u8],
        before: bool,
        pivot: &[u8],
        value: &[u8],
    ) -> RedisObjectResult {
        <Self as RedisListStore>::linsert(self, key, before, pivot, value)
    }

    pub fn sadd(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult {
        <Self as RedisSetStore>::sadd(self, key, members)
    }

    pub fn srem(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult {
        <Self as RedisSetStore>::srem(self, key, members)
    }

    pub fn sismember(&self, key: &[u8], member: &[u8]) -> RedisObjectResult {
        <Self as RedisSetStore>::sismember(self, key, member)
    }

    pub fn smismember(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult {
        <Self as RedisSetStore>::smismember(self, key, members)
    }

    pub fn scard(&self, key: &[u8]) -> RedisObjectResult {
        <Self as RedisSetStore>::scard(self, key)
    }

    pub fn smembers(&self, key: &[u8]) -> RedisObjectResult {
        <Self as RedisSetStore>::smembers(self, key)
    }

    pub fn set_members(&self, key: &[u8]) -> Result<Vec<Bytes>, RedisObjectError> {
        <Self as RedisSetStore>::set_members(self, key)
    }

    pub fn spop(&self, key: &[u8], count: Option<usize>) -> RedisObjectResult {
        <Self as RedisSetStore>::spop(self, key, count)
    }

    pub fn srandmember(&self, key: &[u8], count: Option<i64>) -> RedisObjectResult {
        <Self as RedisSetStore>::srandmember(self, key, count)
    }

    pub fn zadd(&self, key: &[u8], score: f64, member: &[u8]) -> RedisObjectResult {
        <Self as RedisZSetStore>::zadd(self, key, score, member)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn zadd_cond(
        &self,
        key: &[u8],
        score: f64,
        member: &[u8],
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
        ch: bool,
        incr: bool,
    ) -> RedisObjectResult {
        <Self as RedisZSetStore>::zadd_cond(self, key, score, member, nx, xx, gt, lt, ch, incr)
    }

    pub fn zrem(&self, key: &[u8], member: &[u8]) -> RedisObjectResult {
        <Self as RedisZSetStore>::zrem(self, key, member)
    }

    pub fn zrem_many(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult {
        <Self as RedisZSetStore>::zrem_many(self, key, members)
    }

    pub fn zscore(&self, key: &[u8], member: &[u8]) -> RedisObjectResult {
        <Self as RedisZSetStore>::zscore(self, key, member)
    }

    pub fn zmscore(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult {
        <Self as RedisZSetStore>::zmscore(self, key, members)
    }

    pub fn zincrby(&self, key: &[u8], delta: f64, member: &[u8]) -> RedisObjectResult {
        <Self as RedisZSetStore>::zincrby(self, key, delta, member)
    }

    pub fn zcard(&self, key: &[u8]) -> RedisObjectResult {
        <Self as RedisZSetStore>::zcard(self, key)
    }

    pub fn zrange(&self, key: &[u8], start: i64, stop: i64) -> RedisObjectResult {
        <Self as RedisZSetStore>::zrange(self, key, start, stop)
    }

    pub fn zentries(&self, key: &[u8]) -> Result<Vec<(Bytes, f64)>, RedisObjectError> {
        <Self as RedisZSetStore>::zentries(self, key)
    }

    pub fn zrank(&self, key: &[u8], member: &[u8], rev: bool) -> RedisObjectResult {
        <Self as RedisZSetStore>::zrank(self, key, member, rev)
    }

    pub fn zcount(&self, key: &[u8], min: f64, max: f64) -> RedisObjectResult {
        <Self as RedisZSetStore>::zcount(self, key, min, max)
    }

    pub fn zpop(&self, key: &[u8], count: usize, max: bool) -> RedisObjectResult {
        <Self as RedisZSetStore>::zpop(self, key, count, max)
    }

    pub fn rename_key(
        &self,
        source: &[u8],
        dest: &[u8],
        nx: bool,
    ) -> std::result::Result<bool, RedisObjectError> {
        <Self as RedisKeyStore>::rename_key(self, source, dest, nx)
    }
}
