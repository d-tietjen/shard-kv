use super::super::*;
use super::access::RedisObjectStoreAccess;
use crate::storage::{
    HashFieldExpireCond, HashFieldGetExpireAction, HashFieldSetCondition, HashFieldSetExpireAction,
};

#[allow(dead_code)]
pub(crate) trait RedisHashStore {
    fn hset(&self, key: &[u8], field: &[u8], value: &[u8]) -> RedisObjectResult;
    fn hset_many(&self, key: &[u8], fields: &[(&[u8], &[u8])]) -> RedisObjectResult;
    fn hget(&self, key: &[u8], field: &[u8]) -> RedisObjectResult;
    fn hexists(&self, key: &[u8], field: &[u8]) -> RedisObjectResult;
    fn hdel(&self, key: &[u8], field: &[u8]) -> RedisObjectResult;
    fn hdel_many(&self, key: &[u8], fields: &[&[u8]]) -> RedisObjectResult;
    fn hgetdel(&self, key: &[u8], fields: &[&[u8]]) -> RedisObjectResult;
    fn hgetex(
        &self,
        key: &[u8],
        fields: &[&[u8]],
        action: HashFieldGetExpireAction,
    ) -> RedisObjectResult;
    fn hsetex(
        &self,
        key: &[u8],
        fields: &[(&[u8], &[u8])],
        condition: HashFieldSetCondition,
        action: HashFieldSetExpireAction,
    ) -> RedisObjectResult;
    fn hlen(&self, key: &[u8]) -> RedisObjectResult;
    fn hmget(&self, key: &[u8], fields: &[&[u8]]) -> RedisObjectResult;
    fn hmget_visit(
        &self,
        key: &[u8],
        fields: &[&[u8]],
        emit: impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome;
    fn hkeys(&self, key: &[u8]) -> RedisObjectResult;
    fn hvals(&self, key: &[u8]) -> RedisObjectResult;
    fn hkeys_visit(
        &self,
        key: &[u8],
        emit: impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome;
    fn hvals_visit(
        &self,
        key: &[u8],
        emit: impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome;
    fn hgetall(&self, key: &[u8]) -> RedisObjectResult;
    fn hgetall_visit(
        &self,
        key: &[u8],
        emit: impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome;
    fn hsetnx(&self, key: &[u8], field: &[u8], value: &[u8]) -> RedisObjectResult;
    fn hincrby(&self, key: &[u8], field: &[u8], delta: i64) -> RedisObjectResult;
    fn hincrbyfloat(&self, key: &[u8], field: &[u8], delta: f64) -> RedisObjectResult;
    fn hrandfield(&self, key: &[u8], count: Option<i64>, with_values: bool) -> RedisObjectResult;
    fn hset_hashed(
        &self,
        key_hash: u64,
        key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> RedisObjectResult;
    fn hash_field_expire(
        &self,
        key: &[u8],
        fields: &[&[u8]],
        expire_at_ms: u64,
        cond: HashFieldExpireCond,
    ) -> RedisObjectResult;
    fn hash_field_ttl_query(
        &self,
        key: &[u8],
        fields: &[&[u8]],
        as_millis: bool,
        absolute: bool,
    ) -> RedisObjectResult;
    fn hash_field_persist(&self, key: &[u8], fields: &[&[u8]]) -> RedisObjectResult;
}

impl RedisHashStore for EmbeddedStore {
    fn hset(&self, key: &[u8], field: &[u8], value: &[u8]) -> RedisObjectResult {
        self.hset_hashed(hash_key(key), key, field, value)
    }

    fn hset_many(&self, key: &[u8], fields: &[(&[u8], &[u8])]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.hset_many(key, fields))
    }

    fn hget(&self, key: &[u8], field: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.hget(key, field))
    }

    fn hexists(&self, key: &[u8], field: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.hexists(key, field))
    }

    fn hdel(&self, key: &[u8], field: &[u8]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.hdel(key, field))
    }

    fn hdel_many(&self, key: &[u8], fields: &[&[u8]]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.hdel_many(key, fields))
    }

    fn hgetdel(&self, key: &[u8], fields: &[&[u8]]) -> RedisObjectResult {
        let now_ms = now_millis();
        self.object_write(key, |bucket| bucket.hash_field_getdel(key, fields, now_ms))
    }

    fn hgetex(
        &self,
        key: &[u8],
        fields: &[&[u8]],
        action: HashFieldGetExpireAction,
    ) -> RedisObjectResult {
        let now_ms = now_millis();
        self.object_write(key, |bucket| {
            bucket.hash_field_getex(key, fields, action, now_ms)
        })
    }

    fn hsetex(
        &self,
        key: &[u8],
        fields: &[(&[u8], &[u8])],
        condition: HashFieldSetCondition,
        action: HashFieldSetExpireAction,
    ) -> RedisObjectResult {
        let now_ms = now_millis();
        self.object_write(key, |bucket| {
            bucket.hash_field_setex(key, fields, condition, action, now_ms)
        })
    }

    fn hlen(&self, key: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.hlen(key))
    }

    fn hmget(&self, key: &[u8], fields: &[&[u8]]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.hmget(key, fields))
    }

    fn hmget_visit(
        &self,
        key: &[u8],
        fields: &[&[u8]],
        mut emit: impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome {
        self.object_read_hashed_visit(hash_key(key), key, |bucket| {
            bucket.hmget_visit(key, fields, &mut emit)
        })
    }

    fn hkeys(&self, key: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.hkeys(key))
    }

    fn hvals(&self, key: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.hvals(key))
    }

    fn hkeys_visit(
        &self,
        key: &[u8],
        mut emit: impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome {
        self.object_read_hashed_visit(hash_key(key), key, |bucket| {
            bucket.hkeys_visit(key, &mut emit)
        })
    }

    fn hvals_visit(
        &self,
        key: &[u8],
        mut emit: impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome {
        self.object_read_hashed_visit(hash_key(key), key, |bucket| {
            bucket.hvals_visit(key, &mut emit)
        })
    }

    fn hgetall(&self, key: &[u8]) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.hgetall(key))
    }

    fn hgetall_visit(
        &self,
        key: &[u8],
        mut emit: impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome {
        self.object_read_hashed_visit(hash_key(key), key, |bucket| {
            bucket.hgetall_visit(key, &mut emit)
        })
    }

    fn hsetnx(&self, key: &[u8], field: &[u8], value: &[u8]) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.hsetnx(key, field, value))
    }

    fn hincrby(&self, key: &[u8], field: &[u8], delta: i64) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.hincrby(key, field, delta))
    }

    fn hincrbyfloat(&self, key: &[u8], field: &[u8], delta: f64) -> RedisObjectResult {
        self.object_write(key, |bucket| bucket.hincrbyfloat(key, field, delta))
    }

    fn hrandfield(&self, key: &[u8], count: Option<i64>, with_values: bool) -> RedisObjectResult {
        self.object_read(key, |bucket| bucket.hrandfield(key, count, with_values))
    }

    fn hset_hashed(
        &self,
        key_hash: u64,
        key: &[u8],
        field: &[u8],
        value: &[u8],
    ) -> RedisObjectResult {
        self.object_create_hashed(
            key_hash,
            key,
            |bucket, key_hash| {
                bucket.hset_existing_or_wrongtype_hashed(key_hash, key, field, value)
            },
            |bucket, key_hash| bucket.hset_new_unchecked_hashed(key_hash, key, field, value),
        )
    }

    fn hash_field_expire(
        &self,
        key: &[u8],
        fields: &[&[u8]],
        expire_at_ms: u64,
        cond: HashFieldExpireCond,
    ) -> RedisObjectResult {
        let now_ms = now_millis();
        self.object_write(key, |bucket| {
            bucket.hash_field_expire(key, fields, expire_at_ms, cond, now_ms)
        })
    }

    fn hash_field_ttl_query(
        &self,
        key: &[u8],
        fields: &[&[u8]],
        as_millis: bool,
        absolute: bool,
    ) -> RedisObjectResult {
        let now_ms = now_millis();
        self.object_read(key, |bucket| {
            bucket.hash_field_ttl_query(key, fields, as_millis, absolute, now_ms)
        })
    }

    fn hash_field_persist(&self, key: &[u8], fields: &[&[u8]]) -> RedisObjectResult {
        let now_ms = now_millis();
        self.object_write(key, |bucket| bucket.hash_field_persist(key, fields, now_ms))
    }
}
