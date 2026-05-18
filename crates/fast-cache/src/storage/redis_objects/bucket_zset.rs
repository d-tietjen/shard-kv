use super::helpers::format_score;
use super::*;

impl RedisObjectBucket {
    #[inline(always)]
    pub(crate) fn zadd_existing_or_wrongtype_hashed(
        &mut self,
        key_hash: u64,
        key: &[u8],
        score: f64,
        member: &[u8],
    ) -> RedisObjectWriteAttempt {
        if let Some(slot) = self.zsets.get_hashed(key_hash, key) {
            let zset = self
                .zset_slab
                .get_mut(slot)
                .expect("zset slab slot missing");
            return RedisObjectWriteAttempt::Complete(RedisObjectResult::Integer(
                zset.insert_slice(member, score) as i64,
            ));
        }
        if self.has_non_zset(key) {
            RedisObjectWriteAttempt::Complete(RedisObjectResult::WrongType)
        } else {
            RedisObjectWriteAttempt::Missing
        }
    }

    #[inline(always)]
    pub(crate) fn zadd_new_unchecked_hashed(
        &mut self,
        key_hash: u64,
        key: &[u8],
        score: f64,
        member: &[u8],
    ) -> RedisObjectResult {
        let zset = ZSetObject::single(member.to_vec(), score);
        let slot = self.zset_slab.insert(zset);
        self.zsets.insert_hashed(key_hash, key.to_vec(), slot);
        RedisObjectResult::Integer(1)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn zadd_cond(
        &mut self,
        key: &[u8],
        score: f64,
        member: &[u8],
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
        ch: bool,
        incr: bool,
    ) -> (RedisObjectResult, bool) {
        if self.has_non_zset(key) {
            return (RedisObjectResult::WrongType, false);
        }

        let existing_slot = self.zsets.get(key).copied();
        let existing_score = existing_slot.and_then(|slot| {
            self.zset_slab
                .get(slot)
                .expect("zset slab slot missing")
                .score(member)
        });

        if nx && existing_score.is_some()
            || xx && existing_score.is_none()
            || gt && existing_score.is_some_and(|old| score <= old)
            || lt && existing_score.is_some_and(|old| score >= old)
        {
            return if incr {
                (RedisObjectResult::Bulk(None), false)
            } else {
                (RedisObjectResult::Integer(0), false)
            };
        }

        let next_score = if incr {
            let value = existing_score.unwrap_or(0.0) + score;
            if !value.is_finite() {
                return (
                    RedisObjectResult::Simple("ERR resulting score is not finite"),
                    false,
                );
            }
            value
        } else {
            score
        };

        let created = if let Some(slot) = existing_slot {
            let zset = self
                .zset_slab
                .get_mut(slot)
                .expect("zset slab slot missing");
            zset.insert_slice(member, next_score);
            false
        } else {
            let zset = ZSetObject::single(member.to_vec(), next_score);
            let slot = self.zset_slab.insert(zset);
            self.zsets.insert(key.to_vec(), slot);
            true
        };

        if incr {
            return (
                RedisObjectResult::Bulk(Some(format_score(next_score))),
                created,
            );
        }
        let changed = existing_score
            .map(|old| !old.total_cmp(&next_score).is_eq())
            .unwrap_or(true);
        let response = if ch {
            changed as i64
        } else {
            existing_score.is_none() as i64
        };
        (RedisObjectResult::Integer(response), created)
    }

    pub(crate) fn zrem(&mut self, key: &[u8], member: &[u8]) -> (RedisObjectResult, bool) {
        if let Some(slot) = self.zsets.get(key).copied() {
            let zset = self
                .zset_slab
                .get_mut(slot)
                .expect("zset slab slot missing");
            let removed = zset.remove(member);
            let empty = zset.is_empty();
            if empty {
                self.zsets.remove(key);
                self.zset_slab.remove(slot);
                self.expire_at_ms.remove(key);
            }
            return (RedisObjectResult::Integer(removed as i64), empty);
        }
        if self.has_non_zset(key) {
            (RedisObjectResult::WrongType, false)
        } else {
            (RedisObjectResult::Integer(0), false)
        }
    }

    pub(crate) fn zrem_many(&mut self, key: &[u8], members: &[&[u8]]) -> (RedisObjectResult, bool) {
        if let Some(slot) = self.zsets.get(key).copied() {
            let zset = self
                .zset_slab
                .get_mut(slot)
                .expect("zset slab slot missing");
            let removed = members.iter().filter(|member| zset.remove(member)).count();
            let empty = zset.is_empty();
            if empty {
                self.zsets.remove(key);
                self.zset_slab.remove(slot);
                self.expire_at_ms.remove(key);
            }
            return (RedisObjectResult::Integer(removed as i64), empty);
        }
        if self.has_non_zset(key) {
            (RedisObjectResult::WrongType, false)
        } else {
            (RedisObjectResult::Integer(0), false)
        }
    }

    pub(crate) fn zscore(&self, key: &[u8], member: &[u8]) -> RedisObjectResult {
        match self.zsets.get(key).copied() {
            Some(slot) => RedisObjectResult::Bulk(
                self.zset_slab
                    .get(slot)
                    .expect("zset slab slot missing")
                    .score(member)
                    .map(format_score),
            ),
            None if self.has_non_zset(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Bulk(None),
        }
    }

    pub(crate) fn zmscore(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult {
        match self.zsets.get(key).copied() {
            Some(slot) => {
                let zset = self.zset_slab.get(slot).expect("zset slab slot missing");
                RedisObjectResult::Array(
                    members
                        .iter()
                        .map(|member| zset.score(member).map(format_score))
                        .collect(),
                )
            }
            None if self.has_non_zset(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Array(vec![None; members.len()]),
        }
    }

    pub(crate) fn zincrby(
        &mut self,
        key: &[u8],
        delta: f64,
        member: &[u8],
    ) -> (RedisObjectResult, bool) {
        self.zadd_cond(key, delta, member, false, false, false, false, true, true)
    }

    pub(crate) fn zscore_visit(
        &self,
        key: &[u8],
        member: &[u8],
        write: impl FnOnce(Option<f64>),
    ) -> RedisObjectReadOutcome {
        match self.zsets.get(key).copied() {
            Some(slot) => {
                write(
                    self.zset_slab
                        .get(slot)
                        .expect("zset slab slot missing")
                        .score(member),
                );
                RedisObjectReadOutcome::Written
            }
            None if self.has_non_zset(key) => RedisObjectReadOutcome::WrongType,
            None => RedisObjectReadOutcome::Missing,
        }
    }

    pub(crate) fn zcard(&self, key: &[u8]) -> RedisObjectResult {
        match self.zsets.get(key).copied() {
            Some(slot) => RedisObjectResult::Integer(
                self.zset_slab
                    .get(slot)
                    .expect("zset slab slot missing")
                    .len() as i64,
            ),
            None if self.has_non_zset(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Integer(0),
        }
    }

    pub(crate) fn zcard_visit(
        &self,
        key: &[u8],
        write: impl FnOnce(i64),
    ) -> RedisObjectReadOutcome {
        match self.zsets.get(key).copied() {
            Some(slot) => {
                write(
                    self.zset_slab
                        .get(slot)
                        .expect("zset slab slot missing")
                        .len() as i64,
                );
                RedisObjectReadOutcome::Written
            }
            None if self.has_non_zset(key) => RedisObjectReadOutcome::WrongType,
            None => RedisObjectReadOutcome::Missing,
        }
    }

    pub(crate) fn zrange(&self, key: &[u8], start: i64, stop: i64) -> RedisObjectResult {
        match self.zsets.get(key).copied() {
            Some(slot) => RedisObjectResult::Array(
                self.zset_slab
                    .get(slot)
                    .expect("zset slab slot missing")
                    .range(start, stop),
            ),
            None if self.has_non_zset(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Array(Vec::new()),
        }
    }

    pub(crate) fn zentries(&self, key: &[u8]) -> Result<Vec<(Bytes, f64)>, ()> {
        match self.zsets.get(key).copied() {
            Some(slot) => Ok(self
                .zset_slab
                .get(slot)
                .expect("zset slab slot missing")
                .entries()),
            None if self.has_non_zset(key) => Err(()),
            None => Ok(Vec::new()),
        }
    }

    pub(crate) fn zrank(&self, key: &[u8], member: &[u8], rev: bool) -> RedisObjectResult {
        match self.zsets.get(key).copied() {
            Some(slot) => {
                let mut entries = self
                    .zset_slab
                    .get(slot)
                    .expect("zset slab slot missing")
                    .entries();
                if rev {
                    entries.reverse();
                }
                RedisObjectResult::Integer(
                    entries
                        .iter()
                        .position(|(existing, _)| existing.as_slice() == member)
                        .map(|rank| rank as i64)
                        .unwrap_or(-1),
                )
            }
            None if self.has_non_zset(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Integer(-1),
        }
    }

    pub(crate) fn zcount(&self, key: &[u8], min: f64, max: f64) -> RedisObjectResult {
        match self.zsets.get(key).copied() {
            Some(slot) => RedisObjectResult::Integer(
                self.zset_slab
                    .get(slot)
                    .expect("zset slab slot missing")
                    .entries()
                    .into_iter()
                    .filter(|(_, score)| *score >= min && *score <= max)
                    .count() as i64,
            ),
            None if self.has_non_zset(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Integer(0),
        }
    }

    pub(crate) fn zpop(
        &mut self,
        key: &[u8],
        count: usize,
        max: bool,
    ) -> (RedisObjectResult, bool) {
        if let Some(slot) = self.zsets.get(key).copied() {
            let zset = self
                .zset_slab
                .get_mut(slot)
                .expect("zset slab slot missing");
            let mut entries = zset.entries();
            if max {
                entries.reverse();
            }
            let entries = entries.into_iter().take(count).collect::<Vec<_>>();
            for (member, _) in &entries {
                zset.remove(member);
            }
            let empty = zset.is_empty();
            if empty {
                self.zsets.remove(key);
                self.zset_slab.remove(slot);
                self.expire_at_ms.remove(key);
            }
            let array = entries
                .into_iter()
                .flat_map(|(member, score)| [Some(member), Some(format_score(score))])
                .collect();
            return (RedisObjectResult::Array(array), empty);
        }
        if self.has_non_zset(key) {
            (RedisObjectResult::WrongType, false)
        } else {
            (RedisObjectResult::Array(Vec::new()), false)
        }
    }

    pub(crate) fn zrange_visit(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
        emit: impl FnMut(RedisObjectArrayItem<'_>),
    ) -> RedisObjectReadOutcome {
        match self.zsets.get(key).copied() {
            Some(slot) => {
                self.zset_slab
                    .get(slot)
                    .expect("zset slab slot missing")
                    .range_visit(start, stop, emit);
                RedisObjectReadOutcome::Written
            }
            None if self.has_non_zset(key) => RedisObjectReadOutcome::WrongType,
            None => RedisObjectReadOutcome::Missing,
        }
    }
}
