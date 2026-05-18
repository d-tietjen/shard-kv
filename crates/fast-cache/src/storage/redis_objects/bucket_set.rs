use super::*;

impl RedisObjectBucket {
    #[inline(always)]
    pub(crate) fn sadd_existing_or_wrongtype_hashed(
        &mut self,
        key_hash: u64,
        key: &[u8],
        members: &[&[u8]],
    ) -> RedisObjectWriteAttempt {
        if members.is_empty() {
            return RedisObjectWriteAttempt::Complete(RedisObjectResult::Integer(0));
        }
        if let Some(slot) = self.sets.get_hashed(key_hash, key) {
            let set = self.set_slab.get_mut(slot).expect("set slab slot missing");
            let inserted = if let [member] = members {
                set.insert_slice(member) as usize
            } else {
                set.insert_many(members)
            };
            return RedisObjectWriteAttempt::Complete(RedisObjectResult::Integer(inserted as i64));
        }
        if self.has_non_set(key) {
            RedisObjectWriteAttempt::Complete(RedisObjectResult::WrongType)
        } else {
            RedisObjectWriteAttempt::Missing
        }
    }

    #[inline(always)]
    pub(crate) fn sadd_new_unchecked_hashed(
        &mut self,
        key_hash: u64,
        key: &[u8],
        members: &[&[u8]],
    ) -> RedisObjectResult {
        let mut set = SetObject::empty();
        let inserted = if let [member] = members {
            set.insert_slice(member) as usize
        } else {
            set.insert_many(members)
        };
        let slot = self.set_slab.insert(set);
        self.sets.insert_hashed(key_hash, key.to_vec(), slot);
        RedisObjectResult::Integer(inserted as i64)
    }

    pub(crate) fn srem(&mut self, key: &[u8], members: &[&[u8]]) -> (RedisObjectResult, bool) {
        if let Some(slot) = self.sets.get(key).copied() {
            let set = self.set_slab.get_mut(slot).expect("set slab slot missing");
            let removed = members.iter().filter(|member| set.remove(member)).count();
            let empty = set.is_empty();
            if empty {
                self.sets.remove(key);
                self.set_slab.remove(slot);
                self.expire_at_ms.remove(key);
            }
            return (RedisObjectResult::Integer(removed as i64), empty);
        }
        if self.has_non_set(key) {
            (RedisObjectResult::WrongType, false)
        } else {
            (RedisObjectResult::Integer(0), false)
        }
    }

    pub(crate) fn sismember(&self, key: &[u8], member: &[u8]) -> RedisObjectResult {
        match self.sets.get(key).copied() {
            Some(slot) => RedisObjectResult::Integer(
                self.set_slab
                    .get(slot)
                    .expect("set slab slot missing")
                    .contains(member) as i64,
            ),
            None if self.has_non_set(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Integer(0),
        }
    }

    pub(crate) fn smismember(&self, key: &[u8], members: &[&[u8]]) -> RedisObjectResult {
        match self.sets.get(key).copied() {
            Some(slot) => {
                let set = self.set_slab.get(slot).expect("set slab slot missing");
                RedisObjectResult::IntegerArray(
                    members
                        .iter()
                        .map(|member| set.contains(member) as i64)
                        .collect(),
                )
            }
            None if self.has_non_set(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::IntegerArray(vec![0; members.len()]),
        }
    }

    pub(crate) fn sismember_visit(
        &self,
        key: &[u8],
        member: &[u8],
        write: impl FnOnce(i64),
    ) -> RedisObjectReadOutcome {
        match self.sets.get(key).copied() {
            Some(slot) => {
                write(
                    self.set_slab
                        .get(slot)
                        .expect("set slab slot missing")
                        .contains(member) as i64,
                );
                RedisObjectReadOutcome::Written
            }
            None if self.has_non_set(key) => RedisObjectReadOutcome::WrongType,
            None => RedisObjectReadOutcome::Missing,
        }
    }

    pub(crate) fn scard(&self, key: &[u8]) -> RedisObjectResult {
        match self.sets.get(key).copied() {
            Some(slot) => RedisObjectResult::Integer(
                self.set_slab
                    .get(slot)
                    .expect("set slab slot missing")
                    .len() as i64,
            ),
            None if self.has_non_set(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Integer(0),
        }
    }

    pub(crate) fn scard_visit(
        &self,
        key: &[u8],
        write: impl FnOnce(i64),
    ) -> RedisObjectReadOutcome {
        match self.sets.get(key).copied() {
            Some(slot) => {
                write(
                    self.set_slab
                        .get(slot)
                        .expect("set slab slot missing")
                        .len() as i64,
                );
                RedisObjectReadOutcome::Written
            }
            None if self.has_non_set(key) => RedisObjectReadOutcome::WrongType,
            None => RedisObjectReadOutcome::Missing,
        }
    }

    pub(crate) fn smembers(&self, key: &[u8]) -> RedisObjectResult {
        match self.sets.get(key).copied() {
            Some(slot) => {
                let mut members = self
                    .set_slab
                    .get(slot)
                    .expect("set slab slot missing")
                    .iter()
                    .cloned()
                    .map(Some)
                    .collect::<Vec<_>>();
                members.sort();
                RedisObjectResult::Array(members)
            }
            None if self.has_non_set(key) => RedisObjectResult::WrongType,
            None => RedisObjectResult::Array(Vec::new()),
        }
    }

    pub(crate) fn set_members(&self, key: &[u8]) -> Result<Vec<Bytes>, ()> {
        match self.sets.get(key).copied() {
            Some(slot) => {
                let mut members = self
                    .set_slab
                    .get(slot)
                    .expect("set slab slot missing")
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>();
                members.sort();
                Ok(members)
            }
            None if self.has_non_set(key) => Err(()),
            None => Ok(Vec::new()),
        }
    }

    pub(crate) fn spop(&mut self, key: &[u8], count: Option<usize>) -> (RedisObjectResult, bool) {
        if let Some(slot) = self.sets.get(key).copied() {
            let set = self.set_slab.get_mut(slot).expect("set slab slot missing");
            let requested = count.unwrap_or(1);
            let members = set.iter().take(requested).cloned().collect::<Vec<_>>();
            for member in &members {
                set.remove(member);
            }
            let empty = set.is_empty();
            if empty {
                self.sets.remove(key);
                self.set_slab.remove(slot);
                self.expire_at_ms.remove(key);
            }
            let result = if count.is_none() {
                RedisObjectResult::Bulk(members.into_iter().next())
            } else {
                RedisObjectResult::Array(members.into_iter().map(Some).collect())
            };
            return (result, empty);
        }
        if self.has_non_set(key) {
            (RedisObjectResult::WrongType, false)
        } else if count.is_some() {
            (RedisObjectResult::Array(Vec::new()), false)
        } else {
            (RedisObjectResult::Bulk(None), false)
        }
    }

    pub(crate) fn srandmember(&self, key: &[u8], count: Option<i64>) -> RedisObjectResult {
        match self.sets.get(key).copied() {
            Some(slot) => {
                let set = self.set_slab.get(slot).expect("set slab slot missing");
                if let Some(count) = count {
                    let members = set.iter().cloned().collect::<Vec<_>>();
                    if members.is_empty() {
                        return RedisObjectResult::Array(Vec::new());
                    }
                    let requested = count.unsigned_abs() as usize;
                    let values = if count >= 0 {
                        members.into_iter().take(requested).map(Some).collect()
                    } else {
                        (0..requested)
                            .map(|index| Some(members[index % members.len()].clone()))
                            .collect()
                    };
                    RedisObjectResult::Array(values)
                } else {
                    RedisObjectResult::Bulk(set.iter().next().cloned())
                }
            }
            None if self.has_non_set(key) => RedisObjectResult::WrongType,
            None => {
                if count.is_some() {
                    RedisObjectResult::Array(Vec::new())
                } else {
                    RedisObjectResult::Bulk(None)
                }
            }
        }
    }
}
