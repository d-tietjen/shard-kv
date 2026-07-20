use super::*;

impl FlatMap {
    #[inline(always)]
    pub fn set<K, V>(&mut self, key: K, value: V, expire_at_ms: Option<u64>, now_ms: u64)
    where
        K: Into<Bytes>,
        V: Into<Bytes>,
    {
        let key = key.into();
        self.set_hashed(hash_key(&key), key, value, expire_at_ms, now_ms);
    }

    #[inline(always)]
    pub fn set_slice(&mut self, key: &[u8], value: &[u8], expire_at_ms: Option<u64>, now_ms: u64) {
        self.set_slice_hashed(hash_key(key), key, value, expire_at_ms, now_ms);
    }

    #[cfg(feature = "redis")]
    pub(crate) fn transform_value_hashed_no_ttl<R, E>(
        &mut self,
        hash: u64,
        key: &[u8],
        now_ms: u64,
        transform: impl FnOnce(Option<&[u8]>) -> std::result::Result<(R, Bytes), E>,
    ) -> std::result::Result<R, E> {
        self.transform_value_hashed(hash, key, now_ms, true, transform)
    }

    #[cfg(feature = "redis")]
    pub(crate) fn transform_value_hashed_preserve_ttl<R, E>(
        &mut self,
        hash: u64,
        key: &[u8],
        now_ms: u64,
        transform: impl FnOnce(Option<&[u8]>) -> std::result::Result<(R, Bytes), E>,
    ) -> std::result::Result<R, E> {
        self.transform_value_hashed(hash, key, now_ms, false, transform)
    }

    #[cfg(feature = "redis")]
    fn transform_value_hashed<R, E>(
        &mut self,
        hash: u64,
        key: &[u8],
        now_ms: u64,
        clear_ttl: bool,
        transform: impl FnOnce(Option<&[u8]>) -> std::result::Result<(R, Bytes), E>,
    ) -> std::result::Result<R, E> {
        self.disable_fast_point_map();
        self.reclaim_retired_if_quiescent();
        if self.ttl_entries != 0 && self.entry_is_expired_hashed(hash, key, now_ms) {
            self.delete_hashed_internal(hash, key, now_ms, DeleteReason::Expired);
        }
        let access_tick = if self.eviction_policy == EvictionPolicy::None {
            0
        } else {
            self.next_access_tick()
        };
        self.record_lru_touch(hash, access_tick);
        let _ = self.delete_remote_hashed(hash, key, DeleteReason::Explicit);

        let key_tag = hash_key_tag_from_hash(hash);
        match self.entries.entry(
            hash,
            |entry| entry.matches_hashed_key(hash, key),
            |entry| entry.hash,
        ) {
            hashbrown::hash_table::Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();
                let visible_value = (!entry.is_protected()).then_some(entry.value.as_ref());
                let (result, value) = transform(visible_value)?;
                let value = SharedBytes::from(value);
                let had_ttl = entry.expire_at_ms.is_some();
                let previous_entry_bytes = entry.stored_bytes();
                let retired_value = mem::replace(&mut entry.value, value);
                entry.clear_semantic_embedding();
                entry.access.record_access(access_tick);
                self.stored_bytes = self
                    .stored_bytes
                    .saturating_sub(previous_entry_bytes)
                    .saturating_add(entry.stored_bytes());
                if clear_ttl {
                    entry.expire_at_ms = None;
                    self.adjust_ttl_count(had_ttl, false);
                }
                self.retire_value(retired_value);
                self.enforce_memory_limit(now_ms);
                Ok(result)
            }
            hashbrown::hash_table::Entry::Vacant(vacant) => {
                let (result, value) = transform(None)?;
                let key_len = key.len();
                let value = SharedBytes::from(value);
                let value_len = value.len();
                vacant.insert(FlatEntry {
                    hash,
                    key_tag,
                    key_len,
                    key: key.to_vec().into_boxed_slice(),
                    value,
                    expire_at_ms: None,
                    semantic_index_token: None,
                    governance: None,
                    #[cfg(feature = "kv-overflow")]
                    overflow_generation: 0,
                    access: EntryAccessMeta {
                        last_touch: access_tick,
                        frequency: 1,
                    },
                });
                self.stored_bytes = self
                    .stored_bytes
                    .saturating_add(key_len)
                    .saturating_add(value_len);
                self.enforce_memory_limit(now_ms);
                Ok(result)
            }
        }
    }

    /// Zero-copy `SET` for the multi-direct hot path: takes `value` as an
    /// already-owned `SharedBytes` (typically a `split_prefix` slice from the
    /// connection read buffer). Avoids the heap allocation that
    /// `set_slice_hashed` performs to copy `value` into a new `SharedBytes`.
    /// Key is copied into a `Box<[u8]>` so the entry retains a tight key
    /// allocation (keys are small and don't benefit from sharing).
    pub fn set_bytes_hashed(
        &mut self,
        hash: u64,
        key: &[u8],
        value: SharedBytes,
        expire_at_ms: Option<u64>,
        now_ms: u64,
    ) {
        self.set_bytes_hashed_with_governance_option(hash, key, value, None, expire_at_ms, now_ms);
    }

    pub(crate) fn set_bytes_hashed_with_governance(
        &mut self,
        hash: u64,
        key: &[u8],
        value: SharedBytes,
        governance: SharedBytes,
        expire_at_ms: Option<u64>,
        now_ms: u64,
    ) {
        self.set_bytes_hashed_with_governance_option(
            hash,
            key,
            value,
            Some(governance),
            expire_at_ms,
            now_ms,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn set_bytes_hashed_with_governance_option(
        &mut self,
        hash: u64,
        key: &[u8],
        value: SharedBytes,
        governance: Option<SharedBytes>,
        expire_at_ms: Option<u64>,
        now_ms: u64,
    ) {
        self.disable_fast_point_map();
        self.reclaim_retired_if_quiescent();
        let mut governance = governance;
        let access_tick = if self.eviction_policy == EvictionPolicy::None {
            0
        } else {
            self.next_access_tick()
        };
        self.record_lru_touch(hash, access_tick);
        let _ = self.delete_remote_hashed(hash, key, DeleteReason::Explicit);

        let key_tag = hash_key_tag_from_hash(hash);
        match self.entries.entry(
            hash,
            |entry| entry.matches_hashed_key(hash, key),
            |entry| entry.hash,
        ) {
            hashbrown::hash_table::Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();
                let had_ttl = entry.expire_at_ms.is_some();
                let previous_entry_bytes = entry.stored_bytes();
                let retired_value = mem::replace(&mut entry.value, value);
                entry.clear_semantic_embedding();
                entry.governance = governance.take();
                entry.access.record_access(access_tick);
                self.stored_bytes = self
                    .stored_bytes
                    .saturating_sub(previous_entry_bytes)
                    .saturating_add(entry.stored_bytes());
                entry.expire_at_ms = expire_at_ms;
                self.adjust_ttl_count(had_ttl, expire_at_ms.is_some());
                self.retire_value(retired_value);
            }
            hashbrown::hash_table::Entry::Vacant(vacant) => {
                let key_len = key.len();
                let value_len = value.len();
                let governance_len = governance.as_ref().map_or(0, SharedBytes::len);
                vacant.insert(FlatEntry {
                    hash,
                    key_tag,
                    key_len,
                    key: key.to_vec().into_boxed_slice(),
                    value,
                    expire_at_ms,
                    semantic_index_token: None,
                    governance,
                    #[cfg(feature = "kv-overflow")]
                    overflow_generation: 0,
                    access: EntryAccessMeta {
                        last_touch: access_tick,
                        frequency: 1,
                    },
                });
                self.stored_bytes = self
                    .stored_bytes
                    .saturating_add(key_len)
                    .saturating_add(value_len)
                    .saturating_add(governance_len);
                if expire_at_ms.is_some() {
                    self.ttl_entries = self.ttl_entries.saturating_add(1);
                }
            }
        }

        self.enforce_memory_limit(now_ms);
    }

    #[cfg(feature = "kv-overflow")]
    pub(crate) fn can_set_bytes_hashed_with_limit(
        &self,
        hash: u64,
        key: &[u8],
        value_len: usize,
        governance_len: usize,
        hard_limit: usize,
    ) -> bool {
        if hard_limit == usize::MAX {
            return true;
        }
        let previous_bytes = self
            .entries
            .find(hash, |entry| entry.matches_hashed_key(hash, key))
            .map(FlatEntry::stored_bytes)
            .or_else(|| {
                self.remote_entries
                    .get(key)
                    .filter(|entry| entry.matches(hash, key))
                    .map(RemoteEntry::stored_bytes)
            })
            .unwrap_or(0);
        let projected_bytes = self
            .stored_bytes
            .saturating_sub(previous_bytes)
            .saturating_add(key.len())
            .saturating_add(value_len)
            .saturating_add(governance_len);
        projected_bytes <= hard_limit || projected_bytes <= self.stored_bytes
    }

    #[cfg(feature = "kv-overflow")]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn set_bytes_hashed_overflow(
        &mut self,
        hash: u64,
        key: &[u8],
        value: SharedBytes,
        governance: Option<SharedBytes>,
        expire_at_ms: Option<u64>,
        now_ms: u64,
        generation: u64,
    ) {
        self.set_bytes_hashed_with_governance_option(
            hash,
            key,
            value,
            governance,
            expire_at_ms,
            now_ms,
        );
        let entry = self
            .entries
            .find_mut(hash, |entry| entry.matches_hashed_key(hash, key))
            .expect("overflow value was inserted under the same shard lock");
        entry.overflow_generation = generation.saturating_add(1);
    }

    #[cfg(feature = "kv-overflow")]
    pub(crate) fn overflow_generation_matches(
        &self,
        hash: u64,
        key: &[u8],
        generation: u64,
    ) -> bool {
        self.entries
            .find(hash, |entry| entry.matches_hashed_key(hash, key))
            .is_some_and(|entry| entry.overflow_generation == generation.saturating_add(1))
    }

    #[inline(always)]
    pub fn set_hashed<K, V>(
        &mut self,
        hash: u64,
        key: K,
        value: V,
        expire_at_ms: Option<u64>,
        now_ms: u64,
    ) where
        K: Into<Bytes>,
        V: Into<Bytes>,
    {
        self.disable_fast_point_map();
        self.reclaim_retired_if_quiescent();
        #[cfg(feature = "telemetry")]
        let start = self.start_telemetry_latency_sample();

        let key = key.into();
        let mut replacement = Some(SharedBytes::from(value.into()));
        let _ = self.has_active_readers();
        let access_tick = if self.eviction_policy == EvictionPolicy::None {
            0
        } else {
            self.next_access_tick()
        };
        self.record_lru_touch(hash, access_tick);
        let _ = self.delete_remote_hashed(hash, key.as_ref(), DeleteReason::Explicit);
        #[cfg(feature = "telemetry")]
        let written_len = replacement.as_ref().map_or(0, |bytes| bytes.len());
        #[cfg(feature = "telemetry")]
        let (key_delta, memory_delta): (isize, isize);

        match self
            .entries
            .entry(hash, |entry| entry.matches(hash, &key), |entry| entry.hash)
        {
            hashbrown::hash_table::Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();
                let had_ttl = entry.expire_at_ms.is_some();
                let previous_entry_bytes = entry.stored_bytes();
                let retired_value =
                    Some(mem::replace(&mut entry.value, replacement.take().unwrap()));
                entry.clear_semantic_embedding();
                #[cfg(feature = "telemetry")]
                {
                    key_delta = 0isize;
                    memory_delta = entry.stored_bytes() as isize - previous_entry_bytes as isize;
                }
                entry.access.record_access(access_tick);
                self.stored_bytes = self
                    .stored_bytes
                    .saturating_sub(previous_entry_bytes)
                    .saturating_add(entry.stored_bytes());
                entry.expire_at_ms = expire_at_ms;
                self.adjust_ttl_count(had_ttl, expire_at_ms.is_some());
                if let Some(old_value) = retired_value {
                    self.retire_value(old_value);
                }
            }
            hashbrown::hash_table::Entry::Vacant(vacant) => {
                let key_len = key.len();
                let value_len = replacement.as_ref().map_or(0, |bytes| bytes.len());
                vacant.insert(FlatEntry {
                    hash,
                    key_tag: hash_key_tag_from_hash(hash),
                    key_len,
                    key: key.into_boxed_slice(),
                    value: replacement.take().unwrap(),
                    expire_at_ms,
                    semantic_index_token: None,
                    governance: None,
                    #[cfg(feature = "kv-overflow")]
                    overflow_generation: 0,
                    access: EntryAccessMeta {
                        last_touch: access_tick,
                        frequency: 1,
                    },
                });
                self.stored_bytes = self
                    .stored_bytes
                    .saturating_add(key_len)
                    .saturating_add(value_len);
                #[cfg(feature = "telemetry")]
                {
                    key_delta = 1isize;
                    memory_delta = (key_len + value_len) as isize;
                }
                if expire_at_ms.is_some() {
                    self.ttl_entries = self.ttl_entries.saturating_add(1);
                }
            }
        }

        #[cfg(feature = "telemetry")]
        self.record_set_metrics(written_len, key_delta, memory_delta, start);

        self.enforce_memory_limit(now_ms);
    }

    #[inline(always)]
    pub fn set_slice_hashed_no_ttl(&mut self, hash: u64, key: &[u8], value: &[u8]) {
        self.disable_fast_point_map();
        self.reclaim_retired_if_quiescent();
        #[cfg(feature = "telemetry")]
        let start = self.start_telemetry_latency_sample();
        #[cfg(feature = "telemetry")]
        let written_len = value.len();
        #[cfg(feature = "telemetry")]
        let (key_delta, memory_delta): (isize, isize);

        let key_tag = hash_key_tag_from_hash(hash);
        let has_active_readers = self.has_active_readers();
        let should_touch_access = self.eviction_policy != EvictionPolicy::None;
        let access_tick = if should_touch_access {
            self.next_access_tick()
        } else {
            0
        };
        if should_touch_access {
            self.record_lru_touch(hash, access_tick);
        }
        let _ = self.delete_remote_hashed(hash, key, DeleteReason::Explicit);
        let reuse_values =
            should_reuse_value_buffer(value.len()) && !self.reusable_values.is_empty();
        let mut reusable_values = if reuse_values {
            mem::take(&mut self.reusable_values)
        } else {
            Vec::new()
        };
        let mut reusable_value_bytes = if reuse_values {
            mem::take(&mut self.reusable_value_bytes)
        } else {
            0
        };
        match self.entries.entry(
            hash,
            |entry| entry.matches_hashed_key(hash, key),
            |entry| entry.hash,
        ) {
            hashbrown::hash_table::Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();
                let had_ttl = entry.expire_at_ms.is_some();
                let previous_entry_bytes = entry.stored_bytes();
                let previous_value_len = entry.value.len();
                let should_replace_value = previous_value_len != value.len() || has_active_readers;
                let mut retired_value = None;
                if should_replace_value {
                    let new_value = if reuse_values {
                        shared_bytes_from_reusable_pool(
                            value,
                            &mut reusable_values,
                            &mut reusable_value_bytes,
                        )
                    } else {
                        shared_bytes_from_slice(value)
                    };
                    retired_value = Some(mem::replace(&mut entry.value, new_value));
                } else {
                    let current_value = mem::take(&mut entry.value);
                    match current_value.try_into_mut() {
                        Ok(mut writable) => {
                            writable[..].copy_from_slice(value);
                            entry.value = writable.freeze();
                        }
                        Err(current_value) => {
                            entry.value = if reuse_values {
                                shared_bytes_from_reusable_pool(
                                    value,
                                    &mut reusable_values,
                                    &mut reusable_value_bytes,
                                )
                            } else {
                                shared_bytes_from_slice(value)
                            };
                            retired_value = Some(current_value);
                        }
                    }
                }
                #[cfg(feature = "telemetry")]
                {
                    key_delta = 0isize;
                }
                if should_touch_access {
                    entry.access.record_access(access_tick);
                }
                entry.clear_semantic_embedding();
                if had_ttl {
                    entry.expire_at_ms = None;
                    self.ttl_entries = self.ttl_entries.saturating_sub(1);
                }
                let new_entry_bytes = entry.stored_bytes();
                #[cfg(feature = "telemetry")]
                {
                    memory_delta = new_entry_bytes as isize - previous_entry_bytes as isize;
                }
                if previous_entry_bytes != new_entry_bytes {
                    self.stored_bytes = self
                        .stored_bytes
                        .saturating_sub(previous_entry_bytes)
                        .saturating_add(new_entry_bytes);
                }
                match retired_value {
                    Some(old_value) if has_active_readers => self.retire_value(old_value),
                    Some(old_value) if reuse_values => {
                        recycle_value_into_pool(
                            old_value,
                            &mut reusable_values,
                            &mut reusable_value_bytes,
                        );
                    }
                    Some(old_value) => self.recycle_value(old_value),
                    None => {}
                }
            }
            hashbrown::hash_table::Entry::Vacant(vacant) => {
                let key_len = key.len();
                let value_len = value.len();
                let stored_value = if reuse_values {
                    shared_bytes_from_reusable_pool(
                        value,
                        &mut reusable_values,
                        &mut reusable_value_bytes,
                    )
                } else {
                    shared_bytes_from_slice(value)
                };
                vacant.insert(FlatEntry {
                    hash,
                    key_tag,
                    key_len,
                    key: key.to_vec().into_boxed_slice(),
                    value: stored_value,
                    expire_at_ms: None,
                    semantic_index_token: None,
                    governance: None,
                    #[cfg(feature = "kv-overflow")]
                    overflow_generation: 0,
                    access: EntryAccessMeta {
                        last_touch: access_tick,
                        frequency: 1,
                    },
                });
                self.stored_bytes = self
                    .stored_bytes
                    .saturating_add(key_len)
                    .saturating_add(value_len);
                #[cfg(feature = "telemetry")]
                {
                    key_delta = 1isize;
                    memory_delta = (key_len + value_len) as isize;
                }
            }
        }

        if reuse_values {
            self.reusable_values = reusable_values;
            self.reusable_value_bytes = reusable_value_bytes;
        }

        #[cfg(feature = "telemetry")]
        self.record_set_metrics(written_len, key_delta, memory_delta, start);

        self.enforce_memory_limit(0);
    }

    #[inline(always)]
    pub fn set_slice_hashed(
        &mut self,
        hash: u64,
        key: &[u8],
        value: &[u8],
        expire_at_ms: Option<u64>,
        now_ms: u64,
    ) {
        self.disable_fast_point_map();
        self.reclaim_retired_if_quiescent();
        #[cfg(feature = "telemetry")]
        let start = self.start_telemetry_latency_sample();
        let has_active_readers = self.has_active_readers();
        let should_touch_access = self.eviction_policy != EvictionPolicy::None;
        let access_tick = if should_touch_access {
            self.next_access_tick()
        } else {
            0
        };
        self.record_lru_touch(hash, access_tick);
        let _ = self.delete_remote_hashed(hash, key, DeleteReason::Explicit);
        let reuse_values =
            should_reuse_value_buffer(value.len()) && !self.reusable_values.is_empty();
        let mut reusable_values = if reuse_values {
            mem::take(&mut self.reusable_values)
        } else {
            Vec::new()
        };
        let mut reusable_value_bytes = if reuse_values {
            mem::take(&mut self.reusable_value_bytes)
        } else {
            0
        };
        #[cfg(feature = "telemetry")]
        let written_len = value.len();
        #[cfg(feature = "telemetry")]
        let (key_delta, memory_delta): (isize, isize);

        let key_tag = hash_key_tag_from_hash(hash);
        match self.entries.entry(
            hash,
            |entry| entry.matches_hashed_key(hash, key),
            |entry| entry.hash,
        ) {
            hashbrown::hash_table::Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();
                let had_ttl = entry.expire_at_ms.is_some();
                let previous_entry_bytes = entry.stored_bytes();
                let previous_value_len = entry.value.len();
                let should_replace_value = previous_value_len != value.len() || has_active_readers;
                let mut retired_value = None;
                if should_replace_value {
                    let new_value = if reuse_values {
                        shared_bytes_from_reusable_pool(
                            value,
                            &mut reusable_values,
                            &mut reusable_value_bytes,
                        )
                    } else {
                        shared_bytes_from_slice(value)
                    };
                    retired_value = Some(mem::replace(&mut entry.value, new_value));
                } else {
                    let current_value = mem::take(&mut entry.value);
                    match current_value.try_into_mut() {
                        Ok(mut writable) => {
                            writable[..].copy_from_slice(value);
                            entry.value = writable.freeze();
                        }
                        Err(current_value) => {
                            entry.value = if reuse_values {
                                shared_bytes_from_reusable_pool(
                                    value,
                                    &mut reusable_values,
                                    &mut reusable_value_bytes,
                                )
                            } else {
                                shared_bytes_from_slice(value)
                            };
                            retired_value = Some(current_value);
                        }
                    }
                }
                #[cfg(feature = "telemetry")]
                {
                    key_delta = 0isize;
                }
                if should_touch_access {
                    entry.access.record_access(access_tick);
                }
                entry.clear_semantic_embedding();
                if had_ttl || expire_at_ms.is_some() {
                    entry.expire_at_ms = expire_at_ms;
                    match (had_ttl, expire_at_ms.is_some()) {
                        (false, true) => {
                            self.ttl_entries = self.ttl_entries.saturating_add(1);
                        }
                        (true, false) => {
                            self.ttl_entries = self.ttl_entries.saturating_sub(1);
                        }
                        _ => {}
                    }
                }
                let new_entry_bytes = entry.stored_bytes();
                #[cfg(feature = "telemetry")]
                {
                    memory_delta = new_entry_bytes as isize - previous_entry_bytes as isize;
                }
                if previous_entry_bytes != new_entry_bytes {
                    self.stored_bytes = self
                        .stored_bytes
                        .saturating_sub(previous_entry_bytes)
                        .saturating_add(new_entry_bytes);
                }
                match retired_value {
                    Some(old_value) if has_active_readers => self.retire_value(old_value),
                    Some(old_value) if reuse_values => {
                        recycle_value_into_pool(
                            old_value,
                            &mut reusable_values,
                            &mut reusable_value_bytes,
                        );
                    }
                    Some(old_value) => self.recycle_value(old_value),
                    None => {}
                }
            }
            hashbrown::hash_table::Entry::Vacant(vacant) => {
                let key_len = key.len();
                let value_len = value.len();
                let stored_value = if reuse_values {
                    shared_bytes_from_reusable_pool(
                        value,
                        &mut reusable_values,
                        &mut reusable_value_bytes,
                    )
                } else {
                    shared_bytes_from_slice(value)
                };
                vacant.insert(FlatEntry {
                    hash,
                    key_tag,
                    key_len,
                    key: key.to_vec().into_boxed_slice(),
                    value: stored_value,
                    expire_at_ms,
                    semantic_index_token: None,
                    governance: None,
                    #[cfg(feature = "kv-overflow")]
                    overflow_generation: 0,
                    access: EntryAccessMeta {
                        last_touch: access_tick,
                        frequency: 1,
                    },
                });
                self.stored_bytes = self
                    .stored_bytes
                    .saturating_add(key_len)
                    .saturating_add(value_len);
                #[cfg(feature = "telemetry")]
                {
                    key_delta = 1isize;
                    memory_delta = (key_len + value_len) as isize;
                }
                if expire_at_ms.is_some() {
                    self.ttl_entries = self.ttl_entries.saturating_add(1);
                }
            }
        }

        if reuse_values {
            self.reusable_values = reusable_values;
            self.reusable_value_bytes = reusable_value_bytes;
        }

        #[cfg(feature = "telemetry")]
        self.record_set_metrics(written_len, key_delta, memory_delta, start);

        self.enforce_memory_limit(now_ms);
    }
}
