use super::*;

impl FlatMap {
    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub fn set_hashed_local<K, V>(
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
        #[cfg(feature = "telemetry")]
        let start = self.start_telemetry_latency_sample();

        let key = key.into();
        let mut replacement = Some(SharedBytes::from(value.into()));
        let access_tick = if self.eviction_policy == EvictionPolicy::None {
            0
        } else {
            self.next_access_tick()
        };
        self.record_lru_touch(hash, access_tick);
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
                entry.value = replacement.take().unwrap();
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
                    semantic_governance: None,
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

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub fn set_slice_hashed_local(
        &mut self,
        hash: u64,
        key: &[u8],
        value: &[u8],
        expire_at_ms: Option<u64>,
        now_ms: u64,
    ) {
        let key_tag = hash_key_tag_from_hash(hash);
        self.set_slice_hashed_tagged_local(hash, key_tag, key, value, expire_at_ms, now_ms);
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub fn set_slice_hashed_tagged_no_ttl_local(
        &mut self,
        hash: u64,
        key_tag: u64,
        key: &[u8],
        value: &[u8],
    ) {
        debug_assert_eq!(key_tag, hash_key_tag_from_hash(hash));
        self.disable_fast_point_map();
        if !self.retired_values.is_empty() {
            self.reclaim_retired_if_quiescent();
        }
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

        match self.entries.entry(
            hash,
            |entry| entry.matches_prepared(hash, key, key_tag),
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
                    #[cfg(feature = "unsafe")]
                    {
                        // SAFETY: this local no-TTL setter is only used by
                        // embedded worker-local stores after route ownership
                        // has been checked. With no active read epochs and
                        // equal lengths, overwriting the value bytes preserves
                        // stored metadata and avoids `Bytes` promotion checks.
                        unsafe {
                            copy_hot_value_bytes(
                                entry.value.as_ptr().cast_mut(),
                                value.as_ptr(),
                                value.len(),
                            );
                        }
                    }
                    #[cfg(not(feature = "unsafe"))]
                    {
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
                    semantic_governance: None,
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
    }

    #[cfg(feature = "embedded")]
    #[inline(always)]
    pub fn set_slice_hashed_tagged_local(
        &mut self,
        hash: u64,
        key_tag: u64,
        key: &[u8],
        value: &[u8],
        expire_at_ms: Option<u64>,
        now_ms: u64,
    ) {
        debug_assert_eq!(key_tag, hash_key_tag_from_hash(hash));
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
        #[cfg(feature = "telemetry")]
        let written_len = value.len();
        #[cfg(feature = "telemetry")]
        let (key_delta, memory_delta): (isize, isize);

        match self.entries.entry(
            hash,
            |entry| entry.matches_prepared(hash, key, key_tag),
            |entry| entry.hash,
        ) {
            hashbrown::hash_table::Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();
                let had_ttl = entry.expire_at_ms.is_some();
                let previous_entry_bytes = entry.stored_bytes();
                let previous_value_len = entry.value.len();
                let mut retired_value = None;
                if previous_value_len == value.len() && !has_active_readers {
                    let current_value = mem::take(&mut entry.value);
                    match current_value.try_into_mut() {
                        Ok(mut writable) => {
                            writable[..].copy_from_slice(value);
                            entry.value = writable.freeze();
                        }
                        Err(current_value) => {
                            entry.value = shared_bytes_from_slice(value);
                            retired_value = Some(current_value);
                        }
                    }
                } else {
                    retired_value = Some(mem::replace(
                        &mut entry.value,
                        shared_bytes_from_slice(value),
                    ));
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
                if let Some(old_value) = retired_value {
                    self.retire_value(old_value);
                }
            }
            hashbrown::hash_table::Entry::Vacant(vacant) => {
                let key_len = key.len();
                let value_len = value.len();
                vacant.insert(FlatEntry {
                    hash,
                    key_tag,
                    key_len,
                    key: key.to_vec().into_boxed_slice(),
                    value: shared_bytes_from_slice(value),
                    expire_at_ms,
                    semantic_index_token: None,
                    semantic_governance: None,
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
}
