use super::*;

#[cfg(feature = "fast-point-map")]
use super::fast_point::FastPointMutation;

impl FlatMap {
    ///
    /// # Safety
    ///
    /// The caller must guarantee that this map has exclusive ownership of
    /// stored value buffers: no outstanding `bytes::Bytes` clones and no
    /// borrowed value slices may exist while this runs. The 1-shard FCNP server
    /// satisfies this by using borrowed response encoding and single-threaded
    /// shard ownership.
    #[inline(always)]
    pub unsafe fn set_slice_hashed_no_ttl_hot(&mut self, hash: u64, key: &[u8], value: &[u8]) {
        let key_tag = hash_key_tag_from_hash(hash);
        // SAFETY: forwarded from this function's caller.
        unsafe { self.set_slice_hashed_tagged_no_ttl_hot(hash, key_tag, key, value) };
    }

    ///
    /// # Safety
    ///
    /// Same safety contract as `set_slice_hashed_no_ttl_hot`. `key_tag` must
    /// be the `hash_key_tag_from_hash(hash)` value for `key`.
    #[inline(always)]
    pub unsafe fn set_slice_hashed_tagged_no_ttl_hot(
        &mut self,
        hash: u64,
        key_tag: u64,
        key: &[u8],
        value: &[u8],
    ) {
        debug_assert_eq!(self.memory_limit_bytes, None);
        debug_assert_eq!(self.eviction_policy, EvictionPolicy::None);

        if !self.retired_values.is_empty() {
            self.reclaim_retired_if_quiescent();
        }

        #[cfg(feature = "telemetry")]
        let start = self.telemetry.as_ref().map(|_| Instant::now());
        #[cfg(feature = "telemetry")]
        let written_len = value.len();
        #[cfg(feature = "telemetry")]
        let (key_delta, memory_delta): (isize, isize);

        let has_active_readers = self.has_active_readers();
        #[cfg(feature = "fast-point-map")]
        let allow_in_place_replace = !has_active_readers && cfg!(feature = "unsafe");
        #[cfg(feature = "fast-point-map")]
        if self.fast_points.is_active() {
            if let Some(mutation) = unsafe {
                self.fast_points
                    .upsert_slice(hash, key_tag, key, value, allow_in_place_replace)
            } {
                #[cfg(feature = "telemetry")]
                {
                    match &mutation {
                        FastPointMutation::Inserted { key_len, value_len } => {
                            key_delta = 1isize;
                            memory_delta = (key_len + value_len) as isize;
                        }
                        FastPointMutation::Replaced {
                            old_value_len,
                            new_value_len,
                            ..
                        } => {
                            key_delta = 0isize;
                            memory_delta = *new_value_len as isize - *old_value_len as isize;
                        }
                    }
                }
                match mutation {
                    FastPointMutation::Inserted { key_len, value_len } => {
                        self.stored_bytes = self
                            .stored_bytes
                            .saturating_add(key_len)
                            .saturating_add(value_len);
                    }
                    FastPointMutation::Replaced {
                        old_value,
                        old_value_len,
                        new_value_len,
                    } => {
                        if old_value_len != new_value_len {
                            self.stored_bytes = self
                                .stored_bytes
                                .saturating_sub(old_value_len)
                                .saturating_add(new_value_len);
                        }
                        if let Some(old_value) = old_value {
                            self.retire_value(old_value);
                        }
                    }
                }

                #[cfg(feature = "telemetry")]
                self.record_set_metrics(written_len, key_delta, memory_delta, start);
                return;
            }
            self.disable_fast_point_map();
        }

        match self.entries.entry(
            hash,
            |entry| entry.matches_prepared(hash, key, key_tag),
            |entry| entry.hash,
        ) {
            hashbrown::hash_table::Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();
                let had_ttl = entry.expire_at_ms.is_some();
                let previous_value_len = entry.value.len();
                let mut retired_value = None;
                let should_replace_value = has_active_readers || previous_value_len != value.len();

                #[cfg(feature = "unsafe")]
                if !should_replace_value {
                    // SAFETY: guaranteed by this function's caller. The value
                    // buffer was allocated by this map and has no outstanding
                    // aliases in the 1-shard FCNP hot path.
                    unsafe {
                        copy_hot_value_bytes(
                            entry.value.as_ptr().cast_mut(),
                            value.as_ptr(),
                            value.len(),
                        );
                    }
                }
                if cfg!(not(feature = "unsafe")) || should_replace_value {
                    retired_value = Some(mem::replace(
                        &mut entry.value,
                        shared_bytes_from_slice(value),
                    ));
                }

                if previous_value_len != entry.value.len() {
                    self.stored_bytes = self
                        .stored_bytes
                        .saturating_sub(previous_value_len)
                        .saturating_add(entry.value.len());
                }
                if had_ttl {
                    entry.expire_at_ms = None;
                    self.ttl_entries = self.ttl_entries.saturating_sub(1);
                }
                #[cfg(feature = "telemetry")]
                {
                    key_delta = 0isize;
                    memory_delta = entry.value.len() as isize - previous_value_len as isize;
                }
                if let Some(old_value) = retired_value {
                    self.retire_value(old_value);
                }
            }
            hashbrown::hash_table::Entry::Vacant(vacant) => {
                let key_len = key.len();
                let value_len = value.len();
                let stored_value = shared_bytes_from_slice(value);
                vacant.insert(FlatEntry {
                    hash,
                    key_tag,
                    key_len,
                    key: key.to_vec().into_boxed_slice(),
                    value: stored_value,
                    expire_at_ms: None,
                    access: EntryAccessMeta {
                        last_touch: 0,
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

        #[cfg(feature = "telemetry")]
        self.record_set_metrics(written_len, key_delta, memory_delta, start);
    }
}
