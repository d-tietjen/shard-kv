# Prefix-Aware Eviction

`shardmap` exposes memory-pressure eviction through `none`, `lru`, and `lfu`,
with feature-gated `prefix` eviction available behind Cargo feature
`prefix-eviction`. The generic policies are correct for a byte-key cache, but
they do not model the hit-rate objective for LLM KV-cache reuse.

For LMCache/vLLM, the useful unit is a request/session prefix and its layer or
block children. Evicting one cold block at a time can keep byte pressure low
while destroying a long shared prefix that would have produced many future
hits. Prefix-aware eviction therefore belongs at the storage layer, not only in
a caller-side scoring loop.

## Intended Shape

The native policy should track:

- `prefix_id`: stable bytes for a reusable prompt/session prefix.
- `block_key`: the stored KV block key under that prefix.
- `layer_index` and optional `block_index`: enough ordering metadata to evict
  a prefix tail coherently.
- `bytes`: physical payload bytes charged to the prefix.
- `last_touch` and `touch_count`: local recency/frequency signals.
- `pin_count`: blocks that must not be evicted during active GPU restore.

The eviction score should be computed per prefix group first, then per block
inside that prefix. That lets the engine choose outcomes such as:

- keep the longest shared prefix when it is still hot;
- trim cold suffix blocks before evicting root prefix blocks;
- evict whole cold prefixes when fragmentation would otherwise leave unusable
  partial context;
- respect pins without making every pinned block invisible to memory pressure
  accounting.

## API Direction

This should be a deeper API change, not just `EvictionPolicy::PrefixLru`.

The generic byte-key API can stay unchanged. KV-serving integrations should get
an explicit insertion path shaped roughly like:

```rust
store.put_kv_block(KvBlockRecord {
    prefix_id,
    block_key,
    layer_index,
    block_index,
    payload,
    ttl_ms,
    pin_count,
});
```

The LMCache backend can derive `prefix_id` from `lmcache.tag.session` today and
later from LMCache's own prefix metadata when available. The vLLM direct
connector can use the request/session prefix it already passes to the restore
runtime.

## Implemented Milestone

The `prefix-eviction` feature adds `EvictionPolicy::Prefix` / Python
`eviction_policy="prefix"`. On memory pressure, shard-local eviction first
chooses the coldest derived prefix group, then removes cold members inside that
group. Session-slot storage uses the explicit session prefix as the group,
which matches the LMCache backend's session-batched path. Generic byte keys use
`s:*:c:*`, LMCache `session%...` segments, or common separator-derived prefixes.

## Next Milestone

1. Add an explicit KV-block insertion API with caller-supplied `prefix_id`,
   `layer_index`, `block_index`, and `pin_count`.
2. Charge every KV block insert to durable prefix metadata rather than deriving
   the prefix from opaque byte keys.
3. Evict whole cold prefixes when partial tails would be less useful than
   freeing a complete context.
4. Publish hit-rate benchmarks, not only GB/s, against LMCache LocalCPUBackend
   and LMCache Redis backend.
