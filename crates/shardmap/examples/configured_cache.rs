use shardmap::config::EvictionPolicy;
use shardmap::{CacheOptions, ShardCacheWithShards};

fn main() {
    let cache = ShardCacheWithShards::<8>::with_options(CacheOptions {
        capacity_hint: Some(1_024),
        total_memory_bytes: Some(1024 * 1024),
        eviction_policy: EvictionPolicy::Lru,
        ..CacheOptions::default()
    });

    cache.insert_slice(b"feature:alpha", b"enabled");
    cache.insert_slice(b"feature:beta", b"disabled");

    assert_eq!(cache.shard_count(), 8);
    assert_eq!(
        cache.get_owned(b"feature:alpha").unwrap().as_ref(),
        b"enabled"
    );
}
