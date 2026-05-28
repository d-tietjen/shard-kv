use shardmap::ShardMapWithShards;

fn main() {
    let cache = ShardMapWithShards::<8>::new();

    for key in [
        b"user:1".as_slice(),
        b"user:2".as_slice(),
        b"session:alpha".as_slice(),
    ] {
        let route = cache.route_key(key);
        assert!(route.shard_id < cache.shard_count());
        println!(
            "{} -> shard {} hash {}",
            String::from_utf8_lossy(key),
            route.shard_id,
            route.key_hash
        );
    }
}
