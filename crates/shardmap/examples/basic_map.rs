use shardmap::ShardCache;

fn main() {
    let cache = ShardCache::with_capacity(128);

    cache.insert_slice(b"job:1", b"queued");
    assert_eq!(cache.get_owned(b"job:1").unwrap().as_ref(), b"queued");

    if let Some(mut value) = cache.get_mut(b"job:1") {
        value.set_slice(b"running");
    }

    assert!(cache.try_insert_slice(b"job:2", b"queued"));
    assert!(!cache.try_insert_slice(b"job:2", b"duplicate"));

    assert_eq!(cache.remove(b"job:1").unwrap().as_ref(), b"running");
    assert_eq!(cache.get_owned(b"job:2").unwrap().as_ref(), b"queued");
}
