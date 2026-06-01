use std::thread;
use std::time::Duration;

use shardmap::ShardMap;

fn main() -> shardmap::Result<()> {
    let cache = ShardMap::new();

    cache.insert_slice_with_ttl(b"session:short", b"active", Some(10));
    assert!(cache.contains_key(b"session:short"));

    thread::sleep(Duration::from_millis(25));
    assert!(!cache.contains_key(b"session:short"));

    assert!(cache.try_acquire_lock(b"lock:job:1", b"worker-a", 5_000)?);
    assert!(!cache.try_acquire_lock(b"lock:job:1", b"worker-b", 5_000)?);
    assert!(cache.renew_lock(b"lock:job:1", b"worker-a", 5_000)?);
    assert!(cache.release_lock(b"lock:job:1", b"worker-a"));
    assert!(cache.try_acquire_lock(b"lock:job:1", b"worker-b", 5_000)?);
    Ok(())
}
