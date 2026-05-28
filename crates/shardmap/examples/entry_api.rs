use bytes::Bytes;
use shardmap::ShardMap;

fn main() {
    let cache = ShardMap::new();

    let first = cache
        .entry(Bytes::from_static(b"job:42"))
        .or_insert(Bytes::from_static(b"queued"));
    assert_eq!(first.value().unwrap(), b"queued");
    drop(first);

    let mut existing = cache.get_mut(b"job:42").unwrap();
    existing.set_slice(b"running");
    drop(existing);

    let second = cache
        .entry(Bytes::from_static(b"job:42"))
        .or_insert(Bytes::from_static(b"queued-again"));
    assert_eq!(second.value().unwrap(), b"running");
}
