use std::thread;

use shardmap::ShardMap;

fn main() {
    let cache = ShardMap::new();
    cache.insert_slice(b"counter:total", b"42");

    let prepared = cache.prepare_key(b"counter:total");
    let mut workers = Vec::new();

    for _ in 0..4 {
        let cache = cache.clone();
        let prepared = prepared.clone();
        workers.push(thread::spawn(move || {
            let value = cache.get_prepared_owned(&prepared).unwrap();
            assert_eq!(value.as_ref(), b"42");
        }));
    }

    for worker in workers {
        worker.join().unwrap();
    }
}
