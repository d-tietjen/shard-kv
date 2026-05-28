use std::thread;
use std::time::Duration;

use shardmap::{SemanticCacheError, ShardMap};

fn main() -> Result<(), SemanticCacheError> {
    let cache = ShardMap::new();

    cache.insert_semantic_slice_with_ttl(
        b"prompt:temporary",
        b"This answer expires quickly.",
        &[1.0, 0.0],
        Some(10),
    )?;

    let hit = cache.semantic_search(&[1.0, 0.0], 0.9)?;
    assert!(hit.is_some());

    thread::sleep(Duration::from_millis(25));

    let expired = cache.semantic_search(&[1.0, 0.0], 0.9)?;
    assert!(expired.is_none());

    Ok(())
}
