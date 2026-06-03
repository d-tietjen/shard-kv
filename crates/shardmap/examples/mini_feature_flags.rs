use shardmap::ShardCache;

fn set_flag(cache: &ShardCache, name: &str, enabled: bool) {
    let key = format!("flag:{name}");
    let value = if enabled {
        b"on".as_slice()
    } else {
        b"off".as_slice()
    };
    cache.insert_slice_with_ttl(key.as_bytes(), value, Some(60_000));
}

fn is_enabled(cache: &ShardCache, name: &str) -> bool {
    let key = format!("flag:{name}");
    let prepared = cache.prepare_key(key.as_bytes());
    cache
        .get_prepared_owned(&prepared)
        .is_some_and(|value| value.as_ref() == b"on")
}

fn refresh_flags(cache: &ShardCache) -> shardmap::Result<()> {
    if !cache.try_acquire_lock(b"lock:refresh-flags", b"worker-1", 5_000)? {
        return Ok(());
    }

    set_flag(cache, "semantic-cache", true);
    set_flag(cache, "tcp-server", false);
    assert!(cache.release_lock(b"lock:refresh-flags", b"worker-1"));
    Ok(())
}

fn main() -> shardmap::Result<()> {
    let cache = ShardCache::new();

    refresh_flags(&cache)?;

    assert!(is_enabled(&cache, "semantic-cache"));
    assert!(!is_enabled(&cache, "tcp-server"));
    Ok(())
}
