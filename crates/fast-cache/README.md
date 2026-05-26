# fast-cache

Public facade crate for `fast-cache`.

The implementation lives in `fast-cache-core`; this crate preserves the
published `fast_cache` API, forwards feature flags, and owns the
`fast-cache-server` binary. Applications should continue depending on
`fast-cache` unless they specifically need to build another package directly on
the shared core engine.

```toml
[dependencies]
fast-cache = "0.2"
```

```rust
use fast_cache::FastMap;

let cache = FastMap::new();
cache.insert_slice(b"user:42", b"ready");
assert_eq!(cache.get_owned(b"user:42").unwrap().as_ref(), b"ready");
```
