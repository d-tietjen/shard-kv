# shardcache-c

`shardcache-c` exposes shardcache as an embeddable native library with a stable
C ABI. This is the SQLite-style surface: build one shared or static library,
then bind it from C, C++, Python, Go, Java, Node, .NET, Ruby, PHP, and any other
runtime that can call C.

The ABI is cache-only and byte-KV focused:

- open/close an embedded cache handle;
- set/get/delete byte keys;
- prepare hot keys once and reuse route metadata;
- batch set/get byte keys;
- return refcount-backed read buffers that remain valid until freed;
- configure memory-limit eviction, including feature-gated `prefix` eviction;
- set/get session-prefixed KV blocks for LMCache-style workloads.

It intentionally does not expose Redis/Valkey command families or object types.

## Build

```bash
cargo build -p shardcache-c --release
```

The library target is named `shardcache`, so platform artifacts are shaped like
`libshardcache.so`, `libshardcache.dylib`, `shardcache.dll`, or
`libshardcache.a`.

## C Example

```c
#include "shardcache.h"
#include <stdio.h>

int main(void) {
    shardcache_options_t options;
    shardcache_db_t *db = NULL;
    shardcache_bytes_t value = {0};

    shardcache_options_default(&options);
    options.shard_count = 64;
    options.max_memory_bytes = 256 * 1024 * 1024;
    options.eviction_policy = SHARDCACHE_EVICTION_PREFIX;

    if (shardcache_open(&options, &db) != SHARDCACHE_OK) {
        return 1;
    }

    shardcache_set(db, (const unsigned char *)"alpha", 5,
                      (const unsigned char *)"one", 3);

    if (shardcache_get(db, (const unsigned char *)"alpha", 5, &value) == SHARDCACHE_OK) {
        fwrite(value.ptr, 1, value.len, stdout);
        shardcache_bytes_free(&value);
    }

    shardcache_close(db);
    return 0;
}
```

## ABI Notes

`shardcache_bytes_t` is borrowed from the returned owner. The pointer remains
valid until `shardcache_bytes_free` is called. Every successful `get` that fills
`shardcache_bytes_t.owner` must be paired with exactly one free.

`shardcache_batch_t` owns an array of `shardcache_bytes_t` values. Release the
whole result with `shardcache_batch_free`; do not free individual entries first.

`shardcache_prepared_key_t` is tied to the cache handle that created it. Free it
with `shardcache_prepared_key_free` before closing the cache.

All API calls return a `shardcache_status_t` except destructors. No Rust
references, panics, or allocator-owned buffers cross the ABI boundary directly.
