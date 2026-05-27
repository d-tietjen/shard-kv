#ifndef SHARDCACHE_H
#define SHARDCACHE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#if defined(_WIN32)
#define SHARDCACHE_API __declspec(dllimport)
#else
#define SHARDCACHE_API
#endif

typedef int32_t shardcache_status_t;

#define SHARDCACHE_OK 0
#define SHARDCACHE_NOT_FOUND 1
#define SHARDCACHE_INVALID_ARGUMENT 2
#define SHARDCACHE_UNSUPPORTED 3
#define SHARDCACHE_PANIC 255

#define SHARDCACHE_OPTIONS_VERSION 1

#define SHARDCACHE_EVICTION_NONE 0
#define SHARDCACHE_EVICTION_LRU 1
#define SHARDCACHE_EVICTION_LFU 2
#define SHARDCACHE_EVICTION_PREFIX 3

#define SHARDCACHE_ROUTE_FULL_KEY 0
#define SHARDCACHE_ROUTE_SESSION_PREFIX 1

typedef struct shardcache_db_t shardcache_db_t;
typedef struct shardcache_prepared_key_t shardcache_prepared_key_t;

typedef struct shardcache_options_t {
    uint32_t version;
    size_t shard_count;
    size_t max_memory_bytes;
    uint32_t eviction_policy;
    uint32_t route_mode;
    uint64_t reserved[4];
} shardcache_options_t;

typedef struct shardcache_bytes_t {
    const uint8_t *ptr;
    size_t len;
    void *owner;
} shardcache_bytes_t;

typedef struct shardcache_slice_t {
    const uint8_t *ptr;
    size_t len;
} shardcache_slice_t;

typedef struct shardcache_set_item_t {
    shardcache_slice_t key;
    shardcache_slice_t value;
} shardcache_set_item_t;

typedef struct shardcache_batch_t {
    shardcache_bytes_t *values;
    size_t len;
    size_t hit_count;
    void *owner;
} shardcache_batch_t;

SHARDCACHE_API uint32_t shardcache_version(void);
SHARDCACHE_API const char *shardcache_status_string(shardcache_status_t status);

SHARDCACHE_API shardcache_status_t
shardcache_options_default(shardcache_options_t *out_options);

SHARDCACHE_API shardcache_status_t
shardcache_open(const shardcache_options_t *options, shardcache_db_t **out_db);

SHARDCACHE_API void shardcache_close(shardcache_db_t *db);

SHARDCACHE_API shardcache_status_t
shardcache_prepare_key(shardcache_db_t *db,
                       const uint8_t *key_ptr,
                       size_t key_len,
                       shardcache_prepared_key_t **out_prepared);

SHARDCACHE_API void shardcache_prepared_key_free(shardcache_prepared_key_t *prepared);

SHARDCACHE_API shardcache_status_t
shardcache_set(shardcache_db_t *db,
               const uint8_t *key_ptr,
               size_t key_len,
               const uint8_t *value_ptr,
               size_t value_len);

SHARDCACHE_API shardcache_status_t
shardcache_set_ttl(shardcache_db_t *db,
                   const uint8_t *key_ptr,
                   size_t key_len,
                   const uint8_t *value_ptr,
                   size_t value_len,
                   uint64_t ttl_ms);

SHARDCACHE_API shardcache_status_t
shardcache_set_prepared(shardcache_db_t *db,
                        const shardcache_prepared_key_t *prepared,
                        const uint8_t *value_ptr,
                        size_t value_len);

SHARDCACHE_API shardcache_status_t
shardcache_get(shardcache_db_t *db,
               const uint8_t *key_ptr,
               size_t key_len,
               shardcache_bytes_t *out_bytes);

SHARDCACHE_API shardcache_status_t
shardcache_get_prepared(shardcache_db_t *db,
                        const shardcache_prepared_key_t *prepared,
                        shardcache_bytes_t *out_bytes);

SHARDCACHE_API shardcache_status_t
shardcache_delete(shardcache_db_t *db,
                  const uint8_t *key_ptr,
                  size_t key_len);

SHARDCACHE_API shardcache_status_t
shardcache_batch_set(shardcache_db_t *db,
                     const shardcache_set_item_t *items_ptr,
                     size_t items_len);

SHARDCACHE_API shardcache_status_t
shardcache_batch_get(shardcache_db_t *db,
                     const shardcache_slice_t *keys_ptr,
                     size_t keys_len,
                     shardcache_batch_t *out_batch);

SHARDCACHE_API shardcache_status_t
shardcache_session_set(shardcache_db_t *db,
                       const uint8_t *session_ptr,
                       size_t session_len,
                       const uint8_t *key_ptr,
                       size_t key_len,
                       const uint8_t *value_ptr,
                       size_t value_len);

SHARDCACHE_API shardcache_status_t
shardcache_session_get(shardcache_db_t *db,
                       const uint8_t *session_ptr,
                       size_t session_len,
                       const uint8_t *key_ptr,
                       size_t key_len,
                       shardcache_bytes_t *out_bytes);

SHARDCACHE_API shardcache_status_t
shardcache_contains(shardcache_db_t *db,
                    const uint8_t *key_ptr,
                    size_t key_len,
                    bool *out_present);

SHARDCACHE_API shardcache_status_t
shardcache_len(shardcache_db_t *db, size_t *out_len);

SHARDCACHE_API shardcache_status_t
shardcache_stored_bytes(shardcache_db_t *db, size_t *out_bytes);

SHARDCACHE_API void shardcache_bytes_free(shardcache_bytes_t *bytes);
SHARDCACHE_API void shardcache_batch_free(shardcache_batch_t *batch);

#ifdef __cplusplus
}
#endif

#endif
