# fast-cache LMCache backend

An LMCache storage plugin that uses `fast_cache.Store` as the
persistence layer.

## Install

```bash
pip install ./integrations/lmcache_storage_backend
```

Install the matching `fast_cache` PyO3 wheel first:

```bash
maturin develop --release -m crates/fast-cache-py/Cargo.toml --features extension-module
```

## LMCache config

```yaml
storage_plugins: "fast_cache"
extra_config:
  storage_plugin.fast_cache.module_path: fast_cache_lmcache_backend.backend
  storage_plugin.fast_cache.class_name: FastCacheStorageBackend
  storage_plugin.fast_cache.cores: 8
  storage_plugin.fast_cache.enable_metrics: false
```

Supported keys:

- `storage_plugin.<name>.cores`
- `storage_plugin.<name>.connection`
- `storage_plugin.<name>.client_architecture`
- `storage_plugin.<name>.fcnp_addr`
- `storage_plugin.<name>.enable_metrics`
- `storage_plugin.<name>.enable_backend_stage_metrics`
- `storage_plugin.<name>.zero_copy_reads`
- `storage_plugin.<name>.metrics_artifacts_dir`

Use `connection` for the normal LMCache deployment choice:

| `connection` | Meaning |
| --- | --- |
| `embedded` | In-process fast-cache store through PyO3 |
| `tcp` | Remote/shared fast-cache server over FCNP/TCP |

Embedded mode is the default. TCP mode sends LMCache GET/SET traffic to a
fast-cache FCNP server through the Rust/PyO3 FCNP client:

```yaml
extra_config:
  storage_plugin.fast_cache.connection: tcp
  storage_plugin.fast_cache.fcnp_addr: 127.0.0.1:6500
```

`client_architecture` remains available as a lower-level compatibility and
benchmark knob. Use `shared` for multi-threaded benchmark clients with arbitrary
keys, `local_embedded` for caller-owned local routing, `fcnp_tcp` for the Rust
FCNP/TCP adapter, and `fcnp_tcp_python` to force the pure-Python socket adapter
for debugging or regression checks.

## Notes

- Uses `full_key` routing because LMCache keys are content-addressed.
- Prefers zero-copy `BytesBufferMemoryObj` reconstruction for raw buffers.
- The default FCNP/TCP adapter uses the Rust `fcnp-client-rs` transport through
  PyO3 and keeps one connection per Python worker thread.
- `fcnp_tcp_python` keeps the earlier pure-Python socket adapter available for
  comparison.
- Falls back to allocator-backed `MemoryObj` for KV-cache formats when LMCache's GPU connector expects `memory_obj.tensor`.
