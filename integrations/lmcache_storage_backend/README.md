# shardcache LMCache backend

An LMCache storage plugin that uses `shardcache.Store` as the
persistence layer.

## Install

Install LMCache, build the matching `shardcache` PyO3 extension into the active
Python environment, and install the plugin package:

```bash
pip install lmcache
maturin develop --release -m crates/shardcache-py/Cargo.toml --features extension-module
pip install ./integrations/lmcache_storage_backend
```

## LMCache config

```yaml
storage_plugins: "shardcache"
extra_config:
  storage_plugin.shardcache.module_path: shardcache_lmcache_backend.backend
  storage_plugin.shardcache.class_name: ShardCacheStorageBackend
  storage_plugin.shardcache.cores: 8
  storage_plugin.shardcache.enable_metrics: false
```

Supported keys:

- `storage_plugin.<name>.cores`
- `storage_plugin.<name>.connection`
- `storage_plugin.<name>.client_architecture`
- `storage_plugin.<name>.scnp_addr`
- `storage_plugin.<name>.enable_metrics`
- `storage_plugin.<name>.enable_backend_stage_metrics`
- `storage_plugin.<name>.zero_copy_reads`
- `storage_plugin.<name>.metrics_artifacts_dir`

Use `connection` for the normal LMCache deployment choice:

| `connection` | Meaning |
| --- | --- |
| `embedded` | In-process shardcache store through PyO3 |
| `tcp` | Remote/shared shardcache server over SCNP/TCP |

Embedded mode is the default. TCP mode sends LMCache GET/SET traffic to a
shardcache SCNP server through the Rust/PyO3 SCNP client:

```yaml
extra_config:
  storage_plugin.shardcache.connection: tcp
  storage_plugin.shardcache.scnp_addr: 127.0.0.1:6500
```

Start a local SCNP fanout server for TCP mode with:

```bash
cargo run --release -p shardcache --features server --bin shardcache -- \
  --server-mode direct \
  --disable-persistence \
  --bind-addr 127.0.0.1:6500 \
  --shard-count 4
```

For Linux direct-shard experiments, expose shard-owned ports as well:

```bash
SHARDCACHE_DIRECT_SHARD_PORTS=1 \
SHARDCACHE_DIRECT_SHARD_BASE_PORT=6501 \
cargo run --release -p shardcache --features server --bin shardcache -- \
  --server-mode direct \
  --disable-persistence \
  --bind-addr 127.0.0.1:6500 \
  --shard-count 4
```

`client_architecture` remains available as a lower-level compatibility and
benchmark knob. Use `shared` for multi-threaded benchmark clients with arbitrary
keys, `local_embedded` for caller-owned local routing, `scnp_tcp` for the Rust
SCNP/TCP adapter, and `scnp_tcp_python` to force the pure-Python socket adapter
for debugging or regression checks.

## Smoke test

```bash
python - <<'PY'
from shardcache_lmcache_backend.backend import ShardCacheStorageBackend

config = type("Cfg", (), {"extra_config": {
    "storage_plugin.shardcache.cores": 4,
    "storage_plugin.shardcache.connection": "embedded",
    "storage_plugin.shardcache.enable_metrics": False,
}})()

backend = ShardCacheStorageBackend(config=config)
print(type(backend).__name__)
PY
```

## Benchmarks

The LMCache harness drives the real LMCache plugin contract with LMCache
`CacheEngineKey` and `BytesBufferMemoryObj` types:

```bash
python benchmarks/python/fc_lmcache_bench.py \
  --connection embedded \
  --value-size 1048576 \
  --mix get \
  --vcpu-budget 4 \
  --clients 16 \
  --key-count 1024 \
  --latency-sample-rate 0 \
  --warmup 2 \
  --duration 10 \
  --csv benchmarks/results/lmcache.csv
```

For SCNP/TCP, start the server first and switch the harness connection:

```bash
python benchmarks/python/fc_lmcache_bench.py \
  --connection tcp \
  --scnp-addr 127.0.0.1:6500 \
  --value-size 1048576 \
  --mix 80-20 \
  --vcpu-budget 4 \
  --clients 16 \
  --key-count 1024 \
  --latency-sample-rate 0 \
  --warmup 2 \
  --duration 10 \
  --csv benchmarks/results/lmcache.csv
```

Pass `--with-local-cpu` to try LMCache's built-in `LocalCPUBackend` on the same
workload when the installed LMCache version exposes a constructible local CPU
backend. The published head-to-head report currently compares shardcache
embedded LMCache and shardcache SCNP/TCP LMCache against Redis TCP:
[`benchmarks/LMCACHE_VS_REDIS.md`](../../benchmarks/LMCACHE_VS_REDIS.md).

For SCNP/TCP payloads above 4 MiB, start `shardcache` with a larger
request handoff cap, such as `SHARDCACHE_HANDOFF_BUFFER_BYTES=16777216`. The
default cap stays at 4 MiB for ordinary server runs.

## Notes

- Uses `full_key` routing because LMCache keys are content-addressed.
- Prefers zero-copy `BytesBufferMemoryObj` reconstruction for raw buffers.
- The default SCNP/TCP adapter uses the Rust `shardcache-client-rs` transport through
  PyO3 and keeps one connection per Python worker thread.
- `scnp_tcp_python` keeps the earlier pure-Python socket adapter available for
  comparison.
- Falls back to allocator-backed `MemoryObj` for KV-cache formats when LMCache's GPU connector expects `memory_obj.tensor`.
- Real CUDA/GPU-direct proof runs are separate from this LMCache storage plugin
  benchmark. Use the runtime connector tests and host-specific CUDA benchmark
  gates for GPU transfer claims.
