# shardcache LMCache Backend

This package provides an LMCache storage plugin backed by shardcache. It can
run in-process through the `shardcache` PyO3 extension or send LMCache GET/SET
traffic to a shared `shardcache` server over SCNP/TCP.

The plugin is source-shipped from this repository. It is not a crates.io crate.

## Install

Install LMCache, build the matching `shardcache` PyO3 extension into the active
Python environment, and install the plugin package:

```bash
pip install lmcache maturin
maturin develop --release -m crates/shardcache-py/Cargo.toml --features extension-module
pip install ./integrations/lmcache_storage_backend
```

## Configure LMCache

Embedded mode is the default and keeps storage inside the LMCache process:

```yaml
storage_plugins: "shardcache"
extra_config:
  storage_plugin.shardcache.module_path: shardcache_lmcache_backend.backend
  storage_plugin.shardcache.class_name: ShardCacheStorageBackend
  storage_plugin.shardcache.connection: embedded
  storage_plugin.shardcache.cores: 8
  storage_plugin.shardcache.enable_metrics: false
```

Use `connection` for the normal deployment choice:

| `connection` | Meaning |
| --- | --- |
| `embedded` | In-process shardcache store through PyO3. |
| `tcp` | Shared shardcache server over SCNP/TCP. |

TCP mode uses the Rust `shardcache-client-rs` transport through PyO3 by
default:

```yaml
storage_plugins: "shardcache"
extra_config:
  storage_plugin.shardcache.module_path: shardcache_lmcache_backend.backend
  storage_plugin.shardcache.class_name: ShardCacheStorageBackend
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

For direct-shard experiments, expose shard-owned ports as well:

```bash
SHARDCACHE_DIRECT_SHARD_PORTS=1 \
SHARDCACHE_DIRECT_SHARD_BASE_PORT=6501 \
cargo run --release -p shardcache --features server --bin shardcache -- \
  --server-mode direct \
  --disable-persistence \
  --bind-addr 127.0.0.1:6500 \
  --shard-count 4
```

## Supported Config Keys

All keys use the `storage_plugin.<name>.` prefix. With the examples above,
`<name>` is `shardcache`.

| Key | Default | Meaning |
| --- | --- | --- |
| `cores` | host CPU count | Worker/core budget for the embedded store and plugin executor. |
| `connection` | `embedded` | Normal deployment mode: `embedded` or `tcp`. |
| `client_architecture` | derived from `connection` | Lower-level compatibility and benchmark knob. |
| `scnp_addr` | `127.0.0.1:6500` | SCNP server address for TCP mode. |
| `enable_metrics` | `false` | Enable shardcache store metrics. |
| `enable_backend_stage_metrics` | `false` | Record plugin-stage timings. |
| `zero_copy_reads` | `true` | Rebuild raw `BytesBufferMemoryObj` payloads without an extra copy when possible. |
| `wal_path` | unset | Embedded WAL path. Empty disables WAL persistence. |
| `compress_wal` | `true` | Compress embedded WAL records when WAL is enabled. |
| `max_memory_bytes` | `0` | Embedded memory budget. `0` disables memory-limit eviction. |
| `eviction_policy` | `none` | Embedded eviction policy: `none`, `lru`, or `lfu`. |
| `encoded_key_cache_limit` | `65536` | Encoded LMCache key cache entries. |
| `encoded_metadata_cache_limit` | `4096` | Encoded metadata cache entries. |
| `prepared_batch_cache_limit` | `4096` | Prepared batch lookup/put cache entries. |
| `metrics_artifacts_dir` | unset | Directory for metrics dumps when metrics are enabled. |

`client_architecture` accepts `local_embedded`, `scnp_tcp`, and
`scnp_tcp_python`. The Python TCP adapter is retained for debugging and
regression checks; the Rust SCNP adapter is the default TCP path.

## Smoke Test

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

Expected output:

```text
ShardCacheStorageBackend
```

## Benchmarks

The LMCache harness drives the real LMCache plugin contract with LMCache
`CacheEngineKey` and `BytesBufferMemoryObj` types:

```bash
python benchmarks/python/shardcache_lmcache_bench.py \
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
python benchmarks/python/shardcache_lmcache_bench.py \
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

Pass `--with-local-cpu` to compare against LMCache's built-in
`LocalCPUBackend` when the installed LMCache version exposes a constructible
local CPU backend. The published head-to-head report compares shardcache
embedded LMCache and shardcache SCNP/TCP LMCache against Redis TCP:
[`benchmarks/LMCACHE_VS_REDIS.md`](../../benchmarks/LMCACHE_VS_REDIS.md).

For SCNP/TCP payloads above 4 MiB, start `shardcache` with a larger request
handoff cap, such as `SHARDCACHE_HANDOFF_BUFFER_BYTES=16777216`. The default
cap stays at 4 MiB for ordinary server runs.

## Notes

- Uses `full_key` routing because LMCache keys are content-addressed.
- Prefers zero-copy `BytesBufferMemoryObj` reconstruction for raw buffers.
- Keeps one Rust SCNP connection per Python worker thread in TCP mode.
- Falls back to allocator-backed `MemoryObj` for KV-cache formats when LMCache's GPU connector expects `memory_obj.tensor`.
- Real CUDA/GPU-direct proof runs are separate from this LMCache storage plugin benchmark.
