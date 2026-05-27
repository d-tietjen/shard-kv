"""shardcache-lmcache: benchmark shardcache as an LMCache storage plugin.

Drives put / get through `ShardCacheStorageBackend` using LMCache's
`CacheEngineKey` and `BytesBufferMemoryObj` types. This measures the
LMCache plugin contract overhead on top of the underlying shardcache
storage path.

Optional comparison: pass `--with-local-cpu` to additionally bench
LMCache's built-in `LocalCPUBackend` on the same workload. That answers
"how does shardcache backend compare to LMCache's native local backend"
through the same plugin interface.

Usage:
    pip install ./integrations/lmcache_storage_backend
    maturin develop --release -m crates/shardcache-py/Cargo.toml --features extension-module
    python benchmarks/python/shardcache_lmcache_bench.py \
        --value-size 4096 --mix 80-20 --vcpu-budget 4 --clients 4 \
        --key-count 4096 --warmup 1 --duration 5
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from _bench_common import (
    WorkloadSpec,
    Worker,
    append_csv,
    common_argparser,
    parse_mix,
    print_table,
    run_saturation,
)


def _import_lmcache():
    try:
        from lmcache.v1.cache_engine import CacheEngineKey  # type: ignore[import-not-found]
        from lmcache.v1.memory_management import (  # type: ignore[import-not-found]
            BytesBufferMemoryObj,
            MemoryFormat,
            MemoryObjMetadata,
        )
        try:
            from lmcache.v1.storage_backend.local_cpu_backend import (  # type: ignore[import-not-found]
                LocalCPUBackend,
            )
        except Exception:
            LocalCPUBackend = None
        return CacheEngineKey, BytesBufferMemoryObj, MemoryFormat, MemoryObjMetadata, LocalCPUBackend
    except ImportError as exc:
        print(
            "lmcache module not importable. Install lmcache first:\n"
            "  pip install lmcache",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc


def _import_fc_backend():
    try:
        from shardcache_lmcache_backend.backend import (  # type: ignore[import-not-found]
            ShardCacheStorageBackend,
        )
        return ShardCacheStorageBackend
    except ImportError as exc:
        print(
            "shardcache_lmcache_backend not importable. Install it first:\n"
            "  pip install ./integrations/lmcache_storage_backend",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc


def _build_key(key_class, idx: int):
    # CacheEngineKey signature varies across LMCache versions; try the
    # common forms in order.
    try:
        import torch  # type: ignore[import-not-found]

        dtype = torch.float16
    except Exception:
        dtype = None

    if dtype is not None:
        try:
            return key_class("bench", 1, 0, idx, dtype)
        except TypeError:
            try:
                return key_class(
                    model_name="bench",
                    world_size=1,
                    worker_id=0,
                    chunk_hash=idx,
                    dtype=dtype,
                )
            except TypeError:
                pass

    key_text = f"k{idx:016x}"
    try:
        return key_class(
            fmt="vllm",
            model_name="bench",
            world_size=1,
            worker_id=0,
            chunk_hash=key_text,
        )
    except TypeError:
        try:
            return key_class("bench", 1, 0, key_text)
        except TypeError:
            return key_class(key_text)


def _build_memory_obj(buf_class, metadata_class, format_enum, value: bytes):
    # BytesBufferMemoryObj wraps a memoryview / bytes payload with metadata.
    try:
        import torch  # type: ignore[import-not-found]

        shape = torch.Size([len(value)])
    except Exception:
        shape = (len(value),)

    fmt = (
        format_enum.KV_BLOB
        if hasattr(format_enum, "KV_BLOB")
        else format_enum.UNDEFINED
    )
    metadata_attempts = [
        {
            "shape": shape,
            "dtype": None,
            "fmt": fmt,
            "address": 0,
            "phy_size": len(value),
            "ref_count": 1,
        },
        {
            "shape": (len(value),),
            "dtype": None,
            "fmt": fmt,
            "address": 0,
            "phy_size": len(value),
            "ref_count": 1,
            "token_count": 0,
        },
    ]

    last_error: TypeError | None = None
    for kwargs in metadata_attempts:
        try:
            md = metadata_class(**kwargs)
            break
        except TypeError as exc:
            last_error = exc
    else:
        raise TypeError("unsupported LMCache MemoryObjMetadata constructor") from last_error

    for payload in (value, memoryview(bytearray(value))):
        try:
            return buf_class(payload, md)
        except TypeError:
            continue
    return buf_class(memoryview(bytearray(value)), md)


def _build_local_cpu_backend(local_backend_class):
    config = _build_lmcache_config()
    attempts = [
        ((), {}),
        ((config,), {}),
        ((config,), {"dst_device": "cpu"}),
        ((), {"config": config}),
        ((), {"config": config, "dst_device": "cpu"}),
        (
            (),
            {
                "config": config,
                "metadata": None,
                "loop": None,
                "memory_allocator": None,
                "dst_device": "cpu",
            },
        ),
    ]
    errors: list[str] = []
    for positional, keyword in attempts:
        try:
            return local_backend_class(*positional, **keyword)
        except TypeError as exc:
            errors.append(str(exc))
    raise TypeError("; ".join(errors[-3:]))


def _build_lmcache_config():
    config_classes = []
    for module_name in ("lmcache.v1.config", "lmcache.v1.config_base"):
        try:
            module = __import__(module_name, fromlist=["LMCacheEngineConfig"])
            config_classes.append(module.LMCacheEngineConfig)
        except Exception:
            continue
    if not config_classes:
        return type("Cfg", (), {"extra_config": {}})()

    cls = config_classes[0]
    for positional, keyword in (
        ((), {}),
        ((), {"extra_config": {}}),
        ((), {"chunk_size": 256, "local_cpu": True}),
    ):
        try:
            return cls(*positional, **keyword)
        except TypeError:
            continue
    return type("Cfg", (), {"extra_config": {}})()


class LocalCpuBackendAdapter:
    def __init__(self, backend):
        self.backend = backend

    def __getattr__(self, name):
        return getattr(self.backend, name)

    def batched_submit_put_task(self, keys, objs):
        if hasattr(self.backend, "batched_submit_put_task"):
            return self.backend.batched_submit_put_task(keys, objs)
        if hasattr(self.backend, "submit_put_task"):
            for key, obj in zip(keys, objs):
                self.backend.submit_put_task(key, obj)
            return None
        if hasattr(self.backend, "put_blocking"):
            for key, obj in zip(keys, objs):
                self.backend.put_blocking(key, obj)
            return None
        if hasattr(self.backend, "put"):
            for key, obj in zip(keys, objs):
                self.backend.put(key, obj)
            return None
        raise AttributeError("LocalCPUBackend does not expose a known put method")

    def get_blocking(self, key):
        if hasattr(self.backend, "get_blocking"):
            return self.backend.get_blocking(key)
        if hasattr(self.backend, "get"):
            return self.backend.get(key)
        raise AttributeError("LocalCPUBackend does not expose a known get method")

    def batched_get_blocking(self, keys):
        if hasattr(self.backend, "batched_get_blocking"):
            return self.backend.batched_get_blocking(keys)
        if hasattr(self.backend, "batched_get"):
            return self.backend.batched_get(keys)
        return [self.get_blocking(key) for key in keys]


class FcLmcacheWorker(Worker):
    def __init__(self, backend, keys_lm, value_obj):
        self.backend = backend
        self.keys_lm = keys_lm
        self.value_obj = value_obj

    def do_get(self, key: bytes) -> None:
        # Map raw bench key bytes to its preallocated CacheEngineKey.
        idx = int(key[2:].decode("ascii"), 16)
        self.backend.get_blocking(self.keys_lm[idx])

    def do_set(self, key: bytes, value: bytes) -> None:
        idx = int(key[2:].decode("ascii"), 16)
        self.backend.batched_submit_put_task([self.keys_lm[idx]], [self.value_obj])

    def do_get_many(self, keys: list[bytes]) -> None:
        indices = [int(key[2:].decode("ascii"), 16) for key in keys]
        self.backend.batched_get_blocking([self.keys_lm[idx] for idx in indices])

    def do_set_many(self, keys: list[bytes], value: bytes) -> None:
        del value
        indices = [int(key[2:].decode("ascii"), 16) for key in keys]
        self.backend.batched_submit_put_task(
            [self.keys_lm[idx] for idx in indices],
            [self.value_obj] * len(indices),
        )


def _run_backend(
    backend_id: str, backend, keys_lm, value_obj, spec: WorkloadSpec, args
):
    def new_worker() -> Worker:
        return FcLmcacheWorker(backend, keys_lm, value_obj)

    def warmup(keys, value):
        # Pre-populate via the plugin interface so reads hit on the warmup path.
        for k in keys_lm:
            backend.batched_submit_put_task([k], [value_obj])

    return run_saturation(backend_id, spec, args, new_worker, warmup)


def main() -> None:
    parser = common_argparser(default_clients=4)
    parser.add_argument(
        "--with-local-cpu",
        action="store_true",
        help="also bench LMCache's built-in LocalCPUBackend on the same workload",
    )
    parser.add_argument(
        "--client-architecture",
        choices=("shared", "local_embedded", "scnp_tcp", "scnp_tcp_python"),
        default="shared",
        help=(
            "shardcache Python store architecture for the LMCache backend. "
            "Use shared or scnp_tcp for arbitrary multi-client benchmark keys; "
            "local_embedded requires caller-owned shard routing."
        ),
    )
    parser.add_argument(
        "--connection",
        choices=("embedded", "tcp", "tcp_python"),
        default=None,
        help=(
            "High-level LMCache connection mode. Use embedded for in-process "
            "shardcache and tcp for shardcache over SCNP/TCP."
        ),
    )
    parser.add_argument(
        "--scnp-addr",
        default="127.0.0.1:6500",
        help="host:port for --client-architecture scnp_tcp",
    )
    parser.add_argument(
        "--numa-policy",
        choices=("off", "worker_pinned", "caller_local"),
        default="off",
        help=(
            "NUMA policy for local_embedded shardcache: off, worker_pinned, "
            "or caller_local node-local routing"
        ),
    )
    args = parser.parse_args()
    get_pct = parse_mix(args.mix)
    spec = WorkloadSpec(
        key_count=args.key_count, value_size=args.value_size, get_pct=get_pct
    )

    KeyClass, BufClass, FormatEnum, MetadataClass, LocalCPUBackend = _import_lmcache()
    ShardCacheBackend = _import_fc_backend()

    print(
        f"shardcache-lmcache: value_size={args.value_size}B mix={spec.mix_label} "
        f"vcpu_budget={args.vcpu_budget} clients={args.clients} "
        f"keys={args.key_count} duration={args.duration}s "
        f"connection={args.connection or 'client_architecture'} "
        f"client_architecture={args.client_architecture} scnp_addr={args.scnp_addr} "
        f"numa_policy={args.numa_policy} "
        f"op_batch_size={args.op_batch_size}"
    )
    print()

    value = bytes((i & 0xFF) for i in range(args.value_size))
    keys_lm = [_build_key(KeyClass, i) for i in range(spec.key_count)]
    value_obj = _build_memory_obj(BufClass, MetadataClass, FormatEnum, value)

    results = []

    extra_config = {
        "storage_plugin.shardcache.cores": args.vcpu_budget,
        "storage_plugin.shardcache.client_architecture": args.client_architecture,
        "storage_plugin.shardcache.scnp_addr": args.scnp_addr,
        "storage_plugin.shardcache.enable_metrics": False,
        "storage_plugin.shardcache.zero_copy_reads": True,
        "storage_plugin.shardcache.numa_policy": args.numa_policy,
    }
    if args.connection is not None:
        extra_config["storage_plugin.shardcache.connection"] = args.connection
    shardcache_backend = ShardCacheBackend(
        config=type("Cfg", (), {"extra_config": extra_config})()
    )
    results.append(
        _run_backend(
            "shardcache-lmcache", shardcache_backend, keys_lm, value_obj, spec, args
        )
    )

    if args.with_local_cpu and LocalCPUBackend is not None:
        try:
            local_backend = _build_local_cpu_backend(LocalCPUBackend)
        except Exception as exc:
            print(
                f"skipping lmcache-local-cpu: backend constructor failed: {exc}",
                file=sys.stderr,
            )
        else:
            local_backend = LocalCpuBackendAdapter(local_backend)
            results.append(
                _run_backend(
                    "lmcache-local-cpu", local_backend, keys_lm, value_obj, spec, args
                )
            )

    print_table(results)
    append_csv(args.csv, results)


if __name__ == "__main__":
    main()
