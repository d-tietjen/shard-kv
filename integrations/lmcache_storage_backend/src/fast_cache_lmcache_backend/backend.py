"""LMCache storage backend implemented on top of ``fast_cache.Store``.

The wrapper intentionally keeps the moving pieces small:

- serialize a ``MemoryObj`` into one binary value for fast-cache
- prefer zero-copy ``BytesBufferMemoryObj`` reconstruction when the stored
  payload is a raw binary buffer or no LMCache allocator backend is available
- expose the ``StoragePluginInterface`` methods LMCache expects for dynamic
  loading

This module targets the LMCache v1 plugin interface from:
`lmcache.v1.storage_backend.abstract_backend`.
"""

from __future__ import annotations

import atexit
import asyncio
from collections import OrderedDict
import json
import os
import struct
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

import fast_cache
import torch
from fast_cache_local_embedded_store import create_fast_cache_store
from lmcache.logging import init_logger
from lmcache.utils import CacheEngineKey
from lmcache.v1.memory_management import (
    BytesBufferMemoryObj,
    MemoryFormat,
    MemoryObj,
    MemoryObjMetadata,
)

try:
    from lmcache.v1.storage_backend.abstract_backend import StoragePluginInterface
except ImportError:  # pragma: no cover - compatibility shim for older LMCache
    from lmcache.v1.storage_backend.abstract_backend import (  # type: ignore
        ConfigurableStorageBackendInterface as StoragePluginInterface,
    )

logger = init_logger(__name__)

_MAGIC = b"FCLM1\0"
_MAGIC_V2 = b"FCLM2\0"
_HEADER_STRUCT = struct.Struct("!I")
_FIXED_META_STRUCT = struct.Struct("!QIII6B")
_U16_STRUCT = struct.Struct("!H")
_U32_STRUCT = struct.Struct("!I")
_I64_STRUCT = struct.Struct("!q")


def _dtype_to_wire(dtype: Optional[torch.dtype]) -> bytes:
    if dtype is None:
        return b""
    return str(dtype).encode("utf-8")


def _dtype_from_wire(raw: bytes) -> Optional[torch.dtype]:
    if not raw:
        return None
    return getattr(torch, raw.decode("utf-8").replace("torch.", ""))


def _pack_u16(value: int) -> bytes:
    return _U16_STRUCT.pack(value)


def _unpack_u16(raw: memoryview, cursor: int) -> tuple[int, int]:
    return _U16_STRUCT.unpack(raw[cursor : cursor + _U16_STRUCT.size])[0], cursor + _U16_STRUCT.size


def _pack_u32(value: int) -> bytes:
    return _U32_STRUCT.pack(value)


def _unpack_u32(raw: memoryview, cursor: int) -> tuple[int, int]:
    return _U32_STRUCT.unpack(raw[cursor : cursor + _U32_STRUCT.size])[0], cursor + _U32_STRUCT.size


def _pack_i64(value: int) -> bytes:
    return _I64_STRUCT.pack(value)


def _unpack_i64(raw: memoryview, cursor: int) -> tuple[int, int]:
    return _I64_STRUCT.unpack(raw[cursor : cursor + _I64_STRUCT.size])[0], cursor + _I64_STRUCT.size


def _pack_bytes(payload: bytes) -> bytes:
    return _pack_u16(len(payload)) + payload


def _unpack_bytes(raw: memoryview, cursor: int) -> tuple[bytes, int]:
    length, cursor = _unpack_u16(raw, cursor)
    end = cursor + length
    return raw[cursor:end].tobytes(), end


def _normalize_client_architecture(value: str) -> str:
    match value.strip().lower():
        case "embedded" | "in_process" | "local":
            return "local_embedded"
        case "tcp" | "remote":
            return "fcnp_tcp"
        case other:
            return other


@dataclass(frozen=True)
class _StoredRecord:
    """Decoded payload stored in fast-cache."""

    metadata: MemoryObjMetadata
    payload: memoryview | bytes


@dataclass
class _BackendStageStats:
    calls: int = 0
    items: int = 0
    bytes: int = 0
    total_ns: int = 0


class FastCacheStorageBackend(StoragePluginInterface):
    """LMCache storage plugin backed by the local fast-cache embedded store.

    LMCache loads this class dynamically from its ``storage_plugins`` config.
    The constructor is intentionally permissive so it remains compatible with
    both the current plugin loader and older external-backend loaders.
    """

    def __init__(
        self,
        config: Any = None,
        metadata: Any = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        memory_allocator: Any = None,
        local_cpu_backend: Any = None,
        dst_device: str = "cuda",
        lookup_server: Any = None,
        **_: Any,
    ) -> None:
        self._init_parent(
            dst_device=dst_device,
            config=config,
            metadata=metadata,
            local_cpu_backend=local_cpu_backend,
            loop=loop,
        )

        self.config = config
        self.metadata = metadata
        self.loop = loop
        self.local_cpu_backend = local_cpu_backend
        self.memory_allocator = memory_allocator
        self.lookup_server = lookup_server

        self._config_prefix = self._detect_config_prefix()
        self._cores = self._get_int_config("cores", max(os.cpu_count() or 1, 1))
        self._connection = str(self._get_config_value("connection", "") or "").strip().lower()
        self._client_architecture = self._resolve_client_architecture()
        self._fcnp_addr = str(self._get_config_value("fcnp_addr", "127.0.0.1:6500")).strip()
        self._enable_metrics = self._get_bool_config("enable_metrics", False)
        self._enable_backend_stage_metrics = self._get_bool_config(
            "enable_backend_stage_metrics", False
        )
        self._zero_copy_reads = self._get_bool_config("zero_copy_reads", True)
        self._wal_path = str(self._get_config_value("wal_path", "") or "").strip()
        self._compress_wal = self._get_bool_config("compress_wal", True)
        self._max_memory_bytes = max(0, self._get_int_config("max_memory_bytes", 0))
        self._eviction_policy = str(
            self._get_config_value("eviction_policy", "none") or "none"
        ).strip().lower()
        self._encoded_key_cache_limit = max(0, self._get_int_config("encoded_key_cache_limit", 65536))
        self._encoded_metadata_cache_limit = max(
            0, self._get_int_config("encoded_metadata_cache_limit", 4096)
        )
        self._prepared_batch_cache_limit = max(
            0, self._get_int_config("prepared_batch_cache_limit", 4096)
        )
        self._metrics_artifacts_dir = str(
            self._get_config_value(
                "metrics_artifacts_dir",
                os.environ.get("FAST_CACHE_METRICS_DIR", ""),
            )
            or ""
        ).strip()
        self._store = create_fast_cache_store(
            cores=self._cores,
            wal_path=self._wal_path or None,
            compress_wal=self._compress_wal,
            max_memory_bytes=self._max_memory_bytes or None,
            eviction_policy=self._eviction_policy,
            route_mode="full_key",
            enable_metrics=self._enable_metrics,
            client_architecture=self._client_architecture,
            prefer_session_tags=True,
            fcnp_addr=self._fcnp_addr,
        )

        self._lock = threading.RLock()
        self._executor = ThreadPoolExecutor(
            max_workers=min(32, max(4, self._cores)),
            thread_name_prefix="fast-cache-lmcache",
        )
        self._encoded_key_cache: OrderedDict[CacheEngineKey, bytes] = OrderedDict()
        self._encoded_metadata_cache: OrderedDict[int, tuple[Any, bytes]] = OrderedDict()
        self._prepared_batch_cache: OrderedDict[tuple[bytes, ...], Any] = OrderedDict()
        self._prepared_put_batch_cache: OrderedDict[tuple[tuple[bytes, ...], tuple[bytes, ...]], Any] = OrderedDict()
        self._put_tasks: set[CacheEngineKey] = set()
        self._pinned: set[CacheEngineKey] = set()
        self._backend_stage_metrics_lock = threading.Lock()
        self._backend_stage_metrics: dict[str, _BackendStageStats] = {}
        self._metrics_dumped = False

        # Long-lived serving processes do not always call close() explicitly, so
        # register a best-effort dump when metrics collection is enabled.
        if (self._enable_metrics or self._enable_backend_stage_metrics) and self._metrics_artifacts_dir:
            atexit.register(self._dump_metrics_artifacts_safe)

    def __str__(self) -> str:
        return self.__class__.__name__

    def contains(self, key: CacheEngineKey, pin: bool = False) -> bool:
        encoded_key = self._encode_key(key)
        exists = self._store.exists(encoded_key)
        if exists and pin:
            with self._lock:
                self._pinned.add(key)
        return exists

    def exists_in_put_tasks(self, key: CacheEngineKey) -> bool:
        with self._lock:
            return key in self._put_tasks

    def batched_submit_put_task(
        self,
        keys: Sequence[CacheEngineKey],
        objs: list[MemoryObj],
        transfer_spec: Any = None,
        on_complete_callback: Optional[Callable[[CacheEngineKey], None]] = None,
    ) -> None:
        del transfer_spec
        if not keys or not objs:
            return None

        total_started_ns = time.perf_counter_ns()
        request_bytes = sum(
            int(getattr(getattr(obj, "metadata", None), "phy_size", 0) or 0) for obj in objs
        )

        with self._lock:
            self._put_tasks.update(keys)

        try:
            if hasattr(self._store, "batch_put_lmcache_payloads_and_metadata_encoded_keys"):
                encoded_keys = [self._encode_key(key) for key in keys]
                metadata_blobs = [self._encode_metadata_cached(obj.metadata) for obj in objs]
                if hasattr(self._store, "prepare_lmcache_put_batch_encoded_keys") and hasattr(
                    self._store, "batch_put_lmcache_payloads_prepared"
                ):
                    prepared = self._prepare_lmcache_put_batch(encoded_keys, metadata_blobs)
                    if hasattr(self._store, "batch_put_lmcache_memory_objs_prepared_bytes"):
                        try:
                            store_started_ns = time.perf_counter_ns()
                            self._store.batch_put_lmcache_memory_objs_prepared_bytes(prepared, objs)
                            self._record_backend_stage(
                                "store.batch_put_memory_objs_prepared_bytes",
                                time.perf_counter_ns() - store_started_ns,
                                item_count=len(keys),
                                byte_count=request_bytes,
                            )
                            return None
                        except TypeError:
                            pass
                    payloads = [obj.byte_array for obj in objs]
                    if hasattr(self._store, "batch_put_lmcache_payload_bytes_prepared") and all(
                        isinstance(payload, bytes) for payload in payloads
                    ):
                        store_started_ns = time.perf_counter_ns()
                        self._store.batch_put_lmcache_payload_bytes_prepared(prepared, payloads)
                        self._record_backend_stage(
                            "store.batch_put_payload_bytes_prepared",
                            time.perf_counter_ns() - store_started_ns,
                            item_count=len(keys),
                            byte_count=request_bytes,
                        )
                    else:
                        store_started_ns = time.perf_counter_ns()
                        self._store.batch_put_lmcache_payloads_prepared(prepared, payloads)
                        self._record_backend_stage(
                            "store.batch_put_payloads_prepared",
                            time.perf_counter_ns() - store_started_ns,
                            item_count=len(keys),
                            byte_count=request_bytes,
                        )
                else:
                    payloads = [obj.byte_array for obj in objs]
                    if hasattr(
                        self._store, "batch_put_lmcache_payload_bytes_and_metadata_encoded_keys"
                    ) and all(isinstance(payload, bytes) for payload in payloads):
                        store_started_ns = time.perf_counter_ns()
                        self._store.batch_put_lmcache_payload_bytes_and_metadata_encoded_keys(
                            encoded_keys, payloads, metadata_blobs
                        )
                        self._record_backend_stage(
                            "store.batch_put_payload_bytes_and_metadata_encoded_keys",
                            time.perf_counter_ns() - store_started_ns,
                            item_count=len(keys),
                            byte_count=request_bytes,
                        )
                    else:
                        store_started_ns = time.perf_counter_ns()
                        self._store.batch_put_lmcache_payloads_and_metadata_encoded_keys(
                            encoded_keys, payloads, metadata_blobs
                        )
                        self._record_backend_stage(
                            "store.batch_put_payloads_and_metadata_encoded_keys",
                            time.perf_counter_ns() - store_started_ns,
                            item_count=len(keys),
                            byte_count=request_bytes,
                        )
            elif hasattr(self._store, "batch_put_lmcache_memory_objs_encoded_keys"):
                encoded_keys = [self._encode_key(key) for key in keys]
                store_started_ns = time.perf_counter_ns()
                self._store.batch_put_lmcache_memory_objs_encoded_keys(encoded_keys, objs)
                self._record_backend_stage(
                    "store.batch_put_memory_objs_encoded_keys",
                    time.perf_counter_ns() - store_started_ns,
                    item_count=len(keys),
                    byte_count=request_bytes,
                )
            elif hasattr(self._store, "batch_put_lmcache_memory_objs_from_engine_keys"):
                store_started_ns = time.perf_counter_ns()
                self._store.batch_put_lmcache_memory_objs_from_engine_keys(list(keys), objs)
                self._record_backend_stage(
                    "store.batch_put_memory_objs_from_engine_keys",
                    time.perf_counter_ns() - store_started_ns,
                    item_count=len(keys),
                    byte_count=request_bytes,
                )
            else:
                grouped_items: dict[bytes, list[tuple[bytes, bytes]]] = {}
                generic_items: list[tuple[bytes, bytes]] = []
                for key, obj in zip(keys, objs, strict=False):
                    item = (self._encode_key(key), self._encode_memory_obj(obj))
                    session_prefix = self._session_prefix_for_key(key)
                    if session_prefix is not None and hasattr(self._store, "batch_set_session_no_ttl"):
                        grouped_items.setdefault(session_prefix, []).append(item)
                    else:
                        generic_items.append(item)

                for session_prefix, items in grouped_items.items():
                    store_started_ns = time.perf_counter_ns()
                    self._store.batch_set_session_no_ttl(session_prefix, items)
                    self._record_backend_stage(
                        "store.batch_set_session_no_ttl",
                        time.perf_counter_ns() - store_started_ns,
                        item_count=len(items),
                    )
                if generic_items:
                    store_started_ns = time.perf_counter_ns()
                    self._store.batch_set(generic_items, ttl=None)
                    self._record_backend_stage(
                        "store.batch_set",
                        time.perf_counter_ns() - store_started_ns,
                        item_count=len(generic_items),
                    )
        finally:
            with self._lock:
                self._put_tasks.difference_update(keys)
            self._record_backend_stage(
                "backend.batched_submit_put_task.total",
                time.perf_counter_ns() - total_started_ns,
                item_count=len(keys),
                byte_count=request_bytes,
            )

        if on_complete_callback is not None:
            for key in keys:
                try:
                    on_complete_callback(key)
                except Exception as exc:  # pragma: no cover - callback is user code
                    logger.warning("on_complete_callback failed for %s: %s", key, exc)
        return None

    async def async_batched_submit_put_task(
        self,
        keys: Sequence[CacheEngineKey],
        objs: list[MemoryObj],
        transfer_spec: Any = None,
        on_complete_callback: Optional[Callable[[CacheEngineKey], None]] = None,
    ) -> None:
        await asyncio.to_thread(
            self.batched_submit_put_task,
            keys,
            objs,
            transfer_spec,
            on_complete_callback,
        )

    def get_blocking(self, key: CacheEngineKey) -> Optional[MemoryObj]:
        if (
            self._zero_copy_reads
            and self.local_cpu_backend is None
            and (
                hasattr(self._store, "batch_get_lmcache_memory_objs_prepared")
                or hasattr(self._store, "prepare_lmcache_encoded_keys")
                or hasattr(self._store, "get_lmcache_memory_obj_from_engine_key")
                or hasattr(self._store, "batch_get_lmcache_memory_objs")
            )
        ):
            try:
                prepared = self._prepare_lmcache_batch([key])
                if prepared is not None:
                    objs = self._store.batch_get_lmcache_memory_objs_prepared(prepared)
                    return objs[0] if objs else None
                if hasattr(self._store, "get_lmcache_memory_obj_from_engine_key"):
                    return self._store.get_lmcache_memory_obj_from_engine_key(key)
                encoded = self._encode_key(key)
                objs = self._store.batch_get_lmcache_memory_objs([encoded])
                return objs[0] if objs else None
            except Exception:
                pass

        raw_value = self._get_raw_value(key)
        if raw_value is None:
            return None
        return self._restore_memory_obj(self._decode_record(raw_value))

    def get_non_blocking(
        self,
        key: CacheEngineKey,
        location: Optional[str] = None,
    ) -> Optional[Future]:
        del location
        return self._executor.submit(self.get_blocking, key)

    async def batched_async_contains(
        self,
        lookup_id: str,
        keys: list[CacheEngineKey],
        pin: bool = False,
    ) -> int:
        del lookup_id
        return await asyncio.to_thread(self.batched_contains, keys, pin)

    async def batched_get_non_blocking(
        self,
        lookup_id: str,
        keys: list[CacheEngineKey],
        transfer_spec: Any = None,
    ) -> list[MemoryObj]:
        del lookup_id, transfer_spec
        results = await asyncio.to_thread(self.batched_get_blocking, keys)
        return [obj for obj in results if obj is not None]

    def batched_get_blocking(
        self,
        keys: list[CacheEngineKey],
    ) -> list[Optional[MemoryObj]]:
        if not keys:
            return []

        total_started_ns = time.perf_counter_ns()

        if self._zero_copy_reads and self.local_cpu_backend is None:
            try:
                prepared = self._prepare_lmcache_batch(keys)
                if prepared is not None:
                    store_started_ns = time.perf_counter_ns()
                    result = self._store.batch_get_lmcache_memory_objs_prepared(prepared)
                    self._record_backend_stage(
                        "store.batch_get_lmcache_memory_objs_prepared",
                        time.perf_counter_ns() - store_started_ns,
                        item_count=len(keys),
                    )
                    self._record_backend_stage(
                        "backend.batched_get_blocking.total",
                        time.perf_counter_ns() - total_started_ns,
                        item_count=len(keys),
                    )
                    return result
                if hasattr(self._store, "batch_get_lmcache_memory_objs_from_engine_keys"):
                    store_started_ns = time.perf_counter_ns()
                    result = self._store.batch_get_lmcache_memory_objs_from_engine_keys(keys)
                    self._record_backend_stage(
                        "store.batch_get_lmcache_memory_objs_from_engine_keys",
                        time.perf_counter_ns() - store_started_ns,
                        item_count=len(keys),
                    )
                    self._record_backend_stage(
                        "backend.batched_get_blocking.total",
                        time.perf_counter_ns() - total_started_ns,
                        item_count=len(keys),
                    )
                    return result
                encoded_keys = [self._encode_key(key) for key in keys]
                store_started_ns = time.perf_counter_ns()
                result = self._store.batch_get_lmcache_memory_objs(encoded_keys)
                self._record_backend_stage(
                    "store.batch_get_lmcache_memory_objs",
                    time.perf_counter_ns() - store_started_ns,
                    item_count=len(keys),
                )
                self._record_backend_stage(
                    "backend.batched_get_blocking.total",
                    time.perf_counter_ns() - total_started_ns,
                    item_count=len(keys),
                )
                return result
            except Exception:
                pass

        values = self._get_raw_values(keys)
        restore_started_ns = time.perf_counter_ns()
        result = [
            None if value is None else self._restore_memory_obj(self._decode_record(value))
            for value in values
        ]
        self._record_backend_stage(
            "backend.batched_get_blocking.restore_fallback",
            time.perf_counter_ns() - restore_started_ns,
            item_count=len(keys),
        )
        self._record_backend_stage(
            "backend.batched_get_blocking.total",
            time.perf_counter_ns() - total_started_ns,
            item_count=len(keys),
        )
        return result

    def pin(self, key: CacheEngineKey) -> bool:
        if not self.contains(key, pin=False):
            return False
        with self._lock:
            self._pinned.add(key)
        return True

    def unpin(self, key: CacheEngineKey) -> bool:
        with self._lock:
            return self._pinned.discard(key) is None

    def remove(self, key: CacheEngineKey, force: bool = True) -> bool:
        with self._lock:
            if not force and key in self._pinned:
                return False
            self._pinned.discard(key)
        return bool(self._store.delete(self._encode_key(key)))

    def get_allocator_backend(self) -> Any:
        if self.local_cpu_backend is None:
            raise RuntimeError(
                "FastCacheStorageBackend requires local_cpu_backend for MemoryObj reconstruction"
            )
        return self.local_cpu_backend

    def close(self) -> None:
        self._dump_metrics_artifacts_safe()
        self._executor.shutdown(wait=True)
        if hasattr(self._store, "close"):
            self._store.close()

    def touch_cache(self) -> None:
        # fast-cache has no separate eviction policy for this LMCache wrapper, so
        # there is no deferred touch work to apply after a request.
        return None

    def export_metrics_prometheus(self) -> Optional[str]:
        if not hasattr(self._store, "export_metrics_prometheus"):
            return None
        try:
            return self._store.export_metrics_prometheus()
        except Exception:
            return None

    def metrics_snapshot(self) -> Optional[dict[str, Any]]:
        if not hasattr(self._store, "metrics_snapshot"):
            return None
        try:
            return self._store.metrics_snapshot()
        except Exception:
            return None

    def backend_stage_metrics_snapshot(self) -> Optional[dict[str, Any]]:
        if not self._enable_backend_stage_metrics:
            return None
        with self._backend_stage_metrics_lock:
            snapshot: dict[str, Any] = {}
            for stage in sorted(self._backend_stage_metrics):
                stats = self._backend_stage_metrics[stage]
                snapshot[stage] = {
                    "calls": stats.calls,
                    "items": stats.items,
                    "bytes": stats.bytes,
                    "total_ns": stats.total_ns,
                    "avg_ns_per_call": stats.total_ns / max(1, stats.calls),
                    "avg_items_per_call": stats.items / max(1, stats.calls),
                    "avg_bytes_per_call": stats.bytes / max(1, stats.calls),
                }
            return snapshot

    def reset_backend_stage_metrics(self) -> None:
        if not self._enable_backend_stage_metrics:
            return
        with self._backend_stage_metrics_lock:
            self._backend_stage_metrics.clear()

    def _init_parent(
        self,
        *,
        dst_device: str,
        config: Any,
        metadata: Any,
        local_cpu_backend: Any,
        loop: Optional[asyncio.AbstractEventLoop],
    ) -> None:
        try:
            super().__init__(
                dst_device=dst_device,
                config=config,
                metadata=metadata,
                local_cpu_backend=local_cpu_backend,
                loop=loop,
            )
        except TypeError:
            # Older LMCache variants exposed a narrower parent constructor.
            super().__init__(dst_device=dst_device)
            self.config = config
            self.metadata = metadata
            self.local_cpu_backend = local_cpu_backend
            self.loop = loop

    def _detect_config_prefix(self) -> str:
        extra = getattr(self.config, "extra_config", None) or {}
        module_path = __name__
        class_name = self.__class__.__name__
        for key, value in extra.items():
            if not key.endswith(".module_path") or value != module_path:
                continue
            prefix = key[: -len(".module_path")]
            if extra.get(f"{prefix}.class_name") == class_name:
                return prefix
        # Fall back to the documented example name.
        return "storage_plugin.fast_cache"

    def _get_extra_config(self) -> dict[str, Any]:
        return getattr(self.config, "extra_config", None) or {}

    def _get_config_value(self, suffix: str, default: Any) -> Any:
        return self._get_extra_config().get(f"{self._config_prefix}.{suffix}", default)

    def _get_int_config(self, suffix: str, default: int) -> int:
        value = self._get_config_value(suffix, default)
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _get_bool_config(self, suffix: str, default: bool) -> bool:
        value = self._get_config_value(suffix, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    def _resolve_client_architecture(self) -> str:
        architecture = str(
            self._get_config_value("client_architecture", "local_embedded") or "local_embedded"
        ).strip().lower()
        if not self._connection:
            return _normalize_client_architecture(architecture)

        match self._connection:
            case "embedded" | "in_process" | "local":
                if architecture in {"shared", "local_embedded", "embedded", "in_process", "local"}:
                    return _normalize_client_architecture(architecture)
                return "local_embedded"
            case "tcp" | "fcnp_tcp" | "remote":
                if architecture in {"fcnp_tcp_python", "tcp_python"}:
                    return "fcnp_tcp_python"
                return "fcnp_tcp"
            case "tcp_python":
                return "fcnp_tcp_python"
            case other:
                raise ValueError(
                    "unsupported fast-cache LMCache connection "
                    f"{other!r}; expected 'embedded' or 'tcp'"
                )

    def _record_backend_stage(
        self,
        stage: str,
        elapsed_ns: int,
        *,
        item_count: int = 0,
        byte_count: int = 0,
    ) -> None:
        if not self._enable_backend_stage_metrics:
            return
        with self._backend_stage_metrics_lock:
            stats = self._backend_stage_metrics.setdefault(stage, _BackendStageStats())
            stats.calls += 1
            stats.items += item_count
            stats.bytes += byte_count
            stats.total_ns += max(0, int(elapsed_ns))

    def _dump_metrics_artifacts_safe(self) -> None:
        try:
            self._dump_metrics_artifacts()
        except Exception as exc:  # pragma: no cover - best effort dump
            logger.warning("failed to dump fast-cache telemetry artifacts: %s", exc)

    def _dump_metrics_artifacts(self) -> None:
        if (
            self._metrics_dumped
            or not self._metrics_artifacts_dir
            or not (self._enable_metrics or self._enable_backend_stage_metrics)
        ):
            return

        output_dir = Path(self._metrics_artifacts_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        snapshot = self.metrics_snapshot() if self._enable_metrics else None
        prometheus = self.export_metrics_prometheus() if self._enable_metrics else None
        runtime_metadata = {
            "pid": os.getpid(),
            "backend": self.__class__.__name__,
            "cores": self._cores,
            "enable_metrics": self._enable_metrics,
            "enable_backend_stage_metrics": self._enable_backend_stage_metrics,
            "zero_copy_reads": self._zero_copy_reads,
            "connection": self._connection or "client_architecture",
            "client_architecture": self._client_architecture,
            "fcnp_addr": self._fcnp_addr,
            "max_memory_bytes": self._max_memory_bytes,
            "eviction_policy": self._eviction_policy,
            "config_prefix": self._config_prefix,
        }

        (output_dir / "fast_cache_backend_runtime.json").write_text(
            json.dumps(runtime_metadata, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        if snapshot is not None:
            (output_dir / "fast_cache_metrics_snapshot.json").write_text(
                json.dumps(snapshot, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        backend_stage_snapshot = self.backend_stage_metrics_snapshot()
        if backend_stage_snapshot is not None:
            (output_dir / "fast_cache_backend_stage_metrics.json").write_text(
                json.dumps(backend_stage_snapshot, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        if prometheus:
            (output_dir / "fast_cache_metrics.prom").write_text(
                prometheus if prometheus.endswith("\n") else prometheus + "\n",
                encoding="utf-8",
            )
        self._metrics_dumped = True

    def _encode_key(self, key: CacheEngineKey) -> bytes:
        started_ns = time.perf_counter_ns() if self._enable_backend_stage_metrics else 0
        if self._encoded_key_cache_limit <= 0:
            encoded = key.to_string().encode("utf-8")
            self._record_backend_stage(
                "backend.encode_key.direct",
                time.perf_counter_ns() - started_ns,
                item_count=1,
                byte_count=len(encoded),
            )
            return encoded

        with self._lock:
            cached = self._encoded_key_cache.get(key)
            if cached is not None:
                self._encoded_key_cache.move_to_end(key)
                self._record_backend_stage(
                    "backend.encode_key.cache_hit",
                    time.perf_counter_ns() - started_ns,
                    item_count=1,
                    byte_count=len(cached),
                )
                return cached

            encoded = key.to_string().encode("utf-8")
            self._encoded_key_cache[key] = encoded
            if len(self._encoded_key_cache) > self._encoded_key_cache_limit:
                self._encoded_key_cache.popitem(last=False)
            self._record_backend_stage(
                "backend.encode_key.cache_miss",
                time.perf_counter_ns() - started_ns,
                item_count=1,
                byte_count=len(encoded),
            )
            return encoded

    def _encode_metadata_cached(self, metadata: MemoryObjMetadata) -> bytes:
        started_ns = time.perf_counter_ns() if self._enable_backend_stage_metrics else 0
        if self._encoded_metadata_cache_limit <= 0:
            encoded = self._encode_metadata_binary(metadata)
            self._record_backend_stage(
                "backend.encode_metadata.direct",
                time.perf_counter_ns() - started_ns,
                item_count=1,
                byte_count=len(encoded),
            )
            return encoded

        cache_key = id(metadata)
        with self._lock:
            cached = self._encoded_metadata_cache.get(cache_key)
            if cached is not None and cached[0] is metadata:
                self._encoded_metadata_cache.move_to_end(cache_key)
                self._record_backend_stage(
                    "backend.encode_metadata.cache_hit",
                    time.perf_counter_ns() - started_ns,
                    item_count=1,
                    byte_count=len(cached[1]),
                )
                return cached[1]

        encoded = self._encode_metadata_binary(metadata)
        with self._lock:
            self._encoded_metadata_cache[cache_key] = (metadata, encoded)
            if len(self._encoded_metadata_cache) > self._encoded_metadata_cache_limit:
                self._encoded_metadata_cache.popitem(last=False)
        self._record_backend_stage(
            "backend.encode_metadata.cache_miss",
            time.perf_counter_ns() - started_ns,
            item_count=1,
            byte_count=len(encoded),
        )
        return encoded

    def _session_prefix_for_key(self, key: CacheEngineKey) -> Optional[bytes]:
        request_configs = getattr(key, "request_configs", None) or {}
        session_value = request_configs.get("lmcache.tag.session")
        if session_value is None:
            return None
        return f"lmcache-session:{session_value}".encode("utf-8")

    def _shared_session_prefix(self, keys: Sequence[CacheEngineKey]) -> Optional[bytes]:
        session_prefix: Optional[bytes] = None
        for key in keys:
            current = self._session_prefix_for_key(key)
            if current is None:
                return None
            if session_prefix is None:
                session_prefix = current
            elif session_prefix != current:
                return None
        return session_prefix

    def _prepare_lmcache_batch(self, keys: Sequence[CacheEngineKey]) -> Any:
        started_ns = time.perf_counter_ns() if self._enable_backend_stage_metrics else 0
        if not (
            hasattr(self._store, "prepare_lmcache_encoded_keys")
            and hasattr(self._store, "batch_get_lmcache_memory_objs_prepared")
        ):
            return None

        encoded_keys = tuple(self._encode_key(key) for key in keys)
        if self._prepared_batch_cache_limit <= 0:
            return self._store.prepare_lmcache_encoded_keys(list(encoded_keys))

        with self._lock:
            cached = self._prepared_batch_cache.get(encoded_keys)
            if cached is not None:
                self._prepared_batch_cache.move_to_end(encoded_keys)
                self._record_backend_stage(
                    "backend.prepare_lmcache_batch.cache_hit",
                    time.perf_counter_ns() - started_ns,
                    item_count=len(keys),
                )
                return cached

        prepared = self._store.prepare_lmcache_encoded_keys(list(encoded_keys))
        with self._lock:
            self._prepared_batch_cache[encoded_keys] = prepared
            if len(self._prepared_batch_cache) > self._prepared_batch_cache_limit:
                self._prepared_batch_cache.popitem(last=False)
        self._record_backend_stage(
            "backend.prepare_lmcache_batch.cache_miss",
            time.perf_counter_ns() - started_ns,
            item_count=len(keys),
        )
        return prepared

    def _prepare_lmcache_put_batch(
        self, encoded_keys: Sequence[bytes], metadata_blobs: Sequence[bytes]
    ) -> Any:
        started_ns = time.perf_counter_ns() if self._enable_backend_stage_metrics else 0
        if not (
            hasattr(self._store, "prepare_lmcache_put_batch_encoded_keys")
            and hasattr(self._store, "batch_put_lmcache_payloads_prepared")
        ):
            return None

        cache_key = (tuple(encoded_keys), tuple(metadata_blobs))
        if self._prepared_batch_cache_limit > 0:
            with self._lock:
                cached = self._prepared_put_batch_cache.get(cache_key)
                if cached is not None:
                    self._prepared_put_batch_cache.move_to_end(cache_key)
                    self._record_backend_stage(
                        "backend.prepare_lmcache_put_batch.cache_hit",
                        time.perf_counter_ns() - started_ns,
                        item_count=len(encoded_keys),
                        byte_count=sum(len(blob) for blob in metadata_blobs),
                    )
                    return cached

        prepared = self._store.prepare_lmcache_put_batch_encoded_keys(
            list(encoded_keys), list(metadata_blobs)
        )
        if self._prepared_batch_cache_limit > 0:
            with self._lock:
                self._prepared_put_batch_cache[cache_key] = prepared
                if len(self._prepared_put_batch_cache) > self._prepared_batch_cache_limit:
                    self._prepared_put_batch_cache.popitem(last=False)
        self._record_backend_stage(
            "backend.prepare_lmcache_put_batch.cache_miss",
            time.perf_counter_ns() - started_ns,
            item_count=len(encoded_keys),
            byte_count=sum(len(blob) for blob in metadata_blobs),
        )
        return prepared

    def _get_raw_value(self, key: CacheEngineKey) -> Any:
        encoded_key = self._encode_key(key)
        session_prefix = self._session_prefix_for_key(key)
        if session_prefix is not None and hasattr(self._store, "batch_get_session_view"):
            batch = self._store.batch_get_session_view(session_prefix, [encoded_key])
            return batch.memoryview_at(0)
        if hasattr(self._store, "get_view"):
            view = self._store.get_view(encoded_key)
            if view is None:
                return None
            return view.memoryview()
        return self._store.get(encoded_key)

    def _get_raw_values(self, keys: Sequence[CacheEngineKey]) -> list[Any]:
        encoded_keys = [self._encode_key(key) for key in keys]
        session_prefix = self._shared_session_prefix(keys)
        if session_prefix is not None and hasattr(self._store, "batch_get_session_view"):
            batch = self._store.batch_get_session_view(session_prefix, encoded_keys)
            return [batch.memoryview_at(index) for index in range(batch.item_count())]
        if hasattr(self._store, "batch_get_view"):
            batch = self._store.batch_get_view(encoded_keys)
            return [batch.memoryview_at(index) for index in range(batch.item_count())]
        return self._store.batch_get(encoded_keys)

    def _encode_memory_obj(self, obj: MemoryObj) -> bytes:
        meta_bytes = self._encode_metadata_binary(obj.metadata)
        return b"".join(
            (_MAGIC_V2, _HEADER_STRUCT.pack(len(meta_bytes)), meta_bytes, bytes(obj.byte_array))
        )

    def _decode_record(self, raw_value: Any) -> _StoredRecord:
        raw = raw_value if isinstance(raw_value, memoryview) else memoryview(raw_value)
        if raw[: len(_MAGIC_V2)].tobytes() == _MAGIC_V2:
            return self._decode_record_v2(raw)
        if raw[: len(_MAGIC)].tobytes() == _MAGIC:
            return self._decode_record_v1(raw)
        raise ValueError("fast-cache LMCache record is missing the expected header")

    def _decode_record_v1(self, raw: memoryview) -> _StoredRecord:
        cursor = len(_MAGIC)
        (meta_len,) = _HEADER_STRUCT.unpack(raw[cursor : cursor + _HEADER_STRUCT.size])
        cursor += _HEADER_STRUCT.size
        meta_blob = raw[cursor : cursor + meta_len]
        payload = raw[cursor + meta_len :]
        return _StoredRecord(
            metadata=self._metadata_from_legacy_json(meta_blob.tobytes()),
            payload=payload,
        )

    def _decode_record_v2(self, raw: memoryview) -> _StoredRecord:
        cursor = len(_MAGIC_V2)
        (meta_len,) = _HEADER_STRUCT.unpack(raw[cursor : cursor + _HEADER_STRUCT.size])
        cursor += _HEADER_STRUCT.size
        meta_blob = raw[cursor : cursor + meta_len]
        payload = raw[cursor + meta_len :]
        return _StoredRecord(
            metadata=self._decode_metadata_binary(meta_blob),
            payload=payload,
        )

    def _restore_memory_obj(self, record: _StoredRecord) -> Optional[MemoryObj]:
        metadata = record.metadata
        if self._should_restore_zero_copy(metadata):
            zero_copy = self._restore_zero_copy_memory_obj(record.payload, metadata)
            if zero_copy is not None:
                return zero_copy

        if self.local_cpu_backend is None:
            return BytesBufferMemoryObj(record.payload, metadata=metadata)

        if metadata.dtype is None and not metadata.dtypes:
            return BytesBufferMemoryObj(record.payload, metadata=metadata)

        shapes: Any
        dtypes: Any
        if metadata.shapes and metadata.dtypes:
            shapes = metadata.shapes
            dtypes = metadata.dtypes
        else:
            shapes = metadata.shape
            dtypes = metadata.dtype

        allocator = self.get_allocator_backend()
        try:
            mem_obj = allocator.allocate(shapes=shapes, dtypes=dtypes, fmt=metadata.fmt)
        except TypeError:
            mem_obj = allocator.allocate(shapes, dtypes, metadata.fmt)
        if mem_obj is None:
            return None

        self._copy_payload_into_memory_obj(mem_obj, record.payload)
        return mem_obj

    def _should_restore_zero_copy(self, metadata: MemoryObjMetadata) -> bool:
        if not self._zero_copy_reads:
            return False

        # BytesBufferMemoryObj exposes byte-oriented views only and leaves
        # ``tensor`` unset. That is acceptable for binary-buffer benchmark
        # payloads, but LMCache's GPU connector requires tensor-backed
        # ``MemoryObj`` instances for real KV cache formats such as KV_2LTD.
        if metadata.fmt == MemoryFormat.BINARY_BUFFER:
            return True

        return self.local_cpu_backend is None

    def _restore_zero_copy_memory_obj(
        self,
        payload: memoryview | bytes,
        metadata: MemoryObjMetadata,
    ) -> Optional[MemoryObj]:
        try:
            if isinstance(payload, memoryview):
                payload = payload if payload.readonly else payload.toreadonly()
            return BytesBufferMemoryObj(payload, metadata=metadata)
        except Exception:
            return None

    def _copy_payload_into_memory_obj(self, mem_obj: MemoryObj, payload: memoryview | bytes) -> None:
        view = mem_obj.byte_array
        try:
            # Some LMCache allocators expose strided or typed views. Cast both
            # sides to flat byte views so assignment works regardless of the
            # original shape metadata.
            writable = view if isinstance(view, memoryview) else memoryview(view)
            if writable.format != "B" or writable.ndim != 1:
                writable = writable.cast("B")

            source = payload if isinstance(payload, memoryview) else memoryview(payload)
            if source.format != "B" or source.ndim != 1:
                source = source.cast("B")

            writable[: len(source)] = source
            return
        except TypeError:
            pass

        raw_tensor = mem_obj.raw_tensor
        if raw_tensor is None:
            raise RuntimeError("Unable to populate LMCache MemoryObj from stored payload")
        source = payload if isinstance(payload, memoryview) else memoryview(payload)
        if source.format != "B" or source.ndim != 1:
            source = source.cast("B")
        raw_tensor.view(torch.uint8)[: len(source)].copy_(
            torch.frombuffer(source, dtype=torch.uint8)
        )

    def _metadata_from_legacy_json(self, raw: bytes) -> MemoryObjMetadata:
        import json

        data = json.loads(raw.decode("utf-8"))
        cached_positions = data.pop("cached_positions", None)
        pin_count = data.pop("pin_count", 0)
        metadata = MemoryObjMetadata.from_dict(data)
        metadata.pin_count = pin_count
        if cached_positions is not None:
            metadata.cached_positions = torch.tensor(cached_positions, dtype=torch.int64)
        return metadata

    def _encode_metadata_binary(self, metadata: MemoryObjMetadata) -> bytes:
        """Serialize metadata without JSON so per-chunk decode stays cheap."""

        parts = [
            _FIXED_META_STRUCT.pack(
                int(metadata.address),
                int(metadata.phy_size),
                int(metadata.ref_count),
                int(metadata.pin_count),
                int(metadata.fmt.value),
                1 if metadata.dtype is not None else 0,
                1 if metadata.shapes else 0,
                1 if metadata.dtypes else 0,
                1 if metadata.cached_positions is not None else 0,
                len(metadata.shape),
            )
        ]
        for dim in metadata.shape:
            parts.append(_pack_i64(int(dim)))

        if metadata.dtype is not None:
            parts.append(_pack_bytes(_dtype_to_wire(metadata.dtype)))

        if metadata.shapes:
            parts.append(_pack_u16(len(metadata.shapes)))
            for shape in metadata.shapes:
                parts.append(_pack_u16(len(shape)))
                for dim in shape:
                    parts.append(_pack_i64(int(dim)))

        if metadata.dtypes:
            parts.append(_pack_u16(len(metadata.dtypes)))
            for dtype in metadata.dtypes:
                parts.append(_pack_bytes(_dtype_to_wire(dtype)))

        if metadata.cached_positions is not None:
            positions = metadata.cached_positions.tolist()
            parts.append(_pack_u32(len(positions)))
            for position in positions:
                parts.append(_pack_i64(int(position)))

        return b"".join(parts)

    def _decode_metadata_binary(self, raw: memoryview) -> MemoryObjMetadata:
        cursor = 0
        (
            address,
            phy_size,
            ref_count,
            pin_count,
            fmt_value,
            has_dtype,
            has_shapes,
            has_dtypes,
            has_cached_positions,
            shape_rank,
        ) = _FIXED_META_STRUCT.unpack(raw[cursor : cursor + _FIXED_META_STRUCT.size])
        cursor += _FIXED_META_STRUCT.size

        shape_dims: list[int] = []
        for _ in range(shape_rank):
            dim, cursor = _unpack_i64(raw, cursor)
            shape_dims.append(dim)

        dtype = None
        if has_dtype:
            dtype_raw, cursor = _unpack_bytes(raw, cursor)
            dtype = _dtype_from_wire(dtype_raw)

        shapes = None
        if has_shapes:
            shape_count, cursor = _unpack_u16(raw, cursor)
            shapes = []
            for _ in range(shape_count):
                rank, cursor = _unpack_u16(raw, cursor)
                dims: list[int] = []
                for _ in range(rank):
                    dim, cursor = _unpack_i64(raw, cursor)
                    dims.append(dim)
                shapes.append(torch.Size(dims))

        dtypes = None
        if has_dtypes:
            dtype_count, cursor = _unpack_u16(raw, cursor)
            dtypes = []
            for _ in range(dtype_count):
                dtype_raw, cursor = _unpack_bytes(raw, cursor)
                decoded = _dtype_from_wire(dtype_raw)
                if decoded is None:
                    raise ValueError("binary metadata stored an empty dtype entry")
                dtypes.append(decoded)

        cached_positions = None
        if has_cached_positions:
            position_count, cursor = _unpack_u32(raw, cursor)
            positions: list[int] = []
            for _ in range(position_count):
                position, cursor = _unpack_i64(raw, cursor)
                positions.append(position)
            cached_positions = torch.tensor(positions, dtype=torch.int64)

        metadata = MemoryObjMetadata(
            shape=torch.Size(shape_dims),
            dtype=dtype,
            address=address,
            phy_size=phy_size,
            ref_count=ref_count,
            pin_count=pin_count,
            fmt=MemoryFormat(fmt_value),
            cached_positions=cached_positions,
            shapes=shapes,
            dtypes=dtypes,
        )
        return metadata
