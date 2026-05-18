from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

SUPPORTED_VLLM_VERSION = "0.17.1"
HOST_DIRECT_V1_PATH_VERSION = "host_direct_v1"
GPU_DIRECT_API_V0_PATH_VERSION = "gpu_direct_api_v0"


class VersionMismatchError(RuntimeError):
    pass


@dataclass(frozen=True)
class RequestedPage:
    key: bytes
    layer_index: int
    page_index: int
    len_bytes: int


@dataclass(frozen=True)
class BlockAllocation:
    block_index: int
    dst_device_ptr: int
    block_size_bytes: int


_MISSING = object()


def assert_supported_vllm_version(
    version: str | None = None, *, expected: str = SUPPORTED_VLLM_VERSION
) -> str:
    resolved = version
    if resolved is None:
        try:
            import vllm  # type: ignore
        except ImportError as exc:  # pragma: no cover - exercised only with real installs
            raise RuntimeError("vllm is not importable in this environment") from exc
        resolved = getattr(vllm, "__version__", None)
    if resolved != expected:
        raise VersionMismatchError(
            f"unsupported vllm version {resolved!r}; expected {expected!r}"
        )
    return resolved


def _field(obj: Any, *names: str, default: Any = _MISSING) -> Any:
    if isinstance(obj, Mapping):
        for name in names:
            if name in obj:
                return obj[name]
    for name in names:
        if hasattr(obj, name):
            return getattr(obj, name)
    if default is not _MISSING:
        return default
    joined = ", ".join(names)
    raise ValueError(f"missing required field from object: one of {joined}")


def _coerce_bytes(value: Any, label: str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, str):
        return value.encode("utf-8")
    raise TypeError(f"{label} must be bytes-like or str")


def _coerce_int(value: Any, label: str) -> int:
    try:
        return int(value)
    except Exception as exc:  # pragma: no cover - defensive
        raise TypeError(f"{label} must be int-like") from exc


def _normalize_requested_page(obj: Any) -> RequestedPage:
    return RequestedPage(
        key=_coerce_bytes(_field(obj, "key", "cache_key", "chunk_key"), "requested page key"),
        layer_index=_coerce_int(
            _field(obj, "layer_index", "layer_idx"), "requested page layer_index"
        ),
        page_index=_coerce_int(
            _field(obj, "page_index", "page_idx", "block_index"),
            "requested page page_index",
        ),
        len_bytes=_coerce_int(
            _field(
                obj,
                "len_bytes",
                "expected_len",
                "expected_len_bytes",
                "page_size_bytes",
                "size_bytes",
            ),
            "requested page len_bytes",
        ),
    )


def _normalize_block_allocation(obj: Any) -> BlockAllocation:
    return BlockAllocation(
        block_index=_coerce_int(
            _field(obj, "block_index", "page_index", "page_idx", "slot_index"),
            "block allocation block_index",
        ),
        dst_device_ptr=_coerce_int(
            _field(
                obj,
                "dst_device_ptr",
                "device_ptr",
                "dst_ptr",
                "ptr",
                "base_ptr",
            ),
            "block allocation dst_device_ptr",
        ),
        block_size_bytes=_coerce_int(
            _field(obj, "block_size_bytes", "page_size_bytes", "size_bytes"),
            "block allocation block_size_bytes",
        ),
    )


class FastCacheVllmConnectorShim:
    def __init__(
        self,
        store: Any,
        *,
        expected_vllm_version: str = SUPPORTED_VLLM_VERSION,
        validate_version: bool = False,
        installed_vllm_version: str | None = None,
        path_version: str = HOST_DIRECT_V1_PATH_VERSION,
    ) -> None:
        self._store = store
        self._expected_vllm_version = expected_vllm_version
        self._path_version = str(path_version or HOST_DIRECT_V1_PATH_VERSION)
        if validate_version:
            assert_supported_vllm_version(
                installed_vllm_version, expected=expected_vllm_version
            )

    def normalize_requested_pages(
        self, requested_pages: Iterable[Any]
    ) -> list[RequestedPage]:
        return [_normalize_requested_page(page) for page in requested_pages]

    def normalize_block_allocations(
        self, block_allocations: Iterable[Any]
    ) -> list[BlockAllocation]:
        return [_normalize_block_allocation(block) for block in block_allocations]

    def translate_load_spec(
        self,
        *,
        session_prefix: Any,
        requested_pages: Iterable[Any],
        block_allocations: Iterable[Any],
        allocation_id: int = 0,
        device_ordinal: int = 0,
        stream_ordinal: int = 0,
        allow_cpu_fallback: bool = True,
        cuda_enabled: bool = True,
        cpu_fallback_host_ptr: int | None = None,
        cpu_fallback_base_offset_bytes: int = 0,
        cpu_fallback_allocation_id: int = 0,
        path_version: str | None = None,
    ) -> dict[str, Any]:
        normalized_pages = self.normalize_requested_pages(requested_pages)
        normalized_blocks = self.normalize_block_allocations(block_allocations)
        return {
            "session_prefix": _coerce_bytes(session_prefix, "session_prefix"),
            "requested_pages": [
                (
                    page.key,
                    page.layer_index,
                    page.page_index,
                    page.len_bytes,
                )
                for page in normalized_pages
            ],
            "block_allocations": [
                (
                    block.block_index,
                    block.dst_device_ptr,
                    block.block_size_bytes,
                )
                for block in normalized_blocks
            ],
            "allocation_id": int(allocation_id),
            "device_ordinal": int(device_ordinal),
            "stream_ordinal": int(stream_ordinal),
            "allow_cpu_fallback": bool(allow_cpu_fallback),
            "cuda_enabled": bool(cuda_enabled),
            "cpu_fallback_host_ptr": (
                None if cpu_fallback_host_ptr is None else int(cpu_fallback_host_ptr)
            ),
            "cpu_fallback_base_offset_bytes": int(cpu_fallback_base_offset_bytes),
            "cpu_fallback_allocation_id": int(cpu_fallback_allocation_id),
            "path_version": str(path_version or self._path_version),
        }

    def restore_paged(self, **kwargs: Any) -> Any:
        spec = self.translate_load_spec(**kwargs)
        return self._store.restore_vllm_paged(**spec)

    def submit_paged(self, **kwargs: Any) -> Any:
        spec = self.translate_load_spec(**kwargs)
        return self._store.submit_vllm_paged_restore(**spec)

    def submit_normalized_paged(
        self,
        *,
        session_prefix: Any,
        requested_pages: Iterable[tuple[bytes, int, int, int]],
        block_allocations: Iterable[tuple[int, int, int]],
        allocation_id: int = 0,
        device_ordinal: int = 0,
        stream_ordinal: int = 0,
        allow_cpu_fallback: bool = True,
        cuda_enabled: bool = True,
        cpu_fallback_host_ptr: int | None = None,
        cpu_fallback_base_offset_bytes: int = 0,
        cpu_fallback_allocation_id: int = 0,
        path_version: str | None = None,
    ) -> Any:
        return self._store.submit_vllm_paged_restore(
            session_prefix=_coerce_bytes(session_prefix, "session_prefix"),
            requested_pages=list(requested_pages),
            block_allocations=list(block_allocations),
            allocation_id=int(allocation_id),
            device_ordinal=int(device_ordinal),
            stream_ordinal=int(stream_ordinal),
            allow_cpu_fallback=bool(allow_cpu_fallback),
            cuda_enabled=bool(cuda_enabled),
            cpu_fallback_host_ptr=(
                None if cpu_fallback_host_ptr is None else int(cpu_fallback_host_ptr)
            ),
            cpu_fallback_base_offset_bytes=int(cpu_fallback_base_offset_bytes),
            cpu_fallback_allocation_id=int(cpu_fallback_allocation_id),
            path_version=str(path_version or self._path_version),
        )

    def restore_from_start_load_kv(self, **kwargs: Any) -> Any:
        return self.submit_paged(**kwargs)
