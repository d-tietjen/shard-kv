from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import os
from typing import Any, Iterable, Mapping

from .connector import (
    FastCacheVllmConnectorShim,
    HOST_DIRECT_V1_PATH_VERSION,
    _coerce_bytes,
    _coerce_int,
    _field,
)

_MISSING = object()
_DEFAULT_STORE_SINGLETONS: dict[tuple[tuple[str, Any], ...], Any] = {}

try:  # pragma: no cover - exercised only in a real vLLM environment
    from vllm.distributed.kv_transfer.kv_connector.v1.base import KVConnectorBase_V1
except ImportError:  # pragma: no cover - default in local non-vLLM development
    class KVConnectorBase_V1(ABC):
        def __init__(
            self,
            vllm_config: Any = None,
            role: Any = None,
            kv_cache_config: Any | None = None,
        ) -> None:
            self._connector_metadata: Any | None = None
            self._vllm_config = vllm_config
            self._role = role
            self._kv_cache_config = kv_cache_config

        @property
        def role(self) -> Any:
            return self._role

        def bind_connector_metadata(self, connector_metadata: Any) -> None:
            self._connector_metadata = connector_metadata

        def clear_connector_metadata(self) -> None:
            self._connector_metadata = None

        def _get_connector_metadata(self) -> Any:
            assert self._connector_metadata is not None
            return self._connector_metadata

        def has_connector_metadata(self) -> bool:
            return self._connector_metadata is not None

        def register_kv_caches(self, kv_caches: dict[str, Any]) -> None:
            return

        def register_cross_layers_kv_cache(
            self, kv_cache: Any, attn_backend: type[Any]
        ) -> None:
            return

        def set_host_xfer_buffer_ops(self, copy_operation: Any) -> None:
            return

        def handle_preemptions(self, kv_connector_metadata: Any) -> None:
            return

        @abstractmethod
        def start_load_kv(self, forward_context: Any, **kwargs: Any) -> None:
            raise NotImplementedError

        @abstractmethod
        def wait_for_layer_load(self, layer_name: str) -> None:
            raise NotImplementedError

        @abstractmethod
        def save_kv_layer(
            self, layer_name: str, kv_layer: Any, attn_metadata: Any, **kwargs: Any
        ) -> None:
            raise NotImplementedError

        @abstractmethod
        def wait_for_save(self) -> None:
            raise NotImplementedError

        @abstractmethod
        def build_connector_meta(self, scheduler_output: Any, **kwargs: Any) -> Any:
            raise NotImplementedError

        @abstractmethod
        def update_state_after_alloc(
            self, request: Any, blocks: Any, num_external_tokens: int, **kwargs: Any
        ) -> Any:
            raise NotImplementedError

        @abstractmethod
        def get_num_new_matched_tokens(
            self, request: Any, num_computed_tokens: int
        ) -> tuple[int | None, bool]:
            raise NotImplementedError


def _maybe_set(target: Any, name: str, value: Any) -> None:
    if isinstance(target, dict):
        target[name] = value
    else:
        setattr(target, name, value)


def _maybe_get(target: Any, name: str, default: Any = None) -> Any:
    if isinstance(target, Mapping):
        return target.get(name, default)
    return getattr(target, name, default)


def _request_id(request: Any) -> Any:
    return _field(
        request,
        "request_id",
        "req_id",
        "id",
        default=id(request),
    )


def _extract_layer_index(layer_name: Any) -> int | None:
    if layer_name is None:
        return None
    if isinstance(layer_name, int):
        return layer_name
    text = str(layer_name)
    digits = []
    for char in reversed(text):
        if char.isdigit():
            digits.append(char)
        elif digits:
            break
    if not digits:
        return None
    return int("".join(reversed(digits)))


def _normalize_page_tuple(page: Any) -> tuple[bytes, int, int, int]:
    return (
        _coerce_bytes(_field(page, "key", "cache_key"), "requested page key"),
        _coerce_int(_field(page, "layer_index", "layer_idx"), "requested page layer_index"),
        _coerce_int(_field(page, "page_index", "page_idx"), "requested page page_index"),
        _coerce_int(
            _field(page, "len_bytes", "size_bytes", "expected_len_bytes"),
            "requested page len_bytes",
        ),
    )


def _normalize_block_tuple(block: Any) -> tuple[int, int, int]:
    return (
        _coerce_int(_field(block, "block_index", "page_index"), "block_index"),
        _coerce_int(_field(block, "dst_device_ptr", "device_ptr"), "dst_device_ptr"),
        _coerce_int(
            _field(block, "block_size_bytes", "page_size_bytes", "size_bytes"),
            "block_size_bytes",
        ),
    )


def _required_block_ids_for_pages(
    pages: list[tuple[bytes, int, int, int]],
    blocks: list[tuple[int, int, int]],
) -> set[int]:
    by_block_id = {block_id: size_bytes for block_id, _, size_bytes in blocks}
    ordered_block_ids = [block_id for block_id, _, _ in blocks]
    ordered_positions = {block_id: idx for idx, block_id in enumerate(ordered_block_ids)}
    required: set[int] = set()
    for _, _, page_index, len_bytes in pages:
        block_id = int(page_index)
        pos = ordered_positions.get(block_id)
        if pos is None:
            raise ValueError(f"missing block allocation for requested page {page_index}")
        remaining = int(len_bytes)
        while remaining > 0 and pos < len(ordered_block_ids):
            current_id = ordered_block_ids[pos]
            required.add(current_id)
            remaining -= by_block_id[current_id]
            pos += 1
        if remaining > 0:
            raise ValueError(
                f"requested page {page_index} needs more block capacity than provided"
            )
    return required


def _group_pages_by_layer(
    requested_pages: Iterable[Any],
    block_allocations: Iterable[Any],
    *,
    allow_empty_blocks: bool = False,
) -> list[tuple[int, list[tuple[bytes, int, int, int]], list[tuple[int, int, int]]]]:
    pages = [_normalize_page_tuple(page) for page in requested_pages]
    blocks = [_normalize_block_tuple(block) for block in block_allocations]
    pages_by_layer: dict[int, list[tuple[bytes, int, int, int]]] = {}
    for page in pages:
        pages_by_layer.setdefault(page[1], []).append(page)
    grouped = []
    for layer_index in sorted(pages_by_layer):
        layer_pages = pages_by_layer[layer_index]
        if not blocks and allow_empty_blocks:
            layer_blocks = []
        else:
            needed_blocks = _required_block_ids_for_pages(layer_pages, blocks)
            layer_blocks = [block for block in blocks if block[0] in needed_blocks]
        grouped.append((layer_index, layer_pages, layer_blocks))
    return grouped


def _normalize_save_record(obj: Any) -> tuple[bytes, bytes]:
    key = _coerce_bytes(_field(obj, "key", "cache_key", "chunk_key"), "save record key")
    value = _coerce_bytes(
        _field(obj, "value", "payload", "chunk_bytes", "data"),
        "save record value",
    )
    return key, value


def _coerce_items(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, (bytes, bytearray, memoryview, str)):
        return [value]
    if isinstance(value, Mapping):
        return [value]
    if hasattr(value, "shape") or hasattr(value, "dtype"):
        return [value]
    if hasattr(value, "metadata") or hasattr(value, "byte_array"):
        return [value]
    if isinstance(value, Iterable):
        return list(value)
    return [value]


def _first_present(*sources: Any, names: tuple[str, ...]) -> Any | None:
    for source in sources:
        if source is None:
            continue
        for name in names:
            value = _maybe_get(source, name, _MISSING)
            if value is not _MISSING and value is not None:
                return value
    return None


def _select_layer_value(value: Any, layer_name: str | None) -> Any:
    if value is None or layer_name is None or not isinstance(value, Mapping):
        return value
    layer_index = _extract_layer_index(layer_name)
    candidates = [layer_name]
    if layer_index is not None:
        candidates.extend((layer_index, str(layer_index)))
    for candidate in candidates:
        if candidate in value:
            return value[candidate]
    return value


def _first_layer_value(
    layer_name: str | None,
    *sources: Any,
    names: tuple[str, ...],
) -> Any | None:
    return _select_layer_value(_first_present(*sources, names=names), layer_name)


def _maybe_bytes_list(value: Any, label: str) -> list[bytes] | None:
    if value is None:
        return None
    try:
        return [_coerce_bytes(item, label) for item in _coerce_items(value)]
    except TypeError:
        return None


def _extract_request_metadata(request: Any) -> dict[str, Any]:
    if request is None:
        return {}

    metadata: dict[str, Any] = {}

    session_prefix = _first_present(request, names=("session_prefix", "kv_session_prefix"))
    if session_prefix is not None:
        metadata["session_prefix"] = _coerce_bytes(session_prefix, "session_prefix")

    requested_pages = _first_present(
        request,
        names=("requested_pages", "pages", "load_pages"),
    )
    if requested_pages is not None:
        metadata["requested_pages"] = list(requested_pages)

    block_allocations = _first_present(
        request,
        names=("block_allocations", "blocks"),
    )
    if block_allocations is not None:
        metadata["block_allocations"] = list(block_allocations)

    for name in (
        "allocation_id",
        "device_ordinal",
        "stream_ordinal",
        "cpu_fallback_host_ptr",
        "cpu_fallback_base_offset_bytes",
        "cpu_fallback_allocation_id",
        "num_new_matched_tokens",
        "num_external_tokens",
        "resolved_num_new_matched_tokens",
        "path_version",
    ):
        value = _maybe_get(request, name, _MISSING)
        if value is not _MISSING:
            metadata[name] = value

    for name in ("allow_cpu_fallback", "cuda_enabled", "needs_remote"):
        value = _maybe_get(request, name, _MISSING)
        if value is not _MISSING:
            metadata[name] = bool(value)

    return metadata


def _normalize_save_records_from(value: Any) -> list[tuple[bytes, bytes]] | None:
    if value is None:
        return None
    items = _coerce_items(value)
    try:
        return [_normalize_save_record(item) for item in items]
    except (TypeError, ValueError, AttributeError):
        return None


def _normalize_allocated_blocks(
    blocks: Any | None,
) -> tuple[list[Any], tuple[list[int], ...] | None]:
    if blocks is None:
        return ([], None)
    if hasattr(blocks, "get_block_ids"):
        block_id_groups = blocks.get_block_ids(allow_none=True)
        if block_id_groups is None:
            return ([], tuple())
        return ([], tuple(list(group) for group in block_id_groups))
    try:
        return (list(blocks), None)
    except TypeError:
        return ([], None)


def _restore_report_summary(report: Any) -> dict[str, int | bool]:
    page_count = _maybe_get(report, "page_count")
    hit_pages = _maybe_get(report, "hit_pages")
    missed_pages = _maybe_get(report, "missed_pages")
    all_hit = _maybe_get(report, "all_hit")

    if page_count is None:
        if hit_pages is not None and missed_pages is not None:
            page_count = int(hit_pages) + int(missed_pages)
        elif hit_pages is not None and bool(all_hit):
            page_count = int(hit_pages)
        elif missed_pages is not None and bool(all_hit):
            page_count = int(missed_pages)
        else:
            page_count = 0
    page_count = max(0, int(page_count))

    if hit_pages is None:
        if bool(all_hit):
            hit_pages = page_count
        elif missed_pages is not None:
            hit_pages = max(0, page_count - int(missed_pages))
        else:
            hit_pages = 0
    hit_pages = max(0, int(hit_pages))

    if missed_pages is None:
        missed_pages = max(0, page_count - hit_pages)
    missed_pages = max(0, int(missed_pages))

    if all_hit is None:
        all_hit = page_count == 0 or missed_pages == 0

    return {
        "page_count": page_count,
        "hit_pages": hit_pages,
        "missed_pages": missed_pages,
        "all_hit": bool(all_hit),
    }


def _empty_restore_report() -> dict[str, int | bool | str]:
    return {
        "backend": "none",
        "page_count": 0,
        "hit_pages": 0,
        "missed_pages": 0,
        "all_hit": True,
    }


@dataclass
class _RequestLoadState:
    request_id: Any
    expected_page_count: int = 0
    pending_layer_handles: dict[int, Any] = field(default_factory=dict)
    attached_layer_handles: dict[int, Any] = field(default_factory=dict)
    layer_reports: dict[int, Any] = field(default_factory=dict)
    layer_report_summaries: dict[int, dict[str, int | bool]] = field(default_factory=dict)
    pending_layer_device_ordinals: dict[int, int] = field(default_factory=dict)
    last_report: Any | None = None
    active_block_load_signature: tuple[Any, ...] | None = None
    completed_block_load_signature: tuple[Any, ...] | None = None


@dataclass
class _RequestSaveState:
    request_id: Any
    saved_block_count: int = 0
    buffered_generation: tuple[Any, ...] | None = None
    buffered_session_prefix: bytes | None = None
    buffered_layer_payloads: dict[int, list[bytes]] = field(default_factory=dict)
    buffered_target_block_count: int = 0
    buffered_block_hashes: tuple[bytes, ...] = ()


def _env_names(name: str) -> tuple[str, ...]:
    if name.startswith("FAST_CACHE_"):
        return (f"SHARDCACHE_{name[len('FAST_CACHE_'):]}", name)
    if name.startswith("SHARDCACHE_"):
        return (name, f"FAST_CACHE_{name[len('SHARDCACHE_'):]}")
    return (name,)


def _env_raw(name: str) -> str | None:
    for candidate in _env_names(name):
        raw = os.getenv(candidate)
        if raw is not None:
            return raw
    return None


def _env_flag(name: str, default: bool) -> bool:
    raw = _env_raw(name)
    if raw is None:
        return default
    return raw.strip().lower() not in ("", "0", "false", "no", "off")


def _env_int(name: str, default: int) -> int:
    raw = _env_raw(name)
    if raw is None or raw == "":
        return default
    return _coerce_int(raw, name)


def _env_str(name: str, default: str) -> str:
    raw = _env_raw(name)
    if raw is None or raw == "":
        return default
    return raw


def _debug_log(message: str) -> None:
    if not _env_flag("FAST_CACHE_VLLM_DEBUG", False):
        return
    print(f"[shardcache-vllm-direct pid={os.getpid()}] {message}", flush=True)


def _maybe_len(value: Any) -> int | None:
    try:
        return len(value)
    except Exception:
        return None


def _request_block_hashes(request: Any) -> list[bytes]:
    raw_hashes = _maybe_get(request, "block_hashes", ()) or ()
    hashes: list[bytes] = []
    for block_hash in raw_hashes:
        hashes.append(_coerce_bytes(block_hash, "block_hash"))
    return hashes


def _first_block_id_group(value: Any) -> list[int]:
    if value is None:
        return []
    if isinstance(value, tuple) and value:
        return [_coerce_int(item, "block_id") for item in value[0]]
    if isinstance(value, list):
        if value and isinstance(value[0], list):
            return [_coerce_int(item, "block_id") for item in value[0]]
        return [_coerce_int(item, "block_id") for item in value]
    return []


def _resolve_model_tag(vllm_config: Any) -> str:
    model_config = _maybe_get(vllm_config, "model_config")
    for source in (model_config, vllm_config):
        for name in ("served_model_name", "model_tag", "model"):
            value = _maybe_get(source, name, _MISSING)
            if value is not _MISSING and value not in (None, ""):
                return str(value)
    return "unknown-model"


def _resolve_block_size(
    vllm_config: Any,
    kv_cache_config: Any | None,
) -> int:
    default = _env_int("FAST_CACHE_VLLM_BLOCK_SIZE", 16)
    for source in (
        _maybe_get(vllm_config, "cache_config"),
        kv_cache_config,
    ):
        value = _maybe_get(source, "block_size", _MISSING)
        if value is not _MISSING and value not in (None, 0):
            return max(1, _coerce_int(value, "block_size"))
    return max(1, int(default))


def _resolve_num_layers(vllm_config: Any) -> int:
    model_config = _maybe_get(vllm_config, "model_config")
    parallel_config = _maybe_get(vllm_config, "parallel_config")
    if model_config is not None and hasattr(model_config, "get_num_layers"):
        try:
            return max(0, int(model_config.get_num_layers(parallel_config)))
        except Exception:
            pass
    return max(0, _env_int("FAST_CACHE_VLLM_NUM_LAYERS", 0))


def _default_session_prefix_for_model(model_tag: str) -> bytes:
    return f"shardcache:vllm:{model_tag}".encode("utf-8")


def _encode_vllm_page_key(block_hash: bytes, layer_index: int) -> bytes:
    return f"vllm-page:{layer_index}:".encode("utf-8") + block_hash.hex().encode("ascii")


def _extract_layer_page(kv_layer: Any, block_id: int, attn_metadata: Any) -> Any:
    metadata_type = type(attn_metadata).__name__ if attn_metadata is not None else ""
    if metadata_type in {"MLACommonMetadata", "TritonAttentionMetadata"}:
        return kv_layer[block_id, ...]
    shape = _maybe_get(kv_layer, "shape")
    shape_len = _maybe_len(shape) if shape is not None else None
    if shape_len is not None and shape_len >= 2:
        try:
            if int(shape[0]) == 2:
                return kv_layer[:, block_id, ...]
        except Exception:
            pass
    return kv_layer[block_id, ...]


def _page_payload_bytes(page: Any) -> bytes:
    current = page
    try:  # pragma: no cover - exercised in real torch/vLLM environments
        import torch

        if isinstance(current, torch.Tensor):
            tensor = current.detach().contiguous()
            if getattr(tensor, "device", None) is not None and tensor.device.type != "cpu":
                tensor = tensor.cpu()
            return tensor.view(torch.uint8).numpy().tobytes()
    except Exception:
        pass
    for name in ("detach", "contiguous", "cpu"):
        method = getattr(current, name, None)
        if callable(method):
            current = method()
    if hasattr(current, "numpy"):
        try:
            array = current.numpy()
            if hasattr(array, "tobytes"):
                return array.tobytes()
        except Exception:
            pass
    if isinstance(current, memoryview):
        return current.tobytes()
    if isinstance(current, bytearray):
        return bytes(current)
    if isinstance(current, bytes):
        return current
    if hasattr(current, "tobytes"):
        try:
            return current.tobytes()
        except Exception:
            pass
    raise TypeError("unable to serialize kv page payload from tensor-like value")


def _copy_payload_into_page(page: Any, payload: Any) -> None:
    payload_view = memoryview(payload)

    try:  # pragma: no cover - exercised in real torch/vLLM environments
        import torch

        if isinstance(page, torch.Tensor):
            expected_len = int(page.numel()) * int(page.element_size())
            if len(payload_view) != expected_len:
                raise ValueError(
                    f"cached page payload length {len(payload_view)} does not match "
                    f"destination page byte length {expected_len}"
                )
            try:
                src = torch.frombuffer(payload_view, dtype=page.dtype)
            except Exception:
                raw = torch.frombuffer(payload_view, dtype=torch.uint8)
                src = torch.empty(int(page.numel()), dtype=page.dtype)
                src.view(torch.uint8).copy_(raw)
            src = src.reshape(tuple(page.shape))
            if getattr(page, "device", None) is not None and page.device.type != "cpu":
                src = src.to(device=page.device)
            page.copy_(src)
            return
    except Exception:
        pass

    if hasattr(page, "copy_from_bytes"):
        page.copy_from_bytes(payload_view)
        return
    if hasattr(page, "copy_"):
        page.copy_(bytes(payload_view))
        return
    raise TypeError("unable to restore cached kv page into destination tensor-like value")


class _PythonLayerLoadHandle:
    def __init__(self, loader: Any) -> None:
        self._loader = loader
        self._report: Any | None = None
        self._cancelled = False

    def _ensure_loaded(self) -> Any:
        if self._report is None:
            if self._cancelled:
                self._report = {
                    "backend": "python-block-cache",
                    "page_count": 0,
                    "hit_pages": 0,
                    "missed_pages": 0,
                    "all_hit": True,
                }
            else:
                self._report = self._loader()
        return self._report

    def is_ready(self) -> bool:
        return True

    def peek_report(self) -> Any:
        return self._ensure_loaded()

    def wait_on_stream(self, stream_ptr: int) -> bool:
        _ = stream_ptr
        self._ensure_loaded()
        return True

    def try_wait(self) -> Any:
        return self._ensure_loaded()

    def wait(self) -> Any:
        return self._ensure_loaded()

    def cancel(self) -> bool:
        self._cancelled = True
        return True


class _PythonSharedLayerLoadGroup:
    def __init__(self, loader: Any, layer_indices: Iterable[int]) -> None:
        self._loader = loader
        self._layer_indices = tuple(int(layer_index) for layer_index in layer_indices)
        self._reports: dict[int, Any] | None = None
        self._cancelled = False

    def _empty_report(self) -> dict[str, int | bool | str]:
        return {
            "backend": "python-block-cache",
            "page_count": 0,
            "hit_pages": 0,
            "missed_pages": 0,
            "all_hit": True,
        }

    def _ensure_loaded(self) -> dict[int, Any]:
        if self._reports is None:
            if self._cancelled:
                self._reports = {
                    layer_index: dict(self._empty_report())
                    for layer_index in self._layer_indices
                }
            else:
                loaded = self._loader()
                reports = {
                    int(layer_index): report
                    for layer_index, report in dict(loaded).items()
                }
                for layer_index in self._layer_indices:
                    reports.setdefault(layer_index, dict(self._empty_report()))
                self._reports = reports
        return self._reports

    def report_for(self, layer_index: int) -> Any:
        return self._ensure_loaded().get(int(layer_index), dict(self._empty_report()))

    def cancel(self) -> bool:
        self._cancelled = True
        return True


class _PythonSharedLayerLoadHandle:
    def __init__(self, group: _PythonSharedLayerLoadGroup, layer_index: int) -> None:
        self._group = group
        self._layer_index = int(layer_index)

    def is_ready(self) -> bool:
        return True

    def peek_report(self) -> Any:
        return self._group.report_for(self._layer_index)

    def wait_on_stream(self, stream_ptr: int) -> bool:
        _ = stream_ptr
        self._group.report_for(self._layer_index)
        return True

    def try_wait(self) -> Any:
        return self._group.report_for(self._layer_index)

    def wait(self) -> Any:
        return self._group.report_for(self._layer_index)

    def cancel(self) -> bool:
        return self._group.cancel()


def _resolve_default_store_kwargs(
    store_kwargs: Mapping[str, Any] | None,
) -> dict[str, Any]:
    resolved: dict[str, Any] = {
        "cores": _env_int("FAST_CACHE_VLLM_CORES", 1),
        "route_mode": _env_str("FAST_CACHE_VLLM_ROUTE_MODE", "session_prefix"),
        "client_architecture": _env_str(
            "FAST_CACHE_VLLM_CLIENT_ARCHITECTURE", "local_embedded"
        ),
    }
    if _env_raw("FAST_CACHE_VLLM_ENABLE_METRICS") is not None:
        resolved["enable_metrics"] = _env_flag(
            "FAST_CACHE_VLLM_ENABLE_METRICS", False
        )
    if store_kwargs is not None:
        resolved.update(dict(store_kwargs))
    return resolved


def _default_store_singleton_key(store_kwargs: Mapping[str, Any]) -> tuple[tuple[str, Any], ...]:
    normalized: list[tuple[str, Any]] = []
    for key, value in store_kwargs.items():
        if isinstance(value, bytearray):
            value = bytes(value)
        elif isinstance(value, list):
            value = tuple(value)
        elif isinstance(value, dict):
            value = tuple(sorted(value.items()))
        normalized.append((str(key), value))
    return tuple(sorted(normalized))


class FastCacheKVConnectorV1(KVConnectorBase_V1):
    """Pinned vLLM 0.17.1-oriented control shim for direct shardcache restore."""

    def _initialize_local_base_state(
        self,
        vllm_config: Any,
        role: Any,
        kv_cache_config: Any | None,
    ) -> None:
        self._connector_metadata: Any | None = None
        self._vllm_config = vllm_config
        self._role = role
        self._kv_cache_config = kv_cache_config

    @property
    def role(self) -> Any:
        return getattr(self, "_role", None)

    def clear_connector_metadata(self) -> None:
        self._connector_metadata = None

    def _get_connector_metadata(self) -> Any:
        assert self._connector_metadata is not None
        return self._connector_metadata

    def has_connector_metadata(self) -> bool:
        return self._connector_metadata is not None

    def __init__(
        self,
        vllm_config: Any = None,
        role: Any = None,
        kv_cache_config: Any | None = None,
        *,
        store: Any | None = None,
        store_factory: Any | None = None,
        store_kwargs: Mapping[str, Any] | None = None,
        expected_vllm_version: str = "0.17.1",
        validate_version: bool = False,
        installed_vllm_version: str | None = None,
        device_ordinal: int = 0,
        stream_ordinal: int = 0,
        allow_cpu_fallback: bool = True,
        cuda_enabled: bool = True,
        path_version: str = HOST_DIRECT_V1_PATH_VERSION,
    ) -> None:
        self._initialize_local_base_state(vllm_config, role, kv_cache_config)
        try:
            super().__init__(vllm_config, role, kv_cache_config)
        except AttributeError as exc:
            if "kv_transfer_config" not in str(exc):
                raise
        expected_vllm_version = _env_str(
            "FAST_CACHE_VLLM_EXPECTED_VERSION", expected_vllm_version
        )
        validate_version = _env_flag(
            "FAST_CACHE_VLLM_VALIDATE_VERSION", validate_version
        )
        path_version = _env_str("FAST_CACHE_VLLM_PATH_VERSION", path_version)
        if store is None:
            if store_factory is not None:
                store = store_factory()
            else:
                try:
                    import shardcache as store_module  # type: ignore
                except ImportError:
                    import fast_cache as store_module  # type: ignore

                resolved_store_kwargs = _resolve_default_store_kwargs(store_kwargs)
                if _env_flag("FAST_CACHE_VLLM_SHARE_DEFAULT_STORE", True):
                    singleton_key = _default_store_singleton_key(resolved_store_kwargs)
                    store = _DEFAULT_STORE_SINGLETONS.get(singleton_key)
                    if store is None:
                        store = store_module.Store(**resolved_store_kwargs)
                        _DEFAULT_STORE_SINGLETONS[singleton_key] = store
                        _debug_log(
                            f"created shared default store id={id(store)} key={singleton_key}"
                        )
                    else:
                        _debug_log(
                            f"reused shared default store id={id(store)} key={singleton_key}"
                        )
                else:
                    store = store_module.Store(**resolved_store_kwargs)
                    _debug_log(f"created isolated default store id={id(store)}")
        self._shim = FastCacheVllmConnectorShim(
            store,
            expected_vllm_version=expected_vllm_version,
            validate_version=validate_version,
            installed_vllm_version=installed_vllm_version,
            path_version=path_version,
        )
        self._store = store
        self._path_version = path_version
        self._device_ordinal = _env_int(
            "FAST_CACHE_VLLM_DEVICE_ORDINAL", int(device_ordinal)
        )
        self._block_size = _resolve_block_size(vllm_config, kv_cache_config)
        self._num_layers = _resolve_num_layers(vllm_config)
        self._model_tag = _resolve_model_tag(vllm_config)
        self._cache_session_prefix = _default_session_prefix_for_model(self._model_tag)
        self._save_decode_cache = _env_flag("FAST_CACHE_VLLM_SAVE_DECODE_CACHE", False)
        self._stream_ordinal = _env_int(
            "FAST_CACHE_VLLM_STREAM_ORDINAL", int(stream_ordinal)
        )
        self._allow_cpu_fallback = _env_flag(
            "FAST_CACHE_VLLM_ALLOW_CPU_FALLBACK", bool(allow_cpu_fallback)
        )
        self._cuda_enabled = _env_flag(
            "FAST_CACHE_VLLM_CUDA_ENABLED", bool(cuda_enabled)
        )
        self._direct_stream_wait = _env_flag(
            "FAST_CACHE_VLLM_DIRECT_STREAM_WAIT", True
        )
        self._pending_layer_handles: dict[int, Any] = {}
        self._attached_layer_handles: dict[int, Any] = {}
        self._pending_layer_reports: dict[int, Any] = {}
        self._pending_layer_device_ordinals: dict[int, int] = {}
        self._last_load_report: Any | None = None
        self._kv_caches: dict[str, Any] = {}
        self._kv_cache_names_by_index: dict[int, str] = {}
        self._cross_layer_kv_cache: Any | None = None
        self._request_metadata: dict[Any, dict[str, Any]] = {}
        self._load_states: dict[Any, _RequestLoadState] = {}
        self._save_states: dict[Any, _RequestSaveState] = {}
        self._request_load_error_block_ids: dict[Any, list[int]] = {}
        self._finished_request_ids: list[Any] = []
        self._unfinished_requests: dict[Any, Any] = {}
        self._active_load_request_id: Any | None = None
        self._active_step_request_ids: list[Any] = []
        self._anonymous_request_seq = 0
        _debug_log(
            f"connector init role={role!r} store_id={id(self._store)} "
            f"path={self._path_version} model={self._model_tag}"
        )

    def register_kv_caches(self, kv_caches: dict[str, Any]) -> None:
        self._kv_caches = dict(kv_caches)
        self._kv_cache_names_by_index = {}
        for fallback_index, layer_name in enumerate(self._kv_caches):
            layer_index = _extract_layer_index(layer_name)
            if layer_index is None:
                layer_index = fallback_index
            self._kv_cache_names_by_index[layer_index] = layer_name
        _debug_log(
            f"registered kv caches count={len(self._kv_caches)} "
            f"layer_indices={sorted(self._kv_cache_names_by_index)[:4]}"
        )

    def register_cross_layers_kv_cache(
        self, kv_cache: Any, attn_backend: type[Any]
    ) -> None:
        _ = attn_backend
        self._cross_layer_kv_cache = kv_cache
        try:
            layer_count = len(kv_cache)
        except Exception:
            layer_count = -1
        _debug_log(f"registered cross-layer kv cache layers={layer_count}")

    def _probe_layer_indices(self) -> list[int]:
        if self._num_layers > 0:
            return list(range(self._num_layers))
        if self._kv_cache_names_by_index:
            return sorted(self._kv_cache_names_by_index)
        if self._cross_layer_kv_cache is not None:
            try:
                return list(range(len(self._cross_layer_kv_cache)))
            except Exception:
                return []
        return []

    def _load_layer_indices(self) -> list[int]:
        if self._kv_cache_names_by_index:
            return sorted(self._kv_cache_names_by_index)
        if self._cross_layer_kv_cache is not None:
            try:
                return list(range(len(self._cross_layer_kv_cache)))
            except Exception:
                return []
        return self._probe_layer_indices()

    def _expected_save_layer_count(self) -> int:
        if self._num_layers > 0:
            return self._num_layers
        if self._kv_cache_names_by_index:
            return len(self._kv_cache_names_by_index)
        if self._cross_layer_kv_cache is not None:
            try:
                return len(self._cross_layer_kv_cache)
            except Exception:
                return 1
        return 1

    def _layer_cache_for_index(self, layer_index: int) -> Any | None:
        layer_name = self._kv_cache_names_by_index.get(layer_index)
        if layer_name is not None:
            return self._kv_caches.get(layer_name)
        if self._cross_layer_kv_cache is not None:
            try:
                return self._cross_layer_kv_cache[layer_index]
            except Exception:
                return None
        return None

    def _request_total_token_count(self, request: Any) -> int:
        value = _maybe_get(request, "num_tokens", _MISSING)
        if value is not _MISSING and value is not None:
            return max(0, _coerce_int(value, "num_tokens"))
        token_ids = _first_present(request, names=("all_token_ids", "prompt_token_ids"))
        if token_ids is None:
            return 0
        return max(0, len(token_ids))

    def _store_session_all_hit(self, session_prefix: bytes, keys: list[bytes]) -> bool:
        if not keys:
            return True
        if hasattr(self._store, "batch_get_session_stats"):
            _, all_hit = self._store.batch_get_session_stats(session_prefix, list(keys))
            return bool(all_hit)
        if hasattr(self._store, "batch_get_session_view"):
            batch = self._store.batch_get_session_view(session_prefix, list(keys))
            return bool(batch.all_hit())
        if hasattr(self._store, "batch_get_session_packed"):
            batch = self._store.batch_get_session_packed(session_prefix, list(keys))
            return bool(batch.all_hit())
        raise AttributeError("store does not expose a batch session read API")

    def _probe_cached_prefix_block_hashes(
        self,
        session_prefix: bytes,
        block_hashes: list[bytes],
    ) -> list[bytes]:
        layer_indices = self._probe_layer_indices()
        if not layer_indices:
            return []
        if hasattr(self._store, "count_vllm_cached_prefix_blocks"):
            matched_count = _coerce_int(
                self._store.count_vllm_cached_prefix_blocks(
                    session_prefix,
                    list(block_hashes),
                    [int(layer_index) for layer_index in layer_indices],
                ),
                "cached prefix block count",
            )
            return list(block_hashes[:matched_count])
        cached: list[bytes] = []
        for block_hash in block_hashes:
            keys = [_encode_vllm_page_key(block_hash, layer_index) for layer_index in layer_indices]
            if not self._store_session_all_hit(session_prefix, keys):
                break
            cached.append(block_hash)
        return cached

    def _registered_block_load_signature(
        self,
        session_prefix: bytes,
        layer_indices: list[int],
        block_hashes: list[bytes],
        block_ids: list[int],
    ) -> tuple[Any, ...]:
        target_count = min(len(block_hashes), len(block_ids))
        return (
            bytes(session_prefix),
            tuple(int(layer_index) for layer_index in layer_indices),
            tuple(block_hashes[:target_count]),
            tuple(int(block_id) for block_id in block_ids[:target_count]),
        )

    def _request_load_metadata(
        self,
        request_id: Any,
        request: Any,
        num_external_tokens: int,
    ) -> dict[str, Any]:
        request_metadata = self._request_metadata.get(request_id, {})
        cached_prefix_block_hashes = list(
            request_metadata.get("cached_prefix_block_hashes", ())
        )
        if not cached_prefix_block_hashes or int(num_external_tokens) <= 0:
            return {
                "load_block_hashes": [],
                "load_block_ids": [],
                "cached_prefix_block_count": len(cached_prefix_block_hashes),
            }

        block_ids = _first_block_id_group(request_metadata.get("kv_cache_block_ids"))
        if not block_ids:
            return {
                "load_block_hashes": [],
                "load_block_ids": [],
                "cached_prefix_block_count": len(cached_prefix_block_hashes),
            }

        num_computed_tokens = max(
            0,
            _coerce_int(_maybe_get(request, "num_computed_tokens", 0), "num_computed_tokens"),
        )
        start_block = min(len(block_ids), max(0, num_computed_tokens // self._block_size))
        end_block = min(len(cached_prefix_block_hashes), len(block_ids))
        return {
            "load_block_hashes": cached_prefix_block_hashes[start_block:end_block],
            "load_block_ids": block_ids[start_block:end_block],
            "cached_prefix_block_count": len(cached_prefix_block_hashes),
        }

    def _flush_request_save_state(
        self,
        save_state: _RequestSaveState,
        ttl: Any | None = None,
    ) -> int:
        session_prefix = save_state.buffered_session_prefix
        if session_prefix is None or not save_state.buffered_layer_payloads:
            return 0

        record_count = 0
        if ttl in (None, 0) and hasattr(self._store, "batch_set_vllm_layer_payloads_no_ttl"):
            layer_groups = [
                (
                    int(layer_index),
                    list(save_state.buffered_block_hashes[: len(layer_payloads)]),
                    list(layer_payloads),
                )
                for layer_index, layer_payloads in sorted(
                    save_state.buffered_layer_payloads.items()
                )
            ]
            record_count = _coerce_int(
                self._store.batch_set_vllm_layer_payloads_no_ttl(
                    session_prefix,
                    layer_groups,
                ),
                "saved VLLM page count",
            )
        else:
            records: list[tuple[bytes, bytes]] = []
            for layer_index in sorted(save_state.buffered_layer_payloads):
                layer_payloads = save_state.buffered_layer_payloads[layer_index]
                layer_hashes = save_state.buffered_block_hashes[: len(layer_payloads)]
                records.extend(
                    (
                        _encode_vllm_page_key(block_hash, layer_index),
                        payload,
                    )
                    for block_hash, payload in zip(layer_hashes, layer_payloads, strict=False)
                )

            record_count = len(records)
            if ttl not in (None, 0):
                self._store.batch_set(records, ttl=ttl)
            elif hasattr(self._store, "batch_set_session_packed_no_ttl"):
                self._store.batch_set_session_packed_no_ttl(session_prefix, records)
            else:
                self._store.batch_set_session_no_ttl(session_prefix, records)

        _debug_log(
            f"published session_prefix={session_prefix!r} layers={len(save_state.buffered_layer_payloads)} "
            f"records={record_count} store_id={id(self._store)} "
            f"save_hashes={[block_hash.hex() for block_hash in save_state.buffered_block_hashes[:2]]}"
        )

        save_state.saved_block_count = max(
            save_state.saved_block_count,
            save_state.buffered_target_block_count,
        )
        save_state.buffered_generation = None
        save_state.buffered_session_prefix = None
        save_state.buffered_layer_payloads.clear()
        save_state.buffered_target_block_count = 0
        save_state.buffered_block_hashes = ()
        return record_count

    def _buffer_request_layer_save(
        self,
        save_state: _RequestSaveState,
        layer_index: int,
        request_session_prefix: bytes,
        save_block_hashes: list[bytes],
        save_block_ids: list[int],
        layer_payloads: list[bytes],
        target_saved_block_count: int,
        ttl: Any | None = None,
    ) -> int:
        generation = (
            request_session_prefix,
            tuple(save_block_hashes),
            tuple(save_block_ids),
            int(target_saved_block_count),
        )
        if (
            save_state.buffered_generation is not None
            and save_state.buffered_generation != generation
        ):
            self._flush_request_save_state(save_state, ttl=ttl)

        save_state.buffered_generation = generation
        save_state.buffered_session_prefix = request_session_prefix
        save_state.buffered_layer_payloads[layer_index] = list(layer_payloads)
        save_state.buffered_target_block_count = max(
            save_state.buffered_target_block_count,
            int(target_saved_block_count),
        )
        save_state.buffered_block_hashes = tuple(save_block_hashes)

        if len(save_state.buffered_layer_payloads) >= self._expected_save_layer_count():
            return self._flush_request_save_state(save_state, ttl=ttl)
        return 0

    def _restore_registered_layer_blocks(
        self,
        layer_index: int,
        session_prefix: bytes,
        block_hashes: list[bytes],
        block_ids: list[int],
    ) -> dict[str, Any]:
        kv_cache = self._layer_cache_for_index(layer_index)
        if kv_cache is None:
            raise RuntimeError(
                f"registered KV cache for layer {layer_index} is unavailable"
            )

        target_count = min(len(block_hashes), len(block_ids))
        if target_count == 0:
            return {
                "backend": "python-block-cache",
                "page_count": 0,
                "hit_pages": 0,
                "missed_pages": 0,
                "all_hit": True,
            }

        if hasattr(self._store, "restore_vllm_pages_into_layer"):
            page_count, hit_pages, missed_pages, all_hit = (
                self._store.restore_vllm_pages_into_layer(
                    session_prefix,
                    int(layer_index),
                    list(block_hashes[:target_count]),
                    list(block_ids[:target_count]),
                    kv_cache,
                )
            )
            return {
                "backend": "rust-block-cache",
                "page_count": _coerce_int(page_count, "page_count"),
                "hit_pages": _coerce_int(hit_pages, "hit_pages"),
                "missed_pages": _coerce_int(missed_pages, "missed_pages"),
                "all_hit": bool(all_hit),
            }

        if hasattr(self._store, "batch_get_vllm_pages_view"):
            batch = self._store.batch_get_vllm_pages_view(
                session_prefix,
                int(layer_index),
                list(block_hashes[:target_count]),
            )
        else:
            keys = [
                _encode_vllm_page_key(block_hash, layer_index)
                for block_hash in block_hashes[:target_count]
            ]
            if not hasattr(self._store, "batch_get_session_view"):
                raise AttributeError("store must expose batch_get_session_view for block loads")
            batch = self._store.batch_get_session_view(session_prefix, keys)

        hit_pages = 0
        missed_pages = 0
        for index, block_id in enumerate(block_ids[:target_count]):
            payload = batch.memoryview_at(index)
            if payload is None:
                missed_pages += 1
                continue
            _copy_payload_into_page(_extract_layer_page(kv_cache, block_id, None), payload)
            hit_pages += 1

        return {
            "backend": "python-block-cache",
            "page_count": target_count,
            "hit_pages": hit_pages,
            "missed_pages": missed_pages,
            "all_hit": missed_pages == 0,
        }

    def _restore_registered_layer_group_blocks(
        self,
        layer_indices: Iterable[int],
        session_prefix: bytes,
        block_hashes: list[bytes],
        block_ids: list[int],
    ) -> dict[int, dict[str, Any]]:
        normalized_layer_indices = [int(layer_index) for layer_index in layer_indices]
        target_count = min(len(block_hashes), len(block_ids))
        if target_count == 0:
            return {
                layer_index: {
                    "backend": "python-block-cache",
                    "page_count": 0,
                    "hit_pages": 0,
                    "missed_pages": 0,
                    "all_hit": True,
                }
                for layer_index in normalized_layer_indices
            }

        if hasattr(self._store, "restore_vllm_pages_into_registered_layers"):
            kv_layers = []
            for layer_index in normalized_layer_indices:
                kv_cache = self._layer_cache_for_index(layer_index)
                if kv_cache is None:
                    raise RuntimeError(
                        f"registered KV cache for layer {layer_index} is unavailable"
                    )
                kv_layers.append(kv_cache)
            raw_reports = self._store.restore_vllm_pages_into_registered_layers(
                session_prefix,
                list(normalized_layer_indices),
                list(block_hashes[:target_count]),
                list(block_ids[:target_count]),
                kv_layers,
            )
            reports: dict[int, dict[str, Any]] = {}
            for (
                raw_layer_index,
                page_count,
                hit_pages,
                missed_pages,
                all_hit,
            ) in raw_reports:
                reports[int(raw_layer_index)] = {
                    "backend": "rust-block-cache-batch",
                    "page_count": _coerce_int(page_count, "page_count"),
                    "hit_pages": _coerce_int(hit_pages, "hit_pages"),
                    "missed_pages": _coerce_int(missed_pages, "missed_pages"),
                    "all_hit": bool(all_hit),
                }
            return reports

        return {
            layer_index: self._restore_registered_layer_blocks(
                layer_index,
                session_prefix,
                block_hashes,
                block_ids,
            )
            for layer_index in normalized_layer_indices
        }

    def _current_cuda_stream_ptr(self, device_ordinal: int) -> int | None:
        if not self._direct_stream_wait:
            return None
        try:  # pragma: no cover - exercised with real torch/cuda only
            import torch
        except ImportError:
            return None
        try:  # pragma: no cover - exercised with real torch/cuda only
            if not torch.cuda.is_available():
                return None
            stream = torch.cuda.current_stream(device=device_ordinal)
            raw = getattr(stream, "cuda_stream", None)
            if raw in (None, 0):
                return None
            return int(raw)
        except Exception:
            return None

    def _allocate_anonymous_request_id(self) -> str:
        self._anonymous_request_seq += 1
        return f"anon-load-{self._anonymous_request_seq}"

    def _resolve_active_request_id(self, request_id: Any | None = None) -> Any | None:
        if request_id is not None and request_id in self._load_states:
            return request_id
        if self.has_connector_metadata():
            bound_request_id = _maybe_get(self._get_connector_metadata(), "request_id")
            if bound_request_id in self._load_states:
                return bound_request_id
        for active_request_id in self._active_step_request_ids:
            if active_request_id in self._load_states:
                return active_request_id
        if self._active_load_request_id in self._load_states:
            return self._active_load_request_id
        if len(self._load_states) == 1:
            return next(iter(self._load_states))
        return request_id

    def _bind_load_state_views(self, request_id: Any | None = None) -> None:
        resolved_request_id = self._resolve_active_request_id(request_id)
        state = self._load_states.get(resolved_request_id)
        if state is None:
            self._pending_layer_handles = {}
            self._attached_layer_handles = {}
            self._pending_layer_reports = {}
            self._pending_layer_device_ordinals = {}
            self._last_load_report = None
            return
        self._pending_layer_handles = state.pending_layer_handles
        self._attached_layer_handles = state.attached_layer_handles
        self._pending_layer_reports = state.layer_reports
        self._pending_layer_device_ordinals = state.pending_layer_device_ordinals
        self._last_load_report = state.last_report

    def _mark_request_finished(self, request_id: Any) -> None:
        if request_id not in self._finished_request_ids:
            self._finished_request_ids.append(request_id)

    def _set_request_load_error_block_ids(
        self, request_id: Any, block_ids: Iterable[Any] | None
    ) -> None:
        if block_ids is None:
            self._request_load_error_block_ids.pop(request_id, None)
            return
        normalized = []
        seen: set[int] = set()
        for block_id in block_ids:
            normalized_block_id = _coerce_int(block_id, "block_id")
            if normalized_block_id in seen:
                continue
            seen.add(normalized_block_id)
            normalized.append(normalized_block_id)
        if normalized:
            self._request_load_error_block_ids[request_id] = normalized
        else:
            self._request_load_error_block_ids.pop(request_id, None)

    def _ensure_request_metadata(
        self, request_id: Any, metadata: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        merged = dict(self._request_metadata.get(request_id, {}))
        if metadata is not None:
            merged.update(dict(metadata))
        merged["request_id"] = request_id
        if (
            "num_new_matched_tokens" in merged
            and "scheduled_num_new_matched_tokens" not in merged
        ):
            merged["scheduled_num_new_matched_tokens"] = merged["num_new_matched_tokens"]
        self._request_metadata[request_id] = merged
        return merged

    def _capture_request_metadata(self, request: Any) -> dict[str, Any] | None:
        if request is None:
            return None
        request_id = _request_id(request)
        seed = _extract_request_metadata(request)
        if seed and "session_prefix" not in seed and _request_block_hashes(request):
            seed["session_prefix"] = self._cache_session_prefix
        if not seed and request_id not in self._request_metadata:
            return None
        return self._ensure_request_metadata(request_id, seed)

    def _request_save_metadata(
        self,
        request_id: Any,
        request: Any,
        scheduled_tokens: int,
    ) -> dict[str, Any]:
        request_metadata = self._request_metadata.get(request_id, {})
        if request is None:
            return {}

        try:
            num_computed_tokens = _coerce_int(
                _maybe_get(request, "num_computed_tokens", 0),
                "num_computed_tokens",
            )
        except TypeError:
            num_computed_tokens = 0
        total_tokens = max(0, num_computed_tokens + max(0, int(scheduled_tokens)))
        full_block_hashes = _request_block_hashes(request)
        if not full_block_hashes:
            return {}

        block_ids = _first_block_id_group(request_metadata.get("kv_cache_block_ids"))
        full_block_ids = block_ids[: len(full_block_hashes)]
        if not full_block_ids and block_ids:
            full_block_ids = block_ids
        materialized_block_count = min(len(full_block_hashes), len(full_block_ids))
        if materialized_block_count <= 0:
            return {}
        full_block_hashes = full_block_hashes[:materialized_block_count]
        full_block_ids = full_block_ids[:materialized_block_count]

        save_state = self._save_states.setdefault(
            request_id, _RequestSaveState(request_id=request_id)
        )
        cached_prefix_block_count = max(
            0,
            _coerce_int(
                request_metadata.get("cached_prefix_block_count", 0),
                "cached_prefix_block_count",
            ),
        )
        if cached_prefix_block_count > 0:
            save_state.saved_block_count = max(
                save_state.saved_block_count,
                cached_prefix_block_count,
            )
        effective_saved_block_count = max(
            save_state.saved_block_count,
            save_state.buffered_target_block_count,
        )
        scheduler_visible_block_count = min(
            materialized_block_count,
            total_tokens // self._block_size,
        )
        # In live vLLM chunked-prefill runs, num_computed_tokens/scheduled_tokens can lag
        # behind the fully materialized prefix blocks already exposed via block_hashes and
        # kv_cache_block_ids. Trust the materialized block view so we do not under-save the
        # prompt and miss almost the entire cache on replay.
        save_target_block_count = materialized_block_count

        if (
            not self._save_decode_cache
            and _coerce_int(_maybe_get(request, "num_output_tokens", 0), "num_output_tokens") > 0
            and scheduled_tokens <= 1
        ):
            save_target_block_count = effective_saved_block_count

        save_target_block_count = min(save_target_block_count, materialized_block_count)
        save_start_block = min(effective_saved_block_count, save_target_block_count)
        save_block_hashes = full_block_hashes[save_start_block:save_target_block_count]
        save_block_ids = full_block_ids[save_start_block:save_target_block_count]
        _debug_log(
            f"save-plan request={request_id} block_hashes={len(full_block_hashes)} "
            f"block_ids={len(full_block_ids)} visible_blocks={scheduler_visible_block_count} "
            f"saved_blocks={effective_saved_block_count} target_blocks={save_target_block_count} "
            f"save_start_block={save_start_block} save_blocks={len(save_block_hashes)} "
            f"scheduled_tokens={scheduled_tokens} num_computed_tokens={num_computed_tokens}"
        )

        return {
            "session_prefix": request_metadata.get("session_prefix", self._cache_session_prefix),
            "vllm_block_hashes": full_block_hashes,
            "vllm_block_ids": full_block_ids,
            "save_block_hashes": save_block_hashes,
            "save_block_ids": save_block_ids,
            "save_target_block_count": save_target_block_count,
        }

    def _build_step_request_metadata(self, scheduler_output: Any) -> list[dict[str, Any]]:
        request_entries: list[dict[str, Any]] = []
        seen_request_ids: set[Any] = set()
        scheduled_tokens_by_id = dict(_maybe_get(scheduler_output, "num_scheduled_tokens", {}) or {})

        for request in _maybe_get(scheduler_output, "scheduled_new_reqs", ()) or ():
            request_id = _request_id(request)
            if request_id in seen_request_ids:
                continue
            live_request = self._unfinished_requests.get(request_id, request)
            metadata = dict(
                self._capture_request_metadata(live_request)
                or self._request_metadata.get(request_id, {})
            )
            if not metadata and live_request is request and not _request_block_hashes(live_request):
                continue
            metadata.update(
                self._request_save_metadata(
                    request_id,
                    live_request,
                    _coerce_int(scheduled_tokens_by_id.get(request_id, 0), "scheduled_tokens"),
                )
            )
            metadata["request_id"] = request_id
            request_entries.append(metadata)
            seen_request_ids.add(request_id)

        cached_reqs = _maybe_get(scheduler_output, "scheduled_cached_reqs", ())
        if isinstance(cached_reqs, list):
            cached_req_ids = [_request_id(req) for req in cached_reqs]
        else:
            cached_req_ids = list(_maybe_get(cached_reqs, "req_ids", ()) or ())
        for request_id in cached_req_ids:
            if request_id in seen_request_ids:
                continue
            live_request = self._unfinished_requests.get(request_id)
            metadata = dict(self._request_metadata.get(request_id, {}))
            if live_request is not None:
                metadata.update(
                    self._request_save_metadata(
                        request_id,
                        live_request,
                        _coerce_int(scheduled_tokens_by_id.get(request_id, 0), "scheduled_tokens"),
                    )
                )
            if not metadata:
                continue
            metadata["request_id"] = request_id
            request_entries.append(metadata)
            seen_request_ids.add(request_id)

        return request_entries

    def _update_request_match_state(self, request_id: Any) -> None:
        metadata = self._request_metadata.get(request_id)
        state = self._load_states.get(request_id)
        if metadata is None or state is None:
            return

        expected_page_count = max(
            int(state.expected_page_count),
            len(metadata.get("requested_pages", ())),
        )
        reported_page_count = sum(
            int(summary["page_count"]) for summary in state.layer_report_summaries.values()
        )
        hit_pages = sum(
            int(summary["hit_pages"]) for summary in state.layer_report_summaries.values()
        )
        missed_pages = sum(
            int(summary["missed_pages"]) for summary in state.layer_report_summaries.values()
        )

        metadata["load_expected_pages"] = expected_page_count
        metadata["load_reported_pages"] = reported_page_count
        metadata["load_hit_pages"] = hit_pages
        metadata["load_missed_pages"] = missed_pages

        if expected_page_count <= 0 or reported_page_count < expected_page_count:
            return

        total_tokens = metadata.get("num_external_tokens")
        if total_tokens in (None, 0):
            total_tokens = metadata.get("scheduled_num_new_matched_tokens")
        if total_tokens in (None, 0):
            total_tokens = metadata.get("num_new_matched_tokens")

        if total_tokens is not None:
            total_token_count = _coerce_int(total_tokens, "matched token count")
            matched = int(round(total_token_count * (hit_pages / expected_page_count)))
            metadata["resolved_num_new_matched_tokens"] = max(0, matched)

        metadata["needs_remote"] = missed_pages > 0
        if missed_pages > 0:
            self._set_request_load_error_block_ids(
                request_id,
                metadata.get("load_block_ids", ()),
            )
        else:
            self._set_request_load_error_block_ids(request_id, None)
        if state.active_block_load_signature is not None:
            if (
                expected_page_count > 0
                and reported_page_count >= expected_page_count
                and missed_pages == 0
            ):
                state.completed_block_load_signature = state.active_block_load_signature
                state.active_block_load_signature = None
            elif missed_pages > 0:
                state.active_block_load_signature = None
        _debug_log(
            f"load-match request={request_id} expected_pages={expected_page_count} "
            f"reported_pages={reported_page_count} hit_pages={hit_pages} "
            f"missed_pages={missed_pages} scheduled_tokens={metadata.get('scheduled_num_new_matched_tokens')} "
            f"resolved_tokens={metadata.get('resolved_num_new_matched_tokens')} "
            f"needs_remote={metadata.get('needs_remote')}"
        )

    def _record_layer_report(self, request_id: Any, layer_index: int, report: Any) -> Any:
        state = self._load_states.get(request_id)
        if state is None:
            return report
        state.layer_reports[layer_index] = report
        state.layer_report_summaries[layer_index] = _restore_report_summary(report)
        state.last_report = report
        summary = state.layer_report_summaries[layer_index]
        _debug_log(
            f"layer-report request={request_id} layer={layer_index} "
            f"pages={summary['page_count']} hit_pages={summary['hit_pages']} "
            f"missed_pages={summary['missed_pages']} all_hit={summary['all_hit']}"
        )
        self._update_request_match_state(request_id)
        self._bind_load_state_views(request_id)
        return report

    def _prepare_lmcache_put_batch(
        self,
        encoded_keys: list[bytes],
        metadata_blobs: list[bytes],
    ) -> Any | None:
        if not hasattr(self._store, "prepare_lmcache_put_batch_encoded_keys"):
            return None
        return self._store.prepare_lmcache_put_batch_encoded_keys(
            list(encoded_keys),
            list(metadata_blobs),
        )

    def build_connector_meta(
        self, scheduler_output: Any | None = None, **kwargs: Any
    ) -> dict[str, Any]:
        if scheduler_output is not None and not kwargs:
            scheduled_new_reqs = _maybe_get(scheduler_output, "scheduled_new_reqs", _MISSING)
            scheduled_cached_reqs = _maybe_get(
                scheduler_output, "scheduled_cached_reqs", _MISSING
            )
            if scheduled_new_reqs is not _MISSING or scheduled_cached_reqs is not _MISSING:
                requests = self._build_step_request_metadata(scheduler_output)
                return {
                    "request_ids": [request["request_id"] for request in requests],
                    "requests": requests,
                    "path_version": self._path_version,
                }

        request = (
            _maybe_get(scheduler_output, "request")
            if scheduler_output is not None
            else kwargs.pop("request", None)
        )
        request_id = _request_id(request) if request is not None else kwargs.pop("request_id", None)
        metadata = (
            dict(self._capture_request_metadata(request) or self._request_metadata.get(request_id, {}))
            if request_id is not None
            else {}
        )
        if scheduler_output is not None and not kwargs:
            requested_pages = _field(
                scheduler_output,
                "requested_pages",
                "pages",
                "load_pages",
                default=_MISSING,
            )
            session_prefix = _field(
                scheduler_output,
                "session_prefix",
                "kv_session_prefix",
                default=_MISSING,
            )
            allocation_id = _maybe_get(scheduler_output, "allocation_id", _MISSING)
            device_ordinal = _maybe_get(scheduler_output, "device_ordinal", _MISSING)
            stream_ordinal = _maybe_get(scheduler_output, "stream_ordinal", _MISSING)
            allow_cpu_fallback = _maybe_get(
                scheduler_output, "allow_cpu_fallback", _MISSING
            )
            cuda_enabled = _maybe_get(scheduler_output, "cuda_enabled", _MISSING)
            extra = {
                key: value
                for key in ("num_new_matched_tokens",)
                if (value := _maybe_get(scheduler_output, key, _MISSING))
                is not _MISSING
            }
        else:
            requested_pages = kwargs.pop("requested_pages")
            session_prefix = kwargs.pop("session_prefix")
            allocation_id = kwargs.pop("allocation_id", _MISSING)
            device_ordinal = kwargs.pop("device_ordinal", _MISSING)
            stream_ordinal = kwargs.pop("stream_ordinal", _MISSING)
            allow_cpu_fallback = kwargs.pop("allow_cpu_fallback", _MISSING)
            cuda_enabled = kwargs.pop("cuda_enabled", _MISSING)
            extra = kwargs

        if session_prefix is _MISSING:
            metadata.setdefault("session_prefix", b"")
        else:
            metadata["session_prefix"] = _coerce_bytes(session_prefix, "session_prefix")
        if requested_pages is not _MISSING:
            metadata["requested_pages"] = list(requested_pages)
        else:
            metadata.setdefault("requested_pages", [])
        if allocation_id is _MISSING:
            metadata.setdefault("allocation_id", 0)
        else:
            metadata["allocation_id"] = int(allocation_id)
        if device_ordinal is _MISSING:
            metadata.setdefault("device_ordinal", int(self._device_ordinal))
        else:
            metadata["device_ordinal"] = int(device_ordinal)
        if stream_ordinal is _MISSING:
            metadata.setdefault("stream_ordinal", int(self._stream_ordinal))
        else:
            metadata["stream_ordinal"] = int(stream_ordinal)
        if allow_cpu_fallback is _MISSING:
            metadata.setdefault("allow_cpu_fallback", bool(self._allow_cpu_fallback))
        else:
            metadata["allow_cpu_fallback"] = bool(allow_cpu_fallback)
        if cuda_enabled is _MISSING:
            metadata.setdefault("cuda_enabled", bool(self._cuda_enabled))
        else:
            metadata["cuda_enabled"] = bool(cuda_enabled)
        if request_id is not None:
            metadata["request_id"] = request_id
        metadata.update(extra)
        if (
            "num_new_matched_tokens" in metadata
            and "scheduled_num_new_matched_tokens" not in metadata
        ):
            metadata["scheduled_num_new_matched_tokens"] = metadata["num_new_matched_tokens"]
        metadata.setdefault("path_version", self._path_version)
        metadata["block_allocations"] = list(metadata.get("block_allocations", ()))
        if request_id is not None:
            self._request_metadata[request_id] = dict(metadata)
        return metadata

    def bind_connector_metadata(self, *args: Any) -> Any:
        if len(args) == 1:
            connector_metadata = dict(args[0])
            super().bind_connector_metadata(connector_metadata)
            return None
        if len(args) == 2:
            target, connector_metadata = args
            connector_metadata = dict(connector_metadata)
            super().bind_connector_metadata(connector_metadata)
            _maybe_set(target, "fast_cache_connector_metadata", connector_metadata)
            return target
        raise TypeError("bind_connector_metadata expects metadata or (target, metadata)")

    def get_num_new_matched_tokens(
        self, request: Any, num_computed_tokens: int = 0
    ) -> tuple[int | None, bool]:
        request_id = _request_id(request)
        metadata = self._capture_request_metadata(request)
        if metadata is None:
            metadata = self._request_metadata.get(request_id)
        if metadata is None:
            metadata = self._ensure_request_metadata(request_id, {})

        if str(request_id).startswith("mock_req") or bool(
            _maybe_get(request, "skip_reading_prefix_cache", False)
        ):
            metadata["num_new_matched_tokens"] = 0
            metadata["scheduled_num_new_matched_tokens"] = 0
            metadata["resolved_num_new_matched_tokens"] = 0
            metadata["needs_remote"] = False
            metadata["cached_prefix_block_hashes"] = []
            metadata["cached_prefix_block_count"] = 0
            metadata["cached_prefix_tokens"] = 0
            return (0, False)

        block_hashes = _request_block_hashes(request)
        if block_hashes:
            session_prefix = _coerce_bytes(
                metadata.get("session_prefix", self._cache_session_prefix),
                "session_prefix",
            )
            cached_prefix_block_hashes = self._probe_cached_prefix_block_hashes(
                session_prefix,
                block_hashes,
            )
            total_cached_tokens = len(cached_prefix_block_hashes) * self._block_size
            request_total_tokens = self._request_total_token_count(request)
            matched = max(0, total_cached_tokens - int(num_computed_tokens))
            full_prompt_hit = (
                request_total_tokens > 0
                and request_total_tokens % self._block_size == 0
                and total_cached_tokens >= request_total_tokens
            )
            if full_prompt_hit and matched > 0:
                matched -= 1
            metadata["session_prefix"] = session_prefix
            metadata["cached_prefix_block_hashes"] = cached_prefix_block_hashes
            metadata["cached_prefix_block_count"] = len(cached_prefix_block_hashes)
            metadata["cached_prefix_tokens"] = total_cached_tokens
            metadata["request_total_tokens"] = request_total_tokens
            metadata["num_new_matched_tokens"] = matched
            metadata["scheduled_num_new_matched_tokens"] = matched
            metadata["resolved_num_new_matched_tokens"] = matched
            metadata["needs_remote"] = False
            _debug_log(
                f"probe request={request_id} store_id={id(self._store)} "
                f"cached_blocks={len(cached_prefix_block_hashes)}/{len(block_hashes)} "
                f"matched_tokens={matched} session_prefix={session_prefix!r} "
                f"probe_hashes={[block_hash.hex() for block_hash in block_hashes[:2]]}"
            )
            return (matched, False)

        value = metadata.get("resolved_num_new_matched_tokens")
        if value is None:
            value = metadata.get("num_new_matched_tokens")
        if value is None:
            value = metadata.get("num_external_tokens")
        needs_remote = bool(metadata.get("needs_remote", False))
        if value is None:
            return (0, needs_remote)
        matched = max(0, _coerce_int(value, "num_new_matched_tokens") - int(num_computed_tokens))
        return (matched, needs_remote)

    def update_state_after_alloc(
        self,
        request: Any,
        blocks: Any | None = None,
        num_external_tokens: int = 0,
        **kwargs: Any,
    ) -> dict[str, Any]:
        request_id = _request_id(request)
        metadata = dict(self._capture_request_metadata(request) or {})
        if not metadata and isinstance(request, Mapping):
            metadata = dict(request)
        if "session_prefix" not in metadata and _request_block_hashes(request):
            metadata["session_prefix"] = self._cache_session_prefix
        if blocks is None:
            blocks = kwargs.pop("block_allocations", ())
        normalized_blocks, kv_cache_block_ids = _normalize_allocated_blocks(blocks)
        metadata.setdefault("request_id", request_id)
        metadata["block_allocations"] = normalized_blocks
        if kv_cache_block_ids is not None:
            metadata["kv_cache_block_ids"] = kv_cache_block_ids
        metadata["num_external_tokens"] = int(num_external_tokens)
        metadata.update(kwargs)
        self._request_metadata[request_id] = dict(metadata)
        metadata.update(
            self._request_load_metadata(request_id, request, int(num_external_tokens))
        )
        _debug_log(
            f"alloc-state request={request_id} num_external_tokens={int(num_external_tokens)} "
            f"requested_pages={len(metadata.get('requested_pages', ()))} "
            f"block_allocations={len(metadata.get('block_allocations', ()))} "
            f"kv_cache_block_ids={len(_first_block_id_group(metadata.get('kv_cache_block_ids')))} "
            f"load_block_hashes={len(metadata.get('load_block_hashes', ()))} "
            f"load_block_ids={len(metadata.get('load_block_ids', ()))}"
        )
        if (
            "num_new_matched_tokens" in metadata
            and "scheduled_num_new_matched_tokens" not in metadata
        ):
            metadata["scheduled_num_new_matched_tokens"] = metadata["num_new_matched_tokens"]
        self._request_metadata[request_id] = metadata
        self._unfinished_requests[request_id] = request
        return metadata

    def _start_request_load(
        self,
        metadata: Mapping[str, Any],
        forward_context: Any | None = None,
    ) -> Any:
        request = metadata.get("request")
        request_id = metadata.get("request_id")
        if request_id is None and request is not None:
            request_id = _request_id(request)
        if request_id is None:
            forward_request = _maybe_get(forward_context, "request")
            if forward_request is not None:
                request_id = _request_id(forward_request)
        if request_id is None:
            request_id = _maybe_get(forward_context, "request_id")
        if request_id is None:
            request_id = self._allocate_anonymous_request_id()

        normalized_metadata = dict(metadata)
        normalized_metadata["request_id"] = request_id
        if "session_prefix" in normalized_metadata:
            normalized_metadata["session_prefix"] = _coerce_bytes(
                normalized_metadata["session_prefix"],
                "session_prefix",
            )
        else:
            normalized_metadata["session_prefix"] = b""
        normalized_metadata["requested_pages"] = list(
            normalized_metadata.get("requested_pages", ())
        )
        normalized_metadata["block_allocations"] = list(
            normalized_metadata.get("block_allocations", ())
        )
        metadata = self._ensure_request_metadata(request_id, normalized_metadata)
        for key in (
            "resolved_num_new_matched_tokens",
            "needs_remote",
            "load_expected_pages",
            "load_reported_pages",
            "load_hit_pages",
            "load_missed_pages",
        ):
            metadata.pop(key, None)
        state = self._load_states.get(request_id)
        if state is None:
            state = _RequestLoadState(request_id=request_id)
            self._load_states[request_id] = state
        if state.pending_layer_handles or state.attached_layer_handles:
            raise RuntimeError(
                f"direct shardcache connector request {request_id!r} still has in-flight layer loads"
            )
        load_block_hashes = [
            _coerce_bytes(block_hash, "block_hash")
            for block_hash in normalized_metadata.get("load_block_hashes", ())
        ]
        load_block_ids = [
            _coerce_int(block_id, "block_id")
            for block_id in normalized_metadata.get("load_block_ids", ())
        ]
        if load_block_hashes and load_block_ids:
            layer_indices = self._load_layer_indices()
            if not layer_indices:
                raise RuntimeError(
                    "direct shardcache block restore requires registered KV caches"
                )
            target_count = min(len(load_block_hashes), len(load_block_ids))
            load_signature = self._registered_block_load_signature(
                _coerce_bytes(metadata.get("session_prefix", b""), "session_prefix"),
                layer_indices,
                load_block_hashes,
                load_block_ids,
            )
            if (
                target_count > 0
                and state.completed_block_load_signature == load_signature
            ):
                state.expected_page_count = len(layer_indices) * target_count
                state.pending_layer_handles.clear()
                state.attached_layer_handles.clear()
                state.pending_layer_device_ordinals.clear()
                if state.last_report is None:
                    state.last_report = _empty_restore_report()
                _debug_log(
                    f"skip-load request={request_id} mode=registered-blocks "
                    f"load_blocks={target_count} reason=already-restored "
                    f"session_prefix={metadata.get('session_prefix', b'')!r}"
                )
                return request_id

        state.expected_page_count = len(metadata.get("requested_pages", ()))
        state.pending_layer_handles.clear()
        state.attached_layer_handles.clear()
        state.layer_reports.clear()
        state.layer_report_summaries.clear()
        state.pending_layer_device_ordinals.clear()
        state.last_report = None
        state.active_block_load_signature = None

        if load_block_hashes and load_block_ids:
            _debug_log(
                f"start-load request={request_id} mode=registered-blocks "
                f"load_blocks={min(len(load_block_hashes), len(load_block_ids))} "
                f"layer_count={len(layer_indices)} "
                f"scheduled_tokens={metadata.get('scheduled_num_new_matched_tokens')} "
                f"session_prefix={metadata.get('session_prefix', b'')!r}"
            )
            state.expected_page_count = len(layer_indices) * target_count
            state.active_block_load_signature = load_signature
            session_prefix = _coerce_bytes(
                metadata.get("session_prefix", b""),
                "session_prefix",
            )
            shared_group = _PythonSharedLayerLoadGroup(
                lambda: self._restore_registered_layer_group_blocks(
                    layer_indices,
                    session_prefix,
                    load_block_hashes[:target_count],
                    load_block_ids[:target_count],
                ),
                layer_indices,
            )
            for layer_index in layer_indices:
                state.pending_layer_handles[layer_index] = _PythonSharedLayerLoadHandle(
                    shared_group,
                    layer_index,
                )
                state.pending_layer_device_ordinals[layer_index] = self._device_ordinal
            if not state.pending_layer_handles:
                state.last_report = _empty_restore_report()
            return request_id

        _debug_log(
            f"start-load request={request_id} mode=paged-restore "
            f"requested_pages={len(metadata.get('requested_pages', ()))} "
            f"block_allocations={len(metadata.get('block_allocations', ()))} "
            f"scheduled_tokens={metadata.get('scheduled_num_new_matched_tokens')} "
            f"num_external_tokens={metadata.get('num_external_tokens')} "
            f"session_prefix={metadata.get('session_prefix', b'')!r}"
        )
        session_prefix = metadata.get("session_prefix", b"")
        allocation_id = _coerce_int(metadata.get("allocation_id", 0), "allocation_id")
        device_ordinal = _coerce_int(
            metadata.get("device_ordinal", self._device_ordinal),
            "device_ordinal",
        )
        stream_ordinal = _coerce_int(
            metadata.get("stream_ordinal", self._stream_ordinal),
            "stream_ordinal",
        )
        allow_cpu_fallback = bool(
            metadata.get("allow_cpu_fallback", self._allow_cpu_fallback)
        )
        cuda_enabled = bool(metadata.get("cuda_enabled", self._cuda_enabled))
        path_version = str(metadata.get("path_version", self._path_version))
        cpu_fallback_host_ptr = metadata.get("cpu_fallback_host_ptr")
        cpu_fallback_base_offset_bytes = _coerce_int(
            metadata.get("cpu_fallback_base_offset_bytes", 0),
            "cpu_fallback_base_offset_bytes",
        )
        cpu_fallback_allocation_id = _coerce_int(
            metadata.get("cpu_fallback_allocation_id", 0),
            "cpu_fallback_allocation_id",
        )
        allow_cpu_only_fallback = (
            allow_cpu_fallback
            and cpu_fallback_host_ptr not in (None, 0)
            and not metadata["block_allocations"]
        )
        next_cpu_fallback_offset = cpu_fallback_base_offset_bytes
        for layer_index, layer_pages, layer_blocks in _group_pages_by_layer(
            metadata["requested_pages"],
            metadata["block_allocations"],
            allow_empty_blocks=allow_cpu_only_fallback,
        ):
            layer_cpu_fallback_base_offset_bytes = cpu_fallback_base_offset_bytes
            if allow_cpu_only_fallback:
                layer_cpu_fallback_base_offset_bytes = next_cpu_fallback_offset
                next_cpu_fallback_offset += sum(page[3] for page in layer_pages)
            state.pending_layer_handles[layer_index] = self._shim.submit_normalized_paged(
                session_prefix=session_prefix,
                requested_pages=layer_pages,
                block_allocations=layer_blocks,
                allocation_id=allocation_id,
                device_ordinal=device_ordinal,
                stream_ordinal=stream_ordinal,
                allow_cpu_fallback=allow_cpu_fallback,
                cuda_enabled=cuda_enabled,
                cpu_fallback_host_ptr=cpu_fallback_host_ptr,
                cpu_fallback_base_offset_bytes=layer_cpu_fallback_base_offset_bytes,
                cpu_fallback_allocation_id=cpu_fallback_allocation_id,
                path_version=path_version,
            )
            state.pending_layer_device_ordinals[layer_index] = device_ordinal
        if not state.pending_layer_handles and not state.attached_layer_handles:
            state.last_report = _empty_restore_report()
        return request_id

    def start_load_kv(self, forward_context: Any | None = None, **kwargs: Any) -> None:
        if kwargs:
            metadata = dict(kwargs)
        elif isinstance(forward_context, Mapping) and (
            "session_prefix" in forward_context or "requests" in forward_context
        ):
            metadata = dict(forward_context)
        else:
            target_metadata = _maybe_get(
                forward_context,
                "fast_cache_connector_metadata",
                _MISSING,
            )
            if target_metadata is not _MISSING and target_metadata:
                metadata = dict(target_metadata)
            elif self.has_connector_metadata():
                metadata = dict(self._get_connector_metadata())
            else:
                metadata = {}
        if not metadata:
            raise ValueError("missing connector metadata for start_load_kv")
        if "requests" in metadata:
            active_request_ids: list[Any] = []
            for request_metadata in metadata.get("requests", ()) or ():
                request_id = self._start_request_load(request_metadata, forward_context)
                active_request_ids.append(request_id)
            self._active_step_request_ids = active_request_ids
            self._active_load_request_id = active_request_ids[0] if active_request_ids else None
            self._bind_load_state_views(self._active_load_request_id)
            return

        request_id = self._start_request_load(metadata, forward_context)
        self._active_step_request_ids = [request_id]
        self._active_load_request_id = request_id
        self._bind_load_state_views(request_id)

    def _active_request_ids(self, request_id: Any | None = None) -> list[Any]:
        if request_id is not None:
            resolved_request_id = self._resolve_active_request_id(request_id)
            return [resolved_request_id] if resolved_request_id in self._load_states else []
        request_ids = [
            active_request_id
            for active_request_id in self._active_step_request_ids
            if active_request_id in self._load_states
        ]
        if request_ids:
            return request_ids
        resolved_request_id = self._resolve_active_request_id()
        return [resolved_request_id] if resolved_request_id in self._load_states else []

    def _wait_for_request_layer_load(
        self, request_id: Any, layer_name: str | None = None
    ) -> Any:
        state = self._load_states.get(request_id)
        if state is None:
            return None
        layer_index = _extract_layer_index(layer_name)
        if (
            not state.pending_layer_handles
            and not state.attached_layer_handles
            and state.last_report is not None
        ):
            return state.last_report
        if layer_index is None:
            if not state.pending_layer_handles and not state.attached_layer_handles:
                return state.last_report
            pending_layers = sorted(
                set(state.pending_layer_handles)
                | set(state.attached_layer_handles)
                | set(state.layer_reports)
            )
            layer_index = pending_layers[0]
        if layer_index in state.attached_layer_handles:
            state.last_report = state.layer_reports.get(layer_index)
            return state.last_report
        handle = state.pending_layer_handles.get(layer_index)
        if handle is not None:
            stream_ptr = self._current_cuda_stream_ptr(
                state.pending_layer_device_ordinals.get(layer_index, self._device_ordinal)
            )
            if stream_ptr not in (None, 0):
                try:
                    if handle.wait_on_stream(int(stream_ptr)):
                        report = handle.peek_report()
                        state.pending_layer_handles.pop(layer_index, None)
                        state.attached_layer_handles[layer_index] = handle
                        return self._record_layer_report(request_id, layer_index, report)
                except Exception:
                    pass
            handle = state.pending_layer_handles.pop(layer_index, None)
        if handle is not None:
            report = handle.wait()
            state.pending_layer_device_ordinals.pop(layer_index, None)
            return self._record_layer_report(request_id, layer_index, report)
        if layer_index in state.layer_reports:
            state.last_report = state.layer_reports[layer_index]
        return state.last_report

    def wait_for_layer_load(self, layer_name: str | None = None) -> Any:
        request_ids = self._active_request_ids()
        if not request_ids:
            request_id = self._resolve_active_request_id()
            self._bind_load_state_views(request_id)
            return self._last_load_report
        last_report = None
        for request_id in request_ids:
            report = self._wait_for_request_layer_load(request_id, layer_name)
            if report is not None:
                last_report = report
        self._bind_load_state_views(request_ids[0])
        return last_report

    def _is_request_layer_load_ready(
        self, request_id: Any, layer_name: str | None = None
    ) -> bool:
        state = self._load_states.get(request_id)
        if state is None:
            return self._last_load_report is not None
        layer_index = _extract_layer_index(layer_name)
        if (
            not state.pending_layer_handles
            and not state.attached_layer_handles
            and state.last_report is not None
        ):
            return True
        if layer_index is None:
            if not state.pending_layer_handles and not state.attached_layer_handles:
                return state.last_report is not None
            return all(handle.is_ready() for handle in state.pending_layer_handles.values())
        if layer_index in state.attached_layer_handles:
            return True
        if layer_index in state.layer_reports:
            return True
        handle = state.pending_layer_handles.get(layer_index)
        if handle is None:
            return False
        return bool(handle.is_ready())

    def is_layer_load_ready(self, layer_name: str | None = None) -> bool:
        request_ids = self._active_request_ids()
        if not request_ids:
            return self._last_load_report is not None
        return all(
            self._is_request_layer_load_ready(request_id, layer_name)
            for request_id in request_ids
        )

    def _poll_request_layer_load(
        self, request_id: Any, layer_name: str | None = None
    ) -> Any | None:
        state = self._load_states.get(request_id)
        if state is None:
            return self._last_load_report
        layer_index = _extract_layer_index(layer_name)
        if (
            not state.pending_layer_handles
            and not state.attached_layer_handles
            and state.last_report is not None
        ):
            return state.last_report
        if layer_index is None:
            if not state.pending_layer_handles and not state.attached_layer_handles:
                return state.last_report
            for next_layer in sorted(state.pending_layer_handles):
                report = self._poll_request_layer_load(request_id, str(next_layer))
                if report is not None:
                    return report
            for next_layer in sorted(state.attached_layer_handles):
                report = self._poll_request_layer_load(request_id, str(next_layer))
                if report is not None:
                    return report
            return None
        if layer_index in state.attached_layer_handles:
            return state.layer_reports.get(layer_index)
        if layer_index in state.layer_reports:
            return state.layer_reports[layer_index]
        handle = state.pending_layer_handles.get(layer_index)
        if handle is None:
            return None
        report = handle.try_wait()
        if report is None:
            return None
        state.pending_layer_handles.pop(layer_index, None)
        state.pending_layer_device_ordinals.pop(layer_index, None)
        return self._record_layer_report(request_id, layer_index, report)

    def poll_layer_load(self, layer_name: str | None = None) -> Any | None:
        request_ids = self._active_request_ids()
        if not request_ids:
            return self._last_load_report
        last_report = None
        for request_id in request_ids:
            report = self._poll_request_layer_load(request_id, layer_name)
            if report is not None:
                last_report = report
        if request_ids:
            self._bind_load_state_views(request_ids[0])
        return last_report

    @classmethod
    def requires_piecewise_for_cudagraph(
        cls, extra_config: Mapping[str, Any] | None = None
    ) -> bool:
        _ = cls
        if extra_config is not None:
            configured = _field(
                extra_config,
                "requires_piecewise_for_cudagraph",
                "fast_cache_requires_piecewise_for_cudagraph",
                default=_MISSING,
            )
            if configured is not _MISSING:
                return bool(configured)
        return _env_flag("FAST_CACHE_VLLM_REQUIRE_PIECEWISE_CUDAGRAPH", True)

    def wait_for_save(self) -> None:
        for save_state in self._save_states.values():
            self._flush_request_save_state(save_state)
        return None

    def save_kv_layer(
        self,
        layer_name: str | None = None,
        kv_layer: Any | None = None,
        attn_metadata: Any | None = None,
        **kwargs: Any,
    ) -> int:
        ttl = kwargs.get("ttl")
        session_prefix = _first_layer_value(
            layer_name,
            kwargs,
            attn_metadata,
            names=("session_prefix",),
        )
        record_source = _first_layer_value(
            layer_name,
            kwargs,
            attn_metadata,
            names=("records", "save_records", "lmcache_records"),
        )
        if record_source is None:
            record_source = _select_layer_value(kv_layer, layer_name)
        normalized = _normalize_save_records_from(record_source)
        if normalized is not None:
            if ttl not in (None, 0) or session_prefix is None:
                self._store.batch_set(normalized, ttl=None if ttl in (None, 0) else ttl)
            elif hasattr(self._store, "batch_set_session_packed_no_ttl"):
                self._store.batch_set_session_packed_no_ttl(
                    _coerce_bytes(session_prefix, "session_prefix"),
                    normalized,
                )
            else:
                self._store.batch_set_session_no_ttl(
                    _coerce_bytes(session_prefix, "session_prefix"),
                    normalized,
                )
            return len(normalized)

        save_requests = []
        if self.has_connector_metadata():
            save_requests = list(_maybe_get(self._get_connector_metadata(), "requests", ()) or ())
        if save_requests:
            layer_index = _extract_layer_index(layer_name)
            if layer_index is None:
                raise ValueError("save_kv_layer requires a parseable layer_name")
            total_records = 0
            for request in save_requests:
                request_id = _maybe_get(request, "request_id")
                request_session_prefix = _coerce_bytes(
                    _maybe_get(
                        request,
                        "session_prefix",
                        session_prefix if session_prefix is not None else self._cache_session_prefix,
                    ),
                    "session_prefix",
                )
                save_block_hashes = [
                    _coerce_bytes(block_hash, "block_hash")
                    for block_hash in (_maybe_get(request, "save_block_hashes", ()) or ())
                ]
                save_block_ids = [
                    _coerce_int(block_id, "block_id")
                    for block_id in (_maybe_get(request, "save_block_ids", ()) or ())
                ]
                if not save_block_hashes or not save_block_ids:
                    continue
                target_saved_block_count = _coerce_int(
                    _maybe_get(request, "save_target_block_count", 0),
                    "save_target_block_count",
                )
                target_count = min(
                    len(save_block_hashes),
                    len(save_block_ids),
                    target_saved_block_count if target_saved_block_count > 0 else len(save_block_hashes),
                )
                if target_count <= 0:
                    continue
                save_state = (
                    self._save_states.setdefault(
                        request_id, _RequestSaveState(request_id=request_id)
                    )
                    if request_id is not None
                    else None
                )
                effective_saved_block_count = (
                    save_state.saved_block_count
                    if save_state is not None
                    else 0
                )
                if target_saved_block_count <= effective_saved_block_count:
                    _debug_log(
                        f"skip-save request={request_id} target_blocks={target_saved_block_count} "
                        f"saved_blocks={effective_saved_block_count} "
                        f"session_prefix={request_session_prefix!r}"
                    )
                    continue
                target_block_hashes = list(save_block_hashes[:target_count])
                target_block_ids = list(save_block_ids[:target_count])
                if (
                    save_state is None
                    and ttl in (None, 0)
                    and hasattr(self._store, "batch_set_vllm_pages_from_layer_no_ttl")
                ):
                    total_records += _coerce_int(
                        self._store.batch_set_vllm_pages_from_layer_no_ttl(
                            request_session_prefix,
                            int(layer_index),
                            target_block_hashes,
                            target_block_ids,
                            kv_layer,
                        ),
                        "saved VLLM page count",
                    )
                    continue
                if hasattr(self._store, "extract_vllm_layer_payload_bytes"):
                    layer_payloads = [
                        _coerce_bytes(payload, "payload")
                        for payload in self._store.extract_vllm_layer_payload_bytes(
                            kv_layer,
                            target_block_ids,
                        )
                    ]
                    total_records += len(layer_payloads)
                else:
                    layer_payloads = []
                    for _block_hash, block_id in zip(
                        target_block_hashes,
                        target_block_ids,
                        strict=False,
                    ):
                        page = _extract_layer_page(kv_layer, block_id, attn_metadata)
                        layer_payloads.append(_page_payload_bytes(page))
                        total_records += 1
                if save_state is not None:
                    self._buffer_request_layer_save(
                        save_state,
                        layer_index,
                        request_session_prefix,
                        target_block_hashes,
                        target_block_ids,
                        layer_payloads,
                        target_saved_block_count,
                        ttl=ttl,
                    )
                else:
                    if ttl not in (None, 0):
                        layer_records = [
                            (
                                _encode_vllm_page_key(block_hash, layer_index),
                                payload,
                            )
                            for block_hash, payload in zip(
                                target_block_hashes,
                                layer_payloads,
                                strict=False,
                            )
                        ]
                        self._store.batch_set(layer_records, ttl=ttl)
                    elif hasattr(self._store, "batch_set_vllm_pages_no_ttl"):
                        self._store.batch_set_vllm_pages_no_ttl(
                            request_session_prefix,
                            int(layer_index),
                            target_block_hashes,
                            list(layer_payloads),
                        )
                    elif hasattr(self._store, "batch_set_session_packed_no_ttl"):
                        layer_records = [
                            (
                                _encode_vllm_page_key(block_hash, layer_index),
                                payload,
                            )
                            for block_hash, payload in zip(
                                target_block_hashes,
                                layer_payloads,
                                strict=False,
                            )
                        ]
                        self._store.batch_set_session_packed_no_ttl(
                            request_session_prefix,
                            layer_records,
                        )
                    else:
                        layer_records = [
                            (
                                _encode_vllm_page_key(block_hash, layer_index),
                                payload,
                            )
                            for block_hash, payload in zip(
                                target_block_hashes,
                                layer_payloads,
                                strict=False,
                            )
                        ]
                        self._store.batch_set_session_no_ttl(
                            request_session_prefix,
                            layer_records,
                        )
                        _debug_log(
                            f"published session_prefix={request_session_prefix!r} layers=1 "
                            f"records={len(layer_records)} store_id={id(self._store)} "
                            f"save_hashes={[block_hash.hex() for block_hash in target_block_hashes[:2]]}"
                        )
            return total_records

        prepared = _first_layer_value(
            layer_name,
            kwargs,
            attn_metadata,
            names=("prepared_put_batch", "prepared_batch", "prepared"),
        )
        key_source = _first_layer_value(
            layer_name,
            kwargs,
            attn_metadata,
            names=("encoded_keys", "cache_keys", "keys", "engine_keys"),
        )
        metadata_blob_source = _first_layer_value(
            layer_name,
            kwargs,
            attn_metadata,
            names=("metadata_blobs", "encoded_metadata", "metadata_bytes"),
        )

        payload_source = _first_layer_value(
            layer_name,
            kwargs,
            attn_metadata,
            names=("payloads", "payload", "payload_bytes", "byte_arrays"),
        )
        if payload_source is None:
            payload_source = _select_layer_value(kv_layer, layer_name)
        payloads = _coerce_items(payload_source) if payload_source is not None else None

        memory_obj_source = _first_layer_value(
            layer_name,
            kwargs,
            attn_metadata,
            names=("memory_objs", "objs", "lmcache_memory_objs"),
        )
        if memory_obj_source is None and kv_layer is not None:
            kv_items = _coerce_items(_select_layer_value(kv_layer, layer_name))
            if kv_items and all(
                hasattr(item, "metadata") and hasattr(item, "byte_array")
                for item in kv_items
            ):
                memory_obj_source = kv_items
        memory_objs = (
            _coerce_items(memory_obj_source) if memory_obj_source is not None else None
        )

        encoded_keys = _maybe_bytes_list(key_source, "encoded key")
        raw_keys = _coerce_items(key_source) if key_source is not None else None
        metadata_blobs = _maybe_bytes_list(metadata_blob_source, "metadata blob")

        if prepared is None and encoded_keys is not None and metadata_blobs is not None:
            prepared = self._prepare_lmcache_put_batch(encoded_keys, metadata_blobs)

        if prepared is not None and memory_objs:
            if hasattr(self._store, "batch_put_lmcache_memory_objs_prepared_bytes"):
                self._store.batch_put_lmcache_memory_objs_prepared_bytes(prepared, memory_objs)
                return len(memory_objs)

        if prepared is not None and payloads:
            payload_bytes = _maybe_bytes_list(payloads, "payload bytes")
            if payload_bytes is not None and hasattr(
                self._store, "batch_put_lmcache_payload_bytes_prepared"
            ):
                self._store.batch_put_lmcache_payload_bytes_prepared(prepared, payload_bytes)
                return len(payload_bytes)
            if hasattr(self._store, "batch_put_lmcache_payloads_prepared"):
                self._store.batch_put_lmcache_payloads_prepared(prepared, payloads)
                return len(payloads)

        if encoded_keys is not None and memory_objs:
            if hasattr(self._store, "batch_put_lmcache_memory_objs_encoded_keys"):
                self._store.batch_put_lmcache_memory_objs_encoded_keys(
                    encoded_keys,
                    memory_objs,
                )
                return len(memory_objs)

        if raw_keys is not None and memory_objs:
            if hasattr(self._store, "batch_put_lmcache_memory_objs_from_engine_keys"):
                self._store.batch_put_lmcache_memory_objs_from_engine_keys(
                    raw_keys,
                    memory_objs,
                )
                return len(memory_objs)

        if encoded_keys is not None and metadata_blobs is not None and payloads:
            payload_bytes = _maybe_bytes_list(payloads, "payload bytes")
            if payload_bytes is not None and hasattr(
                self._store, "batch_put_lmcache_payload_bytes_and_metadata_encoded_keys"
            ):
                self._store.batch_put_lmcache_payload_bytes_and_metadata_encoded_keys(
                    encoded_keys,
                    payload_bytes,
                    metadata_blobs,
                )
                return len(payload_bytes)
            if hasattr(self._store, "batch_put_lmcache_payloads_and_metadata_encoded_keys"):
                self._store.batch_put_lmcache_payloads_and_metadata_encoded_keys(
                    encoded_keys,
                    payloads,
                    metadata_blobs,
                )
                return len(payloads)

        raise ValueError(
            "save_kv_layer requires explicit records or LMCache-style keys/payload metadata"
        )

    def wait_for_load(self, request: Any | None = None) -> Any:
        request_id = _request_id(request) if request is not None else None
        request_ids = self._active_request_ids(request_id)
        if not request_ids:
            self._bind_load_state_views(request_id)
            return self._last_load_report
        last_report = None
        for request_id in request_ids:
            state = self._load_states.get(request_id)
            if state is None:
                continue
            while state.pending_layer_handles:
                next_layer = min(state.pending_layer_handles)
                last_report = self._wait_for_request_layer_load(
                    request_id, str(next_layer)
                )
            while state.attached_layer_handles:
                next_layer = min(state.attached_layer_handles)
                handle = state.attached_layer_handles.pop(next_layer)
                report = handle.wait()
                state.pending_layer_device_ordinals.pop(next_layer, None)
                last_report = self._record_layer_report(request_id, next_layer, report)
            if state.last_report is not None:
                last_report = state.last_report
        self._bind_load_state_views(request_ids[0])
        return last_report

    def get_finished(
        self,
        finished_req_ids: Iterable[Any] | None = None,
        *_args: Any,
        **_kwargs: Any,
    ) -> tuple[set[Any] | None, set[Any] | None]:
        if finished_req_ids is not None:
            for request_id in finished_req_ids:
                self._mark_request_finished(request_id)
        self._finished_request_ids.clear()
        return (None, None)

    def get_block_ids_with_load_errors(
        self,
        request: Any | None = None,
        *_args: Any,
        **_kwargs: Any,
    ) -> list[int]:
        if request is not None:
            request_id = _request_id(request)
            return list(self._request_load_error_block_ids.pop(request_id, ()))
        block_ids: list[int] = []
        seen: set[int] = set()
        for request_id in list(self._request_load_error_block_ids):
            for block_id in self._request_load_error_block_ids.pop(request_id, ()):
                normalized = _coerce_int(block_id, "block_id")
                if normalized in seen:
                    continue
                seen.add(normalized)
                block_ids.append(normalized)
        return block_ids

    def shutdown(self) -> None:
        for state in list(self._load_states.values()):
            for handle in list(state.pending_layer_handles.values()):
                handle.cancel()
            for handle in list(state.attached_layer_handles.values()):
                handle.cancel()
        for save_state in list(self._save_states.values()):
            self._flush_request_save_state(save_state)
        self._load_states.clear()
        self._save_states.clear()
        self._request_metadata.clear()
        self._unfinished_requests.clear()
        self._request_load_error_block_ids.clear()
        self._finished_request_ids.clear()
        self._active_step_request_ids = []
        self._active_load_request_id = None
        self.clear_connector_metadata()
        self._bind_load_state_views()

    def request_finished(
        self,
        request: Any,
        block_ids: Any | None = None,
    ) -> tuple[bool, dict[str, Any] | None]:
        _ = block_ids
        request_id = _request_id(request)
        state = self._load_states.pop(request_id, None)
        if state is not None:
            for handle in list(state.pending_layer_handles.values()):
                handle.cancel()
            for handle in list(state.attached_layer_handles.values()):
                handle.cancel()
        self._active_step_request_ids = [
            active_request_id
            for active_request_id in self._active_step_request_ids
            if active_request_id != request_id
        ]
        if self._active_load_request_id == request_id:
            self._active_load_request_id = None
            if self._active_step_request_ids:
                self._active_load_request_id = self._active_step_request_ids[0]
            elif len(self._load_states) == 1:
                self._active_load_request_id = next(iter(self._load_states))
        self._bind_load_state_views()
        save_state = self._save_states.pop(request_id, None)
        if save_state is not None:
            self._flush_request_save_state(save_state)
        self._unfinished_requests.pop(request_id, None)
        self._request_metadata.pop(request_id, None)
        self._mark_request_finished(request_id)
        return (False, None)


class ShardCacheKVConnectorV1(FastCacheKVConnectorV1):
    pass
