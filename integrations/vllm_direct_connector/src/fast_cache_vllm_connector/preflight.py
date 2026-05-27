from __future__ import annotations

from typing import Any

from .connector import SUPPORTED_VLLM_VERSION, assert_supported_vllm_version
from .kv_connector_v1 import ShardCacheKVConnectorV1


def run_preflight(
    expected_version: str = SUPPORTED_VLLM_VERSION,
    *,
    instantiate_connector: bool = True,
) -> dict[str, Any]:
    try:
        import shardcache as store_module  # type: ignore
    except ImportError:
        import fast_cache as store_module  # type: ignore
    import vllm  # type: ignore

    resolved_version = getattr(vllm, "__version__", None)
    assert_supported_vllm_version(resolved_version, expected=expected_version)

    summary: dict[str, Any] = {
        "status": "ok",
        "vllm_version": resolved_version,
        "expected_vllm_version": expected_version,
        "shardcache_module": getattr(store_module, "__name__", "shardcache"),
        "connector_class": f"{ShardCacheKVConnectorV1.__module__}.{ShardCacheKVConnectorV1.__name__}",
    }

    if instantiate_connector:
        connector = ShardCacheKVConnectorV1(
            validate_version=False,
            installed_vllm_version=resolved_version,
        )
        summary["connector_ready"] = True
        summary["connector_type"] = type(connector).__name__

    return summary
