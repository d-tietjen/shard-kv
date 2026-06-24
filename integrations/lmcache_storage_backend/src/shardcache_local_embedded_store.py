from __future__ import annotations

import threading
from typing import Any, Optional

import shardcache

_STORE_REGISTRY: dict[tuple[Any, ...], Any] = {}
_STORE_REGISTRY_LOCK = threading.RLock()


class _SharedStoreHandle:
    """Process-local handle for a resident embedded shardcache deployment."""

    def __init__(self, store: Any) -> None:
        self._store = store

    def __getattr__(self, name: str) -> Any:
        return getattr(self._store, name)

    def close(self) -> None:
        # A shared embedded deployment has DashMap-like process lifetime. Keep
        # the resident map alive when one LMCache backend wrapper is closed.
        return None

    def __repr__(self) -> str:
        return repr(self._store)


def create_shardcache_store(
    *,
    cores: int,
    wal_path: Optional[str],
    compress_wal: bool = True,
    max_memory_bytes: Optional[int] = None,
    eviction_policy: str = "none",
    route_mode: str,
    enable_metrics: bool = False,
    client_architecture: str = "local_embedded",
    prefer_session_tags: bool = False,
    scnp_addr: str = "127.0.0.1:6500",
    numa_policy: str = "off",
    deployment_id: Optional[str] = None,
    service_namespace: str = "shardmap",
    resident_service: bool = True,
) -> Any:
    client_architecture = _normalize_client_architecture(client_architecture)
    if client_architecture in {"scnp_tcp_python", "tcp_python"}:
        from shardcache_scnp_store import create_shardcache_store as create_scnp_store

        return create_scnp_store(addr=scnp_addr)
    if client_architecture in {"scnp_tcp", "tcp"}:
        if hasattr(shardcache, "ScnpStore"):
            return shardcache.ScnpStore(scnp_addr)
        from shardcache_scnp_store import create_shardcache_store as create_scnp_store

        return create_scnp_store(addr=scnp_addr)

    if not deployment_id:
        return _new_embedded_store(
            cores=cores,
            wal_path=wal_path,
            compress_wal=compress_wal,
            max_memory_bytes=max_memory_bytes,
            eviction_policy=eviction_policy,
            route_mode=route_mode,
            enable_metrics=enable_metrics,
            client_architecture=client_architecture,
            prefer_session_tags=prefer_session_tags,
            numa_policy=numa_policy,
            service_namespace=service_namespace,
            resident_service=resident_service,
        )

    policy_key, effective_max_memory_bytes, effective_eviction_policy = _deployment_policy_key(
        resident_service=resident_service,
        max_memory_bytes=max_memory_bytes,
        eviction_policy=eviction_policy,
    )
    registry_key = (
        deployment_id,
        policy_key,
        cores,
        wal_path,
        compress_wal,
        effective_max_memory_bytes,
        effective_eviction_policy,
        route_mode,
        enable_metrics,
        client_architecture,
        prefer_session_tags,
        numa_policy,
        resident_service,
    )
    with _STORE_REGISTRY_LOCK:
        store = _STORE_REGISTRY.get(registry_key)
        if store is None:
            store = _new_embedded_store(
                cores=cores,
                wal_path=wal_path,
                compress_wal=compress_wal,
                max_memory_bytes=effective_max_memory_bytes,
                eviction_policy=effective_eviction_policy,
                route_mode=route_mode,
                enable_metrics=enable_metrics,
                client_architecture=client_architecture,
                prefer_session_tags=prefer_session_tags,
                numa_policy=numa_policy,
                service_namespace="",
                resident_service=resident_service,
            )
            _STORE_REGISTRY[registry_key] = store
        return _SharedStoreHandle(
            _store_namespace_view(
                store,
                service_namespace,
                resident_service=resident_service,
            )
        )


def _new_embedded_store(
    *,
    cores: int,
    wal_path: Optional[str],
    compress_wal: bool,
    max_memory_bytes: Optional[int],
    eviction_policy: str,
    route_mode: str,
    enable_metrics: bool,
    client_architecture: str,
    prefer_session_tags: bool,
    numa_policy: str,
    service_namespace: str,
    resident_service: bool,
) -> Any:
    return shardcache.Store(
        cores=cores,
        wal_path=wal_path,
        compress_wal=compress_wal,
        max_memory_bytes=max_memory_bytes,
        eviction_policy=eviction_policy,
        route_mode=route_mode,
        enable_metrics=enable_metrics,
        client_architecture=client_architecture,
        prefer_session_tags=prefer_session_tags,
        numa_policy=numa_policy,
        service_namespace=service_namespace,
        resident_service=resident_service,
    )


def _store_namespace_view(
    store: Any,
    service_namespace: str,
    *,
    resident_service: bool,
) -> Any:
    if hasattr(store, "with_service_namespace"):
        return store.with_service_namespace(
            service_namespace,
            resident_service=resident_service,
        )
    return store


def _deployment_policy_key(
    *,
    resident_service: bool,
    max_memory_bytes: Optional[int],
    eviction_policy: str,
) -> tuple[str, Optional[int], str]:
    # Memory-pressure eviction is configured on the backing engine, not on a
    # namespace view. Keep resident and LRU/LFU cache-style services in
    # different engine pools even when they use the same deployment_id.
    if resident_service:
        return ("resident", None, "none")
    return ("cache", max_memory_bytes, eviction_policy)


def _normalize_client_architecture(value: str) -> str:
    match value.strip().lower():
        case "embedded" | "in_process" | "local":
            return "local_embedded"
        case "tcp" | "remote":
            return "scnp_tcp"
        case other:
            return other
