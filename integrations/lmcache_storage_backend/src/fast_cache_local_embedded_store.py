from __future__ import annotations

from typing import Any, Optional

import fast_cache


def create_fast_cache_store(
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
    fcnp_addr: str = "127.0.0.1:6500",
) -> Any:
    client_architecture = _normalize_client_architecture(client_architecture)
    if client_architecture in {"fcnp_tcp_python", "tcp_python"}:
        from fast_cache_fcnp_store import create_fast_cache_store as create_fcnp_store

        return create_fcnp_store(addr=fcnp_addr)
    if client_architecture in {"fcnp_tcp", "tcp"}:
        if hasattr(fast_cache, "FcnpStore"):
            return fast_cache.FcnpStore(fcnp_addr)
        from fast_cache_fcnp_store import create_fast_cache_store as create_fcnp_store

        return create_fcnp_store(addr=fcnp_addr)

    return fast_cache.Store(
        cores=cores,
        wal_path=wal_path,
        compress_wal=compress_wal,
        max_memory_bytes=max_memory_bytes,
        eviction_policy=eviction_policy,
        route_mode=route_mode,
        enable_metrics=enable_metrics,
        client_architecture=client_architecture,
        prefer_session_tags=prefer_session_tags,
    )


def _normalize_client_architecture(value: str) -> str:
    match value.strip().lower():
        case "embedded" | "in_process" | "local":
            return "local_embedded"
        case "tcp" | "remote":
            return "fcnp_tcp"
        case other:
            return other
