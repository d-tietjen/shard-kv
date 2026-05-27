from __future__ import annotations

from typing import Any, Optional

import shardcache


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
    )


def _normalize_client_architecture(value: str) -> str:
    match value.strip().lower():
        case "embedded" | "in_process" | "local":
            return "local_embedded"
        case "tcp" | "remote":
            return "scnp_tcp"
        case other:
            return other
