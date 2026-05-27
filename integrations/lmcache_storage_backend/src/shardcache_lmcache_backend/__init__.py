"""LMCache storage plugin backed by shardcache."""

from .backend import ShardCacheStorageBackend

FastCacheStorageBackend = ShardCacheStorageBackend

__all__ = ["ShardCacheStorageBackend", "FastCacheStorageBackend"]
