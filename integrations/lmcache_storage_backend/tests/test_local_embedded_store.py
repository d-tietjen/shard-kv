from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "shardcache_local_embedded_store.py"
)


def load_module_with_fake_store():
    fake = types.ModuleType("shardcache")

    class Store:
        creations = []

        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.inner = object()
            self.namespace = kwargs.get("service_namespace")
            self.resident_service = kwargs.get("resident_service")
            Store.creations.append(self)

        def with_service_namespace(self, namespace="shardmap", resident_service=None):
            if resident_service is not None and resident_service != self.resident_service:
                raise ValueError("eviction policy is engine-wide")
            view = Store.__new__(Store)
            view.kwargs = self.kwargs
            view.inner = self.inner
            view.namespace = namespace
            view.resident_service = self.resident_service
            return view

    fake.Store = Store
    sys.modules["shardcache"] = fake

    module_name = "test_shardcache_local_embedded_store"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module, Store


class LocalEmbeddedStoreRegistryTests(unittest.TestCase):
    def test_resident_namespaces_share_resident_engine(self):
        module, store_cls = load_module_with_fake_store()

        first = module.create_shardcache_store(
            cores=1,
            wal_path=None,
            route_mode="full_key",
            deployment_id="shared",
            service_namespace="resident-a",
            max_memory_bytes=1024,
            eviction_policy="lru",
            resident_service=True,
        )
        second = module.create_shardcache_store(
            cores=1,
            wal_path=None,
            route_mode="full_key",
            deployment_id="shared",
            service_namespace="resident-b",
            max_memory_bytes=2048,
            eviction_policy="lfu",
            resident_service=True,
        )

        self.assertIs(first._store.inner, second._store.inner)
        self.assertEqual(first._store.namespace, "resident-a")
        self.assertEqual(second._store.namespace, "resident-b")
        self.assertEqual(len(store_cls.creations), 1)
        self.assertIsNone(store_cls.creations[0].kwargs["max_memory_bytes"])
        self.assertEqual(store_cls.creations[0].kwargs["eviction_policy"], "none")

    def test_lru_deployment_does_not_share_persistent_resident_engine(self):
        module, store_cls = load_module_with_fake_store()

        resident = module.create_shardcache_store(
            cores=1,
            wal_path=None,
            route_mode="full_key",
            deployment_id="shared",
            service_namespace="resident",
            max_memory_bytes=1024,
            eviction_policy="lru",
            resident_service=True,
        )
        cache = module.create_shardcache_store(
            cores=1,
            wal_path=None,
            route_mode="full_key",
            deployment_id="shared",
            service_namespace="cache",
            max_memory_bytes=1024,
            eviction_policy="lru",
            resident_service=False,
        )

        self.assertIsNot(resident._store.inner, cache._store.inner)
        self.assertEqual(len(store_cls.creations), 2)
        self.assertIsNone(store_cls.creations[0].kwargs["max_memory_bytes"])
        self.assertEqual(store_cls.creations[0].kwargs["eviction_policy"], "none")
        self.assertEqual(store_cls.creations[1].kwargs["max_memory_bytes"], 1024)
        self.assertEqual(store_cls.creations[1].kwargs["eviction_policy"], "lru")

    def test_cache_deployments_only_share_identical_eviction_policy(self):
        module, store_cls = load_module_with_fake_store()

        first = module.create_shardcache_store(
            cores=1,
            wal_path=None,
            route_mode="full_key",
            deployment_id="shared-cache",
            service_namespace="cache-a",
            max_memory_bytes=1024,
            eviction_policy="lru",
            resident_service=False,
        )
        second = module.create_shardcache_store(
            cores=1,
            wal_path=None,
            route_mode="full_key",
            deployment_id="shared-cache",
            service_namespace="cache-b",
            max_memory_bytes=1024,
            eviction_policy="lru",
            resident_service=False,
        )
        different_budget = module.create_shardcache_store(
            cores=1,
            wal_path=None,
            route_mode="full_key",
            deployment_id="shared-cache",
            service_namespace="cache-c",
            max_memory_bytes=2048,
            eviction_policy="lru",
            resident_service=False,
        )

        self.assertIs(first._store.inner, second._store.inner)
        self.assertIsNot(first._store.inner, different_budget._store.inner)
        self.assertEqual(len(store_cls.creations), 2)


if __name__ == "__main__":
    unittest.main()
