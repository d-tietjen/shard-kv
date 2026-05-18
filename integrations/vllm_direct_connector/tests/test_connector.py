import unittest

from fast_cache_vllm_connector import (
    FastCacheVllmConnectorShim,
    GPU_DIRECT_API_V0_PATH_VERSION,
    HOST_DIRECT_V1_PATH_VERSION,
    VersionMismatchError,
    assert_supported_vllm_version,
)


class _Page:
    def __init__(self, key, layer_index, page_index, len_bytes):
        self.key = key
        self.layer_index = layer_index
        self.page_index = page_index
        self.len_bytes = len_bytes


class _Block:
    def __init__(self, block_index, dst_device_ptr, block_size_bytes):
        self.block_index = block_index
        self.dst_device_ptr = dst_device_ptr
        self.block_size_bytes = block_size_bytes


class _FakeStore:
    def __init__(self):
        self.calls = []

    def restore_vllm_paged(self, **kwargs):
        self.calls.append(kwargs)
        return {"backend": "cpu", "all_hit": True}

    def submit_vllm_paged_restore(self, **kwargs):
        self.calls.append(kwargs)
        return {"backend": "cpu", "all_hit": True}


class ConnectorShimTests(unittest.TestCase):
    def test_version_guard_rejects_unpinned_version(self):
        with self.assertRaises(VersionMismatchError):
            assert_supported_vllm_version("0.18.0")

    def test_translate_load_spec_accepts_attribute_objects(self):
        shim = FastCacheVllmConnectorShim(_FakeStore())
        spec = shim.translate_load_spec(
            session_prefix="s:1",
            requested_pages=[_Page(b"k0", 3, 7, 4096)],
            block_allocations=[_Block(11, 0x2000, 8192)],
            allocation_id=9,
            device_ordinal=2,
            stream_ordinal=5,
            allow_cpu_fallback=False,
        )

        self.assertEqual(spec["session_prefix"], b"s:1")
        self.assertEqual(spec["requested_pages"], [(b"k0", 3, 7, 4096)])
        self.assertEqual(spec["block_allocations"], [(11, 0x2000, 8192)])
        self.assertEqual(spec["allocation_id"], 9)
        self.assertEqual(spec["device_ordinal"], 2)
        self.assertEqual(spec["stream_ordinal"], 5)
        self.assertFalse(spec["allow_cpu_fallback"])

    def test_translate_load_spec_accepts_mapping_synonyms(self):
        shim = FastCacheVllmConnectorShim(_FakeStore())
        spec = shim.translate_load_spec(
            session_prefix=b"s:2",
            requested_pages=[
                {
                    "cache_key": memoryview(b"k1"),
                    "layer_idx": 1,
                    "page_idx": 4,
                    "expected_len_bytes": 2048,
                }
            ],
            block_allocations=[
                {
                    "page_index": 4,
                    "device_ptr": 0x4000,
                    "page_size_bytes": 4096,
                }
            ],
        )

        self.assertEqual(spec["requested_pages"], [(b"k1", 1, 4, 2048)])
        self.assertEqual(spec["block_allocations"], [(4, 0x4000, 4096)])

    def test_restore_paged_forwards_normalized_inputs_to_store(self):
        store = _FakeStore()
        shim = FastCacheVllmConnectorShim(store)

        result = shim.restore_paged(
            session_prefix="s:3",
            requested_pages=[_Page("k2", 2, 8, 1024)],
            block_allocations=[_Block(8, 0x8000, 2048)],
            cpu_fallback_host_ptr=0x9000,
            cpu_fallback_base_offset_bytes=64,
            cpu_fallback_allocation_id=12,
        )

        self.assertEqual(result["backend"], "cpu")
        self.assertEqual(len(store.calls), 1)
        self.assertEqual(store.calls[0]["session_prefix"], b"s:3")
        self.assertEqual(store.calls[0]["requested_pages"], [(b"k2", 2, 8, 1024)])
        self.assertEqual(store.calls[0]["block_allocations"], [(8, 0x8000, 2048)])
        self.assertEqual(store.calls[0]["cpu_fallback_host_ptr"], 0x9000)
        self.assertEqual(store.calls[0]["cpu_fallback_base_offset_bytes"], 64)
        self.assertEqual(store.calls[0]["cpu_fallback_allocation_id"], 12)
        self.assertEqual(store.calls[0]["path_version"], HOST_DIRECT_V1_PATH_VERSION)

    def test_submit_paged_forwards_selected_path_version(self):
        store = _FakeStore()
        shim = FastCacheVllmConnectorShim(
            store,
            path_version=GPU_DIRECT_API_V0_PATH_VERSION,
        )

        shim.submit_paged(
            session_prefix="s:submit",
            requested_pages=[_Page("k3", 0, 1, 512)],
            block_allocations=[_Block(1, 0xA000, 1024)],
        )

        self.assertEqual(len(store.calls), 1)
        self.assertEqual(
            store.calls[0]["path_version"],
            GPU_DIRECT_API_V0_PATH_VERSION,
        )


if __name__ == "__main__":
    unittest.main()
