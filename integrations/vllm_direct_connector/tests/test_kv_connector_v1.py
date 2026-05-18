import os
import sys
import types
import unittest
from unittest import mock

import fast_cache_vllm_connector.kv_connector_v1 as kv_connector_v1_module
from fast_cache_vllm_connector import FastCacheKVConnectorV1
from fast_cache_vllm_connector.preflight import run_preflight


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


class _SaveRecord:
    def __init__(self, key, value):
        self.key = key
        self.value = value


class _SaveMetadata:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class _MemoryObj:
    def __init__(self, byte_array, metadata=None):
        self.byte_array = byte_array
        self.metadata = metadata if metadata is not None else object()


class _BoundTarget:
    pass


class _Request:
    def __init__(self, request_id):
        self.request_id = request_id


class _FakeTensorPage:
    def __init__(self, payload):
        self._payload = bytes(payload)

    def detach(self):
        return self

    def contiguous(self):
        return self

    def cpu(self):
        return self

    def tobytes(self):
        return self._payload

    def copy_from_bytes(self, payload):
        self._payload = bytes(payload)


class _FakePagedTensor:
    def __init__(self, pages):
        self._pages = {
            int(key): (
                value
                if isinstance(value, _FakeTensorPage)
                else _FakeTensorPage(value)
            )
            for key, value in pages.items()
        }
        self.shape = (len(self._pages),)

    def __getitem__(self, index):
        if isinstance(index, tuple):
            head = index[0]
            if isinstance(head, slice):
                block_id = index[1]
            else:
                block_id = head
        else:
            block_id = index
        return self._pages[int(block_id)]

    def page_bytes(self, block_id):
        return self._pages[int(block_id)].tobytes()


class _FakeSessionReadBatch:
    def __init__(self, payloads):
        self._payloads = list(payloads)

    def all_hit(self):
        return all(payload is not None for payload in self._payloads)

    def memoryview_at(self, index):
        payload = self._payloads[index]
        if payload is None:
            return None
        return memoryview(payload)


class _TritonAttentionMetadata:
    pass


class _SchedulerOutput:
    def __init__(self, request, session_prefix, requested_pages, **kwargs):
        self.request = request
        self.session_prefix = session_prefix
        self.requested_pages = requested_pages
        for key, value in kwargs.items():
            setattr(self, key, value)


class _KVCacheBlocks:
    def __init__(self, *groups):
        self._groups = tuple(list(group) for group in groups)

    def get_block_ids(self, allow_none=False):
        if allow_none and all(len(group) == 0 for group in self._groups):
            return None
        return tuple(list(group) for group in self._groups)


class _FakeStore:
    def __init__(self):
        self.restore_calls = []
        self.submitted_calls = []
        self.next_restore_reports = []
        self.session_writes = []
        self.generic_writes = []
        self.prepared_put_calls = []
        self.payload_bytes_prepared_calls = []
        self.payload_prepared_calls = []
        self.memory_obj_prepared_calls = []
        self.payload_bytes_encoded_calls = []
        self.payload_encoded_calls = []
        self.memory_obj_encoded_calls = []
        self.memory_obj_engine_key_calls = []
        self.cached_session_pages = {}
        self.vllm_prefix_probe_calls = []
        self.vllm_page_view_calls = []
        self.vllm_page_write_calls = []
        self.vllm_layer_write_calls = []
        self.vllm_layer_extract_calls = []
        self.vllm_layer_direct_write_calls = []

    def restore_vllm_paged(self, **kwargs):
        self.restore_calls.append(kwargs)
        return {
            "backend": "cpu",
            "page_count": len(kwargs["requested_pages"]),
            "all_hit": True,
        }

    def submit_vllm_paged_restore(self, **kwargs):
        self.submitted_calls.append(kwargs)
        report = (
            self.next_restore_reports.pop(0)
            if self.next_restore_reports
            else {
                "backend": "cpu",
                "page_count": len(kwargs["requested_pages"]),
                "all_hit": True,
            }
        )
        return _FakeRestoreHandle(
            report
        )

    def batch_set_session_no_ttl(self, session_prefix, records):
        self.session_writes.append((session_prefix, records))
        session_prefix = bytes(session_prefix)
        stale_keys = [
            key
            for key in self.cached_session_pages
            if key[0] == session_prefix
        ]
        for stale_key in stale_keys:
            self.cached_session_pages.pop(stale_key, None)
        for key, value in records:
            self.cached_session_pages[(session_prefix, bytes(key))] = bytes(value)

    def batch_set_session_packed_no_ttl(self, session_prefix, records):
        self.batch_set_session_no_ttl(session_prefix, records)

    def batch_set_vllm_pages_no_ttl(
        self, session_prefix, layer_index, block_hashes, payloads
    ):
        self.vllm_page_write_calls.append(
            (bytes(session_prefix), int(layer_index), list(block_hashes), list(payloads))
        )
        records = [
            (
                f"vllm-page:{int(layer_index)}:".encode("utf-8")
                + bytes(block_hash).hex().encode("ascii"),
                bytes(payload),
            )
            for block_hash, payload in zip(block_hashes, payloads, strict=False)
        ]
        self.batch_set_session_no_ttl(session_prefix, records)
        return len(records)

    def batch_set_vllm_pages_from_layer_no_ttl(
        self, session_prefix, layer_index, block_hashes, block_ids, kv_layer
    ):
        self.vllm_layer_direct_write_calls.append(
            (
                bytes(session_prefix),
                int(layer_index),
                list(block_hashes),
                list(block_ids),
            )
        )
        payloads = [kv_layer.page_bytes(block_id) for block_id in block_ids]
        return self.batch_set_vllm_pages_no_ttl(
            session_prefix,
            layer_index,
            block_hashes,
            payloads,
        )

    def extract_vllm_layer_payload_bytes(self, kv_layer, block_ids):
        self.vllm_layer_extract_calls.append(list(block_ids))
        return [kv_layer.page_bytes(block_id) for block_id in block_ids]

    def batch_set_vllm_layer_payloads_no_ttl(self, session_prefix, layer_groups):
        self.vllm_layer_write_calls.append((bytes(session_prefix), list(layer_groups)))
        records = []
        for layer_index, block_hashes, payloads in layer_groups:
            records.extend(
                (
                    f"vllm-page:{int(layer_index)}:".encode("utf-8")
                    + bytes(block_hash).hex().encode("ascii"),
                    bytes(payload),
                )
                for block_hash, payload in zip(block_hashes, payloads, strict=False)
            )
        self.batch_set_session_no_ttl(session_prefix, records)
        return len(records)

    def batch_set(self, records, ttl=None):
        self.generic_writes.append((records, ttl))

    def batch_get_session_stats(self, session_prefix, keys):
        payloads = [
            self.cached_session_pages.get((bytes(session_prefix), bytes(key)))
            for key in keys
        ]
        total_bytes = sum(len(payload) for payload in payloads if payload is not None)
        return total_bytes, all(payload is not None for payload in payloads)

    def batch_get_session_view(self, session_prefix, keys):
        payloads = [
            self.cached_session_pages.get((bytes(session_prefix), bytes(key)))
            for key in keys
        ]
        return _FakeSessionReadBatch(payloads)

    def batch_get_vllm_pages_view(self, session_prefix, layer_index, block_hashes):
        self.vllm_page_view_calls.append(
            (bytes(session_prefix), int(layer_index), list(block_hashes))
        )
        keys = [
            f"vllm-page:{int(layer_index)}:".encode("utf-8")
            + bytes(block_hash).hex().encode("ascii")
            for block_hash in block_hashes
        ]
        return self.batch_get_session_view(session_prefix, keys)

    def count_vllm_cached_prefix_blocks(self, session_prefix, block_hashes, layer_indices):
        self.vllm_prefix_probe_calls.append(
            (bytes(session_prefix), list(block_hashes), list(layer_indices))
        )
        matched = 0
        for block_hash in block_hashes:
            all_hit = True
            for layer_index in layer_indices:
                key = (
                    f"vllm-page:{int(layer_index)}:".encode("utf-8")
                    + bytes(block_hash).hex().encode("ascii")
                )
                if (bytes(session_prefix), key) not in self.cached_session_pages:
                    all_hit = False
                    break
            if not all_hit:
                break
            matched += 1
        return matched

    def prepare_lmcache_put_batch_encoded_keys(self, keys, metadata_blobs):
        prepared = {
            "keys": list(keys),
            "metadata_blobs": list(metadata_blobs),
        }
        self.prepared_put_calls.append(prepared)
        return prepared

    def batch_put_lmcache_payload_bytes_prepared(self, prepared, payloads):
        self.payload_bytes_prepared_calls.append((prepared, list(payloads)))

    def batch_put_lmcache_payloads_prepared(self, prepared, payloads):
        self.payload_prepared_calls.append((prepared, list(payloads)))

    def batch_put_lmcache_memory_objs_prepared_bytes(self, prepared, objs):
        self.memory_obj_prepared_calls.append((prepared, list(objs)))

    def batch_put_lmcache_payload_bytes_and_metadata_encoded_keys(
        self, keys, payloads, metadata_blobs
    ):
        self.payload_bytes_encoded_calls.append(
            (list(keys), list(payloads), list(metadata_blobs))
        )

    def batch_put_lmcache_payloads_and_metadata_encoded_keys(
        self, keys, payloads, metadata_blobs
    ):
        self.payload_encoded_calls.append((list(keys), list(payloads), list(metadata_blobs)))

    def batch_put_lmcache_memory_objs_encoded_keys(self, keys, objs):
        self.memory_obj_encoded_calls.append((list(keys), list(objs)))

    def batch_put_lmcache_memory_objs_from_engine_keys(self, keys, objs):
        self.memory_obj_engine_key_calls.append((list(keys), list(objs)))


class _FakeRestoreHandle:
    def __init__(self, report, *, ready=True, supports_stream_wait=True):
        self.report = report
        self.ready = ready
        self.supports_stream_wait = supports_stream_wait
        self.cancelled = False
        self.wait_calls = 0
        self.stream_wait_ptrs = []

    def is_ready(self):
        return self.ready

    def peek_report(self):
        return self.report

    def wait_on_stream(self, stream_ptr):
        if not self.supports_stream_wait:
            return False
        self.stream_wait_ptrs.append(stream_ptr)
        return True

    def try_wait(self):
        if not self.ready:
            return None
        return self.wait()

    def wait(self):
        self.wait_calls += 1
        return self.report

    def cancel(self):
        self.cancelled = True
        return True


class KvConnectorV1Tests(unittest.TestCase):
    def test_build_connector_meta_caches_request_state(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        request = _Request("req-1")

        meta = connector.build_connector_meta(
            _SchedulerOutput(
                request,
                b"s:cached",
                [_Page(b"k0", 0, 0, 16)],
                allocation_id=3,
                num_new_matched_tokens=48,
            )
        )

        self.assertEqual(meta["session_prefix"], b"s:cached")
        matched, needs_remote = connector.get_num_new_matched_tokens(request, 8)
        self.assertEqual(matched, 40)
        self.assertFalse(needs_remote)

    def test_get_num_new_matched_tokens_defaults_plain_request_to_miss(self):
        connector = FastCacheKVConnectorV1(store=_FakeStore(), validate_version=False)
        request = _Request("req-cold")

        self.assertEqual(connector.get_num_new_matched_tokens(request, 0), (0, False))

    def test_build_bind_update_and_load_flow(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)

        meta = connector.build_connector_meta(
            session_prefix="s:1",
            requested_pages=[_Page(b"k0", 0, 0, 4096)],
            allocation_id=17,
            num_new_matched_tokens=32,
        )
        bound = connector.bind_connector_metadata(_BoundTarget(), meta)
        self.assertEqual(bound.fast_cache_connector_metadata["allocation_id"], 17)
        self.assertEqual(connector.get_num_new_matched_tokens(meta), (32, False))

        updated = connector.update_state_after_alloc(
            meta,
            block_allocations=[_Block(0, 0x2000, 8192)],
            cpu_fallback_host_ptr=0x5000,
            cpu_fallback_base_offset_bytes=128,
            cpu_fallback_allocation_id=23,
        )

        connector.start_load_kv(updated)
        self.assertIsNone(connector._last_load_report)
        waited = connector.wait_for_layer_load()

        self.assertEqual(waited["backend"], "cpu")
        self.assertEqual(len(store.submitted_calls), 1)
        self.assertEqual(store.submitted_calls[0]["requested_pages"], [(b"k0", 0, 0, 4096)])
        self.assertEqual(store.submitted_calls[0]["block_allocations"], [(0, 0x2000, 8192)])
        self.assertEqual(store.submitted_calls[0]["cpu_fallback_host_ptr"], 0x5000)
        self.assertEqual(store.submitted_calls[0]["path_version"], "host_direct_v1")

    def test_build_connector_meta_preserves_alloc_state_across_rebuilds(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        request = _Request("req-rebuild")

        connector.build_connector_meta(
            _SchedulerOutput(
                request,
                "s:rebuild",
                [_Page("k0", 0, 0, 4)],
                allocation_id=17,
                num_new_matched_tokens=48,
            )
        )
        connector.update_state_after_alloc(
            request,
            [],
            num_external_tokens=12,
            cpu_fallback_host_ptr=0x7000,
            cpu_fallback_base_offset_bytes=96,
            cpu_fallback_allocation_id=23,
        )

        rebuilt = connector.build_connector_meta(
            _SchedulerOutput(request, "s:rebuild", [_Page("k0", 0, 0, 4)])
        )

        self.assertEqual(rebuilt["allocation_id"], 17)
        self.assertEqual(rebuilt["block_allocations"], [])
        self.assertEqual(rebuilt["num_external_tokens"], 12)
        self.assertEqual(rebuilt["cpu_fallback_host_ptr"], 0x7000)
        self.assertEqual(rebuilt["cpu_fallback_base_offset_bytes"], 96)
        self.assertEqual(rebuilt["cpu_fallback_allocation_id"], 23)
        self.assertEqual(connector.get_num_new_matched_tokens(request, 8), (40, False))

        connector.start_load_kv(rebuilt)
        report = connector.wait_for_load()

        self.assertTrue(report["all_hit"])
        self.assertEqual(len(store.submitted_calls), 1)
        self.assertEqual(store.submitted_calls[0]["allocation_id"], 17)
        self.assertEqual(store.submitted_calls[0]["cpu_fallback_host_ptr"], 0x7000)

    def test_build_connector_meta_reuses_cached_pages_when_scheduler_omits_them(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        request = _Request("req-omit-pages")
        pages = [_Page("k0", 0, 0, 4)]
        blocks = [_Block(0, 0x1111, 16)]

        connector.build_connector_meta(
            _SchedulerOutput(
                request,
                "s:omit-pages",
                pages,
                allocation_id=11,
            )
        )
        connector.update_state_after_alloc(
            request,
            blocks,
            num_external_tokens=12,
        )

        rebuilt = connector.build_connector_meta(
            types.SimpleNamespace(
                request=request,
                session_prefix="s:omit-pages",
                num_new_matched_tokens=12,
            )
        )

        self.assertIs(rebuilt["requested_pages"][0], pages[0])
        self.assertEqual(rebuilt["allocation_id"], 11)
        self.assertIs(rebuilt["block_allocations"][0], blocks[0])

        connector.start_load_kv(rebuilt)
        report = connector.wait_for_load(request)

        self.assertTrue(report["all_hit"])
        self.assertEqual(store.submitted_calls[0]["requested_pages"], [(b"k0", 0, 0, 4)])

    def test_build_connector_meta_defaults_to_empty_state_when_scheduler_omits_all_load_fields(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)

        metadata = connector.build_connector_meta(
            types.SimpleNamespace(num_new_matched_tokens=7)
        )

        self.assertEqual(metadata["session_prefix"], b"")
        self.assertEqual(metadata["requested_pages"], [])
        self.assertEqual(metadata["num_new_matched_tokens"], 7)

    def test_update_state_after_alloc_captures_request_object_load_fields(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        request = _Request("req-object-fields")
        request.session_prefix = "s:object"
        request.requested_pages = [_Page("k0", 0, 0, 4)]
        request.num_new_matched_tokens = 12

        metadata = connector.update_state_after_alloc(
            request,
            [_Block(0, 0x1234, 16)],
            num_external_tokens=12,
        )
        step_metadata = connector.build_connector_meta(
            types.SimpleNamespace(
                scheduled_new_reqs=[request],
                scheduled_cached_reqs=types.SimpleNamespace(req_ids=[]),
            )
        )

        self.assertEqual(metadata["session_prefix"], b"s:object")
        self.assertIs(metadata["requested_pages"][0], request.requested_pages[0])
        self.assertEqual(metadata["num_new_matched_tokens"], 12)
        self.assertEqual(step_metadata["request_ids"], ["req-object-fields"])
        self.assertEqual(step_metadata["requests"][0]["session_prefix"], b"s:object")
        self.assertEqual(
            step_metadata["requests"][0]["block_allocations"][0].block_index,
            0,
        )

    def test_update_state_after_alloc_accepts_kv_cache_blocks_wrapper(self):
        connector = FastCacheKVConnectorV1(store=_FakeStore(), validate_version=False)
        request = _Request("req-kv-cache-blocks")

        metadata = connector.update_state_after_alloc(
            request,
            _KVCacheBlocks([3, 4], [9]),
            num_external_tokens=6,
        )

        self.assertEqual(metadata["block_allocations"], [])
        self.assertEqual(metadata["kv_cache_block_ids"], ([3, 4], [9]))

    def test_build_connector_meta_tracks_save_blocks_from_live_request_state(self):
        store = _FakeStore()
        with mock.patch.dict(os.environ, {"FAST_CACHE_VLLM_BLOCK_SIZE": "4"}, clear=False):
            connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        request = _Request("req-save-plan")
        request.block_hashes = [b"\x01\x01", b"\x02\x02", b"\x03\x03"]
        request.num_computed_tokens = 0
        request.num_output_tokens = 0

        connector.update_state_after_alloc(
            request,
            _KVCacheBlocks([10, 11, 12]),
            num_external_tokens=0,
        )
        metadata = connector.build_connector_meta(
            types.SimpleNamespace(
                scheduled_new_reqs=[types.SimpleNamespace(req_id="req-save-plan")],
                scheduled_cached_reqs=types.SimpleNamespace(req_ids=[]),
                num_scheduled_tokens={"req-save-plan": 8},
            )
        )

        request_meta = metadata["requests"][0]
        self.assertEqual(
            request_meta["save_block_hashes"],
            [b"\x01\x01", b"\x02\x02", b"\x03\x03"],
        )
        self.assertEqual(request_meta["save_block_ids"], [10, 11, 12])
        self.assertEqual(request_meta["save_target_block_count"], 3)
        self.assertEqual(
            request_meta["session_prefix"],
            b"fast-cache:vllm:unknown-model",
        )

    def test_build_connector_meta_prefers_materialized_blocks_when_scheduler_tokens_lag(self):
        store = _FakeStore()
        with mock.patch.dict(os.environ, {"FAST_CACHE_VLLM_BLOCK_SIZE": "4"}, clear=False):
            connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        request = _Request("req-save-plan-lag")
        request.block_hashes = [b"\x01\x01", b"\x02\x02", b"\x03\x03", b"\x04\x04"]
        request.num_computed_tokens = 0
        request.num_output_tokens = 0

        connector.update_state_after_alloc(
            request,
            _KVCacheBlocks([10, 11, 12, 13]),
            num_external_tokens=0,
        )
        metadata = connector.build_connector_meta(
            types.SimpleNamespace(
                scheduled_new_reqs=[types.SimpleNamespace(req_id="req-save-plan-lag")],
                scheduled_cached_reqs=types.SimpleNamespace(req_ids=[]),
                num_scheduled_tokens={"req-save-plan-lag": 4},
            )
        )

        request_meta = metadata["requests"][0]
        self.assertEqual(
            request_meta["save_block_hashes"],
            [b"\x01\x01", b"\x02\x02", b"\x03\x03", b"\x04\x04"],
        )
        self.assertEqual(request_meta["save_block_ids"], [10, 11, 12, 13])
        self.assertEqual(request_meta["save_target_block_count"], 4)

    def test_build_connector_meta_skips_cached_prefix_blocks_when_planning_save(self):
        store = _FakeStore()
        store.cached_session_pages.update(
            {
                (b"s:cached-save", b"vllm-page:0:0101"): b"block0",
                (b"s:cached-save", b"vllm-page:0:0202"): b"block1",
            }
        )
        env = {
            "FAST_CACHE_VLLM_BLOCK_SIZE": "4",
            "FAST_CACHE_VLLM_NUM_LAYERS": "1",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        request = _Request("req-cached-save")
        request.session_prefix = b"s:cached-save"
        request.block_hashes = [b"\x01\x01", b"\x02\x02", b"\x03\x03"]
        request.all_token_ids = list(range(12))
        request.num_tokens = 12
        request.num_computed_tokens = 0
        request.num_output_tokens = 0

        matched, _ = connector.get_num_new_matched_tokens(request, 0)
        connector.update_state_after_alloc(
            request,
            _KVCacheBlocks([10, 11, 12]),
            num_external_tokens=matched,
        )
        metadata = connector.build_connector_meta(
            types.SimpleNamespace(
                scheduled_new_reqs=[types.SimpleNamespace(req_id="req-cached-save")],
                scheduled_cached_reqs=types.SimpleNamespace(req_ids=[]),
                num_scheduled_tokens={"req-cached-save": 4},
            )
        )

        request_meta = metadata["requests"][0]
        self.assertEqual(request_meta["save_target_block_count"], 3)
        self.assertEqual(request_meta["save_block_hashes"], [b"\x03\x03"])
        self.assertEqual(request_meta["save_block_ids"], [12])
        self.assertEqual(
            connector._save_states["req-cached-save"].saved_block_count,
            2,
        )

    def test_get_num_new_matched_tokens_probes_cached_block_prefix_across_layers(self):
        store = _FakeStore()
        store.cached_session_pages.update(
            {
                (b"s:prefix", b"vllm-page:0:0101"): b"layer0-block0",
                (b"s:prefix", b"vllm-page:1:0101"): b"layer1-block0",
                (b"s:prefix", b"vllm-page:0:0202"): b"layer0-block1",
                (b"s:prefix", b"vllm-page:1:0202"): b"layer1-block1",
            }
        )
        env = {
            "FAST_CACHE_VLLM_BLOCK_SIZE": "4",
            "FAST_CACHE_VLLM_NUM_LAYERS": "2",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        request = _Request("req-prefix-hit")
        request.session_prefix = b"s:prefix"
        request.block_hashes = [b"\x01\x01", b"\x02\x02", b"\x03\x03"]
        request.all_token_ids = list(range(12))
        request.num_tokens = 12

        matched, needs_remote = connector.get_num_new_matched_tokens(request, 0)

        self.assertEqual((matched, needs_remote), (8, False))
        self.assertEqual(
            connector._request_metadata["req-prefix-hit"]["cached_prefix_block_hashes"],
            [b"\x01\x01", b"\x02\x02"],
        )
        self.assertEqual(len(store.vllm_prefix_probe_calls), 1)

    def test_get_num_new_matched_tokens_recomputes_last_token_on_full_prompt_hit(self):
        store = _FakeStore()
        store.cached_session_pages.update(
            {
                (b"s:full", b"vllm-page:0:0a0b"): b"layer0-block0",
                (b"s:full", b"vllm-page:0:0c0d"): b"layer0-block1",
            }
        )
        env = {
            "FAST_CACHE_VLLM_BLOCK_SIZE": "4",
            "FAST_CACHE_VLLM_NUM_LAYERS": "1",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        request = _Request("req-full-hit")
        request.session_prefix = b"s:full"
        request.block_hashes = [b"\x0a\x0b", b"\x0c\x0d"]
        request.all_token_ids = list(range(8))
        request.num_tokens = 8

        matched, needs_remote = connector.get_num_new_matched_tokens(request, 0)

        self.assertEqual((matched, needs_remote), (7, False))

    def test_update_state_after_alloc_builds_block_load_plan_from_cached_prefix(self):
        store = _FakeStore()
        store.cached_session_pages.update(
            {
                (b"s:load-plan", b"vllm-page:0:0101"): b"layer0-block0",
                (b"s:load-plan", b"vllm-page:1:0101"): b"layer1-block0",
                (b"s:load-plan", b"vllm-page:0:0202"): b"layer0-block1",
                (b"s:load-plan", b"vllm-page:1:0202"): b"layer1-block1",
            }
        )
        env = {
            "FAST_CACHE_VLLM_BLOCK_SIZE": "4",
            "FAST_CACHE_VLLM_NUM_LAYERS": "2",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        request = _Request("req-load-plan")
        request.session_prefix = b"s:load-plan"
        request.block_hashes = [b"\x01\x01", b"\x02\x02", b"\x03\x03"]
        request.all_token_ids = list(range(12))
        request.num_tokens = 12
        request.num_computed_tokens = 0

        matched, _ = connector.get_num_new_matched_tokens(request, 0)
        metadata = connector.update_state_after_alloc(
            request,
            _KVCacheBlocks([10, 11, 12]),
            num_external_tokens=matched,
        )

        self.assertEqual(metadata["load_block_hashes"], [b"\x01\x01", b"\x02\x02"])
        self.assertEqual(metadata["load_block_ids"], [10, 11])

    def test_save_kv_layer_can_store_vllm_block_pages_from_bound_request_metadata(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        connector.bind_connector_metadata(
            {
                "requests": [
                    {
                        "request_id": "req-layer-save",
                        "session_prefix": b"s:block-pages",
                        "save_block_hashes": [b"\x0a\x0b"],
                        "save_block_ids": [1],
                        "save_target_block_count": 1,
                    }
                ]
            }
        )

        count = connector.save_kv_layer(
            "model.layers.3.self_attn.attn",
            _FakePagedTensor({1: b"page-bytes"}),
            _TritonAttentionMetadata(),
        )

        self.assertEqual(count, 1)
        self.assertEqual(
            store.session_writes,
            [(b"s:block-pages", [(b"vllm-page:3:0a0b", b"page-bytes")])],
        )
        self.assertEqual(store.vllm_layer_extract_calls, [[1]])
        self.assertEqual(connector._save_states["req-layer-save"].saved_block_count, 1)

    def test_save_kv_layer_batches_all_layers_into_one_session_publish(self):
        store = _FakeStore()
        env = {
            "FAST_CACHE_VLLM_NUM_LAYERS": "2",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        connector.bind_connector_metadata(
            {
                "requests": [
                    {
                        "request_id": "req-buffered-save",
                        "session_prefix": b"s:buffered-save",
                        "save_block_hashes": [b"\x0a\x0b"],
                        "save_block_ids": [1],
                        "save_target_block_count": 1,
                    }
                ]
            }
        )

        count0 = connector.save_kv_layer(
            "model.layers.0.self_attn.attn",
            _FakePagedTensor({1: b"layer0-page"}),
            _TritonAttentionMetadata(),
        )
        self.assertEqual(count0, 1)
        self.assertEqual(store.session_writes, [])

        count1 = connector.save_kv_layer(
            "model.layers.1.self_attn.attn",
            _FakePagedTensor({1: b"layer1-page"}),
            _TritonAttentionMetadata(),
        )

        self.assertEqual(count1, 1)
        self.assertEqual(len(store.session_writes), 1)
        self.assertEqual(store.session_writes[0][0], b"s:buffered-save")
        self.assertEqual(
            store.session_writes[0][1],
            [
                (b"vllm-page:0:0a0b", b"layer0-page"),
                (b"vllm-page:1:0a0b", b"layer1-page"),
            ],
        )
        self.assertEqual(len(store.vllm_layer_write_calls), 1)
        self.assertEqual(
            store.batch_get_session_stats(
                b"s:buffered-save",
                [b"vllm-page:0:0a0b", b"vllm-page:1:0a0b"],
            ),
            (22, True),
        )
        self.assertEqual(store.vllm_layer_extract_calls, [[1], [1]])
        self.assertEqual(connector._save_states["req-buffered-save"].saved_block_count, 1)

    def test_wait_for_save_flushes_buffered_session_publish(self):
        store = _FakeStore()
        env = {
            "FAST_CACHE_VLLM_NUM_LAYERS": "3",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        connector.bind_connector_metadata(
            {
                "requests": [
                    {
                        "request_id": "req-wait-save",
                        "session_prefix": b"s:wait-save",
                        "save_block_hashes": [b"\x0c\x0d"],
                        "save_block_ids": [2],
                        "save_target_block_count": 1,
                    }
                ]
            }
        )

        connector.save_kv_layer(
            "model.layers.0.self_attn.attn",
            _FakePagedTensor({2: b"layer0-page"}),
            _TritonAttentionMetadata(),
        )
        self.assertEqual(store.session_writes, [])

        connector.wait_for_save()

        self.assertEqual(
            store.session_writes,
            [(b"s:wait-save", [(b"vllm-page:0:0c0d", b"layer0-page")])],
        )
        self.assertEqual(connector._save_states["req-wait-save"].saved_block_count, 1)

    def test_save_kv_layer_skips_duplicate_publish_when_target_already_saved(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        connector.bind_connector_metadata(
            {
                "requests": [
                    {
                        "request_id": "req-skip-duplicate-save",
                        "session_prefix": b"s:skip-duplicate-save",
                        "save_block_hashes": [b"\x0a\x0b", b"\x0c\x0d"],
                        "save_block_ids": [1, 2],
                        "save_target_block_count": 2,
                    }
                ]
            }
        )
        kv_layer = _FakePagedTensor({1: b"page-one", 2: b"page-two"})

        first_count = connector.save_kv_layer(
            "model.layers.0.self_attn.attn",
            kv_layer,
            _TritonAttentionMetadata(),
        )
        second_count = connector.save_kv_layer(
            "model.layers.0.self_attn.attn",
            kv_layer,
            _TritonAttentionMetadata(),
        )

        self.assertEqual(first_count, 2)
        self.assertEqual(second_count, 0)
        self.assertEqual(
            store.session_writes,
            [
                (
                    b"s:skip-duplicate-save",
                    [
                        (b"vllm-page:0:0a0b", b"page-one"),
                        (b"vllm-page:0:0c0d", b"page-two"),
                    ],
                )
            ],
        )
        self.assertEqual(
            connector._save_states["req-skip-duplicate-save"].saved_block_count,
            2,
        )
        self.assertEqual(store.vllm_layer_extract_calls, [[1, 2]])

    def test_save_kv_layer_prefers_direct_layer_write_helper_when_not_buffering(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        connector.bind_connector_metadata(
            {
                "requests": [
                    {
                        "session_prefix": b"s:direct-layer-save",
                        "save_block_hashes": [b"\x0a\x0b", b"\x0c\x0d"],
                        "save_block_ids": [1, 2],
                        "save_target_block_count": 2,
                    }
                ]
            }
        )

        count = connector.save_kv_layer(
            "model.layers.0.self_attn.attn",
            _FakePagedTensor({1: b"page-one", 2: b"page-two"}),
            _TritonAttentionMetadata(),
        )

        self.assertEqual(count, 2)
        self.assertEqual(
            store.vllm_layer_direct_write_calls,
            [(b"s:direct-layer-save", 0, [b"\x0a\x0b", b"\x0c\x0d"], [1, 2])],
        )
        self.assertEqual(store.vllm_layer_extract_calls, [])
        self.assertEqual(
            store.session_writes,
            [
                (
                    b"s:direct-layer-save",
                    [
                        (b"vllm-page:0:0a0b", b"page-one"),
                        (b"vllm-page:0:0c0d", b"page-two"),
                    ],
                )
            ],
        )

    def test_build_connector_meta_batches_step_requests_and_waits_per_layer(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        request_a = _Request("req-step-a")
        request_b = _Request("req-step-b")
        request_a.session_prefix = "s:step-a"
        request_a.requested_pages = [_Page("k0", 0, 0, 4)]
        request_b.session_prefix = "s:step-b"
        request_b.requested_pages = [_Page("k1", 0, 1, 4)]

        connector.update_state_after_alloc(
            request_a,
            [_Block(0, 0x1000, 16)],
            num_external_tokens=4,
        )
        connector.update_state_after_alloc(
            request_b,
            [_Block(1, 0x2000, 16)],
            num_external_tokens=4,
        )

        step_metadata = connector.build_connector_meta(
            types.SimpleNamespace(
                scheduled_new_reqs=[request_a],
                scheduled_cached_reqs=types.SimpleNamespace(req_ids=["req-step-b"]),
            )
        )
        connector.start_load_kv(step_metadata)
        report = connector.wait_for_layer_load("model.layers.0")

        self.assertEqual(step_metadata["request_ids"], ["req-step-a", "req-step-b"])
        self.assertTrue(report["all_hit"])
        self.assertEqual(len(store.submitted_calls), 2)
        self.assertEqual(
            [call["session_prefix"] for call in store.submitted_calls],
            [b"s:step-a", b"s:step-b"],
        )
        self.assertEqual(set(connector._active_step_request_ids), {"req-step-a", "req-step-b"})
        self.assertEqual(
            set(connector._load_states["req-step-a"].layer_reports),
            {0},
        )
        self.assertEqual(
            set(connector._load_states["req-step-b"].layer_reports),
            {0},
        )

    def test_block_load_path_restores_registered_kv_cache_pages(self):
        store = _FakeStore()
        store.cached_session_pages.update(
            {
                (b"s:block-load", b"vllm-page:0:0a0b"): b"layer0-block3",
                (b"s:block-load", b"vllm-page:0:0c0d"): b"layer0-block4",
                (b"s:block-load", b"vllm-page:1:0a0b"): b"layer1-block3",
                (b"s:block-load", b"vllm-page:1:0c0d"): b"layer1-block4",
            }
        )
        env = {
            "FAST_CACHE_VLLM_BLOCK_SIZE": "4",
            "FAST_CACHE_VLLM_NUM_LAYERS": "2",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        request = _Request("req-block-load")
        request.session_prefix = b"s:block-load"
        request.block_hashes = [b"\x0a\x0b", b"\x0c\x0d"]
        request.all_token_ids = list(range(8))
        request.num_tokens = 8
        request.num_computed_tokens = 0

        matched, _ = connector.get_num_new_matched_tokens(request, 0)
        metadata = connector.update_state_after_alloc(
            request,
            _KVCacheBlocks([3, 4]),
            num_external_tokens=matched,
        )
        kv_caches = {
            "model.layers.0.self_attn.attn": _FakePagedTensor(
                {3: b"cold-layer0-block3", 4: b"cold-layer0-block4"}
            ),
            "model.layers.1.self_attn.attn": _FakePagedTensor(
                {3: b"cold-layer1-block3", 4: b"cold-layer1-block4"}
            ),
        }
        connector.register_kv_caches(kv_caches)

        connector.start_load_kv(metadata)
        report0 = connector.wait_for_layer_load("model.layers.0.self_attn.attn")
        report1 = connector.wait_for_layer_load("model.layers.1.self_attn.attn")

        self.assertTrue(report0["all_hit"])
        self.assertTrue(report1["all_hit"])
        self.assertEqual(
            kv_caches["model.layers.0.self_attn.attn"].page_bytes(3),
            b"layer0-block3",
        )
        self.assertEqual(
            kv_caches["model.layers.0.self_attn.attn"].page_bytes(4),
            b"layer0-block4",
        )
        self.assertEqual(
            kv_caches["model.layers.1.self_attn.attn"].page_bytes(3),
            b"layer1-block3",
        )
        self.assertEqual(
            kv_caches["model.layers.1.self_attn.attn"].page_bytes(4),
            b"layer1-block4",
        )
        self.assertEqual(
            store.vllm_page_view_calls,
            [
                (b"s:block-load", 0, [b"\x0a\x0b", b"\x0c\x0d"]),
                (b"s:block-load", 1, [b"\x0a\x0b", b"\x0c\x0d"]),
            ],
        )

    def test_block_load_path_prefers_store_layer_restore_helper(self):
        store = _FakeStore()
        store.cached_session_pages.update(
            {
                (b"s:block-load-rust", b"vllm-page:0:0a0b"): b"layer0-block3",
                (b"s:block-load-rust", b"vllm-page:0:0c0d"): b"layer0-block4",
            }
        )
        restore_calls = []

        def _restore_vllm_pages_into_layer(
            self, session_prefix, layer_index, block_hashes, block_ids, kv_layer
        ):
            restore_calls.append(
                (
                    bytes(session_prefix),
                    int(layer_index),
                    list(block_hashes),
                    list(block_ids),
                )
            )
            hit_pages = 0
            missed_pages = 0
            for block_hash, block_id in zip(block_hashes, block_ids, strict=False):
                key = (
                    f"vllm-page:{int(layer_index)}:".encode("utf-8")
                    + bytes(block_hash).hex().encode("ascii")
                )
                payload = self.cached_session_pages.get((bytes(session_prefix), key))
                if payload is None:
                    missed_pages += 1
                    continue
                kv_layer[block_id].copy_from_bytes(payload)
                hit_pages += 1
            page_count = min(len(block_hashes), len(block_ids))
            return (page_count, hit_pages, missed_pages, missed_pages == 0)

        store.restore_vllm_pages_into_layer = types.MethodType(
            _restore_vllm_pages_into_layer, store
        )

        env = {
            "FAST_CACHE_VLLM_BLOCK_SIZE": "4",
            "FAST_CACHE_VLLM_NUM_LAYERS": "1",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        request = _Request("req-block-load-rust")
        request.session_prefix = b"s:block-load-rust"
        request.block_hashes = [b"\x0a\x0b", b"\x0c\x0d"]
        request.all_token_ids = list(range(8))
        request.num_tokens = 8
        request.num_computed_tokens = 0

        matched, _ = connector.get_num_new_matched_tokens(request, 0)
        metadata = connector.update_state_after_alloc(
            request,
            _KVCacheBlocks([3, 4]),
            num_external_tokens=matched,
        )
        kv_caches = {
            "model.layers.0.self_attn.attn": _FakePagedTensor(
                {3: b"cold-layer0-block3", 4: b"cold-layer0-block4"}
            ),
        }
        connector.register_kv_caches(kv_caches)

        connector.start_load_kv(metadata)
        report = connector.wait_for_layer_load("model.layers.0.self_attn.attn")

        self.assertEqual(report["backend"], "rust-block-cache")
        self.assertTrue(report["all_hit"])
        self.assertEqual(
            kv_caches["model.layers.0.self_attn.attn"].page_bytes(3),
            b"layer0-block3",
        )
        self.assertEqual(
            kv_caches["model.layers.0.self_attn.attn"].page_bytes(4),
            b"layer0-block4",
        )
        self.assertEqual(
            restore_calls,
            [(b"s:block-load-rust", 0, [b"\x0a\x0b", b"\x0c\x0d"], [3, 4])],
        )

    def test_block_load_path_batches_registered_layers_through_store_helper(self):
        store = _FakeStore()
        store.cached_session_pages.update(
            {
                (b"s:block-load-group", b"vllm-page:0:0a0b"): b"layer0-block3",
                (b"s:block-load-group", b"vllm-page:0:0c0d"): b"layer0-block4",
                (b"s:block-load-group", b"vllm-page:1:0a0b"): b"layer1-block3",
                (b"s:block-load-group", b"vllm-page:1:0c0d"): b"layer1-block4",
            }
        )
        restore_calls = []

        def _restore_vllm_pages_into_registered_layers(
            self,
            session_prefix,
            layer_indices,
            block_hashes,
            block_ids,
            kv_layers,
        ):
            restore_calls.append(
                (
                    bytes(session_prefix),
                    list(layer_indices),
                    list(block_hashes),
                    list(block_ids),
                )
            )
            reports = []
            for layer_index, kv_layer in zip(layer_indices, kv_layers, strict=False):
                hit_pages = 0
                missed_pages = 0
                for block_hash, block_id in zip(block_hashes, block_ids, strict=False):
                    key = (
                        f"vllm-page:{int(layer_index)}:".encode("utf-8")
                        + bytes(block_hash).hex().encode("ascii")
                    )
                    payload = self.cached_session_pages.get((bytes(session_prefix), key))
                    if payload is None:
                        missed_pages += 1
                        continue
                    kv_layer[block_id].copy_from_bytes(payload)
                    hit_pages += 1
                page_count = min(len(block_hashes), len(block_ids))
                reports.append(
                    (int(layer_index), page_count, hit_pages, missed_pages, missed_pages == 0)
                )
            return reports

        store.restore_vllm_pages_into_registered_layers = types.MethodType(
            _restore_vllm_pages_into_registered_layers, store
        )

        env = {
            "FAST_CACHE_VLLM_BLOCK_SIZE": "4",
            "FAST_CACHE_VLLM_NUM_LAYERS": "2",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        request = _Request("req-block-load-group")
        request.session_prefix = b"s:block-load-group"
        request.block_hashes = [b"\x0a\x0b", b"\x0c\x0d"]
        request.all_token_ids = list(range(8))
        request.num_tokens = 8
        request.num_computed_tokens = 0

        matched, _ = connector.get_num_new_matched_tokens(request, 0)
        metadata = connector.update_state_after_alloc(
            request,
            _KVCacheBlocks([3, 4]),
            num_external_tokens=matched,
        )
        kv_caches = {
            "model.layers.0.self_attn.attn": _FakePagedTensor(
                {3: b"cold-layer0-block3", 4: b"cold-layer0-block4"}
            ),
            "model.layers.1.self_attn.attn": _FakePagedTensor(
                {3: b"cold-layer1-block3", 4: b"cold-layer1-block4"}
            ),
        }
        connector.register_kv_caches(kv_caches)

        connector.start_load_kv(metadata)
        report0 = connector.wait_for_layer_load("model.layers.0.self_attn.attn")
        report1 = connector.wait_for_layer_load("model.layers.1.self_attn.attn")

        self.assertEqual(report0["backend"], "rust-block-cache-batch")
        self.assertEqual(report1["backend"], "rust-block-cache-batch")
        self.assertTrue(report0["all_hit"])
        self.assertTrue(report1["all_hit"])
        self.assertEqual(
            kv_caches["model.layers.0.self_attn.attn"].page_bytes(3),
            b"layer0-block3",
        )
        self.assertEqual(
            kv_caches["model.layers.1.self_attn.attn"].page_bytes(4),
            b"layer1-block4",
        )
        self.assertEqual(
            restore_calls,
            [(b"s:block-load-group", [0, 1], [b"\x0a\x0b", b"\x0c\x0d"], [3, 4])],
        )

    def test_block_load_path_skips_replaying_identical_completed_restore(self):
        store = _FakeStore()
        store.cached_session_pages.update(
            {
                (b"s:block-load-skip", b"vllm-page:0:0a0b"): b"layer0-block3",
                (b"s:block-load-skip", b"vllm-page:1:0a0b"): b"layer1-block3",
            }
        )
        env = {
            "FAST_CACHE_VLLM_BLOCK_SIZE": "4",
            "FAST_CACHE_VLLM_NUM_LAYERS": "2",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        request = _Request("req-block-load-skip")
        request.session_prefix = b"s:block-load-skip"
        request.block_hashes = [b"\x0a\x0b"]
        request.all_token_ids = list(range(4))
        request.num_tokens = 4
        request.num_computed_tokens = 0

        matched, _ = connector.get_num_new_matched_tokens(request, 0)
        metadata = connector.update_state_after_alloc(
            request,
            _KVCacheBlocks([3]),
            num_external_tokens=matched,
        )
        kv_caches = {
            "model.layers.0.self_attn.attn": _FakePagedTensor({3: b"cold-layer0-block3"}),
            "model.layers.1.self_attn.attn": _FakePagedTensor({3: b"cold-layer1-block3"}),
        }
        connector.register_kv_caches(kv_caches)

        with mock.patch.object(
            connector,
            "_restore_registered_layer_blocks",
            wraps=connector._restore_registered_layer_blocks,
        ) as restore_blocks:
            connector.start_load_kv(metadata)
            first_report = connector.wait_for_load()
            connector.start_load_kv(metadata)
            second_report = connector.wait_for_load()

        self.assertTrue(first_report["all_hit"])
        self.assertTrue(second_report["all_hit"])
        self.assertEqual(restore_blocks.call_count, 2)
        self.assertIsNotNone(
            connector._load_states["req-block-load-skip"].completed_block_load_signature
        )

    def test_start_load_kv_treats_empty_requested_pages_as_ready(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)

        connector.start_load_kv(session_prefix=b"s:empty", requested_pages=[], block_allocations=[])

        self.assertTrue(connector.is_layer_load_ready())
        report = connector.wait_for_load()
        self.assertEqual(report["backend"], "none")
        self.assertEqual(report["page_count"], 0)
        self.assertTrue(report["all_hit"])
        self.assertEqual(store.submitted_calls, [])
        self.assertTrue(connector.is_layer_load_ready("model.layers.0"))
        self.assertEqual(connector.wait_for_layer_load("model.layers.0")["backend"], "none")
        self.assertEqual(connector.poll_layer_load("model.layers.0")["backend"], "none")

    def test_restore_reports_update_matched_tokens_from_actual_hits(self):
        store = _FakeStore()
        store.next_restore_reports = [
            {
                "backend": "cpu",
                "page_count": 1,
                "hit_pages": 1,
                "missed_pages": 0,
                "all_hit": True,
            },
            {
                "backend": "cpu",
                "page_count": 1,
                "hit_pages": 0,
                "missed_pages": 1,
                "all_hit": False,
            },
        ]
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        request = _Request("req-hits")

        connector.build_connector_meta(
            _SchedulerOutput(
                request,
                "s:hits",
                [_Page("k0", 0, 0, 4), _Page("k1", 1, 1, 4)],
                num_new_matched_tokens=48,
            )
        )
        metadata = connector.update_state_after_alloc(
            request,
            [_Block(0, 0x1000, 4), _Block(1, 0x2000, 4)],
            num_external_tokens=48,
        )

        self.assertEqual(connector.get_num_new_matched_tokens(request, 8), (40, False))

        connector.bind_connector_metadata(metadata)
        connector.start_load_kv(_BoundTarget())
        connector.wait_for_load(request)

        self.assertEqual(connector.get_num_new_matched_tokens(request, 0), (24, True))
        self.assertEqual(connector.get_num_new_matched_tokens(request, 8), (16, True))

    def test_start_load_kv_accepts_override_kwargs(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)

        connector.start_load_kv(
            session_prefix=b"s:2",
            requested_pages=[{"cache_key": "k1", "layer_idx": 1, "page_idx": 4, "size_bytes": 64}],
            block_allocations=[{"page_index": 4, "device_ptr": 0x1000, "page_size_bytes": 128}],
        )

        report = connector.wait_for_load()
        self.assertTrue(report["all_hit"])
        self.assertEqual(store.submitted_calls[0]["session_prefix"], b"s:2")

    def test_start_load_kv_submits_one_handle_per_layer(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)

        connector.start_load_kv(
            session_prefix=b"s:layers",
            requested_pages=[
                _Page(b"k0", 0, 0, 64),
                _Page(b"k1", 1, 1, 64),
            ],
            block_allocations=[
                _Block(0, 0x1000, 128),
                _Block(1, 0x2000, 128),
            ],
        )

        self.assertEqual(len(store.submitted_calls), 2)
        self.assertEqual(store.submitted_calls[0]["requested_pages"], [(b"k0", 0, 0, 64)])
        self.assertEqual(store.submitted_calls[1]["requested_pages"], [(b"k1", 1, 1, 64)])

    def test_start_load_kv_can_fall_back_to_cpu_without_gpu_blocks(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)

        connector.start_load_kv(
            session_prefix=b"s:cpu-fallback",
            requested_pages=[
                _Page(b"k0", 0, 0, 4),
                _Page(b"k1", 1, 1, 8),
            ],
            block_allocations=[],
            cpu_fallback_host_ptr=0x5000,
            cpu_fallback_base_offset_bytes=64,
            cpu_fallback_allocation_id=23,
        )

        report = connector.wait_for_load()
        self.assertTrue(report["all_hit"])
        self.assertEqual(len(store.submitted_calls), 2)
        self.assertEqual(store.submitted_calls[0]["block_allocations"], [])
        self.assertEqual(store.submitted_calls[1]["block_allocations"], [])
        self.assertEqual(store.submitted_calls[0]["cpu_fallback_base_offset_bytes"], 64)
        self.assertEqual(store.submitted_calls[1]["cpu_fallback_base_offset_bytes"], 68)

    def test_wait_for_layer_load_resolves_only_requested_layer(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)

        connector.start_load_kv(
            session_prefix=b"s:layers",
            requested_pages=[
                _Page(b"k0", 0, 0, 64),
                _Page(b"k1", 1, 1, 64),
            ],
            block_allocations=[
                _Block(0, 0x1000, 128),
                _Block(1, 0x2000, 128),
            ],
        )

        report = connector.wait_for_layer_load("model.layers.1")
        self.assertTrue(report["all_hit"])
        self.assertNotIn(1, connector._pending_layer_handles)
        self.assertIn(0, connector._pending_layer_handles)

    def test_is_layer_load_ready_and_poll_layer_load(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)

        connector.start_load_kv(
            session_prefix=b"s:layers",
            requested_pages=[
                _Page(b"k0", 0, 0, 64),
                _Page(b"k1", 1, 1, 64),
            ],
            block_allocations=[
                _Block(0, 0x1000, 128),
                _Block(1, 0x2000, 128),
            ],
        )

        connector._pending_layer_handles[1].ready = False
        self.assertFalse(connector.is_layer_load_ready("model.layers.1"))
        self.assertIsNone(connector.poll_layer_load("model.layers.1"))

        connector._pending_layer_handles[1].ready = True
        report = connector.poll_layer_load("model.layers.1")
        self.assertTrue(report["all_hit"])
        self.assertTrue(connector.is_layer_load_ready("model.layers.1"))

    def test_wait_for_layer_load_can_attach_current_stream_without_blocking(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        connector._current_cuda_stream_ptr = lambda _device: 0xDEADBEEF

        connector.start_load_kv(
            session_prefix=b"s:layers",
            requested_pages=[
                _Page(b"k0", 0, 0, 64),
                _Page(b"k1", 1, 1, 64),
            ],
            block_allocations=[
                _Block(0, 0x1000, 128),
                _Block(1, 0x2000, 128),
            ],
        )

        handle = connector._pending_layer_handles[1]
        report = connector.wait_for_layer_load("model.layers.1")

        self.assertTrue(report["all_hit"])
        self.assertEqual(handle.stream_wait_ptrs, [0xDEADBEEF])
        self.assertEqual(handle.wait_calls, 0)
        self.assertNotIn(1, connector._pending_layer_handles)
        self.assertIn(1, connector._attached_layer_handles)

    def test_request_load_state_is_isolated_per_request(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        request_a = _Request("req-a")
        request_b = _Request("req-b")

        metadata_a = connector.update_state_after_alloc(
            connector.build_connector_meta(
                _SchedulerOutput(request_a, "s:a", [_Page("k0", 0, 0, 16)])
            ),
            [_Block(0, 0x1000, 32)],
        )
        metadata_b = connector.update_state_after_alloc(
            connector.build_connector_meta(
                _SchedulerOutput(request_b, "s:b", [_Page("k1", 1, 1, 16)])
            ),
            [_Block(1, 0x2000, 32)],
        )

        connector.start_load_kv(metadata_a)
        connector.start_load_kv(metadata_b)

        handle_a = connector._load_states["req-a"].pending_layer_handles[0]
        handle_b = connector._load_states["req-b"].pending_layer_handles[1]
        connector.request_finished(request_a)

        self.assertTrue(handle_a.cancelled)
        self.assertFalse(handle_b.cancelled)
        self.assertNotIn("req-a", connector._load_states)
        self.assertIn("req-b", connector._load_states)
        self.assertEqual(len(store.submitted_calls), 2)

    def test_start_load_kv_uses_configured_path_version(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(
            store=store,
            validate_version=False,
            path_version="gpu_direct_api_v0",
        )

        connector.start_load_kv(
            session_prefix=b"s:gpu-direct",
            requested_pages=[_Page(b"k0", 0, 0, 64)],
            block_allocations=[_Block(0, 0x1000, 128)],
        )
        connector.wait_for_load()

        self.assertEqual(len(store.submitted_calls), 1)
        self.assertEqual(store.submitted_calls[0]["path_version"], "gpu_direct_api_v0")

    def test_requires_piecewise_for_cudagraph(self):
        connector = FastCacheKVConnectorV1(store=_FakeStore(), validate_version=False)
        self.assertTrue(connector.requires_piecewise_for_cudagraph())

    def test_requires_piecewise_for_cudagraph_can_be_disabled_by_env(self):
        connector = FastCacheKVConnectorV1(store=_FakeStore(), validate_version=False)
        with mock.patch.dict(
            os.environ,
            {"FAST_CACHE_VLLM_REQUIRE_PIECEWISE_CUDAGRAPH": "false"},
            clear=False,
        ):
            self.assertFalse(connector.requires_piecewise_for_cudagraph())

    def test_requires_piecewise_for_cudagraph_honors_extra_config_override(self):
        connector = FastCacheKVConnectorV1(store=_FakeStore(), validate_version=False)
        self.assertFalse(
            connector.requires_piecewise_for_cudagraph(
                {"requires_piecewise_for_cudagraph": False}
            )
        )

    def test_save_kv_layer_uses_session_write_path_by_default(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)

        count = connector.save_kv_layer(
            session_prefix="s:3",
            records=[_SaveRecord("k0", b"abcd"), {"cache_key": b"k1", "payload": memoryview(b"wxyz")}],
        )

        self.assertEqual(count, 2)
        self.assertEqual(len(store.session_writes), 1)
        self.assertEqual(store.session_writes[0][0], b"s:3")
        self.assertEqual(store.session_writes[0][1], [(b"k0", b"abcd"), (b"k1", b"wxyz")])

    def test_save_kv_layer_can_fall_back_to_generic_ttl_write(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)

        connector.save_kv_layer(
            session_prefix=b"s:4",
            records=[_SaveRecord(b"k2", b"payload")],
            ttl=60,
        )

        self.assertEqual(store.generic_writes, [([(b"k2", b"payload")], 60)])

    def test_save_kv_layer_can_use_kv_layer_payload_bytes_with_metadata(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)

        count = connector.save_kv_layer(
            layer_name="model.layers.0",
            kv_layer=b"payload",
            attn_metadata=_SaveMetadata(
                encoded_keys={0: [b"k0"]},
                metadata_blobs={0: [b"meta0"]},
            ),
        )

        self.assertEqual(count, 1)
        self.assertEqual(
            store.prepared_put_calls,
            [{"keys": [b"k0"], "metadata_blobs": [b"meta0"]}],
        )
        self.assertEqual(
            store.payload_bytes_prepared_calls,
            [({"keys": [b"k0"], "metadata_blobs": [b"meta0"]}, [b"payload"])],
        )

    def test_save_kv_layer_can_use_memory_objs_from_kv_layer(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        memory_obj = _MemoryObj(b"payload")

        count = connector.save_kv_layer(
            layer_name="model.layers.2",
            kv_layer=memory_obj,
            attn_metadata=_SaveMetadata(
                encoded_keys={2: [b"k2"]},
                metadata_blobs={2: [b"meta2"]},
            ),
        )

        self.assertEqual(count, 1)
        self.assertEqual(len(store.memory_obj_prepared_calls), 1)
        self.assertEqual(store.memory_obj_prepared_calls[0][0]["keys"], [b"k2"])
        self.assertEqual(store.memory_obj_prepared_calls[0][1], [memory_obj])

    def test_update_state_after_alloc_and_bound_metadata_drive_load(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        request = _Request("req-2")
        meta = connector.build_connector_meta(
            _SchedulerOutput(request, "s:bound", [_Page("k9", 4, 2, 128)])
        )
        updated = connector.update_state_after_alloc(
            request,
            [_Block(2, 0x9000, 256)],
            num_external_tokens=12,
            cpu_fallback_host_ptr=0xA000,
        )
        self.assertEqual(updated["num_external_tokens"], 12)
        connector.bind_connector_metadata(updated)
        connector.start_load_kv(_BoundTarget())

        self.assertEqual(meta["session_prefix"], b"s:bound")
        self.assertEqual(store.submitted_calls[0]["requested_pages"], [(b"k9", 4, 2, 128)])
        self.assertEqual(store.submitted_calls[0]["block_allocations"], [(2, 0x9000, 256)])
        connector.request_finished(request)
        matched, _ = connector.get_num_new_matched_tokens(request, 0)
        self.assertEqual(matched, 0)

    def test_request_finished_cancels_inflight_load(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        request = _Request("req-pending")
        metadata = connector.update_state_after_alloc(
            connector.build_connector_meta(
                _SchedulerOutput(
                    request,
                    "s:pending",
                    [_Page(b"k0", 0, 0, 64), _Page(b"k1", 1, 1, 64)],
                )
            ),
            [_Block(0, 0x1111, 128), _Block(1, 0x2222, 128)],
        )
        connector.start_load_kv(metadata)

        handles = list(connector._pending_layer_handles.values())
        self.assertEqual(len(handles), 2)
        connector._attached_layer_handles[0] = connector._pending_layer_handles.pop(0)
        self.assertEqual(connector.request_finished(request, [0, 1]), (False, None))
        self.assertTrue(all(handle.cancelled for handle in handles))
        self.assertEqual(connector._pending_layer_handles, {})
        self.assertEqual(connector._attached_layer_handles, {})

    def test_get_finished_reports_completed_requests_once(self):
        connector = FastCacheKVConnectorV1(store=_FakeStore(), validate_version=False)
        request = _Request("req-finished")

        connector.request_finished(request)

        self.assertEqual(connector.get_finished(), (None, None))
        self.assertEqual(connector.get_finished(), (None, None))

    def test_get_finished_accepts_vllm_finished_request_ids(self):
        connector = FastCacheKVConnectorV1(store=_FakeStore(), validate_version=False)

        self.assertEqual(connector.get_finished(["req-a", "req-b"]), (None, None))
        self.assertEqual(connector.get_finished(), (None, None))

    def test_get_block_ids_with_load_errors_tracks_failed_registered_block_restore(self):
        store = _FakeStore()
        store.cached_session_pages.update(
            {
                (b"s:load-errors", b"vllm-page:0:0a0b"): b"block3",
            }
        )
        env = {
            "FAST_CACHE_VLLM_BLOCK_SIZE": "4",
            "FAST_CACHE_VLLM_NUM_LAYERS": "1",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            connector = FastCacheKVConnectorV1(store=store, validate_version=False)
        connector.register_kv_caches(
            {
                "model.layers.0.self_attn.attn": _FakePagedTensor(
                    {3: b"cold-block3", 4: b"cold-block4"}
                )
            }
        )
        request = _Request("req-load-errors")
        metadata = {
            "request_id": "req-load-errors",
            "session_prefix": b"s:load-errors",
            "load_block_hashes": [b"\x0a\x0b", b"\x0c\x0d"],
            "load_block_ids": [3, 4],
        }

        connector.start_load_kv(metadata)
        report = connector.wait_for_load(request)

        self.assertFalse(report["all_hit"])
        self.assertEqual(connector.get_block_ids_with_load_errors(request), [3, 4])
        self.assertEqual(connector.get_block_ids_with_load_errors(request), [])

    def test_shutdown_cancels_inflight_loads_and_clears_state(self):
        store = _FakeStore()
        connector = FastCacheKVConnectorV1(store=store, validate_version=False)

        connector.start_load_kv(
            session_prefix=b"s:shutdown",
            requested_pages=[_Page(b"k0", 0, 0, 64)],
            block_allocations=[_Block(0, 0x1000, 128)],
        )
        handle = connector._pending_layer_handles[0]
        save_state = kv_connector_v1_module._RequestSaveState(request_id="req-save")
        save_state.buffered_session_prefix = b"s:shutdown-save"
        save_state.buffered_layer_payloads = {0: [b"payload"]}
        save_state.buffered_target_block_count = 1
        save_state.buffered_block_hashes = (b"\x0a\x0b",)
        connector._save_states["req-save"] = save_state
        connector._request_metadata["req-save"] = {"request_id": "req-save"}
        connector._unfinished_requests["req-save"] = object()
        connector._request_load_error_block_ids["req-load-errors"] = [7]

        connector.shutdown()

        self.assertTrue(handle.cancelled)
        self.assertEqual(connector._load_states, {})
        self.assertEqual(connector._save_states, {})
        self.assertEqual(connector._request_metadata, {})
        self.assertEqual(connector._unfinished_requests, {})
        self.assertEqual(connector._request_load_error_block_ids, {})
        self.assertEqual(connector._active_step_request_ids, [])
        self.assertIsNone(connector._active_load_request_id)

    def test_default_store_uses_environment_configuration(self):
        store_calls = []

        def _store_factory(**kwargs):
            store_calls.append(kwargs)
            return _FakeStore()

        fake_fast_cache = types.SimpleNamespace(Store=_store_factory)
        env = {
            "FAST_CACHE_VLLM_CORES": "6",
            "FAST_CACHE_VLLM_ROUTE_MODE": "session_prefix",
            "FAST_CACHE_VLLM_CLIENT_ARCHITECTURE": "local_embedded",
            "FAST_CACHE_VLLM_ENABLE_METRICS": "true",
            "FAST_CACHE_VLLM_PATH_VERSION": "gpu_direct_api_v0",
            "FAST_CACHE_VLLM_DEVICE_ORDINAL": "3",
            "FAST_CACHE_VLLM_STREAM_ORDINAL": "9",
            "FAST_CACHE_VLLM_ALLOW_CPU_FALLBACK": "false",
            "FAST_CACHE_VLLM_CUDA_ENABLED": "false",
        }
        with mock.patch.dict(kv_connector_v1_module._DEFAULT_STORE_SINGLETONS, {}, clear=True):
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch.dict(sys.modules, {"fast_cache": fake_fast_cache}):
                    connector = FastCacheKVConnectorV1(validate_version=False)

        self.assertEqual(
            store_calls,
            [
                {
                    "cores": 6,
                    "route_mode": "session_prefix",
                    "client_architecture": "local_embedded",
                    "enable_metrics": True,
                }
            ],
        )
        self.assertEqual(connector._device_ordinal, 3)
        self.assertEqual(connector._stream_ordinal, 9)
        self.assertEqual(connector._path_version, "gpu_direct_api_v0")
        self.assertFalse(connector._allow_cpu_fallback)
        self.assertFalse(connector._cuda_enabled)

    def test_default_store_is_shared_across_connector_instances(self):
        store_calls = []

        def _store_factory(**kwargs):
            store_calls.append(kwargs)
            return _FakeStore()

        fake_fast_cache = types.SimpleNamespace(Store=_store_factory)
        with mock.patch.dict(kv_connector_v1_module._DEFAULT_STORE_SINGLETONS, {}, clear=True):
            with mock.patch.dict(sys.modules, {"fast_cache": fake_fast_cache}):
                first = FastCacheKVConnectorV1(validate_version=False)
                second = FastCacheKVConnectorV1(validate_version=False)

        self.assertIs(first._store, second._store)
        self.assertEqual(len(store_calls), 1)

    def test_preflight_validates_version_and_connector_construction(self):
        store_calls = []

        def _store_factory(**kwargs):
            store_calls.append(kwargs)
            return _FakeStore()

        fake_fast_cache = types.SimpleNamespace(Store=_store_factory, __name__="fast_cache")
        fake_vllm = types.SimpleNamespace(__version__="0.17.1")

        with mock.patch.dict(kv_connector_v1_module._DEFAULT_STORE_SINGLETONS, {}, clear=True):
            with mock.patch.dict(
                sys.modules,
                {
                    "fast_cache": fake_fast_cache,
                    "vllm": fake_vllm,
                },
            ):
                summary = run_preflight("0.17.1")

        self.assertEqual(summary["status"], "ok")
        self.assertEqual(summary["vllm_version"], "0.17.1")
        self.assertTrue(summary["connector_ready"])
        self.assertEqual(len(store_calls), 1)

    def test_constructor_tolerates_upstream_base_without_vllm_config(self):
        store = _FakeStore()

        def _failing_base_init(self, vllm_config=None, role=None, kv_cache_config=None):
            raise AttributeError("'NoneType' object has no attribute 'kv_transfer_config'")

        with mock.patch.object(
            FastCacheKVConnectorV1.__mro__[1],
            "__init__",
            _failing_base_init,
        ):
            connector = FastCacheKVConnectorV1(store=store, validate_version=False)

        self.assertIs(connector._store, store)
        self.assertEqual(connector.role, None)
        self.assertFalse(connector.has_connector_metadata())


if __name__ == "__main__":
    unittest.main()
