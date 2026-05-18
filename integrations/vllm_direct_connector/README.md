# fast-cache vLLM connector

A Python control shim for the direct Rust-native fast-cache vLLM path.
Python handles hook registration. Rust runs the validated restore plan
through `fast_cache.Store.restore_vllm_paged(...)`.

Target: `vllm==0.17.1`.

## Install

```bash
pip install ./integrations/vllm_direct_connector
```

Install the matching `fast_cache` PyO3 wheel in the same environment.

## Connector class

`fast_cache_vllm_connector.kv_connector_v1.FastCacheKVConnectorV1`
exposes the pinned vLLM hooks:

- `build_connector_meta`
- `bind_connector_metadata`
- `get_num_new_matched_tokens`
- `update_state_after_alloc`
- `start_load_kv`
- `wait_for_layer_load`
- `save_kv_layer`

## Restore path

Selected through `FAST_CACHE_VLLM_PATH_VERSION`:

- `host_direct_v1` (default): direct Rust restore path.
- `gpu_direct_api_v0`: experimental queue-based GPU-originated path.
