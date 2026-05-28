# shardcache vLLM connector

A Python control shim for the direct Rust-native shardcache vLLM path.
Python handles hook registration. Rust runs the validated restore plan
through `shardcache.Store.restore_vllm_paged(...)`.

Target: `vllm==0.17.1`.

## Install

```bash
pip install ./integrations/vllm_direct_connector
```

Install the matching `shardcache` PyO3 wheel in the same environment.

## Connector class

`shardcache_vllm_connector.kv_connector_v1.ShardCacheKVConnectorV1`
exposes the pinned vLLM hooks:

- `build_connector_meta`
- `bind_connector_metadata`
- `get_num_new_matched_tokens`
- `update_state_after_alloc`
- `start_load_kv`
- `wait_for_layer_load`
- `save_kv_layer`

## Restore path

Selected through `SHARDCACHE_VLLM_PATH_VERSION`:

- `host_direct_v1` (default): direct Rust restore path.
- `gpu_direct_api_v0`: experimental queue-based GPU-originated path.

## Example configuration

```bash
export SHARDCACHE_VLLM_PATH_VERSION=host_direct_v1
export SHARDCACHE_VLLM_CONNECTOR=shardcache_vllm_connector.kv_connector_v1.ShardCacheKVConnectorV1
```

In a vLLM integration, register `ShardCacheKVConnectorV1` as the KV connector
class and let Python handle lifecycle hooks while Rust executes the restore
plan.
