# Semantic Cache Head-to-Head Benchmark Design

This benchmark is intended to support a publishable semantic-cache performance
claim without leaning on a single favorable workload. It separates backend
lookup speed from real application cache behavior, because products differ on
where they cache embeddings, query results, and LLM responses.

## Target Set

### Native semantic-cache products

These are the primary head-to-head competitors because they expose semantic
cache APIs rather than only vector search APIs.

| Target | Mode | Notes |
| --- | --- | --- |
| ShardCache native semantic cache | Embedded Rust | Run with query-result cache disabled for no-memo rows and enabled for cached rows. |
| ShardCache server semantic cache | TCP RESP server | Run through `SEMANTIC.SET` / `SEMANTIC.SEARCH` so the report shows service-mode overhead beside embedded performance. |
| BetterDB semantic cache | Python package + Valkey/Redis-compatible backend | Use precomputed embedding stub for backend rows; use the real product API for cached prompt rows. |
| RedisVL `SemanticCache` | Python package + Redis/Valkey vector index | Run through the official `check` / `store` surface. |
| Redis LangCache | Managed HTTP semantic-cache service | Separate cloud/HTTP section; do not mix with local in-process rankings unless labeled. |
| LangChain `RedisSemanticCache` | LangChain cache integration + Redis | Product-facing app-cache comparison, not a raw vector-engine row. |
| GPTCache | GPTCache API + vector backend | Run at least FAISS and HNSWLib local backends; Milvus/Qdrant variants are optional vector-backed rows. |

### Vector backends used as semantic-cache engines

These are useful context, but they are not semantic-cache products by
themselves. They should be labeled as vector-search cache equivalents.

| Target | Index mode | Notes |
| --- | --- | --- |
| Redis vector search | FLAT and HNSW where available | Baseline Redis/RediSearch vector path. |
| FAISS | FlatIP, HNSW, IVF-Flat | In-process lower bound for vector lookup. |
| Qdrant | HNSW cosine | Local container and optional cloud run. |
| Milvus | HNSW or IVF_FLAT cosine | Local standalone container if setup cost is acceptable. |

## Workloads

Every target runs the same fixture, thresholds, and query order. Each run emits
raw per-scenario rows plus one summary row.

| Scenario | Purpose | Cache state | Query set |
| --- | --- | --- | --- |
| `miss-cold` | Cost paid before falling through to the LLM. | Entries loaded; query-result and embedding memo caches disabled or cleared. | Negative/non-matching queries only. |
| `hit-cold-unique` | First-time semantic hit lookup, no memo help. | Entries loaded; query-result and embedding memo caches disabled or cleared. | Unique positive paraphrase queries. |
| `hit-hot-cached` | Real repeated app traffic. | Product caches enabled and warmed. | Repeated prompt pool with Zipf skew. |
| `mixed-realistic` | Production blend. | Product defaults enabled after warmup. | 60% repeated hits, 25% unique semantic hits, 15% misses by default. |
| `insert-while-read` | Cache churn and invalidation cost. | Product defaults enabled. | Mixed lookups with 1%, 5%, and 10% inserts. |

The first publishable claim should use `miss-cold`, `hit-cold-unique`, and
`hit-hot-cached`. `mixed-realistic` and `insert-while-read` are follow-up rows
for a broader report.

## Dataset Matrix

| Dataset | Entries | Dims | Why |
| --- | ---: | ---: | --- |
| BetterDB-compatible SemBenchmarkLmArena fixture | 5k | model-dependent | Direct comparability to the BetterDB/RedisVL public benchmark shape. |
| BetterDB-compatible PAWS-Wiki fixture | 5k | model-dependent | Paraphrase-heavy quality sanity check. |
| Synthetic scale fixture | 100k, 1M | 384 | Saturation and scaling without external model/runtime variance. |
| Real FAQ/chatbot fixture | 10k-100k | 384 or 768 | App-realistic prompt locality, repeated questions, and misses. |

For backend lookup rows, embeddings are precomputed and loaded from CSV. For
app-cache rows, every target receives the same prompt text, and the embedding
provider is either the same local model process or a deterministic embedding
stub that returns the precomputed vector for each prompt.

## Measurement Modes

### Backend lookup mode

Goal: compare semantic lookup engines, not embedding models or Python client
embedding overhead.

- Precompute all embeddings.
- Load entries before timed section.
- Do not call an LLM.
- Disable or clear query-result memo caches.
- Use cosine similarity and `top_k=1`.
- Report quality metrics from the labeled fixture and latency/throughput from
  the load fixture.

This is the mode for claims like "ShardCache lookup throughput is X times
BetterDB/RedisVL."

### App-cache mode

Goal: compare what an application experiences when using the product normally.

- Use each product's public cache API.
- Product-level embedding caching and exact query caching may be enabled.
- Do not call a paid LLM; use a deterministic mock LLM on misses so miss paths
  still include store/write behavior.
- Measure `check_ms`, `miss_llm_ms`, `store_ms`, `total_ms`, and `hit_rate`.

This is the mode for claims like "Repeated prompt latency is X times lower."

## Fairness Rules

- Run all local targets on the same Adam host.
- Pin Redis/Valkey/vector services to localhost; record container image digests
  or package versions.
- For publishable max-load rows on Adam, pin the system under test to
  `SUT_CPUSET=0-15`, pin external load/client workers to `LOAD_CPUSET=16-31`,
  and use `WORKERS=16`. Embedded/in-process adapters must be marked as such
  because their benchmark process is also the system under test.
- Include both `ShardCache Embedded` and `ShardCache Server` rows whenever the
  report makes a ShardCache performance claim. The embedded row measures native
  library use; the server row measures TCP/RESP service-mode overhead.
- Use the same fixture order for every adapter.
- Convert thresholds explicitly:
  - cosine similarity `>= 1 - distance_threshold`
  - cosine distance `<= distance_threshold`
- Use `top_k=1` and return only the cached value plus score/distance.
- Clear indexes, query caches, and embedding caches between cold scenarios.
- Warm hot scenarios with the exact declared warmup query count.
- Run at least 3 replicates per target/scenario and report median plus min/max.
- Randomize target order within a replicate to avoid thermal/order bias.
- Record CPU model, RAM, kernel, Redis/Valkey version, package versions, git
  SHA, and command line.
- Mark managed/cloud HTTP services separately from local/in-process results.

## Required Metrics

Each result row should include both system-under-test CPU and total CPU. Use
`ops_per_sut_cpu` as the headline efficiency metric; it excludes client/load
driver CPU for networked rows. Keep `ops_per_total_cpu` and the raw client CPU
fields for auditability.

```text
run_id, git_sha, host, timestamp_utc,
target, target_version, backend, scenario,
dataset, entries, dims, threshold_distance,
workers, duration_s, query_pool, warmup_queries,
queries, hits, true_positives, false_positives, true_negatives, false_negatives,
precision, recall, f1, hit_rate,
ops_per_sec, ops_per_sut_cpu, ops_per_total_cpu, p50_ms, p95_ms, p99_ms, max_ms,
process_cpu_seconds, process_vcpu, external_cpu_seconds, external_vcpu,
sut_cpu_seconds, sut_vcpu, client_cpu_seconds, client_vcpu,
total_cpu_seconds, total_vcpu,
build_seconds, load_seconds, rss_mb, index_bytes,
cache_policy, query_cache_enabled, embedding_cache_enabled,
notes
```

For app-cache rows, add:

```text
check_p50_ms, mock_llm_p50_ms, store_p50_ms, total_p50_ms,
cache_miss_count, store_count
```

## Adapter Contract

Every Python peer adapter should expose the same operations:

```python
class SemanticCacheAdapter:
    name: str

    async def setup(self, fixture, config) -> None: ...
    async def load_entries(self, entries) -> None: ...
    async def clear_query_cache(self) -> None: ...
    async def lookup_vector(self, vector, threshold) -> LookupResult: ...
    async def lookup_prompt(self, prompt, threshold) -> LookupResult: ...
    async def store_prompt(self, prompt, response, vector=None) -> None: ...
    async def teardown(self) -> None: ...
```

Adapters may implement only `lookup_vector` or only `lookup_prompt`; unsupported
scenario rows are reported as `n/a`, not silently skipped.

## First Adam Matrix

Run this first, because it is broad enough for an honest claim while still
small enough to debug quickly.

| Group | Targets | Scenarios | Entries | Workers | Duration |
| --- | --- | --- | ---: | ---: | ---: |
| Native/cache products | ShardCache, BetterDB, RedisVL `SemanticCache`, GPTCache+FAISS, LangChain RedisSemanticCache | `miss-cold`, `hit-cold-unique`, `hit-hot-cached` | 100k | 1, 8, 16, 32 | 10s |
| Vector baselines | Redis vector FLAT/HNSW, FAISS FlatIP/HNSW, Qdrant local | `miss-cold`, `hit-cold-unique` | 100k | 1, 8, 16, 32 | 10s |
| Scale check | ShardCache, BetterDB, RedisVL, FAISS HNSW, Qdrant local | `hit-cold-unique` | 1M | 16, 32 | 10s |

Redis LangCache should be run as a separate managed-service table once
credentials are available, because network and service placement dominate the
latency shape.

## Claim Ladder

Use these exact wording tiers:

1. "Fastest in our BetterDB/RedisVL head-to-head" requires ShardCache,
   BetterDB, and RedisVL rows for all three first-run scenarios.
2. "Fastest local semantic cache we measured" requires ShardCache, BetterDB,
   RedisVL, GPTCache+FAISS, and LangChain RedisSemanticCache rows.
3. "Fastest semantic cache we measured" requires the local product set plus
   Redis LangCache and at least one vector-backed cache equivalent section,
   clearly separating local and managed-service rows.
4. "Fastest semantic cache" should not be used unless every row, fixture,
   adapter implementation, and raw CSV is published and reproducible.

## Reference Docs Checked

- BetterDB public comparison: <https://www.betterdb.com/blog/benchmark-semantic-cache-vs-redisvl>
- RedisVL semantic cache API: <https://redis.io/docs/latest/develop/ai/redisvl/api/cache/>
- Redis LangCache overview: <https://redis.io/docs/latest/develop/ai/context-engine/langcache/>
- RedisVL LangCache wrapper: <https://docs.redisvl.com/en/latest/user_guide/13_langcache_semantic_cache.html>
- LangChain Redis semantic cache reference: <https://reference.langchain.com/python/langchain-redis/cache/RedisSemanticCache>
- GPTCache project and vector backend support: <https://github.com/zilliztech/GPTCache>
- GPTCache manager/vector backend reference: <https://gptcache.readthedocs.io/en/dev/references/manager.html>
- Qdrant LangChain integration reference: <https://qdrant.tech/documentation/frameworks/langchain/>
