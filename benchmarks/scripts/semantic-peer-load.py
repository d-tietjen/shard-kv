#!/usr/bin/env python3
"""Semantic-cache load benchmark for semantic-cache peers and vector baselines.

This script is intentionally cache-path focused: vectors are generated once and
the BetterDB embed function returns precomputed vectors. That makes the peer
numbers comparable to shardcache's native semantic-cache benchmark, which also
takes embeddings as input rather than timing an embedding model.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import resource
import shutil
import statistics
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import numpy as np


@dataclass
class VectorFixture:
    cache_vectors: np.ndarray
    query_vectors: np.ndarray


@dataclass
class LoadResult:
    scenario: str
    adapter: str
    workers: int
    entries: int
    dims: int
    query_pool: int
    seconds: float
    queries: int
    hits: int
    ops_per_sec: float
    ops_per_cpu: float
    ops_per_sut_cpu: float
    ops_per_total_cpu: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    process_cpu_seconds: float
    process_vcpu: float
    external_cpu_seconds: float
    external_vcpu: float
    total_cpu_seconds: float
    total_vcpu: float
    sut_cpu_seconds: float
    sut_vcpu: float
    client_cpu_seconds: float
    client_vcpu: float
    process_cpuset: str
    external_pids: str


@dataclass
class CpuSnapshot:
    process_seconds: float
    external_seconds: float


def normalised_vectors(entries: int, dims: int, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    vectors = rng.normal(size=(entries, dims)).astype(np.float32)
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    vectors /= np.maximum(norms, 1e-12)
    return vectors


def load_pairs_csv(
    path: Path,
    entries: int,
    query_pool: int,
    query_source: str,
    seed: int,
) -> VectorFixture:
    cache_rows: list[list[float]] = []
    query_rows: list[list[float]] = []
    with path.open(newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header is None:
            raise ValueError(f"{path} is empty")
        lower = [name.strip().lower() for name in header]
        try:
            cache_index = lower.index("cache_embedding")
            query_index = lower.index("query_embedding")
        except ValueError as exc:
            raise ValueError(f"{path} must contain cache_embedding and query_embedding columns") from exc
        label_index = lower.index("label") if "label" in lower else None
        for row in reader:
            if len(cache_rows) < entries:
                cache_rows.append(parse_embedding(row[cache_index]))
            if query_source == "fixture" and len(query_rows) < query_pool:
                query_rows.append(parse_embedding(row[query_index]))
            elif query_source == "fixture-positive" and len(query_rows) < query_pool:
                if label_index is None or parse_label(row[label_index]):
                    query_rows.append(parse_embedding(row[query_index]))
            elif query_source == "fixture-negative" and len(query_rows) < query_pool:
                if label_index is not None and not parse_label(row[label_index]):
                    query_rows.append(parse_embedding(row[query_index]))
            query_ready = query_source in ("exact", "miss-random") or len(query_rows) >= query_pool
            if len(cache_rows) >= entries and query_ready:
                break
    if not cache_rows or not query_rows:
        if query_source not in ("exact", "miss-random"):
            raise ValueError(f"{path} did not contain enough vector rows for {query_source}")
    cache_vectors = np.array(cache_rows, dtype=np.float32)
    if query_source == "exact":
        query_vectors = np.array(
            [cache_vectors[index % len(cache_vectors)] for index in range(query_pool)],
            dtype=np.float32,
        )
    elif query_source == "miss-random":
        query_vectors = normalised_vectors(query_pool, cache_vectors.shape[1], seed ^ 0xBAD5EED)
    else:
        query_vectors = np.array(query_rows, dtype=np.float32)
    return VectorFixture(
        cache_vectors=cache_vectors,
        query_vectors=query_vectors,
    )


def parse_label(value: str) -> bool:
    lowered = value.strip().lower()
    if lowered in ("1", "true", "yes", "positive", "pos", "match"):
        return True
    if lowered in ("0", "false", "no", "negative", "neg", "miss"):
        return False
    raise ValueError(f"invalid label {value!r}")


def parse_embedding(value: str) -> list[float]:
    return [
        float(part)
        for part in value.replace("|", ";").replace(" ", ";").split(";")
        if part
    ]


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    return float(np.percentile(np.array(values, dtype=np.float64), pct))


def parse_cpuset(raw: str) -> set[int]:
    cpus: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start_raw, end_raw = part.split("-", 1)
            start = int(start_raw)
            end = int(end_raw)
            if end < start:
                raise ValueError(f"invalid cpuset range {part!r}")
            cpus.update(range(start, end + 1))
        else:
            cpus.add(int(part))
    if not cpus:
        raise ValueError("cpuset must contain at least one CPU")
    return cpus


def apply_process_cpuset(raw: str) -> None:
    if not raw:
        return
    if not hasattr(os, "sched_setaffinity"):
        print(f"process cpuset {raw!r} requested but os.sched_setaffinity is unavailable", flush=True)
        return
    os.sched_setaffinity(0, parse_cpuset(raw))


def process_cpu_seconds() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return float(usage.ru_utime + usage.ru_stime)


def external_cpu_seconds(pid: int) -> float:
    total = external_pid_cpu_seconds(pid)
    for child in external_descendant_pids(pid):
        total += external_pid_cpu_seconds(child)
    return total


def external_pid_cpu_seconds(pid: int) -> float:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return 0.0
    close = stat.rfind(")")
    if close < 0:
        return 0.0
    fields = stat[close + 1 :].split()
    try:
        utime = int(fields[11])
        stime = int(fields[12])
        hz = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
    except (IndexError, KeyError, OSError, ValueError):
        return 0.0
    if hz <= 0:
        return 0.0
    return float(utime + stime) / float(hz)


def external_descendant_pids(pid: int) -> list[int]:
    descendants: list[int] = []
    stack = [pid]
    while stack:
        current = stack.pop()
        try:
            raw = Path(f"/proc/{current}/task/{current}/children").read_text()
        except OSError:
            continue
        for part in raw.split():
            try:
                child = int(part)
            except ValueError:
                continue
            descendants.append(child)
            stack.append(child)
    return descendants


def parse_external_pids(raw: str) -> list[int]:
    if not raw:
        return []
    pids: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if part:
            pids.append(int(part))
    return pids


def cpu_snapshot(args: argparse.Namespace) -> CpuSnapshot:
    return CpuSnapshot(
        process_seconds=process_cpu_seconds(),
        external_seconds=sum(external_cpu_seconds(pid) for pid in args.external_pids_list),
    )


def summarize(
    scenario: str,
    adapter: str,
    workers: int,
    entries: int,
    dims: int,
    query_pool: int,
    elapsed: float,
    queries: int,
    hits: int,
    latencies_ms: list[float],
    cpu_start: CpuSnapshot,
    cpu_end: CpuSnapshot,
    process_cpuset: str,
    external_pids: str,
) -> LoadResult:
    process_cpu = max(0.0, cpu_end.process_seconds - cpu_start.process_seconds)
    external_cpu = max(0.0, cpu_end.external_seconds - cpu_start.external_seconds)
    total_cpu = process_cpu + external_cpu
    ops_per_sec = queries / elapsed if elapsed > 0 else 0.0
    process_vcpu = process_cpu / elapsed if elapsed > 0 else 0.0
    external_vcpu = external_cpu / elapsed if elapsed > 0 else 0.0
    total_vcpu = total_cpu / elapsed if elapsed > 0 else 0.0
    if external_pids.strip():
        sut_cpu = external_cpu
        sut_vcpu = external_vcpu
        client_cpu = process_cpu
        client_vcpu = process_vcpu
    else:
        sut_cpu = process_cpu
        sut_vcpu = process_vcpu
        client_cpu = 0.0
        client_vcpu = 0.0
    ops_per_sut_cpu = ops_per_sec / sut_vcpu if sut_vcpu > 0 else 0.0
    ops_per_total_cpu = ops_per_sec / total_vcpu if total_vcpu > 0 else 0.0
    return LoadResult(
        scenario=scenario,
        adapter=adapter,
        workers=workers,
        entries=entries,
        dims=dims,
        query_pool=query_pool,
        seconds=elapsed,
        queries=queries,
        hits=hits,
        ops_per_sec=ops_per_sec,
        ops_per_cpu=ops_per_sut_cpu,
        ops_per_sut_cpu=ops_per_sut_cpu,
        ops_per_total_cpu=ops_per_total_cpu,
        p50_ms=percentile(latencies_ms, 50),
        p95_ms=percentile(latencies_ms, 95),
        p99_ms=percentile(latencies_ms, 99),
        process_cpu_seconds=process_cpu,
        process_vcpu=process_vcpu,
        external_cpu_seconds=external_cpu,
        external_vcpu=external_vcpu,
        total_cpu_seconds=total_cpu,
        total_vcpu=total_vcpu,
        sut_cpu_seconds=sut_cpu,
        sut_vcpu=sut_vcpu,
        client_cpu_seconds=client_cpu,
        client_vcpu=client_vcpu,
        process_cpuset=process_cpuset,
        external_pids=external_pids,
    )


def min_score_from_distance(threshold: float) -> float:
    return 1.0 - threshold


def l2_squared_threshold_from_cosine_distance(threshold: float) -> float:
    return 2.0 * threshold


def run_threaded_vector_queries(
    adapter: str,
    args: argparse.Namespace,
    fixture: VectorFixture,
    query_one,
    close_worker=None,
) -> LoadResult:
    query_vectors = fixture.query_vectors
    end = time.perf_counter() + args.seconds
    latencies_ms: list[float] = []
    lat_lock = threading.Lock()
    counts = [0 for _ in range(args.workers)]
    hits = [0 for _ in range(args.workers)]

    def worker(worker_id: int) -> None:
        worker_state = None
        if close_worker is not None:
            worker_state = close_worker("open")
        index = worker_id % len(query_vectors)
        local_latencies: list[float] = []
        local_hits = 0
        local_count = 0
        try:
            while time.perf_counter() < end:
                if args.unique_queries and index >= len(query_vectors):
                    break
                query = query_vectors[index]
                if args.unique_queries:
                    index += args.workers
                else:
                    index = (index + 1) % len(query_vectors)
                start = time.perf_counter()
                hit = query_one(query, worker_state)
                local_latencies.append((time.perf_counter() - start) * 1000)
                local_count += 1
                if hit:
                    local_hits += 1
        finally:
            if close_worker is not None:
                close_worker("close", worker_state)
        counts[worker_id] = local_count
        hits[worker_id] = local_hits
        with lat_lock:
            latencies_ms.extend(local_latencies)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(args.workers)]
    cpu_start = cpu_snapshot(args)
    start = time.perf_counter()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed = time.perf_counter() - start
    cpu_end = cpu_snapshot(args)

    return summarize(
        args.scenario,
        adapter,
        args.workers,
        args.entries,
        args.dims,
        args.query_pool,
        elapsed,
        sum(counts),
        sum(hits),
        latencies_ms,
        cpu_start,
        cpu_end,
        args.process_cpuset,
        args.external_pids,
    )


async def run_betterdb(args: argparse.Namespace, fixture: VectorFixture) -> LoadResult:
    import valkey.asyncio as valkey
    from betterdb_semantic_cache import SemanticCache
    from betterdb_semantic_cache.types import (
        AnalyticsOptions,
        ConfigRefreshOptions,
        DiscoveryOptions,
        SemanticCacheOptions,
    )

    cache_name = f"bench:betterdb-load:{uuid.uuid4().hex[:8]}"
    client = valkey.Valkey.from_url(args.redis_url, decode_responses=False)

    query_prompts = [f"query:{i}" for i in range(args.query_pool)]
    query_vectors = {
        prompt: fixture.query_vectors[index % len(fixture.query_vectors)].tolist()
        for index, prompt in enumerate(query_prompts)
    }

    async def embed(text: str) -> list[float]:
        if text == "probe":
            return fixture.cache_vectors[0].tolist()
        if text.startswith("query:"):
            return query_vectors[text]
        if text.startswith("entry:"):
            return fixture.cache_vectors[int(text.split(":", 1)[1]) % len(fixture.cache_vectors)].tolist()
        return fixture.cache_vectors[0].tolist()

    opts = SemanticCacheOptions(
        client=client,
        embed_fn=embed,
        name=cache_name,
        default_threshold=args.threshold,
        analytics=AnalyticsOptions(disabled=True),
        discovery=DiscoveryOptions(enabled=False),
        config_refresh=ConfigRefreshOptions(enabled=False),
    )
    cache = SemanticCache(opts)
    await cache.initialize()

    for i in range(args.entries):
        if args.progress_every and i > 0 and i % args.progress_every == 0:
            print(f"betterdb stored {i}/{args.entries}", flush=True)
        await cache.store(f"entry:{i}", f"value:{i}")

    for prompt in query_prompts[: args.warmup_queries]:
        await cache.check(prompt)

    end = time.perf_counter() + args.seconds
    lock = asyncio.Lock()
    latencies_ms: list[float] = []
    counts = [0 for _ in range(args.workers)]
    hits = [0 for _ in range(args.workers)]

    async def worker(worker_id: int) -> None:
        index = worker_id % len(query_prompts)
        local_latencies: list[float] = []
        local_hits = 0
        local_count = 0
        while time.perf_counter() < end:
            if args.unique_queries and index >= len(query_prompts):
                break
            prompt = query_prompts[index]
            if args.unique_queries:
                index += args.workers
            else:
                index = (index + 1) % len(query_prompts)
            start = time.perf_counter()
            result = await cache.check(prompt)
            local_latencies.append((time.perf_counter() - start) * 1000)
            local_count += 1
            if result.hit:
                local_hits += 1
        counts[worker_id] = local_count
        hits[worker_id] = local_hits
        async with lock:
            latencies_ms.extend(local_latencies)

    cpu_start = cpu_snapshot(args)
    start = time.perf_counter()
    await asyncio.gather(*(worker(i) for i in range(args.workers)))
    elapsed = time.perf_counter() - start
    cpu_end = cpu_snapshot(args)

    await cache.flush()
    await cache.shutdown()
    await client.aclose()

    return summarize(
        args.scenario,
        "betterdb",
        args.workers,
        args.entries,
        args.dims,
        args.query_pool,
        elapsed,
        sum(counts),
        sum(hits),
        latencies_ms,
        cpu_start,
        cpu_end,
        args.process_cpuset,
        args.external_pids,
    )


def run_redis(args: argparse.Namespace, fixture: VectorFixture, algorithm: str = "FLAT") -> LoadResult:
    import redis

    algorithm = algorithm.upper()
    index_name = f"bench_redis_{algorithm.lower()}_{uuid.uuid4().hex[:8]}"
    prefix = f"{index_name}:"
    client = redis.Redis.from_url(args.redis_url)
    try:
        client.execute_command("FT.DROPINDEX", index_name, "DD")
    except Exception:
        pass
    vector_args: list[object]
    if algorithm == "HNSW":
        vector_args = [
            "HNSW",
            "10",
            "TYPE",
            "FLOAT32",
            "DIM",
            fixture.cache_vectors.shape[1],
            "DISTANCE_METRIC",
            "COSINE",
            "M",
            args.hnsw_m,
            "EF_CONSTRUCTION",
            args.hnsw_ef_construction,
        ]
    else:
        vector_args = [
            "FLAT",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            fixture.cache_vectors.shape[1],
            "DISTANCE_METRIC",
            "COSINE",
        ]
    client.execute_command(
        "FT.CREATE",
        index_name,
        "ON",
        "HASH",
        "PREFIX",
        "1",
        prefix,
        "SCHEMA",
        "embedding",
        "VECTOR",
        *vector_args,
    )

    pipe = client.pipeline(transaction=False)
    for i in range(args.entries):
        if args.progress_every and i > 0 and i % args.progress_every == 0:
            print(f"redis queued {i}/{args.entries}", flush=True)
        pipe.hset(
            f"{prefix}{i}",
            mapping={"embedding": fixture.cache_vectors[i % len(fixture.cache_vectors)].tobytes()},
        )
        if (i + 1) % args.pipeline == 0:
            pipe.execute()
    pipe.execute()

    query_vectors = [
        fixture.query_vectors[index % len(fixture.query_vectors)].tobytes()
        for index in range(args.query_pool)
    ]
    search_args = (
        "FT.SEARCH",
        index_name,
        "*=>[KNN 1 @embedding $vec AS dist]",
        "PARAMS",
        "2",
        "vec",
        None,
        "SORTBY",
        "dist",
        "RETURN",
        "1",
        "dist",
        "LIMIT",
        "0",
        "1",
        "DIALECT",
        "2",
    )

    for query in query_vectors[: args.warmup_queries]:
        command = list(search_args)
        command[6] = query
        client.execute_command(*command)

    end = time.perf_counter() + args.seconds
    latencies_ms: list[float] = []
    lat_lock = threading.Lock()
    counts = [0 for _ in range(args.workers)]
    hits = [0 for _ in range(args.workers)]

    def worker(worker_id: int) -> None:
        worker_client = redis.Redis.from_url(args.redis_url)
        index = worker_id % len(query_vectors)
        local_latencies: list[float] = []
        local_hits = 0
        local_count = 0
        while time.perf_counter() < end:
            if args.unique_queries and index >= len(query_vectors):
                break
            command = list(search_args)
            command[6] = query_vectors[index]
            if args.unique_queries:
                index += args.workers
            else:
                index = (index + 1) % len(query_vectors)
            start = time.perf_counter()
            result = worker_client.execute_command(*command)
            local_latencies.append((time.perf_counter() - start) * 1000)
            local_count += 1
            if redis_result_hit(result, args.threshold):
                local_hits += 1
        worker_client.close()
        counts[worker_id] = local_count
        hits[worker_id] = local_hits
        with lat_lock:
            latencies_ms.extend(local_latencies)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(args.workers)]
    cpu_start = cpu_snapshot(args)
    start = time.perf_counter()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed = time.perf_counter() - start
    cpu_end = cpu_snapshot(args)

    try:
        client.execute_command("FT.DROPINDEX", index_name, "DD")
    finally:
        client.close()

    return summarize(
        args.scenario,
        f"redis-vector-{algorithm.lower()}",
        args.workers,
        args.entries,
        args.dims,
        args.query_pool,
        elapsed,
        sum(counts),
        sum(hits),
        latencies_ms,
        cpu_start,
        cpu_end,
        args.process_cpuset,
        args.external_pids,
    )


def run_redisvl(args: argparse.Namespace, fixture: VectorFixture) -> LoadResult:
    from redisvl.extensions.cache.llm import SemanticCache
    from redisvl.utils.vectorize.base import BaseVectorizer

    class PrecomputedVectorizer(BaseVectorizer):
        @property
        def type(self) -> str:
            return "precomputed"

        def _embed(self, content, **kwargs):
            raise RuntimeError("redisvl benchmark should pass precomputed vectors")

        async def _aembed(self, content, **kwargs):
            raise RuntimeError("redisvl benchmark should pass precomputed vectors")

    cache = SemanticCache(
        name=f"bench_redisvl_{uuid.uuid4().hex[:8]}",
        redis_url=args.redis_url,
        distance_threshold=args.threshold,
        vectorizer=PrecomputedVectorizer(
            model="precomputed",
            dims=int(fixture.cache_vectors.shape[1]),
        ),
        overwrite=True,
    )

    for i in range(args.entries):
        if args.progress_every and i > 0 and i % args.progress_every == 0:
            print(f"redisvl stored {i}/{args.entries}", flush=True)
        cache.store(
            f"entry:{i}",
            f"value:{i}",
            vector=fixture.cache_vectors[i % len(fixture.cache_vectors)].tolist(),
        )

    for query in fixture.query_vectors[: args.warmup_queries]:
        cache.check(vector=query.tolist(), num_results=1)

    try:
        return run_threaded_vector_queries(
            "redisvl-semantic-cache",
            args,
            fixture,
            lambda query, _state: bool(cache.check(vector=query.tolist(), num_results=1)),
        )
    finally:
        cache.clear()


def run_langchain_redis(args: argparse.Namespace, fixture: VectorFixture) -> LoadResult:
    from langchain_core.embeddings import Embeddings
    from langchain_core.outputs import Generation
    from langchain_redis import RedisSemanticCache

    entry_prompts = [f"entry:{i}" for i in range(args.entries)]
    query_prompts = [f"query:{i}" for i in range(args.query_pool)]
    vectors: dict[str, list[float]] = {}
    for i, prompt in enumerate(entry_prompts):
        vectors[prompt] = fixture.cache_vectors[i % len(fixture.cache_vectors)].tolist()
    for i, prompt in enumerate(query_prompts):
        vectors[prompt] = fixture.query_vectors[i % len(fixture.query_vectors)].tolist()

    class PrecomputedEmbeddings(Embeddings):
        def embed_query(self, text: str) -> list[float]:
            return vectors.get(text, fixture.cache_vectors[0].tolist())

        def embed_documents(self, texts: list[str]) -> list[list[float]]:
            fallback = fixture.cache_vectors[0].tolist()
            return [vectors.get(text, fallback) for text in texts]

    name = f"bench_lc_redis_{uuid.uuid4().hex[:8]}"
    cache = RedisSemanticCache(
        embeddings=PrecomputedEmbeddings(),
        redis_url=args.redis_url,
        distance_threshold=args.threshold,
        name=name,
        prefix=name,
    )
    try:
        cache.clear()
    except Exception:
        pass

    for i, prompt in enumerate(entry_prompts):
        if args.progress_every and i > 0 and i % args.progress_every == 0:
            print(f"langchain-redis stored {i}/{args.entries}", flush=True)
        cache.update(prompt, args.llm_string, [Generation(text=f"value:{i}")])

    for prompt in query_prompts[: args.warmup_queries]:
        cache.lookup(prompt, args.llm_string)

    end = time.perf_counter() + args.seconds
    latencies_ms: list[float] = []
    lat_lock = threading.Lock()
    counts = [0 for _ in range(args.workers)]
    hits = [0 for _ in range(args.workers)]

    def worker(worker_id: int) -> None:
        index = worker_id % len(query_prompts)
        local_latencies: list[float] = []
        local_hits = 0
        local_count = 0
        while time.perf_counter() < end:
            if args.unique_queries and index >= len(query_prompts):
                break
            prompt = query_prompts[index]
            if args.unique_queries:
                index += args.workers
            else:
                index = (index + 1) % len(query_prompts)
            start = time.perf_counter()
            result = cache.lookup(prompt, args.llm_string)
            local_latencies.append((time.perf_counter() - start) * 1000)
            local_count += 1
            if result:
                local_hits += 1
        counts[worker_id] = local_count
        hits[worker_id] = local_hits
        with lat_lock:
            latencies_ms.extend(local_latencies)

    try:
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(args.workers)]
        cpu_start = cpu_snapshot(args)
        start = time.perf_counter()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        elapsed = time.perf_counter() - start
        cpu_end = cpu_snapshot(args)
        return summarize(
            args.scenario,
            "langchain-redis-semantic-cache",
            args.workers,
            args.entries,
            args.dims,
            args.query_pool,
            elapsed,
            sum(counts),
            sum(hits),
            latencies_ms,
            cpu_start,
            cpu_end,
            args.process_cpuset,
            args.external_pids,
        )
    finally:
        cache.clear()


def run_gptcache(args: argparse.Namespace, fixture: VectorFixture) -> LoadResult:
    from gptcache import cache
    from gptcache.adapter.api import get, put
    from gptcache.config import Config
    from gptcache.manager import CacheBase, VectorBase, get_data_manager
    from gptcache.processor.pre import get_prompt
    from gptcache.similarity_evaluation.distance import SearchDistanceEvaluation

    entry_prompts = [f"entry:{i}" for i in range(args.entries)]
    query_prompts = [f"query:{i}" for i in range(args.query_pool)]
    vectors: dict[str, np.ndarray] = {}
    for i, prompt in enumerate(entry_prompts):
        vectors[prompt] = fixture.cache_vectors[i % len(fixture.cache_vectors)]
    for i, prompt in enumerate(query_prompts):
        vectors[prompt] = fixture.query_vectors[i % len(fixture.query_vectors)]

    def embedding_func(data, **kwargs):
        return np.asarray(vectors[data], dtype=np.float32)

    data_dir = Path(tempfile.mkdtemp(prefix="gptcache_bench_"))
    try:
        data_manager = get_data_manager(
            CacheBase("sqlite", sql_url=f"sqlite:///{data_dir / 'cache.db'}"),
            VectorBase(
                "faiss",
                dimension=int(fixture.cache_vectors.shape[1]),
                top_k=1,
                index_path=str(data_dir / "faiss.index"),
            ),
            max_size=args.entries,
        )
        cache.init(
            pre_embedding_func=get_prompt,
            embedding_func=embedding_func,
            data_manager=data_manager,
            similarity_evaluation=SearchDistanceEvaluation(max_distance=2.0),
            config=Config(similarity_threshold=min_score_from_distance(args.threshold)),
        )

        for i, prompt in enumerate(entry_prompts):
            if args.progress_every and i > 0 and i % args.progress_every == 0:
                print(f"gptcache stored {i}/{args.entries}", flush=True)
            put(prompt, f"value:{i}")

        for prompt in query_prompts[: args.warmup_queries]:
            get(prompt)

        end = time.perf_counter() + args.seconds
        latencies_ms: list[float] = []
        lat_lock = threading.Lock()
        errors: list[BaseException] = []
        error_lock = threading.Lock()
        counts = [0 for _ in range(args.workers)]
        hits = [0 for _ in range(args.workers)]

        def worker(worker_id: int) -> None:
            index = worker_id % len(query_prompts)
            local_latencies: list[float] = []
            local_hits = 0
            local_count = 0
            try:
                while time.perf_counter() < end:
                    if args.unique_queries and index >= len(query_prompts):
                        break
                    prompt = query_prompts[index]
                    if args.unique_queries:
                        index += args.workers
                    else:
                        index = (index + 1) % len(query_prompts)
                    start = time.perf_counter()
                    result = get(prompt)
                    local_latencies.append((time.perf_counter() - start) * 1000)
                    local_count += 1
                    if result is not None:
                        local_hits += 1
            except BaseException as exc:
                with error_lock:
                    errors.append(exc)
            counts[worker_id] = local_count
            hits[worker_id] = local_hits
            with lat_lock:
                latencies_ms.extend(local_latencies)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(args.workers)]
        cpu_start = cpu_snapshot(args)
        start = time.perf_counter()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if errors:
            raise RuntimeError(f"gptcache worker failed: {errors[0]!r}")
        elapsed = time.perf_counter() - start
        cpu_end = cpu_snapshot(args)
        return summarize(
            args.scenario,
            "gptcache-faiss",
            args.workers,
            args.entries,
            args.dims,
            args.query_pool,
            elapsed,
            sum(counts),
            sum(hits),
            latencies_ms,
            cpu_start,
            cpu_end,
            args.process_cpuset,
            args.external_pids,
        )
    finally:
        try:
            cache.data_manager.close()
        except Exception:
            pass
        shutil.rmtree(data_dir, ignore_errors=True)


def run_faiss(args: argparse.Namespace, fixture: VectorFixture, algorithm: str) -> LoadResult:
    import faiss

    vectors = np.ascontiguousarray(fixture.cache_vectors[: args.entries], dtype=np.float32)
    dims = int(vectors.shape[1])
    algorithm = algorithm.lower()
    if algorithm == "hnsw":
        index = faiss.IndexHNSWFlat(dims, args.hnsw_m, faiss.METRIC_INNER_PRODUCT)
        index.hnsw.efConstruction = args.hnsw_ef_construction
        index.hnsw.efSearch = args.hnsw_ef_search
    else:
        index = faiss.IndexFlatIP(dims)
    index.add(vectors)
    min_score = min_score_from_distance(args.threshold)

    def query_one(query: np.ndarray, _state) -> bool:
        scores, _ids = index.search(np.ascontiguousarray(query.reshape(1, -1)), 1)
        return bool(scores.size and scores[0][0] >= min_score)

    return run_threaded_vector_queries(f"faiss-{algorithm}", args, fixture, query_one)


def run_hnswlib(args: argparse.Namespace, fixture: VectorFixture) -> LoadResult:
    import hnswlib

    vectors = np.ascontiguousarray(fixture.cache_vectors[: args.entries], dtype=np.float32)
    index = hnswlib.Index(space="cosine", dim=int(vectors.shape[1]))
    index.init_index(
        max_elements=args.entries,
        ef_construction=args.hnsw_ef_construction,
        M=args.hnsw_m,
    )
    index.add_items(vectors, np.arange(args.entries))
    index.set_ef(args.hnsw_ef_search)

    def query_one(query: np.ndarray, _state) -> bool:
        _labels, distances = index.knn_query(query, k=1)
        return bool(distances.size and distances[0][0] <= args.threshold)

    return run_threaded_vector_queries("hnswlib-cosine", args, fixture, query_one)


def run_qdrant(args: argparse.Namespace, fixture: VectorFixture) -> LoadResult:
    from qdrant_client import QdrantClient
    from qdrant_client.models import Distance, VectorParams

    collection = f"bench_qdrant_{uuid.uuid4().hex[:8]}"
    client = QdrantClient(url=args.qdrant_url, timeout=args.qdrant_timeout)
    try:
        try:
            client.delete_collection(collection)
        except Exception:
            pass
        client.create_collection(
            collection_name=collection,
            vectors_config=VectorParams(
                size=int(fixture.cache_vectors.shape[1]),
                distance=Distance.COSINE,
            ),
        )
        client.upload_collection(
            collection_name=collection,
            vectors=fixture.cache_vectors[: args.entries],
            ids=list(range(args.entries)),
            batch_size=args.pipeline,
            parallel=args.qdrant_parallel,
            max_retries=3,
            wait=True,
        )
        min_score = min_score_from_distance(args.threshold)

        def worker_client(action: str, state=None):
            if action == "open":
                return QdrantClient(url=args.qdrant_url, timeout=args.qdrant_timeout)
            if state is not None:
                state.close()
            return None

        def query_one(query: np.ndarray, state) -> bool:
            response = state.query_points(
                collection_name=collection,
                query=query,
                limit=1,
                with_payload=False,
                with_vectors=False,
                score_threshold=min_score,
            )
            return bool(response.points)

        return run_threaded_vector_queries("qdrant-cosine", args, fixture, query_one, worker_client)
    finally:
        try:
            client.delete_collection(collection)
        finally:
            client.close()


def redis_result_hit(result: object, threshold: float) -> bool:
    if not isinstance(result, list) or not result:
        return False
    try:
        if int(result[0]) <= 0:
            return False
    except (TypeError, ValueError):
        return False
    distance = redis_result_distance(result)
    return distance is not None and distance <= threshold


def redis_result_distance(value: object) -> float | None:
    if isinstance(value, bytes):
        try:
            return float(value.decode())
        except ValueError:
            return None
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    if isinstance(value, list):
        for index, item in enumerate(value):
            if item in (b"dist", "dist") and index + 1 < len(value):
                return redis_result_distance(value[index + 1])
            nested = redis_result_distance(item)
            if nested is not None:
                return nested
    return None


def write_csv(path: Path, rows: list[LoadResult]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "scenario",
                "adapter",
                "workers",
                "entries",
                "dims",
                "query_pool",
                "seconds",
                "queries",
                "hits",
                "ops_per_sec",
                "ops_per_cpu",
                "p50_ms",
                "p95_ms",
                "p99_ms",
                "process_cpu_seconds",
                "process_vcpu",
                "external_cpu_seconds",
                "external_vcpu",
                "total_cpu_seconds",
                "total_vcpu",
                "sut_cpu_seconds",
                "sut_vcpu",
                "client_cpu_seconds",
                "client_vcpu",
                "ops_per_sut_cpu",
                "ops_per_total_cpu",
                "process_cpuset",
                "external_pids",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.scenario,
                    row.adapter,
                    row.workers,
                    row.entries,
                    row.dims,
                    row.query_pool,
                    f"{row.seconds:.6f}",
                    row.queries,
                    row.hits,
                    f"{row.ops_per_sec:.6f}",
                    f"{row.ops_per_cpu:.6f}",
                    f"{row.p50_ms:.6f}",
                    f"{row.p95_ms:.6f}",
                    f"{row.p99_ms:.6f}",
                    f"{row.process_cpu_seconds:.6f}",
                    f"{row.process_vcpu:.6f}",
                    f"{row.external_cpu_seconds:.6f}",
                    f"{row.external_vcpu:.6f}",
                    f"{row.total_cpu_seconds:.6f}",
                    f"{row.total_vcpu:.6f}",
                    f"{row.sut_cpu_seconds:.6f}",
                    f"{row.sut_vcpu:.6f}",
                    f"{row.client_cpu_seconds:.6f}",
                    f"{row.client_vcpu:.6f}",
                    f"{row.ops_per_sut_cpu:.6f}",
                    f"{row.ops_per_total_cpu:.6f}",
                    row.process_cpuset,
                    row.external_pids,
                ]
            )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6384")
    parser.add_argument("--adapters", default="betterdb,redis")
    parser.add_argument("--scenario", default="fixture")
    parser.add_argument("--entries", type=int, default=100_000)
    parser.add_argument("--dims", type=int, default=384)
    parser.add_argument("--pairs-csv", type=Path)
    parser.add_argument(
        "--query-source",
        choices=["fixture", "fixture-positive", "fixture-negative", "exact", "miss-random"],
        default="fixture",
    )
    parser.add_argument("--query-pool", type=int, default=64)
    parser.add_argument("--warmup-queries", type=int, default=64)
    parser.add_argument("--unique-queries", action="store_true")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--seconds", type=float, default=10.0)
    parser.add_argument("--threshold", type=float, default=0.35)
    parser.add_argument("--seed", type=int, default=0x5EED)
    parser.add_argument("--pipeline", type=int, default=1000)
    parser.add_argument("--progress-every", type=int, default=10_000)
    parser.add_argument("--hnsw-m", type=int, default=16)
    parser.add_argument("--hnsw-ef-construction", type=int, default=200)
    parser.add_argument("--hnsw-ef-search", type=int, default=64)
    parser.add_argument("--qdrant-url", default="http://127.0.0.1:6333")
    parser.add_argument("--qdrant-timeout", type=int, default=60)
    parser.add_argument("--qdrant-parallel", type=int, default=1)
    parser.add_argument("--llm-string", default="semantic-head-to-head")
    parser.add_argument(
        "--process-cpuset",
        default="",
        help="Optional Linux CPU affinity set for this load process, e.g. 16-31.",
    )
    parser.add_argument(
        "--external-pids",
        default="",
        help="Comma-separated external server PIDs whose CPU should be sampled during the measured window.",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.external_pids_list = parse_external_pids(args.external_pids)
    apply_process_cpuset(args.process_cpuset)

    if args.pairs_csv:
        fixture = load_pairs_csv(
            args.pairs_csv,
            args.entries,
            args.query_pool,
            args.query_source,
            args.seed,
        )
        args.dims = int(fixture.cache_vectors.shape[1])
    else:
        vectors = normalised_vectors(args.entries, args.dims, args.seed)
        query_indices = [(i * args.entries) // args.query_pool for i in range(args.query_pool)]
        if args.query_source == "miss-random":
            query_vectors = normalised_vectors(args.query_pool, args.dims, args.seed ^ 0xBAD5EED)
        else:
            query_vectors = np.array([vectors[index] for index in query_indices], dtype=np.float32)
        fixture = VectorFixture(
            cache_vectors=vectors,
            query_vectors=query_vectors,
        )
    rows: list[LoadResult] = []
    adapters = {name.strip() for name in args.adapters.split(",") if name.strip()}
    if "all" in adapters:
        adapters.update(
            {
                "betterdb",
                "redis-flat",
                "redis-hnsw",
                "redisvl",
                "langchain-redis",
                "gptcache",
                "faiss-flat",
                "faiss-hnsw",
                "hnswlib",
                "qdrant",
            }
        )
    if "betterdb" in adapters:
        rows.append(asyncio.run(run_betterdb(args, fixture)))
    if "redis" in adapters or "redis-flat" in adapters:
        rows.append(run_redis(args, fixture, "FLAT"))
    if "redis-hnsw" in adapters:
        rows.append(run_redis(args, fixture, "HNSW"))
    if "redisvl" in adapters:
        rows.append(run_redisvl(args, fixture))
    if "langchain-redis" in adapters or "langchain" in adapters:
        rows.append(run_langchain_redis(args, fixture))
    if "gptcache" in adapters:
        rows.append(run_gptcache(args, fixture))
    if "faiss-flat" in adapters or "faiss" in adapters:
        rows.append(run_faiss(args, fixture, "flat"))
    if "faiss-hnsw" in adapters:
        rows.append(run_faiss(args, fixture, "hnsw"))
    if "hnswlib" in adapters:
        rows.append(run_hnswlib(args, fixture))
    if "qdrant" in adapters:
        rows.append(run_qdrant(args, fixture))

    write_csv(args.output, rows)
    for row in rows:
        print(
            f"{row.scenario}/{row.adapter}: ops/sec={row.ops_per_sec:.0f} "
            f"ops/sut-cpu={row.ops_per_sut_cpu:.0f} "
            f"p50={row.p50_ms:.4f}ms p95={row.p95_ms:.4f}ms p99={row.p99_ms:.4f}ms "
            f"hits={row.hits}/{row.queries} total_vcpu={row.total_vcpu:.2f} "
            f"sut_vcpu={row.sut_vcpu:.2f} client_vcpu={row.client_vcpu:.2f}",
            flush=True,
        )


if __name__ == "__main__":
    main()
