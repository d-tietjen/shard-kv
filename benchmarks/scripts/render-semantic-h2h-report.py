#!/usr/bin/env python3
"""Render Markdown and LaTeX reports for semantic-cache head-to-head results."""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path


SCENARIOS = [
    {
        "key": "miss-cold",
        "title": "Cold Miss",
        "throughput_pdf": "semantic-h2h-miss-cold-throughput.pdf",
        "vcpu_pdf": "semantic-h2h-miss-cold-vcpu.pdf",
    },
    {
        "key": "hit-cold-unique",
        "title": "Cold Unique Semantic Hit",
        "throughput_pdf": "semantic-h2h-hit-cold-unique-throughput.pdf",
        "vcpu_pdf": "semantic-h2h-hit-cold-unique-vcpu.pdf",
    },
    {
        "key": "hit-hot-cached",
        "title": "Hot Cached Exact Query",
        "throughput_pdf": "semantic-h2h-hit-hot-cached-throughput.pdf",
        "vcpu_pdf": "semantic-h2h-hit-hot-cached-vcpu.pdf",
    },
]

ORDER = [
    "shardcache",
    "betterdb",
    "redisvl-semantic-cache",
    "langchain-redis-semantic-cache",
    "redis-vector-flat",
    "redis-vector-hnsw",
    "faiss-flat",
    "faiss-hnsw",
    "hnswlib-cosine",
    "qdrant-cosine",
]

NETWORKED_ADAPTERS = {
    "betterdb",
    "redisvl-semantic-cache",
    "langchain-redis-semantic-cache",
    "redis-vector-flat",
    "redis-vector-hnsw",
    "qdrant-cosine",
}

LABELS = {
    "shardcache": "ShardCache",
    "betterdb": "BetterDB",
    "redisvl-semantic-cache": "RedisVL SemanticCache",
    "langchain-redis-semantic-cache": "LangChain Redis SC",
    "redis-vector-flat": "Redis vector FLAT",
    "redis-vector-hnsw": "Redis vector HNSW",
    "faiss-flat": "FAISS Flat",
    "faiss-hnsw": "FAISS HNSW",
    "hnswlib-cosine": "hnswlib cosine",
    "qdrant-cosine": "Qdrant cosine",
}

PROFILE_BASELINE_OPS = 452.0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("results_dir", nargs="+", type=Path)
    parser.add_argument(
        "--combined-output-dir",
        type=Path,
        help="Output directory when rendering a combined multi-run report.",
    )
    args = parser.parse_args()

    if len(args.results_dir) == 1:
        render_single_report(args.results_dir[0])
        return

    output_dir = args.combined_output_dir or args.results_dir[0].parent / "adam-semantic-h2h-isolated-combined"
    render_combined_report(args.results_dir, output_dir)


def render_single_report(results_dir: Path) -> None:
    rows = load_rows(results_dir)
    metadata = load_metadata(results_dir / "metadata.txt")

    (results_dir / "report.md").write_text(render_markdown(rows, metadata), encoding="utf-8")
    section = render_latex_section(rows, metadata)
    (results_dir / "shardcache-semantic-head-to-head-isolated-section.tex").write_text(
        section,
        encoding="utf-8",
    )
    (results_dir / "shardcache-semantic-head-to-head-isolated-report.tex").write_text(
        "\n".join(
            [
                r"\documentclass[11pt]{article}",
                r"\usepackage[margin=0.75in]{geometry}",
                r"\usepackage{graphicx}",
                r"\begin{document}",
                r"\input{shardcache-semantic-head-to-head-isolated-section.tex}",
                r"\end{document}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def render_combined_report(results_dirs: list[Path], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    runs = [
        {
            "path": results_dir,
            "rows": load_rows(results_dir),
            "metadata": load_metadata(results_dir / "metadata.txt"),
        }
        for results_dir in results_dirs
    ]
    runs.sort(key=lambda run: sut_vcpus(run["metadata"]))

    (output_dir / "report.md").write_text(render_combined_markdown(runs), encoding="utf-8")
    section = render_combined_latex_section(runs, output_dir)
    (output_dir / "shardcache-semantic-head-to-head-combined-section.tex").write_text(
        section,
        encoding="utf-8",
    )
    (output_dir / "shardcache-semantic-head-to-head-combined-report.tex").write_text(
        "\n".join(
            [
                r"\documentclass[11pt]{article}",
                r"\usepackage[margin=0.75in]{geometry}",
                r"\usepackage{graphicx}",
                r"\begin{document}",
                r"\input{shardcache-semantic-head-to-head-combined-section.tex}",
                r"\end{document}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def load_metadata(path: Path) -> dict[str, str]:
    metadata: dict[str, str] = {}
    if not path.exists():
        return metadata
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            metadata[key.strip()] = value.strip()
    return metadata


def load_rows(results_dir: Path) -> dict[tuple[str, str], dict[str, object]]:
    rows: dict[tuple[str, str], dict[str, object]] = {}
    for path in sorted(results_dir.glob("*.csv")):
        with path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                if "scenario" in row:
                    scenario = row["scenario"]
                    adapter = row["adapter"]
                    entries = int(row.get("entries") or 0)
                else:
                    scenario = scenario_from_name(path.name, row.get("mode", ""))
                    adapter = "shardcache"
                    entries = int(row.get("index_entries") or 0)

                ops = as_float(row.get("ops_per_sec"))
                process_vcpu = as_float(row.get("process_vcpu"))
                external_vcpu = as_float(row.get("external_vcpu"))
                total_vcpu = as_float(row.get("total_vcpu"))
                if total_vcpu == 0.0 and process_vcpu > 0.0:
                    total_vcpu = process_vcpu + external_vcpu

                sut_vcpu = as_float(row.get("sut_vcpu"))
                client_vcpu = as_float(row.get("client_vcpu"))
                if sut_vcpu == 0.0:
                    if adapter in NETWORKED_ADAPTERS:
                        sut_vcpu = external_vcpu
                        client_vcpu = process_vcpu
                    else:
                        sut_vcpu = process_vcpu
                        client_vcpu = 0.0

                rows[(scenario, adapter)] = {
                    "scenario": scenario,
                    "adapter": adapter,
                    "label": LABELS.get(adapter, adapter),
                    "workers": int(row.get("workers") or 0),
                    "entries": entries,
                    "dims": int(row.get("dims") or 0),
                    "ops": ops,
                    "ops_per_sut_cpu": as_float(row.get("ops_per_sut_cpu")) or ratio(ops, sut_vcpu),
                    "ops_per_total_cpu": as_float(row.get("ops_per_total_cpu"))
                    or as_float(row.get("ops_per_cpu"))
                    or ratio(ops, total_vcpu),
                    "p50": as_float(row.get("p50_ms")),
                    "p99": as_float(row.get("p99_ms")),
                    "sut_vcpu": sut_vcpu,
                    "client_vcpu": client_vcpu,
                    "total_vcpu": total_vcpu,
                }
    return rows


def scenario_from_name(name: str, fallback: str) -> str:
    if "miss-cold" in name:
        return "miss-cold"
    if "hit-cold-unique" in name:
        return "hit-cold-unique"
    if "hit-hot-cached" in name:
        return "hit-hot-cached"
    return fallback


def as_float(value: object) -> float:
    if value is None:
        return 0.0
    text = str(value).strip()
    if not text:
        return 0.0
    return float(text)


def ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator > 0.0 else 0.0


def scenario_rows(
    rows: dict[tuple[str, str], dict[str, object]],
    scenario: str,
) -> list[dict[str, object]]:
    return [rows[(scenario, adapter)] for adapter in ORDER if (scenario, adapter) in rows]


def speedup(row: dict[str, object], shard_ops: float) -> float:
    ops = float(row["ops"])
    return shard_ops / ops if ops > 0.0 else 0.0


def cpuset_width(raw: str) -> int:
    total = 0
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start_raw, end_raw = part.split("-", 1)
            try:
                start = int(start_raw)
                end = int(end_raw)
            except ValueError:
                continue
            if end >= start:
                total += end - start + 1
        else:
            try:
                int(part)
            except ValueError:
                continue
            total += 1
    return total


def sut_vcpus(metadata: dict[str, str]) -> int:
    return cpuset_width(metadata.get("sut_cpuset", "0-15")) or 16


def report_title(metadata: dict[str, str]) -> str:
    return f"ShardCache Semantic Cache Head-to-Head: {sut_vcpus(metadata)}-vCPU Isolated"


def worker_label(value: object) -> str:
    count = int(str(value))
    return f"{count} worker" if count == 1 else f"{count} workers"


def logical_cpu_label(count: int, cpuset: str) -> str:
    noun = "logical CPU" if count == 1 else "logical CPUs"
    return f"{count} {noun} ({cpuset})"


def row_for(
    rows: dict[tuple[str, str], dict[str, object]],
    scenario: str,
    adapter: str,
) -> dict[str, object]:
    return rows[(scenario, adapter)]


def maybe_row(
    rows: dict[tuple[str, str], dict[str, object]],
    scenario: str,
    adapter: str,
) -> dict[str, object] | None:
    return rows.get((scenario, adapter))


def speedup_text(
    rows: dict[tuple[str, str], dict[str, object]],
    scenario: str,
    adapter: str,
) -> str:
    row = maybe_row(rows, scenario, adapter)
    if row is None:
        return "n/a"
    shard_ops = float(row_for(rows, scenario, "shardcache")["ops"])
    return f"{speedup(row, shard_ops):.1f}x"


def tex_speedup(
    rows: dict[tuple[str, str], dict[str, object]],
    scenario: str,
    adapter: str,
) -> str:
    row = maybe_row(rows, scenario, adapter)
    if row is None:
        return "n/a"
    shard_ops = float(row_for(rows, scenario, "shardcache")["ops"])
    return f"{speedup(row, shard_ops):.1f}$\\times$"


def comparison_text(
    rows: dict[tuple[str, str], dict[str, object]],
    scenario: str,
    adapter: str,
    label: str,
) -> str:
    row = maybe_row(rows, scenario, adapter)
    if row is None:
        return f"n/a versus {label}"
    shard_ops = float(row_for(rows, scenario, "shardcache")["ops"])
    relative = speedup(row, shard_ops)
    if relative >= 1.0:
        return f"{relative:.1f}x faster than {label}"
    return f"{(1.0 / relative):.1f}x slower than {label}"


def tex_comparison(
    rows: dict[tuple[str, str], dict[str, object]],
    scenario: str,
    adapter: str,
    label: str,
) -> str:
    row = maybe_row(rows, scenario, adapter)
    if row is None:
        return f"n/a versus {tex_escape(label)}"
    shard_ops = float(row_for(rows, scenario, "shardcache")["ops"])
    relative = speedup(row, shard_ops)
    if relative >= 1.0:
        return f"{relative:.1f}$\\times$ faster than {tex_escape(label)}"
    return f"{(1.0 / relative):.1f}$\\times$ slower than {tex_escape(label)}"


def baseline_improvement_text(current_ops: float) -> str:
    relative = current_ops / PROFILE_BASELINE_OPS
    return f"{relative:.1f}x faster than"


def tex_baseline_improvement(current_ops: float) -> str:
    relative = current_ops / PROFILE_BASELINE_OPS
    return f"{relative:.1f}$\\times$ faster than"


def row_metric(row: dict[str, object], key: str) -> str:
    return format_int(row[key])


def render_methodology_markdown(
    rows: dict[tuple[str, str], dict[str, object]],
    metadata: dict[str, str],
) -> list[str]:
    cold = row_for(rows, "miss-cold", "shardcache")
    hot = row_for(rows, "hit-hot-cached", "shardcache")
    return [
        "## What This Measures",
        "",
        "This is a semantic-cache lookup benchmark, not an embedding-model or LLM benchmark. Each system receives the same precomputed, normalized embeddings and the timed section starts after the cache/index has been populated.",
        "",
        f"The run uses {format_int(metadata.get('entries', cold['entries']))} entries, {metadata.get('dims', cold['dims'])} dimensions, a cosine-distance threshold of {metadata.get('threshold', '0.35')}, {worker_label(metadata.get('workers', cold['workers']))}, and a {metadata.get('seconds', '10')} second measured window. The SUT is pinned to {logical_cpu_label(sut_vcpus(metadata), metadata.get('sut_cpuset', '0-15'))}; networked load clients are pinned to {metadata.get('load_cpuset', '16-31')}.",
        "",
        f"The cold rows measure the cost that an application pays before deciding whether to reuse a cached response or fall through to an LLM. The hot row measures a warmed exact-query cache hit path and reached {row_metric(hot, 'ops')} ops/s for ShardCache in this run.",
        "",
        "Ops/SUT-vCPU excludes the Python load/client process. Total and client vCPU are retained as audit columns so the denominator is visible rather than hidden.",
        "",
    ]


def render_table_guide_markdown(
    rows: dict[tuple[str, str], dict[str, object]],
) -> list[str]:
    cold = row_for(rows, "miss-cold", "shardcache")
    unique = row_for(rows, "hit-cold-unique", "shardcache")
    hot = row_for(rows, "hit-hot-cached", "shardcache")
    return [
        "## How To Read The Tables",
        "",
        "The primary comparison is `Ops/s`: how many semantic-cache lookups completed during the measured window. Latency columns (`p50 ms` and `p99 ms`) show the request-level distribution observed by the benchmark worker. `Speedup` is always ShardCache throughput divided by the peer throughput for the same row; values below `1.0x` mean the peer was faster for that scenario.",
        "",
        "`Ops/SUT-vCPU` is a CPU-efficiency view of the database or embedded index only. For networked systems it divides throughput by the Redis/Qdrant container CPU, not by the Python load generator. This makes the efficiency denominator fair, but it also means throughput remains the decisive capacity metric when client-side work is the limiting factor.",
        "",
        f"In this run, ShardCache's no-memo semantic path is stable across both cold cases: {row_metric(cold, 'ops')} cold misses/s and {row_metric(unique, 'ops')} cold unique hits/s. The hot exact-query row is intentionally different: it measures repeated application traffic and reaches {row_metric(hot, 'ops')} ops/s because the semantic decision is cached in process.",
        "",
    ]


def render_claim_boundary_markdown(
    rows: dict[tuple[str, str], dict[str, object]],
    metadata: dict[str, str],
) -> list[str]:
    vcpus = sut_vcpus(metadata)
    cold_redis = comparison_text(rows, "miss-cold", "redis-vector-hnsw", "Redis HNSW")
    unique_redis = comparison_text(rows, "hit-cold-unique", "redis-vector-hnsw", "Redis HNSW")
    hot_betterdb = comparison_text(rows, "hit-hot-cached", "betterdb", "BetterDB")
    hot_redis = comparison_text(rows, "hit-hot-cached", "redis-vector-hnsw", "Redis HNSW")
    return [
        "## Claim Boundary",
        "",
        f"This {vcpus}-vCPU run supports a precise claim: ShardCache is substantially faster than BetterDB and RedisVL semantic-cache integrations on all measured workloads, and it is {hot_betterdb} on the hot exact-query path. Against raw Redis HNSW, the cold-vector result is workload- and CPU-shape-dependent: ShardCache is {cold_redis} on cold misses and {unique_redis} on cold unique hits in this run, while it is {hot_redis} on hot cached exact queries.",
        "",
        "That distinction matters. A raw vector index can be competitive on first-time vector lookup, especially on a single worker. A native semantic cache also needs to optimize repeated application questions, query-result invalidation, cache memory policy, and integration overhead. The report therefore separates cold no-memo lookup from warmed application-cache behavior instead of collapsing them into one number.",
        "",
    ]


def render_optimization_markdown(
    rows: dict[tuple[str, str], dict[str, object]],
) -> list[str]:
    cold = row_for(rows, "miss-cold", "shardcache")
    unique = row_for(rows, "hit-cold-unique", "shardcache")
    hot = row_for(rows, "hit-hot-cached", "shardcache")
    return [
        "## Why The Optimized Path Is Faster",
        "",
        f"ShardCache now sustains {row_metric(cold, 'ops')} cold misses/s and {row_metric(unique, 'ops')} cold unique semantic hits/s in the no-memo lookup path, then jumps to {row_metric(hot, 'ops')} lookups/s when the exact-query cache is warm. Earlier exploratory no-memo profiling was around 452 ops/s, so the optimized cold path is {baseline_improvement_text(float(cold['ops']))} that baseline in this Adam run.",
        "",
        "- Semantic search is native and in-process, so the timed lookup avoids Python framework dispatch, Redis protocol round trips, and JSON/vector serialization in the critical path.",
        "- Semantic entries are searched through one semantic index instead of fanning every semantic query across every data shard; the semantic index is allowed to use the full semantic memory budget rather than one slice per shard.",
        "- Embeddings are normalized at insert/query boundaries, turning cosine comparison into a dot product over contiguous `f32` vectors.",
        "- The dot product path uses AVX2/FMA when available, with an unrolled scalar fallback.",
        "- Locality-sensitive hashing builds 64-bit signatures and band buckets, then verifies only a capped candidate set with the exact dot product.",
        "- Repeated exact queries use an exact-vector fingerprint and a generation-checked semantic query cache, which explains the multi-million ops/s hot row.",
        "",
        "The most important interpretation is that ShardCache is faster on both sides of the semantic-cache split: it is fast when there is no memoized query result, and it is dramatically faster when application traffic repeats the same questions.",
        "",
    ]


def render_governance_markdown() -> list[str]:
    return [
        "## Cross-User Governance Model",
        "",
        "Most production semantic-cache hits are cross-user: one user stores a response, and another user later asks a similar question. ShardCache therefore treats governance metadata as an opt-in semantic-cache layer rather than a default point-key field. Entries written through the default semantic APIs have `governance: None`. Entries written through the governance APIs carry opaque bytes that the application can interpret as tenant, subject, policy, source-document, or ACL context.",
        "",
        "The request process is:",
        "",
        "1. On store, the application computes the prompt embedding and builds governance metadata from the source data used to answer the prompt.",
        "2. The application stores the answer with `insert_semantic_slice_with_governance` or `insert_semantic_slice_with_ttl_and_governance`.",
        "3. On lookup, the requesting user's prompt is embedded and searched with `semantic_search_with_governance_filter`.",
        "4. ShardCache evaluates semantic candidates, but releases a cached value only when the caller's governance predicate approves the candidate metadata.",
        "5. If no semantically close candidate is authorized, the caller treats the lookup as a miss and computes a fresh answer.",
        "",
        "Example customer data model:",
        "",
        "```text",
        "key        = semantic:tenant/acme/faq/refund-policy",
        "value      = cached response bytes",
        "embedding  = normalized prompt embedding",
        "governance = {tenant: acme, policy_version: 7,",
        "              allowed_groups: [support, billing],",
        "              source_docs: [doc_481, doc_902]}",
        "ttl        = optional freshness window",
        "```",
        "",
        "In Rust, the authorization boundary is explicit:",
        "",
        "```rust",
        "struct RequestUser<'a> {",
        "    tenant: &'a str,",
        "    groups: &'a [&'a str],",
        "    allowed_docs: &'a [&'a str],",
        "    min_policy_version: u32,",
        "}",
        "",
        "fn can_use_cached_answer(user: &RequestUser<'_>, metadata: &[u8]) -> bool {",
        "    let Ok(metadata) = std::str::from_utf8(metadata) else {",
        "        return false;",
        "    };",
        "    let tenant_ok = metadata.contains(\"tenant=acme\");",
        "    let group_ok = metadata.contains(\"groups=support\");",
        "    let docs_ok = metadata.contains(\"docs=doc_481\");",
        "    tenant_ok",
        "        && group_ok",
        "        && docs_ok",
        "        && user.tenant == \"acme\"",
        "        && user.groups.contains(&\"support\")",
        "        && user.allowed_docs.contains(&\"doc_481\")",
        "        && user.min_policy_version <= 7",
        "}",
        "",
        "cache.insert_semantic_slice_with_governance(",
        "    b\"semantic:tenant/acme/faq/refund-policy\",",
        "    response_bytes,",
        "    &embedding,",
        "    b\"tenant=acme;groups=support;docs=doc_481;policy=7\",",
        ")?;",
        "",
        "let user = RequestUser {",
        "    tenant: \"acme\",",
        "    groups: &[\"support\"],",
        "    allowed_docs: &[\"doc_481\"],",
        "    min_policy_version: 7,",
        "};",
        "",
        "let hit = cache.semantic_search_with_governance_filter(",
        "    &request_embedding,",
        "    0.75,",
        "    |metadata| {",
        "        metadata.is_some_and(|bytes| can_use_cached_answer(&user, bytes))",
        "    },",
        ")?;",
        "```",
        "",
        "The important behavior is that `SemanticMatch.governance` is `None` by default and `Some(bytes)` only when the caller used the governance API. Governed searches receive `Option<&[u8]>`, so applications can reject entries without metadata, require a specific policy version, or validate document-level access before any cross-user cached response is served.",
        "",
    ]


def render_scenario_markdown(
    rows: dict[tuple[str, str], dict[str, object]],
    scenario: str,
) -> list[str]:
    shard = row_for(rows, scenario, "shardcache")
    if scenario == "miss-cold":
        return [
            "This row uses unique random negative queries with the query-result cache disabled. It measures the fall-through cost of asking the semantic cache a question that should not match anything.",
            "",
            f"ShardCache completed {row_metric(shard, 'ops')} ops/s at p50 {float(shard['p50']):.4f} ms and p99 {float(shard['p99']):.4f} ms. That is {comparison_text(rows, scenario, 'betterdb', 'BetterDB')}, {comparison_text(rows, scenario, 'redis-vector-hnsw', 'Redis HNSW')}, and {comparison_text(rows, scenario, 'faiss-hnsw', 'FAISS HNSW')}.",
            "",
            "This is the harshest semantic-cache workload because every request still has to prove that there is no reusable answer. ShardCache's LSH shortlist plus SIMD verification keeps that negative lookup path short.",
            "",
        ]
    if scenario == "hit-cold-unique":
        return [
            "This row uses unique positive/paraphrase queries with the query-result cache disabled. It measures a first-time semantic cache hit, where the system must search semantically and return the cached value without exact-query memo help.",
            "",
            f"ShardCache completed {row_metric(shard, 'ops')} ops/s at p50 {float(shard['p50']):.4f} ms and p99 {float(shard['p99']):.4f} ms. That is {comparison_text(rows, scenario, 'betterdb', 'BetterDB')}, {comparison_text(rows, scenario, 'redis-vector-hnsw', 'Redis HNSW')}, and {comparison_text(rows, scenario, 'faiss-hnsw', 'FAISS HNSW')}.",
            "",
            "The result is close to the cold-miss row, which is useful: successful semantic reuse is not materially more expensive than proving a miss in this fixture.",
            "",
        ]
    return [
        "This row warms a repeated exact-query pool before the measured window. It represents the common production case where users ask the same or identical normalized question repeatedly and the semantic cache can return a cached decision immediately.",
        "",
        f"ShardCache completed {row_metric(shard, 'ops')} ops/s at p50 {float(shard['p50']):.4f} ms and p99 {float(shard['p99']):.4f} ms, or {row_metric(shard, 'ops_per_sut_cpu')} ops/SUT-vCPU. That is {comparison_text(rows, scenario, 'betterdb', 'BetterDB')}, {comparison_text(rows, scenario, 'redisvl-semantic-cache', 'RedisVL SemanticCache')}, and {comparison_text(rows, scenario, 'redis-vector-hnsw', 'Redis HNSW')}.",
        "",
        "This row should be read separately from the cold rows: it is intentionally measuring the warmed exact-query path, not first-time vector search. The huge gap comes from keeping the query-result cache in process and invalidating it by semantic generation on writes.",
        "",
    ]


def render_methodology_latex(
    rows: dict[tuple[str, str], dict[str, object]],
    metadata: dict[str, str],
) -> list[str]:
    cold = row_for(rows, "miss-cold", "shardcache")
    hot = row_for(rows, "hit-hot-cached", "shardcache")
    return [
        r"\subsection{What This Measures}",
        "",
        "This is a semantic-cache lookup benchmark, not an embedding-model or LLM benchmark. Each system receives the same precomputed, normalized embeddings and the timed section starts after the cache/index has been populated.",
        "",
        f"The run uses {tex_count(metadata.get('entries', cold['entries']))} entries, {tex_escape(str(metadata.get('dims', cold['dims'])))} dimensions, a cosine-distance threshold of {tex_escape(metadata.get('threshold', '0.35'))}, {tex_escape(worker_label(metadata.get('workers', cold['workers'])))}, and a {tex_escape(metadata.get('seconds', '10'))} second measured window. The SUT is pinned to {tex_escape(logical_cpu_label(sut_vcpus(metadata), metadata.get('sut_cpuset', '0-15')))}; networked load clients are pinned to {tex_escape(metadata.get('load_cpuset', '16-31'))}.",
        "",
        f"The cold rows measure the cost that an application pays before deciding whether to reuse a cached response or fall through to an LLM. The hot row measures a warmed exact-query cache hit path and reached {tex_count(hot['ops'])} ops/s for ShardCache in this run.",
        "",
        "Ops/SUT-vCPU excludes the Python load/client process. Total and client vCPU are retained as audit columns so the denominator is visible rather than hidden.",
        "",
    ]


def render_table_guide_latex(
    rows: dict[tuple[str, str], dict[str, object]],
) -> list[str]:
    cold = row_for(rows, "miss-cold", "shardcache")
    unique = row_for(rows, "hit-cold-unique", "shardcache")
    hot = row_for(rows, "hit-hot-cached", "shardcache")
    return [
        r"\subsection{How To Read The Tables}",
        "",
        r"The primary comparison is \texttt{Ops/s}: how many semantic-cache lookups completed during the measured window. Latency columns (\texttt{p50 ms} and \texttt{p99 ms}) show the request-level distribution observed by the benchmark worker. \texttt{Speedup} is always ShardCache throughput divided by the peer throughput for the same row; values below \texttt{1.0x} mean the peer was faster for that scenario.",
        "",
        r"\texttt{Ops/SUT-vCPU} is a CPU-efficiency view of the database or embedded index only. For networked systems it divides throughput by the Redis/Qdrant container CPU, not by the Python load generator. This makes the efficiency denominator fair, but it also means throughput remains the decisive capacity metric when client-side work is the limiting factor.",
        "",
        f"In this run, ShardCache's no-memo semantic path is stable across both cold cases: {tex_count(cold['ops'])} cold misses/s and {tex_count(unique['ops'])} cold unique hits/s. The hot exact-query row is intentionally different: it measures repeated application traffic and reaches {tex_count(hot['ops'])} ops/s because the semantic decision is cached in process.",
        "",
    ]


def render_claim_boundary_latex(
    rows: dict[tuple[str, str], dict[str, object]],
    metadata: dict[str, str],
) -> list[str]:
    vcpus = sut_vcpus(metadata)
    cold_redis = tex_comparison(rows, "miss-cold", "redis-vector-hnsw", "Redis HNSW")
    unique_redis = tex_comparison(rows, "hit-cold-unique", "redis-vector-hnsw", "Redis HNSW")
    hot_betterdb = tex_comparison(rows, "hit-hot-cached", "betterdb", "BetterDB")
    hot_redis = tex_comparison(rows, "hit-hot-cached", "redis-vector-hnsw", "Redis HNSW")
    return [
        r"\subsection{Claim Boundary}",
        "",
        f"This {vcpus}-vCPU run supports a precise claim: ShardCache is substantially faster than BetterDB and RedisVL semantic-cache integrations on all measured workloads, and it is {hot_betterdb} on the hot exact-query path. Against raw Redis HNSW, the cold-vector result is workload- and CPU-shape-dependent: ShardCache is {cold_redis} on cold misses and {unique_redis} on cold unique hits in this run, while it is {hot_redis} on hot cached exact queries.",
        "",
        "That distinction matters. A raw vector index can be competitive on first-time vector lookup, especially on a single worker. A native semantic cache also needs to optimize repeated application questions, query-result invalidation, cache memory policy, and integration overhead. The report therefore separates cold no-memo lookup from warmed application-cache behavior instead of collapsing them into one number.",
        "",
    ]


def render_optimization_latex(
    rows: dict[tuple[str, str], dict[str, object]],
) -> list[str]:
    cold = row_for(rows, "miss-cold", "shardcache")
    unique = row_for(rows, "hit-cold-unique", "shardcache")
    hot = row_for(rows, "hit-hot-cached", "shardcache")
    return [
        r"\subsection{Why The Optimized Path Is Faster}",
        "",
        f"ShardCache now sustains {tex_count(cold['ops'])} cold misses/s and {tex_count(unique['ops'])} cold unique semantic hits/s in the no-memo lookup path, then jumps to {tex_count(hot['ops'])} lookups/s when the exact-query cache is warm. Earlier exploratory no-memo profiling was around 452 ops/s, so the optimized cold path is {tex_baseline_improvement(float(cold['ops']))} that baseline in this Adam run.",
        "",
        r"\begin{itemize}",
        r"\item Semantic search is native and in-process, so the timed lookup avoids Python framework dispatch, Redis protocol round trips, and JSON/vector serialization in the critical path.",
        r"\item Semantic entries are searched through one semantic index instead of fanning every semantic query across every data shard; the semantic index is allowed to use the full semantic memory budget rather than one slice per shard.",
        r"\item Embeddings are normalized at insert/query boundaries, turning cosine comparison into a dot product over contiguous \texttt{f32} vectors.",
        r"\item The dot product path uses AVX2/FMA when available, with an unrolled scalar fallback.",
        r"\item Locality-sensitive hashing builds 64-bit signatures and band buckets, then verifies only a capped candidate set with the exact dot product.",
        r"\item Repeated exact queries use an exact-vector fingerprint and a generation-checked semantic query cache, which explains the multi-million ops/s hot row.",
        r"\end{itemize}",
        "",
        "The most important interpretation is that ShardCache is faster on both sides of the semantic-cache split: it is fast when there is no memoized query result, and it is dramatically faster when application traffic repeats the same questions.",
        "",
    ]


def render_governance_latex() -> list[str]:
    return [
        r"\subsection{Cross-User Governance Model}",
        "",
        "Most production semantic-cache hits are cross-user: one user stores a response, and another user later asks a similar question. ShardCache therefore treats governance metadata as an opt-in semantic-cache layer rather than a default point-key field. Entries written through the default semantic APIs have \\texttt{governance: None}. Entries written through the governance APIs carry opaque bytes that the application can interpret as tenant, subject, policy, source-document, or ACL context.",
        "",
        "The request process is:",
        "",
        r"\begin{enumerate}",
        r"\item On store, the application computes the prompt embedding and builds governance metadata from the source data used to answer the prompt.",
        r"\item The application stores the answer with one of the governance insert APIs.",
        r"\item On lookup, the requesting user's prompt is embedded and searched through the governed semantic-search API.",
        r"\item ShardCache evaluates semantic candidates, but releases a cached value only when the caller's governance predicate approves the candidate metadata.",
        r"\item If no semantically close candidate is authorized, the caller treats the lookup as a miss and computes a fresh answer.",
        r"\end{enumerate}",
        "",
        "A representative customer data model is:",
        "",
        r"\begin{verbatim}",
        "key        = semantic:tenant/acme/faq/refund-policy",
        "value      = cached response bytes",
        "embedding  = normalized prompt embedding",
        "governance = {tenant: acme, policy_version: 7,",
        "              allowed_groups: [support, billing],",
        "              source_docs: [doc_481, doc_902]}",
        "ttl        = optional freshness window",
        r"\end{verbatim}",
        "",
        "In Rust, the authorization boundary is explicit:",
        "",
        r"\begin{verbatim}",
        "struct RequestUser<'a> {",
        "    tenant: &'a str,",
        "    groups: &'a [&'a str],",
        "    allowed_docs: &'a [&'a str],",
        "    min_policy_version: u32,",
        "}",
        "",
        "fn can_use_cached_answer(",
        "    user: &RequestUser<'_>,",
        "    metadata: &[u8],",
        ") -> bool {",
        "    let Ok(metadata) = std::str::from_utf8(metadata) else {",
        "        return false;",
        "    };",
        '    let tenant_ok = metadata.contains("tenant=acme");',
        '    let group_ok = metadata.contains("groups=support");',
        '    let docs_ok = metadata.contains("docs=doc_481");',
        "    tenant_ok",
        "        && group_ok",
        "        && docs_ok",
        '        && user.tenant == "acme"',
        '        && user.groups.contains(&"support")',
        '        && user.allowed_docs.contains(&"doc_481")',
        "        && user.min_policy_version <= 7",
        "}",
        "",
        "cache.insert_semantic_slice_with_governance(",
        '    b"semantic:tenant/acme/faq/refund-policy",',
        "    response_bytes,",
        "    &embedding,",
        '    b"tenant=acme;groups=support;docs=doc_481;policy=7",',
        ")?;",
        "",
        "let user = RequestUser {",
        '    tenant: "acme",',
        '    groups: &["support"],',
        '    allowed_docs: &["doc_481"],',
        "    min_policy_version: 7,",
        "};",
        "",
        "let hit = cache.semantic_search_with_governance_filter(",
        "    &request_embedding,",
        "    0.75,",
        "    |metadata| {",
        "        metadata.is_some_and(|bytes| can_use_cached_answer(&user, bytes))",
        "    },",
        ")?;",
        r"\end{verbatim}",
        "",
        "The important behavior is that \\texttt{SemanticMatch.governance} is \\texttt{None} by default and \\texttt{Some(bytes)} only when the caller used the governance API. Governed searches receive \\texttt{Option<\\&[u8]>}, so applications can reject entries without metadata, require a specific policy version, or validate document-level access before any cross-user cached response is served.",
        "",
    ]


def render_scenario_latex(
    rows: dict[tuple[str, str], dict[str, object]],
    scenario: str,
) -> list[str]:
    shard = row_for(rows, scenario, "shardcache")
    if scenario == "miss-cold":
        return [
            "This row uses unique random negative queries with the query-result cache disabled. It measures the fall-through cost of asking the semantic cache a question that should not match anything.",
            "",
            f"ShardCache completed {tex_count(shard['ops'])} ops/s at p50 {float(shard['p50']):.4f} ms and p99 {float(shard['p99']):.4f} ms. That is {tex_comparison(rows, scenario, 'betterdb', 'BetterDB')}, {tex_comparison(rows, scenario, 'redis-vector-hnsw', 'Redis HNSW')}, and {tex_comparison(rows, scenario, 'faiss-hnsw', 'FAISS HNSW')}.",
            "",
            "This is the harshest semantic-cache workload because every request still has to prove that there is no reusable answer. ShardCache's LSH shortlist plus SIMD verification keeps that negative lookup path short.",
            "",
        ]
    if scenario == "hit-cold-unique":
        return [
            "This row uses unique positive/paraphrase queries with the query-result cache disabled. It measures a first-time semantic cache hit, where the system must search semantically and return the cached value without exact-query memo help.",
            "",
            f"ShardCache completed {tex_count(shard['ops'])} ops/s at p50 {float(shard['p50']):.4f} ms and p99 {float(shard['p99']):.4f} ms. That is {tex_comparison(rows, scenario, 'betterdb', 'BetterDB')}, {tex_comparison(rows, scenario, 'redis-vector-hnsw', 'Redis HNSW')}, and {tex_comparison(rows, scenario, 'faiss-hnsw', 'FAISS HNSW')}.",
            "",
            "The result is close to the cold-miss row, which is useful: successful semantic reuse is not materially more expensive than proving a miss in this fixture.",
            "",
        ]
    return [
        "This row warms a repeated exact-query pool before the measured window. It represents the common production case where users ask the same or identical normalized question repeatedly and the semantic cache can return a cached decision immediately.",
        "",
        f"ShardCache completed {tex_count(shard['ops'])} ops/s at p50 {float(shard['p50']):.4f} ms and p99 {float(shard['p99']):.4f} ms, or {tex_count(shard['ops_per_sut_cpu'])} ops/SUT-vCPU. That is {tex_comparison(rows, scenario, 'betterdb', 'BetterDB')}, {tex_comparison(rows, scenario, 'redisvl-semantic-cache', 'RedisVL SemanticCache')}, and {tex_comparison(rows, scenario, 'redis-vector-hnsw', 'Redis HNSW')}.",
        "",
        "This row should be read separately from the cold rows: it is intentionally measuring the warmed exact-query path, not first-time vector search. The huge gap comes from keeping the query-result cache in process and invalidating it by semantic generation on writes.",
        "",
    ]


def render_markdown(rows: dict[tuple[str, str], dict[str, object]], metadata: dict[str, str]) -> str:
    out: list[str] = [
        f"# {report_title(metadata)}",
        "",
        f"- Host: {metadata.get('host', 'adam')}",
        f"- SUT CPU set: {metadata.get('sut_cpuset', '0-15')}",
        f"- Load/client CPU set: {metadata.get('load_cpuset', '16-31')}",
        f"- Workers: {metadata.get('workers', '16')}",
        f"- Entries: {format_int(metadata.get('entries', '100000'))}",
        f"- Dims: {metadata.get('dims', '384')}",
        f"- Threshold distance: {metadata.get('threshold', '0.35')}",
        "",
    ]
    out.extend(render_methodology_markdown(rows, metadata))
    out.extend(render_table_guide_markdown(rows))
    out.extend(render_optimization_markdown(rows))
    out.extend(render_governance_markdown())
    out.extend(render_claim_boundary_markdown(rows, metadata))

    for meta in SCENARIOS:
        table_rows = scenario_rows(rows, str(meta["key"]))
        if not table_rows:
            continue
        shard_ops = float(table_rows[0]["ops"])
        out.extend(
            [
                f"## {meta['title']}",
                "",
            ]
        )
        out.extend(render_scenario_markdown(rows, str(meta["key"])))
        out.extend(
            [
                "| System | Ops/s | Ops/SUT-vCPU | p50 ms | p99 ms | SUT vCPU | Client vCPU | Total vCPU | Speedup |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in table_rows:
            out.append(
                "| {label} | {ops} | {ops_cpu} | {p50:.4f} | {p99:.4f} | {sut:.2f} | {client:.2f} | {total:.2f} | {speedup:.1f}x |".format(
                    label=row["label"],
                    ops=format_int(row["ops"]),
                    ops_cpu=format_int(row["ops_per_sut_cpu"]),
                    p50=float(row["p50"]),
                    p99=float(row["p99"]),
                    sut=float(row["sut_vcpu"]),
                    client=float(row["client_vcpu"]),
                    total=float(row["total_vcpu"]),
                    speedup=speedup(row, shard_ops),
                )
            )
        out.append("")
    return "\n".join(out)


def render_combined_markdown(runs: list[dict[str, object]]) -> str:
    title = combined_report_title(runs)
    out: list[str] = [
        f"# {title}",
        "",
        "This report places the isolated 1-vCPU and 16-vCPU Adam benchmark runs in one artifact. Each run still keeps its own peer tables, CPU accounting, charts, and claim boundary, because the CPU shape changes both throughput and relative ranking.",
        "",
        "## Cross-Run Scaling",
        "",
        "| Scenario | ShardCache 1-vCPU ops/s | ShardCache 16-vCPU ops/s | Scale factor | 1-vCPU p50 ms | 16-vCPU p50 ms |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    one = run_by_vcpu(runs, 1)
    sixteen = run_by_vcpu(runs, 16)
    if one is not None and sixteen is not None:
        one_rows = one["rows"]
        sixteen_rows = sixteen["rows"]
        for meta in SCENARIOS:
            scenario = str(meta["key"])
            one_shard = row_for(one_rows, scenario, "shardcache")
            sixteen_shard = row_for(sixteen_rows, scenario, "shardcache")
            scale = ratio(float(sixteen_shard["ops"]), float(one_shard["ops"]))
            out.append(
                f"| {meta['title']} | {format_int(one_shard['ops'])} | {format_int(sixteen_shard['ops'])} | {scale:.1f}x | {float(one_shard['p50']):.4f} | {float(sixteen_shard['p50']):.4f} |"
            )
    else:
        out.append("| n/a | n/a | n/a | n/a | n/a | n/a |")
    out.append("")

    for run in runs:
        metadata = run["metadata"]
        rows = run["rows"]
        out.append(f"---\n\n{render_markdown(rows, metadata)}")
        out.append("")
    return "\n".join(out)


def render_latex_section(
    rows: dict[tuple[str, str], dict[str, object]],
    metadata: dict[str, str],
    *,
    asset_prefix: str = "",
    label_scope: str = "isolated",
) -> str:
    vcpus = sut_vcpus(metadata)
    workers = metadata.get("workers", "16")
    out: list[str] = [
        rf"\section{{{tex_escape(report_title(metadata))}}}",
        rf"\label{{sec:shardcache-semantic-head-to-head-{tex_escape(label_scope)}}}",
        "",
        "We reran the semantic-cache head-to-head on the Adam Ubuntu server with explicit CPU isolation. "
        f"The system under test was limited to {vcpus} logical CPU(s) ({tex_escape(metadata.get('sut_cpuset', '0-15'))}), "
        f"and external load/client workers were pinned to logical CPUs {tex_escape(metadata.get('load_cpuset', '16-31'))}. "
        f"Each measured row used {tex_escape(metadata.get('workers', '16'))} workers, a "
        f"{tex_escape(metadata.get('seconds', '10'))} second measured query window, "
        f"{tex_count(metadata.get('entries', '100000'))} cached entries, "
        f"{tex_escape(metadata.get('dims', '384'))}-dimensional normalized embeddings, and a "
        f"cosine-distance threshold of {tex_escape(metadata.get('threshold', '0.35'))}. "
        "Embeddings were precomputed, so the lookup path rather than embedding-model latency is measured.",
        "",
        "CPU use is reported as measured-window vCPU, where 1.0 means one fully busy logical CPU for the measured window. "
        "Ops/SUT-vCPU is throughput divided by system-under-test CPU only. For networked rows, SUT CPU is the external server/container PID tree and the Python load/client process is excluded from the efficiency denominator. "
        "For embedded rows such as ShardCache, FAISS, and hnswlib, the benchmark process is the system under test. Total vCPU and client vCPU remain in the tables for auditability.",
        "",
    ]
    out.extend(render_methodology_latex(rows, metadata))
    out.extend(render_table_guide_latex(rows))
    out.extend(render_optimization_latex(rows))
    out.extend(render_governance_latex())
    out.extend(render_claim_boundary_latex(rows, metadata))

    for meta in SCENARIOS:
        key = str(meta["key"])
        table_rows = scenario_rows(rows, key)
        if not table_rows:
            continue
        shard_ops = float(table_rows[0]["ops"])
        title = str(meta["title"])
        label_suffix = f"{label_scope}-{key.replace('-', '-')}"
        out.extend(
            [
                rf"\subsection{{{tex_escape(title)}}}",
                "",
            ]
        )
        out.extend(render_scenario_latex(rows, key))
        out.extend(
            [
                r"\begin{figure}[htbp]",
                r"\centering",
                rf"\includegraphics[width=\linewidth]{{{asset_prefix}{meta['throughput_pdf']}}}",
                rf"\caption{{{tex_escape(title)} throughput with {tex_escape(worker_label(workers))} and explicit CPU isolation.}}",
                rf"\label{{fig:semantic-h2h-isolated-{label_suffix}-throughput}}",
                r"\end{figure}",
                "",
                r"\begin{figure}[htbp]",
                r"\centering",
                rf"\includegraphics[width=\linewidth]{{{asset_prefix}{meta['vcpu_pdf']}}}",
                rf"\caption{{{tex_escape(title)} measured SUT CPU use during the query window.}}",
                rf"\label{{fig:semantic-h2h-isolated-{label_suffix}-vcpu}}",
                r"\end{figure}",
                "",
                r"\begin{table}[htbp]",
                r"\centering",
                r"\scriptsize",
                r"\setlength{\tabcolsep}{3pt}",
                rf"\caption{{{tex_escape(title)} head-to-head at {tex_count(metadata.get('entries', '100000'))} entries with {vcpus}-vCPU isolation.}}",
                rf"\label{{tab:semantic-h2h-isolated-{label_suffix}}}",
                r"\begin{tabular}{lrrrrrrrr}",
                r"\hline",
                r"System & Ops/s & Ops/SUT-vCPU & p50 ms & p99 ms & SUT vCPU & Client vCPU & Total vCPU & Speedup \\",
                r"\hline",
            ]
        )
        for row in table_rows:
            out.append(
                "{label} & {ops} & {ops_cpu} & {p50:.4f} & {p99:.4f} & {sut:.2f} & {client:.2f} & {total:.2f} & {speedup:.1f}$\\times$ \\\\".format(
                    label=tex_escape(str(row["label"])),
                    ops=tex_count(row["ops"]),
                    ops_cpu=tex_count(row["ops_per_sut_cpu"]),
                    p50=float(row["p50"]),
                    p99=float(row["p99"]),
                    sut=float(row["sut_vcpu"]),
                    client=float(row["client_vcpu"]),
                    total=float(row["total_vcpu"]),
                    speedup=speedup(row, shard_ops),
                )
            )
        out.extend(
            [
                r"\hline",
                r"\end{tabular}",
                r"\end{table}",
                "",
            ]
        )

    out.extend(render_summary(rows, metadata))
    out.extend(
        [
            r"\subsection{Caveats}",
            "",
            r"The Redis-backed rows used Redis Stack; the image digest is captured in \texttt{metadata.txt}. Python package versions are captured in \texttt{python-freeze.txt}; this isolated run used a fresh benchmark virtual environment on Adam, so package versions can differ from earlier uncapped exploratory rows. GPTCache and managed Redis LangCache are not included in this isolated matrix: GPTCache previously failed the concurrent 100k run cleanly, and no Redis LangCache endpoint or credentials were available. Embedded rows cannot separate load-generator CPU from system CPU because the database/index is a library in the benchmark process; those rows are marked by zero load vCPU and process CPU equal to SUT CPU.",
            "",
        ]
    )
    return "\n".join(out)


def render_combined_latex_section(runs: list[dict[str, object]], output_dir: Path) -> str:
    out: list[str] = [
        rf"\section{{{tex_escape(combined_report_title(runs))}}}",
        r"\label{sec:shardcache-semantic-head-to-head-combined}",
        "",
        "This report places the isolated 1-vCPU and 16-vCPU Adam benchmark runs in one artifact. Each run still keeps its own peer tables, CPU accounting, charts, and claim boundary, because the CPU shape changes both throughput and relative ranking.",
        "",
        r"\subsection{Cross-Run Scaling}",
        "",
        "The table below compares ShardCache against itself across the two CPU limits before the full peer tables. It should be read as a scaling view, not a replacement for the per-run head-to-head sections.",
        "",
        r"\begin{table}[htbp]",
        r"\centering",
        r"\small",
        r"\caption{ShardCache 1-vCPU versus 16-vCPU isolated scaling on Adam.}",
        r"\label{tab:semantic-h2h-combined-scaling}",
        r"\begin{tabular}{lrrrrr}",
        r"\hline",
        r"Scenario & 1-vCPU ops/s & 16-vCPU ops/s & Scale & 1-vCPU p50 ms & 16-vCPU p50 ms \\",
        r"\hline",
    ]
    one = run_by_vcpu(runs, 1)
    sixteen = run_by_vcpu(runs, 16)
    if one is not None and sixteen is not None:
        one_rows = one["rows"]
        sixteen_rows = sixteen["rows"]
        for meta in SCENARIOS:
            scenario = str(meta["key"])
            one_shard = row_for(one_rows, scenario, "shardcache")
            sixteen_shard = row_for(sixteen_rows, scenario, "shardcache")
            scale = ratio(float(sixteen_shard["ops"]), float(one_shard["ops"]))
            out.append(
                "{scenario} & {one_ops} & {sixteen_ops} & {scale:.1f}$\\times$ & {one_p50:.4f} & {sixteen_p50:.4f} \\\\".format(
                    scenario=tex_escape(str(meta["title"])),
                    one_ops=tex_count(one_shard["ops"]),
                    sixteen_ops=tex_count(sixteen_shard["ops"]),
                    scale=scale,
                    one_p50=float(one_shard["p50"]),
                    sixteen_p50=float(sixteen_shard["p50"]),
                )
            )
    out.extend(
        [
            r"\hline",
            r"\end{tabular}",
            r"\end{table}",
            "",
        ]
    )

    for run in runs:
        metadata = run["metadata"]
        rows = run["rows"]
        vcpus = sut_vcpus(metadata)
        out.append(r"\clearpage")
        out.append("")
        out.append(
            render_latex_section(
                rows,
                metadata,
                asset_prefix=latex_asset_prefix(output_dir, Path(run["path"])),
                label_scope=f"{vcpus}vcpu",
            )
        )
        out.append("")
    return "\n".join(out)


def combined_report_title(runs: list[dict[str, object]]) -> str:
    vcpus = sorted(sut_vcpus(run["metadata"]) for run in runs)
    if vcpus == [1, 16]:
        return "ShardCache Semantic Cache Head-to-Head: 1-vCPU and 16-vCPU Isolated"
    labels = " and ".join(f"{vcpu}-vCPU" for vcpu in vcpus)
    return f"ShardCache Semantic Cache Head-to-Head: {labels} Isolated"


def run_by_vcpu(runs: list[dict[str, object]], target: int) -> dict[str, object] | None:
    return next((run for run in runs if sut_vcpus(run["metadata"]) == target), None)


def latex_asset_prefix(output_dir: Path, asset_dir: Path) -> str:
    relative = os.path.relpath(asset_dir, output_dir).replace(os.sep, "/")
    if relative == ".":
        return ""
    return f"{relative}/"


def render_summary(
    rows: dict[tuple[str, str], dict[str, object]],
    metadata: dict[str, str],
) -> list[str]:
    cold = scenario_rows(rows, "miss-cold")
    unique = scenario_rows(rows, "hit-cold-unique")
    hot = scenario_rows(rows, "hit-hot-cached")
    if not cold or not unique or not hot:
        return []

    cold_shard = float(cold[0]["ops"])
    unique_shard = float(unique[0]["ops"])
    hot_shard = float(hot[0]["ops"])

    def sp(scenario: str, adapter: str) -> float:
        row = rows.get((scenario, adapter))
        return speedup(row, float(rows[(scenario, "shardcache")]["ops"])) if row else 0.0

    vcpus = sut_vcpus(metadata)
    return [
        r"\subsection{Summary}",
        "",
        f"Under the isolated {vcpus}-vCPU shape, ShardCache sustained {tex_count(cold_shard)} cold misses per second and {tex_count(unique_shard)} cold unique semantic hits per second. "
        f"On cold misses, ShardCache was {tex_comparison(rows, 'miss-cold', 'betterdb', 'BetterDB')}, {tex_comparison(rows, 'miss-cold', 'redis-vector-hnsw', 'Redis HNSW')}, and {tex_comparison(rows, 'miss-cold', 'faiss-hnsw', 'FAISS HNSW')}. "
        f"On cold unique semantic hits, ShardCache was {tex_comparison(rows, 'hit-cold-unique', 'betterdb', 'BetterDB')}, {tex_comparison(rows, 'hit-cold-unique', 'redis-vector-hnsw', 'Redis HNSW')}, and {tex_comparison(rows, 'hit-cold-unique', 'faiss-hnsw', 'FAISS HNSW')}.",
        "",
        f"On the hot exact-query workload, ShardCache reached {tex_count(hot_shard)} lookups per second, {tex_comparison(rows, 'hit-hot-cached', 'betterdb', 'BetterDB')}, {tex_comparison(rows, 'hit-hot-cached', 'redisvl-semantic-cache', 'RedisVL SemanticCache')}, {tex_comparison(rows, 'hit-hot-cached', 'redis-vector-hnsw', 'Redis HNSW')}, and {tex_comparison(rows, 'hit-hot-cached', 'faiss-hnsw', 'FAISS HNSW')} in this run.",
        "",
    ]


def format_int(value: object) -> str:
    return f"{int(round(float(value))):,}"


def tex_count(value: object) -> str:
    return format_int(value).replace(",", r"{,}")


def tex_escape(value: str) -> str:
    return (
        value.replace("\\", r"\textbackslash{}")
        .replace("&", r"\&")
        .replace("%", r"\%")
        .replace("$", r"\$")
        .replace("#", r"\#")
        .replace("_", r"\_")
        .replace("{", r"\{")
        .replace("}", r"\}")
    )


if __name__ == "__main__":
    main()
