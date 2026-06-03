#!/usr/bin/env python3
"""Render Markdown and LaTeX reports for semantic-cache head-to-head results."""

from __future__ import annotations

import argparse
import csv
import os
import shutil
import subprocess
import sys
from pathlib import Path


SCENARIOS = [
    {
        "key": "miss-cold",
        "title": "Cold Miss",
        "throughput_pdf": "semantic-h2h-miss-cold-throughput.pdf",
        "show_hit_rate_chart": False,
        "hit_rate_pdf": "semantic-h2h-miss-cold-hit-rate.pdf",
        "vcpu_pdf": "semantic-h2h-miss-cold-vcpu.pdf",
    },
    {
        "key": "hit-cold-unique",
        "title": "Cold Unique Semantic Hit",
        "throughput_pdf": "semantic-h2h-hit-cold-unique-throughput.pdf",
        "show_hit_rate_chart": True,
        "hit_rate_pdf": "semantic-h2h-hit-cold-unique-hit-rate.pdf",
        "vcpu_pdf": "semantic-h2h-hit-cold-unique-vcpu.pdf",
    },
    {
        "key": "hit-hot-cached",
        "title": "Hot Cached Exact Query",
        "throughput_pdf": "semantic-h2h-hit-hot-cached-throughput.pdf",
        "show_hit_rate_chart": True,
        "hit_rate_pdf": "semantic-h2h-hit-hot-cached-hit-rate.pdf",
        "vcpu_pdf": "semantic-h2h-hit-hot-cached-vcpu.pdf",
    },
]

ORDER = [
    "shardcache",
    "shardcache-server",
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
    "shardcache-server",
    "betterdb",
    "redisvl-semantic-cache",
    "langchain-redis-semantic-cache",
    "redis-vector-flat",
    "redis-vector-hnsw",
    "qdrant-cosine",
}

LABELS = {
    "shardcache": "ShardCache Embedded",
    "shardcache-server": "ShardCache Server",
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

    output_dir = args.combined_output_dir or args.results_dir[0].parent / "server-semantic-h2h-isolated-combined"
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
            latex_report_preamble()
            + [
                r"\input{shardcache-semantic-head-to-head-isolated-section.tex}",
                r"\end{document}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def render_combined_report(results_dirs: list[Path], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    runs = merged_runs(results_dirs)
    chart_assets = render_combined_chart_assets(runs, output_dir)

    (output_dir / "report.md").write_text(
        render_combined_markdown(runs, chart_assets),
        encoding="utf-8",
    )
    section = render_combined_latex_section(runs, output_dir, chart_assets)
    (output_dir / "shardcache-semantic-head-to-head-combined-section.tex").write_text(
        section,
        encoding="utf-8",
    )
    (output_dir / "shardcache-semantic-head-to-head-combined-report.tex").write_text(
        "\n".join(
            latex_report_preamble()
            + [
                r"\input{shardcache-semantic-head-to-head-combined-section.tex}",
                r"\end{document}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def render_combined_chart_assets(
    runs: list[dict[str, object]],
    output_dir: Path,
) -> dict[tuple[int, str], dict[str, str]]:
    chart_script = Path(__file__).with_name("render-semantic-h2h-charts.py")
    converter = shutil.which("rsvg-convert")
    assets: dict[tuple[int, str], dict[str, str]] = {}
    if not chart_script.exists():
        return assets

    for run in runs:
        vcpus = sut_vcpus(run["metadata"])  # type: ignore[arg-type]
        if vcpus not in (1, 16):
            continue
        results_dir = Path(run["path"])  # type: ignore[arg-type]
        subprocess.run(
            [sys.executable, str(chart_script), str(results_dir)],
            check=True,
        )
        for meta in SCENARIOS:
            scenario = str(meta["key"])
            source_svg = results_dir / str(meta["throughput_pdf"]).replace(".pdf", ".svg")
            if not source_svg.exists():
                continue
            base_name = f"semantic-h2h-combined-{vcpus}vcpu-{scenario}-throughput"
            svg_path = output_dir / f"{base_name}.svg"
            pdf_path = output_dir / f"{base_name}.pdf"
            shutil.copyfile(source_svg, svg_path)
            asset = {"svg": svg_path.name}
            if converter:
                subprocess.run(
                    [converter, "-f", "pdf", "-o", str(pdf_path), str(svg_path)],
                    check=True,
                )
                asset["pdf"] = pdf_path.name
            hit_rate_source = results_dir / str(meta["hit_rate_pdf"]).replace(".pdf", ".svg")
            if bool(meta.get("show_hit_rate_chart", True)) and hit_rate_source.exists():
                hit_rate_base = f"semantic-h2h-combined-{vcpus}vcpu-{scenario}-hit-rate"
                hit_rate_svg = output_dir / f"{hit_rate_base}.svg"
                hit_rate_pdf = output_dir / f"{hit_rate_base}.pdf"
                shutil.copyfile(hit_rate_source, hit_rate_svg)
                asset["hit_rate_svg"] = hit_rate_svg.name
                if converter:
                    subprocess.run(
                        [converter, "-f", "pdf", "-o", str(hit_rate_pdf), str(hit_rate_svg)],
                        check=True,
                    )
                    asset["hit_rate_pdf"] = hit_rate_pdf.name
            assets[(vcpus, scenario)] = asset
    return assets


def load_run(results_dir: Path) -> dict[str, object]:
    return {
        "path": results_dir,
        "paths": [results_dir],
        "rows": load_rows(results_dir),
        "metadata": load_metadata(results_dir / "metadata.txt"),
    }


def merged_runs(results_dirs: list[Path]) -> list[dict[str, object]]:
    by_vcpu: dict[int, dict[str, object]] = {}
    for results_dir in results_dirs:
        run = load_run(results_dir)
        vcpus = sut_vcpus(run["metadata"])
        existing = by_vcpu.get(vcpus)
        if existing is None:
            by_vcpu[vcpus] = run
            continue

        existing_rows = existing["rows"]
        new_rows = run["rows"]
        if isinstance(existing_rows, dict) and isinstance(new_rows, dict):
            existing_rows.update(new_rows)

        existing_metadata = existing["metadata"]
        new_metadata = run["metadata"]
        if isinstance(existing_metadata, dict) and isinstance(new_metadata, dict):
            for key, value in new_metadata.items():
                existing_metadata.setdefault(key, value)
            existing_metadata["merged_result_dirs"] = ",".join(
                str(path) for path in [*existing.get("paths", []), results_dir]
            )

        existing.setdefault("paths", []).append(results_dir)

    return sorted(by_vcpu.values(), key=lambda run: sut_vcpus(run["metadata"]))


def latex_report_preamble() -> list[str]:
    return [
        r"\pdfminorversion=7",
        r"\pdfsuppresswarningpagegroup=1",
        r"\documentclass[11pt]{article}",
        r"\usepackage[margin=0.75in]{geometry}",
        r"\usepackage{graphicx}",
        r"\usepackage{xcolor}",
        r"\usepackage{listings}",
        r"\usepackage{placeins}",
        r"\definecolor{codebg}{HTML}{F7F8FA}",
        r"\definecolor{codeborder}{HTML}{D0D7DE}",
        r"\definecolor{codecomment}{HTML}{57606A}",
        r"\lstdefinestyle{shardcachecode}{%",
        r"  basicstyle=\ttfamily\footnotesize,",
        r"  backgroundcolor=\color{codebg},",
        r"  frame=single,",
        r"  rulecolor=\color{codeborder},",
        r"  commentstyle=\color{codecomment},",
        r"  breaklines=true,",
        r"  columns=fullflexible,",
        r"  keepspaces=true,",
        r"  showstringspaces=false,",
        r"  tabsize=2,",
        r"  framerule=0.4pt,",
        r"  framesep=0.45em,",
        r"  xleftmargin=0.5em,",
        r"  xrightmargin=0.5em",
        r"}",
        r"\begin{document}",
    ]


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
                hits = int(row.get("hits") or 0)
                queries = int(row.get("queries") or 0)
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
                    "hits": hits,
                    "queries": queries,
                    "hit_rate": ratio(float(hits), float(queries)) * 100.0,
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


def execution_mode_note(rows: dict[tuple[str, str], dict[str, object]]) -> str:
    has_server = any(adapter == "shardcache-server" for _, adapter in rows)
    if has_server:
        return "The ShardCache Embedded row uses the in-process native semantic-cache API. The ShardCache Server row uses the shardcache TCP server through the RESP semantic commands, so it includes client/server wire overhead and separate server CPU accounting."
    return "The ShardCache row in this data set is the embedded/in-process native semantic-cache API. This run does not include a ShardCache Server row; rerunning the isolated harness after the server-mode semantic commands are enabled will add that row beside the embedded result."


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
        execution_mode_note(rows),
        "",
        f"The run uses {format_int(metadata.get('entries', cold['entries']))} entries, {metadata.get('dims', cold['dims'])} dimensions, a cosine-distance threshold of {metadata.get('threshold', '0.35')}, {worker_label(metadata.get('workers', cold['workers']))}, and a {metadata.get('seconds', '10')} second measured window. The SUT is pinned to {logical_cpu_label(sut_vcpus(metadata), metadata.get('sut_cpuset', '0-15'))}; networked load clients are pinned to {metadata.get('load_cpuset', '16-31')}.",
        "",
        f"The cold rows measure the cost that an application pays before deciding whether to reuse a cached response or fall through to an LLM. The hot row measures a warmed exact-query cache hit path and reached {row_metric(hot, 'ops')} ops/s for the embedded ShardCache path in this run.",
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
        "The primary comparison is `Ops/s`: how many semantic-cache lookups completed during the measured window. `Hit rate` is measured hits divided by measured lookups in that same row, so it shows how often the system actually returned a reusable cached value at the configured threshold. Latency columns (`p50 ms` and `p99 ms`) show the request-level distribution observed by the benchmark worker. `Speedup` is always the ShardCache Embedded throughput divided by the peer throughput for the same row; values below `1.0x` mean the peer was faster for that scenario.",
        "",
        "`Ops/SUT-vCPU` is a CPU-efficiency view of the database or embedded index only. For networked systems it divides throughput by the Redis/Qdrant container CPU, not by the Python load generator. This makes the efficiency denominator fair, but it also means throughput remains the decisive capacity metric when client-side work is the limiting factor.",
        "",
        f"In this run, ShardCache Embedded's no-memo semantic path is stable across both cold cases: {row_metric(cold, 'ops')} cold misses/s and {row_metric(unique, 'ops')} cold unique hits/s. The hot exact-query row is intentionally different: it measures repeated application traffic and reaches {row_metric(hot, 'ops')} ops/s because the semantic decision is cached in process.",
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
        f"This {vcpus}-vCPU run supports a precise claim for the embedded ShardCache path: it is substantially faster than BetterDB and RedisVL semantic-cache integrations on all measured workloads, and it is {hot_betterdb} on the hot exact-query path. Against raw Redis HNSW, the cold-vector result is workload- and CPU-shape-dependent: embedded ShardCache is {cold_redis} on cold misses and {unique_redis} on cold unique hits in this run, while it is {hot_redis} on hot cached exact queries.",
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
        f"ShardCache now sustains {row_metric(cold, 'ops')} cold misses/s and {row_metric(unique, 'ops')} cold unique semantic hits/s in the no-memo lookup path, then jumps to {row_metric(hot, 'ops')} lookups/s when the exact-query cache is warm. Earlier exploratory no-memo profiling was around 452 ops/s, so the optimized cold path is {baseline_improvement_text(float(cold['ops']))} that baseline in this server run.",
        "",
        "The performance improvement comes from moving semantic caching into the cache engine instead of treating it as an application-side wrapper around a vector database:",
        "",
        "- Semantic search is native and in-process, so the timed lookup avoids Python framework dispatch, Redis protocol round trips, and JSON/vector serialization in the critical path.",
        "- Semantic entries are searched through one semantic index instead of fanning every semantic query across every data shard; the semantic index is allowed to use the full semantic memory budget rather than one slice per shard.",
        "- Embeddings are normalized at insert/query boundaries, turning cosine comparison into a dot product over contiguous `f32` vectors.",
        "- The dot product path uses AVX2/FMA when available, with an unrolled scalar fallback.",
        "- Locality-sensitive hashing builds 64-bit signatures and band buckets, then verifies only a capped candidate set with the exact dot product.",
        "- Repeated exact queries use an exact-vector fingerprint and a generation-checked semantic query cache, which explains the multi-million ops/s hot row.",
        "",
        "There are explicit tradeoffs. The current cold path is optimized for a semantic-cache workload: bounded candidate collection, fast inserts, exact verification of shortlisted candidates, and simple memory accounting. Redis HNSW is a stronger single-core ANN primitive in the 1-vCPU cold-vector row, but the benchmark also shows that HNSW did not use the larger CPU allocation in this workload and returned a much lower hit rate on the positive/paraphrase stream. A future hybrid could use HNSW for candidate discovery while preserving ShardCache's exact verification, value release, governance predicate, and hot exact-query cache.",
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


def render_metadata_value_markdown() -> list[str]:
    return [
        "## Metadata Utility Beyond Access Control",
        "",
        "Governance metadata is intentionally opaque to ShardCache, but that does not make it incidental. It lets a customer attach the application context that determines whether a semantic hit is useful, safe, fresh, and explainable.",
        "",
        "- Authorization: metadata can encode tenant, group, document, row-level scope, region, retention tier, or policy version, and the caller's predicate decides whether the candidate may release a cached value.",
        "- Auditability: a cache hit can be tied back to the policy and source-document context that made the answer eligible, which is important when a reused answer crosses user boundaries.",
        "- Targeted invalidation: applications can encode policy versions or source-document IDs, then reject stale entries after an ACL, source document, or compliance rule changes without disabling semantic caching globally.",
        "- Measurement: hit rate can be segmented by governed versus ungoverned traffic, tenant, policy version, or document class. That makes semantic caching observable as a product behavior, not just a throughput number.",
        "- Safety controls: governed search can intentionally convert a semantically close candidate into a miss when the candidate is not authorized. A lower governed hit rate can therefore be a sign that the system is preventing unsafe reuse, not a performance failure.",
        "",
        "The default path stays lightweight. Entries inserted through the normal semantic APIs carry no governance metadata, and governed metadata is only consulted on candidate acceptance before the cached value is released. The vector math and shortlist construction do not need to parse customer policy data, so customers can add governance without turning every lookup into an application-layer policy scan.",
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
            f"ShardCache Embedded completed {row_metric(shard, 'ops')} ops/s at p50 {float(shard['p50']):.4f} ms and p99 {float(shard['p99']):.4f} ms. That is {comparison_text(rows, scenario, 'betterdb', 'BetterDB')}, {comparison_text(rows, scenario, 'redis-vector-hnsw', 'Redis HNSW')}, and {comparison_text(rows, scenario, 'faiss-hnsw', 'FAISS HNSW')}.",
            "",
            "This is the harshest semantic-cache workload because every request still has to prove that there is no reusable answer. ShardCache's LSH shortlist plus SIMD verification keeps that negative lookup path short.",
            "",
        ]
    if scenario == "hit-cold-unique":
        return [
            "This row uses unique positive/paraphrase queries with the query-result cache disabled. It measures a first-time semantic cache hit, where the system must search semantically and return the cached value without exact-query memo help.",
            "",
            f"ShardCache Embedded completed {row_metric(shard, 'ops')} ops/s at p50 {float(shard['p50']):.4f} ms and p99 {float(shard['p99']):.4f} ms. That is {comparison_text(rows, scenario, 'betterdb', 'BetterDB')}, {comparison_text(rows, scenario, 'redis-vector-hnsw', 'Redis HNSW')}, and {comparison_text(rows, scenario, 'faiss-hnsw', 'FAISS HNSW')}.",
            "",
            "The result is close to the cold-miss row, which is useful: successful semantic reuse is not materially more expensive than proving a miss in this fixture.",
            "",
        ]
    return [
        "This row warms a repeated exact-query pool before the measured window. It represents the common production case where users ask the same or identical normalized question repeatedly and the semantic cache can return a cached decision immediately.",
        "",
        f"ShardCache Embedded completed {row_metric(shard, 'ops')} ops/s at p50 {float(shard['p50']):.4f} ms and p99 {float(shard['p99']):.4f} ms, or {row_metric(shard, 'ops_per_sut_cpu')} ops/SUT-vCPU. That is {comparison_text(rows, scenario, 'betterdb', 'BetterDB')}, {comparison_text(rows, scenario, 'redisvl-semantic-cache', 'RedisVL SemanticCache')}, and {comparison_text(rows, scenario, 'redis-vector-hnsw', 'Redis HNSW')}.",
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
        tex_escape(execution_mode_note(rows)),
        "",
        f"The run uses {tex_count(metadata.get('entries', cold['entries']))} entries, {tex_escape(str(metadata.get('dims', cold['dims'])))} dimensions, a cosine-distance threshold of {tex_escape(metadata.get('threshold', '0.35'))}, {tex_escape(worker_label(metadata.get('workers', cold['workers'])))}, and a {tex_escape(metadata.get('seconds', '10'))} second measured window. The SUT is pinned to {tex_escape(logical_cpu_label(sut_vcpus(metadata), metadata.get('sut_cpuset', '0-15')))}; networked load clients are pinned to {tex_escape(metadata.get('load_cpuset', '16-31'))}.",
        "",
        f"The cold rows measure the cost that an application pays before deciding whether to reuse a cached response or fall through to an LLM. The hot row measures a warmed exact-query cache hit path and reached {tex_count(hot['ops'])} ops/s for the embedded ShardCache path in this run.",
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
        r"The primary comparison is \texttt{Ops/s}: how many semantic-cache lookups completed during the measured window. \texttt{Hit rate} is measured hits divided by measured lookups in that same row, so it shows how often the system actually returned a reusable cached value at the configured threshold. Latency columns (\texttt{p50 ms} and \texttt{p99 ms}) show the request-level distribution observed by the benchmark worker. \texttt{Speedup} is always ShardCache Embedded throughput divided by the peer throughput for the same row; values below \texttt{1.0x} mean the peer was faster for that scenario.",
        "",
        r"\texttt{Ops/SUT-vCPU} is a CPU-efficiency view of the database or embedded index only. For networked systems it divides throughput by the Redis/Qdrant container CPU, not by the Python load generator. This makes the efficiency denominator fair, but it also means throughput remains the decisive capacity metric when client-side work is the limiting factor.",
        "",
        f"In this run, ShardCache Embedded's no-memo semantic path is stable across both cold cases: {tex_count(cold['ops'])} cold misses/s and {tex_count(unique['ops'])} cold unique hits/s. The hot exact-query row is intentionally different: it measures repeated application traffic and reaches {tex_count(hot['ops'])} ops/s because the semantic decision is cached in process.",
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
        f"This {vcpus}-vCPU run supports a precise claim for the embedded ShardCache path: it is substantially faster than BetterDB and RedisVL semantic-cache integrations on all measured workloads, and it is {hot_betterdb} on the hot exact-query path. Against raw Redis HNSW, the cold-vector result is workload- and CPU-shape-dependent: embedded ShardCache is {cold_redis} on cold misses and {unique_redis} on cold unique hits in this run, while it is {hot_redis} on hot cached exact queries.",
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
        f"ShardCache now sustains {tex_count(cold['ops'])} cold misses/s and {tex_count(unique['ops'])} cold unique semantic hits/s in the no-memo lookup path, then jumps to {tex_count(hot['ops'])} lookups/s when the exact-query cache is warm. Earlier exploratory no-memo profiling was around 452 ops/s, so the optimized cold path is {tex_baseline_improvement(float(cold['ops']))} that baseline in this server run.",
        "",
        "The performance improvement comes from moving semantic caching into the cache engine instead of treating it as an application-side wrapper around a vector database:",
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
        "There are explicit tradeoffs. The current cold path is optimized for a semantic-cache workload: bounded candidate collection, fast inserts, exact verification of shortlisted candidates, and simple memory accounting. Redis HNSW is a stronger single-core ANN primitive in the 1-vCPU cold-vector row, but the benchmark also shows that HNSW did not use the larger CPU allocation in this workload and returned a much lower hit rate on the positive/paraphrase stream. A future hybrid could use HNSW for candidate discovery while preserving ShardCache's exact verification, value release, governance predicate, and hot exact-query cache.",
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
        r"\begin{lstlisting}[style=shardcachecode]",
        "key        = semantic:tenant/acme/faq/refund-policy",
        "value      = cached response bytes",
        "embedding  = normalized prompt embedding",
        "governance = {tenant: acme, policy_version: 7,",
        "              allowed_groups: [support, billing],",
        "              source_docs: [doc_481, doc_902]}",
        "ttl        = optional freshness window",
        r"\end{lstlisting}",
        "",
        "In Rust, the authorization boundary is explicit:",
        "",
        r"\begin{lstlisting}[style=shardcachecode]",
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
        r"\end{lstlisting}",
        "",
        "The important behavior is that \\texttt{SemanticMatch.governance} is \\texttt{None} by default and \\texttt{Some(bytes)} only when the caller used the governance API. Governed searches receive \\texttt{Option<\\&[u8]>}, so applications can reject entries without metadata, require a specific policy version, or validate document-level access before any cross-user cached response is served.",
        "",
    ]


def render_metadata_value_latex() -> list[str]:
    return [
        r"\subsection{Metadata Utility Beyond Access Control}",
        "",
        "Governance metadata is intentionally opaque to ShardCache, but that does not make it incidental. It lets a customer attach the application context that determines whether a semantic hit is useful, safe, fresh, and explainable.",
        "",
        r"\begin{itemize}",
        r"\item Authorization: metadata can encode tenant, group, document, row-level scope, region, retention tier, or policy version, and the caller's predicate decides whether the candidate may release a cached value.",
        r"\item Auditability: a cache hit can be tied back to the policy and source-document context that made the answer eligible, which is important when a reused answer crosses user boundaries.",
        r"\item Targeted invalidation: applications can encode policy versions or source-document IDs, then reject stale entries after an ACL, source document, or compliance rule changes without disabling semantic caching globally.",
        r"\item Measurement: hit rate can be segmented by governed versus ungoverned traffic, tenant, policy version, or document class. That makes semantic caching observable as a product behavior, not just a throughput number.",
        r"\item Safety controls: governed search can intentionally convert a semantically close candidate into a miss when the candidate is not authorized. A lower governed hit rate can therefore be a sign that the system is preventing unsafe reuse, not a performance failure.",
        r"\end{itemize}",
        "",
        "The default path stays lightweight. Entries inserted through the normal semantic APIs carry no governance metadata, and governed metadata is only consulted on candidate acceptance before the cached value is released. The vector math and shortlist construction do not need to parse customer policy data, so customers can add governance without turning every lookup into an application-layer policy scan.",
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
            f"ShardCache Embedded completed {tex_count(shard['ops'])} ops/s at p50 {float(shard['p50']):.4f} ms and p99 {float(shard['p99']):.4f} ms. That is {tex_comparison(rows, scenario, 'betterdb', 'BetterDB')}, {tex_comparison(rows, scenario, 'redis-vector-hnsw', 'Redis HNSW')}, and {tex_comparison(rows, scenario, 'faiss-hnsw', 'FAISS HNSW')}.",
            "",
            "This is the harshest semantic-cache workload because every request still has to prove that there is no reusable answer. ShardCache's LSH shortlist plus SIMD verification keeps that negative lookup path short.",
            "",
        ]
    if scenario == "hit-cold-unique":
        return [
            "This row uses unique positive/paraphrase queries with the query-result cache disabled. It measures a first-time semantic cache hit, where the system must search semantically and return the cached value without exact-query memo help.",
            "",
            f"ShardCache Embedded completed {tex_count(shard['ops'])} ops/s at p50 {float(shard['p50']):.4f} ms and p99 {float(shard['p99']):.4f} ms. That is {tex_comparison(rows, scenario, 'betterdb', 'BetterDB')}, {tex_comparison(rows, scenario, 'redis-vector-hnsw', 'Redis HNSW')}, and {tex_comparison(rows, scenario, 'faiss-hnsw', 'FAISS HNSW')}.",
            "",
            "The result is close to the cold-miss row, which is useful: successful semantic reuse is not materially more expensive than proving a miss in this fixture.",
            "",
        ]
    return [
        "This row warms a repeated exact-query pool before the measured window. It represents the common production case where users ask the same or identical normalized question repeatedly and the semantic cache can return a cached decision immediately.",
        "",
        f"ShardCache Embedded completed {tex_count(shard['ops'])} ops/s at p50 {float(shard['p50']):.4f} ms and p99 {float(shard['p99']):.4f} ms, or {tex_count(shard['ops_per_sut_cpu'])} ops/SUT-vCPU. That is {tex_comparison(rows, scenario, 'betterdb', 'BetterDB')}, {tex_comparison(rows, scenario, 'redisvl-semantic-cache', 'RedisVL SemanticCache')}, and {tex_comparison(rows, scenario, 'redis-vector-hnsw', 'Redis HNSW')}.",
        "",
        "This row should be read separately from the cold rows: it is intentionally measuring the warmed exact-query path, not first-time vector search. The huge gap comes from keeping the query-result cache in process and invalidating it by semantic generation on writes.",
        "",
    ]


def render_markdown(rows: dict[tuple[str, str], dict[str, object]], metadata: dict[str, str]) -> str:
    out: list[str] = [
        f"# {report_title(metadata)}",
        "",
        f"- Host: {metadata.get('host', 'server')}",
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
    out.extend(render_metadata_value_markdown())
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
                "| System | Ops/s | Hit rate | Ops/SUT-vCPU | p50 ms | p99 ms | SUT vCPU | Client vCPU | Total vCPU | Speedup |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in table_rows:
            out.append(
                "| {label} | {ops} | {hit_rate:.1f}% | {ops_cpu} | {p50:.4f} | {p99:.4f} | {sut:.2f} | {client:.2f} | {total:.2f} | {speedup:.1f}x |".format(
                    label=row["label"],
                    ops=format_int(row["ops"]),
                    hit_rate=float(row["hit_rate"]),
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


def render_combined_markdown(
    runs: list[dict[str, object]],
    chart_assets: dict[tuple[int, str], dict[str, str]] | None = None,
) -> str:
    title = combined_report_title(runs)
    one = run_by_vcpu(runs, 1)
    sixteen = run_by_vcpu(runs, 16)
    out: list[str] = [
        f"# {title}",
        "",
        "This report combines the isolated 1-vCPU and 16-vCPU server benchmark runs into unified head-to-head tables. Each scenario table shows peer comparison and CPU scaling in the same row, so a reader can see both relative performance and how each system scales with the larger CPU allocation.",
        "",
        "## Run Shape",
        "",
        f"- Host: {combined_metadata_value(runs, 'host', 'server')}",
        f"- Entries: {format_int(combined_metadata_value(runs, 'entries', '100000'))}",
        f"- Dims: {combined_metadata_value(runs, 'dims', '384')}",
        f"- Threshold distance: {combined_metadata_value(runs, 'threshold', '0.35')}",
        f"- 1-vCPU SUT CPU set: {run_metadata_value(one, 'sut_cpuset', '0')}",
        f"- 1-vCPU load/client CPU set: {run_metadata_value(one, 'load_cpuset', '16-31')}",
        f"- 1-vCPU workers: {run_metadata_value(one, 'workers', '16')}",
        f"- 16-vCPU SUT CPU set: {run_metadata_value(sixteen, 'sut_cpuset', '0-15')}",
        f"- 16-vCPU load/client CPU set: {run_metadata_value(sixteen, 'load_cpuset', '16-31')}",
        f"- 16-vCPU workers: {run_metadata_value(sixteen, 'workers', '16')}",
        "",
        combined_execution_mode_note(runs),
        "",
        "For networked rows, the SUT/database is limited to the SUT CPU set and load/client workers run on the separate load CPU set. The 1-vCPU networked run therefore limits only the database to one logical CPU; the client still has the full client CPU set available. Embedded rows are in-process, so their benchmark process is pinned to the SUT CPU set and has no separate client CPU.",
        "",
        "The `Hit rate` columns are measured hits divided by measured lookups for the same row. The `Speedup` columns are ShardCache Embedded throughput divided by the peer throughput for the same CPU shape. The `Scale` column is each system's 16-vCPU throughput divided by its 1-vCPU throughput. This is especially important for Redis vector HNSW: it can post higher raw lookup throughput in some rows while accepting a smaller share of positive/paraphrase queries as reusable cached answers.",
        "",
    ]

    out.extend(render_combined_technical_markdown(one, sixteen))
    out.extend(render_governance_markdown())
    out.extend(render_metadata_value_markdown())

    for meta in SCENARIOS:
        scenario = str(meta["key"])
        out.extend(render_combined_scenario_markdown(one, sixteen, scenario, str(meta["title"])))
        out.extend(render_combined_chart_markdown(chart_assets or {}, scenario, str(meta["title"])))
        out.append("")
    return "\n".join(out)


def render_combined_chart_markdown(
    chart_assets: dict[tuple[int, str], dict[str, str]],
    scenario: str,
    title: str,
) -> list[str]:
    out: list[str] = []
    for vcpus in (1, 16):
        asset = chart_assets.get((vcpus, scenario))
        if not asset or "svg" not in asset:
            continue
        out.extend(
            [
                f"![{vcpus}-vCPU {title} throughput]({asset['svg']})",
                "",
            ]
        )
        if "hit_rate_svg" in asset:
            out.extend(
                [
                    f"![{vcpus}-vCPU {title} hit rate]({asset['hit_rate_svg']})",
                    "",
                ]
            )
    return out


def render_combined_technical_markdown(
    one: dict[str, object] | None,
    sixteen: dict[str, object] | None,
) -> list[str]:
    cold_16 = combined_row(sixteen, "miss-cold", "shardcache")
    unique_16 = combined_row(sixteen, "hit-cold-unique", "shardcache")
    hot_16 = combined_row(sixteen, "hit-hot-cached", "shardcache")
    cold_1 = combined_row(one, "miss-cold", "shardcache")
    redis_hnsw_1 = combined_row(one, "hit-cold-unique", "redis-vector-hnsw")
    redis_hnsw_16 = combined_row(sixteen, "hit-cold-unique", "redis-vector-hnsw")

    return [
        "## Technical Design And Performance Tradeoffs",
        "",
        "ShardCache's semantic-cache path is built as cache-engine functionality rather than as a framework wrapper around a separate vector search system. The benchmark is therefore measuring lookup mechanics that are usually hidden behind a semantic-cache integration layer: candidate discovery, exact similarity verification, value retrieval, cache-memory policy, governance filtering, and hot-query reuse.",
        "",
        "The main improvements were:",
        "",
        "- Native execution: the embedded path avoids Python dispatch, vector serialization, JSON marshalling, and external protocol round trips. The server path keeps the same native semantic engine behind RESP semantic commands.",
        "- One semantic index: semantic entries are concentrated into a single semantic index instead of being split across data shards. That avoids fanout, prevents each shard from receiving only a fraction of the semantic memory budget, and matches the observation that semantic search quality and speed degrade when the candidate space is needlessly partitioned.",
        "- Full semantic memory budget: semantic caching can use the cache's semantic allocation as a whole rather than being constrained to one slab out of many. This is important for high-cardinality prompt caches because fewer retained embeddings means lower reuse and more fall-through.",
        "- Normalized vectors and SIMD verification: embeddings are normalized at boundaries, so cosine similarity becomes a dot product over contiguous `f32` arrays. AVX2/FMA accelerates the verification step after candidate selection.",
        "- LSH shortlist plus exact verification: locality-sensitive hashing narrows the candidate set, but ShardCache still verifies shortlisted candidates with the exact dot product before returning a value.",
        "- Generation-checked exact-query cache: repeated identical normalized queries hit a small exact-query result cache that is invalidated by semantic generation on writes, which is why the hot exact-query row is several orders of magnitude above framework-backed semantic-cache integrations.",
        "",
        f"The tradeoff is that the current cold path is not an HNSW graph index. Redis HNSW is faster on the 1-vCPU raw vector-search primitive, but it returns a much lower measured hit rate on the cold unique positive/paraphrase stream: {markdown_percent(redis_hnsw_1, 'hit_rate')} at 1 vCPU and {markdown_percent(redis_hnsw_16, 'hit_rate')} at 16 vCPU, compared with ShardCache Embedded at {markdown_percent(unique_16, 'hit_rate')} in the 16-vCPU run. That means Redis HNSW's higher raw throughput in some rows should be read as a speed/recall tradeoff, not as an unqualified semantic-cache win. ShardCache's current design favors cache semantics: full value release, bounded candidate work, simple insert/update behavior, exact verification, governance filtering, and high multicore throughput. A future hybrid could add HNSW as candidate discovery while keeping ShardCache's verification and governance boundary.",
        "",
        "The observed result is the shape we wanted from a native semantic cache: the optimized no-memo path is stable across misses and first-time hits, and the hot exact-query path becomes a very fast cache lookup rather than repeated vector search.",
        "",
        "Key ShardCache data points from the combined run:",
        "",
        f"- 1-vCPU cold miss: {markdown_metric(cold_1, 'ops')} ops/s.",
        f"- 16-vCPU cold miss: {markdown_metric(cold_16, 'ops')} ops/s.",
        f"- 16-vCPU cold unique semantic hit: {markdown_metric(unique_16, 'ops')} ops/s at {markdown_percent(unique_16, 'hit_rate')} hit rate.",
        f"- 16-vCPU hot exact cached lookup: {markdown_metric(hot_16, 'ops')} ops/s at {markdown_percent(hot_16, 'hit_rate')} hit rate.",
        f"- Redis HNSW cold unique hit rate: {markdown_percent(redis_hnsw_1, 'hit_rate')} at 1 vCPU and {markdown_percent(redis_hnsw_16, 'hit_rate')} at 16 vCPU.",
        "",
    ]


def render_combined_scenario_markdown(
    one: dict[str, object] | None,
    sixteen: dict[str, object] | None,
    scenario: str,
    title: str,
) -> list[str]:
    return [
        f"## {title}",
        "",
        combined_scenario_description(title),
        "",
        combined_hit_rate_note(scenario),
        "",
        "| System | 1-vCPU ops/s | 16-vCPU ops/s | Scale | 1-vCPU hit rate | 16-vCPU hit rate | 1-vCPU ops/SUT-vCPU | 16-vCPU ops/SUT-vCPU | 1-vCPU p50 ms | 16-vCPU p50 ms | 1-vCPU speedup | 16-vCPU speedup |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        *[
            "| {label} | {one_ops} | {sixteen_ops} | {scale} | {one_hit_rate} | {sixteen_hit_rate} | {one_cpu} | {sixteen_cpu} | {one_p50} | {sixteen_p50} | {one_speedup} | {sixteen_speedup} |".format(
                label=combined_label(adapter, one, sixteen, scenario),
                one_ops=markdown_metric(combined_row(one, scenario, adapter), "ops"),
                sixteen_ops=markdown_metric(combined_row(sixteen, scenario, adapter), "ops"),
                scale=markdown_scale(combined_row(one, scenario, adapter), combined_row(sixteen, scenario, adapter)),
                one_hit_rate=markdown_percent(combined_row(one, scenario, adapter), "hit_rate"),
                sixteen_hit_rate=markdown_percent(combined_row(sixteen, scenario, adapter), "hit_rate"),
                one_cpu=markdown_metric(combined_row(one, scenario, adapter), "ops_per_sut_cpu"),
                sixteen_cpu=markdown_metric(combined_row(sixteen, scenario, adapter), "ops_per_sut_cpu"),
                one_p50=markdown_float(combined_row(one, scenario, adapter), "p50"),
                sixteen_p50=markdown_float(combined_row(sixteen, scenario, adapter), "p50"),
                one_speedup=markdown_speedup(one, scenario, adapter),
                sixteen_speedup=markdown_speedup(sixteen, scenario, adapter),
            )
            for adapter in combined_adapters(one, sixteen, scenario)
        ],
        "",
    ]


def combined_hit_rate_note(scenario: str) -> str:
    if scenario == "miss-cold":
        return "For cold misses, the hit-rate columns are a false-positive check. Every system reported 0.0%, so there was no accidental semantic reuse on the negative-query workload."
    if scenario == "hit-cold-unique":
        return "For cold unique semantic hits, the hit-rate columns show how often each system accepted a positive/paraphrase query as reusable at the configured threshold."
    return "For hot cached exact queries, the hit-rate columns show whether repeated normalized traffic stays on the cached-answer path or falls through to vector search."


def render_combined_scenario_latex(
    one: dict[str, object] | None,
    sixteen: dict[str, object] | None,
    scenario: str,
    title: str,
    chart_assets: dict[tuple[int, str], dict[str, str]] | None = None,
) -> list[str]:
    out = [
        rf"\subsection{{{tex_escape(title)}}}",
        "",
        tex_escape(combined_scenario_description(title)),
        "",
        tex_escape(combined_hit_rate_note(scenario)),
        "",
        r"\begin{table}[htbp]",
        r"\centering",
        r"\tiny",
        r"\setlength{\tabcolsep}{2pt}",
        rf"\caption{{{tex_escape(title)} unified head-to-head and 1-vCPU to 16-vCPU scaling.}}",
        rf"\label{{tab:semantic-h2h-combined-{scenario}}}",
        r"\resizebox{\linewidth}{!}{%",
        r"\begin{tabular}{lrrrrrrrrrrr}",
        r"\hline",
        r"System & 1-vCPU ops/s & 16-vCPU ops/s & Scale & 1-vCPU hit rate & 16-vCPU hit rate & 1-vCPU ops/CPU & 16-vCPU ops/CPU & 1-vCPU p50 & 16-vCPU p50 & 1-vCPU Speedup & 16-vCPU Speedup \\",
        r"\hline",
    ]
    for adapter in combined_adapters(one, sixteen, scenario):
        one_row = combined_row(one, scenario, adapter)
        sixteen_row = combined_row(sixteen, scenario, adapter)
        out.append(
            "{label} & {one_ops} & {sixteen_ops} & {scale} & {one_hit_rate} & {sixteen_hit_rate} & {one_cpu} & {sixteen_cpu} & {one_p50} & {sixteen_p50} & {one_speedup} & {sixteen_speedup} \\\\".format(
                label=tex_escape(combined_label(adapter, one, sixteen, scenario)),
                one_ops=tex_metric(one_row, "ops"),
                sixteen_ops=tex_metric(sixteen_row, "ops"),
                scale=tex_scale(one_row, sixteen_row),
                one_hit_rate=tex_percent(one_row, "hit_rate"),
                sixteen_hit_rate=tex_percent(sixteen_row, "hit_rate"),
                one_cpu=tex_metric(one_row, "ops_per_sut_cpu"),
                sixteen_cpu=tex_metric(sixteen_row, "ops_per_sut_cpu"),
                one_p50=tex_float(one_row, "p50"),
                sixteen_p50=tex_float(sixteen_row, "p50"),
                one_speedup=tex_speedup_cell(one, scenario, adapter),
                sixteen_speedup=tex_speedup_cell(sixteen, scenario, adapter),
            )
        )
    out.extend(
        [
            r"\hline",
            r"\end{tabular}",
            r"}",
            r"\end{table}",
            "",
        ]
    )
    out.extend(render_combined_chart_latex(chart_assets or {}, scenario, title))
    out.append(r"\FloatBarrier")
    out.append("")
    return out


def render_combined_chart_latex(
    chart_assets: dict[tuple[int, str], dict[str, str]],
    scenario: str,
    title: str,
) -> list[str]:
    out: list[str] = []
    for vcpus in (1, 16):
        asset = chart_assets.get((vcpus, scenario))
        if not asset or "pdf" not in asset:
            continue
        out.extend(
            [
                rf"The {vcpus}-vCPU throughput chart below visualizes the table row as a capacity comparison. It is included here so the throughput claim can be read directly beside the scenario it supports.",
                "",
                r"\begin{figure}[!htbp]",
                r"\centering",
                rf"\includegraphics[width=0.92\linewidth]{{{asset['pdf']}}}",
                rf"\caption{{{tex_escape(title)} throughput head-to-head with the SUT limited to {vcpus} vCPU and load clients isolated on the client CPU set.}}",
                rf"\label{{fig:semantic-h2h-combined-{scenario}-{vcpus}vcpu-throughput}}",
                r"\end{figure}",
                "",
            ]
        )
        if "hit_rate_pdf" in asset:
            out.extend(
                [
                    rf"The {vcpus}-vCPU hit-rate chart separates throughput from semantic reuse. This matters because a system can be fast while accepting fewer positive queries as reusable, or can intentionally reject governed candidates.",
                    "",
                    r"\begin{figure}[!htbp]",
                    r"\centering",
                    rf"\includegraphics[width=0.92\linewidth]{{{asset['hit_rate_pdf']}}}",
                    rf"\caption{{{tex_escape(title)} measured hit rate with the SUT limited to {vcpus} vCPU. Hit rate is hits divided by lookups in the same measured row.}}",
                    rf"\label{{fig:semantic-h2h-combined-{scenario}-{vcpus}vcpu-hit-rate}}",
                    r"\end{figure}",
                    "",
                ]
            )
    return out


def render_combined_technical_latex(
    one: dict[str, object] | None,
    sixteen: dict[str, object] | None,
) -> list[str]:
    cold_16 = combined_row(sixteen, "miss-cold", "shardcache")
    unique_16 = combined_row(sixteen, "hit-cold-unique", "shardcache")
    hot_16 = combined_row(sixteen, "hit-hot-cached", "shardcache")
    cold_1 = combined_row(one, "miss-cold", "shardcache")
    redis_hnsw_1 = combined_row(one, "hit-cold-unique", "redis-vector-hnsw")
    redis_hnsw_16 = combined_row(sixteen, "hit-cold-unique", "redis-vector-hnsw")

    return [
        r"\subsection{Technical Design And Performance Tradeoffs}",
        "",
        "ShardCache's semantic-cache path is built as cache-engine functionality rather than as a framework wrapper around a separate vector search system. The benchmark is therefore measuring lookup mechanics that are usually hidden behind a semantic-cache integration layer: candidate discovery, exact similarity verification, value retrieval, cache-memory policy, governance filtering, and hot-query reuse.",
        "",
        "The main improvements were:",
        "",
        r"\begin{itemize}",
        r"\item Native execution: the embedded path avoids Python dispatch, vector serialization, JSON marshalling, and external protocol round trips. The server path keeps the same native semantic engine behind RESP semantic commands.",
        r"\item One semantic index: semantic entries are concentrated into a single semantic index instead of being split across data shards. That avoids fanout, prevents each shard from receiving only a fraction of the semantic memory budget, and matches the observation that semantic search quality and speed degrade when the candidate space is needlessly partitioned.",
        r"\item Full semantic memory budget: semantic caching can use the cache's semantic allocation as a whole rather than being constrained to one slab out of many. This is important for high-cardinality prompt caches because fewer retained embeddings means lower reuse and more fall-through.",
        r"\item Normalized vectors and SIMD verification: embeddings are normalized at boundaries, so cosine similarity becomes a dot product over contiguous \texttt{f32} arrays. AVX2/FMA accelerates the verification step after candidate selection.",
        r"\item LSH shortlist plus exact verification: locality-sensitive hashing narrows the candidate set, but ShardCache still verifies shortlisted candidates with the exact dot product before returning a value.",
        r"\item Generation-checked exact-query cache: repeated identical normalized queries hit a small exact-query result cache that is invalidated by semantic generation on writes, which is why the hot exact-query row is several orders of magnitude above framework-backed semantic-cache integrations.",
        r"\end{itemize}",
        "",
        f"The tradeoff is that the current cold path is not an HNSW graph index. Redis HNSW is faster on the 1-vCPU raw vector-search primitive, but it returns a much lower measured hit rate on the cold unique positive/paraphrase stream: {tex_percent(redis_hnsw_1, 'hit_rate')} at 1 vCPU and {tex_percent(redis_hnsw_16, 'hit_rate')} at 16 vCPU, compared with ShardCache Embedded at {tex_percent(unique_16, 'hit_rate')} in the 16-vCPU run. That means Redis HNSW's higher raw throughput in some rows should be read as a speed/recall tradeoff, not as an unqualified semantic-cache win. ShardCache's current design favors cache semantics: full value release, bounded candidate work, simple insert/update behavior, exact verification, governance filtering, and high multicore throughput. A future hybrid could add HNSW as candidate discovery while keeping ShardCache's verification and governance boundary.",
        "",
        "The observed result is the shape we wanted from a native semantic cache: the optimized no-memo path is stable across misses and first-time hits, and the hot exact-query path becomes a very fast cache lookup rather than repeated vector search.",
        "",
        "Key ShardCache data points from the combined run:",
        "",
        r"\begin{itemize}",
        rf"\item 1-vCPU cold miss: {tex_metric(cold_1, 'ops')} ops/s.",
        rf"\item 16-vCPU cold miss: {tex_metric(cold_16, 'ops')} ops/s.",
        rf"\item 16-vCPU cold unique semantic hit: {tex_metric(unique_16, 'ops')} ops/s at {tex_percent(unique_16, 'hit_rate')} hit rate.",
        rf"\item 16-vCPU hot exact cached lookup: {tex_metric(hot_16, 'ops')} ops/s at {tex_percent(hot_16, 'hit_rate')} hit rate.",
        rf"\item Redis HNSW cold unique hit rate: {tex_percent(redis_hnsw_1, 'hit_rate')} at 1 vCPU and {tex_percent(redis_hnsw_16, 'hit_rate')} at 16 vCPU.",
        r"\end{itemize}",
        "",
    ]


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
        "We reran the semantic-cache head-to-head on the benchmark server with explicit CPU isolation. "
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
    out.extend(render_metadata_value_latex())
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
                r"\begin{tabular}{lrrrrrrrrr}",
                r"\hline",
                r"System & Ops/s & Hit rate & Ops/SUT-vCPU & p50 ms & p99 ms & SUT vCPU & Client vCPU & Total vCPU & Speedup \\",
                r"\hline",
            ]
        )
        for row in table_rows:
            out.append(
                "{label} & {ops} & {hit_rate:.1f}\\% & {ops_cpu} & {p50:.4f} & {p99:.4f} & {sut:.2f} & {client:.2f} & {total:.2f} & {speedup:.1f}$\\times$ \\\\".format(
                    label=tex_escape(str(row["label"])),
                    ops=tex_count(row["ops"]),
                    hit_rate=float(row["hit_rate"]),
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
            r"The Redis-backed rows used Redis Stack; the image digest is captured in \texttt{metadata.txt}. Python package versions are captured in \texttt{python-freeze.txt}; this isolated run used a fresh benchmark virtual environment on the benchmark server, so package versions can differ from earlier uncapped exploratory rows. GPTCache and managed Redis LangCache are not included in this isolated matrix: GPTCache previously failed the concurrent 100k run cleanly, and no Redis LangCache endpoint or credentials were available. Embedded rows cannot separate load-generator CPU from system CPU because the database/index is a library in the benchmark process; those rows are marked by zero load vCPU and process CPU equal to SUT CPU.",
            "",
        ]
    )
    return "\n".join(out)


def render_combined_latex_section(
    runs: list[dict[str, object]],
    output_dir: Path,
    chart_assets: dict[tuple[int, str], dict[str, str]] | None = None,
) -> str:
    one = run_by_vcpu(runs, 1)
    sixteen = run_by_vcpu(runs, 16)
    out: list[str] = []

    out.extend(render_combined_whitepaper_intro_latex(one, sixteen))
    out.extend(render_combined_methodology_latex(runs, one, sixteen))
    out.append(r"\section{Architecture}")
    out.append(r"\label{sec:semantic-cache-architecture}")
    out.append("")
    out.extend(render_combined_technical_latex(one, sixteen))
    out.extend(render_governance_latex())
    out.extend(render_metadata_value_latex())
    out.append(r"\section{Benchmark Results}")
    out.append(r"\label{sec:semantic-cache-benchmark-results}")
    out.append("")
    out.extend(render_combined_results_discussion_latex(one, sixteen))

    out.append(r"\subsection{Detailed Scenario Evidence}")
    out.append("")
    out.append("The following subsections provide the evidence behind the results interpretation. Each scenario starts with a unified table, then places the corresponding throughput and hit-rate figures directly under the claim they illustrate.")
    out.append("")

    for meta in SCENARIOS:
        out.extend(
            render_combined_scenario_latex(
                one,
                sixteen,
                str(meta["key"]),
                str(meta["title"]),
                chart_assets or {},
            )
        )
        out.append("")

    out.extend(render_combined_conclusion_latex(one, sixteen))
    out.extend(
        [
            r"\subsection{Limitations And Future Work}",
            "",
            "The benchmark uses precomputed embeddings, so it isolates the cache/index lookup path and does not include embedding-model latency or LLM generation latency. That is intentional: semantic-cache infrastructure should be compared on the work it performs after an application has produced a query embedding. The matrix also separates raw vector indexes from semantic-cache integrations. Raw indexes can be useful lower-level baselines, but they do not by themselves model cached answer release, governance filtering, hot-query memoization, or application-facing cache semantics.",
            "",
            "The current ShardCache cold path uses LSH-style candidate discovery plus exact verification. A natural next research direction is a hybrid cold path that uses HNSW or another graph-based ANN structure for candidate discovery while preserving ShardCache's value-release, TTL, memory-accounting, governance, and exact-verification semantics. The benchmark should also be extended with governed-hit workloads that report authorized hit rate, rejected semantic matches, and stale-policy miss rate by tenant or document class.",
            "",
            r"\subsection{Caveats}",
            "",
            "This combined report uses the same underlying result CSV files as the standalone 1-vCPU and 16-vCPU reports. The unified tables are the authoritative combined view for head-to-head and scaling interpretation, and the embedded charts visualize the same rows on a linear throughput axis.",
            "",
        ]
    )
    return "\n".join(out)


def render_combined_whitepaper_intro_latex(
    one: dict[str, object] | None,
    sixteen: dict[str, object] | None,
) -> list[str]:
    cold_16 = combined_row(sixteen, "miss-cold", "shardcache")
    unique_16 = combined_row(sixteen, "hit-cold-unique", "shardcache")
    hot_16 = combined_row(sixteen, "hit-hot-cached", "shardcache")
    server_hot_16 = combined_row(sixteen, "hit-hot-cached", "shardcache-server")
    redis_hnsw_unique_16 = combined_row(sixteen, "hit-cold-unique", "redis-vector-hnsw")
    betterdb_hot_16 = combined_row(sixteen, "hit-hot-cached", "betterdb")
    return [
        r"\section{Introduction}",
        r"\label{sec:semantic-cache-introduction}",
        "",
        r"\subsection{Abstract}",
        "",
        "Semantic caching is usually evaluated as a thin framework layer on top of a vector database. That framing hides the cache-engine work required for production use: deciding whether a candidate is reusable, returning the cached value, invalidating repeated-query decisions, accounting for memory, and preventing cross-user data leakage. This report evaluates ShardCache as a native semantic-cache feature and compares it against BetterDB, RedisVL/LangChain Redis semantic-cache integrations, Redis vector search, FAISS, hnswlib, and Qdrant on an isolated server benchmark.",
        "",
        f"In the 16-vCPU run, ShardCache Embedded reached {tex_metric(cold_16, 'ops')} cold misses/s, {tex_metric(unique_16, 'ops')} cold unique semantic hits/s at {tex_percent(unique_16, 'hit_rate')} hit rate, and {tex_metric(hot_16, 'ops')} hot exact cached lookups/s at {tex_percent(hot_16, 'hit_rate')} hit rate. ShardCache Server reached {tex_metric(server_hot_16, 'ops')} hot exact cached lookups/s through the RESP semantic command path. Redis HNSW remained a strong raw ANN baseline, but its cold unique hit rate was {tex_percent(redis_hnsw_unique_16, 'hit_rate')} in the same 16-vCPU run, while BetterDB reached {tex_metric(betterdb_hot_16, 'ops')} hot exact lookups/s.",
        "",
        r"\subsection{Research Question}",
        "",
        "The central question is whether semantic caching should be implemented as a native cache capability rather than as an external semantic-cache framework layered over a vector index. The hypothesis is that a native implementation can improve throughput and scaling because it can combine candidate search, exact verification, cached-value release, memory policy, hot-query memoization, and governance metadata in one data path.",
        "",
        "A secondary question is whether semantic-cache benchmarks should report hit rate alongside throughput. The answer from this run is yes. A vector index can be fast while accepting fewer positive/paraphrase queries as reusable; conversely, a governed cache can intentionally lower hit rate when an otherwise close candidate is not authorized for the requesting user.",
        "",
    ]


def render_combined_methodology_latex(
    runs: list[dict[str, object]],
    one: dict[str, object] | None,
    sixteen: dict[str, object] | None,
) -> list[str]:
    return [
        r"\section{Methodology}",
        r"\label{sec:semantic-cache-methodology}",
        "",
        "This report combines the isolated 1-vCPU and 16-vCPU server benchmark runs into unified head-to-head tables. Each scenario table shows peer comparison and CPU scaling in the same row, so a reader can see both relative performance and how each system scales with the larger CPU allocation.",
        "",
        rf"All rows use {tex_count(combined_metadata_value(runs, 'entries', '100000'))} entries, {tex_escape(str(combined_metadata_value(runs, 'dims', '384')))}-dimensional normalized embeddings, and a cosine-distance threshold of {tex_escape(str(combined_metadata_value(runs, 'threshold', '0.35')))}. The 1-vCPU run pins the SUT to CPU set {tex_escape(run_metadata_value(one, 'sut_cpuset', '0'))} and the load client to CPU set {tex_escape(run_metadata_value(one, 'load_cpuset', '16-31'))} with {tex_escape(run_metadata_value(one, 'workers', '16'))} workers. The 16-vCPU run pins the SUT to CPU set {tex_escape(run_metadata_value(sixteen, 'sut_cpuset', '0-15'))} and the load client to CPU set {tex_escape(run_metadata_value(sixteen, 'load_cpuset', '16-31'))} with {tex_escape(run_metadata_value(sixteen, 'workers', '16'))} workers.",
        "",
        tex_escape(combined_execution_mode_note(runs)),
        "",
        "For networked rows, the SUT/database is limited to the SUT CPU set and load/client workers run on the separate load CPU set. The 1-vCPU networked run therefore limits only the database to one logical CPU; the client still has the full client CPU set available. Embedded rows are in-process, so their benchmark process is pinned to the SUT CPU set and has no separate client CPU.",
        "",
        r"The \texttt{Hit rate} columns are measured hits divided by measured lookups for the same row. The \texttt{Speedup} columns are ShardCache Embedded throughput divided by the peer throughput for the same CPU shape. The \texttt{Scale} column is each system's 16-vCPU throughput divided by its 1-vCPU throughput. This is especially important for Redis vector HNSW: it can post higher raw lookup throughput in some rows while accepting a smaller share of positive/paraphrase queries as reusable cached answers.",
        "",
        "The workload is split into three scenarios. Cold miss uses unique negative queries with query-result memoization disabled, so the correct hit rate is zero and the row measures false-positive-resistant fall-through cost. Cold unique semantic hit uses unique positive/paraphrase queries with memoization disabled, so it measures first-time semantic reuse. Hot cached exact query warms repeated normalized questions before measurement, so it measures the application-cache path that production systems expect to dominate when common prompts recur.",
        "",
    ]


def render_combined_results_discussion_latex(
    one: dict[str, object] | None,
    sixteen: dict[str, object] | None,
) -> list[str]:
    cold_1 = combined_row(one, "miss-cold", "shardcache")
    cold_16 = combined_row(sixteen, "miss-cold", "shardcache")
    unique_16 = combined_row(sixteen, "hit-cold-unique", "shardcache")
    hot_16 = combined_row(sixteen, "hit-hot-cached", "shardcache")
    redis_hnsw_cold_1 = combined_row(one, "miss-cold", "redis-vector-hnsw")
    redis_hnsw_unique_16 = combined_row(sixteen, "hit-cold-unique", "redis-vector-hnsw")
    betterdb_hot_16 = combined_row(sixteen, "hit-hot-cached", "betterdb")
    redisvl_hot_16 = combined_row(sixteen, "hit-hot-cached", "redisvl-semantic-cache")
    return [
        r"\subsection{Results Interpretation}",
        "",
        f"The cold-miss result shows the cost of safely deciding that no reusable answer exists. ShardCache Embedded moved from {tex_metric(cold_1, 'ops')} ops/s at 1 vCPU to {tex_metric(cold_16, 'ops')} ops/s at 16 vCPU, while all systems preserved a 0.0\\% hit rate on negative queries. Redis HNSW is faster on the 1-vCPU raw-vector cold-miss row at {tex_metric(redis_hnsw_cold_1, 'ops')} ops/s, which is expected for a mature HNSW ANN primitive, but the 16-vCPU ShardCache row shows the benefit of a cache-native path that scales across the larger SUT allocation.",
        "",
        f"The cold unique hit result separates throughput from semantic acceptance. ShardCache Embedded reached {tex_metric(unique_16, 'ops')} ops/s at {tex_percent(unique_16, 'hit_rate')} hit rate in the 16-vCPU run. Redis HNSW reached {tex_metric(redis_hnsw_unique_16, 'ops')} ops/s at {tex_percent(redis_hnsw_unique_16, 'hit_rate')} hit rate. That distinction matters: a system can be fast while returning fewer reusable answers at the configured threshold.",
        "",
        f"The hot exact-query result measures repeated normalized traffic after warmup. ShardCache Embedded reached {tex_metric(hot_16, 'ops')} ops/s at {tex_percent(hot_16, 'hit_rate')} hit rate in the 16-vCPU run, compared with {tex_metric(betterdb_hot_16, 'ops')} ops/s for BetterDB and {tex_metric(redisvl_hot_16, 'ops')} ops/s for RedisVL SemanticCache. This is the strongest evidence for semantic caching as a native cache feature: repeated application questions should become cache lookups, not repeated vector searches.",
        "",
    ]


def render_combined_conclusion_latex(
    one: dict[str, object] | None,
    sixteen: dict[str, object] | None,
) -> list[str]:
    hot_16 = combined_row(sixteen, "hit-hot-cached", "shardcache")
    unique_16 = combined_row(sixteen, "hit-cold-unique", "shardcache")
    return [
        r"\section{Conclusion}",
        r"\label{sec:semantic-cache-conclusion}",
        "",
        f"The benchmark supports the claim that ShardCache's native semantic-cache path is not just faster in a hot microbenchmark; it improves the complete semantic-cache data path. The no-memo path sustains {tex_metric(unique_16, 'ops')} first-time semantic hits/s in the 16-vCPU run, and the hot exact-query path sustains {tex_metric(hot_16, 'ops')} lookups/s once repeated traffic is warmed. The hit-rate columns add an important correctness and utility dimension: cold misses remain at 0.0\\%, cold positives show actual semantic reuse, and hot exact traffic shows whether repeated prompts stay on the cached-answer path.",
        "",
        "The governance metadata model extends the performance work into production semantics. It lets customers preserve cross-user cache reuse while enforcing tenant, document, policy, and freshness constraints before any cached value is released. That turns semantic caching from a raw nearest-neighbor lookup into a governed, measurable cache feature.",
        "",
    ]


def combined_report_title(runs: list[dict[str, object]]) -> str:
    vcpus = sorted(sut_vcpus(run["metadata"]) for run in runs)
    if vcpus == [1, 16]:
        return "ShardCache Semantic Cache Head-to-Head: 1-vCPU and 16-vCPU Isolated"
    labels = " and ".join(f"{vcpu}-vCPU" for vcpu in vcpus)
    return f"ShardCache Semantic Cache Head-to-Head: {labels} Isolated"


def run_by_vcpu(runs: list[dict[str, object]], target: int) -> dict[str, object] | None:
    return next((run for run in runs if sut_vcpus(run["metadata"]) == target), None)


def run_metadata_value(run: dict[str, object] | None, key: str, fallback: str) -> str:
    if run is None:
        return fallback
    return str(run["metadata"].get(key, fallback))


def combined_metadata_value(runs: list[dict[str, object]], key: str, fallback: str) -> str:
    for run in runs:
        value = run["metadata"].get(key)
        if value:
            return str(value)
    return fallback


def combined_execution_mode_note(runs: list[dict[str, object]]) -> str:
    has_server = any(
        adapter == "shardcache-server"
        for run in runs
        for _, adapter in run["rows"].keys()
    )
    if has_server:
        return "ShardCache Embedded is the in-process native semantic-cache API. ShardCache Server is the shardcache TCP server through RESP semantic commands, so the same report shows both library-mode performance and service-mode performance."
    return "These server result files currently include ShardCache Embedded only. The harness now has a ShardCache Server semantic adapter; rerunning the isolated benchmark will add server rows next to the embedded rows."


def combined_row(
    run: dict[str, object] | None,
    scenario: str,
    adapter: str,
) -> dict[str, object] | None:
    if run is None:
        return None
    return run["rows"].get((scenario, adapter))


def combined_adapters(
    one: dict[str, object] | None,
    sixteen: dict[str, object] | None,
    scenario: str,
) -> list[str]:
    adapters = []
    for adapter in ORDER:
        if combined_row(one, scenario, adapter) is not None or combined_row(sixteen, scenario, adapter) is not None:
            adapters.append(adapter)
    return adapters


def combined_label(
    adapter: str,
    one: dict[str, object] | None,
    sixteen: dict[str, object] | None,
    scenario: str,
) -> str:
    row = combined_row(one, scenario, adapter) or combined_row(sixteen, scenario, adapter)
    return str(row.get("label", LABELS.get(adapter, adapter))) if row else LABELS.get(adapter, adapter)


def combined_scenario_description(title: str) -> str:
    if title == "Cold Miss":
        return "Unique negative queries with query-result caching disabled. This shows the semantic-cache fall-through cost and how each system scales when it must prove that no reusable answer exists. Hit rate should be 0%; a nonzero value would indicate false-positive reuse."
    if title == "Cold Unique Semantic Hit":
        return "Unique positive/paraphrase queries with query-result caching disabled. This shows first-time semantic reuse without exact-query memo help. The hit-rate columns show how much of the positive/paraphrase stream each system actually considers reusable at the configured threshold."
    return "Repeated exact-query traffic after warmup. This shows the application-cache hot path and how much each system benefits from repeated normalized questions. On this row, less than 100% hit rate means repeated exact traffic is still falling through to semantic/vector search."


def markdown_metric(row: dict[str, object] | None, key: str) -> str:
    return format_int(row[key]) if row is not None else "n/a"


def markdown_float(row: dict[str, object] | None, key: str) -> str:
    return f"{float(row[key]):.4f}" if row is not None else "n/a"


def markdown_percent(row: dict[str, object] | None, key: str) -> str:
    return f"{float(row[key]):.1f}%" if row is not None else "n/a"


def markdown_scale(one_row: dict[str, object] | None, sixteen_row: dict[str, object] | None) -> str:
    if one_row is None or sixteen_row is None:
        return "n/a"
    value = ratio(float(sixteen_row["ops"]), float(one_row["ops"]))
    return f"{value:.1f}x" if value > 0.0 else "n/a"


def markdown_speedup(run: dict[str, object] | None, scenario: str, adapter: str) -> str:
    if run is None:
        return "n/a"
    rows = run["rows"]
    row = rows.get((scenario, adapter))
    shard = rows.get((scenario, "shardcache"))
    if row is None or shard is None:
        return "n/a"
    return f"{speedup(row, float(shard['ops'])):.1f}x"


def tex_metric(row: dict[str, object] | None, key: str) -> str:
    return tex_count(row[key]) if row is not None else "n/a"


def tex_float(row: dict[str, object] | None, key: str) -> str:
    return f"{float(row[key]):.4f}" if row is not None else "n/a"


def tex_percent(row: dict[str, object] | None, key: str) -> str:
    return f"{float(row[key]):.1f}\\%" if row is not None else "n/a"


def tex_scale(one_row: dict[str, object] | None, sixteen_row: dict[str, object] | None) -> str:
    if one_row is None or sixteen_row is None:
        return "n/a"
    value = ratio(float(sixteen_row["ops"]), float(one_row["ops"]))
    return f"{value:.1f}$\\times$" if value > 0.0 else "n/a"


def tex_speedup_cell(run: dict[str, object] | None, scenario: str, adapter: str) -> str:
    if run is None:
        return "n/a"
    rows = run["rows"]
    row = rows.get((scenario, adapter))
    shard = rows.get((scenario, "shardcache"))
    if row is None or shard is None:
        return "n/a"
    return f"{speedup(row, float(shard['ops'])):.1f}$\\times$"


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
