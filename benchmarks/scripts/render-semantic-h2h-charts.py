#!/usr/bin/env python3
"""Render semantic-cache head-to-head throughput charts as SVG.

The renderer intentionally uses only the Python standard library so benchmark
report charts can be regenerated on bare machines. Convert the SVGs to PDFs
with a tool such as `rsvg-convert` before including them in LaTeX.
"""

from __future__ import annotations

import argparse
import csv
import math
from pathlib import Path


SCENARIOS = {
    "miss-cold": {
        "title": "Cold miss lookup throughput",
        "cpu_title": "Cold miss lookup SUT CPU use",
        "hit_rate_title": "Cold miss lookup hit rate",
        "subtitle_suffix": "query-result memo disabled",
        "output": "semantic-h2h-miss-cold-throughput.svg",
        "cpu_output": "semantic-h2h-miss-cold-vcpu.svg",
        "hit_rate_output": "semantic-h2h-miss-cold-hit-rate.svg",
    },
    "hit-cold-unique": {
        "title": "Cold unique semantic-hit throughput",
        "cpu_title": "Cold unique semantic-hit SUT CPU use",
        "hit_rate_title": "Cold unique semantic-hit rate",
        "subtitle_suffix": "unique semantic queries",
        "output": "semantic-h2h-hit-cold-unique-throughput.svg",
        "cpu_output": "semantic-h2h-hit-cold-unique-vcpu.svg",
        "hit_rate_output": "semantic-h2h-hit-cold-unique-hit-rate.svg",
    },
    "hit-hot-cached": {
        "title": "Hot cached exact-query throughput",
        "cpu_title": "Hot cached exact-query SUT CPU use",
        "hit_rate_title": "Hot cached exact-query hit rate",
        "subtitle_suffix": "warmed exact-query cache",
        "output": "semantic-h2h-hit-hot-cached-throughput.svg",
        "cpu_output": "semantic-h2h-hit-hot-cached-vcpu.svg",
        "hit_rate_output": "semantic-h2h-hit-hot-cached-hit-rate.svg",
    },
}

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
    "redisvl-semantic-cache": "RedisVL SC",
    "langchain-redis-semantic-cache": "LangChain Redis SC",
    "redis-vector-flat": "Redis vector FLAT",
    "redis-vector-hnsw": "Redis vector HNSW",
    "faiss-flat": "FAISS Flat",
    "faiss-hnsw": "FAISS HNSW",
    "hnswlib-cosine": "hnswlib cosine",
    "qdrant-cosine": "Qdrant cosine",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("results_dir", type=Path)
    args = parser.parse_args()

    metadata = load_metadata(args.results_dir / "metadata.txt")
    sut_vcpu_cap = cpuset_width(metadata.get("sut_cpuset", "0-15")) or 16
    rows = load_rows(args.results_dir)
    for scenario, meta in SCENARIOS.items():
        chart_rows = [rows[(scenario, adapter)] for adapter in ORDER if (scenario, adapter) in rows]
        if not chart_rows:
            continue
        subtitle = scenario_subtitle(chart_rows, str(meta["subtitle_suffix"]))
        svg = render_chart(meta["title"], subtitle, chart_rows)
        (args.results_dir / meta["output"]).write_text(svg, encoding="utf-8")
        hit_rate_svg = render_hit_rate_chart(str(meta["hit_rate_title"]), subtitle, chart_rows)
        (args.results_dir / meta["hit_rate_output"]).write_text(hit_rate_svg, encoding="utf-8")
        if any(row.get("total_vcpu") is not None for row in chart_rows):
            cpu_svg = render_cpu_chart(
                str(meta["cpu_title"]),
                subtitle,
                chart_rows,
                sut_vcpu_cap,
            )
            (args.results_dir / meta["cpu_output"]).write_text(cpu_svg, encoding="utf-8")
    scaling_rows = load_vcpu_scaling_rows(args.results_dir)
    if scaling_rows:
        svg = render_vcpu_scaling_chart(scaling_rows)
        (args.results_dir / "semantic-h2h-shardcache-vcpu-speedup.svg").write_text(
            svg,
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


def load_rows(results_dir: Path) -> dict[tuple[str, str], dict[str, object]]:
    rows: dict[tuple[str, str], dict[str, object]] = {}
    for path in sorted(results_dir.glob("*.csv")):
        with path.open(newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if "scenario" in row:
                    scenario = row["scenario"]
                    adapter = row["adapter"]
                    ops = float(row["ops_per_sec"])
                    hits = int(row["hits"])
                    queries = int(row["queries"])
                    workers = int(row.get("workers") or 0)
                    entries = int(row.get("entries") or 0)
                    dims = int(row.get("dims") or 0)
                else:
                    scenario = scenario_from_name(path.name, row.get("mode", ""))
                    adapter = "shardcache"
                    ops = float(row["ops_per_sec"])
                    hits = int(row["hits"])
                    queries = int(row["queries"])
                    workers = int(row.get("workers") or 0)
                    entries = int(row.get("index_entries") or 0)
                    dims = int(row.get("dims") or 0)
                process_vcpu = maybe_float(row.get("process_vcpu"))
                external_vcpu = maybe_float(row.get("external_vcpu"))
                total_vcpu = maybe_float(row.get("total_vcpu"))
                if total_vcpu is None and process_vcpu is not None:
                    total_vcpu = process_vcpu + (external_vcpu or 0.0)
                sut_vcpu = maybe_float(row.get("sut_vcpu"))
                client_vcpu = maybe_float(row.get("client_vcpu"))
                if sut_vcpu is None:
                    if adapter in NETWORKED_ADAPTERS:
                        sut_vcpu = external_vcpu or 0.0
                        client_vcpu = process_vcpu or 0.0
                    else:
                        sut_vcpu = process_vcpu or 0.0
                        client_vcpu = 0.0
                rows[(scenario, adapter)] = {
                    "scenario": scenario,
                    "adapter": adapter,
                    "label": LABELS.get(adapter, adapter),
                    "ops": ops,
                    "hits": hits,
                    "queries": queries,
                    "hit_rate": (hits / queries * 100.0) if queries > 0 else 0.0,
                    "workers": workers,
                    "entries": entries,
                    "dims": dims,
                    "process_vcpu": process_vcpu,
                    "external_vcpu": external_vcpu,
                    "total_vcpu": total_vcpu,
                    "sut_vcpu": sut_vcpu,
                    "client_vcpu": client_vcpu,
                }
    return rows


def maybe_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def scenario_subtitle(rows: list[dict[str, object]], suffix: str) -> str:
    entries = next((int(row["entries"]) for row in rows if int(row.get("entries") or 0) > 0), 100_000)
    dims = next((int(row["dims"]) for row in rows if int(row.get("dims") or 0) > 0), 384)
    workers = next((int(row["workers"]) for row in rows if int(row.get("workers") or 0) > 0), 0)
    worker_text = f", {workers} workers" if workers else ""
    return f"{format_count(entries)} entries, {dims} dims{worker_text}, {suffix}"


def scenario_from_name(name: str, fallback: str) -> str:
    if "miss-cold" in name:
        return "miss-cold"
    if "hit-cold-unique" in name:
        return "hit-cold-unique"
    if "hit-hot-cached" in name:
        return "hit-hot-cached"
    return fallback


def load_vcpu_scaling_rows(results_dir: Path) -> list[dict[str, object]]:
    names = [
        ("miss-cold", "Cold miss"),
        ("hit-cold-unique", "Cold unique hit"),
        ("hit-hot-cached", "Hot cached"),
    ]
    rows: list[dict[str, object]] = []
    for scenario, label in names:
        values: dict[int, dict[str, object]] = {}
        for vcpu in (1, 8):
            path = results_dir / f"shardcache-vcpu-{vcpu}-{scenario}.csv"
            if not path.exists():
                return []
            with path.open(newline="") as handle:
                row = next(csv.DictReader(handle))
            values[vcpu] = {
                "ops": float(row["ops_per_sec"]),
                "p50": float(row["p50_ms"]),
                "p95": float(row["p95_ms"]),
                "p99": float(row["p99_ms"]),
                "hits": int(row["hits"]),
                "queries": int(row["queries"]),
            }
        rows.append(
            {
                "scenario": scenario,
                "label": label,
                "vcpu1": values[1],
                "vcpu8": values[8],
                "speedup": float(values[8]["ops"]) / float(values[1]["ops"]),
            }
        )
    return rows


def render_chart(title: str, subtitle: str, rows: list[dict[str, object]]) -> str:
    width = 1060
    height = 660
    left = 230
    right = 190
    top = 92
    bottom = 74
    plot_w = width - left - right
    plot_h = height - top - bottom
    row_h = plot_h / len(rows)
    bar_h = min(30, row_h * 0.58)
    max_ops = max(float(row["ops"]) for row in rows)
    axis_min = 0.0
    axis_max, tick_step = nice_axis(max_ops)

    def x_for(value: float) -> float:
        return left + (value / axis_max) * plot_w

    ticks = linear_ticks(axis_max, tick_step)
    out = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="{width}" height="{height}">',
        "<defs>",
        '<style><![CDATA[',
        "text{font-family:Inter,Arial,Helvetica,sans-serif;fill:#16202a}",
        ".title{font-size:28px;font-weight:700}",
        ".subtitle{font-size:14px;fill:#4c5967}",
        ".axis{stroke:#d8dde4;stroke-width:1}",
        ".tick{font-size:12px;fill:#687585}",
        ".label{font-size:15px;fill:#24303d}",
        ".value{font-size:13px;font-weight:650;fill:#16202a}",
        ".note{font-size:12px;fill:#596777}",
        "]]></style>",
        "</defs>",
        '<rect x="0" y="0" width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{left}" y="38" class="title">{esc(title)}</text>',
        f'<text x="{left}" y="63" class="subtitle">{esc(subtitle)}</text>',
    ]

    for tick in ticks:
        x = x_for(tick)
        out.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top + plot_h}" class="axis"/>')
        out.append(f'<text x="{x:.2f}" y="{height - 35}" text-anchor="middle" class="tick">{format_ops(tick)}</text>')
    out.append(f'<line x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top + plot_h}" class="axis"/>')

    for index, row in enumerate(rows):
        adapter = str(row["adapter"])
        label = str(row["label"])
        ops = float(row["ops"])
        y_mid = top + row_h * index + row_h / 2
        x0 = x_for(axis_min)
        x1 = x_for(ops)
        fill = "#d94f30" if adapter == "shardcache" else "#3a78b7" if "semantic-cache" in adapter or adapter == "betterdb" else "#758392"
        if adapter == "shardcache":
            stroke = "#9f2f1a"
        elif "hnsw" in adapter:
            stroke = "#255d8c"
            fill = "#4d8cc7"
        else:
            stroke = "#596777"
        out.append(f'<text x="{left - 14}" y="{y_mid + 5:.2f}" text-anchor="end" class="label">{esc(label)}</text>')
        out.append(
            f'<rect x="{x0:.2f}" y="{y_mid - bar_h / 2:.2f}" width="{max(2.0, x1 - x0):.2f}" '
            f'height="{bar_h:.2f}" rx="4" fill="{fill}" stroke="{stroke}" stroke-width="1"/>'
        )
        out.append(f'<text x="{min(width - right + 8, x1 + 10):.2f}" y="{y_mid + 5:.2f}" class="value">{format_ops(ops)} ops/s</text>')

    out.append(f'<text x="{left}" y="{height - 10}" class="note">Linear throughput axis. Longer bars indicate higher lookup throughput.</text>')
    out.append("</svg>")
    return "\n".join(out)


def render_hit_rate_chart(title: str, subtitle: str, rows: list[dict[str, object]]) -> str:
    width = 1060
    height = 660
    left = 230
    right = 150
    top = 92
    bottom = 74
    plot_w = width - left - right
    plot_h = height - top - bottom
    row_h = plot_h / len(rows)
    bar_h = min(30, row_h * 0.58)
    axis_max = 100.0
    tick_step = 20.0

    def x_for(value: float) -> float:
        return left + (value / axis_max) * plot_w

    ticks = linear_ticks(axis_max, tick_step)
    out = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="{width}" height="{height}">',
        "<defs>",
        '<style><![CDATA[',
        "text{font-family:Inter,Arial,Helvetica,sans-serif;fill:#16202a}",
        ".title{font-size:28px;font-weight:700}",
        ".subtitle{font-size:14px;fill:#4c5967}",
        ".axis{stroke:#d8dde4;stroke-width:1}",
        ".tick{font-size:12px;fill:#687585}",
        ".label{font-size:15px;fill:#24303d}",
        ".value{font-size:13px;font-weight:650;fill:#16202a}",
        ".note{font-size:12px;fill:#596777}",
        "]]></style>",
        "</defs>",
        '<rect x="0" y="0" width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{left}" y="38" class="title">{esc(title)}</text>',
        f'<text x="{left}" y="63" class="subtitle">{esc(subtitle)}</text>',
    ]

    for tick in ticks:
        x = x_for(tick)
        out.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top + plot_h}" class="axis"/>')
        out.append(f'<text x="{x:.2f}" y="{height - 35}" text-anchor="middle" class="tick">{tick:.0f}%</text>')
    out.append(f'<line x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top + plot_h}" class="axis"/>')

    for index, row in enumerate(rows):
        adapter = str(row["adapter"])
        label = str(row["label"])
        hit_rate = float(row.get("hit_rate") or 0.0)
        y_mid = top + row_h * index + row_h / 2
        x0 = x_for(0.0)
        x1 = x_for(hit_rate)
        fill = "#d94f30" if adapter == "shardcache" else "#3a78b7" if "semantic-cache" in adapter or adapter == "betterdb" else "#758392"
        if adapter == "shardcache":
            stroke = "#9f2f1a"
        elif "hnsw" in adapter:
            stroke = "#255d8c"
            fill = "#4d8cc7"
        else:
            stroke = "#596777"
        out.append(f'<text x="{left - 14}" y="{y_mid + 5:.2f}" text-anchor="end" class="label">{esc(label)}</text>')
        out.append(
            f'<rect x="{x0:.2f}" y="{y_mid - bar_h / 2:.2f}" width="{max(2.0, x1 - x0):.2f}" '
            f'height="{bar_h:.2f}" rx="4" fill="{fill}" stroke="{stroke}" stroke-width="1"/>'
        )
        out.append(f'<text x="{min(width - right + 8, x1 + 10):.2f}" y="{y_mid + 5:.2f}" class="value">{hit_rate:.1f}%</text>')

    out.append(f'<text x="{left}" y="{height - 10}" class="note">Hit rate is measured hits divided by measured lookups for the same benchmark row.</text>')
    out.append("</svg>")
    return "\n".join(out)


def render_cpu_chart(
    title: str,
    subtitle: str,
    rows: list[dict[str, object]],
    sut_vcpu_cap: int,
) -> str:
    width = 1060
    height = 660
    left = 230
    right = 210
    top = 104
    bottom = 78
    plot_w = width - left - right
    plot_h = height - top - bottom
    row_h = plot_h / len(rows)
    bar_h = min(30, row_h * 0.58)
    values = [float(row.get("sut_vcpu") or 0.0) for row in rows]
    axis_max, tick_step = nice_axis(max(max(values), float(sut_vcpu_cap)))

    def x_for(value: float) -> float:
        return left + (value / axis_max) * plot_w

    ticks = linear_ticks(axis_max, tick_step)
    out = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="{width}" height="{height}">',
        "<defs>",
        '<style><![CDATA[',
        "text{font-family:Inter,Arial,Helvetica,sans-serif;fill:#16202a}",
        ".title{font-size:28px;font-weight:700}",
        ".subtitle{font-size:14px;fill:#4c5967}",
        ".axis{stroke:#d8dde4;stroke-width:1}",
        ".limit{stroke:#d94f30;stroke-width:1.5;stroke-dasharray:5 5}",
        ".tick{font-size:12px;fill:#687585}",
        ".label{font-size:15px;fill:#24303d}",
        ".value{font-size:13px;font-weight:650;fill:#16202a}",
        ".legend{font-size:12px;fill:#596777}",
        ".note{font-size:12px;fill:#596777}",
        "]]></style>",
        "</defs>",
        '<rect x="0" y="0" width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{left}" y="38" class="title">{esc(title)}</text>',
        f'<text x="{left}" y="63" class="subtitle">{esc(subtitle)}</text>',
        f'<rect x="{left}" y="78" width="12" height="12" fill="#d94f30" stroke="#9f2f1a"/>',
        f'<text x="{left + 18}" y="89" class="legend">SUT/server or embedded process</text>',
    ]

    for tick in ticks:
        x = x_for(tick)
        out.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top + plot_h}" class="axis"/>')
        out.append(f'<text x="{x:.2f}" y="{height - 36}" text-anchor="middle" class="tick">{tick:g}</text>')
    limit_x = x_for(float(sut_vcpu_cap))
    out.append(f'<line x1="{limit_x:.2f}" y1="{top}" x2="{limit_x:.2f}" y2="{top + plot_h}" class="limit"/>')
    out.append(f'<text x="{limit_x + 6:.2f}" y="{top - 8}" class="tick">{sut_vcpu_cap}-vCPU SUT cap</text>')
    out.append(f'<line x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top + plot_h}" class="axis"/>')

    for index, row in enumerate(rows):
        adapter = str(row["adapter"])
        label = str(row["label"])
        sut_vcpu = float(row.get("sut_vcpu") or 0.0)
        y_mid = top + row_h * index + row_h / 2
        x0 = x_for(0)
        server_w = max(0.0, x_for(sut_vcpu) - x0)
        server_fill = "#d94f30" if adapter == "shardcache" else "#3a78b7" if adapter in NETWORKED_ADAPTERS else "#4d8cc7"
        server_stroke = "#9f2f1a" if adapter == "shardcache" else "#255d8c"
        out.append(f'<text x="{left - 14}" y="{y_mid + 5:.2f}" text-anchor="end" class="label">{esc(label)}</text>')
        out.append(
            f'<rect x="{x0:.2f}" y="{y_mid - bar_h / 2:.2f}" width="{max(2.0, server_w):.2f}" '
            f'height="{bar_h:.2f}" rx="4" fill="{server_fill}" stroke="{server_stroke}" stroke-width="1"/>'
        )
        out.append(
            f'<text x="{min(width - right + 8, x_for(sut_vcpu) + 10):.2f}" y="{y_mid + 5:.2f}" '
            f'class="value">{sut_vcpu:.2f} vCPU</text>'
        )

    out.append(f'<text x="{left}" y="{height - 10}" class="note">Measured-window SUT CPU only. Client/load-process CPU is excluded from this chart and retained in raw CSV/report audit columns.</text>')
    out.append("</svg>")
    return "\n".join(out)


def render_vcpu_scaling_chart(rows: list[dict[str, object]]) -> str:
    width = 1060
    height = 430
    left = 210
    right = 130
    top = 92
    bottom = 70
    plot_w = width - left - right
    plot_h = height - top - bottom
    group_h = plot_h / len(rows)
    bar_h = 24
    axis_max, tick_step = nice_axis(max(float(row["speedup"]) for row in rows))

    def x_for(value: float) -> float:
        return left + (value / axis_max) * plot_w

    ticks = linear_ticks(axis_max, tick_step)
    out = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="{width}" height="{height}">',
        "<defs>",
        '<style><![CDATA[',
        "text{font-family:Inter,Arial,Helvetica,sans-serif;fill:#16202a}",
        ".title{font-size:28px;font-weight:700}",
        ".subtitle{font-size:14px;fill:#4c5967}",
        ".axis{stroke:#d8dde4;stroke-width:1}",
        ".tick{font-size:12px;fill:#687585}",
        ".label{font-size:15px;fill:#24303d}",
        ".value{font-size:13px;font-weight:650;fill:#16202a}",
        ".note{font-size:12px;fill:#596777}",
        "]]></style>",
        "</defs>",
        '<rect x="0" y="0" width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{left}" y="38" class="title">ShardCache vCPU scaling</text>',
        f'<text x="{left}" y="63" class="subtitle">Throughput speedup from 1 vCPU to 8 vCPU, taskset-pinned on the benchmark server</text>',
    ]

    for tick in ticks:
        x = x_for(tick)
        out.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top + plot_h}" class="axis"/>')
        out.append(f'<text x="{x:.2f}" y="{height - 34}" text-anchor="middle" class="tick">{tick:g}x</text>')
    out.append(f'<line x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top + plot_h}" class="axis"/>')

    for index, row in enumerate(rows):
        y_mid = top + group_h * index + group_h / 2
        speedup = float(row["speedup"])
        x0 = x_for(0)
        x1 = x_for(speedup)
        baseline_x = x_for(1)
        out.append(f'<text x="{left - 14}" y="{y_mid + 5:.2f}" text-anchor="end" class="label">{esc(str(row["label"]))}</text>')
        out.append(
            f'<rect x="{x0:.2f}" y="{y_mid - bar_h / 2:.2f}" width="{max(2.0, x1 - x0):.2f}" '
            f'height="{bar_h}" rx="4" fill="#d94f30" stroke="#9f2f1a" stroke-width="1"/>'
        )
        out.append(f'<line x1="{baseline_x:.2f}" y1="{y_mid - 22:.2f}" x2="{baseline_x:.2f}" y2="{y_mid + 22:.2f}" stroke="#24303d" stroke-width="1.5"/>')
        out.append(f'<text x="{min(width - right + 8, x1 + 10):.2f}" y="{y_mid + 5:.2f}" class="value">{speedup:.1f}x</text>')

    out.append(f'<text x="{left}" y="{height - 10}" class="note">Baseline marker is 1 vCPU. Bars show 8-vCPU throughput divided by 1-vCPU throughput.</text>')
    out.append("</svg>")
    return "\n".join(out)


def nice_axis(max_value: float) -> tuple[float, float]:
    if max_value <= 0:
        return 1.0, 0.2
    tick_step = nice_step(max_value / 5)
    axis_max = math.ceil(max_value / tick_step) * tick_step
    return float(axis_max), float(tick_step)


def nice_step(value: float) -> float:
    magnitude = 10 ** math.floor(math.log10(value))
    fraction = value / magnitude
    for factor in (1, 2, 2.5, 5, 10):
        if fraction <= factor:
            return float(factor * magnitude)
    return float(10 * magnitude)


def linear_ticks(axis_max: float, step: float) -> list[float]:
    ticks: list[float] = []
    value = 0.0
    while value <= axis_max + (step * 0.5):
        ticks.append(value)
        value += step
    return ticks


def format_ops(value: float) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2g}M"
    if value >= 1_000:
        return f"{value / 1_000:.3g}k"
    return f"{value:.0f}"


def format_count(value: int) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2g}M"
    if value >= 1_000:
        return f"{value // 1_000}k"
    return str(value)


def esc(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


if __name__ == "__main__":
    main()
