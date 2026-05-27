"""Shared bench utilities for the Python harnesses (fc-py, shardcache-lmcache).

Matches the Rust drivers' CSV schema so all rows live in the same file.
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import resource
import statistics
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Optional


SATURATION_CSV_HEADER = [
    "backend",
    "value_size",
    "mix",
    "vcpu_budget",
    "clients",
    "duration_s",
    "ops_total",
    "ops_per_sec",
    "gb_per_sec",
    "vcpu_consumed",
    "p50_ns",
    "p99_ns",
    "p999_ns",
    "errors",
]


@dataclass
class WorkloadSpec:
    key_count: int
    value_size: int
    get_pct: int  # 0..100

    @property
    def mix_label(self) -> str:
        return f"{self.get_pct}-{100 - self.get_pct}"


def parse_mix(s: str) -> int:
    if s == "get" or s == "100-0":
        return 100
    if s == "set" or s == "0-100":
        return 0
    if s == "80-20":
        return 80
    if "-" in s:
        return int(s.split("-", 1)[0])
    raise SystemExit(f"unknown mix: {s}")


def build_keys(key_count: int) -> list[bytes]:
    return [f"k:{i:016x}".encode("ascii") for i in range(key_count)]


def build_value(value_size: int) -> bytes:
    return bytes((i & 0xFF) for i in range(value_size))


def common_argparser(default_clients: int = 4) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--value-size", type=int, default=512)
    p.add_argument("--mix", type=str, default="80-20")
    p.add_argument("--vcpu-budget", type=int, default=4)
    p.add_argument("--clients", type=int, default=default_clients)
    p.add_argument("--key-count", type=int, default=100_000)
    p.add_argument("--warmup", type=int, default=2)
    p.add_argument("--duration", type=int, default=10)
    p.add_argument(
        "--latency-sample-rate",
        type=int,
        default=1,
        help=(
            "Record one latency sample every N measured operations. "
            "Use 0 to disable latency timing for raw throughput/GB/s runs."
        ),
    )
    p.add_argument(
        "--op-batch-size",
        type=int,
        default=1,
        help="Number of same-operation keys to issue per worker loop iteration.",
    )
    p.add_argument("--csv", type=str, default=None)
    return p


class LatencyAccumulator:
    """Lightweight sampling accumulator. Not an HDR histogram, but matches the
    Rust driver's output format closely enough for cross-comparison."""

    __slots__ = ("samples", "max_samples")

    def __init__(self, max_samples: int = 200_000) -> None:
        self.samples: list[int] = []
        self.max_samples = max_samples

    def record(self, latency_ns: int) -> None:
        if len(self.samples) < self.max_samples:
            self.samples.append(latency_ns)
        else:
            idx = random.randrange(self.max_samples + 1)
            if idx < self.max_samples:
                self.samples[idx] = latency_ns

    def merge(self, other: "LatencyAccumulator") -> None:
        for s in other.samples:
            self.record(s)

    def percentile(self, q: float) -> int:
        if not self.samples:
            return 0
        s = sorted(self.samples)
        idx = max(0, min(len(s) - 1, int(round(q * (len(s) - 1)))))
        return s[idx]

    def p50(self) -> int:
        return self.percentile(0.50)

    def p99(self) -> int:
        return self.percentile(0.99)

    def p999(self) -> int:
        return self.percentile(0.999)


def process_cpu_seconds() -> float:
    """User+system CPU consumed by this process, in seconds."""
    ru = resource.getrusage(resource.RUSAGE_SELF)
    return ru.ru_utime + ru.ru_stime


@dataclass
class RunResult:
    backend_id: str
    value_size: int
    mix_label: str
    vcpu_budget: int
    clients: int
    duration_s: float
    ops_total: int
    bytes_total: int
    vcpu_consumed: float
    p50_ns: int
    p99_ns: int
    p999_ns: int
    errors: int

    @property
    def ops_per_sec(self) -> float:
        return self.ops_total / self.duration_s if self.duration_s > 0 else 0.0

    @property
    def gb_per_sec(self) -> float:
        return (self.bytes_total / 1e9) / self.duration_s if self.duration_s > 0 else 0.0

    def to_csv_row(self) -> list[str]:
        return [
            self.backend_id,
            str(self.value_size),
            self.mix_label,
            str(self.vcpu_budget),
            str(self.clients),
            f"{self.duration_s:.3f}",
            str(self.ops_total),
            f"{self.ops_per_sec:.0f}",
            f"{self.gb_per_sec:.3f}",
            f"{self.vcpu_consumed:.3f}",
            str(self.p50_ns),
            str(self.p99_ns),
            str(self.p999_ns),
            str(self.errors),
        ]


def fmt_ns(ns: int) -> str:
    if ns < 1_000:
        return f"{ns}ns"
    if ns < 1_000_000:
        return f"{ns / 1_000:.1f}us"
    if ns < 1_000_000_000:
        return f"{ns / 1_000_000:.1f}ms"
    return f"{ns / 1_000_000_000:.2f}s"


def print_table(rows: Iterable[RunResult]) -> None:
    cols = ("backend", "ops/sec", "GB/s", "vCPU", "p50", "p99", "p999", "errors")
    widths = (20, 14, 10, 10, 10, 10, 10, 8)
    print(
        "| "
        + " | ".join(
            f"{c:<{w}}" if i == 0 else f"{c:>{w}}"
            for i, (c, w) in enumerate(zip(cols, widths))
        )
        + " |"
    )
    print("| " + " | ".join("-" * w for w in widths) + " |")
    for r in rows:
        print(
            "| "
            + " | ".join(
                [
                    f"{r.backend_id:<20}",
                    f"{r.ops_per_sec:>14.0f}",
                    f"{r.gb_per_sec:>10.3f}",
                    f"{r.vcpu_consumed:>10.3f}",
                    f"{fmt_ns(r.p50_ns):>10}",
                    f"{fmt_ns(r.p99_ns):>10}",
                    f"{fmt_ns(r.p999_ns):>10}",
                    f"{r.errors:>8}",
                ]
            )
            + " |"
        )


def append_csv(path: Optional[str], rows: Iterable[RunResult]) -> None:
    if not path:
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    exists = p.exists()
    with p.open("a", newline="") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(SATURATION_CSV_HEADER)
        for r in rows:
            w.writerow(r.to_csv_row())


def run_saturation(
    backend_id: str,
    spec: WorkloadSpec,
    args: argparse.Namespace,
    new_worker: Callable[[], "Worker"],
    warmup_fn: Callable[[list[bytes], bytes], None],
) -> RunResult:
    """Closed-loop saturation runner. Workers each take their own `Worker`
    handle and drive ops until `stop` is set. `warmup_fn` populates the
    backend with the full keyset before timing begins."""

    keys = build_keys(spec.key_count)
    value = build_value(spec.value_size)
    warmup_fn(keys, value)

    stop = threading.Event()
    warm_done = threading.Event()
    histograms: list[LatencyAccumulator] = []
    op_counts = [0] * args.clients
    err_counts = [0] * args.clients
    latency_sample_rate = max(0, int(getattr(args, "latency_sample_rate", 1)))
    op_batch_size = max(1, int(getattr(args, "op_batch_size", 1)))

    def worker_main(idx: int) -> None:
        rng = random.Random(0xDEADBEEF + idx)
        hist = LatencyAccumulator()
        local_ops = 0
        local_errs = 0
        worker = new_worker()
        while not stop.is_set():
            batch_keys = [keys[rng.randrange(spec.key_count)] for _ in range(op_batch_size)]
            is_get = rng.randrange(100) < spec.get_pct
            measured = warm_done.is_set()
            should_sample = (
                measured
                and latency_sample_rate > 0
                and local_ops % latency_sample_rate == 0
            )
            if should_sample:
                t0 = time.perf_counter_ns()
            try:
                if is_get:
                    if op_batch_size == 1:
                        worker.do_get(batch_keys[0])
                    else:
                        worker.do_get_many(batch_keys)
                else:
                    if op_batch_size == 1:
                        worker.do_set(batch_keys[0], value)
                    else:
                        worker.do_set_many(batch_keys, value)
            except Exception:
                local_errs += op_batch_size
                continue
            if measured:
                if should_sample:
                    hist.record(time.perf_counter_ns() - t0)
                local_ops += op_batch_size
        op_counts[idx] = local_ops
        err_counts[idx] = local_errs
        histograms.append(hist)

    threads = [threading.Thread(target=worker_main, args=(i,)) for i in range(args.clients)]
    for t in threads:
        t.start()

    time.sleep(args.warmup)
    pre_cpu = process_cpu_seconds()
    measure_start = time.perf_counter()
    warm_done.set()

    time.sleep(args.duration)
    stop.set()

    for t in threads:
        t.join()

    wall = time.perf_counter() - measure_start
    cpu_used = process_cpu_seconds() - pre_cpu
    vcpu = cpu_used / wall if wall > 0 else 0.0

    combined = LatencyAccumulator()
    for h in histograms:
        combined.merge(h)

    ops_total = sum(op_counts)
    return RunResult(
        backend_id=backend_id,
        value_size=spec.value_size,
        mix_label=spec.mix_label,
        vcpu_budget=args.vcpu_budget,
        clients=args.clients,
        duration_s=wall,
        ops_total=ops_total,
        bytes_total=ops_total * spec.value_size,
        vcpu_consumed=vcpu,
        p50_ns=combined.p50(),
        p99_ns=combined.p99(),
        p999_ns=combined.p999(),
        errors=sum(err_counts),
    )


class Worker:
    """Per-thread handle interface implemented by each harness."""

    def do_get(self, key: bytes) -> None:
        raise NotImplementedError

    def do_set(self, key: bytes, value: bytes) -> None:
        raise NotImplementedError

    def do_get_many(self, keys: list[bytes]) -> None:
        for key in keys:
            self.do_get(key)

    def do_set_many(self, keys: list[bytes], value: bytes) -> None:
        for key in keys:
            self.do_set(key, value)
