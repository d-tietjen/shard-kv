#!/usr/bin/env python3
"""Compare public heap/native APIs, including B only after A exits.

Runs are sequential and CPU-affinity matched. Results are throughput reciprocals,
not request latency or whole-service costs. Run on 64-bit Linux (Adam for Eden).
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import signal
import statistics
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def physical_cpus():
    selected = {}
    for cpu in sorted(os.sched_getaffinity(0)):
        topology = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        key = ((topology / "physical_package_id").read_text().strip(),
               (topology / "core_id").read_text().strip())
        selected.setdefault(key, cpu)
    # Keep the same eight physical cores used by the implementation experiments
    # where available; other hosts choose their first eight distinct cores.
    values = list(selected.values())
    return values[8:16] if len(values) >= 16 else values[:8]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--trials", type=int, default=7)
    parser.add_argument("--iterations", type=int, default=2_000_000)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--binary", type=Path, help="exact previously built artifact")
    parser.add_argument("--adopted-only", action="store_true", help="candidate comparison; omit heap controls")
    parser.add_argument("--cold", action="store_true", help="first batch after A exit, no B warmup")
    args = parser.parse_args()
    if args.trials < 1 or args.iterations < 1:
        parser.error("trials and iterations must be positive")
    args.output.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["CARGO_CACHE_AUTO_CLEAN_FREQUENCY"] = "never"
    if not args.skip_build and args.binary is None:
        subprocess.run(["cargo", "build", "--offline", "--locked", "--release", "-p", "shardmap",
                        "--features", "succession", "--example", "cache_succession_bench"],
                       cwd=ROOT, env=env, check=True)
    target = Path(env.get("CARGO_TARGET_DIR", str(ROOT / "target")))
    binary = (args.binary or target / "release/examples/cache_succession_bench").resolve()
    all_methods = ["native-adopted"] if args.adopted_only else ["heap-default", "heap-matched", "native", "native-adopted"]
    if args.cold:
        all_methods = ["native-adopted-cold", "native-adopted-prefault"]
    cpus = physical_cpus()
    threads = [n for n in (1, 4, 8) if n <= len(cpus)]
    cases = [(mode, n, 8192) for mode in ("read", "ttl", "mixed", "write") for n in threads]
    cases += [("miss", n, count) for count in (8192, 14336) for n in threads if n <= 4]
    if args.cold:
        cases = [("read", 1, 8192)]
    paths = sorted((ROOT / "crates/shardmap/src/succession").glob("*.rs")) + [
        ROOT / "crates/shardmap/build.rs", ROOT / "crates/shardmap/examples/cache_succession_bench.rs",
        ROOT / "Cargo.lock", ROOT / "crates/shardmap/Cargo.toml"]
    manifest = {"source_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                "source_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},
                "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "methods": all_methods,
                "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
                "host": platform.node(), "kernel": platform.release(), "physical_cpu_affinity": cpus,
                "trials": args.trials, "iterations_per_worker": args.iterations,
                "seed": 3839, "cases": cases, "units": "aggregate wall ns/op; not tail latency",
                "controls": {"heap-default": "8192 capacity hint", "heap-matched": "28672 capacity hint, same bucket reservation as native",
                             "native": "public Cache created locally", "native-adopted": "public Cache in independently execed B, A exited/reaped before timing"},
                "scope": "bounded 8-byte-key/64-byte-value cache; no TLS/SQL or full SharedCache engine equivalence"}
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    rng = random.Random(3839)
    with (args.output / "raw.jsonl").open("w") as output:
        for trial in range(args.trials):
            ordered = cases.copy()
            rng.shuffle(ordered)
            for mode, n, count in ordered:
                methods = all_methods.copy()
                rng.shuffle(methods)
                for method in methods:
                    # time reaps A promptly in native-adopted runs; B's self usage
                    # is reported in JSON, since time observes A rather than B.
                    command = ["/usr/bin/time", "-o", os.devnull, "taskset", "-c", ",".join(map(str, cpus)),
                               str(binary), method, mode, str(n), str(count), str(args.iterations)]
                    process = subprocess.Popen(command, text=True, stdout=subprocess.PIPE,
                                               stderr=subprocess.PIPE, env=env, start_new_session=True)
                    try:
                        stdout, stderr = process.communicate(timeout=60)
                        if process.returncode:
                            raise RuntimeError(f"{method}/{mode}/{n}: {stderr}")
                        row = json.loads(stdout)
                        assert row["method"] == method
                        if method.startswith("native-adopted"):
                            assert row["old_owner_exited"]
                        row["trial"] = trial
                        output.write(json.dumps(row) + "\n")
                        output.flush()
                    finally:
                        # The group is exclusive to this invocation, including
                        # a separately execed successor if its owner has exited.
                        try:
                            os.killpg(process.pid, signal.SIGTERM)
                        except ProcessLookupError:
                            pass
                        if process.poll() is None:
                            process.wait(timeout=5)
            print(f"trial {trial + 1}/{args.trials} complete", flush=True)
    rows = [json.loads(line) for line in (args.output / "raw.jsonl").read_text().splitlines()]
    summary = []
    for mode, n, count in cases:
        result = {"mode": mode, "threads": n, "keys": count}
        for method in all_methods:
            selected = [r for r in rows if r["method"] == method and r["measurement"]["mode"] == ("cold" if args.cold else mode)
                        and r["measurement"]["threads"] == n and r["measurement"]["key_count"] == count]
            samples = [r["measurement"]["ns_per_op"] for r in selected]
            result[method] = {"median_ns": statistics.median(samples), "min_ns": min(samples), "max_ns": max(samples),
                              "median_rss_kib": statistics.median(r["process_resource"]["max_rss_kib"] for r in selected)}
        for control, name in (("heap-default", "ordinary"), ("heap-matched", "matched"), ("native", "native")):
            if control in result and "native-adopted" in result:
                result["adopted_vs_" + name] = result["native-adopted"]["median_ns"] / result[control]["median_ns"]
        summary.append(result)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
