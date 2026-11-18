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
import shutil
import signal
import statistics
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
PREDECESSOR = "d5378b1464c6291a22fbece025f46270265a6644"
EXAMPLE = Path("crates/shardmap/examples/cache_succession_bench.rs")
BEGIN_ENGINE_API = "// BEGIN CANDIDATE ENGINE API\n"
END_ENGINE_API = "// END CANDIDATE ENGINE API\n"
PREDECESSOR_ENGINE_API = """
fn engine_digest() -> Result<serde_json::Value, Box<dyn Error>> {
    protect_current_process()?;
    let cache = Cache::create(profile())?;
    // SAFETY: getppid only identifies the supervising process. This disposable
    // preparation reads the local build fingerprint and sends no descriptor.
    let pid = unsafe { libc::getppid() }.try_into()?;
    let peer = Peer { pid, uid: Peer::current().uid };
    let prepared = cache.prepare(Operation([0; 16]), peer, Duration::ZERO)?;
    let digest = serde_json::to_value(prepared.readiness().engine_abi)?;
    drop(prepared.abort()?);
    Ok(digest)
}

fn target_digest(source: [u8; 32]) -> serde_json::Value {
    // The predecessor's exact receiver already checked this against its build.
    json!(source)
}

fn receive_cache(stream: &mut UnixStream, peer: Peer) -> Result<Staged, Box<dyn Error>> {
    // SAFETY: The predecessor driver receives only its own exact build.
    Ok(unsafe { Staged::receive(stream, peer) }?)
}
"""
BENCH_ENV = (
    "SHARDMAP_BENCH_SUCCESSOR",
    "SHARDMAP_BENCH_SOURCE_DIGEST",
    "SHARDMAP_BENCH_TARGET_DIGEST",
)
BUILD_MANIFEST = "build-manifest.json"


def build(root, env):
    """Build the same release example and feature set for either engine."""
    subprocess.run(["cargo", "build", "--offline", "--locked", "--release", "-p", "shardmap",
                    "--features", "succession", "--example", "cache_succession_bench"],
                   cwd=root, env=env, check=True)


def digest(path):
    """Hash exact input or executable bytes for the run manifest."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def engine_digest(binary, env):
    """Read the build identity produced by the actual launched fixture."""
    value = json.loads(subprocess.check_output([binary, "__engine_digest"], env=env))
    if not isinstance(value, list) or len(value) != 32 or any(
        type(byte) is not int or not 0 <= byte <= 255 for byte in value
    ):
        raise ValueError("invalid compiled engine digest")
    return value


def compiler_identity(root, env):
    """Read the compiler selected under this source directory."""
    return subprocess.check_output(["rustc", "--version", "--verbose"], cwd=root, env=env, text=True).strip()


def predecessor_driver(checkout, scratch, target, env):
    """Export the pinned engine and build only a benchmark-specific driver."""
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=checkout, text=True).strip()
    if revision != PREDECESSOR:
        raise ValueError(f"predecessor checkout must identify {PREDECESSOR}, got {revision}")
    source = scratch / "predecessor-source"
    source.mkdir()
    with subprocess.Popen(["git", "archive", revision], cwd=checkout, stdout=subprocess.PIPE) as archive:
        with tarfile.open(fileobj=archive.stdout, mode="r|") as stream:
            stream.extractall(source, filter="data")
        archive.stdout.read()
        if archive.wait():
            raise RuntimeError("failed to export the predecessor source")
    # Instrument only the example. The exact engine source, build script,
    # dependency lock and manifests are all read from the immutable Git export.
    before, start, remainder = (ROOT / EXAMPLE).read_text().partition(BEGIN_ENGINE_API)
    _, end, after = remainder.partition(END_ENGINE_API)
    if not start or not end:
        raise ValueError("candidate-only benchmark adapter markers are missing")
    adapter = source / EXAMPLE
    adapter.write_text(before + PREDECESSOR_ENGINE_API + after)
    compiler = compiler_identity(source, env)
    if compiler != compiler_identity(ROOT, env):
        raise ValueError("predecessor and candidate must use the same Rust compiler")
    build(source, env | {"CARGO_TARGET_DIR": str(target)})
    binary = scratch / "predecessor-bench"
    shutil.copy2(target / "release/examples/cache_succession_bench", binary)
    return binary, {
        "revision": revision, "benchmark_example_sha256": digest(adapter), "rustc": compiler,
        "binary_sha256": digest(binary), "engine_digest": engine_digest(binary, env),
    }


def save_compiled_pair(output, candidate, predecessor, predecessor_receipt, env):
    """Retain the exact binaries and source/build identities for a separate timed run."""
    candidate_receipt = {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "working_tree": subprocess.check_output(["git", "status", "--porcelain=v1"], cwd=ROOT, text=True).splitlines(),
        "benchmark_example_sha256": digest(ROOT / EXAMPLE),
        "binary_sha256": digest(candidate),
        "engine_digest": engine_digest(candidate, env),
    }
    if candidate_receipt["engine_digest"] == predecessor_receipt["engine_digest"]:
        raise ValueError("compiled pair has the same engine identity")
    receipt = {
        "schema": "shardmap.succession-benchmark-build.v1",
        "candidate": candidate_receipt, "predecessor": predecessor_receipt,
        "rustc": compiler_identity(ROOT, env),
        "features": ["succession"], "profile": "release",
    }
    for binary, name in ((candidate, "candidate-bench"), (predecessor, "predecessor-bench")):
        shutil.copy2(binary, output / name)
    (output / BUILD_MANIFEST).write_text(json.dumps(receipt, indent=2) + "\n")


def verify_compiled_pair(candidate, predecessor, env):
    """Recheck both exact fixture engines and ELFs before opening any timing output."""
    receipt_path = predecessor.parent / BUILD_MANIFEST
    receipt = json.loads(receipt_path.read_text())
    if (receipt["schema"] != "shardmap.succession-benchmark-build.v1"
            or receipt["predecessor"]["revision"] != PREDECESSOR):
        raise ValueError("compiled pair manifest has the wrong predecessor or schema")
    if receipt["predecessor"].get("rustc") != receipt["rustc"]:
        raise ValueError("compiled pair used different Rust compilers")
    for role, binary in (("candidate", candidate), ("predecessor", predecessor)):
        pinned = receipt[role]
        if digest(binary) != pinned["binary_sha256"] or engine_digest(binary, env) != pinned["engine_digest"]:
            raise ValueError(f"{role} artifact differs from the compiled pair manifest")
    return receipt


def summarize(selected):
    """Compute trial medians and process-wide page-table changes for one case."""
    samples = [row["measurement"]["ns_per_op"] for row in selected]
    result = {"median_ns": statistics.median(samples), "min_ns": min(samples), "max_ns": max(samples),
              "median_rss_kib": statistics.median(row["process_resource"]["max_rss_kib"] for row in selected)}
    if all(row.get("adoption") and row.get("owner") for row in selected):
        for name in ("prepare_ns", "ready_ns", "commit_ns"):
            result["median_" + name] = statistics.median(row["owner"][name] for row in selected)
        for name in ("attach_ns", "prefault_ns"):
            result["median_" + name] = statistics.median(row["adoption"][name] for row in selected)
        snapshots = [row["adoption"]["vm_pte_kib"] for row in selected]
        for label, before, after in (
            ("attach", "before_attach", "after_attach"),
            ("prefault", "after_attach", "after_prefault"),
            ("workload", "before_workload", "after_workload"),
        ):
            result[f"median_process_vm_pte_{label}_delta_kib"] = statistics.median(
                snapshot[after] - snapshot[before] for snapshot in snapshots
            )
    if all("first_batch_minor_faults" in row["measurement"] for row in selected):
        result["median_first_batch_minor_faults"] = statistics.median(
            row["measurement"]["first_batch_minor_faults"] for row in selected
        )
    return result


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
    predecessor = parser.add_mutually_exclusive_group()
    predecessor.add_argument("--predecessor-root", type=Path,
                             help="exact predecessor checkout for additional A/A and qualified A/B comparisons")
    predecessor.add_argument("--predecessor-binary", type=Path,
                             help="exact A artifact from --compile-only, used with --binary")
    parser.add_argument("--compile-only", action="store_true",
                        help="build and retain both artifacts without running timed cases")
    args = parser.parse_args()
    if args.trials < 1 or args.iterations < 1:
        parser.error("trials and iterations must be positive")
    if args.compile_only and (args.predecessor_root is None or args.skip_build or args.binary is not None):
        parser.error("--compile-only requires --predecessor-root and builds both artifacts")
    if args.predecessor_binary is not None and args.binary is None:
        parser.error("--predecessor-binary requires --binary")
    args.output.mkdir(parents=True, exist_ok=True)
    if args.compile_only and any(
        (args.output / name).exists() for name in ("candidate-bench", "predecessor-bench", BUILD_MANIFEST)
    ):
        parser.error("--compile-only output already contains an artifact or manifest")
    env = os.environ.copy()
    env["CARGO_CACHE_AUTO_CLEAN_FREQUENCY"] = "never"
    for name in BENCH_ENV:
        env.pop(name, None)
    if not args.skip_build and args.binary is None:
        build(ROOT, env)
    target = Path(env.get("CARGO_TARGET_DIR", str(ROOT / "target"))).resolve()
    binary = (args.binary or target / "release/examples/cache_succession_bench").resolve()
    if args.predecessor_binary is not None:
        predecessor = args.predecessor_binary.resolve()
        receipt = verify_compiled_pair(binary, predecessor, env)
        run(args, env, binary, predecessor, receipt["predecessor"], receipt)
        return
    if args.predecessor_root is None:
        run(args, env, binary)
        return
    # Use the checkout's toolchain for the predecessor source copy too. A
    # candidate built under a parent rust-toolchain override must keep it here.
    scratch_root = ROOT / "target"
    scratch_root.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="shardmap-pair-", dir=scratch_root) as directory:
        scratch = Path(directory)
        # A/B share sequential Cargo dependencies. Preserve B before A's build
        # uses the same target output name.
        candidate = scratch / "candidate-bench"
        shutil.copy2(binary, candidate)
        predecessor, receipt = predecessor_driver(args.predecessor_root, scratch, target, env)
        if args.compile_only:
            save_compiled_pair(args.output, candidate, predecessor, receipt, env)
        else:
            run(args, env, candidate, predecessor, receipt)


def run(args, env, binary, predecessor=None, predecessor_receipt=None, build_receipt=None):
    """Run exact-build controls and, when requested, the directional A/B lane."""
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
    variants = [(method, method, binary, env) for method in all_methods]
    manifest = {"source_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                "source_sha256": {str(p.relative_to(ROOT)): digest(p) for p in paths},
                "binary_sha256": digest(binary),
                "methods": all_methods,
                "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
                "host": platform.node(), "kernel": platform.release(), "physical_cpu_affinity": cpus,
                "trials": args.trials, "iterations_per_worker": args.iterations,
                "seed": 3839, "cases": cases, "units": "aggregate wall ns/op; not tail latency",
                "controls": {"heap-default": "8192 capacity hint", "heap-matched": "28672 capacity hint, same bucket reservation as native",
                             "native": "public Cache created locally", "native-adopted": "public Cache in independently execed B, A exited/reaped before timing"},
                "scope": "bounded 8-byte-key/64-byte-value cache; no TLS/SQL or full SharedCache engine equivalence"}
    if predecessor is not None:
        source = predecessor_receipt["engine_digest"]
        destination = engine_digest(binary, env)
        if source == destination or digest(binary) == predecessor_receipt["binary_sha256"]:
            raise ValueError("qualified comparison requires distinct engine and executable identities")
        qualified_env = env | {
            "SHARDMAP_BENCH_SUCCESSOR": str(binary),
            "SHARDMAP_BENCH_SOURCE_DIGEST": json.dumps(source),
            "SHARDMAP_BENCH_TARGET_DIGEST": json.dumps(destination),
        }
        for method in (name for name in all_methods if name.startswith("native-adopted")):
            variants.append(("predecessor-" + method, method, predecessor, env))
            variants.append(("qualified-" + method, method, predecessor, qualified_env))
        manifest["predecessor"] = predecessor_receipt
        manifest["candidate_engine_digest"] = destination
        manifest["methods"] = [variant for variant, _, _, _ in variants]
        manifest["controls"]["predecessor-native-adopted"] = "exact predecessor A to exact predecessor A"
        manifest["controls"]["qualified-native-adopted"] = "exact predecessor A to qualified distinct engine B"
        if build_receipt is not None:
            manifest["compiled_artifacts"] = build_receipt
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    rng = random.Random(3839)
    with (args.output / "raw.jsonl").open("w") as output:
        for trial in range(args.trials):
            ordered = cases.copy()
            rng.shuffle(ordered)
            for mode, n, count in ordered:
                methods = variants.copy()
                rng.shuffle(methods)
                for variant, method, owner_binary, run_env in methods:
                    # time reaps A promptly in native-adopted runs; B's self usage
                    # is reported in JSON, since time observes A rather than B.
                    command = ["/usr/bin/time", "-o", os.devnull, "taskset", "-c", ",".join(map(str, cpus)),
                               str(owner_binary), method, mode, str(n), str(count), str(args.iterations)]
                    process = subprocess.Popen(command, text=True, stdout=subprocess.PIPE,
                                               stderr=subprocess.PIPE, env=run_env, start_new_session=True)
                    try:
                        stdout, stderr = process.communicate(timeout=60)
                        if process.returncode:
                            raise RuntimeError(f"{method}/{mode}/{n}: {stderr}")
                        row = json.loads(stdout)
                        assert row["method"] == method
                        if method.startswith("native-adopted"):
                            assert row["old_owner_exited"]
                            owner = [line.removeprefix("owner-metrics ") for line in stderr.splitlines()
                                     if line.startswith("owner-metrics ")]
                            if owner:
                                assert len(owner) == 1
                                row["owner"] = json.loads(owner[0])
                            if predecessor is not None:
                                expected = source if owner_binary == predecessor else destination
                                target = source if variant.startswith("predecessor-") else destination
                                assert row["owner"]["source_engine_digest"] == expected
                                assert row["adoption"]["source_engine_digest"] == expected
                                assert row["adoption"]["target_engine_digest"] == target
                        row["variant"] = variant
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
        for variant, _, _, _ in variants:
            selected = [r for r in rows if r["variant"] == variant and r["measurement"]["mode"] == ("cold" if args.cold else mode)
                        and r["measurement"]["threads"] == n and r["measurement"]["key_count"] == count]
            result[variant] = summarize(selected)
        for control, name in (("heap-default", "ordinary"), ("heap-matched", "matched"), ("native", "native")):
            if control in result and "native-adopted" in result:
                result["adopted_vs_" + name] = result["native-adopted"]["median_ns"] / result[control]["median_ns"]
        if predecessor is not None:
            for method in (name for name in all_methods if name.startswith("native-adopted")):
                result["qualified_vs_predecessor_" + method] = (
                    result["qualified-" + method]["median_ns"] / result["predecessor-" + method]["median_ns"]
                )
        summary.append(result)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
