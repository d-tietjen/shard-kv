#!/usr/bin/env python3
"""Build distinct A/B interfaces, prove the cache participant, retain JSON only.

Run on 64-bit Linux: python3 scripts/prove-cache-succession.py /path/to/evidence
No real TLS/SQL participant is implemented by this fixture.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]


def wait_ready(path, process):
    deadline = time.monotonic() + 20
    while not path.exists():
        if process.poll() is not None:
            raise RuntimeError("owner exited before ready")
        if time.monotonic() > deadline:
            raise TimeoutError("owner readiness")
        time.sleep(0.01)


def run(output):
    output.mkdir(parents=True, exist_ok=True)
    manifest = {"source_base": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                "source_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD"], cwd=ROOT)).hexdigest(),
                "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
                "scope": "cache component only; no TLS or SQL atomicity qualification", "artifacts": {}}
    # Sources including untracked implementation are identified independently of
    # git status. Nothing secret or host-specific is embedded in public source.
    source_paths = sorted((ROOT / "crates/shardmap/src/succession").glob("*.rs")) + [
        ROOT / "crates/shardmap/examples/cache_succession.rs", ROOT / "Cargo.lock"]
    manifest["source_sha256"] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in source_paths}
    with tempfile.TemporaryDirectory(prefix="shardmap-succession-") as scratch:
        scratch = Path(scratch)
        for name in ("A", "B", "C"):
            env = os.environ.copy()
            env["SHARDMAP_DEMO_INTERFACE"] = "old-interface" if name == "A" else "new-interface"
            subprocess.run(["cargo", "build", "--locked", "--release", "-p", "shardmap", "--features", ("succession,unsafe" if name == "C" else "succession"), "--example", "cache_succession"], cwd=ROOT, env=env, check=True)
            target = Path(env.get("CARGO_TARGET_DIR", str(ROOT / "target")))
            shutil.copy2(target / "release/examples/cache_succession", scratch / name)
            manifest["artifacts"][name] = {"sha256": hashlib.sha256((scratch / name).read_bytes()).hexdigest(), "bytes": (scratch / name).stat().st_size}
        assert manifest["artifacts"]["A"]["sha256"] != manifest["artifacts"]["B"]["sha256"]
        # Each exec must explicitly protect itself before holding shared backing.
        subprocess.run([str(scratch / "A"), "unprotected"], check=True)
        subprocess.run([str(scratch / "B"), "unprotected"], check=True)
        for mode in ("commit", "abort", "ambiguous", "collision", "abi", "precommit-crash", "postcommit-crash"):
            case = scratch / mode
            case.mkdir(mode=0o700)
            a = b = None
            try:
                a = subprocess.Popen([str(scratch / "A"), "owner", str(case), mode], stdin=subprocess.PIPE, text=True)
                wait_ready(case / "owner-ready.json", a)
                # This supervisor is intentionally not admitted to cache custody.
                # Non-dumpability must prevent even same-UID descriptor inspection.
                try:
                    os.listdir(f"/proc/{a.pid}/fd")
                except PermissionError:
                    pass
                else:
                    raise AssertionError("unregistered supervisor could inspect cache descriptors")
                b = subprocess.Popen([str(scratch / ("C" if mode == "abi" else "B")), "successor", str(case), str(a.pid), mode])
                a.stdin.write(json.dumps({"pid": b.pid, "uid": os.geteuid()}) + "\n")
                a.stdin.close()
                assert a.wait(timeout=30) == 0
                assert b.wait(timeout=30) == (77 if mode == "precommit-crash" else 78 if mode == "postcommit-crash" else 0)
                old = json.loads((case / "owner.json").read_text())
                new = json.loads((case / "successor.json").read_text())
                if mode in ("collision", "abi", "precommit-crash", "postcommit-crash"):
                    if mode in ("collision", "abi"):
                        assert new["refused"] == mode
                    (output / (mode + ".json")).write_text(json.dumps({"A": old, "B": new}, indent=2) + "\n")
                    continue
                assert old["receipt"] == new["receipt"]
                assert old["arena_sha256_before_attach"] == new["arena_sha256_after_attach"]
                assert old["mapping_base"] == new["mapping_base"]
                if mode == "commit":
                    assert new["result"]["old_process_exited"]
                    assert old["slot_offset"] == new["result"]["slot_offset"]
                    assert old["value_offset"] == new["result"]["value_offset"]
                (output / (mode + ".json")).write_text(json.dumps({"A": old, "B": new}, indent=2) + "\n")
            finally:
                for proc in (a, b):
                    if proc is not None and proc.poll() is None:
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait(timeout=5)
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print("PASS: distinct A/B artifacts; same native table/backing/pointers; A exit; abort; ambiguous decision; fixed-address profile")


if __name__ == "__main__":
    run(Path(sys.argv[1]).resolve())
