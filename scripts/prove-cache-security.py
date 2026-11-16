#!/usr/bin/env python3
"""Isolated process probes for the default feature and succession protections."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def main(output):
    output.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["CARGO_CACHE_AUTO_CLEAN_FREQUENCY"] = "never"
    target = Path(env.get("CARGO_TARGET_DIR", str(ROOT / "target")))
    results = []
    with tempfile.TemporaryDirectory(prefix="shardmap-security-") as scratch:
        for feature, modes in ((None, ("default",)), ("succession", (
                "default", "protected", "fork", "exec", "locked",
                "locked-denied", "pageable-zero-limit", "deny-madvise", "deny-dontfork"))):
            command = ["cargo", "build", "--locked", "--release", "-p", "shardmap",
                       "--example", "cache_security"]
            if feature:
                command += ["--features", feature]
            subprocess.run(command, cwd=ROOT, env=env, check=True)
            binary = Path(scratch) / (feature or "default")
            shutil.copy2(target / "release/examples/cache_security", binary)
            digest = hashlib.sha256(binary.read_bytes()).hexdigest()
            for mode in modes:
                run = subprocess.run([str(binary), mode], check=True, text=True,
                                     capture_output=True, timeout=15)
                results.append({"feature": feature, "mode": mode,
                                "binary_sha256": digest, "output": run.stdout.strip()})
    (output / "protection-probes.json").write_text(json.dumps(results, indent=2) + "\n")
    print(f"PASS: {len(results)} isolated default/mapping/residency/inheritance/failure probes")


if __name__ == "__main__":
    main(Path(sys.argv[1]).resolve())
