#!/usr/bin/env python3
"""Build distinct A/B interfaces, prove the cache participant, retain JSON only.

Run on 64-bit Linux: python3 scripts/prove-cache-succession.py /path/to/evidence
No real TLS/SQL participant is implemented by this fixture.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import socket
import ctypes
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
PREDECESSOR = "d5378b1464c6291a22fbece025f46270265a6644"


def build(root, target, artifact, features, interface):
    env = dict(os.environ, CARGO_TARGET_DIR=str(target), SHARDMAP_DEMO_INTERFACE=interface,
               CARGO_CACHE_AUTO_CLEAN_FREQUENCY="never")
    result = subprocess.run(
        ["cargo", "build", "--locked", "--release", "-p", "shardmap", "--features", features,
         "--example", "cache_succession", "--message-format=json-render-diagnostics"],
        cwd=root, env=env, text=True, stdout=subprocess.PIPE, check=True,
    )
    digest = executable = None
    for line in result.stdout.splitlines():
        event = json.loads(line)
        if event["reason"] == "build-script-executed":
            identity = Path(event["out_dir"]) / "succession_abi.rs"
            if identity.is_file():
                digest = json.loads(identity.read_text().split("=", 1)[1].strip().removesuffix(";"))
        elif event["reason"] == "compiler-artifact" and event["target"]["name"] == "cache_succession":
            executable = event["executable"]
    assert len(digest) == 32 and executable
    shutil.copy2(executable, artifact)
    return {"sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
            "bytes": artifact.stat().st_size, "engine_digest": digest}


def wait_ready(path, process):
    deadline = time.monotonic() + 20
    while not path.exists():
        if process.poll() is not None:
            raise RuntimeError("owner exited before ready")
        if time.monotonic() > deadline:
            raise TimeoutError("owner readiness")
        time.sleep(0.01)


def run(output, predecessor):
    output.mkdir(parents=True, exist_ok=True)
    manifest = {"source_base": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                "source_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD"], cwd=ROOT)).hexdigest(),
                "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
                "scope": "cache component only; no TLS or SQL atomicity qualification", "artifacts": {}}
    # Sources including untracked implementation are identified independently of
    # git status. Nothing secret or host-specific is embedded in public source.
    source_paths = sorted((ROOT / "crates/shardmap/src/succession").glob("*.rs")) + [
        ROOT / "crates/shardmap/examples/cache_succession.rs", ROOT / "crates/shardmap/Cargo.toml",
        ROOT / "crates/shardmap/build.rs", ROOT / "Cargo.lock"]
    manifest["source_sha256"] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in source_paths}
    if predecessor is not None:
        head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=predecessor, text=True).strip()
        dirty = subprocess.check_output(["git", "status", "--porcelain"], cwd=predecessor, text=True)
        if head != PREDECESSOR or dirty:
            raise ValueError("predecessor proof requires the clean EDEN-3839 merge checkout")
        manifest["predecessor_head"] = head
    target = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")).resolve()
    with tempfile.TemporaryDirectory(prefix="shardmap-succession-") as scratch:
        scratch = Path(scratch)
        for name in ("A", "B", "C"):
            manifest["artifacts"][name] = build(
                predecessor if predecessor is not None and name == "A" else ROOT,
                target, scratch / name, "succession,unsafe" if name == "C" else "succession",
                "old-interface" if name == "A" else "new-interface",
            )
        assert manifest["artifacts"]["A"]["sha256"] != manifest["artifacts"]["B"]["sha256"]
        source, target_digest, incompatible = (
            manifest["artifacts"][role]["engine_digest"] for role in ("A", "B", "C")
        )
        assert (source != target_digest) == (predecessor is not None)
        assert incompatible != source and incompatible != target_digest
        assert json.loads(subprocess.check_output([str(scratch / "B"), "engine-digest"])) == target_digest
        # Each exec must explicitly protect itself before holding shared backing.
        subprocess.run([str(scratch / "A"), "unprotected"], check=True)
        subprocess.run([str(scratch / "B"), "unprotected"], check=True)
        modes = ["commit", "abort", "ambiguous", "collision", "abi", "precommit-crash", "postcommit-crash", "stray", "locked", "lock-denied"]
        if predecessor is not None:
            modes.extend(["wrong-source", "unqualified"])
        for name in modes:
            mode = "abi" if name in ("wrong-source", "unqualified") else name
            case = scratch / name
            case.mkdir(mode=0o700)
            a = b = stray = None
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
                # A same-UID supervisor is deliberately not admitted to memory custody.
                address = json.loads((case / "owner-ready.json").read_text())["mapping_base"]
                class IOVec(ctypes.Structure):
                    _fields_ = [("base", ctypes.c_void_p), ("length", ctypes.c_size_t)]
                data = ctypes.c_uint64()
                local = IOVec(ctypes.addressof(data), 8)
                remote = IOVec(address, 8)
                libc = ctypes.CDLL(None, use_errno=True)
                libc.process_vm_readv.argtypes = [ctypes.c_int, ctypes.POINTER(IOVec), ctypes.c_ulong, ctypes.POINTER(IOVec), ctypes.c_ulong, ctypes.c_ulong]
                libc.process_vm_readv.restype = ctypes.c_ssize_t
                assert libc.process_vm_readv(a.pid, ctypes.byref(local), 1, ctypes.byref(remote), 1, 0) == -1
                assert ctypes.get_errno() in (1, 13)
                if mode == "stray":
                    stray = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    stray.settimeout(10)
                    stray.connect(str(case / "control.sock"))
                receiver = "C" if name == "abi" else "B"
                command = [str(scratch / receiver), "successor", str(case), str(a.pid), mode]
                if predecessor is not None and name not in ("abi", "unqualified"):
                    command.append(json.dumps([incompatible if name == "wrong-source" else source, target_digest]))
                b = subprocess.Popen(command)
                a.stdin.write(json.dumps({"pid": b.pid, "uid": os.geteuid()}) + "\n")
                a.stdin.close()
                if stray is not None:
                    payload, descriptors, _, _ = stray.recvmsg(1, socket.CMSG_SPACE(4))
                    assert not payload and not descriptors
                    stray.close()
                assert a.wait(timeout=30) == 0
                assert b.wait(timeout=30) == (77 if mode == "precommit-crash" else 78 if mode == "postcommit-crash" else 0)
                old = json.loads((case / "owner.json").read_text())
                new = json.loads((case / "successor.json").read_text())
                assert old["receipt"]["engine_abi"] == source
                if mode in ("collision", "abi", "precommit-crash", "postcommit-crash", "lock-denied"):
                    if mode in ("collision", "abi", "lock-denied"):
                        assert new["refused"] == mode
                    (output / (name + ".json")).write_text(json.dumps({"A": old, "B": new}, indent=2) + "\n")
                    continue
                assert old["receipt"] == new["receipt"]
                assert new["engine_digest"] == target_digest
                assert old["arena_sha256_before_attach"] == new["arena_sha256_after_attach"]
                assert old["mapping_base"] == new["mapping_base"]
                if mode in ("commit", "stray", "locked"):
                    assert new["result"]["old_process_exited"]
                    assert new["result"]["crud_after_A_exit"] and new["result"]["ttl_preserved"]
                    assert new["result"]["foreign_reprepare_refused"] == (predecessor is not None)
                    assert old["slot_offset"] == new["result"]["slot_offset"]
                    assert old["value_offset"] == new["result"]["value_offset"]
                    if mode == "stray":
                        assert old["unregistered_peers_rejected_before_prepare"] == 1
                (output / (name + ".json")).write_text(json.dumps({"A": old, "B": new}, indent=2) + "\n")
            finally:
                if stray is not None:
                    stray.close()
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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--predecessor-root", type=Path, help="clean EDEN-3839 predecessor checkout")
    args = parser.parse_args()
    run(args.output.resolve(), args.predecessor_root and args.predecessor_root.resolve())
