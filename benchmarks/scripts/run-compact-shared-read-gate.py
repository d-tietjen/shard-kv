#!/usr/bin/env python3
"""Finite shared-API diagnostic with genuine child waits and identity-bound samples.

An unfinished contract cannot run. Final source/build/evidence constants are
supplied only after independent acceptance; this controller never approves them.
"""
import argparse
import fcntl
import hashlib
import datetime
import json
import math
import os
import pathlib
import select
import resource
import shutil
import signal
import stat
import statistics
import subprocess
import sys
import time

PHASES = ("empty", "borrowed_loaded", "cold_first", "cold_next", "warm_same", "all_owned",
          "quiescent_deleted", "class_reused", "epoch_retired", "epoch_reclaimed_owners_held",
          "epoch_owners_released", "reowned", "maps_dropped")
COHORTS = {
    "empty-value-sparse": (64, 1, 2, 0, 1, 1),
    "small-value-sparse": (64, 1, 18, 16, 1, 1),
    "small-value-dense": (64, 120, 18, 16, 120, 1),
    "point-100k-sparse-owned": (1, 100000, 18, 16, 120, 1),
    "eight-reader-first-owned": (64, 1, 18, 256, 1, 8),
}
FULL_SNAPSHOTS = {"empty", "borrowed_loaded", "cold_first", "quiescent_deleted", "maps_dropped"}
HEX = set("0123456789abcdef")
CGROUP_ROOT = pathlib.Path("/sys/fs/cgroup")
TARGET_BYTES = {}


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def sha(path):
    digest = hashlib.sha256()
    with pathlib.Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1048576), b""):
            digest.update(block)
    return digest.hexdigest()


def valid_sha(value):
    return isinstance(value, str) and len(value) == 64 and set(value) <= HEX


def owned_file(path, digest, maximum=134217728, executable=False):
    path = pathlib.Path(path)
    require(path.is_absolute() and path.resolve(strict=True) == path and not path.is_symlink(), "noncanonical evidence file")
    item = path.stat()
    require(stat.S_ISREG(item.st_mode) and item.st_uid == os.getuid(),
            "file ownership/type differs")
    require(item.st_size <= maximum and valid_sha(digest) and sha(path) == digest, "file size/hash differs")
    if executable:
        require(item.st_mode & stat.S_IXUSR and not item.st_mode & (stat.S_IWGRP | stat.S_IWOTH), "native mode differs")
        with path.open("rb") as stream:
            require(stream.read(4) == b"\x7fELF", "native is not ELF")
    return path


def read_json(path, maximum=65536):
    with pathlib.Path(path).open("rb") as stream:
        data = stream.read(maximum + 1)
    require(len(data) <= maximum, "JSON input exceeds bound")
    return json.loads(data)


def members():
    rows = []
    for round_id, arms in enumerate((("baseline", "candidate"), ("candidate", "baseline"), ("baseline", "candidate")), 1):
        for arm in arms:
            for number, cohort in enumerate(COHORTS, 1):
                rows.append({"id": f"r{round_id}-{arm[0]}-{number}", "round": round_id, "arm": arm,
                             "cohort": cohort, "cpus": 4 if number == 5 else 1})
    return rows


def validate_contract(plan):
    require(plan.get("schema_version") == 1 and plan.get("status") == "frozen with independently accepted source/build evidence", "unfinished contract")
    require(plan.get("classification") == "diagnostic-unreserved-shared-api", "classification differs")
    require(plan.get("members") == members(), "finite30-cohort order differs")
    require(plan.get("batches") == 4 and plan.get("warm_seconds") == 5, "phase work bounds differ")
    require(plan.get("phase_order") == list(PHASES), "phase order differs")
    require(plan.get("budgets") == {"complete_lifecycle_seconds": 5400, "workload_seconds": 4800,
            "cohort_seconds": 180, "phase_seconds": 60, "cleanup_seconds": 300,
            "output_bytes": 1073741824, "file_bytes": 134217728, "owned_disk_bytes": 32212254720}, "budgets differ")
    require(plan.get("acceptance") == {"sparse_total_pss_ratio_max": 1.0, "post_drop_pss_ratio_max": 1.0,
            "cold_p99_ratio_max": 1.05, "drop_p99_ratio_max": 1.05,
            "warm_ops_ratio_min": 0.95, "warm_p99_ratio_max": 1.05}, "prespecified screens differ")
    require(plan["builds"]["baseline"]["product_source_sha"] == "78c83addff5f140236f9a659ff1e332bb9c35522" and
            plan["builds"]["baseline"]["product_source_tree"] == "3b148db21708d2be93c23280eb9d2455a5b5d407", "baseline product differs")
    for arm in ("baseline", "candidate"):
        binding = plan["builds"][arm]
        for key in ("build_source_sha", "build_source_tree", "product_source_sha", "product_source_tree", "harness_sha"):
            require(isinstance(binding.get(key), str) and len(binding[key]) == 40 and set(binding[key]) <= HEX, "source identity is pending/malformed")
        require(binding["argv"] == ["cargo", "build", "--locked", "--release", "--jobs", "4", "-p",
                "shardcache-benchmarks", "--features", "compact-point-storage", "--bin", "compact_shared_read_cost"], "native build command differs")
        require(valid_sha(binding.get("product_input_equivalence_sha256")), "product/input equivalence pending")
    return plan


def check_build(binding):
    worktree = pathlib.Path(binding["worktree"])
    require(worktree.is_absolute() and worktree.resolve(strict=True) == worktree, "source path differs")
    for expression, expected in (("HEAD", binding["build_source_sha"]), ("HEAD^{tree}", binding["build_source_tree"])):
        actual = subprocess.check_output(["git", "-C", str(worktree), "rev-parse", expression], text=True, timeout=10).strip()
        require(actual == expected, "source HEAD/tree differs")
    require(not subprocess.check_output(["git", "-C", str(worktree), "status", "--porcelain"], text=True, timeout=10).strip(), "source worktree is dirty")
    binary = owned_file(binding["binary_path"], binding["binary_sha256"], executable=True)
    require(binary == worktree / "target/release/compact_shared_read_cost", "native path differs")
    owned_file(binding["product_input_equivalence_path"], binding["product_input_equivalence_sha256"])
    for row in binding["admitted_evidence"]:
        owned_file(row["path"], row["sha256"])
    require(len(binding["admitted_evidence"]) >= 4, "fresh build/result/wait/source evidence missing")
    env_path = owned_file(binding["compiler_receipt"]["path"], binding["compiler_receipt"]["sha256"], maximum=65536)
    env = read_json(env_path)
    require(env.get("candidate_sha") == binding["build_source_sha"] and env.get("tree_sha") == binding["build_source_tree"], "compiler source/tree differs")
    require(env.get("worktree") == str(worktree) and env.get("argv") == binding["argv"], "compiler path/command differs")
    require(env.get("artifact_paths") == {"compact_shared_read_cost": str(binary)}, "compiler artifact path differs")
    require(isinstance(env.get("cargo_version"), str) and env["cargo_version"].startswith("cargo "), "compiler cargo version missing")
    require(isinstance(env.get("rustc_vv"), str) and env["rustc_vv"].startswith("rustc ") and all(f"\n{k}: " in env["rustc_vv"] for k in ("host", "release", "LLVM version")), "compiler rustc receipt missing")
    require(isinstance(env.get("effective_path"), str) and all(p.startswith("/") for p in env["effective_path"].split(":")), "compiler PATH differs")
    result_path = owned_file(binding["result_receipt"]["path"], binding["result_receipt"]["sha256"])
    result = read_json(result_path)
    require(result.get("candidate_sha") == binding["build_source_sha"] and result.get("check_id") == binding["build_check_id"] == env.get("check_id"), "native result source/check differs")
    require(result.get("working_directory") == str(worktree) and result.get("target_directory") == str(worktree / "target") and
            result.get("command") == " ".join(binding["argv"]) and result.get("log_path") == binding["log_path"], "native result cwd/target/command/log differs")
    require(result.get("cargo_started") is True and type(result.get("cargo_exit_code")) is int and result["cargo_exit_code"] == 0 and result.get("stop_reason") is None, "native build failed/incomplete")
    checked = datetime.datetime.fromisoformat(env["checked_at_utc"].replace("Z", "+00:00"))
    started = result.get("cargo_started_at_utc_epoch")
    require(checked.utcoffset() == datetime.timedelta(0) and type(started) in (int, float) and math.isfinite(started) and
            0 < checked.timestamp() <= started, "compiler receipt is not UTC/prebuild")
    # Genuine numeric wrapper/build exit admission remains exact hash-pinned metadata;
    # this parser validates the compiler/result correspondence, not an invented status.
    return binary


def start_ticks(raw_stat):
    tail = raw_stat[raw_stat.rfind(")") + 2:].split()
    require(len(tail) >= 20, "short process stat")
    return int(tail[19])


def proc_identity(pid, executable):
    root = pathlib.Path(f"/proc/{pid}")
    raw = (root / "stat").read_text()
    require(int(raw.split(" ", 1)[0]) == pid, "stat PID differs")
    status = (root / "status").read_text()
    fields = dict(line.split(":", 1) for line in status.splitlines() if ":" in line)
    require([int(v) for v in fields["Uid"].split()] == [os.getuid()] * 4, "process UID differs")
    require([int(v) for v in fields["NSpid"].split()] == [pid], "native namespace PID differs")
    require(os.readlink(root / "exe") == str(executable), "executed native differs")
    require((root / "exe").stat().st_ino == executable.stat().st_ino and (root / "exe").stat().st_dev == executable.stat().st_dev, "native executable inode differs")
    return {"pid": pid, "start_ticks": start_ticks(raw), "raw_stat": raw,
            "exe": str(executable), "uid": os.getuid(), "boot_id": pathlib.Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "user_namespace": os.readlink(root / "ns/user")}


def cgroup_path(pid):
    lines = pathlib.Path(f"/proc/{pid}/cgroup").read_text().splitlines()
    unified = [line[3:] for line in lines if line.startswith("0::")]
    require(len(unified) == 1 and unified[0].startswith("/") and ".." not in unified[0].split("/"), "cgroup membership differs")
    path = pathlib.Path("/sys/fs/cgroup") / unified[0].lstrip("/")
    require(path.resolve(strict=True) == path and not path.is_symlink(), "cgroup path is noncanonical")
    return path


def verify_scope(pid, unit, memory, cpus):
    boundary = cgroup_path(pid)
    require(boundary.name == unit, "native left exact owned scope")
    root = CGROUP_ROOT
    chain = []
    for node in (boundary, *boundary.parents):
        if node == root.parent: break
        controls = {}
        for name in ("memory.max", "memory.swap.max", "cpu.max"):
            path = node / name
            if not path.exists():
                require(node == root, "missing non-root hierarchy control")
                controls[name] = None
            else:
                controls[name] = path.read_text().strip()
        chain.append({"path": str(node), "controls": controls})
        if node == boundary:
            require(controls == {"memory.max": str(memory), "memory.swap.max": "0", "cpu.max": f"{cpus * 100000} 100000"}, "owned scope ceilings differ")
        for name in ("memory.max", "memory.swap.max"):
            value = controls[name]
            require(value is None or value == "max" or int(value) >= (memory if name == "memory.max" else 0), "tighter shared memory ancestor")
        value = controls["cpu.max"]
        if value is not None:
            quota, period = value.split()
            require(int(period) > 0, "CPU period is nonpositive")
            require(quota == "max" or int(quota) >= cpus * int(period), "tighter CPU ancestor")
        if node == root: break
    require(chain[-1]["path"] == str(root), "host root missing")
    return {"path": str(boundary), "chain": chain}


def sample(pid, binary, identity, unit, cpus):
    before = proc_identity(pid, binary)
    require(before["start_ticks"] == identity["start_ticks"] and before["boot_id"] == identity["boot_id"], "native identity changed")
    scope = verify_scope(pid, unit, 4294967296, cpus)
    metrics = {}
    for line in pathlib.Path(f"/proc/{pid}/smaps_rollup").read_text().splitlines():
        key, _, value = line.partition(":")
        if key in ("Pss", "Rss", "Private_Clean", "Private_Dirty"):
            words = value.split(); require(words[-1] == "kB", "smaps unit differs")
            metrics[key.lower() + "_bytes"] = int(words[0]) * 1024
    require(set(metrics) == {"pss_bytes", "rss_bytes", "private_clean_bytes", "private_dirty_bytes"}, "missing process memory metric")
    stats = dict(line.split() for line in (pathlib.Path(scope["path"]) / "memory.stat").read_text().splitlines())
    metrics["cgroup_anon_bytes"] = int(stats["anon"])
    metrics["cgroup_current_bytes"] = int((pathlib.Path(scope["path"]) / "memory.current").read_text())
    after = proc_identity(pid, binary)
    require(after["start_ticks"] == identity["start_ticks"] and cgroup_path(pid) == pathlib.Path(scope["path"]), "sample identity/cgroup changed")
    return {"pid": pid, "start_ticks": identity["start_ticks"], "scope": scope, "metrics": metrics}


def key(index, length):
    return index.to_bytes(2, "big") if length == 2 else f"k:{index:016x}".encode()


def expected_hash(shape, live, value_bytes, marker):
    maps, records, length, _, _, _ = shape
    digest = hashlib.sha256(b"compact-shared-read-cost-v1\0")
    if live:
        value = bytes([marker]) * value_bytes
        for m in range(maps):
            for i in range(records):
                k = key(i, length)
                for part in (m.to_bytes(8, "little"), len(k).to_bytes(8, "little"), k,
                             len(value).to_bytes(8, "little"), value): digest.update(part)
    return digest.hexdigest()


def expected_state(cohort, phase):
    shape = COHORTS[cohort]
    live = phase not in ("empty", "quiescent_deleted", "maps_dropped")
    value = shape[3] if PHASES.index(phase) < PHASES.index("class_reused") else (64 if phase == "class_reused" else 17)
    marker = 0x42 if PHASES.index(phase) < PHASES.index("class_reused") else (0x63 if phase == "class_reused" else 0x27)
    count = shape[0] * shape[1] if live else 0
    return shape, count, value, expected_hash(shape, live, value, marker)


def validate_event(event, member, batch, phase):
    require(isinstance(event, dict) and event.get("schema_version") == 1 and event.get("batch") == batch and event.get("phase") == phase,
            "native phase identity/order differs")
    require(event.get("status") == "awaiting_snapshot_ack", "native phase status differs")
    shape, count, value, digest = expected_state(member["cohort"], phase)
    require(event.get("cohort") == dict(zip(("maps", "records_per_map", "key_bytes", "value_bytes", "owned_step", "cold_readers"), shape)), "native shape differs")
    require(event.get("logical_records") == count and event.get("logical_value_bytes") == count * value and
            event.get("logical_key_value_bytes") == count * (shape[2] + value) and event.get("dataset_sha256") == digest,
            "native logical state/digest differs")
    for timing_key in ("timing", "last_live_delete_timing"):
        timing = event.get(timing_key)
        if timing is not None:
            count = timing.get("operations")
            require(type(count) is int and count >= 0 and type(timing.get("elapsed_ns")) is int, "timing count/unit differs")
            for p in ("p50_ns", "p99_ns", "p999_ns"):
                require(timing.get(p) is None if count == 0 else type(timing.get(p)) is int and timing[p] > 0, "missing tail/false absent timing")
            require(timing["elapsed_ns"] >= 0 and (count == 0 or timing["elapsed_ns"] > 0), "timing duration is invalid")
            require(len(timing.get("first_samples_ns", [])) <= 256, "raw timing samples exceed bound")
    selected = (shape[1] + shape[4] - 1) // shape[4] * shape[0]
    if phase == "warm_same":
        require(event["timing"]["operations"] > 0 and event["timing"]["elapsed_ns"] >= 5000000000, "incomplete warm interval")
    if phase == "quiescent_deleted":
        require(event["timing"]["operations"] == shape[0] * shape[1] and event["last_live_delete_timing"]["operations"] == shape[0], "delete counts differ")
    if phase == "cold_first":
        require(event["timing"]["operations"] == selected * shape[5], "cold reader count differs")
    if phase == "cold_next":
        require(event["timing"]["operations"] == (selected if shape[1] > 1 else 0), "next-slot applicability differs")
    if phase == "maps_dropped":
        require(event.get("live_maps") == 0 and event["timing"]["operations"] == shape[0], "drop map count differs")
    return event


def terminal_wait(pid, raw):
    require(pid > 0 and os.WIFEXITED(raw) and os.WEXITSTATUS(raw) == 0, "native did not genuinely exit0")
    return {"waited_pid": pid, "raw_wait_status": raw, "wait_source": "os.waitpid(actual_native_child_pid, os.WNOHANG)",
            "wait_observed": True, "child_reaped": True, "exit_code": os.WEXITSTATUS(raw), "signal": None}


def write(path, obj):
    data = (json.dumps(obj, sort_keys=True) + "\n").encode()
    require(len(data) <= 134217728, "receipt exceeds file bound")
    with pathlib.Path(path).open("xb") as stream:
        stream.write(data); stream.flush(); os.fsync(stream.fileno())


def stop_child(pid):
    for sig, seconds in ((signal.SIGTERM, 10), (signal.SIGKILL, 10)):
        try: os.killpg(pid, sig)
        except ProcessLookupError: pass
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            waited, raw = os.waitpid(pid, os.WNOHANG)
            if waited == pid: return {"waited_pid": waited, "raw_wait_status": raw, "terminated": True}
            time.sleep(0.1)
    raise RuntimeError("native cleanup/reaping deadline reached")


def run_one(plan, member, output):
    binary = check_build(plan["builds"][member["arm"]])
    unit = f"{plan['scope_prefix']}-{member['id']}.scope"
    verify_scope(os.getpid(), unit, 4294967296, member["cpus"])
    resource.setrlimit(resource.RLIMIT_FSIZE, (134217728, 134217728))
    require(signal.getsignal(signal.SIGCHLD) in (signal.SIG_DFL, None), "unexpected SIGCHLD handler")
    signal.signal(signal.SIGCHLD, signal.SIG_DFL)
    output.mkdir(mode=0o700)
    read_fd, child_stdout = os.pipe(); child_stdin, write_fd = os.pipe()
    argv = [str(binary), "--cohort", member["cohort"], "--batches", "4", "--warm-seconds", "5"]
    pid = os.fork()
    if pid == 0:
        os.setsid(); os.dup2(child_stdin, 0); os.dup2(child_stdout, 1)
        stderr = os.open(output / "native-stderr.log", os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.dup2(stderr, 2)
        for fd in (read_fd, child_stdout, child_stdin, write_fd, stderr): os.close(fd)
        os.execv(str(binary), argv)
    os.close(child_stdout); os.close(child_stdin)
    started = time.monotonic(); identity = None; reaped = False; buffer = b""; events = []
    wait = None
    sequence = [(b, p) for b in range(1, 5) for p in PHASES]
    try:
        for batch, phase in sequence + [(None, "complete")]:
            phase_started = time.monotonic()
            while b"\n" not in buffer:
                require(time.monotonic() - started < 180 and time.monotonic() - phase_started < 60, "native cohort/phase deadline")
                require(not pathlib.Path(plan["stop_path"]).exists(), "STOP requested")
                if select.select([read_fd], [], [], 0.2)[0]:
                    chunk = os.read(read_fd, 8192); require(chunk, "native EOF before complete")
                    buffer += chunk; require(len(buffer) <= 65536, "native phase line exceeds bound")
            line, buffer = buffer.split(b"\n", 1); event = json.loads(line)
            if phase == "complete":
                require(event == {"schema_version": 1, "status": "complete", "cohort": member["cohort"], "batches": 4, "phases_per_batch": len(PHASES)}, "native completion differs")
                break
            budget_check(plan)
            validate_event(event, member, batch, phase)
            require(event.get("pid") == pid, "native reported wrong child PID")
            if identity is None:
                identity = proc_identity(pid, binary)
                write(output / "native-start.json", {"argv": argv, "identity": identity, "member": member,
                      "parent_pid": os.getpid(), "build_binding": plan["builds"][member["arm"]], "SIGCHLD": "SIG_DFL", "exec": "fork/setsid/execv exact native; no shell"})
            samples = []
            if phase in FULL_SNAPSHOTS: time.sleep(1)
            for _ in range(5 if phase in FULL_SNAPSHOTS else 1):
                require(time.monotonic() - started < 180, "sampling exceeded cohort deadline")
                samples.append(sample(pid, binary, identity, unit, member["cpus"])); time.sleep(0.2)
            receipt = {"native_event": event, "samples": samples,
                       "medians": {k: statistics.median(s["metrics"][k] for s in samples) for k in samples[0]["metrics"]}}
            write(output / f"batch-{batch:02d}-{phase}.json", receipt); events.append(receipt)
            os.write(write_fd, b"continue\n")
        os.close(write_fd); write_fd = -1
        while time.monotonic() - started < 180:
            waited, raw = os.waitpid(pid, os.WNOHANG)
            if waited == pid:
                reaped = True
                wait = {"waited_pid": waited, "raw_wait_status": raw, "wait_observed": True, "child_reaped": True}
                checked = terminal_wait(waited, raw)
                wait = checked; break
            time.sleep(0.1)
        require(wait is not None, "native terminal wait deadline")
        require(not buffer and not os.read(read_fd, 1), "extra native output after completion")
        require(sha(binary) == plan["builds"][member["arm"]]["binary_sha256"], "native changed during cohort")
        write(output / "native-result.json", {"success": True, "member": member, "wait": wait,
              "identity": identity, "phase_count": len(events), "sample_count": sum(len(e["samples"]) for e in events),
              "argv": argv, "build_binding": plan["builds"][member["arm"]], "elapsed_seconds": time.monotonic() - started})
    except BaseException as exc:
        cleanup = None if reaped else stop_child(pid)
        write(output / "native-failure.json", {"success": False, "error": str(exc), "wait": wait, "cleanup": cleanup,
              "member": member, "completed_phases": len(events)})
        raise
    finally:
        os.close(read_fd)
        if write_fd >= 0: os.close(write_fd)


def summarize(root, plan):
    results = {}
    for member in plan["members"]:
        folder = root / member["id"]
        result = read_json(folder / "native-result.json")
        require(result.get("success") is True and result["wait"]["exit_code"] == 0 and result["wait"]["child_reaped"], "incomplete cohort")
        for phase in ("cold_first", "cold_next", "warm_same", "quiescent_deleted", "maps_dropped"):
            batches = [read_json(folder / f"batch-{b:02d}-{phase}.json") for b in range(1, 5)]
            # Batch summaries stay visible; per-process aggregation is explicitly the median,
            # followed by a median of three independent process-round observations.
            row = {"pss_bytes": statistics.median(r["medians"]["pss_bytes"] for r in batches)}
            if phase != "quiescent_deleted":
                values = [r["native_event"]["timing"] for r in batches]
            else:
                values = [r["native_event"]["last_live_delete_timing"] for r in batches]
            if values[0]["operations"]:
                row["p99_ns"] = statistics.median(v["p99_ns"] for v in values)
                row["ops_per_sec"] = statistics.median(v["operations"] * 1e9 / v["elapsed_ns"] for v in values)
            results.setdefault((member["cohort"], phase, member["arm"]), []).append(row)
    comparisons = []
    misses = []
    for cohort in COHORTS:
        for phase in ("cold_first", "cold_next", "warm_same", "quiescent_deleted", "maps_dropped"):
            arms = {arm: results[(cohort, phase, arm)] for arm in ("baseline", "candidate")}
            require(all(len(rows) == 3 for rows in arms.values()), "missing/extra independent observations")
            medians = {arm: {k: statistics.median(r[k] for r in rows) for k in rows[0]} for arm, rows in arms.items()}
            ratios = {k: medians["candidate"][k] / medians["baseline"][k] for k in medians["baseline"]}
            screens = {}
            if phase == "cold_first": screens["sparse_total_pss"] = ratios["pss_bytes"] <= 1.0
            if phase in ("quiescent_deleted", "maps_dropped"): screens["post_drop_pss"] = ratios["pss_bytes"] <= 1.0
            if "p99_ns" in ratios:
                screens["tail"] = ratios["p99_ns"] <= 1.05
                if phase == "warm_same": screens["throughput"] = ratios["ops_per_sec"] >= 0.95
            for name, passed in screens.items():
                if not passed: misses.append({"cohort": cohort, "phase": phase, "screen": name})
            comparisons.append({"cohort": cohort, "phase": phase, "individual_process_observations": arms,
                                "medians": medians, "ratios_of_medians": ratios, "screens": screens})
    return {"classification": plan["classification"], "functional_success": True, "screen_success": not misses,
            "screen_misses": misses, "comparisons": comparisons, "cause_and_significance": "unknown",
            "aggregation": "Median of four fresh sequential batches per process; ratio of medians of three process-round observations. Per-run p99 medians are not pooled percentiles.",
            "qualification": "Shared host, unreserved and unpinned; diagnostic only; a miss returns to design, no posthoc waiver."}


def budget_check(plan):
    root = pathlib.Path(plan["output_root"])
    if root.exists():
        files = [p for p in root.rglob("*") if p.is_file()]
        require(all(not p.is_symlink() and p.stat().st_size <= 134217728 for p in files), "output file bound/link differs")
        output = sum(p.stat().st_size for p in files)
    else:
        output = 0
    owned = output
    for build in plan["builds"].values():
        target = pathlib.Path(build["worktree"]) / "target"
        # Fresh accounting once per observer process. Reviewed native code never writes
        # target trees; no Cargo/build runs concurrently with the gate. Hardlinks count twice.
        if target not in TARGET_BYTES:
            TARGET_BYTES[target] = sum(p.stat().st_size for p in target.rglob("*") if p.is_file() and not p.is_symlink())
        owned += TARGET_BYTES[target]
    require(output < 1073741824 and owned < 32212254720, "sampled output/owned-disk stop threshold")
    require(shutil.disk_usage(root.parent).free >= 100000000000, "free-disk floor")
    mem = dict(line.split(":", 1) for line in pathlib.Path("/proc/meminfo").read_text().splitlines())
    require(int(mem["MemAvailable"].split()[0]) * 1024 >= 8589934592, "available-RAM floor")
    require(not pathlib.Path(plan["stop_path"]).exists(), "STOP requested")


def scope_absent(member, root, unit):
    folder = root / member["id"]
    result = read_json(folder / "native-result.json")
    identity = result["identity"]
    proc = pathlib.Path(f"/proc/{identity['pid']}/stat")
    require(not proc.exists() or start_ticks(proc.read_text()) != identity["start_ticks"], "recorded native process still exists")
    sample_receipt = read_json(folder / "batch-01-empty.json")
    boundary = pathlib.Path(sample_receipt["samples"][0]["scope"]["path"])
    deadline = time.monotonic() + 10
    while boundary.exists() and time.monotonic() < deadline: time.sleep(0.1)
    require(not boundary.exists(), "owned worker cgroup remains")
    state = subprocess.check_output(["systemctl", "--user", "show", unit, "-p", "ActiveState", "--value"], text=True, timeout=10).strip()
    require(state in ("inactive", "failed"), "owned scope still active")
    write(root / f"{member['id']}-cleanup.json", {"unit": unit, "native_identity": identity,
          "recorded_native_absent": True, "recorded_cgroup_path": str(boundary), "cgroup_absent": True, "active_state": state})


def interrupted(signum, _frame):
    raise RuntimeError(f"received termination signal {signum}")


def main():
    os.umask(0o077)
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("all", "cohort")); parser.add_argument("contract", type=pathlib.Path)
    parser.add_argument("contract_sha256"); parser.add_argument("member", nargs="?"); args = parser.parse_args()
    require(valid_sha(args.contract_sha256) and sha(args.contract) == args.contract_sha256, "contract differs from pinned launcher argument")
    plan = validate_contract(read_json(args.contract, maximum=262144))
    require(os.uname().nodename == "adam", "Adam-only entrypoint")
    script = pathlib.Path(__file__).resolve(); owned_file(script, plan["script_sha256"])
    root = pathlib.Path(plan["output_root"]); require(root.is_absolute() and root.parent.resolve(strict=True) == root.parent, "output root parent differs")
    if args.action == "cohort":
        selected = [m for m in plan["members"] if m["id"] == args.member]; require(len(selected) == 1, "unknown cohort ID")
        run_one(plan, selected[0], root / args.member); return
    require(args.member is None and not root.exists(), "fresh run root/member differs")
    verify_scope(os.getpid(), plan["controller_scope"], 2147483648, 4)
    # All pre-effect helper/native/admission receipts are checked before creating run output.
    for binding in plan["builds"].values(): check_build(binding)
    require(not pathlib.Path(plan["stop_path"]).exists(), "STOP requested")
    require(isinstance(plan["scope_prefix"], str) and plan["scope_prefix"].startswith("eden2266-compact-shared-") and
            len(plan["scope_prefix"]) <= 64 and set(plan["scope_prefix"]) <= set("abcdefghijklmnopqrstuvwxyz0123456789-"), "owned scope prefix differs")
    lock_path = pathlib.Path(plan["lock_path"])
    require(lock_path.parent == root.parent and lock_path.name.startswith(plan["scope_prefix"]), "owned lock path differs")
    lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    lock_identity = os.fstat(lock_fd)
    active = None
    completed = []
    cleanup_closed = True
    root_created = False
    started = time.monotonic()
    try:
        root.mkdir(mode=0o700)
        root_created = True
        for member in plan["members"]:
            require(time.monotonic() - started < 4800, "whole-workload deadline")
            budget_check(plan)
            owned_file(script, plan["script_sha256"])
            unit = f"{plan['scope_prefix']}-{member['id']}"
            state = subprocess.check_output(["systemctl", "--user", "show", unit + ".scope", "-p", "LoadState", "--value"], text=True, timeout=10).strip()
            require(state == "not-found", "owned scope name already exists")
            argv = ["systemd-run", "--user", "--scope", "--quiet", "--unit", unit,
                    "-p", "MemoryMax=4G", "-p", "MemorySwapMax=0", "-p", f"CPUQuota={member['cpus'] * 100}%",
                    "-p", "CPUQuotaPeriodSec=100ms", "-p", "TasksMax=64",
                    sys.executable, str(script), "cohort", str(args.contract), args.contract_sha256, member["id"]]
            write(root / f"{member['id']}-intent.json", {"unit": unit + ".scope", "argv": argv, "member": member,
                  "initial_load_state": state, "script_sha256": plan["script_sha256"], "contract_sha256": args.contract_sha256})
            active = unit + ".scope"
            with (root / f"{member['id']}-wrapper.log").open("xb") as log:
                ran = subprocess.run(argv, stdout=log, stderr=subprocess.STDOUT, timeout=min(240, max(1, 4800 - (time.monotonic() - started))), check=False)
            write(root / f"{member['id']}-wrapper.json", {"argv": argv, "returncode": ran.returncode, "member": member})
            require(ran.returncode == 0, "cohort wrapper failed; stop subsequent cohorts")
            scope_absent(member, root, active)
            active = None
            completed.append(member["id"])
        write(root / "summary.json", summarize(root, plan))
        write(root / "controller-result.json", {"success": True, "elapsed_seconds": time.monotonic() - started,
              "member_count": 30, "external_lifecycle_acceptance": "pending exact operator cleanup and independent review"})
    except BaseException as exc:
        cleanup = None
        cleanup_errors = []
        if active is not None:
            cleanup_closed = False
            cleanup_started = time.monotonic()
            try:
                group = subprocess.check_output(["systemctl", "--user", "show", active, "-p", "ControlGroup", "--value"], text=True, timeout=10).strip()
                require(not group or group.startswith("/") and ".." not in group.split("/") and pathlib.Path(group).name == active, "failure scope membership differs")
                boundary = CGROUP_ROOT / group.lstrip("/") if group else None
                for sig in ("SIGTERM", "SIGKILL"):
                    subprocess.run(["systemctl", "--user", "kill", "--signal", sig, active], timeout=10, check=False)
                    if sig == "SIGTERM": time.sleep(2)
                state = subprocess.check_output(["systemctl", "--user", "show", active, "-p", "ActiveState", "--value"], text=True, timeout=10).strip()
                until = time.monotonic() + 10
                while boundary is not None and boundary.exists() and time.monotonic() < until: time.sleep(0.1)
                require(state in ("inactive", "failed") and (boundary is None or not boundary.exists()), "owned failure scope remains")
                cleanup = {"unit": active, "active_state": state, "recorded_cgroup": str(boundary) if boundary else None,
                           "cgroup_absent": True, "elapsed_seconds": time.monotonic() - cleanup_started}
                cleanup_closed = True
            except BaseException as cleanup_exc:
                cleanup_errors.append(str(cleanup_exc))
        if root_created:
            write(root / "controller-failure.json", {"success": False, "error": str(exc), "completed": completed,
                  "cleanup": cleanup, "cleanup_errors": cleanup_errors, "owned_lock_retained": not cleanup_closed})
        raise
    finally:
        current = lock_path.lstat()
        require((current.st_dev, current.st_ino) == (lock_identity.st_dev, lock_identity.st_ino), "owned lock identity changed")
        fcntl.flock(lock_fd, fcntl.LOCK_UN); os.close(lock_fd)
        if cleanup_closed: lock_path.unlink()



if __name__ == "__main__":
    main()
