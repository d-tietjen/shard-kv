#!/usr/bin/env python3
"""Offline fixture regressions for the early API gate; run only on Adam.

Fixture ELF/receipt bytes and mocked git responses are NOT native build evidence.
"""
import copy
import importlib.util
import json
import os
import pathlib
import resource
import signal
import tempfile
import time
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location("compact_shared_gate", pathlib.Path(__file__).with_name("run-compact-shared-read-gate.py"))
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)
ARGV = ["cargo", "build", "--locked", "--release", "--jobs", "4", "-p", "shardcache-benchmarks", "--features", "compact-point-storage", "--bin", "compact_shared_read_cost"]


def contract():
    source = {"build_source_sha": "a" * 40, "build_source_tree": "b" * 40,
              "product_source_sha": "c" * 40, "product_source_tree": "d" * 40,
              "harness_sha": "e" * 40, "argv": ARGV, "product_input_equivalence_sha256": "0" * 64}
    baseline = dict(source, product_source_sha="78c83addff5f140236f9a659ff1e332bb9c35522", product_source_tree="3b148db21708d2be93c23280eb9d2455a5b5d407")
    return {"schema_version": 1, "status": "frozen with independently accepted source/build evidence",
            "classification": "diagnostic-unreserved-shared-api", "members": gate.members(), "batches": 4, "warm_seconds": 5,
            "phase_order": list(gate.PHASES), "budgets": {"complete_lifecycle_seconds": 5400, "workload_seconds": 4800,
                "cohort_seconds": 180, "phase_seconds": 60, "cleanup_seconds": 300, "output_bytes": 1073741824,
                "file_bytes": 134217728, "owned_disk_bytes": 32212254720},
            "acceptance": {"sparse_total_pss_ratio_max": 1.0, "post_drop_pss_ratio_max": 1.0, "cold_p99_ratio_max": 1.05,
                "drop_p99_ratio_max": 1.05, "warm_ops_ratio_min": 0.95, "warm_p99_ratio_max": 1.05},
            "builds": {"baseline": baseline, "candidate": source}}


def event(member, phase):
    shape, count, value, digest = gate.expected_state(member["cohort"], phase)
    timing = {"operations": 0, "elapsed_ns": 0, "p50_ns": None, "p99_ns": None, "p999_ns": None, "first_samples_ns": []}
    selected = (shape[1] + shape[4] - 1) // shape[4] * shape[0]
    if phase == "cold_first": count_time = selected * shape[5]
    elif phase == "cold_next": count_time = selected if shape[1] > 1 else 0
    elif phase == "maps_dropped": count_time = shape[0]
    elif phase == "warm_same": count_time = 100
    elif phase == "quiescent_deleted": count_time = shape[0] * shape[1]
    else: count_time = 0
    if count_time:
        timing.update(operations=count_time, elapsed_ns=5000000000, p50_ns=10, p99_ns=20, p999_ns=30)
    last_delete = dict(timing, operations=shape[0]) if phase == "quiescent_deleted" else None
    return {"schema_version": 1, "batch": 1, "phase": phase, "status": "awaiting_snapshot_ack",
            "cohort": dict(zip(("maps", "records_per_map", "key_bytes", "value_bytes", "owned_step", "cold_readers"), shape)),
            "logical_records": count, "logical_value_bytes": count * value, "logical_key_value_bytes": count * (shape[2] + value),
            "dataset_sha256": digest, "timing": timing, "last_live_delete_timing": last_delete,
            "live_maps": 0 if phase == "maps_dropped" else shape[0]}


class GateRegressionTests(unittest.TestCase):
    def test_exact_thirty_rotated_members_and_distinct_four_cpu_contention(self):
        rows = gate.members()
        self.assertEqual(len(rows), 30)
        self.assertEqual(sum(r["cpus"] == 4 for r in rows), 6)
        self.assertEqual([rows[i]["arm"] for i in (0, 10, 20)], ["baseline", "candidate", "baseline"])
        self.assertEqual(len({r["id"] for r in rows}), 30)

    def test_unfinished_contract_cannot_admit_effects(self):
        p = contract(); p["status"] = "pending"
        with self.assertRaises(RuntimeError): gate.validate_contract(p)

    def test_acceptance_cannot_be_relaxed_after_data(self):
        p = contract(); gate.validate_contract(p)
        p["acceptance"]["cold_p99_ratio_max"] = 1.2
        with self.assertRaises(RuntimeError): gate.validate_contract(p)

    def test_pending_source_identity_and_changed_baseline_are_rejected(self):
        for field, value in (("build_source_sha", None), ("product_source_sha", "f" * 40)):
            p = contract(); p["builds"]["baseline"][field] = value
            with self.assertRaises(RuntimeError): gate.validate_contract(p)

    def test_four_cpu_case_cannot_be_relabelled_as_single_cpu(self):
        p = contract(); p["members"][4]["cpus"] = 1
        with self.assertRaises(RuntimeError): gate.validate_contract(p)

    def test_phase_and_batch_order_are_enforced(self):
        m = gate.members()[0]; e = event(m, "empty")
        for changed in ({"phase": "cold_first"}, {"batch": 2}):
            bad = dict(e, **changed)
            with self.assertRaises(RuntimeError): gate.validate_event(bad, m, 1, "empty")

    def test_same_cardinality_wrong_dataset_is_rejected(self):
        m = gate.members()[1]; e = event(m, "borrowed_loaded"); e["dataset_sha256"] = "0" * 64
        with self.assertRaises(RuntimeError): gate.validate_event(e, m, 1, "borrowed_loaded")

    def test_logical_key_bytes_are_not_confused_with_value_bytes(self):
        m = gate.members()[0]; e = event(m, "borrowed_loaded")
        self.assertEqual(e["logical_value_bytes"], 0); self.assertEqual(e["logical_key_value_bytes"], 128)
        gate.validate_event(e, m, 1, "borrowed_loaded")
        e["logical_key_value_bytes"] = 0
        with self.assertRaises(RuntimeError): gate.validate_event(e, m, 1, "borrowed_loaded")

    def test_missing_contended_reader_is_rejected(self):
        m = gate.members()[4]; e = event(m, "cold_first"); self.assertEqual(e["timing"]["operations"], 512)
        e["timing"]["operations"] = 64
        with self.assertRaises(RuntimeError): gate.validate_event(e, m, 1, "cold_first")

    def test_no_next_slot_is_explicit_absence_not_zero_tail(self):
        m = gate.members()[0]; e = event(m, "cold_next"); gate.validate_event(e, m, 1, "cold_next")
        e["timing"]["p99_ns"] = 0
        with self.assertRaises(RuntimeError): gate.validate_event(e, m, 1, "cold_next")

    def test_warm_phase_cannot_be_empty_or_shortened(self):
        m = gate.members()[1]
        for changed in ({"operations": 0}, {"elapsed_ns": 1000}):
            e = event(m, "warm_same"); e["timing"].update(changed)
            with self.assertRaises(RuntimeError): gate.validate_event(e, m, 1, "warm_same")

    def test_drop_cannot_leave_logical_records_or_maps(self):
        m = gate.members()[0]; e = event(m, "maps_dropped"); gate.validate_event(e, m, 1, "maps_dropped")
        for changed in ({"logical_records": 1}, {"live_maps": 1}):
            with self.assertRaises(RuntimeError): gate.validate_event(dict(e, **changed), m, 1, "maps_dropped")

    def test_process_stat_comm_parentheses_do_not_shift_start_identity(self):
        fields = ["S"] + ["0"] * 18 + ["123456"] + ["0"] * 5
        self.assertEqual(gate.start_ticks("71 (name with ) and spaces) " + " ".join(fields)), 123456)

    def test_hierarchy_ceiling_inheritance_and_tighter_ancestor_rejection(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp); parent = root / "parent"; boundary = parent / "owned.scope"; boundary.mkdir(parents=True)
            for node, mem, cpu in ((parent, "max", "max 100000"), (boundary, "4294967296", "400000 100000")):
                for name, value in (("memory.max", mem), ("memory.swap.max", "0"), ("cpu.max", cpu)):
                    (node / name).write_text(value)
            with mock.patch.object(gate, "CGROUP_ROOT", root), mock.patch.object(gate, "cgroup_path", return_value=boundary):
                self.assertEqual(gate.verify_scope(71, "owned.scope", 4294967296, 4)["path"], str(boundary))
                (parent / "cpu.max").write_text("100000 100000")
                with self.assertRaises(RuntimeError): gate.verify_scope(71, "owned.scope", 4294967296, 4)

    def test_missing_nonroot_controls_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp); boundary = root / "owned.scope"; boundary.mkdir()
            with mock.patch.object(gate, "CGROUP_ROOT", root), mock.patch.object(gate, "cgroup_path", return_value=boundary):
                with self.assertRaises(RuntimeError): gate.verify_scope(71, "owned.scope", 4294967296, 1)

    def test_owned_elf_0700_and_cargo_hardlinks_are_valid_but_changed_hash_is_not(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = pathlib.Path(tmp) / "native"; p.write_bytes(b"\x7fELFinert-fixture"); p.chmod(0o700)
            os.link(p, p.with_name("cargo-deps-hardlink"))
            self.assertEqual(gate.owned_file(p, gate.sha(p), executable=True), p)
            with self.assertRaises(RuntimeError): gate.owned_file(p, "0" * 64, executable=True)

    def test_genuine_wait_zero_is_distinct_from_nonzero_or_signal(self):
        old = signal.getsignal(signal.SIGCHLD); signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        try:
            for code in (0, 7):
                pid = os.fork()
                if pid == 0: os._exit(code)
                waited, raw = os.waitpid(pid, 0)
                if code == 0: self.assertEqual(gate.terminal_wait(waited, raw)["exit_code"], 0)
                else:
                    with self.assertRaises(RuntimeError): gate.terminal_wait(waited, raw)
            with self.assertRaises(RuntimeError): gate.terminal_wait(71, signal.SIGTERM)
        finally: signal.signal(signal.SIGCHLD, old)

    def test_reaped_child_lost_ownership_never_signals_a_reused_group(self):
        pid = os.fork()
        if pid == 0: os._exit(7)
        waiter = gate.NativeChildWait(pid); waiter.bind()
        os.waitpid(pid, 0)  # Release the actual child PID before exercising cleanup.
        with mock.patch.object(gate.os, "killpg") as groups, mock.patch.object(gate.os, "kill") as processes:
            receipt = gate.stop_child(waiter)
        groups.assert_not_called(); processes.assert_not_called()
        self.assertTrue(receipt["ownership_lost"])
        self.assertFalse(receipt["wait_observed"]); self.assertFalse(receipt["child_reaped"])
        self.assertIsNone(receipt["waited_pid"]); self.assertIsNone(receipt["raw_wait_status"])

    def test_wait_bookkeeping_keeps_genuine_status_when_sigterm_is_pending(self):
        pid = os.fork()
        if pid == 0: os._exit(7)
        waiter = gate.NativeChildWait(pid); waiter.bind()
        real_wait = os.waitpid
        previous = signal.signal(signal.SIGTERM, gate.interrupted)
        def interrupted_wait(actual_pid, flags):
            self.assertEqual(actual_pid, pid); self.assertEqual(flags, os.WNOHANG)
            result = real_wait(actual_pid, 0)
            os.kill(os.getpid(), signal.SIGTERM)  # Pending until observation is saved.
            return result
        try:
            with mock.patch.object(gate.os, "waitpid", side_effect=interrupted_wait):
                with self.assertRaisesRegex(RuntimeError, "termination signal"):
                    waiter.poll()
            self.assertEqual(waiter.wait["waited_pid"], pid)
            self.assertEqual(os.WEXITSTATUS(waiter.wait["raw_wait_status"]), 7)
            with mock.patch.object(gate.os, "killpg") as groups, mock.patch.object(gate.os, "kill") as processes:
                receipt = gate.stop_child(waiter)
            groups.assert_not_called(); processes.assert_not_called()
            self.assertTrue(receipt["wait_observed"]); self.assertTrue(receipt["child_reaped"])
        finally:
            signal.signal(signal.SIGTERM, previous)
            try: real_wait(pid, 0)
            except ChildProcessError: pass

    def test_changed_recorded_child_identity_refuses_cleanup_signals(self):
        reader, writer = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(writer); os.read(reader, 1); os._exit(0)
        os.close(reader)
        waiter = gate.NativeChildWait(pid); waiter.bind(); waiter.identity["start_ticks"] += 1
        try:
            with mock.patch.object(gate.os, "killpg") as groups, mock.patch.object(gate.os, "kill") as processes:
                with self.assertRaisesRegex(RuntimeError, "identity differs"):
                    gate.stop_child(waiter)
            groups.assert_not_called(); processes.assert_not_called()
        finally:
            os.write(writer, b"x"); os.close(writer); os.waitpid(pid, 0)

    def compiler_fixture(self, root):
        work = root / "work"; target = work / "target/release"; target.mkdir(parents=True)
        native = target / "compact_shared_read_cost"; native.write_bytes(b"\x7fELFinert-only"); native.chmod(0o700)
        rows = []
        def retain(name, data):
            p = root / name; p.write_text(json.dumps(data)); return {"path": str(p), "sha256": gate.sha(p)}
        for i in range(4): rows.append(retain(f"admission-{i}.json", {"fixture": "not actual build evidence"}))
        env = {"candidate_sha": "a" * 40, "tree_sha": "b" * 40, "check_id": "api-fixture",
               "worktree": str(work), "argv": ARGV, "artifact_paths": {"compact_shared_read_cost": str(native)},
               "cargo_version": "cargo fixture", "rustc_vv": "rustc fixture\nhost: fixture\nrelease: fixture\nLLVM version: fixture\n",
               "effective_path": "/fixture/bin", "checked_at_utc": "2026-01-01T00:00:00Z"}
        result = {"candidate_sha": "a" * 40, "check_id": "api-fixture", "working_directory": str(work),
                  "target_directory": str(work / "target"), "command": " ".join(ARGV), "log_path": str(root / "build.log"),
                  "cargo_started": True, "cargo_exit_code": 0, "stop_reason": None, "cargo_started_at_utc_epoch": 1767225601}
        binding = dict(contract()["builds"]["candidate"], worktree=str(work), binary_path=str(native), binary_sha256=gate.sha(native),
                       product_input_equivalence_path=rows[0]["path"], product_input_equivalence_sha256=rows[0]["sha256"],
                       admitted_evidence=rows, compiler_receipt=retain("compiler.json", env), result_receipt=retain("result.json", result),
                       build_check_id="api-fixture", log_path=result["log_path"])
        def git_fixture(argv, **_kwargs):
            return "a" * 40 if argv[-1] == "HEAD" else "b" * 40 if argv[-1] == "HEAD^{tree}" else ""
        return binding, env, result, git_fixture

    def test_compiler_hash_alone_cannot_admit_wrong_source_command_or_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            binding, env, _result, git = self.compiler_fixture(pathlib.Path(tmp))
            with mock.patch.object(gate.subprocess, "check_output", side_effect=git):
                self.assertEqual(str(gate.check_build(binding)), binding["binary_path"])
                for field, value in (("candidate_sha", "f" * 40), ("argv", ARGV + ["--unreviewed"]), ("artifact_paths", {})):
                    wrong = dict(env, **{field: value}); p = pathlib.Path(binding["compiler_receipt"]["path"])
                    p.write_text(json.dumps(wrong)); binding["compiler_receipt"]["sha256"] = gate.sha(p)
                    with self.assertRaises(RuntimeError): gate.check_build(binding)

    def test_native_result_and_prebuild_time_correspondence_are_enforced(self):
        with tempfile.TemporaryDirectory() as tmp:
            binding, env, result, git = self.compiler_fixture(pathlib.Path(tmp))
            with mock.patch.object(gate.subprocess, "check_output", side_effect=git):
                for changed in ({"cargo_exit_code": 7}, {"target_directory": str(pathlib.Path(tmp) / "foreign")},
                                {"cargo_started_at_utc_epoch": 1767225500}):
                    p = pathlib.Path(binding["result_receipt"]["path"]); p.write_text(json.dumps(dict(result, **changed)))
                    binding["result_receipt"]["sha256"] = gate.sha(p)
                    with self.assertRaises(RuntimeError): gate.check_build(binding)

    def run_inert_protocol(self, root, variant):
        # Native fixture uses exact JSON protocol and genuine fork/exec/wait, never FlatMap.
        member = gate.members()[0]
        messages = []
        for batch in range(1, 5):
            for phase in gate.PHASES:
                item = event(member, phase); item["batch"] = batch; messages.append(item)
        source = "#!/usr/bin/python3\nimport json,os,sys,time\n"
        if variant == "timeout": source += "time.sleep(3600)\n"
        source += "messages=" + repr(messages) + "\n"
        source += "for i,item in enumerate(messages):\n"
        source += " item['pid']=os.getpid()" + ("+1" if variant == "wrong-pid" else "") + "\n"
        source += " print(json.dumps(item),flush=True)\n"
        if variant == "truncated": source += " if i==0: sys.exit(0)\n"
        source += " if sys.stdin.buffer.read(9)!=b'continue\\n': sys.exit(8)\n"
        source += "print(json.dumps({'schema_version':1,'status':'complete','cohort':'empty-value-sparse','batches':4,'phases_per_batch':13}),flush=True)\n"
        if variant == "interrupted-live": source += "time.sleep(3600)\n"
        source += "sys.exit(" + ("7" if variant == "nonzero" else "0") + ")\n"
        native = root / "inert-native"; native.write_text(source); native.chmod(0o700)
        stop = root / "STOP"
        if variant == "stop": stop.touch()
        plan = {"builds": {"baseline": {"binary_sha256": gate.sha(native)}},
                "scope_prefix": "eden2266-compact-shared-fixture", "stop_path": str(stop)}
        def identity(pid, _binary):
            return {"pid": pid, "start_ticks": gate.child_wait_identity(pid)["start_ticks"], "boot_id": "inert-fixture"}
        def sampled(pid, _binary, who, unit, cpus):
            return {"pid": pid, "start_ticks": who["start_ticks"], "scope": {"path": "/inert-only", "chain": []},
                    "metrics": {"pss_bytes": 1024, "rss_bytes": 2048}}
        real_wait = os.waitpid; real_clock = time.monotonic; real_stop = gate.stop_child
        injected = False; clock_calls = 0
        def observed_wait(pid, flags):
            nonlocal injected
            actual = real_wait(pid, flags)
            if variant == "lost-wait" and actual[0] == pid:
                raise ChildProcessError("fixture external waiter already reaped actual child")
            if not injected and ((variant == "interrupted-reap" and actual[0] == pid) or
                                 (variant == "interrupted-live" and actual[0] == 0)):
                injected = True; os.kill(os.getpid(), signal.SIGTERM)
            return actual
        def clock():
            nonlocal clock_calls
            clock_calls += 1
            return real_clock() + (200 if variant == "timeout" and clock_calls >= 3 else 0)
        def cleanup(waiter):
            return real_stop(waiter)
        previous = signal.signal(signal.SIGTERM, gate.interrupted)
        try:
            with mock.patch.object(gate, "check_build", return_value=native), \
             mock.patch.object(gate, "verify_scope", return_value={}), \
             mock.patch.object(gate, "proc_identity", side_effect=identity), \
             mock.patch.object(gate, "sample", side_effect=sampled), \
             mock.patch.object(gate, "budget_check"), mock.patch.object(gate.time, "sleep"), \
             mock.patch.object(gate.os, "waitpid", side_effect=observed_wait), \
             mock.patch.object(gate.time, "monotonic", side_effect=clock), \
             mock.patch.object(gate, "stop_child", side_effect=cleanup):
                gate.run_one(plan, member, root / "output")
        finally:
            signal.signal(signal.SIGTERM, previous)
        return root / "output"

    def isolated_file_limit_protocol(self, inherited):
        # Lower only an isolated child's real limits; the suite keeps its outer cap.
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp); reader, writer = os.pipe(); pid = None; waiter = None
            try:
                with gate.block_termination() as previous_mask:
                    pid = os.fork()
                    if pid == 0:
                        try:
                            os.close(writer); os.read(reader, 1); os.close(reader)
                            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
                            signal.signal(signal.SIGALRM, signal.SIG_DFL); signal.alarm(8)
                            resource.setrlimit(resource.RLIMIT_FSIZE, inherited)
                            before = resource.getrlimit(resource.RLIMIT_FSIZE)
                            output = self.run_inert_protocol(root, "success")
                            after = resource.getrlimit(resource.RLIMIT_FSIZE)
                            result = gate.read_json(output / "native-result.json")
                            gate.write(root / "limit-result.json", {"inherited": before, "effective": after,
                                       "protocol": result, "child_pid": os.getpid()})
                            os._exit(0)
                        except BaseException as exc:
                            try: gate.write(root / "limit-failure.json", {"error": str(exc), "error_type": type(exc).__name__})
                            except BaseException: pass
                            os._exit(1)
                    waiter = gate.NativeChildWait(pid); waiter.bind()
                os.close(reader); reader = -1
                os.write(writer, b"x"); os.close(writer); writer = -1
                until = time.monotonic() + 10
                while waiter.poll() is None and time.monotonic() < until: time.sleep(.01)
                self.assertIsNotNone(waiter.wait, "isolated file-limit probe deadline")
                failure = (root / "limit-failure.json").read_text() if (root / "limit-failure.json").exists() else ""
                self.assertEqual(waiter.wait["raw_wait_status"], 0, failure)
                self.assertIs(waiter.wait["child_reaped"], True); self.assertIs(waiter.wait["wait_observed"], True)
                row = gate.read_json(root / "limit-result.json")
                self.assertEqual(row["child_pid"], pid); self.assertEqual(row["inherited"], list(inherited))
                self.assertEqual(row["effective"], [min(134217728, v) for v in inherited])
                result = row["protocol"]
                self.assertTrue(result["success"]); self.assertEqual(result["phase_count"], 52)
                self.assertEqual(result["sample_count"], 132); self.assertEqual(result["wait"]["raw_wait_status"], 0)
                self.assertIs(result["wait"]["child_reaped"], True); self.assertIs(result["wait"]["wait_observed"], True)
                row["isolation_wait"] = waiter.wait
                print(json.dumps({"file_limit_probe": row}, sort_keys=True), flush=True)
                return row
            finally:
                for fd in (reader, writer):
                    if fd >= 0: os.close(fd)
                if waiter is not None: gate.stop_child(waiter)

    def test_run_one_preserves_real_inherited_stricter_soft_and_hard_file_limits(self):
        before = resource.getrlimit(resource.RLIMIT_FSIZE)
        for limits in ((16777216, 16777216), (8388608, 16777216)):
            with self.subTest(inherited=limits): self.isolated_file_limit_protocol(limits)
        self.assertEqual(resource.getrlimit(resource.RLIMIT_FSIZE), before)

    def test_run_one_normal128_file_limit_protocol_has_genuine_child_wait(self):
        before = resource.getrlimit(resource.RLIMIT_FSIZE)
        for limits in ((134217728, 134217728), (16777216, 134217728)):
            with self.subTest(inherited=limits): self.isolated_file_limit_protocol(limits)
        self.assertEqual(resource.getrlimit(resource.RLIMIT_FSIZE), before)

    def test_inert_full_protocol_records_fifty_two_phases_and_genuine_exit_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = self.run_inert_protocol(pathlib.Path(tmp), "success")
            result = gate.read_json(folder / "native-result.json")
            self.assertTrue(result["success"]); self.assertEqual(result["phase_count"], 52)
            self.assertEqual(result["sample_count"], 132); self.assertEqual(result["wait"]["raw_wait_status"], 0)
            self.assertTrue(result["wait"]["child_reaped"])

    def test_inert_complete_protocol_nonzero_exit_is_preserved_not_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.assertRaises(RuntimeError): self.run_inert_protocol(root, "nonzero")
            receipt = gate.read_json(root / "output/native-failure.json")
            self.assertFalse(receipt["success"])
            self.assertEqual(os.WEXITSTATUS(receipt["wait"]["raw_wait_status"]), 7)
            self.assertTrue(receipt["wait"]["child_reaped"])
            self.assertEqual(receipt["cleanup"]["signals_sent"], [])
            self.assertFalse((root / "output/native-result.json").exists())

    def test_inert_wrong_reported_pid_stops_and_reaps_child(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.assertRaises(RuntimeError): self.run_inert_protocol(root, "wrong-pid")
            receipt = gate.read_json(root / "output/native-failure.json")
            self.assertEqual(receipt["completed_phases"], 0); self.assertTrue(receipt["cleanup"]["terminated"])

    def test_inert_premature_eof_is_failure_and_reaped(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.assertRaises(RuntimeError): self.run_inert_protocol(root, "truncated")
            receipt = gate.read_json(root / "output/native-failure.json")
            self.assertTrue(receipt["cleanup"]["wait_observed"]); self.assertTrue(receipt["cleanup"]["child_reaped"])
            self.assertEqual(receipt["cleanup"]["waited_pid"], receipt["cleanup"]["identity"]["pid"])
            self.assertLess(receipt["completed_phases"], 52)

    def test_inert_interrupted_terminal_wait_retains_primary_and_genuine_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.assertRaisesRegex(RuntimeError, "termination signal"):
                self.run_inert_protocol(root, "interrupted-reap")
            receipt = gate.read_json(root / "output/native-failure.json")
            self.assertIn("termination signal", receipt["error"])
            self.assertTrue(receipt["wait"]["wait_observed"]); self.assertEqual(receipt["wait"]["raw_wait_status"], 0)
            self.assertEqual(receipt["cleanup"]["signals_sent"], []); self.assertEqual(receipt["cleanup_errors"], [])
            self.assertFalse((root / "output/native-result.json").exists())

    def test_inert_interrupted_live_wait_stops_and_reaps_same_child(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.assertRaisesRegex(RuntimeError, "termination signal"):
                self.run_inert_protocol(root, "interrupted-live")
            receipt = gate.read_json(root / "output/native-failure.json")
            pid = gate.read_json(root / "output/native-child.json")["identity"]["pid"]
            self.assertEqual(receipt["wait"]["waited_pid"], pid); self.assertTrue(receipt["wait"]["child_reaped"])
            self.assertTrue(receipt["cleanup"]["signals_sent"])
            self.assertTrue(all(action["pid"] == pid for action in receipt["cleanup"]["signals_sent"]))
            self.assertIn("termination signal", receipt["error"]); self.assertEqual(receipt["cleanup_errors"], [])

    def test_inert_lost_wait_ownership_preserves_unknown_failure_without_signals(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.assertRaisesRegex(RuntimeError, "ECHILD"):
                self.run_inert_protocol(root, "lost-wait")
            receipt = gate.read_json(root / "output/native-failure.json")
            self.assertIsNone(receipt["wait"]); self.assertTrue(receipt["cleanup"]["ownership_lost"])
            self.assertIsNone(receipt["cleanup"]["raw_wait_status"])
            self.assertFalse(receipt["cleanup"]["wait_observed"])
            self.assertEqual(receipt["cleanup"]["signals_sent"], [])
            self.assertIn("ECHILD", receipt["error"]); self.assertTrue(receipt["cleanup_errors"])
            self.assertFalse((root / "output/native-result.json").exists())

    def test_inert_timeout_retains_failure_and_reaps_same_owned_child(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.assertRaisesRegex(RuntimeError, "deadline"):
                self.run_inert_protocol(root, "timeout")
            receipt = gate.read_json(root / "output/native-failure.json")
            pid = gate.read_json(root / "output/native-child.json")["identity"]["pid"]
            self.assertEqual(receipt["wait"]["waited_pid"], pid); self.assertTrue(receipt["wait"]["child_reaped"])
            self.assertTrue(receipt["cleanup"]["signals_sent"])
            self.assertTrue(all(action["pid"] == pid for action in receipt["cleanup"]["signals_sent"]))
            self.assertIn("deadline", receipt["error"]); self.assertEqual(receipt["cleanup_errors"], [])

    def test_inert_primary_interruption_survives_cleanup_error_and_failure_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            real_stop = gate.stop_child
            def failed_cleanup(waiter):
                real_stop(waiter)
                raise RuntimeError("fixture cleanup failure after actual reaping")
            with mock.patch.object(gate, "stop_child", side_effect=failed_cleanup):
                with self.assertRaisesRegex(RuntimeError, "termination signal"):
                    self.run_inert_protocol(root, "interrupted-live")
            receipt = gate.read_json(root / "output/native-failure.json")
            self.assertIn("termination signal", receipt["error"])
            self.assertEqual(receipt["cleanup_errors"], ["RuntimeError: fixture cleanup failure after actual reaping"])
            self.assertTrue(receipt["wait"]["child_reaped"])

    def test_inert_stop_prevents_first_snapshot_and_reaps_child(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            with self.assertRaises(RuntimeError): self.run_inert_protocol(root, "stop")
            receipt = gate.read_json(root / "output/native-failure.json")
            self.assertEqual(receipt["completed_phases"], 0); self.assertTrue(receipt["cleanup"]["terminated"])

    def test_summary_retains_three_rounds_and_reports_a_prespecified_miss(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            for member in gate.members():
                folder = root / member["id"]; folder.mkdir()
                gate.write(folder / "native-result.json", {"success": True, "wait": {"exit_code": 0, "child_reaped": True}})
                for phase in ("cold_first", "cold_next", "warm_same", "quiescent_deleted", "maps_dropped"):
                    for batch in range(1, 5):
                        e = event(member, phase); e["batch"] = batch
                        if member["arm"] == "candidate" and e["timing"]["operations"]:
                            e["timing"]["p99_ns"] = 30
                        gate.write(folder / f"batch-{batch:02d}-{phase}.json", {"native_event": e, "medians": {"pss_bytes": 1024}})
            summary = gate.summarize(root, contract())
            self.assertTrue(summary["functional_success"]); self.assertFalse(summary["screen_success"])
            first = summary["comparisons"][0]
            self.assertEqual(first["ratios_of_medians"]["p99_ns"], 1.5)
            self.assertEqual(len(first["individual_process_observations"]["baseline"]), 3)
            self.assertTrue(summary["screen_misses"])

    def test_summary_refuses_missing_actual_cohort_receipts(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(FileNotFoundError): gate.summarize(pathlib.Path(tmp), contract())


if __name__ == "__main__":
    unittest.main()
