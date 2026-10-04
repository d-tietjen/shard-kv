#!/usr/bin/env python3
"""Offline contract regressions. Execute only through the reviewed Adam helper."""
import copy
import contextlib
import errno
import hashlib
import importlib.util
import json
import os
import pathlib
import signal
import sys
import tempfile
import types
import unittest
from unittest import mock
SPEC = importlib.util.spec_from_file_location('resp_scenarios', pathlib.Path(__file__).with_name('run-compact-storage-scenarios.py'))
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)

def contract():
    source = {'sha': 'a' * 40, 'tree': 'b' * 40, 'worktree': '/owned/source', 'file_sha256': {}}
    image = {'engine_image_id': 'sha256:' + 'c' * 64, 'source_sha': 'd' * 40, 'source_tree': 'e' * 40, 'features': gate.FEATURES, 'original_tag': 'owned-original', 'provenance': {}, 'resolved_build': {}}
    baseline = dict(image, source_sha=gate.BASELINE, source_tree=gate.BASELINE_TREE)
    return {'schema': 1, 'status': 'frozen with independently accepted gates', 'classification': 'diagnostic-unreserved-closed-loop', 'package': 'stateful-a', 'members': gate.members('stateful-a'), 'budgets': dict(gate.BUDGETS), 'acceptance': dict(gate.ACCEPTANCE), 'images': {'baseline': baseline, 'candidate': image, 'redis': {'engine_image_id': 'sha256:' + 'f' * 64, 'version': '7.4.11', 'repo_digests': [], 'reference_acceptance': {}}}, 'source': source, 'native_build': {'argv': gate.BUILD_ARGV, 'features': [], 'source_sha': source['sha'], 'source_tree': source['tree']}, 'binaries': {name: {'path': '/owned/' + name, 'sha256': '1' * 64} for name in ('compact_resp_scenarios', 'saturation')}, 'helpers': dict.fromkeys(('density', 'child_wait', 'docker', 'git'), {}), 'owner': 'eden2266-offline', 'owners': {arm: 'eden2266-offline-' + arm for arm in ('baseline', 'candidate', 'redis')}, 'builds': {'common': {'worktree': source['worktree']}}, 'sampling': {'idle': 5, 'final': 5, 'intermediate': 5, 'delay_seconds': 0.2, 'settle_seconds': 1.0, 'peak_interval_seconds': 0.2}, 'caps': {'server_memory': 4294967296, 'server_cpus': 1, 'controller_memory': 2147483648, 'controller_cpus': 4, 'swap': 0}}

def small(id='value17', access=False):
    s = gate.scenario(id, access)
    s['keys'] = 32
    return s

def event(s, p):
    witness = gate.expected_phase(s, p)
    e = {'schema': 1, 'event': 'phase', 'pid': 12, 'scenario': s['id'], 'phase': p['id']}
    e.update({k: v for k, v in witness.items() if not k.endswith(('_operations', '_transactions'))})
    for name in ('mutation', 'verification'):
        n = witness[name + '_operations']
        e[name] = {'wire_commands': n, 'completed_records': witness[name + '_transactions'], 'elapsed_ns': 100000 if n else 0, 'wire_commands_per_sec': n * 10000 if n else 0, 'completed_records_per_sec': witness[name + '_transactions'] * 10000 if n else 0, 'latency_unit': ('logical-record-transition' if name == 'mutation' else 'full-record-verification') if n else 'absent', 'p50_ns': 10 if n else 0, 'p99_ns': 20 if n else 0, 'p999_ns': 30 if n else 0, 'maximum_ns': 40 if n else 0}
    return (e, witness)

class RespContractTests(unittest.TestCase):

    def _cleanup_runner(self):
        h1 = gate.import_file('resp_cleanup_h1', pathlib.Path(gate.__file__).with_name('run-compact-shared-read-gate.py'))
        waiters = []
        def actual_waiter(pid):
            waiter = h1.NativeChildWait(pid)
            waiters.append(waiter)
            return waiter
        r = gate.Runner.__new__(gate.Runner)
        r.h1 = types.SimpleNamespace(NativeChildWait=actual_waiter, block_termination=h1.block_termination, defer_cleanup_termination=h1.defer_cleanup_termination, stop_child=h1.stop_child, proc_identity=h1.proc_identity)
        r.children = {}
        r.cleanup_errors = []
        r.row_deadline = None
        return r, waiters

    @contextlib.contextmanager
    def _close_signal_probe(self, target_index, delivery):
        parent = os.getpid()
        actual_pipe, actual_close = os.pipe, os.close
        probe = {'pipes': [], 'closed': [], 'triggered': False}
        def pipe():
            pair = actual_pipe()
            probe['pipes'].extend(pair)
            return pair
        def close(fd):
            if os.getpid() == parent:
                probe['closed'].append(fd)
            actual_close(fd)
            if os.getpid() == parent and fd == probe['pipes'][target_index] and not probe['triggered']:
                probe['triggered'] = True
                if delivery == 'pending-on-unmask':
                    os.kill(parent, signal.SIGTERM)
                else:
                    signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
        previous = signal.signal(signal.SIGTERM, gate.interrupted)
        try:
            with mock.patch.object(gate.os, 'pipe', side_effect=pipe), mock.patch.object(gate.os, 'close', side_effect=close):
                yield probe
        finally:
            signal.signal(signal.SIGTERM, previous)

    def _assert_closed_once(self, probe):
        self.assertTrue(probe['triggered'])
        for fd in probe['pipes']:
            self.assertEqual(probe['closed'].count(fd), 1)
            with self.assertRaises(OSError) as error:
                os.fstat(fd)
            self.assertEqual(error.exception.errno, errno.EBADF)

    def _assert_genuinely_reaped(self, waiter):
        self.assertIs(waiter.wait['wait_observed'], True)
        self.assertIs(waiter.wait['child_reaped'], True)
        self.assertIs(type(waiter.wait['raw_wait_status']), int)
        self.assertEqual(waiter.wait['waited_pid'], waiter.pid)
        self.assertTrue(os.WIFEXITED(waiter.wait['raw_wait_status']) or os.WIFSIGNALED(waiter.wait['raw_wait_status']))
        self.assertIs(waiter.poll(), waiter.wait)
        with self.assertRaises(ChildProcessError):
            os.waitpid(waiter.pid, os.WNOHANG)

    def test_actual_spawn_close_completed_term_preserves_primary_and_reaps(self):
        for delivery in ('pending-on-unmask', 'handler-after-close'):
            with self.subTest(delivery=delivery), tempfile.TemporaryDirectory() as tmp:
                r, waiters = self._cleanup_runner()
                folder = pathlib.Path(tmp)
                try:
                    with self._close_signal_probe(2, delivery) as probe:
                        with self.assertRaisesRegex(RuntimeError, '^controller interrupted by signal 15$'):
                            r.spawn([sys.executable, '-c', 'import time; time.sleep(60)'], folder)
                    self._assert_closed_once(probe)
                    self.assertEqual(len(waiters), 1)
                    self._assert_genuinely_reaped(waiters[0])
                    self.assertFalse(r.children)
                    self.assertFalse(r.cleanup_errors)
                    receipt = json.loads((folder / 'cleanup.json').read_text())
                    self.assertTrue(receipt['original_exception_in_flight'])
                    self.assertEqual(receipt['wait'], waiters[0].wait)
                finally:
                    with r.h1.defer_cleanup_termination():
                        for waiter in waiters:
                            r.h1.stop_child(waiter)

    def test_actual_run_native_ack_close_completed_term_preserves_primary_and_reaps(self):
        protocol = "import json, os, sys, time\npid = os.getpid()\nprint(json.dumps({'schema': 1, 'event': 'ready', 'pid': pid}), flush=True)\nsys.stdin.readline()\nprint(json.dumps({'phase': 'load', 'live_keys': 0}), flush=True)\nsys.stdin.readline()\nprint(json.dumps({'schema': 1, 'event': 'complete', 'pid': pid, 'scenario': 'value17', 'phases': 1, 'errors': 0}), flush=True)\nsys.stdin.read()\ntime.sleep(60)\n"
        for delivery in ('pending-on-unmask', 'handler-after-close'):
            with self.subTest(delivery=delivery), tempfile.TemporaryDirectory() as tmp:
                r, waiters = self._cleanup_runner()
                r.plan = contract()
                r.budget = mock.Mock()
                r.snapshots = mock.Mock(return_value={})
                r.density = types.SimpleNamespace(redis_command=lambda *args: b'0')
                actual_spawn = r.spawn
                r.spawn = lambda argv, folder, interactive: actual_spawn([argv[0], '-c', protocol], folder, interactive)
                folder = pathlib.Path(tmp) / 'native'
                try:
                    with self._close_signal_probe(3, delivery) as probe, mock.patch.object(gate, 'pinned', return_value=pathlib.Path(sys.executable).resolve()), mock.patch.object(gate, 'validate_ready'), mock.patch.object(gate, 'validate_phase'):
                        with self.assertRaisesRegex(RuntimeError, '^controller interrupted by signal 15$'):
                            r.run_native({'scenario': 'value17'}, folder, 'owned', 123, 'user', pathlib.Path('/owned/cgroup'), 1234, {'load': {}})
                    self._assert_closed_once(probe)
                    self.assertEqual(len(waiters), 1)
                    self._assert_genuinely_reaped(waiters[0])
                    self.assertFalse(r.children)
                    self.assertFalse(r.cleanup_errors)
                    receipt = json.loads((folder / 'cleanup.json').read_text())
                    self.assertEqual(receipt['identity'], waiters[0].identity)
                    self.assertEqual(receipt['wait'], waiters[0].wait)
                finally:
                    with r.h1.defer_cleanup_termination():
                        for waiter in waiters:
                            r.h1.stop_child(waiter)

    def test_actual_spawn_secondary_stop_failure_keeps_primary_receipt_and_wait_owner(self):
        with tempfile.TemporaryDirectory() as tmp:
            r, waiters = self._cleanup_runner()
            folder = pathlib.Path(tmp)
            def secondary_stop(waiter):
                os.kill(os.getpid(), signal.SIGINT)
                raise RuntimeError('secondary stop exact')
            try:
                with self._close_signal_probe(2, 'pending-on-unmask') as probe, mock.patch.object(r.h1, 'stop_child', side_effect=secondary_stop):
                    with self.assertRaisesRegex(RuntimeError, '^controller interrupted by signal 15$'):
                        r.spawn([sys.executable, '-c', 'import time; time.sleep(60)'], folder)
                self._assert_closed_once(probe)
                self.assertEqual(len(waiters), 1)
                self.assertIs(r.children[waiters[0].pid], waiters[0])
                self.assertIsNone(waiters[0].wait)
                receipt = json.loads((folder / 'cleanup-failure.json').read_text())
                self.assertTrue(receipt['original_exception_in_flight'])
                self.assertIn('secondary stop exact', receipt['cleanup_error'])
                self.assertEqual(receipt['cleanup_signals'], [signal.SIGINT])
                self.assertIn('deferred cleanup signals', receipt['cleanup_error'])
                self.assertEqual(receipt['identity'], waiters[0].identity)
                self.assertIsNone(receipt['wait'])
                r.cleanup_child(waiters[0])
                self._assert_genuinely_reaped(waiters[0])
                self.assertFalse(r.children)
            finally:
                with r.h1.defer_cleanup_termination():
                    for waiter in waiters:
                        r.h1.stop_child(waiter)

    def _access_cleanup_runner(self):
        r, waiters = self._cleanup_runner()
        r.plan = contract()
        r.density = types.SimpleNamespace(TARGETS={'redis': {'backend': 'redis'}})
        r.budget = lambda: None
        r.row_deadline = gate.time.monotonic() + 2
        actual_spawn = r.spawn
        # The real child closes stdout while still alive. Access must observe EOF,
        # enter its genuine waiter, then preserve that wait deadline during cleanup.
        protocol = 'import os, time; os.close(1); time.sleep(60)'
        r.spawn = lambda argv, folder, interactive=False: actual_spawn([argv[0], '-c', protocol], folder, interactive)
        return r, waiters

    @contextlib.contextmanager
    def _access_close_error_probe(self):
        parent = os.getpid()
        actual_pipe, actual_close = os.pipe, os.close
        probe = {'pipes': [], 'closed': [], 'triggered': False}
        def pipe():
            pair = actual_pipe()
            probe['pipes'].extend(pair)
            return pair
        def close(fd):
            if os.getpid() == parent:
                probe['closed'].append(fd)
            actual_close(fd)
            if os.getpid() == parent and fd == probe['pipes'][0] and not probe['triggered']:
                probe['triggered'] = True
                raise OSError(errno.EIO, 'access completed close exact')
        with mock.patch.object(gate.os, 'pipe', side_effect=pipe), mock.patch.object(gate.os, 'close', side_effect=close):
            yield probe

    def test_actual_access_eof_close_pending_term_keeps_primary_and_reaps(self):
        with tempfile.TemporaryDirectory() as tmp:
            r, waiters = self._access_cleanup_runner()
            folder = pathlib.Path(tmp)
            try:
                with self._close_signal_probe(0, 'pending-on-unmask') as probe, mock.patch.object(gate, 'pinned', return_value=pathlib.Path(sys.executable).resolve()):
                    with self.assertRaisesRegex(RuntimeError, '^actual child wait deadline$'):
                        r.access({'scenario': 'v16-get'}, folder, 123, 1234, 'redis')
                self._assert_closed_once(probe)
                self.assertEqual(len(waiters), 1)
                self._assert_genuinely_reaped(waiters[0])
                self.assertFalse(r.children)
                receipt = json.loads((folder / 'saturation/cleanup-failure.json').read_text())
                self.assertTrue(receipt['original_exception_in_flight'])
                self.assertEqual(receipt['cleanup_signals'], [signal.SIGTERM])
                self.assertEqual(receipt['identity'], waiters[0].identity)
                self.assertEqual(receipt['wait'], waiters[0].wait)
                self.assertIn('deferred cleanup signals', receipt['cleanup_error'])
            finally:
                with r.h1.defer_cleanup_termination():
                    for waiter in waiters:
                        r.h1.stop_child(waiter)

    def test_actual_access_eof_completed_close_error_keeps_primary_and_reaps(self):
        with tempfile.TemporaryDirectory() as tmp:
            r, waiters = self._access_cleanup_runner()
            folder = pathlib.Path(tmp)
            try:
                with self._access_close_error_probe() as probe, mock.patch.object(gate, 'pinned', return_value=pathlib.Path(sys.executable).resolve()):
                    with self.assertRaisesRegex(RuntimeError, '^actual child wait deadline$'):
                        r.access({'scenario': 'v16-get'}, folder, 123, 1234, 'redis')
                self._assert_closed_once(probe)
                self.assertEqual(len(waiters), 1)
                self._assert_genuinely_reaped(waiters[0])
                self.assertFalse(r.children)
                receipt = json.loads((folder / 'saturation/cleanup-failure.json').read_text())
                self.assertTrue(receipt['original_exception_in_flight'])
                self.assertIn('fd cleanup: OSError', receipt['cleanup_error'])
                self.assertIn('access completed close exact', receipt['cleanup_error'])
                self.assertEqual(receipt['wait'], waiters[0].wait)
            finally:
                with r.h1.defer_cleanup_termination():
                    for waiter in waiters:
                        r.h1.stop_child(waiter)

    def test_actual_access_primary_survives_secondary_stop_error_and_deferred_signals(self):
        with tempfile.TemporaryDirectory() as tmp:
            r, waiters = self._access_cleanup_runner()
            folder = pathlib.Path(tmp)
            actual_cleanup = r.cleanup_child
            def secondary_cleanup(waiter):
                actual_cleanup(waiter)
                os.kill(os.getpid(), signal.SIGINT)
                raise RuntimeError('access secondary after genuine stop exact')
            r.cleanup_child = secondary_cleanup
            try:
                with self._close_signal_probe(0, 'pending-on-unmask') as probe, mock.patch.object(gate, 'pinned', return_value=pathlib.Path(sys.executable).resolve()):
                    with self.assertRaisesRegex(RuntimeError, '^actual child wait deadline$'):
                        r.access({'scenario': 'v16-get'}, folder, 123, 1234, 'redis')
                self._assert_closed_once(probe)
                self.assertEqual(len(waiters), 1)
                self._assert_genuinely_reaped(waiters[0])
                self.assertFalse(r.children)
                receipt = json.loads((folder / 'saturation/cleanup-failure.json').read_text())
                self.assertTrue(receipt['original_exception_in_flight'])
                self.assertIn('access secondary after genuine stop exact', receipt['cleanup_error'])
                self.assertEqual(receipt['cleanup_signals'], [signal.SIGTERM, signal.SIGINT])
                self.assertEqual(receipt['identity'], waiters[0].identity)
                self.assertEqual(receipt['wait'], waiters[0].wait)
            finally:
                with r.h1.defer_cleanup_termination():
                    for waiter in waiters:
                        r.h1.stop_child(waiter)

    def _verify_inputs_with_receipts(self, mutate=None):
        p = contract()
        script = pathlib.Path(gate.__file__).resolve()
        p['source']['worktree'] = str(script.parents[2])
        p['source']['file_sha256'] = dict.fromkeys(('benchmarks/scripts/run-compact-storage-scenarios.py', 'benchmarks/src/bin/compact_resp_scenarios.rs', 'benchmarks/Cargo.toml', 'Cargo.lock', 'benchmarks/scripts/run-memory-density-benchmark.py'), '1' * 64)
        p['controller_scope'] = '/owned/controller.scope'
        p['docker_receipt'] = '/owned/docker-receipt.json'
        p['native_build']['compiler'] = {'path': '/owned/rustc', 'sha256': '2' * 64}
        for name in ('density', 'child_wait'):
            p['helpers'][name] = {'path': str(script.parent / (name + '.py')), 'sha256': '3' * 64}
        base = {'status': 'independently accepted', 'source_sha': p['source']['sha'], 'source_tree': p['source']['tree']}
        receipts = {}
        for kind in ('early_gate_acceptance', 'regression_acceptance', 'native_build_acceptance', 'operating_acceptance'):
            p[kind] = {'path': '/owned/' + kind, 'sha256': '4' * 64}
            receipts[p[kind]['path']] = dict(base)
        receipts[p['early_gate_acceptance']['path']].update(functional_success=True, screen_success=True, processes=30)
        receipts[p['regression_acceptance']['path']].update(native_unit_tests=21, python_tests=40, exit_codes=[0, 0], child_reaped=True)
        receipts[p['native_build_acceptance']['path']].update(argv=list(gate.BUILD_ARGV), exit_code=0, raw_wait_status=0, child_reaped=True, wait_observed=True, artifact_sha256={k: v['sha256'] for k, v in p['binaries'].items()}, artifact_paths={k: v['path'] for k, v in p['binaries'].items()}, compiler_sha256=p['native_build']['compiler']['sha256'], rustflags=[])
        commands = {('git', '-C', p['source']['worktree'], 'rev-parse', 'HEAD'): p['source']['sha'], ('git', '-C', p['source']['worktree'], 'rev-parse', 'HEAD^{tree}'): p['source']['tree'], ('git', '-C', p['source']['worktree'], 'status', '--porcelain'): ''}
        for arm, b in p['images'].items():
            commands[('docker', 'image', 'inspect', b['engine_image_id'])] = json.dumps([{'Id': b['engine_image_id'], 'RepoDigests': []}])
            if arm == 'redis':
                continue
            b['provenance'] = {}
            for kind in ('metadata', 'dockerfile', 'export_log', 'source_context_acceptance'):
                b['provenance'][kind] = {'path': '/owned/' + arm + '-' + kind, 'sha256': '5' * 64}
            receipts[b['provenance']['metadata']['path']] = {}
            receipts[b['provenance']['source_context_acceptance']['path']] = dict(base, source_sha=b['source_sha'], source_tree=b['source_tree'])
        if mutate is not None:
            mutate({kind: receipts[p[kind]['path']] for kind in ('early_gate_acceptance', 'regression_acceptance', 'native_build_acceptance', 'operating_acceptance')})
        r = gate.Runner.__new__(gate.Runner)
        r.plan = p
        r.h1 = types.SimpleNamespace(verify_scope=mock.Mock())
        r.density = types.SimpleNamespace(resolve_shardcache_build=mock.Mock(return_value={}))
        r.command = mock.Mock(side_effect=lambda argv: commands[tuple(argv)])
        inert_file = types.SimpleNamespace(read_bytes=lambda: b'', read_text=lambda: '')
        environment = {'EDEN_DENSITY_OWNER': p['owners']['candidate'], 'EDEN_DENSITY_DOCKER_RECEIPT': p['docker_receipt']}
        with mock.patch.object(gate, 'sha', return_value='1' * 64), mock.patch.object(gate, 'pinned', return_value=inert_file), mock.patch.object(gate, 'read_json', side_effect=lambda binding, *args: json.loads(json.dumps(receipts[binding['path']]))), mock.patch.dict(gate.os.environ, environment, clear=True):
            r.verify_inputs()
        r.h1.verify_scope.assert_called_once_with(gate.os.getpid(), p['controller_scope'], 2147483648, 4)
        self.assertEqual(r.density.resolve_shardcache_build.call_count, 2)
        self.assertEqual(r.command.call_count, 6)

    def test_verify_inputs_genuine_numeric_zero_and_true_flags_admitted(self):
        self._verify_inputs_with_receipts()

    def test_verify_inputs_regression_exit_codes_reject_noninteger_and_missing(self):
        for index in (0, 1):
            for value in (False, True, 0.0, '0', None, 1):
                with self.subTest(index=index, value=value, value_type=type(value).__name__):
                    def mutate(receipts):
                        receipts['regression_acceptance']['exit_codes'][index] = value
                    with self.assertRaisesRegex(RuntimeError, 'focused native/Python regression admission differs'):
                        self._verify_inputs_with_receipts(mutate)
        for value in (False, True, 0.0, '0', None, [], [0], [0, 0, 0]):
            with self.subTest(exit_codes=value):
                with self.assertRaisesRegex(RuntimeError, 'focused native/Python regression admission differs'):
                    self._verify_inputs_with_receipts(lambda receipts: receipts['regression_acceptance'].update(exit_codes=value))
        with self.subTest(missing='exit_codes'):
            with self.assertRaisesRegex(RuntimeError, 'focused native/Python regression admission differs'):
                self._verify_inputs_with_receipts(lambda receipts: receipts['regression_acceptance'].pop('exit_codes'))

    def test_verify_inputs_native_exit_and_raw_status_reject_noninteger_and_missing(self):
        for field in ('exit_code', 'raw_wait_status'):
            message = 'native build/compiler/exit receipt differs' if field == 'exit_code' else 'native compiler/actual artifact/flags/wait closure differs'
            for value in (False, True, 0.0, '0', None, 1):
                with self.subTest(field=field, value=value, value_type=type(value).__name__):
                    with self.assertRaisesRegex(RuntimeError, message):
                        self._verify_inputs_with_receipts(lambda receipts: receipts['native_build_acceptance'].update({field: value}))
            with self.subTest(missing=field):
                with self.assertRaisesRegex(RuntimeError, message):
                    self._verify_inputs_with_receipts(lambda receipts: receipts['native_build_acceptance'].pop(field))

    def test_verify_inputs_reap_and_wait_flags_require_exact_true(self):
        for kind, field, message in (('regression_acceptance', 'child_reaped', 'focused native/Python regression admission differs'), ('native_build_acceptance', 'child_reaped', 'native build/compiler/exit receipt differs'), ('native_build_acceptance', 'wait_observed', 'native compiler/actual artifact/flags/wait closure differs')):
            for value in (False, 0, 1, 1.0, 'true', None):
                with self.subTest(kind=kind, field=field, value=value, value_type=type(value).__name__):
                    with self.assertRaisesRegex(RuntimeError, message):
                        self._verify_inputs_with_receipts(lambda receipts: receipts[kind].update({field: value}))
            with self.subTest(kind=kind, missing=field):
                with self.assertRaisesRegex(RuntimeError, message):
                    self._verify_inputs_with_receipts(lambda receipts: receipts[kind].pop(field))

    def test_exact_four72row_packages_and378_total(self):
        self.assertEqual(sum((len(gate.members(p)) for p in ('stateful-a', 'stateful-b', 'stateful-c', 'access'))), 288)
        self.assertEqual(288 + 90, 378)
        self.assertEqual((288 + 90) * 10, 3780)

    def test_rotation_order_has_all_unique_round_arm_scenarios(self):
        for p in ('stateful-a', 'stateful-b', 'stateful-c', 'access'):
            rows = gate.members(p)
            self.assertEqual(len({r['id'] for r in rows}), 72)
            self.assertEqual([rows[i]['arm'] for i in (0, 24, 48)], ['baseline', 'candidate', 'redis'])

    def test_unknown_package_and_scenario_rejected(self):
        for value in ('stateful-d', 'core', '../bad'):
            with self.assertRaises(RuntimeError):
                gate.members(value)
        with self.assertRaises(RuntimeError):
            gate.scenario('key1')

    def test_unfinished_or_release_classification_rejected(self):
        for field, value in (('status', 'pending'), ('classification', 'release-qualified')):
            p = contract()
            p[field] = value
            with self.assertRaises(RuntimeError):
                gate.validate_contract(p)

    def test_missing_extra_duplicate_and_reordered_rows_rejected(self):
        for mutate in (lambda r: r.pop(), lambda r: r.append(r[0]), lambda r: r.reverse()):
            p = contract()
            mutate(p['members'])
            with self.assertRaises(RuntimeError):
                gate.validate_contract(p)

    def test_budgets_and_acceptance_cannot_be_relaxed(self):
        for field in ('budgets', 'acceptance'):
            p = contract()
            key = next(iter(p[field]))
            p[field][key] += 1
            with self.assertRaises(RuntimeError):
                gate.validate_contract(p)

    def test_original78_baseline_and_actual_redis_version_required(self):
        for arm, key in (('baseline', 'source_sha'), ('redis', 'version')):
            p = contract()
            p['images'][arm][key] = '0' * 40
            with self.assertRaises(RuntimeError):
                gate.validate_contract(p)

    def test_default_common_native_build_cannot_enable_product_features(self):
        p = contract()
        p['native_build']['features'] = ['compact-point-storage']
        with self.assertRaises(RuntimeError):
            gate.validate_contract(p)

    def test_arm_specific_binary_and_missing_common_binary_rejected(self):
        p = contract()
        p['images']['baseline']['binary'] = 'old'
        with self.assertRaises(RuntimeError):
            gate.validate_contract(p)
        p = contract()
        del p['binaries']['saturation']
        with self.assertRaises(RuntimeError):
            gate.validate_contract(p)

    def test_source_tree_mismatch_and_cap_change_rejected(self):
        for mutate in (lambda p: p['source'].update(tree='0' * 40), lambda p: p['caps'].update(swap=1)):
            p = contract()
            mutate(p)
            with self.assertRaises(RuntimeError):
                gate.validate_contract(p)

    def test_boundary_keys_are_unique_binary_exact_length(self):
        for length in (8, 18, 63, 64, 65):
            keys = {gate.fixed_key(i, length) for i in range(1000)}
            self.assertEqual(len(keys), 1000)
            self.assertEqual({len(k) for k in keys}, {length})

    def test_all160classes_have_exact_unique_feasible_short_keys(self):
        s = gate.scenario('classes-entropy')
        p = gate.phases(s)[0]
        classes = set()
        keys = set()
        for i in range(s['keys']):
            key, n, g, kind, expiry = gate.record(s, p, i)
            self.assertLessEqual(len(key), 64)
            self.assertLessEqual(n, 256)
            self.assertNotIn(key, keys)
            keys.add(key)
            classes.add((len(key) + n + 1) // 2)
        self.assertEqual(classes, set(range(1, 161)))
        self.assertIn(b'', keys)
        self.assertIn(b'\x02', keys)

    def test_mixed80_20_weights_and_logical_distribution_exact(self):
        s = gate.scenario('mixed-entropy')
        d = gate.distribution(s)
        self.assertEqual(sum((r['records'] for r in d)), 100000)
        self.assertEqual(sum((r['records'] for r in d if r['value_bytes'] <= 256)), 80000)
        self.assertTrue(all((r['logical_bytes'] == r['records'] * (r['key_bytes'] + r['value_bytes']) for r in d)))

    def test_entropy_seed_matches_original_and_generations_differ(self):
        self.assertEqual(gate.payload(16, 'high-entropy', 0).hex(), '2b3ffefc73d7df7469f3854acf6a59cf')
        self.assertNotEqual(gate.payload(64, 'compressible', 0), gate.payload(64, 'compressible', 1))

    def test_framed_digest_prevents_concatenation_alias(self):
        a = hashlib.sha256()
        b = hashlib.sha256()
        gate.frame(a, b'a')
        gate.frame(a, b'bc')
        gate.frame(b, b'ab')
        gate.frame(b, b'c')
        self.assertNotEqual(a.digest(), b.digest())

    def test_resize_and_both_pattern_churn_phase_counts(self):
        for pattern in ('entropy', 'compressible'):
            s = small('resize-' + pattern)
            self.assertEqual(len(gate.phases(s)), 13)
            self.assertEqual([gate.record(s, p, 0)[1] for p in gate.phases(s)[:5]], [16, 64, 255, 257, 16])
            for family, count in (('overwrite', 11), ('half', 7), ('groups', 7)):
                self.assertEqual(len(gate.phases(small(family + '-' + pattern))), count)

    def test_delete_absences_reinsert_state_and_sentinels_are_explicit(self):
        s = small('half-entropy')
        p = gate.phases(s)
        self.assertEqual(gate.expected_phase(s, p[1])['absent_keys'], 16)
        self.assertEqual(gate.expected_phase(s, p[2])['absent_keys'], 0)
        s = small('groups-entropy')
        p = gate.phases(s)
        self.assertEqual(gate.record(s, p[1], 3)[3], 1)
        self.assertEqual(gate.record(s, p[1], 0)[3], 0)

    def test_metadata_expire_persist_and_reinsert_are_distinct(self):
        s = small('metadata')
        p = gate.phases(s)
        self.assertTrue(gate.record(s, p[1], 0)[4])
        self.assertFalse(gate.record(s, p[2], 0)[4])
        self.assertEqual(gate.record(s, p[3], 0)[3], 0)
        self.assertEqual(gate.record(s, p[4], 0)[2], 1)

    def test_typed_trace_counts_wrongtype_type_contents_ttl_commands(self):
        s = small('types')
        p = gate.phases(s)[1]
        r = gate.record(s, p, 0)
        self.assertEqual([c[0] for c in gate.verification_commands(r)], [b'GET', b'TYPE', b'HGETALL', b'PTTL'])
        self.assertEqual(gate.record(s, p, 10)[3], 3)

    def test_correct_small_phase_and_zero_access_mutation_admitted(self):
        for s in (small(), small('v16-get', True)):
            p = gate.phases(s)[0]
            e, w = event(s, p)
            gate.validate_phase(e, s, p, 12, w)

    def test_unknown_or_out_of_order_phase_and_wrong_pid_rejected(self):
        s = small()
        p = gate.phases(s)[0]
        e, w = event(s, p)
        for field, value in (('phase', 'other'), ('pid', 13), ('scenario', 'value15')):
            bad = copy.deepcopy(e)
            bad[field] = value
            with self.assertRaises(RuntimeError):
                gate.validate_phase(bad, s, p, 12, w)

    def test_wrong_cardinality_logical_bytes_payload_or_trace_digest_rejected(self):
        s = small()
        p = gate.phases(s)[0]
        e, w = event(s, p)
        for k in ('live_keys', 'logical_bytes', 'state_sha256', 'mutation_trace_sha256', 'verification_trace_sha256'):
            bad = copy.deepcopy(e)
            bad[k] = bad[k] + 1 if type(bad[k]) is int else '0' * 64
            with self.assertRaises(RuntimeError):
                gate.validate_phase(bad, s, p, 12, w)

    def test_wire_count_transaction_count_and_throughput_arithmetic_rejected(self):
        s = small()
        p = gate.phases(s)[0]
        e, w = event(s, p)
        for field in ('wire_commands', 'completed_records', 'wire_commands_per_sec'):
            bad = copy.deepcopy(e)
            bad['mutation'][field] += 1
            with self.assertRaises(RuntimeError):
                gate.validate_phase(bad, s, p, 12, w)

    def test_nonfinite_and_inverted_percentiles_rejected(self):
        s = small()
        p = gate.phases(s)[0]
        e, w = event(s, p)
        for field, value in (('wire_commands_per_sec', float('nan')), ('p99_ns', 5)):
            bad = copy.deepcopy(e)
            bad['mutation'][field] = value
            with self.assertRaises(RuntimeError):
                gate.validate_phase(bad, s, p, 12, w)

    def test_ready_wrong_distribution_or_shortened_deadline_rejected(self):
        s = small()
        e = {'schema': 1, 'event': 'ready', 'pid': 12, 'scenario': s, 'seed': gate.SEED, 'clients': 16, 'pipeline': 1, 'deadline_seconds': 900, 'phases': gate.phases(s), 'distribution': gate.distribution(s), 'initial_dbsize_checks': 1}
        gate.validate_ready(e, s, 12)
        for field, value in (('distribution', []), ('deadline_seconds', 901)):
            bad = dict(e)
            bad[field] = value
            with self.assertRaises(RuntimeError):
                gate.validate_ready(bad, s, 12)

    def test_missing_sample_zero_pss_and_wrong_gate_count_rejected(self):
        row = {k: 1 for k in ('rss_bytes', 'pss_bytes', 'private_bytes', 'cgroup_current_bytes', 'cgroup_anon_bytes', 'cgroup_file_bytes')}
        gate.median_samples([row] * 5)
        with self.assertRaises(RuntimeError):
            gate.median_samples([row] * 4)
        with self.assertRaises(RuntimeError):
            gate.median_samples([dict(row, pss_bytes=0)] * 5)

    def test_summary_cannot_accept_partial_or_duplicate_final_rows(self):
        with self.assertRaises(RuntimeError):
            gate.summarize([])
        with self.assertRaises(RuntimeError):
            gate.summarize([{'member': {'id': 'duplicate'}}] * 72)

    def test_owned_terminal_wait_preserves_true_nonzero_and_rejects_signal(self):
        r = gate.Runner.__new__(gate.Runner)
        waiter = types.SimpleNamespace(poll=lambda: {'raw_wait_status': 7 << 8, 'waited_pid': 12, 'child_reaped': True, 'wait_observed': True})
        self.assertEqual(r.wait(waiter, gate.time.monotonic() + 1)['exit_code'], 7)
        waiter.poll = lambda: {'raw_wait_status': 9}
        with self.assertRaises(RuntimeError):
            r.wait(waiter, gate.time.monotonic() + 1)

    def test_lost_wait_ownership_cannot_be_treated_as_successful_cleanup(self):
        r = gate.Runner.__new__(gate.Runner)
        child = types.SimpleNamespace(pid=12)
        r.children = {12: child}
        r.h1 = types.SimpleNamespace(stop_child=lambda c: {'ownership_lost': True, 'child_reaped': False})
        with self.assertRaises(RuntimeError):
            r.cleanup_child(child)
        self.assertIn(12, r.children)

    def test_primary_failure_survives_cleanup_failure_with_separate_receipt(self):
        r = gate.Runner.__new__(gate.Runner)
        child = types.SimpleNamespace(pid=12, identity={'pid': 12}, wait=None)
        r.children = {12: child}
        r.cleanup_errors = []
        r.h1 = types.SimpleNamespace(defer_cleanup_termination=lambda: contextlib.nullcontext([]))
        r.cleanup_child = mock.Mock(side_effect=RuntimeError('cleanup distinct'))
        with mock.patch.object(gate, 'write') as write:
            try:
                try:
                    raise ValueError('primary exact')
                finally:
                    r.cleanup_preserving(child, pathlib.Path('/owned'))
            except ValueError as exc:
                self.assertEqual(str(exc), 'primary exact')
            self.assertEqual(len(r.cleanup_errors), 1)
            self.assertTrue(write.call_args.args[1]['original_exception_in_flight'])
class HwmBaselineTests(unittest.TestCase):
    """New branch regressions; definitions only until exact Adam qualification."""

    def _small(self):
        s = gate.scenario(gate.HWM_ID)
        s['keys'] = 64
        return s

    def _event(self,s,p):
        witness = gate.hwm_expected_phase(s,p)
        e = {'schema':2,'event':'phase','pid':12,'scenario':s['id'],'phase':p['id'],
             'idle_elapsed_ns':10000000000 if p['step']%3==2 else 0}
        e.update({k:v for k,v in witness.items() if k!='workers' and not k.endswith(('_operations','_transactions'))})
        for name in ('mutation','verification'):
            n = witness[name+'_operations']; records = witness[name+'_transactions']
            e[name+'_workers'] = copy.deepcopy(witness['workers'][name])
            for worker in e[name+'_workers']:
                worker['rust_thread_id'] = 'isolated-unit-worker-'+str(worker['worker'])
            e[name] = {'wire_commands':n,'completed_records':records,'elapsed_ns':100000 if n else 0,
                'wire_commands_per_sec':n*10000,'completed_records_per_sec':records*10000,
                'latency_unit':('logical-record-transition' if name=='mutation' else 'full-record-verification') if n else 'absent',
                'p50_ns':10 if n else 0,'p99_ns':20 if n else 0,'p999_ns':30 if n else 0,'maximum_ns':40 if n else 0}
        return e,witness

    def test_hwm_catalog_members_and_legacy_catalog(self):
        self.assertEqual(len(gate.IDS),24)
        self.assertNotIn(gate.HWM_ID,gate.IDS)
        for name in ('stateful-a','stateful-b','stateful-c','access'):
            self.assertEqual(len(gate.members(name)),72)
        ms = gate.members(gate.HWM_PACKAGE)
        self.assertEqual([m['arm'] for m in ms],['current','redis','redis','current','current','redis'])
        self.assertEqual(len({m['id'] for m in ms}),6)
        phases = gate.phases(gate.scenario(gate.HWM_ID))
        self.assertEqual([p['step'] for p in phases],list(range(10)))
        self.assertEqual(phases[-1]['id'],'refill-3')
        self.assertEqual(sum(gate.hwm_survivor(i) for i in range(gate.HWM_KEYS)),50000)
        self.assertEqual(gate.HWM_BUDGETS['runtime_seconds'],840)
        self.assertEqual(gate.BUDGETS['runtime_seconds'],900)
        self.assertIsNone(gate.hwm_plan_template()['actual_measurements'])

    def test_hwm_payload_mixed_generations(self):
        prefixes = set()
        for i in range(gate.HWM_KEYS):
            g = 0 if gate.hwm_survivor(i) else i%3+1
            value = gate.hwm_payload(i,g)
            self.assertEqual(len(value),64)
            self.assertEqual(value[:8],gate.hwm_payload(i,0)[:8])
            prefixes.add(value[:8])
        self.assertEqual(len(prefixes),gate.HWM_KEYS)
        self.assertNotEqual(gate.hwm_payload(0,0),gate.hwm_payload(1,1))
        self.assertNotEqual(gate.hwm_payload(17,0)[8:],gate.hwm_payload(17,1)[8:])
        # A genuine native --describe event must carry these independently computed
        # Python hashes; the production ready validator compares all five.
        self.assertEqual([(w['index'],w['generation']) for w in gate.hwm_payload_witnesses()],
                         [(0,0),(0,1),(17,0),(500001,2),(999999,3)])

    def test_hwm_oracle_counts_and_workers(self):
        s = self._small()
        previous = None
        for p in gate.phases(s):
            e,w = self._event(s,p)
            gate.validate_hwm_phase(e,s,p,12,w)
            self.assertEqual(w['verification_operations'],3*s['keys'])
            self.assertEqual(sum(r['completed_records'] for r in w['workers']['verification']),s['keys'])
            self.assertEqual([r['worker'] for r in w['workers']['verification']],list(range(16)))
            if p['step']%3==2:
                self.assertEqual(w['mutation_operations'],0)
                self.assertEqual(w['state_sha256'],previous['state_sha256'])
            if p['step']%3==0:
                self.assertEqual(w['live_keys'],s['keys'])
            else:
                self.assertEqual(w['live_keys'],sum(gate.hwm_survivor(i) for i in range(s['keys'])))
            previous = w

    def test_hwm_corruption_and_worker_rejection(self):
        s = self._small(); p = gate.phases(s)[3]
        e,w = self._event(s,p)
        for key in ('live_keys','logical_bytes','state_sha256','mutation_trace_sha256','verification_trace_sha256'):
            bad = copy.deepcopy(e)
            bad[key] = '0'*64 if isinstance(bad[key],str) else bad[key]+1
            with self.assertRaises(RuntimeError):
                gate.validate_hwm_phase(bad,s,p,12,w)
        for replacement in (e['verification_workers'][:-1],e['verification_workers']+[e['verification_workers'][0]]):
            bad = copy.deepcopy(e); bad['verification_workers'] = replacement
            with self.assertRaises(RuntimeError):
                gate.validate_hwm_phase(bad,s,p,12,w)
        for field,value in (('errors',1),('range_end',0),('state_sha256','0'*64),('completed_records',0)):
            bad = copy.deepcopy(e); bad['verification_workers'][15][field] = value
            with self.assertRaises(RuntimeError):
                gate.validate_hwm_phase(bad,s,p,12,w)
        with self.assertRaises(RuntimeError):
            gate.validate_hwm_phase(e,s,p,13,w)

    def test_hwm_refuses_missing_delegation(self):
        r = gate.HwmBaselineRunner.__new__(gate.HwmBaselineRunner)
        r.plan = gate.hwm_plan_template()
        with mock.patch.object(gate.os,'fork') as fork:
            with self.assertRaisesRegex(RuntimeError,'resource admission error'):
                r.open_client_procs()
            fork.assert_not_called()

    def test_hwm_boundary_identity_and_caps(self):
        with tempfile.TemporaryDirectory() as directory:
            p = pathlib.Path(directory)
            info = p.stat()
            binding = dict(zip(('device','inode','uid','gid','mode'),
                (info.st_dev,info.st_ino,info.st_uid,info.st_gid,info.st_mode & 0o777)))
            gate.hwm_stat_matches(info,binding)
            bad = dict(binding,uid=info.st_uid+1)
            with self.assertRaises(RuntimeError):
                gate.hwm_stat_matches(info,bad)
            with self.assertRaisesRegex(RuntimeError,'canonical cgroup'):
                gate.hwm_boundary(dict(binding,path=str(p)))
            with self.assertRaises(RuntimeError):
                gate.hwm_stat_matches(info,binding,False)
        caps = {'memory.max':'2147483648','memory.swap.max':'0','cpu.max':'400000 100000','pids.max':'512'}
        self.assertEqual(gate.hwm_cap_values(caps.__getitem__,2147483648,4,512),caps)
        for key,value in (('memory.max','4294967296'),('memory.swap.max','max'),('cpu.max','100000 100000'),('pids.max','max')):
            bad = dict(caps,**{key:value})
            with self.assertRaises(RuntimeError):
                gate.hwm_cap_values(bad.__getitem__,2147483648,4,512)

    def _placement_runner(self,executable):
        r,waiters = RespContractTests()._cleanup_runner()
        r.__class__ = gate.HwmBaselineRunner
        r.plan = {'binaries':{'compact_resp_scenarios':{'path':executable}},'resources':{'client':{'path':'isolated-unit-fixture'}}}
        r.budget = lambda:None
        return r,waiters

    def test_hwm_placement_handshake_and_reaping(self):
        executable = str(pathlib.Path(sys.executable).resolve())
        r,waiters = self._placement_runner(executable)
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory); output = root/'output'; output.mkdir()
            target = root/'procs-unit-fixture'
            confirmed = []
            def confirm(waiter):
                self.assertEqual(target.read_text(),str(waiter.pid)+'\n')
                self.assertIsNone(waiter.poll())
                self.assertEqual(r.h1.proc_identity(waiter.pid,pathlib.Path(executable))['start_ticks'],waiter.identity['start_ticks'])
                self.assertEqual(os.getpgid(waiter.pid),waiter.pid)
                confirmed.append(waiter.pid)
            # Unit placement mechanics use a private regular FD. This is never an
            # admission for Linux cgroups; resource validation has separate tests
            # and an actual delegated-kernel integration remains mandatory.
            with mock.patch.object(r,'open_client_procs',side_effect=lambda:os.open(target,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)),\
                 mock.patch.object(r,'placement_confirmed',side_effect=confirm):
                waiter,readfd,ack = r.spawn([executable,'-c','import os,sys; print(os.getpid(),flush=True); sys.stdin.readline()'],output,True)
            self.assertEqual(confirmed,[waiter.pid])
            self.assertTrue(gate.select.select([readfd],[],[],5)[0])
            self.assertEqual(os.read(readfd,128).strip(),str(waiter.pid).encode())
            os.write(ack,b'done\n')
            wait = r.wait(waiter,gate.time.monotonic()+5)
            self.assertEqual(wait['raw_wait_status'],0)
            r.children.pop(waiter.pid)
            os.close(readfd); os.close(ack)
            RespContractTests()._assert_genuinely_reaped(waiters[0])

    def test_hwm_placement_failure_reaped(self):
        executable = str(pathlib.Path(sys.executable).resolve())
        for stage in ('child-write','parent-check','exec'):
            r,waiters = self._placement_runner(executable if stage!='exec' else '/nonexistent/hwm-native')
            with tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                fd_factory = (lambda:os.open(root/'private-procs',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)) if stage!='child-write' else lambda:os.open('/dev/null',os.O_RDONLY)
                def confirmed(waiter):
                    if stage=='parent-check':
                        raise RuntimeError('injected isolated parent-check failure')
                with mock.patch.object(r,'open_client_procs',side_effect=fd_factory),\
                     mock.patch.object(r,'placement_confirmed',side_effect=confirmed):
                    if stage!='exec':
                        with self.assertRaises(RuntimeError):
                            r.spawn([executable,'-c','raise SystemExit(99)'],root,True)
                    else:
                        waiter,readfd,ack = r.spawn(['/nonexistent/hwm-native'],root,True)
                        wait = r.wait(waiter,gate.time.monotonic()+5)
                        self.assertEqual(wait['exit_code'],127)
                        r.children.pop(waiter.pid)
                        os.close(readfd); os.close(ack)
                self.assertFalse(r.children)
                self.assertEqual(len(waiters),1)
                RespContractTests()._assert_genuinely_reaped(waiters[0])

    def test_hwm_guard_freshness(self):
        now = gate.datetime.datetime(2026,10,4,tzinfo=gate.datetime.timezone.utc)
        def text(age):
            when = now-gate.datetime.timedelta(seconds=age)
            return 'checked free used status\n'+when.isoformat()+' 100000000000 0 OK\n'
        self.assertEqual(gate.hwm_guard_status(text(90),False,now)['age_seconds'],90)
        for age,stop in ((91,False),(-1,False),(0,True)):
            with self.assertRaises(RuntimeError):
                gate.hwm_guard_status(text(age),stop,now)

    def test_hwm_docker_parent_mapping(self):
        self.assertEqual(gate.hwm_docker_parent_path('eden2266-hwm1.slice','systemd'),
            pathlib.Path('/sys/fs/cgroup/eden2266.slice/eden2266-hwm1.slice'))
        self.assertEqual(gate.hwm_docker_parent_path('eden2266-hwm1-work.slice','systemd'),
            pathlib.Path('/sys/fs/cgroup/eden2266.slice/eden2266-hwm1.slice/eden2266-hwm1-work.slice'))
        for name in ('','-.slice','-eden.slice','eden-.slice','eden--work.slice',
                     'eden/other.slice','eden\\other.slice','eden_work.slice',
                     'Eden.slice','eden.service','éden.slice','a'*250+'.slice'):
            with self.subTest(name=name), self.assertRaisesRegex(RuntimeError,'unsupported Docker'):
                gate.hwm_docker_parent_path(name,'systemd')
        with self.assertRaisesRegex(RuntimeError,'systemd parent binding'):
            gate.hwm_docker_parent_path('eden2266-hwm1.slice','cgroupfs')

    def test_hwm_docker_parent_refuses_before_effect(self):
        r = gate.HwmBaselineRunner.__new__(gate.HwmBaselineRunner)
        r.plan = {'resources':{'docker_parent':'eden2266-hwm1.slice'}}
        mapped = gate.hwm_docker_parent_path(r.plan['resources']['docker_parent'],'systemd')
        nested = pathlib.Path('/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/eden2266-hwm1.slice')
        for verb in ('run','create'):
            argv = ['docker',verb,'--name','isolated-unit-fixture','image-id']
            for parent,driver in ((nested,'systemd'),(mapped,'cgroupfs'),(mapped,'systemd\ncgroupfs')):
                with self.subTest(verb=verb,parent=parent,driver=driver), \
                     mock.patch.object(r,'verify_resources',return_value=(parent,None,None)) as verify, \
                     mock.patch.object(gate.Runner,'command',autospec=True,return_value=driver) as command:
                    with self.assertRaises(RuntimeError):
                        r.command(argv,17)
                    verify.assert_called_once_with(True)
                    # The sole helper was read-only info; no Docker create/run was dispatched.
                    self.assertEqual(command.call_args_list,
                        [mock.call(r,['docker','info','--format','{{.CgroupDriver}}'],17)])
            with mock.patch.object(r,'verify_resources',return_value=(mapped,None,None)), \
                 mock.patch.object(gate.Runner,'command',autospec=True,side_effect=['systemd','owned-id']) as command:
                self.assertEqual(r.command(argv,17),'owned-id')
                self.assertEqual(command.call_args_list,
                    [mock.call(r,['docker','info','--format','{{.CgroupDriver}}'],17),
                     mock.call(r,argv[:2]+['--pids-limit','512','--cgroup-parent','eden2266-hwm1.slice']+argv[2:],17)])
            with mock.patch.object(r,'verify_resources',return_value=(mapped,None,None)), \
                 mock.patch.object(gate.Runner,'command',autospec=True) as command:
                with self.assertRaisesRegex(RuntimeError,'preexisting Docker placement'):
                    r.command(argv+['--cgroup-parent=other.slice'])
                command.assert_not_called()

    def test_hwm_terminal_controller_deadline(self):
        r = gate.HwmBaselineRunner.__new__(gate.HwmBaselineRunner)
        r.started = 100.0
        r.row_deadline = None  # Last-row cleanup must not reset the controller clock.
        for before,after,accepted in ((5499.999,5499.999,True),(5500.0,5500.0,False),
                                     (5500.001,5500.001,False),(5499.999,5500.0,False)):
            clock = [before]
            r.budget = mock.Mock(side_effect=lambda:clock.__setitem__(0,after))
            with self.subTest(before=before,after=after), \
                 mock.patch.object(gate.time,'monotonic',side_effect=lambda:clock[0]):
                if accepted:
                    self.assertAlmostEqual(r.terminal_elapsed(),5399.999)
                else:
                    with self.assertRaisesRegex(RuntimeError,'controller deadline before terminal success'):
                        r.terminal_elapsed()
            r.budget.assert_called_once_with()
            self.assertEqual(r.started,100.0)

    def test_hwm_terminal_deadline_preserves_cleanup(self):
        # Unit orchestration only: fake clock and labeled cleanup receipts do not
        # qualify an actual Docker mapping, Linux delegation or genuine wait.
        for overrun in (None,'last-row-cleanup','summary'):
            with self.subTest(overrun=overrun), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)/'output'
                plan = {'package':gate.HWM_PACKAGE,'output_root':str(root),'members':gate.members(gate.HWM_PACKAGE),
                        'binaries':{},'helpers':{},'source':{'file_sha256':{},'worktree':directory}}
                clock = [5499.999]
                observations = {}
                r = gate.HwmBaselineRunner.__new__(gate.HwmBaselineRunner)
                r.started = 100.0
                r.root = root
                r.row_deadline = None
                r.cleanup_errors = []
                r.verify_inputs = mock.Mock()
                r.budget = mock.Mock()
                fixture_wait = {'fake_clock_fixture':True,'child_reaped':True,'wait_observed':True,'raw_wait_status':0}
                r.cleanup_child = mock.Mock(return_value=[fixture_wait])
                r.h1 = types.SimpleNamespace(defer_cleanup_termination=lambda:contextlib.nullcontext([]))
                count = [0]
                def row(_member,_witness):
                    count[0] += 1
                    if count[0]==6:
                        gate.write(root/'last-row-owned-cleanup.json',fixture_wait)
                        if overrun=='last-row-cleanup':
                            clock[0] = 5500.0
                    return {'phases':[]}
                r.row = mock.Mock(side_effect=row)
                def record(path,value):
                    observations[path.name] = value
                    if path.name=='summary.json' and overrun=='summary':
                        clock[0] = 5500.0
                def publish(candidate,terminal,**kw):
                    self.assertEqual(candidate,root/'result-candidate.json')
                    self.assertEqual(terminal,root/'result.json')
                    self.assertEqual(kw,{'follow_symlinks':False})
                    self.assertNotIn('result.json',observations)
                    observations['result.json'] = observations['result-candidate.json']
                with mock.patch.object(sys,'argv',['controller','--plan',str(root.parent/'plan.json'),'--plan-sha256','1'*64]), \
                     mock.patch.object(gate,'read_json',return_value=plan), \
                     mock.patch.object(gate,'validate_contract',side_effect=lambda p:p), \
                     mock.patch.object(gate,'HwmBaselineRunner',return_value=r), \
                     mock.patch.object(gate,'hwm_expected_phase',return_value={}), \
                     mock.patch.object(gate,'sha',return_value='1'*64), \
                     mock.patch.object(gate,'summarize',return_value={'screen_success':None}), \
                     mock.patch.object(gate,'write',side_effect=record), \
                     mock.patch.object(gate.os,'link',side_effect=publish), \
                     mock.patch.object(gate.resource,'setrlimit'), \
                     mock.patch.object(gate.signal,'signal'), \
                     mock.patch.object(gate.time,'monotonic',side_effect=lambda:clock[0]):
                    status = gate.main()
                self.assertEqual(count[0],6)
                self.assertIs(observations['last-row-owned-cleanup.json'],fixture_wait)
                if overrun is None:
                    self.assertEqual(status,0)
                    self.assertTrue(observations['result.json']['success'])
                    self.assertLess(observations['result.json']['elapsed_seconds'],5400)
                    self.assertNotIn('failure.json',observations)
                    r.cleanup_child.assert_not_called()
                else:
                    self.assertEqual(status,1)
                    self.assertNotIn('result.json',observations)
                    self.assertFalse(observations['failure.json']['success'])
                    self.assertIn('controller deadline before terminal success',observations['failure.json']['error'])
                    self.assertEqual(observations['failure.json']['cleanup'],[fixture_wait])
                    r.cleanup_child.assert_called_once_with()

    def test_hwm_terminal_result_write_overrun(self):
        # The real candidate writer/fsync and create-only link operate on private
        # unit files; the clock remains synthetic and provides no runtime admission.
        actual_write = gate.write
        for after,existing in ((5499.999,False),(5500.0,False),(5500.001,False),(5499.999,True)):
            with self.subTest(after=after,existing=existing), tempfile.TemporaryDirectory() as directory:
                r = gate.HwmBaselineRunner.__new__(gate.HwmBaselineRunner)
                r.root = pathlib.Path(directory)
                r.started = 100.0
                r.budget = mock.Mock()
                clock = [5499.0]
                terminal = r.root/'result.json'
                if existing:
                    terminal.write_bytes(b'preserved-existing-terminal')
                def record(path,value):
                    actual_write(path,value)
                    clock[0] = after
                with mock.patch.object(gate,'write',side_effect=record), \
                     mock.patch.object(gate.time,'monotonic',side_effect=lambda:clock[0]):
                    if after>=5500:
                        with self.assertRaisesRegex(RuntimeError,'controller deadline before terminal success'):
                            r.publish_result({'success':True})
                    elif existing:
                        with self.assertRaises(FileExistsError):
                            r.publish_result({'success':True})
                    else:
                        r.publish_result({'success':True})
                candidate = r.root/'result-candidate.json'
                self.assertTrue(json.loads(candidate.read_text())['success'])
                self.assertIn('post-fsync',json.loads(candidate.read_text())['elapsed_seconds_observation'])
                if after>=5500:
                    self.assertFalse(terminal.exists())
                elif existing:
                    self.assertEqual(terminal.read_bytes(),b'preserved-existing-terminal')
                else:
                    self.assertEqual(terminal.read_bytes(),candidate.read_bytes())
                    self.assertEqual(terminal.stat().st_ino,candidate.stat().st_ino)
    def test_hwm_steady_catalog_and_windows(self):
        s = gate.scenario(gate.HWM_STEADY_ID)
        expected = ['load','delete-1','refill-1','delete-2','refill-2','delete-3','refill-3']
        self.assertEqual(gate.hwm_steady_contract(s)['phases'],expected)
        self.assertEqual(gate.hwm_steady_contract(s)['measured_seconds'],20)
        self.assertEqual(gate.hwm_steady_contract(s)['warmup_seconds'],3)
        self.assertEqual(len(gate.members(gate.HWM_STEADY_PACKAGE)),6)
        plan = gate.hwm_plan_template(True)
        self.assertEqual(plan['package'],gate.HWM_STEADY_PACKAGE)
        self.assertEqual(plan['budgets']['runtime_seconds'],840)
        self.assertIsNone(plan['actual_measurements'])
        self.assertFalse(gate.hwm_plan_template()['steady_get_set_tested'])
        self.assertNotIn(gate.HWM_STEADY_ID,gate.IDS)

    def test_hwm_steady_prefix_oracle(self):
        s = gate.scenario(gate.HWM_STEADY_ID); s['keys'] = 64
        for p in gate.phases(s):
            if p['step']%3==2:
                continue
            for worker in range(16):
                first = gate.hwm_steady_prefix(s,p,worker,37)
                self.assertEqual(first['wire_commands'],{'GET':30,'SET':7})
                self.assertEqual(first,gate.hwm_steady_prefix(s,p,worker,37))
        self.assertNotEqual(gate.hwm_steady_prefix(s,gate.phases(s)[0],0,37)['trace_sha256'],
                            gate.hwm_steady_prefix(s,gate.phases(s)[3],0,37)['trace_sha256'])
        with self.assertRaises(RuntimeError):
            gate.hwm_uniform_rank(0,0,0)

    def _steady_event(self):
        s = gate.scenario(gate.HWM_STEADY_ID); s['keys'] = 64; p = gate.phases(s)[3]
        workers = []; outer = hashlib.sha256()
        for worker in range(16):
            prefix = gate.hwm_steady_prefix(s,p,worker,37)
            workers.append({'worker':worker,'rust_thread_id':'isolated-unit-worker-'+str(worker),
                'sequence_start':0,'sequence_end':37,'wire_commands':prefix['wire_commands'],'trace_sha256':prefix['trace_sha256'],
                'successful_replies':37,'warmup_completions':10,'warmup_straddled':0,'late_completions_excluded':1,
                'measured_command_counts':{'GET':21,'SET':5},'histogram_samples':26,
                'last_completion_ns_from_epoch':23000000001,'errors':0,'error':None})
            gate.frame(outer,prefix['trace_sha256'].encode())
        e = {'schema':2,'event':'steady-complete','pid':12,'scenario':s['id'],'phase':p['id'],
            'pipeline':1,'clients':16,'warmup_seconds':3,'measured_seconds':20,'live_keys':64,
            'access_policy':'uniform-live-key-rejection-v1','mix':'4GET-1SET-XX','trace_sha256':outer.hexdigest(),
            'workers':workers,'errors':0,'measured_command_counts':{'GET':336,'SET':80},'completed_requests':416,
            'histogram_samples':416,'ops_per_sec':416/20,'latency_unit':'checked-P1-request-completion',
            'histogram_scope':'fully measured requests only; warmup straddles and late completions excluded',
            'elapsed_ns_from_epoch':23000000002,'p50_ns':10,'p99_ns':20,'p999_ns':30,'maximum_ns':40}
        return s,p,e

    def test_hwm_steady_rejects_missing_workers_errors_and_bad_counts(self):
        s,p,e = self._steady_event()
        gate.validate_hwm_steady(e,s,p,12)
        for field,value in (('errors',1),('histogram_samples',0),('warmup_completions',9),('late_completions_excluded',2),
                            ('trace_sha256','0'*64),('rust_thread_id',e['workers'][0]['rust_thread_id'])):
            bad = copy.deepcopy(e); bad['workers'][15][field] = value
            with self.assertRaises(RuntimeError):
                gate.validate_hwm_steady(bad,s,p,12)
        for field,value in (('workers',e['workers'][:-1]),('completed_requests',415),('ops_per_sec',0),('measured_seconds',21),
                            ('phase','idle-1'),('trace_sha256','0'*64)):
            bad = copy.deepcopy(e); bad[field] = value
            with self.assertRaises(RuntimeError):
                gate.validate_hwm_steady(bad,s,p,12)

if __name__ == '__main__':
    unittest.main()
