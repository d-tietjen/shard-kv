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
        receipts[p['regression_acceptance']['path']].update(native_unit_tests=21, python_tests=37, exit_codes=[0, 0], child_reaped=True)
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
if __name__ == '__main__':
    unittest.main()
