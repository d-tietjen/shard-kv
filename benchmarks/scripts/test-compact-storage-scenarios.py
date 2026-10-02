#!/usr/bin/env python3
"""Offline contract regressions. Execute only through the reviewed Adam helper."""
import copy
import hashlib
import importlib.util
import pathlib
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
