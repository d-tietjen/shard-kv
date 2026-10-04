#!/usr/bin/env python3
"""Finite RESP diagnostics; immutable operating admission is required before effects.

The original density script is imported unchanged. A separately reviewed H1 child
wait implementation must be composed beside this file; the unfixed3eef helper is
explicitly inadmissible. Native timing, not Python elapsed time, is performance.
"""
from __future__ import annotations
import argparse
import collections
import csv
import datetime
import functools
import hashlib
import importlib.util
import json
import math
import os
import pathlib
import resource
import select
import signal
import statistics
import stat
import struct
import sys
import threading
import time
IDS = ('key63', 'key64', 'key65', 'value0', 'value15', 'value17', 'value255', 'value257', 'cardinality1k', 'cardinality1m', 'mixed-compressible', 'mixed-entropy', 'classes-compressible', 'classes-entropy', 'overwrite-compressible', 'overwrite-entropy', 'resize-compressible', 'resize-entropy', 'half-compressible', 'half-entropy', 'groups-compressible', 'groups-entropy', 'metadata', 'types')
CORE = tuple((f'core-v{size}-{pattern}' for size in (16, 64, 256, 1024, 4096) for pattern in ('compressible', 'high-entropy')))
ACCESS = tuple((f'v{size}-{access}' for size in (16, 64) for access in ('get', 'set', 'hot', 'pipeline')))
BASELINE = '78c83addff5f140236f9a659ff1e332bb9c35522'
BASELINE_TREE = '3b148db21708d2be93c23280eb9d2455a5b5d407'
SEED = 4991190488920227841
BUDGETS = {'complete_lifecycle_seconds': 8400, 'build_seconds': 2700, 'controller_seconds': 5400, 'runtime_seconds': 900, 'cleanup_seconds': 300, 'watchdog_seconds': 5, 'file_bytes': 134217728, 'output_bytes': 1073741824, 'owned_disk_bytes': 32212254720}
ACCEPTANCE = {'settled_delta_pss_ratio_max': 1.0, 'throughput_ratio_min': 0.95, 'p99_ratio_max': 1.05}
FEATURES = 'redis-server,experimental-compact-point-storage'
BUILD_ARGV = ['cargo', 'build', '--locked', '--release', '--jobs', '4', '-p', 'shardcache-benchmarks', '--bin', 'compact_resp_scenarios', '--bin', 'saturation']
HWM_ID = 'storage-hwm-str-v1'
HWM_PACKAGE = 'storage-hwm-baseline6-v1'
HWM_STEADY_ID = 'storage-hwm-str-steady-v1'
HWM_STEADY_PACKAGE = 'storage-hwm-baseline6-steady-v1'
HWM_PACKAGES = (HWM_PACKAGE,HWM_STEADY_PACKAGE)
HWM_SOURCE = '016a521b12968f38b703b3e3e39a8eb0be745429'
HWM_TREE = '16f855f045f44877d53d4ce5e840d20a555d31de'
HWM_SEED = 0xC0FFEE
HWM_KEYS = 1000000
HWM_BUDGETS = dict(BUDGETS, runtime_seconds=840)
HWM_BUILD_ARGV = BUILD_ARGV[:-2]
HWM_CAPS = {'server_memory':4294967296,'server_cpus':1,'server_pids':512,
            'client_memory':2147483648,'client_cpus':4,'client_pids':512,
            'controller_memory':2147483648,'controller_cpus':1,'controller_pids':64,
            'parent_memory':8589934592,'parent_cpus':6,'parent_pids':1088,'swap':0}
HWM_INPUTS = ('benchmarks/scripts/run-compact-storage-scenarios.py',
              'benchmarks/src/bin/compact_resp_scenarios.rs', 'benchmarks/Cargo.toml',
              'Cargo.lock', 'benchmarks/scripts/run-memory-density-benchmark.py',
              'benchmarks/scripts/test-compact-storage-scenarios.py')
HWM_NATIVE_TESTS = ('hwm_catalog_and_survivor_permutation', 'hwm_payload_mixed_generations_are_unique',
                    'hwm_survivors_keep_generation_zero', 'hwm_mutation_preserves_survivors_and_idle',
                    'hwm_verifier_requires_bytes_type_and_absence', 'hwm_verifier_rejects_stale_generation',
                    'hwm_state_digest_binds_generation_and_absence')
HWM_PYTHON_TESTS = ('test_hwm_catalog_members_and_legacy_catalog', 'test_hwm_payload_mixed_generations',
                    'test_hwm_oracle_counts_and_workers', 'test_hwm_corruption_and_worker_rejection',
                    'test_hwm_refuses_missing_delegation', 'test_hwm_boundary_identity_and_caps',
                    'test_hwm_placement_handshake_and_reaping', 'test_hwm_placement_failure_reaped',
                    'test_hwm_guard_freshness', 'test_hwm_docker_parent_mapping',
                    'test_hwm_docker_parent_refuses_before_effect', 'test_hwm_terminal_controller_deadline',
                    'test_hwm_terminal_deadline_preserves_cleanup', 'test_hwm_terminal_result_write_overrun')
HWM_STEADY_NATIVE_TESTS = ('hwm_steady_selects_live_uniform_keys',
                         'hwm_steady_set_xx_preserves_value_and_requires_exists','hwm_steady_checked_prefix_counts_trace')
HWM_STEADY_PYTHON_TESTS = ('test_hwm_steady_catalog_and_windows','test_hwm_steady_prefix_oracle',
                         'test_hwm_steady_rejects_missing_workers_errors_and_bad_counts')

def require(ok, message):
    if not ok:
        raise RuntimeError(message)

def sha(path):
    h = hashlib.sha256()
    with pathlib.Path(path).open('rb') as f:
        for b in iter(lambda: f.read(1048576), b''):
            h.update(b)
    return h.hexdigest()

def digest(value, length=64):
    return isinstance(value, str) and len(value) == length and all((c in '0123456789abcdef' for c in value))

def pinned(binding, maximum=134217728, executable=False):
    p = pathlib.Path(binding['path'])
    require(p.is_absolute() and p.resolve(strict=True) == p and p.is_file() and (not p.is_symlink()), 'noncanonical pinned file')
    require(p.stat().st_size <= maximum and digest(binding['sha256']) and (sha(p) == binding['sha256']), 'pinned file bound/hash differs')
    if executable:
        require(os.access(p, os.X_OK), 'pinned executable is not executable')
    return p

def read_json(binding, maximum=1048576):
    return json.loads(pinned(binding, maximum).read_text())

def write(path, value):
    with pathlib.Path(path).open('x') as f:
        json.dump(value, f, sort_keys=True, indent=2)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())

def import_file(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    sys.modules[name] = m
    spec.loader.exec_module(m)
    return m

def scenario(id, access=False):
    if id in (HWM_ID,HWM_STEADY_ID) and not access:
        return {'id':id,'keys':HWM_KEYS,'key_length':18,'value_length':64,
                'pattern':'per-key-unique-high-entropy-v1','family':'hwm','saturation':False}
    require(id in (ACCESS + CORE if access else IDS), 'unknown finite scenario')
    s = {'id': id, 'keys': 100000, 'key_length': 18, 'value_length': 16, 'pattern': 'high-entropy', 'family': 'fixed', 'saturation': access}
    if access and id in CORE:
        _, value, pattern = id.split('-', 2)
        s.update(value_length=int(value[1:]), pattern=pattern, family='access')
        return s
    if access:
        if id.startswith('v64-'):
            s.update(value_length=64, pattern='compressible')
        s['family'] = 'access'
        return s
    if id.startswith('key'):
        s['key_length'] = int(id[3:])
    elif id.startswith('value'):
        s['value_length'] = int(id[5:])
    elif id == 'cardinality1k':
        s['keys'] = 1000
    elif id == 'cardinality1m':
        s['keys'] = 1000000
    elif id in ('metadata', 'types'):
        s['family'] = id
    else:
        s['family'] = id.split('-')[0]
        if id.endswith('compressible'):
            s['pattern'] = 'compressible'
        if s['family'] == 'overwrite':
            s['value_length'] = 64
    return s

def phases(s):
    if s['family'] == 'hwm':
        return [{'id':'load','step':0}, *({'id':f'{name}-{cycle}','step':(cycle-1)*3+offset+1}
            for cycle in range(1,4) for offset,name in enumerate(('delete','idle','refill')))]
    family = s['family']
    if s['saturation']:
        return [{'id': 'verify', 'step': 0}]
    p = [{'id': 'load', 'step': 0}]
    if family in ('overwrite', 'resize', 'half', 'groups'):
        n = {'overwrite': 10, 'resize': 12, 'half': 6, 'groups': 6}[family]
        p.extend(({'id': f'{family}-{i}', 'step': i} for i in range(1, n + 1)))
    elif family in ('metadata', 'types'):
        names = ('expire', 'persist', 'delete', 'reinsert') if family == 'metadata' else ('typed', 'delete', 'reinsert')
        p.extend(({'id': v, 'step': i} for i, v in enumerate(names, 1)))
    return p

def fixed_key(index, length):
    require(length >= 8, 'insufficient unique key encoding')
    return b'k' * (length - 8) + index.to_bytes(8, 'big')

def record(s, p, index):
    if s['family'] == 'hwm':
        survivor = hwm_survivor(index)
        step = p['step']
        absent = step > 0 and step % 3 != 0 and not survivor
        generation = 0 if survivor or step == 0 else (step-1)//3 + int(step%3 == 0)
        return (fixed_key(index,18),64,generation,0 if absent else 1,False)
    key = fixed_key(index, s['key_length'])
    n = s['value_length']
    g = 0
    kind = 1
    expiry = False
    if s['saturation']:
        key = f'k:{index:016x}'.encode()
    family = s['family']
    step = p['step']
    if family == 'mixed':
        key = fixed_key(index, (8, 18, 63, 64)[index % 4])
        n = (16, 16, 16, 64, 64, 64, 256, 256, 1024, 4096)[index % 10]
    elif family == 'classes':
        cls = index + 1 if index < 9 else 10 + (index - 9) % 151
        stride = cls * 2
        k = cls - 1 if cls < 10 else max(18, stride - 256)
        key = bytes([cls]) * k if k < 18 else cls.to_bytes(2, 'big') + fixed_key(index, k)[2:]
        n = stride - k
    elif family == 'overwrite':
        g = step
    elif family == 'resize':
        n = (16, 64, 255, 257)[step % 4]
        g = step
    elif family == 'half' and index % 2 == 0:
        kind = 0 if step % 2 else 1
        g = step // 2
    elif family == 'groups' and index % 4 != 3:
        kind = 0 if step % 2 else 1
        n = (16, 64, 255, 17)[step // 2]
        g = step // 2
    elif family == 'metadata' and index % 10 == 0:
        expiry = step == 1
        kind = 0 if step == 3 else 1
        g = int(step == 4)
    elif family == 'types' and index % 10 == 0:
        kind = (2 if index % 20 == 0 else 3) if step == 1 else 0 if step == 2 else 1
        g = int(step == 3)
    return (key, n, g, kind, expiry)

@functools.lru_cache(maxsize=512)
def payload(size, pattern, generation):
    if pattern == 'compressible':
        return bytes([120 + generation % 26]) * size
    state = SEED + generation * 268435456 & (1 << 64) - 1
    out = bytearray()
    mask = (1 << 64) - 1
    while len(out) < size:
        state = state + 11400714819323198485 & mask
        word = state
        word = (word ^ word >> 30) * 13787848793156543929 & mask
        word = (word ^ word >> 27) * 10723151780598845931 & mask
        word ^= word >> 31
        out.extend(word.to_bytes(8, 'little'))
    return bytes(out[:size])

def frame(h, b):
    h.update(struct.pack('>Q', len(b)))
    h.update(b)

def command_frame(h, parts):
    h.update(struct.pack('>Q', len(parts)))
    for part in parts:
        frame(h, part)

def mutation_commands(s, p, i, r, value):
    key, n, g, kind, expiry = r
    step = p['step']
    family = s['family']
    if s['saturation']:
        return []
    if step == 0:
        return [(b'SET', key, value)]
    affected = {'half': i % 2 == 0, 'groups': i % 4 != 3, 'metadata': i % 10 == 0, 'types': i % 10 == 0}.get(family, True)
    if not affected:
        return []
    if kind == 0:
        return [(b'DEL', key)]
    if family == 'metadata' and step == 1:
        return [(b'PEXPIRE', key, b'7200000')]
    if family == 'metadata' and step == 2:
        return [(b'PERSIST', key)]
    if family == 'types' and step == 1:
        return [(b'DEL', key), (b'HSET', key, b'f', value) if kind == 2 else (b'RPUSH', key, value)]
    return [(b'SET', key, value)]

def verification_commands(r):
    key, n, g, kind, expiry = r
    commands = [(b'GET', key)]
    if kind == 1:
        commands.append((b'PTTL', key))
    elif kind in (2, 3):
        commands.extend([(b'TYPE', key), (b'HGETALL', key) if kind == 2 else (b'LRANGE', key, b'0', b'-1'), (b'PTTL', key)])
    return commands

def distribution(s):
    counts = collections.Counter(((len(r[0]), r[1]) for r in (record(s, phases(s)[0], i) for i in range(s['keys']))))
    return [{'key_bytes': k, 'value_bytes': v, 'records': n, 'logical_bytes': n * (k + v), 'compact_class': max(1, (k + v + 1) // 2) if k <= 64 and v <= 256 else None} for (k, v), n in sorted(counts.items())]

def expected_phase(s, p):
    if s['family'] == 'hwm':
        return hwm_expected_phase(s,p)
    state = hashlib.sha256()
    mutation = hashlib.sha256()
    verification = hashlib.sha256()
    live = logical = mops = vops = mtx = 0
    mix = collections.Counter()
    vmix = collections.Counter()
    for worker in range(16):
        hs = hashlib.sha256()
        hm = hashlib.sha256()
        hv = hashlib.sha256()
        for i in range(s['keys'] * worker // 16, s['keys'] * (worker + 1) // 16):
            r = record(s, p, i)
            key, n, g, kind, expiry = r
            value = payload(n, s['pattern'], g)
            live += int(kind != 0)
            logical += len(key) + n + int(kind == 2) if kind else 0
            for b in (key, bytes([kind]), bytes([expiry]), value if kind else b''):
                frame(hs, b)
            cmds = mutation_commands(s, p, i, r, value)
            mops += len(cmds)
            mtx += int(bool(cmds))
            for cmd in cmds:
                command_frame(hm, cmd)
                mix[cmd[0].decode()] += 1
            cmds = verification_commands(r)
            vops += len(cmds)
            for cmd in cmds:
                command_frame(hv, cmd)
                vmix[cmd[0].decode()] += 1
        frame(state, hs.hexdigest().encode())
        frame(mutation, hm.hexdigest().encode())
        frame(verification, hv.hexdigest().encode())
    return {'live_keys': live, 'absent_keys': s['keys'] - live, 'logical_bytes': logical, 'state_sha256': state.hexdigest(), 'mutation_trace_sha256': hashlib.sha256().hexdigest() if s['saturation'] else mutation.hexdigest(), 'verification_trace_sha256': verification.hexdigest(), 'mutation_operations': mops, 'verification_operations': vops, 'mutation_transactions': mtx, 'verification_transactions': s['keys'], 'mutation_command_counts': dict(mix), 'verification_command_counts': dict(vmix), 'control_commands': {'DBSIZE': 1}}

def members(package):
    if package in HWM_PACKAGES:
        id = HWM_STEADY_ID if package==HWM_STEADY_PACKAGE else HWM_ID
        return [{'id':f'r{r}-{arm}-{id}','round':r,'arm':arm,'scenario':id}
            for r in range(1,4) for arm in (('current','redis') if r%2 else ('redis','current'))]
    require(package in ('stateful-a', 'stateful-b', 'stateful-c', 'access'), 'unknown bounded package')
    ids = ACCESS if package == 'access' else IDS[(ord(package[-1]) - 97) * 8:(ord(package[-1]) - 96) * 8]
    out = []
    for round, arms in enumerate((('baseline', 'candidate', 'redis'), ('candidate', 'redis', 'baseline'), ('redis', 'baseline', 'candidate')), 1):
        for arm in arms:
            out.extend(({'id': f'r{round}-{arm}-{id}', 'round': round, 'arm': arm, 'scenario': id} for id in ids))
    return out

def validate_contract(p):
    if p.get('package') in HWM_PACKAGES:
        return validate_hwm_contract(p)
    require(p['schema'] == 1 and p['status'] == 'frozen with independently accepted gates', 'unfinished RESP package')
    require(p['classification'] == 'diagnostic-unreserved-closed-loop', 'qualification classification differs')
    require(p['members'] == members(p['package']), 'missing/extra/duplicate/reordered members')
    require(p['budgets'] == BUDGETS and p['acceptance'] == ACCEPTANCE, 'deadline/acceptance changed')
    require(p['images']['baseline']['source_sha'] == BASELINE and p['images']['baseline']['source_tree'] == BASELINE_TREE, 'baseline source differs')
    require(set(p['images']) == {'baseline', 'candidate', 'redis'}, 'image arms differ')
    require(p['images']['redis']['version'] == '7.4.11', 'Redis reference version differs')
    for arm, b in p['images'].items():
        require(b['engine_image_id'].startswith('sha256:') and digest(b['engine_image_id'][7:]), 'invalid typed Engine image identity')
        fields = {'source_sha', 'source_tree', 'features', 'engine_image_id', 'original_tag', 'provenance', 'resolved_build'} if arm != 'redis' else {'version', 'engine_image_id', 'repo_digests', 'reference_acceptance'}
        require(set(b) == fields, 'unexpected arm/image fields or arm-specific native')
        if arm != 'redis':
            require(digest(b['source_sha'], 40) and digest(b['source_tree'], 40) and (b['features'] == FEATURES), 'source/features differ')
    require(p['native_build']['argv'] == BUILD_ARGV and p['native_build']['features'] == [], 'native source-neutral default build differs')
    require(digest(p['native_build']['source_sha'], 40) and digest(p['native_build']['source_tree'], 40), 'missing common native source')
    require(p['sampling'] == {'idle': 5, 'final': 5, 'intermediate': 5, 'delay_seconds': 0.2, 'settle_seconds': 1.0, 'peak_interval_seconds': 0.2}, 'sample contract differs')
    require(p['caps'] == {'server_memory': 4294967296, 'server_cpus': 1, 'controller_memory': 2147483648, 'controller_cpus': 4, 'swap': 0}, 'caps differ')
    require(p['owners'] == {arm: p['owner'] + '-' + arm for arm in ('baseline', 'candidate', 'redis')}, 'arm owners differ')
    require(p['owner'].startswith('eden2266-') and len(p['owner']) <= 96 and all((c.isalnum() or c == '-' for c in p['owner'])), 'invalid owner')
    require(set(p['binaries']) == {'compact_resp_scenarios', 'saturation'}, 'common native artifacts differ')
    require(all((set(v) == {'path', 'sha256'} and digest(v['sha256']) for v in p['binaries'].values())), 'common native binding differs')
    require(set(p['helpers']) == {'density', 'child_wait', 'docker', 'git'}, 'helper closure differs')
    require(p['builds'] == {'common': {'worktree': p['source']['worktree']}}, 'native target accounting closure differs')
    require(p['source']['sha'] == p['native_build']['source_sha'] and p['source']['tree'] == p['native_build']['source_tree'], 'executed harness/source differs')
    return p

def validate_ready(e, s, pid):
    if s['family'] == 'hwm':
        return validate_hwm_ready(e,s,pid)
    require(e['schema'] == 1 and e['event'] == 'ready' and (e['pid'] == pid) and (e['scenario'] == s), 'native ready identity differs')
    require(e['seed'] == SEED and e['clients'] == 16 and (e['pipeline'] == 1) and (e['deadline_seconds'] == 900), 'native workload bounds differ')
    require(e['initial_dbsize_checks'] == int(not s['saturation']), 'initial DBsize check differs')
    require(e['phases'] == phases(s) and e['distribution'] == distribution(s), 'native phase/distribution differs')

def validate_phase(e, s, p, pid, witness):
    if s['family'] == 'hwm':
        return validate_hwm_phase(e,s,p,pid,witness)
    require((e['schema'], e['event'], e['pid'], e['scenario'], e['phase']) == (1, 'phase', pid, s['id'], p['id']), 'native phase order/identity differs')
    for k, v in witness.items():
        if k.endswith(('_operations', '_transactions')):
            continue
        require(e[k] == v, f'native {k} differs')
    for name in ('mutation', 'verification'):
        t = e[name]
        n = witness[name + '_operations']
        require(type(t['wire_commands']) is int and type(t['completed_records']) is int and (t['wire_commands'] == n) and (t['completed_records'] == witness[name + '_transactions']), 'operation/transaction count differs')
        require(all((type(t[k]) is int and t[k] >= 0 for k in ('elapsed_ns', 'p50_ns', 'p99_ns', 'p999_ns', 'maximum_ns'))), 'malformed timing')
        require(all((isinstance(t[k], (float, int)) and math.isfinite(t[k]) for k in ('wire_commands_per_sec', 'completed_records_per_sec'))), 'nonfinite throughput')
        if n:
            require(t['elapsed_ns'] > 0 and 0 < t['p50_ns'] <= t['p99_ns'] <= t['p999_ns'] <= t['maximum_ns'] <= 60000000000, 'latency order/bound differs')
            require(t['latency_unit'] == ('logical-record-transition' if name == 'mutation' else 'full-record-verification'), 'native percentile units differ')
            require(math.isclose(t['wire_commands_per_sec'], n * 1000000000.0 / t['elapsed_ns'], rel_tol=1e-09) and math.isclose(t['completed_records_per_sec'], t['completed_records'] * 1000000000.0 / t['elapsed_ns'], rel_tol=1e-09), 'native throughput arithmetic differs')
        else:
            require(t['latency_unit'] == 'absent' and all((t[k] == 0 for k in t if k != 'latency_unit')), 'absent mutation timing must be zero')

def median_samples(samples):
    require(len(samples) == 5, 'settled sample count differs')
    keys = ('rss_bytes', 'pss_bytes', 'private_bytes', 'cgroup_current_bytes', 'cgroup_anon_bytes', 'cgroup_file_bytes')
    require(all((all((type(s[k]) is int and s[k] >= (1 if k in ('rss_bytes', 'pss_bytes', 'cgroup_current_bytes') else 0) for k in keys)) for s in samples)), 'missing/invalid sampler metrics')
    return {k: int(statistics.median((s[k] for s in samples))) for k in keys}

class Runner:

    def __init__(self, plan, root):
        self.plan = plan
        self.root = root
        self.started = time.monotonic()
        self.density = None
        self.children = {}
        self.row_deadline = None
        self.next_budget = 0
        self.cleanup_errors = []
        self.h1 = import_file('resp_h1', pinned(plan['helpers']['child_wait']))
        require(all((hasattr(self.h1, n) for n in ('NativeChildWait', 'block_termination', 'defer_cleanup_termination', 'stop_child'))), 'H1 reviewed ownership helper not composed; unfixed3eef inadmissible')
        self.density = import_file('resp_density', pinned(plan['helpers']['density']))
        self.density.command = lambda argv, **kw: self.command(argv, timeout=60)
        self.density.subprocess = self
        self.PIPE = -1
        require(signal.getsignal(signal.SIGCHLD) in (signal.SIG_DFL, None), 'unexpected SIGCHLD ownership')
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)

    def budget(self):
        require(time.monotonic() - self.started < 5400, 'controller deadline')
        require(self.row_deadline is None or time.monotonic() < self.row_deadline, 'server-row runtime deadline')
        require(not pathlib.Path(self.plan['stop_path']).exists(), 'STOP requested')
        if time.monotonic() >= self.next_budget:
            self.h1.budget_check(self.plan)
            self.next_budget = time.monotonic() + 5

    def spawn(self, argv, folder, interactive=False):
        require(len(self.children) < 2, 'too many simultaneously owned children')
        fds = [-1, -1, -1, -1]
        pid = None
        waiter = None
        try:
            with self.h1.block_termination() as mask:
                fds[0], fds[1] = os.pipe()
                fds[2], fds[3] = os.pipe()
                read, stdout, stdin, writefd = fds
                pid = os.fork()
                if pid == 0:
                    try:
                        os.setsid()
                        os.dup2(stdin, 0)
                        os.dup2(stdout, 1)
                        fd = os.open(folder / 'stderr.log', os.O_CREAT | os.O_EXCL | os.O_WRONLY, 384)
                        os.dup2(fd, 2)
                        for n in (read, stdout, stdin, writefd, fd):
                            os.close(n)
                        signal.pthread_sigmask(signal.SIG_SETMASK, mask)
                        os.execv(argv[0], argv)
                    except BaseException:
                        os._exit(127)
                waiter = self.h1.NativeChildWait(pid)
                self.children[pid] = waiter
                waiter.bind()
            self.close_owned_fd(fds, 2)
            self.close_owned_fd(fds, 1)
            if not interactive:
                self.close_owned_fd(fds, 3)
            write(folder / 'child.json', {'argv': argv, 'identity': waiter.identity})
            return (waiter, fds[0], fds[3])
        except BaseException:
            self.cleanup_preserving(waiter, folder, fds)
            raise

    def close_owned_fd(self, fds, index):
        with self.h1.block_termination():
            fd = fds[index]
            if fd >= 0:
                try:
                    os.close(fd)
                finally:
                    # Never retry a possibly completed close after interruption.
                    fds[index] = -1

    def wait(self, waiter, until):
        while time.monotonic() < until:
            wait = waiter.poll()
            if wait is not None:
                require(os.WIFEXITED(wait['raw_wait_status']), 'child terminated by signal')
                return dict(wait, exit_code=os.WEXITSTATUS(wait['raw_wait_status']))
            time.sleep(0.05)
        raise RuntimeError('actual child wait deadline')

    def cleanup_child(self, waiter=None):
        selected = [waiter] if waiter is not None else list(self.children.values())
        receipts = []
        errors = []
        for child in selected:
            try:
                result = self.h1.stop_child(child)
                require(not result['ownership_lost'] and result['child_reaped'], 'child ownership/reaping cleanup failed')
                receipts.append(result)
                self.children.pop(child.pid, None)
            except BaseException as exc:
                errors.append(f'{type(exc).__name__}: {exc}')
        require(not errors, '; '.join(errors))
        return receipts

    def cleanup_preserving(self, waiter, folder, fds=()):
        original_error = sys.exc_info()[0] is not None
        errors = []
        cleanup = None
        with self.h1.defer_cleanup_termination() as received:
            for index in range(len(fds)):
                try:
                    self.close_owned_fd(fds, index)
                except BaseException as exc:
                    errors.append(f'fd cleanup: {type(exc).__name__}: {exc}')
            if waiter is not None and waiter.pid in self.children:
                try:
                    cleanup = self.cleanup_child(waiter)
                except BaseException as exc:
                    errors.append(f'child cleanup: {type(exc).__name__}: {exc}')
            if received:
                errors.append(f'deferred cleanup signals: {received}')
            if errors or original_error:
                self.cleanup_errors.extend(errors)
                message = '; '.join(errors)
                try:
                    write(folder / ('cleanup-failure.json' if errors else 'cleanup.json'), {'cleanup_error': message, 'cleanup': cleanup, 'cleanup_errors': errors, 'cleanup_signals': received, 'original_exception_in_flight': original_error, 'identity': waiter.identity if waiter is not None else None, 'wait': waiter.wait if waiter is not None else None})
                except BaseException as exc:
                    self.cleanup_errors.append(f'cleanup receipt: {type(exc).__name__}: {exc}')
                    if not original_error:
                        raise
            if not original_error:
                require(not errors, '; '.join(errors))

    def command(self, argv, timeout=30):
        require(argv[0] in ('docker', 'git'), 'unapproved helper command')
        argv = [str(pinned(self.plan['helpers'][argv[0]], executable=True)), *argv[1:]]
        folder = self.root / 'commands' / f"{len(list((self.root / 'commands').iterdir())):06d}"
        folder.mkdir()
        waiter, fd, _ = self.spawn(argv, folder)
        fds = [fd]
        data = bytearray()
        until = min(time.monotonic() + timeout, self.row_deadline or float('inf'))
        try:
            while True:
                require(time.monotonic() < until, 'helper command deadline')
                if select.select([fd], [], [], 0.1)[0]:
                    b = os.read(fd, 65536)
                    if not b:
                        break
                    data.extend(b)
                    require(len(data) <= 1048576, 'helper stdout bound')
            wait = self.wait(waiter, until)
            write(folder / 'wait.json', wait)
            require(wait['exit_code'] == 0, 'helper genuinely exited nonzero')
            (folder / 'stdout.log').write_bytes(data)
            self.children.pop(waiter.pid, None)
            return data.decode().strip()
        finally:
            self.cleanup_preserving(waiter, folder, fds)

    def run(self, argv, check=True, text=True, capture_output=True, **kw):
        """Route only the unchanged sampler's exact smaps observation through an owned wait."""
        require(argv[:3] == ['docker', 'exec', '--user'] and argv[-2:] == ['cat', '/proc/1/smaps_rollup'], 'unexpected sampler subprocess')
        return argparse.Namespace(stdout=self.command(argv), returncode=0)

    def verify_inputs(self):
        p = self.plan
        self.h1.verify_scope(os.getpid(), p['controller_scope'], 2147483648, 4)
        work = pathlib.Path(p['source']['worktree'])
        require(self.command(['git', '-C', str(work), 'rev-parse', 'HEAD']) == p['source']['sha'] and self.command(['git', '-C', str(work), 'rev-parse', 'HEAD^{tree}']) == p['source']['tree'], 'source HEAD/tree differs')
        require(not self.command(['git', '-C', str(work), 'status', '--porcelain']), 'dirty executed source')
        require(pathlib.Path(__file__).resolve() == work / 'benchmarks/scripts/run-compact-storage-scenarios.py', 'executed controller path differs')
        for relative, expected in p['source']['file_sha256'].items():
            require(relative in ('benchmarks/scripts/run-compact-storage-scenarios.py', 'benchmarks/src/bin/compact_resp_scenarios.rs', 'benchmarks/Cargo.toml', 'Cargo.lock', 'benchmarks/scripts/run-memory-density-benchmark.py'), 'unexpected source binding path')
            require(sha(work / relative) == expected, 'source file hash differs')
        require(set(p['source']['file_sha256']) == {'benchmarks/scripts/run-compact-storage-scenarios.py', 'benchmarks/src/bin/compact_resp_scenarios.rs', 'benchmarks/Cargo.toml', 'Cargo.lock', 'benchmarks/scripts/run-memory-density-benchmark.py'}, 'incomplete source input closure')
        for b in p['binaries'].values():
            pinned(b, executable=True)
        for kind in ('early_gate_acceptance', 'regression_acceptance', 'native_build_acceptance', 'operating_acceptance'):
            receipt = read_json(p[kind])
            require(receipt['status'] == 'independently accepted' and receipt['source_sha'] == p['source']['sha'] and (receipt['source_tree'] == p['source']['tree']), 'missing exact-source independent admission')
        regression = read_json(p['regression_acceptance'])
        require(regression['native_unit_tests'] == 21 and regression['python_tests'] == 40 and type(regression.get('exit_codes')) is list and len(regression['exit_codes']) == 2 and all(type(code) is int and code == 0 for code in regression['exit_codes']) and (regression.get('child_reaped') is True), 'focused native/Python regression admission differs')
        early = read_json(p['early_gate_acceptance'])
        require(early['functional_success'] is True and early['screen_success'] is True and (early['processes'] == 30), 'early API gate did not pass')
        receipt = read_json(p['native_build_acceptance'])
        require(receipt['argv'] == BUILD_ARGV and type(receipt.get('exit_code')) is int and receipt['exit_code'] == 0 and (receipt.get('child_reaped') is True) and (receipt['artifact_sha256'] == {k: v['sha256'] for k, v in p['binaries'].items()}), 'native build/compiler/exit receipt differs')
        require(receipt['compiler_sha256'] == p['native_build']['compiler']['sha256'] and receipt['artifact_paths'] == {k: v['path'] for k, v in p['binaries'].items()} and (receipt['rustflags'] == []) and (receipt.get('wait_observed') is True) and type(receipt.get('raw_wait_status')) is int and receipt['raw_wait_status'] == 0, 'native compiler/actual artifact/flags/wait closure differs')
        pinned(p['native_build']['compiler'], executable=True)
        for arm, b in p['images'].items():
            inspected = json.loads(self.command(['docker', 'image', 'inspect', b['engine_image_id']]))
            require(len(inspected) == 1 and inspected[0]['Id'] == b['engine_image_id'], 'cached image identity differs')
            if arm != 'redis':
                provenance = b['provenance']
                resolved = self.density.resolve_shardcache_build(read_json(provenance['metadata'], 134217728), pinned(provenance['dockerfile']).read_bytes(), pinned(provenance['export_log']).read_text(), features=FEATURES, build_jobs=4, candidate_sha=b['source_sha'], image_tag=b['original_tag'], engine_image_id=b['engine_image_id'])
                require(resolved == b['resolved_build'], 'original material/export/source provenance differs')
                closure = read_json(provenance['source_context_acceptance'])
                require(closure['status'] == 'independently accepted' and closure['source_sha'] == b['source_sha'] and (closure['source_tree'] == b['source_tree']), 'original image source/tree context lacks acceptance')
            else:
                require(inspected[0]['RepoDigests'] == b['repo_digests'], 'Redis digest closure differs')
                pinned(b['reference_acceptance'])
        require(pathlib.Path(self.plan['helpers']['density']['path']).parent == pathlib.Path(__file__).parent and pathlib.Path(self.plan['helpers']['child_wait']['path']).parent == pathlib.Path(__file__).parent, 'helpers not from executed source')
        require(os.environ.get('EDEN_DENSITY_OWNER') == p['owners']['candidate'] and pathlib.Path(os.environ['EDEN_DENSITY_DOCKER_RECEIPT']) == pathlib.Path(p['docker_receipt']), 'adapter owner/receipt environment differs')

    def snapshot(self, cid, pid, user, cg, port):
        log = pathlib.Path(self.plan['docker_receipt'])
        before = log.stat().st_size
        s = self.density.process_memory(pid, cid, user, cg, port)
        with log.open('rb') as f:
            f.seek(before)
            records = [json.loads(line) for line in f if line.strip()]
        matches = [r for r in records if r.get('kind') == 'memory-sample' and r.get('status') == 'verified']
        require(len(matches) == 1 and matches[0]['container_id'] == cid and (matches[0]['pid'] == pid) and (matches[0]['owner'] == os.environ['EDEN_DENSITY_OWNER']) and (matches[0]['cgroup_path'] == str(cg)), 'sampler lacks one fresh verified PID1/owned-boundary gate')
        require(not any((r.get('kind') == 'memory-sample-gate-failure' for r in records)), 'sampler gate failed')
        rollup = matches[0]['smaps_rollup']
        counts = collections.Counter((line.split(':', 1)[0] for line in rollup.splitlines() if ':' in line))
        require(all((counts[k] == 1 for k in ('Rss', 'Pss', 'Private_Clean', 'Private_Dirty'))), 'incomplete/ambiguous verified smaps fields')
        return s

    def snapshots(self, cid, pid, user, cg, port):
        out = []
        time.sleep(1)
        for _ in range(5):
            self.budget()
            out.append(self.snapshot(cid, pid, user, cg, port))
            time.sleep(0.2)
        return {'samples': out, 'medians': median_samples(out)}

    def run_native(self, member, folder, cid, pid, user, cg, port, witnesses):
        s = scenario(member['scenario'], self.plan['package'] == 'access')
        argv = [str(pinned(self.plan['binaries']['compact_resp_scenarios'], executable=True)), '--scenario', s['id'], '--addr', f'127.0.0.1:{port}']
        if s['saturation']:
            argv.append('--verify-saturation')
        folder.mkdir()
        waiter, fd, ack = self.spawn(argv, folder, True)
        fds = [fd, ack]
        buffer = b''
        events = []
        until = min(time.monotonic() + 900, self.row_deadline or float('inf'))
        try:
            for phase in [None, *phases(s), 'complete']:
                quiet = None
                steady = None
                while b'\n' not in buffer:
                    self.budget()
                    require(time.monotonic() < until, 'native runtime deadline')
                    if select.select([fd], [], [], 0.2)[0]:
                        b = os.read(fd, 8192)
                        require(b, 'native EOF before terminal event')
                        buffer += b
                        require(len(buffer) <= 65536, 'native event line bound')
                line, buffer = buffer.split(b'\n', 1)
                e = json.loads(line)
                if s['family']=='hwm' and isinstance(phase,dict) and phase['step']%3==2:
                    write(folder/('observed-'+phase['id']+'-quiet.json'),e)
                    require((e['schema'],e['event'],e['pid'],e['scenario'],e['phase'])==
                            (2,'idle-boundary',waiter.pid,s['id'],phase['id']),'quiet boundary differs')
                    require(type(e['idle_elapsed_ns']) is int and 10000000000<=e['idle_elapsed_ns']<840000000000,'quiet duration differs')
                    actual = int(self.density.redis_command(port,b'DBSIZE'))
                    require(actual==witnesses[phase['id']]['live_keys'],'quiet logical count differs')
                    quiet = {'native_boundary':e,'controller_dbsize':actual,'settled':self.snapshots(cid,pid,user,cg,port),
                             'client_and_controller':self.client_snapshot(waiter),'before_exhaustive_reads':True}
                    write(folder/(phase['id']+'-quiet.json'),quiet)
                    os.write(ack,b'continue\n')
                    while b'\n' not in buffer:
                        self.budget()
                        require(time.monotonic()<until,'native runtime deadline after quiet boundary')
                        if select.select([fd],[],[],0.2)[0]:
                            chunk = os.read(fd,8192)
                            require(bool(chunk),'native EOF after quiet boundary')
                            buffer += chunk
                            require(len(buffer)<=65536,'native event line bound')
                    line,buffer = buffer.split(b'\n',1)
                    e = json.loads(line)
                if s['id']==HWM_STEADY_ID and isinstance(phase,dict) and phase['step']%3!=2:
                    write(folder/('observed-'+phase['id']+'-steady-ready.json'),e)
                    require(e=={'schema':2,'event':'steady-ready','pid':waiter.pid,'scenario':s['id'],
                        'phase':phase['id'],'warmup_seconds':3,'measured_seconds':20,'clients':16,'pipeline':1},'steady boundary differs')
                    before = self.steady_snapshot(waiter,cid,pid,user,cg,port)
                    os.write(ack,b'continue\n')
                    while b'\n' not in buffer:
                        self.budget()
                        require(time.monotonic()<until,'native steady runtime deadline')
                        if select.select([fd],[],[],0.2)[0]:
                            chunk = os.read(fd,8192)
                            require(bool(chunk),'native EOF in steady window')
                            buffer += chunk
                            require(len(buffer)<=65536,'native steady event line bound')
                    line,buffer = buffer.split(b'\n',1)
                    window = json.loads(line)
                    write(folder/('observed-'+phase['id']+'-steady-complete.json'),window)
                    after = self.steady_snapshot(waiter,cid,pid,user,cg,port)
                    # CPU/memory boundaries are sampled before oracle work and
                    # before permitting exhaustive reads, so validation CPU is
                    # not incorrectly attributed to the traffic interval.
                    validate_hwm_steady(window,s,phase,waiter.pid,self.budget)
                    steady = {'native':window,'before':before,'after':after,
                        'cpu_intervals':hwm_cpu_intervals(before,after),
                        'cpu_scope':'warmup + measured traffic + boundary/connection overhead, before exhaustive verification'}
                    write(folder/(phase['id']+'-steady.json'),steady)
                    os.write(ack,b'continue\n')
                    while b'\n' not in buffer:
                        self.budget()
                        require(time.monotonic()<until,'native runtime deadline after steady window')
                        if select.select([fd],[],[],0.2)[0]:
                            chunk = os.read(fd,8192)
                            require(bool(chunk),'native EOF after steady window')
                            buffer += chunk
                            require(len(buffer)<=65536,'native event line bound')
                    line,buffer = buffer.split(b'\n',1)
                    e = json.loads(line)
                if s['family']=='hwm':
                    label = 'ready' if phase is None else 'complete' if phase=='complete' else phase['id']
                    write(folder / ('observed-'+label+'.json'),e)
                if phase is None:
                    validate_ready(e, s, waiter.pid)
                    identity = self.h1.proc_identity(waiter.pid, pathlib.Path(argv[0]))
                    require(identity['start_ticks'] == waiter.identity['start_ticks'], 'native fork/executed ELF identity differs')
                    write(folder / 'ready.json', dict(e, executed_identity=identity))
                    if s['family']=='hwm':
                        self.client_started(waiter,folder)
                elif phase == 'complete':
                    require(e == {'schema': 2 if s['family']=='hwm' else 1, 'event': 'complete', 'pid': waiter.pid, 'scenario': s['id'], 'phases': len(phases(s)), 'errors': 0}, 'native terminal event differs')
                    break
                else:
                    validate_phase(e, s, phase, waiter.pid, witnesses[phase['id']])
                    dbsize = int(self.density.redis_command(port, b'DBSIZE'))
                    require(dbsize == e['live_keys'], 'post-phase DBSIZE differs')
                    samples = self.snapshots(cid, pid, user, cg, port)
                    receipt = {'native': e, 'settled': samples, 'controller_dbsize': {'checks': 1, 'actual': dbsize, 'expected': e['live_keys']}}
                    if s['family']=='hwm':
                        receipt['client_and_controller'] = self.client_snapshot(waiter)
                        receipt['quiet_after_idle'] = quiet
                        if s['id']==HWM_STEADY_ID:
                            receipt['steady'] = steady
                    write(folder / (phase['id'] + '.json'), receipt)
                    events.append(receipt)
                os.write(ack, b'continue\n')
            self.close_owned_fd(fds, 1)
            wait = self.wait(waiter, until)
            require(wait['exit_code'] == 0 and (not buffer) and (not os.read(fd, 1)), 'native nonzero/extra terminal output')
            write(folder / 'wait.json', wait)
            self.children.pop(waiter.pid, None)
            return events
        finally:
            self.cleanup_preserving(waiter, folder, fds)

    def row(self, m, witnesses):
        self.budget()
        row_started = time.monotonic()
        self.row_deadline = row_started + (840 if self.plan['package'] in HWM_PACKAGES else 900)
        folder = self.root / m['id']
        folder.mkdir()
        cid = ''
        primary = None
        cleanup = []
        result = None
        b = self.plan['images'][m['arm']]
        target = 'redis' if m['arm'] == 'redis' else 'shardcache-resp'
        port = self.density.available_host_port()
        os.environ['EDEN_DENSITY_OWNER'] = self.plan['owners'][m['arm']]
        args = argparse.Namespace(runtime_image_tag=b['engine_image_id'], run_id=self.plan['owner'], candidate_sha=b.get('source_sha', self.plan['source']['sha']), vcpus=1, memory_limit='4g')
        name = f"{self.plan['owner'][-20:]}-{m['id']}"[:63]
        write(folder / 'container-attempt.json', {'name': name, 'owner': self.plan['owners'][m['arm']], 'image_id': b['engine_image_id'], 'source_sha_label': args.candidate_sha})
        old = self.density.TARGETS['redis']['image']
        self.density.TARGETS['redis']['image'] = b['engine_image_id']
        try:
            cid = self.density.start_target_container(args, target, name, port)
            require(digest(cid), 'Docker returned non-full containerID')
            self.density.wait_for_target(port, 60)
            pid = int(self.density.get_inspect(cid, 'State.Pid'))
            require(self.density.get_inspect(cid, 'Image') == b['engine_image_id'], 'runtime image differs')
            cg = self.density.cgroup_v2_dir(pid)
            require(cg is not None, 'missing live server cgroup')
            user = self.density.TARGETS[target]['process_user']
            idle = self.snapshots(cid, pid, user, cg, port)
            write(folder / 'idle.json', idle)
            peak = Peak(cg, pid)
            peak.start()
            perf = None
            native_error = None
            try:
                if self.plan['package'] == 'access':
                    perf = self.access(m, folder, pid, port, target)
                events = self.run_native(m, folder / 'native', cid, pid, user, cg, port, witnesses)
            except BaseException as exc:
                native_error = exc
            finally:
                try:
                    peak.stop()
                except BaseException as exc:
                    cleanup.append(f'peak cleanup: {type(exc).__name__}: {exc}')
                write(folder / 'sampled-peak.json', peak.receipt())
            if native_error:
                raise native_error
            require(not cleanup, 'peak observer failed')
            info = self.density.parse_info(port, b'server')
            if target == 'redis':
                require(info.get('redis_version') == '7.4.11', 'actual Redis runtime version differs')
            final = events[-1]
            metrics = final['settled']['medians']
            result = {'member': m, 'source': b, 'common_artifacts': self.plan['binaries'], 'container_id': cid, 'pid': pid, 'idle': idle['medians'], 'final': metrics, 'delta_pss_bytes': max(0, metrics['pss_bytes'] - idle['medians']['pss_bytes']), 'total_pss_bytes_per_live_key': metrics['pss_bytes'] / final['native']['live_keys'], 'incremental_pss_bytes_per_live_key': max(0, metrics['pss_bytes'] - idle['medians']['pss_bytes']) / final['native']['live_keys'], 'logical_dataset': {'live_keys': final['native']['live_keys'], 'logical_bytes': final['native']['logical_bytes'], 'state_sha256': final['native']['state_sha256']}, 'phases': events, 'native_performance': perf, 'sampled_peak': peak.receipt(), 'server_version': info.get('redis_version'), 'correctness': True}
        except BaseException as exc:
            primary = exc
        finally:
            with self.h1.defer_cleanup_termination() as received:
                self.density.TARGETS['redis']['image'] = old
                self.row_deadline = None
                cleanup.extend(self.cleanup_errors)
                self.cleanup_errors = []
                if not cid:
                    cleanup.append('startup returned no owned fullID; exact recorded name requires operating recovery/absence proof')
                if cid:
                    for argv in (['docker', 'stop', '--time', '15', cid], ['docker', 'rm', cid]):
                        try:
                            self.command(argv, 30)
                        except BaseException as exc:
                            cleanup.append(f'{type(exc).__name__}: {exc}')
                    try:
                        remaining = self.command(['docker', 'ps', '-aq', '--no-trunc', '--filter', f'id={cid}'], 30)
                        require(not remaining, 'owned fullID container remains')
                    except BaseException as exc:
                        cleanup.append(f'{type(exc).__name__}: {exc}')
                if received:
                    cleanup.append(f'deferred cleanup signals: {received}')
                write(folder / 'result.json', {'success': primary is None and (not cleanup), 'error': None if primary is None else f'{type(primary).__name__}: {primary}', 'cleanup_errors': cleanup, 'row': result, 'elapsed_seconds': time.monotonic() - row_started})
        if primary:
            raise primary
        require(not cleanup, 'owned container cleanup failed')
        return result

    def access(self, m, folder, pid, port, target):
        s = scenario(m['scenario'], True)
        profile = m['scenario'].split('-')[1]
        csvpath = folder / 'saturation.csv'
        argv = [str(pinned(self.plan['binaries']['saturation'], executable=True)), '--backends', self.density.TARGETS[target]['backend'], '--addr', f'127.0.0.1:{port}', '--server-pid', str(pid), '--vcpu-budget', '1', '--clients', '16', '--pipeline-depth', '16' if profile == 'pipeline' else '1', '--value-size', str(s['value_length']), '--value-pattern', s['pattern'], '--mix', profile if profile in ('get', 'set') else '80-20', '--key-count', '100000', '--key-distribution', 'hot:1000:90' if profile == 'hot' else 'uniform', '--duration', '20', '--warmup', '3', '--csv', str(csvpath)]
        out = folder / 'saturation'
        out.mkdir()
        waiter, fd, _ = self.spawn(argv, out)
        fds = [fd]
        until = min(time.monotonic() + 900, self.row_deadline or float('inf'))
        data = bytearray()
        try:
            while True:
                self.budget()
                require(time.monotonic() < until, 'saturation runtime deadline')
                if select.select([fd], [], [], 0.2)[0]:
                    chunk = os.read(fd, 8192)
                    if not chunk:
                        break
                    data.extend(chunk)
                    require(len(data) <= 1048576, 'saturation stdout bound')
            wait = self.wait(waiter, until)
            write(out / 'wait.json', wait)
            require(wait['exit_code'] == 0, 'saturation genuinely exited nonzero')
            self.children.pop(waiter.pid, None)
            (out / 'stdout.log').write_bytes(data)
            with csvpath.open() as f:
                rows = list(csv.DictReader(f))
            require(len(rows) == 1 and rows[0]['errors'] == '0', 'saturation rows/errors differ')
            r = rows[0]
            for name in ('ops_per_sec', 'p50_ns', 'p99_ns', 'p999_ns'):
                require(math.isfinite(float(r[name])) and float(r[name]) > 0, 'invalid saturation metric')
            return r
        finally:
            self.cleanup_preserving(waiter, out, fds)

class Peak:
    """Sampled current/anon maxima, not exact peaks or settled PID1 gates."""

    def __init__(self, cg, pid):
        self.cg = cg
        self.pid = pid
        self.done = threading.Event()
        self.thread = None
        self.count = 0
        self.maximum = {'current': 0, 'anon': 0}
        self.error = None
        self.start_ticks = None

    def identity(self):
        raw = pathlib.Path(f'/proc/{self.pid}/stat').read_text()
        ticks = raw.rsplit(') ', 1)[1].split()[19]
        rows = pathlib.Path(f'/proc/{self.pid}/cgroup').read_text().splitlines()
        require(rows == ['0::/' + str(self.cg.relative_to('/sys/fs/cgroup'))], 'peak PID cgroup differs')
        return ticks

    def start(self):
        self.start_ticks = self.identity()
        self.thread = threading.Thread(target=self.observe)
        self.thread.start()

    def observe(self):
        try:
            while not self.done.is_set():
                require(self.identity() == self.start_ticks, 'peak PID start changed')
                for file, expected in (('memory.max', '4294967296'), ('memory.swap.max', '0'), ('cpu.max', '100000 100000')):
                    require((self.cg / file).read_text().strip() == expected, 'peak runtime cap changed')
                current = int((self.cg / 'memory.current').read_text())
                stat = dict((line.split() for line in (self.cg / 'memory.stat').read_text().splitlines()))
                anon = int(stat['anon'])
                require(self.identity() == self.start_ticks, 'peak observation identity changed')
                self.count += 1
                self.maximum = {'current': max(self.maximum['current'], current), 'anon': max(self.maximum['anon'], anon)}
                self.done.wait(0.2)
        except BaseException as exc:
            self.error = f'{type(exc).__name__}: {exc}'

    def stop(self):
        self.done.set()
        self.thread.join(timeout=5)
        require(not self.thread.is_alive() and self.error is None and (self.count > 0), 'sampled peak observer failed')

    def receipt(self):
        return {'interval_seconds': 0.2, 'samples': self.count, 'sampled_maximum_bytes': self.maximum, 'exact_peak': False, 'error': self.error, 'pid': self.pid, 'start_ticks': self.start_ticks, 'cgroup_path': str(self.cg)}

def summarize(rows):
    if rows and all(r['member']['scenario'] in (HWM_ID,HWM_STEADY_ID) for r in rows):
        return summarize_hwm(rows)
    require(len(rows) == 72 and len({r['member']['id'] for r in rows}) == 72, 'missing/duplicate final rows')
    grouped = collections.defaultdict(list)
    for r in rows:
        grouped[r['member']['scenario'], r['member']['arm']].append(r)
    comparisons = []
    misses = []
    for id in sorted({r['member']['scenario'] for r in rows}):
        arms = {arm: grouped[id, arm] for arm in ('baseline', 'candidate', 'redis')}
        require(all((len(v) == 3 for v in arms.values())), 'missing arm/round')
        median = {arm: statistics.median((r['delta_pss_bytes'] for r in v)) for arm, v in arms.items()}
        datasets = {tuple((r['logical_dataset'][k] for k in ('live_keys', 'logical_bytes', 'state_sha256'))) for v in arms.values() for r in v}
        require(len(datasets) == 1, 'arms/rounds have unequal final logical datasets')
        memory = {arm: {'total_pss_bytes': statistics.median((r['final']['pss_bytes'] for r in v)), 'total_pss_bytes_per_live_key': statistics.median((r['total_pss_bytes_per_live_key'] for r in v)), 'incremental_pss_bytes_per_live_key': statistics.median((r['incremental_pss_bytes_per_live_key'] for r in v)), 'total_pss_range_bytes': [min((r['final']['pss_bytes'] for r in v)), max((r['final']['pss_bytes'] for r in v))], 'delta_pss_range_bytes': [min((r['delta_pss_bytes'] for r in v)), max((r['delta_pss_bytes'] for r in v))], 'sampled_peak_current_median_bytes': statistics.median((r['sampled_peak']['sampled_maximum_bytes']['current'] for r in v))} for arm, v in arms.items()}
        ratios = {reference: {'total_pss_candidate_over_reference': memory['candidate']['total_pss_bytes'] / memory[reference]['total_pss_bytes'], 'delta_pss_candidate_over_reference': median['candidate'] / median[reference] if median[reference] > 0 else None} for reference in ('baseline', 'redis')}
        screen = median['candidate'] <= median['baseline']
        if not screen:
            misses.append({'scenario': id, 'screen': 'settled_delta_pss'})
        perf = {}
        for phase in [p['native']['phase'] for p in arms['baseline'][0]['phases']]:
            point = {}
            for arm, v in arms.items():
                ps = [next((p for p in r['phases'] if p['native']['phase'] == phase)) for r in v]
                point[arm] = {'pss_bytes': statistics.median((p['settled']['medians']['pss_bytes'] for p in ps))}
                for name in ('completed_records_per_sec', 'wire_commands_per_sec', 'p50_ns', 'p99_ns', 'p999_ns'):
                    point[arm][name] = statistics.median((p['native']['mutation'][name] for p in ps))
            phase_datasets = {tuple((p['native'][k] for k in ('live_keys', 'absent_keys', 'logical_bytes', 'state_sha256'))) for v in arms.values() for r in v for p in r['phases'] if p['native']['phase'] == phase}
            require(len(phase_datasets) == 1, 'phase logical states differ across arms/rounds')
            if point['baseline']['completed_records_per_sec']:
                good = point['candidate']['completed_records_per_sec'] >= 0.95 * point['baseline']['completed_records_per_sec'] and point['candidate']['p99_ns'] <= 1.05 * point['baseline']['p99_ns']
                if not good:
                    misses.append({'scenario': id, 'phase': phase, 'screen': 'native_mutation_95_105'})
            if point['candidate']['pss_bytes'] > point['baseline']['pss_bytes']:
                misses.append({'scenario': id, 'phase': phase, 'screen': 'phase_total_pss'})
            perf[phase] = point
        if arms['baseline'][0]['native_performance'] is not None:
            native = {arm: {k: statistics.median((float(r['native_performance'][k]) for r in v)) for k in ('ops_per_sec', 'p50_ns', 'p99_ns', 'p999_ns')} for arm, v in arms.items()}
            if native['candidate']['ops_per_sec'] < 0.95 * native['baseline']['ops_per_sec'] or native['candidate']['p99_ns'] > 1.05 * native['baseline']['p99_ns']:
                misses.append({'scenario': id, 'screen': 'access_95_105'})
        else:
            native = None
        comparisons.append({'scenario': id, 'observations': arms, 'logical_dataset': dict(arms['baseline'][0]['logical_dataset']), 'delta_pss_medians': median, 'memory_medians_and_ranges': memory, 'explicit_candidate_over_reference_memory_ratios': ratios, 'ratio_meaning': 'less than1 means candidate uses less memory at this exact logical dataset, not extra inserted data', 'settled_memory_screen': screen, 'phase_medians': perf, 'access_native_medians': native})
    return {'functional_success': True, 'screen_success': not misses, 'screen_misses': misses, 'comparisons': comparisons, 'classification': 'diagnostic-unreserved-closed-loop', 'cause_and_significance': 'unknown', 'aggregation': 'ratio/screens of three-run medians; median per-run p99 is not a pooled percentile; no confidence interval', 'qualification': 'external reservation and matched offered-load authority absent; diagnostic only'}

def hwm_survivor(index):
    return (index*104729+12345)%HWM_KEYS < 50000

def hwm_mix(word):
    mask = (1<<64)-1
    word = (word ^ word>>30)*0xbf58476d1ce4e5b9 & mask
    word = (word ^ word>>27)*0x94d049bb133111eb & mask
    return word ^ word>>31

def hwm_payload(index,generation):
    mask = (1<<64)-1
    rotated = ((index<<17)|(index>>(64-17))) & mask
    out = bytearray(hwm_mix(HWM_SEED ^ index).to_bytes(8,'little'))
    for word in range(1,8):
        value = HWM_SEED ^ rotated ^ (generation*0x9e3779b97f4a7c15 & mask) ^ (word*0xd1342543de82ef95 & mask)
        out.extend(hwm_mix(value).to_bytes(8,'little'))
    return bytes(out)

def hwm_payload_witnesses():
    return [{'index':i,'generation':g,'sha256':hashlib.sha256(hwm_payload(i,g)).hexdigest()}
            for i,g in ((0,0),(0,1),(17,0),(500001,2),(999999,3))]

def hwm_steady_contract(s):
    return {'warmup_seconds':3,'measured_seconds':20,'pipeline':1,'clients':16,
        'phases':[p['id'] for p in phases(s) if p['step']%3!=2],
        'access_policy':'uniform-live-key-rejection-v1','mix':'4GET-1SET-XX','verification':'exhaustive after every window'}

def hwm_uniform_rank(worker,sequence,cardinality):
    require(type(cardinality) is int and cardinality>0,'empty steady live domain')
    mask = (1<<64)-1
    threshold = ((-cardinality)&mask)%cardinality
    for retry in range(threshold+1):
        word = hwm_mix((HWM_SEED ^ (worker<<32) ^ sequence)+retry*0x9e3779b97f4a7c15 & mask)
        if word>=threshold:
            return word%cardinality
    raise RuntimeError('uniform rejection bound violated')

def hwm_steady_prefix(s,p,worker,attempts,budget=lambda:None):
    require(type(worker) is int and 0<=worker<16 and type(attempts) is int and 0<=attempts<=100000000,'steady prefix bound differs')
    live = [i for i in range(s['keys']) if p['step']%3==0 or hwm_survivor(i)]
    require(bool(live),'steady live domain empty')
    trace = hashlib.sha256()
    counts = collections.Counter()
    for sequence in range(attempts):
        if sequence%4096==0:
            budget()
        i = live[hwm_uniform_rank(worker,sequence,len(live))]
        key,n,g,kind,expiry = record(s,p,i)
        require(kind==1 and not expiry,'steady selection is not a persistent live string')
        command = (b'SET',key,hwm_payload(i,g),b'XX') if sequence%5==4 else (b'GET',key)
        command_frame(trace,command)
        counts[command[0].decode()] += 1
    return {'trace_sha256':trace.hexdigest(),'wire_commands':dict(counts)}

def hwm_mix_counts(attempts):
    sets = attempts//5
    return {'GET':attempts-sets,'SET':sets}

def validate_hwm_steady(e,s,p,pid,budget=lambda:None):
    require((e['schema'],e['event'],e['pid'],e['scenario'],e['phase'])==
            (2,'steady-complete',pid,s['id'],p['id']) and s['id']==HWM_STEADY_ID and p['step']%3!=2,'steady identity/phase differs')
    require((e['pipeline'],e['clients'],e['warmup_seconds'],e['measured_seconds'],e['errors'])==(1,16,3,20,0)
            and e['access_policy']=='uniform-live-key-rejection-v1' and e['mix']=='4GET-1SET-XX','steady workload differs')
    require(e['live_keys']==sum(p['step']%3==0 or hwm_survivor(i) for i in range(s['keys'])),'steady live cardinality differs')
    workers = e['workers']
    require(len(workers)==16 and [w['worker'] for w in workers]==list(range(16))
            and all(isinstance(w.get('rust_thread_id'),str) and len(w['rust_thread_id'])<=80 for w in workers)
            and len({w['rust_thread_id'] for w in workers})==16,'all16 steady identities missing')
    outer = hashlib.sha256(); counts = collections.Counter(); samples = 0
    for worker in workers:
        numeric = ('sequence_start','sequence_end','successful_replies','warmup_completions','warmup_straddled',
                   'late_completions_excluded','histogram_samples','last_completion_ns_from_epoch','errors')
        require(all(type(worker[k]) is int and worker[k]>=0 for k in numeric),'steady counter type differs')
        require(worker['errors']==0 and worker['error'] is None and worker['sequence_start']==0
                and worker['sequence_end']==worker['successful_replies']
                and worker['warmup_completions']>0 and worker['histogram_samples']>0
                and worker['warmup_straddled']<=1 and worker['late_completions_excluded']<=1,'steady worker missing/failed/no measured completions')
        require(worker['successful_replies']==worker['warmup_completions']+worker['histogram_samples']+worker['late_completions_excluded'],
                'steady warmup/measured/late count conservation differs')
        prefix = hwm_steady_prefix(s,p,worker['worker'],worker['sequence_end'],budget)
        require(worker['wire_commands']==prefix['wire_commands'] and worker['trace_sha256']==prefix['trace_sha256'],'steady full prefix differs')
        start = hwm_mix_counts(worker['warmup_completions']); end = hwm_mix_counts(worker['warmup_completions']+worker['histogram_samples'])
        measured = {k:end[k]-start[k] for k in ('GET','SET') if end[k]!=start[k]}
        require(worker['measured_command_counts']==measured,'steady measured command mix differs')
        counts.update(measured); samples += worker['histogram_samples']
        frame(outer,worker['trace_sha256'].encode())
    require(e['trace_sha256']==outer.hexdigest() and e['measured_command_counts']==dict(counts)
            and e['completed_requests']==samples==e['histogram_samples'],'steady aggregate differs')
    require(type(e['ops_per_sec']) in (int,float) and math.isfinite(e['ops_per_sec'])
            and math.isclose(e['ops_per_sec'],samples/20.0,rel_tol=1e-9),'steady throughput arithmetic differs')
    require(type(e['elapsed_ns_from_epoch']) is int and 23000000000<=e['elapsed_ns_from_epoch']<840000000000,'steady elapsed bound differs')
    require(e['latency_unit']=='checked-P1-request-completion'
            and e['histogram_scope']=='fully measured requests only; warmup straddles and late completions excluded','steady latency scope differs')
    require(all(type(e[k]) is int for k in ('p50_ns','p99_ns','p999_ns','maximum_ns'))
            and 0<e['p50_ns']<=e['p99_ns']<=e['p999_ns']<=e['maximum_ns']<=60000000000,'steady request histogram differs')

def hwm_cpu_intervals(before,after):
    out = {}
    for role in ('server','client','controller','parent'):
        b = before['server_scope'] if role=='server' else before['client_and_controller'][role]
        a = after['server_scope'] if role=='server' else after['client_and_controller'][role]
        require(a['path']==b['path'] and a['cpu_max']==b['cpu_max'] and a['monotonic_ns']>b['monotonic_ns'],'steady CPU scope/interval differs')
        delta = {k:int(a['cpu_stat'][k])-int(b['cpu_stat'][k]) for k in ('usage_usec','user_usec','system_usec','nr_periods','nr_throttled','throttled_usec')}
        require(all(v>=0 for v in delta.values()),'steady CPU counters reset')
        elapsed = (a['monotonic_ns']-b['monotonic_ns'])/1e9
        out[role] = {'elapsed_seconds':elapsed,'counter_deltas':delta,'actual_consumed_cores':delta['usage_usec']/1e6/elapsed,
            'cpu_max':a['cpu_max'],'cpuset_before':b['cpuset_effective'],'cpuset_after':a['cpuset_effective'],
            'reserved':False,'scope':'warmup plus measured traffic and connection/boundary overhead; not per-command CPU'}
    return out

def hwm_expected_phase(s,p,budget=lambda:None):
    state,mutation,verification = (hashlib.sha256() for _ in range(3))
    live = logical = mops = mtx = 0
    mix,vmix = collections.Counter(),collections.Counter()
    workers = {'mutation':[],'verification':[]}
    for worker in range(16):
        hs,hm,hv = (hashlib.sha256() for _ in range(3))
        mc,vc = collections.Counter(),collections.Counter()
        local_records = 0
        start,end = s['keys']*worker//16,s['keys']*(worker+1)//16
        for i in range(start,end):
            if i % 4096 == 0:
                budget()
            r = record(s,p,i)
            key,n,g,kind,expiry = r
            value = hwm_payload(i,g)
            live += int(kind!=0)
            logical += 82 if kind else 0
            for b in (key,bytes([kind]),b'\x00',value if kind else b''):
                frame(hs,b)
            changed = p['step']==0 or p['step']%3!=2 and not hwm_survivor(i)
            cmds = [(b'DEL',key)] if changed and kind==0 else [(b'SET',key,value)] if changed else []
            local_records += int(bool(cmds))
            for cmd in cmds:
                command_frame(hm,cmd)
                mc[cmd[0].decode()] += 1
            for cmd in ((b'GET',key),(b'TYPE',key),(b'PTTL',key)):
                command_frame(hv,cmd)
                vc[cmd[0].decode()] += 1
        for name,trace,st,counts,records in (('mutation',hm,hashlib.sha256(),mc,local_records),('verification',hv,hs,vc,end-start)):
            workers[name].append({'worker':worker,'range_start':start,'range_end':end,
                'wire_commands':sum(counts.values()),'completed_records':records,'histogram_samples':records,
                'trace_sha256':trace.hexdigest(),'state_sha256':st.hexdigest(),
                'command_counts':dict(counts),'errors':0,'error':None})
        frame(state,hs.hexdigest().encode())
        frame(mutation,hm.hexdigest().encode())
        frame(verification,hv.hexdigest().encode())
        mix.update(mc); vmix.update(vc)
        mops += sum(mc.values()); mtx += local_records
    return {'live_keys':live,'absent_keys':s['keys']-live,'logical_bytes':logical,'state_sha256':state.hexdigest(),
        'mutation_trace_sha256':mutation.hexdigest(),'verification_trace_sha256':verification.hexdigest(),
        'mutation_operations':mops,'verification_operations':s['keys']*3,'mutation_transactions':mtx,
        'verification_transactions':s['keys'],'mutation_command_counts':dict(mix),'verification_command_counts':dict(vmix),
        'control_commands':{'DBSIZE':1},'workers':workers}

def validate_hwm_ready(e,s,pid):
    require((e['schema'],e['event'],e['pid'],e['scenario'])==(2,'ready',pid,s),'HWM native identity differs')
    require((e['seed'],e['clients'],e['pipeline'],e['deadline_seconds'])==(HWM_SEED,16,1,840),'HWM bounds differ')
    require(e['initial_dbsize_checks']==1 and e['phases']==phases(s) and e['distribution']==distribution(s),'HWM catalog differs')
    require(e['survivor_rule']=={'multiplier':104729,'offset':12345,'modulus':HWM_KEYS,'below':50000},'survivor rule differs')
    steady = s['id']==HWM_STEADY_ID
    require(e['idle_seconds']==10 and e['classification']==('high-water-steady-lifecycle-diagnostic' if steady else 'high-water-lifecycle-subset')
            and e['steady_get_set_tested'] is steady,'HWM scope differs')
    if steady:
        require(e['steady_contract']==hwm_steady_contract(s),'HWM steady window contract differs')
    require(e['payload_witnesses']==hwm_payload_witnesses(),'Rust/Python payload correspondence differs')

def validate_hwm_phase(e,s,p,pid,w):
    require((e['schema'],e['event'],e['pid'],e['scenario'],e['phase'])==(2,'phase',pid,s['id'],p['id']),'HWM phase identity differs')
    for k,v in w.items():
        if k=='workers' or k.endswith(('_operations','_transactions')):
            continue
        require(e[k]==v,f'HWM {k} differs')
    for name in ('mutation','verification'):
        t = e[name]
        n,records = w[name+'_operations'],w[name+'_transactions']
        require(type(t['wire_commands']) is int and type(t['completed_records']) is int and (t['wire_commands'],t['completed_records'])==(n,records),'HWM counts differ')
        actual_workers = e[name+'_workers']
        require(len(actual_workers)==16 and all(isinstance(r.get('rust_thread_id'),str) and len(r['rust_thread_id'])<=80 for r in actual_workers)
                and len({r['rust_thread_id'] for r in actual_workers})==16,'all16 distinct Rust worker identities missing')
        require([{k:v for k,v in r.items() if k!='rust_thread_id'} for r in actual_workers]==w['workers'][name],
                'all16 worker counts/ranges/trace/state/errors differ')
        require(all(type(t[k]) is int and t[k]>=0 for k in ('elapsed_ns','p50_ns','p99_ns','p999_ns','maximum_ns')),'HWM timing type differs')
        if n:
            require(t['elapsed_ns']>0 and 0<t['p50_ns']<=t['p99_ns']<=t['p999_ns']<=t['maximum_ns']<=60000000000,'HWM latency bounds differ')
            require(t['latency_unit']==('logical-record-transition' if name=='mutation' else 'full-record-verification'),'HWM latency unit differs')
            for k,count in (('wire_commands_per_sec',n),('completed_records_per_sec',records)):
                require(type(t[k]) in (int,float) and math.isfinite(t[k]) and math.isclose(t[k],count*1e9/t['elapsed_ns'],rel_tol=1e-9),'HWM rate arithmetic differs')
        else:
            require(t['latency_unit']=='absent' and all(v==0 for k,v in t.items() if k!='latency_unit'),'zero-command timing differs')
    idle = e['idle_elapsed_ns']
    require(type(idle) is int and (10000000000<=idle<840000000000 if p['step']%3==2 else idle==0),'idle duration differs')

def validate_hwm_contract(p):
    require(p['schema']==2 and p['status']=='frozen with independently accepted gates','HWM operating inputs pending')
    steady = p['package']==HWM_STEADY_PACKAGE
    require(p['classification']==('high-water-steady-lifecycle-diagnostic' if steady else 'high-water-lifecycle-subset')
            and p['steady_get_set_tested'] is steady,'HWM subset attribution differs')
    require(p['members']==members(p['package']),'HWM missing/extra/reordered members')
    if steady:
        require(p['steady_contract']==hwm_steady_contract(scenario(HWM_STEADY_ID)),'HWM steady windows differ')
    require(p['budgets']==HWM_BUDGETS and p['caps']==HWM_CAPS,'HWM clocks/resources differ')
    require(set(p['images'])=={'current','redis'} and p['images']['current']['source_sha']==HWM_SOURCE and p['images']['current']['source_tree']==HWM_TREE,'HWM server source/arms differ')
    require(p['images']['current']['features']==FEATURES and p['images']['redis']['version']=='7.4.11','HWM features/reference differ')
    require(p['native_build']['argv']==HWM_BUILD_ARGV and p['native_build']['features']==[],'HWM native build differs')
    require(p['native_build']['rust_version']=='1.93.1','HWM pinned Rust version differs')
    require(p['native_build']['source_sha']==p['source']['sha'] and p['native_build']['source_tree']==p['source']['tree'],'driver build/source differs')
    require(set(p['binaries'])=={'compact_resp_scenarios'} and set(p['helpers'])=={'density','child_wait','docker','git'},'HWM artifacts/helper closure differs')
    require(p['owners']=={a:p['owner']+'-'+a for a in ('current','redis')},'HWM owners differ')
    require(p['owner'].startswith('eden2266-') and len(p['owner'])<=80 and set(p['owner'])<=set('abcdefghijklmnopqrstuvwxyz0123456789-'),'HWM owner differs')
    require(p['builds']=={'common':{'worktree':p['source']['worktree']}},'HWM target accounting differs')
    require(p['sampling']=={'idle':5,'final':5,'intermediate':5,'delay_seconds':0.2,'settle_seconds':1.0,'peak_interval_seconds':0.2},'HWM sample contract differs')
    require(set(p['resources'])=={'parent','controller','client','docker_parent','delegation_acceptance'},'HWM resource admission absent')
    require(hwm_docker_parent_path(p['resources']['docker_parent'],'systemd')==pathlib.Path(p['resources']['parent']['path']),
            'Docker slice mapping differs from admitted aggregate parent')
    require(digest(p['source']['sha'],40) and digest(p['source']['tree'],40) and p['source']['sha']!=HWM_SOURCE,'fresh benchmark driver source missing')
    require(set(p['source']['file_sha256'])==set(HWM_INPUTS),'HWM driver input closure incomplete')
    for b in p['images'].values():
        require(b['engine_image_id'].startswith('sha256:') and digest(b['engine_image_id'][7:]),'invalid Engine image identity')
    require(p['stop_path']=='/home/dtietjen/.local/state/eden-resource-guard/DO_NOT_START_NEW_ADAM_RUNS' and p['global_disk_guard']=='/home/dtietjen/.local/state/eden-resource-guard/disk-status.tsv','global resource guard paths differ')
    run = pathlib.Path(p['owned_run_root'])
    require(run.is_absolute() and str(run).startswith('/home/dtietjen/validation/EDEN-2266/'),'owned RUN missing')
    require(pathlib.Path(p['output_root']).parent==run,'output must be a fresh direct RUN child')
    return p

def hwm_guard_status(text,stop,now):
    # Same 90-second consumer predicate as the retained runtime.
    rows = text.splitlines()
    fields = rows[1].split() if len(rows)>1 else []
    require(len(fields)>=4 and fields[3]=='OK' and not stop,'global disk guard/STOP blocks launch')
    checked = datetime.datetime.fromisoformat(fields[0].replace('Z','+00:00'))
    age = (now-checked).total_seconds()
    require(0<=age<=90,'global disk guard stale/future')
    return {'checked_utc':fields[0],'status':fields[3],'age_seconds':age,'STOP':False}

def hwm_stat_matches(actual,binding,directory=True):
    require((stat.S_ISDIR(actual.st_mode) if directory else stat.S_ISREG(actual.st_mode)), 'delegated cgroup type differs')
    require((actual.st_dev,actual.st_ino,actual.st_uid,actual.st_gid,stat.S_IMODE(actual.st_mode))==
            tuple(binding[k] for k in ('device','inode','uid','gid','mode')),'delegated cgroup metadata differs')
    require(actual.st_uid==os.getuid() and actual.st_gid==os.getgid() and not actual.st_mode & 0o022,'delegation owner/write permissions differ')

def hwm_cap_values(read,memory,cpus,pids,exact=True):
    actual = {name:read(name).strip() for name in ('memory.max','memory.swap.max','cpu.max','pids.max')}
    quota,period = actual['cpu.max'].split()
    require(int(period)>0,'cgroup CPU period invalid')
    if exact:
        require(actual['memory.max']==str(memory) and actual['memory.swap.max']=='0' and actual['pids.max']==str(pids)
                and quota!='max' and int(quota)==cpus*int(period),'actual cgroup caps differ')
    else:
        require((actual['memory.max']=='max' or int(actual['memory.max'])>=memory)
                and (actual['pids.max']=='max' or int(actual['pids.max'])>=pids)
                and (quota=='max' or int(quota)>=cpus*int(period)),'ancestor is tighter than aggregate envelope')
    return actual

def hwm_boundary(binding,parent=None):
    path = pathlib.Path(binding['path'])
    require(path.is_absolute() and path!=pathlib.Path('/sys/fs/cgroup') and path.is_relative_to('/sys/fs/cgroup')
            and path.resolve(strict=True)==path and not path.is_symlink(),'delegation path not canonical cgroup')
    if parent is not None:
        require(path.parent==parent,'delegation is not a direct aggregate child')
    hwm_stat_matches(path.stat(),binding)
    return path

def hwm_docker_parent_path(name,driver):
    """Docker systemd slice names encode the complete ancestry from the root."""
    require(driver=='systemd','Docker systemd parent binding unavailable')
    require(isinstance(name,str) and len(name)<=255 and name.endswith('.slice'),
            'unsupported Docker systemd slice name')
    parts = name[:-6].split('-')
    require(all(part and set(part)<=set('abcdefghijklmnopqrstuvwxyz0123456789') for part in parts),
            'unsupported Docker systemd slice name')
    return pathlib.Path('/sys/fs/cgroup').joinpath(*('-'.join(parts[:i])+'.slice' for i in range(1,len(parts)+1)))

class HwmBaselineRunner(Runner):
    """Separate admitted resources; writes only the owned client cgroup.procs FD."""

    def verify_resources(self,empty_client=False):
        r = self.plan['resources']
        require(isinstance(r.get('delegation_acceptance'),dict)
                and isinstance(r['delegation_acceptance'].get('path'),str)
                and digest(r['delegation_acceptance'].get('sha256')),'resource admission error: writable client delegation is pending')
        admission = read_json(r['delegation_acceptance'])
        expected = {k:r[k] for k in ('parent','controller','client','docker_parent')}
        require(admission.get('status')=='independently accepted' and admission.get('writable_delegation_proven') is True
                and admission.get('resources')==expected and admission.get('caps')==HWM_CAPS
                and admission.get('source_sha')==self.plan['source']['sha']
                and admission.get('source_tree')==self.plan['source']['tree'],'resource admission absent or wrong driver')
        parent = hwm_boundary(r['parent'])
        require(parent.name==r['docker_parent'] and parent.name.startswith(self.plan['owner']), 'aggregate slice not task owned')
        require(hwm_docker_parent_path(r['docker_parent'],'systemd')==parent,
                'Docker slice mapping differs from admitted aggregate parent')
        control = hwm_boundary(r['controller'],parent)
        client = hwm_boundary(r['client'],parent)
        require(control!=client and control==self.h1.cgroup_path(os.getpid()),'controller placement differs')
        for path,memory,cpus,pids in ((parent,8589934592,6,1088),(control,2147483648,1,64),(client,2147483648,4,512)):
            hwm_cap_values(lambda name:(path/name).read_text(),memory,cpus,pids)
        for ancestor in parent.parents:
            if not ancestor.is_relative_to('/sys/fs/cgroup'):
                break
            def read_control(name):
                file = ancestor/name
                if file.exists():
                    return file.read_text()
                require(ancestor==pathlib.Path('/sys/fs/cgroup'),'missing non-root hierarchy control')
                return 'max 100000' if name=='cpu.max' else 'max'
            hwm_cap_values(read_control,8589934592,6,1088,False)
        if empty_client:
            require(not (client/'cgroup.procs').read_text().strip(),'client cgroup contains an unowned process')
        return parent,control,client

    def verify_inputs(self):
        p = self.plan
        self.verify_resources(True)
        self.h1.verify_scope(os.getpid(),p['controller_scope'],2147483648,1)
        work = pathlib.Path(p['source']['worktree'])
        require(work.resolve(strict=True)==work and not work.is_symlink(),'driver checkout not canonical')
        require(self.command(['git','-C',str(work),'rev-parse','HEAD'])==p['source']['sha']
                and self.command(['git','-C',str(work),'rev-parse','HEAD^{tree}'])==p['source']['tree']
                and not self.command(['git','-C',str(work),'status','--porcelain']),'driver HEAD/tree/clean differs')
        require(pathlib.Path(__file__).resolve()==work/'benchmarks/scripts/run-compact-storage-scenarios.py','executed controller differs')
        for relative,expected in p['source']['file_sha256'].items():
            require(sha(work/relative)==expected,'driver input bytes differ')
        for b in p['binaries'].values():
            pinned(b,executable=True)
        for key in ('regression_acceptance','native_build_acceptance','operating_acceptance'):
            receipt = read_json(p[key])
            require(receipt['status']=='independently accepted' and receipt['source_sha']==p['source']['sha']
                    and receipt['source_tree']==p['source']['tree'],'fresh driver admission missing')
        regression = read_json(p['regression_acceptance'])
        extra_native = HWM_STEADY_NATIVE_TESTS if p['package']==HWM_STEADY_PACKAGE else ()
        extra_python = HWM_STEADY_PYTHON_TESTS if p['package']==HWM_STEADY_PACKAGE else ()
        require(regression['new_native_names']==list(HWM_NATIVE_TESTS+extra_native) and regression['new_python_names']==list(HWM_PYTHON_TESTS+extra_python)
                and regression['legacy_native_tests']==21 and regression['legacy_python_tests']==40
                and regression['exit_codes']==[0,0] and regression['wait_observed'] is True
                and regression['child_reaped'] is True and regression['raw_wait_status']==0,'fresh focused/legacy regression evidence missing')
        operating = read_json(p['operating_acceptance'])
        require(operating['owner']==p['owner'] and operating['caps']==HWM_CAPS and operating['budgets']==HWM_BUDGETS
                and operating['owned_run_root']==p['owned_run_root'] and operating['output_root']==p['output_root']
                and operating['helper_sha256']=={k:v['sha256'] for k,v in p['helpers'].items()}
                and operating['docker_receipt']==p['docker_receipt'],'current operating/runtime boundary differs')
        mapping = {'driver':'systemd','parent_argument':p['resources']['docker_parent'],
                   'parent_path':str(hwm_docker_parent_path(p['resources']['docker_parent'],'systemd'))}
        require(operating.get('docker_parent_mapping')==mapping and operating.get('docker_parent_mapping_proven') is True,
                'genuine delegated kernel/Docker parent mapping admission missing')
        build = read_json(p['native_build_acceptance'])
        require(build['argv']==HWM_BUILD_ARGV and build['exit_code']==0 and build['wait_observed'] is True
                and build['child_reaped'] is True and build['raw_wait_status']==0 and build['rustflags']==[]
                and build['artifact_sha256']=={k:v['sha256'] for k,v in p['binaries'].items()}
                and build['artifact_paths']=={k:v['path'] for k,v in p['binaries'].items()}
                and build['compiler_sha256']==p['native_build']['compiler']['sha256']
                and build['rust_version']=='1.93.1','fresh native ELF/build/wait missing')
        pinned(p['native_build']['compiler'],executable=True)
        early = read_json(p['early_gate_acceptance'])
        require(early['status']=='independently accepted' and early['source_sha']==HWM_SOURCE and early['source_tree']==HWM_TREE
                and early['functional_success'] is True and early['screen_success'] is True and early['processes']==30,'source016 strict EARLY gate missing')
        equivalence = read_json(p['product_input_equivalence'])
        require(equivalence['status']=='independently accepted' and equivalence['server_source_sha']==HWM_SOURCE
                and equivalence['server_source_tree']==HWM_TREE and equivalence['driver_source_sha']==p['source']['sha']
                and equivalence['driver_source_tree']==p['source']['tree'] and equivalence['all_product_inputs_equal'] is True
                and equivalence['early_gate_sha256']==p['early_gate_acceptance']['sha256'],'complete relevant product equivalence missing')
        for arm,b in p['images'].items():
            inspected = json.loads(self.command(['docker','image','inspect',b['engine_image_id']]))
            require(len(inspected)==1 and inspected[0]['Id']==b['engine_image_id'],'cached image identity differs')
            if arm=='current':
                provenance = b['provenance']
                resolved = self.density.resolve_shardcache_build(read_json(provenance['metadata'],134217728),
                    pinned(provenance['dockerfile']).read_bytes(),pinned(provenance['export_log']).read_text(),
                    features=FEATURES,build_jobs=4,candidate_sha=HWM_SOURCE,image_tag=b['original_tag'],engine_image_id=b['engine_image_id'])
                require(resolved==b['resolved_build'],'source016 image provenance differs')
                context = read_json(provenance['source_context_acceptance'])
                require(context['status']=='independently accepted' and context['source_sha']==HWM_SOURCE
                        and context['source_tree']==HWM_TREE,'source016 image context missing')
            else:
                require(inspected[0]['RepoDigests']==b['repo_digests'] and bool(b['repo_digests']),'Redis digest closure differs')
                reference = read_json(b['reference_acceptance'])
                require(reference['status']=='independently accepted' and reference['engine_image_id']==b['engine_image_id']
                        and reference['version']=='7.4.11' and reference['repo_digests']==b['repo_digests'],'Redis reference admission differs')
        require(os.environ.get('EDEN_DENSITY_OWNER')==p['owners']['current']
                and pathlib.Path(os.environ['EDEN_DENSITY_DOCKER_RECEIPT'])==pathlib.Path(p['docker_receipt']),'adapter environment differs')
        require(self.command(['docker','info','--format','{{.CgroupDriver}}'])=='systemd','Docker systemd parent binding unavailable')

    def budget(self):
        super().budget()
        now = time.monotonic()
        if now < getattr(self,'next_hwm_budget',0):
            return
        guard = hwm_guard_status(pathlib.Path(self.plan['global_disk_guard']).read_text(),
            pathlib.Path(self.plan['stop_path']).exists(),datetime.datetime.now(datetime.timezone.utc))
        parent,control,client = self.verify_resources()
        root = pathlib.Path(self.plan['owned_run_root'])
        require(root.resolve(strict=True)==root and root.stat().st_uid==os.getuid(),'owned RUN differs')
        # Conservatively count hard links again. Targets outside RUN are separately counted
        # by the unchanged H1 gate; the operating receipt binds their aggregate footprint.
        total = 0
        for directory,dirs,files in os.walk(root,followlinks=False):
            for name in dirs+files:
                path = pathlib.Path(directory)/name
                require(not path.is_symlink(),'owned RUN symlink')
                if path.is_file():
                    total += path.stat().st_size
        target = pathlib.Path(self.plan['source']['worktree'])/'target'
        if not target.is_relative_to(root):
            total += self.h1.TARGET_BYTES[target]
        require(total<32212254720,'whole RUN plus driver target exceeds 30GiB')
        self.last_resource_observation = {'guard':guard,'owned_bytes':total,'monotonic':now,
            'parent':str(parent),'controller':str(control),'client':str(client)}
        self.next_hwm_budget = time.monotonic()+5

    def command(self,argv,timeout=30):
        if argv[:2] in (['docker','run'],['docker','create']):
            parent,_,_ = self.verify_resources(True)
            require(not any(a in ('--cgroup-parent','--pids-limit') or a.startswith(('--cgroup-parent=','--pids-limit=')) for a in argv),'unexpected preexisting Docker placement')
            driver = super().command(['docker','info','--format','{{.CgroupDriver}}'],timeout)
            require(hwm_docker_parent_path(self.plan['resources']['docker_parent'],driver)==parent,
                    'Docker slice mapping differs from admitted aggregate parent')
            argv = argv[:2]+['--pids-limit','512','--cgroup-parent',self.plan['resources']['docker_parent']]+argv[2:]
        return super().command(argv,timeout)

    def terminal_elapsed(self):
        # Row cleanup and terminal hashes/reporting never extend successful work.
        # Recheck after budget's observations too; owned failure cleanup remains separate.
        self.budget()
        elapsed = time.monotonic()-self.started
        require(elapsed<5400,'controller deadline before terminal success')
        return elapsed

    def publish_result(self,result):
        # The candidate is evidence only. Include serialization/fsync in the
        # original work clock before a create-only atomic terminal publication.
        candidate = self.root/'result-candidate.json'
        result = dict(result,elapsed_seconds=self.terminal_elapsed(),
            elapsed_seconds_observation='before candidate serialization; terminal publication requires a post-fsync clock check')
        write(candidate,result)
        self.terminal_elapsed()
        os.link(candidate,self.root/'result.json',follow_symlinks=False)

    def snapshot(self,cid,pid,user,cg,port):
        parent,_,_ = self.verify_resources()
        require(cg.parent==parent,'Docker server is outside admitted aggregate parent')
        hwm_cap_values(lambda name:(cg/name).read_text(),4294967296,1,512)
        sample = super().snapshot(cid,pid,user,cg,port)
        sample['resource_scope'] = self.scope_sample(cg)
        return sample

    def scope_sample(self,path):
        return {'path':str(path),'cpu_stat':dict(line.split() for line in (path/'cpu.stat').read_text().splitlines()),
            'cpu_max':(path/'cpu.max').read_text().strip(),'cpuset_effective':(path/'cpuset.cpus.effective').read_text().strip(),
            'memory_current':int((path/'memory.current').read_text()),'memory_peak_since_cgroup_creation':int((path/'memory.peak').read_text()),
            'pids_current':int((path/'pids.current').read_text()),'pids_max':(path/'pids.max').read_text().strip(),
            'monotonic_ns':time.monotonic_ns()}

    def client_started(self,waiter,folder):
        require(self.h1.cgroup_path(waiter.pid)==pathlib.Path(self.plan['resources']['client']['path']),'native left admitted client cgroup')
        write(folder/'client-ready.json',self.client_snapshot(waiter))

    def steady_snapshot(self,waiter,cid,pid,user,cg,port):
        self.budget()
        memory = self.snapshot(cid,pid,user,cg,port)
        return {'server_pid1_memory':memory,'server_scope':self.scope_sample(cg),
                'client_and_controller':self.client_snapshot(waiter)}

    def client_snapshot(self,waiter):
        parent,control,client = self.verify_resources()
        executable = pinned(self.plan['binaries']['compact_resp_scenarios'],executable=True)
        identity = self.h1.proc_identity(waiter.pid,executable)
        require(identity['start_ticks']==waiter.identity['start_ticks'] and self.h1.cgroup_path(waiter.pid)==client,'native client identity/placement changed')
        def memory(pid):
            fields = dict(line.split(':',1) for line in pathlib.Path(f'/proc/{pid}/smaps_rollup').read_text().splitlines() if ':' in line)
            return {k:int(fields[k].split()[0])*1024 for k in ('Pss','Rss','Private_Clean','Private_Dirty')}
        result = {'identity':identity,'client_process_bytes':memory(waiter.pid),'controller_process_bytes':memory(os.getpid()),
            'client':self.scope_sample(client),'controller':self.scope_sample(control),'parent':self.scope_sample(parent)}
        after = self.h1.proc_identity(waiter.pid,executable)
        require(all(after[k]==identity[k] for k in ('pid','start_ticks','exe','uid','boot_id','user_namespace'))
                and self.h1.cgroup_path(waiter.pid)==client,'client identity changed during sample')
        return result

    def open_client_procs(self):
        _,_,client = self.verify_resources(True)
        binding = self.plan['resources']['client']
        directory = os.open(client,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
        try:
            hwm_stat_matches(os.fstat(directory),binding)
            fd = os.open('cgroup.procs',os.O_WRONLY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=directory)
            try:
                hwm_stat_matches(os.fstat(fd),binding['procs'],False)
                require(os.fstat(fd).st_mode & stat.S_IWUSR,'no writable client delegation')
                return fd
            except BaseException:
                os.close(fd)
                raise
        finally:
            os.close(directory)

    def placement_confirmed(self,waiter):
        self.verify_resources()
        require(waiter.poll() is None and self.h1.child_wait_identity(waiter.pid)==waiter.identity,'placed child no longer directly owned')
        client = pathlib.Path(self.plan['resources']['client']['path'])
        require(self.h1.cgroup_path(waiter.pid)==client and (client/'cgroup.procs').read_text().split()==[str(waiter.pid)],'partial/wrong client placement')
        require(os.getpgid(waiter.pid)==waiter.pid and os.getsid(waiter.pid)==waiter.pid,'child session handshake incomplete')

    def spawn(self,argv,folder,interactive=False):
        if argv[0]!=self.plan['binaries']['compact_resp_scenarios']['path']:
            return super().spawn(argv,folder,interactive)
        require(interactive and len(self.children)<2,'native placement requires owned interactive child')
        fds = [-1]*9
        waiter = None
        try:
            with self.h1.block_termination() as mask:
                fds[8] = self.open_client_procs()
                fds[0],fds[1] = os.pipe()  # native stdout
                fds[2],fds[3] = os.pipe()  # native acknowledgement
                fds[4],fds[5] = os.pipe()  # placement ready
                fds[6],fds[7] = os.pipe()  # parent permits exec
                pid = os.fork()
                if pid==0:
                    try:
                        os.setsid()
                        for i in (0,3,4,7):
                            os.close(fds[i])
                        message = f'{os.getpid()}\n'.encode()
                        if os.write(fds[8],message)!=len(message):
                            os._exit(126)
                        os.close(fds[8])
                        if os.write(fds[5],b'placed\n')!=7:
                            os._exit(126)
                        os.close(fds[5])
                        if os.read(fds[6],5)!=b'exec\n':
                            os._exit(126)
                        os.close(fds[6])
                        os.dup2(fds[2],0); os.dup2(fds[1],1)
                        stderr = os.open(folder/'stderr.log',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
                        os.dup2(stderr,2)
                        for fd in (fds[2],fds[1],stderr):
                            os.close(fd)
                        signal.pthread_sigmask(signal.SIG_SETMASK,mask)
                        os.execv(argv[0],argv)
                    except BaseException:
                        os._exit(127)
                waiter = self.h1.NativeChildWait(pid)
                self.children[pid] = waiter
                waiter.bind()  # still masked; own direct child before any pending signal
            for i in (1,2,5,6,8):
                self.close_owned_fd(fds,i)
            until = min(time.monotonic()+10,self.row_deadline or float('inf'))
            placed = b''
            while b'\n' not in placed:
                self.budget()
                require(waiter.poll() is None and time.monotonic()<until,'placement handshake failed/expired')
                if select.select([fds[4]],[],[],0.05)[0]:
                    data = os.read(fds[4],16)
                    require(bool(data),'placement handshake EOF')
                    placed += data
                    require(len(placed)<=7,'placement handshake extra bytes')
            require(placed==b'placed\n','placement handshake differs')
            self.placement_confirmed(waiter)
            write(folder/'placement.json',{'identity':waiter.identity,'client':self.plan['resources']['client'],
                'handshake':'placement-before-parent-check-before-exec','native_pid_is_direct_child':True})
            require(os.write(fds[7],b'exec\n')==5,'partial exec acknowledgement')
            for i in (4,7):
                self.close_owned_fd(fds,i)
            write(folder/'child.json',{'argv':argv,'identity':waiter.identity})
            return waiter,fds[0],fds[3]
        except BaseException:
            self.cleanup_preserving(waiter,folder,fds)
            raise

def summarize_hwm(rows):
    steady = rows[0]['member']['scenario']==HWM_STEADY_ID
    package = HWM_STEADY_PACKAGE if steady else HWM_PACKAGE
    require([r['member'] for r in rows]==members(package),'HWM incomplete/reordered rows')
    by_arm = {a:[r for r in rows if r['member']['arm']==a] for a in ('current','redis')}
    comparison = {}
    for phase in phases(scenario(HWM_STEADY_ID if steady else HWM_ID)):
        selected = {a:[next(p for p in r['phases'] if p['native']['phase']==phase['id']) for r in v] for a,v in by_arm.items()}
        states = {tuple(p['native'][k] for k in ('live_keys','absent_keys','logical_bytes','state_sha256')) for v in selected.values() for p in v}
        require(len(states)==1,'HWM matched logical states differ')
        comparison[phase['id']] = {a:{'total_pss_median_bytes':statistics.median(p['settled']['medians']['pss_bytes'] for p in v),
            'mutation_records_per_sec_median':statistics.median(p['native']['mutation']['completed_records_per_sec'] for p in v),
            'mutation_wire_commands_per_sec_median':statistics.median(p['native']['mutation']['wire_commands_per_sec'] for p in v),
            'record_transition_p99_median_ns':statistics.median(p['native']['mutation']['p99_ns'] for p in v),
            'full_observations':v} for a,v in selected.items()}
        if steady and phase['step']%3!=2:
            for arm,observations in selected.items():
                require(all(p['steady'] is not None for p in observations),'required steady window missing')
                comparison[phase['id']][arm]['steady'] = {
                    'ops_per_sec_median':statistics.median(p['steady']['native']['ops_per_sec'] for p in observations),
                    'P1_request_p99_median_ns':statistics.median(p['steady']['native']['p99_ns'] for p in observations),
                    'cpu_boundary_scope':'warmup + measured + connection/boundary overhead; no exhaustive reads',
                    'actual_server_cores_median':statistics.median(p['steady']['cpu_intervals']['server']['actual_consumed_cores'] for p in observations),
                    'actual_client_cores_median':statistics.median(p['steady']['cpu_intervals']['client']['actual_consumed_cores'] for p in observations),
                    'full_observations':[p['steady'] for p in observations]}
    return {'functional_success':True,'classification':'high-water-steady-lifecycle-diagnostic' if steady else 'high-water-lifecycle-subset','steady_get_set_tested':steady,
        'optimized_variant':None,'source_reference':HWM_SOURCE,'rows':rows,'phase_comparisons':comparison,
        'screen_success':None,'qualification':'baseline diagnostic observations only; no optimization, fixed offered load, or inserted capacity result',
        'aggregation':'three-run medians; p99 medians not pooled percentiles; no confidence interval'}

def hwm_plan_template(steady=False):
    """Concrete schema producer, not an admission or a source/ELF receipt builder."""
    binding = lambda:{'path':None,'sha256':None}
    metadata = lambda:{'path':None,'device':None,'inode':None,'uid':None,'gid':None,'mode':None}
    client = metadata()
    client['procs'] = {k:None for k in ('device','inode','uid','gid','mode')}
    package = HWM_STEADY_PACKAGE if steady else HWM_PACKAGE
    plan = {'schema':2,'status':'planned; runtime and delegation admissions pending',
        'classification':'high-water-steady-lifecycle-diagnostic' if steady else 'high-water-lifecycle-subset','steady_get_set_tested':steady,
        'package':package,'members':members(package),'budgets':dict(HWM_BUDGETS),'caps':dict(HWM_CAPS),
        'source':{'sha':None,'tree':None,'worktree':None,'file_sha256':dict.fromkeys(HWM_INPUTS)},
        'images':{'current':{'source_sha':HWM_SOURCE,'source_tree':HWM_TREE,'features':FEATURES,
            'engine_image_id':None,'original_tag':None,'resolved_build':None,
            'provenance':{k:binding() for k in ('metadata','dockerfile','export_log','source_context_acceptance')}},
            'redis':{'version':'7.4.11','engine_image_id':None,'repo_digests':None,'reference_acceptance':binding()}},
        'native_build':{'argv':list(HWM_BUILD_ARGV),'features':[],'source_sha':None,'source_tree':None,'rust_version':'1.93.1','compiler':binding()},
        'binaries':{'compact_resp_scenarios':binding()},'helpers':{k:binding() for k in ('density','child_wait','docker','git')},
        'owner':None,'owners':{'current':None,'redis':None},'builds':{'common':{'worktree':None}},
        'sampling':{'idle':5,'final':5,'intermediate':5,'delay_seconds':0.2,'settle_seconds':1.0,'peak_interval_seconds':0.2},
        'resources':{'parent':metadata(),'controller':metadata(),'client':client,'docker_parent':None,'delegation_acceptance':binding()},
        'controller_scope':None,'owned_run_root':None,'output_root':None,'docker_receipt':None,
        'stop_path':'/home/dtietjen/.local/state/eden-resource-guard/DO_NOT_START_NEW_ADAM_RUNS',
        'global_disk_guard':'/home/dtietjen/.local/state/eden-resource-guard/disk-status.tsv',
        **{k:binding() for k in ('regression_acceptance','native_build_acceptance','operating_acceptance',
                               'early_gate_acceptance','product_input_equivalence')},
        'actual_measurements':None,'qualified_native_elf':None}
    if steady:
        plan['steady_contract'] = hwm_steady_contract(scenario(HWM_STEADY_ID))
    return plan

def interrupted(signum, _frame):
    raise RuntimeError(f'controller interrupted by signal {signum}')

def main():
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, interrupted)
    a = argparse.ArgumentParser()
    a.add_argument('--plan', type=pathlib.Path)
    a.add_argument('--plan-sha256')
    a.add_argument('--write-hwm-baseline-template',type=pathlib.Path,
                   help='Write the finite six-row schema with NULL runtime inputs; no helper imports or effects')
    a.add_argument('--hwm-profile',choices=('lifecycle','steady'),default='lifecycle',help='Template profile only; runtime profile is fixed by the frozen plan')
    args = a.parse_args()
    if args.write_hwm_baseline_template:
        require(args.plan is None and args.plan_sha256 is None,'template mode cannot accept runtime plan')
        write(args.write_hwm_baseline_template,hwm_plan_template(args.hwm_profile=='steady'))
        return 0
    require(args.plan is not None and args.plan_sha256 is not None,'--plan and --plan-sha256 are required for execution')
    plan = validate_contract(read_json({'path': str(args.plan), 'sha256': args.plan_sha256}))
    root = pathlib.Path(plan['output_root'])
    require(root.is_absolute() and (not root.exists()) and (root.parent.resolve() == root.parent), 'fresh canonical output root required')
    root.mkdir(mode=448)
    (root / 'commands').mkdir()
    resource.setrlimit(resource.RLIMIT_FSIZE, (134217728, 134217728))
    runner = None
    try:
        runner = HwmBaselineRunner(plan,root) if plan['package'] in HWM_PACKAGES else Runner(plan,root)
        runner.verify_inputs()
        runner.budget()
        contracts = {}
        for id in {m['scenario'] for m in plan['members']}:
            s = scenario(id, plan['package'] == 'access')
            contracts[id] = {p['id']: hwm_expected_phase(s,p,runner.budget) if s['family']=='hwm' else expected_phase(s, p) for p in phases(s)}
        write(root / 'expected-state-contracts.json', contracts)
        rows = []
        for m in plan['members']:
            rows.append(runner.row(m, contracts[m['scenario']]))
            write(root / f'progress-{len(rows):02d}.json', {'complete': False, 'rows': len(rows), 'last_member': m})
        for b in plan['binaries'].values():
            pinned(b, executable=True)
        for b in plan['helpers'].values():
            pinned(b)
        require(sha(args.plan) == args.plan_sha256, 'plan changed during effects')
        for relative, expected in plan['source']['file_sha256'].items():
            require(sha(pathlib.Path(plan['source']['worktree']) / relative) == expected, 'bound source changed during effects')
        report = summarize(rows)
        write(root / 'summary.json', report)
        result_writer = runner.publish_result if plan['package'] in HWM_PACKAGES else functools.partial(write,root/'result.json')
        result_writer({'success': True, 'rows': 6 if plan['package'] in HWM_PACKAGES else 72,
            'minimum_idle_final_gates':60 if plan['package'] in HWM_PACKAGES else 720,
            'phase_gates': sum((len(r['phases']) * 5 for r in rows)), 'screen_success': report['screen_success'], 'elapsed_seconds': time.monotonic() - runner.started})
        return 0
    except BaseException as exc:
        cleanup = None
        errors = []
        if runner:
            with runner.h1.defer_cleanup_termination() as received:
                try:
                    cleanup = runner.cleanup_child()
                except BaseException as ce:
                    errors.append(f'{type(ce).__name__}: {ce}')
        write(root / 'failure.json', {'success': False, 'error': f'{type(exc).__name__}: {exc}', 'cleanup': cleanup, 'cleanup_errors': errors + (runner.cleanup_errors if runner else [])})
        return 1
if __name__ == '__main__':
    sys.exit(main())
