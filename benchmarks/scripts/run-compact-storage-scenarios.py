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
    require(package in ('stateful-a', 'stateful-b', 'stateful-c', 'access'), 'unknown bounded package')
    ids = ACCESS if package == 'access' else IDS[(ord(package[-1]) - 97) * 8:(ord(package[-1]) - 96) * 8]
    out = []
    for round, arms in enumerate((('baseline', 'candidate', 'redis'), ('candidate', 'redis', 'baseline'), ('redis', 'baseline', 'candidate')), 1):
        for arm in arms:
            out.extend(({'id': f'r{round}-{arm}-{id}', 'round': round, 'arm': arm, 'scenario': id} for id in ids))
    return out

def validate_contract(p):
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
    require(e['schema'] == 1 and e['event'] == 'ready' and (e['pid'] == pid) and (e['scenario'] == s), 'native ready identity differs')
    require(e['seed'] == SEED and e['clients'] == 16 and (e['pipeline'] == 1) and (e['deadline_seconds'] == 900), 'native workload bounds differ')
    require(e['initial_dbsize_checks'] == int(not s['saturation']), 'initial DBsize check differs')
    require(e['phases'] == phases(s) and e['distribution'] == distribution(s), 'native phase/distribution differs')

def validate_phase(e, s, p, pid, witness):
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
        read, stdout = os.pipe()
        stdin, writefd = os.pipe()
        pid = None
        waiter = None
        try:
            with self.h1.block_termination() as mask:
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
                waiter.bind()
                self.children[pid] = waiter
            os.close(stdin)
            stdin = -1
            os.close(stdout)
            stdout = -1
            if not interactive:
                os.close(writefd)
                writefd = -1
            write(folder / 'child.json', {'argv': argv, 'identity': waiter.identity})
            return (waiter, read, writefd)
        except BaseException:
            if waiter:
                self.h1.stop_child(waiter)
            for fd in (read, stdout, stdin, writefd):
                if fd >= 0:
                    os.close(fd)
            self.children.pop(pid, None)
            raise

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

    def cleanup_preserving(self, waiter, folder):
        if waiter.pid not in self.children:
            return
        original_error = sys.exc_info()[0] is not None
        try:
            self.cleanup_child(waiter)
        except BaseException as exc:
            message = f'{type(exc).__name__}: {exc}'
            self.cleanup_errors.append(message)
            write(folder / 'cleanup-failure.json', {'cleanup_error': message, 'original_exception_in_flight': original_error, 'identity': waiter.identity, 'wait': waiter.wait})
            if not original_error:
                raise

    def command(self, argv, timeout=30):
        require(argv[0] in ('docker', 'git'), 'unapproved helper command')
        argv = [str(pinned(self.plan['helpers'][argv[0]], executable=True)), *argv[1:]]
        folder = self.root / 'commands' / f"{len(list((self.root / 'commands').iterdir())):06d}"
        folder.mkdir()
        waiter, fd, _ = self.spawn(argv, folder)
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
            os.close(fd)
            self.cleanup_preserving(waiter, folder)

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
        require(regression['native_unit_tests'] == 21 and regression['python_tests'] == 30 and (regression['exit_codes'] == [0, 0]) and (regression['child_reaped'] is True), 'focused native/Python regression admission differs')
        early = read_json(p['early_gate_acceptance'])
        require(early['functional_success'] is True and early['screen_success'] is True and (early['processes'] == 30), 'early API gate did not pass')
        receipt = read_json(p['native_build_acceptance'])
        require(receipt['argv'] == BUILD_ARGV and receipt['exit_code'] == 0 and receipt['child_reaped'] and (receipt['artifact_sha256'] == {k: v['sha256'] for k, v in p['binaries'].items()}), 'native build/compiler/exit receipt differs')
        require(receipt['compiler_sha256'] == p['native_build']['compiler']['sha256'] and receipt['artifact_paths'] == {k: v['path'] for k, v in p['binaries'].items()} and (receipt['rustflags'] == []) and (receipt['wait_observed'] is True) and (receipt['raw_wait_status'] == 0), 'native compiler/actual artifact/flags/wait closure differs')
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
        buffer = b''
        events = []
        until = min(time.monotonic() + 900, self.row_deadline or float('inf'))
        try:
            for phase in [None, *phases(s), 'complete']:
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
                if phase is None:
                    validate_ready(e, s, waiter.pid)
                    identity = self.h1.proc_identity(waiter.pid, pathlib.Path(argv[0]))
                    require(identity['start_ticks'] == waiter.identity['start_ticks'], 'native fork/executed ELF identity differs')
                    write(folder / 'ready.json', dict(e, executed_identity=identity))
                elif phase == 'complete':
                    require(e == {'schema': 1, 'event': 'complete', 'pid': waiter.pid, 'scenario': s['id'], 'phases': len(phases(s)), 'errors': 0}, 'native terminal event differs')
                    break
                else:
                    validate_phase(e, s, phase, waiter.pid, witnesses[phase['id']])
                    dbsize = int(self.density.redis_command(port, b'DBSIZE'))
                    require(dbsize == e['live_keys'], 'post-phase DBSIZE differs')
                    samples = self.snapshots(cid, pid, user, cg, port)
                    receipt = {'native': e, 'settled': samples, 'controller_dbsize': {'checks': 1, 'actual': dbsize, 'expected': e['live_keys']}}
                    write(folder / (phase['id'] + '.json'), receipt)
                    events.append(receipt)
                os.write(ack, b'continue\n')
            os.close(ack)
            ack = -1
            wait = self.wait(waiter, until)
            require(wait['exit_code'] == 0 and (not buffer) and (not os.read(fd, 1)), 'native nonzero/extra terminal output')
            write(folder / 'wait.json', wait)
            self.children.pop(waiter.pid, None)
            return events
        finally:
            for n in (fd, ack):
                if n >= 0:
                    os.close(n)
            self.cleanup_preserving(waiter, folder)

    def row(self, m, witnesses):
        self.budget()
        row_started = time.monotonic()
        self.row_deadline = row_started + 900
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
            os.close(fd)
            self.cleanup_preserving(waiter, out)

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

def interrupted(signum, _frame):
    raise RuntimeError(f'controller interrupted by signal {signum}')

def main():
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, interrupted)
    a = argparse.ArgumentParser()
    a.add_argument('--plan', type=pathlib.Path, required=True)
    a.add_argument('--plan-sha256', required=True)
    args = a.parse_args()
    plan = validate_contract(read_json({'path': str(args.plan), 'sha256': args.plan_sha256}))
    root = pathlib.Path(plan['output_root'])
    require(root.is_absolute() and (not root.exists()) and (root.parent.resolve() == root.parent), 'fresh canonical output root required')
    root.mkdir(mode=448)
    (root / 'commands').mkdir()
    resource.setrlimit(resource.RLIMIT_FSIZE, (134217728, 134217728))
    runner = None
    try:
        runner = Runner(plan, root)
        runner.verify_inputs()
        runner.budget()
        contracts = {}
        for id in {m['scenario'] for m in plan['members']}:
            s = scenario(id, plan['package'] == 'access')
            contracts[id] = {p['id']: expected_phase(s, p) for p in phases(s)}
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
        write(root / 'result.json', {'success': True, 'rows': 72, 'minimum_idle_final_gates': 720, 'phase_gates': sum((len(r['phases']) * 5 for r in rows)), 'screen_success': report['screen_success'], 'elapsed_seconds': time.monotonic() - runner.started})
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
