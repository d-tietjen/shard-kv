#!/usr/bin/env python3
"""Adam-only HWM fault probes; skipped prerequisites are never qualification.

The loopback peer is deliberately faulty and provides no product/data credit.
The publisher probe exercises a terminal function with a test start timestamp.
Kernel placement requires a real independently accepted delegation receipt.
No probe creates acceptance, controls a Docker server, or runs a benchmark row.
"""
import contextlib
import copy
import importlib.util
import json
import os
import pathlib
import resource
import signal
import socket
import struct
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('hwm_fault_driver', ROOT / 'run-compact-storage-scenarios.py')
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)
h1 = gate.import_file('hwm_fault_h1', ROOT / 'run-compact-shared-read-gate.py')
CLASSIFICATION = 'release fault probe only; no benchmark, product, image or operating admission'


def evidence_root():
    value = os.environ.get('EDEN_HWM_RELEASE_FAULT_OUTPUT')
    if not value:
        raise unittest.SkipTest('real Adam owned output root is required; no qualification')
    root = pathlib.Path(value)
    gate.require(root.is_absolute() and root.resolve(strict=True) == root and not root.is_symlink(), 'noncanonical probe output')
    item = root.stat()
    gate.require(item.st_uid == os.getuid() and item.st_gid == os.getgid() and item.st_mode & 0o777 == 0o700, 'probe output ownership/mode differs')
    return root


def case_folder(name):
    return pathlib.Path(tempfile.mkdtemp(prefix=name + '-', dir=evidence_root()))


def direct_runner():
    runner = gate.Runner.__new__(gate.Runner)
    runner.h1 = h1
    runner.children = {}
    runner.cleanup_errors = []
    runner.row_deadline = time.monotonic() + 30
    return runner


def retain_wait(runner, waiter, folder):
    wait = runner.wait(waiter, time.monotonic() + 25)
    gate.require(wait['wait_observed'] is True and wait['child_reaped'] is True
                 and wait['waited_pid'] == waiter.pid and type(wait['raw_wait_status']) is int,
                 'probe has no genuine direct child wait')
    gate.write(folder / 'probe-wait.json', dict(wait, identity=waiter.identity, classification=CLASSIFICATION))
    runner.children.pop(waiter.pid)
    return wait


def publication_probe(folder, fault):
    """Child entry point; faults affect the actual writer or kernel file limit."""
    gate.require(fault in ('none', 'existing-result', 'candidate-symlink', 'terminal-symlink',
                           'partial-file', 'fsync-signal', 'clock-edge'), 'unknown finite publication fault')
    gate.require(folder.resolve(strict=True) == folder and not folder.is_symlink()
                 and (folder.stat().st_uid, folder.stat().st_gid, folder.stat().st_mode & 0o777)
                 == (os.getuid(), os.getgid(), 0o700), 'publication probe folder differs')
    runner = gate.HwmBaselineRunner.__new__(gate.HwmBaselineRunner)
    runner.root = folder
    runner.started = time.monotonic() - (5399.9 if fault == 'clock-edge' else 1)
    runner.budget = lambda: None  # Terminal-function probe only, no operating admission.
    sentinel = b'preserved-existing-evidence'
    candidate = folder / 'result-candidate.json'
    terminal = folder / 'result.json'
    if fault == 'existing-result':
        terminal.write_bytes(sentinel)
    if fault in ('candidate-symlink', 'terminal-symlink'):
        target = folder / 'symlink-target'
        target.write_bytes(sentinel)
        (candidate if fault == 'candidate-symlink' else terminal).symlink_to(target)
    previous_limits = resource.getrlimit(resource.RLIMIT_FSIZE)
    previous_xfsz = signal.getsignal(signal.SIGXFSZ)
    status = 1
    observation = {'success': False, 'fault': fault, 'classification': CLASSIFICATION}
    try:
        signal.signal(signal.SIGTERM, gate.interrupted)
        if fault == 'partial-file':
            signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
            resource.setrlimit(resource.RLIMIT_FSIZE, (512, previous_limits[1]))
        actual_fsync = os.fsync
        def fsync(fd):
            actual_fsync(fd)
            if fault == 'fsync-signal':
                os.kill(os.getpid(), signal.SIGTERM)
            elif fault == 'clock-edge':
                while time.monotonic() - runner.started < 5400:
                    time.sleep(0.001)
        with mock.patch.object(gate.os, 'fsync', side_effect=fsync):
            runner.publish_result({'success': True, 'probe_only': True,
                                   'payload': 'x' * (16384 if fault == 'partial-file' else 1)})
        status = 0
        observation['success'] = True
    except BaseException as error:
        observation['error'] = f'{type(error).__name__}: {error}'
    finally:
        resource.setrlimit(resource.RLIMIT_FSIZE, previous_limits)
        signal.signal(signal.SIGXFSZ, previous_xfsz)
    gate.write(folder / 'probe-result.json', observation)
    return status


@unittest.skipUnless(sys.platform == 'linux' and hasattr(os, 'fork'), 'actual Linux H1 wait required; no qualification')
class HwmPublicationFaultTests(unittest.TestCase):
    def test_actual_publication_faults_preserve_terminal_and_raw_wait(self):
        for fault in ('none', 'existing-result', 'candidate-symlink', 'terminal-symlink',
                      'partial-file', 'fsync-signal', 'clock-edge'):
            with self.subTest(fault=fault):
                folder = case_folder('publish-' + fault)
                runner = direct_runner()
                waiter = None
                fds = []
                try:
                    waiter, stdout, _ = runner.spawn([str(pathlib.Path(sys.executable).resolve()), '-B',
                        str(pathlib.Path(__file__).resolve()), '--publication-probe', str(folder), fault], folder)
                    fds = [stdout]
                    wait = retain_wait(runner, waiter, folder)
                    result = json.loads((folder / 'probe-result.json').read_text())
                    self.assertEqual(wait['exit_code'], int(fault != 'none'))
                    self.assertEqual(result['success'], fault == 'none')
                    terminal = folder / 'result.json'
                    candidate = folder / 'result-candidate.json'
                    if fault == 'none':
                        self.assertEqual(terminal.read_bytes(), candidate.read_bytes())
                        self.assertEqual(terminal.stat().st_ino, candidate.stat().st_ino)
                    elif fault == 'existing-result':
                        self.assertEqual(terminal.read_bytes(), b'preserved-existing-evidence')
                    elif fault == 'terminal-symlink':
                        self.assertTrue(terminal.is_symlink())
                        self.assertEqual((folder / 'symlink-target').read_bytes(), b'preserved-existing-evidence')
                    else:
                        self.assertFalse(terminal.exists() or terminal.is_symlink())
                    if fault == 'candidate-symlink':
                        self.assertEqual((folder / 'symlink-target').read_bytes(), b'preserved-existing-evidence')
                    if fault == 'partial-file':
                        self.assertGreater(candidate.stat().st_size, 0)
                        self.assertLessEqual(candidate.stat().st_size, 512)
                        with self.assertRaises(json.JSONDecodeError):
                            json.loads(candidate.read_text())
                finally:
                    runner.cleanup_preserving(waiter, folder, fds)


def read_fault_request(stream):
    """Independent bounded observation of this test peer's one RESP request."""
    file = stream.makefile('rb')
    header = file.readline(128)
    gate.require(header.startswith(b'*') and header.endswith(b'\r\n'), 'fault request array differs')
    count = int(header[1:-2])
    gate.require(1 <= count <= 4, 'fault request part count differs')
    parts = []
    for _ in range(count):
        header = file.readline(128)
        gate.require(header.startswith(b'$') and header.endswith(b'\r\n'), 'fault request bulk header differs')
        size = int(header[1:-2])
        gate.require(0 <= size <= 8192, 'fault request size differs')
        data = file.read(size + 2)
        gate.require(len(data) == size + 2 and data[-2:] == b'\r\n', 'fault request incomplete')
        parts.append(data[:-2])
    file.close()
    return parts


@unittest.skipUnless(sys.platform == 'linux' and hasattr(os, 'fork'), 'actual Linux H1 wait required; no qualification')
class HwmNativeTransportFaultTests(unittest.TestCase):
    def test_actual_native_all_workers_fail_closed_on_faulty_peer(self):
        path = os.environ.get('EDEN_HWM_RELEASE_NATIVE')
        digest = os.environ.get('EDEN_HWM_RELEASE_NATIVE_SHA256')
        if not path or not digest:
            self.skipTest('genuine exact native ELF binding is required; no qualification')
        executable = gate.pinned({'path': path, 'sha256': digest}, executable=True)
        for scenario in (gate.HWM_ID, gate.HWM_STEADY_ID):
            for fault, response in (('truncated', b'$64\r\npartial'), ('wrongtype', b'-WRONGTYPE injected\r\n'), ('reset', None)):
                with self.subTest(scenario=scenario, fault=fault):
                    folder = case_folder('wire-' + fault)
                    listener = socket.socket()
                    listener.bind(('127.0.0.1', 0)); listener.listen(17); listener.settimeout(5)
                    address = listener.getsockname()
                    observations, errors = [], []
                    def peer():
                        try:
                            for slot in range(17):
                                stream, _ = listener.accept()
                                with stream:
                                    stream.settimeout(5)
                                    parts = read_fault_request(stream)
                                    if slot == 0:
                                        gate.require(parts == [b'DBSIZE'], 'first native control differs')
                                        stream.sendall(b':0\r\n')
                                    else:
                                        gate.require(len(parts) == 3 and parts[0] == b'SET'
                                                     and len(parts[1]) == 18 and len(parts[2]) == 64,
                                                     'fault missed HWM fill mutation')
                                        index = int.from_bytes(parts[1][-8:], 'big')
                                        gate.require(parts[1] == b'k' * 10 + index.to_bytes(8, 'big')
                                                     and parts[2] == gate.hwm_payload(index, 0), 'fault fill input differs')
                                        observations.append(index)
                                        if response is None:
                                            stream.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                                        else:
                                            stream.sendall(response)
                        except BaseException as error:
                            errors.append(f'{type(error).__name__}: {error}')
                    thread = threading.Thread(target=peer, daemon=True)
                    thread.start()
                    runner = direct_runner(); waiter = None; fds = []
                    raw = bytearray()
                    try:
                        waiter, stdout, ack = runner.spawn([str(executable), '--scenario', scenario,
                            '--addr', f'127.0.0.1:{address[1]}'], folder, True)
                        fds = [stdout, ack]
                        until = time.monotonic() + 20
                        while b'\n' not in raw:
                            gate.require(time.monotonic() < until, 'native ready deadline')
                            if gate.select.select([stdout], [], [], 0.1)[0]:
                                data = os.read(stdout, 8192); gate.require(data, 'native ready EOF'); raw.extend(data)
                        first, _ = bytes(raw).split(b'\n', 1)
                        ready = json.loads(first)
                        self.assertEqual((ready['event'], ready['pid'], ready['scenario']['id']), ('ready', waiter.pid, scenario))
                        gate.validate_hwm_ready(ready, gate.scenario(scenario), waiter.pid)
                        os.write(ack, b'continue\n')
                        while True:
                            gate.require(time.monotonic() < until, 'native fault output deadline')
                            if gate.select.select([stdout], [], [], 0.1)[0]:
                                data = os.read(stdout, 8192)
                                if not data: break
                                raw.extend(data); gate.require(len(raw) <= 262144, 'native fault output bound')
                        (folder / 'native-fault.stdout').write_bytes(raw)
                        wait = retain_wait(runner, waiter, folder)
                        self.assertNotEqual(wait['exit_code'], 0)
                        events = [json.loads(line) for line in bytes(raw).splitlines()]
                        failures = [event for event in events if event['event'] == 'worker-failure']
                        self.assertEqual(len(failures), 1)
                        self.assertEqual((failures[0]['schema'], failures[0]['pid'], failures[0]['scenario'],
                                          failures[0]['phase'], failures[0]['mutation']),
                                         (2, waiter.pid, scenario, 'load', True))
                        workers = failures[0]['workers']
                        self.assertEqual([worker['worker'] for worker in workers], list(range(16)))
                        self.assertTrue(all(worker['errors'] == 1 and worker['completed_records'] == 0 for worker in workers))
                        for number, worker in enumerate(workers):
                            self.assertEqual((worker['range_start'], worker['range_end']),
                                             (1000000 * number // 16, 1000000 * (number + 1) // 16))
                            self.assertEqual((worker['wire_commands'], worker['command_counts'], worker['histogram_samples']),
                                             (1, {'SET': 1}, 0))
                            self.assertTrue(worker['error'])
                        self.assertFalse(any(event['event'] in ('phase', 'steady-complete', 'complete') for event in events))
                    finally:
                        try:
                            runner.cleanup_preserving(waiter, folder, fds)
                        finally:
                            listener.close(); thread.join(6)
                            if not (folder / 'native-fault.stdout').exists():
                                (folder / 'native-fault.stdout').write_bytes(raw)
                            gate.write(folder / 'fault-peer.json', {'first_set_indices': sorted(observations),
                                'classification': CLASSIFICATION, 'fault': fault, 'synthetic_peer': True,
                                'errors': errors, 'thread_alive': thread.is_alive()})
                    self.assertFalse(thread.is_alive())
                    self.assertFalse(errors, errors)
                    self.assertEqual(sorted(observations), [1000000 * worker // 16 for worker in range(16)])


@unittest.skipUnless(sys.platform == 'linux' and hasattr(os, 'fork'), 'real delegated Linux kernel required; no qualification')
class HwmKernelPlacementFaultTests(unittest.TestCase):
    def setUp(self):
        path = os.environ.get('EDEN_HWM_RELEASE_KERNEL_BINDING')
        digest = os.environ.get('EDEN_HWM_RELEASE_KERNEL_BINDING_SHA256')
        if not path or not digest:
            self.skipTest('accepted real delegation binding is required; regular FD fixtures do not qualify')
        self.binding = gate.read_json({'path': path, 'sha256': digest})
        self.assertEqual(self.binding['classification'], 'delegated kernel fault probe only; no benchmark admission')
        self.assertEqual(self.binding['helpers']['child_wait']['sha256'], gate.sha(ROOT / 'run-compact-shared-read-gate.py'))
        self.assertEqual(self.binding['driver_sha256'], gate.sha(ROOT / 'run-compact-storage-scenarios.py'))
        self.executable = gate.pinned(self.binding['probe_executable'], executable=True)
        self.assertEqual(self.executable, pathlib.Path(sys.executable).resolve())
        self.runner = direct_runner()
        self.runner.__class__ = gate.HwmBaselineRunner
        self.runner.plan = {key: copy.deepcopy(self.binding[key]) for key in ('source', 'owner', 'resources')}
        # Test alias chooses the unchanged native placement branch for a known
        # Python PID marker. It is never passed to benchmark verify_inputs.
        self.runner.plan['binaries'] = {'compact_resp_scenarios': {'path': str(self.executable)}}
        self.runner.budget = lambda: self.runner.verify_resources()
        self.runner.verify_resources(True)  # Unchanged live resource/receipt predicate.

    def test_actual_kernel_placement_and_next_fresh_child(self):
        for sequence in range(2):
            folder = case_folder('kernel-fresh')
            waiter = None; fds = []
            try:
                waiter, stdout, ack = self.runner.spawn([str(self.executable), '-B', '-c',
                    'import os,sys; print(os.getpid(),flush=True); sys.stdin.readline()'], folder, True)
                fds = [stdout, ack]
                self.assertTrue(gate.select.select([stdout], [], [], 5)[0])
                self.assertEqual(os.read(stdout, 128).strip(), str(waiter.pid).encode())
                client = pathlib.Path(self.binding['resources']['client']['path'])
                self.assertEqual(h1.cgroup_path(waiter.pid), client)
                self.assertEqual((client / 'cgroup.procs').read_text().split(), [str(waiter.pid)])
                self.assertEqual((os.getpgid(waiter.pid), os.getsid(waiter.pid)), (waiter.pid, waiter.pid))
                identity = h1.proc_identity(waiter.pid, self.executable)
                self.assertEqual(identity['start_ticks'], waiter.identity['start_ticks'])
                gate.write(folder / 'kernel-native-identity.json', dict(identity, classification=CLASSIFICATION))
                os.write(ack, b'finish\n')
                self.assertEqual(retain_wait(self.runner, waiter, folder)['raw_wait_status'], 0)
            finally:
                self.runner.cleanup_preserving(waiter, folder, fds)
            self.runner.verify_resources(True)

    def test_actual_kernel_placement_failures_never_exec_and_reap(self):
        for fault in ('fork', 'setsid', 'membership-write', 'before-placed', 'parent-check', 'exec', 'pending-term'):
            with self.subTest(fault=fault):
                folder = case_folder('kernel-' + fault)
                parent = os.getpid(); waiters = []; procs = []
                actual_open = self.runner.open_client_procs
                actual_confirm = self.runner.placement_confirmed
                actual_write = os.write
                actual_bind = h1.NativeChildWait.bind
                def open_procs():
                    fd = actual_open(); procs.append(fd); return fd
                def write(fd, data):
                    if os.getpid() != parent:
                        if fault == 'membership-write' and fd == procs[0]:
                            return actual_write(fd, b'not-a-pid\n')  # Genuine cgroup.procs kernel refusal.
                        if fault == 'before-placed' and data == b'placed\n':
                            os.kill(os.getpid(), signal.SIGKILL)
                    return actual_write(fd, data)
                def bind(waiter):
                    actual_bind(waiter); waiters.append(waiter)
                    if fault == 'pending-term': os.kill(parent, signal.SIGTERM)
                def confirmed(waiter):
                    actual_confirm(waiter)
                    if fault == 'parent-check': raise RuntimeError('injected after actual parent placement check')
                previous = signal.signal(signal.SIGTERM, gate.interrupted)
                try:
                    with contextlib.ExitStack() as stack:
                        stack.enter_context(mock.patch.object(self.runner, 'open_client_procs', side_effect=open_procs))
                        stack.enter_context(mock.patch.object(self.runner, 'placement_confirmed', side_effect=confirmed))
                        stack.enter_context(mock.patch.object(gate.os, 'write', side_effect=write))
                        stack.enter_context(mock.patch.object(h1.NativeChildWait, 'bind', bind))
                        if fault == 'fork': stack.enter_context(mock.patch.object(gate.os, 'fork', side_effect=OSError('injected fork failure')))
                        if fault == 'setsid': stack.enter_context(mock.patch.object(gate.os, 'setsid', side_effect=OSError('injected setsid failure')))
                        if fault == 'exec': stack.enter_context(mock.patch.object(gate.os, 'execv', side_effect=OSError('injected exec failure')))
                        argv = [str(self.executable), '-B', '-c',
                                'import pathlib,sys; pathlib.Path(sys.argv[1]).touch()', str(folder / 'UNEXPECTED-EXEC')]
                        if fault == 'exec':
                            waiter, stdout, ack = self.runner.spawn(argv, folder, True)
                            try: self.assertEqual(retain_wait(self.runner, waiter, folder)['exit_code'], 127)
                            finally: self.runner.cleanup_preserving(waiter, folder, [stdout, ack])
                        else:
                            with self.assertRaises((RuntimeError, OSError)):
                                self.runner.spawn(argv, folder, True)
                finally:
                    signal.signal(signal.SIGTERM, previous)
                self.assertFalse((folder / 'UNEXPECTED-EXEC').exists())
                self.assertFalse(self.runner.children)
                if fault == 'fork':
                    self.assertFalse(waiters)
                else:
                    self.assertEqual(len(waiters), 1)
                    waiter = waiters[0]
                    self.assertIs(waiter.wait['child_reaped'], True)
                    self.assertIs(waiter.wait['wait_observed'], True)
                    self.assertEqual(waiter.wait['waited_pid'], waiter.pid)
                    if fault == 'before-placed':
                        self.assertTrue(os.WIFSIGNALED(waiter.wait['raw_wait_status']))
                        self.assertEqual(os.WTERMSIG(waiter.wait['raw_wait_status']), signal.SIGKILL)
                    gate.write(folder / 'kernel-fault-wait.json', dict(waiter.wait, identity=waiter.identity,
                        fault=fault, classification=CLASSIFICATION))
                self.runner.verify_resources(True)

    def test_actual_kernel_lost_wait_owner_sends_no_signal(self):
        folder = case_folder('kernel-lost-wait-owner')
        waiter = None; fds = []
        try:
            waiter, stdout, ack = self.runner.spawn([str(self.executable), '-B', '-c',
                'import os,sys; print(os.getpid(),flush=True); sys.stdin.readline()'], folder, True)
            fds = [stdout, ack]
            self.assertTrue(gate.select.select([stdout], [], [], 5)[0])
            self.assertEqual(os.read(stdout, 128).strip(), str(waiter.pid).encode())
            os.write(ack, b'finish\n')
            # This intentionally acts as an external reaper of our one child.
            # The true status is retained; H1 must report ownership lost and
            # must not present that externally consumed wait as its own wait.
            until = time.monotonic() + 5
            while True:
                pid, raw = os.waitpid(waiter.pid, os.WNOHANG)
                if pid:
                    break
                gate.require(time.monotonic() < until, 'external reaper deadline')
                time.sleep(0.01)
            self.assertEqual((pid, raw), (waiter.pid, 0))
            receipt = h1.stop_child(waiter)
            self.assertIs(receipt['ownership_lost'], True)
            self.assertIs(receipt['wait_observed'], False)
            self.assertIs(receipt['child_reaped'], False)
            self.assertEqual(receipt['signals_sent'], [])
            gate.write(folder / 'lost-owner.json', {'external_waited_pid': pid, 'external_raw_wait_status': raw,
                'identity': waiter.identity, 'H1': receipt, 'classification': CLASSIFICATION})
            self.runner.children.pop(waiter.pid)
        finally:
            self.runner.cleanup_preserving(waiter, folder, fds)
        self.runner.verify_resources(True)


if __name__ == '__main__':
    if sys.argv[1:2] == ['--publication-probe']:
        if len(sys.argv) != 4:
            raise SystemExit('exact publication probe argv required')
        raise SystemExit(publication_probe(pathlib.Path(sys.argv[2]), sys.argv[3]))
    unittest.main()
