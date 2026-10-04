# String high-water lifecycle baseline

This is the first executable producer slice for the storage optimization plan.
It compares current product `016a521b12968f38b703b3e3e39a8eb0be745429`
(tree `16f855f045f44877d53d4ce5e840d20a555d31de`) with Redis 7.4.11 over
the same loopback RESP transport. It is a **lifecycle diagnostic subset** of
HWM_STR. Steady uniform 80/20 GET/SET windows are pending in this revision.
There are no measurements, qualified new native ELF, or accepted runtime
admissions in this change. The new benchmark driver commit is distinct from
the source016 server build; neither identity may be substituted for the other.

## Workload and six rows

The new selector is `storage-hwm-str-v1`. The existing 24 selectors, original
seed/payloads and 378-row composition remain unchanged. The new package
`storage-hwm-baseline6-v1` has these exact members, in execution order:

| Round | First arm | Second arm |
|---|---|---|
| 1 | current016 | Redis |
| 2 | Redis | current016 |
| 3 | current016 | Redis |

Each fresh, empty server receives 1,000,000 strings with 18-byte keys and
64-byte values (82,000,000 logical bytes). There is one load phase followed
by `delete-N`, `idle-N`, `refill-N` for N=1,2,3. Delete removes exactly 95%:
`(index * 104729 + 12345) % 1000000 >= 50000`. The permutation is scattered
and retains the same 50,000 survivors. Idle has 10 seconds with no workload
traffic. Refill restores exactly the deleted 950,000 keys.

Each value starts with a bijective 64-bit mixed key index, independent of
generation. Seven following words use generation-sensitive entropy. Thus
different keys cannot alias across mixed survivor/refill generations; the
same refilled key changes across generations 1,2,3. Survivors retain generation
0 throughout. This differs from the original MD05 entropy pattern and is a
new workload contract, not an equivalent replay.

Every phase verifies every key's exact GET bytes or absence, TYPE, PTTL
(-1 live; -2 absent), cardinality and framed state digest. No expiration,
eviction, persistence, or data loss is permitted. The independent Python
oracle derives the same state and command traces from the contract. Native
ready events include five cross-language payload hashes. All 16 workers
report canonical ranges, distinct Rust thread identities, checked-record and
wire-command counts, histogram samples, hashes and errors. Failure observations
are retained before validation; missing workers or incorrect state fail the row.

## Timing and memory meaning

P1 mutation timing is **logical record transition latency**, including checked
replies. Verification timing is **full record verification latency** for
GET/TYPE/PTTL together. Record throughput and wire-command throughput are
reported separately. Neither is steady 80/20 throughput; verification p99
is not per-command request p99. Three-run median p99 is not a pooled percentile.

The original PID1 sampler provides five settled PSS/RSS/private-memory samples
at idle and every phase, with Docker ownership and PID/cgroup identity guards.
A separate `idle-boundary` handshake captures memory after the genuine quiet
interval and **before** exhaustive reads. Post-verification memory is separately
labeled. The server observer reports sampled cgroup current/anon maxima, not an
exact allocation peak. Client and controller snapshots include their complete
process PSS, cgroup usage, effective CPU sets, quota, CPU usage/throttle, current
memory and pids. `memory_peak_since_cgroup_creation` is cumulative, not a fresh
per-row peak. Raw snapshots remain available; shared CPU sets are not a reservation.
No optimization attribution, fixed-RAM inserted capacity, or competitive
percentage is claimed from this diagnostic producer alone.

## Entry points

The existing executable gains one finite selector:

```bash
compact_resp_scenarios --scenario storage-hwm-str-v1 --describe
```

The controller has a concrete template producer. It writes NULL runtime inputs
and does not import runtime helpers or start a workload:

```bash
python3 benchmarks/scripts/run-compact-storage-scenarios.py \
  --write-hwm-baseline-template /absolute/new/hwm-plan.json
```

A separately bound and independently reviewed plan uses the existing invocation:

```bash
python3 benchmarks/scripts/run-compact-storage-scenarios.py \
  --plan /absolute/frozen/hwm-plan.json --plan-sha256 EXACT_SHA256
```

These are producer interfaces, not authorization to execute. All tests/builds
and engineering runtime occur only on Adam through designated executors.

## Resource admission and bounded placement

This new package requires three separate, genuinely admitted cgroups under
one exact task-owned systemd slice. It never substitutes the old combined
4-CPU controller envelope:

| Scope | CPU ceiling | Memory | Swap | Pids |
|---|---:|---:|---:|---:|
| Server | 1 | 4 GiB | 0 | 512 |
| Native client | 4 | 2 GiB | 0 | 512 |
| Controller including helper children | 1 | 2 GiB | 0 | 64 |
| Aggregate parent | 6 | 8 GiB | 0 | 1088 |

The plan binds canonical parent/controller/client paths, current device/inode,
UID/GID/mode, the exact writable client `cgroup.procs` file metadata and an
independently accepted delegation receipt. Real caps, parent relationships,
controller placement and all ancestors are checked. Before Docker create/run,
the configured driver must be systemd and the complete root hierarchy encoded
by its slice name must equal the admitted canonical parent. This finite adapter
accepts only nonempty lowercase ASCII letter/digit components separated by
dashes, ending in `.slice`, at most255 bytes. For example, `eden2266-hwm1.slice`
encodes `/sys/fs/cgroup/eden2266.slice/eden2266-hwm1.slice`; an identically named
slice nested inside a user manager is refused before container creation.
Docker's [systemd parent rules](https://docs.docker.com/reference/cli/dockerd/#default-cgroup-parent)
define this ancestry. Other drivers or ambiguous names are unsupported.
The independently accepted operating receipt must bind the actual driver,
parent argument, complete parent path and genuine delegated kernel/Docker
mapping proof. Those admissions remain NULL until actual Adam qualification.
The Docker server uses that exact parent and pids ceiling; its actual live
PID/cgroup/caps are still checked after startup. Runtime setup must obtain real delegation; this
controller does not create or adjust scopes or permissions.

Before native exec, the directly forked child enters a private session and
writes its own PID through the preopened, identity-checked client `cgroup.procs`
FD. It sends `placed`, then waits for the parent. The parent binds H1 ownership,
checks PID/start/parent, actual cgroup, caps and session, then permits exec.
Only that exact client membership file is written. The same direct PID is
owned and genuinely waited; placement/fork/exec/handshake failures use the
unchanged H1 reaping path. No systemd launcher PID substitutes for the client.
Missing writable delegation is an explicit resource admission error.

Per-row runtime/native deadline is 840 seconds; six rows fit a planned 90-minute
controller cap with 360 seconds left for oracle/preparation. This is a ceiling,
not a promised duration; exceeding it fails and preserves evidence. Build is
bounded at 2700 seconds, cleanup at 300, complete package at 8400. Output limits
remain 128 MiB/file and 1 GiB/package.
The original monotonic controller start is checked again after last-row cleanup,
terminal hashes and summary reporting, including time spent on final resource
observations. At/after5400 seconds, no successful terminal result is admitted.
Result serialization/fsync first writes `result-candidate.json`, which alone is
unadmitted evidence. A fresh controller-clock check after that write precedes
the create-only atomic publication of `result.json`. Overrun retains the candidate
and failure receipt without a successful terminal result. The elapsed field is
explicitly observed before candidate serialization; publication checks the clock
again. A genuine outer controller/package deadline remains required.
Owned stop/remove/reaping and failure receipts remain mandatory under their
separate bounded cleanup allowance; cleanup never extends successful work.
The original free-disk >=100 GB,
global guard age <=90 seconds, STOP predicate and whole RUN plus driver target
<30 GiB remain enforced. No clock or predicate is relaxed.

## Remaining qualifications and profiles

Before any new row: independent code review, fresh driver regression/native
build receipts, compiler/ELF hashes, genuine resource/Docker adapter admission,
source016 image provenance and Redis image digest/reference admission are
required. Strict source016 EARLY acceptance and complete relevant product-input
equivalence are distinct from the new benchmark driver admission. The template
does not manufacture these receipts. Exact helper hashes and source inputs are
rechecked before and after the package; full raw output must be retained.

Seven Rust and fourteen Python regression methods are defined for new phases,
mixed-generation uniqueness, survivor counts, corruption/stale/absent rejection,
all-worker witnesses, admission/path/owner/cap failures, guard freshness and
direct-child placement/handshake/exec reaping. New focused regressions also cover
exact systemd ancestry, nested-user-manager/wrong-driver refusal
before Docker effects, and fake-clock terminal boundaries after cleanup/reporting.
They also inject candidate-write overruns and verify no terminal publication or
overwrite of an existing terminal artifact.
Fake-clock cleanup receipts are explicitly unit fixtures, not genuine waits.
Isolated unit placement probes use a private regular FD, never qualify Linux delegation. A genuine delegated
kernel placement integration is still required by the operating admission.
The original 21 native/40 Python regressions retain their definitions.

Full HWM_STR steady windows, other five optimization proposal profiles,
optimized-head attribution, SCNP/embedded modes, offered-load latency and
actual fixed-RAM capacity remain pending. This slice implements no optimization.
