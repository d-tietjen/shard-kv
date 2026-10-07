# HWM release fault coverage

These repository tests exercise the HWM producer's failure boundaries. They do
not run or replace either six-row benchmark, qualify a server image, supply an
operating admission, or support a memory or performance claim. All definitions
are initially unrun. A skipped prerequisite leaves its release coverage pending.

The production controller, native functions, H1 ownership helper, resource caps,
placement selectors, and workload contracts are unchanged. The Rust additions
are inside `cfg(test)`. Python probes call the existing production functions;
fault injection lives only in the test module.

## Defined checks

| Release scenario | Defined coverage | Required observations | Remaining integration |
| --- | --- | --- | --- |
| RF-HWM-TRANSPORT | Three Rust socket tests cover 14 checked request faults; one Rust steady worker test reaches real cancellation. One Python case has six native subprocess subcases: lifecycle/steady profile, truncated/WRONGTYPE first fill reply or actual TCP reset. | Real loopback request bytes; actual checked error; failed record never enters state digest; real native nonzero direct wait; all 16 worker IDs/error observations; no phase, steady completion, or terminal event. | Faults after a real server accepted mutation; reset/reconnect during a fully initialized steady window; independent fresh-server semantic check. |
| RF-HWM-PUBLISH | One Python case has seven actual child subcases: success control, existing result, candidate symlink, terminal symlink, partial file, post-fsync TERM, and clock edge. | Genuine H1 direct PID/start/raw wait; real candidate writer/fsync/create-only link; sentinel and symlink target unchanged; partial evidence retained; failed probe has no new terminal result. | Whole-controller cleanup/reporting path under a natural campaign deadline; outer-controller crash during candidate writing and publication. |
| RF-PLACE-CRASH | One Python case has seven real delegated-kernel subcases: fork error, setsid error, invalid PID kernel write, SIGKILL after membership/before `placed`, post-parent-check refusal, exec error, and pending TERM after actual parent bind. | Unchanged live resource/receipt predicates; actual cgroup.procs FD; direct H1 child identity/wait; no probe exec marker; client empty afterward; SIGKILL raw status in its selected case. | Outer-owner controller SIGKILL while handshake is blocked; exact inventory and finite task-owned cleanup from the operating adapter. |
| RF-PLACE-IDENTITY | One positive kernel case places and reaps two separately fresh PID markers. One negative case consumes the genuine wait through an external reaper and checks H1 ownership-lost refusal. | Real membership/session/start identity; original exact controls; separately reaped next child; lost H1 wait ownership sends no signals and reports no H1 reap credit. | Driver/kernel mapping and Docker PID1 probe remain with the operating producer. |
| RF-HWM-CRASH | Concrete adapter placements below use existing stdout/acknowledgement seams. | Controller direct wait; actual native/server lineage; retained failure; no false terminal; exact resource inventory; independently fresh baseline. | Adapter implementation and genuine server/controller crash execution remain mandatory. These test definitions alone do not cover this row. |

The clock-edge probe assigns `started = real_monotonic_now - 5399.9`, performs
the actual fsync, and crosses the unchanged 5400-second boundary using the real
monotonic clock. It qualifies that terminal function boundary. It is not a
natural 5400-second benchmark campaign. The partial-file probe lowers only its
child's inherited file-size limit to 512 bytes, restores it before its receipt,
and observes a genuine kernel write failure. It makes no disk-full claim.

The faulty TCP peer returns zero for the native's initial DBSIZE and invalid
replies or an actual TCP reset at each worker's first SET. The peer is synthetic and stores no keys.
The native's 1M descriptor remains intact, but zero records complete. This
deliberately failed trace has no 1M workload or product correctness credit.

## Exact selected commands

The validation owner runs these only on Adam after source and input readback.
The executor captures genuine outer status, complete stdout/stderr, selected
test names/counts, and all probe files. The four new Rust test names begin with
`hwm_release_`; the Python module defines five tests. Missing native ELF skips
one Python test; missing kernel delegation skips three. A skip is not acceptance.

```sh
cargo test --locked --jobs 4 -p shardcache-benchmarks --bin compact_resp_scenarios hwm_release_ -- --nocapture --test-threads=1
python3 -B benchmarks/scripts/test-hwm-release-faults.py HwmPublicationFaultTests -v
python3 -B benchmarks/scripts/test-hwm-release-faults.py HwmNativeTransportFaultTests -v
python3 -B benchmarks/scripts/test-hwm-release-faults.py HwmKernelPlacementFaultTests -v
```

The operating producer supplies `EDEN_HWM_RELEASE_FAULT_OUTPUT` as a fresh,
canonical, task-owned mode-0700 evidence directory already included in owned
disk accounting. It supplies `EDEN_HWM_RELEASE_NATIVE` and
`EDEN_HWM_RELEASE_NATIVE_SHA256` from genuine exact-source build/readback
evidence. The test never creates a build acceptance. Failed peer/output evidence
is preserved under the given directory.

Kernel tests also require `EDEN_HWM_RELEASE_KERNEL_BINDING` and its
`EDEN_HWM_RELEASE_KERNEL_BINDING_SHA256`. The independently reviewed operating
producer emits that binding only after actual resource discovery, preparation,
identity/cap readback and independent delegation acceptance. It contains:

* classification exactly `delegated kernel fault probe only; no benchmark admission`;
* `source.sha` and `source.tree` matching the actual delegation receipt;
* `driver_sha256` and `helpers.child_wait` for the exact source files;
* `probe_executable` pinning the actual canonical Python interpreter;
* `owner` and the existing `resources` bindings for parent, controller, client,
  Docker parent, and `delegation_acceptance`.

The Python test process must already be in the admitted controller cgroup. It
calls the unchanged `verify_resources` and `open_client_procs` functions. A
known Python PID marker selects the unchanged native-placement branch through
a clearly test-only binary alias; this plan is never passed to benchmark
`verify_inputs`. No regular file is substituted for cgroup.procs. No acceptance
JSON, resource hierarchy, Docker container, or product state is invented by
the tests. The operating producer owns preparation and final inventory.

## Outer adapter crash placements

The adapter composes the same reviewed H1 runtime and records these placements
before releasing the next acknowledgement:

1. Placement: actual child PID written to the admitted client cgroup,
   `placed\n` observed, actual H1 bind and parent placement check complete,
   `exec\n` not yet sent. Observe exact PID/start/session/cgroup and distinguish
   child SIGKILL from caught controller TERM or controller SIGKILL.
2. Accepted mutation: native `steady-ready` is emitted after the fill mutation
   and before the steady acknowledgement. Require genuine producer worker and
   command ledgers plus independently observed live keys before killing the
   exact native or task-owned server PID. A synthetic peer does not cover this.
3. Verified phase: native `phase` is emitted after exhaustive verification and
   remains blocked on `continue\n`. Retain the phase and independently checked
   state before terminating the controller prior to terminal publication.
4. Publication: observe the actual candidate fsync before the create-only
   terminal link. Preserve candidate alone as unadmitted evidence. If the
   controller is SIGKILLed, the outer owner retains the actual raw wait and
   failure observation; a self-authored cleanup receipt cannot be required.

The outer owner must retain its genuine controller PID/start/raw wait and exact
native, server full-ID, cgroup and socket inventory. After controller SIGKILL,
native direct-parent wait ownership is not inferred or fabricated. Physical
absence is not a kernel wait. Cleanup requires current task ownership, exact
identity checks and finite controls from the reviewed operating adapter; loss
of ownership authorizes no signals. No global cgroup, process or Docker cleanup
is introduced by this catalog.

A subsequent baseline uses separately fresh output, server/container, native
PID and mutable namespace. It must observe the new server's initial DBSIZE
zero, execute a checked SET/GET, verify exact key/value/type/PTTL/count, and
observe its own genuine wait and cleanup before any qualification. This is an
availability/cleanup diagnostic, not a complete six-row benchmark. There is no
resume, durable checkpoint recovery or globally exact retry guarantee.
