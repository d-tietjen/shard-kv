# Redis baselines for six storage optimization proposals

Status: **planned coverage**, with no new measurements or executable runner.
The machine-readable [plan](reference/storage-optimization-baseline-plan/plan.json)
defines finite cases and NULL admissions. A selected proposal can proceed alone;
the plan does not require implementing six variants together.

## Source and comparison contract

The prechange reference is published `016a521b12968f38b703b3e3e39a8eb0be745429`,
tree `16f855f045f44877d53d4ce5e840d20a555d31de`. This reference is **not a
qualified benchmark build**. Every optimization head, feature/build admission,
Redis image digest and matched configuration SHA remains NULL.
Redis is standalone **7.4.11**, with persistence disabled and the same logical
data, policy, physical limits, command lifecycle and relative validation schedule
as each arm.
Normal profiles disable eviction; policy profiles require zero actual evictions.

Each claim needs two separately reported comparisons:

- **Optimization/current016:** attribute savings and costs to that single change.
- **Optimization/Redis:** assess competitive memory at the same retained state.

Redis alone cannot establish that a change caused savings. Report all three arms,
all phases and all misses; no backend-specific best-cell selection or unmeasured
savings percentage. Inverse PSS ratios are estimates, never inserted capacity.

First establish current016/Redis diagnostic traces while no optimized head exists.
Later evaluate one independently reviewed optimization using three balanced orders:
current/optimized/Redis, optimized/Redis/current, Redis/current/optimized.
Diagnostics have three paired rounds with alternating order; three rounds cannot
exactly balance first position for two arms. Their actuals remain NULL, and their
results do not silently substitute for later contemporaneous three-arm evidence.

## Preserve existing suites and use exact subsets

[COMPACT_RESP_SCENARIOS.md](COMPACT_RESP_SCENARIOS.md) retains **378 rows**:
90 core, 72 access and 216 stateful rows from 24 fixed traces. Nothing here changes
their profiles, limits, drivers, timing, source attribution or acceptance screens.
The independently reviewed RESP scaling plan retains **243 rows**: CPU 1/4/16,
workers 16/64/256, pipelines 1/16/64, three original arms and three rotations.
Its contract SHA is `b99942c2b14acf5abe32ea2d03a9afba5956419f3c96cb62990ade232e14cac0`;
its matrix SHA is `860b493657e8356252868bfb2f4a94821cc771dd9f9a0334267981fb250b20f4`.

Reuse requires independent acceptance of exact source/features, logical inputs,
trace/generation, driver ELF, protocol, resources, latency unit and complete raw
evidence. Old baseline78 rows keep their identities. A new optimization arm is
fresh. Planned rows supply no evidence. Existing `metadata` is a candidate exact
subset for HINT_RESP_CONTROL; differing deletion ratios, sizes, unique payloads
and new collection/expiry/cold transitions are explicit coverage gaps.
HINT_RESP_CONTROL explicitly overrides the global new per-key-unique generator
and steady access policy: its original metadata phases, arguments and values
remain byte-identical source016 subsets. An added timed access window or changed
generator/TTL policy is a separately labeled fresh extension within the planned
row, never whole-row equivalence or inherited evidence for that extension.
The scaling plan can supply identical steady-state cells only when this complete
closure matches; it cannot establish new lifecycle or hinted API behavior.

## Initial finite case matrix

Default: 1 server CPU/one ShardCache shard, 16 workers, pipeline 1, sample every
successful operation. New string access windows use the TTL-safe policy below;
aggregate profiles use the typed access contract below. Every profile
uses a fresh store per arm and round. TTL/eviction/overflow are off unless stated.
Keys contain a collision-free binary index suffix; seeds, generations and full
logical byte manifests are frozen before execution. No Redis representation
workaround, alternate packing, or different input distribution is allowed.

| Claim / profile | Exact initial shape and lifecycle |
|---|---|
| Reclaim: HWM_STR | 1M keys, key18/value64, unique entropy; delete scattered95%, idle10s, refill; three delete/refill cycles. |
| Reclaim: HWM_OBJECT | 100k objects equally hash/set/zset/list, four16B elements; scattered95% deletion, idle10s and three refill cycles. |
| Metadata: META_GENERAL | 100k key65/value64 entropy, always general; TTL0/1%/100%, PERSIST, overwrite; controlled expiry of1k keys. |
| Metadata: META_POLICY | 100k key18/value1024 entropy; matched2GiB logical policy budget/LRU, zero evictions; sparse TTL, overwrite, half-delete/refill. |
| Arena: ARENA_DENSE | key18/value64 entropy; cardinalities802000→802816→803000→900000, then95% scattered delete/refill. |
| Arena: ARENA_EDGE | 90k keys, nine equal cohorts from key63/64/65 × value255/256/257;95% delete/refill with rotating sizes. |
| Hint: HINT_RESP_CONTROL | Exact existing metadata trace:100k key18/value16,10% two-hour TTL, PERSIST/delete/reinsert. Default RESP hint=None. |
| Aggregates: AGG_SMALL | 100k equal four-type objects;16/256B binary and integer-like fields;1→4→5→8→1 elements, zset score ties/order checks. |
| Aggregates: AGG_OSCILLATE | 100k four-type objects; ten4↔5 oscillations (lists8↔9), grow33, shrink1, idle10s. |
| Compression: COLD_TEXT | 100k key18/value4096 unique compressible JSON/text; hot1000:90, idle, hot-set rotation,99k cold-read burst, overwrite/TTL/PERSIST. |
| Compression: COLD_ENTROPY | Same lifecycle and size, deterministic **per-key-unique** incompressible values. |

### String access, TTL cohorts and absence

Freeze the live-key, persistent-key and expiring-key manifests before each new
steady window. Reads select live keys uniformly (or the declared hot/cold split)
and require GET to return exact canonical bytes. Writes select only existing
persistent keys, use `SET key canonical_value XX`, and require OK. The value is
an idempotent touch; no steady write changes generation, clears a TTL or recreates
an absent key. Exclude every TTL-bearing key from writes. Where persistent keys
exist, repeat four reads then one write per worker with frozen choices; this is
80/20 reads/persistent-key touches, not uniform writes across the TTL cohort.
When all keys carry TTL, label a read-only GET window with zero writes and its
actual operation counts; it is not an 80/20 cell. Use identical policy in all arms.

META_GENERAL's controlled-expiry1000 keys are excluded from **all** steady access
from deadline installation until both declared observations finish. Validate
their exact bytes/type before expiry and absence after expiry through the separate
oracle. Other access targets must remain live throughout the bounded window;
unplanned expiry, nil, a failed XX condition or an error fails qualification.
Every actual worker prefix/count is retained; differing throughput cannot mutate
the frozen type/value/TTL/absence state. One steady logical access is one checked
command; pipeline1 latency is ns per command completion. Validation is separate.

TTL changes, value-generation overwrites, deletion and refill remain exact
separate lifecycle transitions. In new profiles an ordinary overwrite/creation
SET deliberately clears TTL; its phase oracle records persistent state unless
the declared TTL-setting transition applies afterward. TTL installation/PERSIST
have checked replies and matched per-arm expiry/absence schedules; timed
touches do not reset deadlines. The byte-identical original metadata trace is
exempt from this new-window policy and retains its exact original semantics.
Any added TTL-safe window is fresh coverage. The common cohort-aware driver,
including equivalent checked API/SCNP semantics, is unimplemented and unqualified;
its implementation and mode admissions remain NULL.

### Typed aggregate access and separate lifecycle changes

HWM_OBJECT, AGG_SMALL and AGG_OSCILLATE retain their hash/set/zset/list types.
Between transitions, choose uniformly from the phase's frozen live object keys,
then one existing field/member/list index from that object's canonical manifest.
Each worker repeats four reads then one write, with the same deterministic
seed/choice sequence and exact command arguments in every arm. Freeze profile,
phase, worker and sequence identities before dispatch. Actual timed prefixes may
differ with throughput; retain each prefix, read/write counts and every reply.

| Type | Read and expected reply | Idempotent existing-element write and expected reply |
|---|---|---|
| Hash | `HGET key field`: exact canonical value bytes, never nil | `HSET key field canonical_value`: integer0 for the existing field |
| Set | `SISMEMBER key member`: integer1 | `SADD key existing_member`: integer0 |
| Sorted set | `ZSCORE key member`: the canonical finite numeric score, never nil | `ZADD key canonical_score existing_member`: integer0, with no CH option |
| List | `LINDEX key index`: exact canonical bytes at an in-range index | `LSET key index canonical_value`: OK |

These writes touch existing elements with their current canonical bytes/scores.
They preserve cardinality, membership, type, list order and zset ranks/ties,
regardless of concurrent worker order or completed prefix. Label the measured
mix **80/20 typed reads/idempotent writes**; it is not changing-value or
membership-churn throughput. GET/SET is never issued to a collection key.
Field TTL/snapshot obligations remain the separate isolated checks already
stated; even a same-value HSET is not a field-TTL preservation operation.

Growth/shrink/deletion/refill are separate deterministic phase traces. Grow or
refill a canonical object one element per `HSET`/`SADD`/`ZADD`/`RPUSH`; additions
return1 for hash/set/zset and the new list length for RPUSH. Shrink hash/set/zset
by one prescribed `HDEL`/`SREM`/`ZREM` per removed element, each returning1.
For lists, `LTRIM key 0 m-1` retains the prescribed nonempty prefix of length m
and returns OK. HWM_OBJECT deletes each selected whole key with `DEL` returning1
and refills it through its original type's creation commands. Its scattered95%
deletion is deterministic within each25k type cohort, leaving1250 objects/type
and5000 total. Retained prefix elements keep their canonical identities; only
the declared transition manifest advances added/refilled generations. Every arm
replays identical logical changes and exact phase payloads, with full state
validation after each phase. Timed steady touches never advance generation.

One steady logical access is one request/response command: completed access ops/s
equals completed data commands/s, with reads and touch writes reported separately.
Lifecycle growth n→m uses m-n element commands; shrink n→m uses n-m deletion
commands for hash/set/zset or one LTRIM for a list. Creation of an m-element object
uses m commands, but is one complete logical record transition. Report both
record transitions and underlying commands, plus element/cardinality deltas.
Validation/control commands have separate counts and do not inflate access ops.
Steady pipeline1 latency is ns per checked single-command completion; multi-command
lifecycle latency is ns from the first mutation request through its last checked
reply for one whole record transition. Keep their histograms/denominators separate.
Errors, missing elements, changed bytes/scores/types, rank/order drift or incomplete
transitions fail qualification; neither partial records nor retries become success.

These **11 matched RESP profiles × 3 arms × 3 rounds = 99 evaluation rows**.
Repeated phases remain within each row and are individually validated/measured.
Each profile has current016/Redis baseline diagnostics first:66 RESP rows total.

Two conditional shared-embedded profiles, HINT_API_NONE and HINT_API_1M, use
100k key18/value64 entropy and respectively None/1M capacity hints. Phases are
load compact→first TTL general key→1% TTL→PERSIST→first257B general value→10%
oversized values. Use16 caller workers, borrowed/owned-shared reads as distinct
phase labels, and at most1024 retained owners. Redis TCP executes identical
logical transitions through the common caller. This adds **18 deployment rows**
to evaluation, or12 paired baseline diagnostic rows; it is not matched transport.
Sparse semantic/governance, field TTL/snapshots and long-lived read epochs need
isolated current/candidate correctness coverage; unsupported Redis API semantics
cannot receive comparative performance credit or modify accepted datasets.

Total initial evaluation: **117 rows**. Baseline diagnostics: **78 separate rows**.
No optimized head or result is populated. Select only the module whose single
optimization exists; both totals describe the finite six-proposal upper scope.

## Mechanisms and conditions that would disprove the claims

| Proposal | Expected mechanism | Required observation / falsification |
|---|---|---|
| Bounded high-water reclamation | Trim unused table/free-list/slab capacity and eligible idle buffers with bounded maintenance. | Post-delete/idle PSS falls versus current016; otherwise no density claim. Include regrowth/maintenance p99 and CPU, scratch and retained reader owners. |
| Optional general metadata | Remove absent metadata, net of side indexes and extra lookups. | Total PSS including side storage falls without TTL/identity/governance/policy changes or hot-read regression. Compact TTL/PERSIST demotion is a separate follow-on. |
| Arena growth | Avoid general fallback within a separately admitted aggregate budget. | Measured fallback/occupancy and PSS improve at identical keys and limits; record retired space/slack/peak scratch. More arena alone does not fix partly full chunks. |
| Hinted reservation | Reserve for actual general occupancy instead of a large nonzero hint. | First-general insertion peak and p99 improve versus identically hinted current016, with later costs retained. None-hint RESP control cannot substantiate this benefit. |
| Packed/demoted aggregates | Remove separate field/member allocations, duplicate zset payloads and high-water Map capacity. | Whole PSS improves net of packed indexes/copying; all binary bytes, order/ranks and threshold oscillation p99 remain valid. |
| Resident cold compression | Save cold resident bytes net of codec headers, cache and decode buffers. | Compressible gains survive hot/cold rotation and burst peaks; entropy controls and CPU/GET p99 do not conceal costs. Exact bytes/TTL/length remain identical. |

Existing32B compact descriptors, small aggregate encodings, released empty4KiB
chunks, sparse access maps and overflow LZ4/Zstd receive no duplicate savings
credit. Current dense18+64B theoretical arena capacity is802816 records per shard,
not an observed fallback boundary; new telemetry must identify actual occupancy.
General buffer reuse is already bounded and policy-conditional. Preserve shared
ownership/OnceLock behavior; allocator freeing alone is not returned OS PSS.

## Memory, correctness and latency in every cell

Record five identity-verified idle and settled samples per phase: server PID1
PSS/RSS, total PSS and idle subtraction, cgroup memory, post-delete/idle/refill PSS,
and0.2s observed peaks. Peaks are sampled maxima, not exact allocation peaks.
Include client/controller PSS, peak memory and CPU separately, plus server+client
deployment totals. Embedded rows use the **entire process**, including callers,
held owners, cache and decode scratch; their idle delta keeps the same threads.
Redis allocator diagnostics are auxiliary, never interchangeable with PSS.

Validate every reply and every retained key/absence after every phase. Record
DBSIZE, exact types/bytes, collection order/ranks, logical counts/bytes, expected
and observed canonical state checksums and command/worker counts. TTL checks use
the same logical deadline/observation schedule relative to each declared arm
anchor. Serial fresh arms have different real absolute Unix deadlines: record
each arm's anchor, absolute deadline and bracketed observation intervals, then
normalize to its anchor only for canonical oracle comparison. Never equate their
absolute timestamps. META_GENERAL observes1s before/after its own declared
deadline with≤10ms verified clock error;
failure to bracket expiry fails admission. Compare PTTL to its query-time interval,
not byte-equal remaining milliseconds. No unplanned expiry or eviction is a win.
Report every worker ID0..N-1, including errors/zero work; sums must match aggregates.

Pipeline1 saturation measures individual response completion; stateful timing is
explicitly per record transition, potentially multiple commands. At pipeline16/64,
existing saturation/curve histograms divide total batch elapsed by successful
operations and repeat that quotient: **batch-amortized ns/op**, not request p99.
True pipelined request p99 needs a separately qualified timing implementation.
All required paired memory/latency rows keep sample rate1; disabled histograms
would be separate throughput-ceiling rows, not scheduled by this plan.

## Targeted later profiles, without expanding every dimension

- **CPU4/16 views:** one representative per claim: HWM_STR, META_GENERAL,
  ARENA_DENSE, HINT_API_1M, AGG_SMALL, COLD_TEXT. Keep the same global dataset,
 16 workers and pipeline1; do not multiply cardinality by shard count.108 rows:
 90 RESP plus18 conditional deployment. Arena pressure at1CPU need not recur
 at16 shards; that limitation stays visible. Redis standalone gets the same CPU
 budget; report its actual consumed cores and label multicore scaling advantage.
 AGG_SMALL inherits the identical typed access/transition contract at each CPU
 budget; additional shards do not change the command mix or retained state.
- **Fixed offered load:** HWM_STR, META_GENERAL, ARENA_DENSE,
  HINT_RESP_CONTROL, AGG_SMALL, COLD_TEXT at10k/50k offered ops/s, CPU1,
 16 workers/P1,3s warmup/20s measure:108 RESP rows. Existing `curve --target-rates`
 measures client-call latency after pacing, excluding scheduled-arrival/queue delay.
 Record offered/achieved, misses/deferred work, backlog and errors; inability to
 sustain a target cannot pass a latency claim. Phase/payload/telemetry adaptations
 are still missing; there is no `saturation` target-rate flag.
 AGG_SMALL's offered target counts logical typed accesses, one command/access;
 its idempotent writes remain separate from lifecycle transitions and validation.
 Existing curve GET/SET cannot run this aggregate selector: a common typed paced
 driver with checked replies, all-worker and offered/miss/backlog accounting must
 first be implemented and independently qualified. Admission remains NULL.
- **Actual fixed-RAM insertion:** one representative per claim as in CPU views,
  CPU1,128MiB soft total-PSS budget under unchanged4GiB physical cap. Insert10k
 strings/checkpoint up to2M, or1k aggregates up to200k. Exhaustively validate and
 sample each checkpoint; last complete in-budget retained state is actual capacity.
 Stop at first crossing/count cap; retain crossing, timeout and censored rows.
 No expiry/eviction/OOM loss or reciprocal ratio counts as inserted capacity.
 This adds54 later rows (45 RESP/9 hinted deployment). Caller-inclusive hinted
 deployment budget is a distinct scope from the RESP server-only budget.
 For AGG_SMALL, one capacity record is one fully populated typed object at the
 declared phase shape; a1k-object batch is not1k field/member commands. Record
 its exact elements/bytes and all creation/transition command counts. Checkpoints
 use identical canonical types/payloads in every arm; no partial object counts.

These later subsets are conditional plans, not part of an implemented runner.
No mode, CPU, pipeline or worker count is chosen after seeing favorable results.
Both optimization/current and optimization/Redis report total throughput and
throughput/actual CPU, p99, memory and errors together. Use medians/ranges of three
rounds; median p99 values are not pooled percentiles or a confidence interval.
Retain the existing matched95% throughput/105% p99 screens and every miss.

## Explicit mode coverage and existing entrypoints

| Mode | Planned row IDs / coverage | Status and interpretation |
|---|---|---|
| RESP | `evaluation:PROFILE:r1..3:ARM` for11 profiles above | Required same TCP/RESP comparison. |
| Shared embedded | `deployment:HINT_API_NONE/1M:r1..3:ARM` | Conditional whole-process versus Redis TCP deployment, including caller/transport. |
| SCNP direct | `scnp-direct:PROFILE:r1..3:ARM`, six fixed-offered selectors | Future conditional54 rows; Redis arm is RESP, so cross-protocol deployment. |
| SCNP shared | `scnp-shared:PROFILE:r1..3:ARM`, same six selectors | Future conditional54 rows; independent mode/driver/ownership qualification. |

SCNP rows are not admitted or substituted for RESP. Default None-hint SCNP/RESP
can only be applicability controls. Shared/direct routing and borrowed/copy APIs
cannot pool results; a claim across these modes needs its mode-specific evidence.
The optional108 SCNP rows are outside the initial and three later subset counts.
Their AGG_SMALL selector retains the same logical typed accesses, idempotent
writes, lifecycle transitions, operation counts and checked state. The existing
SCNP benchmark workers expose GET/SET and provide no qualified aggregate path.
Typed direct/shared support, mode-specific common drivers and admissions remain
NULL; do not flatten collections into strings or infer protocol support from a
mode label. Redis still uses the specified typed RESP commands in those future
cross-protocol comparisons, with deployment latency labeled by its actual path.

Existing executable sources, with hashes in plan.json:

- `run-memory-density-benchmark.py`: fresh string containers/PSS plus saturation;
  it checks DBSIZE and only three sampled GETs, not exhaustive new lifecycles.
- `compact_resp_scenarios` and `run-compact-storage-scenarios.py`: fixed24 traces,
  exhaustive state validation and72-row access/stateful packages; this JSON is
  not an accepted controller input. One-field hash/list coverage is not the new
  multi-element aggregate, cold-codec, hinted API or threshold driver.
- `saturation`: closed-loop RESP/SCNP/embedded backend support; no target-rate flag.
  `curve`: paced GET/SET with `--target-rates`/`--submitters`; lifecycle, all-worker,
  missed/backlog and scheduled-arrival instrumentation need qualification.
  Their existing GET/SET workers cannot consume the typed aggregate access
  contract, including their SET warmup; that future common driver is unimplemented.
- `run-embedded-benchmark-suite.sh` and `compact_shared_read_cost`: existing
  backend/owned-read entrypoints, not ready explicit-hint lifecycle or Redis
  caller-inclusive comparative controllers. Common fresh mode-specific ELFs,
  exact server images/config and source/build receipts are still required.

Missing work is targeted: phase selectors and expected-state/absolute-expiry
oracle; unique per-key payloads; collection/arena/reclaim/decode telemetry;
hinted shared API harness; actual-capacity checkpoints; allN worker/error vectors;
offered-load miss/backlog reporting; and precise mode/resource/runtime admission.
Typed aggregate access/transition clocks, reply checks and mode-specific command
paths are also missing; all such implementations and admissions remain NULL.
String TTL-cohort selection, absence-safe touches, read-only all-TTL windows and
controlled-expiry exclusions also need the fresh common driver and qualification.
Each new affected driver requires focused Adam regression and independent review.
There is no new executable benchmark, product change or helper implementation here.

## Finite resources, package arithmetic and gates

All jobs are serial and HELD until genuine guard restoration, source/native,
strict EARLY, original378/current scaling qualification and new exact runtime/
resource/ownership gates are independently accepted. No new full-review route or
human approval flow is introduced. Preserve Adam free disk≥100GB, global guard
age≤90s, STOP absent, whole-owned RUN≤30GiB including retained roots/image storage,
5s watchdog and complete raw finite copies. Fresh source/image/build pins are NULL.

Planned server: CPU1 (later4/16),4GiB/zero swap/pids512. Initial client:
4CPU/2GiB/zero swap/pids512; separately admitted scaling client16CPU/2GiB.
Controller:1CPU/2GiB/pids64; build:4CPU/16GiB/pids512/jobs4, exclusive with runtime.
These are proposed ceilings, not host reservations. Controller, client and server
are siblings under an explicitly admitted parent, not nested under a1CPU leaf.
Initial runtime sums to6CPU/8GiB/1088tasks; scale maximum33CPU/8GiB/1088tasks;
build+controller5CPU/18GiB/576tasks. Parent maximum33CPU/18GiB/1088tasks,
zero swap, requires exact real ancestors/phase charges and reaping before lowering.
Use disjoint client/server CPU sets only with actual safe host availability;
otherwise label shared/unreserved and hold a qualified headline. Client CPU,
throttle, quotas/affinity and host contention must rule out a client ceiling.

New packages contain at most6 rows, each≤840s complete lifecycle.6×840=5040s
leaves360s overhead in a90min controller.45min build+90min controller+5min
cleanup remains **140min**;128MiB/file and1GiB/package output remain finite.
Large-cardinality validation may time out: fail and retain it, never extend clocks.
All-six initial evaluation upper bound is20 packages/46h40m; diagnostics are
13 packages/30h20m separately. Later scaling18, offered-load18 and capacity9
packages total105h; optional SCNP adds18 packages/42h. These are serial wall caps,
not runtime estimates/reservations or an instruction to execute every future row.
Per selected proposal, reuse exact qualified rows and run only its prescribed gaps.
No test execution, new measurement, code push, or qualification is established.
