# Design: Hash field TTL family (Redis 7.2 / 7.4)

Implementation spec for the v7 "Group B" commands. This is the substantial part
of Redis 7.x compatibility: per-field expiration on hashes, plus the read/get
variants that honor it.

Status: **design only — not yet implemented.** This document is the contract for
the implementation PR.

## Scope — 11 commands

| Command | Redis | Semantics |
|---------|-------|-----------|
| `HEXPIRE key seconds [NX\|XX\|GT\|LT] FIELDS numfields field...` | 7.2 | set per-field TTL in seconds (relative) |
| `HPEXPIRE` | 7.2 | …in milliseconds (relative) |
| `HEXPIREAT` | 7.2 | absolute unix seconds |
| `HPEXPIREAT` | 7.2 | absolute unix milliseconds |
| `HTTL key FIELDS numfields field...` | 7.2 | remaining TTL per field, seconds |
| `HPTTL` | 7.2 | remaining TTL per field, milliseconds |
| `HEXPIRETIME` | 7.2 | absolute expiry per field, seconds |
| `HPEXPIRETIME` | 7.2 | absolute expiry per field, milliseconds |
| `HPERSIST key FIELDS numfields field...` | 7.2 | remove per-field TTL |
| `HGETEX key [EX\|PX\|EXAT\|PXAT\|PERSIST] FIELDS numfields field...` | 7.4 | get values, optionally (re)set/clear TTL |
| `HGETDEL key FIELDS numfields field...` | 7.4 | get values and delete those fields |

### Per-field reply conventions (Redis-exact)

The `*EXPIRE*`, `*TTL`, `*EXPIRETIME`, and `HPERSIST` commands return an **array
of integers, one per requested field**, with these sentinel codes:

- `HEXPIRE`/`HPEXPIRE`/`HEXPIREAT`/`HPEXPIREAT` per field:
  - `-2` field (or key) does not exist
  - `0` condition (NX/XX/GT/LT) not met, no change
  - `1` TTL set
  - `2` field deleted because the resolved time is already in the past
    (HEXPIREAT/HPEXPIREAT with a past timestamp, or HEXPIRE with seconds ≤ 0)
- `HTTL`/`HPTTL`/`HEXPIRETIME`/`HPEXPIRETIME` per field:
  - `-2` no such field/key
  - `-1` field exists but has no TTL
  - `>= 0` the TTL / expiry time
- `HPERSIST` per field:
  - `-2` no such field/key
  - `-1` field exists but had no TTL
  - `1` TTL removed

### Missing-key / wrong-type behavior (verified against Redis 7.4)

These differ by command family — confirmed empirically, not assumed:

- **Missing key** (verified against redis 7.4.5 — these do NOT error):
  - `HEXPIRE`/`HPEXPIRE`/`HEXPIREAT`/`HPEXPIREAT`/`HTTL`/`HPTTL`/`HEXPIRETIME`/
    `HPEXPIRETIME`/`HPERSIST` → integer array with one `-2` per requested field.
  - `HGETEX`/`HGETDEL` → an array of nils, one nil per requested field.
- **Wrong type** (key holds a non-hash): all return
  `WRONGTYPE Operation against a key holding the wrong kind of value`.
- **Past/zero expiry** (`HEXPIREAT` past, or `HEXPIRE 0`): the field is deleted
  and that field's code is `2`. If it was the last field, the hash key is
  removed (verified: `HGET` of the deleted field returns nil).
- **HGETEX** returns the field *values* (bulk array), optionally applying an
  EX/PX/EXAT/PXAT/PERSIST TTL op as a side effect; missing field → nil element.
- **HGETDEL** returns the field values and deletes those fields.

## Storage representation

**Correction (from reading the code):** the hash value is NOT a struct. It is an
enum stored in a per-bucket slab (`storage/redis_objects.rs`):

```rust
enum HashObject {
    Small(SmallVec<[(Bytes, Bytes); 4]>),   // inline for <= 4 fields
    Map(FastHashMap<Bytes, Bytes>),          // promoted beyond that
}
// held as: hash_slab: ObjectSlab<HashObject>, keyed via `hashes: SlotMap`.
```

Key-level expiry already lives at the **bucket** level, next to the slab:

```rust
pub(crate) struct RedisObjectBucket {
    hashes: SlotMap,
    expire_at_ms: FastHashMap<Bytes, u64>,   // KEY-level TTL
    hash_slab: ObjectSlab<HashObject>,
    ...
}
```

So field TTLs are cleanest as a **sibling bucket-level map**, mirroring
`expire_at_ms`, rather than a field on the `HashObject` enum (which would force
touching both the `Small` and `Map` variants and the slab):

```rust
    /// Absolute field expiry (unix ms), keyed by (hash key, field). Empty when
    /// no field anywhere has a TTL, so TTL-free hashes pay nothing.
    hash_field_expire_at_ms: FastHashMap<(Bytes, Bytes), u64>,
```

Design choices (unchanged from the original intent):

- Absolute ms internally; seconds/relative converted at the command layer.
- Lazy expiry: a field is logically gone once `now_ms >= expiry`. Reads filter,
  writes opportunistically purge. No background sweeper needed for correctness.
- Empty-hash collapse: when the last live field expires/deletes, remove the hash
  object and its `hashes` slot, exactly like `HDEL` of the last field.
- `HSET`/`HSETNX`/`HINCRBY*` on a field clear any stale `(key, field)` expiry.
- Key deletion / overwrite / key-level expiry must also drop all
  `(key, *)` field-expiry entries to avoid leaks.

Design choices:

- **Absolute ms internally.** All four set-variants normalize to absolute unix
  milliseconds (mirrors how key-level expiry is already stored). Seconds vs ms
  and relative vs absolute are converted at the command layer.
- **Lazy allocation.** `expirations` stays `None` until the first field TTL is
  set, so the common no-TTL hash is unchanged in size and hot-path cost.
- **Lazy expiry (no background sweeper required for correctness).** A field is
  logically gone once `now_ms >= expiry`. Reads filter expired fields; writes
  may opportunistically purge them. This matches the existing key-level lazy
  model and avoids a new timer subsystem.
- **Empty-hash collapse.** When the last live field of a hash expires or is
  deleted, the hash object itself must be removed from the bucket (Redis deletes
  an empty hash), exactly like `HDEL` of the last field today.

## The hard part: every existing hash read must filter expired fields

This is the bulk of the work and the main regression risk. Each of these
operations (in `storage/redis_objects/bucket_hash.rs` and the store API in
`storage/embedded_store/objects/hashes.rs`) must treat an expired field as
absent:

`HGET`, `HMGET`, `HGETALL`, `HKEYS`, `HVALS`, `HLEN`, `HEXISTS`, `HSCAN`,
`HRANDFIELD`, `HSTRLEN`, and the existing `*_visit` fast-path variants.

Recommended approach: a single internal helper on `HashObject`, e.g.
`fn field_is_live(&self, field, now_ms) -> bool` and
`fn get_live(&self, field, now_ms) -> Option<&[u8]>`, and route every read
through it. `HLEN` must count only live fields. To keep `HLEN`/`HGETALL` O(n)
rather than re-checking a side map per element when no TTLs exist, short-circuit
on `expirations.is_none()`.

Writes (`HSET`, `HSETNX`, `HINCRBY`, `HINCRBYFLOAT`) clear any stale expiry for a
field they overwrite (Redis: setting a field via HSET removes its TTL).

## Command-layer plumbing (per command)

For each command: new file under `crates/shardcache-redis/src/commands/hash/`,
`define_redis_command!`, parse + validate (`FIELDS numfields` arity, NX/XX/GT/LT
mutual exclusivity for the EXPIRE family), call the new store API, build the
per-field integer array reply.

Wiring (mirror exactly what WAITAOF/SINTERCARD did — this is the part that has a
checklist-shaped failure mode):

1. `crates/shardmap/src/commands.rs` — `#[path]` module decl **and** the
   `RAW_DIRECT_CATALOG` entry, per command.
2. `crates/shardmap/src/server/commands.rs` — add to the correct length bucket
   (`RAW_DIRECT_LEN_N` by command-name length: HTTL=4, HPTTL=5, HEXPIRE=7,
   HGETEX=6, HGETDEL=7, HPERSIST=8, HEXPIREAT=9, HPEXPIRE=8, HPEXPIREAT=10,
   HEXPIRETIME=11, HPEXPIRETIME=12). Verify each bucket length before inserting.
3. `crates/shardmap/src/server/tests.rs` — a RESP2 smoke arm for **every** new
   command (the `raw_resp2_supported_command_surface_has_smoke_coverage` test
   enforces this), plus functional tests.
4. `benchmarks/src/redis_command_cases.rs` — a `case!` for each, **and** add each
   name to the `BENCHMARKED_COMMANDS` array (the
   `benchmark_cases_cover_declared_commands` test enforces this; it is a separate
   array from the cases — do not confuse them).
5. Regenerate `docs/REDIS_COMPATIBILITY.md` via the manifest binary.

## SCNP note

These are keyed hash commands, so they route fine over the RESP path and the
SCNP RESP-command fallback. Adding compact one-byte SCNP opcodes for them is
optional and out of scope for the first PR.

## Test plan

- Per-command functional tests in `shardmap/src/server/tests.rs`: set/get TTL,
  the NX/XX/GT/LT conditions, the `-2/-1/0/1/2` sentinels, missing-key
  per-field `-2` replies, past-timestamp field deletion, and empty-hash collapse.
- Lazy-expiry tests: set a short TTL, advance the clock (the store already has a
  test seam for `now_ms`), confirm the field is invisible to **every** reader
  (HGET/HGETALL/HLEN/HKEYS/HVALS/HSCAN/HEXISTS/HRANDFIELD/HMGET) and that the
  hash collapses when the last field expires.
- HGETEX option matrix (EX/PX/EXAT/PXAT/PERSIST and none) and HGETDEL.
- Gate the commit on `cargo test -p shardmap -p shardcache-redis
  -p shardcache-benchmarks --features redis-server --lib` returning exit 0.

## Out of scope (tracked separately)

- General Redis functions execution remains out of scope. The `redis-functions`
  feature accepts `FUNCTION`/`FCALL` with empty-registry semantics so Redis 7
  clients do not hit an unsupported-command path.
- Minor 7.x subcommand stubs: `CLIENT NO-EVICT` / `NO-TOUCH` / `UNPAUSE`,
  `COMMAND LIST FILTERBY`. Small; can ride along or be a follow-up.
