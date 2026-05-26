# fast-cache

`fast-cache` is an embedded-first, in-memory key-value database written in
Rust. It provides a direct Rust API by default, an optional RESP/FCNP TCP
server for applications that want network access, and Redis/Valkey
compatibility as an opt-in extension.

The public documentation is centered on rustdoc, the same way users encounter
open source crates on crates.io:

- [Crate guide and API docs](https://docs.rs/fast-cache)
- [Facade crate README](crates/fast-cache/README.md)
- [Core crate README](crates/fast-cache-core/README.md)
- [Safety notes](crates/fast-cache-core/SAFETY.md)
- [Project structure](docs/PROJECT_STRUCTURE.md)
- [Redis compatibility manifest](docs/REDIS_COMPATIBILITY.md)
- [Proof gates](docs/PROOF_GATES.md)
- [Operations](docs/OPERATIONS.md)

## Quick Start

Add the embedded database to a Rust project:

```toml
[dependencies]
fast-cache = "0.2"
```

```rust
use fast_cache::FastMap;

let cache = FastMap::new();
cache.insert_slice(b"user:42", b"ready");

{
    let value = cache.get(b"user:42").expect("cache hit");
    assert_eq!(value.value(), b"ready");
}
cache
    .get_mut(b"user:42")
    .expect("cache hit")
    .set_slice(b"updated");
assert_eq!(cache.get(b"user:42").unwrap().value(), b"updated");
assert_eq!(cache.get_owned(b"user:42").unwrap().as_ref(), b"updated");
```

`FastMap` is the DashMap-like embedded handle: it is cloneable, internally
sharded, and exposes borrowed reads through `get`. Use `get_owned` when the
caller needs a value handle that outlives the shard read guard. Use `get_mut`
when an existing point value should be replaced or removed while preserving its
TTL.

The public `fast-cache` crate is a facade over `fast-cache-core`. It exposes
layered surfaces: `FastMap` for cloneable embedded use, `FastCache` as the
cache-flavored alias, `embedded::ShardedEngine` for the full sharded core used
by server mode, `embedded::LocalEmbeddedStore` for pinned owner-local workers,
the optional `server` feature for RESP/FCNP access from other processes, and
`redis` when Redis/Valkey data-type semantics are required.

For callers that need raw in-place mutation, the opt-in
`mutable-value-slices` feature adds `value_mut_no_ttl()` to embedded mutation
guards. It returns `&mut [u8]` only for uniquely-owned no-TTL values. TTL-backed
values are rejected because this path intentionally skips the TTL-preserving
replacement logic; use `set_slice` for TTL entries.

Install the core server binary:

```bash
cargo install fast-cache --features server --locked
fast-cache-server --data-dir ./var/fast-cache
```

From a source checkout:

```bash
cargo run -p fast-cache --features server --bin fast-cache-server -- --data-dir ./var/fast-cache
```

For a Redis/Valkey-compatible deployment, enable the compatibility extension:

```bash
cargo install fast-cache --features redis-server --locked
```

Or run the server with Docker Compose:

```bash
docker compose up --build fast-cache
```

The Compose service publishes the RESP/FCNP fanout port on `127.0.0.1:6380`
and enables direct shard-owned FCNP ports on `127.0.0.1:6501-6504` by default.
Those direct ports match the default `FAST_CACHE_SHARD_COUNT=4`; if you change
the shard count, update `FAST_CACHE_DIRECT_SHARD_PORT_RANGE` and
`FAST_CACHE_DIRECT_SHARD_BASE_PORT` together. Persistent data is stored in the
`fast-cache-data` volume. For same-host benchmark images, build with native CPU
codegen:

```bash
RUSTFLAGS="-C target-cpu=native" docker compose build fast-cache
```

In another shell:

```bash
printf '*1\r\n$4\r\nPING\r\n' | nc 127.0.0.1 6380
printf '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n' | nc 127.0.0.1 6380
printf '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n' | nc 127.0.0.1 6380
```

### Docker Configuration

The Docker image runs `fast-cache-server`. Compose exposes two TCP surfaces:

| Surface | Default | Purpose |
| --- | ---: | --- |
| Fanout port | `6380` | RESP and FCNP requests accepted on one listener |
| Direct shard ports | `6501-6504` | FCNP clients can route directly to shard-owned listeners |

Compose variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `FAST_CACHE_PORT` | `6380` | Host/container fanout port |
| `FAST_CACHE_SHARD_COUNT` | `4` | Server shard count |
| `FAST_CACHE_DIRECT_SHARD_PORTS` | `1` | Enables shard-owned direct FCNP listeners |
| `FAST_CACHE_DIRECT_SHARD_BASE_PORT` | `6501` | First direct shard port inside the container |
| `FAST_CACHE_DIRECT_SHARD_PORT_RANGE` | `6501-6504` | Host/container port range published by Compose |
| `FAST_CACHE_MAX_CONNECTIONS` | `4096` | Server connection limit |
| `FAST_CACHE_FEATURES` | `server` | Cargo features used while building the image |
| `RUSTFLAGS` | empty | Optional build flags, for example native CPU tuning |

`FAST_CACHE_DIRECT_SHARD_PORT_RANGE` is a Compose publishing setting; the
server itself reads `FAST_CACHE_DIRECT_SHARD_PORTS` and
`FAST_CACHE_DIRECT_SHARD_BASE_PORT`. Keep the published range length equal to
`FAST_CACHE_SHARD_COUNT`.

For example, a 16-shard local deployment can be started with:

```bash
FAST_CACHE_SHARD_COUNT=16 \
FAST_CACHE_DIRECT_SHARD_BASE_PORT=6501 \
FAST_CACHE_DIRECT_SHARD_PORT_RANGE=6501-6516 \
docker compose up --build fast-cache
```

## Benchmark Highlights

`fast-cache` is designed around two deployment shapes:

- Embedded Rust APIs for direct in-process use.
- TCP server APIs: RESP command frames and FCNP for Rust clients that can route
  directly to shard-owned server ports. Redis/Valkey compatibility is enabled
  with `redis`.

Current benchmark artifacts are standalone Linux runs. Throughput rows
disable latency sampling so the harness can measure ceiling throughput; latency
should be measured in separate sampled reruns before making latency claims.

### TCP Server

On Linux, with server processes pinned to CPUs `0-15` and benchmark
clients pinned to CPUs `16-31`, the current large TCP matrix compares
fast-cache against Redis, Valkey, and Dragonfly. The headline small-value row
uses `64B` values, `100k` keys, an `80/20` read/write mix, `64` clients, and
pipeline depth `64`:

| Backend | Throughput | Server CPU |
| --- | ---: | ---: |
| fast-cache FCNP direct | 31.18M ops/s | 14.6 vCPU |
| fast-cache RESP | 19.76M ops/s | 16.0 vCPU |
| Redis | 1.15M ops/s | 1.0 vCPU |
| Valkey | 1.15M ops/s | 1.0 vCPU |
| Dragonfly | 4.10M ops/s | 15.9 vCPU |

For large values, the comparison becomes bandwidth and copy dominated. On the
`64KiB` `80/20` row, fast-cache FCNP direct reaches `9.26 GB/s`, fast-cache
RESP reaches `9.24 GB/s`, Dragonfly reaches `6.41 GB/s`, and Redis/Valkey are
around `2.4 GB/s`.

Same-core pipelining is also important. In an earlier Redis-only pipeline
sweep with fast-cache pinned to one CPU and Redis running as the single-threaded
baseline, `64B`, `80/20`, `64` clients, and pipeline depth `64` measured:

| Backend | Throughput | Server CPU |
| --- | ---: | ---: |
| fast-cache FCNP direct | 3.26M ops/s | 1.0 vCPU |
| fast-cache RESP | 2.28M ops/s | 1.0 vCPU |
| Redis | 1.20M ops/s | 1.0 vCPU |

For fixed-load CPU efficiency, the target-rate curve uses the same `64B`,
`80/20`, `64` client, pipeline depth `64` shape and measures server CPU while
holding the requested rate:

| Target | fast-cache FCNP direct | fast-cache RESP | Redis |
| ---: | ---: | ---: | ---: |
| 100K ops/s | 0.052 vCPU | 0.060 vCPU | 0.098 vCPU |
| 1M ops/s | 0.439 vCPU | 0.531 vCPU | 0.909 vCPU |
| 2M ops/s | 0.843 vCPU | 0.975 vCPU | saturated at 1.23M |

See [Redis Head-to-Head Benchmarks](benchmarks/REDIS_HEAD_TO_HEAD_BENCHMARKS.md)
for the curated 1-vCPU and 16-vCPU summary, and
[fast-cache vs Redis, Valkey, and Dragonfly over TCP](benchmarks/FAST_CACHE_VS_REDIS_TCP.md)
for the full TCP matrix, fixed-shape rows, pipeline sweeps, caveats, and
artifact paths.

### Embedded Rust

The embedded release matrix validates the direct, shared-handle, TTL, and LRU
paths against Rust cache baselines. A current no-TTL direct-shard headline on
Linux with `16` workers and `64B` values reaches:

| Workload | Throughput |
| --- | ---: |
| GET | 422.82M ops/s |
| SET | 114.85M ops/s |
| 80/20 | 253.88M ops/s |

Capacity-bounded LRU and larger-value rows are documented separately because
their bottlenecks are different from the small-value hot path. The 64KiB
write-only LRU row is the current large-value write-pressure outlier, but it is
not representative of normal LRU behavior: the same no-TTL, 25%
resident-capacity LRU matrix shows fast-cache embedded direct at
`756.93M ops/s` on 64KiB read-only and `1.45M ops/s` on 64KiB `80/20`,
compared with Moka at `3.31M ops/s` and `1.42M ops/s` respectively. On a
smaller 4KiB LRU row, fast-cache embedded direct reaches `676.37M ops/s`
read-only and `26.70M ops/s` on `80/20`.

Large-value embedded GET rows are reported in the default reference-read mode:
they measure lookup plus borrowed value access, not copying 1MiB into a new
buffer on every hit. The resulting GB/s is a logical payload rate, not physical
data throughput. Use `--read-mode copy` in the benchmark harness for
materialized read comparisons. See
[fast-cache Embedded Release Matrix](benchmarks/FAST_CACHE_EMBEDDED_RELEASE.md)
and [benchmarks/README.md](benchmarks/README.md) for harness details and
reproduction commands.

## What Is Public

This repository now contains the open source crate surface:

- `crates/fast-cache`: public facade crate and optional `fast-cache-server`
  binary.
- `crates/fast-cache-core`: core embedded cache, storage, protocol,
  persistence, replication, and server runtime implementation.
- `crates/fast-cache-redis`: Redis/Valkey compatibility crate. Its source root
  is `crates/fast-cache-redis/src`; core includes it by path only while the
  remaining extension points are being narrowed.
- `crates/fcnp-client-rs`: blocking Rust client for the native FCNP TCP
  protocol and direct shard routing.
- `crates/fast-cache-runtime`: Rust-native runtime and GPU transfer layer for
  model-serving integrations.
- `crates/fast-cache-py`: PyO3 bindings used by benchmark and integration
  adapters.
- `benchmarks`: local comparison harnesses for embedded and server
  performance validation. They are not part of the published crate.
- `integrations`: LMCache and vLLM adapter code.
- `.github/workflows/ci.yml`: formatting, tests, rustdoc generation, and crate
  packaging checks.
- `fast-cache.toml.example`: example server configuration.
- `CONTRIBUTING.md`, `SECURITY.md`, `RELEASE.md`, and `LICENSE`.
- `CHANGELOG.md` and `docs/RELEASE_0_2_READINESS.md` for release notes,
  validation commands, and known compatibility limits.
- `docs/REDIS_COMPATIBILITY.md`, generated from the live command matrix
  registry, for supported commands, expected-error standalone behavior, and
  any missing Redis command coverage.
- `scripts/proof-gate.sh` and focused checks under `scripts/` for local and CI
  validation of docs, feature flags, and release proofs.

For a contributor-oriented map of the workspace, crate internals, generated
artifact policy, and common change locations, see
[Project Structure](docs/PROJECT_STRUCTURE.md).

## Feature Flags

Public product surfaces:

| Feature | Purpose |
| --- | --- |
| `default` | Embedded, sharded Rust API. Equivalent to `sharded`. |
| `embedded` | Minimal in-process cache API. |
| `sharded` | Embedded sharded storage and owner-local APIs; implies `embedded`. |
| `redis` | Redis/Valkey object types, command families, and wrong-type behavior without server networking. |
| `redis-compat` | Compatibility alias for `redis`. |
| `server` | RESP/FCNP `fast-cache-server` runtime without the full Redis compatibility catalog. |
| `redis-server` | Redis/Valkey-compatible server build; implies `server` and `redis`. |

Runtime integrations:

| Feature | Purpose |
| --- | --- |
| `monoio` | Linux-only server runtime selected with `FAST_CACHE_USE_MONOIO=1`; portable builds continue to use Tokio/std. |
| `telemetry` | Integrates with `fast-telemetry`. |
| `cuda` | Exposes GPU-facing configuration and transfer descriptors. |

Internal and benchmark knobs:

| Feature | Purpose |
| --- | --- |
| `mutable-value-slices` | Exposes no-TTL mutable slice access for uniquely owned embedded values. |
| `no-ttl` | Specializes shared embedded point-key hot paths for TTL-free deployments. |
| `experimental-no-ttl-point-hot-path` | Benchmark-only point-key hot path; implies `no-ttl`. |
| `shared-parking-lot-lock` | Benchmark comparison knob for shared embedded store locks. |
| `embedded-read-biased-lock` | Benchmark comparison knob for server/direct embedded shard locks. |
| `unsafe` | Opts into reviewed unsafe hot paths for lower overhead. |

Tokio direct mode writes responses inline by default, avoiding a per-connection
writer task for Redis-style request/response traffic. Set
`FAST_CACHE_TOKIO_WRITER_MODE=split` to restore the split read/write handoff
for deployments that prefer full-duplex connection overlap. Monoio still uses
`bytes-handoff` for connection read buffering. Its server driver defaults to
`FAST_CACHE_MONOIO_DRIVER=auto`: io_uring for one worker, legacy poll for
multi-worker socket runs; set `FAST_CACHE_MONOIO_DRIVER=legacy|io_uring` to
force either path. With `FAST_CACHE_DIRECT_SHARD_PORTS=1`,
the server also binds one listener per shard, starting at
`FAST_CACHE_DIRECT_SHARD_BASE_PORT` or the fanout port + 1, so direct clients
can route while fanout RESP/FCNP stays available. WAL TCP export and native
replication use separate Linux-only monoio switches:
`FAST_CACHE_WAL_TCP_USE_MONOIO=1` and `FAST_CACHE_REPLICATION_USE_MONOIO=1`.

## Development

```bash
./scripts/proof-gate.sh quick
```

Use `./scripts/proof-gate.sh redis` before merging Redis compatibility changes
and `./scripts/proof-gate.sh release` before tagging or publishing.

For local benchmark and profiling builds, use the repo-level native CPU
aliases:

```bash
cargo native -p fast-cache-benchmarks --bin saturation
cargo native-server
```

These compile with `-C target-cpu=native`, so the resulting binaries are tuned
for the current host CPU and should not be treated as portable release
artifacts.

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution expectations and
[SECURITY.md](SECURITY.md) for vulnerability reporting.

## License

Apache-2.0. See [LICENSE](LICENSE).
