# fast-cache

`fast-cache` is an embedded-first, in-memory key-value database written in
Rust. It provides a direct Rust API by default and an optional Redis-compatible
TCP server for applications that want network access.

The public documentation is centered on rustdoc, the same way users encounter
open source crates on crates.io:

- [Crate guide and API docs](https://docs.rs/fast-cache)
- [Crate README](crates/fast-cache/README.md)
- [Safety notes](crates/fast-cache/SAFETY.md)

## Quick Start

Add the embedded database to a Rust project:

```toml
[dependencies]
fast-cache = "0.1"
```

```rust
use fast_cache::storage::EmbeddedStore;

let cache = EmbeddedStore::new(16);
cache.set(b"user:42".to_vec(), b"ready".to_vec(), None);

{
    let value = cache.get_ref(b"user:42").expect("cache hit");
    assert_eq!(value.value(), b"ready");
}
cache
    .get_mut(b"user:42")
    .expect("cache hit")
    .set_slice(b"updated");
assert_eq!(cache.get_ref(b"user:42").unwrap().value(), b"updated");
assert_eq!(cache.get(b"user:42"), Some(b"updated".to_vec()));
```

Use `get_ref` for zero-copy embedded reads when the value is only needed while
the cache borrow is alive. Use `get` when the caller needs an owned
materialized `Vec<u8>`. Use `get_mut` when an existing point value should be
replaced or removed while preserving its TTL.

For callers that need raw in-place mutation, the opt-in
`mutable-value-slices` feature adds `value_mut_no_ttl()` to embedded mutation
guards. It returns `&mut [u8]` only for uniquely-owned no-TTL values. TTL-backed
values are rejected because this path intentionally skips the TTL-preserving
replacement logic; use `set_slice` for TTL entries.

Install the optional server binary:

```bash
cargo install fast-cache --features server --locked
fast-cache-server --data-dir ./var/fast-cache
```

From a source checkout:

```bash
cargo run -p fast-cache --features server --bin fast-cache-server -- --data-dir ./var/fast-cache
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
- TCP server APIs: RESP for Redis-compatible clients and FCNP for Rust clients
  that can route directly to shard-owned server ports.

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

See
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

- `crates/fast-cache`: the library crate and optional `fast-cache-server`
  binary.
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

## Feature Flags

- `embedded`: default embedded Rust database API.
- `sharded`: default sharded storage and owner-local embedded API.
- `server`: builds the Redis-compatible `fast-cache-server` binary.
- `monoio`: enables the Linux-only server runtime selected with
  `FAST_CACHE_USE_MONOIO=1`. The server still uses `bytes-handoff` for
  connection read buffering, using its monoio adapter on Linux. With
  `FAST_CACHE_DIRECT_SHARD_PORTS=1`, the server also binds one listener per
  shard, starting at `FAST_CACHE_DIRECT_SHARD_BASE_PORT` or the fanout port + 1,
  so direct clients can route while fanout RESP/FCNP stays available. Monoio
  writer experiments are selected with
  `FAST_CACHE_MONOIO_SAFE_WRITER=inline|split|writev`. WAL TCP export and
  native replication have separate Linux-only monoio switches,
  `FAST_CACHE_WAL_TCP_USE_MONOIO=1` and
  `FAST_CACHE_REPLICATION_USE_MONOIO=1`, so those paths can be benchmarked
  independently. Tokio/std remain the portable defaults.
- `telemetry`: integrates with `fast-telemetry`.
- `cuda`: exposes GPU-facing configuration and transfer descriptors.
- `fast-point-map`: enables the experimental point-map storage path.
- `unsafe`: opts into reviewed unsafe hot paths for lower overhead.

## Development

```bash
cargo fmt --all -- --check
cargo test -p fast-cache
cargo test -p fast-cache --features unsafe
cargo doc -p fast-cache --no-deps --all-features
cargo package -p fast-cache --locked
```

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
