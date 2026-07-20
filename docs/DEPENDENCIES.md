# Dependency Inventory

This is the complete locked dependency inventory for the public Shardcache
workspace. It includes publishable and vendored test-support workspace packages
plus normal, build, development, optional, platform-specific, and transitive
packages reachable by the all-feature workspace graph. A deployed binary
includes only the subset selected by its package, Cargo features, and target.

The inventory is generated from `Cargo.lock` with:

```sh
./scripts/generate-dependency-docs.sh
```

CI runs the same command with `--check`. Do not edit the package tables by
hand. Versions are exact resolved versions, while crate manifests retain their
declared compatible version ranges.

## Feature-Sensitive Dependencies

| Dependency | Activated By | Purpose |
| --- | --- | --- |
| `object_store` | `object-overflow-s3` | S3/RustFS-compatible object transport. |
| `redis` | `kv-overflow-redis` | Redis/Valkey overflow transport with Rustls-backed TLS URLs. |
| `rustls`, `tokio-rustls`, `rustls-pemfile`, `ring` | `scnp-tls`, `active-sync-tls` | TLS 1.3, mTLS, certificate parsing, and cryptography for SCNP overflow and active-active peer sync. |
| `shardcache-client-rs` | `kv-overflow` | SCNP framing and direct replica communication. |
| `lz4_flex`, `zstd` | Overflow features | Optional value and object compression. |
| `crc32fast`, `sha2` | Overflow and active-sync integrity, TLS identity, `active-sync-consensus-ordered-eventual` | Envelope and sync-block integrity, certificate fingerprints, and content-addressed conflict claims. |
| `deterministic-test-env` | Test builds only | Non-published harness vendored from pinned Blossom revision `46750a97a70fd301e3e6f3255316c1d7e837a9dd` for replayable active-sync fault schedules. |
| `tokio`, `flume` | Server and asynchronous overflow paths | Event loops, sockets, timers, bounded asynchronous lanes, and shutdown. |
| `bytes-handoff`, `monoio` | Optional server transport | Buffer handoff and Linux transport experiments. |
| `fast-telemetry` | `telemetry` | Metrics integration. |
| `serde`, `serde_json`, `toml` | Configuration and persistence metadata | Structured configuration and metadata encoding. |
| `crossbeam-channel`, `crossbeam-utils`, `parking_lot`, `rblock` | Core concurrency | Bounded channels and shard-local synchronization. |
| `hashbrown`, `indextreemap`, `smallvec`, `xxhash-rust` | Core storage and routing | Tables, ordered indexes, inline collections, and stable fast hashing. |

TLS dependency policy is enforced by
[`scripts/check-tls-dependency-policy.sh`](../scripts/check-tls-dependency-policy.sh):
the all-feature production graph must not contain OpenSSL, native-tls, or an
OpenSSL-backed Rustls provider.

The publishable `shardmap` `active-sync-consensus-ordered-eventual` graph deliberately has no
Blossom runtime dependency. It exposes a bounded `BlossomConflictConsensus`
service boundary. The standalone source-only `shardmap-blossom-bridge` package
is excluded from the public workspace so normal builds need no private Git
credentials. It pins the Blossom runtime and implements quorum finality
verification behind a loopback or identity-bound mTLS proxy. The bridge
transmits conflict claims only; active-sync WAL blocks and values never cross
this boundary.

## Standalone Source Integration

`crates/shardmap-blossom-bridge` is a non-published standalone crate. Its
manifest pins `blossom` `2.0.0-pre-release` to Git revision
`46750a97a70fd301e3e6f3255316c1d7e837a9dd`; it also directly depends on
`parking_lot`, `serde`, `serde_json`, `sha2`, `shardmap`, and `tokio`, with
`tempfile` for tests. Its adjacent `Cargo.lock` records the complete exact
standalone graph. Run its tests separately in an authenticated source checkout.
Because it is outside the public workspace, those packages are not included in
the generated tables below.

## Workspace Packages (9)

| Package | Version | License | Manifest |
| --- | --- | --- | --- |
| `deterministic-test-env` | `2.0.0-pre-release` | MIT | `crates/deterministic-test-env/Cargo.toml` |
| `shardcache-benchmarks` | `0.7.2` | Apache-2.0 | `benchmarks/Cargo.toml` |
| `shardcache-c` | `0.7.2` | Apache-2.0 | `crates/shardcache-c/Cargo.toml` |
| `shardcache-client-rs` | `0.7.2` | Apache-2.0 | `crates/shardcache-client-rs/Cargo.toml` |
| `shardcache-formal` | `0.7.2` | Apache-2.0 | `crates/shardcache-formal/Cargo.toml` |
| `shardcache-py` | `0.7.2` | Apache-2.0 | `crates/shardcache-py/Cargo.toml` |
| `shardcache-runtime` | `0.7.2` | Apache-2.0 | `crates/shardcache-runtime/Cargo.toml` |
| `shardcache` | `0.7.2` | Apache-2.0 | `crates/shardcache/Cargo.toml` |
| `shardmap` | `0.7.2` | Apache-2.0 | `crates/shardmap/Cargo.toml` |

## Third-Party Packages (380)

| Package | Version | License | Source |
| --- | --- | --- | --- |
| `adler2` | `2.0.1` | 0BSD OR MIT OR Apache-2.0 | crates.io |
| `ahash` | `0.8.12` | MIT OR Apache-2.0 | crates.io |
| `aho-corasick` | `1.1.4` | Unlicense OR MIT | crates.io |
| `allocator-api2` | `0.2.21` | MIT OR Apache-2.0 | crates.io |
| `android_system_properties` | `0.1.5` | MIT/Apache-2.0 | crates.io |
| `anstream` | `1.0.0` | MIT OR Apache-2.0 | crates.io |
| `anstyle-parse` | `1.0.0` | MIT OR Apache-2.0 | crates.io |
| `anstyle-query` | `1.1.5` | MIT OR Apache-2.0 | crates.io |
| `anstyle-wincon` | `3.0.11` | MIT OR Apache-2.0 | crates.io |
| `anstyle` | `1.0.14` | MIT OR Apache-2.0 | crates.io |
| `anyhow` | `1.0.102` | MIT OR Apache-2.0 | crates.io |
| `approx` | `0.5.1` | Apache-2.0 | crates.io |
| `asn1-rs-derive` | `0.6.0` | MIT OR Apache-2.0 | crates.io |
| `asn1-rs-impl` | `0.2.0` | MIT/Apache-2.0 | crates.io |
| `asn1-rs` | `0.7.2` | MIT OR Apache-2.0 | crates.io |
| `async-trait` | `0.1.89` | MIT OR Apache-2.0 | crates.io |
| `atomic-waker` | `1.1.2` | Apache-2.0 OR MIT | crates.io |
| `auto-const-array` | `0.2.2` | MIT/Apache-2.0 | crates.io |
| `autocfg` | `1.5.0` | Apache-2.0 OR MIT | crates.io |
| `aws-lc-rs` | `1.17.1` | ISC AND (Apache-2.0 OR ISC) | crates.io |
| `aws-lc-sys` | `0.42.0` | ISC AND (Apache-2.0 OR ISC) AND Apache-2.0 AND MIT AND BSD-3-Clause AND (Apache-2.0 OR ISC OR MIT) AND (Apache-2.0 OR ISC OR MIT-0) | crates.io |
| `base64` | `0.21.7` | MIT OR Apache-2.0 | crates.io |
| `base64` | `0.22.1` | MIT OR Apache-2.0 | crates.io |
| `bit-vec` | `0.9.1` | Apache-2.0 OR MIT | crates.io |
| `bitflags` | `1.3.2` | MIT/Apache-2.0 | crates.io |
| `bitflags` | `2.11.1` | MIT OR Apache-2.0 | crates.io |
| `block-buffer` | `0.10.4` | MIT OR Apache-2.0 | crates.io |
| `block-buffer` | `0.12.1` | MIT OR Apache-2.0 | crates.io |
| `bumpalo` | `3.20.2` | MIT OR Apache-2.0 | crates.io |
| `bytemuck` | `1.25.0` | Zlib OR Apache-2.0 OR MIT | crates.io |
| `byteorder` | `1.5.0` | Unlicense OR MIT | crates.io |
| `bytes-handoff` | `1.2.0` | MIT | crates.io |
| `bytes` | `1.11.1` | MIT | crates.io |
| `cc` | `1.2.62` | MIT OR Apache-2.0 | crates.io |
| `cfg-if` | `1.0.4` | MIT OR Apache-2.0 | crates.io |
| `cfg_aliases` | `0.2.1` | MIT | crates.io |
| `chacha20` | `0.10.1` | MIT OR Apache-2.0 | crates.io |
| `chrono` | `0.4.45` | MIT OR Apache-2.0 | crates.io |
| `clap_builder` | `4.6.0` | MIT OR Apache-2.0 | crates.io |
| `clap_derive` | `4.6.1` | MIT OR Apache-2.0 | crates.io |
| `clap_lex` | `1.1.0` | MIT OR Apache-2.0 | crates.io |
| `clap` | `4.6.1` | MIT OR Apache-2.0 | crates.io |
| `cmake` | `0.1.58` | MIT OR Apache-2.0 | crates.io |
| `colorchoice` | `1.0.5` | MIT OR Apache-2.0 | crates.io |
| `combine` | `4.6.7` | MIT | crates.io |
| `core-foundation-sys` | `0.8.7` | MIT OR Apache-2.0 | crates.io |
| `core-foundation` | `0.10.1` | MIT OR Apache-2.0 | crates.io |
| `core_affinity` | `0.8.3` | MIT/Apache-2.0 | crates.io |
| `cpufeatures` | `0.2.17` | MIT OR Apache-2.0 | crates.io |
| `cpufeatures` | `0.3.0` | MIT OR Apache-2.0 | crates.io |
| `crc-fast` | `1.10.0` | MIT OR Apache-2.0 | crates.io |
| `crc32fast` | `1.5.0` | MIT OR Apache-2.0 | crates.io |
| `creusot-std-proc` | `0.11.0` | LGPL-2.1-or-later | crates.io |
| `creusot-std` | `0.11.0` | LGPL-2.1-or-later | crates.io |
| `crossbeam-channel` | `0.5.15` | MIT OR Apache-2.0 | crates.io |
| `crossbeam-epoch` | `0.9.18` | MIT OR Apache-2.0 | crates.io |
| `crossbeam-utils` | `0.8.21` | MIT OR Apache-2.0 | crates.io |
| `crypto-common` | `0.1.7` | MIT OR Apache-2.0 | crates.io |
| `crypto-common` | `0.2.2` | MIT OR Apache-2.0 | crates.io |
| `cust_core` | `0.1.1` | MIT OR Apache-2.0 | crates.io |
| `cust_derive` | `0.2.0` | MIT OR Apache-2.0 | crates.io |
| `cust_raw` | `0.11.3` | MIT OR Apache-2.0 | crates.io |
| `cust` | `0.3.2` | MIT OR Apache-2.0 | crates.io |
| `dashmap` | `6.1.0` | MIT | crates.io |
| `data-encoding` | `2.11.0` | MIT | crates.io |
| `der-parser` | `10.0.0` | MIT OR Apache-2.0 | crates.io |
| `deranged` | `0.5.8` | MIT OR Apache-2.0 | crates.io |
| `digest` | `0.10.7` | MIT OR Apache-2.0 | crates.io |
| `digest` | `0.11.3` | MIT OR Apache-2.0 | crates.io |
| `displaydoc` | `0.2.6` | MIT OR Apache-2.0 | crates.io |
| `dunce` | `1.0.5` | CC0-1.0 OR MIT-0 OR Apache-2.0 | crates.io |
| `either` | `1.16.0` | MIT OR Apache-2.0 | crates.io |
| `equivalent` | `1.0.2` | Apache-2.0 OR MIT | crates.io |
| `errno` | `0.3.14` | MIT OR Apache-2.0 | crates.io |
| `fast-telemetry-macros` | `0.7.1` | Apache-2.0 | crates.io |
| `fast-telemetry` | `0.7.1` | Apache-2.0 | crates.io |
| `fastrand` | `2.4.1` | Apache-2.0 OR MIT | crates.io |
| `find-msvc-tools` | `0.1.9` | MIT OR Apache-2.0 | crates.io |
| `find_cuda_helper` | `0.2.0` | MIT OR Apache-2.0 | crates.io |
| `flate2` | `1.1.9` | MIT OR Apache-2.0 | crates.io |
| `flume` | `0.11.1` | Apache-2.0/MIT | crates.io |
| `fnv` | `1.0.7` | Apache-2.0 / MIT | crates.io |
| `foldhash` | `0.1.5` | Zlib | crates.io |
| `form_urlencoded` | `1.2.2` | MIT OR Apache-2.0 | crates.io |
| `fs_extra` | `1.3.0` | MIT | crates.io |
| `futures-channel` | `0.3.32` | MIT OR Apache-2.0 | crates.io |
| `futures-core` | `0.3.32` | MIT OR Apache-2.0 | crates.io |
| `futures-io` | `0.3.32` | MIT OR Apache-2.0 | crates.io |
| `futures-macro` | `0.3.32` | MIT OR Apache-2.0 | crates.io |
| `futures-sink` | `0.3.32` | MIT OR Apache-2.0 | crates.io |
| `futures-task` | `0.3.32` | MIT OR Apache-2.0 | crates.io |
| `futures-util` | `0.3.32` | MIT OR Apache-2.0 | crates.io |
| `fxhash` | `0.2.1` | Apache-2.0/MIT | crates.io |
| `generic-array` | `0.14.7` | MIT | crates.io |
| `getrandom` | `0.2.17` | MIT OR Apache-2.0 | crates.io |
| `getrandom` | `0.3.4` | MIT OR Apache-2.0 | crates.io |
| `getrandom` | `0.4.2` | MIT OR Apache-2.0 | crates.io |
| `glam` | `0.20.5` | MIT OR Apache-2.0 | crates.io |
| `glob` | `0.3.3` | MIT OR Apache-2.0 | crates.io |
| `h2` | `0.4.15` | MIT | crates.io |
| `hashbrown` | `0.14.5` | MIT OR Apache-2.0 | crates.io |
| `hashbrown` | `0.15.5` | MIT OR Apache-2.0 | crates.io |
| `hashbrown` | `0.17.1` | MIT OR Apache-2.0 | crates.io |
| `hdrhistogram` | `7.5.4` | MIT/Apache-2.0 | crates.io |
| `heck` | `0.5.0` | MIT OR Apache-2.0 | crates.io |
| `hermit-abi` | `0.5.2` | MIT OR Apache-2.0 | crates.io |
| `http-body-util` | `0.1.3` | MIT | crates.io |
| `http-body` | `1.0.1` | MIT | crates.io |
| `http` | `1.4.2` | MIT OR Apache-2.0 | crates.io |
| `httparse` | `1.10.1` | MIT OR Apache-2.0 | crates.io |
| `humantime` | `2.4.0` | MIT OR Apache-2.0 | crates.io |
| `hybrid-array` | `0.4.13` | MIT OR Apache-2.0 | crates.io |
| `hyper-rustls` | `0.27.9` | Apache-2.0 OR ISC OR MIT | crates.io |
| `hyper-util` | `0.1.20` | MIT | crates.io |
| `hyper` | `1.10.1` | MIT | crates.io |
| `iana-time-zone-haiku` | `0.1.2` | MIT OR Apache-2.0 | crates.io |
| `iana-time-zone` | `0.1.65` | MIT OR Apache-2.0 | crates.io |
| `icu_collections` | `2.2.0` | Unicode-3.0 | crates.io |
| `icu_locale_core` | `2.2.0` | Unicode-3.0 | crates.io |
| `icu_normalizer_data` | `2.2.0` | Unicode-3.0 | crates.io |
| `icu_normalizer` | `2.2.0` | Unicode-3.0 | crates.io |
| `icu_properties_data` | `2.2.0` | Unicode-3.0 | crates.io |
| `icu_properties` | `2.2.0` | Unicode-3.0 | crates.io |
| `icu_provider` | `2.2.0` | Unicode-3.0 | crates.io |
| `id-arena` | `2.3.0` | MIT/Apache-2.0 | crates.io |
| `idna_adapter` | `1.2.2` | Apache-2.0 OR MIT | crates.io |
| `idna` | `1.1.0` | MIT OR Apache-2.0 | crates.io |
| `indexmap` | `2.14.0` | Apache-2.0 OR MIT | crates.io |
| `indextreemap` | `0.2.0` | MIT | crates.io |
| `indoc` | `2.0.7` | MIT OR Apache-2.0 | crates.io |
| `io-uring` | `0.6.4` | MIT OR Apache-2.0 | crates.io |
| `ipnet` | `2.12.0` | MIT OR Apache-2.0 | crates.io |
| `is_terminal_polyfill` | `1.70.2` | MIT OR Apache-2.0 | crates.io |
| `itertools` | `0.15.0` | MIT OR Apache-2.0 | crates.io |
| `itoa` | `1.0.18` | MIT OR Apache-2.0 | crates.io |
| `jni-macros` | `0.22.4` | MIT OR Apache-2.0 | crates.io |
| `jni-sys-macros` | `0.4.1` | MIT OR Apache-2.0 | crates.io |
| `jni-sys` | `0.4.1` | MIT OR Apache-2.0 | crates.io |
| `jni` | `0.22.4` | MIT OR Apache-2.0 | crates.io |
| `jobserver` | `0.1.34` | MIT OR Apache-2.0 | crates.io |
| `js-sys` | `0.3.98` | MIT OR Apache-2.0 | crates.io |
| `lazy_static` | `1.5.0` | MIT OR Apache-2.0 | crates.io |
| `leb128fmt` | `0.1.0` | MIT OR Apache-2.0 | crates.io |
| `libc` | `0.2.186` | MIT OR Apache-2.0 | crates.io |
| `libm` | `0.2.16` | MIT | crates.io |
| `linux-raw-sys` | `0.12.1` | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | crates.io |
| `litemap` | `0.8.2` | Unicode-3.0 | crates.io |
| `lock_api` | `0.4.14` | MIT OR Apache-2.0 | crates.io |
| `log` | `0.4.29` | MIT OR Apache-2.0 | crates.io |
| `lru-slab` | `0.1.2` | MIT OR Apache-2.0 OR Zlib | crates.io |
| `lru` | `0.12.5` | MIT | crates.io |
| `lz4_flex` | `0.11.6` | MIT | crates.io |
| `matchers` | `0.2.0` | MIT | crates.io |
| `md-5` | `0.11.0` | MIT OR Apache-2.0 | crates.io |
| `memchr` | `2.8.0` | Unlicense OR MIT | crates.io |
| `memoffset` | `0.7.1` | MIT | crates.io |
| `memoffset` | `0.9.1` | MIT | crates.io |
| `minimal-lexical` | `0.2.1` | MIT/Apache-2.0 | crates.io |
| `miniz_oxide` | `0.8.9` | MIT OR Zlib OR Apache-2.0 | crates.io |
| `mint` | `0.5.9` | MIT | crates.io |
| `mio` | `0.8.11` | MIT | crates.io |
| `mio` | `1.2.0` | MIT | crates.io |
| `moka` | `0.12.15` | (MIT OR Apache-2.0) AND Apache-2.0 | crates.io |
| `monoio-macros` | `0.1.0` | MIT/Apache-2.0 | crates.io |
| `monoio` | `0.2.4` | MIT OR Apache-2.0 | crates.io |
| `nix` | `0.26.4` | MIT | crates.io |
| `nom` | `7.1.3` | MIT | crates.io |
| `nu-ansi-term` | `0.50.3` | MIT | crates.io |
| `num-bigint` | `0.4.6` | MIT OR Apache-2.0 | crates.io |
| `num-conv` | `0.2.2` | MIT OR Apache-2.0 | crates.io |
| `num-integer` | `0.1.46` | MIT OR Apache-2.0 | crates.io |
| `num-rational` | `0.4.2` | MIT OR Apache-2.0 | crates.io |
| `num-traits` | `0.2.19` | MIT OR Apache-2.0 | crates.io |
| `num_cpus` | `1.17.0` | MIT OR Apache-2.0 | crates.io |
| `object_store` | `0.14.0` | MIT/Apache-2.0 | crates.io |
| `oid-registry` | `0.8.1` | MIT OR Apache-2.0 | crates.io |
| `once_cell_polyfill` | `1.70.2` | MIT OR Apache-2.0 | crates.io |
| `once_cell` | `1.21.4` | MIT OR Apache-2.0 | crates.io |
| `openssl-probe` | `0.2.1` | MIT OR Apache-2.0 | crates.io |
| `parking_lot_core` | `0.9.12` | MIT OR Apache-2.0 | crates.io |
| `parking_lot` | `0.12.5` | MIT OR Apache-2.0 | crates.io |
| `pem` | `3.0.6` | MIT | crates.io |
| `percent-encoding` | `2.3.2` | MIT OR Apache-2.0 | crates.io |
| `pin-project-lite` | `0.2.17` | Apache-2.0 OR MIT | crates.io |
| `pin-utils` | `0.1.0` | MIT OR Apache-2.0 | crates.io |
| `pkg-config` | `0.3.33` | MIT OR Apache-2.0 | crates.io |
| `portable-atomic` | `1.13.1` | Apache-2.0 OR MIT | crates.io |
| `potential_utf` | `0.1.5` | Unicode-3.0 | crates.io |
| `powerfmt` | `0.2.0` | MIT OR Apache-2.0 | crates.io |
| `ppv-lite86` | `0.2.21` | MIT OR Apache-2.0 | crates.io |
| `prettyplease` | `0.2.37` | MIT OR Apache-2.0 | crates.io |
| `proc-macro2` | `1.0.106` | MIT OR Apache-2.0 | crates.io |
| `pyo3-build-config` | `0.23.5` | MIT OR Apache-2.0 | crates.io |
| `pyo3-ffi` | `0.23.5` | MIT OR Apache-2.0 | crates.io |
| `pyo3-macros-backend` | `0.23.5` | MIT OR Apache-2.0 | crates.io |
| `pyo3-macros` | `0.23.5` | MIT OR Apache-2.0 | crates.io |
| `pyo3` | `0.23.5` | MIT OR Apache-2.0 | crates.io |
| `quick-xml` | `0.40.1` | MIT | crates.io |
| `quinn-proto` | `0.11.16` | MIT OR Apache-2.0 | crates.io |
| `quinn-udp` | `0.5.15` | MIT OR Apache-2.0 | crates.io |
| `quinn` | `0.11.11` | MIT OR Apache-2.0 | crates.io |
| `quote` | `1.0.45` | MIT OR Apache-2.0 | crates.io |
| `r-efi` | `5.3.0` | MIT OR Apache-2.0 OR LGPL-2.1-or-later | crates.io |
| `r-efi` | `6.0.0` | MIT OR Apache-2.0 OR LGPL-2.1-or-later | crates.io |
| `rand_chacha` | `0.3.1` | MIT OR Apache-2.0 | crates.io |
| `rand_core` | `0.10.1` | MIT OR Apache-2.0 | crates.io |
| `rand_core` | `0.6.4` | MIT OR Apache-2.0 | crates.io |
| `rand_pcg` | `0.10.2` | MIT OR Apache-2.0 | crates.io |
| `rand` | `0.10.2` | MIT OR Apache-2.0 | crates.io |
| `rand` | `0.8.6` | MIT OR Apache-2.0 | crates.io |
| `rblock` | `0.1.0` | Apache-2.0 | crates.io |
| `rcgen` | `0.14.8` | MIT OR Apache-2.0 | crates.io |
| `redis` | `0.32.7` | BSD-3-Clause | crates.io |
| `redox_syscall` | `0.5.18` | MIT | crates.io |
| `regex-automata` | `0.4.14` | MIT OR Apache-2.0 | crates.io |
| `regex-syntax` | `0.8.10` | MIT OR Apache-2.0 | crates.io |
| `reqwest` | `0.13.4` | MIT OR Apache-2.0 | crates.io |
| `ring` | `0.17.14` | Apache-2.0 AND ISC | crates.io |
| `rustc-hash` | `2.1.3` | Apache-2.0 OR MIT | crates.io |
| `rustc_version` | `0.4.1` | MIT OR Apache-2.0 | crates.io |
| `rusticata-macros` | `4.1.0` | MIT/Apache-2.0 | crates.io |
| `rustix` | `1.1.4` | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | crates.io |
| `rustls-native-certs` | `0.8.4` | Apache-2.0 OR ISC OR MIT | crates.io |
| `rustls-pemfile` | `2.2.0` | Apache-2.0 OR ISC OR MIT | crates.io |
| `rustls-pki-types` | `1.15.0` | MIT OR Apache-2.0 | crates.io |
| `rustls-platform-verifier-android` | `0.1.1` | MIT OR Apache-2.0 | crates.io |
| `rustls-platform-verifier` | `0.7.0` | MIT OR Apache-2.0 | crates.io |
| `rustls-webpki` | `0.103.13` | ISC | crates.io |
| `rustls` | `0.23.41` | Apache-2.0 OR ISC OR MIT | crates.io |
| `rustversion` | `1.0.22` | MIT OR Apache-2.0 | crates.io |
| `ryu` | `1.0.23` | Apache-2.0 OR BSL-1.0 | crates.io |
| `same-file` | `1.0.6` | Unlicense/MIT | crates.io |
| `schannel` | `0.1.29` | MIT | crates.io |
| `scopeguard` | `1.2.0` | MIT OR Apache-2.0 | crates.io |
| `security-framework-sys` | `2.17.0` | MIT OR Apache-2.0 | crates.io |
| `security-framework` | `3.7.0` | MIT OR Apache-2.0 | crates.io |
| `semver` | `1.0.28` | MIT OR Apache-2.0 | crates.io |
| `serde_core` | `1.0.228` | MIT OR Apache-2.0 | crates.io |
| `serde_derive` | `1.0.228` | MIT OR Apache-2.0 | crates.io |
| `serde_json` | `1.0.149` | MIT OR Apache-2.0 | crates.io |
| `serde_spanned` | `1.1.1` | MIT OR Apache-2.0 | crates.io |
| `serde_urlencoded` | `0.7.1` | MIT/Apache-2.0 | crates.io |
| `serde` | `1.0.228` | MIT OR Apache-2.0 | crates.io |
| `sha2` | `0.10.9` | MIT OR Apache-2.0 | crates.io |
| `sharded-slab` | `0.1.7` | MIT | crates.io |
| `shlex` | `1.3.0` | MIT OR Apache-2.0 | crates.io |
| `signal-hook-registry` | `1.4.8` | MIT OR Apache-2.0 | crates.io |
| `simd-adler32` | `0.3.9` | MIT | crates.io |
| `simd_cesu8` | `1.1.1` | Apache-2.0 OR MIT | crates.io |
| `simdutf8` | `0.1.5` | MIT OR Apache-2.0 | crates.io |
| `slab` | `0.4.12` | MIT | crates.io |
| `smallvec` | `1.15.1` | MIT OR Apache-2.0 | crates.io |
| `socket2` | `0.5.10` | MIT OR Apache-2.0 | crates.io |
| `socket2` | `0.6.3` | MIT OR Apache-2.0 | crates.io |
| `spin` | `0.10.1` | MIT | crates.io |
| `spin` | `0.9.9` | MIT | crates.io |
| `stable_deref_trait` | `1.2.1` | MIT OR Apache-2.0 | crates.io |
| `strsim` | `0.11.1` | MIT | crates.io |
| `subtle` | `2.6.1` | BSD-3-Clause | crates.io |
| `syn` | `1.0.109` | MIT OR Apache-2.0 | crates.io |
| `syn` | `2.0.117` | MIT OR Apache-2.0 | crates.io |
| `sync_wrapper` | `1.0.2` | Apache-2.0 | crates.io |
| `synstructure` | `0.13.2` | MIT | crates.io |
| `tagptr` | `0.2.0` | MIT/Apache-2.0 | crates.io |
| `target-lexicon` | `0.12.16` | Apache-2.0 WITH LLVM-exception | crates.io |
| `tempfile` | `3.27.0` | MIT OR Apache-2.0 | crates.io |
| `thiserror-impl` | `2.0.18` | MIT OR Apache-2.0 | crates.io |
| `thiserror` | `2.0.18` | MIT OR Apache-2.0 | crates.io |
| `thread_local` | `1.1.9` | MIT OR Apache-2.0 | crates.io |
| `time-core` | `0.1.9` | MIT OR Apache-2.0 | crates.io |
| `time-macros` | `0.2.31` | MIT OR Apache-2.0 | crates.io |
| `time` | `0.3.53` | MIT OR Apache-2.0 | crates.io |
| `tinystr` | `0.8.3` | Unicode-3.0 | crates.io |
| `tinyvec_macros` | `0.1.1` | MIT OR Apache-2.0 OR Zlib | crates.io |
| `tinyvec` | `1.11.0` | Zlib OR Apache-2.0 OR MIT | crates.io |
| `tokio-macros` | `2.7.0` | MIT | crates.io |
| `tokio-rustls` | `0.26.4` | MIT OR Apache-2.0 | crates.io |
| `tokio-util` | `0.7.18` | MIT | crates.io |
| `tokio` | `1.52.3` | MIT | crates.io |
| `toml_datetime` | `0.7.5+spec-1.1.0` | MIT OR Apache-2.0 | crates.io |
| `toml_parser` | `1.1.2+spec-1.1.0` | MIT OR Apache-2.0 | crates.io |
| `toml_writer` | `1.1.1+spec-1.1.0` | MIT OR Apache-2.0 | crates.io |
| `toml` | `0.9.12+spec-1.1.0` | MIT OR Apache-2.0 | crates.io |
| `tower-http` | `0.6.11` | MIT | crates.io |
| `tower-layer` | `0.3.3` | MIT | crates.io |
| `tower-service` | `0.3.3` | MIT | crates.io |
| `tower` | `0.5.3` | MIT | crates.io |
| `tracing-attributes` | `0.1.31` | MIT | crates.io |
| `tracing-core` | `0.1.36` | MIT | crates.io |
| `tracing-log` | `0.2.0` | MIT | crates.io |
| `tracing-subscriber` | `0.3.23` | MIT | crates.io |
| `tracing` | `0.1.44` | MIT | crates.io |
| `try-lock` | `0.2.5` | MIT | crates.io |
| `twox-hash` | `2.1.2` | MIT | crates.io |
| `typenum` | `1.20.1` | MIT OR Apache-2.0 | crates.io |
| `unicode-ident` | `1.0.24` | (MIT OR Apache-2.0) AND Unicode-3.0 | crates.io |
| `unicode-xid` | `0.2.6` | MIT OR Apache-2.0 | crates.io |
| `unindent` | `0.2.4` | MIT OR Apache-2.0 | crates.io |
| `untrusted` | `0.9.0` | ISC | crates.io |
| `url` | `2.5.8` | MIT OR Apache-2.0 | crates.io |
| `utf8_iter` | `1.0.4` | Apache-2.0 OR MIT | crates.io |
| `utf8parse` | `0.2.2` | Apache-2.0 OR MIT | crates.io |
| `uuid` | `1.23.1` | Apache-2.0 OR MIT | crates.io |
| `valuable` | `0.1.1` | MIT | crates.io |
| `vek` | `0.15.10` | MIT OR Apache-2.0 | crates.io |
| `version_check` | `0.9.5` | MIT/Apache-2.0 | crates.io |
| `walkdir` | `2.5.0` | Unlicense/MIT | crates.io |
| `want` | `0.3.1` | MIT | crates.io |
| `wasi` | `0.11.1+wasi-snapshot-preview1` | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | crates.io |
| `wasip2` | `1.0.3+wasi-0.2.9` | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | crates.io |
| `wasip3` | `0.4.0+wasi-0.3.0-rc-2026-01-06` | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | crates.io |
| `wasm-bindgen-futures` | `0.4.71` | MIT OR Apache-2.0 | crates.io |
| `wasm-bindgen-macro-support` | `0.2.121` | MIT OR Apache-2.0 | crates.io |
| `wasm-bindgen-macro` | `0.2.121` | MIT OR Apache-2.0 | crates.io |
| `wasm-bindgen-shared` | `0.2.121` | MIT OR Apache-2.0 | crates.io |
| `wasm-bindgen` | `0.2.121` | MIT OR Apache-2.0 | crates.io |
| `wasm-encoder` | `0.244.0` | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | crates.io |
| `wasm-metadata` | `0.244.0` | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | crates.io |
| `wasm-streams` | `0.5.0` | MIT OR Apache-2.0 | crates.io |
| `wasmparser` | `0.244.0` | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | crates.io |
| `web-sys` | `0.3.98` | MIT OR Apache-2.0 | crates.io |
| `web-time` | `1.1.0` | MIT OR Apache-2.0 | crates.io |
| `webpki-root-certs` | `1.0.8` | CDLA-Permissive-2.0 | crates.io |
| `winapi-i686-pc-windows-gnu` | `0.4.0` | MIT/Apache-2.0 | crates.io |
| `winapi-util` | `0.1.11` | Unlicense OR MIT | crates.io |
| `winapi-x86_64-pc-windows-gnu` | `0.4.0` | MIT/Apache-2.0 | crates.io |
| `winapi` | `0.3.9` | MIT/Apache-2.0 | crates.io |
| `windows-core` | `0.62.2` | MIT OR Apache-2.0 | crates.io |
| `windows-implement` | `0.60.2` | MIT OR Apache-2.0 | crates.io |
| `windows-interface` | `0.59.3` | MIT OR Apache-2.0 | crates.io |
| `windows-link` | `0.2.1` | MIT OR Apache-2.0 | crates.io |
| `windows-result` | `0.4.1` | MIT OR Apache-2.0 | crates.io |
| `windows-strings` | `0.5.1` | MIT OR Apache-2.0 | crates.io |
| `windows-sys` | `0.48.0` | MIT OR Apache-2.0 | crates.io |
| `windows-sys` | `0.52.0` | MIT OR Apache-2.0 | crates.io |
| `windows-sys` | `0.61.2` | MIT OR Apache-2.0 | crates.io |
| `windows-targets` | `0.48.5` | MIT OR Apache-2.0 | crates.io |
| `windows-targets` | `0.52.6` | MIT OR Apache-2.0 | crates.io |
| `windows_aarch64_gnullvm` | `0.48.5` | MIT OR Apache-2.0 | crates.io |
| `windows_aarch64_gnullvm` | `0.52.6` | MIT OR Apache-2.0 | crates.io |
| `windows_aarch64_msvc` | `0.48.5` | MIT OR Apache-2.0 | crates.io |
| `windows_aarch64_msvc` | `0.52.6` | MIT OR Apache-2.0 | crates.io |
| `windows_i686_gnu` | `0.48.5` | MIT OR Apache-2.0 | crates.io |
| `windows_i686_gnu` | `0.52.6` | MIT OR Apache-2.0 | crates.io |
| `windows_i686_gnullvm` | `0.52.6` | MIT OR Apache-2.0 | crates.io |
| `windows_i686_msvc` | `0.48.5` | MIT OR Apache-2.0 | crates.io |
| `windows_i686_msvc` | `0.52.6` | MIT OR Apache-2.0 | crates.io |
| `windows_x86_64_gnu` | `0.48.5` | MIT OR Apache-2.0 | crates.io |
| `windows_x86_64_gnu` | `0.52.6` | MIT OR Apache-2.0 | crates.io |
| `windows_x86_64_gnullvm` | `0.48.5` | MIT OR Apache-2.0 | crates.io |
| `windows_x86_64_gnullvm` | `0.52.6` | MIT OR Apache-2.0 | crates.io |
| `windows_x86_64_msvc` | `0.48.5` | MIT OR Apache-2.0 | crates.io |
| `windows_x86_64_msvc` | `0.52.6` | MIT OR Apache-2.0 | crates.io |
| `winnow` | `0.7.15` | MIT | crates.io |
| `winnow` | `1.0.2` | MIT | crates.io |
| `wit-bindgen-core` | `0.51.0` | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | crates.io |
| `wit-bindgen-rust-macro` | `0.51.0` | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | crates.io |
| `wit-bindgen-rust` | `0.51.0` | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | crates.io |
| `wit-bindgen` | `0.51.0` | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | crates.io |
| `wit-bindgen` | `0.57.1` | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | crates.io |
| `wit-component` | `0.244.0` | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | crates.io |
| `wit-parser` | `0.244.0` | Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT | crates.io |
| `writeable` | `0.6.3` | Unicode-3.0 | crates.io |
| `x509-parser` | `0.18.1` | MIT OR Apache-2.0 | crates.io |
| `xxhash-rust` | `0.8.15` | BSL-1.0 | crates.io |
| `yasna` | `0.6.0` | MIT OR Apache-2.0 | crates.io |
| `yoke-derive` | `0.8.2` | Unicode-3.0 | crates.io |
| `yoke` | `0.8.3` | Unicode-3.0 | crates.io |
| `zerocopy-derive` | `0.8.48` | BSD-2-Clause OR Apache-2.0 OR MIT | crates.io |
| `zerocopy` | `0.8.48` | BSD-2-Clause OR Apache-2.0 OR MIT | crates.io |
| `zerofrom-derive` | `0.1.7` | Unicode-3.0 | crates.io |
| `zerofrom` | `0.1.8` | Unicode-3.0 | crates.io |
| `zeroize` | `1.9.0` | Apache-2.0 OR MIT | crates.io |
| `zerotrie` | `0.2.4` | Unicode-3.0 | crates.io |
| `zerovec-derive` | `0.11.3` | Unicode-3.0 | crates.io |
| `zerovec` | `0.11.6` | Unicode-3.0 | crates.io |
| `zmij` | `1.0.21` | MIT | crates.io |
| `zstd-safe` | `7.2.4` | MIT OR Apache-2.0 | crates.io |
| `zstd-sys` | `2.0.16+zstd.1.5.7` | MIT/Apache-2.0 | crates.io |
| `zstd` | `0.13.3` | MIT | crates.io |

## Audit Commands

```sh
# Verify this document matches Cargo.lock.
./scripts/generate-dependency-docs.sh --check

# Inspect duplicate resolved versions.
cargo tree --workspace --all-features --duplicates

# Inspect the production TLS implementation policy.
./scripts/check-tls-dependency-policy.sh
```

Licenses above are package metadata, not legal advice. Release owners should
apply their normal source, notice, export, and vulnerability review to the
exact artifact they distribute.
