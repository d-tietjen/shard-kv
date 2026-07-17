# Security policy

## Supported versions

`shard-kv` is pre-1.0. Fixes target the latest `main` branch and the
latest published crate version.

## Reporting a vulnerability

Report suspected vulnerabilities privately. Open a private security
advisory on GitHub, or email the maintainer listed in `Cargo.toml`.

Include:

- affected version or commit
- reproduction steps
- expected impact
- which build the issue affects (default, `unsafe`, server, persistence, or protocol parsing)

Do not open a public issue for an unpatched vulnerability.

## Unsafe code

The default build uses safe code. Reviewed performance paths that use
unsafe are opt-in through `--features unsafe` and documented in
[`crates/shardmap/SAFETY.md`](crates/shardmap/SAFETY.md).

## Network and overflow hardening

The general Redis-compatible RESP/SCNP listener defaults to loopback and is not
the authenticated transport used by dedicated overflow or active-sync
peers. Do not bind the general listener to an untrusted interface without an
external authenticated TLS boundary and firewall policy. The dedicated KV
overflow listener described below has separate enforced authentication.

SCNP overflow TLS uses Rustls with the `ring` provider and TLS 1.3. The
workspace does not use OpenSSL or native-tls for this path. Non-loopback SCNP
overflow requires certificate verification plus token authentication or mTLS,
unless an operator explicitly enables the private-overlay escape hatch.

Protocol decoders validate declared frame, collection, and decompressed sizes
before allocation. KV overflow separately bounds resident values, key metadata,
queue depth, pipeline bytes, retained connection buffers, key/value sizes, and
compression expansion. These limits are part of the security boundary; do not
set them to unbounded values on an untrusted network deployment.

The detailed TLS, authentication, rotation, memory-limit, and failure-mode
contract is documented in [`docs/KV_OVERFLOW.md`](docs/KV_OVERFLOW.md).
Reports involving resource exhaustion, malformed frames, authentication
bypass, stale topology placement, or unintended memory disclosure should be
treated as security reports.

The feature-gated 0.7 active-active transport does not reuse the general client
listener. It uses Rustls TLS 1.3 with mandatory mutual authentication, ALPN,
certificate-fingerprint peer authorization, bounded credential overlap,
deadlines, frame limits, and immediate node revocation. It does not use OpenSSL.
The current direct transport rejects forwarded blocks whose origin differs from
the authenticated peer; multi-hop forwarding remains disabled until origin
signatures are implemented. The complete contract and remaining gates are in
[`docs/ACTIVE_ACTIVE_REPLICATION.md`](docs/ACTIVE_ACTIVE_REPLICATION.md).

## Dependency inventory

The complete locked all-feature workspace inventory, including transitive,
build, development, optional, and target-specific packages, is published in
[`docs/DEPENDENCIES.md`](docs/DEPENDENCIES.md). CI regenerates the inventory
from Cargo metadata and fails when the checked-in document is stale. The same
release gate enforces the Rustls-only TLS dependency policy.
