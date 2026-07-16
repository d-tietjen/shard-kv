# ShardMap Blossom Bridge

This source-only workspace crate connects `ActiveShardMap` ambiguous conflict
ordering to a pinned Blossom consensus deployment. It does not replicate cache
values, keys, WAL blocks, TTLs, or governance metadata through Blossom.

The bridge is intentionally not published to crates.io while Blossom is a
pre-release Git dependency. The publishable `shardmap` crate depends only on
the `BlossomConflictConsensus` trait.

## Security Boundary

- Every configured Blossom address must be loopback.
- Remote validators require one identity-bound mTLS proxy listener per
  validator. Do not expose Blossom's plaintext TCP port to a network.
- Each listener is paired with its expected validator public key. Every
  validator generation must expose a supermajority of distinct identities.
- Response frame lengths are checked against `max_response_bytes` before
  allocation; endpoint and group counts and aggregate in-flight response bytes
  also have hard ceilings.
- The signing key is read from an owner-only file for each submission. Rotate
  it with an atomic file replacement and configure validator-set overlap at an
  explicit nonce.
- Debug output redacts the signing-key path.

## Finality And Recovery

The adapter accepts an epoch only when a supermajority of configured validator
endpoints reports the same hash. It then verifies the trusted hash chain, group,
nonce, exact validator generation, block signatures, and claim inclusion.

Candidate ranks and exact-claim receipts are checksummed, atomically replaced,
fsynced, bounded by entry count and bytes, and restored before networking. A
corrupt or mismatched state file fails startup. Keep this directory on durable
local storage and include it in node backup and restore procedures.

See [the active-active replication guide](../../docs/ACTIVE_ACTIVE_REPLICATION.md)
for consistency semantics, configuration requirements, and operational limits.
