#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
destination="$root_dir/docs/DEPENDENCIES.md"
mode="${1:-write}"

if [[ "$mode" != "write" && "$mode" != "--check" ]]; then
  echo "usage: $0 [--check]" >&2
  exit 2
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to generate the dependency inventory" >&2
  exit 1
fi

metadata_file="$(mktemp)"
rendered_file="$(mktemp)"
trap 'rm -f "$metadata_file" "$rendered_file"' EXIT

(
  cd "$root_dir"
  cargo metadata --locked --all-features --format-version 1 >"$metadata_file"
)

workspace_count="$(jq '[.packages[] | select(.source == null)] | length' "$metadata_file")"
third_party_count="$(jq '[.packages[] | select(.source != null)] | length' "$metadata_file")"

{
  cat <<'EOF'
# Dependency Inventory

This is the complete locked dependency inventory for the Shardcache workspace.
It includes publishable and source-only workspace packages plus normal, build,
development, optional, platform-specific, and transitive packages reachable by
the all-feature workspace graph. A deployed binary includes only the subset
selected by its package, Cargo features, and target.

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
| `rustls`, `tokio-rustls`, `rustls-pemfile`, `ring` | `scnp-tls` | TLS 1.3, mTLS, certificate parsing, and cryptography for SCNP overflow. |
| `shardcache-client-rs` | `kv-overflow` | SCNP framing and direct replica communication. |
| `lz4_flex`, `zstd` | Overflow features | Optional value and object compression. |
| `crc32fast`, `sha2` | Overflow integrity and TLS identity | Envelope integrity and certificate fingerprints. |
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

EOF

  printf '## Workspace Packages (%s)\n\n' "$workspace_count"
  printf '| Package | Version | License | Manifest |\n'
  printf '| --- | --- | --- | --- |\n'
  jq -r --arg root "$root_dir/" '
    def escape: gsub("\\|"; "\\|") | gsub("[\\r\\n]+"; " ");
    .packages[]
    | select(.source == null)
    | [(.name | escape), .version, ((.license // "not declared") | escape), (.manifest_path | sub("^" + $root; ""))]
    | "| `\(.[0])` | `\(.[1])` | \(.[2]) | `\(.[3])` |"
  ' "$metadata_file" | LC_ALL=C sort

  printf '\n## Third-Party Packages (%s)\n\n' "$third_party_count"
  printf '| Package | Version | License | Source |\n'
  printf '| --- | --- | --- | --- |\n'
  jq -r '
    def escape: gsub("\\|"; "\\|") | gsub("[\\r\\n]+"; " ");
    def source_label:
      if startswith("registry+") then "crates.io"
      elif startswith("git+") then .
      else .
      end;
    .packages[]
    | select(.source != null)
    | [(.name | escape), .version, ((.license // "not declared") | escape), (.source | source_label | escape)]
    | "| `\(.[0])` | `\(.[1])` | \(.[2]) | \(.[3]) |"
  ' "$metadata_file" | LC_ALL=C sort

  cat <<'EOF'

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
EOF
} >"$rendered_file"

if [[ "$mode" == "--check" ]]; then
  if ! cmp -s "$rendered_file" "$destination"; then
    echo "$destination is stale; run ./scripts/generate-dependency-docs.sh" >&2
    diff -u "$destination" "$rendered_file" || true
    exit 1
  fi
  echo "dependency inventory is current"
else
  mv "$rendered_file" "$destination"
fi
