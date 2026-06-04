#!/usr/bin/env bash
# Guard the crates.io release shape.

set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

publishable=" shardmap shardcache shardcache-client-rs "
failed=0

for manifest in crates/*/Cargo.toml; do
  name="$(awk -F '"' '/^name = "/ { print $2; exit }' "$manifest")"
  if [[ "$publishable" == *" $name "* ]]; then
    if grep -Eq '^publish[[:space:]]*=[[:space:]]*false' "$manifest"; then
      echo "$manifest: $name must remain publishable" >&2
      failed=1
    fi
  elif ! grep -Eq '^publish[[:space:]]*=[[:space:]]*false' "$manifest"; then
    echo "$manifest: non-release crates must set publish = false" >&2
    failed=1
  fi
done

exit "$failed"
