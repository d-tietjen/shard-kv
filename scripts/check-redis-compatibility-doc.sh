#!/usr/bin/env bash
# Verify docs/REDIS_COMPATIBILITY.md matches the benchmark command registry.

set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

cargo run --quiet -p fast-cache-benchmarks --bin redis_command_manifest -- \
  --format markdown \
  --output "$tmp"

diff -u docs/REDIS_COMPATIBILITY.md "$tmp"
