#!/usr/bin/env bash
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
exec "$here/run-server-semantic-cache-benchmark.sh" "$@"
