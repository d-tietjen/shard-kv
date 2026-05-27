#!/usr/bin/env bash
# Python harness runner for fc-py and (optionally) fc-lmcache.
#
#   ./scripts/run-python.sh                  # fc-py only
#   LMCACHE=1 ./scripts/run-python.sh        # also runs fc-lmcache (lmcache must be installed)
#
# The harnesses append to the same CSV schema as the Rust drivers, so
# results merge cleanly into a single summary.

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/.." && pwd)"
ws_root="$(cd "$root/.." && pwd)"

cd "$ws_root"

# Build the PyO3 wheel into the active Python environment.
if ! python3 -c "import shardcache" 2>/dev/null; then
  echo "building shardcache PyO3 wheel via maturin"
  maturin develop --release -m crates/shardcache-py/Cargo.toml --features extension-module
fi

mkdir -p "$root/results"
out="$root/results/python_$(date +%Y%m%d_%H%M%S).csv"

python3 "$root/python/fc_py_bench.py" \
  --value-size "${VALUE_SIZE:-512}" \
  --mix "${MIX:-80-20}" \
  --vcpu-budget "${VCPU_BUDGET:-4}" \
  --clients "${CLIENTS:-4}" \
  --key-count "${KEY_COUNT:-100000}" \
  --warmup "${WARMUP:-2}" \
  --duration "${DURATION:-10}" \
  --csv "$out"

if [[ "${LMCACHE:-0}" == "1" ]]; then
  if ! python3 -c "import lmcache" 2>/dev/null; then
    echo "lmcache not installed; skipping fc-lmcache (pip install lmcache)"
  else
    if ! python3 -c "import shardcache_lmcache_backend" 2>/dev/null; then
      echo "installing shardcache LMCache backend plugin"
      pip install ./integrations/lmcache_storage_backend
    fi
    python3 "$root/python/fc_lmcache_bench.py" \
      --value-size "${VALUE_SIZE:-4096}" \
      --mix "${MIX:-80-20}" \
      --vcpu-budget "${VCPU_BUDGET:-4}" \
      --clients "${CLIENTS:-4}" \
      --key-count "${KEY_COUNT:-4096}" \
      --warmup "${WARMUP:-2}" \
      --duration "${DURATION:-10}" \
      --csv "$out" \
      ${WITH_LOCAL_CPU:+--with-local-cpu}
  fi
fi

echo "wrote $out"
