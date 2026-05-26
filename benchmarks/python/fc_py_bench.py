"""fc-py: in-process Python benchmark against `fast_cache.Store`.

Usage:
    maturin develop --release -m crates/fast-cache-py/Cargo.toml
    python benchmarks/python/fc_py_bench.py \
        --value-size 512 --mix 80-20 --vcpu-budget 4 --clients 4 \
        --key-count 100000 --warmup 1 --duration 5

Reports a single saturation row in the same schema as the Rust drivers.
Append-only CSV via `--csv`.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from _bench_common import (
    WorkloadSpec,
    Worker,
    append_csv,
    common_argparser,
    parse_mix,
    print_table,
    run_saturation,
)

try:
    import fast_cache  # type: ignore[import-not-found]
except ImportError as exc:
    print(
        "fast_cache module not importable. Build the PyO3 wheel first:\n"
        "  maturin develop --release -m crates/fast-cache-py/Cargo.toml --features extension-module",
        file=sys.stderr,
    )
    raise SystemExit(1) from exc


class FcPyWorker(Worker):
    """Thin wrapper around a shared Store. Store is thread-safe."""

    def __init__(self, store: "fast_cache.Store") -> None:
        self.store = store

    def do_get(self, key: bytes) -> None:
        self.store.get(key)

    def do_set(self, key: bytes, value: bytes) -> None:
        self.store.set(key, value)


def main() -> None:
    parser = common_argparser(default_clients=4)
    parser.add_argument(
        "--client-architecture",
        choices=("shared", "local_embedded"),
        default="shared",
        help=(
            "fast-cache Store architecture. The benchmark default is shared "
            "because it generates arbitrary multi-client keys; local_embedded "
            "requires caller-owned shard routing."
        ),
    )
    args = parser.parse_args()
    get_pct = parse_mix(args.mix)
    spec = WorkloadSpec(
        key_count=args.key_count, value_size=args.value_size, get_pct=get_pct
    )

    store = fast_cache.Store(
        cores=max(1, args.vcpu_budget),
        client_architecture=args.client_architecture,
    )

    def new_worker() -> Worker:
        return FcPyWorker(store)

    def warmup(keys: list[bytes], value: bytes) -> None:
        for k in keys:
            store.set(k, value)

    print(
        f"fc-py: value_size={args.value_size}B mix={spec.mix_label} "
        f"vcpu_budget={args.vcpu_budget} clients={args.clients} "
        f"keys={args.key_count} duration={args.duration}s "
        f"client_architecture={args.client_architecture}"
    )
    print()

    result = run_saturation("fc-py", spec, args, new_worker, warmup)
    print_table([result])
    append_csv(args.csv, [result])


if __name__ == "__main__":
    main()
