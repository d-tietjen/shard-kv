#!/usr/bin/env python3
"""Measure Redis-compatible server memory density and paired GET/SET speed."""

from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import hashlib
import io
import json
import os
import pathlib
import platform
import re
import statistics
import subprocess
import sys
import time
import uuid
from urllib.parse import parse_qs, urlsplit


ROOT = pathlib.Path(__file__).resolve().parents[2]
SATURATION = ROOT / "target" / "release" / "saturation"
SHARDCACHE_BUILDER_IMAGE = "rust:1.93-slim-bookworm"
SHARDCACHE_RUNTIME_BASE_IMAGE = "debian:bookworm-slim"
TARGETS = {
    "redis": {
        "backend": "redis",
        "container_port": 6379,
        "image": "redis:7.4-alpine",
        "process_user": "redis",
    },
    "shardcache-resp": {
        "backend": "fc-server-resp",
        "container_port": 6383,
        "image": "",
        "process_user": "shardcache",
    },
}
KEY_BYTES = 18  # Workload's point keys are `k:` followed by 16 hex digits.


def payload_bytes(size: int, pattern: str) -> bytes:
    if pattern == "repeating":
        return bytes(index & 255 for index in range(size))
    if pattern == "compressible":
        return b"x" * size
    state = 0x4544454E22660001
    mask = (1 << 64) - 1
    value = bytearray()
    while len(value) < size:
        state = (state + 0x9E3779B97F4A7C15) & mask
        word = state
        word = ((word ^ (word >> 30)) * 0xBF58476D1CE4E5B9) & mask
        word = ((word ^ (word >> 27)) * 0x94D049BB133111EB) & mask
        word ^= word >> 31
        value.extend(word.to_bytes(8, "little"))
    return bytes(value[:size])


def parse_csv_ints(value: str, label: str) -> list[int]:
    try:
        parsed = [int(part.strip()) for part in value.split(",") if part.strip()]
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{label} must be comma-separated integers") from exc
    if not parsed or any(number <= 0 for number in parsed):
        raise argparse.ArgumentTypeError(f"{label} values must be positive")
    return parsed


def command(
    argv: list[str],
    *,
    cwd: pathlib.Path = ROOT,
    log: pathlib.Path | None = None,
    env: dict[str, str] | None = None,
) -> str:
    if log is None:
        result = subprocess.run(argv, cwd=cwd, env=env, check=True, text=True, capture_output=True)
        return result.stdout.strip()
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("w", encoding="utf-8") as output:
        subprocess.run(argv, cwd=cwd, env=env, check=True, text=True, stdout=output, stderr=subprocess.STDOUT)
    return ""


def available_host_port() -> int:
    import socket

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def file_sha256(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def host_memory_info() -> dict[str, int]:
    values: dict[str, int] = {}
    for line in pathlib.Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
        name, sep, value = line.partition(":")
        if sep and name in {"MemTotal", "MemAvailable"}:
            values[name.lower() + "_bytes"] = int(value.split()[0]) * 1024
    return values


def tool_version(argv: list[str]) -> str:
    result = subprocess.run(argv, check=False, text=True, capture_output=True)
    return result.stdout.strip() if result.returncode == 0 else "unavailable"


def parse_resp_bulk(sock_file: io.BufferedReader) -> bytes:
    prefix = sock_file.read(1)
    if prefix == b"$":
        length = int(sock_file.readline().strip())
        if length < 0:
            return b""
        value = sock_file.read(length)
        if sock_file.read(2) != b"\r\n":
            raise RuntimeError("invalid RESP bulk terminator")
        return value
    if prefix == b":":
        return sock_file.readline().strip()
    if prefix == b"+":
        return sock_file.readline().strip()
    if prefix == b"-":
        raise RuntimeError(sock_file.readline().decode("utf-8", "replace").strip())
    raise RuntimeError(f"unexpected RESP reply prefix: {prefix!r}")


def redis_command(port: int, *parts: bytes) -> bytes:
    import socket

    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        request = bytearray(f"*{len(parts)}\r\n".encode())
        for part in parts:
            request.extend(f"${len(part)}\r\n".encode())
            request.extend(part)
            request.extend(b"\r\n")
        sock.sendall(request)
        with sock.makefile("rb") as reader:
            return parse_resp_bulk(reader)


def parse_info(port: int, section: bytes = b"memory") -> dict[str, str]:
    try:
        body = redis_command(port, b"INFO", section).decode("utf-8", "replace")
    except (OSError, RuntimeError, ValueError):
        return {}
    fields: dict[str, str] = {}
    for line in body.splitlines():
        if ":" in line and not line.startswith("#"):
            name, value = line.split(":", 1)
            fields[name] = value
    return fields


def cgroup_v2_dir(pid: int) -> pathlib.Path | None:
    try:
        rows = pathlib.Path(f"/proc/{pid}/cgroup").read_text(encoding="utf-8").splitlines()
        cgroup = next(row.split(":", 2)[2] for row in rows if row.startswith("0::"))
        path = pathlib.Path("/sys/fs/cgroup") / cgroup.lstrip("/")
        return path if (path / "memory.current").is_file() else None
    except (OSError, StopIteration):
        return None


def process_memory(
    pid: int,
    container_id: str,
    process_user: str,
    cg_dir: pathlib.Path | None,
    port: int,
) -> dict[str, int | str]:
    values: dict[str, int | str] = {
        "rss_bytes": 0,
        "pss_bytes": 0,
        "private_bytes": 0,
        "cgroup_current_bytes": 0,
        "cgroup_anon_bytes": 0,
        "cgroup_file_bytes": 0,
        "redis_used_memory_bytes": "",
        "redis_allocator_rss_bytes": "",
    }
    try:
        rollup = subprocess.run(
            ["docker", "exec", "--user", process_user, container_id, "cat", "/proc/1/smaps_rollup"],
            check=True,
            text=True,
            capture_output=True,
        ).stdout
        for line in rollup.splitlines():
            key, sep, value = line.partition(":")
            if not sep:
                continue
            parts = value.split()
            if key == "Rss":
                values["rss_bytes"] = int(parts[0]) * 1024
            elif key == "Pss":
                values["pss_bytes"] = int(parts[0]) * 1024
            elif key in {"Private_Clean", "Private_Dirty"}:
                values["private_bytes"] = int(values["private_bytes"]) + int(parts[0]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    if cg_dir is not None:
        try:
            values["cgroup_current_bytes"] = int((cg_dir / "memory.current").read_text().strip())
            stat = (cg_dir / "memory.stat").read_text().splitlines()
            for line in stat:
                name, value = line.split()
                if name in {"anon", "file"}:
                    values[f"cgroup_{name}_bytes"] = int(value)
        except (OSError, ValueError):
            pass
    info = parse_info(port)
    values["redis_used_memory_bytes"] = int(info["used_memory"]) if info.get("used_memory", "").isdigit() else ""
    values["redis_allocator_rss_bytes"] = int(info["used_memory_rss"]) if info.get("used_memory_rss", "").isdigit() else ""
    return values


def median_snapshot(
    pid: int,
    container_id: str,
    process_user: str,
    cg_dir: pathlib.Path | None,
    port: int,
    count: int,
    delay: float,
) -> dict[str, int | str]:
    samples = []
    for index in range(count):
        samples.append(process_memory(pid, container_id, process_user, cg_dir, port))
        if delay > 0 and index + 1 < count:
            time.sleep(delay)
    result: dict[str, int | str] = {}
    for key in samples[0]:
        numbers = [sample[key] for sample in samples if isinstance(sample[key], int)]
        result[key] = int(statistics.median(numbers)) if numbers else ""
    return result


def wait_for_target(port: int, timeout: float = 60.0) -> None:
    import socket

    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if redis_command(port, b"PING") == b"PONG":
                return
        except (OSError, RuntimeError, ValueError) as exc:
            last_error = exc
        time.sleep(0.25)
    raise TimeoutError(f"server on port {port} did not become ready: {last_error}")


def get_inspect(container_id: str, field: str) -> str:
    return command(["docker", "inspect", "--format", f"{{{{.{field}}}}}", container_id])


def resolve_shardcache_build(
    metadata: dict,
    dockerfile: bytes,
    build_log: str,
    *,
    features: str,
    build_jobs: int,
    candidate_sha: str,
    image_tag: str,
    engine_image_id: str,
) -> dict:
    """Bind this single-platform build's FROM materials and typed image identities.

    BuildKit's material digests identify image manifests, not Engine config IDs.
    Some Engine exporters omit config.digest in metadata; their plain export log
    supplies it. Neither evidence source requires FROM tags in the Engine store.
    """
    def require(condition: bool, message: str) -> None:
        if not condition:
            raise RuntimeError(f"shardcache build provenance: {message}")

    def digest(value: str) -> str:
        require(isinstance(value, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", value) is not None, "invalid image digest")
        return value

    provenance = metadata["buildx.build.provenance"]
    require(provenance["buildType"] == "https://mobyproject.org/buildkit@v1", "unexpected build type")
    invocation = provenance["invocation"]
    require(invocation["configSource"]["entryPoint"] == "Dockerfile", "Dockerfile entry point differs")
    parameters = invocation["parameters"]
    require(parameters["frontend"] == "dockerfile.v0", "unexpected frontend")
    for name, value in (("SHARDCACHE_FEATURES", features), ("CARGO_BUILD_JOBS", str(build_jobs))):
        require(parameters["args"].get(f"build-arg:{name}") == value, f"build argument differs: {name}")
    platform_name = invocation["environment"]["platform"]
    require(isinstance(platform_name, str) and re.fullmatch(r"linux/[a-z0-9_]+", platform_name) is not None, "unsupported build platform")
    details = provenance["metadata"]["https://mobyproject.org/buildkit@v1#metadata"]
    require(details["vcs"]["revision"] == candidate_sha, "source revision differs")
    sources = [item for item in details["source"]["infos"] if item.get("filename") == "Dockerfile"]
    require(len(sources) == 1, "missing or ambiguous Dockerfile source")
    require(base64.b64decode(sources[0]["data"], validate=True) == dockerfile, "retained Dockerfile differs from build input")
    declared = re.findall(rb"^FROM (\S+) AS (\S+)$", dockerfile, re.MULTILINE)
    expected = {"builder": SHARDCACHE_BUILDER_IMAGE, "runtime": SHARDCACHE_RUNTIME_BASE_IMAGE}
    require(declared == [(value.encode(), stage.encode()) for stage, value in expected.items()], "declared FROM stages differ")

    materials = provenance["materials"]
    require(isinstance(materials, list), "materials must be a direct list")
    bases = {}
    for stage, reference in expected.items():
        repository, tag = reference.split(":", 1)
        matches = [item for item in materials if isinstance(item, dict) and isinstance(item.get("uri"), str)
                   and re.match(rf"^pkg:docker/(?:docker\.io/library/|library/)?{repository}@", item["uri"])]
        require(len(matches) == 1, f"missing or ambiguous FROM material: {reference}")
        material = matches[0]
        uri = urlsplit(material["uri"])
        require(re.fullmatch(rf"docker/(?:docker\.io/library/|library/)?{repository}@{re.escape(tag)}", uri.path) is not None
                and not uri.fragment and parse_qs(uri.query, strict_parsing=True) == {"platform": [platform_name]}, f"FROM tag/platform differs: {reference}")
        require(set(material["digest"]) == {"sha256"}, f"unexpected material digest: {reference}")
        manifest_digest = digest("sha256:" + material["digest"]["sha256"])
        identifiers = [step.get("op", {}).get("Op", {}).get("source", {}).get("identifier", "")
                       for step in provenance["buildConfig"]["llbDefinition"]]
        actual_sources = [value for value in identifiers if value.startswith(f"docker-image://docker.io/library/{repository}:")]
        require(actual_sources == [f"docker-image://docker.io/library/{reference}@{manifest_digest}"], f"material and resolved FROM source differ: {reference}")
        bases[stage] = {"reference": reference, "uri": material["uri"], "platform": platform_name,
                        "manifest_digest": manifest_digest}

    descriptor = metadata["containerimage.descriptor"]
    require(descriptor["mediaType"] in {"application/vnd.oci.image.manifest.v1+json", "application/vnd.docker.distribution.manifest.v2+json"}, "export must be one image manifest")
    manifest_digest = digest(metadata["containerimage.digest"])
    require(descriptor["digest"] == manifest_digest, "export descriptor differs")
    require(f"{descriptor['platform']['os']}/{descriptor['platform']['architecture']}" == platform_name, "export platform differs")
    require(metadata["image.name"] in {image_tag, "docker.io/library/" + image_tag}, "export image tag differs")
    require(isinstance(metadata["buildx.build.ref"], str) and bool(metadata["buildx.build.ref"]), "missing build reference")
    exports = re.findall(r"^#(\d+) exporting (manifest|config) (sha256:[0-9a-f]{64})(?: [0-9.]+s)? done$", build_log, re.MULTILINE)
    manifests = [(step, value) for step, kind, value in exports if kind == "manifest"]
    configs = [(step, value) for step, kind, value in exports if kind == "config"]
    require(len(manifests) == len(configs) == 1 and manifests[0][0] == configs[0][0]
            and manifests[0][1] == manifest_digest, "missing, ambiguous, or mismatched export log")
    config_digest = digest(configs[0][1])
    for value in (metadata.get("containerimage.config.digest"), descriptor.get("annotations", {}).get("config.digest")):
        if value is not None:
            require(digest(value) == config_digest, "metadata and exported config differ")
    require(manifest_digest != config_digest, "manifest and config identities are ambiguous")
    digest(engine_image_id)
    require(engine_image_id in {manifest_digest, config_digest}, "loaded Engine image differs from export")
    return {
        "build_ref": metadata["buildx.build.ref"],
        "base_materials": bases,
        "materials_complete": provenance["metadata"]["completeness"]["materials"],
        "dockerfile_sha256": hashlib.sha256(dockerfile).hexdigest(),
        "features": features,
        "runtime_manifest_digest": manifest_digest,
        "runtime_config_digest": config_digest,
        "runtime_engine_image_id": engine_image_id,
        "runtime_engine_image_id_kind": "manifest-digest" if engine_image_id == manifest_digest else "config-digest",
    }


def build_shardcache_image(args: argparse.Namespace, output: pathlib.Path) -> dict:
    context = output / "runtime-context"
    dockerfile = context / "Dockerfile"
    metadata_path = output / "build-metadata.json"
    build_log = output / "logs" / "docker-build-shardcache-runtime.log"
    for retained in (dockerfile, metadata_path, build_log):
        if retained.exists():
            raise RuntimeError(f"preserve prior build evidence: {retained}")
    context.mkdir(parents=True, exist_ok=True)
    dockerfile.write_text(
        f"FROM {SHARDCACHE_BUILDER_IMAGE} AS builder\n"
        "WORKDIR /app\n"
        f"ARG SHARDCACHE_FEATURES={args.shardcache_features}\n"
        f"ARG CARGO_BUILD_JOBS={args.build_jobs}\n"
        "ARG RUSTFLAGS=\n"
        "ENV RUSTFLAGS=${RUSTFLAGS}\n"
        "COPY . .\n"
        "RUN cargo build --locked --release --jobs ${CARGO_BUILD_JOBS} "
        "-p shardcache --features \"${SHARDCACHE_FEATURES}\" --bin shardcache\n"
        f"FROM {SHARDCACHE_RUNTIME_BASE_IMAGE} AS runtime\n"
        "RUN groupadd --system shardcache \\\n"
        "    && useradd --system --gid shardcache --home-dir /var/lib/shardcache --create-home shardcache\n"
        "COPY --from=builder /app/target/release/shardcache /usr/local/bin/shardcache\n"
        "RUN mkdir -p /var/lib/shardcache \\\n"
        "    && chown -R shardcache:shardcache /var/lib/shardcache\n"
        "USER shardcache\n"
        "EXPOSE 6380 6501 6502 6503 6504\n"
        'ENTRYPOINT ["shardcache"]\n'
        'CMD ["--bind-addr", "0.0.0.0:6380", "--disable-persistence", "--server-mode", "direct"]\n',
        encoding="utf-8",
    )
    build_env = dict(os.environ, BUILDX_METADATA_PROVENANCE="max")
    command(
        [
            "docker",
            "build",
            "--metadata-file",
            str(metadata_path),
            "--progress",
            "plain",
            "--build-arg",
            f"CARGO_BUILD_JOBS={args.build_jobs}",
            "--build-arg",
            f"SHARDCACHE_FEATURES={args.shardcache_features}",
            "--tag",
            args.runtime_image_tag,
            "--file",
            str(dockerfile),
            str(ROOT),
        ],
        log=build_log,
        env=build_env,
    )
    resolved = resolve_shardcache_build(
        json.loads(metadata_path.read_text(encoding="utf-8")), dockerfile.read_bytes(),
        build_log.read_text(encoding="utf-8"), features=args.shardcache_features,
        build_jobs=args.build_jobs, candidate_sha=args.candidate_sha,
        image_tag=args.runtime_image_tag, engine_image_id=get_inspect(args.runtime_image_tag, "Id"),
    )
    resolved.update(metadata_sha256=file_sha256(metadata_path), export_log_sha256=file_sha256(build_log))
    return resolved


def start_target_container(
    args: argparse.Namespace,
    target: str,
    name: str,
    port: int,
) -> str:
    spec = TARGETS[target]
    container_image = spec["image"] or args.runtime_image_tag
    argv = [
        "docker",
        "run",
        "--detach",
        "--name",
        name,
        "--label",
        f"io.shard-kv.memory-density-run={args.run_id}",
        "--label",
        f"io.shard-kv.memory-density-candidate={args.candidate_sha}",
        "--cpus",
        str(args.vcpus),
        "--memory",
        args.memory_limit,
        "--memory-swap",
        args.memory_limit,
        "--publish",
        f"127.0.0.1:{port}:{spec['container_port']}",
    ]
    if target == "shardcache-resp":
        argv.extend(
            [
                container_image,
                "--bind-addr",
                "0.0.0.0:6383",
                "--shard-count",
                str(args.vcpus),
                "--disable-persistence",
                "--eviction-policy",
                "none",
                "--server-mode",
                "direct",
            ]
        )
    else:
        argv.extend(
            [
                container_image,
                "redis-server",
                "--save",
                "",
                "--appendonly",
                "no",
                "--protected-mode",
                "no",
                "--maxmemory",
                "0",
                "--port",
                str(spec["container_port"]),
            ]
        )
    return command(argv)


def run_case(args: argparse.Namespace, output: pathlib.Path, target: str, value_size: int, repeat: int) -> dict[str, str]:
    target_spec = TARGETS[target]
    stamp = uuid.uuid4().hex[:8]
    profile = f"{args.value_pattern}-{hashlib.sha256(args.distribution.encode()).hexdigest()[:8]}"
    name = f"md-{args.run_id[-8:]}-{target.replace('-', '')}-{value_size}-{repeat}-{stamp}"[:64]
    port = available_host_port()
    log_dir = output / "logs"
    container_id = ""
    try:
        container_id = start_target_container(args, target, name, port)
        wait_for_target(port)
        host_pid = int(get_inspect(container_id, "State.Pid"))
        image_id = get_inspect(container_id, "Image")
        cg_dir = cgroup_v2_dir(host_pid)
        if cg_dir is None:
            raise RuntimeError(f"container {container_id} has no readable cgroup v2 memory.current")
        process_user = target_spec["process_user"]
        idle = median_snapshot(host_pid, container_id, process_user, cg_dir, port, args.samples, args.sample_delay)

        perf_path = output / "raw" / f"saturation-{target}-{value_size}-{profile}-r{repeat}.csv"
        backend = target_spec["backend"]
        saturation_args = [
            str(SATURATION),
            "--backends",
            backend,
            "--addr",
            f"127.0.0.1:{port}",
            "--server-pid",
            str(host_pid),
            "--vcpu-budget",
            str(args.vcpus),
            "--clients",
            str(args.clients),
            "--pipeline-depth",
            str(args.pipeline),
            "--value-size",
            str(value_size),
            "--value-pattern",
            args.value_pattern,
            "--mix",
            args.mix,
            "--key-count",
            str(args.keys),
            "--key-distribution",
            args.distribution,
            "--duration",
            str(args.duration),
            "--warmup",
            str(args.warmup),
            "--csv",
            str(perf_path),
        ]
        command(saturation_args, log=log_dir / f"saturation-{target}-{value_size}-{profile}-r{repeat}.log")
        expected_keys = args.keys
        actual_keys = int(redis_command(port, b"DBSIZE"))
        if actual_keys != expected_keys:
            raise RuntimeError(f"{target} contains {actual_keys} keys after preload; expected {expected_keys}")
        expected_value = payload_bytes(value_size, args.value_pattern)
        for key_index in sorted({0, args.keys // 2, args.keys - 1}):
            key = f"k:{key_index:016x}".encode()
            if redis_command(port, b"GET", key) != expected_value:
                raise RuntimeError(f"{target} value mismatch for sample key {key!r}")
        if args.settle_seconds > 0:
            time.sleep(args.settle_seconds)
        loaded = median_snapshot(host_pid, container_id, process_user, cg_dir, port, args.samples, args.sample_delay)
        with perf_path.open(newline="", encoding="utf-8") as source:
            perf_rows = list(csv.DictReader(source))
        if len(perf_rows) != 1:
            raise RuntimeError(f"expected one saturation CSV row, found {len(perf_rows)} in {perf_path}")
        perf = perf_rows[0]
        if int(perf.get("errors", "0")) != 0:
            raise RuntimeError(f"saturation reported errors for {target}: {perf.get('errors')}")
        server_info = parse_info(port, b"server")
        logical_bytes = args.keys * (KEY_BYTES + value_size)
        row: dict[str, str] = {
            "candidate_sha": args.candidate_sha,
            "target": target,
            "repeat": str(repeat),
            "image_tag": target_spec["image"] or args.runtime_image_tag,
            "image_id": image_id,
            "container_id": container_id,
            "vcpus": str(args.vcpus),
            "cpu_affinity": "none",
            "shardcache_features": args.shardcache_features if target == "shardcache-resp" else "",
            "clients": str(args.clients),
            "pipeline_depth": str(args.pipeline),
            "mix": args.mix,
            "distribution": args.distribution,
            "value_pattern": args.value_pattern,
            "value_sha256": hashlib.sha256(expected_value).hexdigest(),
            "value_sample_checks": str(len({0, args.keys // 2, args.keys - 1})),
            "key_count": str(args.keys),
            "key_bytes_each": str(KEY_BYTES),
            "value_bytes_each": str(value_size),
            "logical_key_value_bytes": str(logical_bytes),
            "actual_key_count": str(actual_keys),
            "server_version": server_info.get("redis_version", ""),
            "idle_rss_bytes": str(idle["rss_bytes"]),
            "loaded_rss_bytes": str(loaded["rss_bytes"]),
            "delta_rss_bytes": str(max(0, int(loaded["rss_bytes"]) - int(idle["rss_bytes"]))),
            "idle_pss_bytes": str(idle["pss_bytes"]),
            "loaded_pss_bytes": str(loaded["pss_bytes"]),
            "delta_pss_bytes": str(max(0, int(loaded["pss_bytes"]) - int(idle["pss_bytes"]))),
            "idle_private_bytes": str(idle["private_bytes"]),
            "loaded_private_bytes": str(loaded["private_bytes"]),
            "delta_private_bytes": str(max(0, int(loaded["private_bytes"]) - int(idle["private_bytes"]))),
            "idle_cgroup_current_bytes": str(idle["cgroup_current_bytes"]),
            "loaded_cgroup_current_bytes": str(loaded["cgroup_current_bytes"]),
            "delta_cgroup_current_bytes": str(max(0, int(loaded["cgroup_current_bytes"]) - int(idle["cgroup_current_bytes"]))),
            "idle_cgroup_anon_bytes": str(idle["cgroup_anon_bytes"]),
            "loaded_cgroup_anon_bytes": str(loaded["cgroup_anon_bytes"]),
            "delta_cgroup_anon_bytes": str(max(0, int(loaded["cgroup_anon_bytes"]) - int(idle["cgroup_anon_bytes"]))),
            "idle_cgroup_file_bytes": str(idle["cgroup_file_bytes"]),
            "loaded_cgroup_file_bytes": str(loaded["cgroup_file_bytes"]),
            "redis_used_memory_idle_bytes": str(idle["redis_used_memory_bytes"]),
            "redis_used_memory_loaded_bytes": str(loaded["redis_used_memory_bytes"]),
            "redis_allocator_rss_idle_bytes": str(idle["redis_allocator_rss_bytes"]),
            "redis_allocator_rss_loaded_bytes": str(loaded["redis_allocator_rss_bytes"]),
            "delta_pss_per_key_bytes": f"{int(max(0, int(loaded['pss_bytes']) - int(idle['pss_bytes']))) / args.keys:.3f}",
            "pss_amplification": f"{int(max(0, int(loaded['pss_bytes']) - int(idle['pss_bytes']))) / logical_bytes:.5f}",
            "ops_per_sec": perf.get("ops_per_sec", ""),
            "logical_payload_gb_per_sec": perf.get("logical_payload_gb_per_sec", ""),
            "server_vcpu_consumed": perf.get("vcpu_consumed", ""),
            "p50_ns": perf.get("p50_ns", ""),
            "p99_ns": perf.get("p99_ns", ""),
            "p999_ns": perf.get("p999_ns", ""),
            "errors": perf.get("errors", ""),
        }
        return row
    finally:
        if container_id:
            try:
                logs = subprocess.run(["docker", "logs", container_id], cwd=ROOT, check=False, text=True, capture_output=True)
                (log_dir / f"container-{target}-{value_size}-{profile}-r{repeat}.log").write_text(
                    logs.stdout + logs.stderr,
                    encoding="utf-8",
                )
                metadata = subprocess.run(["docker", "inspect", container_id], cwd=ROOT, check=False, text=True, capture_output=True)
                (log_dir / f"container-{target}-{value_size}-{profile}-r{repeat}.json").write_text(
                    metadata.stdout,
                    encoding="utf-8",
                )
                stopped = subprocess.run(["docker", "stop", "--time", "15", container_id], cwd=ROOT, check=False, text=True, capture_output=True)
                if stopped.returncode != 0:
                    print(f"cleanup: failed to stop run-owned container {container_id}: {stopped.stderr.strip()}", file=sys.stderr)
                removed = subprocess.run(["docker", "rm", container_id], cwd=ROOT, check=False, text=True, capture_output=True)
                if removed.returncode != 0:
                    print(f"cleanup: failed to remove run-owned container {container_id}: {removed.stderr.strip()}", file=sys.stderr)
            except OSError as exc:
                print(f"cleanup: container {container_id} may remain: {exc}", file=sys.stderr)


def summarize(rows: list[dict[str, str]], path: pathlib.Path) -> None:
    grouped: dict[tuple[str, str, str, str], list[dict[str, str]]] = {}
    for row in rows:
        shape = (row["value_pattern"], row["distribution"], row["value_bytes_each"], row["target"])
        grouped.setdefault(shape, []).append(row)
    profiles = sorted({(row["value_pattern"], row["distribution"], int(row["value_bytes_each"])) for row in rows})
    lines = [
        "# Redis Memory Density Diagnostic Results", "",
        f"Candidate: `{rows[0]['candidate_sha']}`", "",
        "Classification: diagnostic run without an authoritative resource reservation; CPU affinity is not pinned, and concurrent host workloads may affect performance results.", "",
        "Incremental PSS is the primary density metric; incremental cgroup ANON is the cross-check. Every comparison below matches value pattern, access distribution, and value size. The loaded keyspace is checked with DBSIZE and exact GET payload samples.", "",
        "| Pattern | Distribution | Value B | Target | PSS B/key | ANON B/key | Ops/sec | p99 us |",
        "|---|---|---:|---|---:|---:|---:|---:|",
    ]
    ratios = []
    for pattern, distribution, size in profiles:
        medians = {}
        for target in sorted({row["target"] for row in rows}):
            group = grouped.get((pattern, distribution, str(size), target), [])
            if not group:
                continue
            def med(field: str) -> float:
                return statistics.median(float(item[field]) for item in group if item[field] != "")
            pss = med("delta_pss_per_key_bytes")
            anon = med("delta_cgroup_anon_bytes") / int(group[0]["key_count"])
            medians[target] = (pss, anon)
            lines.append(f"| {pattern} | {distribution} | {size} | {target} | {pss:.2f} | {anon:.2f} | {med('ops_per_sec'):,.0f} | {med('p99_ns') / 1000:.2f} |")
        if "redis" in medians and "shardcache-resp" in medians:
            redis_pss, redis_anon = medians["redis"]
            candidate_pss, candidate_anon = medians["shardcache-resp"]
            pss_ratio = candidate_pss / redis_pss if redis_pss > 0 else float("nan")
            anon_ratio = candidate_anon / redis_anon if redis_anon > 0 else float("nan")
            ratios.append(f"| {pattern} | {distribution} | {size} | {pss_ratio:.3f}x | {anon_ratio:.3f}x |")
    if ratios:
        lines.extend(["", "ShardCache / Redis (below 1.0x uses less incremental memory):", "",
                      "| Pattern | Distribution | Value B | PSS ratio | ANON ratio |",
                      "|---|---|---:|---:|---:|", *ratios])
    lines.extend(["", "Values are stored without compression in the compact candidate. High-entropy bytes use deterministic SplitMix64; compressible bytes repeat x; repeating bytes preserve the original 0..255 baseline. Each profile uses the same payload for every key, so this suite does not qualify inter-key deduplication.", "",
                  "Performance is closed-loop saturation with fresh containers. Compare each candidate row with the same ShardCache baseline shape: throughput must be at least 95% and p99 at most 105%. Resource reservation and matched offered-load qualification remain external requirements.", ""])
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--targets", default="redis,shardcache-resp")
    parser.add_argument("--keys", type=int, default=100_000)
    parser.add_argument("--value-sizes", default="16,64,256,1024,4096")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--vcpus", type=int, default=1)
    parser.add_argument("--memory-limit", default="4g")
    parser.add_argument("--build-jobs", type=int, default=4)
    parser.add_argument("--clients", type=int, default=16)
    parser.add_argument("--pipeline", type=int, default=1)
    parser.add_argument("--mix", default="80-20")
    parser.add_argument("--distribution", default="uniform", help="single access profile (backward compatible)")
    parser.add_argument("--distributions", help="semicolon-separated access profiles, e.g. 'uniform;hot:1000:90'")
    parser.add_argument("--value-patterns", default="compressible,high-entropy", help="comma-separated repeating,compressible,high-entropy")
    parser.add_argument("--duration", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--sample-delay", type=float, default=0.2)
    parser.add_argument("--settle-seconds", type=float, default=1.0)
    parser.add_argument("--out-dir", type=pathlib.Path)
    parser.add_argument("--candidate-sha", default="unknown")
    parser.add_argument("--candidate-tag", default="local")
    parser.add_argument("--shardcache-features", default="redis-server")
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()

    targets = [part.strip() for part in args.targets.split(",") if part.strip()]
    value_patterns = [part.strip() for part in args.value_patterns.split(",") if part.strip()]
    if not value_patterns or set(value_patterns) - {"repeating", "compressible", "high-entropy"}:
        parser.error("--value-patterns must use repeating, compressible, or high-entropy")
    distributions = [part.strip() for part in (args.distributions or args.distribution).split(";") if part.strip()]
    if not distributions:
        parser.error("--distributions must have at least one access profile")
    unknown = sorted(set(targets) - set(TARGETS))
    if unknown:
        parser.error(f"unsupported targets: {', '.join(unknown)}")
    if not targets or args.keys <= 0 or args.repeats <= 0 or args.vcpus <= 0:
        parser.error("targets, keys, repeats, and vcpus must be positive")
    if args.pipeline <= 0 or args.clients <= 0 or args.duration <= 0 or args.warmup < 0:
        parser.error("clients, pipeline, and duration must be positive; warmup cannot be negative")
    if args.build_jobs <= 0 or args.samples <= 0 or args.sample_delay < 0 or args.settle_seconds < 0:
        parser.error("build jobs and memory samples must be positive; delays cannot be negative")
    if not args.shardcache_features.strip():
        parser.error("--shardcache-features must be non-empty")
    try:
        value_sizes = parse_csv_ints(args.value_sizes, "value sizes")
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))
    if platform.system() != "Linux":
        parser.error("run this density suite on Linux so process and cgroup memory are comparable")
    if not pathlib.Path("/sys/fs/cgroup/cgroup.controllers").is_file():
        parser.error("this suite requires cgroup v2 on the benchmark host")
    if not SATURATION.is_file() and args.skip_build:
        parser.error(f"--skip-build requested but {SATURATION} is missing")
    try:
        dirty_state = command(["git", "status", "--porcelain", "--untracked-files=normal"])
    except subprocess.CalledProcessError:
        dirty_state = "unable to read git status"
    if dirty_state:
        parser.error("benchmark candidate must have a clean worktree; commit the exact source snapshot before running")
    try:
        actual_sha = command(["git", "rev-parse", "HEAD"])
    except subprocess.CalledProcessError:
        parser.error("could not resolve the benchmark candidate SHA")
    if args.candidate_sha == "unknown":
        args.candidate_sha = actual_sha
    elif args.candidate_sha != actual_sha:
        parser.error(f"--candidate-sha {args.candidate_sha} does not match HEAD {actual_sha}")
    if args.candidate_tag == "local":
        args.candidate_tag = args.candidate_sha[:12]
    args.run_id = f"{dt.datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
    args.runtime_image_tag = f"shardcache-memory-density:{args.candidate_tag}-{args.run_id[-8:]}"

    out_dir = args.out_dir or ROOT / "benchmarks" / "results" / f"memory-density-{args.run_id}"
    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "raw").mkdir(exist_ok=True)
    (out_dir / "logs").mkdir(exist_ok=True)
    if not args.skip_build:
        command(
            ["cargo", "build", "--locked", "--release", "--jobs", str(args.build_jobs), "-p", "shardcache-benchmarks", "--bin", "saturation"],
            log=out_dir / "logs" / "cargo-build-saturation.log",
        )
    if not SATURATION.is_file():
        parser.error(f"saturation binary is missing: {SATURATION}")
    runtime_build = build_shardcache_image(args, out_dir) if "shardcache-resp" in targets else {}
    runtime_image_id = runtime_build.get("runtime_engine_image_id", "")
    for target in targets:
        image_tag = TARGETS[target]["image"]
        if image_tag and subprocess.run(["docker", "image", "inspect", image_tag], check=False, capture_output=True).returncode != 0:
            command(["docker", "pull", image_tag], log=out_dir / "logs" / f"docker-pull-{target}.log")

    raw_path = out_dir / "memory-density.csv"
    rows: list[dict[str, str]] = []
    run_manifest = {
        "candidate_sha": args.candidate_sha,
        "host": platform.node(),
        "kernel": platform.release(),
        "architecture": platform.machine(),
        "logical_cpus": os.cpu_count(),
        "host_memory": host_memory_info(),
        "host_loadavg": pathlib.Path("/proc/loadavg").read_text(encoding="utf-8").strip(),
        "cargo_version": tool_version(["cargo", "--version"]),
        "rustc_version": tool_version(["rustc", "--version"]),
        "docker_version": tool_version(["docker", "version", "--format", "{{.Server.Version}}"]),
        "run_id": args.run_id,
        "classification": "diagnostic-unreserved",
        "cpu_affinity": "none",
        "build_jobs": args.build_jobs,
        "samples": args.samples,
        "sample_delay_seconds": args.sample_delay,
        "settle_seconds": args.settle_seconds,
        "shardcache_features": args.shardcache_features if runtime_image_id else "",
        "shardcache_builder_image": SHARDCACHE_BUILDER_IMAGE if runtime_image_id else "",
        "shardcache_builder_manifest_digest": runtime_build.get("base_materials", {}).get("builder", {}).get("manifest_digest", ""),
        "shardcache_runtime_base_image": SHARDCACHE_RUNTIME_BASE_IMAGE if runtime_image_id else "",
        "shardcache_runtime_base_manifest_digest": runtime_build.get("base_materials", {}).get("runtime", {}).get("manifest_digest", ""),
        "shardcache_runtime_image_tag": args.runtime_image_tag if runtime_image_id else "",
        "shardcache_runtime_image_id": runtime_image_id,
        "shardcache_build_provenance": runtime_build,
        "shardcache_dockerfile_sha256": file_sha256(out_dir / "runtime-context" / "Dockerfile") if runtime_image_id else "",
        "saturation_binary_sha256": file_sha256(SATURATION),
        "value_patterns": value_patterns,
        "payload_scope": "same payload for every key within each profile",
        "high_entropy_generator": "SplitMix64 seed 0x4544454e22660001, little endian",
        "distributions": distributions,
        "targets": targets,
        "keys": args.keys,
        "key_bytes_each": KEY_BYTES,
        "value_sizes": value_sizes,
        "repeats": args.repeats,
        "vcpus": args.vcpus,
        "clients": args.clients,
        "pipeline": args.pipeline,
        "mix": args.mix,
        "distribution": args.distribution,
        "duration_seconds": args.duration,
        "warmup_seconds": args.warmup,
        "memory_limit": args.memory_limit,
        "started_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    (out_dir / "manifest.json").write_text(json.dumps(run_manifest, indent=2) + "\n", encoding="utf-8")

    for value_pattern in value_patterns:
        args.value_pattern = value_pattern
        for distribution in distributions:
            args.distribution = distribution
            for value_size in value_sizes:
                for repeat in range(1, args.repeats + 1):
                    target_order = targets if repeat % 2 == 1 else list(reversed(targets))
                    for target in target_order:
                        print(f"memory-density: target={target} keys={args.keys} value_size={value_size} pattern={value_pattern} distribution={distribution} repeat={repeat}/{args.repeats}", flush=True)
                        row = run_case(args, out_dir, target, value_size, repeat)
                        rows.append(row)
                        with raw_path.open("w", newline="", encoding="utf-8") as output:
                            writer = csv.DictWriter(output, fieldnames=list(rows[0]))
                            writer.writeheader()
                            writer.writerows(rows)
                        summarize(rows, out_dir / "report.md")
    remaining = command(
        [
            "docker",
            "ps",
            "--all",
            "--quiet",
            "--filter",
            f"label=io.shard-kv.memory-density-run={args.run_id}",
        ]
    )
    if remaining:
        raise RuntimeError(f"run-owned containers remain after cleanup: {remaining.splitlines()}")
    print(f"memory-density report: {out_dir / 'report.md'}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as exc:
        print(f"command failed ({exc.returncode}): {exc.cmd}", file=sys.stderr)
        raise
