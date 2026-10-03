#!/usr/bin/env python3
"""Offline regressions for the memory density build provenance contract."""

import argparse
import base64
import copy
import importlib.util
import json
import pathlib
import tempfile
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).with_name("run-memory-density-benchmark.py")
SPEC = importlib.util.spec_from_file_location("density_benchmark", SCRIPT)
BENCHMARK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCHMARK)
MANIFEST = "sha256:" + "a" * 64
CONFIG = "sha256:" + "b" * 64
CANDIDATE = "c" * 40
TAG = "shardcache-memory-density:test"
FEATURES = "redis-server,experimental-compact-point-storage"
DOCKERFILE = b"FROM rust:1.93-slim-bookworm AS builder\nFROM debian:bookworm-slim AS runtime\n"
LOG = f"#14 exporting manifest {MANIFEST} 0.0s done\n#14 exporting config {CONFIG} 0.0s done\n"


def metadata(dockerfile=DOCKERFILE):
    materials = []
    steps = []
    for reference, digest in (("rust:1.93-slim-bookworm", "d" * 64), ("debian:bookworm-slim", "e" * 64)):
        name, tag = reference.split(":")
        materials.append({"uri": f"pkg:docker/{name}@{tag}?platform=linux%2Famd64", "digest": {"sha256": digest}})
        steps.append({"op": {"Op": {"source": {"identifier": f"docker-image://docker.io/library/{reference}@sha256:{digest}"}}}})
    return {
        "buildx.build.provenance": {
            "buildType": "https://mobyproject.org/buildkit@v1",
            "materials": materials,
            "buildConfig": {"llbDefinition": steps},
            "invocation": {
                "configSource": {"entryPoint": "Dockerfile"},
                "parameters": {"frontend": "dockerfile.v0", "args": {
                    "build-arg:SHARDCACHE_FEATURES": FEATURES, "build-arg:CARGO_BUILD_JOBS": "4"}},
                "environment": {"platform": "linux/amd64"},
            },
            "metadata": {"completeness": {"materials": False}, "https://mobyproject.org/buildkit@v1#metadata": {
                "vcs": {"revision": CANDIDATE},
                "source": {"infos": [{"filename": "Dockerfile", "data": base64.b64encode(dockerfile).decode()}]},
            }},
        },
        "buildx.build.ref": "owned-builder/owned-node/unique-build",
        "image.name": "docker.io/library/" + TAG,
        "containerimage.digest": MANIFEST,
        "containerimage.descriptor": {"digest": MANIFEST, "mediaType": "application/vnd.oci.image.manifest.v1+json",
                                      "platform": {"os": "linux", "architecture": "amd64"}},
    }


class BuildProvenanceTests(unittest.TestCase):
    def resolve(self, data=None, **kwargs):
        options = dict(features=FEATURES, build_jobs=4, candidate_sha=CANDIDATE, image_tag=TAG, engine_image_id=MANIFEST)
        options.update(kwargs)
        return BENCHMARK.resolve_shardcache_build(metadata() if data is None else data, DOCKERFILE, LOG, **options)

    def test_exporter_without_metadata_config_accepts_manifest_engine_id(self):
        result = self.resolve()
        self.assertEqual(result["runtime_config_digest"], CONFIG)
        self.assertEqual(result["runtime_engine_image_id_kind"], "manifest-digest")
        self.assertEqual(result["base_materials"]["builder"]["manifest_digest"], "sha256:" + "d" * 64)
        self.assertEqual(result["base_materials"]["runtime"]["manifest_digest"], "sha256:" + "e" * 64)
        self.assertIs(result["materials_complete"], False)

    def test_config_engine_id_is_distinguished(self):
        self.assertEqual(self.resolve(engine_image_id=CONFIG)["runtime_engine_image_id_kind"], "config-digest")

    def test_optional_config_metadata_is_cross_checked(self):
        data = metadata()
        data["containerimage.config.digest"] = CONFIG
        self.resolve(data)
        data["containerimage.config.digest"] = MANIFEST
        with self.assertRaises(RuntimeError):
            self.resolve(data)

    def test_missing_or_nested_material_cannot_replace_direct_from_material(self):
        for stage in (0, 1):
            data = metadata()
            removed = data["buildx.build.provenance"]["materials"].pop(stage)
            data["buildx.build.provenance"]["unrelated"] = {"materials": [removed]}
            with self.subTest(stage=stage), self.assertRaises(RuntimeError):
                self.resolve(data)

    def test_duplicate_and_conflicting_materials_are_rejected(self):
        for conflicting in (False, True):
            data = metadata()
            extra = copy.deepcopy(data["buildx.build.provenance"]["materials"][0])
            if conflicting:
                extra["digest"]["sha256"] = "f" * 64
            data["buildx.build.provenance"]["materials"].append(extra)
            with self.subTest(conflicting=conflicting), self.assertRaises(RuntimeError):
                self.resolve(data)

    def test_wrong_tag_platform_and_malformed_digest_are_rejected(self):
        for stage in (0, 1):
            for field, value in (("tag", "unrelated"), ("platform", "linux%2Farm64"), ("digest", "short")):
                data = metadata()
                item = data["buildx.build.provenance"]["materials"][stage]
                if field == "digest":
                    item["digest"]["sha256"] = value
                elif field == "platform":
                    item["uri"] = item["uri"].replace("linux%2Famd64", value)
                else:
                    item["uri"] = item["uri"].split("@")[0] + "@" + value + "?platform=linux%2Famd64"
                with self.subTest(stage=stage, field=field), self.assertRaises(RuntimeError):
                    self.resolve(data)

    def test_material_must_match_resolved_llb_source(self):
        data = metadata()
        data["buildx.build.provenance"]["materials"][0]["digest"]["sha256"] = "f" * 64
        with self.assertRaises(RuntimeError):
            self.resolve(data)

    def test_source_dockerfile_revision_and_features_are_bound(self):
        for field in ("dockerfile", "revision", "features"):
            data = metadata()
            provenance = data["buildx.build.provenance"]
            if field == "dockerfile":
                provenance["metadata"]["https://mobyproject.org/buildkit@v1#metadata"]["source"]["infos"][0]["data"] = base64.b64encode(b"other").decode()
            elif field == "revision":
                provenance["metadata"]["https://mobyproject.org/buildkit@v1#metadata"]["vcs"]["revision"] = "f" * 40
            else:
                provenance["invocation"]["parameters"]["args"]["build-arg:SHARDCACHE_FEATURES"] = "redis-server"
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                self.resolve(data)

    def test_export_descriptor_tag_and_engine_identity_are_bound(self):
        for field in ("descriptor", "tag", "engine"):
            data = metadata()
            options = {}
            if field == "descriptor":
                data["containerimage.descriptor"]["digest"] = CONFIG
            elif field == "tag":
                data["image.name"] = "other:tag"
            else:
                options["engine_image_id"] = "sha256:" + "f" * 64
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                self.resolve(data, **options)

    def test_missing_ambiguous_or_other_export_vertex_is_rejected(self):
        for log in ("", LOG + LOG, LOG.replace("#14 exporting config", "#15 exporting config"), LOG.replace(MANIFEST, CONFIG)):
            with self.subTest(log=log), self.assertRaises(RuntimeError):
                BENCHMARK.resolve_shardcache_build(metadata(), DOCKERFILE, log, features=FEATURES,
                    build_jobs=4, candidate_sha=CANDIDATE, image_tag=TAG, engine_image_id=MANIFEST)

    def test_build_owns_one_metadata_path_and_only_inspects_exported_tag(self):
        args = argparse.Namespace(shardcache_features=FEATURES, build_jobs=4, candidate_sha=CANDIDATE, runtime_image_tag=TAG)
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory)
            def build(argv, *, log, env):
                self.assertEqual(argv.count("--metadata-file"), 1)
                self.assertEqual(env["BUILDX_METADATA_PROVENANCE"], "max")
                data = metadata((output / "runtime-context/Dockerfile").read_bytes())
                pathlib.Path(argv[argv.index("--metadata-file") + 1]).write_text(json.dumps(data))
                log.parent.mkdir()
                log.write_text(LOG)
            with mock.patch.object(BENCHMARK, "command", side_effect=build) as command, mock.patch.object(BENCHMARK, "get_inspect", return_value=MANIFEST) as inspect:
                result = BENCHMARK.build_shardcache_image(args, output)
                self.assertEqual(result["runtime_config_digest"], CONFIG)
                inspect.assert_called_once_with(TAG, "Id")
                with self.assertRaises(RuntimeError):
                    BENCHMARK.build_shardcache_image(args, output)
                self.assertEqual(command.call_count, 1)


if __name__ == "__main__":
    unittest.main()
