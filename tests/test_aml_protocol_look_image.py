"""The DAT-2 look image (CD-8): the default datagen image, its lineage row,
the pins docs/internal/aml-protocol.md names, and the configs that pin it
agree with the tree."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import yaml

from lakebench.config.schema import ImagesConfig
from lakebench.metrics import corpus_identity as ci
from lakebench.metrics.release_record import release_datagen_digest
from lakebench.modules.pipeline_engines.spark.job import REFERENCE_PY_DEPS

ROOT = Path(__file__).resolve().parents[1]
PROTOCOL = ROOT / "docs" / "internal" / "aml-protocol.md"
V16_ROOT = "sha256:5fda9025fb9b455b390e1138d82e9f6ef16d214dfa9419815be0111d2f6fce0a"


def _section() -> str:
    text = PROTOCOL.read_text()
    start = text.index("## The look image")
    return text[start : text.index("\n## ", start + 1)]


def _default_parts() -> tuple[str, str]:
    image = ImagesConfig().datagen
    m = re.fullmatch(r"docker\.io/sillidata/lb-datagen:([0-9a-f]{7})@(sha256:[0-9a-f]{64})", image)
    assert m, f"the default datagen image is not <repo>:<commit tag>@sha256:<digest>: {image}"
    return m.group(1), m.group(2)


def test_default_is_pinned_by_digest_with_lineage_evidence():
    tag, digest = _default_parts()
    assert release_datagen_digest() == digest
    table = ci.load_lineage()
    row = table.get(digest)
    assert row is not None, "the default datagen image has no lineage row"
    assert row.canonical == V16_ROOT
    assert ci.check_lineage_evidence({digest: row}, ROOT) == []
    assert row.build_commit and row.build_commit.startswith(tag)


def test_protocol_names_the_default_image_and_its_build():
    tag, digest = _default_parts()
    sec = _section()
    assert digest in sec and f"lb-datagen:{tag}" in sec
    row = ci.load_lineage()[digest]
    assert row.build_commit in sec
    assert f"compare-{digest.split(':')[1][:12]}.json" in sec


def test_protocol_names_the_generator_and_scorer_pins():
    sec = _section()
    lock = (ROOT / "datagen_rs" / "Cargo.lock").read_bytes()
    assert hashlib.sha256(lock).hexdigest() in sec, "Cargo.lock changed: re-pin the look image"
    froms = re.findall(r"^FROM (\S+)", (ROOT / "datagen_rs" / "Dockerfile").read_text(), re.M)
    assert len(froms) == 2
    for ref in froms:
        assert ref in sec, ref
    for pin in REFERENCE_PY_DEPS:
        name, version = pin.split("==")
        assert f"{name} {version}" in sec, pin


def test_perf_configs_pin_the_default_image():
    """LB-263: a pinned config naming a deleted tag cannot run."""
    for path in sorted((ROOT / "benchmarks" / "perf").glob("*.yaml")):
        if path.name == "baselines.yaml":
            continue
        doc = yaml.safe_load(path.read_text())
        assert doc["images"]["datagen"] == ImagesConfig().datagen, path.name
