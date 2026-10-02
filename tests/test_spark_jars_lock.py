"""The Spark-tier jar lock pins what the product requests (QA-2, C2-23).

tests/spark/jars.lock.json is what CI and ``scripts/fetch_test_jars.py`` put
on the test classpath. The product's own coordinates come from job.py
(``_FORMAT_VERSION_DEFAULTS``, ``iceberg_runtime_suffix_for``,
``_delta_spark_artifact``), so a default bump without a lock bump fails
here instead of testing one Iceberg locally and another in CI. When SD-2
moves the coordinates out of job.py, this test moves with them.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from lakebench.modules.pipeline_engines.spark import job

ROOT = Path(__file__).resolve().parents[1]
LOCK = ROOT / "tests" / "spark" / "jars.lock.json"


def _product_coords(leg: str) -> list[str]:
    key = tuple(int(x) for x in leg.split("."))
    spark_key = (key[0], key[1])
    defaults = job._FORMAT_VERSION_DEFAULTS[spark_key]
    ice = defaults["iceberg"]
    suffix = job.iceberg_runtime_suffix_for(spark_key, ice)
    return [
        f"org.apache.iceberg:iceberg-spark-runtime-{suffix}_2.13:{ice}",
        job._delta_spark_artifact("_2.13", defaults["delta"]),
    ]


def _lock_coords(lock: dict, leg: str) -> list[str]:
    return [j["coord"] for j in lock["legs"][leg]["jars"] if "transitive_of" not in j]


def _mismatches(lock: dict) -> dict[str, tuple[list[str], list[str]]]:
    out = {}
    for leg in lock["legs"]:
        want, have = _product_coords(leg), _lock_coords(lock, leg)
        if want != have:
            out[leg] = (want, have)
    return out


def test_jars_lock_matches_product_defaults():
    lock = json.loads(LOCK.read_text())
    assert lock["schema"] == 1
    assert set(lock["legs"]) == {"4.0", "4.1"}
    assert _mismatches(lock) == {}


def test_a_product_default_bump_without_a_lock_bump_fails(monkeypatch):
    lock = json.loads(LOCK.read_text())
    bumped = dict(job._FORMAT_VERSION_DEFAULTS)
    bumped[(4, 1)] = dict(bumped[(4, 1)], iceberg="1.12.0")
    monkeypatch.setattr(job, "_FORMAT_VERSION_DEFAULTS", bumped)
    assert "4.1" in _mismatches(lock)


def test_lock_entries_are_pinned_and_paired():
    lock = json.loads(LOCK.read_text())
    for leg, body in lock["legs"].items():
        assert re.fullmatch(rf"{re.escape(leg)}\.\d+", body["pyspark"]), leg
        kinds = sorted(j["kind"] for j in body["jars"])
        assert kinds == ["delta", "delta", "iceberg"], leg
        for j in body["jars"]:
            assert re.fullmatch(r"[0-9a-f]{64}", j["sha256"]), j["coord"]
            if "transitive_of" in j:
                assert j["coord"].startswith("io.delta:delta-storage:")
                assert j["transitive_of"] in _lock_coords(lock, leg)
                # delta-storage is released with delta-spark.
                assert j["coord"].split(":")[2] == j["transitive_of"].split(":")[2]


@pytest.mark.parametrize("leg", ["4.0", "4.1"])
def test_lock_jars_pass_the_harness_classifier(leg, tmp_path):
    """The file names the fetcher writes are the names the Spark-tier
    harness accepts for that line."""
    import importlib.util
    import sys

    spec = importlib.util.spec_from_file_location(
        "lb_spark_harness_lock", ROOT / "tests" / "spark" / "conftest.py"
    )
    assert spec is not None and spec.loader is not None
    harness = importlib.util.module_from_spec(spec)
    sys.modules["lb_spark_harness_lock"] = harness
    try:
        spec.loader.exec_module(harness)
        lock = json.loads(LOCK.read_text())
        names = []
        for j in lock["legs"][leg]["jars"]:
            _g, artifact, version = j["coord"].split(":")
            (tmp_path / f"{artifact}-{version}.jar").write_bytes(b"")
            names.append(str(tmp_path / f"{artifact}-{version}.jar"))
        jars = harness.resolve_jars({"LB_SPARK_TEST_JARS": ",".join(names)}, leg)
        assert jars.has("iceberg") and jars.has("delta") and not jars.other
    finally:
        sys.modules.pop("lb_spark_harness_lock", None)
