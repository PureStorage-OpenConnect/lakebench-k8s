"""scripts/spark_shard_weights.py: the recorded seconds per Spark test file
that --lb-shard balances on, from JUnit reports."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location(
    "spark_shard_weights", ROOT / "scripts" / "spark_shard_weights.py"
)
assert _spec is not None and _spec.loader is not None
ssw = importlib.util.module_from_spec(_spec)
sys.modules["spark_shard_weights"] = ssw
_spec.loader.exec_module(ssw)


def _report(path: Path, cases: list[tuple[str, float]]) -> Path:
    body = "".join(
        f'<testcase classname="{c}" name="t{i}" time="{t}"/>' for i, (c, t) in enumerate(cases)
    )
    path.write_text(
        f'<?xml version="1.0"?><testsuites><testsuite name="pytest">{body}</testsuite></testsuites>'
    )
    return path


def _tree(tmp_path: Path) -> Path:
    spark = tmp_path / "tests" / "spark"
    spark.mkdir(parents=True)
    for name in ("test_x.py", "test_y.py"):
        (spark / name).write_text("")
    return tmp_path


def test_classname_resolves_to_its_file(tmp_path):
    root = _tree(tmp_path)
    assert ssw.file_of("tests.spark.test_x", root) == "tests/spark/test_x.py"
    assert ssw.file_of("tests.spark.test_y.TestThing", root) == "tests/spark/test_y.py"
    assert ssw.file_of("tests.spark.test_gone", root) is None


def test_weights_sum_per_report_and_average_over_reports(tmp_path):
    root = _tree(tmp_path)
    one = _report(
        tmp_path / "1.xml",
        [("tests.spark.test_x", 10.0), ("tests.spark.test_x", 5.0), ("tests.spark.test_y.T", 2.0)],
    )
    two = _report(
        tmp_path / "2.xml", [("tests.spark.test_x", 25.0), ("tests.spark.test_gone", 9.0)]
    )
    assert ssw.weights([one, two], root) == {
        "tests/spark/test_x.py": 20.0,
        "tests/spark/test_y.py": 2.0,
    }


def test_main_writes_the_weights_json(tmp_path):
    report = _report(tmp_path / "r.xml", [("tests.spark.test_spark_harness_jvm", 12.5)])
    out = tmp_path / "w.json"
    assert ssw.main(["--source", "unit test", "--out", str(out), str(report)]) == 0
    doc = json.loads(out.read_text())
    assert doc["source"] == "unit test"
    assert doc["seconds"] == {"tests/spark/test_spark_harness_jvm.py": 12.5}


def test_main_refuses_reports_without_spark_files(tmp_path):
    report = _report(tmp_path / "r.xml", [("tests.unit.test_nothing", 1.0)])
    assert ssw.main(["--source", "x", "--out", str(tmp_path / "w.json"), str(report)]) == 1
    assert not (tmp_path / "w.json").exists()
