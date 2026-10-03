"""scripts/check_coverage.py: per-file floors fail loudly and name the file."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "check_coverage", ROOT / "scripts" / "check_coverage.py"
)
cc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cc)


def _report(**pcts):
    return {"files": {f"src/{k}": {"summary": {"percent_covered": v}} for k, v in pcts.items()}}


def test_at_or_above_floor_passes():
    failures, lines = cc.check(_report(**{"lakebench/a.py": 80.0}), {"lakebench/a.py": 80.0})
    assert failures == [] and lines[0].startswith("ok")


def test_below_floor_fails_and_names_file():
    failures, _ = cc.check(_report(**{"lakebench/a.py": 79.99}), {"lakebench/a.py": 80.0})
    assert len(failures) == 1 and "lakebench/a.py" in failures[0]


def test_missing_file_fails():
    failures, _ = cc.check(_report(), {"lakebench/a.py": 10.0})
    assert "not in the coverage report" in failures[0]


def test_main_exit_code(tmp_path):
    path = tmp_path / "c.json"
    path.write_text(json.dumps(_report(**dict.fromkeys(cc.FLOORS["unit"], 100.0))))
    assert cc.main(["--suite", "unit", str(path)]) == 0
    path.write_text(json.dumps(_report(**dict.fromkeys(cc.FLOORS["unit"], 0.0))))
    assert cc.main(["--suite", "unit", str(path)]) == 1


def test_ci_checks_both_suites():
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
    assert "check_coverage.py --suite unit" in ci
    assert "check_coverage.py --suite spark" in ci


def test_new_floor_fails_below():
    """The 2026-10-01 floors are in force: one point under destroy.py's fails."""
    pcts = dict.fromkeys(cc.FLOORS["unit"], 100.0)
    pcts["lakebench/deploy/destroy.py"] = 78.0
    failures, _ = cc.check(_report(**pcts), cc.FLOORS["unit"])
    assert failures == ["lakebench/deploy/destroy.py: 78.00% is below the 79% floor"]
    for name in (
        "lakebench/deploy/ownership.py",
        "lakebench/s3/client.py",
        "lakebench/metrics/experiment.py",
        "lakebench/metrics/c360_correctness.py",
    ):
        assert name in cc.FLOORS["unit"], name


def test_slow_tests_import_no_floored_module():
    """Deselecting the slow tests from the unit legs cannot lower a floored
    number: the slow AML statistics modules import none of the floored ones."""
    import subprocess
    import sys

    floored = sorted(
        "lakebench." + k[len("lakebench/") : -len(".py")].replace("/", ".")
        for k in cc.FLOORS["unit"]
    )
    code = (
        "import sys\n"
        "import lakebench.aml.fidelity_gate, lakebench.aml.scale_invariance\n"
        "import lakebench.aml.d8_shards, lakebench.aml.predictions\n"
        f"print([m for m in {floored!r} if m in sys.modules])\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env={"PYTHONPATH": str(ROOT / "src")},
        check=True,
    ).stdout.strip()
    assert out == "[]", out


def test_spark_floors_cover_the_stream_files_and_fail_below():
    """The Spark suite floors the AML silver stream, common.py and the Delta
    silver stream as well as detection_rules.py, and each fails one point
    under its floor."""
    spark = cc.FLOORS["spark"]
    for name in (
        "lakebench/spark/scripts/silver_stream_financial.py",
        "lakebench/spark/scripts/common.py",
        "lakebench/spark/scripts/silver_stream_delta.py",
        "lakebench/spark/scripts/detection_rules.py",
    ):
        assert name in spark, name
        values = {k: (v - 1.0 if k == name else 100.0) for k, v in spark.items()}
        failures, _ = cc.check(_report(**values), spark)
        assert len(failures) == 1 and name in failures[0], failures
