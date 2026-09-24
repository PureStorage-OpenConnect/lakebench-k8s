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
