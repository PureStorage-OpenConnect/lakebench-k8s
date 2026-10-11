"""score_financial scores only the run that launched it.

gold.detection_status names the run whose alerts gold holds. In a
continuous run on a reused catalog that was the previous run's until
gold-refresh's first tick, and the score took it without comparing it to
its own LB_RUN_ID, so it could publish another run's recall as this run's.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts/score_financial.py"


@pytest.fixture
def check():
    """The function lifted from the script, which imports pyspark at the top
    (not installed in the unit tier)."""
    tree = ast.parse(_SCRIPT.read_text())
    fns = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "check_status_run"]
    assert fns, "check_status_run is gone from score_financial.py"
    ns = {"CATALOG": "lakehouse", "GOLD_STATUS": "gold.detection_status"}
    exec(compile(ast.Module(fns, []), str(_SCRIPT), "exec"), ns)  # noqa: S102
    return ns["check_status_run"]


@pytest.mark.parametrize(
    ("status_run", "run_id"),
    [
        ("20261002-101010-abcdef", "20261002-101010-abcdef"),  # the run's own status
        ("20261002-101010-abcdef-c3", "20261002-101010-abcdef"),  # a batch cycle <run>-c<n>
        ("20261001-090909-123456", ""),  # outside a run, gold's status decides
    ],
)
def test_status_of_this_run_is_accepted(check, status_run, run_id):
    check(status_run, run_id)


@pytest.mark.parametrize(
    "status_run",
    ["20261001-090909-123456", "20261001-090909-123456-c1", "20261002-101010-abcdef0"],
)
def test_another_runs_status_is_refused(check, status_run):
    with pytest.raises(SystemExit, match="not this run"):
        check(status_run, "20261002-101010-abcdef")
