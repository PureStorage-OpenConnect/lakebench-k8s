"""scripts/check_parity_job.py: the AML parity job passes only when exactly
the registered guards ran and each passed."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.conftest import exec_repo_script

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def job():
    return exec_repo_script(REPO / "scripts" / "check_parity_job.py", "check_parity_job_test")


GUARDS = ["a.py::t1", "b.py::t2[x]"]


def test_all_passed(job):
    assert job.problems(dict.fromkeys(GUARDS, "passed"), GUARDS, 0) == []


@pytest.mark.parametrize(
    ("outcomes", "rc", "expect"),
    [
        ({"a.py::t1": "passed"}, 0, "did not run"),
        ({"a.py::t1": "passed", "b.py::t2[x]": "xfailed"}, 0, "xfailed"),
        ({"a.py::t1": "passed", "b.py::t2[x]": "skipped in setup"}, 0, "skipped"),
        ({"a.py::t1": "passed", "b.py::t2[x]": "failed in teardown"}, 0, "teardown"),
        (
            {"a.py::t1": "passed", "b.py::t2[x]": "passed", "c.py::t3": "passed"},
            0,
            "not a registered",
        ),
        ({"a.py::t1": "passed", "b.py::t2[x]": "passed"}, 1, "pytest exited 1"),
    ],
    ids=["missing", "xfail", "skip", "teardown", "extra", "rc"],
)
def test_fails(job, outcomes, rc, expect):
    found = job.problems(outcomes, GUARDS, rc)
    assert any(expect in p for p in found), found


def test_registered_list_matches_the_marked_tests(job):
    """Every registered guard names a test file that exists; the list is not
    empty (an empty list would make an empty run pass)."""
    guards = job.expected()
    assert len(guards) >= 5
    for g in guards:
        assert (REPO / g.split("::")[0]).is_file(), g


def test_cli(job, tmp_path):
    out = tmp_path / "o.json"
    out.write_text(json.dumps(dict.fromkeys(job.expected(), "passed")))
    assert job.main([str(out), "--pytest-rc", "0"]) == 0
    assert job.main([str(out), "--pytest-rc", "2"]) == 1


def test_junit_no_skips(job, tmp_path):
    ok = tmp_path / "ok.xml"
    ok.write_text('<testsuite><testcase classname="a" name="t"/></testsuite>')
    assert job.main(["--junit-no-skips", str(ok)]) == 0
    skipped = tmp_path / "s.xml"
    skipped.write_text(
        '<testsuite><testcase classname="a" name="t"><skipped/></testcase></testsuite>'
    )
    assert job.main(["--junit-no-skips", str(skipped)]) == 1
    empty = tmp_path / "e.xml"
    empty.write_text("<testsuite/>")
    assert job.main(["--junit-no-skips", str(empty)]) == 1
