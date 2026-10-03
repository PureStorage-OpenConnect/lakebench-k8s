"""RELEASING.md and the Makefile's release steps stay one process.

- The step table between the release-steps markers lists the ids of
  ``RELEASE_STEPS``, in order, and each has an ``rc-<id>`` target; every
  script a step names exists.
- From the run-up to a freeze on (an expected-results file for a version
  with no CHANGELOG release section yet, or a final ``__version__`` with
  its ``uat/freeze-<version>``), no step is still pending. The Makefile is
  not on the post-freeze allowlist, so a pending step found later would
  force a new freeze.
- No tracked file points at the maintainer workspace directory, apart from
  the two guards that forbid it (this file spells the name in two parts).
- RELEASING.md says what release.yml's gate ``--only`` runs.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BEGIN, END = "<!-- release-steps:begin -->", "<!-- release-steps:end -->"

#: The maintainer workspace directory, spelled in two parts so this file is
#: not a hit of its own check.
WORKSPACE = "dev" + "-artifacts"
#: The files that hold the name as the thing they forbid.
DEV_ARTIFACTS_EXEMPT = (
    "tests/test_community_files.py",
    "tests/test_no_hazardous_polaris_guidance.py",
)


def doc_steps(text: str) -> list[str]:
    """Step ids from the first column of the table between the markers."""
    i, j = text.index(BEGIN), text.index(END)
    ids = []
    for line in text[i:j].splitlines():
        m = re.match(r"\|\s*`([a-z0-9-]+)`\s*\|", line)
        if m:
            ids.append(m.group(1))
    return ids


def makefile_steps(text: str) -> list[str]:
    """The ids in ``RELEASE_STEPS :=``, continuation lines included."""
    m = re.search(r"^RELEASE_STEPS\s*:?=((?:[^\n]*\\\n)*[^\n]*)", text, re.M)
    if not m:
        return []
    return m.group(1).replace("\\\n", " ").split()


def target_bodies(text: str) -> dict[str, str]:
    """{target: recipe text} for every ``rc-<id>:`` target, conditionals included."""
    out: dict[str, str] = {}
    current = None
    for line in text.splitlines():
        m = re.match(r"^(rc-[a-z0-9-]+):", line)
        if m:
            current = m.group(1)
            out[current] = ""
        elif current and (
            line.startswith("\t") or re.match(r"^(ifdef|ifndef|ifeq|ifneq|else|endif)\b", line)
        ):
            out[current] += line + "\n"
        else:
            current = None
    return out


def table_problems(doc: str, makefile: str, root: Path | None = None) -> list[str]:
    problems = []
    if re.search(r"^RELEASE_STEPS\s*\+=", makefile, re.M):
        problems.append("RELEASE_STEPS is extended with +=; list every step in one place")
    listed, steps = doc_steps(doc), makefile_steps(makefile)
    if listed != steps:
        problems.append(f"RELEASING.md steps {listed} != Makefile RELEASE_STEPS {steps}")
    bodies = target_bodies(makefile)
    problems += [f"no rc-{s} target" for s in steps if f"rc-{s}" not in bodies]
    if root is not None:
        for target, body in sorted(bodies.items()):
            for script in sorted(set(re.findall(r"\b(scripts/[\w/]+\.py|tests/[\w/]+\.py)", body))):
                if not (root / script).is_file():
                    problems.append(f"{target}: {script} does not exist")
    return problems


def pending_steps(makefile: str) -> list[str]:
    return sorted(t for t, body in target_bodies(makefile).items() if "pending:" in body)


def release_declared(root: Path) -> bool:
    """The run-up to a freeze, or the release commit.

    True when ``uat/expected-results-<v>.json`` exists for a version ``v``
    that has no ``## [v]`` section in CHANGELOG.md yet (that file is
    committed before the freeze), or when ``__version__`` is final and
    ``uat/freeze-<version>`` exists."""
    changelog = root / "CHANGELOG.md"
    released = (
        set(re.findall(r"^## \[([^\]]+)\]", changelog.read_text(), re.M))
        if (changelog.exists())
        else set()
    )
    for f in (root / "uat").glob("expected-results-*.json"):
        if f.stem.removeprefix("expected-results-") not in released:
            return True
    init = (root / "src" / "lakebench" / "__init__.py").read_text()
    m = re.search(r'^__version__\s*=\s*"([^"]+)"', init, re.M)
    if not m or ".dev" in m.group(1):
        return False
    return (root / "uat" / f"freeze-{m.group(1)}").exists()


def _git_env() -> dict[str, str]:
    return {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


def dev_artifacts_refs(root: Path) -> list[str]:
    r = subprocess.run(
        ["git", "-C", str(root), "grep", "-nIF", WORKSPACE, "--", "."]
        + [f":!{p}" for p in DEV_ARTIFACTS_EXEMPT],
        capture_output=True,
        text=True,
        env=_git_env(),
        check=False,
    )
    assert r.returncode in (0, 1), r.stderr
    return r.stdout.splitlines()


# --- the tree ---------------------------------------------------------------


def test_steps_table_matches_makefile():
    doc = (ROOT / "RELEASING.md").read_text()
    assert table_problems(doc, (ROOT / "Makefile").read_text(), ROOT) == []


def test_no_pending_steps_at_release():
    if not release_declared(ROOT):
        pytest.skip("no freeze declared for the current version")
    assert pending_steps((ROOT / "Makefile").read_text()) == []


def test_no_dev_artifacts_reference():
    assert dev_artifacts_refs(ROOT) == []


def test_release_workflow_only_list_in_releasing():
    """RELEASING.md must say what release.yml's gate --only runs."""
    wf = (ROOT / ".github" / "workflows" / "release.yml").read_text()
    m = re.search(r"release_gate\.py[^\n]*\n?[^\n]*--only ([\w,-]+)", wf)
    assert m, "release.yml no longer passes --only to release_gate.py"
    only = set(m.group(1).split(","))
    doc = (ROOT / "RELEASING.md").read_text()
    in_wf = "perf-baselines" in only
    says_not_in = "not in the release workflow's `--only` list" in doc
    assert in_wf != says_not_in, (sorted(only), says_not_in)
    listed = re.search(r"\(`release\.yml` runs ([^)]*)\)", doc)
    assert listed, "RELEASING.md no longer lists what release.yml runs"
    doc_names = set(re.split(r",\s*|\s+and\s+", " ".join(listed.group(1).split())))
    assert doc_names == only, (sorted(doc_names), sorted(only))


# --- planted cases ----------------------------------------------------------

_DOC = f"""x
{BEGIN}
| Step | Command |
|---|---|
| `version` | a |
| `gate` | b |
{END}
"""


def test_a_missing_target_is_found():
    mk = "RELEASE_STEPS := version \\\n    gate\nrc-version:\n\techo v\n"
    assert table_problems(_DOC, mk) == ["no rc-gate target"]


def test_a_step_out_of_order_is_found():
    mk = "RELEASE_STEPS := gate version\nrc-version:\n\techo v\nrc-gate:\n\techo g\n"
    assert any("!=" in p for p in table_problems(_DOC, mk))


def test_a_pending_step_is_found_inside_a_conditional():
    mk = (
        'rc-matrix:\n\t@echo "pending: the release harness"; exit 1\n'
        "rc-gate:\nifdef DRY\n\techo list\nelse\n\techo gate\nendif\n"
        'rc-support-record:\nifdef DRY\n\t@echo "pending: check mode"; exit 1\nelse\n\techo ok\nendif\n'
    )
    assert pending_steps(mk) == ["rc-matrix", "rc-support-record"]


def test_a_missing_script_or_an_extended_list_is_found(tmp_path):
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "here.py").write_text("")
    mk = (
        "RELEASE_STEPS := version gate\nRELEASE_STEPS += extra\n"
        "rc-version:\n\tpython scripts/here.py\n"
        "rc-gate:\n\tpython scripts/gone.py --check\n"
    )
    problems = table_problems(_DOC, mk, tmp_path)
    assert "rc-gate: scripts/gone.py does not exist" in problems
    assert any("+=" in p for p in problems)
    assert not any("here.py" in p for p in problems)


def test_unreleased_expected_results_turn_the_check_on(tmp_path):
    (tmp_path / "src" / "lakebench").mkdir(parents=True)
    (tmp_path / "src" / "lakebench" / "__init__.py").write_text('__version__ = "1.6.0"\n')
    (tmp_path / "uat").mkdir()
    (tmp_path / "CHANGELOG.md").write_text("## [Unreleased]\n## [1.6.0] - 2026-09-30\n")
    (tmp_path / "uat" / "expected-results-1.6.0.json").write_text("{}")
    assert not release_declared(tmp_path)
    (tmp_path / "uat" / "expected-results-1.7.0.json").write_text("{}")
    assert release_declared(tmp_path)


def test_release_declared_needs_a_final_version_and_its_freeze_file(tmp_path):
    (tmp_path / "src" / "lakebench").mkdir(parents=True)
    init = tmp_path / "src" / "lakebench" / "__init__.py"
    (tmp_path / "uat").mkdir()
    init.write_text('__version__ = "1.7.0.dev0"\n')
    (tmp_path / "uat" / "freeze-1.7.0.dev0").write_text("x\n")
    assert not release_declared(tmp_path)
    init.write_text('__version__ = "1.7.0"\n')
    assert not release_declared(tmp_path)
    (tmp_path / "uat" / "freeze-1.7.0").write_text("a" * 40 + "\n")
    assert release_declared(tmp_path)


def test_a_dev_artifacts_citation_is_found(tmp_path):
    def git(*args: str) -> None:
        subprocess.run(["git", "-C", str(tmp_path), *args], check=True, env=_git_env())

    git("init", "-q")
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "x.md").write_text(f"See {WORKSPACE}/AML.md.\n")
    (tmp_path / "tests").mkdir()
    for exempt in DEV_ARTIFACTS_EXEMPT:
        (tmp_path / exempt).write_text(f"FORBIDDEN = '{WORKSPACE}/'\n")
    git("add", "-A")
    assert dev_artifacts_refs(tmp_path) == [f"docs/x.md:1:See {WORKSPACE}/AML.md."]


@pytest.mark.parametrize("flag", ["-i", "-k", "-ik"])
def test_release_check_refuses_ignore_and_keep_going(flag):
    """make -i would ignore every step's failure; -k would run past one."""
    if shutil.which("make") is None:
        pytest.skip("GNU make is not installed")
    r = subprocess.run(
        ["make", "-C", str(ROOT), "--no-print-directory", flag, "release-check", "VERSION=0.0.0"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert r.returncode != 0
    assert "release-check: run it without" in r.stderr
