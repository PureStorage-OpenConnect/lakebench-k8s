"""GOALS P8.4: the community files an outside contributor looks for exist."""

from __future__ import annotations

import ast
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_files_present():
    for name in (
        "SECURITY.md",
        "CODE_OF_CONDUCT.md",
        "CONTRIBUTING.md",
        "LICENSE",
        ".github/pull_request_template.md",
        ".github/ISSUE_TEMPLATE/bug_report.yml",
        ".github/ISSUE_TEMPLATE/feature_request.yml",
        ".github/ISSUE_TEMPLATE/config.yml",
    ):
        assert (ROOT / name).is_file(), name


def test_code_of_conduct_references_covenant_2_1():
    text = (ROOT / "CODE_OF_CONDUCT.md").read_text()
    assert "contributor-covenant.org/version/2/1/code_of_conduct" in text


def test_issue_forms_parse():
    for path in (ROOT / ".github" / "ISSUE_TEMPLATE").glob("*.yml"):
        assert isinstance(yaml.safe_load(path.read_text()), dict), path.name


def test_contributing_points_only_at_tracked_paths():
    # dev-artifacts/ and CLAUDE.md are gitignored; an outside contributor
    # cannot follow a pointer to them.
    text = (ROOT / "CONTRIBUTING.md").read_text()
    assert "dev-artifacts/" not in text
    assert "CLAUDE.md" not in text
    assert "pytest tests/ -x --timeout" not in text  # pytest-timeout is not a dev dependency


# Maintainer material that exists only in the maintainers' checkout (excluded
# locally, not in this repository). A tracked file that points at it sends an
# outside reader to a path they do not have.
_LOCAL_ONLY_EVERYWHERE = ("dev-artifacts/", "CLAUDE.md")
# Cited from code comments as decision ids; public docs must not rely on it.
_LOCAL_ONLY_IN_DOCS = ("AML-GOALS",)
_PUBLIC_DOCS = ("docs", "README.md", "CHANGELOG.md", "CONTRIBUTING.md")


def _git_grep(patterns: tuple[str, ...], *pathspec: str) -> list[str]:
    if not (ROOT / ".git").exists():
        pytest.skip("not a git checkout")
    args = ["git", "grep", "-nIF"]
    for pattern in patterns:
        args += ["-e", pattern]
    args += ["--", *pathspec, ":!tests/test_community_files.py", ":!docs/internal/"]
    result = subprocess.run(args, cwd=ROOT, capture_output=True, text=True)
    assert result.returncode in (0, 1), result.stderr  # 1 means no match
    return result.stdout.splitlines()


def test_tracked_files_do_not_point_at_local_only_paths():
    hits = _git_grep(_LOCAL_ONLY_EVERYWHERE, ".")
    hits += _git_grep(_LOCAL_ONLY_IN_DOCS, *_PUBLIC_DOCS)
    assert not hits, "tracked files cite local-only maintainer material:\n" + "\n".join(hits)


def test_package_is_marked_typed():
    # PEP 561: type checkers read lakebench's annotations only with this marker.
    marker = ROOT / "src" / "lakebench" / "py.typed"
    assert marker.is_file()
    assert marker.read_bytes() == b""


def test_wheel_carries_py_typed(tmp_path):
    pytest.importorskip("hatchling")  # CI checks the built wheel in the build job
    result = subprocess.run(
        [sys.executable, "-m", "hatchling", "build", "-t", "wheel", "-d", str(tmp_path)],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    (wheel,) = tmp_path.glob("*.whl")
    assert "lakebench/py.typed" in zipfile.ZipFile(wheel).namelist()


def test_ci_build_job_checks_py_typed_in_the_wheel():
    ci = yaml.safe_load((ROOT / ".github" / "workflows" / "ci.yml").read_text())
    steps = " ".join(str(s.get("run", "")) for s in ci["jobs"]["build"]["steps"])
    assert "lakebench/py.typed" in steps


def _reference_packages() -> set[str]:
    """Names in REFERENCE_PY_DEPS, read from source like scripts/aml_gate.py."""
    job = ROOT / "src/lakebench/modules/pipeline_engines/spark/job.py"
    for node in ast.parse(job.read_text()).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "REFERENCE_PY_DEPS" for t in node.targets
        ):
            return {spec.split("==")[0] for spec in ast.literal_eval(node.value)}
    raise AssertionError("REFERENCE_PY_DEPS not found")


def test_dependabot_config():
    cfg = yaml.safe_load((ROOT / ".github" / "dependabot.yml").read_text())
    assert cfg["version"] == 2
    updates = {u["package-ecosystem"]: u for u in cfg["updates"]}
    # No cargo: datagen_rs/Cargo.lock is a datagen image input.
    assert set(updates) == {"github-actions", "pip"}
    for update in updates.values():
        assert update["target-branch"] == "integrate/v1.5.0"
        assert update["schedule"]["interval"] == "monthly"
    ignored = {i["dependency-name"] for i in updates["pip"]["ignore"]}
    reference = {"numpy", "scipy", "pandas", "scikit-learn", "joblib", "threadpoolctl"}
    assert ignored == reference
    assert reference <= _reference_packages()
