"""The CONTRIBUTING.md quick path and the CI job that runs it.

The block must stay runnable as written: one clone line (the only line CI
rewrites), make targets that exist, and a job that runs it where it can
have changed. The live proof is the "Contributor path" CI job.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
import yaml

from tests.conftest import exec_repo_script

ROOT = Path(__file__).resolve().parents[1]
cp = exec_repo_script(ROOT / "scripts/contributor_path.py", "contributor_path")

BLOCK = """intro

<!-- contributor-path:begin -->
```bash
git clone https://example.com/org/lakebench-k8s.git
cd lakebench-k8s
# a comment line is not a command
make dev
make check-fast
```
<!-- contributor-path:end -->
"""
MAKEFILE = "dev:\n\tpip install -e .\n\ncheck-fast:\n\truff check\n"


def test_the_tracked_quick_path_is_runnable():
    text = (ROOT / "CONTRIBUTING.md").read_text()
    lines, found = cp.extract(text)
    assert found == []
    assert cp.problems(lines, (ROOT / "Makefile").read_text()) == []
    script = cp.runnable(lines)
    for cmd in ("make dev", "make check-fast", "make test-spark"):
        assert f"\n{cmd}\n" in script, cmd
    # The clone is the one rewritten line, into the directory the next line enters.
    assert script.count("git clone") == 1
    assert 'git clone -q "$REPO_URL" lakebench-k8s' in script
    assert 'git -C lakebench-k8s checkout -q "$SHA"' in script
    assert "\ncd lakebench-k8s\n" in script


def test_main_prints_the_script(capsys):
    assert cp.main([]) == 0
    assert "make check-fast" in capsys.readouterr().out


def test_a_removed_make_target_is_reported(tmp_path):
    lines, _ = cp.extract(BLOCK)
    assert cp.problems(lines, MAKEFILE) == []
    assert cp.problems(lines, "dev:\n\tpip install -e .\n") == [
        "`make check-fast` has no Makefile target"
    ]
    # Through main, as CI calls it: exit 1 and no script on stdout.
    (tmp_path / "C.md").write_text(BLOCK)
    (tmp_path / "Makefile").write_text("dev:\n")
    args = ["--contributing", str(tmp_path / "C.md"), "--makefile", str(tmp_path / "Makefile")]
    assert cp.main(args) == 1


def test_comment_lines_are_dropped_and_the_rest_kept_in_order():
    lines, _ = cp.extract(BLOCK)
    assert lines == [
        "git clone https://example.com/org/lakebench-k8s.git",
        "cd lakebench-k8s",
        "make dev",
        "make check-fast",
    ]


@pytest.mark.parametrize(
    ("text", "problem"),
    [
        ("no markers here", "exactly one"),
        (BLOCK + BLOCK, "exactly one"),
        (BLOCK.replace("```bash\n", "").replace("```\n", ""), "fenced code block"),
    ],
)
def test_malformed_block_is_reported(text, problem):
    lines, found = cp.extract(text)
    assert lines == [] and len(found) == 1 and problem in found[0]


def test_clone_line_must_be_exactly_one():
    lines, _ = cp.extract(BLOCK)
    assert cp.problems([*lines, "git clone https://example.com/x.git"], MAKEFILE)
    assert cp.problems(lines[1:], MAKEFILE)
    assert cp.problems(["git clone --depth 1 https://example.com/x.git"], MAKEFILE)


def test_clone_into_a_named_directory_keeps_it():
    script = cp.runnable(["git clone https://example.com/x.git work", "cd work"])
    assert 'git clone -q "$REPO_URL" work' in script


@pytest.mark.parametrize(
    "ref",
    [
        "refs/heads/main",
        "refs/heads/integrate/v1.5.0",
        "refs/heads/train/1002-a",
        "refs/tags/v1.7.0",
    ],
)
def test_job_always_runs_on_main_integrate_trains_and_tags(ref):
    assert cp.should_run("push", ref, []) is True


def test_job_runs_elsewhere_only_when_an_input_changed():
    lane = "refs/heads/lane/x"
    assert cp.should_run("push", lane, ["src/lakebench/cli/_run.py"]) is False
    for path in ("CONTRIBUTING.md", "Makefile", "pyproject.toml", ".github/workflows/ci.yml"):
        assert cp.should_run("push", lane, [path]) is True, path
        assert cp.should_run("pull_request", "refs/pull/3/merge", [path]) is True, path
    assert cp.should_run("pull_request", "refs/pull/3/merge", ["docs/x.md"]) is False
    # A name that only starts like an input is not one.
    assert cp.should_run("push", lane, ["Makefile.local", "pyproject.toml.bak"]) is False
    # Unknown change set: run.
    assert cp.should_run("push", lane, None) is True


def test_changed_files_against_the_integrate_merge_base(tmp_path):
    git = ["git", "-c", "user.name=t", "-c", "user.email=t@example.com"]

    def run(*args):
        subprocess.run([*git, *args], cwd=tmp_path, check=True, capture_output=True)

    run("init", "-q")
    (tmp_path / "a.txt").write_text("a\n")
    run("add", ".")
    run("commit", "-qm", "base")
    run("update-ref", "refs/remotes/origin/integrate/v1.5.0", "HEAD")
    (tmp_path / "Makefile").write_text("x:\n")
    run("add", ".")
    run("commit", "-qm", "change")
    assert cp.changed_files("push", "", tmp_path) == ["Makefile"]
    # No integrate ref to compare with: unknown, so the job runs.
    run("update-ref", "-d", "refs/remotes/origin/integrate/v1.5.0")
    assert cp.changed_files("push", "", tmp_path) is None
    assert cp.changed_files("pull_request", "", tmp_path) is None


def _job() -> dict:
    return yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"][
        "contributor-path"
    ]


def test_ci_job_runs_the_script_in_a_pinned_clean_container():
    job = _job()
    image = job["container"]["image"]
    assert (
        image.startswith("python:3.11-bookworm@sha256:") and len(image.split("@sha256:")[1]) == 64
    )
    steps = {s.get("name"): s for s in job["steps"]}
    assert "--should-run" in steps["Changed inputs"]["run"]
    install = steps["Install Java 17 and make"]
    assert "openjdk-17-jre-headless" in install["run"] and "make" in install["run"]
    quick = steps["Run the quick path"]
    assert "python3 scripts/contributor_path.py >" in quick["run"]
    assert 'bash -euo pipefail "$RUNNER_TEMP/quick-path.sh"' in quick["run"]
    assert quick["env"]["SHA"] == "${{ github.sha }}"
    for name in ("Install Java 17 and make", "Run the quick path"):
        assert steps[name]["if"] == "steps.inputs.outputs.run == 'true'"
    assert int(job["timeout-minutes"]) >= 100  # make test-spark: up to about 52 min a pass
