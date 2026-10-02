"""scripts/check_doc_overlap.py on planted files: shared text and shared
facts are reported with both line numbers, a pointer line is not."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from tests.conftest import exec_repo_script

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/check_doc_overlap.py"
cdo = exec_repo_script(SCRIPT, "check_doc_overlap")

DOC = """# Sizing

Intro line.

The silver build keeps its scratch volume on the repl-one class and the
executor count scales with data, capped well below the polling storm.

Silver scratch is 300Gi per executor.
"""


def _repo(tmp_path: Path, doc: str = DOC) -> Path:
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "sizing.md").write_text(doc)
    return tmp_path


def _run(tmp_path: Path, text: str) -> tuple[int, str]:
    f = tmp_path / "agent.txt"
    f.write_text(text)
    return _main(f, tmp_path)


def _main(f: Path, repo: Path) -> tuple[int, str]:
    out = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            str(f),
            "--repo",
            str(repo),
            "--corpus",
            "**/*.md",
        ],
        capture_output=True,
        text=True,
    )
    return out.returncode, out.stdout


def test_planted_duplicate_reported(tmp_path):
    repo = _repo(tmp_path)
    rc, out = _run(
        repo,
        "Rules.\n\nRemember: the **silver build** keeps its scratch volume on the\n"
        "repl-one class and the executor count scales with data.\n",
    )
    assert rc == 1
    # FILE line 3 (where the shared run starts) -> corpus line 5
    assert f"{repo / 'agent.txt'}:3 -> docs/sizing.md:5 (text:" in out


def test_planted_fact_reported(tmp_path):
    repo = _repo(tmp_path)
    rc, out = _run(repo, "First.\nKeep silver scratch at 300 Gi or more.\n")
    assert rc == 1
    assert out.strip().endswith(":2 -> docs/sizing.md:8 (fact: 300gi)")


def test_backticked_identifier_with_digit_reported(tmp_path):
    repo = _repo(tmp_path, DOC + "\nThe image is lb-datagen:1.6.0 today.\n")
    rc, out = _run(repo, "Pin `lb-datagen:1.6.0` in the config.\n")
    assert rc == 1
    assert "(fact: lb-datagen:1.6.0)" in out
    # A longer identifier that only contains the token is not the same fact.
    rc, out = _run(repo, "Pin `lb-datagen:1.6.01` in the config.\n")
    assert rc == 0, out


def test_pointer_line_not_reported(tmp_path):
    repo = _repo(tmp_path)
    for line in (
        "Silver scratch sizes (300Gi) are in `docs/sizing.md`.\n",
        "Silver scratch sizes (300Gi): [sizing](docs/sizing.md#sizing).\n",
        "Silver scratch sizes (300Gi): `/some/checkout/docs/sizing.md:8`.\n",
    ):
        rc, out = _run(repo, line)
        assert rc == 0, (line, out)


def test_backticked_path_outside_the_corpus_is_not_a_pointer(tmp_path):
    repo = _repo(tmp_path)
    rc, out = _run(repo, "Silver scratch is 300Gi, see `src/job.py`.\n")
    assert rc == 1, out


def test_clean_file_exits_zero(tmp_path):
    repo = _repo(tmp_path)
    rc, out = _run(repo, "Talk to the owner as to a director.\nRun 2 lanes.\n")
    assert (rc, out) == (0, "")


def test_file_inside_the_repo_is_not_its_own_corpus(tmp_path):
    repo = _repo(tmp_path)
    rc, out = _main(repo / "docs" / "sizing.md", repo)
    assert rc == 0, out


def test_default_corpus_is_the_tracked_markdown(tmp_path):
    repo = _repo(tmp_path)
    (repo / "untracked.md").write_text("Silver scratch is 300Gi.\n")
    git = ["git", "-c", "user.name=t", "-c", "user.email=t@example.com"]
    subprocess.run([*git, "init", "-q"], cwd=repo, check=True)
    subprocess.run([*git, "add", "docs/sizing.md"], cwd=repo, check=True)
    assert cdo.corpus_files(repo, None) == ["docs/sizing.md"]


@pytest.mark.parametrize(
    ("line", "facts"),
    [
        ("4,349 GB of 434 cores", ["4349gb", "434cores"]),
        ("timeout 1200 s, then 25 min", ["1200s", "25min"]),
        ("v1.7 and s3a and 2x", []),
        ("1.5 h", ["1.5h"]),
    ],
)
def test_unit_facts(line, facts):
    assert cdo._unit_facts(line) == facts


def test_missing_file_is_a_usage_error(tmp_path):
    assert cdo.main([str(tmp_path / "nope.md"), "--repo", str(tmp_path)]) == 2
