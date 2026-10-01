"""Shipped code and docs gain no new bug-id or internal-goal citations (OSS-3).

``LB-NNN`` bug ids, ``AML-GOALS``, ``GOALS P<n>``, ``MISSION-v1.<n>``, the
v1.7 requirement and work-item ids (``QR-13``, ``PRC-5``, ``SD-8a``) and
``gotcha <n>`` point at local files a reader of the published package cannot
open. This is the
ratchet half of OSS-3: per-file counts are pinned in
``tests/fixtures/citation_counts.json``, taken once at the ratchet commit. A
file whose count rises, or a file that is not in the list and has a
citation, fails. Counts may fall freely, so removing a citation needs no
fixture edit; the sweep (QR-14b) replaces the rest with a one-clause reason
and empties the file.

Scope: the tracked files under ``src/``, ``docs/``, ``scripts/``,
``datagen_rs/`` and ``examples/``, plus ``README.md``, ``CONTRIBUTING.md``
and the part of ``CHANGELOG.md`` above the first release older than 1.7.0
(older sections are history). ``tests/`` does not ship and is not scanned.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COUNTS = ROOT / "tests" / "fixtures" / "citation_counts.json"

#: The v1.7 requirement families (SPEC) and work-item prefixes (DESIGN index).
_PLAN_IDS = "QR|PC|SD|CC|CD|ER|AM|DEP|QA|OSS|PRC|SAF|CFG|DAT|REL|V16"
CITATION = re.compile(
    r"\bLB-[0-9]{3}\b|AML-GOALS|\bGOALS P[0-9]|MISSION-v1\.[0-9]"
    rf"|\b(?:{_PLAN_IDS})-[0-9]+[a-z]?\b"
    r"|\b[Gg]otcha [0-9]+"
)
SCOPE = ("src", "docs", "scripts", "datagen_rs", "examples", "README.md", "CONTRIBUTING.md")
CHANGELOG = "CHANGELOG.md"
#: The CHANGELOG is scanned down to the first section of a release before this.
FIRST_SCANNED_RELEASE = (1, 7, 0)

_RELEASE_HEADING = re.compile(r"^## \[(\d+)\.(\d+)\.(\d+)\]")


def changelog_current_part(text: str) -> str:
    """CHANGELOG.md text above the first heading of a release before 1.7.0."""
    out = []
    for line in text.splitlines():
        m = _RELEASE_HEADING.match(line)
        if m and tuple(int(g) for g in m.groups()) < FIRST_SCANNED_RELEASE:
            break
        out.append(line)
    return "\n".join(out)


def _git_env() -> dict[str, str]:
    # Under a git hook GIT_DIR and GIT_INDEX_FILE point at the hook's
    # repository; inherited, they would make `git -C <tmp>` list or stage the
    # real index instead of the throwaway repo's.
    return {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


def citation_counts(root: Path) -> dict[str, int]:
    """{repo-relative path: number of citations} for every tracked file in scope."""
    listed = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z", "--", *SCOPE, CHANGELOG],
        capture_output=True,
        check=True,
        env=_git_env(),
    ).stdout.decode()
    counts: dict[str, int] = {}
    for rel in filter(None, listed.split("\0")):
        path = root / rel
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue  # binary
        if rel == CHANGELOG:
            text = changelog_current_part(text)
        n = len(CITATION.findall(text))
        if n:
            counts[rel] = n
    return counts


def ratchet_problems(current: dict[str, int], pinned: dict[str, int]) -> list[str]:
    problems = []
    for rel, n in sorted(current.items()):
        was = pinned.get(rel)
        if was is None:
            problems.append(f"{rel}: {n} new citation(s) in a file with none pinned")
        elif n > was:
            problems.append(f"{rel}: {n} citations, pinned at {was}")
    return problems


def _pinned() -> dict[str, int]:
    return json.loads(COUNTS.read_text())["counts"]


def test_no_new_citations():
    problems = ratchet_problems(citation_counts(ROOT), _pinned())
    assert not problems, (
        "bug-id or internal-goal citations added to shipped files; replace each "
        "with a one-clause reason (a renamed or split file moves its entry in "
        "tests/fixtures/citation_counts.json, with the same total):\n  " + "\n  ".join(problems)
    )


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@t", *args],
        check=True,
        capture_output=True,
        env=_git_env(),
    )


def test_citation_ratchet_fails_on_new_citation(tmp_path):
    _git(tmp_path, "init", "-q")
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "a.py").write_text("x = 1  # see LB-123\n")
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "t.py").write_text("# LB-999 is fine in tests\n")
    _git(tmp_path, "add", ".")
    pinned = citation_counts(tmp_path)
    assert pinned == {"src/a.py": 1}
    assert ratchet_problems(citation_counts(tmp_path), pinned) == []

    # A second citation in a pinned file, and one in a new file.
    (tmp_path / "src" / "a.py").write_text("x = 1  # see LB-123\ny = 2  # AML-GOALS 5a\n")
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "new.md").write_text("Per MISSION-v1.7 section 2.\n")
    _git(tmp_path, "add", ".")
    problems = ratchet_problems(citation_counts(tmp_path), pinned)
    assert len(problems) == 2
    assert problems[0].startswith("docs/new.md: 1 new") and "src/a.py: 2" in problems[1]

    # Removing citations needs no fixture edit.
    (tmp_path / "src" / "a.py").write_text("x = 1\n")
    (tmp_path / "docs" / "new.md").unlink()
    _git(tmp_path, "add", "-A")
    assert ratchet_problems(citation_counts(tmp_path), pinned) == []


def test_untracked_files_are_not_scanned(tmp_path):
    _git(tmp_path, "init", "-q")
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "scratch.py").write_text("# LB-001\n")
    assert citation_counts(tmp_path) == {}


def test_changelog_history_is_not_scanned():
    text = (
        "# Changelog\n\n## [Unreleased]\n- fixes LB-300\n\n## [1.7.0] - 2026-11-11\n"
        "- per GOALS P9\n\n## [1.6.0] - 2026-09-30\n- LB-210 known\n## [1.5.0]\n- LB-100\n"
    )
    part = changelog_current_part(text)
    assert "LB-300" in part and "GOALS P9" in part
    assert "LB-210" not in part and "LB-100" not in part


def test_pattern_matches_the_citation_forms():
    hits = CITATION.findall(
        "LB-044 and LB-1 and XLB-123 and AML-GOALS 9 and GOALS P8.3 and MISSION-v1.6.md"
    )
    assert hits == ["LB-044", "AML-GOALS", "GOALS P8", "MISSION-v1.6"]
    hits = CITATION.findall(
        "QR-13, PRC-5 and SD-8a; (gotcha 34) and Gotcha 6; not CC-BY-4.0, XQR-1, AM-x"
    )
    assert hits == ["QR-13", "PRC-5", "SD-8a", "gotcha 34", "Gotcha 6"]


def test_planted_repo_ignores_an_inherited_git_dir(tmp_path, monkeypatch):
    """A hook's GIT_DIR must not redirect the planted repo to the real one."""
    _git(tmp_path, "init", "-q")
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "a.py").write_text("# LB-123\n")
    _git(tmp_path, "add", ".")
    monkeypatch.setenv("GIT_DIR", str(ROOT / ".git"))
    monkeypatch.setenv("GIT_INDEX_FILE", str(tmp_path / "no-such-index"))
    assert citation_counts(tmp_path) == {"src/a.py": 1}


def test_counts_fixture_is_well_formed():
    data = json.loads(COUNTS.read_text())
    counts = data["counts"]
    assert all(isinstance(n, int) and n > 0 for n in counts.values())
    assert list(counts) == sorted(counts)
