"""Every tracked doc has a reader (PRC-5): scripts/check_doc_readers.py.

The checks run on this tree, and each rule is shown failing on a planted
case in a throwaway repository.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "check_doc_readers", ROOT / "scripts" / "check_doc_readers.py"
)
cdr = importlib.util.module_from_spec(_spec)
sys.modules["check_doc_readers"] = cdr  # dataclasses look the module up by name
_spec.loader.exec_module(cdr)


def _git(repo: Path, *args: str) -> None:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, env=env)


def _repo(tmp_path: Path, files: dict[str, str]) -> Path:
    for rel, text in files.items():
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "add", "-A")
    return tmp_path


_BASE = {
    "README.md": "# Project\n\nSee [the docs](docs/README.md).\n",
    "docs/README.md": "# Docs\n\n- [Guide](guide.md)\n",
    "docs/guide.md": "# Guide\n",
}


@pytest.fixture(scope="module")
def root_unread() -> list[str]:
    """unread_files(ROOT), computed once for the module (about 4 s)."""
    return cdr.unread_files(ROOT)


def test_every_tracked_doc_has_a_reader(root_unread):
    unread = [p for p in root_unread if p not in cdr.KNOWN_ORPHANS]
    assert not unread, (
        "tracked docs with no reader; link each from docs/README.md, read it from "
        f"code, or delete it: {unread}"
    )


def test_a_planted_orphan_fails(tmp_path):
    repo = _repo(tmp_path, {**_BASE, "docs/x.md": "# Nobody links me\n"})
    assert cdr.unread_files(repo) == ["docs/x.md"]


def test_a_docstring_or_comment_citation_is_not_a_reader(tmp_path):
    code = '"""See docs/x.md."""\n# also docs/x.md\nX = 1\n'
    repo = _repo(tmp_path, {**_BASE, "docs/x.md": "# x\n", "src/m.py": code})
    assert cdr.unread_files(repo) == ["docs/x.md"]
    (repo / "src" / "m.py").write_text(code + 'DOC = "docs/x.md"\n')
    assert cdr.unread_files(repo) == []


def test_readers_by_basename_config_and_link(tmp_path):
    files = {
        **_BASE,
        "docs/a.md": "# a\n",
        "docs/b.md": "# b\n",
        "docs/c.md": "# c\n",
        "docs/d.md": "# d\n",
        "src/tool.py": 'P = ROOT / "docs" / "a.md"\n',
        "Makefile": "docs:\n\tcat docs/b.md\n# docs/c.md is only in a comment\n",
        "docs/guide.md": "# Guide\n\n[d](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/d.md)\n",
    }
    repo = _repo(tmp_path, files)
    assert cdr.unread_files(repo) == ["docs/c.md"]


def test_a_packaging_exclude_is_not_a_reader(tmp_path):
    pyproject = (
        '[tool.hatch.build.targets.sdist]\nexclude = [\n    "tool.py",\n    "docs/x.md",\n]\n'
        '[tool.other]\nreads = "docs/y.md"\n'
    )
    files = {**_BASE, "tool.py": "print(1)\n", "docs/x.md": "# x\n", "docs/y.md": "# y\n"}
    repo = _repo(tmp_path, {**files, "pyproject.toml": pyproject})
    assert cdr.unread_files(repo) == ["docs/x.md", "tool.py"]


def test_a_path_counts_only_as_a_whole_path(tmp_path):
    files = {
        **_BASE,
        "docs/a.md": "# a\n",
        "docs/b.md": "# b\n",
        "docs/c.md": "# c\n",
        "src/m.py": 'A = "docs/a.md.bak"\nB = "mydocs/b.md"\nC = "see docs/c.md."\n',
    }
    repo = _repo(tmp_path, files)
    assert cdr.unread_files(repo) == ["docs/a.md", "docs/b.md"]


def test_basename_reader_needs_a_unique_basename(tmp_path):
    files = {
        **_BASE,
        "docs/notes.md": "# a\n",
        "docs/sub/notes.md": "# b\n",
        "tests/t.py": 'N = "notes.md"\n',
    }
    repo = _repo(tmp_path, files)
    assert cdr.unread_files(repo) == ["docs/notes.md", "docs/sub/notes.md"]


def test_pattern_reader_literal_present(tmp_path):
    files = {
        **_BASE,
        "uat/results-9.9.9.md": "# results\n",
        "scripts/release_gate.py": 'PREFIX = "uat/" + "results-"\nX = "uat/results-"\n',
    }
    repo = _repo(tmp_path, files)
    # scripts/release_gate.py itself has no reader in this throwaway repo.
    assert cdr.unread_files(repo) == ["scripts/release_gate.py"]
    (repo / "scripts" / "release_gate.py").write_text('X = "somewhere-else"\n')
    assert cdr.unread_files(repo) == ["scripts/release_gate.py", "uat/results-9.9.9.md"]
    # The real entry still names its literal.
    for _glob, reader, literal in cdr.PATTERN_READERS:
        assert literal in (ROOT / reader).read_text(), (reader, literal)


#: KNOWN_ORPHANS when the check landed. The list may lose entries, never gain one.
LANDED_ORPHANS = {
    "docs/deep-dive/datagen-metrics.md",
    "docs/design/README.md",
    "docs/design/namespace-isolation.md",
    "docs/internal/design-contradictions.md",
    "docs/internal/observability-pushgateway.md",
    "docs/reproductions/README.md",
    "docs/reproductions/c360-scale-0-1.yaml",
}


def test_known_orphans_only_shrink(root_unread):
    added = set(cdr.KNOWN_ORPHANS) - LANDED_ORPHANS
    assert not added, f"KNOWN_ORPHANS may only shrink; give these a reader instead: {added}"
    unread = set(root_unread)
    for path in cdr.KNOWN_ORPHANS:
        assert (ROOT / path).is_file(), f"{path} is gone; remove it from KNOWN_ORPHANS"
        assert path in unread, f"{path} now has a reader; remove it from KNOWN_ORPHANS"


def test_platform_files_only_need_to_exist(tmp_path):
    repo = _repo(tmp_path, {**_BASE, "SECURITY.md": "# s\n", "UPGRADING-1.7.md": "# u\n"})
    assert cdr.unread_files(repo) == []


def test_platform_files_must_exist(tmp_path):
    assert cdr.platform_problems(ROOT) == []
    repo = _repo(tmp_path, {**_BASE})
    missing = cdr.platform_problems(repo)
    assert "install.sh: platform file missing" in missing
    assert not any(m.startswith(("RELEASING.md", "UPGRADING-")) for m in missing)


def test_pending_platform_files_only_shrink():
    for rel in cdr.PENDING_PLATFORM_FILES:
        assert not (ROOT / rel).exists(), f"{rel} exists; remove it from PENDING_PLATFORM_FILES"


def test_prefixed_paths_in_scripts_are_readers(tmp_path):
    wf = (
        "jobs:\n  a:\n    steps:\n"
        "      - run: ./scripts/a.sh\n"
        "      - run: python $PWD/scripts/b.py\n"
        "      - run: python ${{ github.workspace }}/scripts/c.py\n"
        "      - run: python other/scripts/d.py\n"
    )
    files = {**_BASE, ".github/workflows/ci.yml": wf}
    files.update({f"scripts/{n}": "x\n" for n in ("a.sh", "b.py", "c.py", "d.py")})
    repo = _repo(tmp_path, files)
    assert cdr.unread_files(repo) == ["scripts/d.py"]


def test_pyproject_exclude_keys_are_not_readers(tmp_path):
    pyproject = (
        '[tool.hatch.build.targets.sdist]\nexclude = ["tool.py"]\n'
        '[tool.ruff]\nextend-exclude = ["docs/x.md"]\n'
        '[tool.coverage.run]\nomit = ["scripts/o.py"]\n'
        '[tool.other]\nreads = ["docs/y.md"]  # comment docs/x.md\n'
    )
    files = {**_BASE, "tool.py": "1\n", "docs/x.md": "# x\n", "docs/y.md": "# y\n"}
    files["scripts/o.py"] = "1\n"
    repo = _repo(tmp_path, {**files, "pyproject.toml": pyproject})
    assert cdr.unread_files(repo) == ["docs/x.md", "scripts/o.py", "tool.py"]


def test_inherited_git_dir_does_not_redirect_the_listing(tmp_path, monkeypatch):
    repo = _repo(tmp_path, {**_BASE, "docs/x.md": "# x\n"})
    monkeypatch.setenv("GIT_DIR", str(ROOT / ".git"))
    monkeypatch.setenv("GIT_INDEX_FILE", str(tmp_path / "no-such-index"))
    assert cdr.unread_files(repo) == ["docs/x.md"]


def test_stub_rule(tmp_path, monkeypatch):
    monkeypatch.setattr(cdr, "PUBLISHED_STUBS", ("docs/moved.md",))
    repo = _repo(tmp_path, {**_BASE, "docs/new.md": "# New\n"})
    assert cdr.stub_problems(repo) == ["docs/moved.md: published stub is missing"]
    (repo / "docs" / "moved.md").write_text("# Moved\n\nSee [the new page](new.md).\n")
    assert cdr.stub_problems(repo) == []
    (repo / "docs" / "moved.md").write_text("line\n" * 6)
    assert cdr.stub_problems(repo) == ["docs/moved.md: published stub is longer than 5 lines"]


def test_resolve_paths(tmp_path):
    (tmp_path / "real").mkdir()
    (tmp_path / "real" / "f.md").write_text("x\n")
    (tmp_path / "real" / "c.py").write_text("x\n")
    (tmp_path / "mem").mkdir()
    (tmp_path / "mem" / "note.md").write_text("x\n")
    local = tmp_path / "mem" / "INDEX.md"
    local.write_text(
        f"Read `{tmp_path}/real/f.md`, `real/f.md:12`, `real/c.py:empty_bucket()`, "
        "`real/c.py::test_x` and [a note](note.md).\n"
        "Missing: `real/nope.md` and [gone](gone.md).\n"
        "Not paths: `integrate/v1.5.0`, `docker.io/org`, `HOME=/tmp`, `~/.local`, "
        "`notes/<lane>/x`, `*.md`, `--flag`, [web](https://example.com/a.md).\n"
        "Ignored: `ledger/x.md`.\n"
    )
    got = cdr.unresolved_paths(local, [tmp_path], ignore=[r"^ledger/"])
    assert got == [
        f"{local}:2: real/nope.md: no such path",
        f"{local}:2: gone.md: no such path",
    ]
    assert cdr.main(["--resolve-paths", str(local), "--base", str(tmp_path)]) == 1


def test_resolve_paths_prefix_base(tmp_path):
    """A PREFIX=DIR base takes its prefix's tokens, which resolve only there."""
    (tmp_path / "lane" / "src").mkdir(parents=True)
    (tmp_path / "old" / "src").mkdir(parents=True)
    (tmp_path / "old" / "notes").mkdir(parents=True)
    (tmp_path / "old" / "src" / "gone.py").write_text("x\n")
    (tmp_path / "old" / "notes" / "plan.md").write_text("x\n")
    local = tmp_path / "NOTES.md"
    local.write_text("`src/gone.py` and `notes/plan.md`\n")
    # With two plain bases the stale path resolves in the old tree.
    assert cdr.unresolved_paths(local, [tmp_path / "lane", tmp_path / "old"]) == []
    bases = [str(tmp_path / "lane"), f"notes/={tmp_path / 'old'}"]
    assert cdr.unresolved_paths(local, bases) == [f"{local}:1: src/gone.py: no such path"]


def test_main_passes_on_this_tree(capsys):
    assert cdr.main([]) == 0, capsys.readouterr().out


def test_the_checker_is_not_its_own_reader(tmp_path):
    files = {
        **_BASE,
        "docs/x.md": "# x\n",
        "scripts/check_doc_readers.py": 'KNOWN = ("docs/x.md",)\n',
    }
    repo = _repo(tmp_path, files)
    assert "docs/x.md" in cdr.unread_files(repo)
