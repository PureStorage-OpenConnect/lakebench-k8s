"""Links, anchors and code references in tracked markdown resolve (OSS-6, PRC-3).

Relative links and absolute links into this repository must name a tracked
file or directory; a ``#anchor`` must name a heading (GitHub's slug rule) or
an HTML anchor in its target page; a backticked ``src/``, ``tests/``,
``scripts/`` or ``datagen_rs/`` reference must name an existing path, and
with ``:symbol`` or ``::test`` a name the file defines. External links are
not fetched. The paths that published PyPI READMEs link stay valid.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "check_doc_readers", ROOT / "scripts" / "check_doc_readers.py"
)
cdr = importlib.util.module_from_spec(_spec)
sys.modules["check_doc_readers"] = cdr
_spec.loader.exec_module(cdr)

#: Every `blob/main/<path>` the 1.5.0 and 1.6.0 PyPI READMEs link (checked at
#: tag v1.6.0 and at c499cc2; 1.5.0's are a subset). Pinned here because CI
#: has a shallow clone and no tags. Each must exist as a page or as a
#: published stub.
PUBLISHED_README_PATHS = (
    "CHANGELOG.md",
    "LICENSE",
    "docs/aml-scoring.md",
    "docs/architecture.md",
    "docs/benchmarking.md",
    "docs/cli-reference.md",
    "docs/compatibility-matrix.md",
    "docs/configuration.md",
    "docs/getting-started.md",
    "docs/recipes.md",
    "docs/running-pipelines.md",
    "docs/storage-backends.md",
    "docs/supported-components.md",
    "docs/troubleshooting.md",
)
PUBLISHED_README_ANCHORS = (("docs/compatibility-matrix.md", "support-states"),)
#: `tree/main/<dir>` links: the 1.6.0 README links `examples`, and the
#: pyproject Documentation URL is `tree/main/docs`.
PUBLISHED_README_DIRS = ("docs", "examples")


def _repo(tmp_path: Path, files: dict[str, str]) -> Path:
    for rel, text in files.items():
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    for args in (["init", "-q"], ["add", "-A"]):
        subprocess.run(
            ["git", "-C", str(tmp_path), *args], check=True, capture_output=True, env=env
        )
    return tmp_path


def test_relative_links_resolve():
    bad = [p for p in cdr.link_problems(ROOT) if "no tracked file" in p]
    assert not bad, "\n".join(bad)


def test_anchors_resolve():
    bad = [p for p in cdr.link_problems(ROOT) if "no anchor" in p]
    assert not bad, "\n".join(bad)


def test_planted_dead_link_and_anchor_fail(tmp_path):
    page = (
        "# Title\n\n## Second `code` part: here!\n\n"
        "[ok](other.md) [ok](other.md#a-b) [ok](#second-code-part-here) [ok](sub/)\n"
        "[ok](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/other.md)\n"
        "[ext](https://example.com/x.md) [mail](mailto:a@b.c)\n"
        "`[in code](missing.md)`\n```\n[in a fence](missing.md)\n```\n"
        "[dead](missing.md) [bad](other.md#nope) [dead](https://github.com/PureStorage-OpenConnect/lakebench-k8s/tree/main/gone)\n"
    )
    repo = _repo(
        tmp_path,
        {"docs/page.md": page, "docs/other.md": "# A B\n", "docs/sub/README.md": "# Sub\n"},
    )
    problems = cdr.link_problems(repo)
    assert len(problems) == 3, problems
    joined = "\n".join(problems)
    assert "missing.md: no tracked" in joined and "no anchor #nope" in joined
    assert "directory gone" in joined


def test_slug_follows_github():
    text = (
        '# Hello, World!\n## Hello World\n# Hello World\n## `cfg.key` (v1.7)\n<a name="x-y"></a>\n'
    )
    assert cdr.anchors(text) >= {
        "hello-world",
        "hello-world-1",
        "hello-world-2",
        "cfgkey-v17",
        "x-y",
    }
    assert cdr.slug("Snake_case and **bold** [link](u)") == "snake_case-and-bold-link"
    # Code spans keep their underscores; only matched emphasis pairs drop.
    assert cdr.slug("The `_JOB_PROFILES` table") == "the-_job_profiles-table"
    assert cdr.slug("`__init__.py` and _em_ and foo_ bar") == "__init__py-and-em-and-foo_-bar"


def published_path_problems(root: Path) -> list[str]:
    problems = [f"{p}: missing" for p in PUBLISHED_README_PATHS if not (root / p).is_file()]
    problems += [f"{d}/: missing" for d in PUBLISHED_README_DIRS if not (root / d).is_dir()]
    for page, anchor in PUBLISHED_README_ANCHORS:
        if (root / page).is_file() and anchor not in cdr.anchors((root / page).read_text()):
            problems.append(f"{page}#{anchor}: no such anchor")
    extra = set(cdr.PUBLISHED_STUBS) - set(PUBLISHED_README_PATHS)
    problems += [f"{s}: a stub for a path no README published" for s in sorted(extra)]
    return problems + cdr.stub_problems(root)


def test_published_readme_paths_exist():
    assert published_path_problems(ROOT) == []


def test_published_path_check_fails_without_the_page(tmp_path):
    files = {p: "x\n" for p in PUBLISHED_README_PATHS if p != "docs/aml-scoring.md"}
    files["docs/compatibility-matrix.md"] = "# Matrix\n"
    repo = _repo(tmp_path, files)
    assert published_path_problems(repo) == [
        "docs/aml-scoring.md: missing",
        "examples/: missing",
        "docs/compatibility-matrix.md#support-states: no such anchor",
    ]


def test_a_stub_must_link_its_new_page(tmp_path, monkeypatch):
    monkeypatch.setattr(cdr, "PUBLISHED_STUBS", ("docs/aml-scoring.md",))
    repo = _repo(tmp_path, {"docs/aml-scoring.md": "# Moved\n\nNow elsewhere.\n"})
    assert cdr.stub_problems(repo) == ["docs/aml-scoring.md: published stub links no tracked page"]


def test_wrapped_links_footnotes_and_repo_urls(tmp_path):
    page = (
        "# T\n\nA [link whose text\nwraps](dead-wrapped.md) and [ok](other.md).\n\n"
        "[^1]: a footnote, not a link\n"
        "[upper](https://github.com/purestorage-openconnect/LAKEBENCH-K8S/blob/main/docs/nope.md)\n"
        "[pinned](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/v1.6.0/docs/old.md)\n"
        "Setext\n---\n\n[s](#setext) [n](#note) [e](#a--b) [h](#Foo) [h2](#foo)\n"
        '## _Note_\n## A &amp; B\n<div id="Foo"></div>\n'
    )
    repo = _repo(tmp_path, {"docs/page.md": page, "docs/other.md": "# Other\n"})
    problems = cdr.link_problems(repo)
    assert len(problems) == 2, problems
    assert problems[0].startswith("docs/page.md:3: dead-wrapped.md")
    assert "docs/nope.md" in problems[1]


def test_code_refs_resolve():
    bad = cdr.code_ref_problems(ROOT)
    assert not bad, "\n".join(bad)


def test_planted_code_refs_fail(tmp_path):
    job = "_JOB_PROFILES = {}\ndef build():\n    pass\n"
    page = (
        "`src/lakebench/job.py` `src/lakebench/job.py:_JOB_PROFILES` `src/lakebench/job.py:12`\n"
        "`tests/test_x.py::test_ok` `tests/test_x.py::TestThing::test_ok` `src/lakebench/`\n"
        "`src/lakebench/<module>.py` `tests/test_*.py`\n"
        "`src/lakebench/nope.py` `src/lakebench/job.py:_NO_SUCH_PROFILE` `tests/test_x.py::test_gone`\n"
        "`src/lakebench::test_dir`\n"
    )
    repo = _repo(
        tmp_path,
        {
            "docs/page.md": page,
            "src/lakebench/job.py": job,
            "tests/test_x.py": "class TestThing:\n    def test_ok(self):\n        pass\n\ndef test_ok():\n    pass\n",
        },
    )
    problems = cdr.code_ref_problems(repo)
    assert len(problems) == 4, problems
    assert "is not a file" in problems[3]
    assert (
        "nope.py" in problems[0]
        and "_NO_SUCH_PROFILE" in problems[1]
        and "test_gone" in problems[2]
    )
