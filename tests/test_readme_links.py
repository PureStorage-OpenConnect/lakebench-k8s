"""The README is the PyPI project page, and PyPI does not resolve relative links.

`docs/cli-reference.md` on that page becomes
`pypi.org/project/lakebench-k8s/<version>/docs/cli-reference.md`, which PyPI does
not serve, and a published version's description cannot be edited. Every link
and image in the README must therefore be absolute, and a link into this
repository's docs must name a file and heading that exist.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
REPO_DOCS = "https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/"

_LINK = re.compile(r"!?\[[^\]]*\]\(([^)\s]+)[^)]*\)")
_SRC = re.compile(r'(?:src|href)="([^"]+)"')


def _targets() -> list[str]:
    text = (ROOT / "README.md").read_text()
    return _LINK.findall(text) + _SRC.findall(text)


def _slug(heading: str) -> str:
    """GitHub's heading anchor: lower case, punctuation dropped, spaces to hyphens."""
    kept = re.sub(r"[^\w\- ]", "", heading.strip().lower())
    return kept.replace(" ", "-")


def test_the_readme_links_somewhere():
    assert any(t.startswith(REPO_DOCS) for t in _targets())


@pytest.mark.parametrize("target", _targets())
def test_readme_links_are_absolute(target):
    assert re.match(r"https?://|mailto:", target), (
        f"{target!r} is relative; on PyPI it resolves under the project page and 404s"
    )


@pytest.mark.parametrize("target", [t for t in _targets() if t.startswith(REPO_DOCS)])
def test_readme_doc_links_name_a_real_file_and_heading(target):
    path, _, anchor = target.removeprefix(REPO_DOCS).partition("#")
    page = ROOT / "docs" / path
    assert page.is_file(), f"{target} has no file docs/{path}"
    if anchor:
        headings = re.findall(r"^#+\s+(.*?)\s*$", page.read_text(), re.MULTILINE)
        assert anchor in {_slug(h) for h in headings}, f"docs/{path} has no heading for #{anchor}"
