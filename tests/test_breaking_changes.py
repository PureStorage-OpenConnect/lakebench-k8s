"""The 1.7 breaking-changes list against the code, UPGRADING-1.7.md and the CHANGELOG (OSS-7).

* list to docs: every entry is a heading of UPGRADING-1.7.md whose slug is
  its ``id``, with its ``text`` and ``fix`` under that heading, and its
  ``text`` is a bullet under the 1.7 "Breaking changes";
* code to list: ``scripts/upgrading.py``'s ``missing_entries()`` is empty
  (removed keys since the 1.6.0 snapshot, the alias and refusal tables,
  renumbered exit codes, image defaults, workload versions and the two
  changes no table carries);
* every ``source`` names a path that exists and a symbol in it.
"""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
LIST = ROOT / "docs" / "upgrading" / "breaking-1.7.yaml"
UPGRADING = ROOT / "UPGRADING-1.7.md"


@pytest.fixture(scope="module")
def up():
    spec = importlib.util.spec_from_file_location("upgrading", ROOT / "scripts" / "upgrading.py")
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _entries() -> list[dict]:
    return yaml.safe_load(LIST.read_text())


def slug(title: str) -> str:
    """GitHub's heading anchor: lower case, punctuation dropped, spaces to hyphens."""
    return re.sub(r"[^a-z0-9 _-]", "", title.lower()).replace(" ", "-")


def _release_part(text: str) -> str:
    """The 1.7 section of the CHANGELOG: ``[1.7.0]`` once released, else
    ``[Unreleased]``."""
    for heading in ("## [1.7.0]", "## [Unreleased]"):
        if heading in text:
            start = text.index(heading)
            end = text.find("\n## [", start + 1)
            return text[start : end if end != -1 else len(text)]
    raise AssertionError("CHANGELOG.md has neither [1.7.0] nor [Unreleased]")


def _changelog_breaking_bullets(text: str | None = None) -> list[str]:
    part = _release_part(text if text is not None else (ROOT / "CHANGELOG.md").read_text())
    bullets: list[str] = []
    for section in re.split(r"^### ", part, flags=re.M)[1:]:
        head, _, body = section.partition("\n")
        if head.strip() == "Breaking changes":
            bullets += [line[2:] for line in body.splitlines() if line.startswith("- ")]
    return bullets


def _upgrading_sections() -> dict[str, str]:
    """{heading slug: the text under that heading, up to the next heading}."""
    parts = re.split(r"^#{2,3} (.+)$", UPGRADING.read_text(), flags=re.M)
    return {slug(parts[i]): parts[i + 1] for i in range(1, len(parts) - 1, 2)}


def _squash(text: str) -> str:
    return " ".join(text.split())


def test_entries_are_well_formed(up):
    entries = _entries()
    ids = [e["id"] for e in entries]
    assert len(ids) == len(set(ids))
    for e in entries:
        assert e["kind"] in up.KINDS, e["id"]
        assert e["id"] == slug(e["title"]), e["id"]
        for key in ("text", "fix", "source"):
            assert isinstance(e[key], str) and e[key].strip() and "\n" not in e[key], (e["id"], key)


def test_list_entries_in_upgrading_and_changelog():
    sections = _upgrading_sections()
    bullets = set(_changelog_breaking_bullets())
    for e in _entries():
        assert e["id"] in sections, f"{e['id']}: no heading in UPGRADING-1.7.md"
        body = _squash(sections[e["id"]])
        assert _squash(e["text"]) in body, f"{e['id']}: text not under its heading"
        assert _squash(e["fix"]) in body, f"{e['id']}: fix not under its heading"
        assert e["text"] in bullets, f"{e['id']}: its text is not a 1.7 breaking bullet"


def test_no_todo_left():
    assert "TODO" not in LIST.read_text()


def test_no_missing_entries(up):
    missing = up.missing_entries(LIST)
    assert missing == [], (
        "breaking changes with no entry in docs/upgrading/breaking-1.7.yaml; run "
        "`python3.11 scripts/upgrading.py missing` for skeletons: "
        + ", ".join(f"{m['kind']} {m['subject']}" for m in missing)
    )


def test_sources_resolve():
    for e in _entries():
        path, _, symbol = e["source"].rpartition(":")
        file = ROOT / path
        assert file.is_file(), f"{e['id']}: {path} does not exist"
        assert re.search(rf"\b{re.escape(symbol)}\b", file.read_text()), (
            f"{e['id']}: {symbol} is not in {path}"
        )


def test_snapshots_are_the_v1_6_0_tag(up):
    assert up.snapshot_commit(up.REMOVED_SNAPSHOT) == up.V160_COMMIT
    assert json.loads(up.IMAGES_SNAPSHOT.read_text())["commit"] == up.V160_COMMIT
    assert up.removed_keys_1_6()  # not empty


def test_the_release_section_is_read_after_the_cut():
    released = (
        "## [Unreleased]\n\n## [1.7.0] - 2026-11-03\n\n### Breaking changes\n- one\n\n## [1.6.0]\n"
    )
    assert _changelog_breaking_bullets(released) == ["one"]


# -- planted omissions: each code table, one new item ----------------------------


def test_a_new_removed_key_is_missing(up, monkeypatch):
    real = up.removed_keys
    monkeypatch.setattr(up, "removed_keys", lambda: [*real(), "platform.storage.s3.new_key"])
    missing = up.missing_entries(LIST)
    assert [(m["kind"], m["subject"]) for m in missing] == [
        ("removed-key", "platform.storage.s3.new_key")
    ]


def test_image_default_bumps_listed(up, monkeypatch):
    real = up.image_defaults
    monkeypatch.setattr(up, "image_defaults", lambda: {**real(), "trino": "trinodb/trino:999"})
    missing = up.missing_entries(LIST)
    assert [(m["kind"], m["subject"]) for m in missing] == [("version-bump", "images.trino")]


def test_a_new_alias_and_exit_code_are_missing(up, monkeypatch):
    from lakebench.cli import _aliases
    from lakebench.exit_codes import PATHS, ExitCode, ExitPath

    monkeypatch.setitem(_aliases.ALIASES, "old-verb", _aliases.Alias("report"))
    monkeypatch.setattr(
        "lakebench.exit_codes.PATHS",
        (*PATHS, ExitPath("new.path", ExitCode.FAILED, "x", v16_code=0)),
    )
    subjects = {(m["kind"], m["subject"]) for m in up.missing_entries(LIST)}
    assert subjects == {("alias", "old-verb"), ("exit-code", "new.path")}


def test_a_workload_version_bump_is_missing(up, monkeypatch):
    monkeypatch.setitem(
        __import__("lakebench.metrics.experiment", fromlist=["x"]).WORKLOAD_VERSIONS,
        "financial",
        "aml-999",
    )
    subjects = {(m["kind"], m["subject"]) for m in up.missing_entries(LIST)}
    assert subjects == {("identity", "workload financial")}


def test_a_tableless_entry_is_required_by_id(up, tmp_path):
    entries = [e for e in _entries() if e["id"] != "deploy-generates-the-polaris-client-secret"]
    planted = tmp_path / "list.yaml"
    planted.write_text(yaml.safe_dump(entries))
    missing = up.missing_entries(planted)
    assert [m["subject"] for m in missing] == ["deploy-generates-the-polaris-client-secret"]


def test_missing_prints_a_skeleton(up):
    text = up.skeleton({"kind": "removed-key", "subject": "a.b", "hint": "h"})
    entry = yaml.safe_load(text)[0]
    assert entry["kind"] == "removed-key" and entry["subject"] == "a.b"
    assert entry["id"] == slug(entry["title"])  # well formed once its TODOs are replaced
    assert {"id", "title", "kind", "subject", "source", "text", "fix"} <= set(entry)
