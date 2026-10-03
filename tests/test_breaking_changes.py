"""The 1.7 breaking-changes list against the code, UPGRADING-1.7.md and the CHANGELOG (OSS-7).

* list to docs: every entry is a heading of UPGRADING-1.7.md whose slug is
  its ``id``, and its ``text`` is a bullet under 1.7.0 "Breaking changes";
* code to list: ``scripts/upgrading.py``'s ``missing_entries()`` is empty
  (removed keys since the 1.6.0 snapshot, the alias and refusal tables,
  renumbered exit codes, image defaults, workload versions and the two
  changes no table carries);
* every ``source`` names a path that exists and a symbol in it.
"""

from __future__ import annotations

import importlib.util
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


def _changelog_breaking_bullets() -> list[str]:
    text = (ROOT / "CHANGELOG.md").read_text()
    current = text[
        text.index("## [Unreleased]") : text.index("\n## [", text.index("## [Unreleased]") + 1)
    ]
    bullets: list[str] = []
    for section in re.split(r"^### ", current, flags=re.M)[1:]:
        head, _, body = section.partition("\n")
        if head.strip() == "Breaking changes":
            bullets += [line[2:] for line in body.splitlines() if line.startswith("- ")]
    return bullets


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
    headings = {slug(m.group(1)) for m in re.finditer(r"^### (.+)$", UPGRADING.read_text(), re.M)}
    bullets = set(_changelog_breaking_bullets())
    for e in _entries():
        assert e["id"] in headings, f"{e['id']}: no heading in UPGRADING-1.7.md"
        assert e["text"] in bullets, f"{e['id']}: its text is not a 1.7.0 breaking bullet"
        assert e["text"] in UPGRADING.read_text() and e["fix"] in UPGRADING.read_text(), e["id"]


def test_no_missing_entries(up):
    assert up.missing_entries(LIST) == []


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
    import json

    assert json.loads(up.IMAGES_SNAPSHOT.read_text())["commit"] == up.V160_COMMIT
    assert up.removed_keys_1_6()  # not empty


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
