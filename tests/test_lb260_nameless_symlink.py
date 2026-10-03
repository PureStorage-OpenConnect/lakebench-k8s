"""LB-260: a nameless config reached through a symbolic link. 1.6 read the
v1.6 name in ``.lakebench/state.json`` beside the path it was given; the
nameless checks read it there too, and refuse when the directory of the
file the link points to records a different name, so one config cannot act
on another directory's deployment."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lakebench.config._load_context import LoadPurpose
from lakebench.config.deploy_state import legacy_state_path, read_legacy_name, resolve_name
from lakebench.config.loader import ConfigValidationError, load_config

CONFIG = """\
recipe: hive-iceberg-spark-trino
platform:
  storage:
    s3:
      endpoint: http://10.0.1.50:80
      access_key: a
      secret_key: b
"""


def _state(directory: Path, name: str) -> None:
    (directory / ".lakebench").mkdir(parents=True, exist_ok=True)
    (directory / ".lakebench" / "state.json").write_text(json.dumps({"name": name}))


@pytest.fixture
def dirs(tmp_path):
    real = tmp_path / "real"
    link_dir = tmp_path / "linked"
    real.mkdir()
    link_dir.mkdir()
    (real / "c.yaml").write_text(CONFIG)
    (link_dir / "c.yaml").symlink_to(real / "c.yaml")
    return real, link_dir


def test_the_name_is_read_beside_the_path_given(dirs):
    real, link_dir = dirs
    _state(link_dir, "lb-link")
    link = link_dir / "c.yaml"
    assert legacy_state_path(link) == real / ".lakebench" / "state.json"
    assert read_legacy_name(link) == "lb-link"
    r = resolve_name(link, {})
    assert (r.name, r.source, r.legacy_state_path) == (
        "lb-link",
        "legacy-state",
        link_dir / ".lakebench" / "state.json",
    )


def test_two_different_names_are_refused(dirs):
    real, link_dir = dirs
    _state(link_dir, "lb-link")
    _state(real, "lb-real")
    with pytest.raises(ValueError, match="symbolic link.*'lb-link'.*'lb-real'"):
        resolve_name(link_dir / "c.yaml", {})


def test_a_name_only_beside_the_target_is_refused(dirs):
    """1.6 never read it through the link: it names the deployment of the
    config's own directory, not one made through the link."""
    real, link_dir = dirs
    _state(real, "lb-real")
    with pytest.raises(ValueError, match="no name in .*'lb-real'"):
        resolve_name(link_dir / "c.yaml", {})
    # Through its own path the config reads its own directory, as before.
    assert resolve_name(real / "c.yaml", {}).name == "lb-real"


@pytest.mark.parametrize("purpose", [LoadPurpose.TEARDOWN, LoadPurpose.READ, LoadPurpose.INSPECT])
def test_destroy_and_status_through_the_link_are_refused(dirs, purpose):
    real, link_dir = dirs
    _state(link_dir, "lb-link")
    _state(real, "lb-real")
    with pytest.raises(ConfigValidationError, match="symbolic link"):
        load_config(link_dir / "c.yaml", purpose=purpose, print_notes=False)


def test_name_override_goes_to_the_stamp_checks(dirs):
    real, link_dir = dirs
    _state(link_dir, "lb-link")
    _state(real, "lb-real")
    r = resolve_name(link_dir / "c.yaml", {}, "lb-link")
    assert (r.name, r.source, r.legacy_name) == ("lb-link", "override", "lb-link")
    assert r.resolved_legacy == (real / ".lakebench" / "state.json", "lb-real")


def test_equal_names_and_a_linked_directory_are_one_answer(tmp_path, dirs):
    real, link_dir = dirs
    _state(link_dir, "lb-same")
    _state(real, "lb-same")
    assert resolve_name(link_dir / "c.yaml", {}).name == "lb-same"
    # A symbolic link to the directory: one state file, one name.
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)
    _state(real, "lb-real")
    r = resolve_name(alias / "c.yaml", {})
    assert r.name == "lb-real" and r.resolved_legacy is None


def test_a_named_config_is_unaffected(dirs):
    real, link_dir = dirs
    _state(link_dir, "lb-link")
    _state(real, "lb-real")
    assert resolve_name(link_dir / "c.yaml", {"name": "mine"}).name == "mine"
