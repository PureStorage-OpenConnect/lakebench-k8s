"""LB-260: a nameless config reached through a symbolic link. 1.6 read the
v1.6 name in ``.lakebench/state.json`` beside the path it was given; v1.7
reads it there too. When the directory of the file the link points to
records another name, every command that may look at a deployment refuses
a nameless load without --name, so one config cannot act on, or report,
another directory's deployment; init and relocate refuse too."""

from __future__ import annotations

import json
import warnings
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from lakebench.cli import app
from lakebench.config import deploy_state as ds
from lakebench.config._load_context import LoadPurpose
from lakebench.config.loader import ConfigNameRequired, load_config, load_notes
from lakebench.exit_codes import SafetyRefusal
from tests.test_saf2_deploy_state import (
    ANNOTATION_CREATED_BUCKETS,
    ANNOTATION_DEPLOY_NONCE,
    ANNOTATION_DEPLOYMENT_NAME,
    FakeCore,
)

CONFIG = "recipe: hive-iceberg-spark-trino\n"
LOOKS_AT_A_DEPLOYMENT = [
    LoadPurpose.TEARDOWN,
    LoadPurpose.READ,
    LoadPurpose.COMPARE,
    LoadPurpose.MUTATE,
    LoadPurpose.RUN,
]


def _state(directory: Path, name: str) -> None:
    (directory / ".lakebench").mkdir(parents=True, exist_ok=True)
    (directory / ".lakebench" / "state.json").write_text(json.dumps({"name": name}))


def _stamped(core: FakeCore, name: str) -> None:
    core.add(
        name,
        **{
            ANNOTATION_DEPLOYMENT_NAME: name,
            ANNOTATION_DEPLOY_NONCE: "n16",
            ANNOTATION_CREATED_BUCKETS: f"{name}-bronze,{name}-gold,{name}-silver",
        },
    )


@pytest.fixture
def dirs(tmp_path):
    real = tmp_path / "real"
    link_dir = tmp_path / "linked"
    real.mkdir()
    link_dir.mkdir()
    (real / "c.yaml").write_text(CONFIG)
    (link_dir / "c.yaml").symlink_to(real / "c.yaml")
    return real, link_dir


def _load(path: Path, purpose: LoadPurpose, name: str | None = None):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return load_config(path, purpose=purpose, name_override=name, print_notes=False)


def test_the_name_is_read_beside_the_path_given(dirs):
    real, link_dir = dirs
    _state(link_dir, "lb-link")
    link = link_dir / "c.yaml"
    assert ds.legacy_state_path(link) == real / ".lakebench" / "state.json"
    assert ds.read_legacy_name(link) == "lb-link"
    r = ds.resolve_name(link, {})
    assert (r.name, r.source, r.legacy_state_path, r.resolved_legacy) == (
        "lb-link",
        "legacy-state",
        link_dir / ".lakebench" / "state.json",
        None,
    )
    assert _load(link, LoadPurpose.READ, "lb-link").name == "lb-link"


@pytest.mark.parametrize("purpose", LOOKS_AT_A_DEPLOYMENT)
def test_two_directories_disagree_and_the_load_is_refused(dirs, purpose):
    """Before the fix the load read lb-real, the target's name, through the
    link, and a destroy or status checked that deployment."""
    real, link_dir = dirs
    _state(link_dir, "lb-link")
    _state(real, "lb-real")
    r = ds.resolve_name(link_dir / "c.yaml", {})
    assert (r.name, r.legacy_name) == ("lb-link", "lb-link")
    assert r.resolved_legacy == (real / ".lakebench" / "state.json", "lb-real")
    with pytest.raises(ConfigNameRequired, match="symbolic link") as ei:
        _load(link_dir / "c.yaml", purpose)
    msg = str(ei.value)
    assert "'lb-link'" in msg and "'lb-real'" in msg
    # The advice never says to name the file both directories share.
    assert "copy of the file" in msg and "add the deployment's name" not in msg


@pytest.mark.parametrize("purpose", LOOKS_AT_A_DEPLOYMENT)
def test_a_name_only_beside_the_target_is_refused(dirs, purpose):
    """1.6 never read it through the link, and a read load would otherwise
    go on under a suggested name; refuse rather than guess either."""
    real, link_dir = dirs
    _state(real, "lb-real")
    r = ds.resolve_name(link_dir / "c.yaml", {})
    assert r.source == "suggested" and r.resolved_legacy is not None
    with pytest.raises(ConfigNameRequired, match="no name in .*'lb-real'"):
        _load(link_dir / "c.yaml", purpose)
    # Through its own path the config reads its own directory, as before.
    assert ds.resolve_name(real / "c.yaml", {}).name == "lb-real"


def test_inspect_loads_under_the_name_1_6_read_with_a_note(dirs):
    real, link_dir = dirs
    _state(link_dir, "lb-link")
    _state(real, "lb-real")
    cfg = _load(link_dir / "c.yaml", LoadPurpose.INSPECT)
    assert cfg.name == "lb-link"
    assert any("'lb-real'" in t for t in load_notes(cfg).texts())


def test_destroy_with_name_is_checked_against_the_link_directory(dirs):
    """--name goes to the stamp checks, which compare it with the name 1.6
    read through this path, then with the namespace's own stamps."""
    real, link_dir = dirs
    _state(link_dir, "lb-link")
    _state(real, "lb-real")
    link = link_dir / "c.yaml"
    core = FakeCore()
    _stamped(core, "lb-link")
    _stamped(core, "lb-real")

    def check(name: str):
        cfg = _load(link, LoadPurpose.TEARDOWN, name)
        return ds.check_nameless_target(
            cfg, lambda: core, config_path=link, bucket_owned=lambda b: False
        )

    with pytest.raises(SafetyRefusal) as ei:
        check("lb-real")
    assert ei.value.path == "nameless.stamp_mismatch"
    assert check("lb-link") == "u1#n16"


def test_equal_names_and_a_linked_directory_are_one_answer(tmp_path, dirs):
    real, link_dir = dirs
    _state(link_dir, "lb-same")
    _state(real, "lb-same")
    assert ds.resolve_name(link_dir / "c.yaml", {}).resolved_legacy is None
    # A symbolic link to the directory: one state file, one name.
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)
    _state(real, "lb-real")
    r = ds.resolve_name(alias / "c.yaml", {})
    assert r.name == "lb-real" and r.resolved_legacy is None
    assert ds.legacy_names(alias / "c.yaml") == {alias / ".lakebench" / "state.json": "lb-real"}


def test_a_named_config_is_unaffected(dirs):
    real, link_dir = dirs
    _state(link_dir, "lb-link")
    _state(real, "lb-real")
    (real / "c.yaml").write_text("name: mine\n" + CONFIG)
    assert _load(link_dir / "c.yaml", LoadPurpose.TEARDOWN).name == "mine"


@pytest.mark.parametrize("where", ["real", "linked"])
def test_init_overwrite_through_a_link_refuses_either_name(dirs, where):
    """The file replaced is the target, the config of whichever directory
    deployed it, so a v1.6 name in either refuses the overwrite."""
    real, link_dir = dirs
    _state(real if where == "real" else link_dir, "lb-v16")
    r = CliRunner().invoke(app, ["init", "-o", str(link_dir / "c.yaml"), "--overwrite"])
    assert r.exit_code == 2, r.output
    assert "lb-v16" in r.output
    assert (real / "c.yaml").read_text() == CONFIG
    assert yaml.safe_load(CONFIG) == {"recipe": "hive-iceberg-spark-trino"}


def test_relocate_through_a_link_with_v16_names_is_refused(dirs, tmp_path):
    real, link_dir = dirs
    _state(link_dir, "lb-link")
    with pytest.raises(ds.RelocateRefused, match="symbolic link"):
        ds.relocate_state(link_dir / "c.yaml", tmp_path / "new", name="lb-link")
    assert not (tmp_path / "new").exists()
