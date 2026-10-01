"""LoadPurpose (CC-1): SAF-2 parts (a) and (e), and CFG-1's per-purpose refusals.

A config with no name cannot change data, a removed key is refused by the
commands that change data and dropped with a note by the others, and no
read-only command writes a file.
"""

from __future__ import annotations

import importlib
import json
import shutil
import warnings
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from lakebench.cli import app
from lakebench.config import load_config
from lakebench.config.loader import (
    ConfigFileNotFoundError,
    ConfigNameRequired,
    ConfigValidationError,
    LoadPurpose,
    load_notes,
    name_resolution,
)

runner = CliRunner()

FIXTURES = Path(__file__).parent / "fixtures"
NAMELESS = {"endpoint": "http://127.0.0.1:9", "access_key": "k", "secret_key": "s", "scale": 1}


def _write(directory: Path, data: dict, filename: str = "lakebench.yaml") -> Path:
    path = directory / filename
    path.write_text(yaml.safe_dump(data))
    return path


def _listing(root: Path) -> list[str]:
    return sorted(str(p.relative_to(root)) for p in root.rglob("*"))


# -- SAF-2 (a): nameless configs ---------------------------------------------


@pytest.mark.parametrize("purpose", [LoadPurpose.MUTATE, LoadPurpose.RUN])
def test_nameless_config_refused_for_commands_that_change_data(tmp_path, purpose):
    cfg_path = _write(tmp_path, NAMELESS)
    with pytest.raises(ConfigNameRequired) as e:
        load_config(cfg_path, purpose=purpose)
    msg = str(e.value)
    assert "cannot change data" in msg
    assert e.value.resolution.source == "suggested"
    assert f"name: {e.value.resolution.name}" in msg
    assert _listing(tmp_path) == ["lakebench.yaml"]


def test_nameless_suggestion_is_stable_and_shaped(tmp_path, monkeypatch):
    monkeypatch.setattr("getpass.getuser", lambda: "Jane.Doe-42x")
    cfg_path = _write(tmp_path, NAMELESS)
    first = name_resolution(load_config(cfg_path, purpose=LoadPurpose.READ))
    second = name_resolution(load_config(cfg_path, purpose=LoadPurpose.READ))
    assert first is not None and second is not None
    assert first.name == second.name
    assert first.source == "suggested"
    user, digest = first.name.split("-")[1:]
    assert first.name.startswith("lb-")
    assert user == "janedoe4"  # [a-z0-9] only, cut to 8
    assert len(digest) == 6 and int(digest, 16) >= 0


def test_suggestion_differs_by_host(tmp_path, monkeypatch):
    # Every lane here runs as root, so the path alone would collide across hosts.
    from lakebench.config.deploy_state import suggested_name

    cfg_path = _write(tmp_path, NAMELESS)
    monkeypatch.setattr("socket.gethostname", lambda: "host-a")
    a = suggested_name(cfg_path)
    monkeypatch.setattr("socket.gethostname", lambda: "host-b")
    assert suggested_name(cfg_path) != a


def test_nameless_teardown_without_state_refused(tmp_path):
    # No v1.6 state and no name: a deployment under the suggested name was
    # made by some other config, so destroy and admin must not target it.
    cfg_path = _write(tmp_path, NAMELESS)
    with pytest.raises(ConfigNameRequired) as e:
        load_config(cfg_path, purpose=LoadPurpose.TEARDOWN)
    assert "only a suggestion" in str(e.value)
    # With an explicit override it loads (CC-2 adds the checks on top).
    assert load_config(cfg_path, purpose=LoadPurpose.TEARDOWN, name_override="x").name == "x"


def test_v16_state_name_read_verbatim(tmp_path):
    # R7: the v1.6 name is read, never written. SPEC SAF-2 refuses a v1.6
    # directory without --name, so teardown and read loads refuse and name
    # it; the override (CC-2's --name) and COMPARE load under it.
    (tmp_path / ".lakebench").mkdir()
    shutil.copy(FIXTURES / "v16-state" / "state.json", tmp_path / ".lakebench" / "state.json")
    before = (tmp_path / ".lakebench" / "state.json").read_bytes()
    cfg_path = _write(tmp_path, NAMELESS)
    for purpose in (LoadPurpose.TEARDOWN, LoadPurpose.READ, LoadPurpose.MUTATE):
        with pytest.raises(ConfigNameRequired) as e:
            load_config(cfg_path, purpose=purpose)
        assert "name: lb-20260915-101530" in str(e.value)
        assert e.value.resolution.source == "legacy-state"
    for purpose in (LoadPurpose.COMPARE, LoadPurpose.INSPECT):
        cfg = load_config(cfg_path, purpose=purpose)
        assert cfg.name == "lb-20260915-101530"
        res = name_resolution(cfg)
        assert res is not None and res.source == "legacy-state"
    over = load_config(cfg_path, purpose=LoadPurpose.TEARDOWN, name_override="lb-20260915-101530")
    assert over.name == "lb-20260915-101530"
    assert (tmp_path / ".lakebench" / "state.json").read_bytes() == before
    assert _listing(tmp_path) == [".lakebench", ".lakebench/state.json", "lakebench.yaml"]


def test_corrupt_legacy_state_is_left_alone(tmp_path):
    (tmp_path / ".lakebench").mkdir()
    (tmp_path / ".lakebench" / "state.json").write_text("{not json")
    cfg = load_config(_write(tmp_path, NAMELESS), purpose=LoadPurpose.READ)
    res = name_resolution(cfg)
    assert res is not None and res.source == "suggested"
    assert (tmp_path / ".lakebench" / "state.json").read_text() == "{not json"


def test_name_override_for_nameless_config(tmp_path):
    cfg = load_config(_write(tmp_path, NAMELESS), purpose=LoadPurpose.TEARDOWN, name_override="x1")
    assert cfg.name == "x1"
    res = name_resolution(cfg)
    assert res is not None and res.source == "override"


def test_name_override_must_match_config_name(tmp_path):
    cfg_path = _write(tmp_path, {**NAMELESS, "name": "mine"})
    assert load_config(cfg_path, purpose=LoadPurpose.READ, name_override="mine").name == "mine"
    with pytest.raises(ConfigValidationError, match="does not match the config's name"):
        load_config(cfg_path, purpose=LoadPurpose.READ, name_override="other")


def test_named_config_loads_for_every_purpose(tmp_path):
    cfg_path = _write(tmp_path, {**NAMELESS, "name": "mine"})
    for purpose in LoadPurpose:
        cfg = load_config(cfg_path, purpose=purpose)
        res = name_resolution(cfg)
        assert cfg.name == "mine" and res is not None and res.source == "config"


# -- CFG-1 (purpose): removed keys -------------------------------------------

REMOVED_KEY = {"name": "rk", "images": {"pull_secrets": ["regcred"]}}


@pytest.mark.parametrize("purpose", [LoadPurpose.MUTATE, LoadPurpose.RUN])
def test_removed_key_refused_under_mutate(tmp_path, purpose):
    with pytest.raises(ConfigValidationError) as e:
        load_config(_write(tmp_path, REMOVED_KEY), purpose=purpose)
    msg = str(e.value)
    assert "'pull_secrets' was removed" in msg
    assert "No deployer ever applied it" in msg  # the key's fix text
    assert "Delete it from the config" in msg


@pytest.mark.parametrize("purpose", [LoadPurpose.TEARDOWN, LoadPurpose.READ, LoadPurpose.COMPARE])
def test_removed_key_loads_for_destroy(tmp_path, purpose):
    cfg = load_config(_write(tmp_path, REMOVED_KEY), purpose=purpose)
    assert not hasattr(cfg.images, "pull_secrets")
    notes = load_notes(cfg)
    assert [n.kind for n in notes] == ["removed"]
    assert "pull_secrets" in notes[0].text
    assert "refuse the config" in notes[0].text


def test_default_purpose_is_mutate(tmp_path):
    with pytest.raises(ConfigValidationError, match="'pull_secrets' was removed"):
        load_config(_write(tmp_path, REMOVED_KEY))
    with pytest.raises(ConfigNameRequired):
        load_config(_write(tmp_path, NAMELESS, "nameless.yaml"))


def test_allow_long_names_alone_means_teardown(tmp_path):
    cfg = load_config(_write(tmp_path, REMOVED_KEY), allow_long_names=True)
    assert load_notes(cfg)


def test_allow_long_names_with_mutate_keeps_mutate_refusals(tmp_path):
    # clean: the LB-153 length skip, but still refused like deploy.
    with pytest.raises(ConfigValidationError, match="'pull_secrets' was removed"):
        load_config(
            _write(tmp_path, REMOVED_KEY), purpose=LoadPurpose.MUTATE, allow_long_names=True
        )
    long_name = {"name": "ov-perf-c360-continuous-s10", "recipe": "hive-iceberg-spark-trino"}
    path = _write(tmp_path, long_name, "long.yaml")
    with pytest.raises(ConfigValidationError, match="at most 23"):
        load_config(path, purpose=LoadPurpose.MUTATE)
    cfg = load_config(path, purpose=LoadPurpose.MUTATE, allow_long_names=True)
    assert cfg.name == "ov-perf-c360-continuous-s10"


@pytest.mark.parametrize(
    ("purpose", "skips"),
    [
        (LoadPurpose.MUTATE, False),
        (LoadPurpose.RUN, False),
        (LoadPurpose.COMPARE, False),
        (LoadPurpose.TEARDOWN, True),
        (LoadPurpose.READ, True),
    ],
)
def test_name_length_check_per_purpose(tmp_path, purpose, skips):
    path = _write(
        tmp_path, {"name": "ov-perf-c360-continuous-s10", "recipe": "hive-iceberg-spark-trino"}
    )
    if skips:
        assert load_config(path, purpose=purpose).name == "ov-perf-c360-continuous-s10"
    else:
        with pytest.raises(ConfigValidationError, match="at most 23"):
            load_config(path, purpose=purpose)


# -- LoadNotes ---------------------------------------------------------------


def test_load_notes_printed_once_as_one_block(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr("lakebench.config.loader._printed_notes", set())
    data = {
        "name": "notes",
        "images": {"pull_secrets": ["x"]},
        "architecture": {"workload": {"datagen": {"scale": 1}}},
    }
    cfg_path = _write(tmp_path, data)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        load_config(cfg_path, purpose=LoadPurpose.READ)
        first = capsys.readouterr().err
        load_config(cfg_path, purpose=LoadPurpose.READ)
        second = capsys.readouterr().err
    assert first.count("Upgrade notes") == 1
    assert "pull_secrets" in first and "architecture.workload" in first
    assert "Upgrade notes" not in second


def test_load_notes_printed_per_config(tmp_path, capsys, monkeypatch):
    # compare loads A then B: B's notes print even when the text matches A's.
    monkeypatch.setattr("lakebench.config.loader._printed_notes", set())
    a = _write(tmp_path, REMOVED_KEY, "a.yaml")
    b = _write(tmp_path, {**REMOVED_KEY, "name": "rk2"}, "b.yaml")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        load_config(a, purpose=LoadPurpose.READ)
        load_config(b, purpose=LoadPurpose.READ)
    err = capsys.readouterr().err
    assert err.count("Upgrade notes") == 2 and err.count("pull_secrets") == 2


def test_load_notes_can_be_left_to_the_caller(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr("lakebench.config.loader._printed_notes", set())
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        cfg = load_config(
            _write(tmp_path, REMOVED_KEY), purpose=LoadPurpose.READ, print_notes=False
        )
    assert load_notes(cfg) and "Upgrade notes" not in capsys.readouterr().err


@pytest.mark.parametrize(
    ("purpose", "loads"),
    [
        (LoadPurpose.MUTATE, False),
        (LoadPurpose.RUN, False),
        (LoadPurpose.TEARDOWN, True),
        (LoadPurpose.READ, True),
        (LoadPurpose.COMPARE, True),
    ],
)
def test_old_file_size_follows_the_removed_key_rule(tmp_path, purpose, loads):
    # One rule: what refuses a removed key refuses an old file_size, and the
    # long-name switch has nothing to do with it.
    path = _write(tmp_path, {"name": "fs", "workload": {"datagen": {"file_size": "128mb"}}})
    if loads:
        with pytest.warns(DeprecationWarning, match="fixed at 64mb"):
            cfg = load_config(path, purpose=purpose)
        assert cfg.workload.datagen.file_size == "64mb"
    else:
        with pytest.raises(ConfigValidationError, match="fixed at 64mb"):
            load_config(path, purpose=purpose, allow_long_names=True)


def test_load_notes_still_raise_python_warnings(tmp_path):
    with pytest.warns(DeprecationWarning, match="pull_secrets"):
        load_config(_write(tmp_path, REMOVED_KEY), purpose=LoadPurpose.READ)


# -- Which purpose each verb loads with --------------------------------------

VERB_PURPOSES = [
    ("lakebench.cli._deploy", ["deploy"], LoadPurpose.MUTATE, False),
    ("lakebench.cli._generate", ["generate"], LoadPurpose.MUTATE, False),
    ("lakebench.cli._run", ["run"], LoadPurpose.RUN, False),
    ("lakebench.cli._query", ["benchmark"], LoadPurpose.MUTATE, False),
    ("lakebench.cli._query", ["query", "--example", "count"], LoadPurpose.MUTATE, False),
    ("lakebench.cli._clean", ["clean", "data"], LoadPurpose.MUTATE, True),
    ("lakebench.cli", ["stop"], LoadPurpose.TEARDOWN, False),
    ("lakebench.cli._destroy", ["destroy"], LoadPurpose.TEARDOWN, False),
    ("lakebench.cli._admin", ["admin", "doctor"], LoadPurpose.TEARDOWN, False),
    ("lakebench.cli", ["status"], LoadPurpose.READ, False),
    ("lakebench.cli", ["logs", "hive"], LoadPurpose.READ, False),
    ("lakebench.cli", ["info"], LoadPurpose.INSPECT, False),
    ("lakebench.cli", ["validate"], LoadPurpose.MUTATE, False),
    ("lakebench.cli._config", ["config", "storage"], LoadPurpose.INSPECT, False),
    ("lakebench.cli._config", ["config", "show"], LoadPurpose.INSPECT, False),
    ("lakebench.cli._config", ["config", "recommend"], LoadPurpose.INSPECT, False),
    ("lakebench.cli", ["report"], LoadPurpose.READ, False),
    ("lakebench.cli", ["results"], LoadPurpose.READ, False),
    ("lakebench.cli._compare", ["compare", "CFG"], LoadPurpose.MUTATE, False),
    (
        "lakebench.cli._financial",
        ["financial", "score", "--manifest", "s3://m", "--output", "s3://o"],
        LoadPurpose.MUTATE,
        False,
    ),
    ("lakebench.cli._admin", ["admin", "migrate-deployment", "ns1"], LoadPurpose.TEARDOWN, False),
    ("lakebench.cli._admin", ["admin", "reclaim-bucket"], LoadPurpose.TEARDOWN, False),
]


@pytest.mark.parametrize(
    ("module", "argv", "purpose", "long_names"),
    VERB_PURPOSES,
    ids=[" ".join(v[1]) for v in VERB_PURPOSES],
)
def test_cli_verbs_load_with_their_purpose(
    module, argv, purpose, long_names, tmp_path, monkeypatch
):
    monkeypatch.setenv("KUBECONFIG", "/nonexistent")
    monkeypatch.chdir(tmp_path)
    seen: list[dict] = []

    def spy(path, **kwargs):
        seen.append(kwargs)
        raise ConfigFileNotFoundError(str(path))

    mod = importlib.import_module(module)
    # Modules that import load_config inside the command read it from
    # lakebench.config at call time.
    target = mod if hasattr(mod, "load_config") else importlib.import_module("lakebench.config")
    monkeypatch.setattr(target, "load_config", spy)
    cfg_path = _write(tmp_path, {**NAMELESS, "name": "verb"})
    runner.invoke(app, [str(cfg_path) if a == "CFG" else a for a in argv] + [str(cfg_path)])
    assert seen, f"{argv} never loaded the config"
    assert seen[0].get("purpose") == purpose
    assert bool(seen[0].get("allow_long_names")) is long_names


def test_readonly_load_writes_nothing_with_legacy_state(tmp_path):
    (tmp_path / ".lakebench").mkdir()
    (tmp_path / ".lakebench" / "state.json").write_text(json.dumps({"name": "lb-20260101-000000"}))
    cfg_path = _write(tmp_path, NAMELESS)
    before = _listing(tmp_path)
    with pytest.raises(ConfigNameRequired):
        load_config(cfg_path, purpose=LoadPurpose.READ)
    assert load_config(cfg_path, purpose=LoadPurpose.COMPARE).name == "lb-20260101-000000"
    assert _listing(tmp_path) == before


# -- v1.6 directories with several nameless configs (SAF-2 c, check 1) --------


def _v16_dir(tmp_path: Path) -> Path:
    (tmp_path / ".lakebench").mkdir()
    shutil.copy(FIXTURES / "v16-state" / "state.json", tmp_path / ".lakebench" / "state.json")
    return _write(tmp_path, NAMELESS, "a.yaml")


def test_v16_state_teardown_refused_when_siblings_share_the_name(tmp_path):
    # v1.6 gave a.yaml and b.yaml one name. destroy b.yaml must not reach the
    # deployment a.yaml made (finding 1 of the CC-1 review).
    a = _v16_dir(tmp_path)
    b = _write(tmp_path, NAMELESS, "b.yaml")
    for path, other in ((a, "b.yaml"), (b, "a.yaml")):
        with pytest.raises(ConfigNameRequired) as e:
            load_config(path, purpose=LoadPurpose.TEARDOWN)
        msg = " ".join(str(e.value).split())
        assert f"nameless {other}" in msg
        assert "name: lb-20260915-101530" in msg
        assert e.value.siblings == [tmp_path / other]
    # status reads the same way (SPEC SAF-2: status refuses as destroy does).
    with pytest.raises(ConfigNameRequired):
        load_config(a, purpose=LoadPurpose.READ)
    # Naming the one that deployed it makes it loadable as itself, and the
    # other, now the only nameless config, still does not reach that
    # deployment (finding 1 of the CC-1 fix review).
    named = _write(tmp_path, {**NAMELESS, "name": "lb-20260915-101530"}, "a.yaml")
    assert load_config(named, purpose=LoadPurpose.TEARDOWN).name == "lb-20260915-101530"
    for purpose in (LoadPurpose.TEARDOWN, LoadPurpose.READ):
        with pytest.raises(ConfigNameRequired) as e:
            load_config(b, purpose=purpose)
        assert e.value.siblings == []


def test_sibling_scan_ignores_files_that_are_not_nameless_configs(tmp_path):
    import os

    from lakebench.config.deploy_state import other_nameless_configs

    a = _v16_dir(tmp_path)
    _write(tmp_path, {**NAMELESS, "name": "other"}, "named.yaml")
    _write(tmp_path, {"apiVersion": "v1", "kind": "ConfigMap"}, "manifest.yaml")
    (tmp_path / "broken.yml").write_text("{not: [yaml")
    # yaml.safe_load raises a plain ValueError on an impossible date.
    (tmp_path / "dated.yaml").write_text("release: 2026-02-30\n")
    (tmp_path / "huge.yaml").write_text(yaml.safe_dump(NAMELESS) + "#" * (1 << 20))
    (tmp_path / "notes.txt").write_text(yaml.safe_dump(NAMELESS))
    (tmp_path / "alias.yaml").symlink_to(a)
    os.link(a, tmp_path / "hard.yaml")
    (tmp_path / "sub").mkdir()
    _write(tmp_path / "sub", NAMELESS, "c.yaml")
    assert other_nameless_configs(a) == []
    # The links are the same file, not a second config.
    assert other_nameless_configs(tmp_path / "alias.yaml") == []
    assert other_nameless_configs(tmp_path / "hard.yaml") == []


def test_v16_state_mutate_refusal_does_not_offer_a_shared_name(tmp_path):
    # Offering "name: X" when another nameless config may have made X steers
    # the user into redeploying over that deployment (finding 3).
    a = _v16_dir(tmp_path)
    with pytest.raises(ConfigNameRequired) as alone:
        load_config(a, purpose=LoadPurpose.MUTATE)
    assert "if this config made deployment 'lb-20260915-101530'" in str(alone.value)
    _write(tmp_path, NAMELESS, "b.yaml")
    with pytest.raises(ConfigNameRequired) as shared:
        load_config(a, purpose=LoadPurpose.MUTATE)
    msg = " ".join(str(shared.value).split())
    assert "nameless b.yaml" in msg
    assert "new unique name, for example 'name: lb-" in msg
    assert "only to the one config that deployed" in msg


@pytest.mark.parametrize(
    "sibling",
    [
        {"secret_ref": "s3-creds", "mode": "batch"},  # flat keys only
        {**NAMELESS, "name": "${LB_TEST_NAME:-}"},
        {**NAMELESS, "name": "${LB_TEST_NAME}"},
    ],
    ids=["flat-only", "env-name-default", "env-name"],
)
def test_sibling_scan_counts_every_nameless_form(tmp_path, monkeypatch, sibling):
    # An env-reference name may have resolved to nothing when v1.6 deployed,
    # whatever the environment says now, so it counts as nameless.
    from lakebench.config.deploy_state import other_nameless_configs

    monkeypatch.setenv("LB_TEST_NAME", "other")
    a = _v16_dir(tmp_path)
    _write(tmp_path, sibling, "b.yaml")
    assert other_nameless_configs(a) == [tmp_path / "b.yaml"]
