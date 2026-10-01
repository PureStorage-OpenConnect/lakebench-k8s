"""SAF-2 (a, e) and CFG-1 through the CLI (CC-1).

These tests use only names that existed before LoadPurpose, so each one runs,
and fails, against the v1.6 loader: a nameless run went ahead under a
time-based name written to .lakebench/state.json, validate opened a journal,
report created lakebench-output/runs, and deploy dropped removed keys.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from lakebench.cli import app

runner = CliRunner()

FIXTURES = Path(__file__).parent / "fixtures"
NAMELESS = {"endpoint": "http://127.0.0.1:9", "access_key": "k", "secret_key": "s", "scale": 1}


def _write(directory: Path, data: dict, filename: str = "lakebench.yaml") -> Path:
    path = directory / filename
    path.write_text(yaml.safe_dump(data))
    return path


def _listing(root: Path) -> list[str]:
    return sorted(str(p.relative_to(root)) for p in root.rglob("*"))


def test_nameless_run_refused_names_suggestion(tmp_path, monkeypatch):
    # The CLI path: run refuses before any cluster call, names the name it
    # would have used, and writes nothing (v1.6 wrote .lakebench/state.json
    # here and went on to deploy under a time-based name).
    monkeypatch.setenv("KUBECONFIG", "/nonexistent")
    monkeypatch.chdir(tmp_path)
    cfg_path = _write(tmp_path, NAMELESS)
    result = runner.invoke(app, ["run", str(cfg_path), "--yes"])
    assert result.exit_code != 0
    out = " ".join(result.output.split())
    assert "cannot change data" in out
    assert "name: lb-" in out
    assert _listing(tmp_path) == ["lakebench.yaml"]


# -- SAF-2 (e): read-only commands create no files ---------------------------

READ_VERBS = [
    ["status"],
    ["logs", "hive"],
    ["report"],
    ["results"],
    ["info"],
    ["validate"],
    ["config", "show"],
    ["config", "validate"],
    ["config", "recommend"],
]


@pytest.mark.parametrize("named", [False, True], ids=["nameless", "named"])
@pytest.mark.parametrize("argv", READ_VERBS, ids=[" ".join(v) for v in READ_VERBS])
def test_readonly_commands_create_no_files(argv, named, tmp_path, monkeypatch):
    # The empty tmp_path is both the working directory and the config's
    # directory. v1.6 wrote .lakebench/state.json here on every nameless
    # load, and validate opened a journal under lakebench-output/.
    monkeypatch.setenv("KUBECONFIG", "/nonexistent")
    monkeypatch.chdir(tmp_path)
    data = {**NAMELESS, "name": "ro"} if named else dict(NAMELESS)
    cfg_path = _write(tmp_path, data)
    before = _listing(tmp_path)
    result = runner.invoke(app, [*argv, str(cfg_path)])
    assert "Usage:" not in result.output  # reached the command, not an argument error
    assert _listing(tmp_path) == before


@pytest.mark.parametrize("argv", [["deploy", "--yes"], ["run", "--yes"], ["generate", "--yes"]])
def test_removed_key_refused_by_commands_that_change_data(argv, tmp_path, monkeypatch):
    # CFG-1: the v1.4 user config carries four removed keys. deploy, run and
    # generate refuse it before any cluster call and give each key's fix
    # text; v1.6 dropped them with a warning and went on to the cluster.
    monkeypatch.setenv("KUBECONFIG", "/nonexistent")
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, [argv[0], str(FIXTURES / "v14user.yaml"), *argv[1:]])
    assert result.exit_code != 0
    out = " ".join(result.output.split())
    for key in ("pull_secrets", "create_storage_class", "channels", "quality_distribution"):
        assert f"'{key}' was removed" in out, key


REMOVED_KEY = {"name": "rk", "images": {"pull_secrets": ["regcred"]}}


def test_reproduce_refuses_before_its_pre_run_destroy(tmp_path, monkeypatch):
    # reproduce destroys first, through destroy (a TEARDOWN load that accepts
    # a removed key). The MUTATE refusal must come before that call.
    import lakebench.cli._destroy as destroy_mod
    from lakebench.cli._reproduce import ReproduceError, _run_pipeline

    calls: list = []
    monkeypatch.setattr(destroy_mod, "destroy", lambda **kw: calls.append(kw))
    monkeypatch.chdir(tmp_path)
    for data in (NAMELESS, REMOVED_KEY):
        cfg_path = _write(tmp_path, data)
        with pytest.raises(ReproduceError):
            _run_pipeline(cfg_path, None, keep=False)
    assert calls == []


def test_destroy_refuses_a_v16_name_two_nameless_configs_share(tmp_path, monkeypatch):
    # v1.6 resolved a.yaml and b.yaml to the one name in state.json, so
    # destroy b.yaml went for the namespace a.yaml deployed. It must stop
    # before any cluster call.
    monkeypatch.setenv("KUBECONFIG", "/nonexistent")
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".lakebench").mkdir()
    (tmp_path / ".lakebench" / "state.json").write_text('{"name": "lb-20260915-101530"}')
    _write(tmp_path, NAMELESS, "a.yaml")
    b = _write(tmp_path, NAMELESS, "b.yaml")
    result = runner.invoke(app, ["destroy", str(b), "--force"])
    assert result.exit_code != 0
    out = " ".join(result.output.split())
    assert "nameless a.yaml" in out
    assert "name: lb-20260915-101530" in out
