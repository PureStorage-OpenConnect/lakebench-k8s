"""CLI-1 (CC-8): one exit-code enum, a top-level handler, a generated table.

The expected code values below are copied from the target UX design table
(TUD 4.3), not from ``lakebench.exit_codes``, so a renumbering in the module
fails here.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest
import typer
from typer.testing import CliRunner

import lakebench
from lakebench import exit_codes
from lakebench.cli import _exit as cli_exit
from lakebench.cli import app
from lakebench.exit_codes import (
    LEGACY_CODES,
    PATHS,
    ExitCode,
    Incomplete,
    LakebenchError,
    NotConfirmed,
    PrerequisiteError,
    SafetyRefusal,
    UsageError,
)

ROOT = Path(__file__).resolve().parents[1]
SRC = Path(lakebench.__file__).resolve().parents[1]

# TUD 4.3, by hand.
TUD_CODES = {
    "OK": 0,
    "FAILED": 1,
    "USAGE": 2,
    "REFUSED": 3,
    "PREREQUISITE": 4,
    "NOT_CONFIRMED": 5,
    "INCOMPLETE": 6,
    "COMPARE_NOT_COMPARABLE": 10,
    "COMPARE_NOT_ESTABLISHED": 11,
    "COMPARE_NOT_LIKE_FOR_LIKE": 12,
    "COMPARE_CONFOUNDED": 13,
    "REQUIREMENT_UNMET": 14,
    "INTERRUPTED": 130,
}


def _runner() -> CliRunner:
    return CliRunner()


def _stderr(result) -> str:
    try:
        return result.stderr
    except ValueError:  # Click < 8.2 without mix_stderr=False
        return result.output


# -- the enum and the module ---------------------------------------------------


def test_exit_code_values_match_tud():
    assert {m.name: int(m) for m in ExitCode} == TUD_CODES


def test_exit_module_imports_no_typer():
    """The harness imports ExitCode without loading the CLI (ch07 C16 c)."""
    probe = (
        "import sys, lakebench.exit_codes as m; "
        "heavy = [n for n in ('typer', 'click', 'rich', 'kubernetes', 'lakebench.cli') "
        "if n in sys.modules]; "
        "print(','.join(heavy)); "
        "sys.exit(1 if heavy else 0)"
    )
    env = dict(os.environ, PYTHONPATH=str(SRC))
    proc = subprocess.run(
        [sys.executable, "-c", probe], env=env, capture_output=True, text=True, check=False
    )
    assert proc.returncode == 0, f"loaded: {proc.stdout.strip()} {proc.stderr[-500:]}"


def test_cli_exit_reexports_the_same_objects():
    for name in ("ExitCode", "LakebenchError", "UsageError", "SafetyRefusal", "PATHS"):
        assert getattr(cli_exit, name) is getattr(exit_codes, name)


@pytest.mark.parametrize(
    ("cls", "code"),
    [
        (LakebenchError, 1),
        (UsageError, 2),
        (SafetyRefusal, 3),
        (PrerequisiteError, 4),
        (NotConfirmed, 5),
        (Incomplete, 6),
    ],
)
def test_error_class_codes(cls, code):
    assert int(cls("x").code) == code


def test_error_lines_leave_out_unset_fields():
    err = SafetyRefusal("what", next="do this")
    assert err.lines() == [("ERROR", "what"), ("Next", "do this")]


# -- PATHS ---------------------------------------------------------------------


def test_path_names_unique():
    names = [p.name for p in PATHS]
    assert len(names) == len(set(names))


def test_every_exit_code_has_a_path():
    """The test fails if an ExitCode member has no named producer path."""
    missing = [m.name for m in ExitCode if not any(p.code == m for p in PATHS)]
    assert not missing


def test_every_exit_code_has_a_meaning():
    assert set(exit_codes.MEANINGS) == set(ExitCode)


def test_owned_paths_name_a_work_item():
    bad = [p.name for p in PATHS if p.owner and not re.fullmatch(r"[A-Z]{2}-\d+[a-z]?", p.owner)]
    assert not bad


def test_legacy_codes_match_the_unconverted_constants():
    """LEGACY_CODES documents exactly the v1.6 constants still in the tree."""
    from lakebench.cli import _destroy, _helpers

    present = {
        getattr(mod, name)
        for mod, name in (
            (_helpers, "EXIT_DECLINED"),
            (_helpers, "EXIT_DATAGEN_TIMEOUT"),
            (_destroy, "EXIT_NAMESPACE_STILL_TERMINATING"),
        )
        if hasattr(mod, name)
    }
    assert set(LEGACY_CODES) == present


# -- the handler, on a throwaway app that uses the same group class ----------


def _probe_app(exc_factory: Callable[[], BaseException]) -> typer.Typer:
    probe = typer.Typer(cls=cli_exit.LakebenchGroup, pretty_exceptions_enable=False)

    @probe.command()
    def boom() -> None:
        raise exc_factory()

    @probe.command()
    def other() -> None:  # a second command keeps `boom` a subcommand
        pass

    return probe


def _config_validation_error():
    from lakebench.config import ConfigValidationError

    return ConfigValidationError(
        "2 errors", errors=[{"loc": ("name",), "msg": "required"}, {"loc": ("a", 0), "msg": "bad"}]
    )


def _config_error():
    from lakebench.config import ConfigError

    return ConfigError("cannot parse c.yaml")


def _k8s_error():
    from lakebench.k8s import K8sConnectionError

    return K8sConnectionError("connection refused")


def _kubeconfig_error():
    from kubernetes.config import ConfigException

    return ConfigException("Invalid kube-config file. No configuration found.")


@pytest.mark.parametrize(
    ("factory", "code"),
    [
        (lambda: SafetyRefusal("refused"), 3),
        (lambda: PrerequisiteError("missing"), 4),
        (lambda: Incomplete("still going"), 6),
        (lambda: LakebenchError("custom", code=ExitCode.COMPARE_CONFOUNDED), 13),
        (_config_validation_error, 2),
        (_config_error, 2),
        (_k8s_error, 4),
        (_kubeconfig_error, 4),
        (lambda: typer.Abort(), 5),
        (lambda: EOFError(), 5),
        (lambda: KeyboardInterrupt(), 130),
        (lambda: RuntimeError("unexpected"), 1),
        (lambda: typer.Exit(7), 7),
        (lambda: SystemExit(9), 9),
    ],
    ids=[
        "safety",
        "prerequisite",
        "incomplete",
        "explicit-code",
        "config-validation",
        "config-error",
        "k8s-connection",
        "kubeconfig",
        "abort",
        "eof",
        "keyboard-interrupt",
        "unhandled",
        "typer-exit-passes-through",
        "system-exit-passes-through",
    ],
)
def test_handler_maps_exception(factory, code):
    result = _runner().invoke(_probe_app(factory), ["boom"])
    assert result.exit_code == code, result.output


def test_handler_leaves_click_usage_errors_to_click():
    result = _runner().invoke(_probe_app(lambda: typer.BadParameter("nope")), ["boom"])
    assert result.exit_code == 2
    assert "nope" in result.output


def test_unhandled_error_is_one_line_without_traceback(monkeypatch):
    monkeypatch.delenv(cli_exit.DEBUG_ENV, raising=False)
    text = "s3a://b/[x]/y under [/tmp] and [main]\nsecond line"
    result = _runner().invoke(_probe_app(lambda: RuntimeError(text)), ["boom"])
    err = _stderr(result)
    assert result.exit_code == 1
    assert "Traceback" not in result.output
    error_lines = [ln for ln in err.splitlines() if ln.startswith("ERROR")]
    assert error_lines == ["ERROR  RuntimeError: s3a://b/[x]/y under [/tmp] and [main]"]
    assert "second line" not in err
    assert "LAKEBENCH_DEBUG=1" in err


def test_debug_env_prints_the_traceback(monkeypatch):
    monkeypatch.setenv(cli_exit.DEBUG_ENV, "1")
    result = _runner().invoke(_probe_app(lambda: RuntimeError("deep")), ["boom"])
    assert result.exit_code == 1
    assert "Traceback" in _stderr(result)


def test_typed_error_prints_its_shape():
    err = SafetyRefusal(
        "Namespace lb-a belongs to another deployment.", why="stamp", where="ns lb-a"
    )
    result = _runner().invoke(_probe_app(lambda: err), ["boom"])
    lines = _stderr(result).splitlines()
    assert lines[0] == "ERROR  Namespace lb-a belongs to another deployment."
    assert lines[1] == "Why    stamp"
    assert lines[2] == "Where  ns lb-a"


def test_root_app_uses_the_handler():
    cmd = typer.main.get_command(app)
    assert isinstance(cmd, cli_exit.LakebenchGroup)


# -- named paths on the real CLI -----------------------------------------------


def _scenario_version_ok(monkeypatch, tmp_path):
    return _runner().invoke(app, ["version"])


def _scenario_click_usage(monkeypatch, tmp_path):
    return _runner().invoke(app, ["deploy", "--no-such-flag"])


def _scenario_unhandled_exception(monkeypatch, tmp_path):
    import lakebench.cli as cli

    def explode(*_a, **_k):
        raise RuntimeError("unexpected [/tmp] failure")

    monkeypatch.setattr(cli, "load_config", explode)
    (tmp_path / "c.yaml").write_text("name: x\n")
    return _runner().invoke(app, ["status", str(tmp_path / "c.yaml")])


def _scenario_confirm_non_tty(monkeypatch, tmp_path):
    import lakebench.cli._deploy as deploy_mod

    runner = _runner()
    init = runner.invoke(app, ["init", "--output", str(tmp_path / "c.yaml")])
    assert init.exit_code == 0, init.output
    # Stop before any cluster call: the prompt is the first thing left.
    monkeypatch.setattr(deploy_mod, "_preflight_check", lambda cfg: None)
    monkeypatch.setattr(deploy_mod, "check_datagen_scale", lambda cfg: None)
    return runner.invoke(app, ["deploy", str(tmp_path / "c.yaml")], input="")


def _scenario_sigint(monkeypatch, tmp_path):
    import lakebench.cli as cli

    def interrupt(*_a, **_k):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "load_config", interrupt)
    (tmp_path / "c.yaml").write_text("name: x\n")
    return _runner().invoke(app, ["status", str(tmp_path / "c.yaml")])


def _nameless_destroy(monkeypatch, tmp_path, setup, argv_extra=()):
    """`destroy --force` on a nameless config against a fake namespace (SAF-2)."""
    import lakebench.cli._nameless as nameless
    from tests import test_saf2_deploy_state as t

    core = t.FakeCore()
    monkeypatch.setattr(nameless, "_core_v1_factory", lambda cfg: lambda: core)
    monkeypatch.setattr(nameless, "_bucket_owned_factory", lambda cfg: lambda b: False)
    cfg = t._nameless(tmp_path)
    setup(t, core, cfg)
    return _runner().invoke(app, ["destroy", str(cfg), "--force", *argv_extra])


def _scenario_nameless_ambiguous(monkeypatch, tmp_path):
    def setup(t, core, cfg):
        t._nameless(tmp_path, "b.yaml")
        t._legacy_state(tmp_path)

    return _nameless_destroy(monkeypatch, tmp_path, setup)


def _scenario_nameless_nonce_mismatch(monkeypatch, tmp_path):
    def setup(t, core, cfg):
        t._v16_namespace(core, nonce="foreign")
        t._v17_state(cfg, [("mine", "confirmed")])

    return _nameless_destroy(monkeypatch, tmp_path, setup, ["--name", "lb-x"])


def _scenario_nameless_copied_dir(monkeypatch, tmp_path):
    def setup(t, core, cfg):
        t._v16_namespace(core, nonce="n1")
        t._v17_state(cfg, [("n1", "confirmed")], config_dir="/elsewhere")

    return _nameless_destroy(monkeypatch, tmp_path, setup, ["--name", "lb-x"])


def _scenario_nameless_moved(monkeypatch, tmp_path):
    def setup(t, core, cfg):
        t._v16_namespace(core, nonce="n1")
        t._v17_state(cfg, [("n1", "confirmed")], moved_to="/new/home")

    return _nameless_destroy(monkeypatch, tmp_path, setup, ["--name", "lb-x"])


def _scenario_nameless_name_required(monkeypatch, tmp_path):
    def setup(t, core, cfg):
        t._legacy_state(tmp_path)
        t._v16_namespace(core)

    return _nameless_destroy(monkeypatch, tmp_path, setup)


def _scenario_nameless_stamp_mismatch(monkeypatch, tmp_path):
    def setup(t, core, cfg):
        t._legacy_state(tmp_path)
        core.add("lb-x", **{t.ANNOTATION_DEPLOYMENT_NAME: "lb-other"})

    return _nameless_destroy(monkeypatch, tmp_path, setup, ["--name", "lb-x"])


def _scenario_nameless_v17_state_elsewhere(monkeypatch, tmp_path):
    def setup(t, core, cfg):
        t._legacy_state(tmp_path)
        t._v16_namespace(core, **{t.ANNOTATION_STATE_SCHEMA: "lb-state/1"})

    return _nameless_destroy(monkeypatch, tmp_path, setup, ["--name", "lb-x"])


def _scenario_nameless_namespace_missing(monkeypatch, tmp_path):
    def setup(t, core, cfg):
        t._legacy_state(tmp_path)

    return _nameless_destroy(monkeypatch, tmp_path, setup, ["--name", "lb-x"])


def _scenario_nameless_namespace_unreadable(monkeypatch, tmp_path):
    def setup(t, core, cfg):
        t._legacy_state(tmp_path)
        core.fail_reads = True

    return _nameless_destroy(monkeypatch, tmp_path, setup, ["--name", "lb-x"])


def _scenario_deploy_state_unrecordable(monkeypatch, tmp_path):
    import lakebench.cli._deploy as deploy_mod
    from lakebench.config import deploy_state as ds
    from tests import test_saf2_deploy_state as t

    core = t.FakeCore()
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **k: core)
    monkeypatch.setattr(deploy_mod, "_preflight_check", lambda cfg: None)
    monkeypatch.setattr(deploy_mod, "check_datagen_scale", lambda cfg: None)

    class Engine:
        def __init__(self, cfg, dry_run=False, **kw):
            self.results = []

        def deploy_all(self, **kw):  # pragma: no cover -- must not be reached
            raise AssertionError("deploy_all after a failed state write")

    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", Engine)

    def no_write(path, state):
        raise OSError("read-only file system")

    monkeypatch.setattr(ds, "write_state", no_write)
    cfg = t._named(tmp_path)
    return _runner().invoke(app, ["deploy", str(cfg), "--yes"])


SCENARIOS = {
    "deploy.state_unrecordable": _scenario_deploy_state_unrecordable,
    "nameless.ambiguous": _scenario_nameless_ambiguous,
    "nameless.nonce_mismatch": _scenario_nameless_nonce_mismatch,
    "nameless.copied_dir": _scenario_nameless_copied_dir,
    "nameless.moved": _scenario_nameless_moved,
    "nameless.name_required": _scenario_nameless_name_required,
    "nameless.stamp_mismatch": _scenario_nameless_stamp_mismatch,
    "nameless.v17_state_elsewhere": _scenario_nameless_v17_state_elsewhere,
    "nameless.namespace_missing": _scenario_nameless_namespace_missing,
    "nameless.namespace_unreadable": _scenario_nameless_namespace_unreadable,
    "version.ok": _scenario_version_ok,
    "click.usage": _scenario_click_usage,
    "unhandled_exception": _scenario_unhandled_exception,
    "confirm.non_tty": _scenario_confirm_non_tty,
    "sigint": _scenario_sigint,
}

# The line each path must print on stderr, where it prints one.
EXPECTED_STDERR = {
    "unhandled_exception": "ERROR  RuntimeError: unexpected [/tmp] failure",
    "confirm.non_tty": "ERROR  Not confirmed",
    "sigint": "ERROR  Interrupted.",
}


def test_scenarios_cover_exactly_the_live_paths():
    """A live path needs a scenario; a path with a scenario must not keep an owner."""
    assert set(SCENARIOS) == {p.name for p in PATHS if p.live}


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_exit_code_paths(name, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)  # journal and state files land here
    monkeypatch.setenv("KUBECONFIG", "/nonexistent/kubeconfig")
    result = SCENARIOS[name](monkeypatch, tmp_path)
    assert result.exit_code == exit_codes.path_code(name), result.output
    assert "Traceback" not in result.output
    if name in EXPECTED_STDERR:
        assert EXPECTED_STDERR[name] in _stderr(result), result.output


# -- generated table -----------------------------------------------------------


def test_exit_table_drift():
    doc = ROOT / "docs" / "exit-codes.md"
    assert doc.read_text() == exit_codes.render_markdown(), (
        "docs/exit-codes.md is stale: run python scripts/gen_exit_codes.py"
    )


def test_exit_table_lists_only_live_paths():
    text = exit_codes.render_markdown()
    for p in PATHS:
        assert (f"| `{p.name}` |" in text) == p.live, p.name


def test_gen_script_check_mode():
    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "gen_exit_codes.py"), "--check"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
