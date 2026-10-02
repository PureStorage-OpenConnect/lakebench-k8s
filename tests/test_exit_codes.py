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


# The v1.7 work item that makes each planned path live. Kept here, not in
# the shipped module, which does not cite plan ids.
PLANNED_BY = {
    "admin.version_change_in_use": "SD-10",
    "admin.version_change_needs_flag": "SD-10",
    "alias.refused": "CC-28",
    "capacity.shortfall": "CC-24",
    "capacity.unknown": "CC-24",
    "compare.confounded": "ER-11",
    "compare.equal_names": "CC-3",
    "compare.like_for_like": "ER-11",
    "compare.not_comparable": "ER-11",
    "compare.not_established": "ER-11",
    "compare.not_like_for_like": "ER-11",
    "destroy.unverified_cluster": "SD-18a",
    "financial.reproduce.mismatch": "AM-18",
    "financial.reproduce.snapshot_gone": "AM-18",
    "logs.no_pod": "CC-27",
    "plan.missing_storage_class": "CC-23",
    "plan.ok": "CC-23",
    "repeat.no_verified_corpus": "CC-30",
    "reproduce.verify_out_of_band": "ER-13",
    "run.args": "CC-6",
    "run.deps_missing": "SD-5c",
    "run.interrupted": "CD-16",
    "run.namespace_gone": "CD-17",
    "run.protected_corpus": "AM-22",
    "series.corpus_changed": "CC-30",
    "status.drift": "CC-27",
    "status.namespace_missing": "CC-27",
    "status.ok": "CC-27",
    "stop.api_error": "CC-27",
}


def test_planned_paths_name_a_work_item():
    assert {p.name for p in PATHS if p.planned} == set(PLANNED_BY)
    bad = [n for n, wi in PLANNED_BY.items() if not re.fullmatch(r"[A-Z]{2}-\d+[a-z]?", wi)]
    assert not bad, bad


def test_legacy_codes_are_gone():
    """CC-9 converted every command: no 1.6 constant and no transition table."""
    from lakebench.cli import _destroy, _helpers

    assert LEGACY_CODES == {}
    for mod, name in (
        (_helpers, "EXIT_DECLINED"),
        (_helpers, "EXIT_DATAGEN_TIMEOUT"),
        (_destroy, "EXIT_NAMESPACE_STILL_TERMINATING"),
    ):
        assert not hasattr(mod, name), name


def _is_exit_call(func) -> bool:
    """An exit call: ``*.Exit``/``Exit``/``SystemExit``, ``*.exit``/``exit``.

    Covers ``typer.Exit``, ``from typer import Exit``, ``click.exceptions.Exit``,
    ``sys.exit``, ``ctx.exit`` and the bare builtin.
    """
    import ast

    if isinstance(func, ast.Attribute):
        return func.attr in ("Exit", "exit")
    return isinstance(func, ast.Name) and func.id in ("Exit", "SystemExit", "exit")


def _is_literal_code(node) -> bool:
    """A code written as a literal other than 0: ``1``, ``-1``, ``int(2)``,
    ``1 if x else 2``, or a message string (Click exits 1 with it)."""
    import ast

    if isinstance(node, ast.Constant):
        return node.value not in (0, None, False)
    if isinstance(node, ast.UnaryOp):
        return _is_literal_code(node.operand)
    if isinstance(node, ast.IfExp):
        return _is_literal_code(node.body) or _is_literal_code(node.orelse)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "int":
        return any(_is_literal_code(a) for a in node.args)
    return False


_CODE_NAME = re.compile(r"(^|_)(exit|exit_code|code|rc)$")


def literal_exit_sites(root: Path) -> list[str]:
    """Exit codes under *root* written as a literal (CLI-1).

    An exit call given a literal, or a name such as ``exit_code`` assigned a
    non-zero literal (the value then reaches an exit through the variable).
    """
    import ast

    sites = []
    for path in sorted(root.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Call) and _is_exit_call(node.func):
                args = list(node.args) + [kw.value for kw in node.keywords if kw.arg == "code"]
                if args and _is_literal_code(args[0]):
                    sites.append(f"{path.name}:{node.lineno}: {ast.unparse(node)}")
            elif isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None:
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                named = [t for t in targets if isinstance(t, ast.Name) and _CODE_NAME.search(t.id)]
                if named and _is_literal_code(node.value):
                    sites.append(f"{path.name}:{node.lineno}: {ast.unparse(node)}")
    return sites


def test_no_literal_exit_codes():
    """Every exit under cli/ names its code (``ExitCode.X``) or a typed error."""
    sites = literal_exit_sites(SRC / "lakebench" / "cli")
    assert not sites, "use ExitCode.<NAME> or a typed error:\n  " + "\n  ".join(sites)


_LINT_CASES = [
    ("typer.Exit(1)", True),
    ("typer.Exit(code=2)", True),
    ("SystemExit(3)", True),
    ("sys.exit(4)", True),
    ("Exit(1)", True),  # from typer import Exit
    ("click.exceptions.Exit(1)", True),
    ("ctx.exit(1)", True),
    ("exit(1)", True),
    ("typer.Exit(-1)", True),
    ("typer.Exit(int(1))", True),
    ("typer.Exit(1 if x else 2)", True),
    ('typer.Exit("failed")', True),
    ("exit_code = 2", True),
    ("_pipeline_exit_code: int = 1", True),
    ("typer.Exit(0)", False),
    ("typer.Exit()", False),
    ("typer.Exit(ExitCode.USAGE)", False),
    ("typer.Exit(ExitCode.USAGE if x else ExitCode.FAILED)", False),
    ("exit_code = ExitCode.FAILED", False),
    ("outcome = 2", False),
]


@pytest.mark.parametrize(("source", "flagged"), _LINT_CASES)
def test_literal_exit_lint_sees_each_spelling(tmp_path, source, flagged):
    (tmp_path / "m.py").write_text(source + "\n")
    assert bool(literal_exit_sites(tmp_path)) is flagged


# -- refusals reported as failed step results ---------------------------------
# The producers' own tests (test_destroy_namespace_wait, test_destroy_bucket_delete,
# test_destroy_legacy_refuse, test_destroy_unwatches_namespace, test_deploy) check
# that each real refusal carries the flag; these check the rule.


def _result(component: str, status: str, message: str = "", **details):
    from lakebench.deploy.engine import DeploymentResult, DeploymentStatus

    return DeploymentResult(component, DeploymentStatus(status), message, details=details)


_REFUSED = {exit_codes.REFUSAL_DETAIL: "deploy.identity_foreign"}
_FOLLOWS = {exit_codes.FOLLOWS_REFUSAL_DETAIL: True}


def test_refusal_results_exit_refused():
    results = [_result("postgres", "success"), _result("s3-buckets", "failed", **_REFUSED)]
    assert cli_exit.refused_result_code(results) == ExitCode.REFUSED


def test_a_real_failure_is_never_hidden_by_a_refusal():
    refused = _result("s3-buckets", "failed", **_REFUSED)
    kept = _result("namespace", "failed", **_FOLLOWS)
    broken = _result("trino", "failed", "helm uninstall failed")
    assert cli_exit.refused_result_code([refused, kept]) == ExitCode.REFUSED
    assert cli_exit.refused_result_code([refused, broken]) is None
    assert cli_exit.refused_result_code([kept]) is None  # a consequence alone is a failure


def test_refusal_text_without_the_flag_is_a_failure():
    """Classification reads the flag, never the wording (CLI-2 rewrites text)."""
    text_only = _result("namespace", "failed", "Destroy NOT completed: a redeploy")
    assert cli_exit.refused_result_code([text_only]) is None
    ok = _result("namespace", "success", **_REFUSED)  # a flag on a step that did not fail
    assert cli_exit.refused_result_code([ok]) is None


def test_cluster_lock_held_maps_to_refused():
    from lakebench.deploy.cluster_lock import ClusterLockHeld

    exc = ClusterLockHeld("h@u@s", "2026-10-01T00:00:00Z", 300, "2026-10-01T00:05:00Z")
    err = cli_exit.error_for(exc)
    assert err is not None and err.code == ExitCode.REFUSED and err.path == "lease.held"


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
        (lambda: EOFError(), 1),
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
        "bare-eof-is-unclassified",
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
    assert app.pretty_exceptions_enable is False


def test_broken_pipe_passes_through_quietly():
    result = _runner().invoke(_probe_app(lambda: BrokenPipeError(32, "Broken pipe")), ["boom"])
    assert result.exit_code == 1  # Typer's own EPIPE branch, no message
    assert "ERROR" not in result.output


class _GoneStream:
    """A stderr that cannot be written (EIO). Not EPIPE: Rich answers that by
    dup2-ing /dev/null over fd 1, which would hit the test process."""

    def write(self, _text):
        raise OSError(5, "Input/output error")

    def flush(self):
        pass


def test_exit_code_survives_a_closed_stderr(monkeypatch):
    from rich.console import Console

    from lakebench.cli import _helpers

    # A throwaway console: Rich keeps buffer state after a failed write.
    monkeypatch.setattr(_helpers, "err_console", Console(stderr=True))

    kept = []

    def closed_then_refuse():
        # Keep CliRunner's wrapper alive: collecting it would close its buffer.
        kept.append(sys.stderr)
        sys.stderr = _GoneStream()  # CliRunner restores its own streams afterwards
        return SafetyRefusal("refused")

    result = _runner().invoke(_probe_app(closed_then_refuse), ["boom"])
    assert result.exit_code == 3


def test_exit_code_survives_rich_broken_pipe_exit(monkeypatch):
    from lakebench.cli import _helpers

    def rich_broken_pipe(_err):
        raise SystemExit(1)  # what Console.on_broken_pipe does

    monkeypatch.setattr(_helpers, "emit_error", rich_broken_pipe)
    result = _runner().invoke(_probe_app(lambda: SafetyRefusal("refused")), ["boom"])
    assert result.exit_code == 3


def test_config_validation_details_go_on_the_why_line():
    result = _runner().invoke(_probe_app(_config_validation_error), ["boom"])
    assert "Why    name: required; a.0: bad" in _stderr(result)


def test_prompt_abort_starts_on_a_new_line():
    result = _runner().invoke(_probe_app(lambda: typer.Abort()), ["boom"])
    assert _stderr(result).startswith("\nERROR  Not confirmed")


def test_v16_codes_differ_from_the_new_code():
    same = [p.name for p in PATHS if p.v16_code is not None and p.v16_code == int(p.code)]
    assert not same


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


def _scenario_financial_k8s_unreachable(monkeypatch, tmp_path):
    from types import SimpleNamespace

    import lakebench.cli._financial as fin
    import lakebench.k8s as k8s_mod
    from lakebench.k8s import K8sConnectionError

    cfg = SimpleNamespace(
        platform=SimpleNamespace(kubernetes=SimpleNamespace(context=None)),
        get_namespace=lambda: "ns-x",
    )
    monkeypatch.setattr(fin, "_load_config", lambda path: cfg)

    def unreachable(**_k):
        raise K8sConnectionError("connection refused")

    monkeypatch.setattr(k8s_mod, "get_k8s_client", unreachable)
    (tmp_path / "c.yaml").write_text("name: x\n")
    argv = ["financial", "score", str(tmp_path / "c.yaml"), "--manifest", "s3://m"]
    return _runner().invoke(app, [*argv, "--output", "s3://o"])


def _scenario_sigint(monkeypatch, tmp_path):
    import lakebench.cli as cli

    def interrupt(*_a, **_k):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "load_config", interrupt)
    (tmp_path / "c.yaml").write_text("name: x\n")
    return _runner().invoke(app, ["status", str(tmp_path / "c.yaml")])


def _scenario_config_upgrade_refused(monkeypatch, tmp_path):
    (tmp_path / "c.yaml").write_text("name: x\n")
    return _runner().invoke(app, ["config", "upgrade", str(tmp_path / "c.yaml")])


def _init_config(tmp_path) -> Path:
    init = _runner().invoke(app, ["init", "--output", str(tmp_path / "c.yaml")])
    assert init.exit_code == 0, init.output
    return tmp_path / "c.yaml"


def _scenario_config_validation(monkeypatch, tmp_path):
    (tmp_path / "c.yaml").write_text("name: x\nplatform: 5\n")
    return _runner().invoke(app, ["info", str(tmp_path / "c.yaml")])


def _scenario_config_name_required(monkeypatch, tmp_path):
    (tmp_path / "c.yaml").write_text("recipe: hive-iceberg-spark-trino\n")
    return _runner().invoke(app, ["deploy", str(tmp_path / "c.yaml"), "--yes"])


def _scenario_config_unsupported(monkeypatch, tmp_path):
    monkeypatch.setattr("lakebench.cli._run._run_local_mode", lambda *a, **k: None)
    cfg = _init_config(tmp_path)
    return _runner().invoke(app, ["run", str(cfg), "--local", "--continuous"])


def _scenario_cli_bad_argument(monkeypatch, tmp_path):
    return _runner().invoke(app, ["config", "recipes", "no-such-recipe"])


def _scenario_k8s_unreachable(monkeypatch, tmp_path):
    import lakebench.cli as cli
    from lakebench.k8s import K8sConnectionError

    def unreachable(**_k):
        raise K8sConnectionError("connection refused")

    monkeypatch.setattr(cli, "get_k8s_client", unreachable)
    return _runner().invoke(app, ["status", str(_init_config(tmp_path))])


def _destroy_with(monkeypatch, results):
    from unittest.mock import MagicMock

    engine = MagicMock()
    engine.destroy_all.return_value = results
    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", lambda *a, **k: engine)
    fixture = ROOT / "tests" / "fixtures" / "v14user.yaml"
    return _runner().invoke(app, ["destroy", str(fixture), "--force"])


def _scenario_destroy_redeployed(monkeypatch, tmp_path):
    msg = (
        "Destroy NOT completed: namespace v14user is now a newer deployment with "
        "the same name (a redeploy); it was left alone"
    )
    flag = {exit_codes.REFUSAL_DETAIL: "destroy.redeployed"}
    return _destroy_with(monkeypatch, [_result("namespace", "failed", msg, **flag)])


def _scenario_lease_held(monkeypatch, tmp_path):
    msg = (
        "another lakebench process holds the cluster lock (h@u@sha); wait for it, or "
        "run `lakebench admin release-lock` once its lease has expired."
    )
    flag = {exit_codes.REFUSAL_DETAIL: "lease.held"}
    return _destroy_with(monkeypatch, [_result("spark-operator-watch", "failed", msg, **flag)])


def _scenario_context_changed(monkeypatch, tmp_path):
    from lakebench.k8s.target import ContextConflictError

    def conflict(*a, **k):
        raise ContextConflictError("one cluster context per process: A is active, refusing B")

    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", conflict)
    (tmp_path / "c.yaml").write_text("name: x\n")
    return _runner().invoke(app, ["destroy", str(tmp_path / "c.yaml"), "--yes"])


def _scenario_destroy_namespace_terminating(monkeypatch, tmp_path):
    from lakebench.deploy.engine import DeploymentResult, DeploymentStatus

    pending = DeploymentResult(
        "namespace",
        DeploymentStatus.SKIPPED,
        "Namespace v14user is still terminating after 600s",
        details={"still_terminating": True},
    )
    return _destroy_with(monkeypatch, [_result("postgres", "success", "removed"), pending])


def _scenario_deploy_identity_foreign(monkeypatch, tmp_path):
    from unittest.mock import MagicMock

    import lakebench.cli._deploy as deploy_mod
    from tests import test_saf2_deploy_state as t

    cfg = _init_config(tmp_path)
    # deploy reads the namespace to reconcile its recorded nonces first.
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **k: t.FakeCore())
    monkeypatch.setattr(deploy_mod, "_preflight_check", lambda cfg: None)
    monkeypatch.setattr(deploy_mod, "check_datagen_scale", lambda cfg: None)
    engine = MagicMock()
    engine.deploy_all.return_value = [
        _result(
            "namespace",
            "failed",
            "Namespace ownership refused: namespace 'x' is owned by another lakebench deployment",
            **_REFUSED,
        )
    ]
    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", lambda *a, **k: engine)
    return _runner().invoke(app, ["deploy", str(cfg), "--yes"])


def _fake_s3(monkeypatch, *, info=None, init_error=None):
    from tests import test_datagen_timeout_and_regenerate as dg

    monkeypatch.setattr(dg._FakeS3, "instances", [])
    monkeypatch.setattr(dg._FakeS3, "_next_info", info)
    monkeypatch.setattr(dg._FakeS3, "_next_init_error", init_error)
    monkeypatch.setattr("lakebench.s3.S3Client", dg._FakeS3)
    return dg


_RUN_GENERATE = ["--generate", "--skip-preflight", "--skip-benchmark", "--skip-maintenance"]


def _scenario_run_bronze_nonempty(monkeypatch, tmp_path):
    from lakebench.s3.client import BucketInfo

    dg = _fake_s3(monkeypatch, info=BucketInfo(name="b", exists=True, object_count=9, size_bytes=9))
    dg._stub_full_run(monkeypatch)
    cfg = dg._write_cfg(tmp_path)
    return _runner().invoke(app, ["run", str(cfg), *_RUN_GENERATE, "--yes"])


def _scenario_s3_unreachable(monkeypatch, tmp_path):
    dg = _fake_s3(monkeypatch, init_error="endpoint unreachable")
    dg._stub_run_deps(monkeypatch)
    cfg = dg._write_cfg(tmp_path)
    return _runner().invoke(app, ["generate", str(cfg), "--yes"])


def _scenario_run_datagen_timeout(monkeypatch, tmp_path):
    import time as _time

    dg = _fake_s3(monkeypatch)
    dg._stub_full_run(monkeypatch)
    monkeypatch.setattr("lakebench.deploy.DatagenDeployer", dg._FakeDatagenDeployer)
    clock = {"t": 1_000.0}
    monkeypatch.setattr(_time, "time", lambda: clock["t"])

    def _sleep(seconds: float) -> None:
        clock["t"] += max(float(seconds), 0.001)

    monkeypatch.setattr(_time, "sleep", _sleep)
    cfg = dg._write_cfg(tmp_path)
    return _runner().invoke(app, ["run", str(cfg), *_RUN_GENERATE, "--timeout", "60", "--yes"])


def _scenario_run_prereq_failed(monkeypatch, tmp_path):
    from types import SimpleNamespace

    report = SimpleNamespace(checks=[], all_passed=False)
    monkeypatch.setattr("lakebench.cli._prerequisites.run_prerequisites", lambda *a, **k: report)
    return _runner().invoke(app, ["run", str(_init_config(tmp_path))])


def _scenario_run_namespace_missing_no_yes(monkeypatch, tmp_path):
    from types import SimpleNamespace

    from tests import test_datagen_timeout_and_regenerate as dg

    stubs = dg._stub_full_run(monkeypatch)
    stubs["k8s"].namespace_exists.return_value = False
    report = SimpleNamespace(checks=[], all_passed=True)
    monkeypatch.setattr("lakebench.cli._prerequisites.run_prerequisites", lambda *a, **k: report)
    return _runner().invoke(app, ["run", str(dg._write_cfg(tmp_path))])


def _local_run(monkeypatch, tmp_path, success: bool):
    from types import SimpleNamespace

    import lakebench.cli._local as local
    import lakebench.cli._run as run_mod

    stack = SimpleNamespace()
    monkeypatch.setattr(local, "check_local_supported", lambda *a, **k: None)
    monkeypatch.setattr(local, "deploy_local", lambda *a, **k: stack)
    monkeypatch.setattr(
        local,
        "run_local",
        lambda *a, **k: SimpleNamespace(success=success, stages=[], elapsed_seconds=1.0),
    )
    monkeypatch.setattr(local, "print_local_summary", lambda *a, **k: None)
    monkeypatch.setattr(run_mod, "_record_local_jobs", lambda *a, **k: None)
    monkeypatch.setattr(run_mod, "_save_local_metrics", lambda *a, **k: None)
    cfg = _init_config(tmp_path)
    return _runner().invoke(app, ["run", str(cfg), "--local", "--skip-benchmark", "--yes"])


def _scenario_run_pass(monkeypatch, tmp_path):
    return _local_run(monkeypatch, tmp_path, success=True)


def _scenario_run_verdict_failed(monkeypatch, tmp_path):
    return _local_run(monkeypatch, tmp_path, success=False)


def _scenario_confirm_declined(monkeypatch, tmp_path):
    a = _init_config(tmp_path)
    b = tmp_path / "b.yaml"
    b.write_text(a.read_text().replace(f"name: {_config_name(a)}", "name: other-side", 1))
    return _runner().invoke(app, ["compare", str(a), str(b)], input="n\n")


def _reproduce_package(tmp_path, commit_sha: str) -> Path:
    import yaml

    from tests import test_reproduce as rp

    (tmp_path / "cfg.yaml").write_text(rp._ONE_SAMPLE_CFG)
    pkg = rp._build_package(rp._metrics(), config_reference="cfg.yaml", commit_sha=commit_sha)
    path = tmp_path / "pkg.yaml"
    path.write_text(yaml.safe_dump(pkg))
    return path


def _scenario_reproduce_commit_drift(monkeypatch, tmp_path):
    import lakebench.cli._reproduce as rep

    pkg = _reproduce_package(tmp_path, "AAA1111")
    monkeypatch.setattr(rep, "_current_commit_sha", lambda: "BBB2222")
    return _runner().invoke(app, ["reproduce", str(pkg), "--dry-run"])


def _scenario_reproduce_drift(monkeypatch, tmp_path):
    import lakebench.cli._reproduce as rep

    pkg = _reproduce_package(tmp_path, "abc")
    monkeypatch.setattr(rep, "_current_commit_sha", lambda: "abc")
    monkeypatch.setattr(rep, "_run_pipeline", lambda *a, **k: object())
    for check in ("_sample_mismatch", "_policy_refusal", "_experiment_refusal"):
        monkeypatch.setattr(rep, check, lambda *a, **k: None)
    monkeypatch.setattr(rep, "_benchmark_samples", lambda m: 1)
    monkeypatch.setattr(rep, "_run_maintenance_policy", lambda m: None)
    monkeypatch.setattr(rep, "_run_query_set", lambda m: None)
    # The run reproduced nothing: every expected number measured as zero.
    monkeypatch.setattr(rep, "_measure_actual_numbers", lambda m: {"scale_ratio": 0.0})
    return _runner().invoke(app, ["reproduce", str(pkg)])


def _scenario_reproduce_existing_namespace(monkeypatch, tmp_path):
    import lakebench.cli._reproduce as rep

    pkg = _reproduce_package(tmp_path, "abc")
    monkeypatch.setattr(rep, "_current_commit_sha", lambda: "abc")
    monkeypatch.setattr("lakebench.k8s.client.K8sClient.namespace_exists", lambda self, n: True)
    return _runner().invoke(app, ["reproduce", str(pkg)])


def _scenario_reproduce_nonce_changed(monkeypatch, tmp_path):
    import lakebench.cli._reproduce as rep
    from lakebench.exit_codes import SafetyRefusal

    pkg = _reproduce_package(tmp_path, "abc")
    monkeypatch.setattr(rep, "_current_commit_sha", lambda: "abc")

    def pipeline(config_file, timeout, keep, refusals=None):
        # Its post-run destroy found another incarnation and deleted nothing.
        refusals.append(SafetyRefusal("Destroy NOT started", path="destroy.incarnation_mismatch"))
        return object()

    monkeypatch.setattr(rep, "_run_pipeline", pipeline)
    for check in ("_sample_mismatch", "_policy_refusal", "_experiment_refusal"):
        monkeypatch.setattr(rep, check, lambda *a, **k: None)
    monkeypatch.setattr(rep, "_benchmark_samples", lambda m: 1)
    monkeypatch.setattr(rep, "_run_maintenance_policy", lambda m: None)
    monkeypatch.setattr(rep, "_run_query_set", lambda m: None)
    monkeypatch.setattr(rep, "_measure_actual_numbers", lambda m: {"scale_ratio": 0.992})
    return _runner().invoke(app, ["reproduce", str(pkg)])


def _config_name(path: Path) -> str:
    for line in path.read_text().splitlines():
        if line.startswith("name:"):
            return line.split(":", 1)[1].strip()
    raise AssertionError(f"no name in {path}")


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


def _nameless_status(monkeypatch, tmp_path, setup):
    """`status` on a nameless config with no v1.6 state (a suggested name)."""
    import lakebench.cli._nameless as nameless
    from lakebench.config import deploy_state as ds
    from tests import test_saf2_deploy_state as t

    core = t.FakeCore()
    monkeypatch.setattr(nameless, "_core_v1_factory", lambda cfg: lambda: core)
    monkeypatch.setattr(ds, "suggested_name", lambda *a, **k: t.NAME)
    cfg = t._nameless(tmp_path)
    setup(t, core, cfg)
    return _runner().invoke(app, ["status", str(cfg)])


def _scenario_nameless_ambiguous(monkeypatch, tmp_path):
    def setup(t, core, cfg):
        t._nameless(tmp_path, "b.yaml")

    return _nameless_status(monkeypatch, tmp_path, setup)


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
    # The suggested name happens to name a live namespace: no v1.7 state
    # proves it, so status refuses without --name.
    def setup(t, core, cfg):
        t._v16_namespace(core)

    return _nameless_status(monkeypatch, tmp_path, setup)


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


def _scenario_deploy_state_copied(monkeypatch, tmp_path):
    import lakebench.cli._deploy as deploy_mod
    from tests import test_saf2_deploy_state as t

    core = t.FakeCore()
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **k: core)
    monkeypatch.setattr(deploy_mod, "_preflight_check", lambda cfg: None)
    monkeypatch.setattr(deploy_mod, "check_datagen_scale", lambda cfg: None)

    class Engine:
        def __init__(self, cfg, dry_run=False, **kw):
            self.results = []

        def deploy_all(self, **kw):  # pragma: no cover -- must not be reached
            raise AssertionError("deploy_all from a copied directory")

    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", Engine)
    cfg = t._named(tmp_path)
    t._v17_state(cfg, [("n1", "confirmed")], config_dir="/elsewhere")
    return _runner().invoke(app, ["deploy", str(cfg), "--yes"])


def _scenario_destroy_incarnation_mismatch(monkeypatch, tmp_path):
    from lakebench.deploy import DeploymentResult, DeploymentStatus

    class Engine:
        def __init__(self, cfg, **kw):
            pass

        def destroy_all(self, **kw):
            assert kw["expected_incarnation"] == "u1#n1"
            return [
                DeploymentResult(
                    component="ownership-check",
                    status=DeploymentStatus.FAILED,
                    message="Destroy NOT started: namespace lb-x is not the deployment "
                    "this command checked. Nothing was changed.",
                    details={"incarnation_mismatch": True},
                )
            ]

    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", Engine)

    def setup(t, core, cfg):
        t._v16_namespace(core, nonce="n1")
        t._v17_state(cfg, [("n1", "confirmed")])

    return _nameless_destroy(monkeypatch, tmp_path, setup, ["--name", "lb-x"])


SCENARIOS = {
    "config.upgrade_refused": _scenario_config_upgrade_refused,
    "deploy.state_copied": _scenario_deploy_state_copied,
    "destroy.incarnation_mismatch": _scenario_destroy_incarnation_mismatch,
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
    "financial.k8s_unreachable": _scenario_financial_k8s_unreachable,
    "config.validation": _scenario_config_validation,
    "config.unsupported": _scenario_config_unsupported,
    "config.name_required": _scenario_config_name_required,
    "cli.bad_argument": _scenario_cli_bad_argument,
    "k8s.unreachable": _scenario_k8s_unreachable,
    "s3.unreachable": _scenario_s3_unreachable,
    "destroy.redeployed": _scenario_destroy_redeployed,
    "lease.held": _scenario_lease_held,
    "context.changed": _scenario_context_changed,
    "destroy.namespace_terminating": _scenario_destroy_namespace_terminating,
    "deploy.identity_foreign": _scenario_deploy_identity_foreign,
    "run.bronze_nonempty": _scenario_run_bronze_nonempty,
    "run.datagen_timeout": _scenario_run_datagen_timeout,
    "run.prereq_failed": _scenario_run_prereq_failed,
    "run.namespace_missing_no_yes": _scenario_run_namespace_missing_no_yes,
    "run.pass": _scenario_run_pass,
    "run.verdict_failed": _scenario_run_verdict_failed,
    "confirm.declined": _scenario_confirm_declined,
    "reproduce.commit_drift": _scenario_reproduce_commit_drift,
    "reproduce.drift": _scenario_reproduce_drift,
    "reproduce.existing_namespace": _scenario_reproduce_existing_namespace,
    "reproduce.nonce_changed": _scenario_reproduce_nonce_changed,
}

# The line each path must print on stderr, where it prints one.
EXPECTED_STDERR = {
    "config.upgrade_refused": "ERROR  `config upgrade` is removed",
    "unhandled_exception": "ERROR  RuntimeError: unexpected [/tmp] failure",
    "confirm.non_tty": "ERROR  Not confirmed",
    "sigint": "ERROR  Interrupted.",
    "financial.k8s_unreachable": "ERROR  Cannot reach the Kubernetes cluster: connection refused",
    "k8s.unreachable": "ERROR Kubernetes connection failed: connection refused",
    "run.bronze_nonempty": "Refusing to generate over it",
    "s3.unreachable": "refusing to generate",
    "run.prereq_failed": "ERROR Prerequisites not met",
    "run.namespace_missing_no_yes": "does not exist",
    "cli.bad_argument": "ERROR Unknown recipe: no-such-recipe",
    "config.unsupported": "Unsupported combination, refused",
}


def test_scenarios_cover_exactly_the_live_paths():
    """A live path needs a scenario; a path with a scenario must not stay planned."""
    assert set(SCENARIOS) == {p.name for p in PATHS if p.live}


# Text in the combined output that shows the scenario took its named path,
# where the code alone has more than one producer.
EXPECTED_OUTPUT = {
    "run.datagen_timeout": "wait budget",
    "destroy.redeployed": "Destroy Incomplete",
    "lease.held": "Destroy Incomplete",
    "destroy.namespace_terminating": "still terminating",
    "deploy.identity_foreign": "Deployment Failed",
    "run.pass": "Local mode is sized",
    "run.verdict_failed": "Local mode is sized",
    "config.validation": "Config error",
    "config.name_required": "config has no name, so it cannot change data",
    "reproduce.commit_drift": "Commit drift",
    "reproduce.drift": "scale_ratio",
}
# Text that must not appear: a declined prompt is not an unanswered one.
UNEXPECTED_OUTPUT = {"confirm.declined": "Not confirmed", "run.verdict_failed": "ERROR"}


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_exit_code_paths(name, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)  # journal and state files land here
    monkeypatch.setenv("KUBECONFIG", "/nonexistent/kubeconfig")
    result = SCENARIOS[name](monkeypatch, tmp_path)
    assert result.exit_code == exit_codes.path_code(name), result.output
    assert "Traceback" not in result.output
    if name in EXPECTED_STDERR:
        assert EXPECTED_STDERR[name] in _stderr(result), result.output
    if name in EXPECTED_OUTPUT:
        assert EXPECTED_OUTPUT[name] in result.output, result.output
    if name in UNEXPECTED_OUTPUT:
        assert UNEXPECTED_OUTPUT[name] not in result.output, result.output


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


def _foreign_class(module: str, name: str, base: type[BaseException]) -> type[BaseException]:
    return type(name, (base,), {"__module__": module})


@pytest.mark.parametrize(
    ("module", "name"),
    [
        ("click.exceptions", "UsageError"),  # stock click from a dependency
        ("click.exceptions", "Exit"),
        ("typer._click.exceptions", "NoSuchOption"),
        ("typer._click.exceptions", "RenamedClickException"),  # a later rename
    ],
)
def test_click_family_passes_through_by_package(module, name):
    """Click prints its own usage error and exits 2; the handler must not wrap it."""
    exc = _foreign_class(module, name, RuntimeError)("x")
    assert cli_exit.error_for(exc) is None


@pytest.mark.parametrize("module", ["click.exceptions", "typer._click.exceptions"])
def test_any_click_abort_is_not_confirmed(module):
    exc = _foreign_class(module, "Abort", RuntimeError)()
    assert cli_exit.exit_code_for(exc) == ExitCode.NOT_CONFIRMED


def test_non_click_runtime_error_is_failed():
    exc = _foreign_class("somelib.errors", "UsageError", RuntimeError)("x")
    assert cli_exit.exit_code_for(exc) == ExitCode.FAILED


# -- batch `run` on a cluster: a specific code survives the finally block ----


def _cluster_run(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("KUBECONFIG", "/nonexistent/kubeconfig")
    dg = _fake_s3(monkeypatch)  # the finally block measures bucket sizes
    stubs = dg._stub_full_run(monkeypatch)
    return stubs, dg._write_cfg(tmp_path)


def test_batch_run_operator_not_ready_exits_prerequisite(monkeypatch, tmp_path):
    """The finally block re-raises the run's code; it must keep 4, not 1."""
    from unittest.mock import MagicMock

    stubs, cfg = _cluster_run(monkeypatch, tmp_path)
    stubs["op"].check_status.return_value = MagicMock(
        ready=False, installed=True, version="2.5.1", message="controller down"
    )
    flags = ["--skip-preflight", "--skip-generate", "--skip-benchmark", "--yes"]
    result = _runner().invoke(app, ["run", str(cfg), *flags])
    assert result.exit_code == ExitCode.PREREQUISITE, result.output
    assert "Spark Operator not ready" in result.output


def test_batch_run_failed_step_exits_failed(monkeypatch, tmp_path):
    stubs, cfg = _cluster_run(monkeypatch, tmp_path)
    stubs["job_manager"].deploy_scripts_configmap.return_value = False
    flags = ["--skip-preflight", "--skip-generate", "--skip-benchmark", "--yes"]
    result = _runner().invoke(app, ["run", str(cfg), *flags])
    assert result.exit_code == ExitCode.FAILED, result.output
    assert "Failed to deploy Spark scripts ConfigMap" in result.output


def test_unknown_stage_is_refused_before_any_work(monkeypatch, tmp_path):
    """A typo in --stage exits 2 before the operator, datagen or a record."""
    stubs, cfg = _cluster_run(monkeypatch, tmp_path)
    flags = ["--skip-preflight", "--skip-generate", "--skip-benchmark", "--yes"]
    result = _runner().invoke(app, ["run", str(cfg), *flags, "--stage", "bogus"])
    assert result.exit_code == ExitCode.USAGE, result.output
    assert "Unknown stage: bogus" in result.output
    stubs["op"].check_status.assert_not_called()
    assert not list(tmp_path.glob("lakebench-output/runs/*/metrics.json"))
