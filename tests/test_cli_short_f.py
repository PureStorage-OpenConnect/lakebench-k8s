"""``-f`` means ``--file`` (the config path) on every command (GOALS P5.5).

It used to mean --file, --force, --format or --follow depending on the
command. The old meanings keep working for one release behind hidden options
that print a deprecation warning."""

from __future__ import annotations

import click
import pytest
import typer
from typer.testing import CliRunner

from lakebench.cli import app

runner = CliRunner()
ROOT_CMD = typer.main.get_command(app)

# Commands whose -f still carries its old meaning, and the new spelling.
DEPRECATED_F = {
    "destroy": ("force_short_f", "-y"),
    "clean": ("force_short_f", "-y"),
    "init": ("force_short_f", "--force"),
    "results": ("format_short_f", "-o"),
    "logs": ("follow_short_f", "-F"),
}


def _commands():
    ctx = click.Context(ROOT_CMD)

    def walk(cmd, path):
        if hasattr(cmd, "list_commands"):
            for name in cmd.list_commands(ctx):
                yield from walk(cmd.get_command(ctx, name), [*path, name])
        else:
            yield " ".join(path), cmd

    return dict(walk(ROOT_CMD, []))


COMMANDS = _commands()


def _owner_of(cmd, flag):
    for p in cmd.params:
        if flag in getattr(p, "opts", []) or flag in getattr(p, "secondary_opts", []):
            return p
    return None


@pytest.mark.parametrize("name", sorted(COMMANDS))
def test_short_f_is_file_or_a_deprecated_alias(name):
    cmd = COMMANDS[name]
    p = _owner_of(cmd, "-f")
    if p is None:
        return
    if name in DEPRECATED_F:
        assert p.name == DEPRECATED_F[name][0] and p.hidden, name
    else:
        assert "--file" in p.opts, f"{name}: -f is bound to {p.opts}"


@pytest.mark.parametrize("name", sorted(COMMANDS))
def test_every_file_option_has_short_f_unless_deprecated(name):
    cmd = COMMANDS[name]
    p = _owner_of(cmd, "--file")
    if p is None or name in DEPRECATED_F:
        return
    assert "-f" in p.opts, name


@pytest.mark.parametrize(("name", "new_flag"), [(k, v[1]) for k, v in DEPRECATED_F.items()])
def test_new_spelling_exists(name, new_flag):
    assert _owner_of(COMMANDS[name], new_flag) is not None


def _parse(name, args):
    cmd = COMMANDS[name]
    return cmd.make_context(name, list(args)).params


def test_destroy_parsing():
    assert _parse("destroy", ["x.yaml", "-y"])["force"] is True
    assert _parse("destroy", ["x.yaml", "--yes"])["force"] is True
    old = _parse("destroy", ["-f", "x.yaml"])
    assert old["force_short_f"] is True and str(old["config_file"]).endswith("x.yaml")


def test_results_and_logs_parsing():
    assert _parse("results", ["-o", "json"])["output_format"] == "json"
    assert _parse("results", ["-f", "json"])["format_short_f"] == "json"
    assert _parse("logs", ["trino", "-F"])["follow"] is True
    assert _parse("logs", ["trino", "-f"])["follow_short_f"] is True


def test_admin_doctor_short_f_is_file():
    assert str(_parse("admin doctor", ["-f", "x.yaml"])["file_option"]).endswith("x.yaml")


def test_init_old_short_f_still_overwrites_and_warns(tmp_path):
    out = tmp_path / "lakebench.yaml"
    out.write_text("old: true\n")
    result = runner.invoke(app, ["init", "--no-interactive", "--name", "t", "-o", str(out), "-f"])
    assert result.exit_code == 0, result.output
    assert "deprecated" in result.output and "--force" in result.output
    assert "old: true" not in out.read_text()


def test_init_without_force_refuses_to_overwrite(tmp_path):
    out = tmp_path / "lakebench.yaml"
    out.write_text("old: true\n")
    result = runner.invoke(app, ["init", "--no-interactive", "--name", "t", "-o", str(out)])
    assert result.exit_code == 1
    assert out.read_text() == "old: true\n"


@pytest.fixture
def no_cluster(monkeypatch, tmp_path):
    # No test here may reach a cluster, whatever -f ends up meaning.
    monkeypatch.setenv("KUBECONFIG", str(tmp_path / "nonexistent-kubeconfig"))
    monkeypatch.delenv("LAKEBENCH_LEGACY_SHORT_F", raising=False)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _cfg(tmp_path):
    p = tmp_path / "cfg.yaml"
    p.write_text("name: shortf-test\n")
    return p


@pytest.mark.parametrize("command", ["destroy", "clean"])
@pytest.mark.parametrize("stdin", ["", None])
def test_short_f_force_is_refused_everywhere(no_cluster, command, stdin):
    # stdin="" is a non-terminal pipe (agents, IDE runners, ssh host cmd).
    result = runner.invoke(app, [command, str(_cfg(no_cluster)), "-f"], input=stdin)
    assert result.exit_code == 2, result.output
    assert "no longer skips confirmation" in result.output
    assert "--force or -y" in result.output


@pytest.mark.parametrize("command", ["destroy", "clean"])
def test_short_f_force_refused_with_closed_stdin(no_cluster, monkeypatch, command):
    import sys

    monkeypatch.setattr(sys, "stdin", None)
    from lakebench.cli._helpers import stdin_is_tty

    assert stdin_is_tty() is False
    result = runner.invoke(app, [command, str(_cfg(no_cluster)), "-f"])
    assert result.exit_code == 2


@pytest.mark.parametrize("command", ["destroy", "clean"])
def test_force_plus_short_f_is_not_refused(no_cluster, command):
    import lakebench.cli._helpers as helpers

    result = runner.invoke(app, [command, str(no_cluster / "missing.yaml"), "--force", "-f"])
    assert result.exit_code != 2
    assert "no longer skips confirmation" not in result.output
    # Sanity: the -f handler returned force unchanged rather than exiting.
    assert helpers.deprecated_short_f_force("--force or -y", True) is True


@pytest.mark.parametrize("command", ["destroy", "clean"])
def test_legacy_env_restores_old_meaning(no_cluster, monkeypatch, command):
    monkeypatch.setenv("LAKEBENCH_LEGACY_SHORT_F", "1")
    # The config does not exist, so the command stops before any cluster
    # call; it must get past the -f handling to fail there.
    result = runner.invoke(app, [command, str(no_cluster / "missing.yaml"), "-f"])
    assert result.exit_code != 2
    assert "deprecated" in result.output


def test_deprecation_warning_goes_to_stderr(capsys):
    from lakebench.cli._helpers import warn_deprecated_short_f

    warn_deprecated_short_f("-o")
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "deprecated" in captured.err


@pytest.mark.parametrize("args", [["-o", "json"], ["-f", "json"]])
def test_results_json_is_parseable_stdout(monkeypatch, tmp_path, args):
    import json
    import types

    import lakebench.metrics as metrics_pkg

    long_value = "x" * 400  # longer than any terminal: Rich would wrap it
    pb = types.SimpleNamespace(to_dict=lambda: {"deployment_name": long_value, "scores": {}})

    class FakeStorage:
        def __init__(self, *_a, **_k):
            pass

        def get_latest_run(self):
            return types.SimpleNamespace(pipeline_benchmark=pb)

    monkeypatch.setattr(metrics_pkg, "MetricsStorage", FakeStorage)
    result = runner.invoke(app, ["results", "-m", str(tmp_path), *args])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["deployment_name"] == long_value


def test_results_format_table_and_short_f_conflict(tmp_path):
    result = runner.invoke(app, ["results", "-m", str(tmp_path), "--format", "table", "-f", "json"])
    assert result.exit_code == 2
    assert "both --format table and -f json" in result.output
