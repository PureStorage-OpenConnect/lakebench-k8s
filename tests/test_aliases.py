"""The alias and refusal tables (``cli/_aliases.py``) against the CLI (CLI-7).

Each alias prints its one stderr line exactly once and runs its target;
each refusal exits 2, names the replacement and echoes no argument; every
table entry exists in the command tree, hidden, and every alias target is a
live command.
"""

from __future__ import annotations

from unittest import mock

import pytest
import typer
from typer.testing import CliRunner

from lakebench.cli import app
from lakebench.cli._aliases import (
    ALIASED_FLAGS,
    ALIASES,
    DEPRECATED_COMMANDS,
    HIDDEN_FLAGS,
    REFUSED,
    REFUSED_FLAGS,
    REMOVED_IN,
)

SECRET = "zz-do-not-echo-9f3"


def _tree():
    return typer.main.get_command(app)


def _command(path: str):
    cmd = _tree()
    for part in path.split():
        if not hasattr(cmd, "commands"):  # a group (Typer's own Click classes)
            return None
        cmd = cmd.commands.get(part)
        if cmd is None:
            return None
    return cmd


def _line(old: str) -> str:
    return (
        f"`lakebench {old}` is now `lakebench {ALIASES[old].target}`; "
        f"the old name is removed in {REMOVED_IN}"
    )


# ---------------------------------------------------------------------------
# The tables against the command tree
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("old", sorted(ALIASES))
def test_alias_is_hidden_and_its_target_is_live(old):
    cmd = _command(old)
    assert cmd is not None and cmd.hidden, old
    words = ALIASES[old].target.split()
    flags_at = next((i for i, w in enumerate(words) if w.startswith("-")), len(words))
    target = _command(" ".join(words[:flags_at]))
    assert target is not None and not target.hidden, ALIASES[old].target


@pytest.mark.parametrize("old", sorted(REFUSED))
def test_refused_entry_exists(old):
    head, _, last = old.rpartition(" ")
    cmd = _command(old)
    if cmd is not None:
        assert cmd.hidden, f"{old} is refused but shown in help"
        return
    # An argument value of a command that remains (clean bronze).
    parent = _command(head)
    assert parent is not None and not parent.hidden, old
    from lakebench.cli._clean import CLEAN_TARGETS

    assert last not in CLEAN_TARGETS


@pytest.mark.parametrize(
    ("command", "flag"),
    sorted(
        {
            (c, f)
            for table in (REFUSED_FLAGS, ALIASED_FLAGS)
            for c, flags in table.items()
            for f in flags
        }
    ),
)
def test_refused_and_aliased_flags_are_declared_hidden(command, flag):
    cmd = _command(command)
    assert cmd is not None
    params = [
        p for p in cmd.params if flag in getattr(p, "opts", []) + getattr(p, "secondary_opts", [])
    ]
    assert params, f"{command} {flag} is not declared"
    assert all(getattr(p, "hidden", False) for p in params), f"{command} {flag} is shown"


# ---------------------------------------------------------------------------
# Aliases: one line, then the target
# ---------------------------------------------------------------------------


def test_results_prints_one_line_and_runs_report(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with mock.patch("lakebench.cli.report") as report:
        res = CliRunner().invoke(app, ["results", "some-run", "--format", "json"])
    assert res.exit_code == 0, res.output
    assert res.stderr.count(_line("results")) == 1
    report.assert_called_once()
    kw = report.call_args.kwargs
    assert (kw["target"], kw["output_format"]) == ("some-run", "json")


def test_results_defaults_to_the_table(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with mock.patch("lakebench.cli.report") as report:
        CliRunner().invoke(app, ["results"])
    assert report.call_args.kwargs["output_format"] == "table"


@pytest.mark.parametrize(
    ("old", "component"),
    [
        ("admin install-spark-operator", "spark-operator"),
        ("admin install-scratch-storage-class", "scratch-storage-class"),
    ],
)
def test_admin_aliases_print_one_line_and_install(old, component, tmp_path):
    cfg = tmp_path / "c.yaml"
    cfg.write_text("name: x\n")
    with (
        mock.patch("lakebench.cli._admin._load_cfg", return_value=object()),
        mock.patch("lakebench.cli._admin._run_admin_install") as install,
    ):
        res = CliRunner().invoke(app, [*old.split(), str(cfg)])
    assert res.exit_code == 0, res.output
    assert res.stderr.count(_line(old)) == 1
    assert install.call_args.kwargs["components"] == [component]


# ---------------------------------------------------------------------------
# Refusals: exit 2, the replacement, no argument echoed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "argv",
    [
        ["config", "upgrade", f"/tmp/{SECRET}.yaml", "--output", SECRET, f"--{SECRET}"],
        ["clean", "bronze", f"{SECRET}.yaml", "--force"],
        ["clean", "data", f"{SECRET}.yaml"],
        ["clean", "metrics", f"{SECRET}.yaml", "--force"],
        ["clean", "journal", f"{SECRET}.yaml"],
        ["clean", "BRONZE", f"{SECRET}.yaml"],
    ],
    ids=["config-upgrade", "clean-bronze", "clean-data", "clean-metrics", "clean-journal", "case"],
)
def test_refusal_exits_2_names_the_replacement_and_echoes_nothing(argv, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("KUBECONFIG", "/nonexistent")
    res = CliRunner().invoke(app, argv)
    assert res.exit_code == 2, res.output
    assert SECRET not in res.output
    old = " ".join(argv[:2]).lower()
    r = REFUSED[old]
    out = " ".join(res.output.split())
    assert f"`lakebench {old}` is removed: {r.reason}" in out
    assert (r.replacement or "nothing replaces it") in out
    assert list(tmp_path.iterdir()) == []  # nothing written


def test_clean_silver_is_not_refused(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    res = CliRunner().invoke(app, ["clean", "silver", "missing.yaml", "--force"])
    assert "is removed" not in res.output


def _walk(cmd, path=()):
    yield path, cmd
    for name, sub in sorted(getattr(cmd, "commands", {}).items()):
        yield from _walk(sub, (*path, name))


def test_every_hidden_command_and_flag_is_in_a_table():
    """The other direction: nothing is hidden without an entry, so the doc
    lint (which reads these tables) sees every old name."""
    from lakebench.cli._helpers import DEPRECATED_SHORT_F_HELP

    hidden_cmds, hidden_flags, seen = [], [], set()
    for path, cmd in _walk(_tree()):
        name = " ".join(path)
        if path and cmd.hidden and name not in {**ALIASES, **REFUSED, **DEPRECATED_COMMANDS}:
            hidden_cmds.append(name)
        for p in getattr(cmd, "params", []):
            if not getattr(p, "hidden", False):
                continue
            known = (
                set(REFUSED_FLAGS.get(name, {}))
                | set(ALIASED_FLAGS.get(name, {}))
                | set(HIDDEN_FLAGS.get(name, ()))
            )
            # The wildcard covers only the deprecated short -f (cli/_helpers.py).
            if getattr(p, "help", None) == DEPRECATED_SHORT_F_HELP:
                known |= set(HIDDEN_FLAGS["*"])
            for opt in [*p.opts, *getattr(p, "secondary_opts", [])]:
                seen.add((name, opt))
                if opt not in known:
                    hidden_flags.append(f"{name} {opt}")
    assert hidden_cmds == []
    assert hidden_flags == []
    # Not vacuous: hidden params are visible to the walk.
    assert {("clean", "--metrics-dir"), ("compare", "--keep"), ("results", "-f")} <= seen


@pytest.mark.parametrize("old", sorted(DEPRECATED_COMMANDS))
def test_deprecated_commands_point_at_live_commands(old):
    assert _command(old).hidden
    target = _command(DEPRECATED_COMMANDS[old])
    assert target is not None and not target.hidden


def test_compare_refusal_says_the_tables_reason(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for flag in ("--keep", "--generate", "-y"):
        res = CliRunner().invoke(app, ["compare", "a.yaml", "b.yaml", flag])
        assert res.exit_code == 2, res.output
        assert REFUSED_FLAGS["compare"][flag].reason in " ".join(res.output.split())


@pytest.mark.parametrize("flag", ["--access-key", "--secret-key"])
def test_init_credential_refusal_says_the_tables_reason(flag, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    res = CliRunner().invoke(app, ["init", flag, SECRET])
    assert res.exit_code == 2, res.output
    out = " ".join(res.output.split())
    assert REFUSED_FLAGS["init"][flag].reason in out
    assert SECRET not in out
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("flag", ["--interactive", "-i", "--advanced"])
def test_init_wizard_flags_print_their_line(flag, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("LAKEBENCH_S3_ENDPOINT", "http://10.0.1.50:80")
    res = CliRunner().invoke(app, ["init", flag, "--output", str(tmp_path / "c.yaml")])
    assert ALIASED_FLAGS["init"][flag] in " ".join(res.output.split()), res.output


@pytest.mark.parametrize("flag", ["--metrics-dir", "-m"])
def test_clean_metrics_dir_flag_is_refused(flag, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    res = CliRunner().invoke(app, ["clean", "silver", "c.yaml", flag, SECRET])
    assert res.exit_code == 2, res.output
    assert SECRET not in res.output
    assert REFUSED_FLAGS["clean"][flag].reason in " ".join(res.output.split())
