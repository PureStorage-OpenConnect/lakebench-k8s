"""Every refused ``lakebench run`` argument exits 2 before any cluster call.

``run`` used to reach the cluster (capacity read, auto-deploy, the operator
watch list, the scripts ConfigMaps) before it checked its flags. Each row of
``cli._run_args.RUN_RULES`` is driven through the real CLI with every way of
reaching a cluster, S3 or a subprocess patched to fail the test: the run
must exit 2 (usage) with the rule's message, and nothing may have fired.
On the tree before the rules, the stage row fails here: the stage was
checked after the capacity read.
"""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from lakebench.cli import app
from lakebench.cli._run_args import (
    RUN_RULES,
    RunArgs,
    RunContext,
    run_args_problems,
)
from lakebench.exit_codes import ExitCode
from tests.fixtures import run_args_helpers as _run_args_helpers
from tests.fixtures.run_args_helpers import CONFIG as CONFIG

no_cluster = _run_args_helpers.no_cluster  # the SAF-6 fixture

#: One argument list per rule, in RUN_RULES order, and the text it prints.
CASES = [
    (["--stage", "bogus"], "Unknown stage: bogus"),
    (["--continuous", "--stage", "silver-build"], "--stage does not apply to a continuous run"),
    (["--deploy-only", "--generate-only"], "--deploy-only and --generate-only cannot be combined"),
    (["--deploy-only", "--stage", "silver-build"], "--deploy-only only deploys"),
    (["--skip-deploy", "--deploy-only"], "--skip-deploy skips the deploy"),
    (["--generate-only", "--skip-generate"], "--generate-only and --skip-generate cannot be"),
    (["--local", "--force-rebuild"], "--local does not deploy, generate on its own, rebuild"),
    (["--regenerate"], "--regenerate only applies when the run generates"),
    (["--continuous", "--generate", "--regenerate"], "--regenerate does not apply to a local"),
    (["--allow-stale-bronze"], "--allow-stale-bronze only applies when the run generates"),
    (["--generate", "--skip-generate"], "--skip-generate and --generate cannot be combined"),
    (["--generate", "--cycles"], "--generate does not apply to a multi-cycle run"),
    (["--generate-only", "--cycles"], "--generate-only does not apply to a multi-cycle run"),
    (
        ["--skip-generate", "--cycles", "--financial"],
        "--skip-generate does not apply to a multi-cycle financial (AML) run",
    ),
    (["--force-reset"], "--force-reset only applies to a continuous run"),
    (["--continuous", "--force-rebuild"], "--force-rebuild only applies to a batch run"),
    (["--duration", "600"], "--duration only applies to a continuous run"),
    (["--continuous", "--duration", "30"], "--duration is below 60 s"),
    (["--timeout", "0"], "--timeout must be at least 1 s"),
    (["--repeat", "0"], "--repeat must be between 1 and 20"),
    (["--continuous", "--repeat", "2"], "--repeat does not apply to a continuous run"),
    (["--repeat", "2", "--cycles"], "--repeat does not apply to a multi-cycle run"),
    (["--stage", "silver-build", "--repeat", "2"], "--repeat runs the whole batch pipeline"),
    # Not a flag: a batch AML config that sets benchmark.investigator_sessions
    # (it loads; run refuses it in batch).
    (["--investigators"], "runs only on an AML continuous run with TM operations"),
]


#: Refused just after the rules, by the support check: no cluster call either.
SUPPORT_CASES = [(["--local", "--continuous"], "batch mode only")]


@pytest.mark.parametrize(
    ("argv", "message"),
    CASES + SUPPORT_CASES,
    ids=lambda v: " ".join(v) if isinstance(v, list) else "",
)
def test_run_validation_zero_cluster_calls(argv, message, tmp_path, monkeypatch, no_cluster):
    monkeypatch.chdir(tmp_path)
    cfg = tmp_path / "runargs.yaml"
    text = CONFIG
    if "--cycles" in argv:  # not a flag: the config's cycle count
        argv = [a for a in argv if a != "--cycles"]
        text = text.replace("    mode: batch\n", "    mode: batch\n    cycles: 2\n")
    if "--financial" in argv:  # not a flag: the config's schema
        argv = [a for a in argv if a != "--financial"]
        text = text.replace("  schema: customer360\n", "  schema: financial\n")
    if "--investigators" in argv:  # not a flag: the config's sessions key
        argv = [a for a in argv if a != "--investigators"]
        text = text.replace(
            "  pipeline:\n", "  benchmark:\n    investigator_sessions: 8\n  pipeline:\n"
        ).replace("schema: customer360", "schema: financial")
    cfg.write_text(text)
    result = CliRunner().invoke(app, ["run", str(cfg), *argv, "--yes"])
    assert result.exit_code == ExitCode.USAGE, result.output
    if argv[:2] != ["--repeat", "0"]:  # the CLI's own range check answers first
        assert message in result.output, result.output
    assert no_cluster == []
    assert not list(tmp_path.glob("lakebench-output/runs/*/metrics.json"))


@pytest.mark.parametrize(
    ("kw", "mode", "cycles", "refused"),
    [
        ({"include_datagen": True}, "batch", 1, False),
        ({"generate_only": True}, "batch", 1, False),
        ({"generate_only": True}, "continuous", 1, False),
        ({}, "batch", 2, False),
        ({"skip_generate": True}, "batch", 2, True),
        ({}, "batch", 1, True),
        ({"include_datagen": True, "skip_generate": True}, "batch", 1, True),
        ({"include_datagen": True}, "continuous", 1, True),
        ({"include_datagen": True, "local": True}, "batch", 1, True),
        ({"deploy_only": True}, "batch", 2, True),
    ],
)
def test_allow_stale_bronze_only_where_a_generate_reads_it(kw, mode, cycles, refused):
    """The flag is read only by the bronze gate before a run's own datagen:
    --generate-only, a batch --generate, or a multi-cycle batch run that
    generates (a multi-cycle --skip-generate reuses its corpus, no gate)."""
    rule = next(r for r in RUN_RULES if "--allow-stale-bronze" in str(r.message))
    ctx = RunContext(mode=mode, cycles=cycles)
    assert rule.broken(RunArgs(allow_stale_bronze=True, **kw), ctx) is refused
    assert not rule.broken(RunArgs(**kw), ctx)


def _cycles_cfg(cycles: int, schema: str = "customer360"):
    from types import SimpleNamespace

    return SimpleNamespace(
        architecture=SimpleNamespace(
            pipeline=SimpleNamespace(mode="batch", cycles=cycles),
            workload=SimpleNamespace(schema_type=SimpleNamespace(value=schema)),
        )
    )


def test_multi_cycle_skip_generate_is_refused_only_for_financial():
    args = RunArgs(skip_generate=True)
    assert run_args_problems(args, _cycles_cfg(3)) == []
    assert run_args_problems(args, _cycles_cfg(1, "financial")) == []
    assert run_args_problems(args, _cycles_cfg(3, "financial"))
