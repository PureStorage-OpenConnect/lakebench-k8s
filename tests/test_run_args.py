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
from lakebench.cli._run_args import RUN_RULES, RunArgs, run_args_problems, validate_run_args
from lakebench.exit_codes import ExitCode, UsageError

CONFIG = """\
name: runargs
recipe: hive-iceberg-spark-trino
platform:
  kubernetes:
    namespace: runargs
  storage:
    s3:
      endpoint: http://10.0.1.50:80
      access_key: x
      secret_key: y
architecture:
  pipeline:
    mode: batch
workload:
  schema: customer360
  datagen:
    scale: 1
"""

#: One argument list per rule, in RUN_RULES order, and the text it prints.
CASES = [
    (["--stage", "bogus"], "Unknown stage: bogus"),
    (["--continuous", "--stage", "silver-build"], "--stage does not apply to a continuous run"),
    (["--deploy-only", "--generate-only"], "--deploy-only and --generate-only cannot be combined"),
    (["--deploy-only", "--stage", "silver-build"], "--deploy-only only deploys"),
    (["--generate-only", "--skip-generate"], "--generate-only and --skip-generate cannot be"),
    (["--local", "--force-rebuild"], "--local does not deploy, generate on its own, rebuild"),
    (["--regenerate"], "--regenerate only applies with --generate or --generate-only"),
    (["--continuous", "--generate", "--regenerate"], "--regenerate does not apply to a local"),
    (["--generate", "--skip-generate"], "--skip-generate and --generate cannot be combined"),
    (["--force-reset"], "--force-reset only applies to a continuous run"),
    (["--continuous", "--force-rebuild"], "--force-rebuild only applies to a batch run"),
    (["--duration", "600"], "--duration only applies to a continuous run"),
    (["--continuous", "--duration", "30"], "--duration is below 60 s"),
    (["--timeout", "0"], "--timeout must be at least 1 s"),
]


#: Refused just after the rules, by the support check: no cluster call either.
SUPPORT_CASES = [(["--local", "--continuous"], "batch mode only")]


def test_every_rule_has_a_case():
    """One case per rule, in table order, each refused by its own rule."""
    assert len(CASES) == len(RUN_RULES)
    for (argv, message), rule in zip(CASES, RUN_RULES, strict=True):
        args = _args_of(argv)
        mode = "continuous" if args.continuous else "batch"
        assert rule.broken(args, mode), argv
        assert message in rule.text(args), (argv, rule.next)


def _args_of(argv: list[str]) -> RunArgs:
    flags = {
        "--continuous": "continuous",
        "--deploy-only": "deploy_only",
        "--generate-only": "generate_only",
        "--local": "local",
        "--force-rebuild": "force_rebuild",
        "--force-reset": "force_reset",
        "--regenerate": "regenerate",
        "--generate": "include_datagen",
        "--skip-generate": "skip_generate",
    }
    kw: dict = {}
    it = iter(argv)
    for a in it:
        if a in flags:
            kw[flags[a]] = True
        elif a == "--stage":
            kw["stage"] = next(it)
        elif a in ("--duration", "--timeout"):
            kw[a[2:]] = int(next(it))
    return RunArgs(**kw)


def test_the_cli_reference_lists_exactly_the_rules():
    """docs/cli-reference.md's "Refused arguments" list is the table."""
    from pathlib import Path

    doc = (Path(__file__).resolve().parents[1] / "docs" / "cli-reference.md").read_text()
    block = doc.split("**Refused arguments.**", 1)[1].split("\n\n", 2)[1]
    listed = [line[2:].rstrip(";.") for line in block.splitlines() if line.startswith("- ")]
    assert listed == [r.doc for r in RUN_RULES]


@pytest.fixture
def no_cluster(monkeypatch):
    """Every way ``run`` reaches a cluster, S3 or a child process records
    the call and raises."""
    import subprocess

    import boto3
    import kubernetes.client
    import kubernetes.config

    from lakebench.k8s.client import K8sClient

    fired: list[str] = []

    def stop(name):
        def call(*a, **k):
            fired.append(name)
            raise AssertionError(f"cluster call: {name}")

        return call

    monkeypatch.setattr(kubernetes.config, "load_kube_config", stop("load_kube_config"))
    monkeypatch.setattr(kubernetes.config, "load_incluster_config", stop("load_incluster_config"))
    monkeypatch.setattr(kubernetes.client.ApiClient, "__init__", stop("ApiClient"))
    monkeypatch.setattr(K8sClient, "__init__", stop("K8sClient"))
    monkeypatch.setattr(subprocess, "run", stop("subprocess.run"))
    monkeypatch.setattr(subprocess, "Popen", stop("subprocess.Popen"))
    monkeypatch.setattr(boto3, "client", stop("boto3.client"))
    return fired


@pytest.mark.parametrize(
    "argv, message", CASES + SUPPORT_CASES, ids=[" ".join(c[0]) for c in CASES + SUPPORT_CASES]
)
def test_run_validation_zero_cluster_calls(tmp_path, monkeypatch, no_cluster, argv, message):
    monkeypatch.chdir(tmp_path)
    cfg = tmp_path / "runargs.yaml"
    cfg.write_text(CONFIG)
    result = CliRunner().invoke(app, ["run", str(cfg), *argv, "--yes"])
    assert result.exit_code == ExitCode.USAGE, result.output
    assert message in result.output, result.output
    assert no_cluster == []
    assert not list(tmp_path.glob("lakebench-output/runs/*/metrics.json"))


def _cfg(mode="batch"):
    from types import SimpleNamespace

    return SimpleNamespace(architecture=SimpleNamespace(pipeline=SimpleNamespace(mode=mode)))


def test_valid_arguments_resolve_the_mode():
    assert validate_run_args(RunArgs(), _cfg()).mode == "batch"
    assert validate_run_args(RunArgs(continuous=True), _cfg()).mode == "continuous"
    assert validate_run_args(RunArgs(sustained=True), _cfg()).mode == "continuous"
    assert validate_run_args(RunArgs(), _cfg("continuous")).mode == "continuous"
    assert validate_run_args(RunArgs(), _cfg("sustained")).mode == "continuous"
    # Allowed combinations stay allowed.
    for ok in (
        RunArgs(stage="silver-build"),
        RunArgs(include_datagen=True, regenerate=True),
        RunArgs(generate_only=True, regenerate=True),
        RunArgs(continuous=True, force_reset=True, duration=60),
        RunArgs(force_rebuild=True, timeout=1),
        RunArgs(local=True, stage="gold-finalize"),
    ):
        assert run_args_problems(ok, _cfg()) == [], ok
    # --generate-only honours --regenerate whatever the config's mode.
    regen = RunArgs(generate_only=True, regenerate=True)
    assert run_args_problems(regen, _cfg("continuous")) == []


def test_several_refusals_name_the_first_and_count_the_rest():
    with pytest.raises(UsageError) as info:
        validate_run_args(RunArgs(stage="bogus", force_reset=True, timeout=0), _cfg())
    assert str(info.value) == "Unknown stage: bogus (2 more refused argument(s))"
    assert info.value.code == ExitCode.USAGE and info.value.path == "run.args"


def test_reproduce_refuses_a_bad_timeout_before_it_destroys(tmp_path, monkeypatch, no_cluster):
    """reproduce destroys, deploys and generates before it calls run; a
    timeout run would refuse is refused first, with nothing destroyed."""
    import lakebench.cli._destroy as destroy_mod
    from lakebench.cli._reproduce import _run_pipeline

    monkeypatch.chdir(tmp_path)
    cfg = tmp_path / "runargs.yaml"
    cfg.write_text(CONFIG)
    called: list[str] = []
    monkeypatch.setattr(destroy_mod, "destroy", lambda **k: called.append("destroy"))
    with pytest.raises(UsageError, match="--timeout must be at least 1 s"):
        _run_pipeline(cfg, 0, keep=True)
    assert called == [] and no_cluster == []
