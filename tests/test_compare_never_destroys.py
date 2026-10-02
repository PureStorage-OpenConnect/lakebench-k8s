"""`lakebench compare` must not destroy the deployments it compares.

Until 1.7 compare ran `destroy --force` on both deployments, buckets
included, unless `--keep` was given, and `-y` skipped the only confirmation,
which never mentioned a destroy. compare now reads stored records only. This
test drives the CLI with every child process and in-process destroy
recorded: over two configs with stored runs it reaches a verdict and
destroys nothing, and the flags of the old command are refused (exit 2)
before anything else.
"""

from __future__ import annotations

import copy
import json
import subprocess
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from lakebench.exit_codes import ExitCode

CONFIG = """\
name: {name}
recipe: hive-iceberg-spark-trino
platform:
  kubernetes:
    namespace: {name}
  storage:
    s3:
      endpoint: http://10.0.1.50:80
      access_key: x
      secret_key: y
workload:
  schema: customer360
  datagen:
    scale: 1
"""


def _stored_runs(tmp_path) -> None:
    """One stored run per config, as `lakebench run` would leave them."""
    from tests.fixtures import stored_records as sr

    base = sr.load_record("5105a0")
    for i, name in enumerate(("cmpa", "cmpb")):
        rec = copy.deepcopy(base)
        rec["run_id"] = f"20261001-00000{i}-c0f00{i}"
        rec["deployment_name"] = name
        d = tmp_path / "lakebench-output" / "runs" / f"run-{rec['run_id']}"
        d.mkdir(parents=True)
        (d / "metrics.json").write_text(json.dumps(rec))


@pytest.mark.parametrize("argv", [[], ["--yes"], ["--keep"]])
def test_compare_runs_no_destroy(argv, tmp_path, monkeypatch):
    from lakebench.cli import app

    monkeypatch.chdir(tmp_path)
    children: list[list[str]] = []

    def record(argv, *a, **k):
        children.append([str(x) for x in argv])
        return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")

    monkeypatch.setattr(subprocess, "run", record)
    # An in-process destroy counts too.
    import lakebench.cli._destroy as destroy_cli
    import lakebench.deploy.destroy as destroy_mod

    def in_process(*a, **k):
        children.append(["destroy", "(in process)"])
        raise RuntimeError("compare destroyed in process")

    monkeypatch.setattr(destroy_cli, "_destroy_impl", in_process)
    monkeypatch.setattr(destroy_mod, "destroy_all", in_process)
    for name in ("cmpa", "cmpb"):
        (tmp_path / f"{name}.yaml").write_text(CONFIG.format(name=name))
    _stored_runs(tmp_path)
    result = CliRunner().invoke(app, ["compare", "cmpa.yaml", "cmpb.yaml", *argv])
    destroys = [c for c in children if "destroy" in c]
    assert destroys == [], destroys
    assert children == [], children
    assert result.exception is None or isinstance(result.exception, SystemExit), result.output
    if argv:
        # A flag of the command that ran both configs: refused, nothing run.
        assert result.exit_code == ExitCode.USAGE, result.output
        assert "no longer runs configs" in " ".join(result.output.split())
    else:
        # Not vacuous: both configs resolved to their stored runs and the
        # pair got a verdict (the records differ only in deployment name).
        assert result.exit_code == ExitCode.OK, result.output
        assert "LIKE-FOR-LIKE" in result.output
