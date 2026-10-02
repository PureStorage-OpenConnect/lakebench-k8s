"""`lakebench compare` must not destroy the deployments it compares.

LB-218 (open, owner ER-11, v17-identity): compare runs `destroy --force` on
both deployments, buckets included, unless `--keep` is given, and `-y` skips
the only confirmation, which never mentions a destroy. ER-11 makes compare
read-only over stored records. This test drives the CLI with every child
process recorded and fails on the v1.6 behaviour; it is a strict xfail until
ER-11 lands, so the merge that fixes it must remove the marker.
"""

from __future__ import annotations

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


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="LB-218 open: compare destroys both deployments unless --keep (ER-11)",
)
def test_compare_runs_no_destroy(tmp_path, monkeypatch):
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
    result = CliRunner().invoke(app, ["compare", "cmpa.yaml", "cmpb.yaml", "--yes"])
    destroys = [c for c in children if "destroy" in c]
    assert destroys == [], destroys
    # Not vacuous: compare got past its config load and did not crash.
    assert result.exception is None or isinstance(result.exception, SystemExit), result.output
    assert result.exit_code != ExitCode.USAGE, result.output
