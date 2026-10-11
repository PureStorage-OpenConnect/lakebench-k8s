"""``lakebench plan CONFIG...`` names where the Polaris client secret comes
from without printing it."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from lakebench.cli import app

runner = CliRunner()


def _write(tmp_path: Path, name="plan-t", **overrides) -> Path:
    data = {
        "name": name,
        "recipe": "hive-iceberg-spark-trino",
        "workload": {"schema": "customer360", "datagen": {"scale": 1}},
        "platform": {
            "storage": {
                "s3": {"endpoint": "http://10.0.1.50:80", "access_key": "a", "secret_key": "b"}
            }
        },
    }
    for k, v in overrides.items():
        data[k] = v
    path = tmp_path / f"{name}.yaml"
    path.write_text(yaml.safe_dump(data, sort_keys=False))
    return path


# -- Polaris client secret -------------------------------------------------------


def _polaris(tmp_path, secret=None):
    arch = {"catalog": {"polaris": {"client_secret": secret}}} if secret is not None else None
    extra = {"architecture": arch} if arch else {}
    return _write(tmp_path, recipe="polaris-iceberg-spark-trino", **extra)


@pytest.mark.parametrize("ref", ["${LB_PLAN_REF}", "${LB_PLAN_REF:-plan-default-value}"])
def test_plan_polaris_secret_from_a_variable_is_named_not_printed(tmp_path, monkeypatch, ref):
    monkeypatch.setenv("LB_PLAN_REF", "plan-sentinel")
    for extra in (["--offline"], ["--json"]):
        res = runner.invoke(app, ["plan", str(_polaris(tmp_path, ref)), *extra])
        assert res.exit_code == 0, res.output
        assert "client secret: from ${LB_PLAN_REF}" in res.stdout
        assert "plan-sentinel" not in res.output
        assert "plan-default-value" not in res.output
