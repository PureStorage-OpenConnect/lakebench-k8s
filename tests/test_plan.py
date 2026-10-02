"""CLI-3: ``lakebench plan CONFIG...``.

``plan`` reads the same sizing source as the run preflight and the docs
tables, the same prerequisite registry as ``deploy``, says where the Polaris
client secret comes from without printing it, and lists the hosts the deploy
contacts. Offline it makes no cluster call.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from types import SimpleNamespace as NS
from unittest import mock

import pytest
import yaml
from typer.testing import CliRunner

from lakebench.cli import app
from tests.test_run_args import no_cluster  # noqa: F401 -- the SAF-6 fixture

ROOT = Path(__file__).resolve().parents[1]
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


def _plan_json(*args) -> dict:
    res = runner.invoke(app, ["plan", *map(str, args), "--json"])
    assert res.exit_code == 0, res.output
    return json.loads(res.stdout)


# -- plan, the preflight and the docs tables agree -------------------------------


def _readme_table() -> dict[tuple[str, str, int], tuple[int, int]]:
    text = (ROOT / "README.md").read_text()
    block = text.split("BEGIN GENERATED: sizing-minimums")[1].split("END GENERATED")[0]
    rows = {}
    for line in block.splitlines():
        m = re.match(
            r"\| (Customer 360|AML) \| (batch|continuous) \| (\d+) \| ([\d,]+) cores \| ([\d,]+) GB",
            line,
        )
        if m:
            wl = "customer360" if m[1] == "Customer 360" else "financial"
            rows[(wl, m[2], int(m[3]))] = (
                int(m[4].replace(",", "")),
                int(m[5].replace(",", "")),
            )
    return rows


@pytest.mark.parametrize("scale", [1, 10, 100])
@pytest.mark.parametrize("mode", ["batch", "continuous"])
@pytest.mark.parametrize("workload", ["customer360", "financial"])
def test_plan_agrees_with_the_docs_tables(tmp_path, workload, mode, scale):
    path = _write(
        tmp_path,
        workload={"schema": workload, "datagen": {"scale": scale}},
        architecture={"pipeline": {"mode": mode}},
    )
    (p,) = _plan_json(path)["plans"]
    assert (p["sizing"]["floor"]["cpu_cores"], p["sizing"]["floor"]["memory_gb"]) == (
        _readme_table()[(workload, mode, scale)]
    )


@pytest.mark.parametrize("mode", ["batch", "continuous"])
@pytest.mark.parametrize("workload", ["customer360", "financial"])
def test_plan_offline_golden(tmp_path, workload, mode):
    """The offline text, per workload x mode at scale 1."""
    path = _write(
        tmp_path,
        workload={"schema": workload, "datagen": {"scale": 1}},
        architecture={"pipeline": {"mode": mode}},
    )
    res = runner.invoke(app, ["plan", str(path), "--offline"])
    assert res.exit_code == 0, res.output
    out = " ".join(res.stdout.split())
    cores, gb = _readme_table()[(workload, mode, 1)]
    for expected in (
        f"{path}: plan-t (namespace plan-t)",
        "recipe hive-iceberg-spark-trino: catalog hive, format iceberg, pipeline spark, "
        "query trino",
        f"workload {workload}, {mode}, scale 1;",
        f"needs {cores} cores / {gb} GB at once",
        "(sized against no cluster)",
        "scratch: not requested (scratch disabled)",
        "prerequisites: not checked (offline)",
        "egress at deploy (no pod contacts these after deploy):",
        "repo1.maven.org: Maven Central",
        "docker.io: image registry",
    ):
        assert expected in out, (expected, out)
    assert "Polaris" not in out  # a Hive recipe has no client secret line


def test_plan_zero_cluster_calls_offline(tmp_path, no_cluster):  # noqa: F811
    path = _write(tmp_path)
    for extra in (["--offline"], ["--cores", "434", "--memory", "4349"], ["--json"]):
        res = runner.invoke(app, ["plan", str(path), *extra])
        assert res.exit_code == 0, (extra, res.output)
    assert no_cluster == []


def test_plan_cores_memory_too_small_exits_4(tmp_path):
    res = runner.invoke(app, ["plan", str(_write(tmp_path)), "--cores", "8", "--memory", "64"])
    assert res.exit_code == 4, res.output
    assert "capacity (--cores/--memory): refused" in res.stdout


def test_plan_cores_without_memory_is_usage(tmp_path):
    res = runner.invoke(app, ["plan", str(_write(tmp_path)), "--cores", "8"])
    assert res.exit_code == 2


# -- prerequisites ---------------------------------------------------------------


def _outcomes(**status_by_id):
    from lakebench.deploy.prereqs import PREREQS, PrereqOutcome, PrereqResult, PrereqStatus

    out = []
    for p in PREREQS:
        st = status_by_id.get(p.id, "ok")
        out.append(PrereqOutcome(p, PrereqResult(PrereqStatus(st), f"{p.id} {st}")))
    return out


def _online(monkeypatch, outcomes, k8s=None):
    from lakebench.cli import _prerequisites as pre

    k8s = k8s or mock.MagicMock()
    k8s.get_cluster_capacity.return_value = None
    monkeypatch.setattr("lakebench.k8s.get_k8s_client", lambda **kw: k8s)
    monkeypatch.setattr("lakebench.deploy.prereqs.run_prereqs", lambda cfg: outcomes)
    monkeypatch.setattr(
        pre, "_check_cluster_capacity", lambda cfg, **kw: pre.PrereqResult("cc", True, "ok")
    )
    return k8s


@pytest.mark.parametrize(
    "prereq, component",
    [
        ("scratch-storage-class", "scratch-storage-class"),
        ("spark-operator", "spark-operator"),
        ("stackable", "stackable"),
    ],
)
def test_plan_missing_sc_exit4(tmp_path, monkeypatch, prereq, component):
    _online(monkeypatch, _outcomes(**{prereq: "fail"}))
    res = runner.invoke(app, ["plan", str(_write(tmp_path))])
    assert res.exit_code == 4, res.output
    assert f"Next: (cluster admin) lakebench admin install --component {component}" in " ".join(
        res.stdout.split()
    )


def test_plan_online_all_present_exits_0(tmp_path, monkeypatch):
    _online(monkeypatch, _outcomes())
    res = runner.invoke(app, ["plan", str(_write(tmp_path))])
    assert res.exit_code == 0, res.output
    assert "ok: Kubeflow Spark Operator 2.x" in res.stdout


def test_plan_unreachable_cluster_exits_4(tmp_path, monkeypatch):
    from lakebench.k8s import K8sConnectionError

    def down(**kw):
        raise K8sConnectionError("no kubeconfig")

    monkeypatch.setattr("lakebench.k8s.get_k8s_client", down)
    res = runner.invoke(app, ["plan", str(_write(tmp_path))])
    assert res.exit_code == 4
    assert "use --offline to size without a cluster" in res.output


# -- Polaris client secret -------------------------------------------------------


def _polaris(tmp_path, secret=None):
    arch = {"catalog": {"polaris": {"client_secret": secret}}} if secret is not None else None
    extra = {"architecture": arch} if arch else {}
    return _write(tmp_path, recipe="polaris-iceberg-spark-trino", **extra)


def test_plan_polaris_secret_informational(tmp_path):
    res = runner.invoke(app, ["plan", str(_polaris(tmp_path)), "--offline"])
    assert res.exit_code == 0, res.output
    assert "client secret: generated at deploy for a new Polaris" in " ".join(res.stdout.split())


def test_plan_polaris_secret_from_a_variable_is_named_not_printed(tmp_path, monkeypatch):
    monkeypatch.setenv("LB_PLAN_REF", "plan-sentinel")
    res = runner.invoke(app, ["plan", str(_polaris(tmp_path, "${LB_PLAN_REF}")), "--offline"])
    assert res.exit_code == 0, res.output
    assert "client secret: from ${LB_PLAN_REF}" in res.stdout
    assert "plan-sentinel" not in res.output


def test_plan_polaris_existing_without_secret_says_deploy_refuses(tmp_path, monkeypatch):
    """An already bootstrapped Polaris with no per-deployment Secret: deploy
    asks for client_secret, so plan must not say it is generated."""
    k8s = mock.MagicMock()
    k8s.secret_exists.return_value = False
    k8s.namespace_exists.return_value = True
    k8s._apps_v1.read_namespaced_deployment.return_value = NS()
    _online(monkeypatch, _outcomes(stackable="skipped"), k8s)
    res = runner.invoke(app, ["plan", str(_polaris(tmp_path))])
    out = " ".join(res.stdout.split())
    assert res.exit_code == 0, res.output  # informational: never the exit code
    assert "already runs Polaris with no Secret lakebench-polaris-client" in out
    assert "generated at deploy" not in out
    k8s.secret_exists.return_value = True
    res = runner.invoke(app, ["plan", str(_polaris(tmp_path))])
    assert "client secret: from Secret lakebench-polaris-client in plan-t" in " ".join(
        res.stdout.split()
    )


# -- egress and several configs ---------------------------------------------------


def test_plan_egress_follows_the_components(tmp_path):
    duck = _write(tmp_path, name="duck-t", recipe="hive-iceberg-spark-duckdb")
    (p,) = _plan_json(duck)["plans"]
    hosts = {e["host"] for e in p["egress"]}
    assert {"repo1.maven.org", "pypi.org", "extensions.duckdb.org", "docker.io"} <= hosts
    assert "oci.stackable.tech" in hosts  # the Hive image
    pol = _write(tmp_path, name="pol-t", recipe="polaris-iceberg-spark-none")
    (q,) = _plan_json(pol)["plans"]
    assert "oci.stackable.tech" not in {e["host"] for e in q["egress"]}
    assert "pypi.org" not in {e["host"] for e in q["egress"]}  # c360, no DuckDB


def test_plan_several_configs_name_their_differences(tmp_path):
    a = _write(tmp_path, name="plan-a")
    b = _write(
        tmp_path, name="plan-b", workload={"schema": "customer360", "datagen": {"scale": 10}}
    )
    doc = _plan_json(a, b)
    assert [p["name"] for p in doc["plans"]] == ["plan-a", "plan-b"]
    (d,) = doc["differences"]
    assert any("scale differs" in line for line in d["identity"])
    res = runner.invoke(app, ["plan", str(a), str(b), "--offline"])
    assert "plan-a vs plan-b" in res.stdout and "identity: scale differs" in res.stdout


def test_plan_nameless_config_takes_name(tmp_path):
    path = _write(tmp_path)
    data = yaml.safe_load(path.read_text())
    del data["name"]
    path.write_text(yaml.safe_dump(data))
    (p,) = _plan_json(path, "--name", "given-n")["plans"]
    assert p["name"] == "given-n"
