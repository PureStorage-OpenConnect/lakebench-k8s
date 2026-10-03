"""The datagen fleet record of a run that generated its own corpus.

``experiment.corpus.datagen`` (the generator image digest and the corpus
parameters the pods ran with) is read from the run's ``datagen_fleet``.
Until 1.7 only ``lakebench generate`` wrote the fleet sidecar, so a run that
generated with ``--generate`` (batch) or by default (continuous) recorded
"no datagen fleet record for this run", or attributed an older generate's
sidecar to a corpus it had replaced. Driven through the QA-9 harness.
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.harness.run_harness import (
    DATAGEN_FLEET_IMAGE_ID,
    SCENARIOS,
    run_scenario_full,
    saved_record,
)

NAMESPACE = "runchar"
DIGEST = DATAGEN_FLEET_IMAGE_ID.split("@", 1)[1]
STALE_ID = "docker.io/sillidata/lb-datagen@sha256:" + "0" * 64


def _sidecar(tmp_path: Path) -> Path:
    return tmp_path / "lakebench-output" / "datagen" / f"{NAMESPACE}-datagen-metrics.json"


def _stale(tmp_path: Path) -> None:
    p = _sidecar(tmp_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        json.dumps({"namespace": NAMESPACE, "image_ids": [STALE_ID], "image": "old", "seed": 7})
    )


def _run(tmp_path, monkeypatch, name: str, argv: list[str]) -> dict:
    scenario = dataclasses.replace(SCENARIOS[name], argv=argv)
    trace, _rec = run_scenario_full(scenario, tmp_path, monkeypatch)
    assert trace["unscripted"] == [], trace["unscripted"]
    record = saved_record(tmp_path)
    assert record, "the run saved no record"
    return record


def _datagen(record: dict) -> dict:
    return record["experiment"]["corpus"]["datagen"]


@pytest.mark.parametrize("stale", [False, True])
def test_batch_generate_records_its_own_fleet(stale, tmp_path, monkeypatch):
    if stale:
        _stale(tmp_path)
    record = _run(tmp_path, monkeypatch, "batch_c360", ["--generate", "--yes"])
    assert record["datagen_fleet"]["image_ids"] == [DATAGEN_FLEET_IMAGE_ID]
    dg = _datagen(record)
    assert dg["digest"] == DIGEST and "digest_reason" not in dg
    side = json.loads(_sidecar(tmp_path).read_text())
    assert side["namespace"] == NAMESPACE and side["image_ids"] == [DATAGEN_FLEET_IMAGE_ID]


def test_batch_generate_never_borrows_an_older_sidecar(tmp_path, monkeypatch):
    """The fleet of this run cannot be read: the record says so, and the old
    sidecar (another corpus) is neither used nor left for a later run."""
    _stale(tmp_path)

    def broken(*a, **k):
        raise RuntimeError("pods gone")

    import tests.harness.run_harness as harness

    # The harness installs its fleet fake per run; this run's pods are gone.
    monkeypatch.setattr(harness, "_fake_collect_from_k8s", lambda rec: broken)
    record = _run(tmp_path, monkeypatch, "batch_c360", ["--generate", "--yes"])
    assert "datagen_fleet" not in record
    assert STALE_ID not in json.dumps(record)
    assert _datagen(record)["digest"] is None
    assert not _sidecar(tmp_path).exists()


def test_batch_skip_generate_reads_the_sidecar(tmp_path, monkeypatch):
    """A run over an earlier corpus still takes that generate's record."""
    _stale(tmp_path)
    record = _run(tmp_path, monkeypatch, "batch_c360", ["--skip-generate", "--yes"])
    assert record["datagen_fleet"]["image_ids"] == [STALE_ID]


def test_continuous_records_its_own_fleet(tmp_path, monkeypatch):
    _stale(tmp_path)
    scenario = SCENARIOS["continuous_c360"]
    record = _run(tmp_path, monkeypatch, "continuous_c360", list(scenario.argv))
    assert record["datagen_fleet"]["image_ids"] == [DATAGEN_FLEET_IMAGE_ID]
    dg = _datagen(record)
    assert dg["digest"] == DIGEST and "digest_reason" not in dg
    assert json.loads(_sidecar(tmp_path).read_text())["image_ids"] == [DATAGEN_FLEET_IMAGE_ID]


def test_continuous_skip_generate_records_no_fleet_of_its_own(tmp_path, monkeypatch):
    scenario = SCENARIOS["continuous_c360_skip_generate"]
    record = _run(tmp_path, monkeypatch, "continuous_c360_skip_generate", list(scenario.argv))
    assert "datagen_fleet" not in record


def test_continuous_drops_the_old_sidecar_when_datagen_starts(tmp_path, monkeypatch):
    """The corpus is replaced when datagen starts: a run interrupted inside
    its window leaves no sidecar describing the corpus it replaced."""
    import signal

    _stale(tmp_path)
    previous = signal.getsignal(signal.SIGINT)
    try:
        scenario = dataclasses.replace(SCENARIOS["continuous_c360"], events=((300.0, "SIGINT"),))
        trace, _rec = run_scenario_full(scenario, tmp_path, monkeypatch)
    finally:
        signal.signal(signal.SIGINT, previous)
    assert trace["exit_code"] == 130
    record = saved_record(tmp_path)
    assert record["verdict"]["status"] == "INTERRUPTED"
    assert STALE_ID not in json.dumps(record)
    assert not _sidecar(tmp_path).exists()


def test_generate_drops_the_old_sidecar_before_it_replaces_the_corpus(tmp_path, monkeypatch):
    """`lakebench generate --regenerate` empties bronze, then submits: when
    the submit (or anything after it) fails, the old sidecar no longer
    claims the corpus."""
    from unittest.mock import MagicMock

    from typer.testing import CliRunner

    from lakebench.cli import app
    from lakebench.config import load_config
    from lakebench.s3.client import BucketInfo
    from tests.test_datagen_timeout_and_regenerate import _FakeS3, _stub_run_deps, _write_cfg

    monkeypatch.chdir(tmp_path)
    cfg_file = _write_cfg(tmp_path)
    namespace = load_config(cfg_file).get_namespace()
    side = tmp_path / "lakebench-output" / "datagen" / f"{namespace}-datagen-metrics.json"
    side.parent.mkdir(parents=True)
    side.write_text(json.dumps({"namespace": namespace, "image_ids": [STALE_ID]}))
    _stub_run_deps(monkeypatch)
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    # This deployment owns the bronze bucket (tests/test_bronze_gate.py
    # covers ownership); the fake's class state is restored after the test.
    monkeypatch.setattr("lakebench.deploy.datagen.deployment_may_empty", lambda *a, **k: True)
    monkeypatch.setattr(_FakeS3, "instances", [])
    monkeypatch.setattr(
        _FakeS3,
        "_next_info",
        BucketInfo(name="b", exists=True, object_count=5, size_bytes=1_000_000),
    )
    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", MagicMock(side_effect=SystemExit(7)))
    res = CliRunner().invoke(app, ["generate", str(cfg_file), "--yes", "--regenerate"])
    assert res.exit_code == 7, (res.output, repr(res.exception))
    assert not side.exists()


def test_sidecar_helpers_never_raise(tmp_path, monkeypatch):
    from lakebench.metrics import datagen_aggregator

    monkeypatch.chdir(tmp_path)
    _stale(tmp_path)
    datagen_aggregator.drop_sidecar(NAMESPACE)
    assert not _sidecar(tmp_path).exists()
    # Never raises on a record json cannot encode, or a missing file.
    datagen_aggregator.drop_sidecar(NAMESPACE)
    datagen_aggregator.write_sidecar({"x": object()}, NAMESPACE)


@pytest.mark.parametrize("fails", ["scripts", "datagen"])
def test_continuous_failure_after_the_reset_leaves_no_old_sidecar(fails, tmp_path, monkeypatch):
    """The reset cleared the corpus, then the scripts deploy or the datagen
    submit failed: the sidecar of the corpus the reset removed is gone too."""
    import tests.harness.run_harness as harness
    from lakebench.deploy.engine import DeploymentResult, DeploymentStatus

    _stale(tmp_path)

    def failing(self, *a, **k):
        return DeploymentResult(component="datagen", status=DeploymentStatus.FAILED, message="no")

    if fails == "datagen":
        monkeypatch.setattr(harness.FakeDatagenDeployer, "deploy", failing)
    else:
        monkeypatch.setattr(
            harness.FakeJobManager, "deploy_scripts_configmap", lambda *a, **k: False
        )
    scenario = SCENARIOS["continuous_c360"]
    trace, _rec = run_scenario_full(scenario, tmp_path, monkeypatch)
    assert trace["exit_code"] != 0
    assert not _sidecar(tmp_path).exists()


def test_generate_refusal_keeps_the_sidecar(tmp_path, monkeypatch):
    """Bronze not empty and no --regenerate: generate refuses before it
    touches the corpus, so the sidecar that describes it stays."""
    from typer.testing import CliRunner

    from lakebench.cli import app
    from lakebench.config import load_config
    from lakebench.exit_codes import ExitCode
    from lakebench.s3.client import BucketInfo
    from tests.test_datagen_timeout_and_regenerate import _FakeS3, _stub_run_deps, _write_cfg

    monkeypatch.chdir(tmp_path)
    cfg_file = _write_cfg(tmp_path)
    namespace = load_config(cfg_file).get_namespace()
    side = tmp_path / "lakebench-output" / "datagen" / f"{namespace}-datagen-metrics.json"
    side.parent.mkdir(parents=True)
    side.write_text(json.dumps({"namespace": namespace, "image_ids": [STALE_ID]}))
    _stub_run_deps(monkeypatch)
    monkeypatch.setattr("lakebench.s3.S3Client", _FakeS3)
    # This deployment owns the bronze bucket (tests/test_bronze_gate.py
    # covers ownership); the fake's class state is restored after the test.
    monkeypatch.setattr("lakebench.deploy.datagen.deployment_may_empty", lambda *a, **k: True)
    monkeypatch.setattr(_FakeS3, "instances", [])
    monkeypatch.setattr(
        _FakeS3,
        "_next_info",
        BucketInfo(name="b", exists=True, object_count=5, size_bytes=1_000_000),
    )
    res = CliRunner().invoke(app, ["generate", str(cfg_file), "--yes"])
    assert res.exit_code == ExitCode.REFUSED, res.output
    assert side.exists()


# run --generate on a multi-cycle run is refused before any of this
# (tests/test_multicycle_generate_bronze.py).
def test_multi_cycle_run_never_borrows_an_older_sidecar(tmp_path, monkeypatch):
    """pipeline.cycles > 1 generates bronze in every cycle (deploy_cycle):
    the sidecar of the corpus it replaces is dropped once the gate lets the
    generate proceed, and the record carries no fleet rather than that
    older generate's."""
    import tests.harness.run_harness as harness
    from lakebench.deploy.engine import DeploymentResult, DeploymentStatus

    _stale(tmp_path)

    def deploy_cycle(self, *a, **k):
        self._rec.add("Datagen", "deploy_cycle")
        return DeploymentResult(
            component="datagen", status=DeploymentStatus.FAILED, message="cycle refused"
        )

    monkeypatch.setattr(harness.FakeDatagenDeployer, "deploy_cycle", deploy_cycle, raising=False)
    # The gate passes (tests/test_bronze_gate.py covers it); it still sees the
    # old sidecar, which describes the corpus until the gate lets it go.
    seen: list[bool] = []

    def gate(*_a, **_k):
        seen.append(_sidecar(tmp_path).exists())
        return SimpleNamespace(record=lambda: None, stale_allowed=False)

    monkeypatch.setattr("lakebench.cli._run.enforce_bronze_gate", gate)
    config = harness.base_config(architecture={"pipeline": {"mode": "batch", "cycles": 2}})
    scenario = dataclasses.replace(SCENARIOS["batch_c360"], argv=["--yes"], config=config)
    trace, _rec = run_scenario_full(scenario, tmp_path, monkeypatch)
    assert seen == [True]
    assert ["Datagen", "deploy_cycle"] in trace["calls"]
    assert not _sidecar(tmp_path).exists()
    record = saved_record(tmp_path)
    assert record, "the run saved no record"
    assert "datagen_fleet" not in record
    assert STALE_ID not in json.dumps(record)
    assert _datagen(record)["digest"] is None


def test_multi_cycle_skip_generate_without_a_series_keeps_the_sidecar(tmp_path, monkeypatch):
    """A multi-cycle --skip-generate reuses only a finished multi-cycle corpus:
    with no series marker it is refused (exit 3) before anything runs, and
    the namespace's sidecar is left as it was."""
    import tests.harness.run_harness as harness

    _stale(tmp_path)
    config = harness.base_config(architecture={"pipeline": {"mode": "batch", "cycles": 2}})
    scenario = dataclasses.replace(
        SCENARIOS["batch_c360"], argv=["--skip-generate", "--yes"], config=config
    )
    trace, _rec = run_scenario_full(scenario, tmp_path, monkeypatch)
    assert trace["exit_code"] == 3, trace
    assert ["Datagen", "deploy_cycle"] not in trace["calls"]
    assert _sidecar(tmp_path).exists()


@pytest.mark.parametrize("command", ["generate", "run"])
def test_regenerate_gate_failure_leaves_no_old_sidecar(command, tmp_path, monkeypatch):
    """--regenerate can empty part of bronze and then fail inside the gate:
    the sidecar is gone before the gate runs, so a later run over the
    partly cleared corpus does not attach the replaced corpus's fleet."""
    import typer

    _stale(tmp_path)

    def gate(*_a, **_k):
        raise typer.Exit(1)

    if command == "generate":
        from typer.testing import CliRunner

        from lakebench.cli import app
        from lakebench.config import load_config
        from tests.test_datagen_timeout_and_regenerate import _stub_run_deps, _write_cfg

        monkeypatch.chdir(tmp_path)
        cfg_file = _write_cfg(tmp_path)
        side = _sidecar(tmp_path).with_name(
            f"{load_config(cfg_file).get_namespace()}-datagen-metrics.json"
        )
        _sidecar(tmp_path).rename(side)
        _stub_run_deps(monkeypatch)
        monkeypatch.setattr("lakebench.cli._generate.enforce_bronze_gate", gate)
        res = CliRunner().invoke(app, ["generate", str(cfg_file), "--yes", "--regenerate"])
        assert res.exit_code == 1, res.output
        assert not side.exists()
    else:
        monkeypatch.setattr("lakebench.cli._run.enforce_bronze_gate", gate)
        scenario = dataclasses.replace(
            SCENARIOS["batch_c360"], argv=["--generate", "--regenerate", "--yes"]
        )
        trace, _rec = run_scenario_full(scenario, tmp_path, monkeypatch)
        assert trace["exit_code"] == 1
    assert not _sidecar(tmp_path).exists()


def test_run_generate_refusal_keeps_the_sidecar(tmp_path, monkeypatch):
    """No --regenerate: run --generate refuses at the bronze gate before it
    touches the corpus, so the sidecar that describes it stays."""
    import typer

    from lakebench.exit_codes import ExitCode

    _stale(tmp_path)

    def gate(*_a, **_k):
        raise typer.Exit(ExitCode.REFUSED)

    monkeypatch.setattr("lakebench.cli._run.enforce_bronze_gate", gate)
    scenario = dataclasses.replace(SCENARIOS["batch_c360"], argv=["--generate", "--yes"])
    trace, _rec = run_scenario_full(scenario, tmp_path, monkeypatch)
    assert trace["exit_code"] == ExitCode.REFUSED
    assert json.loads(_sidecar(tmp_path).read_text())["image_ids"] == [STALE_ID]
