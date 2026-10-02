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
