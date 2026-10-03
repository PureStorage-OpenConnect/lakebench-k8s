"""``run`` with the corpus series marker, through the QA-9 harness (CD-18).

A multi-cycle ``--skip-generate`` reuses a finished multi-cycle corpus: no
datagen Job, every cycle's stages, each cycle recorded ``datagen_skipped``.
Before CD-18 the cycle loop regenerated every cycle whatever
``--skip-generate`` said (LB-213). An unfinished or mismatched marker is
refused (exit 3) before anything is submitted.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json

from tests.harness import run_harness
from tests.harness.run_harness import NAME, SCENARIOS, invoke_scenario, saved_record

SERIES = "customer/interactions/_corpus/series.json"
CYCLES = 3


def _config(cycles: int = CYCLES, **datagen) -> dict:
    cfg = dict(SCENARIOS["batch_c360"].config)
    cfg["architecture"] = {"pipeline": {"mode": "batch", "cycles": cycles}}
    if datagen:
        cfg["workload"] = json.loads(json.dumps(cfg["workload"]))
        cfg["workload"]["datagen"].update(datagen)
    return cfg


def _series_body(config: dict, complete: list[int]) -> bytes:
    from lakebench.config.schema import LakebenchConfig
    from lakebench.deploy import corpus

    cfg = LakebenchConfig.model_validate(config)
    cycles = config["architecture"]["pipeline"]["cycles"]
    return json.dumps(
        corpus._body(cfg, cycles, complete, "run-before", "sha256:" + "1" * 64, None, None)
    ).encode()


def _run(tmp_path, monkeypatch, config: dict, argv: list[str], body: bytes | None):
    real = run_harness.Recorder

    def seeded(*a, **k):
        rec = real(*a, **k)
        if body is not None:
            rec.objects[(f"{NAME}-bronze", SERIES)] = body
            rec.bronze_objects[SERIES] = (len(body), hashlib.md5(body).hexdigest())  # noqa: S324
        return rec

    monkeypatch.setattr(run_harness, "Recorder", seeded)
    # The harness replays single-cycle driver logs; the multi-cycle
    # expected-results count is not what this test checks.
    from lakebench.metrics import c360_correctness as c360

    real_ctx = c360.expected_context
    monkeypatch.setattr(
        c360,
        "expected_context",
        lambda cfg: {**real_ctx(cfg), "bronze_rows_expected": {"snappy": 2_478_560}},
    )
    scenario = dataclasses.replace(SCENARIOS["batch_c360"], argv=argv, config=config)
    return invoke_scenario(scenario, tmp_path, monkeypatch)


def _submits(rec) -> list[str]:
    return [c[2] for c in rec.calls if c[:2] == ["JobManager", "submit_job"]]


def test_multicycle_skip_generate_reuses_a_finished_series(tmp_path, monkeypatch):
    config = _config()
    result, rec = _run(
        tmp_path, monkeypatch, config, ["--skip-generate", "--yes"], _series_body(config, [0, 1, 2])
    )
    assert result.exit_code == 0, result.output
    assert not any(c[0] == "Datagen" for c in rec.calls), [
        c for c in rec.calls if c[0] == "Datagen"
    ]
    assert _submits(rec) == ["bronze-verify", "silver-build", "gold-finalize"] * CYCLES
    record = saved_record(tmp_path)
    assert [c["datagen_skipped"] for c in record["cycles"]] == [True] * CYCLES
    assert [c["timestamp_start"] for c in record["cycles"]] == [
        "2024-01-01",
        "2024-05-02",
        "2024-09-01",
    ]
    assert record["cycle_series"] == {
        "marker": "read",
        "reused": True,
        "cycles_total": CYCLES,
        "windows": [
            ["2024-01-01", "2024-05-02"],
            ["2024-05-02", "2024-09-01"],
            ["2024-09-01", "2025-01-01"],
        ],
    }


def test_multicycle_skip_generate_refuses_an_unfinished_series(tmp_path, monkeypatch):
    config = _config()
    result, rec = _run(
        tmp_path, monkeypatch, config, ["--skip-generate", "--yes"], _series_body(config, [0, 1])
    )
    assert result.exit_code == 3, result.output
    assert "series incomplete: cycle(s) [2] missing" in result.output
    assert f"lakebench run {tmp_path}" in result.output and "--regenerate" in result.output
    assert _submits(rec) == []


def test_single_cycle_run_refuses_a_corpus_of_another_scale(tmp_path, monkeypatch):
    """A single-cycle run without --generate reuses bronze as --skip-generate
    does, so the marker is checked there too."""
    made = _config(cycles=1)
    result, rec = _run(
        tmp_path, monkeypatch, _config(cycles=1, scale=2), ["--yes"], _series_body(made, [0])
    )
    assert result.exit_code == 3, result.output
    assert "series made with" in result.output
    assert _submits(rec) == []


def test_single_cycle_without_a_marker_proceeds_and_says_so(tmp_path, monkeypatch):
    result, rec = _run(tmp_path, monkeypatch, _config(cycles=1), ["--skip-generate", "--yes"], None)
    assert result.exit_code == 0, result.output
    assert "reusing it unchecked" in result.output
    assert saved_record(tmp_path)["cycle_series"]["marker"] == "absent"


def test_failed_datagen_job_is_not_a_corpus(tmp_path, monkeypatch):
    """run --generate: the progress loop stops when no pod is active, which a
    Job that failed after its retries also is; the run fails (exit 1) and no
    cycle is recorded in the series marker."""

    def failed_progress(self, *a, **k):
        self._rec.add("Datagen", "get_progress")
        return {"running": False, "completions": 2, "succeeded": 1, "failed": 1}

    monkeypatch.setattr(run_harness.FakeDatagenDeployer, "get_progress", failed_progress)
    result, rec = _run(tmp_path, monkeypatch, _config(cycles=1), ["--generate", "--yes"], None)
    assert result.exit_code == 1, result.output
    assert "Datagen did not complete: 1/2 pods succeeded, 1 failed" in result.output
    assert not any(c[:2] == ["S3", "put_object"] for c in rec.calls)
    assert _submits(rec) == []


def test_generate_records_the_cycle(tmp_path, monkeypatch):
    result, rec = _run(tmp_path, monkeypatch, _config(cycles=1), ["--generate", "--yes"], None)
    assert result.exit_code == 0, result.output
    body = json.loads(rec.objects[(f"{NAME}-bronze", SERIES)])
    assert body["cycles_complete"] == [0] and body["cycles_total"] == 1
    assert saved_record(tmp_path)["cycle_series"]["marker"] == "written"
