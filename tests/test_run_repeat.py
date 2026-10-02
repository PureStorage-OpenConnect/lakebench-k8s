"""``run --repeat N``: one series, one corpus (CLI-9).

Driven through the QA-9 harness (tests/harness/run_harness.py) on the batch
Customer 360 scenario, whose bronze bucket the fake S3 lists from
``Recorder.bronze_objects``. The series is checked from what it leaves: the
saved records, the series manifest and the fake cluster. A repetition must
never inherit a corpus it did not verify, and a repetition whose bronze or
corpus differs from repetition 1's must never count as a passed member.
"""

from __future__ import annotations

import dataclasses
import json
import signal
from pathlib import Path

import pytest

from tests.harness.run_harness import SCENARIOS, Recorder, invoke_scenario

SCOPE = "customer/interactions/"
CORPUS = {
    f"{SCOPE}part-0000.parquet": (1000, "e0"),
    f"{SCOPE}part-0001.parquet": (1000, "e1"),
}


@pytest.fixture(autouse=True)
def sentinel_sigterm():
    def handler(signum, frame):
        raise AssertionError("SIGTERM reached the test process")

    previous = signal.signal(signal.SIGTERM, handler)
    previous_int = signal.getsignal(signal.SIGINT)
    try:
        yield handler
    finally:
        signal.signal(signal.SIGTERM, previous)
        signal.signal(signal.SIGINT, previous_int)


def _series(tmp_path, monkeypatch, *, argv=None, after=None, objects=None, **changes):
    """Run the batch scenario as a series; *after* maps a repetition index to
    a function of the recorder, run once that repetition returned."""
    import lakebench.cli._series as series_mod

    rec_box: list[Recorder] = []
    real_install = None

    from tests.harness import run_harness

    real_install = run_harness.install_fakes

    def install(monkeypatch_, rec, scenario):
        rec.bronze_objects = dict(CORPUS if objects is None else objects)
        rec_box.append(rec)
        return real_install(monkeypatch_, rec, scenario)

    monkeypatch.setattr(run_harness, "install_fakes", install)
    real_call = series_mod._call
    count = [0]

    def call(*a, **k):
        code = real_call(*a, **k)
        count[0] += 1
        if after and count[0] in after:
            after[count[0]](rec_box[0])
        return code

    monkeypatch.setattr(series_mod, "_call", call)
    scenario = dataclasses.replace(
        SCENARIOS["batch_c360"],
        argv=argv or ["--skip-generate", "--yes", "--repeat", "3"],
        **changes,
    )
    result, rec = invoke_scenario(scenario, tmp_path, monkeypatch)
    records = [
        json.loads(p.read_text())
        for p in sorted((tmp_path / "lakebench-output" / "runs").glob("run-*/metrics.json"))
    ]
    records.sort(key=lambda r: (r.get("series") or {}).get("index", 0))
    manifests = list((tmp_path / "lakebench-output" / "series").glob("s-*.json"))
    manifest = json.loads(manifests[0].read_text()) if manifests else None
    return result, rec, records, manifest


def _corpus(record):
    return record["experiment"]["corpus"]


def _submits(rec, job):
    return [s for s in rec.submits if s[0] == job]


def test_repeat_three_records_one_series(tmp_path, monkeypatch):
    result, rec, records, manifest = _series(tmp_path, monkeypatch)
    assert result.exit_code == 0, result.output
    assert [r["series"]["index"] for r in records] == [1, 2, 3]
    assert len({r["series"]["id"] for r in records}) == 1
    assert all(r["series"]["size"] == 3 for r in records)
    assert manifest["schema"] == "lb-series/1"
    assert manifest["series_id"] == records[0]["series"]["id"]
    assert [r["run_id"] for r in manifest["runs"]] == [r["run_id"] for r in records]
    assert (manifest["attempted"], manifest["passed"], manifest["failed"]) == (3, 3, 0)
    assert all(r["member"] for r in manifest["runs"])
    assert manifest["corpus"]["bronze_objects"] == 2
    assert manifest["corpus"]["bronze_bytes"] == 2000
    assert manifest["corpus"]["digest_scope"] == f"runchar-bronze/{SCOPE}"
    assert manifest["corpus"]["from_run_id"] == records[0]["run_id"]
    assert manifest["stopped_reason"] is None


def test_repeat_inherits_block_without_markers(tmp_path, monkeypatch):
    """No node markers: repetitions 2 and 3 carry repetition 1's corpus
    block verbatim, inherited_from and the series digest. Without the
    inherited block they would carry their own (no inherited_from)."""
    result, rec, records, manifest = _series(tmp_path, monkeypatch)
    rep1 = _corpus(records[0])
    d1 = manifest["corpus"]["bronze_listing_sha256"]
    assert "inherited_from" not in rep1
    for r in records[1:]:
        c = dict(_corpus(r))
        assert c.pop("inherited_from") == records[0]["run_id"]
        assert c.pop("bronze_listing_sha256") == d1
        assert c == {k: v for k, v in rep1.items() if k != "problems"}


def test_series_digest_taken_before_save(tmp_path, monkeypatch):
    """Each repetition's record carries its own observation of bronze, taken
    before its save, and it equals D1."""
    result, rec, records, manifest = _series(tmp_path, monkeypatch)
    d1 = manifest["corpus"]["bronze_listing_sha256"]
    for r in records:
        obs = r["config_snapshot"]["experiment_inputs"]["corpus_observation"]
        assert obs["bronze_listing_sha256"] == d1


def test_repeat_forces_rebuild_after_first(tmp_path, monkeypatch):
    result, rec, records, manifest = _series(tmp_path, monkeypatch)
    silver = _submits(rec, "silver-build")
    assert [s[2]["LB_FORCE_REBUILD"] for s in silver] == [None, "1", "1"]


def test_repeat_datagen_once(tmp_path, monkeypatch):
    result, rec, records, manifest = _series(
        tmp_path, monkeypatch, argv=["--generate", "--yes", "--repeat", "3"]
    )
    assert result.exit_code == 0, result.output
    assert sum(1 for c in rec.calls if c[:2] == ["Datagen", "deploy"]) == 1
    assert manifest["passed"] == 3


def test_repeat_stops_on_bronze_change(tmp_path, monkeypatch):
    """One object rewritten after repetition 1 with the same size: the
    series stops with 3 before repetition 2 submits anything."""

    def rewrite(rec):
        rec.bronze_objects[f"{SCOPE}part-0001.parquet"] = (1000, "e1-rewritten")

    result, rec, records, manifest = _series(tmp_path, monkeypatch, after={1: rewrite})
    assert result.exit_code == 3, result.output
    assert len(records) == 1
    assert len(_submits(rec, "bronze-verify")) == 1
    assert manifest["stopped_reason"].startswith("series.corpus_changed")
    assert manifest["attempted"] == 1


def test_repeat_change_during_a_repetition_is_not_a_member(tmp_path, monkeypatch):
    """Bronze changes while repetition 2 runs (after its start check, before
    its save): it is saved with the corpus problem and without the inherited
    block, is not counted, and the series stops with 3."""
    from lakebench.metrics import series as series_meta

    real_seal = series_meta.SeriesContext.seal

    def seal(self, run_metrics):
        if self.index == 2:
            # The record's own observation, taken just before, saw a change.
            obs = run_metrics.config_snapshot["experiment_inputs"]["corpus_observation"]
            obs["bronze_listing_sha256"] = "f" * 64
        return real_seal(self, run_metrics)

    monkeypatch.setattr(series_meta.SeriesContext, "seal", seal)
    result, rec, records, manifest = _series(tmp_path, monkeypatch)
    assert result.exit_code == 3, result.output
    assert len(records) == 2
    rep2 = _corpus(records[1])
    assert "inherited_from" not in rep2
    assert "bronze changed during this repetition" in rep2.get("problems", [])
    assert manifest["runs"][1]["member"] is False
    assert manifest["passed"] == 1


def test_repeat_failed_rep_continues(tmp_path, monkeypatch):
    """Repetition 2's silver-build fails: repetition 3 still runs; exit 1."""
    from tests.harness import run_harness

    real_wait = run_harness.FakeMonitor.wait_for_completion
    seen = [0]

    def wait(self, *a, **k):
        name = a[0] if a else k["job_name"]
        if name == "lakebench-silver-build":
            seen[0] += 1
            self._failing = ("silver-build",) if seen[0] == 2 else ()
        return real_wait(self, *a, **k)

    monkeypatch.setattr(run_harness.FakeMonitor, "wait_for_completion", wait)
    result, rec, records, manifest = _series(tmp_path, monkeypatch)
    assert result.exit_code == 1, result.output
    assert len(records) == 3
    assert [r["verdict"] for r in manifest["runs"]] == ["PASSED", "FAILED", "PASSED"]
    assert (manifest["passed"], manifest["failed"]) == (2, 1)


def test_repeat_no_verified_corpus(tmp_path, monkeypatch):
    """Repetition 1's bronze-verify fails: one record, exit 1."""
    result, rec, records, manifest = _series(tmp_path, monkeypatch, failing=("bronze-verify",))
    assert result.exit_code == 1, result.output
    assert len(records) == 1
    assert manifest["stopped_reason"].startswith("repeat.no_verified_corpus")


def test_repeat_rep1_without_silver_stops(tmp_path, monkeypatch):
    """Repetition 1's silver-build fails (for example refused over a populated
    table): no later repetition force-rebuilds silver."""
    result, rec, records, manifest = _series(tmp_path, monkeypatch, failing=("silver-build",))
    assert result.exit_code == 1, result.output
    assert len(_submits(rec, "silver-build")) == 1
    assert "did not build silver" in manifest["stopped_reason"]


def test_repeat_empty_bronze_has_no_corpus(tmp_path, monkeypatch):
    """Nothing under the datagen scope: there is no corpus to reuse."""
    result, rec, records, manifest = _series(tmp_path, monkeypatch, objects={})
    assert result.exit_code in (1, 3), result.output
    assert len(records) == 1


def test_listing_digest_scope_is_datagen_prefix(tmp_path, monkeypatch):
    """An object outside the datagen scope (a checkpoint) changes nothing; one
    under it (a marker) stops the series."""

    def outside(rec):
        rec.bronze_objects["checkpoints/bronze-ingest/0"] = (5, "x")

    result, rec, records, manifest = _series(tmp_path, monkeypatch, after={1: outside})
    assert result.exit_code == 0, result.output

    def inside(rec):
        rec.bronze_objects[f"{SCOPE}_corpus/c000-node-0000.json"] = (5, "m")

    (tmp_path / "b").mkdir()
    result, rec, records, manifest = _series(tmp_path / "b", monkeypatch, after={1: inside})
    assert result.exit_code == 3, result.output


def test_series_config_loaded_once(tmp_path, monkeypatch):
    """The config file is edited after repetition 1: repetition 2 still runs
    the loaded config (its benchmark iterations) and records the loaded
    bytes' hash."""
    import yaml

    def edit(rec):
        cfg = tmp_path / "runchar.yaml"
        data = yaml.safe_load(cfg.read_text())
        data.setdefault("architecture", {}).setdefault("benchmark", {})["iterations"] = 5
        cfg.write_text(yaml.safe_dump(data, sort_keys=False))

    result, rec, records, manifest = _series(tmp_path, monkeypatch, after={1: edit})
    assert result.exit_code == 0, result.output
    iterations = [c[3] for c in rec.calls if c[:2] == ["Benchmark", "run_power"]]
    per_rep = len(iterations) // 3
    assert per_rep and iterations == iterations[:per_rep] * 3, iterations
    assert 5 not in iterations
    hashes = {r["config_snapshot"]["config_sha256"] for r in records}
    assert hashes == {manifest["config_sha256"]}


def test_repeat_interrupt_stops_series(tmp_path, monkeypatch, sentinel_sigterm):
    """SIGINT in repetition 2's silver-build: exit 130, repetition 3 never
    starts, the manifest says interrupted, the handlers are restored."""
    from tests.harness import run_harness

    real_wait = run_harness.FakeMonitor.wait_for_completion
    seen = [0]

    def wait(self, *a, **k):
        name = a[0] if a else k["job_name"]
        if name == "lakebench-silver-build":
            seen[0] += 1
            self._rec.interrupt = ("silver-build", "SIGINT") if seen[0] == 2 else None
        return real_wait(self, *a, **k)

    monkeypatch.setattr(run_harness.FakeMonitor, "wait_for_completion", wait)
    result, rec, records, manifest = _series(tmp_path, monkeypatch)
    assert result.exit_code == 130, result.output
    assert len(records) == 2
    assert records[1]["verdict"]["status"] == "INTERRUPTED"
    assert manifest["stopped_reason"] == "interrupted"
    assert manifest["passed"] == 1
    assert signal.getsignal(signal.SIGTERM) is sentinel_sigterm
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler


def test_a_late_signal_in_a_failed_rep_stops_the_series(tmp_path, monkeypatch):
    """A rep whose record carries `interrupted` stops the series even when its
    exit code is not 130 (a failed run keeps its own exit)."""
    from lakebench.metrics import series as series_meta

    real_seal = series_meta.SeriesContext.seal

    def seal(self, run_metrics):
        if self.index == 1:
            run_metrics.interrupted = {
                "signal": "SIGINT",
                "at_stage": "results",
                "prior_failure": True,
            }
        return real_seal(self, run_metrics)

    monkeypatch.setattr(series_meta.SeriesContext, "seal", seal)
    result, rec, records, manifest = _series(tmp_path, monkeypatch)
    assert result.exit_code == 130, result.output
    assert len(records) == 1


@pytest.mark.parametrize(
    "argv, message",
    [
        (["--continuous", "--repeat", "2"], "--repeat does not apply to a continuous run"),
        (["--stage", "silver-build", "--repeat", "2"], "--repeat runs the whole batch pipeline"),
        (["--local", "--repeat", "2"], "--repeat runs the whole batch pipeline"),
    ],
)
def test_repeat_refusals(tmp_path, monkeypatch, argv, message):
    result, rec, records, manifest = _series(tmp_path, monkeypatch, argv=[*argv, "--yes"])
    assert result.exit_code == 2, result.output
    assert message in result.output
    assert records == [] and manifest is None and rec.submits == []


def test_repeat_refused_multicycle(tmp_path, monkeypatch):
    from tests.harness.run_harness import base_config

    result, rec, records, manifest = _series(
        tmp_path,
        monkeypatch,
        config=base_config(architecture={"pipeline": {"mode": "batch", "cycles": 2}}),
    )
    assert result.exit_code == 2, result.output
    assert "--repeat does not apply to a multi-cycle run" in result.output
    assert records == []


def test_repeat_out_of_range_is_a_usage_error(tmp_path, monkeypatch):
    result, rec, records, manifest = _series(
        tmp_path, monkeypatch, argv=["--skip-generate", "--yes", "--repeat", "21"]
    )
    assert result.exit_code == 2
    assert records == []


def test_no_repeat_writes_no_series(tmp_path, monkeypatch):
    """A plain run is unchanged: no series field, no manifest."""
    result, rec, records, manifest = _series(
        tmp_path, monkeypatch, argv=["--skip-generate", "--yes"]
    )
    assert result.exit_code == 0, result.output
    assert len(records) == 1 and "series" not in records[0]
    assert manifest is None
    assert not Path(tmp_path / "lakebench-output" / "series").exists()
